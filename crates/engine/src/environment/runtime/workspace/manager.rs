//! Workspace manager — per-node reconcile (H3).
//!
//! The run loop drives reconciliation for a `Subprocess`-backend node whose
//! effective env binds the workspace with `WorkspaceBinding::Sync`:
//!
//! - `reconcile_in` (host→remote, per attempt): resets the env-side tree to
//!   mirror the host (upload host files + delete remote-only extras) and
//!   records per-key [`WorkspaceState`]. Every non-`Sync` binding delegates to
//!   `dispatcher.translate_path` (behaviour unchanged for
//!   Local/WSL/BindMount/Shared/Translated).
//! - `reconcile_out` (remote→host, after the final attempt): writes changed/new
//!   files back and propagates remote deletions (`None`/`Force`), advancing the
//!   baseline. Skipped only on a genuine user cancel.
//! - `teardown_all`: a `Force`-only write-back safety net for runs that panic
//!   between `reconcile_in` and `reconcile_out` (non-user-cancel only) plus
//!   deletion of every ephemeral root tracked during the run.
//!
//! Same-key concurrency is serialised by a per-key execution lease the run loop
//! holds across a node's reconcile cycle.
//!
//! Not yet implemented (deferred):
//! - Persistent workspace reuse (template without `{{run.id}}`) — H5.
//! - `SafeOrDiverge` write-back (rejected in `reconcile_in`, before any upload)
//!   — next phase.
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex as SyncMutex;
use tokio::sync::OwnedMutexGuard;

use crate::environment::runtime::dispatcher::Dispatcher;
use crate::environment::runtime::env::{EnvId, WorkspaceBinding, WriteBackPolicy};
use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::transport::EnvPath;

use super::safety;
use super::transport::{FileKind, WorkspaceTransport, WorkspaceTransportFactory};

// ── Type aliases ──────────────────────────────────────────────────────────────

/// Identity of one synced workspace; lease and state are keyed by it.
pub(crate) type WorkspaceKey = (EnvId, PathBuf);

/// Inner map type for the per-key execution lease registry.
type LeaseMap = HashMap<WorkspaceKey, Arc<tokio::sync::Mutex<()>>>;

// ── Types ─────────────────────────────────────────────────────────────────────

/// RAII guard for a node's exclusive execution cycle on one workspace key.
///
/// Held for the duration of a node's reconcile cycle; a second call to
/// [`WorkspaceManager::acquire_execution_lease`] with the same key blocks
/// until this guard is dropped.  Distinct keys never contend.
pub struct WorkspaceExecutionLease {
    _guard: OwnedMutexGuard<()>,
}

/// Terminal classification handed to [`WorkspaceManager::teardown_all`]
/// so write-back/cleanup policy can branch on how the run ended.
///
/// Derived from the run's terminal status (or a panic/cancel signal
/// when the run loop unwinds before producing a `RunSummary`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// Clean completion (`status == "done"`).
    Completed,
    /// Node failure or stall (`status == "error"`), or a panic.
    Failed,
    /// User cancellation (`status == "stopped"`).
    CancelledByUser,
}

/// Per-node lifecycle of the uploaded workspace.
///
/// Only `Ephemeral` (root contains `{{run.id}}`, unique per run, deleted on
/// teardown) is supported in H3. Stable/persistent roots are rejected by
/// [`lifecycle_of`] and land in H5, which will add the corresponding variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    /// Workspace root contains `{{run.id}}` — unique per run, deleted on teardown.
    Ephemeral,
}

/// Per-key state for the per-node reconcile machinery (H3+).
///
/// Populated by `reconcile_in` and read by `reconcile_out` for write-back
/// delta diffing. Stored in [`WorkspaceManager::state`] keyed by
/// [`WorkspaceKey`].
struct WorkspaceState {
    /// Absolute env-side root for this key (already expanded from the template).
    env_side_root: String,
    /// Whether the root is unique per-run (Ephemeral) or stable (Persistent).
    lifecycle: Lifecycle,
    /// Factory for reopening a transport during reconcile phases.
    transport_factory: Arc<dyn WorkspaceTransportFactory>,
    /// Write-back policy for this workspace.
    write_back: WriteBackPolicy,
    /// Snapshot of the remote manifest as of the last `reconcile_in`/`reconcile_out`.
    /// Used as the write-back baseline by `reconcile_out`.
    last_remote_manifest: safety::Manifest,
}

impl std::fmt::Debug for WorkspaceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkspaceState")
            .field("env_side_root", &self.env_side_root)
            .field("lifecycle", &self.lifecycle)
            .field("write_back", &self.write_back)
            .field(
                "last_remote_manifest_files",
                &self.last_remote_manifest.files.len(),
            )
            .finish_non_exhaustive()
    }
}

// ── Run scope ─────────────────────────────────────────────────────────────────

/// Lightweight view of the current run's identity; passed to
/// `reconcile_in` so it can expand `env_path_template`.
pub struct RunScope<'a> {
    /// Stable run identifier.
    pub run_id: &'a str,
    /// Workflow id.
    pub workflow_id: &'a str,
    /// Human-readable workflow name.
    pub workflow_name: &'a str,
    /// ISO-8601 run start time.
    pub started_at_iso: &'a str,
}

// ── Manager ───────────────────────────────────────────────────────────────────

/// Run-tree-scoped owner of workspace sync policy.
///
/// Holds the per-key execution leases and the per-key reconcile [`WorkspaceState`]
/// (populated by `reconcile_in`, drained by `teardown_all`). Each map's `parking_lot`
/// lock is only held long enough to read/insert an entry — never across transport I/O.
pub struct WorkspaceManager {
    /// Per-key execution lease registry.  The `parking_lot` (sync) map lock is
    /// only held long enough to clone the `Arc`; the async `tokio::sync::Mutex`
    /// inside each entry serialises concurrent reconcile cycles for the same key.
    leases: SyncMutex<LeaseMap>,

    /// Per-key reconcile state for H3 per-node workspace reconciliation.
    /// Populated by `reconcile_in` and consumed by `reconcile_out`.
    state: SyncMutex<HashMap<WorkspaceKey, WorkspaceState>>,

    /// Every ephemeral env-side root created this run, keyed by root → factory.
    ///
    /// `state` is keyed by `(EnvId, host_ws)`, so `parallel`/`compose` children
    /// that inherit the parent `host_ws` but run under distinct `run_id`s collapse
    /// onto one `state` entry — each `reconcile_in` overwrites the prior. This map
    /// records *every* distinct ephemeral root so `teardown_all` deletes them all,
    /// not just the last; using the root string as the key dedups `loop_for`'s
    /// repeated same-root inserts.
    ephemeral_roots: SyncMutex<HashMap<String, Arc<dyn WorkspaceTransportFactory>>>,

    /// Test-only seam: records the last [`RunOutcome`] passed to
    /// [`Self::teardown_all`]. Lets run-loop tests observe that
    /// teardown fired with the correct outcome on every exit path.
    #[cfg(any(test, feature = "testing"))]
    pub last_outcome: std::sync::Mutex<Option<RunOutcome>>,
}

impl std::fmt::Debug for WorkspaceManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `ephemeral_roots`' factory values are not `Debug`; report a count.
        f.debug_struct("WorkspaceManager")
            .field("leases", &self.leases)
            .field("state", &self.state)
            .field("ephemeral_roots_len", &self.ephemeral_roots.lock().len())
            .finish_non_exhaustive()
    }
}

impl Default for WorkspaceManager {
    fn default() -> Self {
        Self {
            leases: SyncMutex::new(HashMap::new()),
            state: SyncMutex::new(HashMap::new()),
            ephemeral_roots: SyncMutex::new(HashMap::new()),
            #[cfg(any(test, feature = "testing"))]
            last_outcome: std::sync::Mutex::new(None),
        }
    }
}

impl WorkspaceManager {
    /// Construct a new `WorkspaceManager`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquire an exclusive execution lease for `key`.
    ///
    /// Returns a RAII [`WorkspaceExecutionLease`] guard.  A second call with the
    /// same key blocks until the first guard is dropped.  Calls with distinct
    /// keys proceed independently.
    ///
    /// # Ordering contract
    ///
    /// The `parking_lot` sync lock on `leases` is dropped **before** the
    /// `.await` — a sync guard is never held across an await point.
    pub async fn acquire_execution_lease(&self, key: WorkspaceKey) -> WorkspaceExecutionLease {
        let m = {
            let mut leases = self.leases.lock(); // parking_lot — sync; dropped before await
            Arc::clone(
                leases
                    .entry(key)
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
            )
        }; // sync guard dropped here
        WorkspaceExecutionLease {
            _guard: m.lock_owned().await,
        }
    }

    /// Tear down every workspace prepared during the run.
    ///
    /// Fires on every run-loop exit path (success, error, or panic), before the
    /// engine's sender/token/lock cleanup. For each prepared workspace it writes
    /// changed files back to the host (only for [`WriteBackPolicy::Force`],
    /// skipped entirely on user cancel) and deletes every tracked ephemeral
    /// env-side root (not just the last per key).
    ///
    /// Best-effort and panic-free: per-env errors are logged and swallowed so a
    /// failure on one workspace never aborts cleanup of the others nor unwinds
    /// into the run-loop teardown path.
    pub async fn teardown_all(&self, outcome: RunOutcome) {
        #[cfg(any(test, feature = "testing"))]
        {
            *self.last_outcome.lock().unwrap() = Some(outcome);
        }

        // Drain the per-key state — teardown owns these entries; nothing reads
        // `state` after the run loop exits.
        let states: Vec<(WorkspaceKey, WorkspaceState)> = {
            std::mem::take(&mut *self.state.lock())
                .into_iter()
                .collect()
        };

        // Write-back safety net: a node may have panicked between reconcile_in
        // and reconcile_out, leaving changes unwritten. Fire the write-back ONLY
        // for `Force` — `None` never writes back, and `SafeOrDiverge` is rejected
        // up front by `reconcile_in` (so it can never reach here, and must never
        // be force-written). User cancel skips entirely. No-op when reconcile_out
        // already advanced the baseline.
        for (key, s) in states {
            if outcome != RunOutcome::CancelledByUser
                && let WriteBackPolicy::Force { ignore } = &s.write_back
                && let Err(e) = write_back_delta(
                    &s.transport_factory,
                    &s.env_side_root,
                    &key.1,
                    &s.last_remote_manifest,
                    ignore,
                )
                .await
            {
                tracing::warn!(
                    env_root = %s.env_side_root,
                    error = %e,
                    "teardown write-back failed"
                );
            }
        }

        // Ephemeral cleanup: delete *every* root recorded this run, not just the
        // last per key. parallel/compose children share host_ws (one `state`
        // entry) but each gets its own run-id root — all are tracked here.
        let roots: Vec<(String, Arc<dyn WorkspaceTransportFactory>)> = {
            std::mem::take(&mut *self.ephemeral_roots.lock())
                .into_iter()
                .collect()
        };
        for (root, factory) in roots {
            if let Err(e) = remove_tree(factory.as_ref(), &root).await {
                tracing::warn!(env_root = %root, error = %e, "ephemeral teardown failed");
            }
        }
    }

    /// Reset the env-side workspace to mirror the host, returning the env-side cwd.
    ///
    /// Host→remote, run before each attempt of a synced node. For a
    /// `Sync { strategy: Sftp }` binding it expands the env-path template,
    /// classifies its lifecycle (persistent is deferred to H5), makes the remote
    /// tree byte-for-byte equal to the host tree, and records per-key
    /// [`WorkspaceState`] so [`Self::reconcile_out`] can diff against it.
    ///
    /// Every other binding delegates to `dispatcher.translate_path` — behaviour
    /// is unchanged for `Shared`/`Translated`/`BindMount`/`Unsupported` and for
    /// `Sync { strategy: Rsync }` (which `sync_params` rejects).
    ///
    /// On an upload error for an ephemeral root, the partial remote tree is
    /// removed best-effort before the error propagates.
    ///
    /// # Concurrency
    ///
    /// The `parking_lot` `state` lock is only ever held to insert the final
    /// `WorkspaceState`; all transport I/O happens outside it. No sync guard is
    /// held across an `.await`.
    pub async fn reconcile_in(
        &self,
        dispatcher: &dyn Dispatcher,
        binding: &WorkspaceBinding,
        host_ws: &Path,
        run: &RunScope<'_>,
    ) -> Result<EnvPath, DispatchError> {
        // Non-Sync (or Rsync → Err) bindings: unchanged translate_path behaviour.
        let Some((tmpl, write_back)) = sync_params(binding)? else {
            return dispatcher.translate_path(host_ws);
        };

        // SafeOrDiverge write-back is deferred to a later phase. Reject it BEFORE
        // any upload (restoring the pre-H3 `resolve_cwd` gate): the node fails
        // before running and no `WorkspaceState` is stored, so the `teardown_all`
        // safety net can never force-write it.
        if matches!(write_back, WriteBackPolicy::SafeOrDiverge { .. }) {
            return Err(DispatchError::Unsupported(
                "SafeOrDiverge write-back is deferred to a later phase".into(),
            ));
        }

        let root = expand_env_root(tmpl, run, host_ws)?;
        let lifecycle = lifecycle_of(tmpl)?; // Persistent => Err(Unsupported) (H5)

        let factory = dispatcher.workspace_transport().ok_or_else(|| {
            DispatchError::Unsupported("environment has no workspace transport".into())
        })?;

        let uploaded = match reset_remote_to_host(&factory, &root, host_ws).await {
            Ok(manifest) => manifest,
            Err(e) => {
                // Best-effort cleanup of a partial ephemeral root before bubbling.
                if lifecycle == Lifecycle::Ephemeral
                    && let Err(cleanup_err) = remove_tree(factory.as_ref(), &root).await
                {
                    tracing::warn!(
                        env_root = root,
                        error = %cleanup_err,
                        "failed to remove partial reconcile root after reset error"
                    );
                }
                return Err(e);
            },
        };

        let key = (dispatcher.info().id.clone(), host_ws.to_path_buf());
        {
            let mut st = self.state.lock(); // parking_lot — no await held
            st.insert(
                key,
                WorkspaceState {
                    env_side_root: root.clone(),
                    lifecycle,
                    transport_factory: Arc::clone(&factory),
                    write_back: write_back.clone(),
                    last_remote_manifest: uploaded,
                },
            );
        }

        // Record the ephemeral root so teardown deletes *every* root for this key,
        // not just the last (parallel/compose children share host_ws but get
        // distinct run-id roots — the `state` entry above only keeps the latest).
        // The reset succeeded, so the root really exists on the remote. Sync
        // insert; the guard drops before the return — no await held.
        if lifecycle == Lifecycle::Ephemeral {
            self.ephemeral_roots
                .lock()
                .insert(root.clone(), Arc::clone(&factory));
        }

        Ok(EnvPath::new(root))
    }

    /// Write back remote changes + propagate remote deletions for a synced node.
    ///
    /// Remote→host, run after the final attempt. Diffs the current remote tree
    /// against the baseline stored by [`Self::reconcile_in`], writes
    /// changed/new files into the host workspace, deletes host files the node
    /// removed remotely, then advances the stored baseline.
    ///
    /// A no-op when the binding needs no sync, when no [`WorkspaceState`] exists
    /// for the key (e.g. `reconcile_in` was never called / already torn down),
    /// or when the policy is [`WriteBackPolicy::None`]. `SafeOrDiverge` is now
    /// rejected earlier by [`Self::reconcile_in`] (before any upload, so no state
    /// is stored) — the arm here is defensive and unreachable.
    ///
    /// # Concurrency
    ///
    /// The `state` lock is taken only to clone the baseline out and (later) to
    /// store the advanced one — never held across the transport I/O between.
    pub async fn reconcile_out(
        &self,
        dispatcher: &dyn Dispatcher,
        binding: &WorkspaceBinding,
        host_ws: &Path,
    ) -> Result<(), DispatchError> {
        // Non-Sync (or Rsync → Err) bindings need no write-back.
        if sync_params(binding)?.is_none() {
            return Ok(());
        }

        let key = (dispatcher.info().id.clone(), host_ws.to_path_buf());

        // Extract everything we need by clone, then DROP the lock before awaiting.
        let Some((root, factory, write_back, baseline)) = ({
            let st = self.state.lock(); // parking_lot — dropped before await
            st.get(&key).map(|s| {
                (
                    s.env_side_root.clone(),
                    Arc::clone(&s.transport_factory),
                    s.write_back.clone(),
                    s.last_remote_manifest.clone(),
                )
            })
        }) else {
            return Ok(()); // no state for this key — nothing to reconcile
        };

        let ignore = match &write_back {
            WriteBackPolicy::None => return Ok(()),
            WriteBackPolicy::Force { ignore } => ignore.clone(),
            // Defensive + unreachable: reconcile_in rejects SafeOrDiverge before
            // storing any state, so no SafeOrDiverge state can reach here.
            WriteBackPolicy::SafeOrDiverge { .. } => {
                return Err(DispatchError::Unsupported(
                    "SafeOrDiverge write-back is deferred to a later phase".into(),
                ));
            },
        };

        let new_remote = write_back_delta(&factory, &root, host_ws, &baseline, &ignore).await?;

        // Advance the baseline so the next reconcile_out diffs against this state.
        if let Some(s) = self.state.lock().get_mut(&key) {
            s.last_remote_manifest = new_remote;
        }
        Ok(())
    }
}

// ── Teardown helpers ──────────────────────────────────────────────────────────

/// Make the remote tree at `root` byte-for-byte equal to the host tree at
/// `host_ws`, returning a [`safety::Manifest`] of the bytes uploaded.
///
/// Host→remote reset, run before each attempt by [`WorkspaceManager::reconcile_in`].
/// Opens its own transport from `factory` (never holds a `&dyn` across an
/// `.await`).  The returned manifest hashes the EXACT bytes sent — re-reading
/// the host file to build it would reopen the TOCTOU window H2 already closed.
///
/// Reset order is delete-before-upload so a prior attempt's cruft (extra files,
/// type-mismatched dirs) can never survive, and so a remote symlink can never
/// redirect the parent-`mkdir`/`rename` inside `upload_file` outside `root`:
/// 1. `mkdir(root)`.
/// 2. Walk the host workspace → the target file and directory sets (ignores applied).
/// 3. List the current remote tree.
/// 4. Strip-guard every remote entry against `root`; reject any off-root path.
/// 5. Delete (before any upload): every symlink (target-path OR intermediate
///    dir — either could redirect a later write), every non-target file, and
///    every directory whose rel collides with a target *file*. Cruft dirs are
///    pruned best-effort, deepest-first.
/// 6. Upload every target file via [`safety::read_within_caps`] (caps enforced
///    on the bytes actually read) and `mkdir` every target directory, recording
///    both the sent file bytes and the directory rels in the manifest.
async fn reset_remote_to_host(
    factory: &Arc<dyn WorkspaceTransportFactory>,
    root: &str,
    host_ws: &Path,
) -> Result<safety::Manifest, DispatchError> {
    let t = factory.open().await?;
    t.mkdir(root).await?;

    // 2. Target sets from the host walk (forward-slash rels, ignores applied).
    // Files and dirs are tracked separately so the delete pass can tell a stale
    // remote file from a directory the host also has at the same rel.
    let entries = safety::walk_workspace(host_ws)?;
    let target_files: HashSet<&str> = entries
        .iter()
        .filter(|e| e.kind == safety::EntryKind::File)
        .map(|e| e.rel_path.as_str())
        .collect();
    let target_dirs: HashSet<&str> = entries
        .iter()
        .filter(|e| e.kind == safety::EntryKind::Dir)
        .map(|e| e.rel_path.as_str())
        .collect();

    // 3. Current remote tree.
    let remote = t.list_tree(root).await?;
    let prefix = format!("{root}/");

    // 4 + 5 (delete pass): classify each remote entry by stripping the root
    // prefix, then queue deletions. Files/symlinks are unlinked immediately;
    // directories are collected and removed deepest-first afterwards so each is
    // empty when removed.
    let mut cruft_dirs: Vec<String> = Vec::new();
    for entry in &remote {
        // The root dir itself is legitimate (some transports list it); it is
        // neither a target nor cruft — skip without flagging it off-root.
        if entry.rel_path == root {
            continue;
        }
        let Some(rel) = entry.rel_path.strip_prefix(&prefix) else {
            // An entry that neither equals `root` nor sits under `root/` escaped
            // the synced root — never act on it.
            return Err(DispatchError::WorkspaceUnavailable {
                env_id: "<remote>".into(),
                reason: format!(
                    "remote listing entry `{}` is outside reconcile root `{root}`",
                    entry.rel_path
                ),
            });
        };
        if !safety::is_safe_relative(rel) {
            return Err(DispatchError::WorkspaceUnavailable {
                env_id: "<remote>".into(),
                reason: format!(
                    "remote listing entry `{}` is an unsafe path",
                    entry.rel_path
                ),
            });
        }

        match entry.kind {
            // A symlink anywhere — at a target rel OR on an intermediate dir —
            // would let the parent-mkdir/rename in `upload_file` escape `root`
            // via the link target. Always unlink it (no-follow).
            FileKind::Symlink => t.remove_file(&entry.rel_path).await?,
            // A remote regular file that is not a target file is cruft from a
            // failed prior attempt.
            FileKind::File if !target_files.contains(rel) => {
                t.remove_file(&entry.rel_path).await?;
            },
            FileKind::File => {},
            // A directory whose rel collides with a target *file* path must go
            // (we are about to upload a file there). Other cruft dirs are pruned
            // best-effort below.
            FileKind::Dir => cruft_dirs.push(entry.rel_path.clone()),
        }
    }

    // Remove colliding/cruft directories deepest-first (longest path first) so
    // children are gone before their parents. A remote dir whose rel matches a
    // target dir is legitimate and kept (the upload mirrors it). Best-effort: a
    // dir that still has legitimate children (a target file lives under it) will
    // fail to remove — that is expected, so swallow per-dir errors here.
    cruft_dirs.sort_by_key(|d| std::cmp::Reverse(d.len()));
    for dir in cruft_dirs {
        let rel = dir.strip_prefix(&prefix).unwrap_or(dir.as_str());
        if target_dirs.contains(rel) {
            continue; // host also has this dir — keep it
        }
        let collides_with_file = target_files.contains(rel);
        // A dir whose rel collides with a target *file* MUST be cleared — the
        // upload would otherwise fail. Any other removal failure is a
        // non-colliding cruft dir still holding legitimate children: leave it.
        if let Err(e) = t.remove_dir(&dir).await
            && collides_with_file
        {
            return Err(e);
        }
    }

    // 6. Mirror the host tree. The walk is sorted, so directories appear before
    // anything nested under them: create each remote dir (records it in the
    // manifest so reconcile can track empty/parent dirs) and upload each file,
    // cap-checking the bytes actually read (bounded) and hashing the sent bytes
    // into the returned manifest. Write-back stays files-only for now (dir
    // create/prune lands in a later task).
    let mut tracker = safety::CapTracker::new(safety::UploadCaps::default());
    let mut manifest = safety::Manifest::new();
    for entry in &entries {
        match entry.kind {
            safety::EntryKind::Dir => {
                t.mkdir(&format!("{root}/{}", entry.rel_path)).await?;
                manifest.dirs.insert(entry.rel_path.clone());
            },
            safety::EntryKind::File => {
                let bytes = safety::read_within_caps(&entry.abs, &mut tracker)?;
                let remote_path = format!("{root}/{}", entry.rel_path);
                t.upload_file(&remote_path, &bytes).await?;
                manifest.files.insert(
                    entry.rel_path.clone(),
                    safety::FileEntry {
                        sha256_hex: safety::sha256_hex(&bytes),
                        size: bytes.len() as u64,
                        mode: entry.mode,
                    },
                );
            },
        }
    }
    Ok(manifest)
}

/// One regular file from a [`RemoteListing`]: its root-stripped rel, downloaded
/// bytes, and the [`safety::FileEntry`] hashed from those bytes.
struct RemoteFile {
    rel: String,
    bytes: Vec<u8>,
    entry: safety::FileEntry,
}

/// A fully-downloaded snapshot of one remote tree, classified by kind.
///
/// Built only from a transport listing that succeeded end to end: every regular
/// file was listed AND downloaded. A partial failure aborts via `?` so a
/// transport error can never read as "entry absent" (which would drive a
/// spurious host deletion — data loss).
struct RemoteListing {
    /// Regular files (rel root-stripped), with bytes + per-file metadata.
    files: Vec<RemoteFile>,
    /// Directory rels (root-stripped).
    dirs: std::collections::BTreeSet<String>,
    /// Symlink rels (root-stripped). Used to shadow deletions: a host rel under
    /// a remote symlink is not really gone, just hidden by the link.
    symlinks: std::collections::BTreeSet<String>,
}

/// List `root` via a fresh transport and classify every entry under it.
///
/// Strips the `{root}/` prefix and drops the root entry itself (transports
/// differ on whether they list it). Unsafe (`..`/absolute) rels are skipped.
/// Regular files are downloaded and hashed into [`RemoteFile`]; dirs and
/// symlinks are recorded by rel. `list_tree` / `download_file` errors PROPAGATE
/// via `?`: a transport failure must NEVER be treated as "absent".
async fn list_remote_files(
    t: Box<dyn WorkspaceTransport>,
    root: &str,
) -> Result<RemoteListing, DispatchError> {
    let prefix = format!("{root}/");
    let entries = t.list_tree(root).await?;

    let mut listing = RemoteListing {
        files: Vec::new(),
        dirs: std::collections::BTreeSet::new(),
        symlinks: std::collections::BTreeSet::new(),
    };

    for entry in &entries {
        // Drop the root entry itself; only its contents are reconciled.
        if entry.rel_path == root {
            continue;
        }
        let Some(rel) = entry.rel_path.strip_prefix(&prefix) else {
            continue; // defensive: outside the root — ignore
        };
        if !safety::is_safe_relative(rel) {
            continue;
        }
        match entry.kind {
            FileKind::File => {
                let bytes = t.download_file(&entry.rel_path).await?;
                let sha256_hex = safety::sha256_hex(&bytes);
                let size = bytes.len() as u64;
                listing.files.push(RemoteFile {
                    rel: rel.to_string(),
                    bytes,
                    entry: safety::FileEntry {
                        sha256_hex,
                        size,
                        mode: entry.mode,
                    },
                });
            },
            FileKind::Dir => {
                listing.dirs.insert(rel.to_string());
            },
            FileKind::Symlink => {
                listing.symlinks.insert(rel.to_string());
            },
        }
    }

    Ok(listing)
}

/// Whether `rel` is shadowed by some remote symlink in `symlinks` — i.e. the rel
/// equals a symlink, or sits under a symlinked directory. A shadowed host rel is
/// not really "deleted on the remote", just hidden by the link, so its host file
/// or directory must be left alone.
fn is_shadowed_by_symlink<'a>(
    rel: &str,
    symlinks: &'a std::collections::BTreeSet<String>,
) -> Option<&'a String> {
    symlinks
        .iter()
        .find(|s| rel == s.as_str() || rel.starts_with(&format!("{s}/")))
}

/// Propagate remote changes at `root` back to the host workspace at `host_ws`,
/// returning the new remote manifest (the advanced write-back baseline).
///
/// Remote→host delta, run after the final attempt by
/// [`WorkspaceManager::reconcile_out`].  Opens its own transport.  Diffs the
/// current remote tree against `baseline`:
/// - A remote regular file absent from `baseline`, or whose hash differs, is
///   written to the host (atomic, guarded against traversal / host symlinks /
///   ignore globs).
/// - A rel present in `baseline` but absent from the remote is *deleted* on the
///   host (same guards; best-effort per-file so one failed unlink does not
///   abort the rest), UNLESS the rel is shadowed by a remote symlink — a node
///   replacing a file/dir with a symlink at the same rel is not a deletion.
/// - Directories the remote gained are created on the host; directories that
///   became empty (in `baseline.dirs`, gone from the remote) are pruned
///   deepest-first with `remove_dir` (never `remove_dir_all`), so an untracked
///   host file keeps its directory alive.
///
/// `list_tree` / `download_file` errors PROPAGATE via `?`: a transport failure
/// must never read as "file absent", which would trigger a spurious host
/// deletion (data loss). Only a fully-successful listing drives deletions.
async fn write_back_delta(
    factory: &Arc<dyn WorkspaceTransportFactory>,
    root: &str,
    host_ws: &Path,
    baseline: &safety::Manifest,
    ignore: &[String],
) -> Result<safety::Manifest, DispatchError> {
    let t = factory.open().await?;

    // 1. Build the new remote snapshot from a fully-successful listing +
    // downloads. Any transport error here aborts the whole write-back (no
    // spurious deletions). Files, dirs, and symlinks are all classified.
    let listing = list_remote_files(t, root).await?;

    // Collect the new remote manifest (the advanced baseline) and write changed
    // / new files to the host under the same guards as H2 write-back.
    let mut new_remote = safety::Manifest::new();
    new_remote.dirs.clone_from(&listing.dirs);
    for f in &listing.files {
        let rel = f.rel.as_str();
        new_remote.files.insert(rel.to_string(), f.entry.clone());

        // 2. Write changed / new files to the host. Skip silently on a guard
        // failure (host symlink traversal / ignore glob).
        if !safety::host_target_is_symlink_safe(host_ws, rel) {
            tracing::warn!(
                rel,
                env_root = root,
                "skipping write-back: host path traverses a symlink"
            );
            continue;
        }
        if safety::should_ignore(rel, ignore) {
            continue;
        }
        let changed = baseline
            .files
            .get(rel)
            .is_none_or(|meta| meta.sha256_hex != f.entry.sha256_hex);
        if changed {
            write_host_file_atomic(host_ws, rel, &f.bytes)?;
        }
    }

    // 3. File-deletion propagation: a rel in `baseline.files` but absent from the
    // new remote was deleted by the node — mirror that on the host, under
    // identical guards. A rel shadowed by a remote symlink is NOT a deletion (the
    // node replaced the file/dir with a link), so skip it. Best-effort: a failed
    // unlink is logged and skipped, never aborting.
    for rel in baseline.files.keys() {
        if new_remote.files.contains_key(rel) {
            continue;
        }
        if let Some(s) = is_shadowed_by_symlink(rel, &listing.symlinks) {
            tracing::warn!(
                rel,
                env_root = root,
                symlink = %s,
                "skipping host deletion: rel is shadowed by a remote symlink"
            );
            continue;
        }
        if !safety::is_safe_relative(rel)
            || !safety::host_target_is_symlink_safe(host_ws, rel)
            || safety::should_ignore(rel, ignore)
        {
            continue;
        }
        let target = host_ws.join(rel);
        match std::fs::remove_file(&target) {
            Ok(()) => {},
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {},
            Err(e) => {
                tracing::warn!(
                    rel,
                    env_root = root,
                    error = %e,
                    "failed to propagate remote deletion to host (best-effort)"
                );
            },
        }
    }

    // 4 + 5. Mirror directory adds/removes onto the host (create gained dirs,
    // prune emptied ones deepest-first), honouring the symlink-shadow guard.
    reconcile_host_dirs(
        host_ws,
        root,
        baseline,
        &new_remote,
        &listing.symlinks,
        ignore,
    );

    Ok(new_remote)
}

/// Create directories the remote gained and prune host directories that the
/// remote dropped, mirroring `baseline.dirs` → `new_remote.dirs`.
///
/// - **Create:** dirs in `new_remote.dirs − baseline.dirs`, shallow-first,
///   via `create_dir_all` under the host-symlink guard.
/// - **Prune:** dirs in `baseline.dirs − new_remote.dirs`, deepest-first, via
///   `remove_dir` (NEVER `remove_dir_all`) so an untracked host file keeps its
///   directory alive; `DirectoryNotEmpty` / `NotFound` are silent skips.
///
/// Both passes skip a rel shadowed by a remote symlink (the node replaced the
/// dir with a link, not a deletion) and apply the same
/// `is_safe_relative` / `host_target_is_symlink_safe` / `should_ignore` guards
/// used for files. All filesystem errors are best-effort (warn, never abort).
fn reconcile_host_dirs(
    host_ws: &Path,
    root: &str,
    baseline: &safety::Manifest,
    new_remote: &safety::Manifest,
    symlinks: &std::collections::BTreeSet<String>,
    ignore: &[String],
) {
    // Create gained dirs shallow-first (lexicographic — parents precede children).
    let mut new_dirs: Vec<&String> = new_remote.dirs.difference(&baseline.dirs).collect();
    new_dirs.sort_unstable();
    for rel in new_dirs {
        if !safety::is_safe_relative(rel)
            || safety::should_ignore(rel, ignore)
            || is_shadowed_by_symlink(rel, symlinks).is_some()
        {
            continue;
        }
        if !safety::host_target_is_symlink_safe(host_ws, rel) {
            tracing::warn!(
                rel,
                env_root = root,
                "skipping dir create: host path traverses a symlink"
            );
            continue;
        }
        if let Err(e) = std::fs::create_dir_all(host_ws.join(rel)) {
            tracing::warn!(
                rel,
                env_root = root,
                error = %e,
                "failed to create host dir on write-back (best-effort)"
            );
        }
    }

    // Prune emptied dirs deepest-first (longest path first).
    let mut gone_dirs: Vec<&String> = baseline.dirs.difference(&new_remote.dirs).collect();
    gone_dirs.sort_unstable_by_key(|d| std::cmp::Reverse(d.len()));
    for rel in gone_dirs {
        if let Some(s) = is_shadowed_by_symlink(rel, symlinks) {
            tracing::warn!(
                rel,
                env_root = root,
                symlink = %s,
                "skipping host dir prune: rel is shadowed by a remote symlink"
            );
            continue;
        }
        if !safety::is_safe_relative(rel)
            || !safety::host_target_is_symlink_safe(host_ws, rel)
            || safety::should_ignore(rel, ignore)
        {
            continue;
        }
        match std::fs::remove_dir(host_ws.join(rel)) {
            Ok(()) => {},
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::DirectoryNotEmpty | std::io::ErrorKind::NotFound
                ) => {},
            Err(e) => {
                tracing::warn!(
                    rel,
                    env_root = root,
                    error = %e,
                    "failed to prune empty host dir on write-back (best-effort)"
                );
            },
        }
    }
}

/// Recursively remove `root` and everything under it via the transport: files
/// first, then directories deepest-first, then the root itself.
async fn remove_tree(
    factory: &dyn WorkspaceTransportFactory,
    root: &str,
) -> Result<(), DispatchError> {
    let t = factory.open().await?;
    let entries = t.list_tree(root).await?;

    // Remove every non-directory entry (regular files, symlinks) first.
    for entry in &entries {
        if entry.kind != FileKind::Dir {
            t.remove_file(&entry.rel_path).await?;
        }
    }

    // Collect directories from the listing plus the root itself, dedup, and
    // remove deepest-first so each is empty when removed. Including `root`
    // explicitly covers transports whose `list_tree` lists only the contents of
    // `root` (the real SFTP transport) rather than `root` itself.
    let mut dirs: Vec<String> = entries
        .into_iter()
        .filter(|e| e.kind == FileKind::Dir)
        .map(|e| e.rel_path)
        .collect();
    dirs.push(root.to_string());
    dirs.sort_unstable();
    dirs.dedup();
    dirs.sort_by_key(|d| std::cmp::Reverse(d.len())); // deepest (longest) path first
    for dir in dirs {
        t.remove_dir(&dir).await?;
    }
    Ok(())
}

/// Write `bytes` to `host_ws/rel` via a temp sibling + atomic rename, creating
/// parent directories as needed.
fn write_host_file_atomic(host_ws: &Path, rel: &str, bytes: &[u8]) -> Result<(), DispatchError> {
    let target = host_ws.join(rel);
    if let Some(parent) = target.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| host_io_err(parent, "create parent dir", &e))?;
    }
    let tmp = tmp_sibling(&target);
    std::fs::write(&tmp, bytes).map_err(|e| host_io_err(&tmp, "write temp file", &e))?;
    std::fs::rename(&tmp, &target).map_err(|e| host_io_err(&target, "rename into place", &e))?;
    Ok(())
}

/// A sibling temp path next to `target` (same directory → atomic rename).
fn tmp_sibling(target: &Path) -> PathBuf {
    let mut name = target
        .file_name()
        .map_or_else(|| std::ffi::OsString::from("ordius-wb"), ToOwned::to_owned);
    name.push(".ordius-wb.tmp");
    target
        .parent()
        .map_or_else(|| PathBuf::from(&name), |p| p.join(&name))
}

/// Map a host-side I/O error during write-back to a `DispatchError`.
fn host_io_err(path: &Path, op: &str, e: &std::io::Error) -> DispatchError {
    DispatchError::WorkspaceUnavailable {
        env_id: "<host>".into(),
        reason: format!("write-back {op} `{}`: {e}", path.display()),
    }
}

// ── Template expansion ────────────────────────────────────────────────────────

// ── Pure helpers for H3 reconcile ─────────────────────────────────────────────

/// Extract `(env_path_template, write_back)` from a `Sync { strategy: Sftp }`
/// binding, or `None` for every other binding that requires no file sync.
///
/// Returns `Err(DispatchError::Unsupported)` for `Sync { strategy: Rsync }`
/// (not yet implemented) so callers surface a clear error at the point they
/// discover the binding rather than silently no-op-ing.
fn sync_params(
    binding: &WorkspaceBinding,
) -> Result<Option<(&str, &WriteBackPolicy)>, DispatchError> {
    use crate::environment::runtime::env::SyncStrategy;
    match binding {
        WorkspaceBinding::Sync {
            env_path_template,
            strategy: SyncStrategy::Sftp,
            write_back,
        } => Ok(Some((env_path_template.as_str(), write_back))),

        WorkspaceBinding::Sync {
            strategy: SyncStrategy::Rsync,
            ..
        } => Err(DispatchError::Unsupported(
            "only SFTP workspace sync is implemented".into(),
        )),

        // All non-Sync bindings need no per-node file transfer.
        _ => Ok(None),
    }
}

/// Classify a `Sync` env-path template as [`Lifecycle::Ephemeral`] or reject it.
///
/// A template is ephemeral iff it contains the `{{run.id}}` token — only then
/// is the remote root unique per run and safe to delete on teardown.
///
/// Persistent templates (no `{{run.id}}`) are deferred to Phase H5; this
/// function returns `Err(DispatchError::Unsupported)` for them so callers
/// produce a clear error rather than silently falling back.
///
/// The common typo `{{run_id}}` (underscore) is also detected with a hint
/// message naming both forms.
fn lifecycle_of(tmpl: &str) -> Result<Lifecycle, DispatchError> {
    if tmpl.contains("{{run_id}}") && !tmpl.contains("{{run.id}}") {
        return Err(DispatchError::Unsupported(
            "the per-run placeholder is {{run.id}}, not {{run_id}}".into(),
        ));
    }
    if tmpl.contains("{{run.id}}") {
        Ok(Lifecycle::Ephemeral)
    } else {
        Err(DispatchError::Unsupported(
            "persistent workspace reuse is deferred to a later phase (H5)".into(),
        ))
    }
}

/// Expand `template` against `run` + `host_ws`, then validate the result.
///
/// Pure function — no I/O, no transport.  Unit-testable in isolation.
pub(crate) fn expand_env_root(
    template: &str,
    run: &RunScope<'_>,
    host_ws: &Path,
) -> Result<String, DispatchError> {
    use crate::template::{SubstitutionContext, default_env_allowlist, substitute};

    let empty_vars: HashMap<String, String> = HashMap::new();
    let empty_outputs: HashMap<(String, String), crate::types::PortValue> = HashMap::new();
    let empty_inputs: HashMap<String, crate::types::PortValue> = HashMap::new();
    let empty_config: HashMap<String, serde_json::Value> = HashMap::new();
    let env_allowlist: HashSet<String> = default_env_allowlist();

    let ctx = SubstitutionContext {
        vars: &empty_vars,
        secrets: &|_| None,
        upstream_outputs: &empty_outputs,
        current_inputs: &empty_inputs,
        current_config: &empty_config,
        kv: &|_| None,
        env: &|_| None,
        env_allowlist: &env_allowlist,
        resources: &|_, _| None,
        run_id: run.run_id,
        workspace: host_ws,
        started_at_iso: run.started_at_iso,
        workflow_id: run.workflow_id,
        workflow_name: run.workflow_name,
    };

    let root = substitute(template, &ctx)
        .map_err(|e| DispatchError::Unsupported(format!("invalid env_path_template: {e}")))?;

    safety::validate_env_root(&root)?;

    Ok(root)
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::env::{
        ConflictDetect, EnvId, EnvInfo, EnvSpec, EnvState, SyncStrategy, WorkspaceBinding,
        WriteBackPolicy,
    };
    use std::collections::HashMap;
    use std::path::Path;

    use super::super::transport::{
        FakeWorkspaceTransport, FakeWorkspaceTransportFactory, WorkspaceTransport,
    };
    use std::sync::Arc;

    fn sample_run<'a>() -> RunScope<'a> {
        RunScope {
            run_id: "r1",
            workflow_id: "wf1",
            workflow_name: "Test Workflow",
            started_at_iso: "2026-01-01T00:00:00Z",
        }
    }

    // ── expand_env_root ───────────────────────────────────────────────────────

    #[test]
    fn expand_env_root_substitutes_run_dot_id() {
        let run = sample_run();
        // Canonical template syntax is `{{run.id}}` (what users write in env_path_template).
        let result =
            expand_env_root("/tmp/ordius-{{run.id}}", &run, Path::new("/host/ws")).unwrap();
        assert_eq!(result, "/tmp/ordius-r1");
    }

    #[test]
    fn expand_env_root_rejects_dotdot() {
        let run = sample_run();
        let err = expand_env_root("/tmp/{{run.id}}/../x", &run, Path::new("/host/ws")).unwrap_err();
        assert!(
            err.to_string().contains(".."),
            "expected dotdot error; got: {err}"
        );
    }

    /// `{{run.id}}` (dotted, canonical) expands correctly — the Ephemeral
    /// branch is taken and `expand_env_root` returns the substituted path.
    #[test]
    fn expand_env_root_run_dot_id_classified_ephemeral() {
        let run = sample_run();
        let template = "/tmp/ordius-ws-{{run.id}}";
        // expand_env_root succeeds and substitutes the run id.
        let root = expand_env_root(template, &run, Path::new("/host/ws")).unwrap();
        assert_eq!(root, "/tmp/ordius-ws-r1");
        // Verify the discriminant: template contains `{{run.id}}` → Ephemeral.
        // (The persistent guard would have returned Err("persistent …"); the
        // fact that expand_env_root returned Ok confirms the Ephemeral branch.)
        assert!(
            template.contains("{{run.id}}"),
            "sanity: template must contain the canonical marker"
        );
        assert!(
            !template.contains("{{run_id}}"),
            "sanity: template must not contain the underscore form"
        );
    }

    // ── teardown_all ──────────────────────────────────────────────────────────

    /// Seed a manager's per-key `state` by running `reconcile_in` against a
    /// fresh fake "remote" (this uploads `host_ws` as the write-back baseline,
    /// exactly as a real run's first attempt would). Returns the manager, a
    /// state-sharing handle to the fake, and the expanded env-side root so the
    /// test can stage "remote" changes and assert post-teardown.
    ///
    /// `teardown_all` is the safety net for a node that never reached
    /// `reconcile_out` (e.g. a mid-node panic): the staged remote delta is what
    /// it must still write back.
    async fn manager_seeded_via_reconcile(
        root_template: &str,
        host_ws: &Path,
        write_back: WriteBackPolicy,
    ) -> (WorkspaceManager, FakeWorkspaceTransport, String) {
        let (d, fake) = ssh_dispatcher_with_fake("teardown");
        let mgr = WorkspaceManager::new();
        let binding = sftp_binding(root_template, write_back);
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in seeds per-key state");
        (mgr, fake, cwd.as_str().to_string())
    }

    /// Force write-back on a clean completion copies changed + new env-side
    /// files into the host workspace, then deletes the ephemeral root.
    #[tokio::test]
    async fn teardown_force_completed_writes_back_and_deletes_root() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"original").unwrap();

        let (mgr, fake, root) = manager_seeded_via_reconcile(
            "/tmp/ordius-wb-done-{{run.id}}",
            host_ws,
            WriteBackPolicy::Force { ignore: vec![] },
        )
        .await;

        // Simulate the run: modify a.txt and create new.txt on the remote.
        fake.upload_file(&format!("{root}/a.txt"), b"modified")
            .await
            .unwrap();
        fake.upload_file(&format!("{root}/sub/new.txt"), b"created")
            .await
            .unwrap();

        mgr.teardown_all(RunOutcome::Completed).await;

        // Changed + new files are written back to the host.
        assert_eq!(
            std::fs::read(host_ws.join("a.txt")).unwrap(),
            b"modified",
            "changed file must be written back"
        );
        assert_eq!(
            std::fs::read(host_ws.join("sub").join("new.txt")).unwrap(),
            b"created",
            "new file (with new parent dir) must be written back"
        );

        // Ephemeral root is gone.
        assert!(
            fake.stat(&format!("{root}/a.txt")).await.unwrap().is_none(),
            "remote file must be deleted"
        );
        assert!(
            fake.stat(&root).await.unwrap().is_none(),
            "ephemeral root dir must be deleted"
        );
    }

    /// User cancellation skips write-back entirely but STILL deletes the
    /// ephemeral root (cleanup is unconditional).
    #[tokio::test]
    async fn teardown_force_cancelled_skips_writeback_but_deletes_root() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"original").unwrap();

        let (mgr, fake, root) = manager_seeded_via_reconcile(
            "/tmp/ordius-wb-cancel-{{run.id}}",
            host_ws,
            WriteBackPolicy::Force { ignore: vec![] },
        )
        .await;

        fake.upload_file(&format!("{root}/a.txt"), b"modified")
            .await
            .unwrap();

        mgr.teardown_all(RunOutcome::CancelledByUser).await;

        // Write-back skipped: host file is untouched.
        assert_eq!(
            std::fs::read(host_ws.join("a.txt")).unwrap(),
            b"original",
            "user cancel must skip write-back"
        );
        // Cleanup still happens.
        assert!(
            fake.stat(&root).await.unwrap().is_none(),
            "ephemeral root must be deleted even on cancel"
        );
    }

    /// `WriteBackPolicy::None` performs no write-back even on clean completion,
    /// but the ephemeral root is still deleted.
    #[tokio::test]
    async fn teardown_none_completed_skips_writeback_but_deletes_root() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"original").unwrap();

        let (mgr, fake, root) = manager_seeded_via_reconcile(
            "/tmp/ordius-wb-none-{{run.id}}",
            host_ws,
            WriteBackPolicy::None,
        )
        .await;

        fake.upload_file(&format!("{root}/a.txt"), b"modified")
            .await
            .unwrap();

        mgr.teardown_all(RunOutcome::Completed).await;

        assert_eq!(
            std::fs::read(host_ws.join("a.txt")).unwrap(),
            b"original",
            "None policy must not write back"
        );
        assert!(
            fake.stat(&root).await.unwrap().is_none(),
            "ephemeral root must still be deleted under None policy"
        );
    }

    /// Force write-back honours the policy's ignore globs: an ignored env-side
    /// file is not copied back, but a non-ignored sibling is.
    #[tokio::test]
    async fn teardown_force_respects_ignore_globs() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();

        // Empty host workspace ⇒ empty baseline ⇒ every env-side file counts as new.
        let (mgr, fake, root) = manager_seeded_via_reconcile(
            "/tmp/ordius-wb-ignore-{{run.id}}",
            host_ws,
            WriteBackPolicy::Force {
                ignore: vec!["*.log".into()],
            },
        )
        .await;

        fake.upload_file(&format!("{root}/data.txt"), b"keep")
            .await
            .unwrap();
        fake.upload_file(&format!("{root}/debug.log"), b"noise")
            .await
            .unwrap();

        mgr.teardown_all(RunOutcome::Completed).await;

        assert_eq!(
            std::fs::read(host_ws.join("data.txt")).unwrap(),
            b"keep",
            "non-ignored new file must be written back"
        );
        assert!(
            !host_ws.join("debug.log").exists(),
            "ignored *.log file must not be written back"
        );
    }

    /// A malicious / compromised server returning a path that escapes the synced
    /// root must NOT be written outside the host workspace during write-back.
    #[tokio::test]
    async fn teardown_force_skips_traversal_paths() {
        let host = tempfile::TempDir::new().unwrap();
        // Nest the workspace so a `..` escape would land inside the (auto-cleaned)
        // temp dir rather than polluting the real filesystem.
        let host_ws = host.path().join("ws");
        std::fs::create_dir(&host_ws).unwrap();

        let (mgr, fake, root) = manager_seeded_via_reconcile(
            "/tmp/ordius-wb-evil-{{run.id}}",
            &host_ws,
            WriteBackPolicy::Force { ignore: vec![] },
        )
        .await;

        // The "server" presents a traversal path plus a legitimate sibling.
        fake.upload_file(&format!("{root}/../escape.txt"), b"pwned")
            .await
            .unwrap();
        fake.upload_file(&format!("{root}/ok.txt"), b"fine")
            .await
            .unwrap();

        mgr.teardown_all(RunOutcome::Completed).await;

        assert!(
            !host.path().join("escape.txt").exists(),
            "traversal write-back must be skipped (no escape outside the workspace)"
        );
        assert_eq!(
            std::fs::read(host_ws.join("ok.txt")).unwrap(),
            b"fine",
            "the legitimate sibling must still be written back"
        );
    }

    /// `parallel`/`compose` children inherit the parent `host_ws` but run under
    /// distinct `run_id`s → distinct ephemeral roots that collapse onto a single
    /// `(EnvId, host_ws)` `state` entry (each `reconcile_in` overwrites the
    /// prior). Teardown must still delete EVERY tracked root, not just the last.
    #[tokio::test]
    async fn teardown_deletes_all_ephemeral_roots_for_same_key() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"host-a").unwrap();

        // One manager + one dispatcher/fake — both reconcile_ins hit the same
        // "remote", as a real parent + child would share an SSH env.
        let (d, fake) = ssh_dispatcher_with_fake("multi-root");
        let mgr = WorkspaceManager::new();
        let binding = sftp_binding("/tmp/ordius-multi-{{run.id}}", WriteBackPolicy::None);

        // Two distinct run ids → two distinct ephemeral roots, same host_ws.
        let run_a = RunScope {
            run_id: "run-a",
            workflow_id: "wf1",
            workflow_name: "Test Workflow",
            started_at_iso: "2026-01-01T00:00:00Z",
        };
        let run_b = RunScope {
            run_id: "run-b",
            workflow_id: "wf1",
            workflow_name: "Test Workflow",
            started_at_iso: "2026-01-01T00:00:00Z",
        };

        let root_a = mgr
            .reconcile_in(&d, &binding, host_ws, &run_a)
            .await
            .expect("first reconcile_in")
            .as_str()
            .to_string();
        let root_b = mgr
            .reconcile_in(&d, &binding, host_ws, &run_b)
            .await
            .expect("second reconcile_in")
            .as_str()
            .to_string();

        assert_ne!(root_a, root_b, "distinct run ids must yield distinct roots");
        // Both roots exist on the remote after the two reconcile_ins.
        assert!(
            fake.stat(&root_a).await.unwrap().is_some(),
            "root A must exist before teardown"
        );
        assert!(
            fake.stat(&root_b).await.unwrap().is_some(),
            "root B must exist before teardown"
        );

        mgr.teardown_all(RunOutcome::Completed).await;

        // BOTH roots are gone — not just the last (`state` only kept root B).
        assert!(
            fake.stat(&root_a).await.unwrap().is_none(),
            "root A must be deleted by teardown (would leak before the fix)"
        );
        assert!(
            fake.stat(&root_b).await.unwrap().is_none(),
            "root B must be deleted by teardown"
        );
    }

    // ── WorkspaceExecutionLease ───────────────────────────────────────────────

    /// Same key blocks; distinct key does not.
    ///
    /// Proof:
    ///   (a) While A's lease is held, `acquire_execution_lease(A)` wrapped in a
    ///       50 ms timeout returns `Err` (still blocked).
    ///   (b) `acquire_execution_lease(B)` under the same timeout returns `Ok`
    ///       immediately — distinct keys never contend.
    ///   (c) After A's guard is dropped, `acquire_execution_lease(A)` succeeds.
    #[tokio::test]
    async fn lease_serializes_same_key_allows_distinct() {
        use std::time::Duration;
        use tokio::time::timeout;

        let mgr = WorkspaceManager::new();
        let key_a: WorkspaceKey = (EnvId::local(), PathBuf::from("/ws/a"));
        let key_b: WorkspaceKey = (EnvId::local(), PathBuf::from("/ws/b"));

        // Acquire lease A.
        let lease_a = mgr.acquire_execution_lease(key_a.clone()).await;

        // (a) A second acquire on key A must block (timeout expires).
        assert!(
            timeout(
                Duration::from_millis(50),
                mgr.acquire_execution_lease(key_a.clone()),
            )
            .await
            .is_err(),
            "acquire_execution_lease(A) must block while A is held"
        );

        // (b) Key B resolves immediately — no contention with A.
        let lease_b = timeout(
            Duration::from_millis(50),
            mgr.acquire_execution_lease(key_b),
        )
        .await
        .expect("acquire_execution_lease(B) must not block — distinct key");
        drop(lease_b);

        // (c) Drop A; now acquiring A must succeed.
        drop(lease_a);
        let lease_a2 = timeout(
            Duration::from_millis(50),
            mgr.acquire_execution_lease(key_a),
        )
        .await
        .expect("acquire_execution_lease(A) must succeed after guard is dropped");
        drop(lease_a2);
    }

    /// A host-side symlinked directory inside the workspace must not let
    /// write-back escape — writing through it would redirect outside the tree.
    #[cfg(unix)]
    #[tokio::test]
    async fn teardown_force_does_not_follow_host_symlink_dirs() {
        use std::os::unix::fs::symlink;

        let outside = tempfile::TempDir::new().unwrap();
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path().join("ws");
        std::fs::create_dir(&host_ws).unwrap();
        // host_ws/link -> <outside>  (a symlinked directory inside the workspace)
        symlink(outside.path(), host_ws.join("link")).unwrap();

        let (mgr, fake, root) = manager_seeded_via_reconcile(
            "/tmp/ordius-wb-symlink-{{run.id}}",
            &host_ws,
            WriteBackPolicy::Force { ignore: vec![] },
        )
        .await;

        // The "server" creates a file under the host-symlinked `link/` dir.
        fake.upload_file(&format!("{root}/link/pwned.txt"), b"pwned")
            .await
            .unwrap();

        mgr.teardown_all(RunOutcome::Completed).await;

        assert!(
            !outside.path().join("pwned.txt").exists(),
            "write-back must not follow a host-side symlinked directory"
        );
    }

    // ── sync_params ───────────────────────────────────────────────────────────

    /// `Sync { strategy: Sftp }` → `Some((template, write_back))`
    #[test]
    fn sync_params_sftp_returns_some() {
        let wb = WriteBackPolicy::None;
        let binding = WorkspaceBinding::Sync {
            env_path_template: "/tmp/ordius-{{run.id}}".into(),
            strategy: SyncStrategy::Sftp,
            write_back: wb.clone(),
        };
        let result = sync_params(&binding).expect("Sftp must not error");
        let (tmpl, policy) = result.expect("Sftp must return Some");
        assert_eq!(tmpl, "/tmp/ordius-{{run.id}}");
        assert_eq!(*policy, wb);
    }

    /// `Sync { strategy: Rsync }` → `Err(Unsupported)`
    #[test]
    fn sync_params_rsync_returns_err() {
        let binding = WorkspaceBinding::Sync {
            env_path_template: "/tmp/ordius-{{run.id}}".into(),
            strategy: SyncStrategy::Rsync,
            write_back: WriteBackPolicy::None,
        };
        let err = sync_params(&binding).unwrap_err();
        assert!(
            err.to_string().contains("SFTP"),
            "expected SFTP error; got: {err}"
        );
    }

    /// Non-Sync bindings (Shared, Unsupported, etc.) → `Ok(None)`
    #[test]
    fn sync_params_shared_and_unsupported_return_none() {
        assert!(sync_params(&WorkspaceBinding::Shared).unwrap().is_none());
        assert!(
            sync_params(&WorkspaceBinding::Unsupported)
                .unwrap()
                .is_none()
        );
        assert!(
            sync_params(&WorkspaceBinding::Translated)
                .unwrap()
                .is_none()
        );
    }

    // ── lifecycle_of ──────────────────────────────────────────────────────────

    /// `{{run.id}}` in the template → `Lifecycle::Ephemeral`
    #[test]
    fn lifecycle_of_run_dot_id_is_ephemeral() {
        let lc = lifecycle_of("/tmp/ordius-{{run.id}}").expect("must succeed");
        assert_eq!(lc, Lifecycle::Ephemeral);
    }

    /// Template without any run-id token → `Err(Unsupported)` mentioning "persistent"
    #[test]
    fn lifecycle_of_no_token_is_err_persistent() {
        let err = lifecycle_of("/stable/path").unwrap_err();
        assert!(
            err.to_string().contains("persistent"),
            "expected 'persistent' in error; got: {err}"
        );
    }

    /// `{{run_id}}` (underscore typo) → `Err(Unsupported)` hinting both forms
    #[test]
    fn lifecycle_of_run_id_underscore_gives_hint_error() {
        let err = lifecycle_of("/tmp/ordius-{{run_id}}").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("{{run.id}}") && msg.contains("{{run_id}}"),
            "expected hint naming both forms; got: {msg}"
        );
    }

    // ── fake_models_symlinks (manager-side integration) ───────────────────────

    /// Seed a symlink in the fake factory, open a transport, and verify:
    /// `list_tree` emits Symlink, `stat` is Symlink (no-follow), `read_link`
    /// returns the target, and `remove_file` unlinks the symlink entry itself.
    #[tokio::test]
    async fn fake_models_symlinks() {
        use super::super::transport::{
            FakeWorkspaceTransportFactory, FileKind, WorkspaceTransportFactory,
        };

        let factory = FakeWorkspaceTransportFactory::default();
        factory.seed_symlink("root/link.txt", "../target.txt");

        let t = factory.open().await.unwrap();
        t.upload_file("root/real.txt", b"data").await.unwrap();

        // list_tree includes the symlink as Symlink
        let listing = t.list_tree("root").await.unwrap();
        let link = listing
            .iter()
            .find(|m| m.rel_path == "root/link.txt")
            .expect("symlink must appear in list_tree");
        assert_eq!(
            link.kind,
            FileKind::Symlink,
            "list_tree: must report Symlink"
        );

        // stat is Symlink (no-follow)
        let meta = t
            .stat("root/link.txt")
            .await
            .unwrap()
            .expect("stat: must return Some");
        assert_eq!(
            meta.kind,
            FileKind::Symlink,
            "stat: must be Symlink (no-follow)"
        );

        // read_link returns the stored target
        let target = t.read_link("root/link.txt").await.unwrap();
        assert_eq!(target, "../target.txt");

        // read_link on a regular file is an error
        assert!(
            t.read_link("root/real.txt").await.is_err(),
            "read_link on regular file must error"
        );

        // remove_file unlinks the symlink entry; the regular file is untouched
        t.remove_file("root/link.txt").await.unwrap();
        assert!(
            t.stat("root/link.txt").await.unwrap().is_none(),
            "symlink must be gone after remove_file"
        );
        assert!(
            t.stat("root/real.txt").await.unwrap().is_some(),
            "regular file must be untouched"
        );
    }

    // ── reconcile_in / reconcile_out (T2b) ────────────────────────────────────

    use super::super::transport::FileKind;
    use crate::environment::runtime::fake::FakeRemoteDispatcher;

    /// An `EnvInfo` whose id is an SSH env (so `info().id` keys the state map),
    /// reusing the `Local` spec shape (the spec variant is irrelevant here —
    /// reconcile only reads `info().id` and `workspace_transport()`).
    fn ssh_info(label: &str) -> EnvInfo {
        EnvInfo {
            id: EnvId::ssh(label),
            label: label.into(),
            spec: EnvSpec::Local {
                resources: vec![],
                host_direct_verifications: HashMap::default(),
            },
            state: EnvState::Reachable,
            enabled: true,
        }
    }

    /// Build a `FakeRemoteDispatcher` wired to a fresh fake workspace transport,
    /// returning the dispatcher plus a state-sharing handle to the fake "remote".
    fn ssh_dispatcher_with_fake(label: &str) -> (FakeRemoteDispatcher, FakeWorkspaceTransport) {
        let fake = FakeWorkspaceTransport::default();
        let factory = Arc::new(FakeWorkspaceTransportFactory::new(fake.clone()));
        let d = FakeRemoteDispatcher::new(ssh_info(label)).with_workspace_transport(factory);
        (d, fake)
    }

    /// Collect the regular-file rels under `root` from the fake, stripped of the
    /// `root/` prefix and sorted, for compact assertions.
    async fn remote_files(fake: &FakeWorkspaceTransport, root: &str) -> Vec<String> {
        let prefix = format!("{root}/");
        let mut files: Vec<String> = fake
            .list_tree(root)
            .await
            .unwrap()
            .into_iter()
            .filter(|m| m.kind == FileKind::File)
            .filter_map(|m| m.rel_path.strip_prefix(&prefix).map(ToOwned::to_owned))
            .collect();
        files.sort();
        files
    }

    fn sftp_binding(template: &str, write_back: WriteBackPolicy) -> WorkspaceBinding {
        WorkspaceBinding::Sync {
            env_path_template: template.into(),
            strategy: SyncStrategy::Sftp,
            write_back,
        }
    }

    /// `reconcile_in` makes the remote equal the host, and a subsequent
    /// `reconcile_in` after a "failed attempt" (cruft + a mutated file) resets
    /// the remote back to the host tree — cruft is gone, content is host content.
    #[tokio::test]
    async fn reconcile_in_resets_remote_to_host() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"host-a").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("reset");
        let mgr = WorkspaceManager::new();
        let binding = sftp_binding("/tmp/ordius-{{run.id}}", WriteBackPolicy::None);

        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("first reconcile_in");
        let root = cwd.as_str().to_string();
        assert_eq!(root, "/tmp/ordius-r1");

        // Remote now mirrors the host.
        assert_eq!(remote_files(&fake, &root).await, vec!["a.txt".to_string()]);
        assert_eq!(
            fake.download_file(&format!("{root}/a.txt")).await.unwrap(),
            b"host-a"
        );

        // Simulate a failed attempt: mutate a.txt and drop cruft on the remote.
        fake.upload_file(&format!("{root}/a.txt"), b"stale-remote")
            .await
            .unwrap();
        fake.upload_file(&format!("{root}/cruft.txt"), b"left-over")
            .await
            .unwrap();
        fake.upload_file(&format!("{root}/junk/deep.txt"), b"nested-cruft")
            .await
            .unwrap();

        // Re-run: reset must restore the host tree exactly.
        mgr.reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("second reconcile_in");

        assert_eq!(
            remote_files(&fake, &root).await,
            vec!["a.txt".to_string()],
            "cruft files must be gone after reset"
        );
        assert_eq!(
            fake.download_file(&format!("{root}/a.txt")).await.unwrap(),
            b"host-a",
            "remote a.txt must be reset to host content"
        );
    }

    /// `reconcile_in` clears remote symlinks — at a target-file rel AND at an
    /// intermediate directory rel — replacing them with the host's real files.
    #[tokio::test]
    async fn reconcile_in_clears_remote_symlinks() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        // host: a.txt (regular) + sub/b.txt (regular under a real dir)
        std::fs::write(host_ws.join("a.txt"), b"real-a").unwrap();
        std::fs::create_dir(host_ws.join("sub")).unwrap();
        std::fs::write(host_ws.join("sub").join("b.txt"), b"real-b").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("symlink");
        let factory = FakeWorkspaceTransportFactory::new(fake.clone());

        let root = "/tmp/ordius-r1";
        // Seed a symlink AT a target-file rel and a symlink AT an intermediate dir.
        factory.seed_symlink(&format!("{root}/a.txt"), "/etc/passwd");
        factory.seed_symlink(&format!("{root}/sub"), "/var/evil");
        // Also seed the root dir so list_tree includes it (exercise the root-skip).
        fake.mkdir(root).await.unwrap();

        let mgr = WorkspaceManager::new();
        let binding = sftp_binding("/tmp/ordius-{{run.id}}", WriteBackPolicy::None);

        mgr.reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");

        // Both symlinks must be gone.
        assert!(
            fake.stat(&format!("{root}/a.txt")).await.unwrap().is_some(),
            "a.txt must exist"
        );
        assert_ne!(
            fake.stat(&format!("{root}/a.txt"))
                .await
                .unwrap()
                .unwrap()
                .kind,
            FileKind::Symlink,
            "a.txt must no longer be a symlink"
        );
        // `sub` symlink removed; the real file lives under it now.
        let sub_meta = fake.stat(&format!("{root}/sub")).await.unwrap();
        if let Some(m) = sub_meta {
            assert_ne!(m.kind, FileKind::Symlink, "sub must not be a symlink");
        }
        // Remote files are the host's real files with host content.
        assert_eq!(
            remote_files(&fake, root).await,
            vec!["a.txt".to_string(), "sub/b.txt".to_string()]
        );
        assert_eq!(
            fake.download_file(&format!("{root}/a.txt")).await.unwrap(),
            b"real-a"
        );
        assert_eq!(
            fake.download_file(&format!("{root}/sub/b.txt"))
                .await
                .unwrap(),
            b"real-b"
        );
    }

    /// `reconcile_out` (Force) writes changed + new files back, propagates
    /// remote deletions to the host, advances the baseline, and honours ignore
    /// globs + traversal/symlink guards. `None` is a no-op.
    #[tokio::test]
    async fn reconcile_out_writes_delta_and_propagates_deletions() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"host-a").unwrap();
        std::fs::write(host_ws.join("b.txt"), b"host-b").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("delta");
        let mgr = WorkspaceManager::new();
        let binding = sftp_binding(
            "/tmp/ordius-{{run.id}}",
            WriteBackPolicy::Force {
                ignore: vec!["*.log".into()],
            },
        );

        // reconcile_in establishes baseline {a.txt, b.txt} on the remote.
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        // Node activity on the remote: modify a.txt, create c.txt, delete b.txt,
        // create an ignored .log, and present a traversal escape + a host-symlink
        // escape target that must be refused.
        fake.upload_file(&format!("{root}/a.txt"), b"changed-a")
            .await
            .unwrap();
        fake.upload_file(&format!("{root}/c.txt"), b"new-c")
            .await
            .unwrap();
        fake.remove_file(&format!("{root}/b.txt")).await.unwrap();
        fake.upload_file(&format!("{root}/debug.log"), b"noise")
            .await
            .unwrap();
        // Traversal escape: must never be written outside host_ws.
        fake.upload_file(&format!("{root}/../escape.txt"), b"pwned")
            .await
            .unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out Force");

        // Changed + new written back.
        assert_eq!(std::fs::read(host_ws.join("a.txt")).unwrap(), b"changed-a");
        assert_eq!(std::fs::read(host_ws.join("c.txt")).unwrap(), b"new-c");
        // Remote deletion propagated to host.
        assert!(
            !host_ws.join("b.txt").exists(),
            "b.txt deleted on remote must be removed on host"
        );
        // Ignored + traversal entries honoured.
        assert!(
            !host_ws.join("debug.log").exists(),
            "ignored *.log must not be written back"
        );
        assert!(
            !host.path().join("escape.txt").exists(),
            "traversal write-back must be skipped"
        );

        // Baseline advanced: a no-further-change reconcile_out is a clean no-op
        // (and would re-delete b.txt if the baseline still listed it — it must
        // not, since b.txt no longer exists on the remote). Recreate b.txt on
        // host to prove it is NOT re-deleted by the advanced baseline.
        std::fs::write(host_ws.join("b.txt"), b"recreated").unwrap();
        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("second reconcile_out");
        assert_eq!(
            std::fs::read(host_ws.join("b.txt")).unwrap(),
            b"recreated",
            "advanced baseline must not re-propagate the stale b.txt deletion"
        );

        // `None` policy is a no-op: set up fresh state and confirm nothing changes.
        let (d2, fake2) = ssh_dispatcher_with_fake("delta-none");
        let mgr2 = WorkspaceManager::new();
        let host2 = tempfile::TempDir::new().unwrap();
        let host_ws2 = host2.path();
        std::fs::write(host_ws2.join("a.txt"), b"orig").unwrap();
        let none_binding = sftp_binding("/tmp/ordius-{{run.id}}", WriteBackPolicy::None);
        let cwd2 = mgr2
            .reconcile_in(&d2, &none_binding, host_ws2, &sample_run())
            .await
            .unwrap();
        let root2 = cwd2.as_str().to_string();
        fake2
            .upload_file(&format!("{root2}/a.txt"), b"changed-on-remote")
            .await
            .unwrap();
        mgr2.reconcile_out(&d2, &none_binding, host_ws2)
            .await
            .expect("reconcile_out None");
        assert_eq!(
            std::fs::read(host_ws2.join("a.txt")).unwrap(),
            b"orig",
            "None policy must not write back"
        );
    }

    /// Force write-back prunes a host directory that became empty after its only
    /// file was deleted on the remote — but keeps a dir that still holds an
    /// untracked host file (present on host, absent from baseline + remote).
    #[tokio::test]
    async fn write_back_prunes_empty_host_dir_but_keeps_nonempty() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        // Baseline: dir `d` holding `d/x.txt`, plus dir `e` holding `e/y.txt`.
        std::fs::create_dir(host_ws.join("d")).unwrap();
        std::fs::write(host_ws.join("d").join("x.txt"), b"x").unwrap();
        std::fs::create_dir(host_ws.join("e")).unwrap();
        std::fs::write(host_ws.join("e").join("y.txt"), b"y").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("prune-empty");
        let mgr = WorkspaceManager::new();
        let binding = sftp_binding(
            "/tmp/ordius-{{run.id}}",
            WriteBackPolicy::Force { ignore: vec![] },
        );

        // reconcile_in establishes baseline {dirs: d, e; files: d/x.txt, e/y.txt}.
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        // Node deletes BOTH dirs' files on the remote so neither dir remains.
        fake.remove_file(&format!("{root}/d/x.txt")).await.unwrap();
        fake.remove_dir(&format!("{root}/d")).await.unwrap();
        fake.remove_file(&format!("{root}/e/y.txt")).await.unwrap();
        fake.remove_dir(&format!("{root}/e")).await.unwrap();

        // Drop an UNTRACKED file into host `e/` (present on host, NOT in baseline,
        // NOT on remote) so the prune must leave `e/` standing.
        std::fs::write(host_ws.join("e").join("keep.txt"), b"keep").unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out Force");

        // `d` had only its tracked file: deletion propagates AND the empty dir is pruned.
        assert!(
            !host_ws.join("d").join("x.txt").exists(),
            "d/x.txt must be deleted"
        );
        assert!(
            !host_ws.join("d").exists(),
            "empty host dir d must be pruned"
        );
        // `e` still holds an untracked file: its tracked file goes, but the dir stays.
        assert!(
            !host_ws.join("e").join("y.txt").exists(),
            "e/y.txt must be deleted"
        );
        assert!(
            host_ws.join("e").exists(),
            "non-empty host dir e must be kept"
        );
        assert!(
            host_ws.join("e").join("keep.txt").exists(),
            "untracked host file e/keep.txt must survive"
        );
    }

    /// Force write-back must NOT delete a host file/subtree when the remote
    /// replaces it with a symlink at the same rel — the symlink shadows the rel,
    /// so the absence of a regular file there is not a real deletion.
    #[tokio::test]
    async fn write_back_does_not_delete_host_when_remote_replaces_file_with_symlink() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        // Baseline: file `a.txt` and dir `d` holding `d/c.txt`.
        std::fs::write(host_ws.join("a.txt"), b"host-a").unwrap();
        std::fs::create_dir(host_ws.join("d")).unwrap();
        std::fs::write(host_ws.join("d").join("c.txt"), b"host-c").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("symlink-shadow");
        let factory = FakeWorkspaceTransportFactory::new(fake.clone());
        let mgr = WorkspaceManager::new();
        let binding = sftp_binding(
            "/tmp/ordius-{{run.id}}",
            WriteBackPolicy::Force { ignore: vec![] },
        );

        // reconcile_in establishes baseline {files: a.txt, d/c.txt; dirs: d}.
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        // The node replaces the regular file `a.txt` with a symlink, and replaces
        // the directory `d` (and thus the file under it) with a symlink at `d`.
        fake.remove_file(&format!("{root}/a.txt")).await.unwrap();
        factory.seed_symlink(&format!("{root}/a.txt"), "/etc/passwd");
        fake.remove_file(&format!("{root}/d/c.txt")).await.unwrap();
        fake.remove_dir(&format!("{root}/d")).await.unwrap();
        factory.seed_symlink(&format!("{root}/d"), "/var/evil");

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out Force");

        // The symlink shadows `a.txt`: the host file must NOT be deleted.
        assert!(
            host_ws.join("a.txt").exists(),
            "host a.txt must NOT be deleted when remote replaces it with a symlink"
        );
        assert_eq!(
            std::fs::read(host_ws.join("a.txt")).unwrap(),
            b"host-a",
            "host a.txt content must be untouched"
        );
        // The symlink at `d` shadows the whole `d/` subtree: `d/c.txt` survives.
        assert!(
            host_ws.join("d").join("c.txt").exists(),
            "host d/c.txt must NOT be deleted when remote replaces dir d with a symlink"
        );
        assert_eq!(
            std::fs::read(host_ws.join("d").join("c.txt")).unwrap(),
            b"host-c",
            "host d/c.txt content must be untouched"
        );
    }

    /// `reconcile_in` on a non-Sync binding (Shared) delegates to
    /// `translate_path` and records no reconcile state; `reconcile_out` is then
    /// a clean no-op.
    #[tokio::test]
    async fn reconcile_in_non_sync_delegates_and_out_is_noop() {
        let (d, _fake) = ssh_dispatcher_with_fake("shared");
        let mgr = WorkspaceManager::new();
        let cwd = mgr
            .reconcile_in(
                &d,
                &WorkspaceBinding::Shared,
                Path::new("/ws"),
                &sample_run(),
            )
            .await
            .expect("shared reconcile_in");
        // FakeRemoteDispatcher::translate_path prefixes `/fake`.
        assert_eq!(cwd.as_str(), "/fake/ws");

        // No state recorded → reconcile_out is a no-op (Ok).
        mgr.reconcile_out(&d, &WorkspaceBinding::Shared, Path::new("/ws"))
            .await
            .expect("shared reconcile_out is a no-op");
    }

    /// `SafeOrDiverge` write-back is deferred to a later phase: `reconcile_in`
    /// is the gate that rejects it BEFORE any upload (restoring the pre-H3
    /// `resolve_cwd` behaviour). The node fails before running and before any
    /// `WorkspaceState` is stored, so `teardown_all` can never force-write a
    /// `SafeOrDiverge` binding — the data-integrity property.
    #[tokio::test]
    async fn reconcile_in_rejects_safe_or_diverge() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"host-a").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("safe-or-diverge");
        let mgr = WorkspaceManager::new();
        let binding = sftp_binding(
            "/tmp/ordius-{{run.id}}",
            WriteBackPolicy::SafeOrDiverge {
                mode: ConflictDetect::Manifest,
                ignore: vec![],
                max_files: 5_000,
            },
        );

        // reconcile_in rejects SafeOrDiverge before any upload.
        let err = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("SafeOrDiverge"),
            "expected SafeOrDiverge error; got: {err}"
        );

        // No state was stored: nothing was uploaded to the remote, and a
        // subsequent teardown is a no-op (cannot force-write a rejected binding).
        assert!(
            fake.stat("/tmp/ordius-r1").await.unwrap().is_none(),
            "no remote root must be created when reconcile_in rejects the binding"
        );
        mgr.teardown_all(RunOutcome::Failed).await;
        assert_eq!(
            std::fs::read(host_ws.join("a.txt")).unwrap(),
            b"host-a",
            "teardown must not write anything back for a rejected SafeOrDiverge binding"
        );
    }
}
