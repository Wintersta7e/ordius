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
//!   files back and propagates remote deletions, advancing the baseline.
//!   Policy-dispatched: `None` no-ops, `Force` overwrites, `SafeOrDiverge`
//!   diverges host-changed paths into `.ordius/diverged/`. Skipped only on a
//!   genuine user cancel.
//! - `teardown_all`: a `Force`-only write-back safety net for runs that panic
//!   between `reconcile_in` and `reconcile_out` (non-user-cancel only) plus
//!   deletion of every ephemeral root tracked during the run.
//!
//! Same-key concurrency is serialised by a per-key execution lease the run loop
//! holds across a node's reconcile cycle.
//!
//! Not yet implemented (deferred):
//! - Persistent workspace reuse (template without `{{run.id}}`) — H5.
//! - `SafeOrDiverge` conflict modes other than `Manifest` (`Checksum`/`MtimeSize`
//!   are rejected in `reconcile_in`, before any upload).
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex as SyncMutex;
use serde::Serialize;
use tokio::sync::OwnedMutexGuard;

use crate::environment::runtime::dispatcher::Dispatcher;
use crate::environment::runtime::env::{ConflictDetect, EnvId, WorkspaceBinding, WriteBackPolicy};
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
    /// Run id of the reconcile that populated this state (for divergence paths).
    run_id: String,
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

    /// Every ephemeral env-side root created this run, keyed by `(EnvId, root)` → factory.
    ///
    /// `state` is keyed by `(EnvId, host_ws)`, so `parallel`/`compose` children
    /// that inherit the parent `host_ws` but run under distinct `run_id`s collapse
    /// onto one `state` entry — each `reconcile_in` overwrites the prior. This map
    /// records *every* distinct ephemeral root so `teardown_all` deletes them all,
    /// not just the last. Keying by `(EnvId, root)` prevents two envs whose
    /// templates expand to the same root string on different servers from
    /// colliding (the later factory would otherwise clobber the earlier — a leak
    /// or a delete against the wrong server), while still dedup'ing `loop_for`'s
    /// repeated same-`(env, root)` inserts.
    ephemeral_roots: SyncMutex<HashMap<(EnvId, String), Arc<dyn WorkspaceTransportFactory>>>,

    /// Remote roots whose write-back failed and hold the only copy of a node's
    /// output (recoverable). A `reconcile_out` write-back failure records
    /// `(EnvId, root)` here so `teardown_all` keeps the root on the server, and
    /// a later same-key `reconcile_in` moves it aside to a recovery sibling
    /// (clearing the entry) before resetting the root clean.
    preserved_roots: SyncMutex<HashSet<(EnvId, String)>>,

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
            preserved_roots: SyncMutex::new(HashSet::new()),
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
    /// changed files back to the host via the policy dispatcher
    /// ([`reconcile_write_back`]) — `None` is a no-op, `Force` overwrites, and
    /// `SafeOrDiverge` writes back where the host is unchanged while preserving
    /// the host copy where it conflicts — skipped entirely on user cancel. It
    /// then deletes every tracked ephemeral env-side root (not just the last per
    /// key), EXCEPT any root whose write-back failed: that root is the only copy
    /// of the node's output, so it is kept on the server for manual recovery
    /// rather than destroyed.
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
        // and reconcile_out, leaving changes unwritten. Route through the policy
        // dispatcher so every policy applies its real semantics — `None` no-ops,
        // `Force` overwrites, `SafeOrDiverge` writes back where the host is
        // unchanged and diverges where it conflicts. User cancel skips entirely.
        // A no-op when reconcile_out already advanced the baseline.
        //
        // A root whose write-back FAILED is recorded in `preserve`: its env-side
        // tree is the only copy of the node's output, so cleanup below must keep
        // it for recovery rather than destroy it.
        let mut preserve: std::collections::HashSet<(EnvId, String)> =
            std::collections::HashSet::new();
        for (key, s) in states {
            if outcome != RunOutcome::CancelledByUser
                && let Err(e) = reconcile_write_back(
                    &s.write_back,
                    &s.transport_factory,
                    &s.env_side_root,
                    &key.1,
                    &s.last_remote_manifest,
                    &s.run_id,
                    key.0.as_str(),
                )
                .await
            {
                tracing::warn!(
                    env_root = %s.env_side_root,
                    error = %e,
                    "teardown write-back failed"
                );
                preserve.insert((key.0.clone(), s.env_side_root.clone()));
            }
        }

        // Merge in roots preserved by a `reconcile_out` write-back failure: those
        // env-side trees are the only copy of a node's output and must not be
        // deleted here either.
        preserve.extend(std::mem::take(&mut *self.preserved_roots.lock()));

        // Ephemeral cleanup: delete *every* root recorded this run, not just the
        // last per key. parallel/compose children share host_ws (one `state`
        // entry) but each gets its own run-id root — all are tracked here. A root
        // in `preserve` (its write-back failed) is the sole copy of the node's
        // output, so it is kept on the server for recovery instead of deleted.
        let roots: Vec<((EnvId, String), Arc<dyn WorkspaceTransportFactory>)> = {
            std::mem::take(&mut *self.ephemeral_roots.lock())
                .into_iter()
                .collect()
        };
        for (key, factory) in roots {
            if preserve.contains(&key) {
                tracing::warn!(
                    env_id = %key.0.as_str(),
                    env_root = %key.1,
                    "keeping remote workspace root after failed write-back (the only copy of the node's output); not deleting it so it can be recovered"
                );
                continue;
            }
            if let Err(e) = remove_tree(factory.as_ref(), &key.1).await {
                tracing::warn!(env_root = %key.1, error = %e, "ephemeral teardown failed");
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

        // SafeOrDiverge supports only the `Manifest` (content-hash) conflict
        // mode. Reject the unimplemented modes BEFORE any upload: the node fails
        // before running and no `WorkspaceState` is stored.
        if let WriteBackPolicy::SafeOrDiverge { mode, .. } = &write_back
            && *mode != ConflictDetect::Manifest
        {
            return Err(DispatchError::Unsupported(
                "SafeOrDiverge conflict mode is not implemented (only Manifest)".into(),
            ));
        }

        let root = expand_env_root(tmpl, run, host_ws)?;
        let lifecycle = lifecycle_of(tmpl)?; // Persistent => Err(Unsupported) (H5)

        let factory = dispatcher.workspace_transport().ok_or_else(|| {
            DispatchError::Unsupported("environment has no workspace transport".into())
        })?;

        // A root whose earlier `reconcile_out` write-back failed holds the only
        // copy of that node's output. Resetting it host→remote would destroy the
        // unreconciled changes, so move it aside to a recovery sibling and clear
        // the flag; the reset below then recreates the root clean. The recovery
        // copy stays on the server for manual retrieval. If the move itself fails
        // the error propagates and the root is left intact (fail closed).
        let preserve_key = (dispatcher.info().id.clone(), root.clone());
        if self.preserved_roots.lock().contains(&preserve_key) {
            let recovery = recover_preserved_root(&factory, &root).await?;
            self.preserved_roots.lock().remove(&preserve_key);
            tracing::warn!(
                env_id = %preserve_key.0.as_str(),
                env_root = root,
                recovery_path = recovery,
                "earlier write-back failed; moved the unreconciled output aside for recovery and reset the workspace"
            );
        }

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
                    run_id: run.run_id.to_string(),
                },
            );
        }

        // Record the ephemeral root so teardown deletes *every* root for this key,
        // not just the last (parallel/compose children share host_ws but get
        // distinct run-id roots — the `state` entry above only keeps the latest).
        // The reset succeeded, so the root really exists on the remote. Sync
        // insert; the guard drops before the return — no await held.
        if lifecycle == Lifecycle::Ephemeral {
            self.ephemeral_roots.lock().insert(
                (dispatcher.info().id.clone(), root.clone()),
                Arc::clone(&factory),
            );
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
    /// or when the policy is [`WriteBackPolicy::None`]. The write-back is
    /// policy-dispatched via `reconcile_write_back`: `Force` overwrites the host,
    /// `SafeOrDiverge` writes back where the host is unchanged and diverges where
    /// it conflicts (preserving the host copy under `.ordius/diverged/`).
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
        let Some((root, factory, write_back, baseline, run_id)) = ({
            let st = self.state.lock(); // parking_lot — dropped before await
            st.get(&key).map(|s| {
                (
                    s.env_side_root.clone(),
                    Arc::clone(&s.transport_factory),
                    s.write_back.clone(),
                    s.last_remote_manifest.clone(),
                    s.run_id.clone(),
                )
            })
        }) else {
            return Ok(()); // no state for this key — nothing to reconcile
        };

        // Policy-dispatched: None no-ops, Force overwrites, SafeOrDiverge writes
        // back where the host is unchanged and diverges where it conflicts.
        let new_remote = match reconcile_write_back(
            &write_back,
            &factory,
            &root,
            host_ws,
            &baseline,
            &run_id,
            key.0.as_str(),
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                // The env-side root still holds the node's unreconciled output.
                // Record it so `teardown_all` keeps it for recovery instead of
                // deleting, and a later same-key `reconcile_in` moves it aside
                // (not reset over it).
                self.preserved_roots
                    .lock()
                    .insert((key.0.clone(), root.clone()));
                return Err(e);
            },
        };

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
    // into the returned manifest. Write-back mirrors dir creates/prunes back to
    // the host via `reconcile_host_dirs`.
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

/// Run-invariant context shared by the `SafeOrDiverge` phases.
struct SodCtx<'a> {
    host_ws: &'a Path,
    run_id: &'a str,
    env_id: &'a str,
    baseline: &'a safety::Manifest,
}

/// The host file hash when the host is a regular file, else `None`
/// (for `DivergeEntry.host_sha256`).
fn host_sha_opt(host: &HostState) -> Option<String> {
    match host {
        HostState::File { sha256_hex } => Some(sha256_hex.clone()),
        _ => None,
    }
}

/// Classify a host that has *diverged* from what `reconcile_in` uploaded into a
/// [`DivergeReason`]. Only meaningful once `matches_host_at_in` has returned
/// `false` for this rel.
fn conflict_reason(host: &HostState, baseline: &safety::Manifest, rel: &str) -> DivergeReason {
    match host {
        HostState::UnsafeSymlink | HostState::Unreadable => DivergeReason::HostUnsafe,
        HostState::File { .. } => {
            if baseline.dirs.contains(rel) {
                DivergeReason::HostTypeChanged
            } else if baseline.files.contains_key(rel) {
                DivergeReason::HostModified
            } else {
                DivergeReason::HostCreated
            }
        },
        HostState::Dir => {
            if baseline.files.contains_key(rel) {
                DivergeReason::HostTypeChanged
            } else {
                DivergeReason::HostCreated
            }
        },
        HostState::Absent => DivergeReason::HostDeleted,
    }
}

/// A report entry for a conflict where no remote bytes are preserved (the host
/// is kept in place).
fn report_only(rel: &str, reason: DivergeReason, host: &HostState) -> DivergeEntry {
    DivergeEntry {
        rel: rel.to_string(),
        reason,
        host_sha256: host_sha_opt(host),
        remote_sha256: None,
        diverged_path: None,
    }
}

/// Write a remote file's bytes into the divergence dir and record a report
/// entry preserving both hashes + the artifact path. Fails closed (propagates)
/// when the artifact path is unsafe.
fn diverge_file(
    report: &mut DivergeReport,
    ctx: &SodCtx,
    rel: &str,
    bytes: &[u8],
    reason: DivergeReason,
    host: &HostState,
    remote_sha256: &str,
) -> Result<(), DispatchError> {
    let diverged_path = write_diverged_artifact(ctx.host_ws, ctx.run_id, ctx.env_id, rel, bytes)?;
    report.diverged.push(DivergeEntry {
        rel: rel.to_string(),
        reason,
        host_sha256: host_sha_opt(host),
        remote_sha256: Some(remote_sha256.to_string()),
        diverged_path: Some(diverged_path),
    });
    Ok(())
}

/// Phase 1 — apply plain deletions (deepest-first). Each `(rel, is_dir)` whose
/// host still matches `host@in` is removed (`remove_file`/`remove_dir`, never
/// recursive); a non-empty host dir is kept (`dir_delete_nonempty`); a host that
/// diverged is kept (`delete_vs_host_modified` / `host_unsafe`).
fn sod_deletions(
    ctx: &SodCtx,
    report: &mut DivergeReport,
    deletions: &[(String, bool)],
) -> Result<(), DispatchError> {
    for (rel, is_dir) in deletions {
        let host = classify_host_state(ctx.host_ws, rel);
        // No-op: the node deleted this rel and the host is already absent (the
        // user agreed) — nothing to remove and no false conflict to report.
        if host == HostState::Absent {
            continue;
        }
        if !matches_host_at_in(&host, ctx.baseline, rel) {
            let reason = match host {
                HostState::UnsafeSymlink | HostState::Unreadable => DivergeReason::HostUnsafe,
                _ => DivergeReason::DeleteVsHostModified,
            };
            report.diverged.push(report_only(rel, reason, &host));
            continue;
        }
        let target = ctx.host_ws.join(rel);
        let res = if *is_dir {
            std::fs::remove_dir(&target)
        } else {
            std::fs::remove_file(&target)
        };
        match res {
            Ok(()) => {},
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {},
            Err(e) if *is_dir && e.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                report
                    .diverged
                    .push(report_only(rel, DivergeReason::DirDeleteNonempty, &host));
            },
            // An unexpected host fs error means a node mutation failed; abort the
            // phase so write-back returns `Err`, the baseline is not advanced,
            // and teardown preserves the remote root.
            Err(e) => return Err(host_io_err(&target, "delete", &e)),
        }
    }
    Ok(())
}

/// Phase 2 — file↔dir type-change replacements (the dir→file children were
/// already removed in phase 1, so the host dir is empty here). A host that no
/// longer matches `host@in` is preserved (diverged / reported).
fn sod_type_changes(
    ctx: &SodCtx,
    report: &mut DivergeReport,
    file_to_dir: &[String],
    dir_to_file: &[&RemoteFile],
) -> Result<(), DispatchError> {
    for rel in file_to_dir {
        let host = classify_host_state(ctx.host_ws, rel);
        if host == HostState::Dir {
            continue; // already a dir
        }
        if matches_host_at_in(&host, ctx.baseline, rel) {
            // Host is the uploaded file — remove it, then create the dir in its
            // place. A NotFound here is benign (already gone); any other fs
            // error means the node's dir output would be lost — propagate.
            let target = ctx.host_ws.join(rel);
            if let Err(e) = std::fs::remove_file(&target)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(host_io_err(&target, "type-change file remove", &e));
            }
            if safety::host_target_is_symlink_safe(ctx.host_ws, rel)
                && let Err(e) = std::fs::create_dir_all(&target)
            {
                return Err(host_io_err(&target, "type-change dir create", &e));
            }
        } else {
            report.diverged.push(report_only(
                rel,
                conflict_reason(&host, ctx.baseline, rel),
                &host,
            ));
        }
    }
    for f in dir_to_file {
        let rel = f.rel.as_str();
        let host = classify_host_state(ctx.host_ws, rel);
        if let HostState::File { sha256_hex } = &host
            && *sha256_hex == f.entry.sha256_hex
        {
            continue; // host already holds the node's bytes
        }
        if matches_host_at_in(&host, ctx.baseline, rel) {
            match std::fs::remove_dir(ctx.host_ws.join(rel)) {
                Ok(()) => {
                    if safety::host_target_is_symlink_safe(ctx.host_ws, rel) {
                        write_host_file_atomic(ctx.host_ws, rel, &f.bytes)?;
                    }
                },
                // Untracked host children remain — cannot replace; preserve bytes.
                Err(_) => diverge_file(
                    report,
                    ctx,
                    rel,
                    &f.bytes,
                    DivergeReason::HostTypeChanged,
                    &host,
                    &f.entry.sha256_hex,
                )?,
            }
        } else {
            diverge_file(
                report,
                ctx,
                rel,
                &f.bytes,
                conflict_reason(&host, ctx.baseline, rel),
                &host,
                &f.entry.sha256_hex,
            )?;
        }
    }
    Ok(())
}

/// Phase 3 — create plain new directories (shallow-first). A host that diverged
/// from `host@in` (now holds a file/symlink) is reported, not overwritten.
fn sod_dir_creates(
    ctx: &SodCtx,
    report: &mut DivergeReport,
    creates: &[String],
) -> Result<(), DispatchError> {
    for rel in creates {
        let host = classify_host_state(ctx.host_ws, rel);
        if host == HostState::Dir {
            continue; // already a dir
        }
        if matches_host_at_in(&host, ctx.baseline, rel) {
            if safety::host_target_is_symlink_safe(ctx.host_ws, rel) {
                let target = ctx.host_ws.join(rel);
                if let Err(e) = std::fs::create_dir_all(&target) {
                    return Err(host_io_err(&target, "dir create", &e));
                }
            }
        } else {
            report.diverged.push(report_only(
                rel,
                conflict_reason(&host, ctx.baseline, rel),
                &host,
            ));
        }
    }
    Ok(())
}

/// Phase 4 — write plain changed/new files. The host is overwritten only where
/// it still matches `host@in`; otherwise the node's bytes are diverged and the
/// host is kept.
fn sod_file_writes(
    ctx: &SodCtx,
    report: &mut DivergeReport,
    writes: &[&RemoteFile],
) -> Result<(), DispatchError> {
    for f in writes {
        let rel = f.rel.as_str();
        let host = classify_host_state(ctx.host_ws, rel);
        if let HostState::File { sha256_hex } = &host
            && *sha256_hex == f.entry.sha256_hex
        {
            continue; // host already holds the node's bytes
        }
        if matches_host_at_in(&host, ctx.baseline, rel) {
            if safety::host_target_is_symlink_safe(ctx.host_ws, rel) {
                write_host_file_atomic(ctx.host_ws, rel, &f.bytes)?;
            }
        } else {
            diverge_file(
                report,
                ctx,
                rel,
                &f.bytes,
                conflict_reason(&host, ctx.baseline, rel),
                &host,
                &f.entry.sha256_hex,
            )?;
        }
    }
    Ok(())
}

/// The classified work for a `SafeOrDiverge` write-back: which rels are plain
/// deletions / dir-creates / file-writes, which are file↔dir type changes, and
/// which baseline rels a remote symlink shadows. Computed up front so the
/// `max_files` cap can fail-fast before any host mutation.
struct SodPlan<'a> {
    /// Baseline files the node turned into a dir (file→dir), replaced atomically.
    file_to_dir: Vec<String>,
    /// Baseline dirs the node turned into a file (dir→file), replaced atomically.
    dir_to_file: Vec<&'a RemoteFile>,
    /// Plain deletions `(rel, is_dir)`, deepest-first.
    deletions: Vec<(String, bool)>,
    /// Plain new directories, shallow-first.
    dir_creates: Vec<String>,
    /// Plain changed/new files to write.
    file_writes: Vec<&'a RemoteFile>,
    /// Remote symlinks that shadow a baseline rel (reported, host untouched).
    shadowing_symlinks: Vec<String>,
    /// Total entries this write-back would mutate (drives the `max_files` cap).
    considered: usize,
}

/// Partition the remote delta into the `SafeOrDiverge` phase work-sets (pure —
/// no host mutation). Type-change rels are excluded from the plain
/// delete/create/write sets so phase 2 can replace them atomically.
fn sod_partition<'a>(
    baseline: &safety::Manifest,
    new_remote: &safety::Manifest,
    listing: &'a RemoteListing,
    ignore: &[String],
) -> SodPlan<'a> {
    // A rel survives the plain passes only if it is safe, not ignored, and not
    // hidden behind a remote symlink.
    let keep = |rel: &str| {
        safety::is_safe_relative(rel)
            && !safety::should_ignore(rel, ignore)
            && is_shadowed_by_symlink(rel, &listing.symlinks).is_none()
    };

    let file_to_dir: Vec<String> = baseline
        .files
        .keys()
        .filter(|rel| new_remote.dirs.contains(*rel) && keep(rel))
        .cloned()
        .collect();
    let dir_to_file: Vec<&RemoteFile> = listing
        .files
        .iter()
        .filter(|f| baseline.dirs.contains(&f.rel) && keep(&f.rel))
        .collect();

    // Plain deletions (baseline rels truly gone — not a type change), deepest-first.
    let mut deletions: Vec<(String, bool)> = Vec::new();
    for rel in baseline.files.keys() {
        if !new_remote.files.contains_key(rel) && !new_remote.dirs.contains(rel) && keep(rel) {
            deletions.push((rel.clone(), false));
        }
    }
    for rel in &baseline.dirs {
        if !new_remote.dirs.contains(rel) && !new_remote.files.contains_key(rel) && keep(rel) {
            deletions.push((rel.clone(), true));
        }
    }
    deletions.sort_by_key(|(r, _)| std::cmp::Reverse(r.len()));

    // Plain new dirs (not a file→dir type change), shallow-first.
    let mut dir_creates: Vec<String> = new_remote
        .dirs
        .difference(&baseline.dirs)
        .filter(|rel| !baseline.files.contains_key(*rel) && keep(rel))
        .cloned()
        .collect();
    dir_creates.sort_unstable();

    // Plain changed/new files (not a dir→file type change). `keep` excludes
    // unsafe/ignored/symlink-shadowed rels — without it a malformed listing
    // (symlink `d` + a listed child `d/x`) could write `d/x` over a host file.
    let file_writes: Vec<&RemoteFile> = listing
        .files
        .iter()
        .filter(|f| {
            keep(&f.rel)
                && !baseline.dirs.contains(&f.rel)
                && baseline
                    .files
                    .get(&f.rel)
                    .is_none_or(|m| m.sha256_hex != f.entry.sha256_hex)
        })
        .collect();

    // Remote symlinks that shadow a baseline rel (or subtree) — unsupported content.
    let shadowing_symlinks: Vec<String> = listing
        .symlinks
        .iter()
        .filter(|s| {
            let prefix = format!("{s}/");
            baseline
                .files
                .keys()
                .any(|r| r == *s || r.starts_with(&prefix))
                || baseline
                    .dirs
                    .iter()
                    .any(|r| r == *s || r.starts_with(&prefix))
        })
        .cloned()
        .collect();

    let considered = deletions.len()
        + file_to_dir.len()
        + dir_to_file.len()
        + dir_creates.len()
        + file_writes.len();

    SodPlan {
        file_to_dir,
        dir_to_file,
        deletions,
        dir_creates,
        file_writes,
        shadowing_symlinks,
        considered,
    }
}

/// `SafeOrDiverge` write-back: write a node's output back to the host **only where
/// the host still matches what `reconcile_in` uploaded** (`host@in`); where the
/// host changed concurrently, preserve the node's version under
/// `.ordius/diverged/<run>/<env>/<rel>` + a JSON report and leave the host alone.
///
/// Remote symlinks are unsupported content: never synced, never counted as a
/// deletion (a baseline rel they shadow is reported `remote_unsupported_symlink`,
/// host untouched). The four phases (deletions → type changes → dir creates →
/// file writes) keep parents/children from colliding; each mutation re-classifies
/// the host immediately before acting (the best-effort TOCTOU recheck). Returns
/// the advanced remote manifest (the new baseline) and the report.
#[allow(clippy::too_many_arguments)]
async fn write_back_safe_or_diverge(
    factory: &Arc<dyn WorkspaceTransportFactory>,
    root: &str,
    host_ws: &Path,
    baseline: &safety::Manifest,
    ignore: &[String],
    max_files: usize,
    run_id: &str,
    env_id: &str,
) -> Result<(safety::Manifest, DivergeReport), DispatchError> {
    let t = factory.open().await?;
    let listing = list_remote_files(t, root).await?;

    // Advanced baseline (returned regardless of conflicts).
    let mut new_remote = safety::Manifest::new();
    new_remote.dirs.clone_from(&listing.dirs);
    for f in &listing.files {
        new_remote.files.insert(f.rel.clone(), f.entry.clone());
    }

    // Classify the work up front so the cap fails fast before any host mutation.
    let plan = sod_partition(baseline, &new_remote, &listing, ignore);
    if plan.considered > max_files {
        return Err(DispatchError::WorkspaceUnavailable {
            env_id: "<host>".into(),
            reason: format!(
                "SafeOrDiverge write-back would touch {} entries, exceeding max_files={max_files}",
                plan.considered
            ),
        });
    }

    let mut report = DivergeReport {
        run_id: run_id.to_string(),
        env_id: env_id.to_string(),
        diverged: Vec::new(),
    };
    for rel in &plan.shadowing_symlinks {
        report.diverged.push(DivergeEntry {
            rel: rel.clone(),
            reason: DivergeReason::RemoteUnsupportedSymlink,
            host_sha256: None,
            remote_sha256: None,
            diverged_path: None,
        });
    }

    // Four-phase application (each mutation re-classifies the host immediately
    // before acting — the best-effort TOCTOU recheck).
    let ctx = SodCtx {
        host_ws,
        run_id,
        env_id,
        baseline,
    };
    sod_deletions(&ctx, &mut report, &plan.deletions)?;
    sod_type_changes(&ctx, &mut report, &plan.file_to_dir, &plan.dir_to_file)?;
    sod_dir_creates(&ctx, &mut report, &plan.dir_creates)?;
    sod_file_writes(&ctx, &mut report, &plan.file_writes)?;

    if !report.diverged.is_empty() {
        write_diverge_report(host_ws, run_id, env_id, &report)?;
    }
    Ok((new_remote, report))
}

/// Dispatch a node's final write-back by policy, returning the advanced remote
/// manifest (the new baseline). `None` is a no-op (baseline unchanged); `Force`
/// overwrites the host; `SafeOrDiverge` writes back where the host is unchanged
/// and diverges where it conflicts. Used by both `reconcile_out` and the
/// `teardown_all` safety net so the two paths apply identical semantics.
async fn reconcile_write_back(
    policy: &WriteBackPolicy,
    factory: &Arc<dyn WorkspaceTransportFactory>,
    root: &str,
    host_ws: &Path,
    baseline: &safety::Manifest,
    run_id: &str,
    env_id: &str,
) -> Result<safety::Manifest, DispatchError> {
    match policy {
        WriteBackPolicy::None => Ok(baseline.clone()),
        WriteBackPolicy::Force { ignore } => {
            write_back_delta(factory, root, host_ws, baseline, ignore).await
        },
        WriteBackPolicy::SafeOrDiverge {
            mode,
            ignore,
            max_files,
        } => {
            if *mode != ConflictDetect::Manifest {
                return Err(DispatchError::Unsupported(
                    "SafeOrDiverge conflict mode is not implemented (only Manifest)".into(),
                ));
            }
            write_back_safe_or_diverge(
                factory, root, host_ws, baseline, ignore, *max_files, run_id, env_id,
            )
            .await
            .map(|(manifest, _report)| manifest)
        },
    }
}

/// Upper bound on recovery-sibling probes before giving up. A run could move
/// the same preserved root aside more than once (e.g. a `loop_for` that keeps
/// failing write-back), so several `.recovery.N` names may already exist; the
/// cap stops an always-"exists" transport from looping forever.
const RECOVERY_PROBE_LIMIT: u32 = 1000;

/// Move a preserved remote `root` aside so a fresh reconcile can recreate it
/// clean, returning the path the output was moved to.
///
/// A root reaches here only when an earlier write-back failed and left the sole
/// copy of a node's output under it. Rather than wedge the run, rename `root` to
/// the first free sibling `<root>.recovery[.N]` — probed via `stat` so repeated
/// recoveries within one run do not collide. A directory rename is recursive and
/// atomic server-side, so the whole subtree moves in one step. The recovery copy
/// is left on the server for the user to retrieve; teardown neither tracks nor
/// deletes it. Any `stat`/`rename` transport error propagates so the caller
/// fails closed with `root` untouched.
async fn recover_preserved_root(
    factory: &Arc<dyn WorkspaceTransportFactory>,
    root: &str,
) -> Result<String, DispatchError> {
    let t = factory.open().await?;
    for n in 0..RECOVERY_PROBE_LIMIT {
        let recovery = if n == 0 {
            format!("{root}.recovery")
        } else {
            format!("{root}.recovery.{n}")
        };
        if t.stat(&recovery).await?.is_none() {
            t.rename(root, &recovery).await?;
            return Ok(recovery);
        }
    }
    Err(DispatchError::WorkspaceUnavailable {
        env_id: "<remote>".into(),
        reason: format!(
            "no free recovery path for preserved root `{root}` after {RECOVERY_PROBE_LIMIT} attempts"
        ),
    })
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

// ── SafeOrDiverge divergence artifacts ─────────────────────────────────────────

/// Why a file written back from the remote diverged from the host baseline and
/// could not be applied in place.
///
/// Serializes as `snake_case` for the on-disk `diverge-report.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DivergeReason {
    /// The host file changed since the baseline (and so did the remote).
    HostModified,
    /// The host file was deleted since the baseline.
    HostDeleted,
    /// A new host file appeared where the remote wants to write.
    HostCreated,
    /// The host entry changed file-type (e.g. file → dir) since the baseline.
    HostTypeChanged,
    /// The host path is unsafe to write through (symlink/unreadable component).
    HostUnsafe,
    /// The remote deleted a file the host had concurrently modified.
    DeleteVsHostModified,
    /// A directory the remote deleted is non-empty on the host.
    DirDeleteNonempty,
    /// The remote returned a symlink, which write-back does not support.
    RemoteUnsupportedSymlink,
}

/// One diverged path in a [`DivergeReport`].
#[derive(Debug, Clone, Serialize)]
struct DivergeEntry {
    /// Forward-slash relative path (from the workspace root) that diverged.
    rel: String,
    /// Why it diverged.
    reason: DivergeReason,
    /// Host baseline content hash, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    host_sha256: Option<String>,
    /// Remote content hash, when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    remote_sha256: Option<String>,
    /// Relative path of the side-written divergence artifact, when one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    diverged_path: Option<String>,
}

/// A run's diverged write-backs, serialized to
/// `.ordius/diverged/<run>/<env>/diverge-report.json`.
#[derive(Debug, Clone, Serialize)]
struct DivergeReport {
    /// Run that produced the divergence.
    run_id: String,
    /// Env whose write-back diverged.
    env_id: String,
    /// One entry per diverged path.
    diverged: Vec<DivergeEntry>,
}

/// Fail-closed reject for a divergence path that traverses a symlinked or
/// unreadable host component, mirroring [`safety::classify_artifact_path`].
fn reject_unsafe_artifact_path(artifact_rel: &str) -> DispatchError {
    DispatchError::WorkspaceUnavailable {
        env_id: "<host>".into(),
        reason: format!(
            "refusing to write divergence artifact through a symlinked/unreadable path: \
             {artifact_rel}"
        ),
    }
}

/// Reject an empty `run_id`/`env_id`: `encode_segment("")` is `""`, which would
/// collapse the `.ordius/diverged/<run>/<env>/` path layout.
fn reject_empty_id(kind: &str) -> DispatchError {
    DispatchError::WorkspaceUnavailable {
        env_id: "<host>".into(),
        reason: format!("refusing to write divergence artifact: empty {kind}"),
    }
}

/// Atomically write `bytes` to `host_ws/<artifact_rel>`, creating each missing
/// `.ordius/diverged/...` parent component one at a time and re-checking — with
/// `symlink_metadata` immediately before each `create_dir` — that the component
/// is a real directory, not a symlink. This narrows (but cannot fully close,
/// absent an `openat`-style primitive) the TOCTOU window where a concurrent
/// actor swaps a freshly-created dir for a symlink that would redirect the write
/// outside the workspace. The leaf is written via temp+rename for atomicity.
///
/// Fail-closed: any component that is, or becomes, a symlink/non-directory →
/// `Err`; an unreadable component (non-`NotFound` `symlink_metadata` error) →
/// `Err`.
fn write_artifact_atomic(
    host_ws: &Path,
    artifact_rel: &str,
    bytes: &[u8],
) -> Result<(), DispatchError> {
    // Preflight the whole path (the existing-components symlink/unreadable gate).
    match safety::classify_artifact_path(host_ws, artifact_rel) {
        safety::ArtifactPathState::Ok => {},
        safety::ArtifactPathState::Symlink | safety::ArtifactPathState::Unreadable => {
            return Err(reject_unsafe_artifact_path(artifact_rel));
        },
    }

    let target = host_ws.join(artifact_rel);

    // Create each missing parent component individually, re-checking right before
    // each `create_dir` that the existing/just-created component is a real dir.
    let mut cur = host_ws.to_path_buf();
    for comp in Path::new(artifact_rel).components() {
        match comp {
            std::path::Component::Normal(c) => cur.push(c),
            std::path::Component::CurDir => continue,
            // is_safe_relative / the preflight reject these; fail closed anyway.
            _ => return Err(reject_unsafe_artifact_path(artifact_rel)),
        }
        // Only walk parent components here; the leaf is written via temp+rename.
        if cur == target {
            break;
        }
        match std::fs::symlink_metadata(&cur) {
            Ok(md) if md.file_type().is_symlink() || !md.file_type().is_dir() => {
                return Err(reject_unsafe_artifact_path(artifact_rel));
            },
            Ok(_) => {},
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&cur)
                    .map_err(|e| host_io_err(&cur, "create diverged dir", &e))?;
                // Re-check the component we just created is a real dir (not a
                // symlink a racing actor planted between the check and mkdir).
                match std::fs::symlink_metadata(&cur) {
                    Ok(md) if md.file_type().is_dir() => {},
                    _ => return Err(reject_unsafe_artifact_path(artifact_rel)),
                }
            },
            Err(_) => return Err(reject_unsafe_artifact_path(artifact_rel)),
        }
    }

    let tmp = tmp_sibling(&target);
    std::fs::write(&tmp, bytes).map_err(|e| host_io_err(&tmp, "write diverged temp file", &e))?;
    std::fs::rename(&tmp, &target)
        .map_err(|e| host_io_err(&target, "rename diverged into place", &e))?;
    // Residual race: between each `symlink_metadata` recheck and the subsequent
    // `create_dir`/write there is a sub-syscall gap an attacker could still win.
    // Closing it fully needs an `openat`/`O_NOFOLLOW`-style primitive we don't
    // have on the Windows host; this keeps the window as small as practical.
    Ok(())
}

/// Write `bytes` to a side artifact under
/// `.ordius/diverged/<enc(run_id)>/<enc(env_id)>/<rel>` instead of clobbering
/// `host_ws/<rel>` in place, returning the forward-slash artifact rel path.
///
/// Fail-closed: rejects an empty `run_id`/`env_id`, and (via
/// [`write_artifact_atomic`]) rejects when any existing component of the artifact
/// path is a symlink or unreadable.
fn write_diverged_artifact(
    host_ws: &Path,
    run_id: &str,
    env_id: &str,
    rel: &str,
    bytes: &[u8],
) -> Result<String, DispatchError> {
    if run_id.is_empty() {
        return Err(reject_empty_id("run_id"));
    }
    if env_id.is_empty() {
        return Err(reject_empty_id("env_id"));
    }
    let artifact_rel = format!(
        ".ordius/diverged/{}/{}/{}",
        safety::encode_segment(run_id),
        safety::encode_segment(env_id),
        rel
    );

    write_artifact_atomic(host_ws, &artifact_rel, bytes)?;
    Ok(artifact_rel)
}

/// Write `report` as pretty JSON to
/// `.ordius/diverged/<enc(run_id)>/<enc(env_id)>/diverge-report.json`.
///
/// The caller guards `!report.diverged.is_empty()`. Same fail-closed empty-id +
/// per-component symlink gate and atomic temp+rename write as
/// [`write_diverged_artifact`] (both go through [`write_artifact_atomic`]).
fn write_diverge_report(
    host_ws: &Path,
    run_id: &str,
    env_id: &str,
    report: &DivergeReport,
) -> Result<(), DispatchError> {
    if run_id.is_empty() {
        return Err(reject_empty_id("run_id"));
    }
    if env_id.is_empty() {
        return Err(reject_empty_id("env_id"));
    }
    let report_rel = format!(
        ".ordius/diverged/{}/{}/diverge-report.json",
        safety::encode_segment(run_id),
        safety::encode_segment(env_id),
    );

    let json =
        serde_json::to_vec_pretty(report).map_err(|e| DispatchError::WorkspaceUnavailable {
            env_id: "<host>".into(),
            reason: format!("serialize diverge report: {e}"),
        })?;

    write_artifact_atomic(host_ws, &report_rel, &json)
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

// ── host@in conflict spine ──────────────────────────────────────────────────────

/// The live shape of a host workspace path, captured for conflict detection.
///
/// Produced by [`classify_host_state`] and compared against the `host@in`
/// baseline manifest via [`matches_host_at_in`] to answer: "is the host path
/// still byte-and-type identical to what we uploaded at the start of the run?".
/// `UnsafeSymlink` and `Unreadable` are distinct *fail-closed* states — a path
/// we cannot safely classify must never be treated as "unchanged".
#[derive(Debug, Clone, PartialEq, Eq)]
enum HostState {
    /// The path does not exist on the host.
    Absent,
    /// A regular file with the given lowercase-hex SHA-256 of its bytes
    /// (computed via [`safety::sha256_hex`], so it is comparable to a manifest
    /// [`safety::FileEntry::sha256_hex`]).
    File {
        /// Lowercase-hex SHA-256 of the file contents.
        sha256_hex: String,
    },
    /// A directory.
    Dir,
    /// A component of the path (including the terminal entry) is a symlink, so
    /// the path cannot be classified or mutated safely. Fail closed.
    UnsafeSymlink,
    /// The path's metadata could not be read for a reason other than "does not
    /// exist" (e.g. a permission error), or it is a non-file/non-dir entry
    /// (fifo, socket, …) we cannot safely mutate. Fail closed.
    Unreadable,
}

/// Classify the live shape of `host_ws/rel` for conflict detection.
///
/// First defers to [`safety::classify_artifact_path`] for the symlink-traversal
/// and unreadable-component checks (so the same fail-closed rules used for
/// write-back govern conflict detection). Only when every component is clean
/// does it stat the terminal path: missing → [`HostState::Absent`], a dir →
/// [`HostState::Dir`], a regular file → its content hash, and anything else
/// (other stat error, fifo/socket/…, or a hash read error) →
/// [`HostState::Unreadable`].
fn classify_host_state(host_ws: &std::path::Path, rel: &str) -> HostState {
    use std::io::ErrorKind;

    match safety::classify_artifact_path(host_ws, rel) {
        safety::ArtifactPathState::Symlink => return HostState::UnsafeSymlink,
        safety::ArtifactPathState::Unreadable => return HostState::Unreadable,
        safety::ArtifactPathState::Ok => {},
    }

    let target = host_ws.join(rel);
    let md = match std::fs::symlink_metadata(&target) {
        Ok(md) => md,
        Err(e) if e.kind() == ErrorKind::NotFound => return HostState::Absent,
        Err(_) => return HostState::Unreadable,
    };

    if md.is_dir() {
        HostState::Dir
    } else if md.is_file() {
        safety::hash_file(&target).map_or(HostState::Unreadable, |sha256_hex| HostState::File {
            sha256_hex,
        })
    } else {
        // fifo / socket / device / etc. — cannot safely mutate. Fail closed.
        HostState::Unreadable
    }
}

/// Whether `state` is byte-and-type identical to the `host@in` `baseline` at
/// `rel` — i.e. the host path is unchanged since upload.
///
/// `UnsafeSymlink` and `Unreadable` always return `false` (fail closed): a path
/// we cannot trust to be unchanged is treated as a conflict.
fn matches_host_at_in(state: &HostState, baseline: &safety::Manifest, rel: &str) -> bool {
    match state {
        HostState::Absent => !baseline.files.contains_key(rel) && !baseline.dirs.contains(rel),
        HostState::File { sha256_hex } => baseline
            .files
            .get(rel)
            .is_some_and(|e| &e.sha256_hex == sha256_hex),
        HostState::Dir => baseline.dirs.contains(rel),
        HostState::UnsafeSymlink | HostState::Unreadable => false,
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::env::{
        ConflictDetect, EnvId, EnvInfo, EnvSpec, EnvState, SyncStrategy, WorkspaceBinding,
        WriteBackPolicy, default_max_files,
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

    /// A write-back that FAILS during teardown must NOT destroy the ephemeral
    /// root: that env-side tree is the only copy of the node's output. The fake's
    /// injected `download` failure drives the write-back error (while `list_tree`/
    /// `remove_*` stay healthy, so the only reason the root survives is the
    /// preserve skip — not a coincidentally-failed deletion). The root is kept
    /// for manual recovery.
    #[tokio::test]
    async fn teardown_preserves_root_when_writeback_fails() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"original").unwrap();

        let (mgr, fake, root) = manager_seeded_via_reconcile(
            "/tmp/ordius-wb-fail-{{run.id}}",
            host_ws,
            WriteBackPolicy::Force { ignore: vec![] },
        )
        .await;

        // Node produced output on the remote.
        fake.upload_file(&format!("{root}/a.txt"), b"modified")
            .await
            .unwrap();

        // Make the write-back fail: `list_remote_files` downloads each file and
        // propagates the error, so `reconcile_write_back` returns Err. `list_tree`
        // and removal stay healthy, so a non-preserved root WOULD be deleted —
        // surviving here proves the preserve skip fired, not a failed remove_tree.
        fake.set_fail_download(true);

        mgr.teardown_all(RunOutcome::Failed).await;

        // The ephemeral root and the node's output must still be present (NOT
        // deleted) — `stat`/`list_tree` are unaffected by the download hook.
        assert!(
            fake.stat(&root).await.unwrap().is_some(),
            "ephemeral root must be preserved when its write-back failed"
        );
        assert!(
            fake.stat(&format!("{root}/a.txt")).await.unwrap().is_some(),
            "the node's only copy of its output must not be destroyed"
        );
    }

    /// A `reconcile_out` write-back that FAILS must persist the env-side root as
    /// preserved so a later `teardown_all` does NOT delete it — that tree is the
    /// node's only output copy. The download hook is turned back ON before
    /// teardown so teardown's own write-back would SUCCEED and (absent the
    /// persisted preserve) delete the root: its survival here proves the
    /// persisted-preserve path, not a coincidentally-failed teardown write-back.
    #[tokio::test]
    async fn reconcile_out_failure_preserves_root_from_teardown() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"orig").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-out-fail");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let root = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in")
            .as_str()
            .to_string();

        // The node produced output on the remote.
        fake.upload_file(&format!("{root}/a.txt"), b"node-out")
            .await
            .unwrap();

        // Drive the reconcile_out write-back to Err: `list_remote_files` downloads
        // each file and propagates the failure.
        fake.set_fail_download(true);
        let err = mgr
            .reconcile_out(&d, &binding, host_ws)
            .await
            .expect_err("reconcile_out write-back must fail");
        assert!(
            matches!(err, DispatchError::WorkspaceUnavailable { .. }),
            "reconcile_out surfaces the injected transport error: {err}"
        );

        // Heal the transport so teardown's OWN write-back succeeds and WOULD
        // delete the root were it not persisted as preserved.
        fake.set_fail_download(false);
        mgr.teardown_all(RunOutcome::Failed).await;

        // The root + the node's output survive: the persisted preserve skipped it.
        assert!(
            fake.stat(&root).await.unwrap().is_some(),
            "root preserved by reconcile_out failure must not be deleted by teardown"
        );
        assert!(
            fake.stat(&format!("{root}/a.txt")).await.unwrap().is_some(),
            "the node's only output copy must survive teardown"
        );
    }

    /// After a `reconcile_out` write-back failure preserves a root, a later
    /// same-key `reconcile_in` MOVES the unreconciled output aside to a recovery
    /// sibling and resets the root clean — rather than wedging the run. The
    /// recovery copy keeps the node's bytes; the reset root mirrors the host; and
    /// the root, no longer preserved, is deleted by teardown while the recovery
    /// copy survives for manual retrieval.
    #[tokio::test]
    async fn reconcile_in_recovers_preserved_root() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"orig").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-in-recover");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let root = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in")
            .as_str()
            .to_string();

        fake.upload_file(&format!("{root}/a.txt"), b"node-out")
            .await
            .unwrap();

        // Fail the reconcile_out write-back to preserve (env, root).
        fake.set_fail_download(true);
        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect_err("reconcile_out write-back must fail");

        // Heal the transport and reconcile_in for the SAME key again: the
        // preserved output is moved aside and the root is reset to the host.
        fake.set_fail_download(false);
        let reused = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in must recover, not refuse")
            .as_str()
            .to_string();
        assert_eq!(reused, root, "the same key resolves to the same root");

        // The recovery sibling holds the node's unreconciled output verbatim.
        let recovery = format!("{root}.recovery");
        assert!(
            fake.stat(&recovery).await.unwrap().is_some(),
            "preserved output must be moved to a recovery sibling"
        );
        assert_eq!(
            fake.download_file(&format!("{recovery}/a.txt"))
                .await
                .unwrap(),
            b"node-out",
            "the recovery copy must keep the node's output"
        );

        // The reset root now mirrors the host (a.txt == the host bytes).
        assert_eq!(
            fake.download_file(&format!("{root}/a.txt")).await.unwrap(),
            b"orig",
            "the recovered root must be reset to the host workspace"
        );

        // The root is no longer preserved: teardown deletes it (a normal
        // ephemeral root now) while keeping the recovery copy for the user.
        mgr.teardown_all(RunOutcome::Failed).await;
        assert!(
            fake.stat(&root).await.unwrap().is_none(),
            "the recovered (reset) root is a normal ephemeral root — teardown deletes it"
        );
        assert!(
            fake.stat(&recovery).await.unwrap().is_some(),
            "the recovery copy survives teardown for manual retrieval"
        );
    }

    /// Two envs with DIFFERENT ids whose templates expand to the SAME root string
    /// (e.g. the same path on different servers) must each be tracked + torn down
    /// via their OWN factory. Before the `(EnvId, root)` re-key, the second
    /// `reconcile_in` clobbered the first in the root-keyed map — one server's
    /// root leaked, or the wrong server's tree was deleted.
    #[tokio::test]
    async fn teardown_two_envs_same_root_no_cross_clobber() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"host-a").unwrap();

        // Two distinct envs ("ssh:a" / "ssh:b"), each with its OWN fake "server".
        let (d_a, fake_a) = ssh_dispatcher_with_fake("a");
        let (d_b, fake_b) = ssh_dispatcher_with_fake("b");
        let mgr = WorkspaceManager::new();

        // Same template → same expanded root string on both servers (the run id
        // is identical via `sample_run`), so the strings collide exactly — still
        // Ephemeral via the `{{run.id}}` marker.
        let binding = sftp_binding("/tmp/ordius-shared-{{run.id}}", WriteBackPolicy::None);

        let root_a = mgr
            .reconcile_in(&d_a, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in env a")
            .as_str()
            .to_string();
        let root_b = mgr
            .reconcile_in(&d_b, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in env b")
            .as_str()
            .to_string();

        assert_eq!(
            root_a, root_b,
            "same template + run id → identical root string"
        );

        // Each server has its own copy of the workspace before teardown.
        assert!(fake_a.stat(&root_a).await.unwrap().is_some());
        assert!(fake_b.stat(&root_b).await.unwrap().is_some());

        mgr.teardown_all(RunOutcome::Completed).await;

        // BOTH roots deleted, each via its OWN factory — no cross-clobber and no
        // leak. Before the fix the env-b factory overwrote env-a's entry, so
        // env-a's root was never reached (it would still exist here).
        assert!(
            fake_a.stat(&root_a).await.unwrap().is_none(),
            "env a's root must be deleted via env a's own factory"
        );
        assert!(
            fake_b.stat(&root_b).await.unwrap().is_none(),
            "env b's root must be deleted via env b's own factory"
        );
    }

    /// A `SafeOrDiverge` node that never reached `reconcile_out` (e.g. it failed
    /// mid-run) still gets its write-back via the teardown dispatcher. When the
    /// host changed concurrently, teardown must DIVERGE (keep the host, stash the
    /// node bytes under `.ordius/diverged/`) — NOT force-overwrite the host.
    #[tokio::test]
    async fn teardown_safe_or_diverge_failed_run_diverges() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"orig").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-teardown");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let root = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in")
            .as_str()
            .to_string();

        // Node wrote output remotely; the host was edited concurrently AFTER the
        // baseline upload — a conflict. reconcile_out is intentionally NOT called
        // (simulating a node that failed before its write-back ran).
        fake.upload_file(&format!("{root}/a.txt"), b"node-out")
            .await
            .unwrap();
        std::fs::write(host_ws.join("a.txt"), b"user-edit").unwrap();

        mgr.teardown_all(RunOutcome::Failed).await;

        // Host preserved (NOT force-clobbered).
        assert_eq!(
            std::fs::read(host_ws.join("a.txt")).unwrap(),
            b"user-edit",
            "teardown must diverge, not force-overwrite the host"
        );
        // Node bytes stashed under the divergence dir.
        let artifact = diverged_dir(host_ws, "ssh:sod-teardown").join("a.txt");
        assert_eq!(
            std::fs::read(&artifact).unwrap(),
            b"node-out",
            "node output must be preserved under .ordius/diverged/"
        );
        let report = read_diverge_report(host_ws, "ssh:sod-teardown");
        let entry = report_entry(&report, "a.txt");
        assert_eq!(entry["reason"], "host_modified");
    }

    /// User cancellation skips the `SafeOrDiverge` write-back entirely (no host
    /// write, no divergence artifacts) but STILL deletes the ephemeral root —
    /// cleanup is unconditional even though write-back is skipped.
    #[tokio::test]
    async fn teardown_cancelled_skips_writeback() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"orig").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-cancel");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let root = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in")
            .as_str()
            .to_string();

        // Both a remote change and a conflicting host edit are present — yet user
        // cancel must skip the write-back regardless.
        fake.upload_file(&format!("{root}/a.txt"), b"node-out")
            .await
            .unwrap();
        std::fs::write(host_ws.join("a.txt"), b"user-edit").unwrap();

        mgr.teardown_all(RunOutcome::CancelledByUser).await;

        // No write-back: host untouched, no divergence dir created.
        assert_eq!(
            std::fs::read(host_ws.join("a.txt")).unwrap(),
            b"user-edit",
            "user cancel must skip write-back"
        );
        assert!(
            !diverged_dir(host_ws, "ssh:sod-cancel").exists(),
            "user cancel must not produce divergence artifacts"
        );
        // Cleanup still happens.
        assert!(
            fake.stat(&root).await.unwrap().is_none(),
            "ephemeral root must be deleted even on user cancel"
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

    /// Only the `Manifest` conflict mode is implemented for `SafeOrDiverge`:
    /// `Checksum` and `MtimeSize` are rejected by `reconcile_in` BEFORE any
    /// upload (the node fails before running and no `WorkspaceState` is stored),
    /// while `Manifest` succeeds and uploads the host tree as the baseline.
    #[tokio::test]
    async fn reconcile_in_rejects_unsupported_safe_or_diverge_modes() {
        // Unsupported modes (Checksum, MtimeSize) are rejected before any upload.
        for mode in [ConflictDetect::Checksum, ConflictDetect::MtimeSize] {
            let host = tempfile::TempDir::new().unwrap();
            let host_ws = host.path();
            std::fs::write(host_ws.join("a.txt"), b"host-a").unwrap();

            let (d, fake) = ssh_dispatcher_with_fake("sod-reject");
            let mgr = WorkspaceManager::new();
            let binding = sftp_binding(
                "/tmp/ordius-{{run.id}}",
                WriteBackPolicy::SafeOrDiverge {
                    mode,
                    ignore: vec![],
                    max_files: default_max_files(),
                },
            );

            let err = mgr
                .reconcile_in(&d, &binding, host_ws, &sample_run())
                .await
                .unwrap_err();
            assert!(
                matches!(err, DispatchError::Unsupported(_)),
                "expected Unsupported for mode {mode:?}; got: {err}"
            );
            assert!(
                err.to_string().contains("SafeOrDiverge"),
                "error should name SafeOrDiverge; got: {err}"
            );

            // No state stored: nothing uploaded, teardown writes nothing back.
            assert!(
                fake.stat("/tmp/ordius-r1").await.unwrap().is_none(),
                "no remote root must be created when reconcile_in rejects the mode"
            );
            mgr.teardown_all(RunOutcome::Failed).await;
            assert_eq!(
                std::fs::read(host_ws.join("a.txt")).unwrap(),
                b"host-a",
                "teardown must not write back for a rejected SafeOrDiverge mode"
            );
        }

        // The supported Manifest mode succeeds and uploads the host tree.
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"host-a").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-manifest");
        let mgr = WorkspaceManager::new();
        let binding = sftp_binding(
            "/tmp/ordius-{{run.id}}",
            WriteBackPolicy::SafeOrDiverge {
                mode: ConflictDetect::Manifest,
                ignore: vec![],
                max_files: default_max_files(),
            },
        );

        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("Manifest mode must be accepted by reconcile_in");
        let root = cwd.as_str().to_string();
        assert_eq!(root, "/tmp/ordius-r1");
        // The host tree was uploaded as the baseline.
        assert_eq!(remote_files(&fake, &root).await, vec!["a.txt".to_string()]);
        assert_eq!(
            fake.download_file(&format!("{root}/a.txt")).await.unwrap(),
            b"host-a",
        );
    }

    // ── SafeOrDiverge (Manifest) conflict-divergence matrix ───────────────────

    /// A `SafeOrDiverge { Manifest }` binding with default caps and no ignores.
    fn sod_binding() -> WorkspaceBinding {
        sftp_binding(
            "/tmp/ordius-{{run.id}}",
            WriteBackPolicy::SafeOrDiverge {
                mode: ConflictDetect::Manifest,
                ignore: vec![],
                max_files: default_max_files(),
            },
        )
    }

    /// The `.ordius/diverged/<enc run>/<enc env>` directory for `env_id` under
    /// `host_ws` (run id is always `"r1"` via `sample_run`).
    fn diverged_dir(host_ws: &Path, env_id: &str) -> std::path::PathBuf {
        host_ws
            .join(".ordius")
            .join("diverged")
            .join(safety::encode_segment("r1"))
            .join(safety::encode_segment(env_id))
    }

    /// Read + parse the `diverge-report.json` under the divergence dir for `env_id`.
    fn read_diverge_report(host_ws: &Path, env_id: &str) -> serde_json::Value {
        let path = diverged_dir(host_ws, env_id).join("diverge-report.json");
        let raw = std::fs::read(&path)
            .unwrap_or_else(|e| panic!("diverge report at {}: {e}", path.display()));
        serde_json::from_slice(&raw).unwrap()
    }

    /// Find the single report entry whose `rel` equals `rel`.
    fn report_entry<'a>(report: &'a serde_json::Value, rel: &str) -> &'a serde_json::Value {
        report["diverged"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["rel"] == rel)
            .unwrap_or_else(|| panic!("no report entry for rel `{rel}` in {report}"))
    }

    /// Host untouched since `reconcile_in` → the node's bytes are written back in
    /// place; no divergence dir is created.
    #[tokio::test]
    async fn sod_host_untouched_writes_back() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"orig").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-untouched");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        fake.upload_file(&format!("{root}/a.txt"), b"node-out")
            .await
            .unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        assert_eq!(std::fs::read(host_ws.join("a.txt")).unwrap(), b"node-out");
        assert!(
            !diverged_dir(host_ws, "ssh:sod-untouched").exists(),
            "no divergence dir for a clean write-back"
        );
    }

    /// Host modified concurrently → host KEPT, the node's bytes diverge under
    /// `.ordius/diverged/...`, report reason `host_modified`.
    #[tokio::test]
    async fn sod_host_modified_diverges() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"orig").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-modified");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        fake.upload_file(&format!("{root}/a.txt"), b"node-out")
            .await
            .unwrap();
        // Concurrent host edit AFTER reconcile_in uploaded the baseline.
        std::fs::write(host_ws.join("a.txt"), b"user-edit").unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        // Host is preserved.
        assert_eq!(std::fs::read(host_ws.join("a.txt")).unwrap(), b"user-edit");
        // Node bytes preserved under the divergence dir.
        let artifact = diverged_dir(host_ws, "ssh:sod-modified").join("a.txt");
        assert_eq!(std::fs::read(&artifact).unwrap(), b"node-out");
        // Report records the conflict.
        let report = read_diverge_report(host_ws, "ssh:sod-modified");
        let entry = report_entry(&report, "a.txt");
        assert_eq!(entry["reason"], "host_modified");
        assert!(entry.get("remote_sha256").is_some(), "got {entry}");
        assert!(entry.get("diverged_path").is_some(), "got {entry}");
    }

    /// A new node file at a rel the host never had → written back in place; no
    /// divergence.
    #[tokio::test]
    async fn sod_new_file_host_absent_writes() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("keep.txt"), b"k").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-new");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        fake.upload_file(&format!("{root}/c.txt"), b"new-c")
            .await
            .unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        assert_eq!(std::fs::read(host_ws.join("c.txt")).unwrap(), b"new-c");
        assert!(
            !diverged_dir(host_ws, "ssh:sod-new").exists(),
            "a brand-new file is not a conflict"
        );
    }

    /// Host created a file at the rel the node also writes → host KEPT, node
    /// bytes diverge, report reason `host_created`.
    #[tokio::test]
    async fn sod_host_created_diverges() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("keep.txt"), b"k").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-created");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        fake.upload_file(&format!("{root}/c.txt"), b"node-c")
            .await
            .unwrap();
        // Host concurrently creates c.txt too.
        std::fs::write(host_ws.join("c.txt"), b"user-c").unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        assert_eq!(std::fs::read(host_ws.join("c.txt")).unwrap(), b"user-c");
        let artifact = diverged_dir(host_ws, "ssh:sod-created").join("c.txt");
        assert_eq!(std::fs::read(&artifact).unwrap(), b"node-c");
        let report = read_diverge_report(host_ws, "ssh:sod-created");
        assert_eq!(report_entry(&report, "c.txt")["reason"], "host_created");
    }

    /// Remote deletion, host unchanged → the deletion propagates to the host.
    #[tokio::test]
    async fn sod_deletion_host_unchanged_removes() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"a").unwrap();
        std::fs::write(host_ws.join("b.txt"), b"b").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-del-clean");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        fake.remove_file(&format!("{root}/b.txt")).await.unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        assert!(!host_ws.join("b.txt").exists(), "b.txt deletion propagates");
        assert!(host_ws.join("a.txt").exists(), "a.txt survives");
    }

    /// Remote deletion AND a concurrent host deletion of the same rel agree →
    /// no-op: host stays absent and NO divergence report is written.
    #[tokio::test]
    async fn sod_deletion_host_also_absent_is_noop() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"a").unwrap();
        std::fs::write(host_ws.join("b.txt"), b"b").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-del-agree");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        // Node deletes b.txt on the remote AND the user deletes it on the host.
        fake.remove_file(&format!("{root}/b.txt")).await.unwrap();
        std::fs::remove_file(host_ws.join("b.txt")).unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        assert!(!host_ws.join("b.txt").exists(), "b.txt stays absent");
        assert!(
            !host_ws.join(".ordius").exists(),
            "agreeing deletions must not write a divergence report"
        );
    }

    /// Remote deletion vs a concurrent host edit → host KEPT, report reason
    /// `delete_vs_host_modified` with NO remote bytes recorded.
    #[tokio::test]
    async fn sod_deletion_host_modified_keeps() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"a").unwrap();
        std::fs::write(host_ws.join("b.txt"), b"orig-b").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-del-mod");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        fake.remove_file(&format!("{root}/b.txt")).await.unwrap();
        std::fs::write(host_ws.join("b.txt"), b"user-b").unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        assert_eq!(std::fs::read(host_ws.join("b.txt")).unwrap(), b"user-b");
        let report = read_diverge_report(host_ws, "ssh:sod-del-mod");
        let entry = report_entry(&report, "b.txt");
        assert_eq!(entry["reason"], "delete_vs_host_modified");
        assert!(
            entry.get("diverged_path").is_none(),
            "a delete conflict has no remote artifact; got {entry}"
        );
        assert!(
            entry.get("remote_sha256").is_none(),
            "a delete conflict has no remote bytes; got {entry}"
        );
    }

    /// A remote symlink shadows a host file AND a host subtree → neither is
    /// deleted; both are reported `remote_unsupported_symlink`.
    #[tokio::test]
    async fn sod_remote_symlink_keeps_host_file_and_subtree() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"ha").unwrap();
        std::fs::create_dir(host_ws.join("sub")).unwrap();
        std::fs::write(host_ws.join("sub").join("c.txt"), b"hc").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-symlink");
        // The factory shares state with `fake`, so seed symlinks through it.
        let factory = FakeWorkspaceTransportFactory::new(fake.clone());
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        // Node shadows a.txt and the sub subtree with remote symlinks.
        factory.seed_symlink(&format!("{root}/a.txt"), "/etc/passwd");
        factory.seed_symlink(&format!("{root}/sub"), "/var/evil");

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        // Both host paths survive untouched.
        assert_eq!(std::fs::read(host_ws.join("a.txt")).unwrap(), b"ha");
        assert_eq!(
            std::fs::read(host_ws.join("sub").join("c.txt")).unwrap(),
            b"hc",
        );
        // Report flags the unsupported symlinks.
        let report = read_diverge_report(host_ws, "ssh:sod-symlink");
        let reasons: Vec<&str> = report["diverged"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["reason"].as_str().unwrap())
            .collect();
        assert!(
            reasons.iter().all(|r| *r == "remote_unsupported_symlink"),
            "all entries must be remote_unsupported_symlink; got {reasons:?}"
        );
        assert!(
            reasons.len() >= 2,
            "both the file and the subtree symlink must be reported; got {reasons:?}"
        );
    }

    /// The node creates an empty directory the host lacks → created on the host.
    #[tokio::test]
    async fn sod_empty_dir_created() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("keep.txt"), b"k").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-dir-create");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        fake.mkdir(&format!("{root}/newdir")).await.unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        assert!(host_ws.join("newdir").is_dir(), "newdir must be created");
    }

    /// The node empties then removes a directory the host did not touch → the
    /// file is deleted AND the now-empty host dir is pruned.
    #[tokio::test]
    async fn sod_empty_dir_pruned() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::create_dir(host_ws.join("d")).unwrap();
        std::fs::write(host_ws.join("d").join("x.txt"), b"x").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-dir-prune");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        fake.remove_file(&format!("{root}/d/x.txt")).await.unwrap();
        fake.remove_dir(&format!("{root}/d")).await.unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        assert!(!host_ws.join("d").join("x.txt").exists(), "d/x.txt removed");
        assert!(!host_ws.join("d").exists(), "empty host dir d pruned");
    }

    /// The node removes a dir, but the host added an untracked file under it →
    /// the dir + untracked file survive; report reason `dir_delete_nonempty`.
    #[tokio::test]
    async fn sod_nonempty_dir_kept_reports() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::create_dir(host_ws.join("d")).unwrap();
        std::fs::write(host_ws.join("d").join("x.txt"), b"x").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-dir-nonempty");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        fake.remove_file(&format!("{root}/d/x.txt")).await.unwrap();
        fake.remove_dir(&format!("{root}/d")).await.unwrap();
        // Host adds an untracked file under d AFTER the baseline.
        std::fs::write(host_ws.join("d").join("keep.txt"), b"u").unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        assert_eq!(
            std::fs::read(host_ws.join("d").join("keep.txt")).unwrap(),
            b"u",
            "untracked host file survives",
        );
        assert!(host_ws.join("d").is_dir(), "non-empty host dir d kept");
        let report = read_diverge_report(host_ws, "ssh:sod-dir-nonempty");
        assert_eq!(report_entry(&report, "d")["reason"], "dir_delete_nonempty");
    }

    /// The node replaces a directory with a file at the same rel (host
    /// unchanged) → the host dir + its file go, and `d` becomes a regular file.
    #[tokio::test]
    async fn sod_type_change_dir_to_file() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::create_dir(host_ws.join("d")).unwrap();
        std::fs::write(host_ws.join("d").join("old.txt"), b"o").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-type-change");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        fake.remove_file(&format!("{root}/d/old.txt"))
            .await
            .unwrap();
        fake.remove_dir(&format!("{root}/d")).await.unwrap();
        fake.upload_file(&format!("{root}/d"), b"now-a-file")
            .await
            .unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        assert!(
            !host_ws.join("d").join("old.txt").exists(),
            "the old dir child is gone",
        );
        let d_path = host_ws.join("d");
        assert!(d_path.is_file(), "d must now be a regular file");
        assert_eq!(std::fs::read(&d_path).unwrap(), b"now-a-file");
    }

    /// The node replaces a dir with a file, but the host concurrently edits the
    /// dir's child after `reconcile_in`. The child no longer matches `host@in`,
    /// so its deletion diverges (`delete_vs_host_modified`) and the child is
    /// KEPT — which leaves the host dir non-empty, so the dir→file replacement
    /// can't complete and the node's file `d` diverges (`host_type_changed`).
    #[tokio::test]
    async fn sod_type_change_dir_to_file_under_concurrent_host_modify() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::create_dir(host_ws.join("d")).unwrap();
        std::fs::write(host_ws.join("d").join("old.txt"), b"o").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-type-change-race");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        // Node output: remove the child + the dir, replace `d` with a file.
        fake.remove_file(&format!("{root}/d/old.txt"))
            .await
            .unwrap();
        fake.remove_dir(&format!("{root}/d")).await.unwrap();
        fake.upload_file(&format!("{root}/d"), b"now-a-file")
            .await
            .unwrap();
        // Concurrent host edit of the child AFTER reconcile_in uploaded baseline.
        std::fs::write(host_ws.join("d").join("old.txt"), b"user-edit").unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        // The user's concurrent edit is preserved (the deletion diverged).
        assert_eq!(
            std::fs::read(host_ws.join("d").join("old.txt")).unwrap(),
            b"user-edit",
            "concurrently-edited child must be kept",
        );
        // `d` stays a directory — it could not be emptied, so the file can't land.
        assert!(
            host_ws.join("d").is_dir(),
            "host `d` must stay a dir (non-empty, can't be replaced)",
        );
        // The node's file bytes diverge instead of clobbering the host dir.
        let artifact = diverged_dir(host_ws, "ssh:sod-type-change-race").join("d");
        assert_eq!(std::fs::read(&artifact).unwrap(), b"now-a-file");

        let report = read_diverge_report(host_ws, "ssh:sod-type-change-race");
        assert_eq!(
            report_entry(&report, "d/old.txt")["reason"],
            "delete_vs_host_modified",
        );
        assert_eq!(report_entry(&report, "d")["reason"], "host_type_changed");
    }

    /// `max_files` is a fail-fast cap: when more entries would be touched than
    /// allowed, `reconcile_out` errors BEFORE any host mutation.
    #[tokio::test]
    async fn sod_max_files_fail_fast() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"a").unwrap();
        std::fs::write(host_ws.join("b.txt"), b"b").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-maxfiles");
        let mgr = WorkspaceManager::new();
        let binding = sftp_binding(
            "/tmp/ordius-{{run.id}}",
            WriteBackPolicy::SafeOrDiverge {
                mode: ConflictDetect::Manifest,
                ignore: vec![],
                max_files: 1,
            },
        );
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        // Two changed files → 2 > max_files=1.
        fake.upload_file(&format!("{root}/a.txt"), b"node-a")
            .await
            .unwrap();
        fake.upload_file(&format!("{root}/b.txt"), b"node-b")
            .await
            .unwrap();

        let err = mgr
            .reconcile_out(&d, &binding, host_ws)
            .await
            .expect_err("max_files cap must error");
        assert!(
            err.to_string().contains("max_files"),
            "error should mention the cap; got: {err}"
        );

        // Nothing was written — both host files keep their ORIGINAL content.
        assert_eq!(std::fs::read(host_ws.join("a.txt")).unwrap(), b"a");
        assert_eq!(std::fs::read(host_ws.join("b.txt")).unwrap(), b"b");
        assert!(
            !diverged_dir(host_ws, "ssh:sod-maxfiles").exists(),
            "fail-fast leaves no divergence artifacts",
        );
    }

    /// A clean (conflict-free) write-back leaves no `.ordius/` trace at all.
    #[tokio::test]
    async fn sod_report_omitted_no_divergence() {
        let host = tempfile::TempDir::new().unwrap();
        let host_ws = host.path();
        std::fs::write(host_ws.join("a.txt"), b"orig").unwrap();

        let (d, fake) = ssh_dispatcher_with_fake("sod-clean");
        let mgr = WorkspaceManager::new();
        let binding = sod_binding();
        let cwd = mgr
            .reconcile_in(&d, &binding, host_ws, &sample_run())
            .await
            .expect("reconcile_in");
        let root = cwd.as_str().to_string();

        fake.upload_file(&format!("{root}/a.txt"), b"node")
            .await
            .unwrap();

        mgr.reconcile_out(&d, &binding, host_ws)
            .await
            .expect("reconcile_out");

        assert_eq!(std::fs::read(host_ws.join("a.txt")).unwrap(), b"node");
        assert!(
            !host_ws.join(".ordius").exists(),
            "a clean run must leave no .ordius trace",
        );
    }

    // ── classify_host_state / matches_host_at_in ──────────────────────────────

    /// `classify_host_state` reports each live host shape correctly, and its
    /// `File` hash is byte-comparable with a manifest `FileEntry.sha256_hex`
    /// (both go through `safety::sha256_hex` of the same bytes).
    #[test]
    fn classify_host_state_table() {
        let tmp = tempfile::tempdir().unwrap();
        let host_ws = tmp.path();

        // A regular file → File { sha256_hex } matching sha256_hex(<bytes>).
        let bytes = b"host file contents";
        std::fs::write(host_ws.join("f.txt"), bytes).unwrap();
        assert_eq!(
            classify_host_state(host_ws, "f.txt"),
            HostState::File {
                sha256_hex: safety::sha256_hex(bytes),
            },
        );

        // A directory → Dir.
        std::fs::create_dir(host_ws.join("d")).unwrap();
        assert_eq!(classify_host_state(host_ws, "d"), HostState::Dir);

        // A missing rel → Absent.
        assert_eq!(classify_host_state(host_ws, "missing"), HostState::Absent);

        // A path whose component is a symlink → UnsafeSymlink.
        #[cfg(unix)]
        {
            let target = host_ws.join("real");
            std::fs::create_dir(&target).unwrap();
            std::os::unix::fs::symlink(&target, host_ws.join("link")).unwrap();
            assert_eq!(
                classify_host_state(host_ws, "link/inside.txt"),
                HostState::UnsafeSymlink,
            );
            // The symlink itself (terminal component) is also unsafe.
            assert_eq!(
                classify_host_state(host_ws, "link"),
                HostState::UnsafeSymlink
            );
        }

        // An unreadable component (chmod 0o000 parent) → Unreadable. Skipped as
        // root, where permission bits are bypassed.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            if !running_as_root() {
                let blocked = host_ws.join("blocked");
                std::fs::create_dir(&blocked).unwrap();
                std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o000)).unwrap();
                let state = classify_host_state(host_ws, "blocked/child");
                // Restore perms so tempdir cleanup can recurse.
                std::fs::set_permissions(&blocked, std::fs::Permissions::from_mode(0o755)).unwrap();
                assert_eq!(state, HostState::Unreadable, "got {state:?}");
            }
        }
    }

    /// `matches_host_at_in` compares a `HostState` against the `host@in`
    /// baseline manifest: identical → true; any byte/type/presence drift or an
    /// unsafe/unreadable host → false.
    #[test]
    fn matches_host_at_in_predicate() {
        let mut baseline = safety::Manifest::new();
        baseline.files.insert(
            "f.txt".to_string(),
            safety::FileEntry {
                sha256_hex: safety::sha256_hex(b"baseline bytes"),
                size: 14,
                mode: 0o644,
            },
        );
        baseline.dirs.insert("d".to_string());

        // File with the baseline sha → matches; a different sha → no match.
        assert!(matches_host_at_in(
            &HostState::File {
                sha256_hex: safety::sha256_hex(b"baseline bytes"),
            },
            &baseline,
            "f.txt",
        ));
        assert!(!matches_host_at_in(
            &HostState::File {
                sha256_hex: safety::sha256_hex(b"drifted bytes"),
            },
            &baseline,
            "f.txt",
        ));

        // Dir for a baseline dir → matches; for an unknown rel → no match.
        assert!(matches_host_at_in(&HostState::Dir, &baseline, "d"));
        assert!(!matches_host_at_in(&HostState::Dir, &baseline, "unknown"));

        // Absent for an unknown rel → matches (nothing was uploaded there);
        // Absent for a baseline file → no match (the host lost it).
        assert!(matches_host_at_in(&HostState::Absent, &baseline, "unknown"));
        assert!(!matches_host_at_in(&HostState::Absent, &baseline, "f.txt"));

        // Unsafe / unreadable host states never match — fail closed.
        assert!(!matches_host_at_in(
            &HostState::UnsafeSymlink,
            &baseline,
            "f.txt"
        ));
        assert!(!matches_host_at_in(
            &HostState::Unreadable,
            &baseline,
            "f.txt"
        ));
    }

    /// True when the test process can lstat inside a 0o000 directory it owns —
    /// the signature of running as root (permission bits are bypassed).
    #[cfg(unix)]
    fn running_as_root() -> bool {
        use std::os::unix::fs::PermissionsExt as _;
        let probe = tempfile::tempdir().unwrap();
        let dir = probe.path().join("p");
        std::fs::create_dir(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o000)).unwrap();
        let can_read = std::fs::symlink_metadata(dir.join("anything")).is_ok()
            || std::fs::read_dir(&dir).is_ok();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        can_read
    }

    // ── SafeOrDiverge artifact / report writers ───────────────────────────────

    /// Writing a divergence artifact must fail closed when `.ordius` is a
    /// symlink: nothing may be written *through* it (a symlinked dir could
    /// redirect the write outside the workspace).
    #[cfg(unix)]
    #[test]
    fn write_diverged_artifact_fails_closed_on_symlinked_ordius() {
        let tmp = tempfile::tempdir().unwrap();
        let host_ws = tmp.path();

        // A real target dir that `.ordius` will point at, plus the symlink.
        let target = tmp.path().join("redirect-target");
        std::fs::create_dir(&target).unwrap();
        std::os::unix::fs::symlink(&target, host_ws.join(".ordius")).unwrap();

        let res = write_diverged_artifact(host_ws, "run1", "ssh:h", "src/o.txt", b"remote");
        assert!(res.is_err(), "expected fail-closed Err, got {res:?}");

        // Nothing was written through the symlink: the target dir stays empty.
        let leaked: Vec<_> = std::fs::read_dir(&target).unwrap().collect();
        assert!(
            leaked.is_empty(),
            "symlink target must stay empty; found {leaked:?}"
        );
    }

    /// Happy path: the artifact lands under `.ordius/diverged/<run>/<env>/<rel>`
    /// and the in-place host path is never touched.
    #[test]
    fn write_diverged_artifact_happy_path() {
        let tmp = tempfile::tempdir().unwrap();
        let host_ws = tmp.path();

        let artifact_rel =
            write_diverged_artifact(host_ws, "run1", "ssh:h", "src/o.txt", b"remote-bytes")
                .unwrap();

        assert!(
            artifact_rel.starts_with(".ordius/diverged/"),
            "rel must be under .ordius/diverged/; got {artifact_rel}"
        );
        assert!(
            artifact_rel.ends_with("/src/o.txt"),
            "rel must end with the original relative path; got {artifact_rel}"
        );

        let written = std::fs::read(host_ws.join(&artifact_rel)).unwrap();
        assert_eq!(written, b"remote-bytes");

        // Divergence never clobbers the in-place path.
        assert!(
            !host_ws.join("src/o.txt").exists(),
            "the in-place host file must not be created by divergence"
        );
    }

    /// The report serializes `reason` in `snake_case` and omits the optional sha
    /// / path fields when they are `None` (`skip_serializing_if`).
    #[test]
    fn write_diverge_report_writes_json() {
        let tmp = tempfile::tempdir().unwrap();
        let host_ws = tmp.path();

        let report = DivergeReport {
            run_id: "run1".into(),
            env_id: "ssh:h".into(),
            diverged: vec![
                DivergeEntry {
                    rel: "src/o.txt".into(),
                    reason: DivergeReason::HostModified,
                    host_sha256: Some("ab".into()),
                    remote_sha256: Some("cd".into()),
                    diverged_path: Some(".ordius/diverged/.../src/o.txt".into()),
                },
                DivergeEntry {
                    rel: "gone".into(),
                    reason: DivergeReason::DeleteVsHostModified,
                    host_sha256: None,
                    remote_sha256: None,
                    diverged_path: None,
                },
            ],
        };

        write_diverge_report(host_ws, "run1", "ssh:h", &report).unwrap();

        let report_rel = format!(
            ".ordius/diverged/{}/{}/diverge-report.json",
            safety::encode_segment("run1"),
            safety::encode_segment("ssh:h"),
        );
        let raw = std::fs::read(host_ws.join(&report_rel)).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&raw).unwrap();

        let entries = json["diverged"].as_array().unwrap();
        assert_eq!(entries.len(), 2);

        // First entry: snake_case reason + all three optional fields present.
        let first = &entries[0];
        assert_eq!(first["reason"], "host_modified");
        assert_eq!(first["host_sha256"], "ab");
        assert_eq!(first["remote_sha256"], "cd");
        assert_eq!(first["diverged_path"], ".ordius/diverged/.../src/o.txt");

        // Second entry: snake_case reason + the three optional fields OMITTED.
        let second = &entries[1];
        assert_eq!(second["reason"], "delete_vs_host_modified");
        assert!(
            second.get("host_sha256").is_none(),
            "host_sha256 must be omitted when None; got {second}"
        );
        assert!(
            second.get("remote_sha256").is_none(),
            "remote_sha256 must be omitted when None; got {second}"
        );
        assert!(
            second.get("diverged_path").is_none(),
            "diverged_path must be omitted when None; got {second}"
        );
    }

    /// Fail-closed: an empty `run_id` or `env_id` would `encode_segment` to `""`
    /// and collapse the `.ordius/diverged/<run>/<env>/` layout. Both writers
    /// reject it instead of writing.
    #[test]
    fn divergence_writers_reject_empty_ids() {
        let tmp = tempfile::tempdir().unwrap();
        let host_ws = tmp.path();

        assert!(
            write_diverged_artifact(host_ws, "", "ssh:h", "src/o.txt", b"x").is_err(),
            "empty run_id must be rejected"
        );
        assert!(
            write_diverged_artifact(host_ws, "run1", "", "src/o.txt", b"x").is_err(),
            "empty env_id must be rejected"
        );

        let report = DivergeReport {
            run_id: String::new(),
            env_id: "ssh:h".into(),
            diverged: vec![],
        };
        assert!(
            write_diverge_report(host_ws, "", "ssh:h", &report).is_err(),
            "empty run_id must be rejected by the report writer"
        );
        assert!(
            write_diverge_report(host_ws, "run1", "", &report).is_err(),
            "empty env_id must be rejected by the report writer"
        );

        // Nothing was written: `.ordius` was never created.
        assert!(
            !host_ws.join(".ordius").exists(),
            "no divergence dir must be created for a rejected empty id"
        );
    }
}
