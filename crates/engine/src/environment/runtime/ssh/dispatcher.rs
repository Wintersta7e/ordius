//! SSH dispatcher — process spawn + resource probe over the cached connection.
//!
//! `Dispatcher::spawn` runs `ordius-helper exec` over an exec channel (T10);
//! `Dispatcher::probe`/`probe_resource` run `ordius-helper probe` and stream
//! its JSONL outcomes (T12); `http_transport` tunnels HTTP through a local
//! listener (T11). Workspace preparation and path translation remain deferred
//! (compute-first).

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use std::collections::HashMap;

use ordius_helper::protocol::ProbeOutcomeV1;

use crate::environment::runtime::catalog::{ResourceCatalog, ResourceProbeOutcome};
use crate::environment::runtime::dispatcher::{Dispatcher, HttpTransport};
use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::helper_wire::wire_outcome_to_engine;
use crate::environment::runtime::plan::{ProbePlan, ProbeSummary};
use crate::environment::runtime::resource::{ResourceDefinition, ResourceId};
use crate::environment::runtime::transport::{
    EnvPath, EnvProcess, ProcessCmd, ProcessPipe, WorkspaceHandle,
};
use crate::environment::runtime::{EnvInfo, RunId, WorkspaceBinding};
use crate::secrets::Store;

use super::bootstrap::{RusshSftp, SshBootstrappedHelper, SshBootstrapper};
use super::config::SshConfig;
use super::connection::{
    RusshConnector, SshConnection, SshConnectionCache, SshConnectionLike as _,
};
use super::transport::RusshDirectTcpipOpener;
use super::transport::SshHttpTransport;

/// Target triple of the only helper binary we currently cross-compile and
/// embed. SSH targets are assumed to be `x86_64` Linux; remote-arch detection
/// is deferred (a later task can probe `uname -m` and pick a triple).
const HELPER_TRIPLE: &str = "x86_64-unknown-linux-musl";

/// Dispatches work to a remote SSH environment.
///
/// Holds one [`SshConnectionCache`] per dispatcher instance. The cache opens
/// a single authenticated session on first use and reuses it for subsequent
/// operations; if the session closes it reconnects once.
///
/// The cache is stored behind an `Arc` so that the [`RusshDirectTcpipOpener`]
/// inside `transport` can share the same connection without duplicating state.
/// The transport is built once in [`new`] and returned by clone from
/// [`http_transport`], matching the `LocalDispatcher`/`WslDispatcher` pattern.
pub struct SshDispatcher {
    /// Metadata describing the environment (id, label, state …).
    pub env_info: EnvInfo,
    /// Cached, authenticated connection to the remote host.
    pub(crate) cache: Arc<SshConnectionCache<RusshConnector>>,
    /// Lazily bootstrapped helper state. Initialised on the first `spawn` or
    /// `probe` via [`ensure_helper`]; subsequent calls return the cached result
    /// without re-uploading.
    ///
    /// [`ensure_helper`]: Self::ensure_helper
    pub(crate) helper: OnceCell<SshBootstrappedHelper>,
    /// HTTP transport, built once and shared by clone.
    ///
    /// Stored as `Arc<dyn HttpTransport>` so `http_transport()` can return a
    /// trivial `Arc::clone` with no cast — matches `LocalDispatcher` /
    /// `WslDispatcher`.
    transport: Arc<dyn HttpTransport>,
}

impl SshDispatcher {
    /// Build a dispatcher from an environment info record, the SSH connection
    /// config, and a secret store.
    ///
    /// The connection is not opened here — it is opened lazily on first use by
    /// [`SshConnectionCache::connection`].
    pub fn new(env_info: EnvInfo, cfg: SshConfig, secrets: Arc<Store>) -> Self {
        let env_id = env_info.id.to_string();
        let connector = RusshConnector {
            env_id: env_id.clone(),
            host: cfg.host,
            port: cfg.port,
            user: cfg.user,
            auth: cfg.auth,
            host_key_pins: cfg.host_key_pins,
            secrets,
        };
        let cache = Arc::new(SshConnectionCache::new(connector, env_id));
        // Build the transport once; the opener shares the same cache Arc so
        // every accepted socket reuses the same authenticated session.
        // Stored as Arc<dyn HttpTransport> so http_transport() is a trivial clone.
        let opener = Arc::new(RusshDirectTcpipOpener::new(Arc::clone(&cache)));
        let transport: Arc<dyn HttpTransport> = Arc::new(SshHttpTransport::new(opener));
        Self {
            env_info,
            cache,
            helper: OnceCell::new(),
            transport,
        }
    }

    /// Bootstrap with an explicit triple (used from integration tests and T10).
    #[allow(dead_code)] // used starting T10
    pub(crate) async fn bootstrap_helper_with_triple(
        &self,
        sftp: RusshSftp,
        triple: &str,
    ) -> Result<&SshBootstrappedHelper, DispatchError> {
        self.helper
            .get_or_try_init(|| async move {
                let bootstrapper = SshBootstrapper::new(sftp);
                bootstrapper.bootstrap(triple).await
            })
            .await
    }

    /// Return an open, cached connection (reconnecting once if dropped).
    pub(crate) async fn connection(&self) -> Result<Arc<SshConnection>, DispatchError> {
        self.cache.connection().await
    }

    /// Ensure the helper is bootstrapped on the remote, returning its installed
    /// state. The SFTP upload runs exactly once: subsequent calls return the
    /// cached [`SshBootstrappedHelper`] without opening an SFTP channel.
    pub(crate) async fn ensure_helper(&self) -> Result<&SshBootstrappedHelper, DispatchError> {
        // Fast path: already bootstrapped — avoid opening an SFTP channel.
        if let Some(helper) = self.helper.get() {
            return Ok(helper);
        }
        let conn = self.connection().await?;
        let sftp = open_sftp(&conn).await?;
        self.bootstrap_helper_with_triple(sftp, HELPER_TRIPLE).await
    }

    /// Build a helper probe plan and dispatch it over the SSH connection.
    ///
    /// The plan is serialised with the shared wire-format logic from
    /// [`crate::environment::runtime::helper_wire`] so SSH and WSL produce
    /// identical plans, then dispatched over the SSH connection (T12).
    async fn probe_plan_via_helper(
        &self,
        helper_path: &str,
        plan: ProbePlan,
        cancel: CancellationToken,
    ) -> Result<ProbeSummary, DispatchError> {
        let wire_plan = crate::environment::runtime::helper_wire::build_wire_plan(&plan)?;
        let plan_json = serde_json::to_vec(&wire_plan)
            .map_err(|e| DispatchError::PlanBuild(format!("serialize helper probe plan: {e}")))?;
        self.run_helper_probe_stream(helper_path, plan, plan_json, cancel)
            .await
    }

    /// Run `<helper> probe` over an SSH exec channel and stream its JSONL
    /// outcomes into a [`ProbeSummary`].
    ///
    /// Delegates line-by-line parsing to [`consume_ssh_helper_stream`] and
    /// guards against a crashed helper (no output + non-zero exit) mirroring
    /// the WSL dispatcher's `probe_plan_via_helper` pattern.
    async fn run_helper_probe_stream(
        &self,
        helper_path: &str,
        plan: ProbePlan,
        plan_json: Vec<u8>,
        cancel: CancellationToken,
    ) -> Result<ProbeSummary, DispatchError> {
        use tokio::io::{AsyncBufReadExt as _, BufReader};

        let started = std::time::Instant::now();
        let conn = self.connection().await?;
        let mut proc = super::exec::open_helper_probe(conn, helper_path, plan_json).await?;

        // Drain stderr in the background so a chatty helper can't back up its
        // pipe; the demux task tolerates a dropped reader.
        let stderr_drainer = proc.take_stderr().map(|stderr| {
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    tracing::debug!(target: "ordius::ssh::helper", "helper stderr: {line}");
                }
            })
        });

        let Some(stdout) = proc.take_stdout() else {
            if let Some(h) = stderr_drainer {
                h.abort();
            }
            return Err(DispatchError::HelperBootstrap(
                "helper stdout missing".into(),
            ));
        };

        let outcome =
            consume_ssh_helper_stream(&mut proc, BufReader::new(stdout), &plan, &cancel).await;

        // Reap the remote process so the exec channel closes cleanly.
        // Capture exit status to detect a helper crash (no output + non-zero).
        let exit_status = proc.wait().await;
        if let Some(h) = stderr_drainer {
            h.abort();
        }

        // Helper exited non-zero before emitting any outcomes — likely a crash
        // (wrong binary for the remote arch, segfault, linker mismatch). Surface
        // as DispatchError so the caller records the env as Unreachable instead
        // of silently flooding the catalog with Skipped entries.
        if !outcome.cancelled
            && outcome.total_probed == 0
            && !plan.defs.is_empty()
            && let Ok(exit) = &exit_status
            && exit.code != 0
        {
            return Err(DispatchError::HelperBootstrap(format!(
                "remote helper probe exited without output (exit code {})",
                exit.code
            )));
        }

        let mut resources = outcome.resources;
        // Any definition the helper didn't report on is recorded as Skipped so
        // the catalog has an entry per requested resource.
        for def in &plan.defs {
            resources
                .entry(def.id.clone())
                .or_insert_with(|| ResourceProbeOutcome::Skipped {
                    reason: "helper did not return an outcome".into(),
                });
        }

        Ok(ProbeSummary {
            catalog: ResourceCatalog {
                env_id: plan.env_id.clone(),
                registry_revision: plan.registry_revision,
                probed_at: chrono::Utc::now(),
                resources,
            },
            total_probed: outcome.total_probed,
            elapsed: started.elapsed(),
        })
    }
}

/// Outcome of [`consume_ssh_helper_stream`].
struct SshHelperStreamOutcome {
    resources: HashMap<ResourceId, ResourceProbeOutcome>,
    total_probed: usize,
    /// `true` when the outer cancel token fired; guards the crash-detect path.
    cancelled: bool,
}

/// Drain the JSONL stdout of a remote helper probe into a map of outcomes.
///
/// Each non-empty line is parsed as a [`ProbeOutcomeV1`], matched back to its
/// definition by id, and translated through [`wire_outcome_to_engine`] with
/// `host_direct_verified = false` (SSH is compute-first, no host-direct today).
/// Cancellation closes the exec channel via [`EnvProcess::cancel`].
async fn consume_ssh_helper_stream(
    proc: &mut impl EnvProcess,
    stdout: tokio::io::BufReader<ProcessPipe>,
    plan: &ProbePlan,
    cancel: &CancellationToken,
) -> SshHelperStreamOutcome {
    use tokio::io::AsyncBufReadExt as _;

    let defs_by_id: HashMap<&str, &ResourceDefinition> = plan
        .defs
        .iter()
        .map(|def| (def.id.0.as_str(), def))
        .collect();
    let mut resources: HashMap<ResourceId, ResourceProbeOutcome> = HashMap::new();
    let mut total_probed = 0usize;
    let mut cancelled = false;
    let mut reader = stdout.lines();

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                drop(proc.cancel().await);
                cancelled = true;
                for def in &plan.defs {
                    resources.entry(def.id.clone()).or_insert_with(|| {
                        ResourceProbeOutcome::Skipped {
                            reason: "probe cancelled".into(),
                        }
                    });
                }
                break;
            },
            line = reader.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        let Ok(wire) = serde_json::from_str::<ProbeOutcomeV1>(&line) else {
                            continue;
                        };
                        if wire.version != 1 {
                            continue;
                        }
                        let Some(def) = defs_by_id.get(wire.id.as_str()).copied() else {
                            continue;
                        };
                        // SSH is compute-first: no host-direct route today, so
                        // always tag HTTP endpoints as env-loopback.
                        if resources
                            .insert(def.id.clone(), wire_outcome_to_engine(wire.outcome, def, false))
                            .is_none()
                        {
                            total_probed += 1;
                        }
                    },
                    Ok(None) | Err(_) => break,
                }
            },
        }
    }

    SshHelperStreamOutcome {
        resources,
        total_probed,
        cancelled,
    }
}

/// Open an SFTP subsystem channel on `conn` and wrap it in the `SftpOps` adapter.
///
/// Holds the `Handle` lock only long enough to open the session channel and
/// request the `sftp` subsystem; the resulting stream is owned by the returned
/// [`RusshSftp`], so the lock is released before any SFTP I/O.
async fn open_sftp(conn: &SshConnection) -> Result<RusshSftp, DispatchError> {
    let map_err = |what: &str, e: russh::Error| {
        conn.mark_closed();
        DispatchError::EnvUnreachable {
            env_id: conn.id().to_string(),
            reason: format!("{what}: {e}"),
        }
    };

    let stream = {
        let handle = conn.handle().await;
        let channel = handle
            .channel_open_session()
            .await
            .map_err(|e| map_err("open sftp channel", e))?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(|e| map_err("request sftp subsystem", e))?;
        // The channel is owned, not borrowed from the handle — release the lock
        // before converting to a stream and running the SFTP handshake.
        drop(handle);
        channel.into_stream()
    };

    let session = russh_sftp::client::SftpSession::new(stream)
        .await
        .map_err(|e| {
            DispatchError::HelperBootstrap(format!("open sftp session on {}: {e}", conn.id()))
        })?;
    Ok(RusshSftp::new(session))
}

#[async_trait]
impl Dispatcher for SshDispatcher {
    fn info(&self) -> &EnvInfo {
        &self.env_info
    }

    async fn probe(
        &self,
        plan: ProbePlan,
        cancel: CancellationToken,
    ) -> Result<ProbeSummary, DispatchError> {
        if cancel.is_cancelled() {
            return Err(DispatchError::Cancelled);
        }

        // Race the helper bootstrap against cancel so a cancelled probe does not
        // block on a cold SFTP push (which can take tens of seconds). SSH has no
        // shell fallback (compute-first): if the helper can't be installed the
        // env is genuinely unreachable, surfaced as Err and recorded as
        // `Unreachable` by the boot/refresh loop.
        let helper = tokio::select! {
            () = cancel.cancelled() => return Err(DispatchError::Cancelled),
            r = self.ensure_helper() => r,
        }?;
        let helper_path = helper.env_side_path.clone();
        self.probe_plan_via_helper(&helper_path, plan, cancel).await
    }

    async fn probe_resource(
        &self,
        def: &ResourceDefinition,
        cancel: CancellationToken,
    ) -> ResourceProbeOutcome {
        if cancel.is_cancelled() {
            return ResourceProbeOutcome::Skipped {
                reason: "probe cancelled".into(),
            };
        }
        // Re-probe a single resource by running a one-def plan through the same
        // helper stream, then lift out its outcome.
        let id = def.id.clone();
        let plan = ProbePlan {
            env_id: self.env_info.id.clone(),
            registry_revision: 0,
            defs: vec![def.clone()],
            per_resource_timeout: std::time::Duration::from_secs(5),
            max_concurrency: 1,
            overall_budget: std::time::Duration::from_secs(30),
        };
        match self.probe(plan, cancel).await {
            Ok(mut summary) => summary.catalog.resources.remove(&id).unwrap_or_else(|| {
                ResourceProbeOutcome::Skipped {
                    reason: "helper did not return an outcome".into(),
                }
            }),
            Err(e) => ResourceProbeOutcome::ProbeFailed {
                reason: e.to_string(),
            },
        }
    }

    async fn spawn(&self, cmd: ProcessCmd) -> Result<Box<dyn EnvProcess>, DispatchError> {
        let helper_path = self.ensure_helper().await?.env_side_path.clone();
        let conn = self.connection().await?;
        let proc = super::exec::open_helper_exec(conn, &helper_path, cmd).await?;
        Ok(Box::new(proc))
    }

    fn http_transport(&self) -> Arc<dyn HttpTransport> {
        // Return a clone of the pre-built transport so every caller shares the
        // same reqwest client and listener cache — no new client or empty map on
        // each call (matches LocalDispatcher / WslDispatcher pattern).
        Arc::clone(&self.transport)
    }

    fn translate_path(&self, host_path: &Path) -> Result<EnvPath, DispatchError> {
        // Remote hosts share no filesystem with the host; path translation is a
        // workspace-sync concern deferred past T10.
        Err(DispatchError::PathTranslation {
            host_path: host_path.display().to_string(),
            reason: "SSH path translation is deferred (compute-first; workspace sync not wired)"
                .into(),
        })
    }

    async fn prepare_workspace(
        &self,
        _workspace_host: &Path,
        _binding: &WorkspaceBinding,
        _run_id: &RunId,
    ) -> Result<WorkspaceHandle, DispatchError> {
        Err(DispatchError::Unsupported(
            "SSH workspace preparation is deferred (compute-first; sync not wired)".into(),
        ))
    }
}
