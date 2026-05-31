//! SSH dispatcher — process spawn (T10) over the cached connection.
//!
//! `Dispatcher::spawn` is wired here (T10) via the `ordius-helper exec` channel;
//! the HTTP transport lands in T11 and boot-probe wiring in T12. The other
//! `Dispatcher` methods stay stubbed until their owning task.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use crate::environment::runtime::catalog::ResourceProbeOutcome;
use crate::environment::runtime::dispatcher::{Dispatcher, HttpTransport};
use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::plan::{ProbePlan, ProbeSummary};
use crate::environment::runtime::resource::ResourceDefinition;
use crate::environment::runtime::transport::{EnvPath, EnvProcess, ProcessCmd, WorkspaceHandle};
use crate::environment::runtime::{EnvInfo, RunId, SshAuth, SshHostKeyPin, WorkspaceBinding};
use crate::secrets::Store;

use super::bootstrap::{RusshSftp, SshBootstrappedHelper, SshBootstrapper};
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
    /// Lazily bootstrapped helper state. Initialised on first call to
    /// [`bootstrap_helper_once`]; subsequent calls return the cached result.
    ///
    /// The actual bootstrap (SFTP write + verify + rename) is deferred to T10
    /// when `spawn` wires it into the dispatch path.
    pub(crate) helper: OnceCell<SshBootstrappedHelper>,
    /// HTTP transport, built once and shared by clone.
    ///
    /// Stored as `Arc<dyn HttpTransport>` so `http_transport()` can return a
    /// trivial `Arc::clone` with no cast — matches `LocalDispatcher` /
    /// `WslDispatcher`.
    transport: Arc<dyn HttpTransport>,
}

impl SshDispatcher {
    /// Build a dispatcher from an environment info record and a secret store.
    ///
    /// The connection is not opened here — it is opened lazily on first use by
    /// [`SshConnectionCache::connection`].
    pub fn new(
        env_info: EnvInfo,
        host: String,
        port: u16,
        user: String,
        auth: SshAuth,
        host_key_pins: Vec<SshHostKeyPin>,
        secrets: Arc<Store>,
    ) -> Self {
        let env_id = env_info.id.to_string();
        let connector = RusshConnector {
            env_id: env_id.clone(),
            host,
            port,
            user,
            auth,
            host_key_pins,
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

    /// Bootstrap the helper over SFTP exactly once; subsequent calls return
    /// the cached [`SshBootstrappedHelper`] without re-uploading.
    ///
    /// `sftp` is obtained by the caller from an open session channel.
    /// The actual wiring into the dispatch path happens in T10.
    #[allow(clippy::unused_self)] // T10 wires the real body; signature must stay as &self
    pub fn bootstrap_helper_once(
        &self,
        sftp: RusshSftp,
    ) -> Result<&SshBootstrappedHelper, DispatchError> {
        // Triple detection is stubbed until T9; the full dispatch path is
        // wired in T10.  The parameter is accepted here so the public
        // signature is stable from T8 onward.
        drop(sftp);
        Err(DispatchError::NotImplemented(
            "bootstrap_helper_once: wired in T10".into(),
        ))
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
    /// identical plans.  The actual helper execution is wired in T10 once
    /// `SshProcess` exists.
    #[allow(dead_code)] // wired in T10
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

    /// Execute the helper binary over SSH and stream its probe outcomes.
    ///
    /// Stub until T10 wires in `SshProcess`.
    #[allow(dead_code)] // wired in T10
    async fn run_helper_probe_stream(
        &self,
        _helper_path: &str,
        _plan: ProbePlan,
        _plan_json: Vec<u8>,
        _cancel: CancellationToken,
    ) -> Result<ProbeSummary, DispatchError> {
        // T10 will replace this body with real SSH execution.
        // The `async` + trivial await below keeps `clippy::unused_async` quiet
        // while the signature is stable.
        std::future::ready(Err(DispatchError::NotImplemented(
            "run_helper_probe_stream: wired in T10".into(),
        )))
        .await
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
        _plan: ProbePlan,
        _cancel: CancellationToken,
    ) -> Result<ProbeSummary, DispatchError> {
        Err(DispatchError::NotImplemented(
            "SSH probe: wired in T12".into(),
        ))
    }

    async fn probe_resource(
        &self,
        _def: &ResourceDefinition,
        _cancel: CancellationToken,
    ) -> ResourceProbeOutcome {
        ResourceProbeOutcome::Skipped {
            reason: "SSH probe_resource: wired in T12".into(),
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
