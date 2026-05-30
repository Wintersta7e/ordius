//! SSH dispatcher — struct and constructor only (T7).
//!
//! `Dispatcher::spawn` is wired in T10; the HTTP transport in T11;
//! boot-probe wiring in T12. This file exists so the public module
//! surface is stable from T7 onward.

use std::sync::Arc;

use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::plan::{ProbePlan, ProbeSummary};
use crate::environment::runtime::{EnvInfo, SshAuth, SshHostKeyPin};
use crate::secrets::Store;

use super::bootstrap::{RusshSftp, SshBootstrappedHelper, SshBootstrapper};
use super::connection::{RusshConnector, SshConnectionCache};

/// Dispatches work to a remote SSH environment.
///
/// Holds one [`SshConnectionCache`] per dispatcher instance. The cache opens
/// a single authenticated session on first use and reuses it for subsequent
/// operations; if the session closes it reconnects once.
pub struct SshDispatcher {
    /// Metadata describing the environment (id, label, state …).
    pub env_info: EnvInfo,
    /// Cached, authenticated connection to the remote host.
    #[allow(dead_code)] // used starting T10
    pub(crate) cache: SshConnectionCache<RusshConnector>,
    /// Lazily bootstrapped helper state. Initialised on first call to
    /// [`bootstrap_helper_once`]; subsequent calls return the cached result.
    ///
    /// The actual bootstrap (SFTP write + verify + rename) is deferred to T10
    /// when `spawn` wires it into the dispatch path.
    #[allow(dead_code)] // used starting T10
    pub(crate) helper: OnceCell<SshBootstrappedHelper>,
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
        Self {
            env_info,
            cache: SshConnectionCache::new(connector, env_id),
            helper: OnceCell::new(),
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
