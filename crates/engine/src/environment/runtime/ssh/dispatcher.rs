//! SSH dispatcher — struct and constructor only (T7).
//!
//! `Dispatcher::spawn` is wired in T10; the HTTP transport in T11;
//! boot-probe wiring in T12. This file exists so the public module
//! surface is stable from T7 onward.

use std::sync::Arc;

use tokio::sync::OnceCell;

use crate::environment::runtime::error::DispatchError;
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
}
