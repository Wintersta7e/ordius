//! SSH dispatcher — struct and constructor only (T7).
//!
//! `Dispatcher::spawn` is wired in T10; the HTTP transport in T11;
//! boot-probe wiring in T12. This file exists so the public module
//! surface is stable from T7 onward.

use std::sync::Arc;

use crate::environment::runtime::{EnvInfo, SshAuth, SshHostKeyPin};
use crate::secrets::Store;

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
        }
    }
}
