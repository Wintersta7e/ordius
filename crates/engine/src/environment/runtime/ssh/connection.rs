//! Cached SSH connection and reconnect-once helpers.
//!
//! The [`SshConnectionCache`] holds one authenticated connection per dispatcher.
//! When a cached connection reports [`SshConnectionLike::is_closed`] it is
//! replaced with a fresh one; the reconnect is attempted at most once.
//!
//! The [`SshConnectionLike`] / [`SshConnector`] traits form a fakeable
//! boundary so unit tests can exercise the cache logic without opening a
//! network socket.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::environment::runtime::DispatchError;

// ── Traits ────────────────────────────────────────────────────────────────────

/// Minimal connection behaviour the dispatcher needs.
///
/// Both methods must be callable from any thread context — production wraps
/// the russh `Handle<H>` (which is `Send` but `!Sync`) inside a
/// `tokio::sync::Mutex` and tracks liveness with an `AtomicBool`.
#[async_trait]
pub trait SshConnectionLike: Send + Sync {
    /// Stable identifier used in logging and tests.
    fn id(&self) -> &str;

    /// `true` when the underlying SSH session can no longer open channels.
    ///
    /// This is a **synchronous** method — it must never `.await` a lock.
    /// Production implementations track liveness via a separate `AtomicBool`.
    fn is_closed(&self) -> bool;
}

/// Opens new SSH connections.
#[async_trait]
pub trait SshConnector: Send + Sync + Clone + 'static {
    /// Concrete connection type produced by [`connect`](Self::connect).
    type Connection: SshConnectionLike + 'static;

    /// Connect and fully authenticate. Called once per connection attempt.
    async fn connect(&self) -> Result<Self::Connection, DispatchError>;
}

// ── Cache ─────────────────────────────────────────────────────────────────────

/// One cached connection per dispatcher.
///
/// [`connection`](Self::connection) returns the cached connection when it is
/// still open, or reconnects once when the cached connection is closed.
pub struct SshConnectionCache<C>
where
    C: SshConnector,
{
    connector: C,
    /// Environment label used in error messages.
    env_id: String,
    current: Mutex<Option<Arc<C::Connection>>>,
}

impl<C> SshConnectionCache<C>
where
    C: SshConnector,
{
    /// Build an empty cache backed by `connector`.
    ///
    /// `env_id` is an opaque label (e.g. `"ssh:host:port"`) included in error
    /// messages so failures are attributable without reaching for a log context.
    pub fn new(connector: C, env_id: impl Into<String>) -> Self {
        Self {
            connector,
            env_id: env_id.into(),
            current: Mutex::new(None),
        }
    }

    /// Return an open connection, reconnecting once if the cached connection
    /// has been closed.
    ///
    /// The `Mutex` is held across `connect().await` on purpose: a single
    /// authenticated session is shared per env, and serializing reconnect
    /// attempts prevents concurrent connect storms when the session drops.
    pub async fn connection(&self) -> Result<Arc<C::Connection>, DispatchError> {
        let mut guard = self.current.lock().await;

        // Fast path: cached connection is still open.
        if let Some(conn) = guard.as_ref()
            && !conn.is_closed()
        {
            return Ok(Arc::clone(conn));
        }

        // The cache is empty or the connection is closed — try once.
        let first = Arc::new(self.connector.connect().await?);
        if !first.is_closed() {
            *guard = Some(Arc::clone(&first));
            return Ok(first);
        }

        // The freshly opened connection is already closed (pathological).
        // Try once more; if it is also closed, surface an error rather than
        // caching a dead connection — caching it would cause the very next
        // call to re-enter this branch and soft-loop until the remote recovers.
        let second = Arc::new(self.connector.connect().await?);
        if second.is_closed() {
            return Err(DispatchError::EnvUnreachable {
                env_id: self.env_id.clone(),
                reason: "SSH connection was closed immediately after connect (two attempts)".into(),
            });
        }
        *guard = Some(Arc::clone(&second));
        drop(guard); // release mutex before returning — avoids significant-drop-tightening
        Ok(second)
    }

    /// Drop the cached connection so the next call to [`connection`](Self::connection)
    /// triggers a fresh handshake.
    pub async fn invalidate(&self) {
        *self.current.lock().await = None;
    }
}

// ── Production connection (wraps russh Handle) ────────────────────────────────

/// A fully-authenticated russh connection.
///
/// # `Send + Sync`
///
/// `russh::client::Handle<H>` is `Send` but `!Sync` (its inner
/// `UnboundedReceiver` is `!Sync`).  Wrapping it in a
/// `tokio::sync::Mutex` makes the enclosing struct `Send + Sync`
/// because `Mutex<T: Send>` is `Send + Sync`.
///
/// `is_closed` is a **sync** method and cannot await the async mutex.
/// A separate `AtomicBool` tracks liveness; T10/T11 call
/// [`mark_closed`](Self::mark_closed) when a channel operation fails.
pub struct SshConnection {
    /// Stable identifier (e.g. `"ssh:host:port"`).
    id: String,
    /// The russh session — guarded by a `tokio::sync::Mutex` to restore `Sync`.
    // confirm signature against the T1 spike output
    handle: Mutex<russh::client::Handle<super::host_key::HostKeyHandler>>,
    /// Tracks whether the session is still usable without holding the async lock.
    closed: AtomicBool,
}

impl SshConnection {
    /// Build a new connection wrapping an authenticated russh handle.
    pub(crate) fn new(
        id: String,
        handle: russh::client::Handle<super::host_key::HostKeyHandler>,
    ) -> Self {
        Self {
            id,
            handle: Mutex::new(handle),
            closed: AtomicBool::new(false),
        }
    }

    /// Mark the connection as permanently closed.
    ///
    /// Called by T10/T11 session/channel code when a transport error occurs.
    pub fn mark_closed(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    /// Acquire the russh handle for opening a new exec/sftp channel.
    ///
    /// Used by T10 (`spawn`) and T11 (HTTP transport / SFTP). Holding the
    /// guard serialises channel-open calls; the caller should open the channel
    /// quickly and release the guard before any blocking I/O on the channel.
    pub async fn handle(
        &self,
    ) -> tokio::sync::MutexGuard<'_, russh::client::Handle<super::host_key::HostKeyHandler>> {
        self.handle.lock().await
    }
}

#[async_trait]
impl SshConnectionLike for SshConnection {
    fn id(&self) -> &str {
        &self.id
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

// ── Compile-time Send + Sync proof ───────────────────────────────────────────

const fn assert_send_sync<T: Send + Sync>() {}

#[allow(dead_code)]
const fn _assert_ssh_connection_send_sync() {
    assert_send_sync::<SshConnection>();
}

// ── Production connector ──────────────────────────────────────────────────────

/// russh-native connector.
///
/// Holds the per-env parameters extracted from [`EnvSpec::Ssh`].  The secrets
/// store is shared with the engine; no plaintext credentials are stored in the
/// struct (they are fetched just-in-time in [`connect`](Self::connect)).
#[derive(Clone)]
pub struct RusshConnector {
    /// Identifier string used in error messages.
    pub env_id: String,
    /// SSH host name or address.
    pub host: String,
    /// SSH TCP port.
    pub port: u16,
    /// SSH user name.
    pub user: String,
    /// Authentication configuration (references secrets by name).
    pub auth: crate::environment::runtime::SshAuth,
    /// Trusted inline host-key pins.
    pub host_key_pins: Vec<crate::environment::runtime::SshHostKeyPin>,
    /// Shared secret store for resolving auth credentials.
    pub secrets: Arc<crate::secrets::Store>,
}

#[async_trait]
impl SshConnector for RusshConnector {
    type Connection = SshConnection;

    async fn connect(&self) -> Result<SshConnection, DispatchError> {
        use super::auth::{SshAuthError, authenticate_session, resolve_auth_material};
        use super::host_key::HostKeyHandler;

        let map_unreachable = |reason: String| DispatchError::EnvUnreachable {
            env_id: self.env_id.clone(),
            reason,
        };

        // Resolve auth material before touching the network, so a missing
        // secret fails fast without opening a TCP connection.
        let resolved = resolve_auth_material(&self.secrets, &self.auth)
            .map_err(|e| map_unreachable(format!("auth resolution: {e}")))?;

        // Build a pinned handler that will reject any unexpected host key.
        let handler = HostKeyHandler::pinned(self.host_key_pins.clone());

        // 10-second connect timeout — matches the T5 enrollment timeout.
        // confirm signature against the T1 spike output
        let config = russh::client::Config {
            inactivity_timeout: Some(Duration::from_secs(30)),
            ..Default::default()
        };

        let mut session = tokio::time::timeout(
            Duration::from_secs(10),
            russh::client::connect(Arc::new(config), (self.host.as_str(), self.port), handler),
        )
        .await
        .map_err(|_| map_unreachable("SSH connect timed out".into()))?
        .map_err(|e| map_unreachable(format!("SSH connect: {e}")))?;

        authenticate_session(&mut session, &self.user, resolved)
            .await
            .map_err(|e: SshAuthError| map_unreachable(format!("SSH auth: {e}")))?;

        let id = format!("ssh:{}:{}", self.host, self.port);
        Ok(SshConnection::new(id, session))
    }
}

// ── Fake connector for tests ──────────────────────────────────────────────────

#[cfg(any(test, feature = "testing"))]
#[allow(missing_docs)]
#[derive(Clone, Default)]
pub struct FakeSshConnector {
    inner: Arc<FakeInner>,
}

#[cfg(any(test, feature = "testing"))]
#[derive(Default)]
struct FakeInner {
    queue: parking_lot::Mutex<std::collections::VecDeque<FakeSshConnection>>,
    connects: AtomicUsize,
}

#[cfg(any(test, feature = "testing"))]
use std::sync::atomic::AtomicUsize;

#[cfg(any(test, feature = "testing"))]
impl FakeSshConnector {
    /// Build an empty fake connector.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a connection with the given id and closed state.
    #[must_use]
    pub fn with_connection(self, id: &str, closed: bool) -> Self {
        self.inner.queue.lock().push_back(FakeSshConnection {
            id: id.to_string(),
            closed,
        });
        self
    }

    /// Number of times [`SshConnector::connect`] has been called.
    pub fn connect_count(&self) -> usize {
        self.inner.connects.load(Ordering::SeqCst)
    }
}

#[cfg(any(test, feature = "testing"))]
#[allow(missing_docs)]
#[derive(Debug)]
pub struct FakeSshConnection {
    id: String,
    closed: bool,
}

#[cfg(any(test, feature = "testing"))]
#[async_trait]
impl SshConnectionLike for FakeSshConnection {
    fn id(&self) -> &str {
        &self.id
    }

    fn is_closed(&self) -> bool {
        self.closed
    }
}

#[cfg(any(test, feature = "testing"))]
#[async_trait]
impl SshConnector for FakeSshConnector {
    type Connection = FakeSshConnection;

    async fn connect(&self) -> Result<FakeSshConnection, DispatchError> {
        self.inner.connects.fetch_add(1, Ordering::SeqCst);
        self.inner
            .queue
            .lock()
            .pop_front()
            .ok_or_else(|| DispatchError::EnvUnreachable {
                env_id: "ssh:test".into(),
                reason: "fake connection queue empty".into(),
            })
    }
}
