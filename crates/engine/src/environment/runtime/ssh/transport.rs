//! SSH HTTP transport via local listener and direct-tcpip channels.
//!
//! [`SshHttpTransport`] intercepts HTTP requests destined for a remote
//! environment and routes them through russh `direct-tcpip` channels:
//!
//! 1. The first request for a given `(host, port)` pair spawns a
//!    `tokio::net::TcpListener` bound to `127.0.0.1:0`.
//! 2. Each accepted connection hands the socket to a
//!    [`DirectTcpipOpener`], which opens a russh `direct-tcpip` channel
//!    and `copy_bidirectional`s the socket against the channel stream.
//! 3. The original request URL is rewritten to `127.0.0.1:<listener-port>`
//!    so the `reqwest` client talks to the local listener.
//!
//! This keeps streaming semantics intact: `execute_stream` → reqwest byte
//! stream → local listener → russh channel → remote service.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::Mutex;
use url::Url;

// Fix 2: std::sync::Mutex is used for abort_handles so Drop can lock without .await.
use std::sync::Mutex as StdMutex;

use crate::environment::runtime::dispatcher::{HttpTransport, ResponseStream};
use crate::environment::runtime::transport::{
    HttpError, HttpRequest, HttpResponse, reqwest_direct_execute, reqwest_direct_execute_stream,
};

// ── Authority ─────────────────────────────────────────────────────────────────

/// A `(host, port)` pair identifying the remote service endpoint.
///
/// Used as a cache key for the per-authority local listener.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RemoteAuthority {
    /// Hostname or IP address of the remote service (as it appears in the URL).
    pub host: String,
    /// TCP port of the remote service.
    pub port: u16,
}

// ── Errors ────────────────────────────────────────────────────────────────────

/// Errors produced when building transport helpers from a URL.
#[derive(Debug, Error)]
pub enum SshTransportBuildError {
    /// The URL has no host component.
    #[error("url has no host")]
    MissingHost,
    /// The URL has no explicit port component.
    ///
    /// SSH HTTP transport requires an explicit port so the tunnel can be
    /// addressed precisely on the remote side. Default-port inference is
    /// intentionally not supported.
    #[error("url has no explicit port")]
    MissingPort,
    /// The URL string is not valid.
    #[error("invalid url: {0}")]
    InvalidUrl(String),
}

// ── Pure helpers ──────────────────────────────────────────────────────────────

/// Extract a `(host, port)` authority from `url`.
///
/// Returns [`SshTransportBuildError::MissingPort`] when the URL has no
/// explicit port, which is the expected error tested by
/// `ssh_transport_rewrite_rejects_missing_port`.
pub fn remote_authority(url: &Url) -> Result<RemoteAuthority, SshTransportBuildError> {
    let host = url
        .host_str()
        .ok_or(SshTransportBuildError::MissingHost)?
        .to_string();
    let port = url.port().ok_or(SshTransportBuildError::MissingPort)?;
    Ok(RemoteAuthority { host, port })
}

/// Rewrite `raw` URL so that its host is `127.0.0.1` and its port is
/// `listener_port`.
///
/// The path, query, and all other URL components are preserved.
pub fn rewrite_url_to_listener(
    raw: &str,
    listener_port: u16,
) -> Result<String, SshTransportBuildError> {
    let mut url = Url::parse(raw).map_err(|e| SshTransportBuildError::InvalidUrl(e.to_string()))?;
    url.set_host(Some("127.0.0.1"))
        .map_err(|_| SshTransportBuildError::InvalidUrl("set host failed".into()))?;
    url.set_port(Some(listener_port))
        .map_err(|()| SshTransportBuildError::InvalidUrl("set port failed".into()))?;
    Ok(url.to_string())
}

// ── DirectTcpipOpener trait ───────────────────────────────────────────────────

/// Opens a russh `direct-tcpip` channel and pumps it against the accepted
/// TCP socket.
///
/// The accept loop in [`SshHttpTransport::spawn_listener`] calls this for
/// every incoming connection, passing the accepted socket (Correction A).
/// Implementations **must not** hold a `Handle` lock across the
/// `copy_bidirectional` call (Correction B: use `.into_stream()` to convert
/// the channel to `AsyncRead+AsyncWrite` first, then drop the guard).
#[async_trait]
pub trait DirectTcpipOpener: Send + Sync {
    /// Accept `socket` and forward it to `authority` via a `direct-tcpip` channel.
    ///
    /// The method is fire-and-forget from the caller's perspective; errors are
    /// logged or silently discarded — the accept loop continues regardless.
    async fn open_direct_tcpip(
        &self,
        socket: tokio::net::TcpStream,
        authority: RemoteAuthority,
    ) -> Result<(), HttpError>;
}

// ── SshHttpTransport ──────────────────────────────────────────────────────────

/// HTTP transport that tunnels requests through russh `direct-tcpip` channels.
///
/// One persistent `TcpListener` is spawned per remote `(host, port)` authority
/// on first use.  All subsequent requests to the same authority reuse the same
/// listener port, so the reqwest client can be configured with a stable base
/// URL.
///
/// # Lifecycle
///
/// Accept-loop tasks are tracked via [`tokio::task::AbortHandle`]s stored in
/// `abort_handles`.  When the transport is dropped, [`Drop`] aborts every
/// accept loop — this releases each loop's `Arc<dyn DirectTcpipOpener>` and
/// lets the underlying SSH session drop once no other holder remains.
pub struct SshHttpTransport {
    /// Shared reqwest client — reusing it across requests amortises TLS setup
    /// and connection pooling within the local loopback listener.
    client: reqwest::Client,
    /// Maps each remote authority to the local listener port allocated for it.
    ///
    /// `tokio::sync::Mutex` so `local_port_for` can hold the guard across the
    /// `spawn_listener` await, eliminating the TOCTOU double-spawn window.
    listeners: Mutex<HashMap<RemoteAuthority, u16>>,
    /// Pluggable opener — production uses `RusshDirectTcpipOpener`; tests can
    /// supply a fake.
    direct_tcpip: Arc<dyn DirectTcpipOpener>,
    /// Abort handles for every accept-loop task spawned by [`spawn_listener`].
    ///
    /// Uses `std::sync::Mutex` (not tokio) so [`Drop`] can lock it without
    /// `.await`.
    abort_handles: StdMutex<Vec<tokio::task::AbortHandle>>,
}

impl SshHttpTransport {
    /// Construct a transport backed by `direct_tcpip`.
    pub fn new(direct_tcpip: Arc<dyn DirectTcpipOpener>) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client build must not fail with default config"),
            listeners: Mutex::new(HashMap::new()),
            direct_tcpip,
            abort_handles: StdMutex::new(Vec::new()),
        }
    }

    /// Return the local port for `authority`, starting a listener if this is
    /// the first request for that authority.
    ///
    /// # TOCTOU safety
    ///
    /// The tokio `listeners` lock is held across the check → `spawn_listener`
    /// → insert, so two concurrent callers for the same authority can never
    /// both spawn a listener.
    async fn local_port_for(&self, authority: RemoteAuthority) -> Result<u16, HttpError> {
        let mut guard = self.listeners.lock().await;
        if let Some(&port) = guard.get(&authority) {
            return Ok(port);
        }
        let port = self.spawn_listener(authority.clone()).await?;
        guard.insert(authority, port);
        drop(guard);
        Ok(port)
    }

    /// Bind a `127.0.0.1:0` listener and spawn the accept loop.
    ///
    /// Returns the OS-assigned port number.  The abort handle for the accept
    /// loop is stored in `abort_handles` so [`Drop`] can cancel it.
    ///
    /// # Locking
    ///
    /// Called while the caller holds the tokio `listeners` lock.  This method
    /// briefly takes the separate `std::sync::Mutex` `abort_handles` lock to
    /// push the abort handle — the two mutexes are independent, so there is no
    /// deadlock risk.
    async fn spawn_listener(&self, authority: RemoteAuthority) -> Result<u16, HttpError> {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .map_err(|e| HttpError::Transport(format!("bind ssh tunnel listener: {e}")))?;
        let port = listener
            .local_addr()
            .map_err(|e| HttpError::Transport(format!("local listener addr: {e}")))?
            .port();

        let opener = Arc::clone(&self.direct_tcpip);
        let task = tokio::spawn(async move {
            loop {
                // Correction A: pass the accepted socket to the opener.
                let Ok((socket, _addr)) = listener.accept().await else {
                    // Listener closed — exit loop.
                    break;
                };
                let authority = authority.clone();
                let opener = Arc::clone(&opener);
                tokio::spawn(async move {
                    drop(opener.open_direct_tcpip(socket, authority).await);
                });
            }
        });

        // Track the abort handle so Drop can cancel the loop.
        if let Ok(mut hs) = self.abort_handles.lock() {
            hs.push(task.abort_handle());
        }

        Ok(port)
    }
}

impl Drop for SshHttpTransport {
    fn drop(&mut self) {
        if let Ok(mut hs) = self.abort_handles.lock() {
            for h in hs.drain(..) {
                h.abort();
            }
        }
    }
}

#[async_trait]
impl HttpTransport for SshHttpTransport {
    async fn execute(&self, mut req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let url =
            Url::parse(&req.url).map_err(|e| HttpError::Transport(format!("invalid url: {e}")))?;
        let authority = remote_authority(&url).map_err(|e| HttpError::Transport(e.to_string()))?;
        let local_port = self.local_port_for(authority).await?;
        req.url = rewrite_url_to_listener(&req.url, local_port)
            .map_err(|e| HttpError::Transport(e.to_string()))?;
        reqwest_direct_execute(&self.client, req).await
    }

    async fn execute_stream(&self, mut req: HttpRequest) -> Result<ResponseStream, HttpError> {
        let url =
            Url::parse(&req.url).map_err(|e| HttpError::Transport(format!("invalid url: {e}")))?;
        let authority = remote_authority(&url).map_err(|e| HttpError::Transport(e.to_string()))?;
        let local_port = self.local_port_for(authority).await?;
        req.url = rewrite_url_to_listener(&req.url, local_port)
            .map_err(|e| HttpError::Transport(e.to_string()))?;
        reqwest_direct_execute_stream(&self.client, req).await
    }

    fn can_stream(&self, _url: &Url) -> bool {
        // Raw TCP tunnel — streaming works for any URL through this transport.
        true
    }
}

// ── Production opener — russh direct-tcpip ────────────────────────────────────

/// Production [`DirectTcpipOpener`] that tunnels via a cached russh session.
///
/// The opener holds an `Arc` to the dispatcher's connection cache so it can
/// obtain (or reconnect) the authenticated session on each accepted socket.
pub struct RusshDirectTcpipOpener {
    cache: Arc<super::connection::SshConnectionCache<super::connection::RusshConnector>>,
}

impl RusshDirectTcpipOpener {
    /// Wrap a shared connection cache.
    pub const fn new(
        cache: Arc<super::connection::SshConnectionCache<super::connection::RusshConnector>>,
    ) -> Self {
        Self { cache }
    }
}

#[async_trait]
impl DirectTcpipOpener for RusshDirectTcpipOpener {
    async fn open_direct_tcpip(
        &self,
        mut socket: tokio::net::TcpStream,
        authority: RemoteAuthority,
    ) -> Result<(), HttpError> {
        let conn = self
            .cache
            .connection()
            .await
            .map_err(|e| HttpError::Transport(format!("SSH connection: {e}")))?;

        // Open the direct-tcpip channel.
        // Signature (confirmed from spike): channel_open_direct_tcpip(host, port, orig_addr, orig_port)
        // Returns Channel<client::Msg> — mut required for wait().
        // Hold the Handle lock only for the channel-open call; drop before pumping
        // so the session can serve other channels concurrently.
        let channel = {
            let handle = conn.handle().await;
            let ch = handle
                .channel_open_direct_tcpip(&authority.host, authority.port.into(), "127.0.0.1", 0)
                .await
                .map_err(|e| {
                    conn.mark_closed();
                    HttpError::Transport(format!("direct-tcpip open: {e}"))
                })?;
            // Drop the Handle guard before any blocking I/O.
            drop(handle);
            ch
        };

        // Correction B: Channel<client::Msg> does NOT implement AsyncRead+AsyncWrite.
        // into_stream() → ChannelStream<client::Msg>, which IS AsyncRead+AsyncWrite+Unpin+Send.
        let mut stream = channel.into_stream();

        // Pump bytes between the local socket and the SSH channel until one side
        // closes.  Teardown is best-effort per T11 note (lines 495–497 of the
        // plan): copy_bidirectional drives the channel until the remote closes it,
        // so no explicit eof()+drain is needed here.
        drop(tokio::io::copy_bidirectional(&mut socket, &mut stream).await);

        Ok(())
    }
}
