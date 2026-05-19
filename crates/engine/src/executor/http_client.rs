//! Process-wide `reqwest::Client` shared by every built-in that
//! talks HTTP. One client = one TLS session cache + DNS resolver +
//! connection pool, so an `http` node and an `llm` node hitting
//! the same host reuse the same underlying TCP connections.
//!
//! Per-request timeouts / headers / body are applied via the
//! `RequestBuilder` returned by `.request(method, url)`; nothing
//! in this module configures the client itself, by design.

use reqwest::Client;
use std::sync::OnceLock;

/// Lazily-built shared client. First call initializes; later calls
/// hand back the same handle.
pub(super) fn shared() -> &'static Client {
    static CLIENT: OnceLock<Client> = OnceLock::new();
    CLIENT.get_or_init(Client::new)
}
