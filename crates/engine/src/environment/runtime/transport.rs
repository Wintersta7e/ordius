//! Transport types used by Dispatcher trait: HTTP req/resp, process command,
//! env-side path, workspace handle.
//!
//! Also exposes free helpers `reqwest_direct_execute*` so HTTP transports that
//! talk to the network directly (Local, WSL on HostDirect/PublicDirect paths)
//! share one implementation of header iteration, timeout mapping, and body
//! collection.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::dispatcher::ResponseStream;

/// HTTP method for dispatch-level requests.
///
/// Serialised as lowercase `snake_case` (e.g. `"get"`, `"delete"`).
#[allow(missing_docs)] // variant names are self-describing RFC 7231 tokens
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpMethod {
    Get,
    Head,
    Post,
    Put,
    Patch,
    Delete,
}

/// A single HTTP request to be issued by a [`crate::environment::runtime::dispatcher::HttpTransport`].
#[derive(Debug, Clone)]
pub struct HttpRequest {
    /// HTTP method.
    pub method: HttpMethod,
    /// Absolute URL including scheme, host, port, and path.
    pub url: String,
    /// Additional request headers.
    pub headers: HashMap<String, String>,
    /// Optional request body.
    pub body: Option<Bytes>,
    /// Per-request timeout. Overrides any transport-level default.
    pub timeout: Duration,
}

/// The response returned by a successful HTTP dispatch.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    /// HTTP status code.
    pub status: u16,
    /// Response headers (lower-cased keys).
    pub headers: HashMap<String, String>,
    /// Full response body, buffered.
    pub body: Bytes,
}

/// Errors returned by HTTP transport.
#[derive(Debug, Error)]
pub enum HttpError {
    /// A network or TLS error occurred.
    #[error("transport error: {0}")]
    Transport(String),
    /// The request exceeded its timeout budget.
    #[error("timeout after {0:?}")]
    Timeout(Duration),
    /// Caller requested streaming on a route that doesn't support it.
    /// `route_origin` is a plain string (not a typed enum) so this module
    /// stays free of catalog dependencies.
    #[error("streaming not supported on this route ({route_origin})")]
    StreamingUnsupported {
        /// Human-readable origin label (e.g. `"env_loopback"`, `"forwarded_tunnel"`).
        route_origin: String,
    },
}

/// Disposition for a child process's stdout/stderr stream.
///
/// `Inherit` (the default) sends the stream to the parent process — used by
/// fire-and-forget spawns like the boot probe helpers. `Piped` opens a pipe
/// the supervisor reads line-by-line, used by the `shell` / `docker-run`
/// nodes that forward output to the run emitter. `Null` discards the
/// stream entirely.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stdio {
    /// Inherit the parent's handle.
    #[default]
    Inherit,
    /// Open a pipe the caller can read.
    Piped,
    /// Discard the stream (`/dev/null` / `nul:`).
    Null,
}

/// argv-only process command. The [`crate::environment::runtime::dispatcher::Dispatcher`]
/// wraps it per env type (e.g. prefixes `wsl.exe -d <name> --exec` for WSL).
///
/// Constructed without shell escaping — individual tokens are kept separate to
/// avoid double-escaping when the dispatcher builds the final invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessCmd {
    /// Executable name or absolute path (no shell metacharacters).
    pub program: String,
    /// Positional arguments, already split into individual tokens.
    pub args: Vec<String>,
    /// Extra environment variables merged into the child process environment.
    pub env: HashMap<String, String>,
    /// Optional working directory (env-side path, not host path).
    pub cwd: Option<EnvPath>,
    /// Optional data piped to the process's stdin.
    pub stdin: Option<Bytes>,
    /// stdout disposition. Defaults to `Inherit` for back-compat with Phase
    /// A/B call sites that don't set the field.
    #[serde(default)]
    pub stdout: Stdio,
    /// stderr disposition. Same default as `stdout`.
    #[serde(default)]
    pub stderr: Stdio,
}

/// An env-side path. Distinct newtype from `std::path::PathBuf` / host paths so
/// the type system prevents silent mix-ups across environment boundaries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvPath(String);

impl EnvPath {
    /// Construct from any `Into<String>` (string literal, owned `String`, …).
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the inner path string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for EnvPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// RAII handle returned from `Dispatcher::prepare_workspace`.
///
/// Dropping the handle triggers the teardown closure, which may perform
/// operations such as rsync write-back for SSH environments or unmounting
/// bind-mount overlays for container environments.
pub struct WorkspaceHandle {
    /// The path on the env side where the workspace is available.
    pub env_path: EnvPath,
    /// Optional teardown closure. Runs exactly once on drop.
    pub teardown: Option<Box<dyn FnOnce() + Send>>,
}

impl std::fmt::Debug for WorkspaceHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkspaceHandle")
            .field("env_path", &self.env_path)
            .field("teardown", &self.teardown.as_ref().map(|_| "<fn>"))
            .finish()
    }
}

impl Drop for WorkspaceHandle {
    fn drop(&mut self) {
        if let Some(td) = self.teardown.take() {
            td();
        }
    }
}

/// Execute an `HttpRequest` against a shared `reqwest::Client`.
///
/// Used by `LocalHttpTransport` and by `WslHttpTransport`'s
/// `HostDirect`/`PublicDirect` routes — both share one implementation.
pub async fn reqwest_direct_execute(
    client: &reqwest::Client,
    req: HttpRequest,
) -> Result<HttpResponse, HttpError> {
    let method = http_method_to_reqwest(req.method);
    let mut builder = client.request(method, &req.url).timeout(req.timeout);
    let timeout = req.timeout;
    for (k, v) in &req.headers {
        builder = builder.header(k, v);
    }
    if let Some(b) = req.body {
        builder = builder.body(b);
    }
    let resp = builder.send().await.map_err(|e| {
        if e.is_timeout() {
            HttpError::Timeout(timeout)
        } else {
            HttpError::Transport(e.to_string())
        }
    })?;
    let status = resp.status().as_u16();
    let headers = resp
        .headers()
        .iter()
        .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
        .collect();
    let body = resp.bytes().await.map_err(|e| {
        if e.is_timeout() {
            HttpError::Timeout(timeout)
        } else {
            HttpError::Transport(e.to_string())
        }
    })?;
    Ok(HttpResponse {
        status,
        headers,
        body,
    })
}

/// Execute an `HttpRequest` and return a streaming response body. Streaming is
/// supported only for GET and POST.
pub async fn reqwest_direct_execute_stream(
    client: &reqwest::Client,
    req: HttpRequest,
) -> Result<ResponseStream, HttpError> {
    let method = match req.method {
        HttpMethod::Get => reqwest::Method::GET,
        HttpMethod::Post => reqwest::Method::POST,
        _ => return Err(HttpError::Transport("stream supports GET/POST only".into())),
    };
    let mut builder = client.request(method, &req.url).timeout(req.timeout);
    for (k, v) in &req.headers {
        builder = builder.header(k, v);
    }
    if let Some(b) = req.body {
        builder = builder.body(b);
    }
    let resp = builder
        .send()
        .await
        .map_err(|e| HttpError::Transport(e.to_string()))?;
    let stream = resp
        .bytes_stream()
        .map_err(|e: reqwest::Error| HttpError::Transport(e.to_string()));
    Ok(Box::pin(stream))
}

/// Map the 6-variant `HttpMethod` enum onto `reqwest::Method`.
const fn http_method_to_reqwest(m: HttpMethod) -> reqwest::Method {
    match m {
        HttpMethod::Get => reqwest::Method::GET,
        HttpMethod::Head => reqwest::Method::HEAD,
        HttpMethod::Post => reqwest::Method::POST,
        HttpMethod::Put => reqwest::Method::PUT,
        HttpMethod::Patch => reqwest::Method::PATCH,
        HttpMethod::Delete => reqwest::Method::DELETE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_method_serializes() {
        assert_eq!(serde_json::to_string(&HttpMethod::Get).unwrap(), "\"get\"");
    }

    #[test]
    fn http_request_constructs() {
        let req = HttpRequest {
            method: HttpMethod::Get,
            url: "http://example.com".into(),
            headers: HashMap::default(),
            body: None,
            timeout: std::time::Duration::from_secs(5),
        };
        assert_eq!(req.url, "http://example.com");
    }

    #[test]
    fn process_cmd_argv_only() {
        let cmd = ProcessCmd {
            program: "echo".into(),
            args: vec!["hello".into()],
            env: HashMap::default(),
            cwd: None,
            stdin: None,
            stdout: Stdio::default(),
            stderr: Stdio::default(),
        };
        assert_eq!(cmd.args, vec!["hello".to_string()]);
    }

    #[test]
    fn process_cmd_serde_defaults_stdout_stderr_to_inherit() {
        // Wire shape from an older caller (no stdout/stderr fields) must
        // still deserialize, with the new fields defaulting to Inherit.
        let json = r#"{"program":"echo","args":[],"env":{},"cwd":null,"stdin":null}"#;
        let cmd: ProcessCmd = serde_json::from_str(json).unwrap();
        assert_eq!(cmd.stdout, Stdio::Inherit);
        assert_eq!(cmd.stderr, Stdio::Inherit);
    }

    #[test]
    fn stdio_round_trips_through_serde() {
        let s = serde_json::to_string(&Stdio::Piped).unwrap();
        assert_eq!(s, "\"piped\"");
        let back: Stdio = serde_json::from_str(&s).unwrap();
        assert_eq!(back, Stdio::Piped);
    }

    #[test]
    fn env_path_displays_inner() {
        let p = EnvPath::new("/home/user");
        assert_eq!(p.as_str(), "/home/user");
    }

    #[test]
    fn workspace_handle_drop_fires_teardown() {
        use std::sync::{Arc, Mutex};
        let fired = Arc::new(Mutex::new(false));
        let fired_clone = Arc::clone(&fired);
        {
            let _handle = WorkspaceHandle {
                env_path: EnvPath::new("/tmp/ws"),
                teardown: Some(Box::new(move || {
                    *fired_clone.lock().unwrap() = true;
                })),
            };
        }
        assert!(*fired.lock().unwrap(), "teardown should have fired on drop");
    }
}
