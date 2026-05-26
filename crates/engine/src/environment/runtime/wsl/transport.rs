//! HTTP transport for `WslDispatcher`: env-loopback wrap vs `HostDirect` direct
//! vs public direct.

use std::collections::HashMap;
use std::process::Output;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use url::Url;

use crate::environment::runtime::dispatcher::{HttpTransport, ResponseStream};
use crate::environment::runtime::env::HostDirectVerification;
use crate::environment::runtime::resource::ResourceId;
use crate::environment::runtime::transport::{HttpError, HttpMethod, HttpRequest, HttpResponse};

/// Shared, mutation-observable host-direct verification map.
pub type HostDirectMap = Arc<ArcSwap<HashMap<ResourceId, HostDirectVerification>>>;

const CURL_STATUS_WRITE_OUT: &str = "\n\n%{http_code}\n";
const CURL_STATUS_DELIMITER: &[u8] = b"\n\n";

/// WSL-aware HTTP transport that routes env-loopback requests through curl
/// inside the distro while keeping verified host-direct and public URLs direct.
///
/// The `host_direct` map is held behind `Arc<ArcSwap<_>>` so the dispatcher
/// can share it and live-update it (via `WslDispatcher::set_host_direct`)
/// without rebuilding the transport — and therefore without throwing away
/// the underlying `reqwest::Client`'s connection pool.
#[derive(Debug, Clone)]
pub struct WslHttpTransport {
    distro: String,
    direct: reqwest::Client,
    host_direct: HostDirectMap,
}

impl WslHttpTransport {
    /// Construct a transport that owns its own (initially empty or seeded)
    /// host-direct snapshot. Test ergonomics — production code uses
    /// [`Self::with_host_direct`] to share state with the dispatcher.
    pub fn new(
        distro: impl Into<String>,
        host_direct: HashMap<ResourceId, HostDirectVerification>,
    ) -> Self {
        Self::with_host_direct(distro, Arc::new(ArcSwap::from_pointee(host_direct)))
    }

    /// Construct a transport that shares a host-direct verification map with
    /// its owning dispatcher; `WslDispatcher::set_host_direct` updates are
    /// visible without reconstructing the transport.
    pub fn with_host_direct(distro: impl Into<String>, host_direct: HostDirectMap) -> Self {
        Self {
            distro: distro.into(),
            direct: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
            host_direct,
        }
    }

    fn url_classification(&self, url: &Url) -> UrlClass {
        if is_loopback_url(url) {
            if self.has_host_direct_for(url) {
                UrlClass::HostDirect
            } else {
                UrlClass::EnvLoopback
            }
        } else {
            UrlClass::PublicDirect
        }
    }

    fn has_host_direct_for(&self, url: &Url) -> bool {
        let target = url.as_str();
        self.host_direct
            .load()
            .values()
            .any(|verification| target.starts_with(&verification.host_url))
    }
}

enum UrlClass {
    EnvLoopback,
    HostDirect,
    PublicDirect,
}

/// Reject header names/values containing CR or LF (or `:` in a name) so curl's
/// argv-level `--header "name: value"` cannot smuggle a forged second header
/// into the HTTP request line.
fn validate_headers(req: &HttpRequest) -> Result<(), HttpError> {
    for (k, v) in &req.headers {
        if k.is_empty() || k.contains(['\r', '\n', ':']) {
            return Err(HttpError::Transport(format!("invalid header name: {k:?}")));
        }
        if v.contains(['\r', '\n']) {
            return Err(HttpError::Transport(format!(
                "invalid header value for {k}: contains CR/LF"
            )));
        }
    }
    Ok(())
}

fn is_loopback_url(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain(d)) => d.eq_ignore_ascii_case("localhost"),
        None => false,
    }
}

#[async_trait]
impl HttpTransport for WslHttpTransport {
    async fn execute(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        validate_headers(&req)?;
        let url =
            Url::parse(&req.url).map_err(|e| HttpError::Transport(format!("invalid url: {e}")))?;
        match self.url_classification(&url) {
            UrlClass::PublicDirect | UrlClass::HostDirect => {
                crate::environment::runtime::transport::reqwest_direct_execute(&self.direct, req)
                    .await
            },
            UrlClass::EnvLoopback => execute_wrapped(&self.distro, req).await,
        }
    }

    async fn execute_stream(&self, req: HttpRequest) -> Result<ResponseStream, HttpError> {
        validate_headers(&req)?;
        let url =
            Url::parse(&req.url).map_err(|e| HttpError::Transport(format!("invalid url: {e}")))?;
        match self.url_classification(&url) {
            UrlClass::PublicDirect | UrlClass::HostDirect => {
                crate::environment::runtime::transport::reqwest_direct_execute_stream(
                    &self.direct,
                    req,
                )
                .await
            },
            UrlClass::EnvLoopback => Err(HttpError::StreamingUnsupported {
                route_origin: "env_loopback".into(),
            }),
        }
    }

    fn can_stream(&self, url: &Url) -> bool {
        !matches!(self.url_classification(url), UrlClass::EnvLoopback)
    }
}

async fn execute_wrapped(distro: &str, req: HttpRequest) -> Result<HttpResponse, HttpError> {
    let timeout = req.timeout;
    let mut command = build_curl_command(distro, &req);
    let output = tokio::time::timeout(timeout, run_curl_command(&mut command, req.body))
        .await
        .map_err(|_| HttpError::Timeout(timeout))??;

    if !output.status.success() {
        return Err(curl_failure(&output));
    }

    let (body, status) = split_curl_status(&output.stdout);
    let status = status
        .parse::<u16>()
        .map_err(|e| HttpError::Transport(format!("curl emitted invalid status trailer: {e}")))?;
    Ok(HttpResponse {
        status,
        headers: HashMap::new(),
        body,
    })
}

fn build_curl_command(distro: &str, req: &HttpRequest) -> tokio::process::Command {
    let mut command = tokio::process::Command::new("wsl.exe");
    command
        .arg("-d")
        .arg(distro)
        .arg("--exec")
        .arg("curl")
        .arg("--silent")
        .arg("--show-error")
        .arg("--request")
        .arg(http_method_as_str(req.method))
        .arg("--max-time")
        .arg(format_timeout(req.timeout))
        .arg("--write-out")
        .arg(CURL_STATUS_WRITE_OUT)
        .arg("--output")
        .arg("-");

    for (k, v) in &req.headers {
        command.arg("--header").arg(format!("{k}: {v}"));
    }

    if req.body.is_some() {
        command.arg("--data-binary").arg("@-");
        command.stdin(std::process::Stdio::piped());
    }

    command
        .arg("--")
        .arg(&req.url)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    command
}

async fn run_curl_command(
    command: &mut tokio::process::Command,
    body: Option<Bytes>,
) -> Result<Output, HttpError> {
    let Some(body) = body else {
        return command
            .output()
            .await
            .map_err(|e| HttpError::Transport(format!("spawn wsl curl: {e}")));
    };

    let mut child = command
        .spawn()
        .map_err(|e| HttpError::Transport(format!("spawn wsl curl: {e}")))?;
    let Some(mut stdin) = child.stdin.take() else {
        return Err(HttpError::Transport("wsl curl stdin unavailable".into()));
    };

    let write_body = async move {
        stdin.write_all(&body).await?;
        stdin.shutdown().await
    };
    let wait_output = child.wait_with_output();
    let (write_result, output_result) = tokio::join!(write_body, wait_output);
    let output = output_result.map_err(|e| HttpError::Transport(format!("wait wsl curl: {e}")))?;
    if output.status.success() {
        write_result.map_err(|e| HttpError::Transport(format!("write wsl curl stdin: {e}")))?;
    }
    Ok(output)
}

fn curl_failure(output: &Output) -> HttpError {
    let code = output
        .status
        .code()
        .map_or_else(|| "signal".to_string(), |c| c.to_string());
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.is_empty() {
        HttpError::Transport(format!("wsl curl exited with {code}"))
    } else {
        HttpError::Transport(format!("wsl curl exited with {code}: {stderr}"))
    }
}

fn split_curl_status(raw: &[u8]) -> (Bytes, String) {
    let raw = raw.strip_suffix(b"\n").unwrap_or(raw);
    let Some(delimiter_start) = raw
        .windows(CURL_STATUS_DELIMITER.len())
        .rposition(|window| window == CURL_STATUS_DELIMITER)
    else {
        return (Bytes::copy_from_slice(raw), String::new());
    };
    let body = Bytes::copy_from_slice(&raw[..delimiter_start]);
    let status =
        String::from_utf8_lossy(&raw[delimiter_start + CURL_STATUS_DELIMITER.len()..]).to_string();
    (body, status)
}

const fn http_method_as_str(m: HttpMethod) -> &'static str {
    match m {
        HttpMethod::Get => "GET",
        HttpMethod::Head => "HEAD",
        HttpMethod::Post => "POST",
        HttpMethod::Put => "PUT",
        HttpMethod::Patch => "PATCH",
        HttpMethod::Delete => "DELETE",
    }
}

fn format_timeout(timeout: Duration) -> String {
    format!("{:.3}", timeout.as_secs_f64())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_without_host_direct_classifies_env_loopback() {
        let t = WslHttpTransport::new("Ubuntu", HashMap::new());
        let url = Url::parse("http://127.0.0.1:11434/api/version").unwrap();
        assert!(matches!(t.url_classification(&url), UrlClass::EnvLoopback));
    }

    #[test]
    fn loopback_with_host_direct_classifies_host_direct() {
        let mut hd = HashMap::new();
        hd.insert(
            ResourceId("ollama".into()),
            HostDirectVerification {
                verified_at: chrono::Utc::now(),
                method: crate::environment::runtime::env::HostDirectMethod::WslMirroredNetworking,
                host_url: "http://127.0.0.1:11434".into(),
                probe_route_path: "/api/version".into(),
                stable_fingerprint: "abc".into(),
                recompute_jsonpaths: vec!["$.version".into()],
            },
        );
        let t = WslHttpTransport::new("Ubuntu", hd);
        let url = Url::parse("http://127.0.0.1:11434/api/version").unwrap();
        assert!(matches!(t.url_classification(&url), UrlClass::HostDirect));
    }

    #[test]
    fn public_url_classifies_public_direct() {
        let t = WslHttpTransport::new("Ubuntu", HashMap::new());
        let url = Url::parse("https://api.openai.com/v1/models").unwrap();
        assert!(matches!(t.url_classification(&url), UrlClass::PublicDirect));
    }

    #[test]
    fn split_curl_status_extracts_trailing_status() {
        let raw = b"hello world\n\n200\n";
        let (body, status) = split_curl_status(raw);
        assert_eq!(body.as_ref(), b"hello world");
        assert_eq!(status, "200");
    }

    #[test]
    fn ipv6_loopback_classifies_env_loopback() {
        let t = WslHttpTransport::new("Ubuntu", HashMap::new());
        let url = Url::parse("http://[::1]:11434/api/version").unwrap();
        assert!(matches!(t.url_classification(&url), UrlClass::EnvLoopback));
    }

    #[test]
    fn localhost_uppercase_classifies_env_loopback() {
        // Case-insensitive match for localhost.
        let t = WslHttpTransport::new("Ubuntu", HashMap::new());
        let url = Url::parse("http://LOCALHOST:11434/api/version").unwrap();
        assert!(matches!(t.url_classification(&url), UrlClass::EnvLoopback));
    }

    #[test]
    fn localhost_without_explicit_port_classifies_env_loopback() {
        let t = WslHttpTransport::new("Ubuntu", HashMap::new());
        let url = Url::parse("http://localhost/api/version").unwrap();
        assert!(matches!(t.url_classification(&url), UrlClass::EnvLoopback));
    }

    #[test]
    fn localhost_subdomain_does_not_classify_as_loopback() {
        // `localhost.evil.example` must NOT match the loopback rule; otherwise
        // requests to an attacker-controlled hostname would be proxied
        // through the distro's curl instead of going direct.
        let t = WslHttpTransport::new("Ubuntu", HashMap::new());
        let url = Url::parse("http://localhost.evil.example/api/version").unwrap();
        assert!(matches!(t.url_classification(&url), UrlClass::PublicDirect));
    }

    #[test]
    fn split_curl_status_no_delimiter_returns_empty_status() {
        let raw = b"rawbody";
        let (body, status) = split_curl_status(raw);
        assert_eq!(body.as_ref(), b"rawbody");
        assert_eq!(status, "");
    }

    #[test]
    fn split_curl_status_empty_input_returns_empty_pair() {
        let (body, status) = split_curl_status(b"");
        assert_eq!(body.as_ref(), b"");
        assert_eq!(status, "");
    }

    #[test]
    fn validate_headers_rejects_crlf_in_value() {
        let req = HttpRequest {
            method: HttpMethod::Get,
            url: "http://localhost/".into(),
            headers: HashMap::from([(
                "X-Forwarded".to_string(),
                "ok\r\nAuthorization: bearer evil".to_string(),
            )]),
            body: None,
            timeout: Duration::from_secs(1),
        };
        let err = validate_headers(&req).unwrap_err();
        assert!(matches!(err, HttpError::Transport(_)));
    }

    #[test]
    fn validate_headers_rejects_colon_in_name() {
        let req = HttpRequest {
            method: HttpMethod::Get,
            url: "http://localhost/".into(),
            headers: HashMap::from([("bogus:name".to_string(), "x".to_string())]),
            body: None,
            timeout: Duration::from_secs(1),
        };
        let err = validate_headers(&req).unwrap_err();
        assert!(matches!(err, HttpError::Transport(_)));
    }

    #[test]
    fn validate_headers_accepts_normal_headers() {
        let req = HttpRequest {
            method: HttpMethod::Get,
            url: "http://localhost/".into(),
            headers: HashMap::from([
                ("Accept".to_string(), "application/json".to_string()),
                ("Authorization".to_string(), "bearer x".to_string()),
            ]),
            body: None,
            timeout: Duration::from_secs(1),
        };
        assert!(validate_headers(&req).is_ok());
    }
}
