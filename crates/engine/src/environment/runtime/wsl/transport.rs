//! HTTP transport for `WslDispatcher`: env-loopback wrap vs `HostDirect` direct
//! vs public direct.

use std::collections::HashMap;
use std::process::Output;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use tokio::io::AsyncWriteExt;
use url::Url;

use crate::environment::runtime::dispatcher::{HttpTransport, ResponseStream};
use crate::environment::runtime::env::HostDirectVerification;
use crate::environment::runtime::resource::ResourceId;
use crate::environment::runtime::transport::{HttpError, HttpMethod, HttpRequest, HttpResponse};

const CURL_STATUS_WRITE_OUT: &str = "\n\n%{http_code}\n";
const CURL_STATUS_DELIMITER: &[u8] = b"\n\n";

/// WSL-aware HTTP transport that routes env-loopback requests through curl
/// inside the distro while keeping verified host-direct and public URLs direct.
#[derive(Debug, Clone)]
pub struct WslHttpTransport {
    distro: String,
    direct: reqwest::Client,
    host_direct: Arc<HashMap<ResourceId, HostDirectVerification>>,
}

impl WslHttpTransport {
    /// Construct a new transport with a 30-second default direct-client timeout.
    pub fn new(
        distro: impl Into<String>,
        host_direct: HashMap<ResourceId, HostDirectVerification>,
    ) -> Self {
        Self {
            distro: distro.into(),
            direct: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
            host_direct: Arc::new(host_direct),
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
            .values()
            .any(|verification| target.starts_with(&verification.host_url))
    }
}

enum UrlClass {
    EnvLoopback,
    HostDirect,
    PublicDirect,
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
        let url =
            Url::parse(&req.url).map_err(|e| HttpError::Transport(format!("invalid url: {e}")))?;
        match self.url_classification(&url) {
            UrlClass::PublicDirect | UrlClass::HostDirect => {
                execute_direct(&self.direct, req).await
            },
            UrlClass::EnvLoopback => execute_wrapped(&self.distro, req).await,
        }
    }

    async fn execute_stream(&self, req: HttpRequest) -> Result<ResponseStream, HttpError> {
        let url =
            Url::parse(&req.url).map_err(|e| HttpError::Transport(format!("invalid url: {e}")))?;
        match self.url_classification(&url) {
            UrlClass::PublicDirect | UrlClass::HostDirect => {
                execute_stream_direct(&self.direct, req).await
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

async fn execute_direct(
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

async fn execute_stream_direct(
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
}
