//! Local dispatcher and HTTP transport implementations for the host environment.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::TryStreamExt;
use url::Url;

use super::dispatcher::{HttpTransport, ResponseStream};
use super::env::EnvInfo;
use super::transport::{HttpError, HttpMethod, HttpRequest, HttpResponse};

/// Reqwest-backed HTTP transport that operates in the host process's network
/// namespace (direct loopback access, no tunnelling).
///
/// `can_stream` always returns `true` — local transports have no routing
/// restrictions that would prevent SSE or chunked streaming.
#[derive(Debug, Clone)]
pub struct LocalHttpTransport {
    client: reqwest::Client,
}

impl LocalHttpTransport {
    /// Construct a new transport with a 30-second default timeout.
    ///
    /// The per-request `HttpRequest::timeout` field always overrides this value.
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
        }
    }
}

impl Default for LocalHttpTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HttpTransport for LocalHttpTransport {
    async fn execute(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let method = http_method_to_reqwest(req.method);
        let mut builder = self.client.request(method, &req.url).timeout(req.timeout);
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
        let status = resp.status().as_u16();
        let headers = resp
            .headers()
            .iter()
            .filter_map(|(k, v)| v.to_str().ok().map(|s| (k.to_string(), s.to_string())))
            .collect();
        let body = resp
            .bytes()
            .await
            .map_err(|e| HttpError::Transport(e.to_string()))?;
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }

    async fn execute_stream(&self, req: HttpRequest) -> Result<ResponseStream, HttpError> {
        // Streaming is only wired for GET and POST; other methods require
        // buffered `execute` instead. Callers that probe via HEAD or PUT should
        // not request streaming.
        let method = match req.method {
            HttpMethod::Get => reqwest::Method::GET,
            HttpMethod::Post => reqwest::Method::POST,
            _ => return Err(HttpError::Transport("stream supports GET/POST only".into())),
        };
        let mut builder = self.client.request(method, &req.url).timeout(req.timeout);
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

    /// Local transports can always stream — no routing or tunnel constraints.
    fn can_stream(&self, _url: &Url) -> bool {
        true
    }
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

/// Placeholder for the local-environment dispatcher.
///
/// `LocalDispatcher` runs everything in the host process's own namespace:
/// direct filesystem access, direct network loopback, host `PATH`.
/// Tasks 14–18 fill in the `Dispatcher` trait implementation.
#[derive(Debug)]
pub struct LocalDispatcher {
    /// Metadata about this environment (id, label, spec, state).
    pub info: EnvInfo,
    /// Shared HTTP transport bound to the host network namespace.
    pub transport: Arc<LocalHttpTransport>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::transport::{HttpMethod, HttpRequest};
    use std::collections::HashMap;
    use std::time::Duration;
    use url::Url;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn local_http_get_returns_body() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/hi"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello".to_vec()))
            .mount(&server)
            .await;

        let transport = LocalHttpTransport::new();
        let req = HttpRequest {
            method: HttpMethod::Get,
            url: format!("{}/hi", server.uri()),
            headers: HashMap::default(),
            body: None,
            timeout: Duration::from_secs(5),
        };
        let resp = transport.execute(req).await.expect("ok");
        assert_eq!(resp.status, 200);
        assert_eq!(&resp.body[..], b"hello");
    }

    #[tokio::test]
    async fn local_http_can_stream_arbitrary_url() {
        let t = LocalHttpTransport::new();
        assert!(t.can_stream(&Url::parse("http://anything").unwrap()));
    }
}
