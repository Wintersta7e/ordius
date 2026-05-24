//! Local dispatcher and HTTP transport implementations for the host environment.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::TryStreamExt;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::catalog::ResourceProbeOutcome;
use super::dispatcher::{Dispatcher, HttpTransport, ResponseStream};
use super::env::{EnvInfo, RunId, WorkspaceBinding};
use super::error::DispatchError;
use super::plan::{ProbePlan, ProbeSummary};
use super::resource::ResourceDefinition;
use super::transport::{
    EnvPath, HttpError, HttpMethod, HttpRequest, HttpResponse, ProcessCmd, WorkspaceHandle,
};

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

/// Runs everything in the host process's own namespace: direct filesystem
/// access, direct network loopback, host `PATH`.
///
/// `translate_path` is the identity — host paths are already env paths.
/// `prepare_workspace` accepts only `WorkspaceBinding::Shared`; other
/// bindings (`Translated`, `BindMount`, `Sync`) have no meaning on the local env.
#[derive(Debug)]
pub struct LocalDispatcher {
    /// Metadata about this environment (id, label, spec, state).
    info: EnvInfo,
    /// Shared HTTP transport bound to the host network namespace.
    transport: Arc<LocalHttpTransport>,
}

impl LocalDispatcher {
    /// Construct a `LocalDispatcher` for the given environment info.
    pub fn new(info: EnvInfo) -> Self {
        Self {
            info,
            transport: Arc::new(LocalHttpTransport::new()),
        }
    }
}

#[async_trait]
impl Dispatcher for LocalDispatcher {
    /// Return metadata for this environment.
    fn info(&self) -> &EnvInfo {
        &self.info
    }

    /// Full probe pass — implemented in Task 18.
    async fn probe(
        &self,
        _plan: ProbePlan,
        _cancel: CancellationToken,
    ) -> Result<ProbeSummary, DispatchError> {
        unimplemented!("Task 18 wires the probe orchestrator")
    }

    /// Single-resource re-probe — implemented in Tasks 16 + 17.
    async fn probe_resource(
        &self,
        _def: &ResourceDefinition,
        _cancel: CancellationToken,
    ) -> ResourceProbeOutcome {
        unimplemented!("Tasks 16+17 wire single-resource probing")
    }

    /// Spawn a subprocess in the host namespace.
    ///
    /// Builds a `tokio::process::Command` from the argv-only `ProcessCmd` and
    /// delegates to `executor::supervisor::spawn`, which sets up a process group
    /// (Unix) or Job Object (Windows) for correct tree teardown.
    fn spawn(&self, cmd: ProcessCmd) -> std::io::Result<crate::executor::supervisor::Supervised> {
        let mut command = tokio::process::Command::new(&cmd.program);
        command.args(&cmd.args);
        for (k, v) in &cmd.env {
            command.env(k, v);
        }
        if let Some(cwd) = &cmd.cwd {
            command.current_dir(cwd.as_str());
        }
        // Note: ProcessCmd.stdin is currently ignored; the supervisor takes
        // ownership of the Child after spawn. Callers that need stdin should
        // pipe it after spawn via Supervised::child_mut(), matching the pattern
        // in executor/builtins/subprocess.rs.
        crate::executor::supervisor::spawn(command)
    }

    /// Return the host-network HTTP transport.
    fn http_transport(&self) -> Arc<dyn HttpTransport> {
        self.transport.clone()
    }

    /// Identity translation: the local env shares the host filesystem.
    fn translate_path(&self, host_path: &Path) -> Result<EnvPath, DispatchError> {
        Ok(EnvPath::new(host_path.to_string_lossy().into_owned()))
    }

    /// For `WorkspaceBinding::Shared` the env path equals the host path and
    /// no teardown is needed. Any other binding is unsupported on the local env.
    async fn prepare_workspace(
        &self,
        workspace_host: &Path,
        binding: &WorkspaceBinding,
        _run_id: &RunId,
    ) -> Result<WorkspaceHandle, DispatchError> {
        match binding {
            WorkspaceBinding::Shared => Ok(WorkspaceHandle {
                env_path: EnvPath::new(workspace_host.to_string_lossy().into_owned()),
                teardown: None,
            }),
            other => Err(DispatchError::WorkspaceUnavailable {
                env_id: self.info.id.to_string(),
                reason: format!("LocalDispatcher only supports Shared binding, got {other:?}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::env::{EnvId, EnvSpec, EnvState};
    use crate::environment::runtime::transport::{HttpMethod, HttpRequest};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::time::Duration;
    use url::Url;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn local_info() -> EnvInfo {
        EnvInfo {
            id: EnvId::local(),
            label: "Local (host)".into(),
            spec: EnvSpec::Local {
                resources: vec![],
                host_direct_verifications: HashMap::default(),
            },
            state: EnvState::Reachable,
            enabled: true,
        }
    }

    #[test]
    fn local_dispatcher_info() {
        let d = LocalDispatcher::new(local_info());
        assert_eq!(d.info().id, EnvId::local());
    }

    #[test]
    fn local_translate_path_is_identity() {
        let d = LocalDispatcher::new(local_info());
        let host = PathBuf::from("/some/path");
        let env_path = d.translate_path(&host).expect("ok");
        assert_eq!(env_path.as_str(), "/some/path");
    }

    #[tokio::test]
    async fn local_prepare_workspace_shared_is_noop() {
        let d = LocalDispatcher::new(local_info());
        let host = PathBuf::from("/workspaces/wf");
        let handle = d
            .prepare_workspace(&host, &WorkspaceBinding::Shared, &RunId("r1".into()))
            .await
            .expect("ok");
        assert_eq!(handle.env_path.as_str(), "/workspaces/wf");
    }

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

    #[tokio::test]
    async fn local_spawn_echo_succeeds() {
        let d = LocalDispatcher::new(local_info());
        let cmd = ProcessCmd {
            program: if cfg!(windows) {
                "cmd".into()
            } else {
                "echo".into()
            },
            args: if cfg!(windows) {
                vec!["/C".into(), "echo hello".into()]
            } else {
                vec!["hello".into()]
            },
            env: std::collections::HashMap::new(),
            cwd: None,
            stdin: None,
        };
        let mut sup = d.spawn(cmd).expect("spawn");
        let _exit = crate::executor::supervisor::cancel(&mut sup).await;
    }
}
