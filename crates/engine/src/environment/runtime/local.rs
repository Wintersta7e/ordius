//! Local dispatcher and HTTP transport implementations for the host environment.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::TryStreamExt;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::catalog::{ProvenRoute, ResourceDetail, ResourceProbeOutcome, RouteOrigin};
use super::dispatcher::{Dispatcher, HttpTransport, ResponseStream};
use super::env::{EnvInfo, RunId, WorkspaceBinding};
use super::error::DispatchError;
use super::plan::{ProbePlan, ProbeSummary};
use super::resource::{HttpProbeMethod, HttpProbeRoute, ProbeSpec, ResourceDefinition};
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
        def: &ResourceDefinition,
        cancel: CancellationToken,
    ) -> ResourceProbeOutcome {
        match &def.probe {
            ProbeSpec::Http {
                ports,
                routes,
                timeout_ms,
            } => self.probe_http(ports, routes, *timeout_ms, cancel).await,
            ProbeSpec::Binary {
                bin,
                version_args,
                version_regex,
                extra_search_paths,
                timeout_ms,
            } => {
                self.probe_binary_or_toolchain(
                    def,
                    bin,
                    version_args,
                    version_regex,
                    extra_search_paths,
                    *timeout_ms,
                    false,
                )
                .await
            },
            ProbeSpec::Toolchain {
                bin,
                version_args,
                version_regex,
                timeout_ms,
            } => {
                self.probe_binary_or_toolchain(
                    def,
                    bin,
                    version_args,
                    version_regex,
                    &[],
                    *timeout_ms,
                    true,
                )
                .await
            },
        }
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

impl LocalDispatcher {
    async fn probe_http(
        &self,
        ports: &[u16],
        routes: &[HttpProbeRoute],
        timeout_ms: Option<u64>,
        _cancel: CancellationToken,
    ) -> ResourceProbeOutcome {
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(1000));

        for &port in ports {
            let mut routes_by_capability = std::collections::HashMap::new();
            let mut any_2xx = false;

            for route in routes {
                let req = HttpRequest {
                    method: match route.method {
                        HttpProbeMethod::Get => HttpMethod::Get,
                        HttpProbeMethod::Head => HttpMethod::Head,
                    },
                    url: format!("http://127.0.0.1:{port}{}", route.path),
                    headers: std::collections::HashMap::default(),
                    body: None,
                    timeout,
                };

                if let Ok(resp) = self.transport.execute(req).await
                    && (200..300).contains(&resp.status)
                {
                    any_2xx = true;
                    for cap in &route.proves {
                        routes_by_capability
                            .entry(*cap)
                            .or_insert_with(|| ProvenRoute {
                                path: route.path.clone(),
                                method: route.method,
                                flavor: route.flavor,
                            });
                    }
                }
            }

            if any_2xx {
                return ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
                    base_url: format!("http://127.0.0.1:{port}"),
                    routes_by_capability,
                    version: None,
                    models_list: None,
                    auth_secret_ref: None,
                    streaming_supported_natively: true,
                    route_origin: RouteOrigin::HostDirect,
                });
            }
        }

        ResourceProbeOutcome::NotFound
    }

    /// Locate a binary + run its version command, returning `Found(Binary)`
    /// or `Found(Toolchain)` on success.
    ///
    /// Resolution order:
    /// 1. `which::which(bin)` — searches `PATH` exactly as a shell would.
    /// 2. Each path in `extra_search_paths` joined with `bin` — useful for
    ///    tools installed outside `PATH` (e.g. `~/.local/bin` on some distros).
    ///
    /// The binary is spawned with `version_args`; stdout and stderr are
    /// concatenated before the regex is applied. Group 1 of `version_regex`
    /// becomes the version string. If the regex has no capture groups the
    /// version is `None` (not an error). A hard `timeout_ms` (default 2 s)
    /// guards the spawn so a stalled binary never blocks the probe loop.
    ///
    /// `is_toolchain` selects the `ResourceDetail` variant: `Toolchain` when
    /// `true`, `Binary` when `false`.
    async fn probe_binary_or_toolchain(
        &self,
        def: &ResourceDefinition,
        bin: &str,
        version_args: &[String],
        version_regex: &str,
        extra_search_paths: &[String],
        timeout_ms: Option<u64>,
        is_toolchain: bool,
    ) -> ResourceProbeOutcome {
        // 1. Locate the binary.
        let Some(path) = which_with_fallback(bin, extra_search_paths) else {
            return ResourceProbeOutcome::NotFound;
        };

        // 2. Spawn with a hard timeout.
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(2_000));
        let mut command = tokio::process::Command::new(&path);
        command.args(version_args);
        command.stdout(std::process::Stdio::piped());
        command.stderr(std::process::Stdio::piped());

        let output = match tokio::time::timeout(timeout, command.output()).await {
            Ok(Ok(out)) => out,
            Ok(Err(e)) => {
                return ResourceProbeOutcome::ProbeFailed {
                    reason: format!("spawn {bin}: {e}"),
                };
            },
            Err(_elapsed) => return ResourceProbeOutcome::TimedOut,
        };

        // 3. Combine stdout + stderr (some tools, e.g. older Java, print to stderr).
        let combined = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        // 4. Extract version via capture group 1, if present.
        let re = match regex::Regex::new(version_regex) {
            Ok(r) => r,
            Err(e) => {
                return ResourceProbeOutcome::ProbeFailed {
                    reason: format!("invalid version_regex: {e}"),
                };
            },
        };
        let version = re
            .captures(&combined)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_owned());

        // 5. Return the appropriate Found variant.
        if is_toolchain {
            ResourceProbeOutcome::Found(ResourceDetail::Toolchain {
                name: def.id.0.clone(),
                version,
                exe_path: path.to_string_lossy().into_owned(),
            })
        } else {
            ResourceProbeOutcome::Found(ResourceDetail::Binary {
                path: path.to_string_lossy().into_owned(),
                version,
                capabilities: def.advertised_capabilities.clone(),
            })
        }
    }
}

/// Resolve `bin` to an absolute path.
///
/// Tries `which::which` first (honours `PATH`), then walks `extra_search_paths`
/// appending `bin` directly. Returns the first candidate that is a regular file.
fn which_with_fallback(bin: &str, extras: &[String]) -> Option<PathBuf> {
    if let Ok(p) = which::which(bin) {
        return Some(p);
    }
    for dir in extras {
        let cand = PathBuf::from(dir).join(bin);
        if cand.is_file() {
            return Some(cand);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::catalog::{ResourceDetail, RouteOrigin};
    use crate::environment::runtime::env::{EnvId, EnvSpec, EnvState};
    use crate::environment::runtime::resource::{
        ApiFlavor, Capability, HttpProbeMethod, HttpProbeRoute, ProbeSpec, ResourceId, ResourceKind,
    };
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

    #[tokio::test]
    async fn probe_resource_http_found_with_proven_capability() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/version"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"version":"0.3.14"}"#))
            .mount(&server)
            .await;

        let port = server.address().port();
        let def = ResourceDefinition {
            id: ResourceId("ollama".into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: vec![Capability::OllamaNative],
            probe: ProbeSpec::Http {
                ports: vec![port],
                routes: vec![HttpProbeRoute {
                    path: "/api/version".into(),
                    method: HttpProbeMethod::Get,
                    flavor: ApiFlavor::OllamaNative,
                    proves: vec![Capability::OllamaNative],
                    models_jsonpath: None,
                    fingerprint_jsonpaths: vec!["$.version".into()],
                }],
                timeout_ms: None,
            },
            override_lower_scope: false,
        };

        let d = LocalDispatcher::new(local_info());
        let outcome = d.probe_resource(&def, CancellationToken::new()).await;

        let ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
            routes_by_capability,
            route_origin,
            ..
        }) = outcome
        else {
            panic!("expected Found, got {outcome:?}");
        };
        assert_eq!(route_origin, RouteOrigin::HostDirect);
        assert!(routes_by_capability.contains_key(&Capability::OllamaNative));
    }

    #[tokio::test]
    async fn probe_resource_binary_git_found() {
        // git is required to run the Ordius test suite anyway, so it's always present.
        let def = ResourceDefinition {
            id: ResourceId("git".into()),
            kind: ResourceKind::Toolchain,
            advertised_capabilities: vec![],
            probe: ProbeSpec::Toolchain {
                bin: "git".into(),
                version_args: vec!["--version".into()],
                version_regex: r"^git version (\S+)".into(),
                timeout_ms: None,
            },
            override_lower_scope: false,
        };

        let d = LocalDispatcher::new(local_info());
        let outcome = d.probe_resource(&def, CancellationToken::new()).await;
        let ResourceProbeOutcome::Found(ResourceDetail::Toolchain { name, version, .. }) = outcome
        else {
            panic!("expected Found Toolchain, got {outcome:?}")
        };
        assert_eq!(name, "git");
        assert!(version.is_some(), "version captured by regex");
    }

    #[tokio::test]
    async fn probe_resource_binary_missing_returns_not_found() {
        let def = ResourceDefinition {
            id: ResourceId("definitely-not-installed-xyz".into()),
            kind: ResourceKind::Binary,
            advertised_capabilities: vec![],
            probe: ProbeSpec::Binary {
                bin: "definitely-not-installed-xyz".into(),
                version_args: vec!["--version".into()],
                version_regex: r"(\S+)".into(),
                extra_search_paths: vec![],
                timeout_ms: None,
            },
            override_lower_scope: false,
        };

        let d = LocalDispatcher::new(local_info());
        let outcome = d.probe_resource(&def, CancellationToken::new()).await;
        assert!(matches!(outcome, ResourceProbeOutcome::NotFound));
    }

    #[tokio::test]
    async fn probe_resource_http_not_found_on_404() {
        let server = MockServer::start().await;
        let port = server.address().port();
        let def = ResourceDefinition {
            id: ResourceId("ollama".into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: vec![Capability::OllamaNative],
            probe: ProbeSpec::Http {
                ports: vec![port],
                routes: vec![HttpProbeRoute {
                    path: "/api/version".into(),
                    method: HttpProbeMethod::Get,
                    flavor: ApiFlavor::OllamaNative,
                    proves: vec![Capability::OllamaNative],
                    models_jsonpath: None,
                    fingerprint_jsonpaths: vec![],
                }],
                timeout_ms: None,
            },
            override_lower_scope: false,
        };

        let d = LocalDispatcher::new(local_info());
        let outcome = d.probe_resource(&def, CancellationToken::new()).await;
        assert!(matches!(outcome, ResourceProbeOutcome::NotFound));
    }
}
