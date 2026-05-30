//! `WslDispatcher` — `Dispatcher` impl for a Windows Subsystem for Linux distro.

use std::collections::HashMap;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use ordius_helper::protocol::{
    HttpProbeMethodV1, HttpProbeRouteV1, ProbeDetailV1, ProbeOutcomeBodyV1, ProbeOutcomeV1,
    ProbePlanV1, ProvenRouteV1, ResourceKindV1, ResourceSpecV1,
};
use tokio::io::AsyncWriteExt;
use tokio::sync::OnceCell;
use tokio_util::sync::CancellationToken;

use crate::environment::runtime::catalog::{
    ProvenRoute, ResourceCatalog, ResourceDetail, ResourceProbeOutcome, RouteOrigin,
};
use crate::environment::runtime::dispatcher::{Dispatcher, HttpTransport};
use crate::environment::runtime::env::{EnvInfo, HostDirectVerification, RunId, WorkspaceBinding};
use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::plan::{ProbePlan, ProbeSummary};
use crate::environment::runtime::resource::{
    Capability, HttpProbeMethod, ProbeSpec, ResourceDefinition, ResourceId, ResourceKind,
};
use crate::environment::runtime::transport::{
    EnvPath, LocalProcess, ProcessCmd, Stdio as ProcessStdio, WorkspaceHandle,
};

use super::bootstrap::BootstrappedHelper;

const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const HELPER_PROBE_GRACE: Duration = Duration::from_secs(2);

/// `Dispatcher` implementation backed by a named WSL distribution.
///
/// `helper_cache`, `host_direct`, and `transport` are `Arc`-shared so clones
/// observe the same bootstrap result, `set_host_direct` updates, and (critically)
/// the same `reqwest::Client` connection pool — rebuilding the client per call
/// was a measurable hot-path leak before this was cached.
#[derive(Debug, Clone)]
pub struct WslDispatcher {
    info: EnvInfo,
    distro_name: String,
    helper_cache: Arc<OnceCell<BootstrappedHelper>>,
    host_direct: super::transport::HostDirectMap,
    transport: Arc<super::transport::WslHttpTransport>,
}

impl WslDispatcher {
    /// Build a `WslDispatcher` for the given environment metadata and distro name.
    pub fn new(info: EnvInfo, distro_name: impl Into<String>) -> Self {
        let distro_name = distro_name.into();
        let host_direct: super::transport::HostDirectMap =
            Arc::new(ArcSwap::from_pointee(HashMap::new()));
        let transport = Arc::new(super::transport::WslHttpTransport::with_host_direct(
            &distro_name,
            Arc::clone(&host_direct),
        ));
        Self {
            info,
            distro_name,
            helper_cache: Arc::new(OnceCell::new()),
            host_direct,
            transport,
        }
    }

    /// Return the WSL distribution name passed to `wsl.exe -d <name>`.
    pub fn distro_name(&self) -> &str {
        &self.distro_name
    }

    /// Replace the dispatcher's `HostDirect` verification map. Visible to all
    /// clones because the map is stored behind an `Arc<ArcSwap<_>>`.
    ///
    /// Phase E wires this from `EnvSpec::WslDistro::host_direct_verifications`
    /// whenever the environment spec changes.
    pub fn set_host_direct(&self, verifications: HashMap<ResourceId, HostDirectVerification>) {
        self.host_direct.store(Arc::new(verifications));
    }

    fn host_direct_snapshot(&self) -> HashMap<ResourceId, HostDirectVerification> {
        (*self.host_direct.load_full()).clone()
    }

    async fn ensure_helper(&self) -> Result<BootstrappedHelper, DispatchError> {
        self.helper_cache
            .get_or_try_init(|| async {
                let triple = super::bootstrap::probe_env_triple(&self.distro_name).await?;
                super::bootstrap::bootstrap_helper(&self.distro_name, &triple)
                    .await
                    .map_err(DispatchError::from)
            })
            .await
            .cloned()
    }

    async fn probe_resource_via_helper(
        &self,
        helper_path: &str,
        def: &ResourceDefinition,
        cancel: &CancellationToken,
    ) -> ResourceProbeOutcome {
        let (plan_json, timeout_ms) = match helper_probe_plan_json(def) {
            Ok(plan) => plan,
            Err(reason) => return probe_failed(reason),
        };
        let host_direct = self.host_direct_snapshot();
        let wire = match self
            .run_helper_probe(helper_path, &plan_json, timeout_ms, cancel)
            .await
        {
            Ok(wire) => wire,
            Err(outcome) => return outcome,
        };
        if wire.version != 1 {
            return probe_failed(format!(
                "unsupported helper probe outcome version: {}",
                wire.version
            ));
        }
        if wire.id != def.id.0 {
            return probe_failed(format!(
                "helper returned outcome for {}, expected {}",
                wire.id, def.id
            ));
        }

        wire_outcome_to_engine(wire.outcome, def, &host_direct)
    }

    fn helper_probe_command(&self, helper_path: &str) -> tokio::process::Command {
        let mut command = tokio::process::Command::new("wsl.exe");
        command
            .args(["-d", &self.distro_name, "--exec", helper_path, "probe"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        command
    }

    async fn run_helper_probe(
        &self,
        helper_path: &str,
        plan_json: &[u8],
        timeout_ms: u64,
        cancel: &CancellationToken,
    ) -> Result<ProbeOutcomeV1, ResourceProbeOutcome> {
        let mut child = self
            .helper_probe_command(helper_path)
            .spawn()
            .map_err(|e| probe_failed(format!("spawn ordius-helper probe: {e}")))?;

        write_helper_plan(&mut child, plan_json).await?;
        let output = wait_for_helper_probe(child, timeout_ms, cancel).await?;
        parse_helper_probe_output(&output).map_err(probe_failed)
    }

    async fn probe_plan_via_helper(
        &self,
        helper_path: &str,
        plan: &ProbePlan,
        cancel: &CancellationToken,
    ) -> Result<ProbeSummary, DispatchError> {
        let started = std::time::Instant::now();
        let wire_plan = build_wire_plan(plan)?;
        let host_direct = self.host_direct_snapshot();
        let plan_json = serde_json::to_string(&wire_plan)
            .map_err(|e| DispatchError::PlanBuild(format!("serialize probe plan: {e}")))?;

        let mut child = self
            .helper_probe_command(helper_path)
            .spawn()
            .map_err(|e| DispatchError::HelperBootstrap(format!("helper spawn: {e}")))?;

        // Drain the helper's stderr in the background so a chatty helper can't
        // fill the ~64 KB pipe buffer and deadlock the stdout reader below.
        // Track the handle so we can abort it on any exit path.
        let stderr_drainer = spawn_stderr_drainer(&mut child);

        let shutdown_err = match write_plan_to_helper(&mut child, plan_json.as_bytes()).await {
            Ok(err) => err,
            Err(e) => {
                abort_drainer(stderr_drainer);
                return Err(e);
            },
        };

        let Some(stdout) = child.stdout.take() else {
            abort_drainer(stderr_drainer);
            return Err(DispatchError::HelperBootstrap(
                "helper stdout missing".into(),
            ));
        };

        let outcome = consume_helper_stream(&mut child, stdout, plan, &host_direct, cancel).await;
        let exit_status = child.wait().await;
        abort_drainer(stderr_drainer);

        // Helper exited non-zero before emitting any outcomes — likely a crash
        // (linker mismatch, corrupt binary). Surface as DispatchError so the
        // caller can decide whether to fall back, instead of silently flooding
        // the catalog with "no outcome" skips.
        if !outcome.cancelled
            && outcome.total_probed == 0
            && !plan.defs.is_empty()
            && let Ok(status) = &exit_status
            && !status.success()
        {
            use std::fmt::Write as _;
            let mut msg = format!(
                "helper exited with {:?} before emitting outcomes",
                status.code()
            );
            if let Some(err) = &shutdown_err {
                let _ = write!(msg, " (stdin shutdown: {err})");
            }
            return Err(DispatchError::HelperBootstrap(msg));
        }

        let mut resources = outcome.resources;
        for def in &plan.defs {
            resources
                .entry(def.id.clone())
                .or_insert_with(|| ResourceProbeOutcome::Skipped {
                    reason: "helper did not return an outcome".into(),
                });
        }

        Ok(ProbeSummary {
            catalog: ResourceCatalog {
                env_id: plan.env_id.clone(),
                registry_revision: plan.registry_revision,
                probed_at: chrono::Utc::now(),
                resources,
            },
            total_probed: outcome.total_probed,
            elapsed: started.elapsed(),
        })
    }
}

struct HelperStreamOutcome {
    resources: HashMap<ResourceId, ResourceProbeOutcome>,
    total_probed: usize,
    cancelled: bool,
}

fn spawn_stderr_drainer(child: &mut tokio::process::Child) -> Option<tokio::task::JoinHandle<()>> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    child.stderr.take().map(|stderr| {
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::debug!(target: "ordius::wsl::helper", "helper stderr: {line}");
            }
        })
    })
}

fn abort_drainer(drainer: Option<tokio::task::JoinHandle<()>>) {
    if let Some(handle) = drainer {
        handle.abort();
    }
}

/// Pipe the probe-plan JSON into the helper's stdin and close it. Returns the
/// shutdown error if it was anything other than `BrokenPipe` (a tolerated
/// child-closes-early condition).
async fn write_plan_to_helper(
    child: &mut tokio::process::Child,
    plan_json: &[u8],
) -> Result<Option<std::io::Error>, DispatchError> {
    let Some(mut stdin) = child.stdin.take() else {
        return Err(DispatchError::HelperBootstrap(
            "helper stdin missing".into(),
        ));
    };
    stdin
        .write_all(plan_json)
        .await
        .map_err(|e| DispatchError::HelperBootstrap(format!("helper stdin: {e}")))?;
    let shutdown_err = stdin
        .shutdown()
        .await
        .err()
        .and_then(|e| (e.kind() != std::io::ErrorKind::BrokenPipe).then_some(e));
    Ok(shutdown_err)
}

async fn consume_helper_stream(
    child: &mut tokio::process::Child,
    stdout: tokio::process::ChildStdout,
    plan: &ProbePlan,
    host_direct: &HashMap<ResourceId, HostDirectVerification>,
    cancel: &CancellationToken,
) -> HelperStreamOutcome {
    use tokio::io::{AsyncBufReadExt, BufReader};
    let mut reader = BufReader::new(stdout).lines();
    let mut resources: HashMap<ResourceId, ResourceProbeOutcome> = HashMap::default();
    let mut total_probed: usize = 0;
    let defs_by_id: HashMap<&str, &ResourceDefinition> = plan
        .defs
        .iter()
        .map(|def| (def.id.0.as_str(), def))
        .collect();
    let mut cancelled = false;

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                drop(child.kill().await);
                cancelled = true;
                for def in &plan.defs {
                    resources
                        .entry(def.id.clone())
                        .or_insert_with(cancelled_probe_outcome);
                }
                break;
            },
            line = reader.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        let Ok(wire) = serde_json::from_str::<ProbeOutcomeV1>(&line) else {
                            continue;
                        };
                        if wire.version != 1 {
                            continue;
                        }
                        let Some(def) = defs_by_id.get(wire.id.as_str()).copied() else {
                            continue;
                        };
                        if resources
                            .insert(
                                def.id.clone(),
                                wire_outcome_to_engine(wire.outcome, def, host_direct),
                            )
                            .is_none()
                        {
                            total_probed += 1;
                        }
                    },
                    Ok(None) | Err(_) => break,
                }
            },
        }
    }
    HelperStreamOutcome {
        resources,
        total_probed,
        cancelled,
    }
}

async fn write_helper_plan(
    child: &mut tokio::process::Child,
    plan_json: &[u8],
) -> Result<(), ResourceProbeOutcome> {
    let Some(mut stdin) = child.stdin.take() else {
        return Err(probe_failed("ordius-helper probe stdin unavailable"));
    };
    stdin
        .write_all(plan_json)
        .await
        .map_err(|e| probe_failed(format!("write helper probe plan: {e}")))?;
    stdin
        .shutdown()
        .await
        .map_err(|e| probe_failed(format!("finish helper probe plan: {e}")))?;
    Ok(())
}

fn abort_reader_handles(
    stdout_handle: Option<&tokio::task::JoinHandle<Vec<u8>>>,
    stderr_handle: Option<&tokio::task::JoinHandle<Vec<u8>>>,
) {
    if let Some(h) = stdout_handle {
        h.abort();
    }
    if let Some(h) = stderr_handle {
        h.abort();
    }
}

enum CancelOrWait {
    Status(std::process::ExitStatus),
    WaitErr(std::io::Error),
    TimedOut,
    Cancelled,
}

async fn wait_for_helper_probe(
    mut child: tokio::process::Child,
    timeout_ms: u64,
    cancel: &CancellationToken,
) -> Result<std::process::Output, ResourceProbeOutcome> {
    use tokio::io::AsyncReadExt;
    let stdout_handle = child.stdout.take().map(|mut s| {
        tokio::spawn(async move {
            let mut buf = Vec::new();
            drop(s.read_to_end(&mut buf).await);
            buf
        })
    });
    let stderr_handle = child.stderr.take().map(|mut s| {
        tokio::spawn(async move {
            let mut buf = Vec::new();
            drop(s.read_to_end(&mut buf).await);
            buf
        })
    });
    // Race three signals: child exit, helper-wait timeout, outer cancel.
    let wait_outcome = tokio::select! {
        () = cancel.cancelled() => CancelOrWait::Cancelled,
        outcome = tokio::time::timeout(helper_wait_timeout(timeout_ms), child.wait()) => match outcome {
            Ok(Ok(status)) => CancelOrWait::Status(status),
            Ok(Err(e)) => CancelOrWait::WaitErr(e),
            Err(_) => CancelOrWait::TimedOut,
        },
    };
    let status = match wait_outcome {
        CancelOrWait::Status(status) => status,
        CancelOrWait::WaitErr(e) => {
            abort_reader_handles(stdout_handle.as_ref(), stderr_handle.as_ref());
            drop(child.kill().await);
            drop(child.wait().await);
            return Err(probe_failed(format!("wait for ordius-helper probe: {e}")));
        },
        CancelOrWait::TimedOut => {
            abort_reader_handles(stdout_handle.as_ref(), stderr_handle.as_ref());
            drop(child.kill().await);
            drop(child.wait().await);
            return Err(ResourceProbeOutcome::TimedOut);
        },
        CancelOrWait::Cancelled => {
            abort_reader_handles(stdout_handle.as_ref(), stderr_handle.as_ref());
            drop(child.kill().await);
            drop(child.wait().await);
            return Err(cancelled_probe_outcome());
        },
    };
    let stdout = match stdout_handle {
        Some(h) => h.await.unwrap_or_default(),
        None => Vec::new(),
    };
    let stderr = match stderr_handle {
        Some(h) => h.await.unwrap_or_default(),
        None => Vec::new(),
    };
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

fn parse_helper_probe_output(output: &std::process::Output) -> Result<ProbeOutcomeV1, String> {
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stderr = stderr.trim();
        let code = output.status.code();
        tracing::debug!(
            status = ?code,
            stderr,
            "ordius-helper probe exited unsuccessfully"
        );
        return Err(if stderr.is_empty() {
            format!("ordius-helper probe exited with {code:?}")
        } else {
            format!("ordius-helper probe exited with {code:?}: {stderr}")
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout
        .lines()
        .find(|line| !line.trim().is_empty())
        .ok_or_else(|| "ordius-helper probe emitted no outcome".to_string())?;
    serde_json::from_str(line.trim()).map_err(|e| format!("parse helper probe outcome: {e}"))
}

fn helper_probe_plan_json(def: &ResourceDefinition) -> Result<(Vec<u8>, u64), String> {
    let spec = resource_spec_v1_from_def(def)?;
    let timeout_ms = probe_timeout_ms(def);
    let plan = ProbePlanV1 {
        version: 1,
        per_resource_timeout_ms: timeout_ms,
        max_concurrency: 1,
        overall_budget_ms: timeout_ms,
        resources: vec![spec],
    };
    serde_json::to_vec(&plan)
        .map(|json| (json, timeout_ms))
        .map_err(|e| format!("serialize helper probe plan: {e}"))
}

fn build_wire_plan(plan: &ProbePlan) -> Result<ProbePlanV1, DispatchError> {
    let mut resources = Vec::with_capacity(plan.defs.len());
    for def in &plan.defs {
        let spec = resource_spec_v1_from_def(def)
            .map_err(|e| DispatchError::PlanBuild(format!("plan build: {e}")))?;
        resources.push(spec);
    }
    Ok(ProbePlanV1 {
        version: 1,
        per_resource_timeout_ms: duration_millis_u64(plan.per_resource_timeout),
        max_concurrency: u32::try_from(plan.max_concurrency).unwrap_or(u32::MAX),
        overall_budget_ms: duration_millis_u64(plan.overall_budget),
        resources,
    })
}

fn duration_millis_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn probe_failed(reason: impl Into<String>) -> ResourceProbeOutcome {
    ResourceProbeOutcome::ProbeFailed {
        reason: reason.into(),
    }
}

fn build_wsl_command(distro: &str, cmd: &ProcessCmd) -> tokio::process::Command {
    let mut c = tokio::process::Command::new("wsl.exe");
    c.arg("-d").arg(distro);
    // `cmd.cwd` is an env-side path (e.g. `/home/me/work`).  Pass it via
    // `wsl.exe --cd` so the in-distro working directory is set before `--exec`
    // — setting `current_dir` on the host-side Command would point the
    // launcher at a path that doesn't exist on Windows.
    if let Some(cwd) = cmd.cwd.as_ref() {
        c.arg("--cd").arg(cwd.as_str());
    }
    c.arg("--exec").arg(&cmd.program);
    for a in &cmd.args {
        c.arg(a);
    }
    for (k, v) in &cmd.env {
        c.env(k, v);
    }
    c
}

#[async_trait]
impl Dispatcher for WslDispatcher {
    fn info(&self) -> &EnvInfo {
        &self.info
    }

    async fn probe(
        &self,
        plan: ProbePlan,
        cancel: CancellationToken,
    ) -> Result<ProbeSummary, DispatchError> {
        if cancel.is_cancelled() {
            return Err(DispatchError::Cancelled);
        }

        // Race the bootstrap against the cancel token; otherwise a cancelled
        // probe would block until the cold helper push completes (~tens of
        // seconds in the worst case).
        let helper_result = tokio::select! {
            () = cancel.cancelled() => return Err(DispatchError::Cancelled),
            r = self.ensure_helper() => r,
        };
        match helper_result {
            Ok(helper) => {
                self.probe_plan_via_helper(&helper.env_side_path, &plan, &cancel)
                    .await
            },
            Err(err) => {
                tracing::debug!(error = %err, "falling back to WSL shell probe plan");
                let catalog = super::shell_fallback::probe_plan_shell_fallback(
                    &self.distro_name,
                    plan.env_id.clone(),
                    plan.registry_revision,
                    &plan.defs,
                    plan.overall_budget,
                )
                .await?;
                let total_probed = catalog
                    .resources
                    .values()
                    .filter(|o| !matches!(o, ResourceProbeOutcome::Skipped { .. }))
                    .count();
                Ok(ProbeSummary {
                    catalog,
                    total_probed,
                    elapsed: Duration::ZERO,
                })
            },
        }
    }

    async fn probe_resource(
        &self,
        def: &ResourceDefinition,
        cancel: CancellationToken,
    ) -> ResourceProbeOutcome {
        if cancel.is_cancelled() {
            return cancelled_probe_outcome();
        }

        let helper_result = tokio::select! {
            () = cancel.cancelled() => return cancelled_probe_outcome(),
            r = self.ensure_helper() => r,
        };
        match helper_result {
            Ok(helper) => {
                self.probe_resource_via_helper(&helper.env_side_path, def, &cancel)
                    .await
            },
            Err(err) => {
                tracing::debug!(error = %err, "falling back to WSL shell probe");
                // Shell fallback has its own internal per-route timeout but no
                // cancel handling. Race the outer cancel against it so a
                // cancelled reprobe doesn't run the full curl probe to its end.
                tokio::select! {
                    () = cancel.cancelled() => cancelled_probe_outcome(),
                    outcome = super::shell_fallback::probe_http_resource(&self.distro_name, def) => outcome,
                }
            },
        }
    }

    async fn spawn(
        &self,
        cmd: ProcessCmd,
    ) -> Result<Box<dyn crate::environment::runtime::transport::EnvProcess>, DispatchError> {
        use std::process::Stdio as StdStdio;
        let mut tokio_cmd = build_wsl_command(&self.distro_name, &cmd);
        // Only pipe stdin when bytes are actually queued; otherwise the
        // in-distro process would block on EOF (e.g. `cat` with no input).
        // Mirrors LocalDispatcher::spawn.
        tokio_cmd.stdin(if cmd.stdin.is_some() {
            StdStdio::piped()
        } else {
            StdStdio::null()
        });
        tokio_cmd.stdout(map_stdio(cmd.stdout));
        tokio_cmd.stderr(map_stdio(cmd.stderr));
        let mut sup = crate::executor::supervisor::spawn(tokio_cmd).map_err(|source| {
            DispatchError::Spawn {
                env_id: self.info.id.to_string(),
                source,
            }
        })?;
        if let Some(bytes) = cmd.stdin
            && let Some(mut child_stdin) = sup.child_mut().stdin.take()
        {
            tokio::spawn(async move {
                use tokio::io::AsyncWriteExt;
                // Best-effort write; child closing stdin early is legitimate.
                drop(child_stdin.write_all(&bytes).await);
                drop(child_stdin.shutdown().await);
            });
        }
        Ok(Box::new(LocalProcess::new(self.info.id.to_string(), sup)))
    }

    fn http_transport(&self) -> Arc<dyn HttpTransport> {
        self.transport.clone()
    }

    fn translate_path(&self, host_path: &Path) -> Result<EnvPath, DispatchError> {
        super::path::translate_path(&self.distro_name, host_path)
    }

    async fn prepare_workspace(
        &self,
        workspace_host: &Path,
        binding: &WorkspaceBinding,
        _run_id: &RunId,
    ) -> Result<WorkspaceHandle, DispatchError> {
        match binding {
            WorkspaceBinding::Translated => {
                let env_path = super::path::translate_path(&self.distro_name, workspace_host)?;
                Ok(WorkspaceHandle {
                    env_path,
                    teardown: None,
                })
            },
            WorkspaceBinding::Shared => Err(DispatchError::Unsupported(
                "WSL distros cannot use Shared binding (host filesystem differs)".into(),
            )),
            WorkspaceBinding::BindMount { .. } => Err(DispatchError::Unsupported(
                "BindMount binding is container-only".into(),
            )),
            WorkspaceBinding::Sync { .. } => Err(DispatchError::Unsupported(
                "Sync binding is SSH-only (Phase G)".into(),
            )),
            WorkspaceBinding::Unsupported => Err(DispatchError::Unsupported(
                "workspace binding declared Unsupported".into(),
            )),
        }
    }
}

fn probe_timeout_ms(def: &ResourceDefinition) -> u64 {
    match &def.probe {
        ProbeSpec::Http { timeout_ms, .. }
        | ProbeSpec::Binary { timeout_ms, .. }
        | ProbeSpec::Toolchain { timeout_ms, .. } => {
            timeout_ms.unwrap_or_else(|| duration_millis_u64(DEFAULT_PROBE_TIMEOUT))
        },
    }
}

fn helper_wait_timeout(timeout_ms: u64) -> Duration {
    if timeout_ms == 0 {
        DEFAULT_PROBE_TIMEOUT + HELPER_PROBE_GRACE
    } else {
        Duration::from_millis(timeout_ms).saturating_add(HELPER_PROBE_GRACE)
    }
}

/// Translate the runtime's `Stdio` enum to a `std::process::Stdio`.
fn map_stdio(s: ProcessStdio) -> std::process::Stdio {
    match s {
        ProcessStdio::Inherit => std::process::Stdio::inherit(),
        ProcessStdio::Piped => std::process::Stdio::piped(),
        ProcessStdio::Null => std::process::Stdio::null(),
    }
}

fn cancelled_probe_outcome() -> ResourceProbeOutcome {
    ResourceProbeOutcome::Skipped {
        reason: "cancelled".into(),
    }
}

fn resource_spec_v1_from_def(def: &ResourceDefinition) -> Result<ResourceSpecV1, String> {
    let kind = match &def.probe {
        ProbeSpec::Http { ports, routes, .. } => {
            if def.kind != ResourceKind::HttpEndpoint {
                return Err(format!(
                    "resource {} has HTTP probe but {:?} kind",
                    def.id, def.kind
                ));
            }
            ResourceKindV1::Http {
                bases: ports
                    .iter()
                    .map(|port| format!("http://127.0.0.1:{port}"))
                    .collect(),
                routes: routes
                    .iter()
                    .map(|route| HttpProbeRouteV1 {
                        path: route.path.clone(),
                        method: match route.method {
                            HttpProbeMethod::Get => HttpProbeMethodV1::Get,
                            HttpProbeMethod::Head => HttpProbeMethodV1::Head,
                        },
                        proves: route
                            .proves
                            .iter()
                            .map(|cap| capability_to_wire(*cap))
                            .collect(),
                        expect_status: Vec::new(),
                        fingerprint_jsonpaths: route.fingerprint_jsonpaths.clone(),
                    })
                    .collect(),
            }
        },
        ProbeSpec::Binary {
            bin,
            extra_search_paths,
            ..
        } => {
            if def.kind != ResourceKind::Binary {
                return Err(format!(
                    "resource {} has binary probe but {:?} kind",
                    def.id, def.kind
                ));
            }
            ResourceKindV1::Binary {
                bin: bin.clone(),
                extra_search_paths: extra_search_paths.clone(),
            }
        },
        ProbeSpec::Toolchain {
            bin,
            version_args,
            version_regex,
            extra_search_paths,
            ..
        } => {
            if def.kind != ResourceKind::Toolchain {
                return Err(format!(
                    "resource {} has toolchain probe but {:?} kind",
                    def.id, def.kind
                ));
            }
            ResourceKindV1::Toolchain {
                bin: bin.clone(),
                version_args: version_args.clone(),
                version_regex: version_regex.clone(),
                extra_search_paths: extra_search_paths.clone(),
            }
        },
    };

    Ok(ResourceSpecV1 {
        id: def.id.0.clone(),
        kind,
    })
}

fn wire_outcome_to_engine(
    body: ProbeOutcomeBodyV1,
    def: &ResourceDefinition,
    host_direct: &HashMap<ResourceId, HostDirectVerification>,
) -> ResourceProbeOutcome {
    match body {
        ProbeOutcomeBodyV1::Found(detail) => wire_detail_to_engine(detail, def, host_direct),
        ProbeOutcomeBodyV1::NotFound => ResourceProbeOutcome::NotFound,
        ProbeOutcomeBodyV1::Skipped { reason } => ResourceProbeOutcome::Skipped { reason },
        ProbeOutcomeBodyV1::ProbeFailed { reason } => ResourceProbeOutcome::ProbeFailed { reason },
        ProbeOutcomeBodyV1::TimedOut => ResourceProbeOutcome::TimedOut,
    }
}

fn wire_detail_to_engine(
    detail: ProbeDetailV1,
    def: &ResourceDefinition,
    host_direct: &HashMap<ResourceId, HostDirectVerification>,
) -> ResourceProbeOutcome {
    match detail {
        ProbeDetailV1::HttpEndpoint {
            base_url,
            proven_routes,
        } => {
            let route_origin = if host_direct.contains_key(&def.id) {
                RouteOrigin::HostDirect
            } else {
                RouteOrigin::EnvLoopback
            };
            ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
                base_url,
                routes_by_capability: routes_by_capability_from_wire(&proven_routes, def),
                version: None,
                models_list: None,
                auth_secret_ref: None,
                streaming_supported_natively: false,
                route_origin,
            })
        },
        ProbeDetailV1::Binary { path } => ResourceProbeOutcome::Found(ResourceDetail::Binary {
            path,
            version: None,
            capabilities: def.advertised_capabilities.clone(),
        }),
        ProbeDetailV1::Toolchain { path, version } => match &def.probe {
            ProbeSpec::Binary { .. } => ResourceProbeOutcome::Found(ResourceDetail::Binary {
                path,
                version: Some(version),
                capabilities: def.advertised_capabilities.clone(),
            }),
            ProbeSpec::Http { .. } | ProbeSpec::Toolchain { .. } => {
                ResourceProbeOutcome::Found(ResourceDetail::Toolchain {
                    name: def.id.0.clone(),
                    version: Some(version),
                    exe_path: path,
                })
            },
        },
    }
}

fn routes_by_capability_from_wire(
    proven_routes: &[ProvenRouteV1],
    def: &ResourceDefinition,
) -> HashMap<Capability, ProvenRoute> {
    let ProbeSpec::Http { routes, .. } = &def.probe else {
        return HashMap::new();
    };

    let mut by_capability = HashMap::new();
    for wire_route in proven_routes {
        for wire_cap in &wire_route.capabilities {
            let Some(capability) = capability_from_wire(wire_cap) else {
                continue;
            };
            let Some(route) = routes
                .iter()
                .find(|route| route.path == wire_route.path && route.proves.contains(&capability))
            else {
                tracing::debug!(
                    capability = %wire_cap,
                    path = %wire_route.path,
                    "dropping helper proven route absent from original probe definition"
                );
                continue;
            };

            by_capability
                .entry(capability)
                .or_insert_with(|| ProvenRoute {
                    path: wire_route.path.clone(),
                    method: route.method,
                    flavor: route.flavor,
                });
        }
    }
    by_capability
}

fn capability_to_wire(capability: Capability) -> String {
    match serde_json::to_value(capability).expect("capability serializes as JSON") {
        serde_json::Value::String(value) => value,
        other => unreachable!("capability serialized to non-string JSON value: {other:?}"),
    }
}

fn capability_from_wire(value: &str) -> Option<Capability> {
    serde_json::from_value(serde_json::Value::String(value.to_string()))
        .inspect_err(|err| {
            tracing::debug!(
                capability = value,
                error = %err,
                "dropping helper proven route with unknown capability"
            );
        })
        .ok()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::environment::runtime::env::{EnvId, EnvSpec, EnvState};
    use crate::environment::runtime::resource::{
        ApiFlavor, Capability, HttpProbeRoute, ResourceId,
    };
    use crate::environment::runtime::transport::EnvPath;

    fn info(distro: &str) -> EnvInfo {
        EnvInfo {
            id: EnvId::wsl(distro),
            label: format!("WSL: {distro}"),
            spec: EnvSpec::WslDistro {
                name: distro.to_string(),
                resources: vec![],
                host_direct_verifications: HashMap::default(),
            },
            state: EnvState::Reachable,
            enabled: true,
        }
    }

    // Compile-only check that `WslDispatcher` satisfies the `Dispatcher` trait.
    fn assert_dispatcher_impl(_d: &dyn Dispatcher) {}

    #[test]
    fn dispatcher_stores_distro_name() {
        let d = WslDispatcher::new(info("Ubuntu"), "Ubuntu");
        assert_eq!(d.distro_name(), "Ubuntu");
        assert_eq!(d.info().id.as_str(), "wsl:Ubuntu");
        // Exercise the trait-bound check so the helper isn't dead code.
        assert_dispatcher_impl(&d);
    }

    #[tokio::test]
    async fn translated_binding_returns_mapped_path() {
        let d = WslDispatcher::new(info("Ubuntu"), "Ubuntu");
        let host = std::path::PathBuf::from(r"C:\Users\me\runs\abc");
        let run = crate::environment::runtime::env::RunId("abc".into());
        let handle = d
            .prepare_workspace(&host, &WorkspaceBinding::Translated, &run)
            .await
            .unwrap();
        assert_eq!(handle.env_path.as_str(), "/mnt/c/Users/me/runs/abc");
        drop(handle);
    }

    #[tokio::test]
    async fn shared_binding_errors_on_wsl() {
        let d = WslDispatcher::new(info("Ubuntu"), "Ubuntu");
        let host = std::path::PathBuf::from(r"C:\Users\me\runs\abc");
        let run = crate::environment::runtime::env::RunId("abc".into());
        let err = d
            .prepare_workspace(&host, &WorkspaceBinding::Shared, &run)
            .await
            .unwrap_err();
        assert!(matches!(err, DispatchError::Unsupported(_)));
    }

    #[tokio::test]
    async fn bind_mount_binding_errors_on_wsl() {
        let d = WslDispatcher::new(info("Ubuntu"), "Ubuntu");
        let host = std::path::PathBuf::from(r"C:\x");
        let run = crate::environment::runtime::env::RunId("a".into());
        let err = d
            .prepare_workspace(
                &host,
                &WorkspaceBinding::BindMount {
                    env_path: "/work".into(),
                },
                &run,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, DispatchError::Unsupported(_)));
    }

    #[test]
    fn build_command_uses_dash_exec_flag() {
        let cmd = ProcessCmd {
            program: "/bin/ls".into(),
            args: vec!["-la".into(), "/home".into()],
            env: HashMap::new(),
            cwd: None,
            stdin: None,
            stdout: ProcessStdio::default(),
            stderr: ProcessStdio::default(),
        };
        let built = build_wsl_command("Ubuntu", &cmd);
        let dbg = format!("{built:?}");
        assert!(dbg.contains("wsl.exe"));
        assert!(dbg.contains("\"-d\""));
        assert!(dbg.contains("\"Ubuntu\""));
        assert!(dbg.contains("\"--exec\""));
        assert!(dbg.contains("\"/bin/ls\""));
        assert!(dbg.contains("\"-la\""));
        assert!(dbg.contains("\"/home\""));
    }

    #[test]
    fn build_command_preserves_arg_order() {
        let cmd = ProcessCmd {
            program: "/bin/sh".into(),
            args: vec!["-c".into(), "echo hi".into()],
            env: HashMap::new(),
            cwd: None,
            stdin: None,
            stdout: ProcessStdio::default(),
            stderr: ProcessStdio::default(),
        };
        let built = build_wsl_command("Ubuntu", &cmd);
        let dbg = format!("{built:?}");
        let sh_pos = dbg.find("\"/bin/sh\"").unwrap();
        let c_pos = dbg.find("\"-c\"").unwrap();
        let script_pos = dbg.find("\"echo hi\"").unwrap();
        assert!(sh_pos < c_pos && c_pos < script_pos);
    }

    #[test]
    fn build_command_passes_cwd_via_dash_cd() {
        let cmd = ProcessCmd {
            program: "/bin/ls".into(),
            args: vec![],
            env: HashMap::new(),
            cwd: Some(EnvPath::new("/home/me/work")),
            stdin: None,
            stdout: ProcessStdio::default(),
            stderr: ProcessStdio::default(),
        };
        let built = build_wsl_command("Ubuntu", &cmd);
        let dbg = format!("{built:?}");
        assert!(dbg.contains("\"--cd\""));
        assert!(dbg.contains("\"/home/me/work\""));
        let dash_cd_pos = dbg.find("\"--cd\"").unwrap();
        let dash_exec_pos = dbg.find("\"--exec\"").unwrap();
        assert!(dash_cd_pos < dash_exec_pos, "--cd must precede --exec");
    }

    #[tokio::test]
    async fn probe_resource_cancelled_skips_before_bootstrap() {
        let d = WslDispatcher::new(info("Ubuntu"), "Ubuntu");
        let def = http_def();
        let cancel = CancellationToken::new();
        cancel.cancel();

        let outcome = d.probe_resource(&def, cancel).await;

        assert_eq!(
            outcome,
            ResourceProbeOutcome::Skipped {
                reason: "cancelled".into()
            }
        );
    }

    #[test]
    fn resource_spec_maps_http_ports_head_and_all_capabilities() {
        let def = ResourceDefinition {
            id: ResourceId("api".into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: vec![
                Capability::OpenaiChatCompletions,
                Capability::OpenaiToolCalling,
            ],
            probe: ProbeSpec::Http {
                ports: vec![11434, 1234],
                routes: vec![HttpProbeRoute {
                    path: "/v1/models".into(),
                    method: HttpProbeMethod::Head,
                    flavor: ApiFlavor::OpenaiChat,
                    proves: vec![
                        Capability::OpenaiChatCompletions,
                        Capability::OpenaiToolCalling,
                    ],
                    models_jsonpath: None,
                    fingerprint_jsonpaths: vec!["$.version".into()],
                }],
                timeout_ms: None,
            },
            override_lower_scope: false,
        };

        let spec = resource_spec_v1_from_def(&def).expect("spec");
        let ResourceKindV1::Http { bases, routes } = spec.kind else {
            panic!("expected HTTP spec");
        };

        assert_eq!(
            bases,
            vec![
                "http://127.0.0.1:11434".to_string(),
                "http://127.0.0.1:1234".to_string(),
            ]
        );
        assert_eq!(routes.len(), 1);
        assert!(matches!(routes[0].method, HttpProbeMethodV1::Head));
        assert_eq!(
            routes[0].proves,
            vec![
                "openai_chat_completions".to_string(),
                "openai_tool_calling".to_string(),
            ]
        );
        assert!(routes[0].expect_status.is_empty());
    }

    #[test]
    fn wire_http_outcome_rebuilds_routes_by_capability() {
        let def = http_def();
        let outcome = wire_outcome_to_engine(
            ProbeOutcomeBodyV1::Found(ProbeDetailV1::HttpEndpoint {
                base_url: "http://127.0.0.1:11434".into(),
                proven_routes: vec![ProvenRouteV1 {
                    capabilities: vec!["ollama_native".into()],
                    path: "/api/version".into(),
                    status: 200,
                    fingerprint: "abc".into(),
                }],
            }),
            &def,
            &HashMap::new(),
        );

        let ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
            routes_by_capability,
            route_origin,
            streaming_supported_natively,
            ..
        }) = outcome
        else {
            panic!("expected Found HTTP outcome");
        };
        assert_eq!(route_origin, RouteOrigin::EnvLoopback);
        assert!(!streaming_supported_natively);
        assert_eq!(
            routes_by_capability.get(&Capability::OllamaNative),
            Some(&ProvenRoute {
                path: "/api/version".into(),
                method: HttpProbeMethod::Get,
                flavor: ApiFlavor::OllamaNative,
            })
        );
    }

    #[test]
    fn wire_http_outcome_expands_multi_capability_routes() {
        // OpenAI-shaped /v1/models simultaneously proves chat-completions
        // and tool-calling.  Both capabilities must show up in the engine
        // catalog, not just the first.
        let def = ResourceDefinition {
            id: ResourceId("openai-shaped".into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: vec![
                Capability::OpenaiChatCompletions,
                Capability::OpenaiToolCalling,
            ],
            probe: ProbeSpec::Http {
                ports: vec![1234],
                routes: vec![HttpProbeRoute {
                    path: "/v1/models".into(),
                    method: HttpProbeMethod::Get,
                    flavor: ApiFlavor::OpenaiChat,
                    proves: vec![
                        Capability::OpenaiChatCompletions,
                        Capability::OpenaiToolCalling,
                    ],
                    models_jsonpath: None,
                    fingerprint_jsonpaths: vec![],
                }],
                timeout_ms: None,
            },
            override_lower_scope: false,
        };

        let outcome = wire_outcome_to_engine(
            ProbeOutcomeBodyV1::Found(ProbeDetailV1::HttpEndpoint {
                base_url: "http://127.0.0.1:1234".into(),
                proven_routes: vec![ProvenRouteV1 {
                    capabilities: vec![
                        "openai_chat_completions".into(),
                        "openai_tool_calling".into(),
                    ],
                    path: "/v1/models".into(),
                    status: 200,
                    fingerprint: String::new(),
                }],
            }),
            &def,
            &HashMap::new(),
        );

        let ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
            routes_by_capability,
            ..
        }) = outcome
        else {
            panic!("expected Found HTTP outcome");
        };
        assert!(routes_by_capability.contains_key(&Capability::OpenaiChatCompletions));
        assert!(routes_by_capability.contains_key(&Capability::OpenaiToolCalling));
        assert_eq!(routes_by_capability.len(), 2);
    }

    #[test]
    fn host_direct_setter_changes_route_origin() {
        use crate::environment::runtime::env::{HostDirectMethod, HostDirectVerification};

        let d = WslDispatcher::new(info("Ubuntu"), "Ubuntu");
        let mut hd = HashMap::new();
        hd.insert(
            ResourceId("ollama".into()),
            HostDirectVerification {
                verified_at: chrono::Utc::now(),
                method: HostDirectMethod::WslMirroredNetworking,
                host_url: "http://127.0.0.1:11434".into(),
                probe_route_path: "/api/version".into(),
                stable_fingerprint: "abc".into(),
                recompute_jsonpaths: vec!["$.version".into()],
            },
        );

        d.set_host_direct(hd);

        let snap = d.host_direct_snapshot();
        assert!(snap.contains_key(&ResourceId("ollama".into())));
    }

    #[test]
    fn set_host_direct_visible_through_cached_http_transport() {
        // The dispatcher caches `Arc<WslHttpTransport>` and shares the
        // `Arc<ArcSwap<HostDirectMap>>` with it. A `set_host_direct` update
        // must be observable to the already-handed-out transport — otherwise
        // requests post-update would still route through env-loopback.
        use crate::environment::runtime::env::{HostDirectMethod, HostDirectVerification};
        use url::Url;

        let d = WslDispatcher::new(info("Ubuntu"), "Ubuntu");
        let transport = d.http_transport();
        // Pre-update: nothing in host_direct → loopback URLs are NOT streamable
        // (env-loopback can't stream).
        let url = Url::parse("http://127.0.0.1:11434/v1/chat/completions").unwrap();
        assert!(!transport.can_stream(&url));

        let mut hd = HashMap::new();
        hd.insert(
            ResourceId("ollama".into()),
            HostDirectVerification {
                verified_at: chrono::Utc::now(),
                method: HostDirectMethod::WslMirroredNetworking,
                host_url: "http://127.0.0.1:11434".into(),
                probe_route_path: "/api/version".into(),
                stable_fingerprint: "abc".into(),
                recompute_jsonpaths: vec!["$.version".into()],
            },
        );
        d.set_host_direct(hd);

        // Post-update: same handed-out transport now classifies the URL as
        // HostDirect and CAN stream.
        assert!(transport.can_stream(&url));
    }

    #[test]
    fn build_wire_plan_kind_mismatch_returns_plan_build_error() {
        // `resource_spec_v1_from_def` rejects definitions whose declared `kind`
        // and `probe` disagree; the surrounding `build_wire_plan` wraps that
        // failure as `DispatchError::PlanBuild`. Phase E's scheduler will rely
        // on this discrimination to route plan-build failures distinctly from
        // path-translation failures.
        let mismatched = ResourceDefinition {
            id: ResourceId("oops".into()),
            kind: ResourceKind::Binary,
            advertised_capabilities: vec![],
            probe: ProbeSpec::Http {
                ports: vec![11434],
                routes: vec![],
                timeout_ms: None,
            },
            override_lower_scope: false,
        };
        let plan = ProbePlan {
            env_id: crate::environment::runtime::env::EnvId::wsl("Ubuntu"),
            registry_revision: 0,
            defs: vec![mismatched],
            per_resource_timeout: Duration::from_secs(1),
            max_concurrency: 1,
            overall_budget: Duration::from_secs(5),
        };
        let err = build_wire_plan(&plan).unwrap_err();
        assert!(matches!(err, DispatchError::PlanBuild(_)), "got {err:?}");
    }

    #[test]
    fn wire_detail_to_engine_tags_host_direct_when_verified() {
        use crate::environment::runtime::env::{HostDirectMethod, HostDirectVerification};

        let def = http_def();
        let mut host_direct = HashMap::new();
        host_direct.insert(
            ResourceId("ollama".into()),
            HostDirectVerification {
                verified_at: chrono::Utc::now(),
                method: HostDirectMethod::WslMirroredNetworking,
                host_url: "http://127.0.0.1:11434".into(),
                probe_route_path: "/api/version".into(),
                stable_fingerprint: "abc".into(),
                recompute_jsonpaths: vec!["$.version".into()],
            },
        );

        let outcome = wire_detail_to_engine(
            ProbeDetailV1::HttpEndpoint {
                base_url: "http://127.0.0.1:11434".into(),
                proven_routes: vec![],
            },
            &def,
            &host_direct,
        );

        let ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint { route_origin, .. }) =
            outcome
        else {
            panic!("expected Found HTTP outcome");
        };
        assert_eq!(route_origin, RouteOrigin::HostDirect);
    }

    #[test]
    fn wire_http_outcome_drops_unknown_capability() {
        let def = http_def();
        let outcome = wire_outcome_to_engine(
            ProbeOutcomeBodyV1::Found(ProbeDetailV1::HttpEndpoint {
                base_url: "http://127.0.0.1:11434".into(),
                proven_routes: vec![ProvenRouteV1 {
                    capabilities: vec!["not_a_capability".into()],
                    path: "/api/version".into(),
                    status: 200,
                    fingerprint: "abc".into(),
                }],
            }),
            &def,
            &HashMap::new(),
        );

        let ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
            routes_by_capability,
            ..
        }) = outcome
        else {
            panic!("expected Found HTTP outcome");
        };
        assert!(routes_by_capability.is_empty());
    }

    fn http_def() -> ResourceDefinition {
        ResourceDefinition {
            id: ResourceId("ollama".into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: vec![Capability::OllamaNative],
            probe: ProbeSpec::Http {
                ports: vec![11434],
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
        }
    }
}
