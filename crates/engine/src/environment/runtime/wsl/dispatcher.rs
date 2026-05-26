//! `WslDispatcher` — `Dispatcher` impl for a Windows Subsystem for Linux distro.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::environment::runtime::catalog::ResourceProbeOutcome;
use crate::environment::runtime::dispatcher::{Dispatcher, HttpTransport};
use crate::environment::runtime::env::{EnvInfo, RunId, WorkspaceBinding};
use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::plan::{ProbePlan, ProbeSummary};
use crate::environment::runtime::resource::ResourceDefinition;
use crate::environment::runtime::transport::{EnvPath, ProcessCmd, WorkspaceHandle};
use crate::executor::supervisor::Supervised;

/// `Dispatcher` implementation backed by a named WSL distribution.
#[derive(Debug, Clone)]
pub struct WslDispatcher {
    info: EnvInfo,
    distro_name: String,
}

impl WslDispatcher {
    /// Build a `WslDispatcher` for the given environment metadata and distro name.
    pub fn new(info: EnvInfo, distro_name: impl Into<String>) -> Self {
        Self {
            info,
            distro_name: distro_name.into(),
        }
    }

    /// Return the WSL distribution name passed to `wsl.exe -d <name>`.
    pub fn distro_name(&self) -> &str {
        &self.distro_name
    }
}

#[async_trait]
impl Dispatcher for WslDispatcher {
    fn info(&self) -> &EnvInfo {
        &self.info
    }

    async fn probe(
        &self,
        _plan: ProbePlan,
        _cancel: CancellationToken,
    ) -> Result<ProbeSummary, DispatchError> {
        Err(DispatchError::NotImplemented(
            "WslDispatcher::probe pending T18".into(),
        ))
    }

    async fn probe_resource(
        &self,
        _def: &ResourceDefinition,
        _cancel: CancellationToken,
    ) -> ResourceProbeOutcome {
        ResourceProbeOutcome::Skipped {
            reason: "WslDispatcher::probe_resource pending T17".into(),
        }
    }

    fn spawn(&self, _cmd: ProcessCmd) -> std::io::Result<Supervised> {
        Err(std::io::Error::other("WslDispatcher::spawn pending T12"))
    }

    fn http_transport(&self) -> Arc<dyn HttpTransport> {
        panic!("WslDispatcher::http_transport pending T15")
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::environment::runtime::env::{EnvId, EnvSpec, EnvState};

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
}
