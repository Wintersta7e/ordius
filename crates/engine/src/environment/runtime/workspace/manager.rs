/// Workspace manager skeleton — H1 scope only.
///
/// H1 contract: `resolve_cwd` delegates to `dispatcher.translate_path` for all
/// bindings, so behaviour is identical to the pre-H1 inline call sites.
/// Transfer / manifest / teardown logic arrives in later phases.
use std::path::Path;

use crate::environment::runtime::dispatcher::Dispatcher;
use crate::environment::runtime::env::WorkspaceBinding;
use crate::environment::runtime::error::DispatchError;
use crate::environment::runtime::transport::EnvPath;

/// Run-tree-scoped owner of workspace sync policy.
///
/// H1: only resolves the env-side cwd by delegating to `translate_path`.
/// Transfer / manifest / teardown arrive in later phases.
#[derive(Debug, Default)]
pub struct WorkspaceManager {
    // Later phases: per-(EnvId, host_ws) PreparedWorkspace map + lease semaphores.
}

impl WorkspaceManager {
    /// Construct a new `WorkspaceManager`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolve the working directory inside the target env.
    ///
    /// H1 contract: identical to `dispatcher.translate_path(host_ws)` for all
    /// bindings. Later phases will branch on `binding` to drive sync/mount.
    // `async` is required by the public contract; real awaits arrive in later phases.
    #[allow(clippy::unused_async)]
    pub async fn resolve_cwd(
        &self,
        dispatcher: &dyn Dispatcher,
        _binding: &WorkspaceBinding,
        host_ws: &Path,
    ) -> Result<EnvPath, DispatchError> {
        dispatcher.translate_path(host_ws)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::env::{EnvId, EnvInfo, EnvSpec, EnvState, WorkspaceBinding};
    use crate::environment::runtime::local::LocalDispatcher;
    use std::collections::HashMap;
    use std::path::Path;

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

    #[tokio::test]
    async fn resolve_cwd_shared_delegates_to_translate_path() {
        let d = LocalDispatcher::new(local_info());
        let mgr = WorkspaceManager::new();
        let cwd = mgr
            .resolve_cwd(&d, &WorkspaceBinding::Shared, Path::new("/workspaces/wf"))
            .await
            .expect("ok");
        assert_eq!(cwd.as_str(), "/workspaces/wf");
    }
}
