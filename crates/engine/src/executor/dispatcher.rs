//! Top-level executor that fans a node out to the right backend.
//!
//! `InProcess` → [`super::InProcessExecutor`] (delay / transform /
//! condition / http / llm / file / checkpoint).
//! `Subprocess` → [`super::SubprocessExecutor`] (shell + future
//! manifest-defined subprocess nodes).
//! `Container` → `NodeError::NotImplemented` until the container
//! backend ships in a later release.

use crate::executor::{
    ContainerExecutor, InProcessExecutor, NodeError, NodeExecutor, NodeOutputs, RunContext,
    SubprocessExecutor,
};
use crate::types::{ExecutionBackend, Node, NodeType};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

/// Routes dispatch by `ExecutionSpec::backend`.
pub struct Dispatcher {
    in_process: InProcessExecutor,
    subprocess: SubprocessExecutor,
    container: ContainerExecutor,
}

impl Dispatcher {
    /// Build a dispatcher with all backends wired.
    #[must_use]
    pub fn new() -> Self {
        Self {
            in_process: InProcessExecutor::new(),
            subprocess: SubprocessExecutor,
            container: ContainerExecutor,
        }
    }
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NodeExecutor for Dispatcher {
    fn supports(&self, _nt: &NodeType) -> bool {
        true
    }

    async fn run(
        &self,
        node: &Node,
        nt: &NodeType,
        ctx: &RunContext,
        cancel: CancellationToken,
    ) -> Result<NodeOutputs, NodeError> {
        match nt.execution.backend {
            ExecutionBackend::InProcess => self.in_process.run(node, nt, ctx, cancel).await,
            ExecutionBackend::Subprocess => self.subprocess.run(node, nt, ctx, cancel).await,
            ExecutionBackend::Container => self.container.run(node, nt, ctx, cancel).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::test_support::{make_ctx, subprocess_node_type, trivial_subprocess_node};
    use crate::types::{Category, ExecutionSpec, Node, OutputParse, Pos};
    use std::collections::HashMap;

    fn dummy_inprocess_nt() -> NodeType {
        NodeType {
            id: "delay".into(),
            name: String::new(),
            category: Category::Control,
            tags: vec![],
            icon: String::new(),
            description: String::new(),
            inputs: vec![],
            outputs: vec![],
            config: vec![],
            execution: ExecutionSpec {
                backend: ExecutionBackend::InProcess,
                command: vec![],
                stdin_template: None,
                env: HashMap::new(),
                timeout_ms: None,
                output_parse: OutputParse::Text,
                output_map: HashMap::new(),
            },
            skip_config_templates: false,
        }
    }

    fn container_nt() -> NodeType {
        NodeType {
            id: "container_stub".into(),
            name: String::new(),
            category: Category::Execution,
            tags: vec![],
            icon: String::new(),
            description: String::new(),
            inputs: vec![],
            outputs: vec![],
            config: vec![],
            execution: ExecutionSpec {
                backend: ExecutionBackend::Container,
                command: vec![],
                stdin_template: None,
                env: HashMap::new(),
                timeout_ms: None,
                output_parse: OutputParse::Text,
                output_map: HashMap::new(),
            },
            skip_config_templates: false,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dispatches_inprocess_to_delay() {
        let (ctx, _rx, _dir) = make_ctx();
        let nt = dummy_inprocess_nt();
        let node = Node {
            id: "n".into(),
            ty: "delay".into(),
            name: String::new(),
            config: HashMap::from([("ms".into(), serde_json::json!(1))]),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
        };
        let out = Dispatcher::new()
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect("delay should succeed");
        assert!(out.is_empty());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread")]
    async fn dispatches_subprocess_to_shell() {
        let (ctx, _rx, _dir) = make_ctx();
        let nt = subprocess_node_type(vec!["true".into()]);
        let node = trivial_subprocess_node();
        let out = Dispatcher::new()
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect("true should succeed");
        assert!(out.contains_key("exit_code"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn container_backend_requires_image_config() {
        let (ctx, _rx, _dir) = make_ctx();
        let nt = container_nt();
        let node = Node {
            id: "n".into(),
            ty: "container_stub".into(),
            name: String::new(),
            config: HashMap::new(),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
        };
        // No `image` config → Config error from the executor itself
        // (we don't reach docker, so this works without a daemon).
        let err = Dispatcher::new()
            .run(&node, &nt, &ctx, CancellationToken::new())
            .await
            .expect_err("missing image is a config error");
        assert!(matches!(err, NodeError::Config(_)), "got {err:?}");
    }
}
