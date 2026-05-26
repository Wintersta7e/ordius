//! Workflow loader. JSON is the canonical format; YAML is accepted on
//! read so users who prefer authoring in YAML aren't forced into JSON.

use std::path::Path;

use thiserror::Error;

use crate::types::Workflow;

/// Failure modes for [`load_workflow`].
#[derive(Debug, Error)]
pub enum LoadError {
    /// Filesystem read failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// JSON parse failed.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// YAML parse failed.
    #[error("yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// File extension is not one of `json` / `yaml` / `yml`.
    #[error("unsupported extension: {0}")]
    BadExt(String),
    /// The workflow contains a node whose `type` is a retired id that
    /// has been renamed. The loader surfaces the rename target so users
    /// see the hint at every user-facing entry point (CLI, IPC).
    #[error("workflow node '{node_id}' uses reserved type id '{id}'; rename to '{replacement}'")]
    ReservedNodeType {
        /// Retired node-type id that appeared in the workflow.
        id: String,
        /// Current node-type id the user should switch to.
        replacement: String,
        /// Id of the offending node inside the workflow.
        node_id: String,
    },
}

/// Old node-type ids that workflow JSON files might still reference.
/// Each entry maps the deprecated id to its rename target. The checked
/// loader rejects workflows containing any of these ids with an explicit
/// [`LoadError::ReservedNodeType`] so users see the rename hint instead
/// of a generic "unknown type" error downstream.
pub(crate) const RESERVED_NODE_TYPE_IDS: &[(&str, &str)] =
    &[("agent", "llm"), ("container", "docker-run")];

/// Reject any node that references a retired node-type id.
///
/// Called from [`load_workflow`] right after deserialisation so callers
/// get the rename hint before any downstream processing (scope install,
/// validation) runs.
///
/// Public so non-loader entry points (CLI `import`, anything that
/// deserialises workflow bytes directly via `serde_json` / `serde_yaml`)
/// can apply the same check before persisting or running the workflow.
pub fn reject_reserved_node_types(wf: &Workflow) -> Result<(), LoadError> {
    for node in &wf.nodes {
        if let Some((_, replacement)) = RESERVED_NODE_TYPE_IDS
            .iter()
            .find(|(old, _)| *old == node.ty)
        {
            return Err(LoadError::ReservedNodeType {
                id: node.ty.clone(),
                replacement: (*replacement).to_string(),
                node_id: node.id.clone(),
            });
        }
    }
    Ok(())
}

/// Read a workflow from disk and reject retired node-type ids.
///
/// The file extension selects the parser: `.json` → JSON;
/// `.yaml` / `.yml` → YAML. Any other extension (or no extension)
/// returns [`LoadError::BadExt`] rather than silently guessing.
///
/// After parsing, the workflow is walked for nodes whose `type` matches
/// a retired id (see [`RESERVED_NODE_TYPE_IDS`]). Any match returns
/// [`LoadError::ReservedNodeType`] with the rename target so every
/// user-facing entry point surfaces the hint.
///
/// Callers that need the raw deserialised workflow without the
/// reserved-id check (engine seeds, replay paths) should use
/// [`load_workflow_unchecked`].
pub fn load_workflow<P: AsRef<Path>>(path: P) -> Result<Workflow, LoadError> {
    let wf = load_workflow_unchecked(path)?;
    reject_reserved_node_types(&wf)?;
    Ok(wf)
}

/// Read a workflow from disk without running the reserved-id check.
///
/// Intentionally narrow surface: this is the building block for
/// [`load_workflow`] and for engine-internal paths (seed installation,
/// run-event replay) that already know they're reading trusted bytes.
/// User-facing paths must always use [`load_workflow`] so the rename
/// hint surfaces uniformly.
pub fn load_workflow_unchecked<P: AsRef<Path>>(path: P) -> Result<Workflow, LoadError> {
    let path = path.as_ref();
    let bytes = std::fs::read(path)?;
    let ext = path
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or("");
    match ext {
        "json" => Ok(serde_json::from_slice(&bytes)?),
        "yaml" | "yml" => Ok(serde_yaml::from_slice(&bytes)?),
        other => Err(LoadError::BadExt(other.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use tempfile::NamedTempFile;

    use super::*;

    fn write_named(content: &str, suffix: &str) -> NamedTempFile {
        let mut f = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f
    }

    #[test]
    fn loads_json() {
        let f = write_named(r#"{"id":"a","name":"b"}"#, ".json");
        let w = load_workflow(f.path()).unwrap();
        assert_eq!(w.id, "a");
        assert_eq!(w.name, "b");
    }

    #[test]
    fn loads_yaml() {
        let f = write_named("id: c\nname: d\n", ".yaml");
        let w = load_workflow(f.path()).unwrap();
        assert_eq!(w.id, "c");
        assert_eq!(w.name, "d");
    }

    #[test]
    fn loads_yml_extension() {
        let f = write_named("id: e\nname: f\n", ".yml");
        let w = load_workflow(f.path()).unwrap();
        assert_eq!(w.id, "e");
    }

    #[test]
    fn rejects_unknown_ext() {
        let f = write_named("nothing", ".toml");
        assert!(matches!(load_workflow(f.path()), Err(LoadError::BadExt(_))));
    }

    #[test]
    fn rejects_missing_extension() {
        // NamedTempFile with no suffix has no extension.
        let f = NamedTempFile::new().unwrap();
        assert!(matches!(load_workflow(f.path()), Err(LoadError::BadExt(_))));
    }

    #[test]
    fn checked_load_rejects_reserved_agent_type() {
        // Every user-facing entry point ultimately funnels through
        // `load_workflow`. A workflow using the retired `agent` id must
        // surface the rename hint rather than a generic unknown-type
        // error downstream.
        let f = write_named(
            r#"{
                "id":"retired",
                "name":"x",
                "nodes":[{"id":"n1","type":"agent","name":"x","config":{}}],
                "edges":[]
            }"#,
            ".json",
        );
        match load_workflow(f.path()) {
            Err(LoadError::ReservedNodeType {
                id,
                replacement,
                node_id,
            }) => {
                assert_eq!(id, "agent");
                assert_eq!(replacement, "llm");
                assert_eq!(node_id, "n1");
            },
            other => panic!("expected ReservedNodeType, got {other:?}"),
        }
    }

    #[test]
    fn checked_load_rejects_reserved_container_type() {
        let f = write_named(
            r#"{
                "id":"retired",
                "name":"x",
                "nodes":[{"id":"c","type":"container","name":"x","config":{}}],
                "edges":[]
            }"#,
            ".json",
        );
        match load_workflow(f.path()) {
            Err(LoadError::ReservedNodeType {
                id,
                replacement,
                node_id,
            }) => {
                assert_eq!(id, "container");
                assert_eq!(replacement, "docker-run");
                assert_eq!(node_id, "c");
            },
            other => panic!("expected ReservedNodeType, got {other:?}"),
        }
    }

    #[test]
    fn unchecked_load_skips_reserved_check() {
        // The unchecked variant is for engine-internal seeds + replay
        // paths and must NOT reject retired ids.
        let f = write_named(
            r#"{
                "id":"retired",
                "name":"x",
                "nodes":[{"id":"n1","type":"agent","name":"x","config":{}}],
                "edges":[]
            }"#,
            ".json",
        );
        let wf = load_workflow_unchecked(f.path()).unwrap();
        assert_eq!(wf.nodes[0].ty, "agent");
    }
}
