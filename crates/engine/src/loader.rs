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
}

/// Read a workflow from disk.
///
/// The file extension selects the parser: `.json` → JSON;
/// `.yaml` / `.yml` → YAML. Any other extension (or no extension)
/// returns [`LoadError::BadExt`] rather than silently guessing.
pub fn load_workflow<P: AsRef<Path>>(path: P) -> Result<Workflow, LoadError> {
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
}
