//! Custom node-type manifest loader. Spec:
//! `docs/03-node-types.md` "JSON / YAML manifest format".
//!
//! Reads every `.json`/`.yaml`/`.yml` file in a directory, parses
//! each as a [`NodeType`], runs `v1.0`-scoped validation, and
//! registers it in the provided [`Registry`]. Errors are
//! accumulated rather than fatal so a single broken manifest
//! doesn't black-hole the whole palette — the engine logs the
//! issues and keeps the good entries.

use crate::registry::Registry;
use crate::types::{ExecutionBackend, NodeType, OutputParse};
use std::path::Path;
use thiserror::Error;

/// Per-file failures surfaced by [`load_into`]. Each variant
/// carries the offending path (display-formatted) so callers can
/// surface them in the GUI palette later.
#[derive(Debug, Error)]
pub enum ManifestError {
    /// Filesystem-level read or directory iteration failure.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// JSON or YAML parse failure.
    #[error("parse {path}: {err}")]
    Parse {
        /// Display-formatted manifest path.
        path: String,
        /// Underlying parser message.
        err: String,
    },
    /// Manifest parsed but failed `validate_manifest`.
    #[error("validation {path}: {err}")]
    Validation {
        /// Display-formatted manifest path.
        path: String,
        /// Why the manifest was rejected.
        err: String,
    },
}

/// Read every manifest file under `dir` and register each valid one.
///
/// Files with bad parse or validation results are skipped with an
/// entry in the returned error vector; existing built-in
/// registrations are not overwritten — a manifest with a duplicate
/// id reports a validation error and the built-in stays in place.
/// `dir` not existing is not an error — fresh installs have no
/// `node-types/` until the user creates one.
pub fn load_into<P: AsRef<Path>>(registry: &mut Registry, dir: P) -> Vec<ManifestError> {
    let mut errs = Vec::new();
    let dir = dir.as_ref();
    if !dir.exists() {
        return errs;
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            errs.push(ManifestError::Io(e));
            return errs;
        },
    };
    // Sort so error / load order is deterministic across runs —
    // matters for duplicate-id reporting (first-wins).
    let mut paths: Vec<_> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .and_then(std::ffi::OsStr::to_str)
                .is_some_and(|e| matches!(e, "json" | "yaml" | "yml"))
        })
        .collect();
    paths.sort();

    for path in paths {
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) => {
                errs.push(ManifestError::Io(e));
                continue;
            },
        };
        let is_json = path
            .extension()
            .and_then(std::ffi::OsStr::to_str)
            .is_some_and(|e| e.eq_ignore_ascii_case("json"));
        let parsed: Result<NodeType, String> = if is_json {
            serde_json::from_slice(&bytes).map_err(|e| e.to_string())
        } else {
            serde_yaml::from_slice(&bytes).map_err(|e| e.to_string())
        };
        let nt = match parsed {
            Ok(nt) => nt,
            Err(err) => {
                errs.push(ManifestError::Parse {
                    path: path.display().to_string(),
                    err,
                });
                continue;
            },
        };
        if let Err(err) = validate_manifest(&nt) {
            errs.push(ManifestError::Validation {
                path: path.display().to_string(),
                err,
            });
            continue;
        }
        if registry.get(&nt.id).is_some() {
            errs.push(ManifestError::Validation {
                path: path.display().to_string(),
                err: format!("duplicate id '{}' — already registered", nt.id),
            });
            continue;
        }
        registry.register(nt);
    }
    errs
}

/// `v1.0`-scoped validation for a parsed manifest.
///
/// Enforces id non-empty, `Subprocess` backend, non-empty command,
/// supported `output_parse`, and `JSONPath`-shaped `output_map`
/// expressions. The full template-reference cross-check (verifying
/// every `{{...}}` in `command` / `stdin_template` / `env` resolves
/// to a declared input/config/secret/run-context name) is deferred
/// to v1.1.
pub fn validate_manifest(nt: &NodeType) -> Result<(), String> {
    if nt.id.is_empty() {
        return Err("id empty".into());
    }
    if nt.execution.backend != ExecutionBackend::Subprocess {
        return Err(format!(
            "v1.0 manifests must use backend: subprocess (got {:?})",
            nt.execution.backend
        ));
    }
    if nt.execution.command.is_empty() {
        return Err("execution.command must be non-empty for subprocess backend".into());
    }
    if !matches!(
        nt.execution.output_parse,
        OutputParse::Text | OutputParse::Json
    ) {
        return Err(format!(
            "unsupported output_parse: {:?}",
            nt.execution.output_parse
        ));
    }
    for (port, expr) in &nt.execution.output_map {
        if !expr.starts_with('$') {
            return Err(format!(
                "output_map[{port}]='{expr}' is not JSONPath (must start with $)"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
