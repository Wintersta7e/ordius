//! User-global resources TOML file loader: `<home>/resources.toml`.
//!
//! The schema is one `[[resource]]` array of `ResourceDefinition` records.
//! Missing file is fine — fresh installs have no user-global overrides.
//! Malformed file is an error so the user finds out at boot, not at
//! dispatch.

use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::registry::{ResourceRegistry, ScopeKey};
use super::resource::ResourceDefinition;

/// On-disk representation of `<home>/resources.toml`.
///
/// Wraps a single `resource:` array; future extensions (e.g. user-global
/// `HostDirect` verifications) can land as additional named tables without
/// breaking the wire shape.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserResourcesFile {
    /// All user-global resource definitions in declaration order.
    #[serde(default, rename = "resource")]
    pub resources: Vec<ResourceDefinition>,
}

/// Failure modes for [`load_user_resources`].
#[derive(Debug, Error)]
pub enum ResourcesFileError {
    /// IO failure reading the file (other than `NotFound`, which is silently ok).
    #[error("io reading {path}: {source}")]
    Io {
        /// Display-formatted path.
        path: String,
        /// Underlying `io::Error`.
        #[source]
        source: std::io::Error,
    },
    /// TOML parse failure.
    #[error("parse {path}: {source}")]
    Parse {
        /// Display-formatted path.
        path: String,
        /// `toml::de::Error`.
        #[source]
        source: toml::de::Error,
    },
    /// Registry rejected the upsert (e.g. duplicate id without
    /// `override_lower_scope` against a built-in).
    #[error("registry rejected {id}: {source}")]
    Registry {
        /// Resource id that failed.
        id: String,
        /// Underlying registry error.
        #[source]
        source: super::error::RegistryError,
    },
}

/// Read `<home>/resources.toml`, parse it, and upsert each entry under
/// [`ScopeKey::UserGlobal`].
///
/// Returns the number of definitions installed. A missing file is *not*
/// an error — it is logged at `debug` and we return `Ok(0)`. Parse errors
/// and registry-rejection errors stop the load and bubble back to the caller.
pub fn load_user_resources(
    home: &Path,
    registry: &ResourceRegistry,
) -> Result<usize, ResourcesFileError> {
    let path = home.join("resources.toml");
    let text = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tracing::debug!(path = %path.display(), "no resources.toml; skipping");
            return Ok(0);
        },
        Err(e) => {
            return Err(ResourcesFileError::Io {
                path: path.display().to_string(),
                source: e,
            });
        },
    };

    let file: UserResourcesFile = toml::from_str(&text).map_err(|e| ResourcesFileError::Parse {
        path: path.display().to_string(),
        source: e,
    })?;

    let mut written = 0_usize;
    for def in &file.resources {
        registry
            .upsert(&ScopeKey::UserGlobal, def)
            .map_err(|e| ResourcesFileError::Registry {
                id: def.id.0.clone(),
                source: e,
            })?;
        written += 1;
    }
    tracing::info!(
        path = %path.display(),
        count = written,
        "loaded user-global resources"
    );
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::env::EnvId;
    use crate::environment::runtime::resource::{
        ApiFlavor, Capability, HttpProbeMethod, ProbeSpec, ResourceId, ResourceKind,
    };
    use tempfile::TempDir;

    fn write_toml(home: &Path, body: &str) {
        std::fs::write(home.join("resources.toml"), body).expect("write toml");
    }

    #[test]
    fn parse_minimal_roundtrip() {
        let toml_src = r#"
[[resource]]
id = "my-llm"
override_lower_scope = false
kind = "http_endpoint"
advertised_capabilities = ["openai_chat_completions"]

[resource.probe]
kind = "http"
ports = [9999]

[[resource.probe.routes]]
path = "/v1/models"
method = "get"
flavor = "openai_chat"
proves = ["openai_chat_completions"]
fingerprint_jsonpaths = ["$.version"]
"#;
        let file: UserResourcesFile = toml::from_str(toml_src).expect("parse");
        assert_eq!(file.resources.len(), 1);
        let r = &file.resources[0];
        assert_eq!(r.id, ResourceId("my-llm".into()));
        assert_eq!(r.kind, ResourceKind::HttpEndpoint);
        assert!(
            r.advertised_capabilities
                .contains(&Capability::OpenaiChatCompletions)
        );
        let ProbeSpec::Http { ports, routes, .. } = &r.probe else {
            panic!("http probe")
        };
        assert_eq!(ports, &vec![9999u16]);
        assert_eq!(routes.len(), 1);
        let route = &routes[0];
        assert_eq!(route.path, "/v1/models");
        assert_eq!(route.method, HttpProbeMethod::Get);
        assert_eq!(route.flavor, ApiFlavor::OpenaiChat);
        assert_eq!(route.proves, vec![Capability::OpenaiChatCompletions]);
        assert_eq!(route.fingerprint_jsonpaths, vec!["$.version".to_string()]);
    }

    #[test]
    fn parse_multiple_resources() {
        let toml_src = r#"
[[resource]]
id = "first"
kind = "http_endpoint"
advertised_capabilities = []
[resource.probe]
kind = "http"
ports = [1]
routes = []

[[resource]]
id = "second"
kind = "http_endpoint"
advertised_capabilities = []
[resource.probe]
kind = "http"
ports = [2]
routes = []
"#;
        let file: UserResourcesFile = toml::from_str(toml_src).expect("parse");
        assert_eq!(file.resources.len(), 2);
        assert_eq!(file.resources[0].id.0, "first");
        assert_eq!(file.resources[1].id.0, "second");
    }

    #[test]
    fn missing_file_returns_zero() {
        let tmp = TempDir::new().unwrap();
        let reg = ResourceRegistry::new();
        let written = load_user_resources(tmp.path(), &reg).expect("ok");
        assert_eq!(written, 0);
        assert_eq!(reg.snapshot().revision, 0);
    }

    #[test]
    fn malformed_file_errors() {
        let tmp = TempDir::new().unwrap();
        write_toml(tmp.path(), "this is not valid toml = [[[");
        let reg = ResourceRegistry::new();
        let err = load_user_resources(tmp.path(), &reg).expect_err("parse fail");
        assert!(matches!(err, ResourcesFileError::Parse { .. }));
    }

    #[test]
    fn parses_without_optional_arrays() {
        // `advertised_capabilities` and the HTTP `routes` array now default
        // to empty, so a port-only liveness probe is the smallest valid
        // user-authored TOML entry.
        let toml_src = r#"
[[resource]]
id = "ping-only"
kind = "http_endpoint"

[resource.probe]
kind = "http"
ports = [8080]
"#;
        let file: UserResourcesFile = toml::from_str(toml_src).expect("parse");
        assert_eq!(file.resources.len(), 1);
        let r = &file.resources[0];
        assert!(r.advertised_capabilities.is_empty());
        let ProbeSpec::Http { routes, ports, .. } = &r.probe else {
            panic!("http probe")
        };
        assert_eq!(ports, &vec![8080u16]);
        assert!(routes.is_empty());
    }

    #[test]
    fn valid_file_upserts_under_user_global_scope() {
        let tmp = TempDir::new().unwrap();
        let body = r#"
[[resource]]
id = "site-llm"
override_lower_scope = false
kind = "http_endpoint"
advertised_capabilities = []
[resource.probe]
kind = "http"
ports = [9090]
routes = []
"#;
        write_toml(tmp.path(), body);

        let reg = ResourceRegistry::new();
        let written = load_user_resources(tmp.path(), &reg).expect("ok");
        assert_eq!(written, 1);

        let snap = reg.snapshot();
        let layer = snap.layers.get(&ScopeKey::UserGlobal).expect("user layer");
        assert!(layer.contains_key(&ResourceId("site-llm".into())));

        let (def, scope) = snap
            .resolve(&ResourceId("site-llm".into()), &EnvId::local(), None)
            .expect("resolved");
        assert_eq!(scope, ScopeKey::UserGlobal);
        assert_eq!(def.id.0, "site-llm");
    }

    #[test]
    fn shadowing_builtin_without_override_errors() {
        // First seed a builtin look-alike at builtin scope, then write a
        // user-global file that tries to shadow it WITHOUT override_lower_scope.
        let tmp = TempDir::new().unwrap();
        let reg = ResourceRegistry::new();
        reg.upsert(
            &ScopeKey::Builtin,
            &ResourceDefinition {
                id: ResourceId("ollama".into()),
                kind: ResourceKind::HttpEndpoint,
                advertised_capabilities: vec![],
                probe: ProbeSpec::Http {
                    ports: vec![11434],
                    routes: vec![],
                    timeout_ms: None,
                },
                override_lower_scope: false,
            },
        )
        .unwrap();

        write_toml(
            tmp.path(),
            r#"
[[resource]]
id = "ollama"
kind = "http_endpoint"
advertised_capabilities = []
[resource.probe]
kind = "http"
ports = [9999]
routes = []
"#,
        );
        let err = load_user_resources(tmp.path(), &reg).expect_err("collision");
        assert!(matches!(err, ResourcesFileError::Registry { .. }));
    }

    #[test]
    fn shadowing_builtin_with_override_succeeds() {
        let tmp = TempDir::new().unwrap();
        let reg = ResourceRegistry::new();
        reg.upsert(
            &ScopeKey::Builtin,
            &ResourceDefinition {
                id: ResourceId("ollama".into()),
                kind: ResourceKind::HttpEndpoint,
                advertised_capabilities: vec![],
                probe: ProbeSpec::Http {
                    ports: vec![11434],
                    routes: vec![],
                    timeout_ms: None,
                },
                override_lower_scope: false,
            },
        )
        .unwrap();

        write_toml(
            tmp.path(),
            r#"
[[resource]]
id = "ollama"
override_lower_scope = true
kind = "http_endpoint"
advertised_capabilities = []
[resource.probe]
kind = "http"
ports = [9999]

[[resource.probe.routes]]
path = "/v1/models"
method = "get"
flavor = "openai_chat"
proves = ["openai_chat_completions"]
"#,
        );
        let written = load_user_resources(tmp.path(), &reg).expect("override ok");
        assert_eq!(written, 1);

        let snap = reg.snapshot();
        let (def, scope) = snap
            .resolve(&ResourceId("ollama".into()), &EnvId::local(), None)
            .expect("resolved");
        assert_eq!(scope, ScopeKey::UserGlobal);
        assert!(def.override_lower_scope);
        let ProbeSpec::Http { ports, .. } = &def.probe else {
            panic!()
        };
        assert_eq!(ports, &vec![9999u16]);
    }
}
