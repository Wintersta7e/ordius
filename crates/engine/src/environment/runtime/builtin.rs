//! Built-in resource declarations and builder helpers.
//!
//! [`BUILTIN_RESOURCES`] is the single source of truth for the resources
//! Ordius can probe in any environment. Per-env and per-workflow definitions
//! in the registry layer shadow these at higher precedence.
//!
//! Helpers [`http`], [`binary`], and [`toolchain`] keep the list terse.
//! [`builtin_by_id`] provides O(n) lookup (n ≤ ~25; `LazyLock` amortises init).

use std::sync::LazyLock;

use super::error::RegistryError;
use super::registry::{ResourceRegistry, ScopeKey};
use super::resource::{
    ApiFlavor, Capability, HttpProbeMethod, HttpProbeRoute, ProbeSpec, ResourceDefinition,
    ResourceId, ResourceKind,
};

// ── Private builder helpers ───────────────────────────────────────────────────

/// Build a base [`HttpProbeRoute`] with GET method and no fingerprint paths.
/// Callers use struct-update syntax to set optional fields.
fn route(path: &str, flavor: ApiFlavor, proves: &[Capability]) -> HttpProbeRoute {
    HttpProbeRoute {
        path: path.into(),
        method: HttpProbeMethod::Get,
        flavor,
        proves: proves.to_vec(),
        models_jsonpath: None,
        fingerprint_jsonpaths: vec![],
    }
}

/// Build an [`HttpEndpoint`](ResourceKind::HttpEndpoint) [`ResourceDefinition`].
fn http(
    id: &str,
    ports: &[u16],
    routes: Vec<HttpProbeRoute>,
    advertised: &[Capability],
) -> ResourceDefinition {
    ResourceDefinition {
        id: ResourceId(id.into()),
        kind: ResourceKind::HttpEndpoint,
        advertised_capabilities: advertised.to_vec(),
        probe: ProbeSpec::Http {
            ports: ports.to_vec(),
            routes,
            timeout_ms: None,
        },
        override_lower_scope: false,
    }
}

/// Build a [`Binary`](ResourceKind::Binary) [`ResourceDefinition`].
fn binary(
    id: &str,
    bin: &str,
    version_args: &[&str],
    version_regex: &str,
    caps: &[Capability],
) -> ResourceDefinition {
    ResourceDefinition {
        id: ResourceId(id.into()),
        kind: ResourceKind::Binary,
        advertised_capabilities: caps.to_vec(),
        probe: ProbeSpec::Binary {
            bin: bin.into(),
            version_args: version_args.iter().map(|s| (*s).to_string()).collect(),
            version_regex: version_regex.into(),
            extra_search_paths: vec![],
            timeout_ms: None,
        },
        override_lower_scope: false,
    }
}

/// Build a [`Toolchain`](ResourceKind::Toolchain) [`ResourceDefinition`].
fn toolchain(
    id: &str,
    bin: &str,
    version_args: &[&str],
    version_regex: &str,
) -> ResourceDefinition {
    ResourceDefinition {
        id: ResourceId(id.into()),
        kind: ResourceKind::Toolchain,
        advertised_capabilities: vec![],
        probe: ProbeSpec::Toolchain {
            bin: bin.into(),
            version_args: version_args.iter().map(|s| (*s).to_string()).collect(),
            version_regex: version_regex.into(),
            timeout_ms: None,
        },
        override_lower_scope: false,
    }
}

// ── Static list ───────────────────────────────────────────────────────────────

/// All built-in resources Ordius knows how to probe.
///
/// 21 entries total:
/// - 6 LLM HTTP endpoints: `ollama`, `lm-studio`, `llama-cpp`, `openai-compat`, `vllm`, `tabby`
/// - 8 coding CLI agents:  `claude-code`, `codex`, `aider`, `gemini-cli`, `goose`,
///   `amazon-q`, `opencode`, `cursor-cli`
/// - 7 toolchains:         `git`, `docker`, `node`, `python`, `rust`, `go`, `ffmpeg`
///
/// Per-env or per-workflow definitions with matching ids (plus
/// `override_lower_scope = true`) shadow entries here.
pub static BUILTIN_RESOURCES: LazyLock<Vec<ResourceDefinition>> = LazyLock::new(|| {
    vec![
        // ── LLM HTTP endpoints ────────────────────────────────────────────────
        //
        // Ollama: probes both Ollama-native /api/version (fingerprint) and the
        // OpenAI-compat /v1/models route so the merged `llm` node can use either.
        http(
            "ollama",
            &[11434],
            vec![
                HttpProbeRoute {
                    fingerprint_jsonpaths: vec!["$.version".into()],
                    ..route(
                        "/api/version",
                        ApiFlavor::OllamaNative,
                        &[Capability::OllamaNative],
                    )
                },
                HttpProbeRoute {
                    models_jsonpath: Some("$.data[*].id".into()),
                    ..route(
                        "/v1/models",
                        ApiFlavor::OpenaiChat,
                        &[
                            Capability::OpenaiChatCompletions,
                            Capability::OpenaiEmbeddings,
                        ],
                    )
                },
            ],
            &[
                Capability::OllamaNative,
                Capability::OpenaiChatCompletions,
                Capability::OpenaiEmbeddings,
            ],
        ),
        // LM Studio: native path and OpenAI-compat path differ; probe both.
        http(
            "lm-studio",
            &[1234],
            vec![
                route(
                    "/api/v1/models",
                    ApiFlavor::LmStudioNative,
                    &[
                        Capability::OpenaiChatCompletions,
                        Capability::OpenaiEmbeddings,
                    ],
                ),
                route(
                    "/v1/models",
                    ApiFlavor::OpenaiChat,
                    &[Capability::OpenaiChatCompletions],
                ),
            ],
            &[
                Capability::OpenaiChatCompletions,
                Capability::OpenaiEmbeddings,
            ],
        ),
        // llama.cpp server: default port 8080, OpenAI-compat + health check.
        http(
            "llama-cpp",
            &[8080],
            vec![
                route(
                    "/v1/models",
                    ApiFlavor::OpenaiChat,
                    &[Capability::OpenaiChatCompletions],
                ),
                // liveness check only — no capability proof
                route("/health", ApiFlavor::LlamaCppServer, &[]),
            ],
            &[Capability::OpenaiChatCompletions],
        ),
        // Generic OpenAI-compatible endpoint (port 8000).
        http(
            "openai-compat",
            &[8000],
            vec![route(
                "/v1/models",
                ApiFlavor::OpenaiChat,
                &[Capability::OpenaiChatCompletions],
            )],
            &[Capability::OpenaiChatCompletions],
        ),
        // vLLM: default port 8001, OpenAI-compat.
        http(
            "vllm",
            &[8001],
            vec![route(
                "/v1/models",
                ApiFlavor::OpenaiChat,
                &[Capability::OpenaiChatCompletions],
            )],
            &[Capability::OpenaiChatCompletions],
        ),
        // Tabby: code-completion server; /v1/health is a liveness check only,
        // not a capability probe (no chat caps surfaced yet).
        http(
            "tabby",
            &[8080],
            vec![route("/v1/health", ApiFlavor::Custom, &[])],
            &[],
        ),
        // ── Coding CLI agents ─────────────────────────────────────────────────
        //
        // IDs are AgentDeck-aligned. Note: binary name for claude-code is "claude"
        // (not "claude-code") — the npm package installs a bare `claude` binary.
        binary(
            "claude-code",
            "claude",
            &["--version"],
            r"^(\d+\.\d+\.\d+)",
            &[Capability::CliAgentPrint],
        ),
        binary(
            "codex",
            "codex",
            &["--version"],
            r"(\d+\.\d+\.\d+)",
            &[Capability::CliAgentPrint],
        ),
        binary(
            "aider",
            "aider",
            &["--version"],
            r"^aider (\d+\.\d+\.\d+)",
            &[Capability::CliAgentPrint],
        ),
        // Renamed from "gemini" to "gemini-cli" (round-3 correction).
        // The CLI binary itself is still called `gemini`.
        binary(
            "gemini-cli",
            "gemini",
            &["--version"],
            r"(\d+\.\d+\.\d+)",
            &[Capability::CliAgentPrint],
        ),
        // Goose uses `goose version` (subcommand), not `goose --version`.
        binary(
            "goose",
            "goose",
            &["version"],
            r"(\d+\.\d+\.\d+)",
            &[Capability::CliAgentPrint],
        ),
        binary(
            "amazon-q",
            "q",
            &["--version"],
            r"(\d+\.\d+\.\d+)",
            &[Capability::CliAgentPrint],
        ),
        // opencode uses `opencode version` subcommand.
        binary(
            "opencode",
            "opencode",
            &["version"],
            r"(\d+\.\d+\.\d+)",
            &[Capability::CliAgentPrint],
        ),
        binary(
            "cursor-cli",
            "cursor-agent",
            &["--version"],
            r"(\d+\.\d+\.\d+)",
            &[Capability::CliAgentPrint],
        ),
        // ── Runtimes / toolchains ─────────────────────────────────────────────
        toolchain("git", "git", &["--version"], r"^git version (\S+)"),
        toolchain("docker", "docker", &["--version"], r"^Docker version (\S+)"),
        toolchain("node", "node", &["--version"], r"^v(\d+\.\d+\.\d+)"),
        toolchain(
            "python",
            "python3",
            &["--version"],
            r"^Python (\d+\.\d+\.\d+)",
        ),
        toolchain("rust", "rustc", &["--version"], r"^rustc (\d+\.\d+\.\d+)"),
        // Go uses `go version` subcommand.
        toolchain("go", "go", &["version"], r"^go version go(\S+)"),
        toolchain("ffmpeg", "ffmpeg", &["-version"], r"^ffmpeg version (\S+)"),
    ]
});

// ── Public accessor ───────────────────────────────────────────────────────────

/// Look up a built-in [`ResourceDefinition`] by its string id.
///
/// Returns `None` if no built-in with that id exists. Callers that need to
/// shadow a built-in at a higher scope should look up the entry here first to
/// verify it exists, then set `override_lower_scope = true` on their definition.
pub fn builtin_by_id(id: &str) -> Option<&'static ResourceDefinition> {
    BUILTIN_RESOURCES.iter().find(|r| r.id.0 == id)
}

/// Install every entry in [`BUILTIN_RESOURCES`] into `registry` under
/// [`ScopeKey::Builtin`].
///
/// Returns the number of definitions written. Built-ins always upsert
/// regardless of `override_lower_scope` — they sit at the bottom of the
/// precedence chain, so they can never shadow anything.
///
/// Idempotent: calling this twice is safe. Each call bumps the registry
/// revision by `BUILTIN_RESOURCES.len()` (one bump per upsert).
pub fn install_builtin_resources(registry: &ResourceRegistry) -> Result<usize, RegistryError> {
    let mut written = 0_usize;
    for def in BUILTIN_RESOURCES.iter() {
        registry.upsert(&ScopeKey::Builtin, def)?;
        written += 1;
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::resource::{ApiFlavor, Capability, ProbeSpec, ResourceKind};

    #[test]
    fn ollama_builtin_advertises_correct_caps() {
        let r = builtin_by_id("ollama").expect("ollama in builtins");
        assert_eq!(r.kind, ResourceKind::HttpEndpoint);
        assert!(
            r.advertised_capabilities
                .contains(&Capability::OllamaNative)
        );
        assert!(
            r.advertised_capabilities
                .contains(&Capability::OpenaiChatCompletions)
        );
        let ProbeSpec::Http { ports, routes, .. } = &r.probe else {
            panic!("http")
        };
        assert_eq!(ports, &vec![11434u16]);
        assert_eq!(routes.len(), 2);
        let ollama_route = routes
            .iter()
            .find(|r| r.flavor == ApiFlavor::OllamaNative)
            .unwrap();
        assert_eq!(ollama_route.path, "/api/version");
        assert!(ollama_route.proves.contains(&Capability::OllamaNative));
    }

    #[test]
    fn lm_studio_probes_both_paths() {
        let r = builtin_by_id("lm-studio").unwrap();
        let ProbeSpec::Http { routes, .. } = &r.probe else {
            panic!("http")
        };
        let paths: Vec<&str> = routes.iter().map(|r| r.path.as_str()).collect();
        assert!(paths.contains(&"/api/v1/models"));
        assert!(paths.contains(&"/v1/models"));
    }

    #[test]
    fn claude_code_id_is_hyphenated() {
        // AgentDeck-aligned: id "claude-code" (hyphen), binary "claude"
        let r = builtin_by_id("claude-code").unwrap();
        let ProbeSpec::Binary { bin, .. } = &r.probe else {
            panic!("binary")
        };
        assert_eq!(bin, "claude");
    }

    #[test]
    fn gemini_cli_id_per_round3_correction() {
        assert!(builtin_by_id("gemini-cli").is_some());
        assert!(builtin_by_id("gemini").is_none(), "renamed");
    }

    #[test]
    fn goose_uses_version_subcommand() {
        let r = builtin_by_id("goose").unwrap();
        let ProbeSpec::Binary { version_args, .. } = &r.probe else {
            panic!("binary")
        };
        assert_eq!(version_args, &vec!["version".to_string()]);
    }

    #[test]
    fn all_builtin_ids_unique() {
        let mut ids: Vec<&str> = BUILTIN_RESOURCES.iter().map(|r| r.id.0.as_str()).collect();
        ids.sort_unstable();
        let len = ids.len();
        ids.dedup();
        assert_eq!(ids.len(), len, "duplicate built-in ids");
    }

    #[test]
    fn install_seeds_all_builtins_under_builtin_scope() {
        use crate::environment::runtime::env::EnvId;

        let reg = ResourceRegistry::new();
        let count = install_builtin_resources(&reg).expect("install");
        assert_eq!(count, BUILTIN_RESOURCES.len());

        let snap = reg.snapshot();
        assert!(snap.revision >= count as u64);
        let builtin_layer = snap
            .layers
            .get(&ScopeKey::Builtin)
            .expect("builtin layer present");
        assert_eq!(builtin_layer.len(), BUILTIN_RESOURCES.len());

        let (_def, scope) = snap
            .resolve(&ResourceId("ollama".into()), &EnvId::local(), None)
            .expect("ollama resolved");
        assert_eq!(scope, ScopeKey::Builtin);
    }

    #[test]
    fn install_is_idempotent() {
        let reg = ResourceRegistry::new();
        let first = install_builtin_resources(&reg).expect("first install");
        let rev_after_first = reg.snapshot().revision;
        let second = install_builtin_resources(&reg).expect("second install");
        assert_eq!(first, second);
        assert!(reg.snapshot().revision > rev_after_first);
        let snap = reg.snapshot();
        let layer = snap.layers.get(&ScopeKey::Builtin).unwrap();
        assert_eq!(layer.len(), BUILTIN_RESOURCES.len());
    }
}
