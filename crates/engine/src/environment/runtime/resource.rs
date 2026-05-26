//! Resource definitions: identity, kind, capabilities, probe specs.

use serde::{Deserialize, Serialize};

/// Stable identifier for a resource, such as `"ollama"` or `"lm-studio"`.
/// Persisted as text in `EnvSpec::*::resources` inline lists.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ResourceId(pub String);

impl std::fmt::Display for ResourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Broad category of a resource; drives which probe path is taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    /// A service reachable over HTTP (LLM inference server, etc.).
    HttpEndpoint,
    /// A standalone executable (CLI agent, formatter, etc.).
    Binary,
    /// A language runtime / toolchain (node, python, rustc, etc.).
    Toolchain,
}

/// A specific feature a resource can offer.
///
/// Capabilities are fine-grained so the `llm` node can require exactly what
/// it needs rather than matching against a coarse category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    /// OpenAI-compatible `/v1/chat/completions` endpoint.
    OpenaiChatCompletions,
    /// OpenAI-compatible tool-calling in chat completions.
    OpenaiToolCalling,
    /// OpenAI-compatible streaming chat completions.
    OpenaiStreamingChat,
    /// OpenAI-compatible `/v1/embeddings` endpoint.
    OpenaiEmbeddings,
    /// Ollama-native API (`/api/generate`, `/api/chat`, `/api/version`).
    OllamaNative,
    /// LM Studio native API (`/api/v1/models` etc.).
    LmStudioNative,
    /// Coding CLI agent that accepts `--print` / non-interactive invocation.
    CliAgentPrint,
    /// Code formatter binary (`rustfmt`, `black`, `prettier`, etc.).
    CodeFormatter,
    /// Package manager binary (`npm`, `pip`, `cargo`, etc.).
    PackageManager,
}

/// Full resource declaration. Stored inline in `EnvSpec::*::resources`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceDefinition {
    /// Stable resource identifier (must match a builtin id or be unique within scope).
    pub id: ResourceId,
    /// Broad category; determines which probe path is used.
    pub kind: ResourceKind,
    /// Capabilities the resource *advertises*. The dispatcher must *prove*
    /// each via a successful probe of that capability's route before
    /// surfacing it on the catalog. Defaults to empty so a user-authored
    /// TOML/JSON entry can omit the field for resources whose probe doesn't
    /// declare a capability (Binary / Toolchain).
    #[serde(default)]
    pub advertised_capabilities: Vec<Capability>,
    /// How to probe for this resource.
    pub probe: ProbeSpec,
    /// Must be `true` when shadowing a built-in or user-global with the same id.
    #[serde(default)]
    pub override_lower_scope: bool,
}

/// How to probe a resource for presence and capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProbeSpec {
    /// Probe by sending HTTP requests to one or more ports and routes.
    Http {
        /// Ports to attempt in order; first successful bind wins.
        ports: Vec<u16>,
        /// Routes to probe; each route proves zero or more capabilities.
        /// Defaults to empty so a liveness-only probe (port-open check
        /// with no route assertions) can omit the field entirely.
        #[serde(default)]
        routes: Vec<HttpProbeRoute>,
        /// Override `ProbePlan.per_resource_timeout` for this resource.
        timeout_ms: Option<u64>,
    },
    /// Probe by running a binary with version arguments.
    Binary {
        /// Executable name (looked up on PATH + `extra_search_paths`).
        bin: String,
        /// Arguments passed to get a version string (e.g. `["--version"]`).
        version_args: Vec<String>,
        /// Regex with one capture group extracting the version number.
        version_regex: String,
        /// Additional directories to search before PATH.
        #[serde(default)]
        extra_search_paths: Vec<String>,
        /// Override `ProbePlan.per_resource_timeout` for this resource.
        timeout_ms: Option<u64>,
    },
    /// Probe a language toolchain binary.
    Toolchain {
        /// Executable name (looked up on PATH + `extra_search_paths`).
        bin: String,
        /// Arguments passed to get a version string.
        version_args: Vec<String>,
        /// Regex with one capture group extracting the version number.
        version_regex: String,
        /// Additional directories to search before PATH. Supports `~` home
        /// expansion and `*` glob patterns; engine + helper both expand these
        /// before probing.
        #[serde(default)]
        extra_search_paths: Vec<String>,
        /// Override `ProbePlan.per_resource_timeout` for this resource.
        timeout_ms: Option<u64>,
    },
}

/// A single HTTP route within an `HttpProbeSpec`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpProbeRoute {
    /// Path component of the probe URL (e.g. `"/api/version"`).
    pub path: String,
    /// HTTP method to use for the probe request.
    pub method: HttpProbeMethod,
    /// API flavor to interpret the response as.
    pub flavor: ApiFlavor,
    /// Capabilities a 2xx response on this route proves. The catalog stores
    /// only proven capabilities.
    pub proves: Vec<Capability>,
    /// `JSONPath` expression to extract the model list from the response body.
    pub models_jsonpath: Option<String>,
    /// Stable `JSONPath` subset for `HostDirect` fingerprinting (per spec §2).
    #[serde(default)]
    pub fingerprint_jsonpaths: Vec<String>,
}

/// HTTP method used for probing a route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpProbeMethod {
    /// HTTP GET request.
    Get,
    /// HTTP HEAD request.
    Head,
}

/// Which API dialect a probe route belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiFlavor {
    /// OpenAI-compatible chat/completions API.
    OpenaiChat,
    /// Ollama-native API.
    OllamaNative,
    /// LM Studio native API.
    LmStudioNative,
    /// llama.cpp server API.
    LlamaCppServer,
    /// Custom / unclassified API.
    Custom,
}

/// Carried in node config — references a resource by id, optionally asserting
/// a required capability that must be proven before the node may dispatch.
///
/// Untagged so the wire form accepts either the short `"resource": "openai"`
/// (just the id) or the long form
/// `"resource": { "id": "openai", "required_capability": "openai_tool_calling" }`.
/// Both round-trip through the same field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ResourceRef {
    /// Short form — bare resource id, no capability filter.
    Bare(ResourceId),
    /// Long form — id plus optional capability filter.
    Detailed {
        /// The resource to use.
        id: ResourceId,
        /// If set, the dispatcher enforces this capability before dispatch.
        #[serde(default)]
        required_capability: Option<Capability>,
    },
}

impl ResourceRef {
    /// View the underlying `ResourceId`.
    #[must_use]
    pub const fn id(&self) -> &ResourceId {
        match self {
            Self::Bare(id) | Self::Detailed { id, .. } => id,
        }
    }

    /// View the capability constraint, if any.
    #[must_use]
    pub const fn required_capability(&self) -> Option<Capability> {
        match self {
            Self::Bare(_) => None,
            Self::Detailed {
                required_capability,
                ..
            } => *required_capability,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_id_roundtrips() {
        let id = ResourceId("ollama".into());
        let s = serde_json::to_string(&id).unwrap();
        assert_eq!(s, "\"ollama\"");
        let back: ResourceId = serde_json::from_str(&s).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn probe_spec_http_serializes_with_routes() {
        let spec = ProbeSpec::Http {
            ports: vec![11434],
            routes: vec![HttpProbeRoute {
                path: "/api/version".into(),
                method: HttpProbeMethod::Get,
                flavor: ApiFlavor::OllamaNative,
                proves: vec![Capability::OllamaNative],
                models_jsonpath: None,
                fingerprint_jsonpaths: vec!["$.version".into()],
            }],
            timeout_ms: None,
        };
        let s = serde_json::to_string(&spec).unwrap();
        assert!(s.contains("\"kind\":\"http\""));
        let back: ProbeSpec = serde_json::from_str(&s).unwrap();
        assert_eq!(spec, back);
    }

    #[test]
    fn capability_serializes_snake_case() {
        let cap = Capability::OpenaiChatCompletions;
        let s = serde_json::to_string(&cap).unwrap();
        assert_eq!(s, "\"openai_chat_completions\"");
    }

    #[test]
    fn api_flavor_openai_wire_form() {
        // `OpenaiChat` must serialize to `"openai_chat"` — consistent with the
        // `openai_*` prefix used by `Capability` variants for the same domain.
        assert_eq!(
            serde_json::to_string(&ApiFlavor::OpenaiChat).unwrap(),
            "\"openai_chat\""
        );
    }

    #[test]
    fn resource_definition_with_override_flag() {
        let def = ResourceDefinition {
            id: ResourceId("ollama".into()),
            kind: ResourceKind::HttpEndpoint,
            advertised_capabilities: vec![Capability::OpenaiChatCompletions],
            probe: ProbeSpec::Http {
                ports: vec![11434],
                routes: vec![],
                timeout_ms: None,
            },
            override_lower_scope: true,
        };
        let s = serde_json::to_string(&def).unwrap();
        assert!(s.contains("\"override_lower_scope\":true"));
        let back: ResourceDefinition = serde_json::from_str(&s).unwrap();
        assert_eq!(def, back);
    }

    #[test]
    fn resource_ref_short_form_roundtrips() {
        let json = r#""openai""#;
        let r: ResourceRef = serde_json::from_str(json).unwrap();
        assert_eq!(r.id().0, "openai");
        assert!(r.required_capability().is_none());
        let back = serde_json::to_string(&r).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn resource_ref_long_form_with_capability_roundtrips() {
        let json = r#"{"id":"openai","required_capability":"openai_tool_calling"}"#;
        let r: ResourceRef = serde_json::from_str(json).unwrap();
        assert_eq!(r.id().0, "openai");
        assert_eq!(
            r.required_capability().unwrap(),
            Capability::OpenaiToolCalling
        );
        let back = serde_json::to_string(&r).unwrap();
        assert_eq!(back, json);
    }

    #[test]
    fn resource_ref_long_form_without_capability_is_valid() {
        let json = r#"{"id":"openai"}"#;
        let r: ResourceRef = serde_json::from_str(json).unwrap();
        assert_eq!(r.id().0, "openai");
        assert!(r.required_capability().is_none());
    }
}
