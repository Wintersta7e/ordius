//! Wire protocol types exchanged over stdin/stdout between engine and helper.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// --- Probe plan input ---------------------------------------------------

/// Wire shape of a probe plan exchanged over the helper's stdin protocol.
///
/// This is a separate type from `engine::environment::runtime::plan::ProbePlan`
/// — the engine translates *into* this shape before piping it to the helper,
/// and the helper produces matching [`ProbeOutcomeV1`] lines on stdout.
/// Keeping the wire form distinct lets the helper crate compile without the
/// engine and lets the two sides evolve their internal representations
/// independently.  The shared JSON contract is pinned by a compat test in
/// the engine crate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbePlanV1 {
    /// Format version. Always `1` for this protocol revision.
    pub version: u32,
    /// Per-resource timeout in milliseconds. `0` means "no per-resource cap".
    pub per_resource_timeout_ms: u64,
    /// Max concurrent in-flight probes.  Helper enforces sequentially when
    /// this is 1; in v1 the helper always runs sequentially regardless.
    pub max_concurrency: u32,
    /// Overall budget in milliseconds. `0` means "no overall cap".
    pub overall_budget_ms: u64,
    /// Resource specs to probe, in declaration order.
    pub resources: Vec<ResourceSpecV1>,
}

/// Wire form of a single resource specification to probe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSpecV1 {
    /// Stable resource id (e.g. `"ollama"`).
    pub id: String,
    /// Kind discriminator.
    pub kind: ResourceKindV1,
}

/// Resource kind tag.  Each variant carries its own concrete config.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResourceKindV1 {
    /// HTTP service probed by one or more route patterns.
    Http {
        /// Base URLs to try (in priority order).
        bases: Vec<String>,
        /// Probe routes keyed by capability.
        routes: Vec<HttpProbeRouteV1>,
    },
    /// Local binary discovered via PATH lookup.
    Binary {
        /// Binary name (`which` lookup).
        bin: String,
        /// Optional extra search paths beyond `PATH`.
        extra_search_paths: Vec<String>,
    },
    /// Toolchain — binary plus a `--version`-style invocation whose output
    /// must match `version_regex` to count as Found.
    Toolchain {
        /// Binary name.
        bin: String,
        /// Argv to invoke for the version check.
        version_args: Vec<String>,
        /// Regex string applied to stdout; first capture group becomes the version.
        version_regex: String,
        /// Optional extra search paths beyond `PATH`.
        extra_search_paths: Vec<String>,
    },
}

/// One HTTP probe route, carrying the capabilities it proves.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpProbeRouteV1 {
    /// Path under the base URL (e.g. `"/api/version"`).
    pub path: String,
    /// HTTP method.
    pub method: HttpProbeMethodV1,
    /// Capabilities the route proves on a 2xx response.  A single route may
    /// prove multiple capabilities at once (e.g. an OpenAI-shaped endpoint
    /// often proves both chat-completions and tool-calling).
    pub proves: Vec<String>,
    /// Expected status range — defaults to 200-299 if empty.
    #[serde(default)]
    pub expect_status: Vec<u16>,
    /// `JSONPath` expressions used to derive the stable fingerprint of the response.
    #[serde(default)]
    pub fingerprint_jsonpaths: Vec<String>,
}

/// HTTP method.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpProbeMethodV1 {
    /// HTTP GET.
    Get,
    /// HTTP HEAD — body discarded; useful for probes that check existence
    /// without pulling a response payload.
    Head,
    /// HTTP POST.
    Post,
}

// --- Probe outcome output ----------------------------------------------

/// One JSONL line per probed resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProbeOutcomeV1 {
    /// Format version.
    pub version: u32,
    /// Resource id this outcome corresponds to.
    pub id: String,
    /// Concrete outcome.
    pub outcome: ProbeOutcomeBodyV1,
    /// Wall-clock probe duration in milliseconds.
    pub elapsed_ms: u64,
}

/// Wire outcome variants.
///
/// Distinct from engine's `ResourceProbeOutcome` — both sides intentionally
/// pick their own serde tag name (`"kind"` here vs engine's `"outcome"`)
/// because the two types serve different consumers (helper JSONL on stdout
/// vs engine catalog persisted to `SQLite`).  The translation layer lives in
/// the engine (`runtime::wsl::dispatcher` will add `wire_outcome_to_engine`
/// in T17).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProbeOutcomeBodyV1 {
    /// Resource was reachable; carries the proven details.
    Found(ProbeDetailV1),
    /// Resource was not reachable on any declared route.
    NotFound,
    /// Probe was deliberately skipped (cancelled, budget elapsed, etc.).
    Skipped {
        /// Human-readable reason the probe was skipped.
        reason: String,
    },
    /// Probe reached the resource but the response was invalid.
    ProbeFailed {
        /// Human-readable description of why the probe failed.
        reason: String,
    },
    /// Per-resource deadline elapsed before a response arrived.
    TimedOut,
}

/// Concrete detail for a successful probe.
///
/// The tag is `"detail"` (not `"kind"`) so it doesn't collide with the outer
/// `ProbeOutcomeBodyV1` discriminator. Both are internally-tagged and flatten
/// into the same JSON object; using the same name produced duplicate `kind`
/// keys that `serde_json` rejects on deserialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "detail", rename_all = "snake_case")]
pub enum ProbeDetailV1 {
    /// HTTP service — wire counterpart of engine's `ResourceDetail::HttpEndpoint`.
    HttpEndpoint {
        /// Base URL that successfully answered at least one route.
        base_url: String,
        /// Routes that responded with an acceptable status.
        proven_routes: Vec<ProvenRouteV1>,
    },
    /// Local binary.
    Binary {
        /// Resolved absolute path to the binary.
        path: String,
    },
    /// Toolchain with extracted version.
    Toolchain {
        /// Resolved absolute path to the binary.
        path: String,
        /// Version string captured from the version command output.
        version: String,
    },
}

/// Proven HTTP route — capabilities + path + stable fingerprint of response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvenRouteV1 {
    /// Capabilities the route proved.  A single proven route can satisfy
    /// multiple capabilities; the engine re-keys this into per-capability
    /// `ProvenRoute` entries during translation.
    pub capabilities: Vec<String>,
    /// Path under the base URL that answered.
    pub path: String,
    /// HTTP status code returned.
    pub status: u16,
    /// Stable fingerprint of the response payload (per `fingerprint_jsonpaths`).
    pub fingerprint: String,
}

// --- Exec request -------------------------------------------------------

/// Argv-only exec request.  Read from helper stdin in `Exec { argv_json: true }`
/// mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecRequestV1 {
    /// Format version. Always `1` for this protocol revision.
    pub version: u32,
    /// Program to execute (looked up on `PATH` if not absolute).
    pub program: String,
    /// Positional arguments passed to the program.
    pub args: Vec<String>,
    /// Extra environment variables overlaid on the helper's environment.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Working directory for the spawned process. `None` inherits the helper's cwd.
    #[serde(default)]
    pub cwd: Option<String>,
    /// Optional stdin payload as standard base64 (RFC 4648, `+/` alphabet,
    /// padded).  Absent or empty means no stdin.
    #[serde(default)]
    pub stdin_b64: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_plan_roundtrips() {
        let plan = ProbePlanV1 {
            version: 1,
            per_resource_timeout_ms: 1000,
            max_concurrency: 8,
            overall_budget_ms: 5000,
            resources: vec![ResourceSpecV1 {
                id: "ollama".into(),
                kind: ResourceKindV1::Http {
                    bases: vec!["http://127.0.0.1:11434".into()],
                    routes: vec![HttpProbeRouteV1 {
                        path: "/api/version".into(),
                        method: HttpProbeMethodV1::Get,
                        proves: vec!["ollama_native".into()],
                        expect_status: vec![],
                        fingerprint_jsonpaths: vec!["$.version".into()],
                    }],
                },
            }],
        };
        let s = serde_json::to_string(&plan).unwrap();
        let back: ProbePlanV1 = serde_json::from_str(&s).unwrap();
        assert_eq!(back.resources.len(), 1);
        assert_eq!(back.resources[0].id, "ollama");
    }

    #[test]
    fn probe_outcome_jsonl_shape() {
        let o = ProbeOutcomeV1 {
            version: 1,
            id: "ollama".into(),
            outcome: ProbeOutcomeBodyV1::Found(ProbeDetailV1::HttpEndpoint {
                base_url: "http://127.0.0.1:11434".into(),
                proven_routes: vec![ProvenRouteV1 {
                    capabilities: vec!["ollama_native".into()],
                    path: "/api/version".into(),
                    status: 200,
                    fingerprint: "abc123".into(),
                }],
            }),
            elapsed_ms: 42,
        };
        let s = serde_json::to_string(&o).unwrap();
        assert!(s.contains("\"outcome\":{\"kind\":\"found\""));
        // Pin the renamed inner tag explicitly — using `"http_endpoint"` as a
        // bare substring would not detect a regression to `"kind":"http_endpoint"`
        // that re-introduces the BUG-09 duplicate-`kind` failure.
        assert!(
            s.contains("\"detail\":\"http_endpoint\""),
            "inner tag must be `detail`, got: {s}"
        );
        // Confirm serde_json can parse the wire form back. The duplicate-`kind`
        // bug surfaced as a deserialise-time error; serialise-only assertions
        // missed it.
        let back: ProbeOutcomeV1 =
            serde_json::from_str(&s).expect("Found outcome must round-trip cleanly");
        assert!(matches!(
            back.outcome,
            ProbeOutcomeBodyV1::Found(ProbeDetailV1::HttpEndpoint { .. })
        ));
    }

    #[test]
    fn exec_request_optional_stdin() {
        let s = r#"{"version":1,"program":"echo","args":["hi"]}"#;
        let req: ExecRequestV1 = serde_json::from_str(s).unwrap();
        assert_eq!(req.program, "echo");
        assert!(req.stdin_b64.is_none());
        assert!(req.cwd.is_none());
        assert!(req.env.is_empty());
    }

    #[test]
    fn outcome_not_found_serializes_compact() {
        let o = ProbeOutcomeV1 {
            version: 1,
            id: "missing".into(),
            outcome: ProbeOutcomeBodyV1::NotFound,
            elapsed_ms: 1,
        };
        let s = serde_json::to_string(&o).unwrap();
        assert!(s.contains("\"kind\":\"not_found\""));
    }
}
