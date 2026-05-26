//! Helper protocol round-trip — engine ↔ helper structs share wire shape.
//!
//! This test pins the JSON layout produced by the wire types so accidental
//! renames in either crate fail the test instead of silently breaking the
//! wsl.exe stdio protocol.

use ordius_helper::protocol as wire;

#[test]
fn probe_plan_engine_to_wire_roundtrip() {
    let plan = wire::ProbePlanV1 {
        version: 1,
        per_resource_timeout_ms: 1000,
        max_concurrency: 8,
        overall_budget_ms: 5000,
        resources: vec![wire::ResourceSpecV1 {
            id: "ollama".into(),
            kind: wire::ResourceKindV1::Http {
                bases: vec!["http://127.0.0.1:11434".into()],
                routes: vec![wire::HttpProbeRouteV1 {
                    path: "/api/version".into(),
                    method: wire::HttpProbeMethodV1::Get,
                    proves: vec!["ollama_native".into()],
                    expect_status: vec![],
                    fingerprint_jsonpaths: vec!["$.version".into()],
                }],
            },
        }],
    };
    let s = serde_json::to_string(&plan).unwrap();
    let back: wire::ProbePlanV1 = serde_json::from_str(&s).unwrap();
    assert_eq!(back.resources.len(), 1);
    assert_eq!(back.resources[0].id, "ollama");
}

#[test]
fn outcome_serializes_with_snake_case_tags() {
    let o = wire::ProbeOutcomeV1 {
        version: 1,
        id: "ollama".into(),
        outcome: wire::ProbeOutcomeBodyV1::Found(wire::ProbeDetailV1::HttpEndpoint {
            base_url: "http://127.0.0.1:11434".into(),
            proven_routes: vec![wire::ProvenRouteV1 {
                capabilities: vec!["ollama_native".into()],
                path: "/api/version".into(),
                status: 200,
                fingerprint: "abc".into(),
            }],
        }),
        elapsed_ms: 42,
    };
    let s = serde_json::to_string(&o).unwrap();
    assert!(s.contains("\"kind\":\"found\""));
    assert!(s.contains("\"detail\":\"http_endpoint\""));
    assert!(s.contains("\"base_url\""));
    assert!(s.contains("\"proven_routes\""));
}

#[test]
fn outcome_not_found_uses_snake_case() {
    let o = wire::ProbeOutcomeV1 {
        version: 1,
        id: "ollama".into(),
        outcome: wire::ProbeOutcomeBodyV1::NotFound,
        elapsed_ms: 1,
    };
    let s = serde_json::to_string(&o).unwrap();
    assert!(s.contains("\"kind\":\"not_found\""));
}

#[test]
fn binary_outcome_round_trips() {
    let o = wire::ProbeOutcomeV1 {
        version: 1,
        id: "ripgrep".into(),
        outcome: wire::ProbeOutcomeBodyV1::Found(wire::ProbeDetailV1::Binary {
            path: "/usr/bin/rg".into(),
        }),
        elapsed_ms: 3,
    };
    let s = serde_json::to_string(&o).unwrap();
    assert!(s.contains("\"detail\":\"binary\""));
    assert!(s.contains("\"path\":\"/usr/bin/rg\""));
    let back: wire::ProbeOutcomeV1 = serde_json::from_str(&s).unwrap();
    match back.outcome {
        wire::ProbeOutcomeBodyV1::Found(wire::ProbeDetailV1::Binary { path }) => {
            assert_eq!(path, "/usr/bin/rg");
        },
        other => panic!("expected Binary, got {other:?}"),
    }
}

#[test]
fn toolchain_outcome_round_trips() {
    let o = wire::ProbeOutcomeV1 {
        version: 1,
        id: "node".into(),
        outcome: wire::ProbeOutcomeBodyV1::Found(wire::ProbeDetailV1::Toolchain {
            path: "/usr/bin/node".into(),
            version: "20.5.0".into(),
        }),
        elapsed_ms: 12,
    };
    let s = serde_json::to_string(&o).unwrap();
    assert!(s.contains("\"detail\":\"toolchain\""));
    assert!(s.contains("\"path\":\"/usr/bin/node\""));
    assert!(s.contains("\"version\":\"20.5.0\""));
    let back: wire::ProbeOutcomeV1 = serde_json::from_str(&s).unwrap();
    match back.outcome {
        wire::ProbeOutcomeBodyV1::Found(wire::ProbeDetailV1::Toolchain { path, version }) => {
            assert_eq!(path, "/usr/bin/node");
            assert_eq!(version, "20.5.0");
        },
        other => panic!("expected Toolchain, got {other:?}"),
    }
}

#[test]
fn multi_cap_proven_route_round_trips() {
    // Regression cover for b32dd82 — `proves` was `String` before; multi-cap
    // routes (OpenAI shapes that prove both chat-completions and tool-calling)
    // collapsed to a single capability on the wire.
    let o = wire::ProbeOutcomeV1 {
        version: 1,
        id: "openai_compat".into(),
        outcome: wire::ProbeOutcomeBodyV1::Found(wire::ProbeDetailV1::HttpEndpoint {
            base_url: "http://127.0.0.1:1234".into(),
            proven_routes: vec![wire::ProvenRouteV1 {
                capabilities: vec![
                    "openai_chat_completions".into(),
                    "openai_tool_calling".into(),
                ],
                path: "/v1/models".into(),
                status: 200,
                fingerprint: "x".into(),
            }],
        }),
        elapsed_ms: 1,
    };
    let s = serde_json::to_string(&o).unwrap();
    assert!(s.contains("\"openai_chat_completions\""));
    assert!(s.contains("\"openai_tool_calling\""));
    let back: wire::ProbeOutcomeV1 = serde_json::from_str(&s).unwrap();
    let proven = match back.outcome {
        wire::ProbeOutcomeBodyV1::Found(wire::ProbeDetailV1::HttpEndpoint {
            proven_routes,
            ..
        }) => proven_routes,
        other => panic!("expected HttpEndpoint, got {other:?}"),
    };
    assert_eq!(proven.len(), 1);
    assert_eq!(proven[0].capabilities.len(), 2);
    assert_eq!(proven[0].capabilities[0], "openai_chat_completions");
    assert_eq!(proven[0].capabilities[1], "openai_tool_calling");
}

#[test]
fn head_method_round_trips() {
    let route = wire::HttpProbeRouteV1 {
        path: "/healthz".into(),
        method: wire::HttpProbeMethodV1::Head,
        proves: vec!["ollama_native".into()],
        expect_status: vec![],
        fingerprint_jsonpaths: vec![],
    };
    let s = serde_json::to_string(&route).unwrap();
    assert!(s.contains("\"method\":\"head\""));
    let back: wire::HttpProbeRouteV1 = serde_json::from_str(&s).unwrap();
    assert!(matches!(back.method, wire::HttpProbeMethodV1::Head));
}
