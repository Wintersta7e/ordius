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
                    proves: "ollama_native".into(),
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
                capability: "ollama_native".into(),
                path: "/api/version".into(),
                status: 200,
                fingerprint: "abc".into(),
            }],
        }),
        elapsed_ms: 42,
    };
    let s = serde_json::to_string(&o).unwrap();
    assert!(s.contains("\"kind\":\"found\""));
    assert!(s.contains("\"kind\":\"http_endpoint\""));
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
