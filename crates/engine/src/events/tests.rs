use super::*;

fn make_event(ty: EventType) -> RunEvent {
    RunEvent {
        ty,
        seq: 1,
        emitted_at: 1_716_045_600_000,
        run_id: "r1".into(),
        node_id: Some("n1".into()),
        iteration: Some(1),
        attempt: Some(1),
        payload: HashMap::new(),
    }
}

#[test]
fn node_started_serialises_with_type_discriminator() {
    let e = make_event(EventType::NodeStarted);
    let s = serde_json::to_string(&e).unwrap();
    assert!(s.contains(r#""type":"node:started""#));
    assert!(s.contains(r#""seq":1"#));
    assert!(s.contains(r#""run_id":"r1""#));
}

#[test]
fn workflow_event_omits_node_fields_when_none() {
    let e = RunEvent {
        ty: EventType::WorkflowStarted,
        seq: 0,
        emitted_at: 0,
        run_id: "r1".into(),
        node_id: None,
        iteration: None,
        attempt: None,
        payload: HashMap::new(),
    };
    let s = serde_json::to_string(&e).unwrap();
    assert!(s.contains(r#""type":"workflow:started""#));
    assert!(!s.contains("node_id"));
    assert!(!s.contains("iteration"));
    assert!(!s.contains("attempt"));
}

#[test]
fn event_roundtrips_through_json() {
    let e = make_event(EventType::NodeDone);
    let s = serde_json::to_string(&e).unwrap();
    let back: RunEvent = serde_json::from_str(&s).unwrap();
    assert_eq!(back.ty, EventType::NodeDone);
    assert_eq!(back.seq, e.seq);
    assert_eq!(back.run_id, e.run_id);
    assert_eq!(back.node_id, e.node_id);
}

#[test]
fn payload_fields_flatten_to_top_level() {
    let mut payload = HashMap::new();
    payload.insert("channel".into(), serde_json::Value::String("stdout".into()));
    let e = RunEvent {
        ty: EventType::NodeOutput,
        seq: 7,
        emitted_at: 0,
        run_id: "r1".into(),
        node_id: Some("n1".into()),
        iteration: Some(1),
        attempt: Some(1),
        payload,
    };
    let s = serde_json::to_string(&e).unwrap();
    assert!(s.contains(r#""channel":"stdout""#));
}

#[test]
fn all_thirteen_variants_have_distinct_tags() {
    let tags: std::collections::HashSet<&str> = ALL_VARIANTS.iter().map(|v| v.wire_tag()).collect();
    assert_eq!(tags.len(), 13);
}

const ALL_VARIANTS: &[EventType] = &[
    EventType::WorkflowStarted,
    EventType::WorkflowDone,
    EventType::WorkflowError,
    EventType::WorkflowStopped,
    EventType::NodeStarted,
    EventType::NodeOutput,
    EventType::NodeDone,
    EventType::NodeError,
    EventType::NodeSkipped,
    EventType::NodeRetry,
    EventType::NodeLoop,
    EventType::NodePaused,
    EventType::NodeResumed,
];

#[test]
fn wire_tag_matches_serde_rename() {
    for v in ALL_VARIANTS {
        let serde_tag = serde_json::to_value(v).unwrap();
        assert_eq!(serde_tag.as_str(), Some(v.wire_tag()));
    }
}
