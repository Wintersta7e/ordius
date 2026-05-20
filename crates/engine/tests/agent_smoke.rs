//! End-to-end smoke: `agent` invokes a mock `OpenAI` endpoint, dispatches
//! a tool call, and terminates on the second turn.

use ordius_engine::Engine;
use ordius_engine::types::{Node, Pos, Trigger, Workflow};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn agent_node(id: &str, llm_url: &str, tool_url: &str) -> Node {
    let mut config = HashMap::new();
    config.insert("url".into(), serde_json::json!(llm_url));
    config.insert("model".into(), serde_json::json!("test-model"));
    config.insert(
        "messages".into(),
        serde_json::json!([{"role": "user", "content": "what time is it?"}]),
    );
    config.insert(
        "tools".into(),
        serde_json::json!([{
            "name": "get_time",
            "description": "return the current time",
            "url": tool_url,
            "method": "POST",
            "body_template": "{{args | json}}",
        }]),
    );
    config.insert("max_turns".into(), serde_json::json!(4));
    Node {
        id: id.into(),
        ty: "agent".into(),
        name: String::new(),
        config,
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
    }
}

fn workflow(id: &str, nodes: Vec<Node>) -> Workflow {
    Workflow {
        id: id.into(),
        name: id.into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![Trigger::Manual],
        nodes,
        edges: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn agent_runs_a_tool_call_then_terminates() {
    let llm = MockServer::start().await;
    let tool = MockServer::start().await;

    // First turn: assistant asks to call get_time.
    let first_turn = ResponseTemplate::new(200).set_body_json(serde_json::json!({
        "choices": [{
            "message": {
                "role": "assistant",
                "content": "",
                "tool_calls": [{
                    "id": "tc1",
                    "type": "function",
                    "function": {"name": "get_time", "arguments": "{}"},
                }],
            },
            "finish_reason": "tool_calls",
        }],
        "usage": {"total_tokens": 25},
    }));
    // Second turn: assistant returns the final text and emits no tool calls.
    let second_turn = ResponseTemplate::new(200).set_body_json(serde_json::json!({
        "choices": [{
            "message": {
                "role": "assistant",
                "content": "the time is 12:00",
            },
            "finish_reason": "stop",
        }],
        "usage": {"total_tokens": 50},
    }));
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(first_turn)
        .up_to_n_times(1)
        .mount(&llm)
        .await;
    Mock::given(method("POST"))
        .and(path("/chat/completions"))
        .respond_with(second_turn)
        .mount(&llm)
        .await;
    // Tool endpoint returns the time payload.
    Mock::given(method("POST"))
        .and(path("/time"))
        .respond_with(ResponseTemplate::new(200).set_body_string("12:00"))
        .mount(&tool)
        .await;

    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(dir.path().to_path_buf()).await.unwrap());

    let wf = workflow(
        "p",
        vec![agent_node("a", &llm.uri(), &format!("{}/time", tool.uri()))],
    );
    let summary = engine
        .run_workflow(Arc::new(wf), HashMap::new(), "test", false, None)
        .await
        .expect("agent run");
    assert_eq!(summary.status, "done");
}
