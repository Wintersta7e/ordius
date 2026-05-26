use super::*;
use tempfile::TempDir;

const DEMO_JSON: &str = r#"{
  "id": "demo",
  "name": "Demo",
  "schema_version": 1,
  "nodes": [
    {"id": "n", "type": "delay", "name": "wait", "config": {"ms": 10}}
  ],
  "edges": []
}"#;

fn write_workflow(home: &TempDir, id: &str, body: &str) {
    let dir = home.path().join("workflows");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join(format!("{id}.json")), body).unwrap();
}

#[test]
fn list_returns_empty_when_dir_absent() {
    let home = TempDir::new().unwrap();
    let (wfs, errs) = list(home.path()).unwrap();
    assert!(wfs.is_empty());
    assert!(errs.is_empty());
}

#[test]
fn list_finds_and_sorts_workflows() {
    let home = TempDir::new().unwrap();
    write_workflow(
        &home,
        "z-last",
        DEMO_JSON.replace("\"demo\"", "\"z-last\"").as_str(),
    );
    write_workflow(
        &home,
        "a-first",
        DEMO_JSON.replace("\"demo\"", "\"a-first\"").as_str(),
    );
    let (wfs, errs) = list(home.path()).unwrap();
    assert!(errs.is_empty());
    assert_eq!(wfs.len(), 2);
    assert_eq!(wfs[0].id, "a-first");
    assert_eq!(wfs[1].id, "z-last");
}

#[test]
fn list_collects_parse_errors_without_failing() {
    let home = TempDir::new().unwrap();
    write_workflow(&home, "good", DEMO_JSON);
    write_workflow(&home, "broken", "{not json");
    let (wfs, errs) = list(home.path()).unwrap();
    assert_eq!(wfs.len(), 1);
    assert_eq!(wfs[0].id, "demo");
    assert_eq!(errs.len(), 1);
}

#[test]
fn list_skips_non_json_files() {
    let home = TempDir::new().unwrap();
    write_workflow(&home, "demo", DEMO_JSON);
    std::fs::write(home.path().join("workflows/readme.txt"), "ignored").unwrap();
    let (wfs, _) = list(home.path()).unwrap();
    assert_eq!(wfs.len(), 1);
}

#[test]
fn load_returns_workflow_by_id() {
    let home = TempDir::new().unwrap();
    write_workflow(&home, "demo", DEMO_JSON);
    let wf = load(home.path(), "demo").unwrap();
    assert_eq!(wf.id, "demo");
}

#[test]
fn load_missing_returns_load_error() {
    let home = TempDir::new().unwrap();
    let result = load(home.path(), "ghost");
    assert!(matches!(result, Err(WorkflowsError::Load { .. })));
}

#[test]
fn save_creates_dir_and_writes_pretty_json() {
    let home = TempDir::new().unwrap();
    let wf: Workflow = serde_json::from_str(DEMO_JSON).unwrap();
    save(home.path(), &wf).unwrap();
    let body = std::fs::read_to_string(home.path().join("workflows/demo.json")).unwrap();
    assert!(body.contains(r#""id": "demo""#));
}

#[test]
fn delete_removes_file_and_returns_true() {
    let home = TempDir::new().unwrap();
    write_workflow(&home, "demo", DEMO_JSON);
    assert!(delete(home.path(), "demo").unwrap());
    assert!(!path(home.path(), "demo").exists());
    assert!(
        !delete(home.path(), "demo").unwrap(),
        "second delete reports false"
    );
}

#[test]
fn duplicate_creates_clone_with_copy_suffix() {
    let home = TempDir::new().unwrap();
    write_workflow(&home, "demo", DEMO_JSON);

    let clone = duplicate(home.path(), "demo").unwrap();
    assert_eq!(clone.id, "demo-copy");
    assert!(clone.name.ends_with("(copy)"));
    assert!(path(home.path(), "demo-copy").exists());
    assert!(path(home.path(), "demo").exists(), "original is preserved");
}

#[test]
fn duplicate_collisions_get_numeric_suffix() {
    let home = TempDir::new().unwrap();
    write_workflow(&home, "demo", DEMO_JSON);

    let first = duplicate(home.path(), "demo").unwrap();
    assert_eq!(first.id, "demo-copy");

    let second = duplicate(home.path(), "demo").unwrap();
    assert_eq!(second.id, "demo-copy-2");

    let third = duplicate(home.path(), "demo").unwrap();
    assert_eq!(third.id, "demo-copy-3");
}

#[test]
fn duplicate_missing_source_returns_load_error() {
    let home = TempDir::new().unwrap();
    let result = duplicate(home.path(), "no-such-source");
    assert!(
        matches!(result, Err(WorkflowsError::Load { .. })),
        "expected Load error, got {result:?}",
    );
}

#[test]
fn duplicate_of_duplicate_strips_existing_copy_suffix() {
    let home = TempDir::new().unwrap();
    write_workflow(&home, "demo", DEMO_JSON);

    // First clone: demo → demo-copy
    let first = duplicate(home.path(), "demo").unwrap();
    assert_eq!(first.id, "demo-copy");

    // Duplicating the clone should not produce demo-copy-copy;
    // strip_copy_suffix turns `demo-copy` back into `demo`, and the
    // first available slot is `demo-copy-2`.
    let from_clone = duplicate(home.path(), "demo-copy").unwrap();
    assert_eq!(from_clone.id, "demo-copy-2");
}

#[test]
fn duplicate_of_alphanumeric_copy_suffix_is_not_stripped() {
    // The strip predicate only fires when the tail after `-copy-` is
    // purely numeric. A user-chosen suffix like `-copy-v2` is not a
    // counter and stays in the base, producing `foo-copy-v2-copy`.
    let home = TempDir::new().unwrap();
    let body = DEMO_JSON.replace("\"id\": \"demo\"", "\"id\": \"foo-copy-v2\"");
    write_workflow(&home, "foo-copy-v2", &body);

    let clone = duplicate(home.path(), "foo-copy-v2").unwrap();
    assert_eq!(clone.id, "foo-copy-v2-copy");
}

#[test]
fn duplicate_of_numbered_clone_strips_numeric_suffix() {
    let home = TempDir::new().unwrap();
    write_workflow(&home, "demo", DEMO_JSON);
    duplicate(home.path(), "demo").unwrap(); // demo-copy
    let numbered = duplicate(home.path(), "demo").unwrap();
    assert_eq!(numbered.id, "demo-copy-2");

    // Duplicating demo-copy-2 should treat the base as `demo` again.
    let clone_of_numbered = duplicate(home.path(), &numbered.id).unwrap();
    assert_eq!(clone_of_numbered.id, "demo-copy-3");
}

#[test]
fn load_rejects_agent_node_type_with_rename_hint() {
    let home = TempDir::new().unwrap();
    let id = "wf-uses-agent";
    write_workflow(
        &home,
        id,
        r#"{
            "id": "wf-uses-agent",
            "name": "x",
            "nodes": [{"id":"n1","type":"agent","name":"x","config":{}}],
            "edges": []
        }"#,
    );
    let err = load(home.path(), id).unwrap_err();
    match err {
        WorkflowsError::ReservedNodeType {
            id,
            replacement,
            node_id,
        } => {
            assert_eq!(id, "agent");
            assert_eq!(replacement, "llm");
            assert_eq!(node_id, "n1");
        },
        other => panic!("expected ReservedNodeType, got {other:?}"),
    }
}

#[test]
fn load_rejects_container_node_type_with_rename_hint() {
    let home = TempDir::new().unwrap();
    let id = "wf-uses-container";
    write_workflow(
        &home,
        id,
        r#"{
            "id": "wf-uses-container",
            "name": "x",
            "nodes": [{"id":"n1","type":"container","name":"x","config":{"image":"x"}}],
            "edges": []
        }"#,
    );
    let err = load(home.path(), id).unwrap_err();
    match err {
        WorkflowsError::ReservedNodeType {
            id,
            replacement,
            node_id,
        } => {
            assert_eq!(id, "container");
            assert_eq!(replacement, "docker-run");
            assert_eq!(node_id, "n1");
        },
        other => panic!("expected ReservedNodeType, got {other:?}"),
    }
}

#[cfg(test)]
mod scope_tests {
    use crate::environment::runtime::{EnvId, ResourceId, ResourceRegistry, ScopeKey, WorkflowId};
    use crate::types::Workflow;
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn wf_with_resources(id: &str) -> Workflow {
        Workflow {
            id: id.into(),
            name: format!("workflow {id}"),
            schema_version: 1,
            created_at: None,
            updated_at: None,
            variables: HashMap::default(),
            triggers: vec![],
            nodes: vec![],
            edges: vec![],
            resources: vec![crate::environment::runtime::ResourceDefinition {
                id: ResourceId(format!("{id}-local-llm")),
                kind: crate::environment::runtime::ResourceKind::HttpEndpoint,
                advertised_capabilities: vec![],
                probe: crate::environment::runtime::ProbeSpec::Http {
                    ports: vec![9999],
                    routes: vec![],
                    timeout_ms: None,
                },
                override_lower_scope: false,
            }],
            default_env: None,
        }
    }

    #[test]
    fn load_in_registry_installs_workflow_scope() {
        let home = TempDir::new().unwrap();
        let reg = ResourceRegistry::new();
        let wf = wf_with_resources("my-wf");
        super::save(home.path(), &wf).expect("save");
        let (_loaded, _warnings) =
            super::load_in_registry(home.path(), "my-wf", &reg).expect("load");

        let snap = reg.snapshot();
        let layer = snap
            .layers
            .get(&ScopeKey::Workflow {
                id: WorkflowId("my-wf".into()),
            })
            .expect("wf scope present");
        assert!(layer.contains_key(&ResourceId("my-wf-local-llm".into())));

        let (_def, scope) = snap
            .resolve(
                &ResourceId("my-wf-local-llm".into()),
                &EnvId::local(),
                Some(&WorkflowId("my-wf".into())),
            )
            .expect("visible to my-wf");
        assert!(matches!(scope, ScopeKey::Workflow { .. }));
    }

    #[test]
    fn delete_in_registry_drops_workflow_scope() {
        let home = TempDir::new().unwrap();
        let reg = ResourceRegistry::new();
        let wf = wf_with_resources("doomed");
        super::save(home.path(), &wf).expect("save");
        let (_loaded, _warnings) =
            super::load_in_registry(home.path(), "doomed", &reg).expect("load");

        let removed = super::delete_in_registry(home.path(), "doomed", &reg).expect("delete");
        assert!(removed);
        let snap = reg.snapshot();
        assert!(
            !snap.layers.contains_key(&ScopeKey::Workflow {
                id: WorkflowId("doomed".into())
            }),
            "scope removed"
        );
    }

    #[test]
    fn delete_in_registry_missing_file_returns_false_and_clears_scope() {
        // Even if the file is already gone, the scope may still be installed
        // — delete_in_registry should always clear the scope, then report
        // whether the file existed.
        let home = TempDir::new().unwrap();
        let reg = ResourceRegistry::new();
        let wf = wf_with_resources("orphan");
        super::install_resources_into_registry(&wf, &reg).expect("install");
        let removed = super::delete_in_registry(home.path(), "orphan", &reg).expect("delete");
        assert!(!removed);
        assert!(!reg.snapshot().layers.contains_key(&ScopeKey::Workflow {
            id: WorkflowId("orphan".into())
        }));
    }

    #[test]
    fn duplicate_in_registry_installs_clone_scope() {
        let home = TempDir::new().unwrap();
        let reg = ResourceRegistry::new();
        let wf = wf_with_resources("base");
        super::save(home.path(), &wf).expect("save");

        let clone = super::duplicate_in_registry(home.path(), "base", &reg).expect("dup");
        assert_eq!(clone.id, "base-copy");
        // The clone carries the same resource ids; they should be visible to
        // the clone's workflow id and NOT to the original.
        let snap = reg.snapshot();
        assert!(
            snap.resolve(
                &ResourceId("base-local-llm".into()),
                &EnvId::local(),
                Some(&WorkflowId("base-copy".into()))
            )
            .is_some(),
            "clone sees own scope"
        );
    }
}

#[cfg(test)]
mod validation_tests {
    use crate::environment::runtime::ResourceRegistry;
    use tempfile::TempDir;

    #[test]
    fn load_in_registry_rejects_unknown_resource_id() {
        let tmp = TempDir::new().unwrap();
        let id = "wf-bad-resource";
        let dir = tmp.path().join("workflows");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{id}.json"));
        std::fs::write(
            &path,
            r#"{
                "id":"wf-bad-resource","name":"x",
                "nodes":[{"id":"n1","type":"llm","name":"x","config":{"resource":"no-such-id","model":"m","messages":[]}}],
                "edges":[]
            }"#,
        )
        .unwrap();
        let registry = ResourceRegistry::new();
        let err = super::load_in_registry(tmp.path(), id, &registry).unwrap_err();
        match err {
            super::WorkflowsError::ResourceNotInRegistry {
                node_id,
                resource_id,
            } => {
                assert_eq!(node_id, "n1");
                assert_eq!(resource_id, "no-such-id");
            },
            other => panic!("expected ResourceNotInRegistry, got {other:?}"),
        }
    }

    #[test]
    fn load_in_registry_rejects_unadvertised_capability() {
        let tmp = TempDir::new().unwrap();
        let id = "wf-bad-cap";
        let dir = tmp.path().join("workflows");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{id}.json"));
        // Workflow-scope resource advertises only OpenaiChatCompletions but
        // the node requires OpenaiToolCalling.
        std::fs::write(
            &path,
            r#"{
                "id":"wf-bad-cap","name":"x",
                "resources":[{
                    "id":"local-llm",
                    "kind":"http_endpoint",
                    "advertised_capabilities":["openai_chat_completions"],
                    "probe":{"kind":"http","ports":[7777]},
                    "override_lower_scope":false
                }],
                "nodes":[{"id":"n1","type":"llm","name":"x",
                    "config":{
                        "resource":{"id":"local-llm","required_capability":"openai_tool_calling"},
                        "model":"m","messages":[]
                    }
                }],
                "edges":[]
            }"#,
        )
        .unwrap();
        let registry = ResourceRegistry::new();
        let err = super::load_in_registry(tmp.path(), id, &registry).unwrap_err();
        match err {
            super::WorkflowsError::CapabilityNotAdvertised {
                node_id,
                resource_id,
                capability,
            } => {
                assert_eq!(node_id, "n1");
                assert_eq!(resource_id, "local-llm");
                assert!(capability.contains("OpenaiToolCalling"), "got {capability}");
            },
            other => panic!("expected CapabilityNotAdvertised, got {other:?}"),
        }
    }

    #[test]
    fn load_in_registry_rejects_empty_advertised_capabilities_with_required_cap() {
        // Tightening: an empty advertised_capabilities list used to act as
        // a wildcard for required_capability checks. Now strict — if you
        // ask for a capability, the resource must explicitly advertise it.
        let tmp = TempDir::new().unwrap();
        let id = "wf-empty-caps";
        let dir = tmp.path().join("workflows");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{id}.json"));
        std::fs::write(
            &path,
            r#"{
                "id":"wf-empty-caps","name":"x",
                "resources":[{
                    "id":"untyped-llm",
                    "kind":"http_endpoint",
                    "probe":{"kind":"http","ports":[7777]},
                    "override_lower_scope":false
                }],
                "nodes":[{"id":"n1","type":"llm","name":"x",
                    "config":{
                        "resource":{"id":"untyped-llm","required_capability":"openai_chat_completions"},
                        "model":"m","messages":[]
                    }
                }],
                "edges":[]
            }"#,
        )
        .unwrap();
        let registry = ResourceRegistry::new();
        let err = super::load_in_registry(tmp.path(), id, &registry).unwrap_err();
        match err {
            super::WorkflowsError::CapabilityNotAdvertised {
                node_id,
                resource_id,
                ..
            } => {
                assert_eq!(node_id, "n1");
                assert_eq!(resource_id, "untyped-llm");
            },
            other => panic!("expected CapabilityNotAdvertised, got {other:?}"),
        }
    }

    #[test]
    fn load_in_registry_allows_untyped_resource_via_bare_ref() {
        // The "untyped" escape hatch: a bare ResourceRef against a resource
        // with empty advertised_capabilities resolves cleanly. The strict
        // capability check only fires for explicit required_capability asks.
        let tmp = TempDir::new().unwrap();
        let id = "wf-untyped-ok";
        let dir = tmp.path().join("workflows");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{id}.json"));
        std::fs::write(
            &path,
            r#"{
                "id":"wf-untyped-ok","name":"x",
                "resources":[{
                    "id":"untyped-svc",
                    "kind":"http_endpoint",
                    "probe":{"kind":"http","ports":[8080]},
                    "override_lower_scope":false
                }],
                "nodes":[{"id":"n1","type":"http","name":"ping",
                    "config":{"resource":"untyped-svc","path":"/health","method":"GET"}
                }],
                "edges":[]
            }"#,
        )
        .unwrap();
        let registry = ResourceRegistry::new();
        let (_wf, warnings) = super::load_in_registry(tmp.path(), id, &registry).unwrap();
        assert!(warnings.is_empty(), "warnings: {warnings:?}");
    }

    #[test]
    fn load_in_registry_rejects_invalid_origin_value() {
        let tmp = TempDir::new().unwrap();
        let id = "wf-bad-origin";
        let dir = tmp.path().join("workflows");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{id}.json"));
        std::fs::write(
            &path,
            r#"{
                "id":"wf-bad-origin","name":"x",
                "nodes":[{"id":"n1","type":"http","name":"x",
                    "config":{"url":"http://example.com","origin":"sideways"}
                }],
                "edges":[]
            }"#,
        )
        .unwrap();
        let registry = ResourceRegistry::new();
        let err = super::load_in_registry(tmp.path(), id, &registry).unwrap_err();
        match err {
            super::WorkflowsError::InvalidNodeConfig { node_id, reason } => {
                assert_eq!(node_id, "n1");
                assert!(reason.contains("origin"), "got {reason}");
            },
            other => panic!("expected InvalidNodeConfig, got {other:?}"),
        }
    }

    #[test]
    fn load_in_registry_warns_on_localhost_url_with_remote_target_env() {
        let tmp = TempDir::new().unwrap();
        let id = "wf-lint";
        let dir = tmp.path().join("workflows");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{id}.json"));
        std::fs::write(
            &path,
            r#"{
                "id":"wf-lint","name":"x",
                "nodes":[{
                    "id":"h1","type":"http","name":"ping",
                    "target_env":"wsl:Ubuntu",
                    "config":{"url":"http://127.0.0.1:11434/api/version","method":"GET"}
                }],
                "edges":[]
            }"#,
        )
        .unwrap();
        let registry = ResourceRegistry::new();
        let (_wf, warnings) = super::load_in_registry(tmp.path(), id, &registry).unwrap();
        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].node_id, "h1");
        assert!(matches!(
            warnings[0].kind,
            super::WorkflowWarningKind::LoopbackUrlInRemoteEnv
        ));
    }

    #[test]
    fn load_in_registry_does_not_warn_when_target_env_is_local() {
        let tmp = TempDir::new().unwrap();
        let id = "wf-lint-local";
        let dir = tmp.path().join("workflows");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{id}.json"));
        std::fs::write(
            &path,
            r#"{
                "id":"wf-lint-local","name":"x",
                "nodes":[{
                    "id":"h1","type":"http","name":"ping",
                    "target_env":"local",
                    "config":{"url":"http://127.0.0.1:11434/api/version","method":"GET"}
                }],
                "edges":[]
            }"#,
        )
        .unwrap();
        let registry = ResourceRegistry::new();
        let (_wf, warnings) = super::load_in_registry(tmp.path(), id, &registry).unwrap();
        assert!(warnings.is_empty(), "got warnings: {warnings:?}");
    }

    #[test]
    fn load_in_registry_does_not_warn_when_target_env_absent() {
        let tmp = TempDir::new().unwrap();
        let id = "wf-lint-untargeted";
        let dir = tmp.path().join("workflows");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{id}.json"));
        std::fs::write(
            &path,
            r#"{
                "id":"wf-lint-untargeted","name":"x",
                "nodes":[{
                    "id":"h1","type":"http","name":"ping",
                    "config":{"url":"http://127.0.0.1:11434/api/version","method":"GET"}
                }],
                "edges":[]
            }"#,
        )
        .unwrap();
        let registry = ResourceRegistry::new();
        let (_wf, warnings) = super::load_in_registry(tmp.path(), id, &registry).unwrap();
        assert!(warnings.is_empty(), "got warnings: {warnings:?}");
    }

    #[test]
    fn load_in_registry_validation_failure_rolls_back_workflow_scope() {
        // The resource is declared in the workflow scope (visible to validation)
        // but the node refers to an unknown id, so validation fails. After the
        // failure, the workflow scope MUST be removed from the registry.
        let tmp = TempDir::new().unwrap();
        let id = "wf-rollback";
        let dir = tmp.path().join("workflows");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{id}.json"));
        std::fs::write(
            &path,
            r#"{
                "id":"wf-rollback","name":"x",
                "resources":[{
                    "id":"declared",
                    "kind":"http_endpoint",
                    "advertised_capabilities":[],
                    "probe":{"kind":"http","ports":[1111]},
                    "override_lower_scope":false
                }],
                "nodes":[{"id":"n1","type":"llm","name":"x","config":{"resource":"missing","model":"m","messages":[]}}],
                "edges":[]
            }"#,
        )
        .unwrap();
        let registry = ResourceRegistry::new();
        let err = super::load_in_registry(tmp.path(), id, &registry).unwrap_err();
        assert!(matches!(
            err,
            super::WorkflowsError::ResourceNotInRegistry { .. }
        ));
        let snap = registry.snapshot();
        assert!(
            !snap
                .layers
                .contains_key(&crate::environment::runtime::ScopeKey::Workflow {
                    id: crate::environment::runtime::WorkflowId("wf-rollback".into())
                }),
            "workflow scope must be rolled back on validation failure"
        );
    }

    #[test]
    fn load_in_registry_failure_preserves_prior_valid_scope() {
        // 1. Initial load of `wf-preserve` succeeds with resource `keep`
        //    declared and a node that references it.
        // 2. Overwrite the workflow file with a version that still declares
        //    `keep` but routes the node to an unknown id `missing`.
        // 3. Reload — validation fails — the registry MUST still resolve
        //    `keep` for the workflow id (the prior scope is preserved).
        let tmp = TempDir::new().unwrap();
        let id = "wf-preserve";
        let dir = tmp.path().join("workflows");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{id}.json"));

        let good_body = r#"{
            "id":"wf-preserve","name":"x",
            "resources":[{
                "id":"keep",
                "kind":"http_endpoint",
                "advertised_capabilities":[],
                "probe":{"kind":"http","ports":[2222]},
                "override_lower_scope":false
            }],
            "nodes":[{"id":"n1","type":"llm","name":"x","config":{"resource":"keep","model":"m","messages":[]}}],
            "edges":[]
        }"#;
        std::fs::write(&path, good_body).unwrap();

        let registry = ResourceRegistry::new();
        super::load_in_registry(tmp.path(), id, &registry).expect("initial load ok");

        // Sanity: the prior scope resolves `keep`.
        let wf_id = crate::environment::runtime::WorkflowId(id.into());
        let snap = registry.snapshot();
        assert!(
            snap.resolve(
                &crate::environment::runtime::ResourceId("keep".into()),
                &crate::environment::runtime::EnvId::local(),
                Some(&wf_id),
            )
            .is_some(),
            "initial load made `keep` resolvable"
        );

        // Now rewrite with a node referring to a non-existent resource id.
        let bad_body = r#"{
            "id":"wf-preserve","name":"x",
            "resources":[{
                "id":"keep",
                "kind":"http_endpoint",
                "advertised_capabilities":[],
                "probe":{"kind":"http","ports":[2222]},
                "override_lower_scope":false
            }],
            "nodes":[{"id":"n1","type":"llm","name":"x","config":{"resource":"missing","model":"m","messages":[]}}],
            "edges":[]
        }"#;
        std::fs::write(&path, bad_body).unwrap();

        let err = super::load_in_registry(tmp.path(), id, &registry).unwrap_err();
        assert!(matches!(
            err,
            super::WorkflowsError::ResourceNotInRegistry { .. }
        ));

        // The prior scope must still resolve `keep`. Old buggy behaviour
        // would have wiped the scope (`resolve` returns `None`).
        let snap = registry.snapshot();
        let (def, scope) = snap
            .resolve(
                &crate::environment::runtime::ResourceId("keep".into()),
                &crate::environment::runtime::EnvId::local(),
                Some(&wf_id),
            )
            .expect("prior `keep` still visible after failed reload");
        assert!(matches!(
            scope,
            crate::environment::runtime::ScopeKey::Workflow { .. }
        ));
        assert_eq!(def.id.0, "keep");
    }

    #[test]
    fn load_in_registry_failure_clears_scope_when_no_prior_existed() {
        // First-ever load fails validation — there's no prior scope to
        // restore, so the registry MUST have NO scope for the workflow id.
        let tmp = TempDir::new().unwrap();
        let id = "wf-no-prior";
        let dir = tmp.path().join("workflows");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{id}.json"));
        std::fs::write(
            &path,
            r#"{
                "id":"wf-no-prior","name":"x",
                "resources":[{
                    "id":"declared",
                    "kind":"http_endpoint",
                    "advertised_capabilities":[],
                    "probe":{"kind":"http","ports":[3333]},
                    "override_lower_scope":false
                }],
                "nodes":[{"id":"n1","type":"llm","name":"x","config":{"resource":"missing","model":"m","messages":[]}}],
                "edges":[]
            }"#,
        )
        .unwrap();
        let registry = ResourceRegistry::new();
        let err = super::load_in_registry(tmp.path(), id, &registry).unwrap_err();
        assert!(matches!(
            err,
            super::WorkflowsError::ResourceNotInRegistry { .. }
        ));
        let snap = registry.snapshot();
        assert!(
            !snap
                .layers
                .contains_key(&crate::environment::runtime::ScopeKey::Workflow {
                    id: crate::environment::runtime::WorkflowId(id.into())
                }),
            "failed first-ever load rolls back to no-scope state"
        );
    }
}
