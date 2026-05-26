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
        }
    }

    #[test]
    fn load_in_registry_installs_workflow_scope() {
        let home = TempDir::new().unwrap();
        let reg = ResourceRegistry::new();
        let wf = wf_with_resources("my-wf");
        super::save(home.path(), &wf).expect("save");
        let _loaded = super::load_in_registry(home.path(), "my-wf", &reg).expect("load");

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
