use super::*;
use std::path::PathBuf;

struct Fixture {
    vars: HashMap<String, String>,
    upstream_outputs: HashMap<(String, String), PortValue>,
    current_inputs: HashMap<String, PortValue>,
    current_config: HashMap<String, serde_json::Value>,
    env_allowlist: HashSet<String>,
    env_map: HashMap<String, String>,
    workspace: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        Self {
            vars: HashMap::new(),
            upstream_outputs: HashMap::new(),
            current_inputs: HashMap::new(),
            current_config: HashMap::new(),
            env_allowlist: default_env_allowlist(),
            env_map: HashMap::new(),
            workspace: PathBuf::from("/tmp/run-XYZ"),
        }
    }

    fn with_env(mut self, name: &str, value: &str) -> Self {
        self.env_map.insert(name.into(), value.into());
        self
    }
}

fn empty_secrets(_: &str) -> Option<String> {
    None
}

fn empty_kv(_: &str) -> Option<String> {
    None
}

fn empty_resources(_: &str, _: &str) -> Option<String> {
    None
}

fn ctx_for<'a>(
    f: &'a Fixture,
    secrets: &'a dyn Fn(&str) -> Option<String>,
    kv: &'a dyn Fn(&str) -> Option<String>,
    env: &'a dyn Fn(&str) -> Option<String>,
) -> SubstitutionContext<'a> {
    ctx_for_with_resources(f, secrets, kv, env, &empty_resources)
}

fn ctx_for_with_resources<'a>(
    f: &'a Fixture,
    secrets: &'a dyn Fn(&str) -> Option<String>,
    kv: &'a dyn Fn(&str) -> Option<String>,
    env: &'a dyn Fn(&str) -> Option<String>,
    resources: &'a dyn Fn(&str, &str) -> Option<String>,
) -> SubstitutionContext<'a> {
    SubstitutionContext {
        vars: &f.vars,
        secrets,
        upstream_outputs: &f.upstream_outputs,
        current_inputs: &f.current_inputs,
        current_config: &f.current_config,
        kv,
        env,
        env_allowlist: &f.env_allowlist,
        resources,
        run_id: "run-1",
        workspace: &f.workspace,
        started_at_iso: "2026-05-19T12:00:00Z",
        workflow_id: "wf-demo",
        workflow_name: "Demo",
    }
}

fn map_env(map: &HashMap<String, String>) -> impl Fn(&str) -> Option<String> + '_ {
    move |name| map.get(name).cloned()
}

#[test]
fn passes_through_text_without_braces() {
    let f = Fixture::new();
    let env = map_env(&f.env_map);
    let s = substitute("plain text", &ctx_for(&f, &empty_secrets, &empty_kv, &env)).unwrap();
    assert_eq!(s, "plain text");
}

#[test]
fn vars_resolve() {
    let mut f = Fixture::new();
    f.vars.insert("greet".into(), "hi".into());
    let env = map_env(&f.env_map);
    let s = substitute(
        "{{vars.greet}}, world",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "hi, world");
}

#[test]
fn secrets_resolve_via_closure() {
    let f = Fixture::new();
    let secrets = |name: &str| {
        if name == "TOKEN" {
            Some("abc".into())
        } else {
            None
        }
    };
    let env = map_env(&f.env_map);
    let s = substitute(
        "k={{secrets.TOKEN}}",
        &ctx_for(&f, &secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "k=abc");
}

#[test]
fn upstream_outputs_resolve() {
    let mut f = Fixture::new();
    f.upstream_outputs.insert(
        ("n1".into(), "text".into()),
        PortValue::String("hello".into()),
    );
    let env = map_env(&f.env_map);
    let s = substitute(
        "got: {{nodes.n1.outputs.text}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "got: hello");
}

#[test]
fn current_inputs_resolve() {
    let mut f = Fixture::new();
    f.current_inputs
        .insert("prompt".into(), PortValue::String("write a haiku".into()));
    let env = map_env(&f.env_map);
    let s = substitute(
        "{{inputs.prompt}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "write a haiku");
}

#[test]
fn current_config_resolves() {
    let mut f = Fixture::new();
    f.current_config
        .insert("steps".into(), serde_json::json!(7));
    let env = map_env(&f.env_map);
    let s = substitute(
        "iters={{config.steps}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "iters=7");
}

#[test]
fn kv_resolves_via_closure() {
    let f = Fixture::new();
    let kv = |key: &str| {
        if key == "lastrun" {
            Some("2026-05-19".into())
        } else {
            None
        }
    };
    let env = map_env(&f.env_map);
    let s = substitute("{{kv.lastrun}}", &ctx_for(&f, &empty_secrets, &kv, &env)).unwrap();
    assert_eq!(s, "2026-05-19");
}

#[test]
fn run_namespace_resolves() {
    let f = Fixture::new();
    let env = map_env(&f.env_map);
    let s = substitute(
        "run={{run.id}} ws={{run.workspace}} at={{run.startedAt}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "run=run-1 ws=/tmp/run-XYZ at=2026-05-19T12:00:00Z");
}

#[test]
fn workflow_namespace_resolves() {
    let f = Fixture::new();
    let env = map_env(&f.env_map);
    let s = substitute(
        "{{workflow.id}}/{{workflow.name}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "wf-demo/Demo");
}

#[test]
fn env_allowlisted_resolves() {
    let f = Fixture::new().with_env("HOME", "/home/test");
    let env = map_env(&f.env_map);
    let s = substitute(
        "{{env.HOME}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "/home/test");
}

#[test]
fn env_path_is_blocked_by_allowlist() {
    let f = Fixture::new().with_env("PATH", "/usr/bin:/bin");
    let env = map_env(&f.env_map);
    let res = substitute(
        "{{env.PATH}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    );
    match res {
        Err(TemplateError::NotAllowed(msg)) => assert!(msg.contains("PATH")),
        other => panic!("expected NotAllowed for PATH, got {other:?}"),
    }
}

#[test]
fn env_ld_preload_is_blocked() {
    let f = Fixture::new().with_env("LD_PRELOAD", "/lib/evil.so");
    let env = map_env(&f.env_map);
    let res = substitute(
        "{{env.LD_PRELOAD}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    );
    assert!(matches!(res, Err(TemplateError::NotAllowed(_))));
}

#[test]
fn deeply_nested_json_helper_is_capped() {
    let mut f = Fixture::new();
    f.vars.insert("x".into(), "v".into());
    let env = map_env(&f.env_map);
    let nested = "{{".to_string() + &"json ".repeat(64) + "vars.x}}";
    let res = substitute(&nested, &ctx_for(&f, &empty_secrets, &empty_kv, &env));
    match res {
        Err(TemplateError::Syntax(msg)) => assert!(msg.contains("nested deeper")),
        other => panic!("expected Syntax recursion-cap error, got {other:?}"),
    }
}

#[test]
fn env_ordius_user_prefix_allowed() {
    let f = Fixture::new().with_env("ORDIUS_USER_TIMEZONE", "UTC");
    let env = map_env(&f.env_map);
    let s = substitute(
        "{{env.ORDIUS_USER_TIMEZONE}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "UTC");
}

#[test]
fn env_allowlisted_but_unset_is_undefined() {
    let f = Fixture::new();
    let env = map_env(&f.env_map);
    let res = substitute(
        "{{env.HOME}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    );
    assert!(matches!(res, Err(TemplateError::Undefined(_))));
}

#[test]
fn undefined_var_fails_loud() {
    let f = Fixture::new();
    let env = map_env(&f.env_map);
    let res = substitute(
        "{{vars.missing}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    );
    assert_eq!(res, Err(TemplateError::Undefined("vars.missing".into())));
}

#[test]
fn unclosed_braces_are_syntax_error() {
    let f = Fixture::new();
    let env = map_env(&f.env_map);
    let res = substitute(
        "oops {{vars.x",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    );
    assert!(matches!(res, Err(TemplateError::Syntax(_))));
}

#[test]
fn unknown_namespace_is_syntax_error() {
    let f = Fixture::new();
    let env = map_env(&f.env_map);
    let res = substitute("{{magic.x}}", &ctx_for(&f, &empty_secrets, &empty_kv, &env));
    assert!(matches!(res, Err(TemplateError::Syntax(_))));
}

#[test]
fn multiple_substitutions_in_one_template() {
    let mut f = Fixture::new();
    f.vars.insert("a".into(), "one".into());
    f.vars.insert("b".into(), "two".into());
    let env = map_env(&f.env_map);
    let s = substitute(
        "{{vars.a}}+{{vars.b}}={{vars.a}}{{vars.b}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "one+two=onetwo");
}

#[test]
fn json_helper_escapes_quotes() {
    let mut f = Fixture::new();
    f.current_inputs
        .insert("prompt".into(), PortValue::String("hi \"world\"".into()));
    let env = map_env(&f.env_map);
    let s = substitute(
        "{{json inputs.prompt}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "\"hi \\\"world\\\"\"");
}

#[test]
fn coerces_number_to_decimal() {
    let mut f = Fixture::new();
    f.current_inputs.insert("n".into(), PortValue::Number(2.5));
    let env = map_env(&f.env_map);
    let s = substitute(
        "v={{inputs.n}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "v=2.5");
}

#[test]
fn coerces_boolean_to_word() {
    let mut f = Fixture::new();
    f.current_inputs
        .insert("ok".into(), PortValue::Boolean(true));
    let env = map_env(&f.env_map);
    let s = substitute(
        "{{inputs.ok}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "true");
}

#[test]
fn coerces_json_to_compact_string() {
    let mut f = Fixture::new();
    f.current_inputs
        .insert("obj".into(), PortValue::Json(serde_json::json!({"a": 1})));
    let env = map_env(&f.env_map);
    let s = substitute(
        "{{inputs.obj}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, r#"{"a":1}"#);
}

#[test]
fn coerces_vector_to_json_array() {
    let mut f = Fixture::new();
    f.current_inputs
        .insert("emb".into(), PortValue::Vector(vec![1.0, 2.5]));
    let env = map_env(&f.env_map);
    let s = substitute(
        "{{inputs.emb}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "[1.0,2.5]");
}

#[test]
fn file_value_is_path_string() {
    let mut f = Fixture::new();
    f.current_inputs
        .insert("img".into(), PortValue::File("/tmp/x.png".into()));
    let env = map_env(&f.env_map);
    let s = substitute(
        "{{inputs.img}}",
        &ctx_for(&f, &empty_secrets, &empty_kv, &env),
    )
    .unwrap();
    assert_eq!(s, "/tmp/x.png");
}

#[test]
fn resource_namespace_resolves_base_url() {
    let f = Fixture::new();
    let env = map_env(&f.env_map);
    let resources = |id: &str, field: &str| -> Option<String> {
        match (id, field) {
            ("openai", "base_url") => Some("https://api.openai.com/v1".into()),
            _ => None,
        }
    };
    let s = substitute(
        "URL = {{resource.openai.base_url}}",
        &ctx_for_with_resources(&f, &empty_secrets, &empty_kv, &env, &resources),
    )
    .unwrap();
    assert_eq!(s, "URL = https://api.openai.com/v1");
}

#[test]
fn resource_namespace_missing_field_errors_undefined() {
    let f = Fixture::new();
    let env = map_env(&f.env_map);
    let resources = |_id: &str, _field: &str| -> Option<String> { None };
    let err = substitute(
        "{{resource.x.y}}",
        &ctx_for_with_resources(&f, &empty_secrets, &empty_kv, &env, &resources),
    )
    .unwrap_err();
    assert!(matches!(err, TemplateError::Undefined(_)));
}

#[test]
fn resource_namespace_missing_field_syntax_errors() {
    let f = Fixture::new();
    let env = map_env(&f.env_map);
    let resources = |_id: &str, _field: &str| -> Option<String> { None };
    let err = substitute(
        "{{resource.x}}",
        &ctx_for_with_resources(&f, &empty_secrets, &empty_kv, &env, &resources),
    )
    .unwrap_err();
    assert!(matches!(err, TemplateError::Syntax(_)));
}

#[test]
fn resource_namespace_supports_dotted_ids() {
    let f = Fixture::new();
    let env = map_env(&f.env_map);
    let resources = |id: &str, field: &str| -> Option<String> {
        match (id, field) {
            ("openai.gpt-4", "base_url") => Some("https://api.openai.com/v1".into()),
            _ => None,
        }
    };
    let s = substitute(
        "URL = {{resource.openai.gpt-4.base_url}}",
        &ctx_for_with_resources(&f, &empty_secrets, &empty_kv, &env, &resources),
    )
    .unwrap();
    assert_eq!(s, "URL = https://api.openai.com/v1");
}

#[test]
fn resource_namespace_rejects_empty_id_with_trailing_field() {
    let f = Fixture::new();
    let env = map_env(&f.env_map);
    let resources = |_id: &str, _field: &str| -> Option<String> { None };
    let err = substitute(
        "{{resource..base_url}}",
        &ctx_for_with_resources(&f, &empty_secrets, &empty_kv, &env, &resources),
    )
    .unwrap_err();
    assert!(matches!(err, TemplateError::Syntax(_)));
}

mod run_snapshot_resolver {
    //! Unit coverage for [`super::build_run_snapshot_resources_resolver`]:
    //! `base_url` and `version` come from the per-env `RunCatalog`; `id`
    //! and `kind` come from the per-run frozen `RegistryInner`; the `kind`
    //! spelling matches the serde wire form (`http_endpoint`, not
    //! `httpendpoint`).

    use std::collections::HashMap;
    use std::sync::Arc;

    use chrono::Utc;

    use crate::environment::runtime::ResourceCatalog;
    use crate::environment::runtime::catalog::{ResourceDetail, ResourceProbeOutcome};
    use crate::environment::runtime::env::{EnvId, WorkflowId};
    use crate::environment::runtime::registry::{RegistryInner, ScopeKey};
    use crate::environment::runtime::resource::{
        ProbeSpec, ResourceDefinition, ResourceId, ResourceKind,
    };
    use crate::environment::runtime::run_catalog::RunCatalog;
    use crate::template::build_run_snapshot_resources_resolver;

    /// Build a registry whose `Workflow` scope holds one definition keyed
    /// to the given `kind`.
    fn registry_with_def(id: &str, kind: ResourceKind, wf: &WorkflowId) -> Arc<RegistryInner> {
        let def = ResourceDefinition {
            id: ResourceId(id.to_string()),
            kind,
            advertised_capabilities: Vec::new(),
            probe: ProbeSpec::Http {
                ports: vec![1234],
                routes: Vec::new(),
                timeout_ms: None,
            },
            override_lower_scope: false,
        };
        let mut layers = HashMap::new();
        layers.insert(
            ScopeKey::Workflow { id: wf.clone() },
            HashMap::from([(def.id.clone(), def)]),
        );
        Arc::new(RegistryInner {
            revision: 1,
            layers,
        })
    }

    /// Build a `RunCatalog` whose frozen view holds one resource outcome.
    fn catalog_with_outcome(
        env: EnvId,
        id: ResourceId,
        outcome: ResourceProbeOutcome,
    ) -> Arc<RunCatalog> {
        let mut resources = HashMap::new();
        resources.insert(id, outcome);
        let frozen = Arc::new(ResourceCatalog {
            env_id: env.clone(),
            registry_revision: 1,
            probed_at: Utc::now(),
            resources,
        });
        Arc::new(RunCatalog::new(env, frozen))
    }

    fn http_endpoint_outcome(base_url: &str, version: Option<&str>) -> ResourceProbeOutcome {
        ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint {
            base_url: base_url.to_string(),
            routes_by_capability: HashMap::new(),
            version: version.map(str::to_string),
            models_list: None,
            auth_secret_ref: None,
            streaming_supported_natively: false,
            route_origin: crate::environment::runtime::catalog::RouteOrigin::EnvLoopback,
        })
    }

    #[test]
    fn base_url_reads_from_catalog_not_registry() {
        let wf = WorkflowId("wf".to_string());
        let env = EnvId::local();
        let id = ResourceId("ollama".to_string());

        // Registry advertises probe port 1234, but catalog says the
        // proven base_url is :1111 — the resolver must take the
        // catalog's view, not synthesize `http://127.0.0.1:1234`.
        let registry = registry_with_def("ollama", ResourceKind::HttpEndpoint, &wf);
        let catalog = catalog_with_outcome(
            env.clone(),
            id,
            http_endpoint_outcome("http://x:1111", None),
        );
        let catalogs = Arc::new(HashMap::from([(env.clone(), catalog)]));

        let resolver = build_run_snapshot_resources_resolver(registry, wf, env, catalogs);
        assert_eq!(
            resolver("ollama", "base_url").as_deref(),
            Some("http://x:1111")
        );
    }

    #[test]
    fn base_url_returns_none_for_unprobed_resource() {
        let wf = WorkflowId("wf".to_string());
        let env = EnvId::local();
        let registry = registry_with_def("ollama", ResourceKind::HttpEndpoint, &wf);
        // Empty catalog map for this env — no probe outcome at all.
        let catalogs = Arc::new(HashMap::new());

        let resolver = build_run_snapshot_resources_resolver(registry, wf, env, catalogs);
        assert_eq!(resolver("ollama", "base_url"), None);
    }

    #[test]
    fn version_reads_from_catalog_http_endpoint() {
        let wf = WorkflowId("wf".to_string());
        let env = EnvId::local();
        let id = ResourceId("ollama".to_string());

        let registry = registry_with_def("ollama", ResourceKind::HttpEndpoint, &wf);
        let catalog = catalog_with_outcome(
            env.clone(),
            id,
            http_endpoint_outcome("http://x:1111", Some("0.4.3")),
        );
        let catalogs = Arc::new(HashMap::from([(env.clone(), catalog)]));

        let resolver = build_run_snapshot_resources_resolver(registry, wf, env, catalogs);
        assert_eq!(resolver("ollama", "version").as_deref(), Some("0.4.3"));
    }

    #[test]
    fn id_reads_from_registry_definition() {
        let wf = WorkflowId("wf".to_string());
        let env = EnvId::local();
        let registry = registry_with_def("my-llm", ResourceKind::HttpEndpoint, &wf);
        let catalogs = Arc::new(HashMap::new());

        let resolver = build_run_snapshot_resources_resolver(registry, wf, env, catalogs);
        assert_eq!(resolver("my-llm", "id").as_deref(), Some("my-llm"));
    }

    #[test]
    fn kind_uses_serde_snake_case_spelling() {
        // Regression for the preamble item 16 + plan note: `{:?}.lowercase()`
        // would render `httpendpoint`, breaking workflow-JSON round-trips.
        // Serde's `rename_all = "snake_case"` on `ResourceKind` is the
        // authoritative spelling.
        let wf = WorkflowId("wf".to_string());
        let env = EnvId::local();
        let registry = registry_with_def("foo", ResourceKind::HttpEndpoint, &wf);
        let catalogs = Arc::new(HashMap::new());

        let resolver = build_run_snapshot_resources_resolver(registry, wf, env, catalogs);
        assert_eq!(resolver("foo", "kind").as_deref(), Some("http_endpoint"));
    }

    #[test]
    fn kind_for_binary_and_toolchain() {
        let wf = WorkflowId("wf".to_string());
        let env = EnvId::local();
        let catalogs = Arc::new(HashMap::new());

        let bin_registry = registry_with_def("rg", ResourceKind::Binary, &wf);
        let resolver_bin = build_run_snapshot_resources_resolver(
            bin_registry,
            wf.clone(),
            env.clone(),
            Arc::clone(&catalogs),
        );
        assert_eq!(resolver_bin("rg", "kind").as_deref(), Some("binary"));

        let tc_registry = registry_with_def("rustc", ResourceKind::Toolchain, &wf);
        let resolver_tc = build_run_snapshot_resources_resolver(tc_registry, wf, env, catalogs);
        assert_eq!(resolver_tc("rustc", "kind").as_deref(), Some("toolchain"));
    }

    #[test]
    fn unknown_field_returns_none() {
        let wf = WorkflowId("wf".to_string());
        let env = EnvId::local();
        let registry = registry_with_def("ollama", ResourceKind::HttpEndpoint, &wf);
        let catalogs = Arc::new(HashMap::new());

        let resolver = build_run_snapshot_resources_resolver(registry, wf, env, catalogs);
        assert_eq!(resolver("ollama", "no_such_field"), None);
    }

    #[test]
    fn unknown_resource_id_returns_none() {
        let wf = WorkflowId("wf".to_string());
        let env = EnvId::local();
        let registry = registry_with_def("ollama", ResourceKind::HttpEndpoint, &wf);
        let catalogs = Arc::new(HashMap::new());

        let resolver = build_run_snapshot_resources_resolver(registry, wf, env, catalogs);
        assert_eq!(resolver("nope", "id"), None);
        assert_eq!(resolver("nope", "kind"), None);
        assert_eq!(resolver("nope", "base_url"), None);
        assert_eq!(resolver("nope", "version"), None);
    }
}
