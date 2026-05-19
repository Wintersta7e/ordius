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

fn ctx_for<'a>(
    f: &'a Fixture,
    secrets: &'a dyn Fn(&str) -> Option<String>,
    kv: &'a dyn Fn(&str) -> Option<String>,
    env: &'a dyn Fn(&str) -> Option<String>,
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
        Err(TemplateError::Undefined(msg)) => assert!(msg.contains("PATH")),
        other => panic!("expected Undefined for PATH, got {other:?}"),
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
    assert!(matches!(res, Err(TemplateError::Undefined(_))));
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
