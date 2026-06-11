//! Template substitution engine for `{{namespace.key}}` forms.
//!
//! Non-Turing: no loops or conditionals. Every reference must
//! resolve at substitution time; undefined references produce a
//! loud [`TemplateError::Undefined`] rather than a silent empty
//! string.

use crate::types::PortValue;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use thiserror::Error;

const ORDIUS_USER_PREFIX: &str = "ORDIUS_USER_";

/// Maximum nesting depth of `{{json X}}` expressions. A malformed
/// or adversarial template like `{{json json json … X}}` would
/// otherwise recurse arbitrarily deep on a stack with no other
/// bound. 16 is well above any legitimate use.
const MAX_JSON_HELPER_DEPTH: usize = 16;

/// Default env-var allowlist for `{{env.NAME}}` resolution.
///
/// `PATH` is deliberately excluded — substituting `PATH` into a
/// shell command is an injection vector. The executor uses its
/// own inherited `PATH` instead. Names matching the
/// `ORDIUS_USER_*` prefix are always allowed (checked by the
/// substituter, not present in this set).
#[must_use]
pub fn default_env_allowlist() -> HashSet<String> {
    ["HOME", "USERPROFILE", "LANG", "TZ"]
        .iter()
        .map(|s| (*s).to_string())
        .collect()
}

/// Boxed resolver for `{{resource.<id>.<field>}}` references.
///
/// Call sites that build a closure conditionally
/// (`build_run_snapshot_resources_resolver(...)` on the run-loop path vs
/// a noop fallback) store the result in this type to satisfy
/// `clippy::type_complexity`.
pub type BoxedResourceResolver = Box<dyn Fn(&str, &str) -> Option<String> + Send + Sync>;

/// Per-substitution context.
///
/// Every field is a borrow — the substituter never owns its
/// inputs. Callers populate `secrets` and `kv` with closures
/// wrapping the OS keyring and the `SQLite` kv store
/// respectively.
pub struct SubstitutionContext<'a> {
    /// Workflow variables.
    pub vars: &'a HashMap<String, String>,
    /// Resolver for `{{secrets.NAME}}` — typically wraps the OS keyring.
    pub secrets: &'a dyn Fn(&str) -> Option<String>,
    /// Upstream outputs collected so far, keyed by `(node_id, port_name)`.
    pub upstream_outputs: &'a HashMap<(String, String), PortValue>,
    /// Inputs wired into the current node by the run-loop.
    pub current_inputs: &'a HashMap<String, PortValue>,
    /// Current node's config map (`{{config.X}}` resolves here).
    pub current_config: &'a HashMap<String, serde_json::Value>,
    /// Resolver for `{{kv.KEY}}`.
    pub kv: &'a dyn Fn(&str) -> Option<String>,
    /// Resolver for `{{env.NAME}}` — production callers wrap
    /// `std::env::var(name).ok()`. The allowlist is enforced
    /// before the resolver is called, so resolvers see only
    /// permitted names.
    pub env: &'a dyn Fn(&str) -> Option<String>,
    /// Allowlist for `{{env.NAME}}` resolution.
    pub env_allowlist: &'a HashSet<String>,
    /// Resolver for `{{resource.<id>.<field>}}` — returns the field
    /// value (Phase D supports `base_url` only) or `None` if the id
    /// or field is unknown. Production callers wrap the engine's
    /// `ResourceRegistry` snapshot at substitution time.
    pub resources: &'a dyn Fn(&str, &str) -> Option<String>,
    /// Run id (`{{run.id}}`).
    pub run_id: &'a str,
    /// Run workspace directory (`{{run.workspace}}`).
    pub workspace: &'a Path,
    /// ISO-8601 run start time (`{{run.startedAt}}`).
    pub started_at_iso: &'a str,
    /// Workflow id (`{{workflow.id}}`).
    pub workflow_id: &'a str,
    /// Workflow name (`{{workflow.name}}`).
    pub workflow_name: &'a str,
}

/// Failure modes for [`substitute`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TemplateError {
    /// A reference could not be resolved (name not present in
    /// the supplied data).
    #[error("undefined: {0}")]
    Undefined(String),
    /// Template surface syntax violation (unclosed `{{`, unknown
    /// namespace, malformed expression, recursion limit hit).
    #[error("syntax: {0}")]
    Syntax(String),
    /// Reference targeted a name the policy explicitly blocks
    /// (e.g. `{{env.PATH}}` when `PATH` isn't on the allowlist).
    #[error("not allowed: {0}")]
    NotAllowed(String),
}

/// Substitute every `{{...}}` in `tmpl` using `ctx`.
///
/// Supported forms:
/// - `{{vars.X}}` / `{{secrets.X}}` / `{{kv.X}}` / `{{env.X}}`
/// - `{{inputs.PORT}}` / `{{config.KEY}}`
/// - `{{nodes.NID.outputs.PORT}}`
/// - `{{run.id}}` / `{{run.workspace}}` / `{{run.startedAt}}`
/// - `{{workflow.id}}` / `{{workflow.name}}`
/// - `{{resource.<id>.<field>}}` — registry-resolved resource field
///   (Phase D supports `base_url` only)
/// - `{{json EXPR}}` — JSON-escape the resolved expression
///
/// Undefined references fail loud — they never silently render
/// as the empty string.
pub fn substitute(tmpl: &str, ctx: &SubstitutionContext<'_>) -> Result<String, TemplateError> {
    let mut out = String::new();
    let mut remaining = tmpl;
    while let Some(start) = remaining.find("{{") {
        out.push_str(&remaining[..start]);
        let after_open = &remaining[start + 2..];
        let end = after_open
            .find("}}")
            .ok_or_else(|| TemplateError::Syntax("unclosed `{{`".into()))?;
        let inner = after_open[..end].trim();
        let value = resolve(inner, ctx, 0)?;
        out.push_str(&value);
        remaining = &after_open[end + 2..];
    }
    out.push_str(remaining);
    Ok(out)
}

fn resolve(
    expr: &str,
    ctx: &SubstitutionContext<'_>,
    depth: usize,
) -> Result<String, TemplateError> {
    if let Some(rest) = expr.strip_prefix("json ") {
        if depth >= MAX_JSON_HELPER_DEPTH {
            return Err(TemplateError::Syntax(format!(
                "json helper nested deeper than {MAX_JSON_HELPER_DEPTH}",
            )));
        }
        let inner = resolve(rest.trim(), ctx, depth + 1)?;
        return serde_json::to_string(&inner)
            .map_err(|e| TemplateError::Syntax(format!("json helper: {e}")));
    }
    let mut parts = expr.split('.');
    let ns = parts
        .next()
        .ok_or_else(|| TemplateError::Syntax("empty reference".into()))?;
    match ns {
        "vars" => single_key(&mut parts, "vars", |k| {
            ctx.vars
                .get(k)
                .cloned()
                .ok_or_else(|| TemplateError::Undefined(format!("vars.{k}")))
        }),
        "secrets" => single_key(&mut parts, "secrets", |k| {
            (ctx.secrets)(k).ok_or_else(|| TemplateError::Undefined(format!("secrets.{k}")))
        }),
        "kv" => single_key(&mut parts, "kv", |k| {
            (ctx.kv)(k).ok_or_else(|| TemplateError::Undefined(format!("kv.{k}")))
        }),
        "inputs" => single_key(&mut parts, "inputs", |k| {
            ctx.current_inputs
                .get(k)
                .map(port_value_to_string)
                .transpose()?
                .ok_or_else(|| TemplateError::Undefined(format!("inputs.{k}")))
        }),
        "config" => single_key(&mut parts, "config", |k| {
            ctx.current_config
                .get(k)
                .map(json_value_to_string)
                .ok_or_else(|| TemplateError::Undefined(format!("config.{k}")))
        }),
        "env" => single_key(&mut parts, "env", |k| {
            if !ctx.env_allowlist.contains(k) && !k.starts_with(ORDIUS_USER_PREFIX) {
                return Err(TemplateError::NotAllowed(format!("env.{k}")));
            }
            (ctx.env)(k).ok_or_else(|| TemplateError::Undefined(format!("env.{k}")))
        }),
        "run" => match parts.next() {
            Some("id") => no_trailing(&mut parts, "run.id").map(|()| ctx.run_id.to_string()),
            Some("workspace") => no_trailing(&mut parts, "run.workspace")
                .map(|()| ctx.workspace.display().to_string()),
            Some("startedAt") => {
                no_trailing(&mut parts, "run.startedAt").map(|()| ctx.started_at_iso.to_string())
            },
            Some(other) => Err(TemplateError::Undefined(format!("run.{other}"))),
            None => Err(TemplateError::Syntax("run.X required".into())),
        },
        "workflow" => match parts.next() {
            Some("id") => {
                no_trailing(&mut parts, "workflow.id").map(|()| ctx.workflow_id.to_string())
            },
            Some("name") => {
                no_trailing(&mut parts, "workflow.name").map(|()| ctx.workflow_name.to_string())
            },
            Some(other) => Err(TemplateError::Undefined(format!("workflow.{other}"))),
            None => Err(TemplateError::Syntax("workflow.X required".into())),
        },
        "nodes" => {
            let nid = parts
                .next()
                .ok_or_else(|| TemplateError::Syntax("nodes.NID.outputs.PORT".into()))?;
            let kw = parts
                .next()
                .ok_or_else(|| TemplateError::Syntax("nodes.NID.outputs.PORT".into()))?;
            if kw != "outputs" {
                return Err(TemplateError::Syntax(format!(
                    "nodes.{nid}.{kw}: expected 'outputs'"
                )));
            }
            let port = parts
                .next()
                .ok_or_else(|| TemplateError::Syntax("nodes.NID.outputs.PORT".into()))?;
            no_trailing(&mut parts, "nodes.NID.outputs.PORT")?;
            ctx.upstream_outputs
                .get(&(nid.to_string(), port.to_string()))
                .map(port_value_to_string)
                .transpose()?
                .ok_or_else(|| TemplateError::Undefined(format!("nodes.{nid}.outputs.{port}")))
        },
        "resource" => resolve_resource(expr, ctx),
        other => Err(TemplateError::Syntax(format!("unknown namespace: {other}"))),
    }
}

/// `{{resource.<id>.<field>}}` lookup — kept out-of-line to keep
/// the main `resolve` function under `clippy::too_many_lines`.
///
/// `expr` is the full inner reference, e.g. `resource.openai.gpt-4.base_url`.
/// Strip the `resource.` namespace prefix and right-split on `.` so the
/// last segment is the field and everything before it is the id — ids
/// can legitimately contain `.` (e.g. `openai.gpt-4`). The dispatcher's
/// pre-split `parts` iterator is ignored here for that reason.
fn resolve_resource(expr: &str, ctx: &SubstitutionContext<'_>) -> Result<String, TemplateError> {
    let after_ns = expr
        .strip_prefix("resource.")
        .ok_or_else(|| TemplateError::Syntax(format!("resource ref missing namespace: {expr}")))?;
    let (id, field) = after_ns
        .rsplit_once('.')
        .ok_or_else(|| TemplateError::Syntax(format!("resource ref missing field: {expr}")))?;
    if id.is_empty() {
        return Err(TemplateError::Syntax(format!(
            "resource ref missing id: {expr}"
        )));
    }
    if field.is_empty() {
        return Err(TemplateError::Syntax(format!(
            "resource ref missing field: {expr}"
        )));
    }
    (ctx.resources)(id, field)
        .ok_or_else(|| TemplateError::Undefined(format!("resource.{id}.{field}")))
}

fn single_key<F>(
    parts: &mut std::str::Split<'_, char>,
    ns: &str,
    f: F,
) -> Result<String, TemplateError>
where
    F: FnOnce(&str) -> Result<String, TemplateError>,
{
    let key = parts
        .next()
        .ok_or_else(|| TemplateError::Syntax(format!("{ns}.NAME required")))?;
    no_trailing(parts, ns)?;
    f(key)
}

fn no_trailing(parts: &mut std::str::Split<'_, char>, what: &str) -> Result<(), TemplateError> {
    if parts.next().is_some() {
        Err(TemplateError::Syntax(format!("{what} has too many parts")))
    } else {
        Ok(())
    }
}

fn port_value_to_string(v: &PortValue) -> Result<String, TemplateError> {
    Ok(match v {
        PortValue::String(s) => s.clone(),
        PortValue::Number(n) => n.to_string(),
        PortValue::Boolean(b) => b.to_string(),
        PortValue::Json(j) => serde_json::to_string(j)
            .map_err(|e| TemplateError::Syntax(format!("encode json: {e}")))?,
        PortValue::File(p) | PortValue::Bytes(p) => p.clone(),
        PortValue::Vector(v) => serde_json::to_string(v)
            .map_err(|e| TemplateError::Syntax(format!("encode vector: {e}")))?,
    })
}

fn json_value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Recursively substitute templates inside string values in a JSON value.
///
/// Strings that don't contain `{{` are passed through unchanged (no parse
/// cost). Used by the dispatch loop to apply templates to arbitrary
/// config fields uniformly — `http.url`, `file.path`, header values,
/// etc. — without each built-in having to opt in.
pub fn substitute_in_value(
    value: serde_json::Value,
    ctx: &SubstitutionContext<'_>,
) -> Result<serde_json::Value, TemplateError> {
    match value {
        serde_json::Value::String(s) if s.contains("{{") => {
            Ok(serde_json::Value::String(substitute(&s, ctx)?))
        },
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(substitute_in_value(item, ctx)?);
            }
            Ok(serde_json::Value::Array(out))
        },
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                out.insert(k, substitute_in_value(v, ctx)?);
            }
            Ok(serde_json::Value::Object(out))
        },
        other => Ok(other),
    }
}

/// Substitute templates in every string value of a node-config map.
/// Convenience wrapper around [`substitute_in_value`] for the dispatch
/// loop, which keeps the input shape `HashMap<String, Value>`.
#[allow(clippy::implicit_hasher)]
pub fn substitute_in_config(
    config: &HashMap<String, serde_json::Value>,
    ctx: &SubstitutionContext<'_>,
) -> Result<HashMap<String, serde_json::Value>, TemplateError> {
    let mut out = HashMap::with_capacity(config.len());
    for (k, v) in config {
        out.insert(k.clone(), substitute_in_value(v.clone(), ctx)?);
    }
    Ok(out)
}

/// Build a `resources` resolver for `SubstitutionContext` that consults
/// the run-local frozen registry + per-env catalogs.
///
/// Used by the run loop so every node's `{{resource.<id>.<field>}}`
/// substitution sees the same registry view the snapshot froze at run
/// start, AT the node's effective env. The engine's live registry can
/// refresh underneath an active run without affecting it.
///
/// Supported template fields:
/// - `base_url` — from `RunCatalog::lookup` → `Found(HttpEndpoint::base_url)`.
///   Catalog is authoritative; the proven `RouteAddress` `base_url` wins
///   over any port-synthesis fallback.
/// - `version` — from `Found(HttpEndpoint::version)`, `Binary::version`,
///   or `Toolchain::version`. `None` until the resource is probed.
/// - `path` — absolute filesystem path to the resolved binary.
///   `Binary::path` for binary resources; `Toolchain::exe_path` for
///   toolchains; `None` for `HttpEndpoint` (no path concept).
/// - `capabilities` — comma-separated list of capability names (serde
///   `snake_case` spelling, e.g. `"cli_agent_print"`) for `Binary`
///   resources; `None` for non-binary variants.
/// - `id` — the resource id verbatim from the registry definition.
/// - `kind` — `http_endpoint` / `binary` / `toolchain` via serde
///   (`ResourceKind` carries `rename_all = "snake_case"`), so the
///   spelling matches what shows in workflow JSON elsewhere — `{:?}`
///   would render `HttpEndpoint` and `.to_lowercase()` would render
///   `httpendpoint`, breaking round-trips.
///
/// Unknown ids and unsupported fields return `None`; the template
/// engine surfaces the failure as `TemplateError::Undefined`.
#[must_use]
pub fn build_run_snapshot_resources_resolver<S>(
    registry: std::sync::Arc<crate::environment::runtime::RegistryInner>,
    workflow_id: crate::environment::runtime::WorkflowId,
    effective_env: crate::environment::runtime::EnvId,
    catalogs: std::sync::Arc<
        HashMap<
            crate::environment::runtime::EnvId,
            std::sync::Arc<crate::environment::runtime::RunCatalog>,
            S,
        >,
    >,
) -> BoxedResourceResolver
where
    S: std::hash::BuildHasher + Send + Sync + 'static,
{
    use crate::environment::runtime::{ResourceDetail, ResourceId, ResourceProbeOutcome};

    Box::new(move |id_part: &str, field: &str| -> Option<String> {
        let id = ResourceId(id_part.to_string());

        // base_url and version come from the per-env run-local catalog —
        // the proven RouteAddress base_url is authoritative.
        if field == "base_url" {
            let outcome = catalogs.get(&effective_env)?.lookup(&id)?;
            let ResourceProbeOutcome::Found(ResourceDetail::HttpEndpoint { base_url, .. }) =
                outcome
            else {
                return None;
            };
            return Some(base_url);
        }
        if field == "version" {
            let outcome = catalogs.get(&effective_env)?.lookup(&id)?;
            let ResourceProbeOutcome::Found(detail) = outcome else {
                return None;
            };
            return match detail {
                ResourceDetail::HttpEndpoint { version, .. }
                | ResourceDetail::Binary { version, .. }
                | ResourceDetail::Toolchain { version, .. } => version,
            };
        }
        if field == "path" {
            let outcome = catalogs.get(&effective_env)?.lookup(&id)?;
            let ResourceProbeOutcome::Found(detail) = outcome else {
                return None;
            };
            return match detail {
                ResourceDetail::Binary { path, .. } => Some(path),
                ResourceDetail::Toolchain { exe_path, .. } => Some(exe_path),
                ResourceDetail::HttpEndpoint { .. } => None,
            };
        }
        if field == "capabilities" {
            let outcome = catalogs.get(&effective_env)?.lookup(&id)?;
            let ResourceProbeOutcome::Found(ResourceDetail::Binary { capabilities, .. }) = outcome
            else {
                return None;
            };
            return Some(
                capabilities
                    .iter()
                    .filter_map(|c| serde_json::to_value(c).ok())
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }

        // Registry-derived fields walk the per-run frozen registry at
        // the node's effective env; the snapshot installs `EnvLocal`
        // overlays per Task 14 so env-local resources resolve.
        let (def, _scope) = registry.resolve(&id, &effective_env, Some(&workflow_id))?;
        match field {
            "id" => Some(def.id.0.clone()),
            "kind" => serde_json::to_value(def.kind)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string)),
            _ => None,
        }
    })
}

#[cfg(test)]
mod tests;
