//! Tauri command surface the React UI calls into.
//!
//! Every handler accepts the managed `AppState` (Arc'd engine) and
//! returns a camelCase DTO. Errors come back to JavaScript as plain
//! strings — `Result<_, EngineError>` would force a custom serde
//! impl and the frontend doesn't need the structured error today.

// Tauri's invoke_handler deserialises every argument from JSON, so
// commands take owned values rather than references. The frontend
// sends a fresh JSON payload per call — clippy's
// `needless_pass_by_value` would have us reference-style every
// String, which doesn't make sense at this boundary.
#![allow(clippy::needless_pass_by_value)]
// Returning `Result<_, String>` is the canonical Tauri command
// shape so the frontend always gets a Promise that rejects on
// error. Some commands have no current failure path (`stop_run`'s
// cancel-token lookup, `list_node_types`'s in-memory scan) but
// keeping the Result preserves API stability when failure paths
// land later.
#![allow(clippy::unnecessary_wraps)]

use crate::dto::{
    EndpointStatusDto, JsonCamel, ModelEndpointDto, NodeRunRowDto, NodeTypeDto, RunDetailDto,
    RunEventDto, RunRowDto, RunStartedDto, RunWorkflowArgs, SavedWorkflowDto, SecretMetaDto,
    SettingsDto, SystemStatusDto, WorkflowDto, WorkspaceDto,
};
use crate::state::AppState;
use ordius_engine::settings::Settings as EngineSettings;
use std::path::PathBuf;

/// Workflow ids become the filename stem under `<home>/workflows/`, so a
/// hostile webview payload like `../../etc/passwd` would escape the dir.
/// Restrict to a slug shape — ASCII alnum plus `_` and `-`, 1..=128 chars.
fn validate_workflow_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("workflow id is empty".into());
    }
    if id.len() > 128 {
        return Err(format!("workflow id is too long ({} > 128)", id.len()));
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(format!(
            "workflow id {id:?} contains characters outside [A-Za-z0-9_-]"
        ));
    }
    Ok(())
}

/// Resolve a workspace id from the run-dialog payload to the absolute
/// path stored in `<home>/workspaces.json`. Returns Err if the id is
/// unknown — the GUI lists workspaces from the same source so a missing
/// id is a programmer/path-traversal error rather than a transient one.
fn resolve_workspace_path(home: &std::path::Path, id: &str) -> Result<PathBuf, String> {
    ordius_engine::workspaces::find(home, id)
        .map(|w| w.path)
        .map_err(|e| e.to_string())
}

// ─── Workflows ───────────────────────────────────────────────────

/// List every workflow in `<home>/workflows/`. Parse errors are
/// logged via tracing but don't fail the command.
#[tauri::command]
pub fn list_workflows(state: tauri::State<'_, AppState>) -> Result<Vec<SavedWorkflowDto>, String> {
    let home = state.engine.home();
    let (wfs, errors) = ordius_engine::workflows::list(home).map_err(|e| e.to_string())?;
    for (path, err) in &errors {
        tracing::warn!(path = %path.display(), error = %err, "workflow parse failed");
    }
    Ok(wfs.iter().map(SavedWorkflowDto::from).collect())
}

/// Load a single workflow by id.
#[tauri::command]
pub fn load_workflow(state: tauri::State<'_, AppState>, id: String) -> Result<WorkflowDto, String> {
    validate_workflow_id(&id)?;
    let wf = ordius_engine::workflows::load(state.engine.home(), &id).map_err(|e| e.to_string())?;
    Ok(JsonCamel(wf))
}

/// Persist a workflow to disk. Validates structure before saving;
/// the editor's "Save" button gates on this.
#[tauri::command]
pub fn save_workflow(
    state: tauri::State<'_, AppState>,
    workflow: WorkflowDto,
) -> Result<(), String> {
    let workflow = workflow.0;
    validate_workflow_id(&workflow.id)?;
    ordius_engine::validate(&workflow).map_err(|e| e.to_string())?;
    ordius_engine::workflows::save(state.engine.home(), &workflow).map_err(|e| e.to_string())
}

/// Run the engine's structural validation pass over a workflow without
/// persisting it. Used by the editor's `validate` button.
#[tauri::command]
pub fn validate_workflow(workflow: WorkflowDto) -> Result<(), String> {
    ordius_engine::validate(&workflow.0).map_err(|e| e.to_string())
}

/// Delete a workflow by id. The editor confirms before calling.
#[tauri::command]
pub fn delete_workflow(state: tauri::State<'_, AppState>, id: String) -> Result<bool, String> {
    validate_workflow_id(&id)?;
    ordius_engine::workflows::delete(state.engine.home(), &id).map_err(|e| e.to_string())
}

/// Clone an existing workflow to a fresh `<id>-copy` (collisions are
/// resolved by appending `-2`, `-3`, ...) with a `(copy)` suffix on
/// the display name. Returns the saved clone.
#[tauri::command]
pub fn duplicate_workflow(
    state: tauri::State<'_, AppState>,
    id: String,
) -> Result<WorkflowDto, String> {
    validate_workflow_id(&id)?;
    let wf =
        ordius_engine::workflows::duplicate(state.engine.home(), &id).map_err(|e| e.to_string())?;
    Ok(JsonCamel(wf))
}

// ─── Runs ────────────────────────────────────────────────────────

/// Start a workflow run. Streams events back via the `Channel`
/// argument — the frontend creates a `new Channel<RunEventDto>()`
/// and passes it in; we drain the engine's broadcast into it.
#[tauri::command]
pub async fn run_workflow(
    state: tauri::State<'_, AppState>,
    args: RunWorkflowArgs,
    channel: tauri::ipc::Channel<RunEventDto>,
) -> Result<RunStartedDto, String> {
    validate_workflow_id(&args.workflow_id)?;
    let engine = state.engine.clone();
    // Centralised path: validates retired-id rejection inside the loader
    // and seeds the workflow's `resources:` block into the engine's
    // registry before any node dispatches. Non-fatal warnings are
    // logged via `tracing` for this fix wave; surfacing them in the IPC
    // response is tracked as a follow-up so the UI can render them
    // alongside structural-validation errors.
    let (wf, warnings) = engine
        .load_workflow_for_run(engine.home(), &args.workflow_id)
        .map_err(|e| e.to_string())?;
    for warning in &warnings {
        tracing::warn!(
            workflow_id = %args.workflow_id,
            node_id = %warning.node_id,
            message = %warning.message,
            "workflow load warning",
        );
    }

    // Resolve workspace selection against the user's registered
    // workspaces. `None` falls back to the engine's per-run scratch
    // dir at `<home>/workspaces/<run_id>`.
    let workspace_override = match args.workspace_id {
        Some(id) => Some(resolve_workspace_path(engine.home(), &id)?),
        None => None,
    };

    let handle = engine
        .start_run(
            wf,
            args.variables,
            "gui",
            args.auto_resume,
            workspace_override,
        )
        .map_err(|e| e.to_string())?;
    let run_id = handle.run_id.clone();
    let mut rx = handle.event_rx;
    let join = handle.join;
    let lag_run_id = run_id.clone();
    tokio::spawn(async move {
        let mut next_seq: u64 = 0;
        loop {
            match rx.recv().await {
                Ok(ev) => {
                    next_seq = ev.seq.saturating_add(1);
                    if channel.send(RunEventDto::from(ev)).is_err() {
                        // Frontend closed the channel — stop draining.
                        break;
                    }
                },
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(
                        run_id = %lag_run_id,
                        dropped = n,
                        "run event stream lagged; UI should refresh via get_run",
                    );
                    // Surface to the frontend so the timeline shows a marker
                    // and the run viewer can choose to re-fetch persisted
                    // state from `get_run` for accuracy.
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX));
                    let synthetic = RunEventDto {
                        ty: "stream:lagged".into(),
                        seq: next_seq,
                        emitted_at: now_ms,
                        run_id: lag_run_id.clone(),
                        node_id: None,
                        iteration: None,
                        attempt: None,
                        payload: std::collections::HashMap::from([(
                            "dropped".to_string(),
                            serde_json::json!(n),
                        )]),
                    };
                    if channel.send(synthetic).is_err() {
                        break;
                    }
                },
            }
        }
        // Drain the join handle so panics surface in tracing rather
        // than silent task leaks. We don't propagate the summary — the
        // frontend reconstructs it from the workflow:done event.
        match join.await {
            Ok(Ok(_summary)) => {},
            Ok(Err(e)) => tracing::error!(error = %e, "run loop returned error"),
            Err(e) => tracing::error!(error = ?e, "run task panicked"),
        }
    });
    Ok(RunStartedDto { run_id })
}

/// Cancel an active run via the engine's cancel-token registry.
#[tauri::command]
pub fn stop_run(state: tauri::State<'_, AppState>, run_id: String) -> Result<bool, String> {
    Ok(state.engine.cancel_run(&run_id))
}

/// Deliver an event payload to a parked `wait_event` waiter in `run_id`.
/// Returns true if the payload was delivered to an active waiter.
#[tauri::command]
pub fn deliver_event(
    state: tauri::State<'_, AppState>,
    run_id: String,
    event: String,
    payload: serde_json::Value,
) -> Result<bool, String> {
    Ok(state.engine.deliver_event(&run_id, &event, payload))
}

/// Recent runs for the History page. Filters mirror the CLI's
/// `runs ls` options.
#[tauri::command]
pub fn list_runs(
    state: tauri::State<'_, AppState>,
    workflow: Option<String>,
    status: Option<String>,
    limit: Option<usize>,
) -> Result<Vec<RunRowDto>, String> {
    let mut sql = String::from(
        "SELECT id, workflow_id, status, started_at, finished_at, duration_ms, trigger_kind \
         FROM runs WHERE 1=1",
    );
    let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
    if let Some(w) = workflow {
        sql.push_str(" AND workflow_id = ?");
        params.push(Box::new(w));
    }
    if let Some(s) = status {
        sql.push_str(" AND status = ?");
        params.push(Box::new(s));
    }
    sql.push_str(" ORDER BY started_at DESC LIMIT ?");
    params.push(Box::new(i64::try_from(limit.unwrap_or(50)).unwrap_or(50)));

    let conn = state.engine.pool().get().map_err(|e| e.to_string())?;
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(rusqlite::params_from_iter(params.iter()), |r| {
            Ok(RunRowDto {
                run_id: r.get(0)?,
                workflow_id: r.get(1)?,
                status: r.get(2)?,
                started_at: r.get(3)?,
                finished_at: r.get::<_, Option<i64>>(4)?,
                duration_ms: r.get::<_, Option<i64>>(5)?,
                trigger_kind: r.get(6)?,
            })
        })
        .map_err(|e| e.to_string())?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|e| e.to_string())?;
    Ok(rows)
}

/// Detailed view of one run including every node-run row.
#[tauri::command]
pub fn get_run(state: tauri::State<'_, AppState>, run_id: String) -> Result<RunDetailDto, String> {
    let conn = state.engine.pool().get().map_err(|e| e.to_string())?;
    let row = conn
        .prepare(
            "SELECT id, workflow_id, status, started_at, finished_at, duration_ms, trigger_kind \
             FROM runs WHERE id = ?",
        )
        .map_err(|e| e.to_string())?
        .query_row(rusqlite::params![&run_id], |r| {
            Ok(RunRowDto {
                run_id: r.get(0)?,
                workflow_id: r.get(1)?,
                status: r.get(2)?,
                started_at: r.get(3)?,
                finished_at: r.get::<_, Option<i64>>(4)?,
                duration_ms: r.get::<_, Option<i64>>(5)?,
                trigger_kind: r.get(6)?,
            })
        })
        .map_err(|e| format!("run {run_id} not found: {e}"))?;

    let mut node_stmt = conn
        .prepare(
            "SELECT node_id, iteration, attempt, node_type, status, started_at, \
                    finished_at, duration_ms, error \
             FROM node_runs WHERE run_id = ? ORDER BY started_at",
        )
        .map_err(|e| e.to_string())?;
    let node_runs = node_stmt
        .query_map(rusqlite::params![&run_id], |r| {
            Ok(NodeRunRowDto {
                node_id: r.get(0)?,
                iteration: r.get(1)?,
                attempt: r.get(2)?,
                node_type: r.get(3)?,
                status: r.get(4)?,
                started_at: r.get::<_, Option<i64>>(5)?,
                finished_at: r.get::<_, Option<i64>>(6)?,
                duration_ms: r.get::<_, Option<i64>>(7)?,
                error: r.get::<_, Option<String>>(8)?,
            })
        })
        .map_err(|e| e.to_string())?
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(|e| e.to_string())?;

    Ok(RunDetailDto { row, node_runs })
}

// ─── Nodes ───────────────────────────────────────────────────────

/// Every registered node-type — built-ins plus manifest-loaded
/// custom types — for the palette and properties panel.
#[tauri::command]
pub fn list_node_types(state: tauri::State<'_, AppState>) -> Result<Vec<NodeTypeDto>, String> {
    let registry = state.engine.registry();
    let mut ids = registry.ids();
    ids.sort();
    Ok(ids
        .iter()
        .filter_map(|id| registry.get(id))
        .map(|arc| JsonCamel((*arc).clone()))
        .collect())
}

// ─── Workspaces ──────────────────────────────────────────────────

/// Every registered workspace.
#[tauri::command]
pub fn list_workspaces(state: tauri::State<'_, AppState>) -> Result<Vec<WorkspaceDto>, String> {
    Ok(ordius_engine::workspaces::list(state.engine.home())
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(WorkspaceDto::from)
        .collect())
}

/// Register a new workspace. `path` must already exist as a directory.
#[tauri::command]
pub fn add_workspace(
    state: tauri::State<'_, AppState>,
    name: String,
    path: String,
) -> Result<WorkspaceDto, String> {
    let path = PathBuf::from(path);
    let ws = ordius_engine::workspaces::add(state.engine.home(), &name, &path)
        .map_err(|e| e.to_string())?;
    Ok(ws.into())
}

/// Unregister a workspace.
#[tauri::command]
pub fn remove_workspace(state: tauri::State<'_, AppState>, id: String) -> Result<(), String> {
    ordius_engine::workspaces::remove(state.engine.home(), &id).map_err(|e| e.to_string())
}

/// Change a workspace's display name. Returns the updated record.
#[tauri::command]
pub fn rename_workspace(
    state: tauri::State<'_, AppState>,
    id: String,
    name: String,
) -> Result<WorkspaceDto, String> {
    let ws = ordius_engine::workspaces::rename(state.engine.home(), &id, &name)
        .map_err(|e| e.to_string())?;
    Ok(ws.into())
}

// ─── Secrets ─────────────────────────────────────────────────────

/// Names of every secret stored in the OS keyring (values never
/// exposed).
#[tauri::command]
pub fn list_secrets(state: tauri::State<'_, AppState>) -> Result<Vec<SecretMetaDto>, String> {
    let store = state.engine.secrets_store();
    let names = store.list().map_err(|e| e.to_string())?;
    Ok(names
        .into_iter()
        .map(|name| SecretMetaDto { name })
        .collect())
}

/// Store a secret. Empty values are rejected to match the CLI's
/// safety check.
#[tauri::command]
pub fn add_secret(
    state: tauri::State<'_, AppState>,
    name: String,
    value: String,
) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("refusing to store empty value for {name}"));
    }
    let store = state.engine.secrets_store();
    store.set(&name, &value).map_err(|e| e.to_string())
}

/// Delete a secret.
#[tauri::command]
pub fn remove_secret(state: tauri::State<'_, AppState>, name: String) -> Result<(), String> {
    let store = state.engine.secrets_store();
    store.delete(&name).map_err(|e| e.to_string())
}

// ─── Settings ────────────────────────────────────────────────────

/// Load current settings (returns defaults when no file exists).
#[tauri::command]
pub fn get_settings(state: tauri::State<'_, AppState>) -> Result<SettingsDto, String> {
    Ok(ordius_engine::settings::load(state.engine.home())
        .map_err(|e| e.to_string())?
        .into())
}

/// Save settings, replacing the on-disk file.
#[tauri::command]
pub fn set_settings(
    state: tauri::State<'_, AppState>,
    settings: SettingsDto,
) -> Result<(), String> {
    let engine_settings: EngineSettings = settings.into();
    ordius_engine::settings::save(state.engine.home(), &engine_settings).map_err(|e| e.to_string())
}

// ─── System status ───────────────────────────────────────────────

/// Host platform + endpoints discovered at boot. Cached on the
/// engine, so this is a pure data fetch — no extra probing.
#[tauri::command]
pub fn system_environment(
    state: tauri::State<'_, AppState>,
) -> crate::dto::JsonCamel<ordius_engine::environment::EnvironmentReport> {
    crate::dto::JsonCamel((*state.engine.environment()).clone())
}

/// Re-run namespace + endpoint discovery and return the refreshed
/// report. Used by the GUI's environment picker after the user
/// edits namespace overrides or asks for a manual rescan.
#[tauri::command]
pub async fn refresh_environment(
    state: tauri::State<'_, AppState>,
) -> Result<crate::dto::JsonCamel<ordius_engine::environment::EnvironmentReport>, String> {
    let report = state
        .engine
        .refresh_environment()
        .await
        .map_err(|e| e.to_string())?;
    Ok(crate::dto::JsonCamel((*report).clone()))
}

/// Insert a `custom:*` namespace override (validated host) and
/// return the refreshed environment so the GUI sees the new row
/// + its probe result in a single round-trip.
#[tauri::command]
pub async fn add_custom_namespace(
    state: tauri::State<'_, AppState>,
    label: String,
    host: String,
) -> Result<crate::dto::JsonCamel<ordius_engine::environment::EnvironmentReport>, String> {
    ordius_engine::namespaces::add_custom(&state.engine.pool(), &label, &host)
        .map_err(|e| e.to_string())?;
    let report = state
        .engine
        .refresh_environment()
        .await
        .map_err(|e| e.to_string())?;
    Ok(crate::dto::JsonCamel((*report).clone()))
}

/// Delete a `custom:*` namespace override and return the refreshed
/// environment. WSL/local ids are rejected by the engine layer.
#[tauri::command]
pub async fn remove_custom_namespace(
    state: tauri::State<'_, AppState>,
    id: String,
) -> Result<crate::dto::JsonCamel<ordius_engine::environment::EnvironmentReport>, String> {
    ordius_engine::namespaces::remove_custom(&state.engine.pool(), &id)
        .map_err(|e| e.to_string())?;
    let report = state
        .engine
        .refresh_environment()
        .await
        .map_err(|e| e.to_string())?;
    Ok(crate::dto::JsonCamel((*report).clone()))
}

/// Toggle the enabled flag for any namespace (custom, WSL, or local)
/// and return the refreshed environment. Disabled namespaces are
/// skipped by the probe orchestrator and surface as `Disabled`.
#[tauri::command]
pub async fn set_namespace_enabled(
    state: tauri::State<'_, AppState>,
    id: String,
    enabled: bool,
) -> Result<crate::dto::JsonCamel<ordius_engine::environment::EnvironmentReport>, String> {
    ordius_engine::namespaces::set_enabled(&state.engine.pool(), &id, enabled)
        .map_err(|e| e.to_string())?;
    let report = state
        .engine
        .refresh_environment()
        .await
        .map_err(|e| e.to_string())?;
    Ok(crate::dto::JsonCamel((*report).clone()))
}

/// Snapshot of engine-side state the GUI surfaces on Home + About.
#[tauri::command]
pub fn system_status(state: tauri::State<'_, AppState>) -> Result<SystemStatusDto, String> {
    let snap = ordius_engine::system_status::snapshot(state.engine.home());
    // Fold in registered endpoints from settings so the GUI can
    // render placeholder reachability rows even before pings land.
    let settings = ordius_engine::settings::load(state.engine.home()).map_err(|e| e.to_string())?;
    let mut dto: SystemStatusDto = snap.into();
    dto.endpoints = settings
        .model_endpoints
        .into_iter()
        .map(|e| {
            // Re-use the ModelEndpointDto conversion just for the
            // id+name fields; reachability stays `unknown`.
            let model_dto: ModelEndpointDto = e.into();
            EndpointStatusDto {
                id: model_dto.id,
                name: model_dto.name,
                state: "unknown".into(),
            }
        })
        .collect();
    Ok(dto)
}

#[cfg(test)]
mod tests {
    use super::validate_workflow_id;

    #[test]
    fn accepts_slug_shape() {
        assert!(validate_workflow_id("hello").is_ok());
        assert!(validate_workflow_id("hello-world_2").is_ok());
        assert!(validate_workflow_id("New123").is_ok());
        assert!(validate_workflow_id(&"x".repeat(128)).is_ok());
    }

    #[test]
    fn rejects_path_separators() {
        assert!(validate_workflow_id("../etc/passwd").is_err());
        assert!(validate_workflow_id("..").is_err());
        assert!(validate_workflow_id("a/b").is_err());
        assert!(validate_workflow_id("a\\b").is_err());
        assert!(validate_workflow_id("./hi").is_err());
    }

    #[test]
    fn rejects_empty_and_oversize() {
        assert!(validate_workflow_id("").is_err());
        assert!(validate_workflow_id(&"x".repeat(129)).is_err());
    }

    #[test]
    fn rejects_whitespace_and_dot() {
        assert!(validate_workflow_id("a b").is_err());
        assert!(validate_workflow_id(" ").is_err());
        assert!(validate_workflow_id("a.b").is_err());
        assert!(validate_workflow_id("a\0b").is_err());
    }
}
