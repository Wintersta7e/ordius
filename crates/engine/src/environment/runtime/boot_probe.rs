//! EnvSpec-driven boot/refresh probe orchestrator.
//!
//! Replaces the legacy `environment::detect` flow. Loads `EnvSpec` rows from
//! the `env_specs` table, constructs a `Dispatcher` per spec, probes each,
//! and returns the resolved `EnvEntry`s + per-env `ResourceCatalog`s. The
//! caller (`Engine::new` and later `Engine::refresh_environment`) installs
//! the outcome atomically.
//!
//! Behaviour:
//! - The Local env is always present. If `env_specs` lacks a `local` row,
//!   the boot probe synthesizes an empty `EnvSpec::Local` so every Engine
//!   starts with at least one env that user-authored workflows can target.
//! - Disabled specs land in `disabled_specs` (no dispatcher, no probe);
//!   active specs land in `entries` paired with a probed catalog.
//! - `OVERALL_BUDGET` caps total wall-clock; envs whose probe is in flight
//!   when the budget elapses are cancelled and dropped from the outcome.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::catalog::ResourceCatalog;
use super::dispatcher::Dispatcher;
use super::env::{EnvId, EnvInfo, EnvSpec, EnvState};
use super::env_registry::EnvEntry;
use super::error::DispatchError;
use super::local::LocalDispatcher;
use super::plan::ProbePlan;
use super::registry::ResourceRegistry;
use super::resource::{ResourceDefinition, ResourceId};
use super::wsl::WslDispatcher;
use crate::db::DbPool;

/// Hard overall deadline for a full boot probe across all envs.
const OVERALL_BUDGET: Duration = Duration::from_secs(8);
/// Per-resource timeout (used when a `ProbeSpec` doesn't override).
const PER_RESOURCE_TIMEOUT: Duration = Duration::from_secs(2);
/// Max concurrent probes per dispatcher.
const MAX_CONCURRENCY: usize = 8;

/// Result of a boot probe. Caller installs each field atomically.
pub struct BootProbeOutcome {
    /// Active, enabled, dispatcher-backed envs keyed by env id.
    pub entries: HashMap<EnvId, Arc<EnvEntry>>,
    /// Last-probed catalog per active env. Keyed parallel to `entries`.
    pub catalogs: HashMap<EnvId, Arc<ResourceCatalog>>,
    /// Envs that exist in `env_specs` but are flagged disabled. Not paired
    /// with a dispatcher; surfaced by IPC so the UI can render an
    /// Enable affordance. Workflow validation (Task 8) rejects target envs
    /// that resolve into this map.
    pub disabled_specs: HashMap<EnvId, EnvSpec>,
}

/// Run the boot probe.
///
/// Always returns; per-env probe failures are absorbed into the resolved
/// `EnvInfo.state` (`Unreachable { reason }`) and a catalog with no
/// resources. Probes that exceed `OVERALL_BUDGET` are cancelled and their
/// envs are dropped from the outcome.
pub async fn run(pool: &DbPool, resource_registry: &ResourceRegistry) -> BootProbeOutcome {
    let mut specs = load_specs(pool);
    // Local is ALWAYS synthesized when absent so every workflow that
    // targets the default Local env can validate at load time even on
    // first-run installs with no `env_specs` rows.
    let has_local = specs.iter().any(|(id, _, _)| *id == EnvId::local());
    if !has_local {
        specs.push((
            EnvId::local(),
            EnvSpec::Local {
                resources: Vec::new(),
                host_direct_verifications: HashMap::new(),
            },
            true,
        ));
    }

    let cancel = CancellationToken::new();
    let mut tasks: JoinSet<(EnvId, Arc<EnvEntry>, Arc<ResourceCatalog>)> = JoinSet::new();
    let mut disabled_specs: HashMap<EnvId, EnvSpec> = HashMap::new();

    for (env_id, spec, enabled) in specs {
        if !enabled {
            tracing::info!(env_id = %env_id, "env disabled; staged for disabled_specs");
            disabled_specs.insert(env_id, spec);
            continue;
        }
        // Probing-state info; rebuilt with the resolved state after probe.
        let probing_info = Arc::new(EnvInfo {
            id: env_id.clone(),
            label: human_label(&spec),
            spec: spec.clone(),
            state: EnvState::Probing,
            enabled: true,
        });
        let dispatcher = match construct_dispatcher(&spec, &probing_info) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(env_id = %env_id, error = %e, "skipping env: dispatcher unsupported");
                continue;
            },
        };
        let plan = build_plan(&env_id, &spec, resource_registry);
        let cancel_clone = cancel.clone();
        let dispatcher_for_task = Arc::clone(&dispatcher);
        let info_for_task = Arc::clone(&probing_info);
        let env_id_for_task = env_id.clone();

        tasks.spawn(async move {
            let summary = dispatcher_for_task.probe(plan, cancel_clone).await;
            let (catalog, resolved_state) = match summary {
                Ok(s) => {
                    // Reaching this arm means the dispatcher returned Ok —
                    // even if no resources were Found, the env itself
                    // answered within the budget, so treat as Reachable.
                    (Arc::new(s.catalog), EnvState::Reachable)
                }
                Err(e) => {
                    tracing::warn!(env_id = %env_id_for_task, error = %e, "probe failed; empty catalog");
                    (
                        Arc::new(ResourceCatalog {
                            env_id: env_id_for_task.clone(),
                            registry_revision: 0,
                            probed_at: chrono::Utc::now(),
                            resources: HashMap::new(),
                        }),
                        EnvState::Unreachable { reason: e.to_string() },
                    )
                }
            };
            let resolved_info = Arc::new(EnvInfo {
                state: resolved_state,
                ..(*info_for_task).clone()
            });
            let entry = Arc::new(EnvEntry {
                info: resolved_info,
                dispatcher,
            });
            (env_id_for_task, entry, catalog)
        });
    }

    // Wall-clock guard. `tokio::select!` between draining the JoinSet and
    // the overall budget; if the budget wins we cancel + drain whatever
    // completed before the sleep fired.
    let (entries, catalogs) = tokio::select! {
        result = drain_tasks(&mut tasks) => result,
        () = tokio::time::sleep(OVERALL_BUDGET) => {
            cancel.cancel();
            drain_tasks(&mut tasks).await
        }
    };

    BootProbeOutcome {
        entries,
        catalogs,
        disabled_specs,
    }
}

/// Drain every joined task into `(entries, catalogs)`. `JoinError`s are
/// logged + skipped so a panicking probe task doesn't poison the outcome.
async fn drain_tasks(
    tasks: &mut JoinSet<(EnvId, Arc<EnvEntry>, Arc<ResourceCatalog>)>,
) -> (
    HashMap<EnvId, Arc<EnvEntry>>,
    HashMap<EnvId, Arc<ResourceCatalog>>,
) {
    let mut entries = HashMap::new();
    let mut catalogs = HashMap::new();
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((env_id, entry, catalog)) => {
                entries.insert(env_id.clone(), entry);
                catalogs.insert(env_id, catalog);
            },
            Err(e) => {
                tracing::warn!(error = %e, "boot probe task join failed");
            },
        }
    }
    (entries, catalogs)
}

fn construct_dispatcher(
    spec: &EnvSpec,
    info: &Arc<EnvInfo>,
) -> Result<Arc<dyn Dispatcher>, DispatchError> {
    match spec {
        EnvSpec::Local { .. } => Ok(Arc::new(LocalDispatcher::new((**info).clone()))),
        EnvSpec::WslDistro {
            name,
            host_direct_verifications,
            ..
        } => {
            // `WslDispatcher::new` takes `(info, distro_name)`;
            // `set_host_direct` wires verifications into the transport's
            // ArcSwap.
            let wsl = WslDispatcher::new((**info).clone(), name.clone());
            wsl.set_host_direct(host_direct_verifications.clone());
            Ok(Arc::new(wsl))
        },
        EnvSpec::Ssh { .. } => Err(DispatchError::Unsupported(
            "ssh dispatcher lands in Phase G".into(),
        )),
        EnvSpec::Container { .. } => Err(DispatchError::Unsupported(
            "container dispatcher lands in Phase H".into(),
        )),
    }
}

/// Build a `ProbePlan` from the registry view for `env_id` merged with the
/// env's own `EnvSpec.resources`. Env-local entries that would shadow a
/// builtin/user-global must set `override_lower_scope` (matches the
/// registry's upsert contract); collisions without the flag log a warning
/// and the lower-scope entry wins.
fn build_plan(env_id: &EnvId, spec: &EnvSpec, resource_registry: &ResourceRegistry) -> ProbePlan {
    let snap = resource_registry.snapshot();
    let mut defs: Vec<ResourceDefinition> = snap
        .visible_to(env_id, None)
        .into_iter()
        .map(|(def, _scope)| def.clone())
        .collect();

    let env_local: &[ResourceDefinition] = match spec {
        EnvSpec::Local { resources, .. }
        | EnvSpec::WslDistro { resources, .. }
        | EnvSpec::Ssh { resources, .. }
        | EnvSpec::Container { resources, .. } => resources,
    };

    let mut seen: HashSet<ResourceId> = defs.iter().map(|d| d.id.clone()).collect();
    for def in env_local {
        if seen.contains(&def.id) {
            if !def.override_lower_scope {
                tracing::warn!(
                    env_id = %env_id,
                    resource_id = %def.id,
                    "env-local resource shadows lower scope without override_lower_scope; skipping",
                );
                continue;
            }
            if let Some(slot) = defs.iter_mut().find(|d| d.id == def.id) {
                *slot = def.clone();
            }
        } else {
            seen.insert(def.id.clone());
            defs.push(def.clone());
        }
    }

    ProbePlan {
        env_id: env_id.clone(),
        registry_revision: snap.revision,
        defs,
        per_resource_timeout: PER_RESOURCE_TIMEOUT,
        max_concurrency: MAX_CONCURRENCY,
        overall_budget: OVERALL_BUDGET,
    }
}

fn human_label(spec: &EnvSpec) -> String {
    match spec {
        EnvSpec::Local { .. } => "Local".to_string(),
        EnvSpec::WslDistro { name, .. } => format!("WSL: {name}"),
        EnvSpec::Ssh { user, host, .. } => format!("SSH: {user}@{host}"),
        EnvSpec::Container { image, .. } => format!("Container: {image}"),
    }
}

/// Load every row from `env_specs`. Bad rows (unparseable `spec_json`) are
/// logged + skipped — the boot probe must not panic on a corrupted file.
fn load_specs(pool: &DbPool) -> Vec<(EnvId, EnvSpec, bool)> {
    let conn = match pool.get() {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "env_specs: pool unavailable");
            return Vec::new();
        },
    };
    let mut stmt = match conn.prepare("SELECT id, spec_json, enabled FROM env_specs") {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(error = %e, "env_specs: prepare failed");
            return Vec::new();
        },
    };
    let row_iter = match stmt.query_map([], |row| {
        let id_s: String = row.get(0)?;
        let json: String = row.get(1)?;
        let enabled: i64 = row.get(2)?;
        Ok((id_s, json, enabled != 0))
    }) {
        Ok(it) => it,
        Err(e) => {
            tracing::warn!(error = %e, "env_specs: query_map failed");
            return Vec::new();
        },
    };
    row_iter
        .filter_map(|r| {
            let (id_s, json, enabled) = r.ok()?;
            match serde_json::from_str::<EnvSpec>(&json) {
                Ok(spec) => Some((EnvId::new(id_s), spec, enabled)),
                Err(e) => {
                    tracing::warn!(env_id = %id_s, error = %e, "env_specs: bad spec_json; skipping");
                    None
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::open;
    use tempfile::TempDir;

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_db_synthesizes_local_env() {
        let tmp = TempDir::new().unwrap();
        let pool = open(tmp.path().join("runs.db")).unwrap();
        let registry = ResourceRegistry::new();
        let outcome = run(&pool, &registry).await;
        assert!(
            outcome.entries.contains_key(&EnvId::local()),
            "boot probe must always synthesize Local when env_specs is empty",
        );
        assert!(
            outcome.catalogs.contains_key(&EnvId::local()),
            "Local must have a catalog (possibly empty) after probe",
        );
        assert!(outcome.disabled_specs.is_empty());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disabled_spec_does_not_install_dispatcher() {
        let tmp = TempDir::new().unwrap();
        let pool = open(tmp.path().join("runs.db")).unwrap();
        // Insert a disabled wsl row so we can verify the gate fires.
        {
            let conn = pool.get().unwrap();
            conn.execute(
                "INSERT INTO env_specs (id, label, enabled, spec_json, created_at, updated_at)
                 VALUES ('wsl:Disabled', 'WSL: Disabled', 0,
                         '{\"type\":\"wsl_distro\",\"name\":\"Disabled\",\"resources\":[],\"host_direct_verifications\":{}}',
                         0, 0)",
                [],
            )
            .unwrap();
        }
        let registry = ResourceRegistry::new();
        let outcome = run(&pool, &registry).await;
        assert!(
            !outcome.entries.contains_key(&EnvId::new("wsl:Disabled")),
            "disabled env must not land in entries",
        );
        assert!(
            outcome
                .disabled_specs
                .contains_key(&EnvId::new("wsl:Disabled")),
            "disabled env must land in disabled_specs",
        );
        // Local is still synthesized.
        assert!(outcome.entries.contains_key(&EnvId::local()));
    }
}
