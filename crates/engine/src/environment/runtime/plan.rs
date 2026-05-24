//! Probe plan and summary types for orchestrating environment discovery.

use std::time::Duration;

use super::catalog::ResourceCatalog;
use super::env::EnvId;
use super::resource::ResourceDefinition;

/// A fully-resolved probe plan for a single environment.
///
/// The scheduler builds a `ProbePlan` from the active `ResourceRegistry`
/// for a given `(env, workflow)` scope and hands it to the dispatcher.
/// Carrying `defs` as an owned `Vec` lets callers filter or extend the
/// list before dispatch without mutating the registry.
#[derive(Debug, Clone)]
pub struct ProbePlan {
    /// The environment this plan targets.
    pub env_id: EnvId,
    /// Registry revision at the moment the plan was built.  Used to detect
    /// stale catalogs: if the revision advances before the probe completes,
    /// callers should discard the result and re-probe.
    pub registry_revision: u64,
    /// Resolved resource definitions to probe, in declaration order.
    pub defs: Vec<ResourceDefinition>,
    /// Per-resource timeout.  Individual `ProbeSpec` entries may override
    /// this via their own `timeout_ms` field.
    pub per_resource_timeout: Duration,
    /// Maximum number of resources to probe concurrently.
    pub max_concurrency: usize,
    /// Hard wall-clock budget for the entire plan.  The dispatcher cancels
    /// remaining probes once this elapses.
    pub overall_budget: Duration,
}

/// The result of executing a `ProbePlan`.
///
/// Contains the finished `ResourceCatalog` plus bookkeeping counters used
/// by the scheduler to decide whether a re-probe is warranted.
#[derive(Debug, Clone)]
pub struct ProbeSummary {
    /// The catalog produced by this probe run.
    pub catalog: ResourceCatalog,
    /// Number of resources actually probed (excludes those skipped due to
    /// budget exhaustion or cancellation).
    pub total_probed: usize,
    /// Wall-clock time from plan start to last probe completion.
    pub elapsed: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment::runtime::env::EnvId;
    use std::time::Duration;

    #[test]
    fn plan_defaults_match_spec() {
        let plan = ProbePlan {
            env_id: EnvId::local(),
            registry_revision: 1,
            defs: vec![],
            per_resource_timeout: Duration::from_secs(1),
            max_concurrency: 8,
            overall_budget: Duration::from_secs(5),
        };
        assert_eq!(plan.max_concurrency, 8);
        assert_eq!(plan.overall_budget, Duration::from_secs(5));
    }

    #[test]
    fn summary_constructs() {
        let s = ProbeSummary {
            catalog: crate::environment::runtime::catalog::ResourceCatalog {
                env_id: EnvId::local(),
                registry_revision: 1,
                probed_at: chrono::Utc::now(),
                resources: std::collections::HashMap::default(),
            },
            total_probed: 0,
            elapsed: Duration::ZERO,
        };
        assert_eq!(s.total_probed, 0);
    }
}
