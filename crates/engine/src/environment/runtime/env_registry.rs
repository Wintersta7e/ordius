//! Engine-level env registry: dispatcher + last-probed-info per env id.
//!
//! Replaces the legacy `EnvironmentReport` cache. Each `EnvEntry` couples a
//! cloneable `Arc<EnvInfo>` describing the env (id, label, spec, enabled bit,
//! current state) with the `Arc<dyn Dispatcher>` constructed from that spec
//! at boot or refresh. The wrapping `EnvRegistry` is an `ArcSwap` so refreshes
//! atomically replace the entire map; per-env reads are lock-free.

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use super::dispatcher::Dispatcher;
use super::env::{EnvId, EnvInfo};

/// Per-env state owned by the engine.
pub struct EnvEntry {
    /// Catalog metadata: id, label, `EnvSpec`, enabled bit, current state.
    pub info: Arc<EnvInfo>,
    /// Dispatcher constructed from the `EnvSpec` at boot or refresh.
    pub dispatcher: Arc<dyn Dispatcher>,
}

/// Engine-owned map of `EnvId → EnvEntry`. Wraps an `ArcSwap` so refreshes
/// atomically replace the whole map; per-env state is then read lock-free.
pub struct EnvRegistry {
    inner: ArcSwap<HashMap<EnvId, Arc<EnvEntry>>>,
}

impl EnvRegistry {
    /// Construct an empty registry. The boot probe (Task 4) fills it.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(HashMap::new())),
        }
    }

    /// Lock-free read of the entry for an env id.
    #[must_use]
    pub fn get(&self, env: &EnvId) -> Option<Arc<EnvEntry>> {
        self.inner.load().get(env).cloned()
    }

    /// Lock-free read of every entry.
    #[must_use]
    pub fn entries(&self) -> Arc<HashMap<EnvId, Arc<EnvEntry>>> {
        self.inner.load_full()
    }

    /// Atomic replace.
    pub fn store(&self, next: HashMap<EnvId, Arc<EnvEntry>>) {
        self.inner.store(Arc::new(next));
    }

    /// Convenience: pull just the dispatcher map for the envs the run loop
    /// freezes into `RunSnapshot::dispatchers`. Missing envs are skipped;
    /// the caller decides how to surface `EnvUnreachable` for absent ids.
    #[must_use]
    pub fn dispatchers_for(&self, envs: &[EnvId]) -> HashMap<EnvId, Arc<dyn Dispatcher>> {
        let snapshot = self.inner.load();
        let mut out = HashMap::with_capacity(envs.len());
        for env in envs {
            if let Some(entry) = snapshot.get(env) {
                out.insert(env.clone(), Arc::clone(&entry.dispatcher));
            }
        }
        out
    }
}

impl Default for EnvRegistry {
    fn default() -> Self {
        Self::new()
    }
}
