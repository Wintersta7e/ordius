//! Smoke tests for `Engine::refresh_environment`.
//!
//! The refresh API spawns the probe in a background task and CAS-commits
//! against `env_refresh_epoch`, so callers see `Ok(())` as soon as the spec
//! has been read; these tests assert the synchronous contract and that the
//! happy path stays non-blocking.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ordius_engine::Engine;
use ordius_engine::environment::runtime::EnvId;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn refresh_environment_full_scope_completes() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(
        Engine::new(tmp.path().to_path_buf())
            .await
            .expect("engine opens"),
    );

    engine
        .refresh_environment(None)
        .await
        .expect("full refresh returns Ok after scheduling the probe");

    // The boot probe already installed Local; the background full refresh
    // either confirms it (epoch CAS wins) or drops its result (epoch CAS
    // loses to a stale-epoch contender). Either way the engine still holds
    // a Local entry from the prior install — refresh is non-destructive on
    // the synchronous return path.
    let entries = engine.env_registry().entries();
    assert!(
        entries.contains_key(&EnvId::local()),
        "Local must remain after refresh_environment(None) returns",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn refresh_environment_single_known_is_non_blocking() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(
        Engine::new(tmp.path().to_path_buf())
            .await
            .expect("engine opens"),
    );

    // The synthesized Local env has no `env_specs` row at boot, so a
    // single-env refresh for `local` falls through `load_spec_single`'s
    // `Ok(None)` branch and removes the entry inline. The call must still
    // return promptly; only the synchronous remove runs under the lock.
    let start = Instant::now();
    engine
        .refresh_environment(Some(&EnvId::local()))
        .await
        .expect("single refresh returns Ok");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "single-env refresh must return without awaiting the probe; took {elapsed:?}",
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn refresh_environment_unknown_env_is_noop() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(
        Engine::new(tmp.path().to_path_buf())
            .await
            .expect("engine opens"),
    );

    let unknown = EnvId::new("ssh:does-not-exist");
    // No row in `env_specs` → `Remove` branch → drops nothing (the env was
    // never installed in the registry to begin with).
    engine
        .refresh_environment(Some(&unknown))
        .await
        .expect("refresh for an unknown env is a safe no-op");

    let entries = engine.env_registry().entries();
    assert!(
        !entries.contains_key(&unknown),
        "unknown env must not appear after refresh",
    );
    // Local is unaffected by the unrelated refresh.
    assert!(
        entries.contains_key(&EnvId::local()),
        "Local must remain after refreshing an unrelated env id",
    );
}
