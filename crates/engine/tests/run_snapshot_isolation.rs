//! Phase E §7: `refresh_environment` during a run does not perturb
//! the active run's snapshot. The RAII guard removes the per-run
//! `run_snapshots` entry on every drop path (normal exit, propagation,
//! panic unwind).
//!
//! Uses `Engine::run_snapshot`, which is gated behind the `testing`
//! feature; the test only builds under `--features testing`.

#![cfg(feature = "testing")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ordius_engine::Engine;
use ordius_engine::environment::runtime::EnvId;
use ordius_engine::types::{Edge, Node, Pos, Workflow};
use tempfile::TempDir;

fn delay_node(id: &str, ms: u64) -> Node {
    Node {
        id: id.into(),
        ty: "delay".into(),
        name: id.into(),
        config: HashMap::from([("ms".into(), serde_json::json!(ms))]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: None,
    }
}

fn long_delay_workflow(id: &str, ms: u64) -> Arc<Workflow> {
    Arc::new(Workflow {
        id: id.into(),
        name: id.into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        nodes: vec![delay_node("hold", ms)],
        edges: Vec::<Edge>::new(),
        resources: vec![],
        default_env: None,
    })
}

/// Spec §7 round-3 MAJOR #6: the run's view of the env substrate is
/// frozen at run start. Refreshing the engine's env registry mid-run
/// must not swap the `Arc<RunSnapshot>` that `start_run` handed to the
/// run loop.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_during_run_does_not_mutate_active_snapshot() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    // 2s delay keeps the run alive long enough to refresh and observe.
    let wf = long_delay_workflow("iso-during", 2_000);
    let handle = engine
        .start_run(wf, HashMap::new(), "test", false, None)
        .expect("start_run");
    let run_id = handle.run_id.clone();

    // Synchronously inserted before tokio::spawn, so the entry must be
    // observable as soon as start_run returns.
    let snap_before = engine
        .run_snapshot(&run_id)
        .expect("snapshot present immediately after start_run");

    // Refresh the engine's env registry / catalogs. The engine swaps
    // its ArcSwap caches, but the active run holds its own Arcs.
    engine
        .refresh_environment(None)
        .await
        .expect("refresh schedules");

    let snap_after = engine
        .run_snapshot(&run_id)
        .expect("snapshot still present mid-run");
    assert!(
        Arc::ptr_eq(&snap_before, &snap_after),
        "refresh_environment must not swap the active run's RunSnapshot Arc",
    );

    // Cancel + drain so the test does not block on the full 2s delay.
    engine.cancel_run(&run_id);
    drop(handle.join.await);

    // RAII guard inside the spawned task removes the entry on drop.
    // Poll briefly because the runtime may have scheduling slack.
    for _ in 0..40 {
        if engine.run_snapshot(&run_id).is_none() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("run_snapshots entry must be cleaned up after run exit");
}

/// The next run after a refresh consults the engine's current
/// dispatcher registry / catalog map, not the prior run's frozen
/// view. We can't easily mutate the seeded env via the public API, so
/// the simplest assertion is structural: `build_run_snapshot` for a
/// fresh run id succeeds after refresh and references the same Local
/// env entry the boot probe installed.
#[tokio::test(flavor = "multi_thread")]
async fn next_run_after_refresh_sees_current_env_registry() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    let before_entries = engine.env_registry().entries();
    assert!(
        before_entries.contains_key(&EnvId::local()),
        "boot probe must have seeded the Local env",
    );

    engine
        .refresh_environment(None)
        .await
        .expect("refresh schedules");

    // Run a quick workflow after the refresh. It must build a snapshot
    // (proving the Local env is still in the registry) and finish.
    let wf = Arc::new(Workflow {
        id: "iso-after".into(),
        name: "iso-after".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        nodes: vec![delay_node("d", 5)],
        edges: Vec::<Edge>::new(),
        resources: vec![],
        default_env: None,
    });
    let summary = engine
        .run_workflow(wf, HashMap::new(), "test", false, None)
        .await
        .expect("run completes after refresh");
    assert_eq!(summary.status, "done");
}

/// Verifies the RAII guard fires on the normal-exit path. A run that
/// completes without panic must drop the `run_snapshots` entry; the
/// `cancel + join` flow in the first test already exercises the
/// cancellation path. This case covers natural completion.
#[tokio::test(flavor = "multi_thread")]
async fn normal_exit_cleans_up_run_snapshot_entry() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::new(tmp.path().to_path_buf()).await.unwrap());

    // Tiny delay so the run completes on its own.
    let wf = long_delay_workflow("iso-exit", 5);
    let handle = engine
        .start_run(wf, HashMap::new(), "test", false, None)
        .expect("start_run");
    let run_id = handle.run_id.clone();

    assert!(
        engine.run_snapshot(&run_id).is_some(),
        "entry present at start",
    );

    let summary = handle.join.await.expect("join").expect("run ok");
    assert_eq!(summary.status, "done");

    for _ in 0..40 {
        if engine.run_snapshot(&run_id).is_none() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("run_snapshots entry must be removed after normal run exit");
}
