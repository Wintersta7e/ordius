//! End-to-end seed install: opening `Engine::new` on an empty home
//! materialises starter workflows on disk, and each one round-trips
//! through `workflows::load`.

use ordius_engine::{Engine, workflows};
use tempfile::TempDir;

#[tokio::test]
async fn engine_new_seeds_starter_workflows() {
    let tmp = TempDir::new().unwrap();
    let home = tmp.path().to_path_buf();
    let _engine = Engine::new(home.clone()).await.expect("engine opens");

    let dir = home.join("workflows");
    let installed: Vec<_> = std::fs::read_dir(&dir)
        .expect("workflows dir exists")
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();

    assert!(installed.len() >= 3, "expected ≥3 seeds, got {installed:?}");
    for expected in [
        "starter-hello.json",
        "starter-pipeline.json",
        "starter-schedule.json",
    ] {
        assert!(
            installed.iter().any(|f| f == expected),
            "missing {expected} (have {installed:?})"
        );
    }

    for id in ["starter-hello", "starter-pipeline", "starter-schedule"] {
        let wf = workflows::load(&home, id).expect(id);
        assert_eq!(wf.id, id);
        assert!(!wf.nodes.is_empty(), "{id} has no nodes");
    }
}

#[tokio::test]
async fn engine_new_skips_seeds_when_workflows_exist() {
    let tmp = TempDir::new().unwrap();
    let home = tmp.path().to_path_buf();
    std::fs::create_dir_all(home.join("workflows")).unwrap();
    std::fs::write(
        home.join("workflows").join("user.json"),
        r#"{"id":"user","name":"u","nodes":[],"edges":[]}"#,
    )
    .unwrap();

    let _engine = Engine::new(home.clone()).await.expect("engine opens");

    assert!(
        !home.join("workflows").join("starter-hello.json").exists(),
        "seeds should not overwrite pre-existing user workflows"
    );
}
