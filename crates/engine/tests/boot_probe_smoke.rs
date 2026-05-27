//! Boot-probe smoke test: opening an `Engine` on an empty home seeds the
//! env registry with at least one Local env.

use ordius_engine::Engine;
use ordius_engine::environment::runtime::EnvId;
use tempfile::TempDir;

#[tokio::test(flavor = "multi_thread")]
async fn boot_probe_creates_local_env_when_db_empty() {
    let tmp = TempDir::new().unwrap();
    let engine = Engine::new(tmp.path().to_path_buf())
        .await
        .expect("engine opens");
    let entries = engine.env_registry().entries();
    assert!(
        entries.contains_key(&EnvId::local()),
        "Engine::new must synthesize Local on first run",
    );
    let catalogs = engine.env_catalogs();
    assert!(
        catalogs.contains_key(&EnvId::local()),
        "Local env must have a catalog after boot probe",
    );
}
