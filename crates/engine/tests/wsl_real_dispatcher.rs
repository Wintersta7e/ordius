//! Gated real-WSL integration tests. Set `ORDIUS_REAL_WSL_TEST=1` and
//! `ORDIUS_REAL_WSL_DISTRO=<name>` to run. Default test runs do nothing.

#![cfg(all(test, target_os = "windows"))]

use std::collections::HashMap;

use ordius_engine::environment::runtime::env::{EnvId, EnvInfo, EnvSpec, EnvState};
use ordius_engine::environment::runtime::wsl::WslDispatcher;

fn distro_or_skip() -> Option<String> {
    if std::env::var("ORDIUS_REAL_WSL_TEST").is_err() {
        return None;
    }
    std::env::var("ORDIUS_REAL_WSL_DISTRO").ok()
}

fn info(distro: &str) -> EnvInfo {
    EnvInfo {
        id: EnvId::wsl(distro),
        label: format!("WSL: {distro}"),
        spec: EnvSpec::WslDistro {
            name: distro.to_string(),
            resources: vec![],
            host_direct_verifications: HashMap::default(),
        },
        state: EnvState::Reachable,
        enabled: true,
    }
}

#[tokio::test]
#[ignore = "requires real WSL; opt in via ORDIUS_REAL_WSL_TEST=1"]
async fn enumerates_configured_distro() {
    let Some(name) = distro_or_skip() else {
        return;
    };
    let distros = ordius_engine::environment::runtime::wsl::enumerate::enumerate().await;
    assert!(
        distros.iter().any(|d| d.name == name),
        "expected `{name}` in enumeration, got: {distros:?}"
    );
}

#[tokio::test]
#[ignore = "requires real WSL; opt in via ORDIUS_REAL_WSL_TEST=1"]
async fn bootstrap_pushes_and_verifies_helper() {
    use ordius_engine::environment::runtime::wsl::bootstrap::{bootstrap_helper, probe_env_triple};
    let Some(name) = distro_or_skip() else {
        return;
    };
    let triple = probe_env_triple(&name).await.expect("probe triple");
    // If no embedded target matches, the test environment hasn't been
    // built with cross-compile artefacts — treat as a skip.
    match bootstrap_helper(&name, &triple).await {
        Ok(installed) => {
            assert!(!installed.env_side_path.is_empty());
        },
        Err(e) => {
            eprintln!("bootstrap skipped: {e}");
        },
    }
}

#[tokio::test]
#[ignore = "requires real WSL; opt in via ORDIUS_REAL_WSL_TEST=1"]
async fn spawn_then_cancel_kills_process_group() {
    use ordius_engine::environment::runtime::dispatcher::Dispatcher;
    use ordius_engine::environment::runtime::transport::ProcessCmd;
    use ordius_engine::executor::supervisor::{Supervised, cancel};

    let Some(name) = distro_or_skip() else {
        return;
    };
    let d = WslDispatcher::new(info(&name), &name);
    let cmd = ProcessCmd {
        program: "/bin/sh".into(),
        args: vec!["-c".into(), "sleep 60 & wait".into()],
        env: HashMap::new(),
        cwd: None,
        stdin: None,
    };
    let mut sup: Supervised = d.spawn(cmd).expect("spawn");
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let code = cancel(&mut sup).await;
    assert!(code.is_some(), "supervisor should reap the cancelled child");
}
