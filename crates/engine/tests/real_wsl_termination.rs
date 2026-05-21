//! Real-WSL termination propagation test.
//!
//! Verifies that `supervisor::cancel` propagates termination through
//! `wsl.exe --exec` into the in-distro `sh` and its `sleep` child.
//!
//! Skipped unless `ORDIUS_REAL_WSL_TEST=1` is set AND
//! `ORDIUS_REAL_WSL_DISTRO=<name>` names a running distro.

use ordius_engine::executor::supervisor;
use std::process::Stdio;
use std::time::Duration;
use tokio::process::Command;

#[tokio::test]
#[ignore = "requires real WSL; opt in via ORDIUS_REAL_WSL_TEST=1"]
async fn cancel_kills_in_distro_sleep() {
    if std::env::var("ORDIUS_REAL_WSL_TEST").is_err() {
        return;
    }
    let distro = std::env::var("ORDIUS_REAL_WSL_DISTRO")
        .expect("set ORDIUS_REAL_WSL_DISTRO to the running distro name");

    let mut cmd = Command::new("wsl.exe");
    cmd.arg("-d")
        .arg(&distro)
        .arg("--exec")
        .arg("/bin/sh")
        .arg("-c")
        .arg("sleep 30")
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let mut sup = supervisor::spawn(cmd).expect("spawn");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let start = std::time::Instant::now();
    let _ = supervisor::cancel(&mut sup).await;
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_secs(3),
        "cancel took {elapsed:?}, expected < 3 s",
    );
}
