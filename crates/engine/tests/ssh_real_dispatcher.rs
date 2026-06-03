//! Gated real-SSH integration test for Phase G.
//!
//! Skips unless `ORDIUS_REAL_SSH_TEST=1` and `ORDIUS_TEST_SSH_HOST=user@box`
//! (and optionally `ORDIUS_TEST_SSH_KEY=<path>`) are set.
//!
//! What it covers, in order:
//! 1. TOFU enroll — connect with `HostKeyHandler::enroll`, capture the pin.
//! 2. Dispatcher build — `SshDispatcher::new` with the captured pin.
//! 3. Empty probe — `probe(empty plan)` must succeed and bootstrap the helper.
//! 4. Exec — `spawn(printf real-ssh-ok)` must return `"real-ssh-ok"` on stdout.
//! 5. Cancel — `spawn(sleep 60)` + `cancel()` must terminate promptly.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ordius_engine::environment::runtime::dispatcher::Dispatcher;
use ordius_engine::environment::runtime::ssh::SshDispatcher;
use ordius_engine::environment::runtime::ssh::config::SshConfig;
use ordius_engine::environment::runtime::ssh::host_key::HostKeyHandler;
use ordius_engine::environment::runtime::transport::Stdio;
use ordius_engine::environment::runtime::{
    EnvId, EnvInfo, EnvSpec, EnvState, ProbePlan, ProcessCmd, SshAuth, WorkspaceBinding,
};
use tokio_util::sync::CancellationToken;

// ── Gate ─────────────────────────────────────────────────────────────────────

/// Returns `Some((user, host, port))` when the real-SSH gate is open.
///
/// Requires:
/// - `ORDIUS_REAL_SSH_TEST=1`
/// - `ORDIUS_TEST_SSH_HOST=user@host[:port]`  (port defaults to 22)
fn real_ssh_target() -> Option<(String, String, u16)> {
    if std::env::var("ORDIUS_REAL_SSH_TEST").ok().as_deref() != Some("1") {
        return None;
    }
    let raw = std::env::var("ORDIUS_TEST_SSH_HOST").ok()?;
    let (user, host_port) = raw.split_once('@')?;
    let (host, port) = match host_port.rsplit_once(':') {
        Some((h, p)) if p.chars().all(|c| c.is_ascii_digit()) => (h.to_string(), p.parse().ok()?),
        _ => (host_port.to_string(), 22),
    };
    Some((user.to_string(), host, port))
}

// ── Test ──────────────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::too_many_lines)]
async fn real_ssh_probe_exec_and_cancel() {
    let Some((user, host, port)) = real_ssh_target() else {
        eprintln!(
            "skipping real SSH test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };

    // ── Keyring + secret store ────────────────────────────────────────────────
    keyring::use_sample_store(&HashMap::from([("persist", "false")])).unwrap();
    let tmp = tempfile::TempDir::new().unwrap();
    let store = Arc::new(ordius_engine::Store::with_index_path(
        tmp.path().join("secrets.json"),
    ));

    // ── Resolve key path ──────────────────────────────────────────────────────
    let key_path = std::env::var("ORDIUS_TEST_SSH_KEY").unwrap_or_else(|_| {
        format!(
            "{}/.ssh/id_ed25519",
            std::env::var("HOME").expect("HOME not set")
        )
    });
    let auth = SshAuth::KeyFile {
        path: key_path,
        passphrase_ref: None,
    };

    // ── Step 1: TOFU enroll — capture the host key via an enroll connect ──────
    //
    // `RusshConnector` uses `HostKeyHandler::pinned(pins)` and rejects any key
    // not matching a pin.  With `host_key_pins: vec![]` the pinned handler
    // would reject every key, so we must enroll first.
    //
    // Mirror the pattern from `Engine::test_ssh_enrollment` and
    // `crates/engine/examples/ssh_spike.rs`: open with `HostKeyHandler::enroll`,
    // read the captured key, disconnect, then build a real pin.
    let pin = {
        use ordius_engine::environment::runtime::ssh::auth::{
            authenticate_session, resolve_auth_material,
        };

        let resolved = resolve_auth_material(&store, &auth).expect("resolve auth material");

        let enroll_handler = HostKeyHandler::enroll();
        let captured_arc = enroll_handler.captured_key();

        let config = russh::client::Config {
            inactivity_timeout: Some(Duration::from_secs(30)),
            ..Default::default()
        };

        let mut session = tokio::time::timeout(
            Duration::from_secs(15),
            russh::client::connect(Arc::new(config), (host.as_str(), port), enroll_handler),
        )
        .await
        .expect("enroll connect timed out")
        .expect("enroll connect failed");

        authenticate_session(&mut session, &user, resolved)
            .await
            .expect("enroll auth failed");

        // Grab the key the handler captured during the handshake.
        let presented = captured_arc
            .lock()
            .await
            .take()
            .expect("HostKeyHandler::enroll must capture the server key");

        // Disconnect cleanly before re-connecting through the dispatcher.
        drop(
            session
                .disconnect(russh::Disconnect::ByApplication, "enroll done", "en")
                .await,
        );

        presented.to_pin(chrono::Utc::now())
    };

    // ── Step 2: Build EnvSpec + SshDispatcher with the pinned key ────────────
    let spec = EnvSpec::Ssh {
        host: host.clone(),
        port,
        user: user.clone(),
        auth: auth.clone(),
        host_key_pins: vec![pin],
        workspace_binding: WorkspaceBinding::Unsupported,
        resources: Vec::new(),
    };
    let info = EnvInfo {
        id: EnvId::ssh("real-test"),
        label: "Real SSH Test".into(),
        spec: spec.clone(),
        state: EnvState::Probing,
        enabled: true,
    };
    let cfg =
        SshConfig::from_spec(&spec).expect("SshConfig::from_spec must return Some for SSH spec");
    let dispatcher = SshDispatcher::new(info, cfg, Arc::clone(&store));

    // ── Step 3: Empty probe (also bootstraps the helper) ─────────────────────
    //
    // An empty plan has `defs: vec![]` so `total_probed == 0`, but the probe
    // path still runs `ensure_helper` which SFTP-uploads the binary.  If this
    // succeeds the connection + bootstrap are working end-to-end.
    let probe = dispatcher
        .probe(
            ProbePlan {
                env_id: EnvId::ssh("real-test"),
                registry_revision: 0,
                defs: Vec::new(),
                per_resource_timeout: Duration::from_secs(5),
                max_concurrency: 1,
                overall_budget: Duration::from_secs(30),
            },
            CancellationToken::new(),
        )
        .await
        .expect("empty probe must succeed after connect + bootstrap");

    assert_eq!(probe.total_probed, 0, "empty plan: no resources probed");

    // ── Step 4: Exec — printf real-ssh-ok ────────────────────────────────────
    let mut proc = dispatcher
        .spawn(ProcessCmd {
            program: "sh".into(),
            args: vec!["-c".into(), "printf real-ssh-ok".into()],
            env: HashMap::new(),
            cwd: None,
            stdin: None,
            stdout: Stdio::Piped,
            stderr: Stdio::Piped,
        })
        .await
        .expect("spawn must succeed");

    let mut stdout = String::new();
    if let Some(pipe) = proc.take_stdout() {
        use tokio::io::AsyncReadExt as _;
        tokio::io::BufReader::new(pipe)
            .read_to_string(&mut stdout)
            .await
            .expect("read stdout");
    }
    let exit = proc.wait().await.expect("wait");

    assert_eq!(stdout, "real-ssh-ok", "exec stdout must match");
    assert_eq!(exit.code, 0, "exec exit code must be 0");

    // ── Step 5: Cancel — sleep 60 must terminate promptly ────────────────────
    //
    // Verifies T10's channel-close cancel: sshd receives SIGHUP when the
    // channel closes, which the remote helper's signal-forwarder (T3)
    // translates into a SIGKILL on the remote child group.
    let mut long_proc = dispatcher
        .spawn(ProcessCmd {
            program: "sh".into(),
            args: vec!["-c".into(), "sleep 60".into()],
            env: HashMap::new(),
            cwd: None,
            stdin: None,
            stdout: Stdio::Piped,
            stderr: Stdio::Piped,
        })
        .await
        .expect("spawn sleep must succeed");

    // Give the remote sleep time to start.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let cancel_start = std::time::Instant::now();
    long_proc.cancel().await.expect("cancel");
    let cancel_exit = long_proc.wait().await.expect("wait after cancel");
    let cancel_elapsed = cancel_start.elapsed();

    assert!(
        cancel_elapsed < Duration::from_secs(15),
        "cancel must be prompt — elapsed: {cancel_elapsed:?}"
    );
    assert!(
        cancel_exit.code != 0 || cancel_exit.signal.is_some(),
        "cancelled process must not exit with code 0 and no signal, got: {cancel_exit:?}"
    );
}

// ── Workspace transport round-trip ────────────────────────────────────────────

/// Build an `SshDispatcher` from the real-SSH gate env vars.
///
/// Mirrors the enroll → pin → build pattern in `real_ssh_probe_exec_and_cancel`
/// so this test exercises the same code path independently.
async fn build_dispatcher_for_transport_test() -> Option<SshDispatcher> {
    let (user, host, port) = real_ssh_target()?;

    keyring::use_sample_store(&std::collections::HashMap::from([("persist", "false")])).unwrap();
    let tmp = tempfile::TempDir::new().unwrap();
    let store = Arc::new(ordius_engine::Store::with_index_path(
        tmp.path().join("secrets.json"),
    ));

    let key_path = std::env::var("ORDIUS_TEST_SSH_KEY").unwrap_or_else(|_| {
        format!(
            "{}/.ssh/id_ed25519",
            std::env::var("HOME").expect("HOME not set")
        )
    });
    let auth = SshAuth::KeyFile {
        path: key_path,
        passphrase_ref: None,
    };

    // TOFU enroll
    let pin = {
        use ordius_engine::environment::runtime::ssh::auth::{
            authenticate_session, resolve_auth_material,
        };
        let resolved = resolve_auth_material(&store, &auth).expect("resolve auth material");
        let enroll_handler = HostKeyHandler::enroll();
        let captured_arc = enroll_handler.captured_key();
        let config = russh::client::Config {
            inactivity_timeout: Some(Duration::from_secs(30)),
            ..Default::default()
        };
        let mut session = tokio::time::timeout(
            Duration::from_secs(15),
            russh::client::connect(Arc::new(config), (host.as_str(), port), enroll_handler),
        )
        .await
        .expect("enroll connect timed out")
        .expect("enroll connect failed");
        authenticate_session(&mut session, &user, resolved)
            .await
            .expect("enroll auth failed");
        let presented = captured_arc
            .lock()
            .await
            .take()
            .expect("HostKeyHandler::enroll must capture the server key");
        drop(
            session
                .disconnect(russh::Disconnect::ByApplication, "enroll done", "en")
                .await,
        );
        presented.to_pin(chrono::Utc::now())
    };

    let spec = EnvSpec::Ssh {
        host: host.clone(),
        port,
        user,
        auth,
        host_key_pins: vec![pin],
        workspace_binding: WorkspaceBinding::Unsupported,
        resources: Vec::new(),
    };
    let info = EnvInfo {
        id: EnvId::ssh("transport-test"),
        label: "Transport Test".into(),
        spec: spec.clone(),
        state: EnvState::Probing,
        enabled: true,
    };
    let cfg = SshConfig::from_spec(&spec).expect("SshConfig from spec");
    Some(SshDispatcher::new(info, cfg, store))
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "gated: requires ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"]
async fn real_ssh_workspace_transport_round_trip() {
    let Some(dispatcher) = build_dispatcher_for_transport_test().await else {
        eprintln!(
            "skipping workspace transport test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };

    let factory = dispatcher
        .workspace_transport()
        .expect("SshDispatcher must expose a workspace transport");
    let t = factory.open().await.expect("open sftp transport");

    // mkdir (with parent creation)
    t.mkdir(".cache/ordius/h1-test").await.unwrap();

    // upload + download
    t.upload_file(".cache/ordius/h1-test/x.txt", b"phase-h1")
        .await
        .unwrap();
    assert_eq!(
        t.download_file(".cache/ordius/h1-test/x.txt")
            .await
            .unwrap(),
        b"phase-h1"
    );

    // list_tree
    let listing = t.list_tree(".cache/ordius/h1-test").await.unwrap();
    assert!(
        listing.iter().any(|m| m.rel_path.ends_with("x.txt")),
        "listing must include x.txt; got: {listing:?}"
    );

    // stat
    let md = t
        .stat(".cache/ordius/h1-test/x.txt")
        .await
        .unwrap()
        .expect("stat must find the file");
    assert_eq!(md.size, 8, "size must be 8 bytes");

    // stat non-existent → None
    assert!(
        t.stat(".cache/ordius/h1-test/no-such-file")
            .await
            .unwrap()
            .is_none(),
        "stat of missing path must return None"
    );

    // cleanup
    t.remove_file(".cache/ordius/h1-test/x.txt").await.unwrap();
    t.remove_dir(".cache/ordius/h1-test").await.unwrap();
}

// ── Ephemeral workspace upload ────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
#[ignore = "gated: requires ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"]
async fn real_ssh_ephemeral_upload_makes_files_visible() {
    let Some(dispatcher) = build_dispatcher_for_transport_test().await else {
        eprintln!(
            "skipping ephemeral upload test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };

    // Build a temporary host workspace: a.txt, sub/b.txt, .git/HEAD, .env
    let tmp = tempfile::TempDir::new().unwrap();
    let host_ws = tmp.path();
    std::fs::write(host_ws.join("a.txt"), b"hello-a").unwrap();
    std::fs::create_dir(host_ws.join("sub")).unwrap();
    std::fs::write(host_ws.join("sub").join("b.txt"), b"hello-b").unwrap();
    std::fs::create_dir(host_ws.join(".git")).unwrap();
    std::fs::write(host_ws.join(".git").join("HEAD"), b"ref: refs/heads/main").unwrap();
    std::fs::write(host_ws.join(".env"), b"SECRET=x").unwrap();

    let mgr = ordius_engine::environment::runtime::workspace::WorkspaceManager::new();
    let run = ordius_engine::environment::runtime::workspace::RunScope {
        run_id: "h2t2",
        workflow_id: "wf-test",
        workflow_name: "Upload Test",
        started_at_iso: "2026-01-01T00:00:00Z",
    };
    let binding = ordius_engine::environment::runtime::env::WorkspaceBinding::Sync {
        env_path_template: "/tmp/ordius-{{run.id}}".into(),
        strategy: ordius_engine::environment::runtime::env::SyncStrategy::Sftp,
        write_back: ordius_engine::environment::runtime::env::WriteBackPolicy::None,
    };

    let cwd = mgr
        .resolve_cwd(&dispatcher, &binding, host_ws, &run)
        .await
        .expect("resolve_cwd must succeed");

    assert_eq!(
        cwd.as_str(),
        "/tmp/ordius-h2t2",
        "env-side root must match template expansion"
    );

    // Verify uploaded files via a fresh transport session.
    let factory = dispatcher
        .workspace_transport()
        .expect("SshDispatcher must expose workspace transport");
    let t = factory.open().await.expect("open sftp transport");

    // a.txt must exist with correct content.
    let got_a = t
        .download_file("/tmp/ordius-h2t2/a.txt")
        .await
        .expect("a.txt must be present");
    assert_eq!(got_a, b"hello-a", "a.txt content mismatch");

    // sub/b.txt must exist with correct content.
    let got_b = t
        .download_file("/tmp/ordius-h2t2/sub/b.txt")
        .await
        .expect("sub/b.txt must be present");
    assert_eq!(got_b, b"hello-b", "sub/b.txt content mismatch");

    // .git/HEAD must NOT have been uploaded (default ignore).
    assert!(
        t.stat("/tmp/ordius-h2t2/.git/HEAD")
            .await
            .unwrap()
            .is_none(),
        ".git/HEAD must not be uploaded"
    );

    // .env must NOT have been uploaded (default ignore).
    assert!(
        t.stat("/tmp/ordius-h2t2/.env").await.unwrap().is_none(),
        ".env must not be uploaded"
    );

    // cleanup
    t.remove_file("/tmp/ordius-h2t2/a.txt").await.unwrap();
    t.remove_file("/tmp/ordius-h2t2/sub/b.txt").await.unwrap();
    drop(t.remove_dir("/tmp/ordius-h2t2/sub").await);
    drop(t.remove_dir("/tmp/ordius-h2t2").await);
}

// ── Force write-back + ephemeral cleanup ──────────────────────────────────────

/// End-to-end teardown over real SFTP: upload (Force) → simulate the run
/// changing + creating files → `teardown_all` writes changes back to the host
/// and deletes the ephemeral root. User-cancel skips write-back but still
/// deletes the root.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "gated: requires ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"]
#[allow(clippy::too_many_lines)]
async fn real_ssh_force_writeback_and_ephemeral_cleanup() {
    use ordius_engine::environment::runtime::env::{SyncStrategy, WriteBackPolicy};
    use ordius_engine::environment::runtime::workspace::{RunOutcome, RunScope, WorkspaceManager};

    let Some(dispatcher) = build_dispatcher_for_transport_test().await else {
        eprintln!(
            "skipping force write-back test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };

    let force = || WorkspaceBinding::Sync {
        env_path_template: "/tmp/ordius-wb-{{run.id}}".into(),
        strategy: SyncStrategy::Sftp,
        write_back: WriteBackPolicy::Force { ignore: Vec::new() },
    };

    // Unique per-invocation run ids so re-runs never collide with leftover
    // remote state (production uses a unique run.id for the same reason).
    let uniq = |label: &str| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{label}-{}-{nanos}", std::process::id())
    };

    // ── Case 1: clean completion → write back changed + new files, delete root ──
    {
        let tmp = tempfile::TempDir::new().unwrap();
        let host_ws = tmp.path();
        std::fs::write(host_ws.join("a.txt"), b"original").unwrap();

        let mgr = WorkspaceManager::new();
        let rid = uniq("h2t3done");
        let run = RunScope {
            run_id: &rid,
            workflow_id: "wf-wb",
            workflow_name: "Writeback Test",
            started_at_iso: "2026-01-01T00:00:00Z",
        };
        let root = mgr
            .resolve_cwd(&dispatcher, &force(), host_ws, &run)
            .await
            .expect("resolve_cwd uploads")
            .as_str()
            .to_string();
        assert!(
            root.starts_with("/tmp/ordius-wb-h2t3done-"),
            "root must reflect the unique run id; got {root}"
        );

        // Simulate the run: modify a.txt and create sub/new.txt on the remote.
        let t = dispatcher
            .workspace_transport()
            .unwrap()
            .open()
            .await
            .unwrap();
        // A real run rewrites file contents in place; SFTP rename can't
        // overwrite an existing path, so remove then re-upload to reach the
        // "modified" state this test needs.
        t.remove_file(&format!("{root}/a.txt")).await.unwrap();
        t.upload_file(&format!("{root}/a.txt"), b"modified")
            .await
            .unwrap();
        t.upload_file(&format!("{root}/sub/new.txt"), b"created")
            .await
            .unwrap();
        drop(t);

        mgr.teardown_all(RunOutcome::Completed).await;

        // Changed + new files are written back to the host.
        assert_eq!(std::fs::read(host_ws.join("a.txt")).unwrap(), b"modified");
        assert_eq!(
            std::fs::read(host_ws.join("sub").join("new.txt")).unwrap(),
            b"created"
        );

        // Ephemeral root is deleted.
        let t = dispatcher
            .workspace_transport()
            .unwrap()
            .open()
            .await
            .unwrap();
        assert!(
            t.stat(&format!("{root}/a.txt")).await.unwrap().is_none(),
            "remote file must be gone"
        );
        assert!(
            t.stat(&root).await.unwrap().is_none(),
            "remote root must be gone"
        );
    }

    // ── Case 2: user cancel → skip write-back, still delete root ──
    {
        let tmp = tempfile::TempDir::new().unwrap();
        let host_ws = tmp.path();
        std::fs::write(host_ws.join("a.txt"), b"original").unwrap();

        let mgr = WorkspaceManager::new();
        let rid = uniq("h2t3cancel");
        let run = RunScope {
            run_id: &rid,
            workflow_id: "wf-wb",
            workflow_name: "Writeback Test",
            started_at_iso: "2026-01-01T00:00:00Z",
        };
        let root = mgr
            .resolve_cwd(&dispatcher, &force(), host_ws, &run)
            .await
            .expect("resolve_cwd uploads")
            .as_str()
            .to_string();

        let t = dispatcher
            .workspace_transport()
            .unwrap()
            .open()
            .await
            .unwrap();
        // A real run rewrites file contents in place; SFTP rename can't
        // overwrite an existing path, so remove then re-upload to reach the
        // "modified" state this test needs.
        t.remove_file(&format!("{root}/a.txt")).await.unwrap();
        t.upload_file(&format!("{root}/a.txt"), b"modified")
            .await
            .unwrap();
        drop(t);

        mgr.teardown_all(RunOutcome::CancelledByUser).await;

        // Write-back skipped: host file untouched.
        assert_eq!(std::fs::read(host_ws.join("a.txt")).unwrap(), b"original");
        // Cleanup still happened.
        let t = dispatcher
            .workspace_transport()
            .unwrap()
            .open()
            .await
            .unwrap();
        assert!(
            t.stat(&root).await.unwrap().is_none(),
            "remote root must be gone even after cancel"
        );
    }
}
