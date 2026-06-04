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
use ordius_engine::environment::runtime::workspace::FileKind;
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
        top_run_id: "h2t2",
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
        .reconcile_in(&dispatcher, &binding, host_ws, &run)
        .await
        .expect("reconcile_in must succeed");

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
            top_run_id: &rid,
            workflow_id: "wf-wb",
            workflow_name: "Writeback Test",
            started_at_iso: "2026-01-01T00:00:00Z",
        };
        let root = mgr
            .reconcile_in(&dispatcher, &force(), host_ws, &run)
            .await
            .expect("reconcile_in uploads")
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
            top_run_id: &rid,
            workflow_id: "wf-wb",
            workflow_name: "Writeback Test",
            started_at_iso: "2026-01-01T00:00:00Z",
        };
        let root = mgr
            .reconcile_in(&dispatcher, &force(), host_ws, &run)
            .await
            .expect("reconcile_in uploads")
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

// ── End-to-end: run a shell node on the remote, write its output back ──────────

/// Drives the whole H2 path through the executor (not just the manager): a shell
/// node targeting an SSH env with an ephemeral `Sync{Force}` binding uploads the
/// (empty) host workspace, runs `echo … > result.txt` in the synced remote cwd,
/// and after `teardown_all` the output lands in the host workspace and the
/// remote root is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "gated: requires ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"]
#[allow(clippy::too_many_lines)]
async fn real_ssh_run_uploads_runs_and_writes_back() {
    use ordius_engine::checkpoints::CheckpointRegistry;
    use ordius_engine::db::open;
    use ordius_engine::emitter::Emitter;
    use ordius_engine::environment::runtime::env::{
        EnvSpec, SyncStrategy, WorkspaceBinding, WriteBackPolicy,
    };
    use ordius_engine::environment::runtime::workspace::{RunOutcome, WorkspaceManager};
    use ordius_engine::environment::runtime::{ResourceRegistry, RunSnapshot, WorkflowId};
    use ordius_engine::executor::{NodeExecutor, RunContext, SubprocessExecutor, wrap_process_env};
    use ordius_engine::recorder::RunRecorder;
    use ordius_engine::registry::Registry;
    use ordius_engine::types::{Node, Pos, Workflow};

    let Some(ssh) = build_dispatcher_for_transport_test().await else {
        eprintln!(
            "skipping e2e run test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };
    let ssh_id = ssh.info().id.clone();
    let dispatcher: Arc<dyn Dispatcher> = Arc::new(ssh);

    // Empty host workspace; the SSH env binds with an ephemeral Sync{Force}.
    let tmp = tempfile::TempDir::new().unwrap();
    let host_ws = tmp.path().to_path_buf();

    // Only `workspace_binding` is read from this spec by RunSnapshot; the
    // connection details live on the real dispatcher, so the rest are
    // placeholders.
    let spec = EnvSpec::Ssh {
        host: "unused".into(),
        port: 22,
        user: "unused".into(),
        auth: SshAuth::KeyFile {
            path: "/unused".into(),
            passphrase_ref: None,
        },
        host_key_pins: vec![],
        workspace_binding: WorkspaceBinding::Sync {
            env_path_template: "/tmp/ordius-e2e-{{run.id}}".into(),
            strategy: SyncStrategy::Sftp,
            write_back: WriteBackPolicy::Force { ignore: vec![] },
        },
        resources: vec![],
    };

    let mut dispatchers: HashMap<EnvId, Arc<dyn Dispatcher>> = HashMap::new();
    dispatchers.insert(ssh_id.clone(), Arc::clone(&dispatcher));
    let mut specs: HashMap<EnvId, EnvSpec> = HashMap::new();
    specs.insert(ssh_id.clone(), spec);

    let wf = Workflow {
        id: "ssh-e2e".into(),
        name: "SSH E2E".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        nodes: vec![Node {
            id: "run".into(),
            ty: "shell".into(),
            name: "run".into(),
            config: HashMap::from([(
                "command".into(),
                serde_json::json!("echo synced-ok > result.txt"),
            )]),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
            target_env: Some(ssh_id.clone()),
        }],
        edges: vec![],
        resources: vec![],
        default_env: None,
    };

    let pool = open(tmp.path().join("t.db")).unwrap();
    let rec =
        Arc::new(RunRecorder::start(pool.clone(), &wf, "{}", &HashMap::new(), "test").unwrap());
    let (em, _rx) = Emitter::new(rec.clone());
    let em = Arc::new(em);

    let run_snapshot = Arc::new(RunSnapshot {
        run_id: rec.run_id.clone(),
        workflow_id: WorkflowId(wf.id.clone()),
        default_env: ssh_id.clone(),
        registry: ResourceRegistry::new().snapshot(),
        dispatchers: Arc::new(dispatchers),
        catalogs: Arc::new(HashMap::new()),
        specs: Arc::new(specs),
    });

    let wm = Arc::new(WorkspaceManager::new());
    let ctx = RunContext {
        run_id: rec.run_id.clone(),
        workflow_id: wf.id.clone(),
        workflow_name: wf.name.clone(),
        started_at_iso: "2026-01-01T00:00:00Z".into(),
        workspace: host_ws.clone(),
        variables: HashMap::new(),
        recorder: rec.clone(),
        emitter: em.clone(),
        secrets_store: None,
        env: wrap_process_env(),
        current_inputs: HashMap::new(),
        upstream_outputs: HashMap::new(),
        checkpoints: Arc::new(CheckpointRegistry::new()),
        events: Arc::new(ordius_engine::events_registry::EventRegistry::new()),
        run_snapshot,
        engine: std::sync::Weak::new(),
        compose_depth: 0,
        iteration: 1,
        attempt: std::sync::atomic::AtomicU32::new(1),
        auto_resume: false,
        workspace_manager: Arc::clone(&wm),
        env_cwd: parking_lot::Mutex::new(None),
        run_cancel: tokio_util::sync::CancellationToken::new(),
    };

    // This test drives the executor directly (bypassing `run_with_retry`), so it
    // performs the same reconcile cycle the run loop owns: reconcile_in (upload +
    // record env_cwd) → run → reconcile_out (write the remote delta back).
    let binding = ctx.run_snapshot.workspace_binding(&ssh_id);
    let run_scope = ordius_engine::environment::runtime::workspace::RunScope {
        run_id: &ctx.run_id,
        top_run_id: &ctx.run_snapshot.run_id,
        workflow_id: &ctx.workflow_id,
        workflow_name: &ctx.workflow_name,
        started_at_iso: &ctx.started_at_iso,
    };
    let cwd = wm
        .reconcile_in(dispatcher.as_ref(), &binding, &host_ws, &run_scope)
        .await
        .expect("reconcile_in uploads the workspace");
    ctx.set_env_cwd(cwd);

    let reg = Registry::with_v1_0_builtins();
    let nt = reg.get("shell").expect("shell built-in registered");
    SubprocessExecutor
        .run(&wf.nodes[0], &nt, &ctx, CancellationToken::new())
        .await
        .expect("shell node runs on the remote");

    // reconcile_out writes the remote-created file back to the host workspace.
    wm.reconcile_out(dispatcher.as_ref(), &binding, &host_ws)
        .await
        .expect("reconcile_out writes the delta back");

    // Teardown deletes the ephemeral root (write-back already done above; this is
    // the safety-net no-op for the delta).
    wm.teardown_all(RunOutcome::Completed).await;

    let got = std::fs::read_to_string(host_ws.join("result.txt"))
        .expect("result.txt must be written back to the host workspace");
    assert!(got.contains("synced-ok"), "unexpected content: {got:?}");

    // The ephemeral root (named from the run id) is gone.
    let root = format!("/tmp/ordius-e2e-{}", rec.run_id);
    let t = dispatcher
        .workspace_transport()
        .unwrap()
        .open()
        .await
        .unwrap();
    assert!(
        t.stat(&root).await.unwrap().is_none(),
        "remote ephemeral root must be deleted"
    );
}

// ── Per-node reconcile (H3) e2e against a real SSH server ──────────────────────
//
// The four tests below prove the H3 behaviours the in-memory fake can't:
// downstream visibility of a remote node's output, the per-key lease serialising
// concurrent same-workspace nodes, user-cancel skipping write-back while a
// timeout still writes back, and `reconcile_in` clearing pre-existing remote
// symlinks before upload. They share the helpers in this section.

/// A unique-per-invocation run id so re-runs never collide with leftover remote
/// state under `/tmp` (production uses a unique `run.id` for the same reason).
fn uniq_run_id(label: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("h3-{label}-{}-{nanos}", std::process::id())
}

/// `Sync { Sftp, Force }` binding whose ephemeral root embeds `{{run.id}}`.
/// With a [`uniq_run_id`] the expanded root is `/tmp/ordius-h3-<run_id>`.
fn h3_force_binding() -> WorkspaceBinding {
    use ordius_engine::environment::runtime::env::{SyncStrategy, WriteBackPolicy};
    WorkspaceBinding::Sync {
        env_path_template: "/tmp/ordius-h3-{{run.id}}".into(),
        strategy: SyncStrategy::Sftp,
        write_back: WriteBackPolicy::Force { ignore: vec![] },
    }
}

/// The expanded ephemeral root for `run_id` under [`h3_force_binding`].
fn h3_root(run_id: &str) -> String {
    format!("/tmp/ordius-h3-{run_id}")
}

/// `Sync { Sftp, SafeOrDiverge { Manifest } }` binding whose ephemeral root
/// embeds `{{run.id}}`. With a [`uniq_run_id`] the expanded root is
/// `/tmp/ordius-sod-<run_id>`. Mirrors the engine-internal `sod_binding` helper
/// (manifest-mode conflict detection, no ignore patterns, default file cap).
fn sod_binding() -> WorkspaceBinding {
    use ordius_engine::environment::runtime::env::{
        ConflictDetect, SyncStrategy, WriteBackPolicy, default_max_files,
    };
    WorkspaceBinding::Sync {
        env_path_template: "/tmp/ordius-sod-{{run.id}}".into(),
        strategy: SyncStrategy::Sftp,
        write_back: WriteBackPolicy::SafeOrDiverge {
            mode: ConflictDetect::Manifest,
            ignore: vec![],
            max_files: default_max_files(),
        },
    }
}

/// The expanded ephemeral root for `run_id` under [`sod_binding`].
fn sod_root(run_id: &str) -> String {
    format!("/tmp/ordius-sod-{run_id}")
}

/// `Sync { Sftp, Force }` binding whose env root is STABLE (no `{{run.id}}`),
/// so it classifies as a *persistent* workspace: the same `slug` resolves to
/// the same root across runs, the root is reused (never auto-deleted), and a
/// remote `.ordius.lock` guards concurrent ownership. The slug is a parameter so
/// the reuse test can pass the SAME slug to two runs while the contention/temp
/// tests use their own. Force is the simplest write-back for these tests (the
/// persistent additive sync — not the write-back mode — is what's under test).
fn persistent_binding(slug: &str) -> WorkspaceBinding {
    use ordius_engine::environment::runtime::env::{SyncStrategy, WriteBackPolicy};
    WorkspaceBinding::Sync {
        env_path_template: format!("/home/ordius/ordius-persist-{slug}"),
        strategy: SyncStrategy::Sftp,
        write_back: WriteBackPolicy::Force { ignore: vec![] },
    }
}

/// The expanded persistent root for `slug` under [`persistent_binding`]. Stable
/// across runs (no `{{run.id}}`), so it is the shared root the reuse test drives
/// twice and the cleanup target the tests own (persistent roots aren't deleted
/// at teardown — only the lock is released).
fn persistent_root(slug: &str) -> String {
    format!("/home/ordius/ordius-persist-{slug}")
}

/// Walk `dir` recursively, returning every regular file's path. Used to locate
/// divergence artifacts under `<host_ws>/.ordius/diverged/` without hardcoding
/// the `encode_segment`-encoded `<run>/<env>` path components.
fn walk_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            out.extend(walk_files(&path));
        } else if path.is_file() {
            out.push(path);
        }
    }
    out
}

/// Build a `RunContext` that targets `dispatcher` (an SSH env) with an ephemeral
/// `Sync{Force}` binding, sharing the given `wm` so the per-key lease is visible
/// across contexts that share an `(env, host_ws)` key.
///
/// Mirrors the manual `RunContext` construction in
/// `real_ssh_run_uploads_runs_and_writes_back` (every field incl.
/// `env_cwd: Mutex::new(None)` + a fresh `run_cancel`). The returned context is
/// what a test drives through the run loop's reconcile cycle by hand.
#[allow(clippy::too_many_lines)]
fn h3_run_context(
    dispatcher: &Arc<dyn Dispatcher>,
    env_id: &EnvId,
    host_ws: &std::path::Path,
    run_id: &str,
    wm: &Arc<ordius_engine::environment::runtime::workspace::WorkspaceManager>,
) -> ordius_engine::executor::RunContext {
    use ordius_engine::checkpoints::CheckpointRegistry;
    use ordius_engine::db::open;
    use ordius_engine::emitter::Emitter;
    use ordius_engine::environment::runtime::env::EnvSpec;
    use ordius_engine::environment::runtime::{ResourceRegistry, RunSnapshot, WorkflowId};
    use ordius_engine::executor::{RunContext, wrap_process_env};
    use ordius_engine::recorder::RunRecorder;
    use ordius_engine::types::{Node, Pos, Workflow};

    // Only `workspace_binding` is read from this spec by RunSnapshot; the real
    // connection details live on the dispatcher, so the rest are placeholders.
    let spec = EnvSpec::Ssh {
        host: "unused".into(),
        port: 22,
        user: "unused".into(),
        auth: SshAuth::KeyFile {
            path: "/unused".into(),
            passphrase_ref: None,
        },
        host_key_pins: vec![],
        workspace_binding: h3_force_binding(),
        resources: vec![],
    };

    let mut dispatchers: HashMap<EnvId, Arc<dyn Dispatcher>> = HashMap::new();
    dispatchers.insert(env_id.clone(), Arc::clone(dispatcher));
    let mut specs: HashMap<EnvId, EnvSpec> = HashMap::new();
    specs.insert(env_id.clone(), spec);

    let wf = Workflow {
        id: "ssh-h3".into(),
        name: "SSH H3".into(),
        schema_version: 1,
        created_at: None,
        updated_at: None,
        variables: HashMap::new(),
        triggers: vec![],
        // A placeholder node so RunRecorder::start has a workflow to record;
        // the actual command run per test is supplied at call time.
        nodes: vec![Node {
            id: "run".into(),
            ty: "shell".into(),
            name: "run".into(),
            config: HashMap::new(),
            pos: Pos::default(),
            timeout_ms: None,
            retry: None,
            continue_on_error: false,
            target_env: Some(env_id.clone()),
        }],
        edges: vec![],
        resources: vec![],
        default_env: None,
    };

    // Each context gets its own throwaway DB (a unique temp dir per run_id keeps
    // concurrent contexts from sharing a SQLite file).
    let db_dir = std::env::temp_dir().join(format!("ordius-h3-db-{run_id}"));
    std::fs::create_dir_all(&db_dir).unwrap();
    let pool = open(db_dir.join("t.db")).unwrap();
    // The recorder generates its own DB run_id; we don't use it. Every
    // template/scope/snapshot below takes the caller's unique `run_id` directly,
    // so the expanded ephemeral root is `h3_root(run_id)`.
    let rec = Arc::new(
        RunRecorder::start(pool, &wf, "{}", &HashMap::new(), "test").unwrap_or_else(|e| {
            panic!("RunRecorder::start failed: {e}");
        }),
    );
    let (em, _rx) = Emitter::new(rec.clone());
    let em = Arc::new(em);

    let run_snapshot = Arc::new(RunSnapshot {
        run_id: run_id.to_string(),
        workflow_id: WorkflowId(wf.id.clone()),
        default_env: env_id.clone(),
        registry: ResourceRegistry::new().snapshot(),
        dispatchers: Arc::new(dispatchers),
        catalogs: Arc::new(HashMap::new()),
        specs: Arc::new(specs),
    });

    RunContext {
        run_id: run_id.to_string(),
        workflow_id: wf.id.clone(),
        workflow_name: wf.name,
        started_at_iso: "2026-01-01T00:00:00Z".into(),
        workspace: host_ws.to_path_buf(),
        variables: HashMap::new(),
        recorder: rec,
        emitter: em,
        secrets_store: None,
        env: wrap_process_env(),
        current_inputs: HashMap::new(),
        upstream_outputs: HashMap::new(),
        checkpoints: Arc::new(CheckpointRegistry::new()),
        events: Arc::new(ordius_engine::events_registry::EventRegistry::new()),
        run_snapshot,
        engine: std::sync::Weak::new(),
        compose_depth: 0,
        iteration: 1,
        attempt: std::sync::atomic::AtomicU32::new(1),
        auto_resume: false,
        workspace_manager: Arc::clone(wm),
        env_cwd: parking_lot::Mutex::new(None),
        run_cancel: tokio_util::sync::CancellationToken::new(),
    }
}

/// Run a single `shell` node with `command` against `ctx`'s SSH env, performing
/// the run loop's `reconcile_in → set_env_cwd → SubprocessExecutor.run` cycle.
/// The command executes in the synced remote cwd (the executor reads
/// `ctx.env_cwd()`), so a relative `> file.txt` lands inside the ephemeral root.
///
/// Returns after the node completes; the caller owns `reconcile_out`/teardown.
async fn h3_reconcile_in_and_run(
    ctx: &ordius_engine::executor::RunContext,
    dispatcher: &Arc<dyn Dispatcher>,
    env_id: &EnvId,
    command: &str,
) {
    use ordius_engine::environment::runtime::workspace::RunScope;
    use ordius_engine::executor::{NodeExecutor, SubprocessExecutor};
    use ordius_engine::registry::Registry;
    use ordius_engine::types::{Node, Pos};

    let binding = ctx.run_snapshot.workspace_binding(env_id);
    let run_scope = RunScope {
        run_id: &ctx.run_id,
        top_run_id: &ctx.run_snapshot.run_id,
        workflow_id: &ctx.workflow_id,
        workflow_name: &ctx.workflow_name,
        started_at_iso: &ctx.started_at_iso,
    };
    let cwd = ctx
        .workspace_manager
        .reconcile_in(dispatcher.as_ref(), &binding, &ctx.workspace, &run_scope)
        .await
        .expect("reconcile_in uploads the workspace");
    ctx.set_env_cwd(cwd);

    let node = Node {
        id: "run".into(),
        ty: "shell".into(),
        name: "run".into(),
        config: HashMap::from([("command".into(), serde_json::json!(command))]),
        pos: Pos::default(),
        timeout_ms: None,
        retry: None,
        continue_on_error: false,
        target_env: Some(env_id.clone()),
    };
    let reg = Registry::with_v1_0_builtins();
    let nt = reg.get("shell").expect("shell built-in registered");
    SubprocessExecutor
        .run(&node, &nt, ctx, CancellationToken::new())
        .await
        .expect("shell node runs on the remote");
}

/// Run one raw command on the remote via the dispatcher (no workspace cwd, no
/// node). Used to plant remote state (e.g. symlinks) outside the reconcile path.
/// Asserts a zero exit.
async fn h3_remote_exec(dispatcher: &Arc<dyn Dispatcher>, sh: &str) {
    let mut p = dispatcher
        .spawn(ProcessCmd {
            program: "sh".into(),
            args: vec!["-c".into(), sh.into()],
            env: HashMap::new(),
            cwd: None,
            stdin: None,
            stdout: Stdio::Piped,
            stderr: Stdio::Piped,
        })
        .await
        .expect("remote exec spawn");
    let exit = p.wait().await.expect("remote exec wait");
    assert_eq!(exit.code, 0, "remote exec `{sh}` must exit 0, got {exit:?}");
}

/// Test 1 — `reconcile_out` makes an SSH node's output visible on the host.
///
/// A downstream host node would read `handoff.txt` from the host workspace; this
/// asserts the file (written by the remote node) is present on the host after
/// `reconcile_out`, which is exactly that hand-off property.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "gated: requires ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"]
async fn real_ssh_mid_dag_handoff() {
    let Some(ssh) = build_dispatcher_for_transport_test().await else {
        eprintln!(
            "skipping mid-dag handoff test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };
    let env_id = ssh.info().id.clone();
    let dispatcher: Arc<dyn Dispatcher> = Arc::new(ssh);

    let tmp = tempfile::TempDir::new().unwrap();
    let host_ws = tmp.path().to_path_buf();
    let run_id = uniq_run_id("handoff");

    let wm = Arc::new(ordius_engine::environment::runtime::workspace::WorkspaceManager::new());
    let ctx = h3_run_context(&dispatcher, &env_id, &host_ws, &run_id, &wm);

    // Remote node writes its output into the synced cwd.
    h3_reconcile_in_and_run(&ctx, &dispatcher, &env_id, "echo node1-out > handoff.txt").await;

    // reconcile_out surfaces the remote write on the host (where a downstream
    // host node would read it).
    let binding = ctx.run_snapshot.workspace_binding(&env_id);
    wm.reconcile_out(dispatcher.as_ref(), &binding, &host_ws)
        .await
        .expect("reconcile_out writes the delta back");

    let got = std::fs::read_to_string(host_ws.join("handoff.txt"))
        .expect("handoff.txt must be written back to the host workspace");
    assert_eq!(
        got.trim(),
        "node1-out",
        "downstream host node would read node1's output; got {got:?}"
    );

    wm.teardown_all(ordius_engine::environment::runtime::workspace::RunOutcome::Completed)
        .await;
}

/// Test 2 — the per-key execution lease serialises two concurrent same-`(env,
/// host_ws)` reconcile cycles so the shared host workspace is not torn.
///
/// Two tasks each `acquire_execution_lease` on the SAME key, run a full cycle
/// against the SAME host workspace (distinct `run_id` ⇒ distinct ephemeral root;
/// distinct output file), hold the lease across the whole cycle, then drop it.
/// The lease is the thing under test: both contend the same key, one waits.
///
/// Why this isn't a tautology: both cycles share ONE `WorkspaceManager` and the
/// same `(env, host_ws)` key, so they contend the single per-key `WorkspaceState`
/// entry that `reconcile_in` inserts and `reconcile_out` reads. Without the lease,
/// task B's `reconcile_in` would clobber task A's state (a *different*
/// `env_side_root`) mid-cycle, so A's `reconcile_out` would diff against B's root
/// and either error or write the wrong delta. The lease forces A's whole cycle to
/// finish before B's begins; both files landing correctly is the observable proof.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "gated: requires ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"]
async fn real_ssh_parallel_same_env_serializes() {
    use ordius_engine::environment::runtime::workspace::{RunOutcome, WorkspaceManager};

    let Some(ssh) = build_dispatcher_for_transport_test().await else {
        eprintln!(
            "skipping parallel lease test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };
    let env_id = ssh.info().id.clone();
    let dispatcher: Arc<dyn Dispatcher> = Arc::new(ssh);

    // ONE shared host workspace and ONE shared manager — so both tasks contend
    // the same lease key `(env_id, host_ws)`.
    let tmp = tempfile::TempDir::new().unwrap();
    let host_ws = tmp.path().to_path_buf();
    let wm = Arc::new(WorkspaceManager::new());

    // One full reconcile cycle, holding the lease across the entire cycle.
    let cycle = |file: &'static str, label: &'static str| {
        let dispatcher = Arc::clone(&dispatcher);
        let env_id = env_id.clone();
        let host_ws = host_ws.clone();
        let wm = Arc::clone(&wm);
        async move {
            let key = (env_id.clone(), host_ws.clone());
            // Held across reconcile_in → run → reconcile_out → drop.
            let _lease = wm.acquire_execution_lease(key).await;

            let run_id = uniq_run_id(label);
            let ctx = h3_run_context(&dispatcher, &env_id, &host_ws, &run_id, &wm);
            h3_reconcile_in_and_run(
                &ctx,
                &dispatcher,
                &env_id,
                &format!("echo {label}-data > {file}"),
            )
            .await;
            let binding = ctx.run_snapshot.workspace_binding(&env_id);
            wm.reconcile_out(dispatcher.as_ref(), &binding, &host_ws)
                .await
                .expect("reconcile_out under lease");
            // Lease dropped here, at end of the cycle.
        }
    };

    let a = tokio::spawn(cycle("parallel-a.txt", "a"));
    let b = tokio::spawn(cycle("parallel-b.txt", "b"));
    let (ra, rb) = tokio::join!(a, b);
    ra.expect("task A must not panic");
    rb.expect("task B must not panic");

    // The lease serialised the two host-side reconciles: both files are present
    // and correct, i.e. neither cycle clobbered the shared workspace.
    let got_a = std::fs::read_to_string(host_ws.join("parallel-a.txt"))
        .expect("parallel-a.txt must be present");
    let got_b = std::fs::read_to_string(host_ws.join("parallel-b.txt"))
        .expect("parallel-b.txt must be present");
    assert_eq!(got_a.trim(), "a-data", "task A output mismatch");
    assert_eq!(got_b.trim(), "b-data", "task B output mismatch");

    wm.teardown_all(RunOutcome::Completed).await;
}

/// Test 3 — `reconcile_out` is skipped on a genuine user cancel but runs on a
/// timeout/fail-fast. Replicates the run loop's exact guard
/// `if synced && !ctx.run_cancel.is_cancelled() { reconcile_out }`
/// (see `run_with_retry` in `crates/engine/src/run.rs`, which external tests
/// cannot call directly).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "gated: requires ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"]
#[allow(clippy::too_many_lines)]
async fn real_ssh_cancel_skips_writeback_timeout_keeps() {
    use ordius_engine::environment::runtime::workspace::{RunOutcome, WorkspaceManager};

    let Some(ssh) = build_dispatcher_for_transport_test().await else {
        eprintln!(
            "skipping cancel/timeout test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };
    let env_id = ssh.info().id.clone();
    let dispatcher: Arc<dyn Dispatcher> = Arc::new(ssh);

    // ── Sub-case CANCEL: user cancel ⇒ guard fails ⇒ write-back skipped ──
    {
        let tmp = tempfile::TempDir::new().unwrap();
        let host_ws = tmp.path().to_path_buf();
        let run_id = uniq_run_id("cancel");
        let wm = Arc::new(WorkspaceManager::new());
        let ctx = h3_run_context(&dispatcher, &env_id, &host_ws, &run_id, &wm);

        h3_reconcile_in_and_run(
            &ctx,
            &dispatcher,
            &env_id,
            "echo should-not-land > cancelled.txt",
        )
        .await;

        // A genuine user cancel cancels the run-root token.
        ctx.run_cancel.cancel();

        // The run loop's guard: synced && !run_cancel.is_cancelled().
        let binding = ctx.run_snapshot.workspace_binding(&env_id);
        if !ctx.run_cancel.is_cancelled() {
            wm.reconcile_out(dispatcher.as_ref(), &binding, &host_ws)
                .await
                .expect("reconcile_out");
        }
        wm.teardown_all(RunOutcome::CancelledByUser).await;

        // Write-back was skipped: the remote-written file never reached the host.
        assert!(
            !host_ws.join("cancelled.txt").exists(),
            "user cancel must skip write-back (cancelled.txt must be absent)"
        );
        // Teardown still cleaned up the ephemeral root.
        let t = dispatcher
            .workspace_transport()
            .unwrap()
            .open()
            .await
            .unwrap();
        assert!(
            t.stat(&h3_root(&run_id)).await.unwrap().is_none(),
            "ephemeral root must be deleted even on cancel"
        );
    }

    // ── Sub-case KEEP: timeout/fail-fast ⇒ run_cancel UNcancelled ⇒ write-back ──
    {
        let tmp = tempfile::TempDir::new().unwrap();
        let host_ws = tmp.path().to_path_buf();
        let run_id = uniq_run_id("keep");
        let wm = Arc::new(WorkspaceManager::new());
        let ctx = h3_run_context(&dispatcher, &env_id, &host_ws, &run_id, &wm);

        h3_reconcile_in_and_run(&ctx, &dispatcher, &env_id, "echo should-land > kept.txt").await;

        // A timeout/fail-fast cancels only the local/attempt token, NEVER
        // run_cancel — so the guard passes and write-back runs.
        let binding = ctx.run_snapshot.workspace_binding(&env_id);
        if !ctx.run_cancel.is_cancelled() {
            wm.reconcile_out(dispatcher.as_ref(), &binding, &host_ws)
                .await
                .expect("reconcile_out");
        }
        wm.teardown_all(RunOutcome::Completed).await;

        let got = std::fs::read_to_string(host_ws.join("kept.txt"))
            .expect("kept.txt must be written back (run_cancel was not cancelled)");
        assert_eq!(got.trim(), "should-land", "kept.txt content mismatch");
    }
}

/// Test 4 — `reconcile_in`'s reset clears pre-existing remote symlinks (both a
/// target-path link and an intermediate-dir link) so no stale link redirects an
/// upload. After reset, the target path must be a regular `File` with host
/// content and the intermediate path a real directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "gated: requires ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"]
#[allow(clippy::too_many_lines)]
async fn real_ssh_reconcile_in_clears_symlinks() {
    use ordius_engine::environment::runtime::workspace::{RunOutcome, RunScope, WorkspaceManager};

    let Some(ssh) = build_dispatcher_for_transport_test().await else {
        eprintln!(
            "skipping symlink-clearing test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };
    // The symlink test drives `reconcile_in` directly (no RunContext/snapshot),
    // so it never needs the env id locally — `reconcile_in` keys its state by
    // `dispatcher.info().id` internally.
    let dispatcher: Arc<dyn Dispatcher> = Arc::new(ssh);

    // Host workspace: a regular a.txt and sub/b.txt.
    let tmp = tempfile::TempDir::new().unwrap();
    let host_ws = tmp.path().to_path_buf();
    std::fs::write(host_ws.join("a.txt"), b"host-a").unwrap();
    std::fs::create_dir(host_ws.join("sub")).unwrap();
    std::fs::write(host_ws.join("sub").join("b.txt"), b"host-b").unwrap();

    let run_id = uniq_run_id("symlink");
    let root = h3_root(&run_id);
    let wm = Arc::new(WorkspaceManager::new());

    // Plant pre-existing remote symlinks BEFORE reconcile_in:
    //   <root>/a.txt -> /etc/hostname   (a target-path symlink)
    //   <root>/sub   -> /tmp            (an intermediate-dir symlink)
    // A no-follow-unaware uploader would redirect writes through these.
    h3_remote_exec(
        &dispatcher,
        &format!("mkdir -p {root} && ln -s /etc/hostname {root}/a.txt && ln -s /tmp {root}/sub"),
    )
    .await;

    // Sanity: the planted entries really are symlinks before the reset.
    {
        let t = dispatcher
            .workspace_transport()
            .unwrap()
            .open()
            .await
            .unwrap();
        assert_eq!(
            t.stat(&format!("{root}/a.txt"))
                .await
                .unwrap()
                .map(|m| m.kind),
            Some(FileKind::Symlink),
            "precondition: a.txt must be a symlink before reconcile_in"
        );
        assert_eq!(
            t.stat(&format!("{root}/sub"))
                .await
                .unwrap()
                .map(|m| m.kind),
            Some(FileKind::Symlink),
            "precondition: sub must be a symlink before reconcile_in"
        );
    }

    // reconcile_in resets the remote tree to mirror the host (delete-before-upload
    // clears the symlinks first).
    let binding = h3_force_binding();
    let run_scope = RunScope {
        run_id: &run_id,
        top_run_id: &run_id,
        workflow_id: "wf-symlink",
        workflow_name: "Symlink Test",
        started_at_iso: "2026-01-01T00:00:00Z",
    };
    let cwd = wm
        .reconcile_in(dispatcher.as_ref(), &binding, &host_ws, &run_scope)
        .await
        .expect("reconcile_in must clear the symlinks and upload host files");
    assert_eq!(
        cwd.as_str(),
        root,
        "expanded root must match the planted root"
    );

    // Verify via the transport: a.txt is now a regular File with host content,
    // and sub is a real directory containing b.txt = host-b.
    let t = dispatcher
        .workspace_transport()
        .unwrap()
        .open()
        .await
        .unwrap();

    let a_meta = t
        .stat(&format!("{root}/a.txt"))
        .await
        .unwrap()
        .expect("a.txt must exist after reset");
    assert_eq!(
        a_meta.kind,
        FileKind::File,
        "a.txt must be a regular File after reset (symlink cleared)"
    );
    assert_eq!(
        t.download_file(&format!("{root}/a.txt")).await.unwrap(),
        b"host-a",
        "a.txt must hold host content, not the symlink target"
    );

    let sub_meta = t
        .stat(&format!("{root}/sub"))
        .await
        .unwrap()
        .expect("sub must exist after reset");
    assert_eq!(
        sub_meta.kind,
        FileKind::Dir,
        "sub must be a real directory after reset (symlink cleared)"
    );
    assert_eq!(
        t.download_file(&format!("{root}/sub/b.txt")).await.unwrap(),
        b"host-b",
        "sub/b.txt must hold host content"
    );

    drop(t);
    wm.teardown_all(RunOutcome::Completed).await;
}

/// Test 5 — `SafeOrDiverge` write-back diverges (does NOT clobber) when the host
/// is edited concurrently with the run.
///
/// The real-SSH analogue of the engine-internal `sod_host_modified_diverges`
/// unit test (which uses the in-memory fake). Sequence:
///   1. host workspace has `out.txt` = "host-original"; `reconcile_in` uploads it
///      and captures the baseline manifest.
///   2. the remote node rewrites `out.txt` = "node-output" AND creates `newdir/`.
///   3. BEFORE `reconcile_out`, the host edits `out.txt` = "user-edit" — the
///      concurrent edit that makes the write-back conflict.
///   4. `reconcile_out` (policy `SafeOrDiverge { Manifest }`) must:
///        - KEEP the host `out.txt` = "user-edit" (not clobber it),
///        - still apply the non-conflicting `newdir/` create on the host,
///        - stash the node's "node-output" bytes under
///          `<host_ws>/.ordius/diverged/<enc run>/<enc env>/out.txt`,
///        - write a `diverge-report.json` alongside it.
///
/// The divergence dir name embeds `encode_segment(run_id)`/`encode_segment(env_id)`,
/// which the integration crate can't reconstruct without depending on a private
/// helper — so the artifacts are located by walking `.ordius/diverged/`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "gated: requires ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"]
#[allow(clippy::too_many_lines)]
async fn safe_or_diverge_diverges_on_concurrent_host_edit() {
    use ordius_engine::environment::runtime::workspace::{RunOutcome, RunScope, WorkspaceManager};

    let Some(ssh) = build_dispatcher_for_transport_test().await else {
        eprintln!(
            "skipping SafeOrDiverge divergence test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };
    // This test drives `reconcile_in`/`reconcile_out` directly (no RunContext);
    // both key their state by `dispatcher.info().id` internally.
    let dispatcher: Arc<dyn Dispatcher> = Arc::new(ssh);

    // Host workspace with a single tracked file the run will conflict on.
    let tmp = tempfile::TempDir::new().unwrap();
    let host_ws = tmp.path().to_path_buf();
    std::fs::write(host_ws.join("out.txt"), b"host-original").unwrap();

    let run_id = uniq_run_id("sod");
    let root = sod_root(&run_id);
    let wm = Arc::new(WorkspaceManager::new());
    let binding = sod_binding();
    let run_scope = RunScope {
        run_id: &run_id,
        top_run_id: &run_id,
        workflow_id: "wf-sod",
        workflow_name: "SafeOrDiverge Test",
        started_at_iso: "2026-01-01T00:00:00Z",
    };

    // ── 1. Upload the host workspace + capture the baseline manifest ──
    let cwd = wm
        .reconcile_in(dispatcher.as_ref(), &binding, &host_ws, &run_scope)
        .await
        .expect("reconcile_in uploads the workspace");
    assert_eq!(
        cwd.as_str(),
        root,
        "expanded root must match the SafeOrDiverge template"
    );

    // ── 2. Remote node rewrites out.txt and creates a new dir ──
    // Runs inside the synced ephemeral root (a real node would use the env cwd).
    h3_remote_exec(
        &dispatcher,
        &format!("cd {root} && printf node-output > out.txt && mkdir -p newdir"),
    )
    .await;

    // ── 3. Concurrent host edit AFTER the baseline upload — creates the conflict ──
    std::fs::write(host_ws.join("out.txt"), b"user-edit").unwrap();

    // ── 4. reconcile_out: SafeOrDiverge keeps the host, diverges the node bytes ──
    wm.reconcile_out(dispatcher.as_ref(), &binding, &host_ws)
        .await
        .expect("reconcile_out runs the SafeOrDiverge write-back");

    // Host out.txt is KEPT — the concurrent edit is not clobbered.
    assert_eq!(
        std::fs::read(host_ws.join("out.txt")).unwrap(),
        b"user-edit",
        "SafeOrDiverge must preserve the concurrent host edit, not clobber it"
    );

    // The non-conflicting dir create still applied to the host.
    assert!(
        host_ws.join("newdir").is_dir(),
        "a non-conflicting remote dir create must still be applied to the host"
    );

    // The node's bytes are stashed under .ordius/diverged/ (path components are
    // encode_segment-encoded, so we walk the tree rather than hardcode them).
    let diverged_root = host_ws.join(".ordius").join("diverged");
    assert!(
        diverged_root.is_dir(),
        "a conflict must produce a .ordius/diverged/ tree"
    );
    let artifacts = walk_files(&diverged_root);

    // Some file named out.txt under the divergence tree holds the node output.
    let node_artifact = artifacts
        .iter()
        .find(|p| p.file_name().is_some_and(|n| n == "out.txt"))
        .expect("a diverged out.txt artifact must exist under .ordius/diverged/");
    assert_eq!(
        std::fs::read(node_artifact).unwrap(),
        b"node-output",
        "the diverged artifact must hold the node's output bytes"
    );

    // A diverge-report.json must exist somewhere under the divergence tree.
    let report_path = artifacts
        .iter()
        .find(|p| p.file_name().is_some_and(|n| n == "diverge-report.json"))
        .expect("a diverge-report.json must exist under .ordius/diverged/");
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(report_path).unwrap()).unwrap();
    let entry = report["diverged"]
        .as_array()
        .expect("report must have a diverged array")
        .iter()
        .find(|e| e["rel"] == "out.txt")
        .expect("report must record the out.txt conflict");
    assert_eq!(
        entry["reason"], "host_modified",
        "out.txt diverged because the host modified it concurrently"
    );

    // Teardown deletes the ephemeral root (write-back already ran above).
    wm.teardown_all(RunOutcome::Completed).await;

    let t = dispatcher
        .workspace_transport()
        .unwrap()
        .open()
        .await
        .unwrap();
    assert!(
        t.stat(&root).await.unwrap().is_none(),
        "remote ephemeral root must be deleted after teardown"
    );
}

/// Test 6 — a root preserved by a failed `reconcile_out` write-back is MOVED to a
/// recovery sibling by the next same-key `reconcile_in`, which then resets the
/// root clean. Real-SSH analogue of the in-memory `reconcile_in_recovers_preserved_root`
/// unit test. Sequence:
///   1. host `out.txt` = "host-original"; `reconcile_in` uploads + baselines it.
///   2. the remote node rewrites `out.txt` = "node-output".
///   3. `chmod 000` the remote file so `reconcile_out`'s download fails — the
///      write-back errors and the root is preserved (the real analogue of the
///      fake's `set_fail_download`).
///   4. `chmod 644` to heal, then `reconcile_in` again for the SAME key: the
///      preserved output is renamed to `<root>.recovery` and the root reset to
///      the host.
///   5. teardown deletes the (now ordinary) ephemeral root but leaves the
///      recovery copy for manual retrieval.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "gated: requires ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"]
async fn safe_or_diverge_recovers_preserved_root() {
    use ordius_engine::environment::runtime::workspace::{RunOutcome, RunScope, WorkspaceManager};

    let Some(ssh) = build_dispatcher_for_transport_test().await else {
        eprintln!(
            "skipping recovery test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };
    let dispatcher: Arc<dyn Dispatcher> = Arc::new(ssh);

    let tmp = tempfile::TempDir::new().unwrap();
    let host_ws = tmp.path().to_path_buf();
    std::fs::write(host_ws.join("out.txt"), b"host-original").unwrap();

    let run_id = uniq_run_id("sod-rec");
    let root = sod_root(&run_id);
    let wm = Arc::new(WorkspaceManager::new());
    let binding = sod_binding();
    let run_scope = RunScope {
        run_id: &run_id,
        top_run_id: &run_id,
        workflow_id: "wf-sod-rec",
        workflow_name: "SafeOrDiverge Recovery Test",
        started_at_iso: "2026-01-01T00:00:00Z",
    };

    // ── 1. Upload the workspace + capture the baseline ──
    let cwd = wm
        .reconcile_in(dispatcher.as_ref(), &binding, &host_ws, &run_scope)
        .await
        .expect("reconcile_in uploads the workspace");
    assert_eq!(cwd.as_str(), root);

    // ── 2. Remote node produces output ──
    h3_remote_exec(
        &dispatcher,
        &format!("cd {root} && printf node-output > out.txt"),
    )
    .await;

    // ── 3. Make the output unreadable so reconcile_out's download fails and the
    //       root is preserved ──
    h3_remote_exec(&dispatcher, &format!("chmod 000 {root}/out.txt")).await;
    wm.reconcile_out(dispatcher.as_ref(), &binding, &host_ws)
        .await
        .expect_err("reconcile_out write-back must fail on the unreadable file");

    // ── 4. Heal, then reconcile_in again for the SAME key: recover + reset ──
    h3_remote_exec(&dispatcher, &format!("chmod 644 {root}/out.txt")).await;
    let reused = wm
        .reconcile_in(dispatcher.as_ref(), &binding, &host_ws, &run_scope)
        .await
        .expect("reconcile_in must recover the preserved root, not refuse");
    assert_eq!(
        reused.as_str(),
        root,
        "the same key resolves to the same root"
    );

    let t = dispatcher
        .workspace_transport()
        .unwrap()
        .open()
        .await
        .unwrap();
    let recovery = format!("{root}.recovery");

    // The recovery sibling holds the node's preserved output verbatim.
    assert_eq!(
        t.download_file(&format!("{recovery}/out.txt"))
            .await
            .unwrap(),
        b"node-output",
        "the recovery copy must keep the node's output"
    );
    // The reset root mirrors the host workspace.
    assert_eq!(
        t.download_file(&format!("{root}/out.txt")).await.unwrap(),
        b"host-original",
        "the recovered root must be reset to the host workspace"
    );

    // ── 5. Teardown deletes the (now ordinary) ephemeral root, keeps recovery ──
    wm.teardown_all(RunOutcome::Completed).await;
    assert!(
        t.stat(&root).await.unwrap().is_none(),
        "the recovered (reset) root is a normal ephemeral root — teardown deletes it"
    );
    assert!(
        t.stat(&recovery).await.unwrap().is_some(),
        "the recovery copy survives teardown for manual retrieval"
    );

    // Leave the container tidy: the recovery tree is intentionally untracked, so
    // the test owns cleaning it up.
    h3_remote_exec(&dispatcher, &format!("rm -rf {recovery}")).await;
}

/// Test 7 — a PERSISTENT workspace is reused across two independent runs.
///
/// The stable root (no `{{run.id}}`) is created + locked by run 1, kept on
/// teardown (only the lock is released), then re-locked + re-synced by run 2 on
/// the SAME root. Asserts the three persistent-reuse invariants:
///   - run 2 re-acquires the lock run 1 released (no `WorkspaceUnavailable`),
///   - a foreign file planted between the runs SURVIVES (additive sync never
///     deletes remote-only content),
///   - run 2's host bytes overwrite the tracked file (`out.txt`).
/// The test owns cleanup: persistent roots are never auto-deleted, so it
/// `rm -rf`s the root at the end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "gated: requires ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"]
async fn persistent_reuse_across_two_runs() {
    use ordius_engine::environment::runtime::workspace::{RunOutcome, RunScope, WorkspaceManager};

    let Some(ssh) = build_dispatcher_for_transport_test().await else {
        eprintln!(
            "skipping persistent reuse test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };
    let dispatcher: Arc<dyn Dispatcher> = Arc::new(ssh);

    // One STABLE slug → one shared persistent root for BOTH runs.
    let slug = uniq_run_id("persist-reuse");
    let root = persistent_root(&slug);
    let binding = persistent_binding(&slug);

    // ── RUN 1: create + lock + upload, then teardown keeps the root ──
    let tmp1 = tempfile::TempDir::new().unwrap();
    let host_ws1 = tmp1.path().to_path_buf();
    std::fs::write(host_ws1.join("out.txt"), b"run1-output").unwrap();

    let run_id1 = uniq_run_id("persist-reuse-r1");
    let wm1 = Arc::new(WorkspaceManager::new());
    let run_scope1 = RunScope {
        run_id: &run_id1,
        top_run_id: &run_id1,
        workflow_id: "wf-persist-reuse",
        workflow_name: "Persistent Reuse Test (run 1)",
        started_at_iso: "2026-01-01T00:00:00Z",
    };

    let cwd1 = wm1
        .reconcile_in(dispatcher.as_ref(), &binding, &host_ws1, &run_scope1)
        .await
        .expect("run 1 reconcile_in acquires the lock and uploads");
    assert_eq!(
        cwd1.as_str(),
        root,
        "the persistent root is the stable template (no {{run.id}})"
    );

    // A node writes the tracked file inside the synced root.
    h3_remote_exec(
        &dispatcher,
        &format!("cd {root} && printf run1-output > out.txt"),
    )
    .await;

    wm1.reconcile_out(dispatcher.as_ref(), &binding, &host_ws1)
        .await
        .expect("run 1 reconcile_out writes back");

    // Teardown releases the lock but KEEPS the persistent root.
    wm1.teardown_all(RunOutcome::Completed).await;

    let t = dispatcher
        .workspace_transport()
        .unwrap()
        .open()
        .await
        .unwrap();
    assert!(
        t.stat(&root).await.unwrap().is_some(),
        "a persistent root must survive teardown (only the lock is released)"
    );

    // ── Plant a FOREIGN file directly on the kept root, between the runs ──
    h3_remote_exec(&dispatcher, &format!("printf FOREIGN > {root}/foreign.txt")).await;

    // ── RUN 2: a fresh manager + host re-locks + re-syncs the SAME root ──
    let tmp2 = tempfile::TempDir::new().unwrap();
    let host_ws2 = tmp2.path().to_path_buf();
    std::fs::write(host_ws2.join("out.txt"), b"run2-output").unwrap();

    let run_id2 = uniq_run_id("persist-reuse-r2");
    let wm2 = Arc::new(WorkspaceManager::new());
    let run_scope2 = RunScope {
        run_id: &run_id2,
        top_run_id: &run_id2,
        workflow_id: "wf-persist-reuse",
        workflow_name: "Persistent Reuse Test (run 2)",
        started_at_iso: "2026-01-01T00:00:01Z",
    };

    let cwd2 = wm2
        .reconcile_in(dispatcher.as_ref(), &binding, &host_ws2, &run_scope2)
        .await
        .expect("run 2 must re-acquire the lock run 1 released");
    assert_eq!(
        cwd2.as_str(),
        root,
        "run 2 resolves to the SAME persistent root"
    );

    // The root still exists, the foreign file survived (additive sync never
    // deletes remote-only content), and run 2's host bytes are now in place.
    assert!(
        t.stat(&root).await.unwrap().is_some(),
        "the persistent root is still present for run 2"
    );
    assert_eq!(
        t.download_file(&format!("{root}/foreign.txt"))
            .await
            .unwrap(),
        b"FOREIGN",
        "a foreign file planted between runs must survive the additive re-sync"
    );
    assert_eq!(
        t.download_file(&format!("{root}/out.txt")).await.unwrap(),
        b"run2-output",
        "run 2's host bytes overwrite the tracked file"
    );

    wm2.teardown_all(RunOutcome::Completed).await;
    assert!(
        t.stat(&root).await.unwrap().is_some(),
        "the persistent root is kept after run 2's teardown too"
    );

    // CLEANUP — persistent roots are never auto-deleted, so the test owns it.
    h3_remote_exec(&dispatcher, &format!("rm -rf {root}")).await;
}

/// Test 8 — the persistent `.ordius.lock` excludes a second concurrent run and
/// names the holder, then frees on teardown.
///
/// Manager A locks the root and holds it (no teardown). Manager B (a distinct
/// run id) `reconcile_in` on the same root must fail with `WorkspaceUnavailable`
/// whose message names run A's id. After A tears down, B retries and SUCCEEDS.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "gated: requires ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"]
async fn persistent_lock_contention() {
    use ordius_engine::environment::runtime::error::DispatchError;
    use ordius_engine::environment::runtime::workspace::{RunOutcome, RunScope, WorkspaceManager};

    let Some(ssh) = build_dispatcher_for_transport_test().await else {
        eprintln!(
            "skipping persistent lock contention test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };
    let dispatcher: Arc<dyn Dispatcher> = Arc::new(ssh);

    let slug = uniq_run_id("persist-lock");
    let root = persistent_root(&slug);
    let binding = persistent_binding(&slug);

    let tmp = tempfile::TempDir::new().unwrap();
    let host_ws = tmp.path().to_path_buf();
    std::fs::write(host_ws.join("a.txt"), b"x").unwrap();

    // ── Manager A acquires the lock and HOLDS it (no teardown yet) ──
    let run_id_a = uniq_run_id("persist-lock-a");
    let wm_a = Arc::new(WorkspaceManager::new());
    let scope_a = RunScope {
        run_id: &run_id_a,
        top_run_id: &run_id_a,
        workflow_id: "wf-persist-lock",
        workflow_name: "Persistent Lock Contention (A)",
        started_at_iso: "2026-01-01T00:00:00Z",
    };
    wm_a.reconcile_in(dispatcher.as_ref(), &binding, &host_ws, &scope_a)
        .await
        .expect("manager A acquires the persistent lock");

    // ── Manager B (distinct run id) is excluded while A holds the lock ──
    let run_id_b = uniq_run_id("persist-lock-b");
    let wm_b = Arc::new(WorkspaceManager::new());
    let scope_b = RunScope {
        run_id: &run_id_b,
        top_run_id: &run_id_b,
        workflow_id: "wf-persist-lock",
        workflow_name: "Persistent Lock Contention (B)",
        started_at_iso: "2026-01-01T00:00:01Z",
    };
    let err = wm_b
        .reconcile_in(dispatcher.as_ref(), &binding, &host_ws, &scope_b)
        .await
        .expect_err("manager B must be refused while A holds the lock");
    assert!(
        matches!(err, DispatchError::WorkspaceUnavailable { .. }),
        "contention must surface as WorkspaceUnavailable; got {err:?}"
    );
    assert!(
        err.to_string().contains(&run_id_a),
        "the contention error must name the holding run ({run_id_a}); got: {err}"
    );

    // ── A tears down → lock freed → B now succeeds ──
    wm_a.teardown_all(RunOutcome::Completed).await;
    let cwd_b = wm_b
        .reconcile_in(dispatcher.as_ref(), &binding, &host_ws, &scope_b)
        .await
        .expect("manager B acquires the lock once A released it");
    assert_eq!(
        cwd_b.as_str(),
        root,
        "B resolves to the same persistent root"
    );

    // CLEANUP — release B's lock, then delete the persistent root.
    wm_b.teardown_all(RunOutcome::Completed).await;
    h3_remote_exec(&dispatcher, &format!("rm -rf {root}")).await;
}

/// Test 9 — the persistent additive upload uses a lock-dir staging temp, NOT a
/// deterministic sibling `<target>.ordius.tmp`, so a pre-existing foreign file
/// at that sibling name is never clobbered.
///
/// A foreign `out.txt.ordius.tmp` is planted on the root before `reconcile_in`.
/// After the additive host→remote sync, `out.txt` holds the host's bytes AND the
/// foreign sibling temp is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "gated: requires ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"]
async fn persistent_temp_collision() {
    use ordius_engine::environment::runtime::workspace::{RunOutcome, RunScope, WorkspaceManager};

    let Some(ssh) = build_dispatcher_for_transport_test().await else {
        eprintln!(
            "skipping persistent temp collision test; set ORDIUS_REAL_SSH_TEST=1 ORDIUS_TEST_SSH_HOST=user@box"
        );
        return;
    };
    let dispatcher: Arc<dyn Dispatcher> = Arc::new(ssh);

    let slug = uniq_run_id("persist-temp");
    let root = persistent_root(&slug);
    let binding = persistent_binding(&slug);

    let tmp = tempfile::TempDir::new().unwrap();
    let host_ws = tmp.path().to_path_buf();
    std::fs::write(host_ws.join("out.txt"), b"host-bytes").unwrap();

    // Plant a foreign file at the deterministic sibling-temp name the OLD scheme
    // would have used (create the root first so the file lands inside it).
    h3_remote_exec(
        &dispatcher,
        &format!("mkdir -p {root} && printf foreign-temp > {root}/out.txt.ordius.tmp"),
    )
    .await;

    let run_id = uniq_run_id("persist-temp-r");
    let wm = Arc::new(WorkspaceManager::new());
    let run_scope = RunScope {
        run_id: &run_id,
        top_run_id: &run_id,
        workflow_id: "wf-persist-temp",
        workflow_name: "Persistent Temp Collision Test",
        started_at_iso: "2026-01-01T00:00:00Z",
    };

    let cwd = wm
        .reconcile_in(dispatcher.as_ref(), &binding, &host_ws, &run_scope)
        .await
        .expect("reconcile_in additively uploads the host file");
    assert_eq!(cwd.as_str(), root, "resolves to the stable persistent root");

    let t = dispatcher
        .workspace_transport()
        .unwrap()
        .open()
        .await
        .unwrap();
    assert_eq!(
        t.download_file(&format!("{root}/out.txt")).await.unwrap(),
        b"host-bytes",
        "the host's out.txt is uploaded"
    );
    assert_eq!(
        t.download_file(&format!("{root}/out.txt.ordius.tmp"))
            .await
            .unwrap(),
        b"foreign-temp",
        "the foreign sibling temp must be UNTOUCHED — staging is inside the lock dir"
    );

    wm.teardown_all(RunOutcome::Completed).await;
    // CLEANUP — persistent root is kept by teardown; the test owns its removal.
    h3_remote_exec(&dispatcher, &format!("rm -rf {root}")).await;
}
