//! Pure-logic boundary tests for SSH host-key pinning.
//!
//! These tests exercise [`matches_any_pin`] and [`PresentedHostKey::to_pin`]
//! without any russh network calls. They compile under all feature flags.

use chrono::Utc;
use ordius_engine::environment::runtime::SshHostKeyPin;

#[test]
fn ssh_host_key_pin_accepts_matching_algorithm_and_fingerprint() {
    let pin = SshHostKeyPin {
        algorithm: "ssh-ed25519".into(),
        sha256: "SHA256:abc123".into(),
        public_key_openssh: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIabc".into(),
        pinned_at: Utc::now(),
    };
    let presented = ordius_engine::environment::runtime::ssh::host_key::PresentedHostKey {
        algorithm: "ssh-ed25519".into(),
        sha256: "SHA256:abc123".into(),
        public_key_openssh: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIabc".into(),
    };

    assert!(
        ordius_engine::environment::runtime::ssh::host_key::matches_any_pin(&presented, &[pin])
    );
}

#[test]
fn ssh_host_key_pin_rejects_mismatch() {
    let pin = SshHostKeyPin {
        algorithm: "ssh-ed25519".into(),
        sha256: "SHA256:old".into(),
        public_key_openssh: "ssh-ed25519 AAAAold".into(),
        pinned_at: Utc::now(),
    };
    let presented = ordius_engine::environment::runtime::ssh::host_key::PresentedHostKey {
        algorithm: "ssh-ed25519".into(),
        sha256: "SHA256:new".into(),
        public_key_openssh: "ssh-ed25519 AAAAnew".into(),
    };

    assert!(
        !ordius_engine::environment::runtime::ssh::host_key::matches_any_pin(&presented, &[pin])
    );
}

#[test]
fn ssh_host_key_enrollment_builds_inline_pin() {
    let presented = ordius_engine::environment::runtime::ssh::host_key::PresentedHostKey {
        algorithm: "ssh-ed25519".into(),
        sha256: "SHA256:enrolled".into(),
        public_key_openssh: "ssh-ed25519 AAAAenrolled".into(),
    };

    let pin = presented.to_pin(Utc::now());
    assert_eq!(pin.algorithm, "ssh-ed25519");
    assert_eq!(pin.sha256, "SHA256:enrolled");
    assert_eq!(pin.public_key_openssh, "ssh-ed25519 AAAAenrolled");
}

use ordius_engine::environment::runtime::{SecretRef, SshAuth};

#[tokio::test(flavor = "multi_thread")]
async fn ssh_bootstrap_uses_home_cache_not_tmp() {
    use ordius_engine::environment::runtime::ssh::bootstrap::{FakeSftp, SshBootstrapper};

    let sftp = FakeSftp::new("/home/me")
        .with_uploaded_sha("abc123")
        .with_embedded("x86_64-unknown-linux-musl", b"helper", "abc123");
    let bootstrapper = SshBootstrapper::with_helper_source(sftp.clone(), sftp.helper_source());

    let helper = bootstrapper
        .bootstrap("x86_64-unknown-linux-musl")
        .await
        .unwrap();

    assert!(helper.env_side_path.starts_with("/home/me/.cache/ordius/"));
    assert!(
        sftp.renames()
            .iter()
            .any(|(_, dst)| dst == &helper.env_side_path)
    );
    assert!(sftp.modes().iter().any(|(_, mode)| *mode == 0o755));
    assert!(
        sftp.removes().contains(&helper.env_side_path),
        "remove_file must be called on the destination before rename (SFTP v3 cannot overwrite)"
    );
}

#[test]
fn ssh_password_auth_resolves_secret_ref() {
    keyring::use_sample_store(&std::collections::HashMap::from([("persist", "false")])).unwrap();
    let dir = tempfile::TempDir::new().unwrap();
    let store = ordius_engine::Store::with_index_path(dir.path().join("secrets.json"));
    store.set("ssh-password", "s3cr3t").unwrap();

    let resolved = ordius_engine::environment::runtime::ssh::auth::resolve_auth_material(
        &store,
        &SshAuth::Password {
            secret_ref: SecretRef("ssh-password".into()),
        },
    )
    .unwrap();

    assert_eq!(
        resolved,
        ordius_engine::environment::runtime::ssh::auth::ResolvedSshAuth::Password("s3cr3t".into())
    );
}

#[test]
fn ssh_agent_auth_is_explicitly_deferred() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = ordius_engine::Store::with_index_path(dir.path().join("secrets.json"));

    let err = ordius_engine::environment::runtime::ssh::auth::resolve_auth_material(
        &store,
        &SshAuth::Agent {
            public_key_path: None,
            fingerprint: None,
        },
    )
    .unwrap_err();

    assert!(err.to_string().contains("SSH agent auth is deferred"));
}

// ── Connection-cache tests ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn ssh_connection_cache_reuses_open_connection() {
    use ordius_engine::environment::runtime::ssh::connection::{
        FakeSshConnector, SshConnectionCache, SshConnectionLike,
    };

    let connector = FakeSshConnector::new()
        .with_connection("c1", false)
        .with_connection("c2", false);
    let cache = SshConnectionCache::new(connector.clone(), "ssh:test");

    let first = cache.connection().await.unwrap();
    let second = cache.connection().await.unwrap();

    assert_eq!(first.id(), "c1");
    assert_eq!(second.id(), "c1");
    assert_eq!(connector.connect_count(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn ssh_connection_cache_reconnects_closed_connection_once() {
    use ordius_engine::environment::runtime::ssh::connection::{
        FakeSshConnector, SshConnectionCache, SshConnectionLike,
    };

    let connector = FakeSshConnector::new()
        .with_connection("closed", true)
        .with_connection("fresh", false);
    let cache = SshConnectionCache::new(connector.clone(), "ssh:test");

    let first = cache.connection().await.unwrap();
    assert_eq!(first.id(), "fresh");
    assert_eq!(connector.connect_count(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn ssh_connection_cache_returns_error_when_both_attempts_closed() {
    use ordius_engine::environment::runtime::DispatchError;
    use ordius_engine::environment::runtime::ssh::connection::{
        FakeSshConnector, SshConnectionCache,
    };

    // Both connections the fake returns are already closed.
    let connector = FakeSshConnector::new()
        .with_connection("dead-1", true)
        .with_connection("dead-2", true);
    let cache = SshConnectionCache::new(connector.clone(), "ssh:test");

    let result = cache.connection().await;

    // Must return an error — not a closed connection.
    assert!(
        matches!(result, Err(DispatchError::EnvUnreachable { .. })),
        "expected EnvUnreachable, got {result:?}",
    );
    // Both connect attempts must have been made.
    assert_eq!(connector.connect_count(), 2);
}

// ── SSH transport URL rewrite ─────────────────────────────────────────────────

#[test]
fn ssh_transport_rewrites_url_to_local_listener() {
    let original = "http://127.0.0.1:11434/api/version?x=1";
    let rewritten = ordius_engine::environment::runtime::ssh::transport::rewrite_url_to_listener(
        original, 43210,
    )
    .unwrap();

    assert_eq!(rewritten, "http://127.0.0.1:43210/api/version?x=1");
}

#[test]
fn ssh_transport_rewrite_rejects_missing_port() {
    let err = ordius_engine::environment::runtime::ssh::transport::remote_authority(
        &url::Url::parse("http://127.0.0.1/api").unwrap(),
    )
    .unwrap_err();

    assert!(err.to_string().contains("port"));
}

// ── Exec-request boundary ─────────────────────────────────────────────────────

#[test]
fn ssh_exec_request_preserves_argv_env_cwd_and_stdin() {
    use bytes::Bytes;
    use std::collections::HashMap;

    let req = ordius_engine::environment::runtime::ssh::exec::exec_request_from_cmd(
        &ordius_engine::environment::runtime::ProcessCmd {
            program: "python3".into(),
            args: vec!["-c".into(), "print('hi')".into()],
            env: HashMap::from([("A".into(), "B".into())]),
            cwd: Some(ordius_engine::environment::runtime::EnvPath::new("/work")),
            stdin: Some(Bytes::from_static(b"input")),
            stdout: ordius_engine::environment::runtime::transport::Stdio::Piped,
            stderr: ordius_engine::environment::runtime::transport::Stdio::Piped,
        },
    );

    assert_eq!(req.version, 1);
    assert_eq!(req.program, "python3");
    assert_eq!(req.args, vec!["-c", "print('hi')"]);
    assert_eq!(req.env.get("A").map(String::as_str), Some("B"));
    assert_eq!(req.cwd.as_deref(), Some("/work"));
    assert_eq!(req.stdin_b64.as_deref(), Some("aW5wdXQ="));
}
