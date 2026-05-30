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
