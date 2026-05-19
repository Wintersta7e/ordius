use super::*;
use serial_test::serial;
use std::collections::HashMap;

/// Initialise the sample keyring store. Idempotent — re-running
/// just resets the in-memory backend.
fn init_sample_store() {
    keyring::use_sample_store(&HashMap::from([("persist", "false")])).unwrap();
}

fn store_in(dir: &std::path::Path) -> Store {
    Store::with_index_path(dir.join("secrets-index.json"))
}

#[test]
#[serial]
fn set_then_get_roundtrips() {
    init_sample_store();
    let dir = tempfile::TempDir::new().unwrap();
    let store = store_in(dir.path());
    store.set("set-then-get", "value-1").unwrap();
    assert_eq!(store.get("set-then-get").unwrap(), "value-1");
}

#[test]
#[serial]
fn set_then_list_includes_name() {
    init_sample_store();
    let dir = tempfile::TempDir::new().unwrap();
    let store = store_in(dir.path());
    store.set("listed", "v").unwrap();
    let names = store.list().unwrap();
    assert!(names.iter().any(|n| n == "listed"));
}

#[test]
#[serial]
fn delete_removes_from_keyring_and_index() {
    init_sample_store();
    let dir = tempfile::TempDir::new().unwrap();
    let store = store_in(dir.path());
    store.set("to-delete", "v").unwrap();
    store.delete("to-delete").unwrap();
    assert!(store.get("to-delete").is_err());
    let names = store.list().unwrap();
    assert!(names.iter().all(|n| n != "to-delete"));
}

#[test]
fn list_returns_empty_when_sidecar_absent() {
    let dir = tempfile::TempDir::new().unwrap();
    let store = store_in(dir.path());
    let names = store.list().unwrap();
    assert!(names.is_empty());
}

#[test]
#[serial]
fn list_creates_parent_directory_when_setting() {
    init_sample_store();
    let dir = tempfile::TempDir::new().unwrap();
    let store = Store::with_index_path(dir.path().join("nested").join("index.json"));
    store.set("nested-test", "v").unwrap();
    assert!(dir.path().join("nested").join("index.json").exists());
}

#[test]
fn redact_replaces_full_value_match() {
    let r = redact_secrets("token=abc123", &[("TOKEN".into(), "abc123".into())]);
    assert_eq!(r, "token=<redacted:TOKEN>");
}

#[test]
fn redact_longer_value_wins_on_overlap() {
    let secrets = vec![
        ("SHORT".into(), "abc".into()),
        ("LONG".into(), "abc123".into()),
    ];
    let r = redact_secrets("got=abc123", &secrets);
    assert_eq!(r, "got=<redacted:LONG>");
}

#[test]
fn redact_skips_empty_values() {
    let r = redact_secrets("hello world", &[("EMPTY".into(), String::new())]);
    assert_eq!(r, "hello world");
}

#[test]
fn redact_passes_through_when_no_value_matches() {
    let r = redact_secrets("nothing here", &[("X".into(), "absent".into())]);
    assert_eq!(r, "nothing here");
}

#[test]
fn redact_handles_multiple_secrets() {
    let secrets = vec![("A".into(), "foo".into()), ("B".into(), "bar".into())];
    let r = redact_secrets("foo and bar", &secrets);
    assert_eq!(r, "<redacted:A> and <redacted:B>");
}
