use super::*;
use tempfile::TempDir;

#[test]
fn load_returns_defaults_when_file_missing() {
    let home = TempDir::new().unwrap();
    let s = load(home.path()).unwrap();
    assert_eq!(s, Settings::default());
    assert_eq!(s.theme, "dark");
    assert_eq!(s.max_concurrent_runs, 4);
}

#[test]
fn save_then_load_round_trips() {
    let home = TempDir::new().unwrap();
    let s = Settings {
        theme: "light".into(),
        max_concurrent_runs: 8,
        ..Settings::default()
    };
    save(home.path(), &s).unwrap();
    let loaded = load(home.path()).unwrap();
    assert_eq!(loaded.theme, "light");
    assert_eq!(loaded.max_concurrent_runs, 8);
}

#[test]
fn load_uses_serde_defaults_for_missing_fields() {
    let home = TempDir::new().unwrap();
    // Only theme provided — every other field must fall back to its
    // serde default rather than failing the parse.
    std::fs::write(home.path().join("settings.json"), r#"{"theme":"light"}"#).unwrap();
    let s = load(home.path()).unwrap();
    assert_eq!(s.theme, "light");
    assert_eq!(s.palette_side, "left");
    assert_eq!(s.max_concurrent_runs, 4);
}
