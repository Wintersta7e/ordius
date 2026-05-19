use super::*;
use tempfile::TempDir;

#[test]
fn list_returns_empty_for_fresh_install() {
    let home = TempDir::new().unwrap();
    let ws = list(home.path()).unwrap();
    assert!(ws.is_empty());
}

#[test]
fn add_then_list_round_trips() {
    let home = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    let ws = add(home.path(), "demo project", project.path()).unwrap();
    assert_eq!(ws.name, "demo project");
    assert!(ws.path.is_absolute());
    let listed = list(home.path()).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, ws.id);
}

#[test]
fn add_rejects_non_directory() {
    let home = TempDir::new().unwrap();
    let result = add(home.path(), "x", std::path::Path::new("/no/such/place/abc"));
    assert!(matches!(result, Err(WorkspacesError::NotADirectory(_))));
}

#[test]
fn add_rejects_duplicate_path() {
    let home = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    add(home.path(), "first", project.path()).unwrap();
    let second = add(home.path(), "second", project.path());
    assert!(matches!(second, Err(WorkspacesError::DuplicatePath(_))));
}

#[test]
fn remove_unknown_id_errors() {
    let home = TempDir::new().unwrap();
    let result = remove(home.path(), "no-such-id");
    assert!(matches!(result, Err(WorkspacesError::Unknown(_))));
}

#[test]
fn remove_existing_drops_from_catalog() {
    let home = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    let ws = add(home.path(), "x", project.path()).unwrap();
    remove(home.path(), &ws.id).unwrap();
    assert!(list(home.path()).unwrap().is_empty());
}
