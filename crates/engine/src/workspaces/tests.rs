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

#[test]
fn rename_updates_display_name_keeps_id_and_path() {
    let home = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    let original = add(home.path(), "old name", project.path()).unwrap();

    let updated = rename(home.path(), &original.id, "  new name  ").unwrap();
    assert_eq!(updated.id, original.id);
    assert_eq!(updated.path, original.path);
    assert_eq!(updated.name, "new name", "leading/trailing space trimmed");

    let listed = list(home.path()).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "new name");
}

#[test]
fn rename_unknown_id_errors() {
    let home = TempDir::new().unwrap();
    let result = rename(home.path(), "no-such-id", "any");
    assert!(matches!(result, Err(WorkspacesError::Unknown(_))));
}

#[test]
fn find_returns_workspace_by_id() {
    let home = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    let ws = add(home.path(), "demo", project.path()).unwrap();
    let found = find(home.path(), &ws.id).unwrap();
    assert_eq!(found.id, ws.id);
    assert_eq!(found.name, "demo");
    assert_eq!(found.path, ws.path);
}

#[test]
fn find_unknown_id_errors() {
    let home = TempDir::new().unwrap();
    let result = find(home.path(), "no-such-id");
    assert!(matches!(result, Err(WorkspacesError::Unknown(_))));
}

#[test]
fn rename_rejects_empty_name() {
    let home = TempDir::new().unwrap();
    let project = TempDir::new().unwrap();
    let ws = add(home.path(), "x", project.path()).unwrap();

    assert!(matches!(
        rename(home.path(), &ws.id, ""),
        Err(WorkspacesError::EmptyName),
    ));
    assert!(matches!(
        rename(home.path(), &ws.id, "   "),
        Err(WorkspacesError::EmptyName),
    ));
}
