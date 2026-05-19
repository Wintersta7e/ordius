use super::*;
use tempfile::TempDir;

const VALID_YAML: &str = r"
id: my-node
name: My Node
category: data
inputs: []
outputs: []
config: []
execution:
  backend: subprocess
  command: [sh, -c, 'echo hi']
  output_parse: text
";

#[test]
fn empty_dir_not_an_error() {
    let dir = TempDir::new().unwrap();
    let mut reg = Registry::new();
    let errs = load_into(&mut reg, dir.path());
    assert!(errs.is_empty());
    assert!(reg.ids().is_empty());
}

#[test]
fn missing_dir_not_an_error() {
    let dir = TempDir::new().unwrap();
    let missing = dir.path().join("does-not-exist");
    let mut reg = Registry::new();
    let errs = load_into(&mut reg, missing);
    assert!(errs.is_empty());
}

#[test]
fn loads_valid_yaml_manifest() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("x.yaml"), VALID_YAML).unwrap();
    let mut reg = Registry::new();
    let errs = load_into(&mut reg, dir.path());
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
    assert!(reg.get("my-node").is_some());
}

#[test]
fn loads_valid_json_manifest() {
    let dir = TempDir::new().unwrap();
    let json = r#"{
      "id": "json-node",
      "name": "JSON Node",
      "category": "data",
      "inputs": [],
      "outputs": [],
      "config": [],
      "execution": {
        "backend": "subprocess",
        "command": ["echo", "hi"],
        "output_parse": "text"
      }
    }"#;
    std::fs::write(dir.path().join("x.json"), json).unwrap();
    let mut reg = Registry::new();
    let errs = load_into(&mut reg, dir.path());
    assert!(errs.is_empty(), "unexpected errors: {errs:?}");
    assert!(reg.get("json-node").is_some());
}

#[test]
fn skips_non_manifest_files() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("x.yaml"), VALID_YAML).unwrap();
    std::fs::write(dir.path().join("README.txt"), "ignored").unwrap();
    std::fs::write(dir.path().join("notes.md"), "ignored").unwrap();
    let mut reg = Registry::new();
    let errs = load_into(&mut reg, dir.path());
    assert!(
        errs.is_empty(),
        "non-manifest files must be skipped silently"
    );
    assert_eq!(reg.ids().len(), 1);
}

#[test]
fn rejects_in_process_backend_for_manifests() {
    let dir = TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("x.yaml"),
        r"
id: bad
name: Bad
category: data
execution:
  backend: in_process
  command: []
",
    )
    .unwrap();
    let mut reg = Registry::new();
    let errs = load_into(&mut reg, dir.path());
    assert_eq!(errs.len(), 1);
    assert!(matches!(errs[0], ManifestError::Validation { .. }));
    assert!(reg.get("bad").is_none());
}

#[test]
fn rejects_container_backend_in_v1_0() {
    let dir = TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("x.yaml"),
        r"
id: bad
name: Bad
category: data
execution:
  backend: container
  command: [whatever]
",
    )
    .unwrap();
    let mut reg = Registry::new();
    let errs = load_into(&mut reg, dir.path());
    assert_eq!(errs.len(), 1);
    assert!(reg.get("bad").is_none());
}

#[test]
fn rejects_empty_command() {
    let dir = TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("x.yaml"),
        r"
id: bad
name: Bad
category: data
execution:
  backend: subprocess
  command: []
",
    )
    .unwrap();
    let mut reg = Registry::new();
    let errs = load_into(&mut reg, dir.path());
    assert_eq!(errs.len(), 1);
}

#[test]
fn rejects_non_jsonpath_output_map_entry() {
    let dir = TempDir::new().unwrap();
    std::fs::write(
        dir.path().join("x.yaml"),
        r"
id: bad
name: Bad
category: data
execution:
  backend: subprocess
  command: [echo, hi]
  output_parse: json
  output_map:
    text: 'not-jsonpath'
",
    )
    .unwrap();
    let mut reg = Registry::new();
    let errs = load_into(&mut reg, dir.path());
    assert_eq!(errs.len(), 1);
    if let ManifestError::Validation { err, .. } = &errs[0] {
        assert!(err.contains("JSONPath"), "got: {err}");
    } else {
        panic!("expected Validation error, got {:?}", errs[0]);
    }
}

#[test]
fn rejects_duplicate_id() {
    let dir = TempDir::new().unwrap();
    // Files sort by path; a.yaml lands first, b.yaml second.
    std::fs::write(
        dir.path().join("a.yaml"),
        r"
id: dup
name: A
category: data
execution:
  backend: subprocess
  command: [echo, a]
  output_parse: text
",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("b.yaml"),
        r"
id: dup
name: B
category: data
execution:
  backend: subprocess
  command: [echo, b]
  output_parse: text
",
    )
    .unwrap();
    let mut reg = Registry::new();
    let errs = load_into(&mut reg, dir.path());
    assert_eq!(errs.len(), 1);
    let nt = reg.get("dup").unwrap();
    assert_eq!(nt.name, "A", "first-wins on duplicate id");
}

#[test]
fn parse_error_is_isolated_per_file() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("good.yaml"), VALID_YAML).unwrap();
    // Intentionally malformed YAML.
    std::fs::write(
        dir.path().join("bad.yaml"),
        "id: oops\nname:\n  - this is wrong\n  not a list",
    )
    .unwrap();
    let mut reg = Registry::new();
    let errs = load_into(&mut reg, dir.path());
    // Bad parses, good still lands.
    assert_eq!(errs.len(), 1);
    assert!(matches!(errs[0], ManifestError::Parse { .. }));
    assert!(reg.get("my-node").is_some());
}
