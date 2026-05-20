//! Starter workflows installed on first launch.
//!
//! When `<home>/workflows/` is empty (no `.json` files), drop a small
//! set of runnable templates so a brand-new user has something to open
//! and run instead of facing a blank canvas. Failures here are logged
//! but never bubble up — a fresh install must succeed even if the home
//! directory is read-only or oddly permissioned.
//!
//! The seeds are embedded via `include_str!`, so they ship inside the
//! binary and survive an unzipped distribution with no extra resources.

use std::fs;
use std::path::Path;

const SEEDS: &[(&str, &str)] = &[
    (
        "starter-hello.json",
        include_str!("../seeds/starter-hello.json"),
    ),
    (
        "starter-pipeline.json",
        include_str!("../seeds/starter-pipeline.json"),
    ),
    (
        "starter-schedule.json",
        include_str!("../seeds/starter-schedule.json"),
    ),
];

/// Install starter workflows into `<home>/workflows/` when the
/// directory has no `.json` files. Returns the number of seeds
/// written. Never errors — every failure is logged and skipped.
pub fn install_if_empty(home: &Path) -> usize {
    let dir = home.join("workflows");
    if let Err(e) = fs::create_dir_all(&dir) {
        tracing::warn!(error = ?e, path = %dir.display(), "seeds: create workflows dir");
        return 0;
    }
    if has_any_workflow_json(&dir) {
        return 0;
    }
    let mut written = 0usize;
    for (filename, body) in SEEDS {
        let path = dir.join(filename);
        match fs::write(&path, body) {
            Ok(()) => {
                written += 1;
                tracing::info!(seed = filename, "seeds: installed");
            },
            Err(e) => {
                tracing::warn!(error = ?e, seed = filename, "seeds: write failed");
            },
        }
    }
    written
}

fn has_any_workflow_json(dir: &Path) -> bool {
    let Ok(entries) = fs::read_dir(dir) else {
        return false;
    };
    for entry in entries.flatten() {
        if entry.path().extension().is_some_and(|e| e == "json") {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn installs_into_empty_home() {
        let tmp = TempDir::new().unwrap();
        let written = install_if_empty(tmp.path());
        assert_eq!(written, SEEDS.len());
        for (filename, _) in SEEDS {
            assert!(tmp.path().join("workflows").join(filename).exists());
        }
    }

    #[test]
    fn skips_when_workflows_present() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("workflows");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("existing.json"), "{}").unwrap();
        let written = install_if_empty(tmp.path());
        assert_eq!(written, 0);
        for (filename, _) in SEEDS {
            assert!(!dir.join(filename).exists());
        }
    }

    #[test]
    fn embedded_seeds_parse_as_workflows() {
        for (filename, body) in SEEDS {
            let parsed: crate::types::Workflow =
                serde_json::from_str(body).unwrap_or_else(|e| panic!("{filename}: {e}"));
            assert!(!parsed.id.is_empty(), "{filename}: id should not be empty");
            assert!(
                !parsed.nodes.is_empty(),
                "{filename}: nodes should not be empty"
            );
        }
    }

    #[test]
    fn embedded_seeds_pass_structural_validation() {
        for (filename, body) in SEEDS {
            let parsed: crate::types::Workflow = serde_json::from_str(body).unwrap();
            crate::validation::validate(&parsed).unwrap_or_else(|e| panic!("{filename}: {e}"));
        }
    }
}
