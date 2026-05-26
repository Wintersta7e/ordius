//! Probe subcommand: reads a `ProbePlanV1` from stdin, emits one JSONL
//! `ProbeOutcomeV1` per resource on stdout.

use crate::protocol::{
    HttpProbeMethodV1, HttpProbeRouteV1, ProbeDetailV1, ProbeOutcomeBodyV1, ProbeOutcomeV1,
    ProbePlanV1, ProvenRouteV1, ResourceKindV1, ResourceSpecV1,
};
use anyhow::Context;
use jsonpath_rust::JsonPath;
use std::io::{BufRead, Read, Write};
use std::time::{Duration, Instant};

/// Hard cap on HTTP probe body size. Fingerprint extraction parses the body as
/// JSON; without a cap a malicious endpoint could OOM the helper.
const MAX_PROBE_BODY_BYTES: u64 = 1 << 20; // 1 MiB
/// Hard cap on `fingerprint_jsonpaths` count. Each path runs a full `JSONPath`
/// query against the body; unbounded paths × large bodies is quadratic.
const MAX_FINGERPRINT_JSONPATHS: usize = 32;
/// Hard cap on toolchain version-probe stdout/stderr capture. Version output
/// is small (KBs); the cap protects against pathological tools writing
/// megabytes to stdio before exiting.
const MAX_VERSION_OUTPUT_BYTES: u64 = 64 * 1024;

/// Run the probe subcommand: deserialize one `ProbePlanV1`, probe each
/// resource sequentially, emit one JSONL `ProbeOutcomeV1` per line.
pub fn run<R: BufRead, W: Write>(mut input: R, mut output: W) -> anyhow::Result<()> {
    let mut buf = String::new();
    input
        .read_to_string(&mut buf)
        .context("read probe plan from stdin")?;
    let plan: ProbePlanV1 = serde_json::from_str(&buf).context("parse probe plan from stdin")?;
    anyhow::ensure!(
        plan.version == 1,
        "unsupported probe plan version: {}",
        plan.version
    );

    let started = Instant::now();
    let overall_budget = duration_from_ms(plan.overall_budget_ms);
    let per_resource = duration_from_ms(plan.per_resource_timeout_ms);

    for spec in &plan.resources {
        if overall_elapsed(started, overall_budget) {
            emit(
                &mut output,
                &ProbeOutcomeV1 {
                    version: 1,
                    id: spec.id.clone(),
                    outcome: ProbeOutcomeBodyV1::Skipped {
                        reason: "overall budget elapsed".into(),
                    },
                    elapsed_ms: 0,
                },
            )?;
            continue;
        }

        let resource_started = Instant::now();
        let timeout = effective_timeout(per_resource, started, overall_budget);
        let outcome = probe_one(spec, timeout);
        let elapsed_ms = duration_ms_u64(resource_started.elapsed());
        emit(
            &mut output,
            &ProbeOutcomeV1 {
                version: 1,
                id: spec.id.clone(),
                outcome,
                elapsed_ms,
            },
        )?;
    }

    Ok(())
}

fn emit<W: Write>(out: &mut W, line: &ProbeOutcomeV1) -> anyhow::Result<()> {
    let s = serde_json::to_string(&line).context("serialize probe outcome")?;
    out.write_all(s.as_bytes())?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(())
}

fn probe_one(spec: &ResourceSpecV1, timeout: Option<Duration>) -> ProbeOutcomeBodyV1 {
    match &spec.kind {
        ResourceKindV1::Http { bases, routes } => probe_http(bases, routes, timeout),
        ResourceKindV1::Binary {
            bin,
            extra_search_paths,
        } => probe_binary(bin, extra_search_paths),
        ResourceKindV1::Toolchain {
            bin,
            version_args,
            version_regex,
            extra_search_paths,
        } => probe_toolchain(
            bin,
            version_args,
            version_regex,
            extra_search_paths,
            timeout,
        ),
    }
}

fn probe_http(
    bases: &[String],
    routes: &[HttpProbeRouteV1],
    timeout: Option<Duration>,
) -> ProbeOutcomeBodyV1 {
    if bases.is_empty() || routes.is_empty() {
        return ProbeOutcomeBodyV1::ProbeFailed {
            reason: "no base url or route declared".into(),
        };
    }

    let mut builder = ureq::AgentBuilder::new();
    if let Some(timeout) = timeout {
        builder = builder.timeout(timeout);
    }
    let agent = builder.build();
    let mut any_timeout = false;
    let mut last_err: Option<String> = None;

    for base in bases {
        let mut proven: Vec<ProvenRouteV1> = Vec::new();
        let mut base_transport_failed = false;

        for route in routes {
            let url = format!("{}{}", base.trim_end_matches('/'), route.path);
            let req = match route.method {
                HttpProbeMethodV1::Get => agent.get(&url),
                HttpProbeMethodV1::Head => agent.head(&url),
                HttpProbeMethodV1::Post => agent.post(&url),
            };

            // ureq surfaces non-2xx as `Err(Status(code, resp))` rather than `Ok`.
            // Normalize both arms into `(status, resp_opt)` so `expect_status`
            // is consulted uniformly — a route declaring `expect_status: [401]`
            // must accept the 401 path, not classify it as `ProbeFailed`.
            let (status, resp_opt) = match req.call() {
                Ok(resp) => (resp.status(), Some(resp)),
                Err(err) if is_ureq_timeout(&err) => {
                    any_timeout = true;
                    continue;
                },
                Err(ureq::Error::Status(code, resp)) => (code, Some(resp)),
                Err(ureq::Error::Transport(t)) => {
                    base_transport_failed = true;
                    last_err = Some(format!("transport error: {t}"));
                    continue;
                },
            };

            if classify_response_status(status, &route.expect_status) {
                if let Some(resp) = resp_opt {
                    let mut body = String::new();
                    match resp
                        .into_reader()
                        .take(MAX_PROBE_BODY_BYTES)
                        .read_to_string(&mut body)
                    {
                        Ok(_) => {
                            let fingerprint = fingerprint(&body, &route.fingerprint_jsonpaths);
                            proven.push(ProvenRouteV1 {
                                capabilities: route.proves.clone(),
                                path: route.path.clone(),
                                status,
                                fingerprint,
                            });
                        },
                        Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                            any_timeout = true;
                        },
                        Err(e) => {
                            last_err = Some(format!("read response body: {e}"));
                        },
                    }
                }
            } else {
                last_err = Some(format!("status {status} not in expected range"));
            }
        }

        if !proven.is_empty() {
            return ProbeOutcomeBodyV1::Found(ProbeDetailV1::HttpEndpoint {
                base_url: base.clone(),
                proven_routes: proven,
            });
        }

        // Per-base fallback semantics:
        // * Transport errors (connection refused, DNS fail, etc.) → try the
        //   next base. The address is unreachable, so a different base may
        //   serve the same resource.
        // * Status-mismatch / read-body errors → return ProbeFailed without
        //   falling back. Reaching the server but getting an unexpected
        //   response means *this* is the wrong server; another base would
        //   just be a different wrong server.
        if base_transport_failed {
            continue;
        }

        if let Some(err) = last_err.take() {
            return ProbeOutcomeBodyV1::ProbeFailed { reason: err };
        }
    }

    if any_timeout {
        ProbeOutcomeBodyV1::TimedOut
    } else if let Some(reason) = last_err {
        ProbeOutcomeBodyV1::ProbeFailed { reason }
    } else {
        ProbeOutcomeBodyV1::NotFound
    }
}

/// Decide whether a response status counts as a successful probe.
/// Empty `expect_status` defaults to the 2xx range; otherwise the status
/// must appear in the declared list.
fn classify_response_status(status: u16, expect_status: &[u16]) -> bool {
    if expect_status.is_empty() {
        (200..=299).contains(&status)
    } else {
        expect_status.contains(&status)
    }
}

fn probe_binary(bin: &str, extra_search_paths: &[String]) -> ProbeOutcomeBodyV1 {
    find_in_path(bin, extra_search_paths).map_or(ProbeOutcomeBodyV1::NotFound, |path| {
        ProbeOutcomeBodyV1::Found(ProbeDetailV1::Binary { path })
    })
}

fn probe_toolchain(
    bin: &str,
    version_args: &[String],
    version_regex: &str,
    extra_search_paths: &[String],
    timeout: Option<Duration>,
) -> ProbeOutcomeBodyV1 {
    let Some(path) = find_in_path(bin, extra_search_paths) else {
        return ProbeOutcomeBodyV1::NotFound;
    };
    let re = match regex_like::Regex::new(version_regex) {
        Ok(re) => re,
        Err(e) => {
            return ProbeOutcomeBodyV1::ProbeFailed {
                reason: format!("invalid version_regex: {e}"),
            };
        },
    };

    let mut cmd = std::process::Command::new(&path);
    cmd.args(version_args);
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    match run_command(cmd, timeout) {
        RunResult::Output(output) => {
            let combined = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            // A no-match doesn't mean "binary missing" — the binary is there
            // and ran; we just couldn't extract a version string. Mirror the
            // local dispatcher's behavior and surface as `Found` with an
            // empty version so the catalog stays consistent across envs.
            let version = re.captures(&combined).map_or_else(String::new, |caps| {
                caps.get(1)
                    .or_else(|| caps.get(0))
                    .map_or_else(String::new, |m| m.as_str().to_string())
            });
            ProbeOutcomeBodyV1::Found(ProbeDetailV1::Toolchain { path, version })
        },
        RunResult::Timeout => ProbeOutcomeBodyV1::TimedOut,
        RunResult::SpawnError(e) => ProbeOutcomeBodyV1::ProbeFailed {
            reason: format!("spawn error: {e}"),
        },
    }
}

fn find_in_path(bin: &str, extra_search_paths: &[String]) -> Option<String> {
    if let Ok(p) = which::which(bin) {
        return Some(p.to_string_lossy().into_owned());
    }

    for dir in expand_search_paths(extra_search_paths) {
        let candidate = dir.join(bin);
        if candidate.is_file() {
            return Some(candidate.to_string_lossy().into_owned());
        }
    }

    None
}

/// Expand a slice of search-path patterns into concrete directories.
///
/// Mirrors `crates/engine/src/environment/runtime/search_paths.rs::expand`
/// but lives here so the helper crate can stay standalone — the helper
/// must compile + run without a transitive engine dependency.
fn expand_search_paths(patterns: &[String]) -> Vec<std::path::PathBuf> {
    let home = home_dir();
    let mut out = Vec::new();
    for raw in patterns {
        let expanded = expand_tilde(raw, &home);
        if expanded.contains('*') || expanded.contains('?') || expanded.contains('[') {
            if let Ok(iter) = glob::glob(&expanded) {
                let mut matches: Vec<std::path::PathBuf> = iter.filter_map(Result::ok).collect();
                matches.sort();
                out.extend(matches);
            }
        } else {
            out.push(std::path::PathBuf::from(expanded));
        }
    }
    out
}

fn expand_tilde(raw: &str, home: &std::path::Path) -> String {
    if raw == "~" {
        return home.display().to_string();
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        return home.join(rest).display().to_string();
    }
    raw.to_string()
}

fn home_dir() -> std::path::PathBuf {
    if let Ok(h) = std::env::var("HOME") {
        return std::path::PathBuf::from(h);
    }
    if let Ok(h) = std::env::var("USERPROFILE") {
        return std::path::PathBuf::from(h);
    }
    std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."))
}

enum RunResult {
    Output(std::process::Output),
    Timeout,
    SpawnError(std::io::Error),
}

fn run_command(cmd: std::process::Command, timeout: Option<Duration>) -> RunResult {
    match timeout {
        Some(timeout) => run_with_timeout(cmd, timeout),
        // No-timeout path also caps stdio reads so the output cap is an
        // invariant of `run_command`, not a coincidence of which branch ran.
        None => run_unbounded(cmd),
    }
}

fn run_unbounded(mut cmd: std::process::Command) -> RunResult {
    cmd.stdin(std::process::Stdio::null());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return RunResult::SpawnError(e),
    };
    let (stdout, stderr) = capture_capped_stdio(&mut child);
    match child.wait() {
        Ok(status) => RunResult::Output(std::process::Output {
            status,
            stdout,
            stderr,
        }),
        Err(e) => RunResult::SpawnError(e),
    }
}

fn run_with_timeout(mut cmd: std::process::Command, timeout: Duration) -> RunResult {
    use wait_timeout::ChildExt as _;

    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return RunResult::SpawnError(e),
    };
    let status = match child.wait_timeout(timeout) {
        Ok(Some(s)) => s,
        Ok(None) => {
            let _kill_result = child.kill();
            let _wait_result = child.wait();
            return RunResult::Timeout;
        },
        Err(e) => return RunResult::SpawnError(e),
    };

    let (stdout, stderr) = capture_capped_stdio(&mut child);
    RunResult::Output(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

/// Drain `child`'s stdout/stderr pipes up to `MAX_VERSION_OUTPUT_BYTES` each.
/// Read errors are intentionally swallowed: the caller has already decided
/// the process produced output worth capturing, and a partial read is more
/// useful for diagnostics than an aborted probe.
fn capture_capped_stdio(child: &mut std::process::Child) -> (Vec<u8>, Vec<u8>) {
    let mut stdout_buf = Vec::new();
    let mut stderr_buf = Vec::new();
    if let Some(s) = child.stdout.take() {
        let _read_result = s
            .take(MAX_VERSION_OUTPUT_BYTES)
            .read_to_end(&mut stdout_buf);
    }
    if let Some(s) = child.stderr.take() {
        let _read_result = s
            .take(MAX_VERSION_OUTPUT_BYTES)
            .read_to_end(&mut stderr_buf);
    }
    (stdout_buf, stderr_buf)
}

fn fingerprint(body: &str, jsonpaths: &[String]) -> String {
    if jsonpaths.is_empty() {
        return String::new();
    }
    // Truncate adversarial / misconfigured probe plans so a runaway path list
    // can't push the helper into quadratic time over a large JSON body.
    let jsonpaths = if jsonpaths.len() > MAX_FINGERPRINT_JSONPATHS {
        &jsonpaths[..MAX_FINGERPRINT_JSONPATHS]
    } else {
        jsonpaths
    };
    let v: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return String::new(),
    };

    let mut parts: Vec<String> = Vec::with_capacity(jsonpaths.len());
    for jp in jsonpaths {
        let Ok(matched) = v.query(jp) else {
            return String::new();
        };
        if matched.is_empty() {
            return String::new();
        }
        let s = matched
            .iter()
            .map(|value| value_to_string(value))
            .collect::<Vec<_>>()
            .join(",");
        parts.push(s);
    }

    parts.join("\u{1f}")
}

fn value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn duration_from_ms(ms: u64) -> Option<Duration> {
    (ms != 0).then(|| Duration::from_millis(ms))
}

fn duration_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn effective_timeout(
    per_resource: Option<Duration>,
    started: Instant,
    overall_budget: Option<Duration>,
) -> Option<Duration> {
    let remaining = overall_budget.map(|budget| budget.saturating_sub(started.elapsed()));
    match (per_resource, remaining) {
        (Some(per_resource), Some(remaining)) => Some(per_resource.min(remaining)),
        (Some(per_resource), None) => Some(per_resource),
        (None, Some(remaining)) => Some(remaining),
        (None, None) => None,
    }
}

fn overall_elapsed(started: Instant, overall_budget: Option<Duration>) -> bool {
    overall_budget.is_some_and(|budget| started.elapsed() >= budget)
}

fn is_ureq_timeout(err: &ureq::Error) -> bool {
    std::error::Error::source(err)
        .and_then(|source| source.downcast_ref::<std::io::Error>())
        .is_some_and(|source| source.kind() == std::io::ErrorKind::TimedOut)
}

/// Tiny regex shim — engine pulls in `regex 1.x` via workspace deps; the
/// helper crate imports the same workspace dep.
mod regex_like {
    pub(super) use regex::Regex;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_plan_emits_no_lines() {
        let plan = ProbePlanV1 {
            version: 1,
            per_resource_timeout_ms: 1000,
            max_concurrency: 1,
            overall_budget_ms: 5000,
            resources: vec![],
        };
        let input = serde_json::to_string(&plan).unwrap();
        let mut out: Vec<u8> = Vec::new();
        run(input.as_bytes(), &mut out).unwrap();
        assert!(out.is_empty(), "no resources → no JSONL output");
    }

    #[test]
    fn missing_binary_emits_not_found() {
        let plan = ProbePlanV1 {
            version: 1,
            per_resource_timeout_ms: 500,
            max_concurrency: 1,
            overall_budget_ms: 5000,
            resources: vec![ResourceSpecV1 {
                id: "definitely-not-on-path".into(),
                kind: ResourceKindV1::Binary {
                    bin: "definitely_not_a_real_bin_xyz_123".into(),
                    extra_search_paths: vec![],
                },
            }],
        };
        let input = serde_json::to_string(&plan).unwrap();
        let mut out: Vec<u8> = Vec::new();
        run(input.as_bytes(), &mut out).unwrap();
        let line = std::str::from_utf8(&out).unwrap();
        let outcome: ProbeOutcomeV1 = serde_json::from_str(line.trim()).unwrap();
        assert!(matches!(outcome.outcome, ProbeOutcomeBodyV1::NotFound));
    }

    #[test]
    fn http_unreachable_emits_probe_failed() {
        // 127.0.0.1:1 is reserved + closed on every modern OS.
        let plan = ProbePlanV1 {
            version: 1,
            per_resource_timeout_ms: 300,
            max_concurrency: 1,
            overall_budget_ms: 5000,
            resources: vec![ResourceSpecV1 {
                id: "dead-http".into(),
                kind: ResourceKindV1::Http {
                    bases: vec!["http://127.0.0.1:1".into()],
                    routes: vec![HttpProbeRouteV1 {
                        path: "/probe".into(),
                        method: HttpProbeMethodV1::Get,
                        proves: vec!["alive".into()],
                        expect_status: vec![],
                        fingerprint_jsonpaths: vec![],
                    }],
                },
            }],
        };
        let input = serde_json::to_string(&plan).unwrap();
        let mut out: Vec<u8> = Vec::new();
        run(input.as_bytes(), &mut out).unwrap();
        let line = std::str::from_utf8(&out).unwrap();
        let outcome: ProbeOutcomeV1 = serde_json::from_str(line.trim()).unwrap();
        // ureq surfaces connection refused as ProbeFailed (not TimedOut)
        // because the underlying error path is `Status` / `Transport` rather
        // than a deadline elapsing.
        assert!(matches!(
            outcome.outcome,
            ProbeOutcomeBodyV1::ProbeFailed { .. }
        ));
    }

    #[test]
    fn accept_logic_default_treats_2xx_as_ok() {
        assert!(classify_response_status(200, &[]));
        assert!(classify_response_status(204, &[]));
        assert!(classify_response_status(299, &[]));
    }

    #[test]
    fn accept_logic_default_rejects_4xx() {
        assert!(!classify_response_status(401, &[]));
        assert!(!classify_response_status(404, &[]));
        assert!(!classify_response_status(500, &[]));
    }

    #[test]
    fn accept_logic_custom_accepts_listed_status() {
        assert!(classify_response_status(401, &[401]));
        assert!(classify_response_status(404, &[401, 404, 418]));
    }

    #[test]
    fn accept_logic_custom_rejects_other_status() {
        assert!(!classify_response_status(200, &[401]));
        assert!(!classify_response_status(403, &[401, 404]));
    }

    #[test]
    fn fingerprint_empty_jsonpaths_returns_empty() {
        assert_eq!(fingerprint(r#"{"a":1}"#, &[]), "");
    }

    #[test]
    fn fingerprint_invalid_json_returns_empty() {
        assert_eq!(fingerprint("not json at all", &["$.version".into()]), "");
    }

    #[test]
    fn fingerprint_missing_path_returns_empty() {
        assert_eq!(fingerprint(r#"{"a":1}"#, &["$.missing".into()]), "");
    }

    #[test]
    fn fingerprint_multi_path_joins_with_unit_separator() {
        let body = r#"{"version":"1.0","build":"abc"}"#;
        let fp = fingerprint(body, &["$.version".into(), "$.build".into()]);
        assert!(fp.contains('\u{1f}'), "expected unit separator in {fp:?}");
        assert!(fp.contains("1.0"));
        assert!(fp.contains("abc"));
    }

    #[test]
    fn fingerprint_clamps_excess_jsonpaths() {
        // A pathological caller hands in 1000 paths; the helper should only
        // process the first MAX_FINGERPRINT_JSONPATHS to avoid quadratic CPU
        // against a large response body.
        let mut paths: Vec<String> = (0..1000).map(|_| "$.version".to_string()).collect();
        paths.push("$.never_evaluated_because_we_clamped".into());
        let fp = fingerprint(r#"{"version":"1.0"}"#, &paths);
        // Survives clamp without panic and still produces a fingerprint.
        assert!(!fp.is_empty());
    }

    #[test]
    fn probe_http_all_bases_unreachable_emits_probe_failed() {
        // Two closed ports on loopback — confirms the all-bases-transport-failed
        // path completes deterministically. (Connection-refused surfaces as
        // ProbeFailed via ureq's Transport error, not NotFound.)
        let plan = ProbePlanV1 {
            version: 1,
            per_resource_timeout_ms: 300,
            max_concurrency: 1,
            overall_budget_ms: 5000,
            resources: vec![ResourceSpecV1 {
                id: "double-dead".into(),
                kind: ResourceKindV1::Http {
                    bases: vec!["http://127.0.0.1:1".into(), "http://127.0.0.1:2".into()],
                    routes: vec![HttpProbeRouteV1 {
                        path: "/healthz".into(),
                        method: HttpProbeMethodV1::Get,
                        proves: vec!["alive".into()],
                        expect_status: vec![],
                        fingerprint_jsonpaths: vec![],
                    }],
                },
            }],
        };
        let input = serde_json::to_string(&plan).unwrap();
        let mut out: Vec<u8> = Vec::new();
        run(input.as_bytes(), &mut out).unwrap();
        let line = std::str::from_utf8(&out).unwrap();
        let outcome: ProbeOutcomeV1 = serde_json::from_str(line.trim()).unwrap();
        assert!(matches!(
            outcome.outcome,
            ProbeOutcomeBodyV1::ProbeFailed { .. }
        ));
    }

    #[test]
    fn toolchain_regex_no_match_returns_found_with_empty_version() {
        // Use `true` on Unix — it produces no output but exits zero. Our
        // toolchain probe should still report it Found (matching the local
        // dispatcher's behavior) rather than ProbeFailed.
        #[cfg(unix)]
        {
            let outcome = probe_toolchain(
                "true",
                &[],
                r"version (\S+)",
                &[],
                Some(Duration::from_secs(2)),
            );
            match outcome {
                ProbeOutcomeBodyV1::Found(ProbeDetailV1::Toolchain { version, .. }) => {
                    assert!(
                        version.is_empty(),
                        "expected empty version, got {version:?}"
                    );
                },
                other => panic!("expected Found(Toolchain), got {other:?}"),
            }
        }
    }

    #[test]
    #[cfg(unix)]
    #[allow(unsafe_code)]
    fn find_in_path_expands_tilde_and_glob() {
        use std::os::unix::fs::PermissionsExt;

        let home = tempfile::TempDir::new().unwrap();
        let dir = home.path().join(".nvm/versions/node/v20.0.0/bin");
        std::fs::create_dir_all(&dir).unwrap();
        let bin_name = "ordius-helper-fake-bin";
        let p = dir.join(bin_name);
        std::fs::write(&p, b"#!/bin/sh\necho hi\n").unwrap();
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();

        let prev_home = std::env::var_os("HOME");
        // SAFETY: `std::env::set_var` is unsafe in edition 2024; tests in this
        // module are single-threaded for env mutation. `HOME` is restored
        // before this test returns regardless of assertion outcome.
        unsafe {
            std::env::set_var("HOME", home.path());
        }
        let resolved = find_in_path(bin_name, &["~/.nvm/versions/node/*/bin".to_string()]);
        unsafe {
            if let Some(h) = prev_home {
                std::env::set_var("HOME", h);
            } else {
                std::env::remove_var("HOME");
            }
        }

        assert!(resolved.is_some());
        assert!(
            resolved
                .unwrap()
                .ends_with("v20.0.0/bin/ordius-helper-fake-bin")
        );
    }
}
