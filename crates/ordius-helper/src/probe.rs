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

/// Run the probe subcommand: deserialize one `ProbePlanV1`, probe each
/// resource sequentially, emit one JSONL `ProbeOutcomeV1` per line.
pub fn run<R: BufRead, W: Write>(input: R, mut output: W) -> anyhow::Result<()> {
    let mut buf = String::new();
    let mut input = input;
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
                HttpProbeMethodV1::Post => agent.post(&url),
            };

            match req.call() {
                Ok(resp) => {
                    let status = resp.status();
                    let accepted = if route.expect_status.is_empty() {
                        (200..=299).contains(&status)
                    } else {
                        route.expect_status.contains(&status)
                    };

                    if accepted {
                        match resp.into_string() {
                            Ok(body) => {
                                let fingerprint = fingerprint(&body, &route.fingerprint_jsonpaths);
                                proven.push(ProvenRouteV1 {
                                    capability: route.proves.clone(),
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
                    } else {
                        last_err = Some(format!("status {status} not in expected range"));
                    }
                },
                Err(err) if is_ureq_timeout(&err) => {
                    any_timeout = true;
                },
                Err(ureq::Error::Status(code, _)) => {
                    last_err = Some(format!("status {code}"));
                },
                Err(ureq::Error::Transport(t)) => {
                    base_transport_failed = true;
                    last_err = Some(format!("transport error: {t}"));
                },
            }
        }

        if !proven.is_empty() {
            return ProbeOutcomeBodyV1::Found(ProbeDetailV1::HttpEndpoint {
                base_url: base.clone(),
                proven_routes: proven,
            });
        }

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
            re.captures(&combined).map_or_else(
                || ProbeOutcomeBodyV1::ProbeFailed {
                    reason: "version regex did not match output".into(),
                },
                |caps| {
                    let version = caps
                        .get(1)
                        .or_else(|| caps.get(0))
                        .map_or_else(String::new, |m| m.as_str().to_string());
                    ProbeOutcomeBodyV1::Found(ProbeDetailV1::Toolchain { path, version })
                },
            )
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

    for extra in extra_search_paths {
        let candidate = std::path::Path::new(extra).join(bin);
        if candidate.is_file() {
            return Some(candidate.to_string_lossy().into_owned());
        }
    }

    None
}

enum RunResult {
    Output(std::process::Output),
    Timeout,
    SpawnError(std::io::Error),
}

fn run_command(mut cmd: std::process::Command, timeout: Option<Duration>) -> RunResult {
    match timeout {
        Some(timeout) => run_with_timeout(cmd, timeout),
        None => match cmd.output() {
            Ok(output) => RunResult::Output(output),
            Err(e) => RunResult::SpawnError(e),
        },
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

    let mut stdout_buf = Vec::new();
    let mut stderr_buf = Vec::new();
    if let Some(mut s) = child.stdout.take() {
        let _read_result = s.read_to_end(&mut stdout_buf);
    }
    if let Some(mut s) = child.stderr.take() {
        let _read_result = s.read_to_end(&mut stderr_buf);
    }

    RunResult::Output(std::process::Output {
        status,
        stdout: stdout_buf,
        stderr: stderr_buf,
    })
}

fn fingerprint(body: &str, jsonpaths: &[String]) -> String {
    if jsonpaths.is_empty() {
        return String::new();
    }
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
                        proves: "alive".into(),
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
}
