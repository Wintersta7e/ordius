//! `HostDirectVerification` helpers — stable fingerprint derivation,
//! recompute, invalidation.

use crate::environment::runtime::env::{HostDirectMethod, HostDirectVerification};
use chrono::Utc;
use jsonpath_rust::JsonPath;
use serde_json::Value;

/// Compute the stable fingerprint of a probe response body for the given
/// list of `JSONPath` expressions.
///
/// Returns `None` when the path list is empty, the body fails to parse as
/// JSON, any expression is invalid, or any expression has no match.  When
/// a path matches multiple values, the per-path joined string contains all
/// matches; per-path joins are then concatenated with US (`U+001F`) as a
/// separator so collisions across paths are impossible.
pub fn compute_fingerprint(body: &[u8], jsonpaths: &[String]) -> Option<String> {
    if jsonpaths.is_empty() {
        return None;
    }
    let v: Value = serde_json::from_slice(body).ok()?;
    let mut parts: Vec<String> = Vec::with_capacity(jsonpaths.len());
    for jp in jsonpaths {
        let matched: Vec<&Value> = match v.query(jp) {
            Ok(m) => m,
            Err(_) => return None,
        };
        if matched.is_empty() {
            return None;
        }
        let joined = matched
            .iter()
            .map(|m| value_to_string(m))
            .collect::<Vec<_>>()
            .join(",");
        parts.push(joined);
    }
    Some(parts.join("\u{1f}"))
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Build a fresh verification record given a successful host-direct probe.
///
/// `stable_fingerprint` is computed from `body` and `jsonpaths`; when the
/// fingerprint cannot be derived (empty list, bad JSON, missing match) the
/// record is still produced with an empty fingerprint so the caller can
/// still surface the verification while flagging the `JSONPath` list as
/// misconfigured upstream.
pub fn build_record(
    method: HostDirectMethod,
    host_url: &str,
    route_path: &str,
    body: &[u8],
    jsonpaths: Vec<String>,
) -> HostDirectVerification {
    HostDirectVerification {
        verified_at: Utc::now(),
        method,
        host_url: host_url.into(),
        probe_route_path: route_path.into(),
        stable_fingerprint: compute_fingerprint(body, &jsonpaths).unwrap_or_default(),
        recompute_jsonpaths: jsonpaths,
    }
}

/// Check whether `record` is still valid given a fresh probe response body.
///
/// Returns `true` only when the freshly computed fingerprint matches the
/// stored one — any parse failure, missing match, or value change yields
/// `false`, forcing the user to re-confirm host-direct routing.
pub fn record_still_valid(record: &HostDirectVerification, fresh_body: &[u8]) -> bool {
    let fresh = compute_fingerprint(fresh_body, &record.recompute_jsonpaths);
    fresh.as_deref() == Some(record.stable_fingerprint.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_stable_across_redundant_fields() {
        let body1 = br#"{"version":"0.5.7","build":"abc123","timestamp":1}"#;
        let body2 = br#"{"version":"0.5.7","build":"def456","timestamp":2}"#;
        let jp = vec!["$.version".into()];
        let fp1 = compute_fingerprint(body1, &jp).unwrap();
        let fp2 = compute_fingerprint(body2, &jp).unwrap();
        assert_eq!(
            fp1, fp2,
            "build/timestamp drift must not change fingerprint"
        );
    }

    #[test]
    fn fingerprint_changes_on_targeted_field_change() {
        let body1 = br#"{"version":"0.5.7"}"#;
        let body2 = br#"{"version":"0.6.0"}"#;
        let jp = vec!["$.version".into()];
        assert_ne!(
            compute_fingerprint(body1, &jp),
            compute_fingerprint(body2, &jp)
        );
    }

    #[test]
    fn empty_jsonpaths_yields_none() {
        let body = br#"{"x":1}"#;
        assert!(compute_fingerprint(body, &[]).is_none());
    }

    #[test]
    fn record_still_valid_round_trip() {
        let body = br#"{"version":"0.5.7","build":"abc"}"#;
        let record = build_record(
            HostDirectMethod::WslMirroredNetworking,
            "http://127.0.0.1:11434",
            "/api/version",
            body,
            vec!["$.version".into()],
        );
        let fresh = br#"{"version":"0.5.7","build":"def"}"#;
        assert!(record_still_valid(&record, fresh));
    }

    #[test]
    fn record_invalid_on_version_bump() {
        let body = br#"{"version":"0.5.7"}"#;
        let record = build_record(
            HostDirectMethod::WslMirroredNetworking,
            "http://127.0.0.1:11434",
            "/api/version",
            body,
            vec!["$.version".into()],
        );
        let fresh = br#"{"version":"0.6.0"}"#;
        assert!(!record_still_valid(&record, fresh));
    }
}
