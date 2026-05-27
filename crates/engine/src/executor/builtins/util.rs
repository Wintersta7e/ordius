//! Shared helpers for in-process built-in executors.

use crate::environment::runtime::resource::Capability;
use crate::executor::NodeError;
use std::collections::HashMap;

/// Fetch a required string config field. The error message uses
/// `prefix` (typically the node type id) so callers get
/// `"transform: 'input' (string) required"` rather than a
/// nondescript "config error".
pub(super) fn config_str<'a>(
    cfg: &'a HashMap<String, serde_json::Value>,
    key: &str,
    prefix: &str,
) -> Result<&'a str, NodeError> {
    cfg.get(key)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| NodeError::Config(format!("{prefix}: '{key}' (string) required")))
}

/// Optional string config field — `None` when missing or when the
/// value isn't a JSON string.
pub(super) fn config_str_opt<'a>(
    cfg: &'a HashMap<String, serde_json::Value>,
    key: &str,
) -> Option<&'a str> {
    cfg.get(key).and_then(serde_json::Value::as_str)
}

/// Optional string config field with a `default` if missing /
/// wrong type.
pub(super) fn config_str_or<'a>(
    cfg: &'a HashMap<String, serde_json::Value>,
    key: &str,
    default: &'a str,
) -> &'a str {
    cfg.get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or(default)
}

/// Optional `u64` config field with a `default` if missing /
/// wrong type.
pub(super) fn config_u64_or(
    cfg: &HashMap<String, serde_json::Value>,
    key: &str,
    default: u64,
) -> u64 {
    cfg.get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(default)
}

/// Optional `f64` config field with a `default` if missing /
/// wrong type.
pub(super) fn config_f64_or(
    cfg: &HashMap<String, serde_json::Value>,
    key: &str,
    default: f64,
) -> f64 {
    cfg.get(key)
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(default)
}

/// Optional `bool` config field with a `default` if missing /
/// wrong type.
pub(super) fn config_bool_or(
    cfg: &HashMap<String, serde_json::Value>,
    key: &str,
    default: bool,
) -> bool {
    cfg.get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(default)
}

/// Return the parent-directory stem of a probe path. The result is
/// concatenated onto `http://host:port` to form the base URL prefix
/// for the dispatch URL. Examples (and the edge cases we care about):
///
/// - `"/v1/models"` → `"/v1"` (the normal OpenAI-compat shape)
/// - `"/api/version"` → `"/api"` (Ollama-native, but kept generic)
/// - `"/"` or `""` → `""` (nothing to derive a stem from)
/// - `"/chat/completions"` → `"/chat"` (would be unusual but consistent)
/// - `"models"` (no leading slash) → `""` (no parent segment exists)
/// - `"/v1/"` (trailing slash, empty leaf) → `"/v1"` (drop the trailing
///   slash before taking the parent)
///
/// The stem is meant to be appended verbatim to `http://host:port`, so
/// the empty string degrades gracefully to the bare host:port form.
#[must_use]
pub(super) fn path_parent_stem(path: &str) -> String {
    // A trailing slash marks a directory-shaped path (e.g. `/v1/`).
    // Treat it as already-the-stem: drop the trailing `/` and return.
    if path.len() > 1 && path.ends_with('/') {
        return path.trim_end_matches('/').to_string();
    }
    // Otherwise, lift everything before the last `/`. For `/v1/models`
    // that's `/v1`; for `/models` it's `""`; for `models` (no leading
    // slash) the rfind returns None and we return `""`.
    let Some(idx) = path.rfind('/') else {
        return String::new();
    };
    path[..idx].to_string()
}

/// Map a probe-time capability to the dispatch URL suffix.
///
/// Executors append this onto the proven probe route's
/// `base_url + path_parent_stem` to build the dispatch URL. E.g.:
/// - probe `GET /v1/models` (proves `OpenaiChatCompletions`) + suffix
///   `/chat/completions` → dispatch URL `http://…/v1/chat/completions`.
/// - probe `GET /api/version` (proves `OllamaNative`) + suffix `/chat`
///   → dispatch URL `http://…/api/chat`.
///
/// Returns `None` for capabilities that aren't HTTP dispatches
/// (`CliAgentPrint`, `CodeFormatter`, `PackageManager`). Callers MUST
/// handle `None` as an explicit "this capability can't be used for
/// HTTP dispatch" rather than silently defaulting to an `OpenAI`
/// shape.
///
/// The match is intentionally exhaustive (no `_ =>` arm) so adding a
/// new `Capability` variant fails compilation and forces the lookup
/// table to stay explicit. `LmStudioNative` shares the OpenAI-compat
/// chat-completions arm because LM Studio exposes the same wire
/// shape under `/v1/`.
#[must_use]
pub(super) const fn dispatch_suffix_for_capability(cap: Capability) -> Option<&'static str> {
    match cap {
        // LM Studio reuses the OpenAI-compat chat-completions path
        // (`/v1/chat/completions`); grouping it here keeps the table
        // free of duplicate-body arms.
        Capability::OpenaiChatCompletions
        | Capability::OpenaiToolCalling
        | Capability::OpenaiStreamingChat
        | Capability::LmStudioNative => Some("/chat/completions"),
        Capability::OpenaiEmbeddings => Some("/embeddings"),
        // Ollama-native: probe route is /api/version → stem /api,
        // dispatch path is /api/chat. Suffix "/chat" yields the
        // correct /api/chat.
        Capability::OllamaNative => Some("/chat"),
        // Non-HTTP / non-dispatching capabilities.
        Capability::CliAgentPrint | Capability::CodeFormatter | Capability::PackageManager => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{Capability, dispatch_suffix_for_capability, path_parent_stem};

    #[test]
    fn dispatch_suffix_covers_every_openai_capability() {
        assert_eq!(
            dispatch_suffix_for_capability(Capability::OpenaiChatCompletions),
            Some("/chat/completions"),
        );
        assert_eq!(
            dispatch_suffix_for_capability(Capability::OpenaiToolCalling),
            Some("/chat/completions"),
        );
        assert_eq!(
            dispatch_suffix_for_capability(Capability::OpenaiStreamingChat),
            Some("/chat/completions"),
        );
        assert_eq!(
            dispatch_suffix_for_capability(Capability::OpenaiEmbeddings),
            Some("/embeddings"),
        );
    }

    #[test]
    fn dispatch_suffix_native_apis() {
        assert_eq!(
            dispatch_suffix_for_capability(Capability::OllamaNative),
            Some("/chat"),
        );
        assert_eq!(
            dispatch_suffix_for_capability(Capability::LmStudioNative),
            Some("/chat/completions"),
        );
    }

    #[test]
    fn dispatch_suffix_none_for_non_http_capabilities() {
        assert_eq!(
            dispatch_suffix_for_capability(Capability::CliAgentPrint),
            None,
        );
        assert_eq!(
            dispatch_suffix_for_capability(Capability::CodeFormatter),
            None,
        );
        assert_eq!(
            dispatch_suffix_for_capability(Capability::PackageManager),
            None,
        );
    }

    #[test]
    fn path_parent_stem_examples() {
        assert_eq!(path_parent_stem("/v1/models"), "/v1");
        assert_eq!(path_parent_stem("/api/version"), "/api");
        assert_eq!(path_parent_stem("/v1/"), "/v1");
        assert_eq!(path_parent_stem("/"), "");
        assert_eq!(path_parent_stem(""), "");
        assert_eq!(path_parent_stem("models"), "");
        assert_eq!(path_parent_stem("/models"), "");
    }
}
