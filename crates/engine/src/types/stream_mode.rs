//! `StreamMode` — per-node opt-in/out for SSE streaming on LLM-shaped
//! HTTP routes.
//!
//! Phase D introduces the type and threads it through `LlmExecutor`'s
//! config. Phase E's dispatcher layer enforces the `Force` variant
//! against the resolved route's `streaming_supported` flag.
//!
//! Spec: docs/plans/environment-runtime.md §4 ("StreamMode").

use serde::{Deserialize, Serialize};

/// Streaming policy for nodes whose underlying transport supports SSE.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamMode {
    /// Stream if the resolved route advertises support; otherwise fall
    /// back to non-streaming with a status indicator on the run.
    #[default]
    Auto,
    /// Require streaming; error `StreamingUnsupported` if the route can't.
    Force,
    /// Never stream.
    Off,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_auto() {
        assert_eq!(StreamMode::default(), StreamMode::Auto);
    }

    #[test]
    fn serde_snake_case() {
        assert_eq!(
            serde_json::to_string(&StreamMode::Auto).unwrap(),
            r#""auto""#
        );
        assert_eq!(
            serde_json::to_string(&StreamMode::Force).unwrap(),
            r#""force""#
        );
        assert_eq!(serde_json::to_string(&StreamMode::Off).unwrap(), r#""off""#);
        let auto: StreamMode = serde_json::from_str(r#""auto""#).unwrap();
        assert_eq!(auto, StreamMode::Auto);
    }

    #[test]
    fn unknown_variant_errors() {
        let r: Result<StreamMode, _> = serde_json::from_str(r#""maybe""#);
        assert!(r.is_err());
    }
}
