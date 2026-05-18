//! Port + value types. Spec: docs/06-data-flow.md "Typed ports on every node".

use serde::{Deserialize, Serialize};

/// Wire-format port type tag. Used by `PortDef` (compile-time port spec)
/// and as an implicit discriminator for `PortValue` runtime values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PortType {
    /// UTF-8 string.
    String,
    /// 64-bit float.
    Number,
    /// `true` / `false`.
    Boolean,
    /// Arbitrary JSON value.
    Json,
    /// Filesystem path (string).
    File,
    /// Base64-encoded byte buffer (string).
    Bytes,
    /// Embedding vector (sequence of f64).
    Vector,
}

/// Port specification declared on a `NodeType`. Drives schema validation
/// and the GUI's port-type colouring.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortDef {
    /// Stable port identifier within its node-type.
    pub name: String,
    /// Runtime type tag.
    #[serde(rename = "type")]
    pub ty: PortType,
    /// If true, the workflow loader requires an incoming edge.
    #[serde(default)]
    pub required: bool,
}

/// Runtime value carried across edges. Discriminated by structure
/// (`#[serde(untagged)]`) to keep the wire format compact.
///
/// **Serde note.** `String`, `File`, and `Bytes` all serialise as JSON
/// strings. Untagged deserialisation picks the first match, so any
/// string-shaped input deserialises as `PortValue::String` — never as
/// `File` or `Bytes`. This is intentional: variant identity for the
/// three string-shaped cases is carried out-of-band by the declaring
/// `PortDef::ty`, not by the wire form. Treat this enum as write-typed
/// but read-coalesced for those three variants.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PortValue {
    /// UTF-8 string payload.
    String(String),
    /// 64-bit float payload.
    Number(f64),
    /// Boolean payload.
    Boolean(bool),
    /// Arbitrary JSON payload.
    Json(serde_json::Value),
    /// Filesystem path payload.
    File(String),
    /// Base64-encoded byte buffer payload.
    Bytes(String),
    /// Embedding vector payload.
    Vector(Vec<f64>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn portdef_roundtrips_through_yaml() {
        let p = PortDef {
            name: "prompt".into(),
            ty: PortType::String,
            required: true,
        };
        let y = serde_yaml::to_string(&p).unwrap();
        let back: PortDef = serde_yaml::from_str(&y).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn portvalue_serialises_compactly() {
        assert_eq!(
            serde_json::to_string(&PortValue::Number(1.5)).unwrap(),
            "1.5"
        );
        assert_eq!(
            serde_json::to_string(&PortValue::String("hi".into())).unwrap(),
            "\"hi\""
        );
    }
}
