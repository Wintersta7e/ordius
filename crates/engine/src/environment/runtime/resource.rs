//! Resource identity, kind, capability, probe spec, and reference types.

/// Stable identifier for a resource, such as `"ollama"` or `"lm-studio"`.
/// Persisted as text in the `resources` table.
///
/// Note: Task 2 pre-seeds this type and a minimal `ResourceDefinition` stub
/// so `env.rs` fields can compile. Task 3 will expand this file with
/// `ResourceKind`, `Capability`, `ProbeSpec`, full `ResourceDefinition`, and
/// all tests, replacing the stub body here.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ResourceId(
    /// Stable resource identifier.
    pub String,
);

impl std::fmt::Display for ResourceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Stub for Task 2 compilation. Task 3 replaces this with the full definition:
/// (`kind`, `advertised_capabilities`, `probe`, `override_lower_scope`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ResourceDefinition {
    /// Resource identifier.
    pub id: ResourceId,
}
