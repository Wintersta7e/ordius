//! Host environment discovery.
//!
//! Phase E moved the entire substrate into [`runtime`]. The legacy session-C
//! `EnvironmentReport` types are gone; the desktop IPC consumes
//! `runtime::EnvRegistry` directly.
pub mod runtime;
pub use runtime::*;
