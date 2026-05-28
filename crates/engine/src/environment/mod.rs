//! Host environment discovery.
//!
//! The runtime substrate lives in [`runtime`]. Desktop IPC consumes
//! `runtime::EnvRegistry` and the per-env resource catalogs directly.
pub mod runtime;
pub use runtime::*;
