//! Typed environment runtime — Phase A of the env-runtime rewrite.
//!
//! New code lives here alongside the legacy `environment::{types, detect, local,
//! wsl, custom}` modules. See `docs/plans/environment-runtime.md`.

pub mod builtin;
pub mod catalog;
pub mod dispatcher;
pub mod env;
pub mod error;
pub mod local;
pub mod plan;
pub mod registry;
pub mod resource;
pub mod transport;

#[cfg(any(test, feature = "testing"))]
pub mod fake;
