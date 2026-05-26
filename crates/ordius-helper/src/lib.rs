//! Shared types and helpers used by the `ordius-helper` binary.
//!
//! The engine side (`ordius-engine`) duplicates the wire types in its own
//! `runtime::helper_proto` module; a serde compat test pins the shape so the
//! two crates can evolve independently.

pub mod exec;
pub mod probe;
pub mod protocol;
