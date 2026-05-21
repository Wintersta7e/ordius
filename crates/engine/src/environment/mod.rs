//! Host environment discovery.

mod types;
pub use types::*;

mod local;

mod detect;
pub use detect::{detect, detect_platform};

mod wsl;

// Submodules added in subsequent tasks:
// mod custom;
