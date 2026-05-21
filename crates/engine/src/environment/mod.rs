//! Host environment discovery.

mod types;
pub use types::*;

mod local;

mod detect;
pub use detect::{detect, detect_platform};

// Submodules added in subsequent tasks:
// mod wsl;
// mod custom;
