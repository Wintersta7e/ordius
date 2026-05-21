//! Host environment discovery.

mod types;
pub use types::*;

mod local;

mod detect;
pub use detect::{detect, detect_platform};

mod wsl;
pub use wsl::{WslDistroEntry, enumerate_running_distros, enumerate_wsl_distros};

mod custom;
