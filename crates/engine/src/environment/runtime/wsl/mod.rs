//! WSL dispatcher implementation.
//!
//! Provides `WslDispatcher` (Dispatcher trait impl), `WslHttpTransport`
//! (env-loopback wrap vs `HostDirect` direct vs public direct), path
//! translation via `wslpath`, and helper bootstrap inside the distro.

pub mod bootstrap;
pub mod dispatcher;
pub mod enumerate;
pub mod path;

pub use dispatcher::WslDispatcher;
