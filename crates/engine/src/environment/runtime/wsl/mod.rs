//! WSL dispatcher: `WslDispatcher` runs against a named WSL distribution
//! via `wsl.exe -d <name> --exec`.
//!
//! Submodules:
//! - `enumerate`     — `wsl.exe -l --verbose` parser, distro state.
//! - `path`          — host ↔ env path translation (inline + `wslpath`).
//! - `dispatcher`    — `WslDispatcher` (`Dispatcher` impl).
//! - `transport`     — `WslHttpTransport` (env-loopback wrap / `HostDirect` direct / public).
//! - `bootstrap`     — push helper binary, sha256-verify, atomic install.
//! - `shell_fallback`— constrained POSIX-sh probe runner.
//! - `host_direct`   — fingerprint + recompute for `HostDirectVerification`.
//! - `process`       — bounded `wsl.exe` runner with kill-on-timeout semantics.

pub mod bootstrap;
pub mod dispatcher;
pub mod enumerate;
pub mod host_direct;
pub mod path;
pub mod process;
pub mod shell_fallback;
pub mod transport;

pub use dispatcher::WslDispatcher;
pub use enumerate::{WslDistro, WslState, enumerate, enumerate_running, is_running};
pub use transport::WslHttpTransport;
