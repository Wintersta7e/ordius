/// Workspace sync manager skeleton.
pub mod manager;
/// `SafeOrDiverge` conflict-aware write-back (extracted from `manager`).
mod safe_or_diverge;
/// Upload safety helpers (ignore rules, caps, walk, manifest).
pub mod safety;
/// Workspace transport traits and implementations.
pub mod transport;
pub use manager::{RunOutcome, RunScope, WorkspaceExecutionLease, WorkspaceManager};
pub use transport::{FileKind, FileMeta, WorkspaceTransport, WorkspaceTransportFactory};

#[cfg(any(test, feature = "testing"))]
pub use transport::{FakeWorkspaceTransport, FakeWorkspaceTransportFactory};
