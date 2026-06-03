/// Workspace sync manager skeleton.
pub mod manager;
/// Upload safety helpers (ignore rules, caps, walk, manifest).
pub mod safety;
/// Workspace transport traits and implementations.
pub mod transport;
pub use manager::{RunOutcome, RunScope, WorkspaceManager};
pub use transport::{FileKind, FileMeta, WorkspaceTransport, WorkspaceTransportFactory};

#[cfg(any(test, feature = "testing"))]
pub use transport::{FakeWorkspaceTransport, FakeWorkspaceTransportFactory};
