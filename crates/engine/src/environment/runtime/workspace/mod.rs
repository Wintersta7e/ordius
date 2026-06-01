/// Workspace sync manager skeleton.
pub mod manager;
/// Workspace transport traits and implementations.
pub mod transport;
pub use manager::{RunOutcome, WorkspaceManager};
pub use transport::{FileKind, FileMeta, WorkspaceTransport, WorkspaceTransportFactory};

#[cfg(any(test, feature = "testing"))]
pub use transport::FakeWorkspaceTransport;
