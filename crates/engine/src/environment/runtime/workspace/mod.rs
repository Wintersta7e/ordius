pub mod transport;
pub use transport::{FileKind, FileMeta, WorkspaceTransport, WorkspaceTransportFactory};

#[cfg(any(test, feature = "testing"))]
pub use transport::FakeWorkspaceTransport;
