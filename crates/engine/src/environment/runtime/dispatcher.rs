//! `Dispatcher` and `HttpTransport` traits.
//!
//! Each env type implements `Dispatcher`; it owns its own `HttpTransport`.
//! Phase A provides `LocalDispatcher`; later phases add WSL, SSH, Container.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use url::Url;

use super::catalog::ResourceProbeOutcome;
use super::env::EnvInfo;
use super::error::DispatchError;
use super::plan::{ProbePlan, ProbeSummary};
use super::resource::ResourceDefinition;
use super::transport::{EnvPath, EnvProcess, HttpError, HttpRequest, HttpResponse, ProcessCmd};

/// Async object-safe trait implemented by every env type.
/// Callers hold `Arc<dyn Dispatcher>` and never need to know the concrete type.
#[async_trait]
pub trait Dispatcher: Send + Sync {
    /// Return static metadata about this environment.
    fn info(&self) -> &EnvInfo;

    /// Run a full probe plan against this environment's resource list.
    /// Caller provides the plan (resolved from the registry) and a cancellation
    /// token; implementations should respect it for clean shutdown.
    async fn probe(
        &self,
        plan: ProbePlan,
        cancel: CancellationToken,
    ) -> Result<ProbeSummary, DispatchError>;

    /// Re-probe a single resource definition. Used by `RunCatalog` for
    /// opportunistic re-checks without a full plan.
    async fn probe_resource(
        &self,
        def: &ResourceDefinition,
        cancel: CancellationToken,
    ) -> ResourceProbeOutcome;

    /// Spawn a process inside this environment and return an env-neutral handle.
    ///
    /// Implementations MUST honor `cmd.stdin`: pipe the bytes to the child's
    /// stdin and close it. A child that closes stdin early is legitimate; do
    /// not surface the resulting `BrokenPipe` as an error.
    async fn spawn(&self, cmd: ProcessCmd) -> Result<Box<dyn EnvProcess>, DispatchError>;

    /// Return the HTTP transport bound to this environment.
    /// `Arc<dyn HttpTransport>` keeps the trait object-safe.
    fn http_transport(&self) -> Arc<dyn HttpTransport>;

    /// Translate a host-side `Path` to an env-local `EnvPath`.
    /// For `Local` this is an identity; for WSL it maps `/mnt/...`.
    fn translate_path(&self, host_path: &Path) -> Result<EnvPath, DispatchError>;
}

/// Stream of response body chunks. A top-level type alias so `LocalDispatcher`
/// and future impls can name it without repeating the full bound.
pub type ResponseStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes, HttpError>> + Send>>;

/// Async HTTP transport bound to a specific environment.
/// Object-safe: no generic methods; `ResponseStream` is a concrete type alias.
#[async_trait]
pub trait HttpTransport: Send + Sync {
    /// Send a one-shot HTTP request and collect the complete response body.
    async fn execute(&self, req: HttpRequest) -> Result<HttpResponse, HttpError>;

    /// Send a request and return a streaming body for SSE / chunked responses.
    async fn execute_stream(&self, req: HttpRequest) -> Result<ResponseStream, HttpError>;

    /// Return `true` if this transport can stream from the given URL.
    /// Local transports always return `true`; tunnel-based transports may
    /// restrict based on the host.
    fn can_stream(&self, url: &Url) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compile-only assertion: the traits are object-safe and the public
    // API surface compiles.
    #[allow(dead_code)]
    fn assert_object_safe(_d: &dyn Dispatcher) {}
    #[allow(dead_code)]
    fn assert_http_object_safe(_t: &dyn HttpTransport) {}
}
