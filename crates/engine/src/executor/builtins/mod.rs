//! In-process built-in executors.

// Each builtin module is `pub(crate)` so registry.rs + the run
// loop can reach its `NODE_TYPE_ID` constant directly without an
// awkward re-export shim (clippy's redundant_pub_crate /
// unreachable_pub crossfire is unkind to that pattern).
pub(crate) mod checkpoint;
pub(crate) mod coding_agent;
pub(crate) mod compose;
pub(crate) mod condition;
pub(crate) mod delay;
pub(crate) mod file;
pub(crate) mod http;
pub(crate) mod kv;
pub(crate) mod llm;
pub(crate) mod loop_for;
pub(crate) mod notify;
pub(crate) mod parallel;
pub(crate) mod transform;
mod util;
pub(crate) mod wait_event;

pub use checkpoint::CheckpointExecutor;
pub use compose::ComposeExecutor;
pub use condition::ConditionExecutor;
pub use delay::DelayExecutor;
pub use file::FileExecutor;
pub use http::HttpExecutor;
pub use kv::KvExecutor;
pub use llm::LlmExecutor;
pub use loop_for::LoopForExecutor;
pub use notify::NotifyExecutor;
pub use parallel::ParallelExecutor;
pub use transform::TransformExecutor;
pub use wait_event::WaitEventExecutor;
