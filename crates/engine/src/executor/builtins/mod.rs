//! In-process built-in executors.

mod delay;
mod transform;

pub use delay::DelayExecutor;
pub use transform::TransformExecutor;
