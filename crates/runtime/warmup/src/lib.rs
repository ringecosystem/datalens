//! Application-scoped durable cache warmup task pool.

mod cursor;
mod registry;
mod runtime;
mod task;

pub use cursor::{WarmupCheckpoint, WarmupCursor};
pub use registry::{LocalWarmupRegistry, WarmupRegistry, WarmupSubmitOutcome, WarmupTaskFilter};
pub use runtime::{
    WarmupRunResult, WarmupRunStatus, WarmupRuntime, WarmupRuntimeConfig, WarmupSchedulerConfig,
    WarmupTaskPool,
};
pub use task::{
    WarmupChunkPolicy, WarmupRetryPolicy, WarmupStats, WarmupSubmitRequest, WarmupTask,
    WarmupTaskId, WarmupTaskMode, WarmupTaskState,
};
