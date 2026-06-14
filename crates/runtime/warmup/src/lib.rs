//! Application-scoped durable cache warmup task pool.

mod cursor;
mod pending_commit;
mod registry;
mod runtime;
mod target_planner;
mod task;

pub use cursor::{WarmupCheckpoint, WarmupCursor};
pub use registry::{
    LocalWarmupRegistry, RegistryMigrationFailure, RegistryMigrationReport,
    RegistryMigrationSectionReport, WarmupEnsureOutcome, WarmupRegistry, WarmupSubmitOutcome,
    WarmupTaskFilter,
};
pub use runtime::{
    WarmupRunResult, WarmupRunStatus, WarmupRuntime, WarmupRuntimeConfig, WarmupSchedulerConfig,
    WarmupTaskPool,
};
pub use target_planner::{PlannedWarmupTarget, WarmupTargetPlanInput, WarmupTargetPlanner};
pub use task::{
    WarmupChunkPolicy, WarmupFollowQueryStatus, WarmupRetryPolicy, WarmupStats,
    WarmupSubmitRequest, WarmupTask, WarmupTaskId, WarmupTaskMode, WarmupTaskState,
};
