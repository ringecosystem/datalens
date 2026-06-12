use datalens_cache_repair::{
    CacheRepairFinality, CacheRepairRunResult, CacheRepairTask, CacheRepairTaskId,
    CacheRepairTaskState,
};
use datalens_core::{ChainIdentity, DatalensError, LedgerRangeKind};
use serde::{Deserialize, Serialize};

use crate::contract::warmup::{WarmupDatasetKeyApi, WarmupSelectorApiRequest, WarmupSelectorView};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct CacheRepairSubmitApiRequest {
    pub chain: ChainIdentity,
    pub dataset_key: WarmupDatasetKeyApi,
    pub selector: WarmupSelectorApiRequest,
    pub range_kind: LedgerRangeKind,
    pub start: u64,
    pub end: u64,
    #[serde(default = "default_cache_repair_finality")]
    pub finality: CacheRepairFinality,
    #[serde(default)]
    pub chunk_policy: datalens_cache_repair::CacheRepairChunkPolicy,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CacheRepairSubmitApiResponse {
    pub task_id: CacheRepairTaskId,
    pub created: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CacheRepairTaskApiResponse {
    pub task: CacheRepairTaskView,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CacheRepairTaskListApiResponse {
    pub tasks: Vec<CacheRepairTaskView>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CacheRepairRunOnceApiResponse {
    pub results: Vec<CacheRepairRunResult>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CacheRepairTaskView {
    pub task_id: CacheRepairTaskId,
    pub application_id: String,
    pub chain: ChainIdentity,
    pub dataset_key: String,
    pub selector: WarmupSelectorView,
    pub range_kind: LedgerRangeKind,
    pub start: u64,
    pub end: u64,
    pub finality: CacheRepairFinality,
    pub state: CacheRepairTaskState,
    pub created_at: u64,
    pub updated_at: u64,
    pub last_error: Option<String>,
    pub stats: datalens_cache_repair::CacheRepairStats,
    pub reason: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct CacheRepairTaskListQuery {
    pub chain: Option<String>,
    pub state: Option<CacheRepairTaskState>,
}

impl CacheRepairSubmitApiRequest {
    pub(crate) fn chain(&self) -> &ChainIdentity {
        &self.chain
    }

    pub(crate) fn dataset_for_auth(&self) -> Result<String, DatalensError> {
        Ok(self.dataset_key.dataset_key()?.as_str().to_owned())
    }
}

pub(crate) fn cache_repair_task_view(
    task: CacheRepairTask,
) -> Result<CacheRepairTaskView, DatalensError> {
    Ok(CacheRepairTaskView {
        task_id: task.task_id,
        application_id: task.application_id,
        chain: task.chain,
        dataset_key: task.dataset_key.as_str().to_owned(),
        selector: WarmupSelectorView::from(&task.selector),
        range_kind: task.range_kind,
        start: task.start,
        end: task.end,
        finality: task.finality,
        state: task.state,
        created_at: task.created_at,
        updated_at: task.updated_at,
        last_error: task.last_error,
        stats: task.stats,
        reason: task.reason,
    })
}

fn default_cache_repair_finality() -> CacheRepairFinality {
    CacheRepairFinality::Safe
}
