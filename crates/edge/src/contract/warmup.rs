use datalens_chain::{AdapterKey, DatasetSelector, SelectorKind};
use datalens_core::{ChainIdentity, DatalensError, DatasetKey, LedgerRangeKind, LogFilter};
use datalens_warmup::{
    WarmupChunkPolicy, WarmupRetryPolicy, WarmupRunResult, WarmupTask, WarmupTaskId,
    WarmupTaskMode, WarmupTaskState,
};
use serde::{Deserialize, Serialize};

use crate::contract::query::parse_dataset_key;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize)]
pub struct WarmupSubmitApiRequest {
    pub chain: ChainIdentity,
    pub dataset_key: WarmupDatasetKeyApi,
    pub selector: WarmupSelectorApiRequest,
    pub range_kind: LedgerRangeKind,
    pub start: u64,
    pub end: Option<u64>,
    #[serde(default = "default_warmup_api_mode")]
    pub mode: WarmupTaskMode,
    #[serde(default)]
    pub chunk_policy: WarmupChunkPolicy,
    #[serde(default)]
    pub retry_policy: WarmupRetryPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(untagged)]
pub enum WarmupDatasetKeyApi {
    Key(String),
    Structured(DatasetKey),
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum WarmupSelectorApiRequest {
    All,
    EvmLogs(LogFilter),
    Other {
        kind: String,
        fingerprint: String,
        canonical_key: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupSubmitApiResponse {
    pub task_id: WarmupTaskId,
    pub created: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupEnsureApiResponse {
    pub task_id: WarmupTaskId,
    pub created: bool,
    pub state: WarmupTaskState,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupTaskApiResponse {
    pub task: WarmupTaskView,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupTaskListApiResponse {
    pub tasks: Vec<WarmupTaskView>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupRunOnceApiResponse {
    pub results: Vec<WarmupRunResult>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupTaskView {
    pub task_id: WarmupTaskId,
    pub application_id: String,
    pub chain: ChainIdentity,
    pub dataset_key: String,
    pub selector: WarmupSelectorView,
    pub range_kind: LedgerRangeKind,
    pub start: u64,
    pub end: Option<u64>,
    pub mode: WarmupTaskMode,
    pub state: WarmupTaskState,
    pub created_at: u64,
    pub updated_at: u64,
    pub last_error: Option<String>,
    pub stats: datalens_warmup::WarmupStats,
    pub query_watermark: Option<u64>,
    pub cursor_next: Option<u64>,
    pub cursor_query_distance: Option<u64>,
    pub safe_head: Option<u64>,
    pub lookahead_blocks: Option<u64>,
    pub planned_start: Option<u64>,
    pub planned_end: Option<u64>,
    pub planned_query_distance: Option<u64>,
    pub no_op_reason: Option<String>,
    pub published_coverage_end: Option<u64>,
    pub published_query_distance: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct WarmupSelectorView {
    pub kind: String,
    pub fingerprint: String,
    pub canonical_key: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct WarmupTaskListQuery {
    pub chain: Option<String>,
    pub state: Option<WarmupTaskState>,
}

fn default_warmup_api_mode() -> WarmupTaskMode {
    WarmupTaskMode::FixedRange
}

impl WarmupDatasetKeyApi {
    pub(crate) fn into_dataset_key(self) -> Result<DatasetKey, DatalensError> {
        match self {
            Self::Structured(dataset_key) => Ok(dataset_key),
            Self::Key(value) => parse_dataset_key(&value),
        }
    }

    pub(crate) fn dataset_key(&self) -> Result<DatasetKey, DatalensError> {
        match self {
            Self::Structured(dataset_key) => Ok(dataset_key.clone()),
            Self::Key(value) => parse_dataset_key(value),
        }
    }
}

impl WarmupSelectorApiRequest {
    pub(crate) fn into_selector(self) -> Result<DatasetSelector, DatalensError> {
        match self {
            Self::All => Ok(DatasetSelector::all()),
            Self::EvmLogs(filter) => DatasetSelector::try_evm_logs(filter),
            Self::Other {
                kind,
                fingerprint,
                canonical_key,
            } => DatasetSelector::try_other(AdapterKey::try_new(kind)?, fingerprint, canonical_key),
        }
    }
}

impl From<&DatasetSelector> for WarmupSelectorView {
    fn from(selector: &DatasetSelector) -> Self {
        Self {
            kind: selector_kind_name(&selector.kind()),
            fingerprint: selector.fingerprint(),
            canonical_key: selector.canonical_key(),
        }
    }
}

impl WarmupSubmitApiRequest {
    pub(crate) fn chain(&self) -> &ChainIdentity {
        &self.chain
    }

    pub(crate) fn dataset_for_auth(&self) -> Result<String, DatalensError> {
        Ok(self.dataset_key.dataset_key()?.as_str().to_owned())
    }
}

pub(crate) fn warmup_task_view(task: WarmupTask) -> Result<WarmupTaskView, DatalensError> {
    let follow_query_status = task.follow_query_status.clone();
    Ok(WarmupTaskView {
        task_id: task.task_id,
        application_id: task.application_id,
        chain: task.chain,
        dataset_key: task.dataset_key.as_str().to_owned(),
        selector: WarmupSelectorView::from(&task.selector),
        range_kind: task.range_kind,
        start: task.start,
        end: task.end,
        mode: task.mode,
        state: task.state,
        created_at: task.created_at,
        updated_at: task.updated_at,
        last_error: task.last_error,
        stats: task.stats,
        query_watermark: follow_query_status
            .as_ref()
            .and_then(|status| status.query_watermark),
        cursor_next: follow_query_status
            .as_ref()
            .map(|status| status.cursor_next),
        cursor_query_distance: follow_query_status
            .as_ref()
            .and_then(|status| status.cursor_query_distance),
        safe_head: follow_query_status.as_ref().map(|status| status.safe_head),
        lookahead_blocks: follow_query_status
            .as_ref()
            .map(|status| status.lookahead_blocks),
        planned_start: follow_query_status
            .as_ref()
            .and_then(|status| status.planned_start),
        planned_end: follow_query_status
            .as_ref()
            .and_then(|status| status.planned_end),
        planned_query_distance: follow_query_status
            .as_ref()
            .and_then(|status| status.planned_query_distance),
        no_op_reason: follow_query_status
            .as_ref()
            .and_then(|status| status.no_op_reason.clone()),
        published_coverage_end: follow_query_status
            .as_ref()
            .and_then(|status| status.published_coverage_end),
        published_query_distance: follow_query_status
            .as_ref()
            .and_then(|status| status.published_query_distance),
    })
}

fn selector_kind_name(selector: &SelectorKind) -> String {
    match selector {
        SelectorKind::All => "all".to_owned(),
        SelectorKind::EvmLogs => "evm_logs".to_owned(),
        SelectorKind::Other(kind) => kind.as_str().to_owned(),
    }
}
