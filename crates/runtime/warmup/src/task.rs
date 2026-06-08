use datalens_chain::DatasetSelector;
use datalens_core::{ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRangeKind};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct WarmupTaskId(String);

impl WarmupTaskId {
    pub fn new(value: impl Into<String>) -> Result<Self, DatalensError> {
        let value = value.into();
        let value = value.trim();
        if value.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "warmup task id must not be empty",
            ));
        }
        if value.contains('/') || value.contains('\\') {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "warmup task id must not contain path separators",
            ));
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn from_dedupe_key(key: &str) -> Self {
        Self(format!("warmup-{:016x}", stable_hash(key)))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmupTaskMode {
    FixedRange,
    FollowSafeHeight,
    FollowQuery,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WarmupTaskState {
    Queued,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

impl WarmupTaskState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WarmupChunkPolicy {
    #[serde(default = "default_warmup_chunk_max_range_len")]
    pub max_range_len: u64,
    #[serde(default)]
    pub target_rows_hint: Option<usize>,
}

impl Default for WarmupChunkPolicy {
    fn default() -> Self {
        Self {
            max_range_len: 1_000,
            target_rows_hint: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WarmupRetryPolicy {
    #[serde(default = "default_warmup_retry_max_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_warmup_retry_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "default_warmup_retry_max_backoff_ms")]
    pub max_backoff_ms: u64,
}

impl Default for WarmupRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 250,
            max_backoff_ms: 30_000,
        }
    }
}

fn default_warmup_chunk_max_range_len() -> u64 {
    1_000
}

fn default_warmup_retry_max_attempts() -> u32 {
    3
}

fn default_warmup_retry_initial_backoff_ms() -> u64 {
    250
}

fn default_warmup_retry_max_backoff_ms() -> u64 {
    30_000
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct WarmupStats {
    pub fetched_ranges: u64,
    pub written_ranges: u64,
    pub empty_ranges: u64,
    pub provider_calls: u64,
    pub rows_fetched: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct WarmupFollowQueryStatus {
    pub query_watermark: Option<u64>,
    pub cursor_next: u64,
    pub cursor_query_distance: Option<u64>,
    pub safe_head: u64,
    pub lookahead_blocks: u64,
    pub planned_start: Option<u64>,
    pub planned_end: Option<u64>,
    pub planned_query_distance: Option<u64>,
    pub no_op_reason: Option<String>,
    pub published_coverage_end: Option<u64>,
    pub published_query_distance: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WarmupSubmitRequest {
    pub application_id: String,
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector: DatasetSelector,
    pub range_kind: LedgerRangeKind,
    pub start: u64,
    pub end: Option<u64>,
    pub mode: WarmupTaskMode,
    pub chunk_policy: WarmupChunkPolicy,
    pub retry_policy: WarmupRetryPolicy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Submitted warmup work scoped to one application, chain, dataset, selector,
/// and range policy. The dedupe key uses canonical selector form so equivalent
/// requests map to the same task id.
pub struct WarmupTask {
    pub task_id: WarmupTaskId,
    pub application_id: String,
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector: DatasetSelector,
    pub range_kind: LedgerRangeKind,
    pub start: u64,
    pub end: Option<u64>,
    pub mode: WarmupTaskMode,
    pub chunk_policy: WarmupChunkPolicy,
    pub retry_policy: WarmupRetryPolicy,
    pub state: WarmupTaskState,
    pub created_at: u64,
    pub updated_at: u64,
    pub last_error: Option<String>,
    pub stats: WarmupStats,
    pub follow_query_status: Option<WarmupFollowQueryStatus>,
    pub(crate) dedupe_key: String,
}

impl WarmupTask {
    pub(crate) fn from_submit(
        request: WarmupSubmitRequest,
        now: u64,
    ) -> Result<Self, DatalensError> {
        validate_submit(&request)?;
        Self::from_validated_request(request, now, task_dedupe_key)
    }

    pub(crate) fn from_ensure(
        request: WarmupSubmitRequest,
        now: u64,
    ) -> Result<Self, DatalensError> {
        validate_submit(&request)?;
        validate_follow_query_ensure(&request)?;
        Self::from_validated_request(request, now, task_ensure_key)
    }

    fn from_validated_request(
        request: WarmupSubmitRequest,
        now: u64,
        identity_key: fn(&WarmupSubmitRequest) -> String,
    ) -> Result<Self, DatalensError> {
        let dedupe_key = identity_key(&request);
        Ok(Self {
            task_id: WarmupTaskId::from_dedupe_key(&dedupe_key),
            application_id: request.application_id,
            chain: request.chain,
            dataset_key: request.dataset_key,
            selector: request.selector,
            range_kind: request.range_kind,
            start: request.start,
            end: request.end,
            mode: request.mode,
            chunk_policy: WarmupChunkPolicy {
                max_range_len: request.chunk_policy.max_range_len.max(1),
                target_rows_hint: request.chunk_policy.target_rows_hint,
            },
            retry_policy: WarmupRetryPolicy {
                max_attempts: request.retry_policy.max_attempts.max(1),
                initial_backoff_ms: request.retry_policy.initial_backoff_ms,
                max_backoff_ms: request.retry_policy.max_backoff_ms,
            },
            state: WarmupTaskState::Queued,
            created_at: now,
            updated_at: now,
            last_error: None,
            stats: WarmupStats::default(),
            follow_query_status: None,
            dedupe_key,
        })
    }

    pub(crate) fn touch(&mut self, now: u64) {
        self.updated_at = now;
    }

    pub(crate) fn reset_for_retry(&mut self, now: u64) -> Result<(), DatalensError> {
        if self.state != WarmupTaskState::Failed {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "only failed warmup tasks can be retried",
            ));
        }
        self.state = WarmupTaskState::Queued;
        self.last_error = None;
        self.touch(now);
        Ok(())
    }
}

fn validate_submit(request: &WarmupSubmitRequest) -> Result<(), DatalensError> {
    if request.application_id.trim().is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup application id must not be empty",
        ));
    }
    if let Some(end) = request.end
        && request.start > end
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup start must be less than or equal to end",
        ));
    }
    if request.mode == WarmupTaskMode::FixedRange && request.end.is_none() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "fixed-range warmup requires an end block",
        ));
    }
    Ok(())
}

fn validate_follow_query_ensure(request: &WarmupSubmitRequest) -> Result<(), DatalensError> {
    if request.mode != WarmupTaskMode::FollowQuery {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "warmup ensure requires follow_query mode",
        ));
    }
    Ok(())
}

pub(crate) fn task_dedupe_key(request: &WarmupSubmitRequest) -> String {
    format!(
        "application={};chain={};dataset={};selector={};range_kind={:?};start={};end={:?};mode={:?}",
        request.application_id.trim(),
        request.chain.key_prefix(),
        request.dataset_key.as_str(),
        request.selector.canonical_key(),
        request.range_kind,
        request.start,
        request.end,
        request.mode,
    )
}

pub(crate) fn task_ensure_key(request: &WarmupSubmitRequest) -> String {
    task_identity_key(
        request.application_id.trim(),
        &request.chain,
        &request.dataset_key,
        &request.selector,
        &request.range_kind,
        request.mode,
    )
}

pub(crate) fn task_ensure_key_for_task(task: &WarmupTask) -> String {
    task_identity_key(
        task.application_id.trim(),
        &task.chain,
        &task.dataset_key,
        &task.selector,
        &task.range_kind,
        task.mode,
    )
}

fn task_identity_key(
    application_id: &str,
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    range_kind: &LedgerRangeKind,
    mode: WarmupTaskMode,
) -> String {
    format!(
        "application={};chain={};dataset={};selector={};range_kind={:?};mode={:?}",
        application_id,
        chain.key_prefix(),
        dataset_key.as_str(),
        selector.canonical_key(),
        range_kind,
        mode,
    )
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
