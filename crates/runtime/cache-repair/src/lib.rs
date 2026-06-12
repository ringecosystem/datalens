//! Application-scoped durable cache repair tasks.

use datalens_chain::{
    AdapterKey, ChainAdapter, ChainFetchRequest, DatasetSelector, FetchContext, FinalityLevel,
    validate_durable_range,
};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, EvmLogFilter, LedgerRange,
    LedgerRangeKind,
};
use datalens_storage::{ObjectStore, StorageRepository, StorageWriteRequest};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CacheRepairTaskId(String);

impl CacheRepairTaskId {
    pub fn new(value: impl Into<String>) -> Result<Self, DatalensError> {
        let value = value.into();
        let value = value.trim();
        if value.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "cache repair task id must not be empty",
            ));
        }
        if value.contains('/') || value.contains('\\') {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "cache repair task id must not contain path separators",
            ));
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn from_dedupe_key(key: &str) -> Self {
        Self(format!("cache-repair-{:016x}", stable_hash(key)))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheRepairTaskState {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl CacheRepairTaskState {
    fn is_runnable(self) -> bool {
        matches!(self, Self::Queued | Self::Running | Self::Failed)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheRepairChunkPolicy {
    #[serde(default = "default_cache_repair_chunk_max_range_len")]
    pub max_range_len: u64,
}

impl Default for CacheRepairChunkPolicy {
    fn default() -> Self {
        Self {
            max_range_len: default_cache_repair_chunk_max_range_len(),
        }
    }
}

fn default_cache_repair_chunk_max_range_len() -> u64 {
    1_000
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheRepairStats {
    pub fetched_ranges: u64,
    pub written_ranges: u64,
    pub empty_ranges: u64,
    pub provider_calls: u64,
    pub rows_fetched: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheRepairFinality {
    Safe,
    Finalized,
}

impl CacheRepairFinality {
    pub fn to_finality_level(self) -> FinalityLevel {
        match self {
            Self::Safe => FinalityLevel::Safe,
            Self::Finalized => FinalityLevel::Finalized,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheRepairSubmitRequest {
    pub application_id: String,
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector: DatasetSelector,
    pub range_kind: LedgerRangeKind,
    pub start: u64,
    pub end: u64,
    pub finality: CacheRepairFinality,
    pub chunk_policy: CacheRepairChunkPolicy,
    pub reason: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheRepairTask {
    pub task_id: CacheRepairTaskId,
    pub application_id: String,
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector: DatasetSelector,
    pub range_kind: LedgerRangeKind,
    pub start: u64,
    pub end: u64,
    pub finality: CacheRepairFinality,
    pub chunk_policy: CacheRepairChunkPolicy,
    pub reason: String,
    pub state: CacheRepairTaskState,
    pub created_at: u64,
    pub updated_at: u64,
    pub last_error: Option<String>,
    pub stats: CacheRepairStats,
    dedupe_key: String,
}

impl CacheRepairTask {
    fn from_submit(request: CacheRepairSubmitRequest, now: u64) -> Result<Self, DatalensError> {
        validate_submit(&request)?;
        let dedupe_key = task_dedupe_key(&request);
        Ok(Self {
            task_id: CacheRepairTaskId::from_dedupe_key(&dedupe_key),
            application_id: request.application_id,
            chain: request.chain,
            dataset_key: request.dataset_key,
            selector: request.selector,
            range_kind: request.range_kind,
            start: request.start,
            end: request.end,
            finality: request.finality,
            chunk_policy: CacheRepairChunkPolicy {
                max_range_len: request.chunk_policy.max_range_len.max(1),
            },
            reason: request.reason,
            state: CacheRepairTaskState::Queued,
            created_at: now,
            updated_at: now,
            last_error: None,
            stats: CacheRepairStats::default(),
            dedupe_key,
        })
    }

    fn touch(&mut self, now: u64) {
        self.updated_at = now;
    }

    fn reset_for_retry(&mut self, now: u64) -> Result<(), DatalensError> {
        if self.state != CacheRepairTaskState::Failed {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "only failed cache repair tasks can be retried",
            ));
        }
        self.state = CacheRepairTaskState::Queued;
        self.last_error = None;
        self.touch(now);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheRepairSubmitOutcome {
    pub task_id: CacheRepairTaskId,
    pub created: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CacheRepairTaskFilter {
    pub application_id: Option<String>,
    pub chain_key: Option<String>,
    pub state: Option<CacheRepairTaskState>,
}

pub trait CacheRepairRegistry: Clone + Send + Sync + 'static {
    fn submit(
        &self,
        request: CacheRepairSubmitRequest,
    ) -> Result<CacheRepairSubmitOutcome, DatalensError>;
    fn get(&self, task_id: &CacheRepairTaskId) -> Result<Option<CacheRepairTask>, DatalensError>;
    fn save_task(&self, task: &CacheRepairTask) -> Result<(), DatalensError>;
    fn list(&self, filter: CacheRepairTaskFilter) -> Result<Vec<CacheRepairTask>, DatalensError>;

    fn cancel(&self, task_id: &CacheRepairTaskId) -> Result<(), DatalensError> {
        self.set_state(task_id, CacheRepairTaskState::Cancelled)
    }

    fn retry_failed(&self, task_id: &CacheRepairTaskId) -> Result<(), DatalensError> {
        let mut task = self.get(task_id)?.ok_or_else(|| missing_task(task_id))?;
        task.reset_for_retry(unix_seconds_now()?)?;
        self.save_task(&task)
    }

    fn set_state(
        &self,
        task_id: &CacheRepairTaskId,
        state: CacheRepairTaskState,
    ) -> Result<(), DatalensError> {
        let mut task = self.get(task_id)?.ok_or_else(|| missing_task(task_id))?;
        task.state = state;
        task.touch(unix_seconds_now()?);
        self.save_task(&task)
    }
}

#[derive(Clone, Debug)]
pub struct LocalCacheRepairRegistry<S> {
    object_store: S,
}

impl<S> LocalCacheRepairRegistry<S>
where
    S: ObjectStore + 'static,
{
    pub fn new(object_store: S) -> Self {
        Self { object_store }
    }
}

impl<S> CacheRepairRegistry for LocalCacheRepairRegistry<S>
where
    S: ObjectStore + 'static,
{
    fn submit(
        &self,
        request: CacheRepairSubmitRequest,
    ) -> Result<CacheRepairSubmitOutcome, DatalensError> {
        let dedupe_key = task_dedupe_key(&request);
        if let Some(existing) = self
            .list(CacheRepairTaskFilter::default())?
            .into_iter()
            .find(|task| task.dedupe_key == dedupe_key)
        {
            return Ok(CacheRepairSubmitOutcome {
                task_id: existing.task_id,
                created: false,
            });
        }

        let task = CacheRepairTask::from_submit(request, unix_seconds_now()?)?;
        let task_id = task.task_id.clone();
        self.save_task(&task)?;
        Ok(CacheRepairSubmitOutcome {
            task_id,
            created: true,
        })
    }

    fn get(&self, task_id: &CacheRepairTaskId) -> Result<Option<CacheRepairTask>, DatalensError> {
        let key = task_key(task_id);
        if !self.object_store.exists(&key)? {
            return Ok(None);
        }
        decode_task(&self.object_store.get(&key)?).map(Some)
    }

    fn save_task(&self, task: &CacheRepairTask) -> Result<(), DatalensError> {
        let bytes = serde_json::to_vec_pretty(&StoredCacheRepairTask::from_task(task)?).map_err(
            |error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("encode cache repair task: {error}"),
                )
            },
        )?;
        self.object_store.put(&task_key(&task.task_id), &bytes)
    }

    fn list(&self, filter: CacheRepairTaskFilter) -> Result<Vec<CacheRepairTask>, DatalensError> {
        let mut tasks = Vec::new();
        for object in self.object_store.list("cache-repair/tasks")? {
            if object.key.ends_with(".json") {
                let task = decode_task(&self.object_store.get(&object.key)?)?;
                if matches_filter(&task, &filter) {
                    tasks.push(task);
                }
            }
        }
        tasks.sort_by(|left, right| left.task_id.as_str().cmp(right.task_id.as_str()));
        Ok(tasks)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheRepairRunStatus {
    Completed,
    Partial,
    #[default]
    Stopped,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheRepairRunResult {
    pub status: CacheRepairRunStatus,
    pub fetched_ranges: u64,
    pub written_ranges: u64,
    pub empty_ranges: u64,
    pub provider_calls: u64,
    pub rows_fetched: usize,
}

#[derive(Clone)]
pub struct CacheRepairRuntime<A, S, R> {
    adapter: A,
    storage: S,
    registry: R,
}

#[derive(Clone)]
pub struct CacheRepairTaskPool<A, S, R> {
    runtime: CacheRepairRuntime<A, S, R>,
}

impl<A, S, R> CacheRepairTaskPool<A, S, R>
where
    A: ChainAdapter,
    S: StorageRepository + Clone + 'static,
    R: CacheRepairRegistry,
{
    pub fn new(runtime: CacheRepairRuntime<A, S, R>) -> Self {
        Self { runtime }
    }

    pub fn submit(
        &self,
        request: CacheRepairSubmitRequest,
    ) -> Result<CacheRepairSubmitOutcome, DatalensError> {
        validate_request(&request, &self.runtime.adapter)?;
        self.runtime.registry.submit(request)
    }

    pub fn get(
        &self,
        task_id: &CacheRepairTaskId,
    ) -> Result<Option<CacheRepairTask>, DatalensError> {
        self.runtime.registry.get(task_id)
    }

    pub fn list(
        &self,
        mut filter: CacheRepairTaskFilter,
    ) -> Result<Vec<CacheRepairTask>, DatalensError> {
        let chain_key = self.runtime.adapter.capabilities().chain().key_prefix();
        if filter
            .chain_key
            .as_ref()
            .is_some_and(|filter_chain_key| filter_chain_key != &chain_key)
        {
            return Ok(Vec::new());
        }
        filter.chain_key = Some(chain_key);
        self.runtime.registry.list(filter)
    }

    pub fn cancel(&self, task_id: &CacheRepairTaskId) -> Result<(), DatalensError> {
        self.runtime.registry.cancel(task_id)
    }

    pub fn retry_failed(&self, task_id: &CacheRepairTaskId) -> Result<(), DatalensError> {
        self.runtime.registry.retry_failed(task_id)
    }

    pub fn run_available_once(&self) -> Result<Vec<CacheRepairRunResult>, DatalensError> {
        let mut results = Vec::new();
        let mut tasks = self
            .list(CacheRepairTaskFilter::default())?
            .into_iter()
            .filter(|task| task.state.is_runnable())
            .collect::<Vec<_>>();
        tasks.sort_by_key(|task| (task.created_at, task.task_id.as_str().to_owned()));
        for task in tasks.into_iter().take(1) {
            results.push(self.runtime.run_task_once(&task.task_id)?);
        }
        Ok(results)
    }
}

impl<A, S, R> CacheRepairRuntime<A, S, R>
where
    A: ChainAdapter,
    S: StorageRepository + Clone + 'static,
    R: CacheRepairRegistry,
{
    pub fn new(adapter: A, storage: S, registry: R) -> Self {
        Self {
            adapter,
            storage,
            registry,
        }
    }

    pub fn run_task_once(
        &self,
        task_id: &CacheRepairTaskId,
    ) -> Result<CacheRepairRunResult, DatalensError> {
        let mut task = self
            .registry
            .get(task_id)?
            .ok_or_else(|| missing_task(task_id))?;
        if task.state == CacheRepairTaskState::Cancelled {
            return Ok(CacheRepairRunResult {
                status: CacheRepairRunStatus::Stopped,
                ..CacheRepairRunResult::default()
            });
        }
        if task.state == CacheRepairTaskState::Completed {
            return Ok(CacheRepairRunResult {
                status: CacheRepairRunStatus::Completed,
                ..CacheRepairRunResult::default()
            });
        }

        validate_task(&task, &self.adapter)?;
        let durable_height = match task.finality {
            CacheRepairFinality::Safe => self.adapter.cache_safe_height()?,
            CacheRepairFinality::Finalized => self.adapter.finalized_height()?,
        };
        durable_height.validate_durable_writable()?;
        if durable_height.range_kind != task.range_kind {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "cache repair task range kind does not match adapter durable height",
            ));
        }

        task.state = CacheRepairTaskState::Running;
        task.last_error = None;
        task.touch(unix_seconds_now()?);
        self.registry.save_task(&task)?;

        let mut result = CacheRepairRunResult::default();
        let mut next = task.start;
        while next <= task.end {
            let chunk = LedgerRange::try_new(
                task.range_kind.clone(),
                next,
                task.end
                    .min(next.saturating_add(task.chunk_policy.max_range_len.max(1) - 1)),
            )?;
            validate_durable_range(&chunk, &durable_height)?;
            let request = ChainFetchRequest::new(
                task.chain.clone(),
                task.dataset_key.clone(),
                chunk.clone(),
                task.selector.clone(),
            )
            .with_context(FetchContext {
                request_id: Some(task.task_id.as_str().to_owned()),
                cache_write: true,
            });
            let response = match self.adapter.fetch(request.clone()) {
                Ok(response) => response,
                Err(error) => {
                    self.mark_failed(&mut task, &error)?;
                    return Err(error);
                }
            };
            if let Err(error) = response.validate_for_request(&request) {
                self.mark_failed(&mut task, &error)?;
                return Err(error);
            }
            let provider_calls = response.provider_diagnostics.calls as u64;
            let rows = response.rows;
            let row_count = rows.row_count();
            match self
                .storage
                .write_rows_replacing_existing(StorageWriteRequest {
                    chain: &task.chain,
                    dataset_key: task.dataset_key.clone(),
                    selector: &task.selector,
                    range: chunk.clone(),
                    rows: &rows,
                    finality_level: task.finality.to_finality_level(),
                    record_empty_coverage: true,
                }) {
                Ok(outcome) => {
                    result.fetched_ranges += 1;
                    result.written_ranges +=
                        u64::from(outcome.data_object.is_some() || outcome.recorded_empty_coverage);
                    result.empty_ranges += u64::from(outcome.recorded_empty_coverage);
                    result.provider_calls += provider_calls;
                    result.rows_fetched += row_count;
                    task.stats.fetched_ranges += 1;
                    task.stats.written_ranges +=
                        u64::from(outcome.data_object.is_some() || outcome.recorded_empty_coverage);
                    task.stats.empty_ranges += u64::from(outcome.recorded_empty_coverage);
                    task.stats.provider_calls += provider_calls;
                    task.stats.rows_fetched += row_count;
                    task.touch(unix_seconds_now()?);
                    self.registry.save_task(&task)?;
                }
                Err(error) => {
                    self.mark_failed(&mut task, &error)?;
                    return Err(error);
                }
            }
            next = chunk.end().saturating_add(1);
        }

        task.state = CacheRepairTaskState::Completed;
        task.touch(unix_seconds_now()?);
        self.registry.save_task(&task)?;
        result.status = CacheRepairRunStatus::Completed;
        Ok(result)
    }

    fn mark_failed(
        &self,
        task: &mut CacheRepairTask,
        error: &DatalensError,
    ) -> Result<(), DatalensError> {
        task.state = CacheRepairTaskState::Failed;
        task.last_error = Some(error.message.clone());
        task.touch(unix_seconds_now()?);
        self.registry.save_task(task)
    }
}

#[derive(Serialize, Deserialize)]
struct StoredCacheRepairTask {
    task_id: CacheRepairTaskId,
    application_id: String,
    chain: ChainIdentity,
    dataset_key: DatasetKey,
    selector: StoredSelector,
    range_kind: LedgerRangeKind,
    start: u64,
    end: u64,
    finality: CacheRepairFinality,
    chunk_policy: CacheRepairChunkPolicy,
    reason: String,
    state: CacheRepairTaskState,
    created_at: u64,
    updated_at: u64,
    last_error: Option<String>,
    stats: CacheRepairStats,
    dedupe_key: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
enum StoredSelector {
    All,
    EvmLogs(EvmLogFilter),
    Other(StoredOtherSelector),
}

#[derive(Serialize, Deserialize)]
struct StoredOtherSelector {
    kind: String,
    fingerprint: String,
    canonical_key: String,
}

impl StoredCacheRepairTask {
    fn from_task(task: &CacheRepairTask) -> Result<Self, DatalensError> {
        let selector = match &task.selector {
            DatasetSelector::All => StoredSelector::All,
            DatasetSelector::EvmLogs(filter) => StoredSelector::EvmLogs(filter.clone()),
            DatasetSelector::Other {
                kind,
                fingerprint,
                canonical_key,
            } => StoredSelector::Other(StoredOtherSelector {
                kind: kind.as_str().to_owned(),
                fingerprint: fingerprint.clone(),
                canonical_key: canonical_key.clone(),
            }),
        };
        Ok(Self {
            task_id: task.task_id.clone(),
            application_id: task.application_id.clone(),
            chain: task.chain.clone(),
            dataset_key: task.dataset_key.clone(),
            selector,
            range_kind: task.range_kind.clone(),
            start: task.start,
            end: task.end,
            finality: task.finality,
            chunk_policy: task.chunk_policy.clone(),
            reason: task.reason.clone(),
            state: task.state,
            created_at: task.created_at,
            updated_at: task.updated_at,
            last_error: task.last_error.clone(),
            stats: task.stats.clone(),
            dedupe_key: task.dedupe_key.clone(),
        })
    }

    fn into_task(self) -> Result<CacheRepairTask, DatalensError> {
        let selector = match self.selector {
            StoredSelector::All => DatasetSelector::All,
            StoredSelector::EvmLogs(filter) => DatasetSelector::EvmLogs(filter),
            StoredSelector::Other(stored) => DatasetSelector::try_other(
                AdapterKey::try_new(stored.kind)?,
                stored.fingerprint,
                stored.canonical_key,
            )?,
        };
        Ok(CacheRepairTask {
            task_id: self.task_id,
            application_id: self.application_id,
            chain: self.chain,
            dataset_key: self.dataset_key,
            selector,
            range_kind: self.range_kind,
            start: self.start,
            end: self.end,
            finality: self.finality,
            chunk_policy: self.chunk_policy,
            reason: self.reason,
            state: self.state,
            created_at: self.created_at,
            updated_at: self.updated_at,
            last_error: self.last_error,
            stats: self.stats,
            dedupe_key: self.dedupe_key,
        })
    }
}

fn decode_task(bytes: &[u8]) -> Result<CacheRepairTask, DatalensError> {
    serde_json::from_slice::<StoredCacheRepairTask>(bytes)
        .map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode cache repair task: {error}"),
            )
        })?
        .into_task()
}

fn matches_filter(task: &CacheRepairTask, filter: &CacheRepairTaskFilter) -> bool {
    filter
        .application_id
        .as_ref()
        .is_none_or(|application_id| &task.application_id == application_id)
        && filter
            .chain_key
            .as_ref()
            .is_none_or(|chain_key| &task.chain.key_prefix() == chain_key)
        && filter.state.is_none_or(|state| task.state == state)
}

fn validate_submit(request: &CacheRepairSubmitRequest) -> Result<(), DatalensError> {
    if request.application_id.trim().is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "cache repair application id must not be empty",
        ));
    }
    if request.start > request.end {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "cache repair start must be less than or equal to end",
        ));
    }
    if request.reason.trim().is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "cache repair reason must not be empty",
        ));
    }
    Ok(())
}

fn validate_request<A: ChainAdapter>(
    request: &CacheRepairSubmitRequest,
    adapter: &A,
) -> Result<(), DatalensError> {
    validate_submit(request)?;
    let capabilities = adapter.capabilities();
    if capabilities.chain() != &request.chain {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "cache repair task chain does not match service chain",
        ));
    }
    if !capabilities.datasets().contains(&request.dataset_key) {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            format!(
                "dataset {} is not supported by this chain adapter",
                request.dataset_key.as_str()
            ),
        ));
    }
    if let Some(dataset) = capabilities.dataset(&request.dataset_key)
        && !dataset.supports_selector(request.selector.kind())
    {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "selector is not supported by this chain adapter",
        ));
    }
    Ok(())
}

fn validate_task<A: ChainAdapter>(
    task: &CacheRepairTask,
    adapter: &A,
) -> Result<(), DatalensError> {
    validate_request(
        &CacheRepairSubmitRequest {
            application_id: task.application_id.clone(),
            chain: task.chain.clone(),
            dataset_key: task.dataset_key.clone(),
            selector: task.selector.clone(),
            range_kind: task.range_kind.clone(),
            start: task.start,
            end: task.end,
            finality: task.finality,
            chunk_policy: task.chunk_policy.clone(),
            reason: task.reason.clone(),
        },
        adapter,
    )
}

fn task_dedupe_key(request: &CacheRepairSubmitRequest) -> String {
    format!(
        "application={};chain={};dataset={};selector={};range_kind={:?};start={};end={};finality={:?}",
        request.application_id.trim(),
        request.chain.key_prefix(),
        request.dataset_key.as_str(),
        request.selector.canonical_key(),
        request.range_kind,
        request.start,
        request.end,
        request.finality,
    )
}

fn task_key(task_id: &CacheRepairTaskId) -> String {
    format!("cache-repair/tasks/{}.json", task_id.as_str())
}

fn missing_task(task_id: &CacheRepairTaskId) -> DatalensError {
    DatalensError::new(
        DatalensErrorKind::InvalidInput,
        format!("cache repair task {} was not found", task_id.as_str()),
    )
}

fn unix_seconds_now() -> Result<u64, DatalensError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("system clock before unix epoch: {error}"),
            )
        })
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
