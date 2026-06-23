//! Application-scoped durable cache repair tasks.

use std::{collections::BTreeMap, sync::mpsc, thread, time::Duration};

use datalens_chain::{
    AdapterKey, ChainAdapter, ChainFetchRequest, ChainFetchResponse, DatasetSelector, FetchContext,
    FinalityLevel, validate_durable_range,
};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, EvmLogFilter,
    LedgerRange, LedgerRangeKind, LogRecord, QueryRows,
};
use datalens_storage::{ObjectStore, StorageRepository, StorageWriteRequest};
use serde::{Deserialize, Serialize};

const CACHE_REPAIR_PHASE_IDLE: &str = "idle";
const CACHE_REPAIR_PHASE_HEIGHT: &str = "height";
const CACHE_REPAIR_PHASE_FETCH: &str = "fetch";
const CACHE_REPAIR_PHASE_WRITE: &str = "write";
const CACHE_REPAIR_PHASE_COMPLETED: &str = "completed";

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
    WriteTimedOut,
    Cancelled,
}

impl CacheRepairTaskState {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheRepairRuntimeConfig {
    /// Per-operation timeout for cache repair height, fetch, and write phases.
    #[serde(default = "default_cache_repair_fetch_timeout_ms")]
    pub fetch_timeout_ms: u64,
    #[serde(default = "default_cache_repair_lease_ttl_ms")]
    pub lease_ttl_ms: u64,
}

impl Default for CacheRepairRuntimeConfig {
    fn default() -> Self {
        Self {
            fetch_timeout_ms: default_cache_repair_fetch_timeout_ms(),
            lease_ttl_ms: default_cache_repair_lease_ttl_ms(),
        }
    }
}

pub fn default_cache_repair_fetch_timeout_ms() -> u64 {
    120_000
}

pub fn default_cache_repair_lease_ttl_ms() -> u64 {
    600_000
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
    pub source_selectors: Vec<DatasetSelector>,
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
    pub source_selectors: Vec<DatasetSelector>,
    pub range_kind: LedgerRangeKind,
    pub start: u64,
    pub end: u64,
    pub finality: CacheRepairFinality,
    pub chunk_policy: CacheRepairChunkPolicy,
    pub reason: String,
    pub state: CacheRepairTaskState,
    pub created_at: u64,
    pub updated_at: u64,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<u64>,
    pub last_error: Option<String>,
    pub current_phase: Option<String>,
    pub current_range_start: Option<u64>,
    pub current_range_end: Option<u64>,
    pub current_source_index: Option<usize>,
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
            source_selectors: request.source_selectors,
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
            lease_owner: None,
            lease_expires_at: None,
            last_error: None,
            current_phase: Some(CACHE_REPAIR_PHASE_IDLE.to_owned()),
            current_range_start: None,
            current_range_end: None,
            current_source_index: None,
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
        self.current_phase = Some(CACHE_REPAIR_PHASE_IDLE.to_owned());
        self.current_range_start = None;
        self.current_range_end = None;
        self.current_source_index = None;
        self.touch(now);
        Ok(())
    }

    fn runnable_for_worker(&self, now_ms: u64) -> bool {
        match self.state {
            CacheRepairTaskState::Queued | CacheRepairTaskState::Failed => true,
            CacheRepairTaskState::Running => self.lease_expired(now_ms),
            CacheRepairTaskState::Completed
            | CacheRepairTaskState::WriteTimedOut
            | CacheRepairTaskState::Cancelled => false,
        }
    }

    fn lease_expired(&self, now_ms: u64) -> bool {
        self.lease_expires_at
            .is_none_or(|expires_at| expires_at <= now_ms)
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
        task.lease_owner = None;
        task.lease_expires_at = None;
        self.save_task(&task)
    }

    fn set_state(
        &self,
        task_id: &CacheRepairTaskId,
        state: CacheRepairTaskState,
    ) -> Result<(), DatalensError> {
        let mut task = self.get(task_id)?.ok_or_else(|| missing_task(task_id))?;
        task.state = state;
        if state.is_terminal() {
            task.lease_owner = None;
            task.lease_expires_at = None;
        }
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

    pub fn migrate_legacy_paths(&self) -> Result<RegistryMigrationReport, DatalensError> {
        let mut report = RegistryMigrationReport::default();
        migrate_prefix(
            &self.object_store,
            LEGACY_TASK_PREFIX,
            TASK_PREFIX,
            &mut report.tasks,
        )?;
        Ok(report)
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
        for key in [task_key(task_id), legacy_task_key(task_id)] {
            if self.object_store.exists(&key)? {
                return decode_task(&self.object_store.get(&key)?).map(Some);
            }
        }
        Ok(None)
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
        let mut tasks = BTreeMap::new();
        for object in self.object_store.list(TASK_PREFIX)? {
            if object.key.ends_with(".json") {
                let Some(task_id) = object_id_from_key(&object.key, TASK_PREFIX) else {
                    continue;
                };
                let task = decode_task(&self.object_store.get(&object.key)?)?;
                if matches_filter(&task, &filter) {
                    tasks.insert(task_id, task);
                }
            }
        }
        for object in self.object_store.list(LEGACY_TASK_PREFIX)? {
            if object.key.ends_with(".json") {
                let Some(task_id) = object_id_from_key(&object.key, LEGACY_TASK_PREFIX) else {
                    continue;
                };
                if tasks.contains_key(&task_id) {
                    continue;
                }
                let task = decode_task(&self.object_store.get(&object.key)?)?;
                if matches_filter(&task, &filter) {
                    tasks.insert(task_id, task);
                }
            }
        }
        let tasks = tasks.into_values().collect();
        Ok(tasks)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RegistryMigrationReport {
    pub tasks: RegistryMigrationSectionReport,
}

impl RegistryMigrationReport {
    pub fn total_problems(&self) -> u64 {
        self.tasks.conflicts + self.tasks.failed
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RegistryMigrationSectionReport {
    pub copied: u64,
    pub skipped: u64,
    pub conflicts: u64,
    pub failed: u64,
    pub failures: Vec<RegistryMigrationFailure>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RegistryMigrationFailure {
    pub legacy_key: String,
    pub clean_key: String,
    pub message: String,
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
    config: CacheRepairRuntimeConfig,
    lease_owner: String,
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
        let now_ms = unix_milliseconds_now()?;
        let mut results = Vec::new();
        let mut tasks = self
            .list(CacheRepairTaskFilter::default())?
            .into_iter()
            .filter(|task| task.runnable_for_worker(now_ms))
            .collect::<Vec<_>>();
        tasks.sort_by_key(|task| (task.created_at, task.task_id.as_str().to_owned()));
        for task in tasks.into_iter().take(1) {
            results.push(self.runtime.run_task_once(&task.task_id)?);
        }
        Ok(results)
    }

    pub fn run_task_once(
        &self,
        task_id: &CacheRepairTaskId,
    ) -> Result<CacheRepairRunResult, DatalensError> {
        self.runtime.run_task_once(task_id)
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
            config: CacheRepairRuntimeConfig::default(),
            lease_owner: default_lease_owner(),
        }
    }

    pub fn with_runtime_config(mut self, config: CacheRepairRuntimeConfig) -> Self {
        self.config = config;
        self
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
        if task.state == CacheRepairTaskState::WriteTimedOut {
            return Ok(CacheRepairRunResult {
                status: CacheRepairRunStatus::Stopped,
                ..CacheRepairRunResult::default()
            });
        }
        let now_ms = unix_milliseconds_now()?;
        if task.state == CacheRepairTaskState::Running && !task.lease_expired(now_ms) {
            return Ok(CacheRepairRunResult {
                status: CacheRepairRunStatus::Stopped,
                ..CacheRepairRunResult::default()
            });
        }
        if task.state == CacheRepairTaskState::Running {
            log::warn!(
                "cache repair stale running task recovered task_id={} lease_expires_at={} now={}",
                task.task_id.as_str(),
                task.lease_expires_at.unwrap_or_default(),
                now_ms,
            );
        }

        validate_task(&task, &self.adapter)?;
        log::info!(
            "cache repair task started task_id={} chain={} dataset={} range={}-{} selector_fingerprint={} source_selector_count={}",
            task.task_id.as_str(),
            task.chain.key_prefix(),
            task.dataset_key.as_str(),
            task.start,
            task.end,
            task.selector.fingerprint(),
            task.source_selectors.len(),
        );
        task.state = CacheRepairTaskState::Running;
        task.last_error = None;
        self.set_phase(&mut task, CACHE_REPAIR_PHASE_HEIGHT, None, None)?;
        self.extend_lease(&mut task)?;
        self.registry.save_task(&task)?;

        log::info!(
            "cache repair height lookup started task_id={} finality={:?}",
            task.task_id.as_str(),
            task.finality,
        );
        let height_start = std::time::Instant::now();
        let durable_height = match self.height_with_timeout(task.finality) {
            Ok(durable_height) => {
                log::info!(
                    "cache repair height lookup completed task_id={} height={} duration_ms={}",
                    task.task_id.as_str(),
                    durable_height.value,
                    height_start.elapsed().as_millis(),
                );
                durable_height
            }
            Err(error) => {
                let error = error.with_cache_repair_context(&task);
                self.mark_failed(&mut task, &error)?;
                return Err(error);
            }
        };
        if let Err(error) = durable_height.validate_durable_writable() {
            let error = error.with_cache_repair_context(&task);
            self.mark_failed(&mut task, &error)?;
            return Err(error);
        }
        if durable_height.range_kind != task.range_kind {
            let error = DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "cache repair task range kind does not match adapter durable height",
            )
            .with_cache_repair_context(&task);
            self.mark_failed(&mut task, &error)?;
            return Err(error);
        }
        let finalized_height = if task.finality == CacheRepairFinality::Safe {
            match self.height_with_timeout(CacheRepairFinality::Finalized) {
                Ok(finalized_height) => {
                    if let Err(error) = finalized_height.validate_durable_writable() {
                        log::debug!(
                            "cache repair finalized height skipped task_id={} kind={:?} message={}",
                            task.task_id.as_str(),
                            error.kind,
                            error.message,
                        );
                        None
                    } else if finalized_height.range_kind != task.range_kind {
                        log::debug!(
                            "cache repair finalized height skipped task_id={} task_range_kind={:?} finalized_range_kind={:?}",
                            task.task_id.as_str(),
                            task.range_kind,
                            finalized_height.range_kind,
                        );
                        None
                    } else {
                        Some(finalized_height)
                    }
                }
                Err(error) => {
                    log::debug!(
                        "cache repair finalized height unavailable task_id={} kind={:?} message={}",
                        task.task_id.as_str(),
                        error.kind,
                        error.message,
                    );
                    None
                }
            }
        } else {
            None
        };

        let mut result = CacheRepairRunResult::default();
        let mut next = task.start;
        while next <= task.end {
            let chunk = LedgerRange::try_new(
                task.range_kind.clone(),
                next,
                task.end
                    .min(next.saturating_add(task.chunk_policy.max_range_len.max(1) - 1)),
            )?;
            if let Err(error) = validate_durable_range(&chunk, &durable_height) {
                let error = error.with_cache_repair_context(&task);
                self.mark_failed(&mut task, &error)?;
                return Err(error);
            }
            let fetched = match self.fetch_repair_rows(&mut task, chunk.clone()) {
                Ok(fetched) => fetched,
                Err(error) => {
                    self.mark_failed(&mut task, &error)?;
                    return Err(error);
                }
            };
            let provider_calls = fetched.provider_calls;
            let rows = fetched.rows;
            let row_count = rows.row_count();
            let write_start = std::time::Instant::now();
            self.set_phase(&mut task, CACHE_REPAIR_PHASE_WRITE, Some(&chunk), None)?;
            self.registry.save_task(&task)?;
            let write_finalities =
                repair_write_finalities(&task, &chunk, finalized_height.as_ref());
            log::info!(
                "cache repair replacement write started task_id={} target_selector_fingerprint={} range={}-{} rows={} finalities={:?}",
                task.task_id.as_str(),
                task.selector.fingerprint(),
                chunk.start(),
                chunk.end(),
                row_count,
                write_finalities,
            );
            match self.write_rows_replacing_existing_with_timeout(
                &task,
                chunk.clone(),
                rows,
                write_finalities,
            ) {
                Ok(outcome) => {
                    log::info!(
                        "cache repair replacement write completed task_id={} range={}-{} data_object={} empty_coverage={} duration_ms={}",
                        task.task_id.as_str(),
                        chunk.start(),
                        chunk.end(),
                        outcome.data_object.is_some(),
                        outcome.recorded_empty_coverage,
                        write_start.elapsed().as_millis(),
                    );
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
                    self.extend_lease(&mut task)?;
                    self.registry.save_task(&task)?;
                }
                Err(error) => {
                    let error = error.with_cache_repair_context(&task);
                    if is_write_timeout_error(&error) {
                        self.mark_write_timed_out(&mut task, &error)?;
                    } else {
                        self.mark_failed(&mut task, &error)?;
                    }
                    return Err(error);
                }
            }
            next = chunk.end().saturating_add(1);
        }

        task.state = CacheRepairTaskState::Completed;
        task.lease_owner = None;
        task.lease_expires_at = None;
        self.set_phase(&mut task, CACHE_REPAIR_PHASE_COMPLETED, None, None)?;
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
        task.lease_owner = None;
        task.lease_expires_at = None;
        task.last_error = Some(error.message.clone());
        log::warn!(
            "cache repair task failed task_id={} phase={} range={}-{} source_index={} kind={:?} message={}",
            task.task_id.as_str(),
            task.current_phase
                .as_deref()
                .unwrap_or(CACHE_REPAIR_PHASE_IDLE),
            task.current_range_start
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            task.current_range_end
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            task.current_source_index
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            error.kind,
            error.message,
        );
        task.touch(unix_seconds_now()?);
        self.registry.save_task(task)
    }

    fn mark_write_timed_out(
        &self,
        task: &mut CacheRepairTask,
        error: &DatalensError,
    ) -> Result<(), DatalensError> {
        task.state = CacheRepairTaskState::WriteTimedOut;
        task.last_error = Some(error.message.clone());
        log::warn!(
            "cache repair replacement write timed out task_id={} phase={} range={}-{} lease_owner={} lease_expires_at={} kind={:?} message={}",
            task.task_id.as_str(),
            task.current_phase
                .as_deref()
                .unwrap_or(CACHE_REPAIR_PHASE_IDLE),
            task.current_range_start
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            task.current_range_end
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            task.lease_owner.as_deref().unwrap_or("none"),
            task.lease_expires_at
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
            error.kind,
            error.message,
        );
        task.touch(unix_seconds_now()?);
        self.registry.save_task(task)
    }

    fn extend_lease(&self, task: &mut CacheRepairTask) -> Result<(), DatalensError> {
        let now_ms = unix_milliseconds_now()?;
        task.lease_owner = Some(self.lease_owner.clone());
        task.lease_expires_at = Some(now_ms.saturating_add(self.config.lease_ttl_ms));
        task.touch(unix_seconds_now()?);
        Ok(())
    }

    fn set_phase(
        &self,
        task: &mut CacheRepairTask,
        phase: &str,
        range: Option<&LedgerRange>,
        source_index: Option<usize>,
    ) -> Result<(), DatalensError> {
        task.current_phase = Some(phase.to_owned());
        task.current_range_start = range.map(LedgerRange::start);
        task.current_range_end = range.map(LedgerRange::end);
        task.current_source_index = source_index;
        task.touch(unix_seconds_now()?);
        Ok(())
    }

    fn fetch_repair_rows(
        &self,
        task: &mut CacheRepairTask,
        chunk: LedgerRange,
    ) -> Result<FetchedRepairRows, DatalensError> {
        if task.source_selectors.is_empty() {
            let selector = task.selector.clone();
            return self.fetch_one_selector(task, &selector, chunk, None);
        }

        let mut provider_calls = 0;
        let mut merged = QueryRows::EvmLogs(Vec::new());
        let source_selectors = task.source_selectors.clone();
        for (source_index, source_selector) in source_selectors.iter().enumerate() {
            let fetched =
                self.fetch_one_selector(task, source_selector, chunk.clone(), Some(source_index))?;
            provider_calls += fetched.provider_calls;
            merged.try_append(fetched.rows.into_rows())?;
        }
        let rows = filter_repair_rows_for_target(
            DatasetRows::new(task.dataset_key.clone(), dedupe_repair_rows(merged)?)?,
            &task.selector,
        )?;
        Ok(FetchedRepairRows {
            provider_calls,
            rows,
        })
    }

    fn fetch_one_selector(
        &self,
        task: &mut CacheRepairTask,
        selector: &DatasetSelector,
        chunk: LedgerRange,
        source_index: Option<usize>,
    ) -> Result<FetchedRepairRows, DatalensError> {
        self.set_phase(task, CACHE_REPAIR_PHASE_FETCH, Some(&chunk), source_index)?;
        self.registry.save_task(task)?;
        let request = ChainFetchRequest::new(
            task.chain.clone(),
            task.dataset_key.clone(),
            chunk.clone(),
            selector.clone(),
        )
        .with_context(FetchContext {
            request_id: Some(task.task_id.as_str().to_owned()),
            cache_write: true,
        });
        log::info!(
            "cache repair chunk fetch started task_id={} range={}-{} source_index={}",
            task.task_id.as_str(),
            chunk.start(),
            chunk.end(),
            source_index
                .map(|index| index.to_string())
                .unwrap_or_else(|| "target".to_owned()),
        );
        let fetch_start = std::time::Instant::now();
        let response = self
            .fetch_with_timeout(request.clone())
            .map_err(|error| error.with_cache_repair_context(task))?;
        response
            .validate_for_request(&request)
            .map_err(|error| error.with_cache_repair_context(task))?;
        let provider_calls = response.provider_diagnostics.calls as u64;
        let rows = response.rows;
        let row_count = rows.row_count();
        log::info!(
            "cache repair chunk fetch completed task_id={} range={}-{} rows={} provider_calls={} duration_ms={}",
            task.task_id.as_str(),
            chunk.start(),
            chunk.end(),
            row_count,
            provider_calls,
            fetch_start.elapsed().as_millis(),
        );
        Ok(FetchedRepairRows {
            provider_calls,
            rows,
        })
    }

    fn fetch_with_timeout(
        &self,
        request: ChainFetchRequest,
    ) -> Result<ChainFetchResponse, DatalensError> {
        let adapter = self.adapter.clone();
        self.run_with_operation_timeout(CACHE_REPAIR_PHASE_FETCH, move || adapter.fetch(request))
    }

    fn height_with_timeout(
        &self,
        finality: CacheRepairFinality,
    ) -> Result<datalens_chain::ChainHeight, DatalensError> {
        let adapter = self.adapter.clone();
        self.run_with_operation_timeout(CACHE_REPAIR_PHASE_HEIGHT, move || match finality {
            CacheRepairFinality::Safe => adapter.cache_safe_height(),
            CacheRepairFinality::Finalized => adapter.finalized_height(),
        })
    }

    fn write_rows_replacing_existing_with_timeout(
        &self,
        task: &CacheRepairTask,
        range: LedgerRange,
        rows: DatasetRows,
        finality_levels: Vec<FinalityLevel>,
    ) -> Result<datalens_storage::StorageWriteOutcome, DatalensError> {
        let storage = self.storage.clone();
        let chain = task.chain.clone();
        let dataset_key = task.dataset_key.clone();
        let selector = task.selector.clone();
        self.run_with_operation_timeout(CACHE_REPAIR_PHASE_WRITE, move || {
            let mut outcome = None;
            for finality_level in finality_levels {
                outcome = Some(storage.write_rows_replacing_existing(StorageWriteRequest {
                    chain: &chain,
                    dataset_key: dataset_key.clone(),
                    selector: &selector,
                    range: range.clone(),
                    rows: &rows,
                    finality_level,
                    record_empty_coverage: true,
                })?);
            }
            outcome.ok_or_else(|| DatalensError::internal("cache repair has no write finalities"))
        })
    }

    fn run_with_operation_timeout<T, F>(
        &self,
        phase: &'static str,
        operation: F,
    ) -> Result<T, DatalensError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, DatalensError> + Send + 'static,
    {
        if self.config.fetch_timeout_ms == 0 {
            return operation();
        }
        let timeout = Duration::from_millis(self.config.fetch_timeout_ms);
        let timeout_ms = self.config.fetch_timeout_ms;
        let (sender, receiver) = mpsc::channel();
        thread::spawn(move || {
            let _ = sender.send(operation());
        });
        match receiver.recv_timeout(timeout) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => Err(DatalensError::new(
                timeout_kind_for_phase(phase),
                format!("cache repair {phase} operation timed out after {timeout_ms}ms"),
            )),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(DatalensError::new(
                timeout_kind_for_phase(phase),
                format!("cache repair {phase} operation worker stopped"),
            )),
        }
    }
}

fn repair_write_finalities(
    task: &CacheRepairTask,
    range: &LedgerRange,
    finalized_height: Option<&datalens_chain::ChainHeight>,
) -> Vec<FinalityLevel> {
    let mut finalities = vec![task.finality.to_finality_level()];
    if task.finality == CacheRepairFinality::Safe
        && finalized_height
            .and_then(|height| validate_durable_range(range, height).ok())
            .is_some()
    {
        finalities.push(FinalityLevel::Finalized);
    }
    finalities
}

trait CacheRepairErrorContext {
    fn with_cache_repair_context(self, task: &CacheRepairTask) -> Self;
}

impl CacheRepairErrorContext for DatalensError {
    fn with_cache_repair_context(self, task: &CacheRepairTask) -> Self {
        let phase = task
            .current_phase
            .as_deref()
            .unwrap_or(CACHE_REPAIR_PHASE_IDLE);
        let range = task
            .current_range_start
            .zip(task.current_range_end)
            .map(|(start, end)| format!(" range={start}-{end}"))
            .unwrap_or_default();
        let source_index = task
            .current_source_index
            .map(|index| format!(" source_index={index}"))
            .unwrap_or_default();
        DatalensError::new(
            self.kind,
            format!(
                "cache repair {phase}{range}{source_index} failed: {}",
                self.message
            ),
        )
    }
}

fn timeout_kind_for_phase(phase: &str) -> DatalensErrorKind {
    match phase {
        CACHE_REPAIR_PHASE_WRITE => DatalensErrorKind::Internal,
        _ => DatalensErrorKind::ProviderFailure,
    }
}

fn is_write_timeout_error(error: &DatalensError) -> bool {
    error.kind == DatalensErrorKind::Internal
        && error.message.contains("write")
        && error.message.contains("timed out")
}

struct FetchedRepairRows {
    provider_calls: u64,
    rows: DatasetRows,
}

#[derive(Serialize, Deserialize)]
struct StoredCacheRepairTask {
    task_id: CacheRepairTaskId,
    application_id: String,
    chain: ChainIdentity,
    dataset_key: DatasetKey,
    selector: StoredSelector,
    #[serde(default)]
    source_selectors: Vec<StoredSelector>,
    range_kind: LedgerRangeKind,
    start: u64,
    end: u64,
    finality: CacheRepairFinality,
    chunk_policy: CacheRepairChunkPolicy,
    reason: String,
    state: CacheRepairTaskState,
    created_at: u64,
    updated_at: u64,
    #[serde(default)]
    lease_owner: Option<String>,
    #[serde(default)]
    lease_expires_at: Option<u64>,
    last_error: Option<String>,
    #[serde(default)]
    current_phase: Option<String>,
    #[serde(default)]
    current_range_start: Option<u64>,
    #[serde(default)]
    current_range_end: Option<u64>,
    #[serde(default)]
    current_source_index: Option<usize>,
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
        let source_selectors = task
            .source_selectors
            .iter()
            .map(stored_selector_from_selector)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            task_id: task.task_id.clone(),
            application_id: task.application_id.clone(),
            chain: task.chain.clone(),
            dataset_key: task.dataset_key.clone(),
            selector,
            source_selectors,
            range_kind: task.range_kind.clone(),
            start: task.start,
            end: task.end,
            finality: task.finality,
            chunk_policy: task.chunk_policy.clone(),
            reason: task.reason.clone(),
            state: task.state,
            created_at: task.created_at,
            updated_at: task.updated_at,
            lease_owner: task.lease_owner.clone(),
            lease_expires_at: task.lease_expires_at,
            last_error: task.last_error.clone(),
            current_phase: task.current_phase.clone(),
            current_range_start: task.current_range_start,
            current_range_end: task.current_range_end,
            current_source_index: task.current_source_index,
            stats: task.stats.clone(),
            dedupe_key: task.dedupe_key.clone(),
        })
    }

    fn into_task(self) -> Result<CacheRepairTask, DatalensError> {
        let selector = selector_from_stored_selector(self.selector)?;
        let source_selectors = self
            .source_selectors
            .into_iter()
            .map(selector_from_stored_selector)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(CacheRepairTask {
            task_id: self.task_id,
            application_id: self.application_id,
            chain: self.chain,
            dataset_key: self.dataset_key,
            selector,
            source_selectors,
            range_kind: self.range_kind,
            start: self.start,
            end: self.end,
            finality: self.finality,
            chunk_policy: self.chunk_policy,
            reason: self.reason,
            state: self.state,
            created_at: self.created_at,
            updated_at: self.updated_at,
            lease_owner: self.lease_owner,
            lease_expires_at: self.lease_expires_at,
            last_error: self.last_error,
            current_phase: self.current_phase,
            current_range_start: self.current_range_start,
            current_range_end: self.current_range_end,
            current_source_index: self.current_source_index,
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
    validate_source_selectors(&request.selector, &request.source_selectors)?;
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
            source_selectors: task.source_selectors.clone(),
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
    let source_selectors = request
        .source_selectors
        .iter()
        .map(DatasetSelector::canonical_key)
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "application={};chain={};dataset={};selector={};source_selectors={};range_kind={:?};start={};end={};finality={:?}",
        request.application_id.trim(),
        request.chain.key_prefix(),
        request.dataset_key.as_str(),
        request.selector.canonical_key(),
        source_selectors,
        request.range_kind,
        request.start,
        request.end,
        request.finality,
    )
}

fn validate_source_selectors(
    target: &DatasetSelector,
    source_selectors: &[DatasetSelector],
) -> Result<(), DatalensError> {
    if source_selectors.is_empty() {
        return Ok(());
    }
    for source in source_selectors {
        match (target, source) {
            (DatasetSelector::EvmLogs(_), DatasetSelector::EvmLogs(_)) => {
                if !target.covers(source) && !source.covers(target) {
                    return Err(DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        "cache repair source selector must be compatible with target selector",
                    ));
                }
            }
            _ => {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "cache repair source selectors are only supported for EVM log selectors",
                ));
            }
        }
    }
    Ok(())
}

fn filter_repair_rows_for_target(
    rows: DatasetRows,
    target: &DatasetSelector,
) -> Result<DatasetRows, DatalensError> {
    let DatasetSelector::EvmLogs(filter) = target else {
        return Ok(rows);
    };
    if rows.dataset_key() != &DatasetKey::evm_logs() {
        return Ok(rows);
    }
    let QueryRows::EvmLogs(logs) = rows.into_rows() else {
        return Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "cache repair source selectors are only supported for EVM log rows",
        ));
    };
    DatasetRows::new(
        DatasetKey::evm_logs(),
        QueryRows::EvmLogs(
            logs.into_iter()
                .filter(|row| filter.matches_log(row))
                .collect(),
        ),
    )
}

fn stored_selector_from_selector(
    selector: &DatasetSelector,
) -> Result<StoredSelector, DatalensError> {
    Ok(match selector {
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
    })
}

fn selector_from_stored_selector(
    selector: StoredSelector,
) -> Result<DatasetSelector, DatalensError> {
    match selector {
        StoredSelector::All => Ok(DatasetSelector::All),
        StoredSelector::EvmLogs(filter) => Ok(DatasetSelector::EvmLogs(filter)),
        StoredSelector::Other(stored) => DatasetSelector::try_other(
            AdapterKey::try_new(stored.kind)?,
            stored.fingerprint,
            stored.canonical_key,
        ),
    }
}

fn dedupe_repair_rows(mut rows: QueryRows) -> Result<QueryRows, DatalensError> {
    match &mut rows {
        QueryRows::EvmLogs(logs) => {
            logs.sort_by_key(|row| (row.block_number, row.transaction_index, row.log_index));
            let mut unique = BTreeMap::<(String, u64), LogRecord>::new();
            for log in logs.drain(..) {
                unique
                    .entry((log.transaction_hash.clone(), log.log_index))
                    .or_insert(log);
            }
            *logs = unique.into_values().collect();
            logs.sort_by_key(|row| (row.block_number, row.transaction_index, row.log_index));
            Ok(rows)
        }
        _ => Err(DatalensError::new(
            DatalensErrorKind::UnsupportedDataset,
            "cache repair source selectors are only supported for EVM log rows",
        )),
    }
}

fn missing_task(task_id: &CacheRepairTaskId) -> DatalensError {
    DatalensError::new(
        DatalensErrorKind::InvalidInput,
        format!("cache repair task {} was not found", task_id.as_str()),
    )
}

const TASK_PREFIX: &str = "tasks";
const LEGACY_TASK_PREFIX: &str = "cache-repair/tasks";

fn task_key(task_id: &CacheRepairTaskId) -> String {
    format!("{TASK_PREFIX}/{}.json", task_id.as_str())
}

fn legacy_task_key(task_id: &CacheRepairTaskId) -> String {
    format!("{LEGACY_TASK_PREFIX}/{}.json", task_id.as_str())
}

fn object_id_from_key(key: &str, prefix: &str) -> Option<String> {
    key.strip_prefix(&format!("{prefix}/"))?
        .strip_suffix(".json")
        .map(ToOwned::to_owned)
}

fn migrate_prefix<S>(
    object_store: &S,
    legacy_prefix: &str,
    clean_prefix: &str,
    report: &mut RegistryMigrationSectionReport,
) -> Result<(), DatalensError>
where
    S: ObjectStore + 'static,
{
    for object in object_store.list(legacy_prefix)? {
        if !object.key.ends_with(".json") {
            continue;
        }
        let Some(name) = object.key.strip_prefix(&format!("{legacy_prefix}/")) else {
            continue;
        };
        let clean_key = format!("{clean_prefix}/{name}");
        migrate_object(object_store, &object.key, &clean_key, report);
    }
    Ok(())
}

fn migrate_object<S>(
    object_store: &S,
    legacy_key: &str,
    clean_key: &str,
    report: &mut RegistryMigrationSectionReport,
) where
    S: ObjectStore + 'static,
{
    let legacy_bytes = match object_store.get(legacy_key) {
        Ok(bytes) => bytes,
        Err(error) => {
            report_failure(report, legacy_key, clean_key, error.message);
            return;
        }
    };
    match object_store.exists(clean_key) {
        Ok(true) => match object_store.get(clean_key) {
            Ok(clean_bytes) if clean_bytes == legacy_bytes => {
                report.skipped += 1;
            }
            Ok(clean_bytes) => {
                if clean_object_is_newer(&legacy_bytes, &clean_bytes) {
                    report.skipped += 1;
                } else {
                    report_conflict(
                        report,
                        legacy_key,
                        clean_key,
                        "clean object already exists with different content",
                    );
                }
            }
            Err(error) => {
                report_failure(report, legacy_key, clean_key, error.message);
            }
        },
        Ok(false) => {
            if let Err(error) = object_store.put(clean_key, &legacy_bytes) {
                report_failure(report, legacy_key, clean_key, error.message);
                return;
            }
            match object_store.get(clean_key) {
                Ok(clean_bytes) if clean_bytes == legacy_bytes => {
                    report.copied += 1;
                }
                Ok(_) => {
                    report_failure(
                        report,
                        legacy_key,
                        clean_key,
                        "copied object content did not match legacy object",
                    );
                }
                Err(error) => {
                    report_failure(report, legacy_key, clean_key, error.message);
                }
            }
        }
        Err(error) => {
            report_failure(report, legacy_key, clean_key, error.message);
        }
    }
}

fn clean_object_is_newer(legacy_bytes: &[u8], clean_bytes: &[u8]) -> bool {
    fn updated_at(bytes: &[u8]) -> Option<u64> {
        serde_json::from_slice::<serde_json::Value>(bytes)
            .ok()?
            .get("updated_at")?
            .as_u64()
    }

    match (updated_at(legacy_bytes), updated_at(clean_bytes)) {
        (Some(legacy), Some(clean)) => clean > legacy,
        _ => false,
    }
}

fn report_failure(
    report: &mut RegistryMigrationSectionReport,
    legacy_key: &str,
    clean_key: &str,
    message: impl Into<String>,
) {
    report.failed += 1;
    report.failures.push(RegistryMigrationFailure {
        legacy_key: legacy_key.to_owned(),
        clean_key: clean_key.to_owned(),
        message: message.into(),
    });
}

fn report_conflict(
    report: &mut RegistryMigrationSectionReport,
    legacy_key: &str,
    clean_key: &str,
    message: impl Into<String>,
) {
    report.conflicts += 1;
    report.failures.push(RegistryMigrationFailure {
        legacy_key: legacy_key.to_owned(),
        clean_key: clean_key.to_owned(),
        message: message.into(),
    });
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

fn unix_milliseconds_now() -> Result<u64, DatalensError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("system clock before unix epoch: {error}"),
            )
        })
}

fn default_lease_owner() -> String {
    format!("pid-{}", std::process::id())
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
