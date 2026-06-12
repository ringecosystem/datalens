use datalens_chain::{AdapterKey, DatasetSelector};
use datalens_core::{DatalensError, DatalensErrorKind, EvmLogFilter};
use datalens_storage::ObjectStore;
use serde::{Deserialize, Serialize};

use crate::{
    WarmupCursor, WarmupFollowQueryStatus, WarmupSubmitRequest, WarmupTask, WarmupTaskId,
    WarmupTaskMode, WarmupTaskState,
    task::{task_dedupe_key, task_ensure_key, task_ensure_key_for_task},
};

pub trait WarmupRegistry: Clone + Send + Sync + 'static {
    fn submit(&self, request: WarmupSubmitRequest) -> Result<WarmupSubmitOutcome, DatalensError>;
    fn ensure(&self, request: WarmupSubmitRequest) -> Result<WarmupEnsureOutcome, DatalensError>;
    fn get(&self, task_id: &WarmupTaskId) -> Result<Option<WarmupTask>, DatalensError>;
    fn save_task(&self, task: &WarmupTask) -> Result<(), DatalensError>;
    fn list(&self, filter: WarmupTaskFilter) -> Result<Vec<WarmupTask>, DatalensError>;
    fn load_cursor(&self, task_id: &WarmupTaskId) -> Result<Option<WarmupCursor>, DatalensError>;
    fn save_cursor(&self, cursor: &WarmupCursor) -> Result<(), DatalensError>;

    fn pause(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        self.set_state(task_id, WarmupTaskState::Paused)
    }

    fn cancel(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        self.set_state(task_id, WarmupTaskState::Cancelled)
    }

    fn retry_failed(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        let mut task = self.get(task_id)?.ok_or_else(|| missing_task(task_id))?;
        task.reset_for_retry(unix_seconds_now()?)?;
        self.save_task(&task)
    }

    fn set_state(
        &self,
        task_id: &WarmupTaskId,
        state: WarmupTaskState,
    ) -> Result<(), DatalensError> {
        let mut task = self.get(task_id)?.ok_or_else(|| missing_task(task_id))?;
        task.state = state;
        task.touch(unix_seconds_now()?);
        self.save_task(&task)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WarmupSubmitOutcome {
    pub task_id: WarmupTaskId,
    pub created: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WarmupEnsureOutcome {
    pub task_id: WarmupTaskId,
    pub created: bool,
    pub state: WarmupTaskState,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WarmupTaskFilter {
    pub application_id: Option<String>,
    pub chain_key: Option<String>,
    pub state: Option<WarmupTaskState>,
}

#[derive(Clone, Debug)]
pub struct LocalWarmupRegistry<S> {
    object_store: S,
}

impl<S> LocalWarmupRegistry<S>
where
    S: ObjectStore + 'static,
{
    pub fn new(object_store: S) -> Self {
        Self { object_store }
    }

    pub fn object_store(&self) -> &S {
        &self.object_store
    }

    pub fn submit(
        &self,
        request: WarmupSubmitRequest,
    ) -> Result<WarmupSubmitOutcome, DatalensError> {
        <Self as WarmupRegistry>::submit(self, request)
    }

    pub fn ensure(
        &self,
        request: WarmupSubmitRequest,
    ) -> Result<WarmupEnsureOutcome, DatalensError> {
        <Self as WarmupRegistry>::ensure(self, request)
    }

    pub fn get(&self, task_id: &WarmupTaskId) -> Result<Option<WarmupTask>, DatalensError> {
        <Self as WarmupRegistry>::get(self, task_id)
    }

    pub fn save_task(&self, task: &WarmupTask) -> Result<(), DatalensError> {
        <Self as WarmupRegistry>::save_task(self, task)
    }

    pub fn list(&self, filter: WarmupTaskFilter) -> Result<Vec<WarmupTask>, DatalensError> {
        <Self as WarmupRegistry>::list(self, filter)
    }

    pub fn load_cursor(
        &self,
        task_id: &WarmupTaskId,
    ) -> Result<Option<WarmupCursor>, DatalensError> {
        <Self as WarmupRegistry>::load_cursor(self, task_id)
    }

    pub fn save_cursor(&self, cursor: &WarmupCursor) -> Result<(), DatalensError> {
        <Self as WarmupRegistry>::save_cursor(self, cursor)
    }

    pub fn pause(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        <Self as WarmupRegistry>::pause(self, task_id)
    }

    pub fn cancel(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        <Self as WarmupRegistry>::cancel(self, task_id)
    }

    pub fn retry_failed(&self, task_id: &WarmupTaskId) -> Result<(), DatalensError> {
        <Self as WarmupRegistry>::retry_failed(self, task_id)
    }
}

impl<S> WarmupRegistry for LocalWarmupRegistry<S>
where
    S: ObjectStore + 'static,
{
    fn submit(&self, request: WarmupSubmitRequest) -> Result<WarmupSubmitOutcome, DatalensError> {
        if request.mode == WarmupTaskMode::FollowQuery {
            let outcome = self.ensure(request)?;
            return Ok(WarmupSubmitOutcome {
                task_id: outcome.task_id,
                created: outcome.created,
            });
        }

        let dedupe_key = task_dedupe_key(&request);
        if let Some(existing) = self
            .list(WarmupTaskFilter::default())?
            .into_iter()
            .find(|task| task.dedupe_key == dedupe_key)
        {
            return Ok(WarmupSubmitOutcome {
                task_id: existing.task_id,
                created: false,
            });
        }

        let task = WarmupTask::from_submit(request, unix_seconds_now()?)?;
        let task_id = task.task_id.clone();
        self.save_task(&task)?;
        self.save_cursor(&WarmupCursor::new(
            task.task_id.clone(),
            task.start,
            task.created_at,
        ))?;
        Ok(WarmupSubmitOutcome {
            task_id,
            created: true,
        })
    }

    fn ensure(&self, request: WarmupSubmitRequest) -> Result<WarmupEnsureOutcome, DatalensError> {
        if request.mode != WarmupTaskMode::FollowQuery {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "warmup ensure requires follow_query mode",
            ));
        }
        let ensure_key = task_ensure_key(&request);
        if let Some(mut existing) = self
            .list(WarmupTaskFilter::default())?
            .into_iter()
            .filter(|task| task.mode == WarmupTaskMode::FollowQuery)
            .find(|task| task_ensure_key_for_task(task) == ensure_key)
        {
            if !is_scheduler_runnable(existing.state) {
                existing.state = WarmupTaskState::Queued;
                existing.last_error = None;
                existing.touch(unix_seconds_now()?);
                self.save_task(&existing)?;
            }
            return Ok(WarmupEnsureOutcome {
                task_id: existing.task_id,
                created: false,
                state: existing.state,
            });
        }

        let task = WarmupTask::from_ensure(request, unix_seconds_now()?)?;
        let task_id = task.task_id.clone();
        let state = task.state;
        self.save_task(&task)?;
        self.save_cursor(&WarmupCursor::new(
            task.task_id.clone(),
            task.start,
            task.created_at,
        ))?;
        Ok(WarmupEnsureOutcome {
            task_id,
            created: true,
            state,
        })
    }

    fn get(&self, task_id: &WarmupTaskId) -> Result<Option<WarmupTask>, DatalensError> {
        let key = task_key(task_id);
        if !self.object_store.exists(&key)? {
            return Ok(None);
        }
        decode_task(&self.object_store.get(&key)?).map(Some)
    }

    fn save_task(&self, task: &WarmupTask) -> Result<(), DatalensError> {
        let bytes =
            serde_json::to_vec_pretty(&StoredWarmupTask::from_task(task)?).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("encode warmup task: {error}"),
                )
            })?;
        self.object_store.put(&task_key(&task.task_id), &bytes)
    }

    fn list(&self, filter: WarmupTaskFilter) -> Result<Vec<WarmupTask>, DatalensError> {
        let mut tasks = Vec::new();
        for object in self.object_store.list("warmup/tasks")? {
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

    fn load_cursor(&self, task_id: &WarmupTaskId) -> Result<Option<WarmupCursor>, DatalensError> {
        let key = cursor_key(task_id);
        if !self.object_store.exists(&key)? {
            return Ok(None);
        }
        serde_json::from_slice(&self.object_store.get(&key)?)
            .map(Some)
            .map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("decode warmup cursor {key}: {error}"),
                )
            })
    }

    fn save_cursor(&self, cursor: &WarmupCursor) -> Result<(), DatalensError> {
        let bytes = serde_json::to_vec_pretty(cursor).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode warmup cursor: {error}"),
            )
        })?;
        self.object_store.put(&cursor_key(&cursor.task_id), &bytes)
    }
}

#[derive(Serialize, Deserialize)]
struct StoredWarmupTask {
    task_id: WarmupTaskId,
    application_id: String,
    chain: datalens_core::ChainIdentity,
    dataset_key: datalens_core::DatasetKey,
    selector: StoredSelector,
    range_kind: datalens_core::LedgerRangeKind,
    start: u64,
    end: Option<u64>,
    mode: WarmupTaskMode,
    chunk_policy: crate::WarmupChunkPolicy,
    retry_policy: crate::WarmupRetryPolicy,
    state: WarmupTaskState,
    created_at: u64,
    updated_at: u64,
    last_error: Option<String>,
    stats: crate::WarmupStats,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    follow_query_status: Option<WarmupFollowQueryStatus>,
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

impl StoredWarmupTask {
    fn from_task(task: &WarmupTask) -> Result<Self, DatalensError> {
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
            mode: task.mode,
            chunk_policy: task.chunk_policy.clone(),
            retry_policy: task.retry_policy.clone(),
            state: task.state,
            created_at: task.created_at,
            updated_at: task.updated_at,
            last_error: task.last_error.clone(),
            stats: task.stats.clone(),
            follow_query_status: task.follow_query_status.clone(),
            dedupe_key: task.dedupe_key.clone(),
        })
    }

    fn into_task(self) -> Result<WarmupTask, DatalensError> {
        let selector = match self.selector {
            StoredSelector::All => DatasetSelector::All,
            StoredSelector::EvmLogs(filter) => DatasetSelector::EvmLogs(filter),
            StoredSelector::Other(selector) => DatasetSelector::try_other(
                AdapterKey::try_new(selector.kind)?,
                selector.fingerprint,
                selector.canonical_key,
            )?,
        };
        Ok(WarmupTask {
            task_id: self.task_id,
            application_id: self.application_id,
            chain: self.chain,
            dataset_key: self.dataset_key,
            selector,
            range_kind: self.range_kind,
            start: self.start,
            end: self.end,
            mode: self.mode,
            chunk_policy: self.chunk_policy,
            retry_policy: self.retry_policy,
            state: self.state,
            created_at: self.created_at,
            updated_at: self.updated_at,
            last_error: self.last_error,
            stats: self.stats,
            follow_query_status: self.follow_query_status,
            dedupe_key: self.dedupe_key,
        })
    }
}

pub(crate) fn unix_seconds_now() -> Result<u64, DatalensError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| {
            DatalensError::internal(format!("system clock before unix epoch: {error}"))
        })
}

fn task_key(task_id: &WarmupTaskId) -> String {
    format!("warmup/tasks/{}.json", task_id.as_str())
}

fn cursor_key(task_id: &WarmupTaskId) -> String {
    format!("warmup/cursors/{}.json", task_id.as_str())
}

fn decode_task(bytes: &[u8]) -> Result<WarmupTask, DatalensError> {
    serde_json::from_slice::<StoredWarmupTask>(bytes)
        .map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode warmup task: {error}"),
            )
        })?
        .into_task()
        .map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode warmup task: {error}"),
            )
        })
}

fn matches_filter(task: &WarmupTask, filter: &WarmupTaskFilter) -> bool {
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

fn is_scheduler_runnable(state: WarmupTaskState) -> bool {
    matches!(state, WarmupTaskState::Queued | WarmupTaskState::Running)
}

fn missing_task(task_id: &WarmupTaskId) -> DatalensError {
    DatalensError::new(
        DatalensErrorKind::InvalidInput,
        format!("warmup task {} not found", task_id.as_str()),
    )
}
