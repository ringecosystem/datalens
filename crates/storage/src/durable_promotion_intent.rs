use datalens_chain::{AdapterKey, DatasetSelector, FinalityLevel};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, EvmLogFilter, LedgerRange,
    LedgerRangeKind,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::object_store::ObjectStore;

const INTENT_PREFIX: &str = "durable-promotion-intents/v1/intents";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurablePromotionIntentSource {
    Query,
    Warmup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DurablePromotionIntentStatus {
    Pending,
    Running,
    Completed,
    FailedRetryable,
    FailedTerminal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DurablePromotionIntent {
    pub intent_id: String,
    pub dedupe_key: String,
    pub source: DurablePromotionIntentSource,
    pub application: String,
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    #[serde(with = "stored_selector")]
    pub selector: DatasetSelector,
    pub selector_fingerprint: String,
    pub selector_canonical_key: String,
    pub finality: String,
    pub ranges: Vec<LedgerRange>,
    pub status: DurablePromotionIntentStatus,
    pub attempt_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at_unix_seconds: Option<u64>,
    pub created_at_unix_seconds: u64,
    pub updated_at_unix_seconds: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateDurablePromotionIntent {
    pub source: DurablePromotionIntentSource,
    pub application: String,
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector: DatasetSelector,
    pub selector_fingerprint: String,
    pub selector_canonical_key: String,
    pub finality: String,
    pub ranges: Vec<LedgerRange>,
    pub request_id: Option<String>,
    pub task_id: Option<String>,
    pub now_unix_seconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurablePromotionIntentCreateOutcome {
    Created(DurablePromotionIntent),
    Existing(DurablePromotionIntent),
}

pub trait DurablePromotionIntentRepository: Send + Sync {
    fn create_or_get(
        &self,
        request: CreateDurablePromotionIntent,
    ) -> Result<DurablePromotionIntentCreateOutcome, DatalensError>;
    fn get(&self, intent_id: &str) -> Result<Option<DurablePromotionIntent>, DatalensError>;
    fn list_pending(
        &self,
        now_unix_seconds: u64,
        limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError>;
    fn mark_running(
        &self,
        intent_id: &str,
        now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError>;
    fn mark_completed(
        &self,
        intent_id: &str,
        now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError>;
    fn mark_retryable_failure(
        &self,
        intent_id: &str,
        error: &str,
        now_unix_seconds: u64,
        next_retry_at_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError>;
    fn mark_terminal_failure(
        &self,
        intent_id: &str,
        error: &str,
        now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError>;
    fn reset_stale_running(
        &self,
        stale_before_unix_seconds: u64,
        now_unix_seconds: u64,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError>;
}

impl DurablePromotionIntentRepository for Arc<dyn DurablePromotionIntentRepository> {
    fn create_or_get(
        &self,
        request: CreateDurablePromotionIntent,
    ) -> Result<DurablePromotionIntentCreateOutcome, DatalensError> {
        self.as_ref().create_or_get(request)
    }

    fn get(&self, intent_id: &str) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        self.as_ref().get(intent_id)
    }

    fn list_pending(
        &self,
        now_unix_seconds: u64,
        limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        self.as_ref().list_pending(now_unix_seconds, limit)
    }

    fn mark_running(
        &self,
        intent_id: &str,
        now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        self.as_ref().mark_running(intent_id, now_unix_seconds)
    }

    fn mark_completed(
        &self,
        intent_id: &str,
        now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        self.as_ref().mark_completed(intent_id, now_unix_seconds)
    }

    fn mark_retryable_failure(
        &self,
        intent_id: &str,
        error: &str,
        now_unix_seconds: u64,
        next_retry_at_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        self.as_ref().mark_retryable_failure(
            intent_id,
            error,
            now_unix_seconds,
            next_retry_at_unix_seconds,
        )
    }

    fn mark_terminal_failure(
        &self,
        intent_id: &str,
        error: &str,
        now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        self.as_ref()
            .mark_terminal_failure(intent_id, error, now_unix_seconds)
    }

    fn reset_stale_running(
        &self,
        stale_before_unix_seconds: u64,
        now_unix_seconds: u64,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        self.as_ref()
            .reset_stale_running(stale_before_unix_seconds, now_unix_seconds)
    }
}

#[derive(Clone, Debug)]
pub struct DurablePromotionIntentStore<S> {
    object_store: S,
}

impl<S> DurablePromotionIntentStore<S>
where
    S: ObjectStore + 'static,
{
    pub fn new(object_store: S) -> Self {
        Self { object_store }
    }

    pub fn object_store(&self) -> &S {
        &self.object_store
    }

    fn read_intent_key(&self, key: &str) -> Result<DurablePromotionIntent, DatalensError> {
        let bytes = self.object_store.get(key)?;
        serde_json::from_slice(&bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode durable promotion intent {key}: {error}"),
            )
        })
    }

    fn write_intent(&self, intent: &DurablePromotionIntent) -> Result<(), DatalensError> {
        let key = intent_object_key(&intent.intent_id);
        let bytes = serde_json::to_vec_pretty(intent).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode durable promotion intent: {error}"),
            )
        })?;
        self.object_store.put(&key, &bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!("write durable promotion intent {key}: {}", error.message),
            )
        })
    }

    fn update_intent(
        &self,
        intent_id: &str,
        update: impl FnOnce(&mut DurablePromotionIntent),
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        let Some(mut intent) = self.get(intent_id)? else {
            return Ok(None);
        };
        update(&mut intent);
        self.write_intent(&intent)?;
        Ok(Some(intent))
    }

    fn read_all_intents(&self) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        let mut intents = Vec::new();
        for object in self.object_store.list(INTENT_PREFIX)? {
            if object.key.ends_with(".json") {
                intents.push(self.read_intent_key(&object.key)?);
            }
        }
        intents.sort_by_key(|intent| {
            (
                intent.created_at_unix_seconds,
                intent.updated_at_unix_seconds,
                intent.intent_id.clone(),
            )
        });
        Ok(intents)
    }
}

impl<S> DurablePromotionIntentRepository for DurablePromotionIntentStore<S>
where
    S: ObjectStore + 'static,
{
    fn create_or_get(
        &self,
        request: CreateDurablePromotionIntent,
    ) -> Result<DurablePromotionIntentCreateOutcome, DatalensError> {
        if request.application.trim().is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "durable promotion intent application must not be empty",
            ));
        }
        if request.selector_fingerprint.trim().is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "durable promotion intent selector fingerprint must not be empty",
            ));
        }
        if request.selector_canonical_key.trim().is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "durable promotion intent selector canonical key must not be empty",
            ));
        }
        if request.finality.trim().is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "durable promotion intent finality must not be empty",
            ));
        }
        if !is_durable_finality(&request.finality) {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "durable promotion intent finality must be safe or finalized",
            ));
        }
        if request.ranges.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "durable promotion intent ranges must not be empty",
            ));
        }

        let ranges = normalize_ranges(request.ranges);
        let dedupe_key = durable_coverage_dedupe_key(
            &request.chain,
            &request.dataset_key,
            &request.selector_fingerprint,
            &request.selector_canonical_key,
            &request.finality,
            &ranges,
        );
        let intent_id = intent_id_for_dedupe_key(&dedupe_key);
        if let Some(existing) = self.get(&intent_id)? {
            return Ok(DurablePromotionIntentCreateOutcome::Existing(existing));
        }

        let intent = DurablePromotionIntent {
            intent_id,
            dedupe_key,
            source: request.source,
            application: request.application.trim().to_owned(),
            chain: request.chain,
            dataset_key: request.dataset_key,
            selector: request.selector,
            selector_fingerprint: request.selector_fingerprint,
            selector_canonical_key: request.selector_canonical_key,
            finality: request.finality,
            ranges,
            status: DurablePromotionIntentStatus::Pending,
            attempt_count: 0,
            next_retry_at_unix_seconds: None,
            created_at_unix_seconds: request.now_unix_seconds,
            updated_at_unix_seconds: request.now_unix_seconds,
            last_error: None,
            request_id: request.request_id,
            task_id: request.task_id,
        };
        self.write_intent(&intent)?;
        Ok(DurablePromotionIntentCreateOutcome::Created(intent))
    }

    fn get(&self, intent_id: &str) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        let key = intent_object_key(intent_id);
        if !self.object_store.exists(&key)? {
            return Ok(None);
        }
        self.read_intent_key(&key).map(Some)
    }

    fn list_pending(
        &self,
        now_unix_seconds: u64,
        limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut pending = self
            .read_all_intents()?
            .into_iter()
            .filter(|intent| match intent.status {
                DurablePromotionIntentStatus::Pending => true,
                DurablePromotionIntentStatus::FailedRetryable => intent
                    .next_retry_at_unix_seconds
                    .is_none_or(|next_retry| next_retry <= now_unix_seconds),
                DurablePromotionIntentStatus::Running
                | DurablePromotionIntentStatus::Completed
                | DurablePromotionIntentStatus::FailedTerminal => false,
            })
            .collect::<Vec<_>>();
        pending.sort_by_key(|intent| {
            (
                intent
                    .next_retry_at_unix_seconds
                    .unwrap_or(intent.created_at_unix_seconds),
                intent.created_at_unix_seconds,
                intent.intent_id.clone(),
            )
        });
        pending.truncate(limit);
        Ok(pending)
    }

    fn mark_running(
        &self,
        intent_id: &str,
        now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        let Some(mut intent) = self.get(intent_id)? else {
            return Ok(None);
        };
        let eligible = match intent.status {
            DurablePromotionIntentStatus::Pending => true,
            DurablePromotionIntentStatus::FailedRetryable => intent
                .next_retry_at_unix_seconds
                .is_none_or(|next_retry| next_retry <= now_unix_seconds),
            DurablePromotionIntentStatus::Running
            | DurablePromotionIntentStatus::Completed
            | DurablePromotionIntentStatus::FailedTerminal => false,
        };
        if !eligible {
            return Ok(None);
        }
        intent.status = DurablePromotionIntentStatus::Running;
        intent.updated_at_unix_seconds = now_unix_seconds;
        intent.next_retry_at_unix_seconds = None;
        intent.last_error = None;
        self.write_intent(&intent)?;
        Ok(Some(intent))
    }

    fn mark_completed(
        &self,
        intent_id: &str,
        now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        self.update_intent(intent_id, |intent| {
            intent.status = DurablePromotionIntentStatus::Completed;
            intent.updated_at_unix_seconds = now_unix_seconds;
            intent.next_retry_at_unix_seconds = None;
            intent.last_error = None;
        })
    }

    fn mark_retryable_failure(
        &self,
        intent_id: &str,
        error: &str,
        now_unix_seconds: u64,
        next_retry_at_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        let error = error.to_owned();
        self.update_intent(intent_id, |intent| {
            intent.status = DurablePromotionIntentStatus::FailedRetryable;
            intent.attempt_count = intent.attempt_count.saturating_add(1);
            intent.updated_at_unix_seconds = now_unix_seconds;
            intent.next_retry_at_unix_seconds = Some(next_retry_at_unix_seconds);
            intent.last_error = Some(error);
        })
    }

    fn mark_terminal_failure(
        &self,
        intent_id: &str,
        error: &str,
        now_unix_seconds: u64,
    ) -> Result<Option<DurablePromotionIntent>, DatalensError> {
        let error = error.to_owned();
        self.update_intent(intent_id, |intent| {
            intent.status = DurablePromotionIntentStatus::FailedTerminal;
            intent.attempt_count = intent.attempt_count.saturating_add(1);
            intent.updated_at_unix_seconds = now_unix_seconds;
            intent.next_retry_at_unix_seconds = None;
            intent.last_error = Some(error);
        })
    }

    fn reset_stale_running(
        &self,
        stale_before_unix_seconds: u64,
        now_unix_seconds: u64,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        let mut reset = Vec::new();
        for mut intent in self.read_all_intents()?.into_iter().filter(|intent| {
            intent.status == DurablePromotionIntentStatus::Running
                && intent.updated_at_unix_seconds <= stale_before_unix_seconds
        }) {
            intent.status = DurablePromotionIntentStatus::Pending;
            intent.updated_at_unix_seconds = now_unix_seconds;
            intent.next_retry_at_unix_seconds = None;
            intent.last_error = None;
            self.write_intent(&intent)?;
            reset.push(intent);
        }
        Ok(reset)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableIntentSubmissionRequest {
    pub source: DurablePromotionIntentSource,
    pub application: String,
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector: DatasetSelector,
    pub finality: FinalityLevel,
    pub ranges: Vec<LedgerRange>,
    pub request_id: Option<String>,
    pub task_id: Option<String>,
    pub now_unix_seconds: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableIntentSubmissionOutcome {
    Submitted(DurablePromotionIntent),
    AlreadyPending(DurablePromotionIntent),
    AlreadyCompleted(DurablePromotionIntent),
    Failed(DatalensError),
}

#[derive(Clone, Debug)]
pub struct DurableIntentSubmissionService<R> {
    repository: R,
}

impl<R> DurableIntentSubmissionService<R>
where
    R: DurablePromotionIntentRepository,
{
    pub fn new(repository: R) -> Self {
        Self { repository }
    }

    pub fn repository(&self) -> &R {
        &self.repository
    }

    pub fn submit(
        &self,
        request: DurableIntentSubmissionRequest,
    ) -> DurableIntentSubmissionOutcome {
        if !request.finality.is_durable_writable() {
            return DurableIntentSubmissionOutcome::Failed(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "durable promotion intent finality must be safe or finalized",
            ));
        }
        let create = CreateDurablePromotionIntent {
            source: request.source,
            application: request.application,
            chain: request.chain,
            dataset_key: request.dataset_key,
            selector: request.selector.clone(),
            selector_fingerprint: request.selector.fingerprint(),
            selector_canonical_key: request.selector.canonical_key(),
            finality: finality_name(request.finality).to_owned(),
            ranges: request.ranges,
            request_id: request.request_id,
            task_id: request.task_id,
            now_unix_seconds: request.now_unix_seconds,
        };
        match self.repository.create_or_get(create) {
            Ok(DurablePromotionIntentCreateOutcome::Created(intent)) => {
                DurableIntentSubmissionOutcome::Submitted(intent)
            }
            Ok(DurablePromotionIntentCreateOutcome::Existing(intent)) => match intent.status {
                DurablePromotionIntentStatus::Completed => {
                    DurableIntentSubmissionOutcome::AlreadyCompleted(intent)
                }
                DurablePromotionIntentStatus::FailedTerminal => {
                    DurableIntentSubmissionOutcome::Failed(DatalensError::new(
                        DatalensErrorKind::Internal,
                        format!(
                            "durable promotion intent {} already failed terminally",
                            intent.intent_id
                        ),
                    ))
                }
                DurablePromotionIntentStatus::Pending
                | DurablePromotionIntentStatus::Running
                | DurablePromotionIntentStatus::FailedRetryable => {
                    DurableIntentSubmissionOutcome::AlreadyPending(intent)
                }
            },
            Err(error) => DurableIntentSubmissionOutcome::Failed(error),
        }
    }
}

fn intent_object_key(intent_id: &str) -> String {
    format!("{INTENT_PREFIX}/{intent_id}.json")
}

fn intent_id_for_dedupe_key(dedupe_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(dedupe_key.as_bytes());
    hex_bytes(&hasher.finalize())
}

fn durable_coverage_dedupe_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector_fingerprint: &str,
    selector_canonical_key: &str,
    finality: &str,
    ranges: &[LedgerRange],
) -> String {
    let ranges = ranges
        .iter()
        .map(|range| {
            format!(
                "{}:{}-{}",
                range_kind_key(range.kind()),
                range.start(),
                range.end()
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "coverage:v1|chain={}|dataset={}|selector_fingerprint={}|selector_canonical_key={}|finality={}|ranges={}",
        chain.key_prefix(),
        dataset_key.as_str(),
        selector_fingerprint,
        selector_canonical_key,
        finality,
        ranges
    )
}

fn normalize_ranges(mut ranges: Vec<LedgerRange>) -> Vec<LedgerRange> {
    ranges.sort_by_key(|range| (range_kind_key(range.kind()), range.start(), range.end()));
    let mut normalized: Vec<LedgerRange> = Vec::new();
    for range in ranges {
        let Some(last) = normalized.last_mut() else {
            normalized.push(range);
            continue;
        };
        if last.kind() == range.kind() && range.start() <= last.end().saturating_add(1) {
            let end = last.end().max(range.end());
            *last = LedgerRange::try_new(last.kind(), last.start(), end)
                .expect("merged ledger range remains valid");
        } else {
            normalized.push(range);
        }
    }
    normalized
}

fn range_kind_key(kind: LedgerRangeKind) -> String {
    match kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(value) => value,
    }
}

fn finality_name(finality: FinalityLevel) -> &'static str {
    match finality {
        FinalityLevel::Latest => "latest",
        FinalityLevel::Safe => "safe",
        FinalityLevel::Finalized => "finalized",
        FinalityLevel::ChainSpecific(value) => value,
    }
}

fn is_durable_finality(finality: &str) -> bool {
    matches!(finality, "safe" | "finalized")
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push_str(&format!("{byte:02x}"));
    }
    value
}

mod stored_selector {
    use super::*;

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

    pub fn serialize<S>(selector: &DatasetSelector, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let stored = match selector {
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
        stored.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DatasetSelector, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let stored = StoredSelector::deserialize(deserializer)?;
        match stored {
            StoredSelector::All => Ok(DatasetSelector::All),
            StoredSelector::EvmLogs(filter) => Ok(DatasetSelector::EvmLogs(filter)),
            StoredSelector::Other(selector) => DatasetSelector::try_other(
                AdapterKey::try_new(selector.kind).map_err(serde::de::Error::custom)?,
                selector.fingerprint,
                selector.canonical_key,
            )
            .map_err(serde::de::Error::custom),
        }
    }
}
