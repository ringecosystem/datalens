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
const PENDING_INDEX_PREFIX: &str = "durable-promotion-intents/v1/index/status=pending";
const QUERY_SOURCE_KEY: &str = "query";
const WARMUP_SOURCE_KEY: &str = "warmup";

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurablePromotionIntentBacklog {
    pub chain: ChainIdentity,
    pub source: DurablePromotionIntentSource,
    pub pending_total: usize,
    pub oldest_pending_age_seconds: u64,
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
    fn list_pending_for_chain(
        &self,
        chain: &ChainIdentity,
        now_unix_seconds: u64,
        limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError>;
    fn list_pending_for_chain_and_source(
        &self,
        chain: &ChainIdentity,
        source: DurablePromotionIntentSource,
        now_unix_seconds: u64,
        limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        Ok(self
            .list_pending_for_chain(chain, now_unix_seconds, limit)?
            .into_iter()
            .filter(|intent| intent.source == source)
            .collect())
    }
    fn rebuild_pending_indexes(&self, now_unix_seconds: u64) -> Result<usize, DatalensError> {
        let _ = now_unix_seconds;
        Ok(0)
    }
    fn pending_backlog_for_chain(
        &self,
        chain: &ChainIdentity,
        now_unix_seconds: u64,
    ) -> Result<Vec<DurablePromotionIntentBacklog>, DatalensError> {
        let _ = chain;
        let _ = now_unix_seconds;
        Ok(Vec::new())
    }
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

    fn list_pending_for_chain(
        &self,
        chain: &ChainIdentity,
        now_unix_seconds: u64,
        limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        self.as_ref()
            .list_pending_for_chain(chain, now_unix_seconds, limit)
    }

    fn list_pending_for_chain_and_source(
        &self,
        chain: &ChainIdentity,
        source: DurablePromotionIntentSource,
        now_unix_seconds: u64,
        limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        self.as_ref()
            .list_pending_for_chain_and_source(chain, source, now_unix_seconds, limit)
    }

    fn rebuild_pending_indexes(&self, now_unix_seconds: u64) -> Result<usize, DatalensError> {
        self.as_ref().rebuild_pending_indexes(now_unix_seconds)
    }

    fn pending_backlog_for_chain(
        &self,
        chain: &ChainIdentity,
        now_unix_seconds: u64,
    ) -> Result<Vec<DurablePromotionIntentBacklog>, DatalensError> {
        self.as_ref()
            .pending_backlog_for_chain(chain, now_unix_seconds)
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

    fn write_pending_index(&self, intent: &DurablePromotionIntent) -> Result<(), DatalensError> {
        let Some(key) = pending_index_key(intent) else {
            return Ok(());
        };
        self.object_store.put(&key, b"{}\n").map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!(
                    "write durable promotion intent pending index {key}: {}",
                    error.message
                ),
            )
        })
    }

    fn delete_pending_index(&self, intent: &DurablePromotionIntent) -> Result<(), DatalensError> {
        let Some(key) = pending_index_key(intent) else {
            return Ok(());
        };
        self.object_store.delete(&key).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!(
                    "delete durable promotion intent pending index {key}: {}",
                    error.message
                ),
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
        let previous = intent.clone();
        update(&mut intent);
        self.write_intent(&intent)?;
        self.delete_pending_index(&previous)?;
        self.write_pending_index(&intent)?;
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

    fn pending_index_entries_for_chain(
        &self,
        chain: &ChainIdentity,
        now_unix_seconds: u64,
    ) -> Result<Vec<PendingIndexEntry>, DatalensError> {
        let mut entries = Vec::new();
        for source in [QUERY_SOURCE_KEY, WARMUP_SOURCE_KEY] {
            entries.extend(self.pending_index_entries_for_chain_and_source(
                chain,
                source,
                now_unix_seconds,
            )?);
        }
        entries.sort_by_key(|entry| {
            (
                entry.due_at_unix_seconds,
                entry.intent_id.clone(),
                entry.key.clone(),
            )
        });
        Ok(entries)
    }

    fn pending_index_entries_for_chain_and_source(
        &self,
        chain: &ChainIdentity,
        source: &str,
        now_unix_seconds: u64,
    ) -> Result<Vec<PendingIndexEntry>, DatalensError> {
        let mut entries = Vec::new();
        let prefix = pending_index_prefix(chain, source);
        for object in self.object_store.list(&prefix)? {
            let Some(entry) = parse_pending_index_entry(&object.key) else {
                continue;
            };
            if entry.due_at_unix_seconds <= now_unix_seconds {
                entries.push(entry);
            }
        }
        entries.sort_by_key(|entry| {
            (
                entry.due_at_unix_seconds,
                entry.intent_id.clone(),
                entry.key.clone(),
            )
        });
        Ok(entries)
    }

    fn pending_index_backlog_for_source(
        &self,
        chain: &ChainIdentity,
        source: DurablePromotionIntentSource,
        now_unix_seconds: u64,
    ) -> Result<DurablePromotionIntentBacklog, DatalensError> {
        let prefix = pending_index_prefix(chain, source_key(source));
        let mut pending_total = 0;
        let mut oldest_due_at_unix_seconds = None;
        for object in self.object_store.list(&prefix)? {
            let Some(entry) = parse_pending_index_entry(&object.key) else {
                continue;
            };
            if entry.due_at_unix_seconds > now_unix_seconds {
                continue;
            }
            pending_total += 1;
            oldest_due_at_unix_seconds = Some(
                oldest_due_at_unix_seconds
                    .unwrap_or(entry.due_at_unix_seconds)
                    .min(entry.due_at_unix_seconds),
            );
        }
        Ok(DurablePromotionIntentBacklog {
            chain: chain.clone(),
            source,
            pending_total,
            oldest_pending_age_seconds: oldest_due_at_unix_seconds
                .map(|oldest| now_unix_seconds.saturating_sub(oldest))
                .unwrap_or(0),
        })
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
            self.write_pending_index(&existing)?;
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
        self.write_pending_index(&intent)?;
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

    fn list_pending_for_chain(
        &self,
        chain: &ChainIdentity,
        now_unix_seconds: u64,
        limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut pending = Vec::new();
        for entry in self.pending_index_entries_for_chain(chain, now_unix_seconds)? {
            let Some(intent) = self.get(&entry.intent_id)? else {
                let _ = self.object_store.delete(&entry.key);
                continue;
            };
            if &intent.chain == chain && intent_is_eligible_for_claim(&intent, now_unix_seconds) {
                pending.push(intent);
                if pending.len() >= limit {
                    break;
                }
            } else {
                let _ = self.object_store.delete(&entry.key);
            }
        }
        Ok(pending)
    }

    fn list_pending_for_chain_and_source(
        &self,
        chain: &ChainIdentity,
        source: DurablePromotionIntentSource,
        now_unix_seconds: u64,
        limit: usize,
    ) -> Result<Vec<DurablePromotionIntent>, DatalensError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut pending = Vec::new();
        for entry in self.pending_index_entries_for_chain_and_source(
            chain,
            source_key(source),
            now_unix_seconds,
        )? {
            let Some(intent) = self.get(&entry.intent_id)? else {
                let _ = self.object_store.delete(&entry.key);
                continue;
            };
            if &intent.chain == chain
                && intent.source == source
                && intent_is_eligible_for_claim(&intent, now_unix_seconds)
            {
                pending.push(intent);
                if pending.len() >= limit {
                    break;
                }
            } else {
                let _ = self.object_store.delete(&entry.key);
            }
        }
        Ok(pending)
    }

    fn rebuild_pending_indexes(&self, now_unix_seconds: u64) -> Result<usize, DatalensError> {
        let _ = now_unix_seconds;
        let mut rebuilt = 0;
        for intent in self.read_all_intents()? {
            if intent_is_indexable_for_claim(&intent) {
                self.write_pending_index(&intent)?;
                rebuilt += 1;
            }
        }
        Ok(rebuilt)
    }

    fn pending_backlog_for_chain(
        &self,
        chain: &ChainIdentity,
        now_unix_seconds: u64,
    ) -> Result<Vec<DurablePromotionIntentBacklog>, DatalensError> {
        let mut backlog = Vec::new();
        for source in [
            DurablePromotionIntentSource::Query,
            DurablePromotionIntentSource::Warmup,
        ] {
            backlog.push(self.pending_index_backlog_for_source(chain, source, now_unix_seconds)?);
        }
        Ok(backlog)
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
        let previous = intent.clone();
        intent.status = DurablePromotionIntentStatus::Running;
        intent.updated_at_unix_seconds = now_unix_seconds;
        intent.next_retry_at_unix_seconds = None;
        intent.last_error = None;
        self.write_intent(&intent)?;
        self.delete_pending_index(&previous)?;
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
            let previous = intent.clone();
            intent.status = DurablePromotionIntentStatus::Pending;
            intent.updated_at_unix_seconds = now_unix_seconds;
            intent.next_retry_at_unix_seconds = None;
            intent.last_error = None;
            self.write_intent(&intent)?;
            self.delete_pending_index(&previous)?;
            self.write_pending_index(&intent)?;
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingIndexEntry {
    key: String,
    intent_id: String,
    due_at_unix_seconds: u64,
}

fn pending_index_prefix(chain: &ChainIdentity, source: &str) -> String {
    format!(
        "{PENDING_INDEX_PREFIX}/chain={}/source={source}",
        chain.key_prefix()
    )
}

fn pending_index_key(intent: &DurablePromotionIntent) -> Option<String> {
    let due_at_unix_seconds = match intent.status {
        DurablePromotionIntentStatus::Pending => intent.created_at_unix_seconds,
        DurablePromotionIntentStatus::FailedRetryable => intent
            .next_retry_at_unix_seconds
            .unwrap_or(intent.created_at_unix_seconds),
        DurablePromotionIntentStatus::Running
        | DurablePromotionIntentStatus::Completed
        | DurablePromotionIntentStatus::FailedTerminal => return None,
    };
    Some(format!(
        "{}/created={:019}/intent={}.json",
        pending_index_prefix(&intent.chain, source_key(intent.source)),
        due_at_unix_seconds,
        intent.intent_id
    ))
}

fn parse_pending_index_entry(key: &str) -> Option<PendingIndexEntry> {
    let created = key
        .split('/')
        .find_map(|segment| segment.strip_prefix("created="))?;
    let due_at_unix_seconds = created.parse().ok()?;
    let intent_id = key
        .rsplit('/')
        .next()?
        .strip_prefix("intent=")?
        .strip_suffix(".json")?
        .to_owned();
    Some(PendingIndexEntry {
        key: key.to_owned(),
        intent_id,
        due_at_unix_seconds,
    })
}

fn source_key(source: DurablePromotionIntentSource) -> &'static str {
    match source {
        DurablePromotionIntentSource::Query => QUERY_SOURCE_KEY,
        DurablePromotionIntentSource::Warmup => WARMUP_SOURCE_KEY,
    }
}

fn intent_is_eligible_for_claim(intent: &DurablePromotionIntent, now_unix_seconds: u64) -> bool {
    match intent.status {
        DurablePromotionIntentStatus::Pending => true,
        DurablePromotionIntentStatus::FailedRetryable => intent
            .next_retry_at_unix_seconds
            .is_none_or(|next_retry| next_retry <= now_unix_seconds),
        DurablePromotionIntentStatus::Running
        | DurablePromotionIntentStatus::Completed
        | DurablePromotionIntentStatus::FailedTerminal => false,
    }
}

fn intent_is_indexable_for_claim(intent: &DurablePromotionIntent) -> bool {
    matches!(
        intent.status,
        DurablePromotionIntentStatus::Pending | DurablePromotionIntentStatus::FailedRetryable
    )
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
