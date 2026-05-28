use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange, LedgerRangeKind,
};
use serde::{Deserialize, Serialize};

use crate::object_store::ObjectStore;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryOutcome {
    Requested,
    Hit,
    HotHit,
    Miss,
    HotMiss,
    PartialHit,
    Mixed,
    Filled,
    Empty,
    Denied,
    ReorgRollback,
    PromotionCompleted,
    PromotionSkipped,
    ProviderError,
    StorageError,
    Error,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CacheOutcome {
    Hit,
    HotHit,
    PartialHit,
    Mixed,
    Miss,
    HotMiss,
    Empty,
    Error,
    NotChecked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FillOutcome {
    NotAttempted,
    Written,
    LiveFetch,
    EmptyCoverageRecorded,
    ReorgRollback,
    PromotionWritten,
    PromotionSkipped,
    ProviderError,
    StorageError,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UsageLedgerEntry {
    pub application_id: String,
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector_fingerprint: String,
    pub selector_canonical_key: String,
    pub range: LedgerRange,
    pub finality: String,
    pub requested_hot: bool,
    pub query_outcome: QueryOutcome,
    pub cache_outcome: CacheOutcome,
    pub fill_outcome: FillOutcome,
    pub row_count: usize,
    pub timestamp_unix_seconds: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
}

impl UsageLedgerEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn query_event(
        application_id: impl Into<String>,
        chain: ChainIdentity,
        dataset_key: DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
        finality: FinalityLevel,
        query_outcome: QueryOutcome,
        cache_outcome: CacheOutcome,
        fill_outcome: FillOutcome,
        row_count: usize,
    ) -> Self {
        Self {
            application_id: application_id.into(),
            chain,
            dataset_key,
            selector_fingerprint: selector.fingerprint(),
            selector_canonical_key: selector.canonical_key(),
            range,
            finality: finality_name(finality).to_owned(),
            requested_hot: matches!(finality, FinalityLevel::Latest),
            query_outcome,
            cache_outcome,
            fill_outcome,
            row_count,
            timestamp_unix_seconds: unix_seconds_now().unwrap_or_default(),
            request_id: None,
            trace_id: None,
        }
    }

    pub fn with_requested_hot(mut self, requested_hot: bool) -> Self {
        self.requested_hot = requested_hot;
        self
    }

    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    pub fn with_trace_id(mut self, trace_id: impl Into<String>) -> Self {
        self.trace_id = Some(trace_id.into());
        self
    }
}

pub trait UsageLedgerRepository: Send + Sync {
    fn append(&self, entry: &UsageLedgerEntry) -> Result<(), DatalensError>;
    fn read_application(
        &self,
        application_id: &str,
    ) -> Result<Vec<UsageLedgerEntry>, DatalensError>;
}

#[derive(Clone, Debug)]
pub struct UsageLedgerStore<S> {
    object_store: S,
}

impl<S> UsageLedgerStore<S>
where
    S: ObjectStore,
{
    pub fn new(object_store: S) -> Self {
        Self { object_store }
    }

    pub fn object_store(&self) -> &S {
        &self.object_store
    }
}

impl<S> UsageLedgerRepository for UsageLedgerStore<S>
where
    S: ObjectStore + 'static,
{
    fn append(&self, entry: &UsageLedgerEntry) -> Result<(), DatalensError> {
        let key = ledger_object_key(entry)?;
        let mut bytes = if self.object_store.exists(&key)? {
            self.object_store.get(&key)?
        } else {
            Vec::new()
        };
        if !bytes.is_empty() && !bytes.ends_with(b"\n") {
            bytes.push(b'\n');
        }
        serde_json::to_writer(&mut bytes, entry).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode usage ledger entry: {error}"),
            )
        })?;
        bytes.push(b'\n');
        self.object_store.put(&key, &bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!("write usage ledger {key}: {}", error.message),
            )
        })
    }

    fn read_application(
        &self,
        application_id: &str,
    ) -> Result<Vec<UsageLedgerEntry>, DatalensError> {
        let prefix = format!("usage/applications/{}", application_key(application_id));
        let mut entries = Vec::new();
        for object in self.object_store.list(&prefix)? {
            let bytes = self.object_store.get(&object.key)?;
            let text = std::str::from_utf8(&bytes).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("decode usage ledger {} as utf-8: {error}", object.key),
                )
            })?;
            for line in text.lines().filter(|line| !line.trim().is_empty()) {
                let entry = serde_json::from_str::<UsageLedgerEntry>(line).map_err(|error| {
                    DatalensError::new(
                        DatalensErrorKind::StorageReadFailure,
                        format!("decode usage ledger {}: {error}", object.key),
                    )
                })?;
                entries.push(entry);
            }
        }
        entries.sort_by_key(|entry| {
            (
                entry.timestamp_unix_seconds,
                entry.chain.key_prefix(),
                entry.dataset_key.as_str().to_owned(),
                entry.range.start(),
                entry.range.end(),
            )
        });
        Ok(entries)
    }
}

fn ledger_object_key(entry: &UsageLedgerEntry) -> Result<String, DatalensError> {
    let day = entry.timestamp_unix_seconds / 86_400;
    Ok(format!(
        "usage/applications/{}/chains/{}/datasets/{}/range-kind/{}/days/{day}.jsonl",
        application_key(&entry.application_id),
        entry.chain.key_prefix(),
        dataset_key_segment(entry.dataset_key.as_str()),
        range_kind_key(entry.range.kind()),
    ))
}

fn application_key(application_id: &str) -> String {
    hex_key(application_id)
}

fn dataset_key_segment(dataset_key: &str) -> String {
    hex_key(dataset_key)
}

fn hex_key(value: &str) -> String {
    let mut key = String::with_capacity(4 + value.len() * 2);
    key.push_str("hex-");
    for byte in value.as_bytes() {
        key.push_str(&format!("{byte:02x}"));
    }
    key
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

fn unix_seconds_now() -> Result<u64, DatalensError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| {
            DatalensError::internal(format!("system clock before unix epoch: {error}"))
        })
}
