use std::sync::{Arc, Mutex};

use datalens_chain::DatasetSelector;
use datalens_core::{ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRangeKind};
use serde::{Deserialize, Serialize};

use crate::object_store::ObjectStore;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryWatermarkKey {
    pub application_id: String,
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector_fingerprint: String,
    pub selector_canonical_key: String,
    pub range_kind: LedgerRangeKind,
}

impl QueryWatermarkKey {
    pub fn new(
        application_id: impl Into<String>,
        chain: ChainIdentity,
        dataset_key: DatasetKey,
        selector: &DatasetSelector,
        range_kind: LedgerRangeKind,
    ) -> Self {
        Self {
            application_id: application_id.into(),
            chain,
            dataset_key,
            selector_fingerprint: selector.fingerprint(),
            selector_canonical_key: selector.canonical_key(),
            range_kind,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryWatermark {
    pub key: QueryWatermarkKey,
    pub latest_block: u64,
    pub updated_at_unix_seconds: u64,
}

pub trait QueryWatermarkRepository: Send + Sync {
    fn update(&self, watermark: &QueryWatermark) -> Result<(), DatalensError>;
    fn read(&self, key: &QueryWatermarkKey) -> Result<Option<QueryWatermark>, DatalensError>;
}

#[derive(Clone, Debug)]
pub struct QueryWatermarkStore<S> {
    object_store: S,
    update_lock: Arc<Mutex<()>>,
}

impl<S> QueryWatermarkStore<S>
where
    S: ObjectStore,
{
    pub fn new(object_store: S) -> Self {
        Self {
            object_store,
            update_lock: Arc::new(Mutex::new(())),
        }
    }
}

impl<S> QueryWatermarkRepository for QueryWatermarkStore<S>
where
    S: ObjectStore + 'static,
{
    fn update(&self, watermark: &QueryWatermark) -> Result<(), DatalensError> {
        let _update_guard = self.update_lock.lock().map_err(|error| {
            DatalensError::internal(format!("lock query watermark update: {error}"))
        })?;
        let key = watermark_object_key(&watermark.key);
        if let Some(existing) = self.read(&watermark.key)?
            && existing.latest_block >= watermark.latest_block
        {
            return Ok(());
        }
        let bytes = serde_json::to_vec_pretty(watermark).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode query watermark: {error}"),
            )
        })?;
        self.object_store.put(&key, &bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!("write query watermark {key}: {}", error.message),
            )
        })
    }

    fn read(&self, key: &QueryWatermarkKey) -> Result<Option<QueryWatermark>, DatalensError> {
        let object_key = watermark_object_key(key);
        if !self.object_store.exists(&object_key)? {
            return Ok(None);
        }
        let bytes = self.object_store.get(&object_key)?;
        serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode query watermark {object_key}: {error}"),
            )
        })
    }
}

fn watermark_object_key(key: &QueryWatermarkKey) -> String {
    format!(
        "query-watermarks/applications/{}/chains/{}/datasets/{}/range-kind/{}/selectors/{}.json",
        hex_key(&key.application_id),
        key.chain.key_prefix(),
        hex_key(key.dataset_key.as_str()),
        range_kind_key(key.range_kind.clone()),
        hex_key(&key.selector_fingerprint),
    )
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
