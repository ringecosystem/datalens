use std::sync::{Arc, Mutex};

use datalens_chain::DatasetSelector;
use datalens_core::{ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange};
use serde::{Deserialize, Serialize};

use crate::object_store::ObjectStore;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryActivityKey {
    pub application_id: String,
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector_fingerprint: String,
    pub selector_canonical_key: String,
    pub range_kind: datalens_core::LedgerRangeKind,
}

impl QueryActivityKey {
    pub fn new(
        application_id: impl Into<String>,
        chain: ChainIdentity,
        dataset_key: DatasetKey,
        selector: &DatasetSelector,
        range_kind: datalens_core::LedgerRangeKind,
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
pub struct QueryActivity {
    pub key: QueryActivityKey,
    pub latest_range: LedgerRange,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub follow_query_range: Option<LedgerRange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub follow_query_updated_at_unix_seconds: Option<u64>,
    pub updated_at_unix_seconds: u64,
    pub request_id: Option<String>,
}

pub trait QueryActivityRepository: Send + Sync {
    fn update(&self, activity: &QueryActivity) -> Result<(), DatalensError>;
    fn read(&self, key: &QueryActivityKey) -> Result<Option<QueryActivity>, DatalensError>;
}

#[derive(Clone, Debug)]
pub struct QueryActivityStore<S> {
    object_store: S,
    update_lock: Arc<Mutex<()>>,
}

impl<S> QueryActivityStore<S>
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

impl<S> QueryActivityRepository for QueryActivityStore<S>
where
    S: ObjectStore + 'static,
{
    fn update(&self, activity: &QueryActivity) -> Result<(), DatalensError> {
        let _update_guard = self.update_lock.lock().map_err(|error| {
            DatalensError::internal(format!("lock query activity update: {error}"))
        })?;
        let key = activity_object_key(&activity.key);
        if let Some(existing) = self.read(&activity.key)?
            && !activity_is_newer(activity, &existing)
        {
            return Ok(());
        }
        let bytes = serde_json::to_vec_pretty(activity).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode query activity: {error}"),
            )
        })?;
        self.object_store.put(&key, &bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!("write query activity {key}: {}", error.message),
            )
        })
    }

    fn read(&self, key: &QueryActivityKey) -> Result<Option<QueryActivity>, DatalensError> {
        let object_key = activity_object_key(key);
        if !self.object_store.exists(&object_key)? {
            return Ok(None);
        }
        let bytes = self.object_store.get(&object_key)?;
        serde_json::from_slice(&bytes).map(Some).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode query activity {object_key}: {error}"),
            )
        })
    }
}

fn activity_is_newer(incoming: &QueryActivity, existing: &QueryActivity) -> bool {
    match incoming
        .updated_at_unix_seconds
        .cmp(&existing.updated_at_unix_seconds)
    {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => incoming.request_id > existing.request_id,
    }
}

fn activity_object_key(key: &QueryActivityKey) -> String {
    format!(
        "query-activity/applications/{}/chains/{}/datasets/{}/range-kind/{}/selectors/{}.json",
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

fn range_kind_key(kind: datalens_core::LedgerRangeKind) -> String {
    match kind {
        datalens_core::LedgerRangeKind::Block => "block".to_owned(),
        datalens_core::LedgerRangeKind::Slot => "slot".to_owned(),
        datalens_core::LedgerRangeKind::Height => "height".to_owned(),
        datalens_core::LedgerRangeKind::Other(value) => value,
    }
}
