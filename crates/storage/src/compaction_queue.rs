use datalens_core::{ChainIdentity, DatalensError, DatalensErrorKind};
use serde::{Deserialize, Serialize};

use crate::{ManifestEntry, ObjectStore, manifest_segment_key, range_kind_key, unix_seconds_now};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct CompactionQueueEntry {
    pub schema_version: u32,
    pub segment_key: String,
    pub entry: ManifestEntry,
    pub enqueued_at_unix_seconds: u64,
}

pub(crate) fn queue_prefix(chain: &ChainIdentity) -> String {
    format!("chains/{}/metadata/compaction-queue", chain.key_prefix())
}

pub(crate) fn queue_key(chain: &ChainIdentity, entry: &ManifestEntry) -> String {
    format!(
        "{}/{}/{}/{}/{}/{:020}-{:020}.json",
        queue_prefix(chain),
        entry.dataset_key.as_str(),
        range_kind_key(entry.range.kind()),
        entry.selector_fingerprint,
        entry.finality_level.as_str(),
        entry.range.start(),
        entry.range.end(),
    )
}

pub(crate) fn write_entry<S>(
    object_store: &S,
    chain: &ChainIdentity,
    entry: &ManifestEntry,
) -> Result<(), DatalensError>
where
    S: ObjectStore,
{
    if entry.object_key.is_none() {
        return Ok(());
    }
    let queue_entry = CompactionQueueEntry {
        schema_version: 1,
        segment_key: manifest_segment_key(chain, entry),
        entry: entry.clone(),
        enqueued_at_unix_seconds: unix_seconds_now()?,
    };
    let key = queue_key(chain, entry);
    let bytes = serde_json::to_vec_pretty(&queue_entry).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::Internal,
            format!("encode compaction queue entry: {error}"),
        )
    })?;
    object_store.put(&key, &bytes).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::ManifestUpdateFailure,
            format!("write compaction queue entry {key}: {}", error.message),
        )
    })
}

pub(crate) fn decode_entry(key: &str, bytes: &[u8]) -> Result<CompactionQueueEntry, DatalensError> {
    serde_json::from_slice(bytes).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::StorageReadFailure,
            format!("decode compaction queue entry {key}: {error}"),
        )
    })
}
