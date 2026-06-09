use datalens_chain::DatasetSelector;
use datalens_core::{ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange};
use serde::{Deserialize, Serialize};
use std::time::Instant;

use crate::{Manifest, ManifestEntry, ManifestFinalityLevel, ObjectStore, range_kind_key};

pub(crate) const DEFAULT_COVERAGE_INDEX_BUCKET_SIZE: u64 = 100_000;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoverageIndex {
    #[serde(default)]
    pub(crate) entries: Vec<ManifestEntry>,
}

impl CoverageIndex {
    fn upsert(&mut self, entry: ManifestEntry) {
        self.entries.push(entry);
        self.normalize();
    }

    fn normalize(&mut self) {
        let mut manifest = Manifest {
            entries: std::mem::take(&mut self.entries),
        };
        manifest.normalize();
        self.entries = coalesce_empty_entries(manifest.entries);
    }
}

pub(crate) fn read_entries_for_query<S>(
    object_store: &S,
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    range: &LedgerRange,
) -> Result<Option<Vec<ManifestEntry>>, DatalensError>
where
    S: ObjectStore,
{
    let selector_fingerprint = selector.fingerprint();
    let mut entries = Vec::new();
    let mut any_bucket_has_index = false;
    for (bucket_start, bucket_end) in bucket_ranges(range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE) {
        let mut bucket_has_index = false;
        for finality_level in [
            ManifestFinalityLevel::Safe,
            ManifestFinalityLevel::Finalized,
        ] {
            let key = coverage_index_key(
                chain,
                dataset_key,
                range,
                &selector_fingerprint,
                finality_level,
                bucket_start,
                bucket_end,
            );
            if !object_store.exists(&key)? {
                continue;
            }
            let bytes = object_store.get(&key)?;
            let mut index: CoverageIndex = serde_json::from_slice(&bytes).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("decode coverage index {key}: {error}"),
                )
            })?;
            bucket_has_index = true;
            entries.append(&mut index.entries);
        }
        if bucket_has_index {
            any_bucket_has_index = true;
        }
    }
    if !any_bucket_has_index {
        return Ok(None);
    }

    let mut index = CoverageIndex { entries };
    index.normalize();
    Ok(Some(index.entries))
}

pub(crate) fn write_entry<S>(object_store: &S, entry: &ManifestEntry) -> Result<(), DatalensError>
where
    S: ObjectStore,
{
    for (bucket_start, bucket_end) in
        bucket_ranges(&entry.range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE)
    {
        let started = Instant::now();
        let key = coverage_index_key(
            &entry.chain,
            &entry.dataset_key,
            &entry.range,
            &entry.selector_fingerprint,
            entry.finality_level,
            bucket_start,
            bucket_end,
        );
        let mut index = if object_store.exists(&key)? {
            let bytes = object_store.get(&key)?;
            serde_json::from_slice(&bytes).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("decode coverage index {key}: {error}"),
                )
            })?
        } else {
            CoverageIndex::default()
        };
        index.upsert(entry.clone());
        let bytes = serde_json::to_vec_pretty(&index).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode coverage index: {error}"),
            )
        })?;
        object_store.put(&key, &bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ManifestUpdateFailure,
                format!("write coverage index {key}: {}", error.message),
            )
        })?;
        log::info!(
            "storage wrote coverage index chain_key={} dataset={} selector_fingerprint={} range_kind={} bucket={}-{} entries_count={} duration_ms={}",
            entry.chain.key_prefix(),
            entry.dataset_key.as_str(),
            entry.selector_fingerprint,
            range_kind_key(entry.range.kind()),
            bucket_start,
            bucket_end,
            index.entries.len(),
            started.elapsed().as_millis()
        );
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn write_entries<S>(
    object_store: &S,
    entries: &[ManifestEntry],
) -> Result<(), DatalensError>
where
    S: ObjectStore,
{
    for entry in entries {
        write_entry(object_store, entry)?;
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn delete_chain<S>(object_store: &S, chain: &ChainIdentity) -> Result<(), DatalensError>
where
    S: ObjectStore,
{
    for object in object_store.list(&coverage_index_prefix(chain))? {
        object_store.delete(&object.key).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ManifestUpdateFailure,
                format!("delete coverage index {}: {}", object.key, error.message),
            )
        })?;
    }
    Ok(())
}

#[allow(dead_code)]
fn coverage_index_prefix(chain: &ChainIdentity) -> String {
    format!("chains/{}/coverage-index", chain.key_prefix())
}

fn coverage_index_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: &LedgerRange,
    selector_fingerprint: &str,
    finality_level: ManifestFinalityLevel,
    bucket_start: u64,
    bucket_end: u64,
) -> String {
    format!(
        "chains/{}/coverage-index/{}/{}/{}/{}/{:020}-{:020}.json",
        chain.key_prefix(),
        dataset_key.as_str(),
        range_kind_key(range.kind()),
        selector_fingerprint,
        finality_level.as_str(),
        bucket_start,
        bucket_end,
    )
}

fn bucket_ranges(range: &LedgerRange, bucket_size: u64) -> Vec<(u64, u64)> {
    let bucket_size = bucket_size.max(1);
    let mut buckets = Vec::new();
    let mut bucket_start = (range.start() / bucket_size) * bucket_size;
    loop {
        let bucket_end = bucket_start.saturating_add(bucket_size - 1);
        buckets.push((bucket_start, bucket_end));
        if bucket_end >= range.end() || bucket_end == u64::MAX {
            break;
        }
        bucket_start = bucket_end + 1;
    }
    buckets
}

fn coalesce_empty_entries(entries: Vec<ManifestEntry>) -> Vec<ManifestEntry> {
    let mut entries = entries;
    entries.sort_by(|left, right| {
        (
            left.chain.key_prefix(),
            left.dataset_key.as_str(),
            left.selector_fingerprint.as_str(),
            left.selector_canonical_key.as_str(),
            range_kind_key(left.range.kind()),
            left.finality_level.as_str(),
            left.range.start(),
            left.range.end(),
            left.object_key.is_some(),
        )
            .cmp(&(
                right.chain.key_prefix(),
                right.dataset_key.as_str(),
                right.selector_fingerprint.as_str(),
                right.selector_canonical_key.as_str(),
                range_kind_key(right.range.kind()),
                right.finality_level.as_str(),
                right.range.start(),
                right.range.end(),
                right.object_key.is_some(),
            ))
    });

    let mut coalesced: Vec<ManifestEntry> = Vec::new();
    for entry in entries {
        if let Some(last) = coalesced.last_mut()
            && can_merge_empty(last, &entry)
        {
            let end = last.range.end().max(entry.range.end());
            last.range = LedgerRange::try_new(last.range.kind(), last.range.start(), end)
                .expect("merged range remains valid");
            continue;
        }
        coalesced.push(entry);
    }
    coalesced
}

fn can_merge_empty(left: &ManifestEntry, right: &ManifestEntry) -> bool {
    left.object_key.is_none()
        && right.object_key.is_none()
        && left.chain == right.chain
        && left.dataset_key == right.dataset_key
        && left.selector_fingerprint == right.selector_fingerprint
        && left.selector_canonical_key == right.selector_canonical_key
        && left.finality_level == right.finality_level
        && left.range.kind() == right.range.kind()
        && left.range.end().saturating_add(1) >= right.range.start()
}
