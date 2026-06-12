use datalens_chain::DatasetSelector;
use datalens_core::{ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRange};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, MutexGuard, OnceLock, Weak},
    time::Instant,
};

use crate::{Manifest, ManifestEntry, ManifestFinalityLevel, ObjectStore, range_kind_key};

pub(crate) const DEFAULT_COVERAGE_INDEX_BUCKET_SIZE: u64 = 100_000;

static COVERAGE_INDEX_UPDATE_LOCKS: OnceLock<Mutex<BTreeMap<String, Weak<Mutex<()>>>>> =
    OnceLock::new();

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoverageIndex {
    #[serde(default)]
    pub(crate) entries: Vec<ManifestEntry>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CoverageIndexReplacement {
    pub(crate) replaced_entries: Vec<ManifestEntry>,
    pub(crate) published_entries: Vec<ManifestEntry>,
    bucket_updates: Vec<CoverageIndexReplacementBucket>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct CoverageIndexReplacementPublish {
    pub(crate) replaced_entries: Vec<ManifestEntry>,
    pub(crate) published_entries: Vec<ManifestEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CoverageIndexReplacementBucket {
    key: String,
    bucket_start: u64,
    bucket_end: u64,
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
        let lock = coverage_index_update_lock(object_store, &key)?;
        let _guard = lock_coverage_index_update(&lock)?;
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

pub(crate) fn replace_entry<S>(
    object_store: &S,
    entry: &ManifestEntry,
) -> Result<CoverageIndexReplacement, DatalensError>
where
    S: ObjectStore,
{
    let mut replaced_entries = Vec::new();
    for (bucket_start, bucket_end) in
        bucket_ranges(&entry.range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE)
    {
        let key = coverage_index_key(
            &entry.chain,
            &entry.dataset_key,
            &entry.range,
            &entry.selector_fingerprint,
            entry.finality_level,
            bucket_start,
            bucket_end,
        );
        let lock = coverage_index_update_lock(object_store, &key)?;
        let _guard = lock_coverage_index_update(&lock)?;
        let index = read_index(object_store, &key)?;
        replaced_entries.extend(index.entries.into_iter().filter(|existing_entry| {
            replacement_scope_matches(existing_entry, entry)
                && existing_entry.range.intersection(&entry.range).is_some()
        }));
    }
    let mut replaced_manifest = Manifest {
        entries: replaced_entries,
    };
    replaced_manifest.normalize();

    let mut buckets = BTreeSet::new();
    for replaced_entry in &replaced_manifest.entries {
        buckets.extend(bucket_ranges(
            &replaced_entry.range,
            DEFAULT_COVERAGE_INDEX_BUCKET_SIZE,
        ));
    }
    let replacement_buckets = bucket_ranges(&entry.range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE);
    buckets.extend(replacement_buckets.iter().copied());

    let mut bucket_updates = Vec::new();
    for (bucket_start, bucket_end) in buckets {
        let key = coverage_index_key(
            &entry.chain,
            &entry.dataset_key,
            &entry.range,
            &entry.selector_fingerprint,
            entry.finality_level,
            bucket_start,
            bucket_end,
        );
        bucket_updates.push(CoverageIndexReplacementBucket {
            key,
            bucket_start,
            bucket_end,
        });
    }

    let mut published_manifest = Manifest {
        entries: replacement_published_entries(&replaced_manifest.entries, entry)?,
    };
    published_manifest.normalize();
    Ok(CoverageIndexReplacement {
        replaced_entries: replaced_manifest.entries,
        published_entries: published_manifest.entries,
        bucket_updates,
    })
}

fn replacement_published_entries(
    replaced_entries: &[ManifestEntry],
    entry: &ManifestEntry,
) -> Result<Vec<ManifestEntry>, DatalensError> {
    let mut entries = Vec::new();
    for replaced_entry in replaced_entries {
        entries.extend(split_entry_around_range(
            replaced_entry.clone(),
            &entry.range,
        )?);
    }
    entries.push(entry.clone());
    Ok(entries)
}

pub(crate) fn publish_replacement<S>(
    object_store: &S,
    entry: &ManifestEntry,
    replacement: &CoverageIndexReplacement,
) -> Result<CoverageIndexReplacementPublish, DatalensError>
where
    S: ObjectStore,
{
    let replacement_buckets = bucket_ranges(&entry.range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE);
    let mut pending_buckets = replacement
        .bucket_updates
        .iter()
        .map(|bucket| (bucket.bucket_start, bucket.bucket_end))
        .collect::<BTreeSet<_>>();
    let mut processed_buckets = BTreeSet::new();
    let mut actual_replaced_entries = Vec::new();
    while let Some((bucket_start, bucket_end)) = pending_buckets.pop_first() {
        if !processed_buckets.insert((bucket_start, bucket_end)) {
            continue;
        }
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
        let bucket_range = LedgerRange::try_new(entry.range.kind(), bucket_start, bucket_end)?;
        let lock = coverage_index_update_lock(object_store, &key)?;
        let _guard = lock_coverage_index_update(&lock)?;
        let index = read_index(object_store, &key)?;
        let mut entries = Vec::new();
        let mut bucket_replaced_entries_count = 0usize;
        for existing_entry in index.entries {
            if replacement_scope_matches(&existing_entry, entry)
                && existing_entry.range.intersection(&entry.range).is_some()
            {
                bucket_replaced_entries_count += 1;
                actual_replaced_entries.push(existing_entry.clone());
                for discovered_bucket in
                    bucket_ranges(&existing_entry.range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE)
                {
                    if !processed_buckets.contains(&discovered_bucket) {
                        pending_buckets.insert(discovered_bucket);
                    }
                }
                entries.extend(
                    split_entry_around_range(existing_entry, &entry.range)?
                        .into_iter()
                        .filter(|split_entry| {
                            split_entry.range.intersection(&bucket_range).is_some()
                        }),
                );
            } else if existing_entry.range.intersection(&bucket_range).is_some() {
                entries.push(existing_entry);
            }
        }
        if replacement_buckets.contains(&(bucket_start, bucket_end)) {
            entries.push(entry.clone());
        }
        let mut index = CoverageIndex { entries };
        index.normalize();
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
            "storage replaced coverage index chain_key={} dataset={} selector_fingerprint={} range_kind={} replacement_range={}-{} bucket={}-{} replaced_entries_count={} entries_count={} duration_ms={}",
            entry.chain.key_prefix(),
            entry.dataset_key.as_str(),
            entry.selector_fingerprint,
            range_kind_key(entry.range.kind()),
            entry.range.start(),
            entry.range.end(),
            bucket_start,
            bucket_end,
            bucket_replaced_entries_count,
            index.entries.len(),
            started.elapsed().as_millis()
        );
    }
    let mut replaced_manifest = Manifest {
        entries: actual_replaced_entries,
    };
    replaced_manifest.normalize();
    let mut published_manifest = Manifest {
        entries: replacement_published_entries(&replaced_manifest.entries, entry)?,
    };
    published_manifest.normalize();
    Ok(CoverageIndexReplacementPublish {
        replaced_entries: replaced_manifest.entries,
        published_entries: published_manifest.entries,
    })
}

fn read_index<S>(object_store: &S, key: &str) -> Result<CoverageIndex, DatalensError>
where
    S: ObjectStore,
{
    if !object_store.exists(key)? {
        return Ok(CoverageIndex::default());
    }
    let bytes = object_store.get(key)?;
    serde_json::from_slice(&bytes).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::StorageReadFailure,
            format!("decode coverage index {key}: {error}"),
        )
    })
}

fn coverage_index_update_lock<S>(
    object_store: &S,
    key: &str,
) -> Result<Arc<Mutex<()>>, DatalensError>
where
    S: ObjectStore,
{
    let lock_key = format!("{}:{key}", object_store.lock_namespace());
    let mut locks = COVERAGE_INDEX_UPDATE_LOCKS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .map_err(|_| DatalensError::internal("coverage index update lock poisoned"))?;
    prune_stale_coverage_index_update_locks(&mut locks);
    if let Some(lock) = locks.get(&lock_key).and_then(Weak::upgrade) {
        return Ok(lock);
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(lock_key, Arc::downgrade(&lock));
    Ok(lock)
}

fn lock_coverage_index_update<'a>(
    lock: &'a Mutex<()>,
) -> Result<MutexGuard<'a, ()>, DatalensError> {
    lock.lock()
        .map_err(|_| DatalensError::internal("coverage index update lock poisoned"))
}

fn prune_stale_coverage_index_update_locks(locks: &mut BTreeMap<String, Weak<Mutex<()>>>) {
    locks.retain(|_, lock| lock.strong_count() > 0);
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

fn replacement_scope_matches(existing: &ManifestEntry, replacement: &ManifestEntry) -> bool {
    existing.chain == replacement.chain
        && existing.dataset_key == replacement.dataset_key
        && existing.selector_fingerprint == replacement.selector_fingerprint
        && existing.selector_canonical_key == replacement.selector_canonical_key
        && existing.finality_level == replacement.finality_level
        && existing.range.kind() == replacement.range.kind()
}

fn split_entry_around_range(
    entry: ManifestEntry,
    replacement_range: &LedgerRange,
) -> Result<Vec<ManifestEntry>, DatalensError> {
    let mut entries = Vec::new();
    if entry.range.start() < replacement_range.start() {
        let mut left = entry.clone();
        left.range = LedgerRange::try_new(
            entry.range.kind(),
            entry.range.start(),
            replacement_range.start().saturating_sub(1),
        )?;
        entries.push(left);
    }
    if entry.range.end() > replacement_range.end() {
        let mut right = entry;
        right.range = LedgerRange::try_new(
            right.range.kind(),
            replacement_range.end().saturating_add(1),
            right.range.end(),
        )?;
        entries.push(right);
    }
    Ok(entries)
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

#[cfg(test)]
mod tests {
    use super::*;
    use datalens_core::{ChainFamily, NetworkId};

    fn temp_storage_root(name: &str) -> std::path::PathBuf {
        let root = std::env::temp_dir().join(format!(
            "datalens-coverage-index-{name}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("create temp storage root");
        root
    }

    fn test_chain() -> ChainIdentity {
        ChainIdentity::expect_with_network_id(ChainFamily::Evm, "ethereum", NetworkId::numeric(1))
    }

    fn empty_entry(chain: &ChainIdentity, start: u64, end: u64) -> ManifestEntry {
        ManifestEntry {
            chain: chain.clone(),
            dataset_key: DatasetKey::evm_blocks(),
            range: LedgerRange::blocks(start, end).expect("valid range"),
            selector_fingerprint: "all".to_owned(),
            selector_canonical_key: "all".to_owned(),
            finality_level: ManifestFinalityLevel::Safe,
            object_key: None,
            object_encoding: None,
            object_compression: None,
            row_count: 0,
            object_size_bytes: None,
            checksum: None,
            checksum_algorithm: None,
            written_at_unix_seconds: None,
        }
    }

    fn data_entry(chain: &ChainIdentity, start: u64, end: u64) -> ManifestEntry {
        let range = LedgerRange::blocks(start, end).expect("valid range");
        ManifestEntry {
            chain: chain.clone(),
            dataset_key: DatasetKey::evm_blocks(),
            range: range.clone(),
            selector_fingerprint: "all".to_owned(),
            selector_canonical_key: "all".to_owned(),
            finality_level: ManifestFinalityLevel::Safe,
            object_key: Some(crate::helpers::object_key(
                chain,
                &DatasetKey::evm_blocks(),
                range,
                "all",
                crate::ObjectEncoding::ParquetV1,
            )),
            object_encoding: Some(crate::ObjectEncoding::ParquetV1),
            object_compression: None,
            row_count: 1,
            object_size_bytes: Some(1),
            checksum: Some(format!("{start:064x}")),
            checksum_algorithm: Some("sha256".to_owned()),
            written_at_unix_seconds: Some(1),
        }
    }

    fn index_bucket_ranges(
        object_store: &crate::LocalObjectStore,
        chain: &ChainIdentity,
        bucket_start: u64,
        bucket_end: u64,
    ) -> Vec<(u64, u64)> {
        let key = coverage_index_key(
            chain,
            &DatasetKey::evm_blocks(),
            &LedgerRange::blocks(bucket_start, bucket_start).expect("valid range"),
            "all",
            ManifestFinalityLevel::Safe,
            bucket_start,
            bucket_end,
        );
        read_index(object_store, &key)
            .expect("read index")
            .entries
            .into_iter()
            .map(|entry| (entry.range.start(), entry.range.end()))
            .collect::<Vec<_>>()
    }

    #[test]
    fn test_prune_stale_coverage_index_update_locks_retains_live_locks_only() {
        let live_lock = Arc::new(Mutex::new(()));
        let stale_lock = Arc::new(Mutex::new(()));
        let mut locks = BTreeMap::from([
            ("live".to_owned(), Arc::downgrade(&live_lock)),
            ("stale".to_owned(), Arc::downgrade(&stale_lock)),
        ]);
        drop(stale_lock);

        prune_stale_coverage_index_update_locks(&mut locks);

        assert!(locks.contains_key("live"));
        assert!(!locks.contains_key("stale"));
    }

    #[test]
    fn test_publish_replacement_preserves_concurrent_same_bucket_entry() {
        let object_store = crate::LocalObjectStore::new(temp_storage_root(
            "replacement-preserves-concurrent-entry",
        ));
        let chain = test_chain();
        let selector = DatasetSelector::all();
        let existing = empty_entry(&chain, 70, 72);
        let replacement = empty_entry(&chain, 71, 71);
        let concurrent = empty_entry(&chain, 80, 80);

        write_entry(&object_store, &existing).expect("write existing coverage");
        let plan = replace_entry(&object_store, &replacement).expect("plan replacement");
        write_entry(&object_store, &concurrent).expect("write concurrent coverage");
        publish_replacement(&object_store, &replacement, &plan).expect("publish replacement");

        let entries = read_entries_for_query(
            &object_store,
            &chain,
            &DatasetKey::evm_blocks(),
            &selector,
            &LedgerRange::blocks(70, 80).expect("valid range"),
        )
        .expect("read coverage index")
        .expect("coverage index entries");
        let ranges = entries
            .into_iter()
            .map(|entry| (entry.range.start(), entry.range.end()))
            .collect::<Vec<_>>();

        assert_eq!(ranges, vec![(70, 72), (80, 80)]);
    }

    #[test]
    fn test_publish_replacement_expands_to_concurrent_overlapping_entry_buckets() {
        let object_store = crate::LocalObjectStore::new(temp_storage_root(
            "replacement-expands-concurrent-overlap",
        ));
        let chain = test_chain();
        let replacement = data_entry(&chain, 100_000, 100_000);
        let concurrent = data_entry(&chain, 99_999, 100_001);

        let plan = replace_entry(&object_store, &replacement).expect("plan replacement");
        write_entry(&object_store, &concurrent).expect("write concurrent coverage");
        let published =
            publish_replacement(&object_store, &replacement, &plan).expect("publish replacement");

        assert_eq!(
            published
                .replaced_entries
                .iter()
                .map(|entry| (entry.range.start(), entry.range.end()))
                .collect::<Vec<_>>(),
            vec![(99_999, 100_001)]
        );
        assert_eq!(
            published
                .published_entries
                .iter()
                .map(|entry| (entry.range.start(), entry.range.end()))
                .collect::<Vec<_>>(),
            vec![(99_999, 99_999), (100_000, 100_000), (100_001, 100_001)]
        );

        assert_eq!(
            index_bucket_ranges(&object_store, &chain, 0, 99_999),
            vec![(99_999, 99_999)]
        );
        assert_eq!(
            index_bucket_ranges(&object_store, &chain, 100_000, 199_999),
            vec![(100_000, 100_000), (100_001, 100_001)]
        );
    }
}
