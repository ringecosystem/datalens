use datalens_chain::DatasetSelector;
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, EvmLogFilter, LedgerRange,
    TopicFilter, missing_ranges,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, hash_map::RandomState},
    hash::{BuildHasher, Hash, Hasher},
    process,
    sync::atomic::{AtomicU64, Ordering},
    sync::{Arc, Mutex, MutexGuard, OnceLock, Weak},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use crate::selector_coverage::{parse_evm_log_canonical_key, selector_coverage_candidates};
use crate::{
    Manifest, ManifestEntry, ManifestFinalityLevel, ObjectMetadata, ObjectStore, range_kind_key,
    validate_object_key,
};

pub(crate) const DEFAULT_COVERAGE_INDEX_BUCKET_SIZE: u64 = 100_000;
const COVERAGE_INDEX_V2_SCHEMA_VERSION: u32 = 1;
pub(crate) const COVERAGE_INDEX_V2_LIST_PAGE_SIZE: usize = 1_000;
const EVM_LOG_SEMANTIC_INDEX_VERSION: &str = "evm-logs-v1";
const MAX_EVM_LOG_TOPIC_VALUE_SEMANTIC_KEYS: usize = 8;
const EVM_LOG_LARGE_TOPIC_VALUE_SCOPE: &str = "_large-any-of";
const MAX_COVERAGE_INDEX_V2_BUCKET_READ_THREADS: usize = 8;
const MAX_COVERAGE_INDEX_V2_DELTA_GET_THREADS: usize = 8;
const COVERAGE_INDEX_V2_BUCKET_READ_CACHE_TTL: Duration = Duration::from_secs(300);
const MAX_COVERAGE_INDEX_V2_BUCKET_READ_CACHE_ENTRIES: usize = 512;

static COVERAGE_INDEX_UPDATE_LOCKS: OnceLock<Mutex<BTreeMap<String, Weak<Mutex<()>>>>> =
    OnceLock::new();
static COVERAGE_INDEX_V2_ID_COUNTER: AtomicU64 = AtomicU64::new(0);
static COVERAGE_INDEX_V2_BUCKET_READ_CACHE: OnceLock<
    Mutex<BTreeMap<String, CoverageIndexV2BucketReadCacheEntry>>,
> = OnceLock::new();

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

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CoverageIndexReplacementBucket {
    key: String,
    bucket_start: u64,
    bucket_end: u64,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct CoverageIndexV2Bucket {
    pub(crate) chain_key: String,
    pub(crate) scope: String,
    pub(crate) bucket_start: u64,
    pub(crate) bucket_end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoverageIndexV2Delta {
    pub(crate) schema_version: u32,
    pub(crate) created_at_unix_ms: u64,
    pub(crate) scope: String,
    pub(crate) bucket_start: u64,
    pub(crate) bucket_end: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    replacement: Option<CoverageIndexV2Replacement>,
    pub(crate) entries: Vec<ManifestEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CoverageIndexV2Replacement {
    entry: ManifestEntry,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoverageIndexV2Snapshot {
    pub(crate) schema_version: u32,
    pub(crate) created_at_unix_ms: u64,
    pub(crate) scope: String,
    pub(crate) bucket_start: u64,
    pub(crate) bucket_end: u64,
    pub(crate) entries: Vec<ManifestEntry>,
    pub(crate) compacted_delta_keys: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoverageIndexV2SnapshotHead {
    pub(crate) schema_version: u32,
    pub(crate) created_at_unix_ms: u64,
    pub(crate) scope: String,
    pub(crate) bucket_start: u64,
    pub(crate) bucket_end: u64,
    pub(crate) snapshot_key: String,
    pub(crate) included_delta_high_watermark: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoverageIndexV2CleanupRecord {
    pub(crate) schema_version: u32,
    pub(crate) created_at_unix_ms: u64,
    pub(crate) scope: String,
    pub(crate) bucket_start: u64,
    pub(crate) bucket_end: u64,
    pub(crate) compaction_id: String,
    pub(crate) snapshot_key: String,
    pub(crate) compacted_delta_keys: Vec<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct CoverageIndexV2DeltaObject {
    pub(crate) key: String,
    pub(crate) size: u64,
    pub(crate) delta: CoverageIndexV2Delta,
}

#[derive(Clone, Debug)]
pub(crate) struct CoverageIndexV2BucketCompaction {
    pub(crate) bucket: CoverageIndexV2Bucket,
    pub(crate) entries: Vec<ManifestEntry>,
    pub(crate) compacted_delta_keys: Vec<String>,
    pub(crate) newly_compacted_delta_keys: Vec<String>,
    pub(crate) included_delta_high_watermark: String,
    pub(crate) input_delta_bytes: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct CoverageIndexV2CleanupRecordObject {
    pub(crate) key: String,
    pub(crate) record: CoverageIndexV2CleanupRecord,
}

#[derive(Clone, Debug)]
struct CoverageIndexV2BucketReadCacheEntry {
    inserted_at: Instant,
    dirty: bool,
    has_index: bool,
    snapshot_head_key: Option<String>,
    delta_objects: Vec<ObjectMetadata>,
    entries: Vec<ManifestEntry>,
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
    let mut exact_entries = Vec::new();
    let exact_keys = exact_coverage_index_query_keys_for_ranges(
        chain,
        dataset_key,
        selector,
        std::slice::from_ref(range),
    );
    let exact_v2_buckets = exact_coverage_index_v2_query_buckets_for_ranges(
        chain,
        dataset_key,
        selector,
        std::slice::from_ref(range),
    );
    let exact_has_index = read_entries_for_keys(object_store, exact_keys, &mut exact_entries)?;
    let exact_has_v2_index =
        read_entries_for_v2_buckets(object_store, exact_v2_buckets, &mut exact_entries)?;
    let exact_entries =
        normalized_query_entries(exact_entries, chain, dataset_key, selector, range);
    let exact_covered =
        covered_ranges_from_entries(&exact_entries, chain, dataset_key, selector, range);
    let exact_missing = missing_ranges(range.clone(), &exact_covered);
    if exact_missing.is_empty() {
        return Ok(Some(exact_entries));
    }

    let mut entries = exact_entries;
    let mut any_semantic_has_index = false;
    let mut missing = exact_missing;
    for semantic_scopes in evm_log_query_semantic_scope_groups(dataset_key, selector) {
        let semantic_keys = semantic_coverage_index_query_keys_for_scopes(
            chain,
            dataset_key,
            selector,
            &missing,
            &semantic_scopes,
        );
        let semantic_v2_buckets = semantic_coverage_index_v2_query_buckets_for_scopes(
            chain,
            dataset_key,
            selector,
            &missing,
            &semantic_scopes,
        );
        any_semantic_has_index |= read_entries_for_keys(object_store, semantic_keys, &mut entries)?;
        any_semantic_has_index |=
            read_entries_for_v2_buckets(object_store, semantic_v2_buckets, &mut entries)?;
        let normalized =
            normalized_query_entries(entries.clone(), chain, dataset_key, selector, range);
        let covered = covered_ranges_from_entries(&normalized, chain, dataset_key, selector, range);
        missing = missing_ranges(range.clone(), &covered);
        if missing.is_empty() {
            entries = normalized;
            break;
        }
    }
    if !exact_has_index && !exact_has_v2_index && !any_semantic_has_index {
        return Ok(None);
    }

    Ok(Some(normalized_query_entries(
        entries,
        chain,
        dataset_key,
        selector,
        range,
    )))
}

pub(crate) fn read_compatible_evm_log_data_entries_for_query<S>(
    object_store: &S,
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    range: &LedgerRange,
) -> Result<Vec<ManifestEntry>, DatalensError>
where
    S: ObjectStore,
{
    if *dataset_key != DatasetKey::evm_logs() || !matches!(selector, DatasetSelector::EvmLogs(_)) {
        return Ok(Vec::new());
    }
    let Some(query_filter) = parse_evm_log_canonical_key(&selector.canonical_key()) else {
        return Ok(Vec::new());
    };
    let mut entries = Vec::new();
    let keys = semantic_coverage_index_query_keys_for_ranges(
        chain,
        dataset_key,
        selector,
        std::slice::from_ref(range),
    );
    let v2_buckets = semantic_coverage_index_v2_query_buckets_for_ranges(
        chain,
        dataset_key,
        selector,
        std::slice::from_ref(range),
    );
    read_entries_for_keys(object_store, keys, &mut entries)?;
    read_entries_for_v2_buckets(object_store, v2_buckets, &mut entries)?;
    let mut index = CoverageIndex { entries };
    index.normalize();
    Ok(index
        .entries
        .into_iter()
        .filter(|entry| entry.chain == *chain)
        .filter(|entry| entry.dataset_key == *dataset_key)
        .filter(|entry| entry.object_key.is_some())
        .filter(|entry| entry.range.kind() == range.kind())
        .filter(|entry| entry.range.intersection(range).is_some())
        .filter(|entry| {
            parse_evm_log_canonical_key(&entry.selector_canonical_key)
                .map(|stored_filter| query_filter.covers(&stored_filter))
                .unwrap_or(false)
        })
        .collect())
}

fn read_entries_for_keys<S>(
    object_store: &S,
    keys: BTreeSet<String>,
    entries: &mut Vec<ManifestEntry>,
) -> Result<bool, DatalensError>
where
    S: ObjectStore,
{
    let mut any_key_has_index = false;
    for key in keys {
        let Some(bytes) = object_store.get_optional(&key)? else {
            continue;
        };
        let mut index: CoverageIndex = serde_json::from_slice(&bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode coverage index {key}: {error}"),
            )
        })?;
        any_key_has_index = true;
        entries.append(&mut index.entries);
    }
    Ok(any_key_has_index)
}

fn read_entries_for_v2_buckets<S>(
    object_store: &S,
    buckets: BTreeSet<CoverageIndexV2Bucket>,
    entries: &mut Vec<ManifestEntry>,
) -> Result<bool, DatalensError>
where
    S: ObjectStore,
{
    let mut any_bucket_has_index = false;
    let buckets = buckets.into_iter().collect::<Vec<_>>();
    for chunk in buckets.chunks(MAX_COVERAGE_INDEX_V2_BUCKET_READ_THREADS) {
        let chunk_results = thread::scope(|scope| {
            let handles = chunk
                .iter()
                .cloned()
                .map(|bucket| {
                    let object_store = object_store.clone();
                    scope.spawn(move || {
                        let mut bucket_entries = Vec::new();
                        let has_index = read_entries_for_v2_bucket(
                            &object_store,
                            &bucket,
                            &mut bucket_entries,
                        )?;
                        Ok::<_, DatalensError>((has_index, bucket_entries))
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().map_err(|_| {
                        DatalensError::new(
                            DatalensErrorKind::StorageReadFailure,
                            "coverage index v2 bucket read worker panicked",
                        )
                    })?
                })
                .collect::<Result<Vec<_>, _>>()
        })?;
        for (has_index, mut bucket_entries) in chunk_results {
            any_bucket_has_index |= has_index;
            entries.append(&mut bucket_entries);
        }
    }
    Ok(any_bucket_has_index)
}

fn read_entries_for_v2_bucket<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
    entries: &mut Vec<ManifestEntry>,
) -> Result<bool, DatalensError>
where
    S: ObjectStore,
{
    let cache_key = coverage_index_v2_bucket_read_cache_key(object_store, bucket);
    if let Some(cached) = get_fresh_cached_v2_bucket_entries(&cache_key)
        && cached_v2_bucket_entries_are_present(object_store, &cached)?
    {
        entries.extend(cached.entries);
        return Ok(cached.has_index);
    }
    let mut any_bucket_has_index = false;
    let mut compacted_delta_keys = BTreeSet::new();
    let mut start_after_delta_key = None;
    let mut snapshot_head = latest_v2_snapshot_head_object(object_store, bucket)?;
    let mut snapshot = None;
    if let Some((_, head)) = snapshot_head.as_ref() {
        // Snapshot heads written by the v2 compactor use this as a compacted
        // prefix watermark: all delta objects at or below the key are already
        // represented in the snapshot. Older heads left it empty, which keeps
        // the conservative full-bucket scan.
        if !head.included_delta_high_watermark.is_empty() {
            start_after_delta_key = Some(head.included_delta_high_watermark.as_str());
        } else {
            let loaded_snapshot = read_v2_snapshot(object_store, bucket, &head.snapshot_key)?;
            compacted_delta_keys.extend(loaded_snapshot.compacted_delta_keys.iter().cloned());
            snapshot = Some(loaded_snapshot);
        }
        any_bucket_has_index = true;
    }
    let snapshot_head_key = snapshot_head.as_ref().map(|(key, _)| key.clone());

    let delta_objects = list_v2_delta_object_metadata_for_bucket(
        object_store,
        bucket,
        &compacted_delta_keys,
        None,
        start_after_delta_key,
    )?;
    let (mut bucket_entries, remaining_delta_objects) = if let Some(cached) =
        get_cached_v2_bucket_entries(&cache_key)
        && !cached.dirty
        && object_metadata_prefix_matches(&cached.delta_objects, &delta_objects)
    {
        any_bucket_has_index = cached.has_index;
        (cached.entries, &delta_objects[cached.delta_objects.len()..])
    } else {
        let mut bucket_entries = Vec::new();
        if let Some((_, head)) = snapshot_head.take() {
            let mut loaded_snapshot = match snapshot {
                Some(snapshot) => snapshot,
                None => read_v2_snapshot(object_store, bucket, &head.snapshot_key)?,
            };
            bucket_entries.append(&mut loaded_snapshot.entries);
        }
        (bucket_entries, delta_objects.as_slice())
    };

    for object in
        read_v2_delta_objects_from_metadata(object_store, bucket, remaining_delta_objects)?
    {
        let mut delta = object.delta;
        any_bucket_has_index = true;
        if let Some(replacement) = delta.replacement {
            apply_v2_replacement(&mut bucket_entries, &replacement.entry)?;
        }
        bucket_entries.append(&mut delta.entries);
    }
    put_cached_v2_bucket_entries(
        cache_key,
        any_bucket_has_index,
        snapshot_head_key,
        delta_objects,
        bucket_entries.clone(),
    );
    entries.append(&mut bucket_entries);
    Ok(any_bucket_has_index)
}

pub(crate) fn prepare_v2_bucket_compaction<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
    delta_count_threshold: usize,
    max_delta_objects: usize,
) -> Result<Option<CoverageIndexV2BucketCompaction>, DatalensError>
where
    S: ObjectStore,
{
    let mut entries = Vec::new();
    let mut previous_compacted_delta_keys = BTreeSet::new();
    let mut start_after_delta_key = None;
    if let Some(head) = latest_v2_snapshot_head(object_store, bucket)? {
        let snapshot = read_v2_snapshot(object_store, bucket, &head.snapshot_key)?;
        previous_compacted_delta_keys.extend(snapshot.compacted_delta_keys);
        // See read_entries_for_v2_bucket: non-empty watermarks are only
        // published by compaction after folding the listed delta prefix.
        if !head.included_delta_high_watermark.is_empty() {
            start_after_delta_key = Some(head.included_delta_high_watermark);
        }
        entries.extend(snapshot.entries);
    }

    let deltas = list_v2_delta_objects_for_bucket(
        object_store,
        bucket,
        &previous_compacted_delta_keys,
        Some(max_delta_objects),
        start_after_delta_key.as_deref(),
    )?;
    if deltas.len() < delta_count_threshold.max(1) {
        return Ok(None);
    }
    let input_delta_bytes = deltas.iter().map(|object| object.size).sum();
    let included_delta_high_watermark = deltas
        .last()
        .map(|object| object.key.clone())
        .unwrap_or_default();
    let newly_compacted_delta_keys = deltas
        .iter()
        .map(|object| object.key.clone())
        .collect::<Vec<_>>();
    for object in deltas {
        if let Some(replacement) = object.delta.replacement {
            apply_v2_replacement(&mut entries, &replacement.entry)?;
        }
        entries.extend(object.delta.entries);
    }
    let mut index = CoverageIndex { entries };
    index.normalize();
    let mut compacted_delta_keys = previous_compacted_delta_keys
        .into_iter()
        .chain(newly_compacted_delta_keys.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    compacted_delta_keys.sort();
    compacted_delta_keys.dedup();
    Ok(Some(CoverageIndexV2BucketCompaction {
        bucket: bucket.clone(),
        entries: index.entries,
        compacted_delta_keys,
        newly_compacted_delta_keys,
        included_delta_high_watermark,
        input_delta_bytes,
    }))
}

pub(crate) fn v2_snapshot_cleanup_records_for_bucket<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
) -> Result<Vec<CoverageIndexV2CleanupRecord>, DatalensError>
where
    S: ObjectStore,
{
    let existing_delta_keys = list_v2_delta_object_keys_for_bucket(object_store, bucket)?;
    let mut records = Vec::new();
    for (_, head) in list_v2_snapshot_heads_for_bucket(object_store, bucket)? {
        let snapshot = read_v2_snapshot(object_store, bucket, &head.snapshot_key)?;
        let compacted_delta_keys = snapshot
            .compacted_delta_keys
            .into_iter()
            .filter(|key| existing_delta_keys.contains(key))
            .collect::<Vec<_>>();
        if compacted_delta_keys.is_empty() {
            continue;
        }
        records.push(CoverageIndexV2CleanupRecord {
            schema_version: COVERAGE_INDEX_V2_SCHEMA_VERSION,
            created_at_unix_ms: snapshot.created_at_unix_ms,
            scope: snapshot.scope,
            bucket_start: snapshot.bucket_start,
            bucket_end: snapshot.bucket_end,
            compaction_id: head
                .snapshot_key
                .rsplit('/')
                .next()
                .unwrap_or("")
                .trim_end_matches(".json")
                .to_owned(),
            snapshot_key: head.snapshot_key,
            compacted_delta_keys,
        });
    }
    Ok(records)
}

fn list_v2_delta_objects_for_bucket<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
    skip_delta_keys: &BTreeSet<String>,
    max_objects: Option<usize>,
    start_after_delta_key: Option<&str>,
) -> Result<Vec<CoverageIndexV2DeltaObject>, DatalensError>
where
    S: ObjectStore,
{
    let objects = list_v2_delta_object_metadata_for_bucket(
        object_store,
        bucket,
        skip_delta_keys,
        max_objects,
        start_after_delta_key,
    )?;
    read_v2_delta_objects_from_metadata(object_store, bucket, &objects)
}

fn read_v2_delta_objects_from_metadata<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
    objects: &[ObjectMetadata],
) -> Result<Vec<CoverageIndexV2DeltaObject>, DatalensError>
where
    S: ObjectStore,
{
    let mut deltas = Vec::new();
    for chunk in objects.chunks(MAX_COVERAGE_INDEX_V2_DELTA_GET_THREADS) {
        let chunk_results = thread::scope(|scope| {
            let handles = chunk
                .iter()
                .cloned()
                .map(|object| {
                    let object_store = object_store.clone();
                    let bucket = bucket.clone();
                    scope.spawn(move || read_v2_delta_object(&object_store, &bucket, object))
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().map_err(|_| {
                        DatalensError::new(
                            DatalensErrorKind::StorageReadFailure,
                            "coverage index v2 delta read worker panicked",
                        )
                    })?
                })
                .collect::<Result<Vec<_>, _>>()
        })?;
        deltas.extend(chunk_results);
    }
    Ok(deltas)
}

fn coverage_index_v2_bucket_read_cache_key<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
) -> String
where
    S: ObjectStore,
{
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    #[cfg(debug_assertions)]
    (object_store as *const S as usize).hash(&mut hasher);
    #[cfg(debug_assertions)]
    std::thread::current().id().hash(&mut hasher);
    let debug_namespace = hasher.finish();
    format!(
        "{}\n{}\n{}\n{}\n{:016x}",
        bucket.chain_key, bucket.scope, bucket.bucket_start, bucket.bucket_end, debug_namespace
    )
}

fn object_metadata_prefix_matches(prefix: &[ObjectMetadata], objects: &[ObjectMetadata]) -> bool {
    prefix.len() <= objects.len()
        && prefix
            .iter()
            .zip(objects)
            .all(|(left, right)| left == right)
}

fn get_cached_v2_bucket_entries(key: &str) -> Option<CoverageIndexV2BucketReadCacheEntry> {
    let now = Instant::now();
    let mut cache = coverage_index_v2_bucket_read_cache().lock().ok()?;
    match cache.get(key) {
        Some(entry)
            if now.duration_since(entry.inserted_at) <= COVERAGE_INDEX_V2_BUCKET_READ_CACHE_TTL =>
        {
            Some(entry.clone())
        }
        Some(_) => {
            cache.remove(key);
            None
        }
        None => None,
    }
}

fn get_fresh_cached_v2_bucket_entries(key: &str) -> Option<CoverageIndexV2BucketReadCacheEntry> {
    let cached = get_cached_v2_bucket_entries(key)?;
    (!cached.dirty).then_some(cached)
}

fn cached_v2_bucket_entries_are_present<S>(
    object_store: &S,
    cached: &CoverageIndexV2BucketReadCacheEntry,
) -> Result<bool, DatalensError>
where
    S: ObjectStore,
{
    let Some(last_delta_object) = cached.delta_objects.last() else {
        return Ok(true);
    };
    object_store.exists(&last_delta_object.key)
}

fn put_cached_v2_bucket_entries(
    key: String,
    has_index: bool,
    snapshot_head_key: Option<String>,
    delta_objects: Vec<ObjectMetadata>,
    entries: Vec<ManifestEntry>,
) {
    let Ok(mut cache) = coverage_index_v2_bucket_read_cache().lock() else {
        return;
    };
    let now = Instant::now();
    cache.retain(|_, entry| {
        now.duration_since(entry.inserted_at) <= COVERAGE_INDEX_V2_BUCKET_READ_CACHE_TTL
    });
    if cache.len() >= MAX_COVERAGE_INDEX_V2_BUCKET_READ_CACHE_ENTRIES
        && let Some(oldest_key) = cache
            .iter()
            .min_by_key(|(_, entry)| entry.inserted_at)
            .map(|(key, _)| key.clone())
    {
        cache.remove(&oldest_key);
    }
    cache.insert(
        key,
        CoverageIndexV2BucketReadCacheEntry {
            inserted_at: now,
            dirty: false,
            has_index,
            snapshot_head_key,
            delta_objects,
            entries,
        },
    );
}

fn append_cached_v2_bucket_delta<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
    delta_key: String,
    delta: &CoverageIndexV2Delta,
) where
    S: ObjectStore,
{
    let key = coverage_index_v2_bucket_read_cache_key(object_store, bucket);
    if !bucket.scope.contains("evm.logs") || delta.replacement.is_some() {
        mark_cached_v2_bucket_entries_dirty(object_store, bucket);
        return;
    }
    let now = Instant::now();
    let (snapshot_head_key, last_cached_delta_key) = {
        let Ok(mut cache) = coverage_index_v2_bucket_read_cache().lock() else {
            return;
        };
        let Some(entry) = cache.get_mut(&key) else {
            return;
        };
        if now.duration_since(entry.inserted_at) > COVERAGE_INDEX_V2_BUCKET_READ_CACHE_TTL {
            cache.remove(&key);
            return;
        }
        if !entry.has_index {
            entry.dirty = true;
            return;
        }
        let Some(last_cached_delta) = entry.delta_objects.last() else {
            entry.dirty = true;
            return;
        };
        (
            entry.snapshot_head_key.clone(),
            last_cached_delta.key.clone(),
        )
    };
    let latest_snapshot_head_key = match latest_v2_snapshot_head_object(object_store, bucket) {
        Ok(head) => head.map(|(key, _)| key),
        Err(_) => {
            mark_cached_v2_bucket_entries_dirty(object_store, bucket);
            return;
        }
    };
    if snapshot_head_key != latest_snapshot_head_key {
        mark_cached_v2_bucket_entries_dirty(object_store, bucket);
        return;
    }
    match object_store.exists(&last_cached_delta_key) {
        Ok(true) => {}
        Ok(false) | Err(_) => {
            mark_cached_v2_bucket_entries_dirty(object_store, bucket);
            return;
        }
    }
    let new_delta_objects = match list_v2_delta_object_metadata_for_bucket(
        object_store,
        bucket,
        &BTreeSet::new(),
        Some(2),
        Some(&last_cached_delta_key),
    ) {
        Ok(objects) => objects,
        Err(_) => {
            mark_cached_v2_bucket_entries_dirty(object_store, bucket);
            return;
        }
    };
    let [new_delta_object] = new_delta_objects.as_slice() else {
        mark_cached_v2_bucket_entries_dirty(object_store, bucket);
        return;
    };
    if new_delta_object.key != delta_key {
        mark_cached_v2_bucket_entries_dirty(object_store, bucket);
        return;
    }
    let Ok(mut cache) = coverage_index_v2_bucket_read_cache().lock() else {
        return;
    };
    let Some(entry) = cache.get_mut(&key) else {
        return;
    };
    if now.duration_since(entry.inserted_at) > COVERAGE_INDEX_V2_BUCKET_READ_CACHE_TTL {
        cache.remove(&key);
        return;
    }
    if entry.dirty
        || !entry.has_index
        || entry.snapshot_head_key != snapshot_head_key
        || entry
            .delta_objects
            .last()
            .is_none_or(|object| object.key != last_cached_delta_key)
    {
        entry.dirty = true;
        return;
    }
    entry.entries.extend(delta.entries.iter().cloned());
    entry.delta_objects.push(new_delta_object.clone());
    entry
        .delta_objects
        .sort_by(|left, right| left.key.cmp(&right.key));
    entry
        .delta_objects
        .dedup_by(|left, right| left.key == right.key);
    entry.has_index = true;
    entry.dirty = false;
}

fn mark_cached_v2_bucket_entries_dirty<S>(object_store: &S, bucket: &CoverageIndexV2Bucket)
where
    S: ObjectStore,
{
    let key = coverage_index_v2_bucket_read_cache_key(object_store, bucket);
    if let Ok(mut cache) = coverage_index_v2_bucket_read_cache().lock()
        && let Some(entry) = cache.get_mut(&key)
    {
        entry.dirty = true;
    }
}

fn coverage_index_v2_bucket_read_cache()
-> &'static Mutex<BTreeMap<String, CoverageIndexV2BucketReadCacheEntry>> {
    COVERAGE_INDEX_V2_BUCKET_READ_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn read_v2_delta_object<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
    object: ObjectMetadata,
) -> Result<CoverageIndexV2DeltaObject, DatalensError>
where
    S: ObjectStore,
{
    let bytes = object_store.get(&object.key)?;
    let delta: CoverageIndexV2Delta = serde_json::from_slice(&bytes).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::StorageReadFailure,
            format!("decode coverage index v2 delta {}: {error}", object.key),
        )
    })?;
    validate_v2_record_scope(
        "delta",
        &object.key,
        delta.schema_version,
        &delta.scope,
        delta.bucket_start,
        delta.bucket_end,
        bucket,
    )?;
    Ok(CoverageIndexV2DeltaObject {
        key: object.key,
        size: object.size,
        delta,
    })
}

fn list_v2_delta_object_keys_for_bucket<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
) -> Result<BTreeSet<String>, DatalensError>
where
    S: ObjectStore,
{
    Ok(list_v2_delta_object_metadata_for_bucket(
        object_store,
        bucket,
        &BTreeSet::new(),
        None,
        None,
    )?
    .into_iter()
    .map(|object| object.key)
    .collect())
}

fn list_v2_delta_object_metadata_for_bucket<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
    skip_delta_keys: &BTreeSet<String>,
    max_objects: Option<usize>,
    start_after_delta_key: Option<&str>,
) -> Result<Vec<ObjectMetadata>, DatalensError>
where
    S: ObjectStore,
{
    let delta_prefix = coverage_index_v2_delta_prefix(
        &bucket.chain_key,
        &bucket.scope,
        bucket.bucket_start,
        bucket.bucket_end,
    );
    let strict_prefix = format!("{delta_prefix}/");
    let mut objects = Vec::new();
    let mut start_after = start_after_delta_key.map(ToOwned::to_owned);
    loop {
        let page = object_store.list_page(
            &delta_prefix,
            start_after.as_deref(),
            COVERAGE_INDEX_V2_LIST_PAGE_SIZE,
        )?;
        for object in &page.objects {
            if !object.key.starts_with(&strict_prefix) || skip_delta_keys.contains(&object.key) {
                continue;
            }
            objects.push(object.clone());
            if max_objects.is_some_and(|max_objects| objects.len() >= max_objects) {
                return Ok(objects);
            }
        }
        if !page.has_more {
            break;
        }
        start_after = page.objects.last().map(|object| object.key.clone());
    }
    Ok(objects)
}

fn apply_v2_replacement(
    entries: &mut Vec<ManifestEntry>,
    replacement_entry: &ManifestEntry,
) -> Result<(), DatalensError> {
    let mut retained = Vec::new();
    for existing_entry in entries.drain(..) {
        if replacement_scope_matches(&existing_entry, replacement_entry)
            && existing_entry
                .range
                .intersection(&replacement_entry.range)
                .is_some()
        {
            retained.extend(split_entry_around_range(
                existing_entry,
                &replacement_entry.range,
            )?);
        } else {
            retained.push(existing_entry);
        }
    }
    *entries = retained;
    Ok(())
}

pub(crate) fn latest_v2_snapshot_head<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
) -> Result<Option<CoverageIndexV2SnapshotHead>, DatalensError>
where
    S: ObjectStore,
{
    Ok(latest_v2_snapshot_head_object(object_store, bucket)?.map(|(_, head)| head))
}

fn latest_v2_snapshot_head_object<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
) -> Result<Option<(String, CoverageIndexV2SnapshotHead)>, DatalensError>
where
    S: ObjectStore,
{
    Ok(list_v2_snapshot_heads_for_bucket(object_store, bucket)?
        .into_iter()
        .last())
}

fn list_v2_snapshot_heads_for_bucket<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
) -> Result<Vec<(String, CoverageIndexV2SnapshotHead)>, DatalensError>
where
    S: ObjectStore,
{
    let prefix = coverage_index_v2_snapshot_head_prefix(
        &bucket.chain_key,
        &bucket.scope,
        bucket.bucket_start,
        bucket.bucket_end,
    );
    let strict_prefix = format!("{prefix}/");
    let mut heads = Vec::new();
    let mut start_after = None;
    loop {
        let page = object_store.list_page(
            &prefix,
            start_after.as_deref(),
            COVERAGE_INDEX_V2_LIST_PAGE_SIZE,
        )?;
        for object in &page.objects {
            if !object.key.starts_with(&strict_prefix) {
                continue;
            }
            let bytes = object_store.get(&object.key)?;
            let head: CoverageIndexV2SnapshotHead =
                serde_json::from_slice(&bytes).map_err(|error| {
                    DatalensError::new(
                        DatalensErrorKind::StorageReadFailure,
                        format!(
                            "decode coverage index v2 snapshot head {}: {error}",
                            object.key
                        ),
                    )
                })?;
            validate_v2_record_scope(
                "snapshot head",
                &object.key,
                head.schema_version,
                &head.scope,
                head.bucket_start,
                head.bucket_end,
                bucket,
            )?;
            heads.push((object.key.clone(), head));
        }
        if !page.has_more {
            break;
        }
        start_after = page.objects.last().map(|object| object.key.clone());
    }
    heads.sort_by(|left, right| {
        (left.1.created_at_unix_ms, left.0.as_str())
            .cmp(&(right.1.created_at_unix_ms, right.0.as_str()))
    });
    Ok(heads)
}

pub(crate) fn read_v2_snapshot<S>(
    object_store: &S,
    bucket: &CoverageIndexV2Bucket,
    snapshot_key: &str,
) -> Result<CoverageIndexV2Snapshot, DatalensError>
where
    S: ObjectStore,
{
    let snapshot_bytes = object_store.get(snapshot_key)?;
    let snapshot: CoverageIndexV2Snapshot =
        serde_json::from_slice(&snapshot_bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode coverage index v2 snapshot {snapshot_key}: {error}"),
            )
        })?;
    validate_v2_record_scope(
        "snapshot",
        snapshot_key,
        snapshot.schema_version,
        &snapshot.scope,
        snapshot.bucket_start,
        snapshot.bucket_end,
        bucket,
    )?;
    Ok(snapshot)
}

pub(crate) fn v2_cleanup_record_is_safe_to_delete_with_cache<S>(
    object_store: &S,
    chain: &ChainIdentity,
    cleanup: &CoverageIndexV2CleanupRecordObject,
    latest_delta_key_cache: &mut BTreeMap<CoverageIndexV2Bucket, Option<BTreeSet<String>>>,
) -> Result<bool, DatalensError>
where
    S: ObjectStore,
{
    let record = &cleanup.record;
    let bucket = CoverageIndexV2Bucket {
        chain_key: chain.key_prefix(),
        scope: record.scope.clone(),
        bucket_start: record.bucket_start,
        bucket_end: record.bucket_end,
    };
    validate_v2_record_scope(
        "cleanup record",
        &cleanup.key,
        record.schema_version,
        &record.scope,
        record.bucket_start,
        record.bucket_end,
        &bucket,
    )?;
    if record.compacted_delta_keys.is_empty() {
        return Ok(false);
    }
    if !cleanup_record_key_matches_bucket(&cleanup.key, chain, &bucket) {
        return Ok(false);
    }
    let delta_prefix = format!(
        "{}/",
        coverage_index_v2_delta_prefix(
            &bucket.chain_key,
            &bucket.scope,
            bucket.bucket_start,
            bucket.bucket_end,
        )
    );
    for object_key in &record.compacted_delta_keys {
        if validate_object_key(object_key).is_err()
            || !object_key.starts_with(&delta_prefix)
            || !object_key.ends_with(".json")
        {
            return Ok(false);
        }
    }
    if !latest_delta_key_cache.contains_key(&bucket) {
        let latest_delta_keys = match latest_v2_snapshot_head(object_store, &bucket)? {
            Some(latest_head) => {
                let latest_snapshot =
                    read_v2_snapshot(object_store, &bucket, &latest_head.snapshot_key)?;
                Some(
                    latest_snapshot
                        .compacted_delta_keys
                        .into_iter()
                        .collect::<BTreeSet<_>>(),
                )
            }
            None => None,
        };
        latest_delta_key_cache.insert(bucket.clone(), latest_delta_keys);
    }
    let Some(latest_delta_keys) = latest_delta_key_cache
        .get(&bucket)
        .and_then(|keys| keys.as_ref())
    else {
        return Ok(false);
    };
    Ok(record
        .compacted_delta_keys
        .iter()
        .all(|key| latest_delta_keys.contains(key)))
}

fn cleanup_record_key_matches_bucket(
    key: &str,
    chain: &ChainIdentity,
    bucket: &CoverageIndexV2Bucket,
) -> bool {
    let prefix = format!("{}/", coverage_index_v2_cleanup_prefix(&chain.key_prefix()));
    let bucket_prefix = format!(
        "{}{}/{:020}-{:020}/",
        prefix, bucket.scope, bucket.bucket_start, bucket.bucket_end
    );
    key.starts_with(&bucket_prefix) && key.ends_with(".json")
}

fn validate_v2_record_scope(
    record_kind: &str,
    key: &str,
    schema_version: u32,
    scope: &str,
    bucket_start: u64,
    bucket_end: u64,
    bucket: &CoverageIndexV2Bucket,
) -> Result<(), DatalensError> {
    if schema_version != COVERAGE_INDEX_V2_SCHEMA_VERSION {
        return Err(DatalensError::new(
            DatalensErrorKind::StorageReadFailure,
            format!(
                "coverage index v2 {record_kind} {key} has unsupported schema_version {schema_version}"
            ),
        ));
    }
    if scope != bucket.scope
        || bucket_start != bucket.bucket_start
        || bucket_end != bucket.bucket_end
    {
        return Err(DatalensError::new(
            DatalensErrorKind::StorageReadFailure,
            format!("coverage index v2 {record_kind} {key} scope does not match object key"),
        ));
    }
    Ok(())
}

fn normalized_query_entries(
    entries: Vec<ManifestEntry>,
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    range: &LedgerRange,
) -> Vec<ManifestEntry> {
    let mut index = CoverageIndex { entries };
    index.normalize();
    selector_coverage_candidates(&index.entries, chain, dataset_key, selector, range)
        .into_iter()
        .map(|candidate| candidate.entry.clone())
        .collect()
}

fn covered_ranges_from_entries(
    entries: &[ManifestEntry],
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    range: &LedgerRange,
) -> Vec<LedgerRange> {
    let mut ranges = selector_coverage_candidates(entries, chain, dataset_key, selector, range)
        .into_iter()
        .flat_map(|candidate| candidate.ranges)
        .collect::<Vec<_>>();
    ranges.sort_by_key(|range| range.start());
    crate::merge_ranges(ranges)
}

#[allow(dead_code)]
pub(crate) fn write_entry<S>(object_store: &S, entry: &ManifestEntry) -> Result<(), DatalensError>
where
    S: ObjectStore,
{
    write_entry_with_legacy_index(object_store, entry, true)
}

pub(crate) fn write_entry_with_legacy_index<S>(
    object_store: &S,
    entry: &ManifestEntry,
    legacy_write_enabled: bool,
) -> Result<(), DatalensError>
where
    S: ObjectStore,
{
    if legacy_write_enabled {
        for (bucket_start, bucket_end) in
            bucket_ranges(&entry.range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE)
        {
            let started = Instant::now();
            for key in coverage_index_entry_keys(entry, bucket_start, bucket_end) {
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
        }
    }
    write_v2_entry_delta(object_store, entry)?;
    Ok(())
}

fn write_v2_entry_delta<S>(object_store: &S, entry: &ManifestEntry) -> Result<(), DatalensError>
where
    S: ObjectStore,
{
    for bucket in coverage_index_v2_entry_buckets(entry) {
        let delta = CoverageIndexV2Delta {
            schema_version: COVERAGE_INDEX_V2_SCHEMA_VERSION,
            created_at_unix_ms: unix_ms_now()?,
            scope: bucket.scope.clone(),
            bucket_start: bucket.bucket_start,
            bucket_end: bucket.bucket_end,
            replacement: None,
            entries: vec![entry.clone()],
        };
        let key = format!(
            "{}/{}.json",
            coverage_index_v2_delta_prefix(
                &bucket.chain_key,
                &bucket.scope,
                bucket.bucket_start,
                bucket.bucket_end,
            ),
            coverage_index_v2_immutable_id()?
        );
        let bytes = serde_json::to_vec_pretty(&delta).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode coverage index v2 delta: {error}"),
            )
        })?;
        object_store.put(&key, &bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ManifestUpdateFailure,
                format!("write coverage index v2 delta {key}: {}", error.message),
            )
        })?;
        append_cached_v2_bucket_delta(object_store, &bucket, key, &delta);
    }
    Ok(())
}

fn write_v2_replacement_delta<S>(
    object_store: &S,
    replacement_entry: &ManifestEntry,
    replaced_entries: &[ManifestEntry],
    published_entries: &[ManifestEntry],
) -> Result<(), DatalensError>
where
    S: ObjectStore,
{
    let mut buckets = coverage_index_v2_entry_buckets(replacement_entry);
    for entry in replaced_entries.iter().chain(published_entries) {
        buckets.extend(coverage_index_v2_entry_buckets(entry));
    }
    for bucket in buckets {
        let entries = published_entries
            .iter()
            .filter(|entry| coverage_index_v2_entry_buckets(entry).contains(&bucket))
            .cloned()
            .collect::<Vec<_>>();
        let delta = CoverageIndexV2Delta {
            schema_version: COVERAGE_INDEX_V2_SCHEMA_VERSION,
            created_at_unix_ms: unix_ms_now()?,
            scope: bucket.scope.clone(),
            bucket_start: bucket.bucket_start,
            bucket_end: bucket.bucket_end,
            replacement: Some(CoverageIndexV2Replacement {
                entry: replacement_entry.clone(),
            }),
            entries,
        };
        let key = format!(
            "{}/{}.json",
            coverage_index_v2_delta_prefix(
                &bucket.chain_key,
                &bucket.scope,
                bucket.bucket_start,
                bucket.bucket_end,
            ),
            coverage_index_v2_immutable_id()?
        );
        let bytes = serde_json::to_vec_pretty(&delta).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode coverage index v2 replacement delta: {error}"),
            )
        })?;
        object_store.put(&key, &bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ManifestUpdateFailure,
                format!(
                    "write coverage index v2 replacement delta {key}: {}",
                    error.message
                ),
            )
        })?;
        mark_cached_v2_bucket_entries_dirty(object_store, &bucket);
    }
    Ok(())
}

pub(crate) fn write_v2_snapshot<S>(
    object_store: &S,
    chain: &ChainIdentity,
    scope: &str,
    bucket_start: u64,
    bucket_end: u64,
    entries: Vec<ManifestEntry>,
    compacted_delta_keys: Vec<String>,
) -> Result<String, DatalensError>
where
    S: ObjectStore,
{
    let key = format!(
        "{}/{}.json",
        coverage_index_v2_snapshot_prefix(&chain.key_prefix(), scope, bucket_start, bucket_end),
        coverage_index_v2_immutable_id()?
    );
    let snapshot = CoverageIndexV2Snapshot {
        schema_version: COVERAGE_INDEX_V2_SCHEMA_VERSION,
        created_at_unix_ms: unix_ms_now()?,
        scope: scope.to_owned(),
        bucket_start,
        bucket_end,
        entries,
        compacted_delta_keys,
    };
    let bytes = serde_json::to_vec_pretty(&snapshot).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::Internal,
            format!("encode coverage index v2 snapshot: {error}"),
        )
    })?;
    object_store.put(&key, &bytes).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::ManifestUpdateFailure,
            format!("write coverage index v2 snapshot {key}: {}", error.message),
        )
    })?;
    Ok(key)
}

pub(crate) fn write_v2_snapshot_head<S>(
    object_store: &S,
    chain: &ChainIdentity,
    scope: &str,
    bucket_start: u64,
    bucket_end: u64,
    snapshot_key: String,
    included_delta_high_watermark: String,
) -> Result<String, DatalensError>
where
    S: ObjectStore,
{
    let key = format!(
        "{}/{}.json",
        coverage_index_v2_snapshot_head_prefix(
            &chain.key_prefix(),
            scope,
            bucket_start,
            bucket_end,
        ),
        coverage_index_v2_immutable_id()?
    );
    let head = CoverageIndexV2SnapshotHead {
        schema_version: COVERAGE_INDEX_V2_SCHEMA_VERSION,
        created_at_unix_ms: unix_ms_now()?,
        scope: scope.to_owned(),
        bucket_start,
        bucket_end,
        snapshot_key,
        included_delta_high_watermark,
    };
    let bytes = serde_json::to_vec_pretty(&head).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::Internal,
            format!("encode coverage index v2 snapshot head: {error}"),
        )
    })?;
    object_store.put(&key, &bytes).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::ManifestUpdateFailure,
            format!(
                "write coverage index v2 snapshot head {key}: {}",
                error.message
            ),
        )
    })?;
    mark_cached_v2_bucket_entries_dirty(
        object_store,
        &CoverageIndexV2Bucket {
            chain_key: chain.key_prefix(),
            scope: scope.to_owned(),
            bucket_start,
            bucket_end,
        },
    );
    Ok(key)
}

pub(crate) fn write_v2_cleanup_record<S>(
    object_store: &S,
    chain: &ChainIdentity,
    record: CoverageIndexV2CleanupRecord,
) -> Result<String, DatalensError>
where
    S: ObjectStore,
{
    let key = format!(
        "{}/{}/{:020}-{:020}/{}.json",
        coverage_index_v2_cleanup_prefix(&chain.key_prefix()),
        record.scope,
        record.bucket_start,
        record.bucket_end,
        coverage_index_v2_immutable_id()?
    );
    let bytes = serde_json::to_vec_pretty(&record).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::Internal,
            format!("encode coverage index v2 cleanup record: {error}"),
        )
    })?;
    object_store.put(&key, &bytes).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::ManifestUpdateFailure,
            format!(
                "write coverage index v2 cleanup record {key}: {}",
                error.message
            ),
        )
    })?;
    Ok(key)
}

pub(crate) fn unix_ms_now() -> Result<u64, DatalensError> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DatalensError::internal("system clock before unix epoch"))?
        .as_millis() as u64)
}

fn coverage_index_v2_immutable_id() -> Result<String, DatalensError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| DatalensError::internal("system clock before unix epoch"))?;
    let sequence = COVERAGE_INDEX_V2_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let entropy = RandomState::new().hash_one((duration.as_nanos(), process::id(), sequence));
    Ok(format!(
        "{:020}-{:010}-{:020}-{:016x}",
        duration.as_nanos(),
        process::id(),
        sequence,
        entropy
    ))
}

#[allow(dead_code)]
pub(crate) fn replace_entry<S>(
    object_store: &S,
    entry: &ManifestEntry,
) -> Result<CoverageIndexReplacement, DatalensError>
where
    S: ObjectStore,
{
    replace_entry_with_legacy_index(object_store, entry, true)
}

pub(crate) fn replace_entry_with_legacy_index<S>(
    object_store: &S,
    entry: &ManifestEntry,
    legacy_write_enabled: bool,
) -> Result<CoverageIndexReplacement, DatalensError>
where
    S: ObjectStore,
{
    let mut replaced_entries = Vec::new();
    if legacy_write_enabled {
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
    }
    let mut v2_entries = Vec::new();
    read_entries_for_v2_buckets(
        object_store,
        coverage_index_v2_entry_buckets(entry),
        &mut v2_entries,
    )?;
    replaced_entries.extend(v2_entries.into_iter().filter(|existing_entry| {
        replacement_scope_matches(existing_entry, entry)
            && existing_entry.range.intersection(&entry.range).is_some()
    }));
    let mut replaced_manifest = Manifest {
        entries: replaced_entries,
    };
    replaced_manifest.normalize();

    let mut bucket_updates = BTreeSet::new();
    for (bucket_start, bucket_end) in
        bucket_ranges(&entry.range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE)
    {
        bucket_updates.extend(
            coverage_index_entry_keys(entry, bucket_start, bucket_end)
                .into_iter()
                .map(|key| CoverageIndexReplacementBucket {
                    key,
                    bucket_start,
                    bucket_end,
                }),
        );
    }
    for replaced_entry in &replaced_manifest.entries {
        for (bucket_start, bucket_end) in
            bucket_ranges(&replaced_entry.range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE)
        {
            bucket_updates.extend(
                coverage_index_entry_keys(replaced_entry, bucket_start, bucket_end)
                    .into_iter()
                    .map(|key| CoverageIndexReplacementBucket {
                        key,
                        bucket_start,
                        bucket_end,
                    }),
            );
        }
    }

    let mut published_manifest = Manifest {
        entries: replacement_published_entries(&replaced_manifest.entries, entry)?,
    };
    published_manifest.normalize();
    Ok(CoverageIndexReplacement {
        replaced_entries: replaced_manifest.entries,
        published_entries: published_manifest.entries,
        bucket_updates: bucket_updates.into_iter().collect(),
    })
}

pub(crate) fn replacement_from_replaced_entries(
    replaced_entries: Vec<ManifestEntry>,
    entry: &ManifestEntry,
) -> Result<CoverageIndexReplacement, DatalensError> {
    let mut replaced_manifest = Manifest {
        entries: replaced_entries,
    };
    replaced_manifest.normalize();
    let mut published_manifest = Manifest {
        entries: replacement_published_entries(&replaced_manifest.entries, entry)?,
    };
    published_manifest.normalize();
    Ok(CoverageIndexReplacement {
        replaced_entries: replaced_manifest.entries,
        published_entries: published_manifest.entries,
        bucket_updates: Vec::new(),
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

pub(crate) fn read_entries_for_replacement_scope<S>(
    object_store: &S,
    entry: &ManifestEntry,
) -> Result<Vec<ManifestEntry>, DatalensError>
where
    S: ObjectStore,
{
    let mut entries = Vec::new();
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
        entries.extend(index.entries.into_iter().filter(|existing_entry| {
            replacement_scope_matches(existing_entry, entry)
                && existing_entry.range.intersection(&entry.range).is_some()
        }));
    }
    let mut v2_entries = Vec::new();
    read_entries_for_v2_buckets(
        object_store,
        coverage_index_v2_entry_buckets(entry),
        &mut v2_entries,
    )?;
    entries.extend(v2_entries.into_iter().filter(|existing_entry| {
        replacement_scope_matches(existing_entry, entry)
            && existing_entry.range.intersection(&entry.range).is_some()
    }));
    let mut manifest = Manifest { entries };
    manifest.normalize();
    Ok(manifest.entries)
}

#[allow(dead_code)]
pub(crate) fn publish_replacement<S>(
    object_store: &S,
    entry: &ManifestEntry,
    replacement: &CoverageIndexReplacement,
) -> Result<CoverageIndexReplacementPublish, DatalensError>
where
    S: ObjectStore,
{
    publish_replacement_with_legacy_index(object_store, entry, replacement, true)
}

pub(crate) fn publish_replacement_with_legacy_index<S>(
    object_store: &S,
    entry: &ManifestEntry,
    replacement: &CoverageIndexReplacement,
    legacy_write_enabled: bool,
) -> Result<CoverageIndexReplacementPublish, DatalensError>
where
    S: ObjectStore,
{
    if !legacy_write_enabled {
        write_v2_replacement_delta(
            object_store,
            entry,
            &replacement.replaced_entries,
            &replacement.published_entries,
        )?;
        return Ok(CoverageIndexReplacementPublish {
            replaced_entries: replacement.replaced_entries.clone(),
            published_entries: replacement.published_entries.clone(),
        });
    }

    let replacement_buckets = bucket_ranges(&entry.range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE)
        .into_iter()
        .flat_map(|(bucket_start, bucket_end)| {
            coverage_index_entry_keys(entry, bucket_start, bucket_end)
                .into_iter()
                .map(move |key| CoverageIndexReplacementBucket {
                    key,
                    bucket_start,
                    bucket_end,
                })
        })
        .collect::<BTreeSet<_>>();
    let mut pending_buckets = replacement
        .bucket_updates
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut processed_buckets = BTreeSet::new();
    let mut actual_replaced_entries = Vec::new();
    while let Some(bucket) = pending_buckets.pop_first() {
        if !processed_buckets.insert(bucket.clone()) {
            continue;
        }
        let started = Instant::now();
        let key = bucket.key.clone();
        let bucket_range =
            LedgerRange::try_new(entry.range.kind(), bucket.bucket_start, bucket.bucket_end)?;
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
                    let (bucket_start, bucket_end) = discovered_bucket;
                    for key in coverage_index_entry_keys(&existing_entry, bucket_start, bucket_end)
                    {
                        let discovered = CoverageIndexReplacementBucket {
                            key,
                            bucket_start,
                            bucket_end,
                        };
                        if !processed_buckets.contains(&discovered) {
                            pending_buckets.insert(discovered);
                        }
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
        if replacement_buckets.contains(&bucket) {
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
            bucket.bucket_start,
            bucket.bucket_end,
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
    write_v2_replacement_delta(
        object_store,
        entry,
        &replaced_manifest.entries,
        &published_manifest.entries,
    )?;
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
    write_entries_with_legacy_index(object_store, entries, true)
}

pub(crate) fn write_entries_with_legacy_index<S>(
    object_store: &S,
    entries: &[ManifestEntry],
    legacy_write_enabled: bool,
) -> Result<(), DatalensError>
where
    S: ObjectStore,
{
    for entry in entries {
        write_entry_with_legacy_index(object_store, entry, legacy_write_enabled)?;
    }
    Ok(())
}

#[allow(dead_code)]
pub(crate) fn delete_chain<S>(object_store: &S, chain: &ChainIdentity) -> Result<(), DatalensError>
where
    S: ObjectStore,
{
    for prefix in [
        coverage_index_prefix(chain),
        semantic_coverage_index_prefix(chain),
        coverage_index_v2_prefix(chain),
    ] {
        for object in object_store.list(&prefix)? {
            object_store.delete(&object.key).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::ManifestUpdateFailure,
                    format!("delete coverage index {}: {}", object.key, error.message),
                )
            })?;
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn coverage_index_prefix(chain: &ChainIdentity) -> String {
    format!("chains/{}/coverage-index", chain.key_prefix())
}

#[allow(dead_code)]
fn semantic_coverage_index_prefix(chain: &ChainIdentity) -> String {
    format!("chains/{}/coverage-index-semantic", chain.key_prefix())
}

fn coverage_index_v2_prefix(chain: &ChainIdentity) -> String {
    format!("chains/{}/coverage-index-v2", chain.key_prefix())
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

fn semantic_coverage_index_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: &LedgerRange,
    finality_level: ManifestFinalityLevel,
    scope: &str,
    bucket_start: u64,
    bucket_end: u64,
) -> String {
    format!(
        "chains/{}/coverage-index-semantic/{}/{}/{}/{}/{}/{:020}-{:020}.json",
        chain.key_prefix(),
        dataset_key.as_str(),
        range_kind_key(range.kind()),
        finality_level.as_str(),
        EVM_LOG_SEMANTIC_INDEX_VERSION,
        scope,
        bucket_start,
        bucket_end,
    )
}

fn coverage_index_v2_exact_scope(
    dataset_key: &DatasetKey,
    range: &LedgerRange,
    selector_fingerprint: &str,
    finality_level: ManifestFinalityLevel,
) -> String {
    format!(
        "exact/{}/{}/{}/{}",
        dataset_key.as_str(),
        range_kind_key(range.kind()),
        selector_fingerprint,
        finality_level.as_str()
    )
}

fn coverage_index_v2_semantic_scope(
    dataset_key: &DatasetKey,
    range: &LedgerRange,
    finality_level: ManifestFinalityLevel,
    scope: &str,
) -> String {
    format!(
        "semantic/{}/{}/{}/{}/{}",
        dataset_key.as_str(),
        range_kind_key(range.kind()),
        finality_level.as_str(),
        EVM_LOG_SEMANTIC_INDEX_VERSION,
        scope
    )
}

fn coverage_index_v2_delta_prefix(
    chain_key: &str,
    scope: &str,
    bucket_start: u64,
    bucket_end: u64,
) -> String {
    format!(
        "chains/{chain_key}/coverage-index-v2/deltas/{scope}/{bucket_start:020}-{bucket_end:020}"
    )
}

fn coverage_index_v2_snapshot_prefix(
    chain_key: &str,
    scope: &str,
    bucket_start: u64,
    bucket_end: u64,
) -> String {
    format!(
        "chains/{chain_key}/coverage-index-v2/snapshots/{scope}/{bucket_start:020}-{bucket_end:020}"
    )
}

fn coverage_index_v2_snapshot_head_prefix(
    chain_key: &str,
    scope: &str,
    bucket_start: u64,
    bucket_end: u64,
) -> String {
    format!(
        "chains/{chain_key}/coverage-index-v2/snapshot-heads/{scope}/{bucket_start:020}-{bucket_end:020}"
    )
}

fn coverage_index_v2_cleanup_prefix(chain_key: &str) -> String {
    format!("chains/{chain_key}/coverage-index-v2/cleanup")
}

pub(crate) fn parse_v2_bucket_from_object_key(
    prefix: &str,
    key: &str,
) -> Result<Option<CoverageIndexV2Bucket>, DatalensError> {
    let Some(rest) = key.strip_prefix(prefix) else {
        return Ok(None);
    };
    let rest = rest.trim_start_matches('/');
    let Some((bucket_path, file_name)) = rest.rsplit_once('/') else {
        return Ok(None);
    };
    if !file_name.ends_with(".json") {
        return Ok(None);
    }
    let Some((scope, bucket_range)) = bucket_path.rsplit_once('/') else {
        return Ok(None);
    };
    let Some((bucket_start, bucket_end)) = bucket_range.split_once('-') else {
        return Ok(None);
    };
    let bucket_start = bucket_start.parse::<u64>().map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::StorageReadFailure,
            format!("parse coverage index v2 bucket start from {key}: {error}"),
        )
    })?;
    let bucket_end = bucket_end.parse::<u64>().map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::StorageReadFailure,
            format!("parse coverage index v2 bucket end from {key}: {error}"),
        )
    })?;
    let chain_key = prefix
        .strip_prefix("chains/")
        .and_then(|value| value.strip_suffix("/coverage-index-v2/deltas"))
        .ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("parse coverage index v2 chain from prefix {prefix}"),
            )
        })?
        .to_owned();
    Ok(Some(CoverageIndexV2Bucket {
        chain_key,
        scope: scope.to_owned(),
        bucket_start,
        bucket_end,
    }))
}

fn exact_coverage_index_query_keys_for_ranges(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    ranges: &[LedgerRange],
) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for range in ranges {
        for (bucket_start, bucket_end) in bucket_ranges(range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE) {
            for finality_level in [
                ManifestFinalityLevel::Safe,
                ManifestFinalityLevel::Finalized,
            ] {
                keys.insert(coverage_index_key(
                    chain,
                    dataset_key,
                    range,
                    &selector.fingerprint(),
                    finality_level,
                    bucket_start,
                    bucket_end,
                ));
            }
        }
    }
    keys
}

fn exact_coverage_index_v2_query_buckets_for_ranges(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    ranges: &[LedgerRange],
) -> BTreeSet<CoverageIndexV2Bucket> {
    let mut buckets = BTreeSet::new();
    for range in ranges {
        for (bucket_start, bucket_end) in bucket_ranges(range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE) {
            for finality_level in [
                ManifestFinalityLevel::Safe,
                ManifestFinalityLevel::Finalized,
            ] {
                buckets.insert(CoverageIndexV2Bucket {
                    chain_key: chain.key_prefix(),
                    scope: coverage_index_v2_exact_scope(
                        dataset_key,
                        range,
                        &selector.fingerprint(),
                        finality_level,
                    ),
                    bucket_start,
                    bucket_end,
                });
            }
        }
    }
    buckets
}

fn semantic_coverage_index_query_keys_for_ranges(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    ranges: &[LedgerRange],
) -> BTreeSet<String> {
    let scopes = evm_log_query_semantic_scope_groups(dataset_key, selector)
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    semantic_coverage_index_query_keys_for_scopes(chain, dataset_key, selector, ranges, &scopes)
}

fn semantic_coverage_index_query_keys_for_scopes(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    _selector: &DatasetSelector,
    ranges: &[LedgerRange],
    scopes: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut keys = BTreeSet::new();
    for range in ranges {
        for (bucket_start, bucket_end) in bucket_ranges(range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE) {
            for finality_level in [
                ManifestFinalityLevel::Safe,
                ManifestFinalityLevel::Finalized,
            ] {
                keys.extend(scopes.iter().map(|scope| {
                    semantic_coverage_index_key(
                        chain,
                        dataset_key,
                        range,
                        finality_level,
                        scope,
                        bucket_start,
                        bucket_end,
                    )
                }));
            }
        }
    }
    keys
}

fn semantic_coverage_index_v2_query_buckets_for_ranges(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    ranges: &[LedgerRange],
) -> BTreeSet<CoverageIndexV2Bucket> {
    let scopes = evm_log_query_semantic_scope_groups(dataset_key, selector)
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
    semantic_coverage_index_v2_query_buckets_for_scopes(
        chain,
        dataset_key,
        selector,
        ranges,
        &scopes,
    )
}

fn semantic_coverage_index_v2_query_buckets_for_scopes(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    _selector: &DatasetSelector,
    ranges: &[LedgerRange],
    scopes: &BTreeSet<String>,
) -> BTreeSet<CoverageIndexV2Bucket> {
    let mut buckets = BTreeSet::new();
    for range in ranges {
        for (bucket_start, bucket_end) in bucket_ranges(range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE) {
            for finality_level in [
                ManifestFinalityLevel::Safe,
                ManifestFinalityLevel::Finalized,
            ] {
                for scope in scopes {
                    buckets.insert(CoverageIndexV2Bucket {
                        chain_key: chain.key_prefix(),
                        scope: coverage_index_v2_semantic_scope(
                            dataset_key,
                            range,
                            finality_level,
                            scope,
                        ),
                        bucket_start,
                        bucket_end,
                    });
                }
            }
        }
    }
    buckets
}

fn evm_log_query_semantic_scope_groups(
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
) -> Vec<BTreeSet<String>> {
    if *dataset_key != DatasetKey::evm_logs() {
        return Vec::new();
    }
    let DatasetSelector::EvmLogs(filter) = selector else {
        return Vec::new();
    };
    let groups = evm_log_query_semantic_scope_groups_for_filter(filter);
    groups
        .into_iter()
        .filter(|scopes| !scopes.is_empty())
        .collect()
}

fn coverage_index_entry_keys(
    entry: &ManifestEntry,
    bucket_start: u64,
    bucket_end: u64,
) -> Vec<String> {
    let mut keys = BTreeSet::from([coverage_index_key(
        &entry.chain,
        &entry.dataset_key,
        &entry.range,
        &entry.selector_fingerprint,
        entry.finality_level,
        bucket_start,
        bucket_end,
    )]);
    if entry.dataset_key == DatasetKey::evm_logs()
        && let Some(filter) = parse_evm_log_canonical_key(&entry.selector_canonical_key)
    {
        keys.extend(
            evm_log_entry_semantic_scopes_for_entry(entry, &filter)
                .into_iter()
                .map(|scope| {
                    semantic_coverage_index_key(
                        &entry.chain,
                        &entry.dataset_key,
                        &entry.range,
                        entry.finality_level,
                        &scope,
                        bucket_start,
                        bucket_end,
                    )
                }),
        );
    }
    keys.into_iter().collect()
}

fn coverage_index_v2_entry_buckets(entry: &ManifestEntry) -> BTreeSet<CoverageIndexV2Bucket> {
    let mut buckets = BTreeSet::new();
    for (bucket_start, bucket_end) in
        bucket_ranges(&entry.range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE)
    {
        buckets.insert(CoverageIndexV2Bucket {
            chain_key: entry.chain.key_prefix(),
            scope: coverage_index_v2_exact_scope(
                &entry.dataset_key,
                &entry.range,
                &entry.selector_fingerprint,
                entry.finality_level,
            ),
            bucket_start,
            bucket_end,
        });
        if entry.dataset_key == DatasetKey::evm_logs()
            && let Some(filter) = parse_evm_log_canonical_key(&entry.selector_canonical_key)
        {
            for scope in evm_log_entry_semantic_scopes_for_entry(entry, &filter) {
                buckets.insert(CoverageIndexV2Bucket {
                    chain_key: entry.chain.key_prefix(),
                    scope: coverage_index_v2_semantic_scope(
                        &entry.dataset_key,
                        &entry.range,
                        entry.finality_level,
                        &scope,
                    ),
                    bucket_start,
                    bucket_end,
                });
            }
        }
    }
    buckets
}

fn evm_log_entry_semantic_scopes_for_entry(
    entry: &ManifestEntry,
    filter: &EvmLogFilter,
) -> BTreeSet<String> {
    if entry.object_key.is_none() && !filter.addresses().is_empty() {
        return evm_log_address_semantic_scopes(filter);
    }
    evm_log_entry_semantic_scopes(filter)
}

fn evm_log_entry_semantic_scopes(filter: &EvmLogFilter) -> BTreeSet<String> {
    let mut scopes = evm_log_address_semantic_scopes(filter);

    if filter.topics().is_empty()
        || filter
            .topics()
            .iter()
            .all(|topic| matches!(topic, TopicFilter::Wildcard))
    {
        scopes.insert("topic/*".to_owned());
    } else {
        for (index, topic) in filter.topics().iter().enumerate() {
            match topic {
                TopicFilter::Wildcard => {
                    scopes.insert(format!("topic/{index}/*"));
                }
                TopicFilter::AnyOf(values) if values.is_empty() => {
                    scopes.insert(format!("topic/{index}/[]"));
                }
                TopicFilter::AnyOf(values)
                    if values.len() > MAX_EVM_LOG_TOPIC_VALUE_SEMANTIC_KEYS =>
                {
                    scopes.insert(format!("topic/{index}/{EVM_LOG_LARGE_TOPIC_VALUE_SCOPE}"));
                }
                TopicFilter::AnyOf(values) => {
                    scopes.extend(values.iter().map(|value| format!("topic/{index}/{value}")));
                }
            }
        }
    }
    scopes
}

fn evm_log_address_semantic_scopes(filter: &EvmLogFilter) -> BTreeSet<String> {
    let mut scopes = BTreeSet::new();
    if filter.addresses().is_empty() {
        scopes.insert("addr/*".to_owned());
    } else {
        scopes.extend(
            filter
                .addresses()
                .iter()
                .map(|address| format!("addr/{address}")),
        );
    }
    scopes
}

#[cfg(test)]
fn evm_log_query_semantic_scopes(filter: &EvmLogFilter) -> BTreeSet<String> {
    evm_log_query_semantic_scope_groups_for_filter(filter)
        .into_iter()
        .flatten()
        .collect()
}

fn evm_log_query_semantic_scope_groups_for_filter(filter: &EvmLogFilter) -> Vec<BTreeSet<String>> {
    let mut scopes = BTreeSet::new();
    if filter.addresses().is_empty() {
        scopes.insert("addr/*".to_owned());
    } else {
        scopes.extend(
            filter
                .addresses()
                .iter()
                .map(|address| format!("addr/{address}")),
        );
        return vec![scopes, BTreeSet::from(["addr/*".to_owned()])];
    }

    scopes.insert("topic/*".to_owned());
    for (index, topic) in filter.topics().iter().enumerate() {
        scopes.insert(format!("topic/{index}/*"));
        match topic {
            TopicFilter::Wildcard => {}
            TopicFilter::AnyOf(values) if values.is_empty() => {
                scopes.insert(format!("topic/{index}/[]"));
            }
            TopicFilter::AnyOf(values) => {
                scopes.insert(format!("topic/{index}/{EVM_LOG_LARGE_TOPIC_VALUE_SCOPE}"));
                if values.len() <= MAX_EVM_LOG_TOPIC_VALUE_SEMANTIC_KEYS {
                    scopes.extend(values.iter().map(|value| format!("topic/{index}/{value}")));
                }
            }
        }
    }
    vec![scopes]
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
    use crate::LocalObjectStore;
    use datalens_core::{ChainFamily, LogFilter, NetworkId};

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

    #[derive(Clone, Debug)]
    struct NoExistsObjectStore {
        inner: crate::LocalObjectStore,
    }

    impl NoExistsObjectStore {
        fn new(root: std::path::PathBuf) -> Self {
            Self {
                inner: crate::LocalObjectStore::new(root),
            }
        }
    }

    impl ObjectStore for NoExistsObjectStore {
        fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
            self.inner.get(key)
        }

        fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
            self.inner.put(key, bytes)
        }

        fn put_if_absent(
            &self,
            key: &str,
            bytes: &[u8],
        ) -> Result<crate::ObjectPutIfAbsentResult, DatalensError> {
            self.inner.put_if_absent(key, bytes)
        }

        fn exists(&self, _key: &str) -> Result<bool, DatalensError> {
            panic!("coverage index reads should use get and handle missing keys")
        }

        fn list(&self, prefix: &str) -> Result<Vec<crate::ObjectMetadata>, DatalensError> {
            self.inner.list(prefix)
        }

        fn list_page(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            limit: usize,
        ) -> Result<crate::ObjectListPage, DatalensError> {
            self.inner.list_page(prefix, start_after, limit)
        }

        fn delete(&self, key: &str) -> Result<(), DatalensError> {
            self.inner.delete(key)
        }
    }

    #[derive(Clone, Debug)]
    struct DelayedV2ListObjectStore {
        inner: LocalObjectStore,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl DelayedV2ListObjectStore {
        fn new(root: std::path::PathBuf) -> Self {
            Self {
                inner: LocalObjectStore::new(root),
                in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                max_in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn delay_v2_list(&self, prefix: &str) {
            if !prefix.contains("/coverage-index-v2/") {
                return;
            }
            let current = self
                .in_flight
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            self.max_in_flight
                .fetch_max(current, std::sync::atomic::Ordering::SeqCst);
            std::thread::sleep(std::time::Duration::from_millis(50));
            self.in_flight
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl ObjectStore for DelayedV2ListObjectStore {
        fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
            self.inner.get(key)
        }

        fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
            self.inner.put(key, bytes)
        }

        fn put_if_absent(
            &self,
            key: &str,
            bytes: &[u8],
        ) -> Result<crate::ObjectPutIfAbsentResult, DatalensError> {
            self.inner.put_if_absent(key, bytes)
        }

        fn exists(&self, key: &str) -> Result<bool, DatalensError> {
            self.inner.exists(key)
        }

        fn list(&self, prefix: &str) -> Result<Vec<crate::ObjectMetadata>, DatalensError> {
            self.delay_v2_list(prefix);
            self.inner.list(prefix)
        }

        fn list_page(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            limit: usize,
        ) -> Result<crate::ObjectListPage, DatalensError> {
            self.delay_v2_list(prefix);
            self.inner.list_page(prefix, start_after, limit)
        }

        fn delete(&self, key: &str) -> Result<(), DatalensError> {
            self.inner.delete(key)
        }
    }

    #[derive(Clone, Debug)]
    struct DelayedV2DeltaGetObjectStore {
        inner: LocalObjectStore,
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
        delta_gets: Arc<std::sync::atomic::AtomicUsize>,
        list_pages: Arc<std::sync::atomic::AtomicUsize>,
        delta_list_pages: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl DelayedV2DeltaGetObjectStore {
        fn new(root: std::path::PathBuf) -> Self {
            Self {
                inner: LocalObjectStore::new(root),
                in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                max_in_flight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                delta_gets: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                list_pages: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                delta_list_pages: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        fn max_in_flight(&self) -> usize {
            self.max_in_flight.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn delta_gets(&self) -> usize {
            self.delta_gets.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn list_pages(&self) -> usize {
            self.list_pages.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn delta_list_pages(&self) -> usize {
            self.delta_list_pages
                .load(std::sync::atomic::Ordering::SeqCst)
        }

        fn delay_v2_delta_get(&self, key: &str) {
            if !key.contains("/coverage-index-v2/deltas/") {
                return;
            }
            self.delta_gets
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let current = self
                .in_flight
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            self.max_in_flight
                .fetch_max(current, std::sync::atomic::Ordering::SeqCst);
            let delay_ms = if key.contains("0001-slow") { 80 } else { 10 };
            std::thread::sleep(std::time::Duration::from_millis(delay_ms));
            self.in_flight
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[derive(Clone, Debug)]
    struct ForbiddenScopeReadStore {
        inner: LocalObjectStore,
        forbidden: String,
    }

    impl ForbiddenScopeReadStore {
        fn new(inner: LocalObjectStore, forbidden: &str) -> Self {
            Self {
                inner,
                forbidden: forbidden.to_owned(),
            }
        }

        fn check(&self, key: &str) {
            assert!(
                !key.contains(&self.forbidden),
                "unexpected read of forbidden scope {} via key {}",
                self.forbidden,
                key
            );
        }
    }

    impl ObjectStore for ForbiddenScopeReadStore {
        fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
            self.check(key);
            self.inner.get(key)
        }

        fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
            self.inner.put(key, bytes)
        }

        fn put_if_absent(
            &self,
            key: &str,
            bytes: &[u8],
        ) -> Result<crate::ObjectPutIfAbsentResult, DatalensError> {
            self.inner.put_if_absent(key, bytes)
        }

        fn exists(&self, key: &str) -> Result<bool, DatalensError> {
            self.inner.exists(key)
        }

        fn list(&self, prefix: &str) -> Result<Vec<crate::ObjectMetadata>, DatalensError> {
            self.check(prefix);
            self.inner.list(prefix)
        }

        fn list_page(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            limit: usize,
        ) -> Result<crate::ObjectListPage, DatalensError> {
            self.check(prefix);
            if let Some(start_after) = start_after {
                self.check(start_after);
            }
            self.inner.list_page(prefix, start_after, limit)
        }

        fn delete(&self, key: &str) -> Result<(), DatalensError> {
            self.inner.delete(key)
        }
    }

    impl ObjectStore for DelayedV2DeltaGetObjectStore {
        fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
            self.delay_v2_delta_get(key);
            self.inner.get(key)
        }

        fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
            self.inner.put(key, bytes)
        }

        fn put_if_absent(
            &self,
            key: &str,
            bytes: &[u8],
        ) -> Result<crate::ObjectPutIfAbsentResult, DatalensError> {
            self.inner.put_if_absent(key, bytes)
        }

        fn exists(&self, key: &str) -> Result<bool, DatalensError> {
            self.inner.exists(key)
        }

        fn list(&self, prefix: &str) -> Result<Vec<crate::ObjectMetadata>, DatalensError> {
            self.inner.list(prefix)
        }

        fn list_page(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            limit: usize,
        ) -> Result<crate::ObjectListPage, DatalensError> {
            if prefix.contains("/coverage-index-v2/") {
                self.list_pages
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            if prefix.contains("/coverage-index-v2/deltas/") {
                self.delta_list_pages
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            self.inner.list_page(prefix, start_after, limit)
        }

        fn delete(&self, key: &str) -> Result<(), DatalensError> {
            self.inner.delete(key)
        }
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

    fn evm_logs_selector(topics: Vec<Option<Vec<String>>>) -> DatasetSelector {
        DatasetSelector::try_evm_logs(LogFilter {
            addresses: Vec::new(),
            topics,
        })
        .expect("valid evm logs selector")
    }

    fn evm_logs_selector_with_addresses(
        addresses: Vec<String>,
        topics: Vec<Option<Vec<String>>>,
    ) -> DatasetSelector {
        DatasetSelector::try_evm_logs(LogFilter { addresses, topics })
            .expect("valid evm logs selector")
    }

    fn evm_logs_entry(
        chain: &ChainIdentity,
        selector: &DatasetSelector,
        start: u64,
        end: u64,
        rows: usize,
    ) -> ManifestEntry {
        let range = LedgerRange::blocks(start, end).expect("valid range");
        ManifestEntry {
            chain: chain.clone(),
            dataset_key: DatasetKey::evm_logs(),
            range: range.clone(),
            selector_fingerprint: selector.fingerprint(),
            selector_canonical_key: selector.canonical_key(),
            finality_level: ManifestFinalityLevel::Safe,
            object_key: (rows > 0).then(|| {
                crate::helpers::object_key(
                    chain,
                    &DatasetKey::evm_logs(),
                    range,
                    &selector.fingerprint(),
                    crate::ObjectEncoding::ParquetV1,
                )
            }),
            object_encoding: (rows > 0).then_some(crate::ObjectEncoding::ParquetV1),
            object_compression: None,
            row_count: rows,
            object_size_bytes: (rows > 0).then_some(1),
            checksum: (rows > 0).then(|| format!("{start:064x}")),
            checksum_algorithm: (rows > 0).then(|| "sha256".to_owned()),
            written_at_unix_seconds: Some(1),
        }
    }

    fn topic(value: u16) -> String {
        format!("0x{value:064x}")
    }

    fn address(value: u16) -> String {
        format!("0x{value:040x}")
    }

    fn delete_semantic_bucket(
        object_store: &crate::LocalObjectStore,
        chain: &ChainIdentity,
        scope: &str,
    ) {
        let key = semantic_coverage_index_key(
            chain,
            &DatasetKey::evm_logs(),
            &LedgerRange::blocks(10, 10).expect("valid range"),
            ManifestFinalityLevel::Safe,
            scope,
            0,
            99_999,
        );
        object_store.delete(&key).expect("delete semantic bucket");
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

    fn write_test_v2_delta<S: ObjectStore>(
        object_store: &S,
        chain: &ChainIdentity,
        scope: &str,
        range: &LedgerRange,
        id: &str,
        entries: Vec<ManifestEntry>,
        replacement_entry: Option<ManifestEntry>,
    ) -> String {
        let (bucket_start, bucket_end) = bucket_ranges(range, DEFAULT_COVERAGE_INDEX_BUCKET_SIZE)
            .into_iter()
            .next()
            .expect("bucket range");
        let key = format!(
            "{}/{}.json",
            coverage_index_v2_delta_prefix(&chain.key_prefix(), scope, bucket_start, bucket_end),
            id
        );
        let delta = CoverageIndexV2Delta {
            schema_version: COVERAGE_INDEX_V2_SCHEMA_VERSION,
            created_at_unix_ms: 1,
            scope: scope.to_owned(),
            bucket_start,
            bucket_end,
            replacement: replacement_entry.map(|entry| CoverageIndexV2Replacement { entry }),
            entries,
        };
        let bytes = serde_json::to_vec_pretty(&delta).expect("encode test delta");
        object_store.put(&key, &bytes).expect("write test delta");
        append_cached_v2_bucket_delta(
            object_store,
            &CoverageIndexV2Bucket {
                chain_key: chain.key_prefix(),
                scope: scope.to_owned(),
                bucket_start,
                bucket_end,
            },
            key.clone(),
            &delta,
        );
        key
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
    fn test_read_entries_for_query_skips_missing_keys_without_exists_probe() {
        let object_store = NoExistsObjectStore::new(temp_storage_root("direct-get-missing-index"));
        let entries = read_entries_for_query(
            &object_store,
            &test_chain(),
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            &LedgerRange::blocks(1, 10).expect("valid range"),
        )
        .expect("missing coverage index should not fail");

        assert_eq!(entries, None);
    }

    #[test]
    fn test_read_entries_for_v2_buckets_reads_independent_buckets_concurrently() {
        let object_store =
            DelayedV2ListObjectStore::new(temp_storage_root("parallel-v2-bucket-read"));
        let chain = test_chain();
        let buckets = (0..4)
            .map(|index| CoverageIndexV2Bucket {
                chain_key: chain.key_prefix(),
                scope: format!("scope-{index}"),
                bucket_start: index * DEFAULT_COVERAGE_INDEX_BUCKET_SIZE,
                bucket_end: ((index + 1) * DEFAULT_COVERAGE_INDEX_BUCKET_SIZE) - 1,
            })
            .collect::<BTreeSet<_>>();
        let mut entries = Vec::new();

        let has_index = read_entries_for_v2_buckets(&object_store, buckets, &mut entries)
            .expect("read v2 buckets");

        assert!(!has_index);
        assert!(entries.is_empty());
        assert!(
            object_store.max_in_flight() > 1,
            "v2 bucket reads should overlap instead of running serially"
        );
    }

    #[test]
    fn test_read_entries_for_v2_bucket_reads_deltas_concurrently_in_key_order() {
        let object_store =
            DelayedV2DeltaGetObjectStore::new(temp_storage_root("parallel-v2-delta-read"));
        let chain = test_chain();
        let selector = DatasetSelector::all();
        let scope = coverage_index_v2_exact_scope(
            &DatasetKey::evm_blocks(),
            &LedgerRange::blocks(10, 20).expect("valid range"),
            &selector.fingerprint(),
            ManifestFinalityLevel::Safe,
        );
        let bucket = CoverageIndexV2Bucket {
            chain_key: chain.key_prefix(),
            scope: scope.clone(),
            bucket_start: 0,
            bucket_end: DEFAULT_COVERAGE_INDEX_BUCKET_SIZE - 1,
        };
        let existing = empty_entry(&chain, 10, 20);
        let replacement = empty_entry(&chain, 12, 14);
        let existing_range = existing.range.clone();
        let replacement_range = replacement.range.clone();
        write_test_v2_delta(
            &object_store,
            &chain,
            &scope,
            &existing_range,
            "0001-slow",
            vec![existing],
            None,
        );
        write_test_v2_delta(
            &object_store,
            &chain,
            &scope,
            &replacement_range,
            "0002-fast",
            vec![replacement.clone()],
            Some(replacement),
        );
        let mut entries = Vec::new();

        let has_index = read_entries_for_v2_bucket(&object_store, &bucket, &mut entries)
            .expect("read v2 bucket");

        assert!(has_index);
        assert!(
            object_store.max_in_flight() > 1,
            "v2 delta gets should overlap instead of running serially"
        );
        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.range.start(), entry.range.end()))
                .collect::<Vec<_>>(),
            vec![(10, 11), (15, 20), (12, 14)]
        );
    }

    #[test]
    fn test_read_entries_for_v2_bucket_appends_cached_bucket_entry() {
        let object_store =
            DelayedV2DeltaGetObjectStore::new(temp_storage_root("cached-v2-bucket-read"));
        let chain = test_chain();
        let selector = evm_logs_selector(vec![]);
        let scope = coverage_index_v2_exact_scope(
            &DatasetKey::evm_logs(),
            &LedgerRange::blocks(30, 40).expect("valid range"),
            &selector.fingerprint(),
            ManifestFinalityLevel::Safe,
        );
        let bucket = CoverageIndexV2Bucket {
            chain_key: chain.key_prefix(),
            scope: scope.clone(),
            bucket_start: 0,
            bucket_end: DEFAULT_COVERAGE_INDEX_BUCKET_SIZE - 1,
        };
        let first = evm_logs_entry(&chain, &selector, 30, 35, 0);
        let second = evm_logs_entry(&chain, &selector, 36, 40, 0);
        let first_range = first.range.clone();
        let second_range = second.range.clone();
        write_test_v2_delta(
            &object_store,
            &chain,
            &scope,
            &first_range,
            "0001",
            vec![first],
            None,
        );
        write_test_v2_delta(
            &object_store,
            &chain,
            &scope,
            &second_range,
            "0002",
            vec![second],
            None,
        );

        let mut first_read_entries = Vec::new();
        let first_has_index =
            read_entries_for_v2_bucket(&object_store, &bucket, &mut first_read_entries)
                .expect("read v2 bucket first time");
        let delta_gets_after_first_read = object_store.delta_gets();
        let list_pages_after_first_read = object_store.list_pages();
        let delta_list_pages_after_first_read = object_store.delta_list_pages();
        let mut second_read_entries = Vec::new();
        let second_has_index =
            read_entries_for_v2_bucket(&object_store, &bucket, &mut second_read_entries)
                .expect("read v2 bucket second time");

        assert!(first_has_index);
        assert!(second_has_index);
        assert_eq!(delta_gets_after_first_read, 2);
        assert_eq!(object_store.delta_gets(), delta_gets_after_first_read);
        assert_eq!(object_store.list_pages(), list_pages_after_first_read);
        assert_eq!(
            object_store.delta_list_pages(),
            delta_list_pages_after_first_read
        );
        assert_eq!(second_read_entries, first_read_entries);

        let third = evm_logs_entry(&chain, &selector, 41, 42, 0);
        let third_range = third.range.clone();
        write_test_v2_delta(
            &object_store,
            &chain,
            &scope,
            &third_range,
            "0003",
            vec![third],
            None,
        );
        let delta_list_pages_after_append = object_store.delta_list_pages();
        let mut third_read_entries = Vec::new();
        let third_has_index =
            read_entries_for_v2_bucket(&object_store, &bucket, &mut third_read_entries)
                .expect("read v2 bucket after appended delta");

        assert!(third_has_index);
        assert_eq!(object_store.delta_gets(), delta_gets_after_first_read);
        assert_eq!(
            object_store.delta_list_pages(),
            delta_list_pages_after_append
        );
        assert_eq!(
            third_read_entries
                .iter()
                .map(|entry| (entry.range.start(), entry.range.end()))
                .collect::<Vec<_>>(),
            vec![(30, 35), (36, 40), (41, 42)]
        );
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

    #[test]
    fn test_read_entries_for_query_finds_semantic_evm_log_bucket_candidates() {
        let object_store =
            crate::LocalObjectStore::new(temp_storage_root("semantic-evm-log-bucket-candidates"));
        let chain = test_chain();
        let broad = evm_logs_selector(vec![Some(vec![topic(1), topic(2)])]);
        let query = evm_logs_selector(vec![Some(vec![topic(1)])]);
        let entry = evm_logs_entry(&chain, &broad, 10, 20, 1);

        write_entry(&object_store, &entry).expect("write broad evm logs coverage");

        let entries = read_entries_for_query(
            &object_store,
            &chain,
            &DatasetKey::evm_logs(),
            &query,
            &LedgerRange::blocks(10, 20).expect("valid range"),
        )
        .expect("read coverage index")
        .expect("coverage index entries");

        assert_eq!(entries, vec![entry]);
    }

    #[test]
    fn test_read_entries_for_query_finds_all_wildcard_evm_log_coverage_for_specific_query() {
        let object_store =
            crate::LocalObjectStore::new(temp_storage_root("wildcard-evm-log-specific-query"));
        let chain = test_chain();
        let all_logs = evm_logs_selector(Vec::new());
        let query = evm_logs_selector_with_addresses(
            vec![address(1)],
            vec![Some(vec![topic(1)]), Some(vec![topic(2)])],
        );
        let entry = evm_logs_entry(&chain, &all_logs, 10, 20, 1);

        write_entry(&object_store, &entry).expect("write all logs coverage");

        let entries = read_entries_for_query(
            &object_store,
            &chain,
            &DatasetKey::evm_logs(),
            &query,
            &LedgerRange::blocks(10, 20).expect("valid range"),
        )
        .expect("read coverage index")
        .expect("coverage index entries");

        assert_eq!(entries, vec![entry]);
    }

    #[test]
    fn test_read_entries_for_query_with_address_uses_address_semantic_bucket() {
        let object_store =
            crate::LocalObjectStore::new(temp_storage_root("address-semantic-query"));
        let chain = test_chain();
        let stored =
            evm_logs_selector_with_addresses(vec![address(1)], vec![None, Some(vec![topic(2)])]);
        let query = evm_logs_selector_with_addresses(
            vec![address(1)],
            vec![Some(vec![topic(1)]), Some(vec![topic(2)])],
        );
        let entry = evm_logs_entry(&chain, &stored, 10, 20, 1);

        write_entry(&object_store, &entry).expect("write wildcard topic slot coverage");
        delete_semantic_bucket(&object_store, &chain, &format!("topic/1/{}", topic(2)));

        let entries = read_entries_for_query(
            &object_store,
            &chain,
            &DatasetKey::evm_logs(),
            &query,
            &LedgerRange::blocks(10, 20).expect("valid range"),
        )
        .expect("read coverage index")
        .expect("coverage index entries");

        assert_eq!(entries, vec![entry]);
    }

    #[test]
    fn test_read_entries_for_query_with_address_skips_wildcard_addr_when_address_covers() {
        let object_store =
            crate::LocalObjectStore::new(temp_storage_root("address-semantic-before-wildcard"));
        let guarded_store = ForbiddenScopeReadStore::new(object_store.clone(), "/addr/*/");
        let chain = test_chain();
        let all_logs = evm_logs_selector(Vec::new());
        let addressed = evm_logs_selector_with_addresses(
            vec![address(1)],
            vec![Some(vec![topic(1)]), Some(vec![topic(2)])],
        );
        let all_logs_entry = evm_logs_entry(&chain, &all_logs, 10, 20, 1);
        let addressed_entry = evm_logs_entry(&chain, &addressed, 10, 20, 1);

        write_entry(&object_store, &all_logs_entry).expect("write wildcard coverage");
        write_entry(&object_store, &addressed_entry).expect("write address coverage");

        let entries = read_entries_for_query(
            &guarded_store,
            &chain,
            &DatasetKey::evm_logs(),
            &addressed,
            &LedgerRange::blocks(10, 20).expect("valid range"),
        )
        .expect("read coverage index")
        .expect("coverage index entries");

        assert_eq!(entries, vec![addressed_entry]);
    }

    #[test]
    fn test_read_entries_for_query_without_address_uses_topic_semantic_bucket() {
        let object_store = crate::LocalObjectStore::new(temp_storage_root("topic-semantic-query"));
        let chain = test_chain();
        let stored = evm_logs_selector(vec![None, Some(vec![topic(2)])]);
        let query = evm_logs_selector(vec![Some(vec![topic(1)]), Some(vec![topic(2)])]);
        let entry = evm_logs_entry(&chain, &stored, 10, 20, 1);

        write_entry(&object_store, &entry).expect("write wildcard topic slot coverage");
        delete_semantic_bucket(&object_store, &chain, "addr/*");

        let entries = read_entries_for_query(
            &object_store,
            &chain,
            &DatasetKey::evm_logs(),
            &query,
            &LedgerRange::blocks(10, 20).expect("valid range"),
        )
        .expect("read coverage index")
        .expect("coverage index entries");

        assert_eq!(entries, vec![entry]);
    }

    #[test]
    fn test_read_entries_for_query_does_not_use_narrow_evm_logs_for_broad_query() {
        let object_store =
            crate::LocalObjectStore::new(temp_storage_root("narrow-evm-log-does-not-cover-broad"));
        let chain = test_chain();
        let narrow = evm_logs_selector(vec![Some(vec![topic(1)])]);
        let broad_query = evm_logs_selector(vec![Some(vec![topic(1), topic(2)])]);
        let entry = evm_logs_entry(&chain, &narrow, 10, 20, 1);

        write_entry(&object_store, &entry).expect("write narrow evm logs coverage");

        let entries = read_entries_for_query(
            &object_store,
            &chain,
            &DatasetKey::evm_logs(),
            &broad_query,
            &LedgerRange::blocks(10, 20).expect("valid range"),
        )
        .expect("read coverage index");

        assert!(entries.unwrap_or_default().is_empty());
    }

    #[test]
    fn test_read_entries_for_query_ignores_semantically_invalid_empty_evm_log_coverage() {
        let object_store =
            crate::LocalObjectStore::new(temp_storage_root("invalid-empty-evm-log-coverage"));
        let chain = test_chain();
        let empty_narrow = evm_logs_selector(vec![Some(vec![topic(1)])]);
        let broad_query = evm_logs_selector(vec![Some(vec![topic(1), topic(2)])]);
        let entry = evm_logs_entry(&chain, &empty_narrow, 10, 20, 0);

        write_entry(&object_store, &entry).expect("write empty narrow evm logs coverage");

        let entries = read_entries_for_query(
            &object_store,
            &chain,
            &DatasetKey::evm_logs(),
            &broad_query,
            &LedgerRange::blocks(10, 20).expect("valid range"),
        )
        .expect("read coverage index");

        assert!(entries.unwrap_or_default().is_empty());
    }

    #[test]
    fn test_evm_log_semantic_key_generation_bounds_large_topic_value_sets() {
        let chain = test_chain();
        let topics = (0..512).map(topic).collect::<Vec<_>>();
        let selector = evm_logs_selector(vec![Some(topics)]);
        let entry = evm_logs_entry(&chain, &selector, 10, 20, 1);
        let range = LedgerRange::blocks(10, 20).expect("valid range");

        assert!(coverage_index_entry_keys(&entry, 0, 99_999).len() <= 16);
        assert_eq!(
            exact_coverage_index_query_keys_for_ranges(
                &chain,
                &DatasetKey::evm_logs(),
                &selector,
                std::slice::from_ref(&range),
            )
            .len(),
            2
        );
        assert!(
            semantic_coverage_index_query_keys_for_ranges(
                &chain,
                &DatasetKey::evm_logs(),
                &selector,
                std::slice::from_ref(&range),
            )
            .len()
                <= 32
        );
    }

    #[test]
    fn test_evm_log_semantic_scopes_use_large_scope_for_medium_topic_value_sets() {
        let topics = (0..21).map(topic).collect::<Vec<_>>();
        let DatasetSelector::EvmLogs(filter) = evm_logs_selector(vec![Some(topics.clone())]) else {
            panic!("evm logs selector");
        };

        let entry_scopes = evm_log_entry_semantic_scopes(&filter);
        let query_scopes = evm_log_query_semantic_scopes(&filter);
        let large_scope = format!("topic/0/{EVM_LOG_LARGE_TOPIC_VALUE_SCOPE}");

        assert!(entry_scopes.contains(&large_scope));
        assert!(query_scopes.contains(&large_scope));
        for value in topics {
            assert!(!entry_scopes.contains(&format!("topic/0/{value}")));
            assert!(!query_scopes.contains(&format!("topic/0/{value}")));
        }
    }

    #[test]
    fn test_evm_log_query_semantic_scopes_skip_topic_scopes_when_addressed() {
        let DatasetSelector::EvmLogs(filter) = evm_logs_selector_with_addresses(
            vec![address(1), address(2)],
            vec![Some(vec![topic(1)]), None],
        ) else {
            panic!("evm logs selector");
        };

        let scopes = evm_log_query_semantic_scopes(&filter);

        assert!(scopes.contains("addr/*"));
        assert!(scopes.contains(&format!("addr/{}", address(1))));
        assert!(scopes.contains(&format!("addr/{}", address(2))));
        assert!(!scopes.iter().any(|scope| scope.starts_with("topic/")));
        assert_eq!(
            evm_log_query_semantic_scope_groups_for_filter(&filter),
            vec![
                BTreeSet::from([
                    format!("addr/{}", address(1)),
                    format!("addr/{}", address(2)),
                ]),
                BTreeSet::from(["addr/*".to_owned()])
            ]
        );
    }

    #[test]
    fn test_evm_log_empty_coverage_buckets_skip_topic_scopes_when_addressed() {
        let chain = test_chain();
        let selector =
            evm_logs_selector_with_addresses(vec![address(1)], vec![Some(vec![topic(1)])]);
        let entry = evm_logs_entry(&chain, &selector, 10, 20, 0);

        let scopes = coverage_index_v2_entry_buckets(&entry)
            .into_iter()
            .map(|bucket| bucket.scope)
            .collect::<BTreeSet<_>>();

        assert!(scopes.iter().any(|scope| scope.starts_with("exact/")));
        assert!(
            scopes
                .iter()
                .any(|scope| scope.contains(&format!("/addr/{}", address(1))))
        );
        assert!(!scopes.iter().any(|scope| scope.contains("/topic/")));
    }

    #[test]
    fn test_evm_log_data_buckets_keep_topic_scopes_when_addressed() {
        let chain = test_chain();
        let selector =
            evm_logs_selector_with_addresses(vec![address(1)], vec![Some(vec![topic(1)])]);
        let entry = evm_logs_entry(&chain, &selector, 10, 20, 1);

        let scopes = coverage_index_v2_entry_buckets(&entry)
            .into_iter()
            .map(|bucket| bucket.scope)
            .collect::<BTreeSet<_>>();

        assert!(
            scopes
                .iter()
                .any(|scope| scope.contains(&format!("/addr/{}", address(1))))
        );
        assert!(scopes.iter().any(|scope| scope.contains("/topic/")));
    }
}
