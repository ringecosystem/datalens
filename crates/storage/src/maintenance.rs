use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows,
    LedgerRange, LedgerRangeKind, NetworkId,
};
use serde::{Deserialize, Serialize};

use crate::{
    DurableStorage, Manifest, ManifestEntry, ManifestFinalityLevel, ObjectEncoding, ObjectMetadata,
    ObjectPutIfAbsentResult, ObjectStore, ParquetCompression, StorageDataObject, checksum_hex,
    compaction_queue,
    coverage_index::{
        COVERAGE_INDEX_V2_LIST_PAGE_SIZE, CoverageIndexV2Bucket, CoverageIndexV2CleanupRecord,
        CoverageIndexV2CleanupRecordObject, parse_v2_bucket_from_object_key,
        prepare_v2_bucket_compaction, unix_ms_now as coverage_index_v2_unix_ms_now,
        v2_cleanup_record_is_safe_to_delete_with_cache, v2_snapshot_cleanup_records_for_bucket,
        write_v2_cleanup_record, write_v2_snapshot, write_v2_snapshot_head,
    },
    decode_object_rows, encode_object_rows, manifest_key, manifest_segment_prefix, range_kind_key,
    unix_seconds_now, verify_manifest_object_metadata,
};

const MAX_SPARSE_SOURCE_RANGES_PER_CANDIDATE: usize = 3;
const MAX_SPARSE_CANDIDATE_RANGE_SPAN_BLOCKS: u64 = 100_000;
const COVERAGE_INDEX_V2_CLEANUP_RECORDS_PER_TICK: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceReport {
    pub read_only: bool,
    pub mode: MaintenanceOperationMode,
    pub operations: Vec<MaintenanceOperation>,
    pub check: MaintenanceCheckReport,
    pub fragmentation: MaintenanceFragmentationReport,
    pub compaction_backlog: MaintenanceCompactionBacklogReport,
    pub compaction: MaintenanceCompactionReport,
    pub compaction_reconciliation: MaintenanceCompactionReconciliationReport,
    pub retention: MaintenanceRetentionReport,
    pub usage_ledger: MaintenanceUsageLedgerReport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceOperationMode {
    ReadOnly,
    DryRun,
    Execute,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceOperation {
    InspectCheck,
    Compact,
    Repair,
    RetentionPrune,
    UsageLedgerRollup,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceCheckReport {
    pub issue_count: usize,
    pub issues: Vec<MaintenanceIssue>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceIssue {
    pub issue_kind: MaintenanceIssueKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chain: Option<ChainIdentity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_key: Option<DatasetKey>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selector_fingerprint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub range: Option<LedgerRange>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object_key: Option<String>,
    pub message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceIssueKind {
    ManifestDecodeFailure,
    MissingObject,
    ObjectSizeMismatch,
    ObjectChecksumMismatch,
    UnknownChecksumAlgorithm,
    ObjectDecodeFailure,
    ContradictoryCoverage,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceFragmentationReport {
    pub data_object_small_object_count: usize,
    pub data_object_small_object_bytes: u64,
    pub manifest_segment_count: usize,
    pub coverage_delta_count: usize,
    pub coverage_delta_bytes: u64,
    pub coverage_snapshot_count: usize,
    pub coverage_snapshot_age_ms_max: u64,
    pub coverage_cleanup_record_count: usize,
    pub coverage_delta_backlog_top: Vec<CoverageDeltaBacklogScope>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoverageDeltaBacklogScope {
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub scope_kind: String,
    pub scope_class: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector_fingerprint: Option<String>,
    pub bucket_start: u64,
    pub bucket_end: u64,
    pub object_count: usize,
    pub bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceCompactionBacklogReport {
    pub min_object_bytes: u64,
    pub small_object_count: usize,
    pub small_object_bytes: u64,
    pub manifest_segment_count: usize,
    pub chains: Vec<MaintenanceCompactionBacklogChain>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceCompactionBacklogChain {
    pub chain: ChainIdentity,
    pub small_object_count: usize,
    pub small_object_bytes: u64,
    pub manifest_segment_count: usize,
    pub datasets: Vec<MaintenanceCompactionBacklogDataset>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceCompactionBacklogDataset {
    pub dataset_key: DatasetKey,
    pub small_object_count: usize,
    pub small_object_bytes: u64,
    pub selectors: Vec<MaintenanceCompactionBacklogSelector>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceCompactionBacklogSelector {
    pub selector_fingerprint: String,
    pub selector_canonical_key: String,
    pub small_object_count: usize,
    pub small_object_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceCompactionReport {
    pub read_only: bool,
    pub candidates: Vec<CompactionCandidate>,
    pub candidate_count: usize,
    pub candidate_backlog: usize,
    pub backlog: Vec<CompactionBacklogScope>,
    pub processed_candidates: usize,
    pub duration_ms: u64,
    pub tick_status: MaintenanceCompactionTickStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause_reason: Option<String>,
    pub tick_summary: MaintenanceCompactionTickSummary,
    pub compacted_objects: usize,
    pub compacted_rows: usize,
    pub deleted_source_objects: usize,
    pub source_delete_failures: usize,
    pub get_operations: usize,
    pub put_operations: usize,
    pub delete_operations: usize,
    #[serde(default)]
    pub coverage_index_v2_compacted_buckets: usize,
    #[serde(default)]
    pub coverage_index_v2_compacted_deltas: usize,
    #[serde(default)]
    pub coverage_index_v2_input_delta_bytes: u64,
    #[serde(default)]
    pub coverage_index_v2_cleanup_records: usize,
    #[serde(default)]
    pub coverage_index_v2_deleted_deltas: usize,
    #[serde(default)]
    pub coverage_index_v2_delta_delete_failures: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceCompactionTickSummary {
    pub input_objects: usize,
    pub output_objects: usize,
    pub deleted_source_objects: usize,
    pub deleted_manifest_segments: usize,
    pub duration_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompactionBacklogScope {
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector_fingerprint: String,
    pub selector_canonical_key: String,
    pub selector_kind: String,
    pub small_objects: usize,
    pub manifest_segments: usize,
    pub candidate_backlog: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceCompactionReconciliationReport {
    pub read_only: bool,
    pub orphan_compacted_objects: Vec<String>,
    pub stale_source_objects: Vec<String>,
    pub stale_cleanup_records: Vec<String>,
    pub deleted_orphan_compacted_objects: usize,
    pub deleted_stale_source_objects: usize,
    pub deleted_stale_cleanup_records: usize,
    pub delete_failures: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct SupersededCompactionSource {
    schema_version: u32,
    object_key: String,
    superseded_at_unix_ms: u64,
    delete_after_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceCompactionTickStatus {
    Completed,
    Partial,
    Paused,
    Failed,
}

impl MaintenanceCompactionTickStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Partial => "partial",
            Self::Paused => "paused",
            Self::Failed => "failed",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceCompactionConfig {
    pub min_object_bytes: u64,
    pub target_object_bytes: u64,
    pub max_output_object_bytes: u64,
    pub max_input_objects_per_candidate: usize,
    pub max_input_bytes_per_candidate: u64,
    pub max_tick_duration_ms: u64,
    pub max_candidates_per_tick: usize,
    pub max_concurrent_candidates: usize,
    pub max_manifest_entries_per_tick: usize,
    pub max_gets_per_tick: usize,
    pub max_puts_per_tick: usize,
    pub max_deletes_per_tick: usize,
    pub query_latency_pause_threshold_ms: u64,
    pub write_latency_pause_threshold_ms: u64,
    pub pressure_pause_ms: u64,
    pub pressure: MaintenanceCompactionPressure,
    pub cleanup_enabled: bool,
    pub delete_source_objects: bool,
    pub source_delete_grace_ms: u64,
    pub validate_coverage_index_sources: bool,
    pub coverage_index_v2_delta_count_threshold: usize,
    pub coverage_index_v2_delete_grace_ms: u64,
}

impl Default for MaintenanceCompactionConfig {
    fn default() -> Self {
        Self {
            min_object_bytes: u64::MAX,
            target_object_bytes: 64 * 1024 * 1024,
            max_output_object_bytes: 128 * 1024 * 1024,
            max_input_objects_per_candidate: 512,
            max_input_bytes_per_candidate: 128 * 1024 * 1024,
            max_tick_duration_ms: 30_000,
            max_candidates_per_tick: 1,
            max_concurrent_candidates: 1,
            max_manifest_entries_per_tick: 20_000,
            max_gets_per_tick: 64,
            max_puts_per_tick: 8,
            max_deletes_per_tick: 64,
            query_latency_pause_threshold_ms: 0,
            write_latency_pause_threshold_ms: 0,
            pressure_pause_ms: 60_000,
            pressure: MaintenanceCompactionPressure::default(),
            cleanup_enabled: false,
            delete_source_objects: false,
            source_delete_grace_ms: 300_000,
            validate_coverage_index_sources: true,
            coverage_index_v2_delta_count_threshold: 64,
            coverage_index_v2_delete_grace_ms: 300_000,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceCompactionPressure {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_latency_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_latency_ms: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct MaintenanceCompactionPressureMonitor {
    inner: Arc<MaintenanceCompactionPressureMonitorInner>,
}

#[derive(Debug)]
struct MaintenanceCompactionPressureMonitorInner {
    query_latency_ms: AtomicU64,
    write_latency_ms: AtomicU64,
}

impl Default for MaintenanceCompactionPressureMonitorInner {
    fn default() -> Self {
        Self {
            query_latency_ms: AtomicU64::new(u64::MAX),
            write_latency_ms: AtomicU64::new(u64::MAX),
        }
    }
}

impl MaintenanceCompactionPressureMonitor {
    pub fn record_query_latency(&self, duration: Duration) {
        self.inner
            .query_latency_ms
            .store(duration_millis_value(duration), Ordering::Relaxed);
    }

    pub fn record_write_latency(&self, duration: Duration) {
        self.inner
            .write_latency_ms
            .store(duration_millis_value(duration), Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MaintenanceCompactionPressure {
        MaintenanceCompactionPressure {
            query_latency_ms: optional_latency(self.inner.query_latency_ms.load(Ordering::Relaxed)),
            write_latency_ms: optional_latency(self.inner.write_latency_ms.load(Ordering::Relaxed)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompactionCandidate {
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector_fingerprint: String,
    pub selector_canonical_key: String,
    pub range_kind: String,
    pub finality_level: ManifestFinalityLevel,
    pub object_encoding: ObjectEncoding,
    pub range: LedgerRange,
    pub entry_count: usize,
    pub input_object_bytes: u64,
    pub target_object_bytes: u64,
    pub max_output_object_bytes: u64,
    pub object_keys: Vec<String>,
    pub source_ranges: Vec<LedgerRange>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceRetentionReport {
    pub mode: MaintenanceOperationMode,
    pub policy: RetentionPolicy,
    pub protected_current_objects: Vec<String>,
    pub delete_candidates: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub delete_current_manifest_objects: bool,
    pub require_unreferenced_manifest_proof: bool,
    pub usage_ledger_extends_recent_ranges: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceUsageLedgerReport {
    pub rollup_model: UsageLedgerRollupModel,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UsageLedgerRollupModel {
    pub source: String,
    pub rollup: String,
    pub retention_relationship: String,
    pub secrets_policy: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct CoverageIndexV2CreatedAt {
    #[serde(default)]
    created_at_unix_ms: u64,
}

impl<S> DurableStorage<S>
where
    S: ObjectStore,
{
    pub fn maintenance_report(&self) -> Result<MaintenanceReport, DatalensError> {
        let manifest_objects = self.object_store().list("chains")?;
        let mut manifest_keys = manifest_objects
            .iter()
            .filter(|object| is_manifest_object(&object.key))
            .map(|object| object.key.clone())
            .collect::<Vec<_>>();
        manifest_keys.sort_by_key(|key| (key.contains("/manifest-segments/"), key.clone()));

        let mut issues = Vec::new();
        let mut manifest = Manifest::default();
        let mut raw_entries = Vec::new();
        for manifest_key in manifest_keys {
            let bytes = self.object_store().get(&manifest_key)?;
            match serde_json::from_slice::<Manifest>(&bytes) {
                Ok(object_manifest) => {
                    raw_entries.extend(object_manifest.entries.clone());
                    manifest.merge(object_manifest);
                }
                Err(error) => issues.push(MaintenanceIssue {
                    issue_kind: MaintenanceIssueKind::ManifestDecodeFailure,
                    chain: None,
                    dataset_key: None,
                    selector_fingerprint: None,
                    range: None,
                    object_key: Some(manifest_key),
                    message: format!("decode manifest: {error}"),
                }),
            }
        }
        let entries = manifest.entries;

        issues.extend(self.check_entries(&entries)?);
        issues.extend(contradictory_coverage_issues(&entries));

        let compaction_config = MaintenanceCompactionConfig::default();
        let candidates = compaction_candidates(&entries, compaction_config);
        let compaction_backlog =
            compaction_backlog_report(&entries, &raw_entries, &manifest_objects, compaction_config);
        let coverage_fragmentation = coverage_index_v2_fragmentation_report(self.object_store())?;
        let fragmentation = MaintenanceFragmentationReport {
            data_object_small_object_count: compaction_backlog.small_object_count,
            data_object_small_object_bytes: compaction_backlog.small_object_bytes,
            manifest_segment_count: compaction_backlog.manifest_segment_count,
            coverage_delta_count: coverage_fragmentation.coverage_delta_count,
            coverage_delta_bytes: coverage_fragmentation.coverage_delta_bytes,
            coverage_snapshot_count: coverage_fragmentation.coverage_snapshot_count,
            coverage_snapshot_age_ms_max: coverage_fragmentation.coverage_snapshot_age_ms_max,
            coverage_cleanup_record_count: coverage_fragmentation.coverage_cleanup_record_count,
            coverage_delta_backlog_top: coverage_fragmentation.coverage_delta_backlog_top,
        };
        let compaction_reconciliation =
            self.compaction_reconciliation_report(&entries, &raw_entries, compaction_config, true)?;
        let protected_current_objects = current_object_keys(&entries);
        let delete_candidates = self.retention_delete_candidates(&protected_current_objects)?;

        Ok(MaintenanceReport {
            read_only: true,
            mode: MaintenanceOperationMode::DryRun,
            operations: vec![
                MaintenanceOperation::InspectCheck,
                MaintenanceOperation::Compact,
                MaintenanceOperation::Repair,
                MaintenanceOperation::RetentionPrune,
                MaintenanceOperation::UsageLedgerRollup,
            ],
            check: MaintenanceCheckReport {
                issue_count: issues.len(),
                issues,
            },
            fragmentation,
            compaction_backlog,
            compaction: MaintenanceCompactionReport {
                read_only: true,
                candidate_count: candidates.len(),
                candidate_backlog: candidates.len(),
                backlog: compaction_backlog_scopes(&candidates),
                candidates,
                processed_candidates: 0,
                duration_ms: 0,
                tick_status: MaintenanceCompactionTickStatus::Completed,
                pause_reason: None,
                tick_summary: MaintenanceCompactionTickSummary::default(),
                compacted_objects: 0,
                compacted_rows: 0,
                deleted_source_objects: 0,
                source_delete_failures: 0,
                get_operations: 0,
                put_operations: 0,
                delete_operations: 0,
                coverage_index_v2_compacted_buckets: 0,
                coverage_index_v2_compacted_deltas: 0,
                coverage_index_v2_input_delta_bytes: 0,
                coverage_index_v2_cleanup_records: 0,
                coverage_index_v2_deleted_deltas: 0,
                coverage_index_v2_delta_delete_failures: 0,
            },
            compaction_reconciliation,
            retention: MaintenanceRetentionReport {
                mode: MaintenanceOperationMode::DryRun,
                policy: RetentionPolicy {
                    delete_current_manifest_objects: false,
                    require_unreferenced_manifest_proof: true,
                    usage_ledger_extends_recent_ranges: true,
                },
                protected_current_objects,
                delete_candidates,
            },
            usage_ledger: MaintenanceUsageLedgerReport {
                rollup_model: UsageLedgerRollupModel {
                    source: "append_only_jsonl_events".to_owned(),
                    rollup: "future dry-run aggregation may summarize by application, chain, dataset, selector, range, finality, and day".to_owned(),
                    retention_relationship: "usage ledger may extend durable data retention for recently used ranges, but it does not create manifest coverage".to_owned(),
                    secrets_policy: "ledger and maintenance output must not include credentials, tokens, or raw authorization headers".to_owned(),
                },
            },
        })
    }

    pub fn compact_small_objects(
        &self,
        config: MaintenanceCompactionConfig,
    ) -> Result<MaintenanceCompactionReport, DatalensError> {
        let started = Instant::now();
        let manifest = self.manifest()?;
        let entries = manifest
            .entries
            .into_iter()
            .map(|entry| SelectedManifestEntry {
                segment_key: None,
                cursor_key: None,
                entry,
            })
            .collect::<Vec<_>>();
        self.compact_selected_manifest_entries(CompactSelectedManifestEntriesArgs {
            entries,
            stale_queue_entry_keys: Vec::new(),
            config,
            started,
            cursor: None,
            scan_partial: false,
            chain: None,
            checkpoint: &|| Ok(()),
            coverage_index_v2_checked: false,
            operation_budget: None,
        })
    }

    pub fn compact_small_objects_for_chain(
        &self,
        chain: &ChainIdentity,
        config: MaintenanceCompactionConfig,
    ) -> Result<MaintenanceCompactionReport, DatalensError> {
        self.compact_small_objects_for_chain_with_checkpoint(chain, config, || Ok(()))
    }

    pub fn compact_small_objects_for_chain_with_checkpoint(
        &self,
        chain: &ChainIdentity,
        config: MaintenanceCompactionConfig,
        checkpoint: impl Fn() -> Result<(), DatalensError>,
    ) -> Result<MaintenanceCompactionReport, DatalensError> {
        let started = Instant::now();
        log::info!(
            "storage compaction tick started chain_key={} max_tick_duration_ms={} max_candidates_per_tick={} max_manifest_entries_per_tick={}",
            chain.key_prefix(),
            config.max_tick_duration_ms,
            config.max_candidates_per_tick,
            config.max_manifest_entries_per_tick
        );
        if config.cleanup_enabled || config.coverage_index_v2_delta_count_threshold > 0 {
            let mut operation_budget = CompactionOperationBudget::new(config);
            let (coverage_index_v2_report, v2_partial) = self
                .compact_coverage_index_v2_for_chain_inner(
                    Some(chain),
                    config,
                    started,
                    &checkpoint,
                    &mut operation_budget,
                )?;
            if coverage_index_v2_report_has_work(&coverage_index_v2_report) || v2_partial {
                if started.elapsed() >= Duration::from_millis(config.max_tick_duration_ms.max(1)) {
                    return Ok(coverage_index_v2_only_compaction_report(
                        started,
                        v2_partial,
                        operation_budget,
                        coverage_index_v2_report,
                    ));
                }
                let scan = self.scan_compaction_manifest_entries(chain, config, started)?;
                let mut report =
                    self.compact_selected_manifest_entries(CompactSelectedManifestEntriesArgs {
                        entries: scan.entries,
                        stale_queue_entry_keys: scan.stale_queue_entry_keys,
                        config,
                        started,
                        cursor: Some(scan.cursor_update),
                        scan_partial: scan.partial || v2_partial,
                        chain: Some(chain),
                        checkpoint: &checkpoint,
                        coverage_index_v2_checked: true,
                        operation_budget: Some(operation_budget),
                    })?;
                merge_coverage_index_v2_report(&mut report, coverage_index_v2_report);
                return Ok(report);
            }
        }
        let scan = self.scan_compaction_manifest_entries(chain, config, started)?;
        let report =
            self.compact_selected_manifest_entries(CompactSelectedManifestEntriesArgs {
                entries: scan.entries,
                stale_queue_entry_keys: scan.stale_queue_entry_keys,
                config,
                started,
                cursor: Some(scan.cursor_update),
                scan_partial: scan.partial,
                chain: Some(chain),
                checkpoint: &checkpoint,
                coverage_index_v2_checked: true,
                operation_budget: None,
            })?;
        Ok(report)
    }

    pub fn reconcile_compaction_for_chain(
        &self,
        chain: &ChainIdentity,
        config: MaintenanceCompactionConfig,
    ) -> Result<MaintenanceCompactionReconciliationReport, DatalensError> {
        self.reconcile_compaction_for_chain_with_checkpoint(chain, config, || Ok(()))
    }

    pub fn reconcile_compaction_for_chain_with_checkpoint(
        &self,
        chain: &ChainIdentity,
        config: MaintenanceCompactionConfig,
        checkpoint: impl Fn() -> Result<(), DatalensError>,
    ) -> Result<MaintenanceCompactionReconciliationReport, DatalensError> {
        let current_entries = self.manifest_for_chain(chain)?.entries;
        let raw_entries = self.raw_manifest_entries_for_chain(chain)?;
        let mut report = self.compaction_reconciliation_report_for_chain(
            chain,
            &current_entries,
            &raw_entries,
            config,
            false,
        )?;
        let chain_prefix = format!("chains/{}/", chain.key_prefix());
        report
            .orphan_compacted_objects
            .retain(|object_key| object_key.starts_with(&chain_prefix));
        report
            .stale_source_objects
            .retain(|object_key| object_key.starts_with(&chain_prefix));
        report
            .stale_cleanup_records
            .retain(|object_key| object_key.starts_with(&chain_prefix));

        if config.cleanup_enabled {
            for object_key in report.orphan_compacted_objects.clone() {
                checkpoint()?;
                match self.object_store().delete(&object_key) {
                    Ok(()) => report.deleted_orphan_compacted_objects += 1,
                    Err(error) => {
                        report.delete_failures += 1;
                        log::warn!(
                            "storage compaction reconciliation orphan delete failed chain_key={} object_key={} kind={:?} message={}",
                            chain.key_prefix(),
                            object_key,
                            error.kind,
                            error.message
                        );
                    }
                }
            }
            if config.delete_source_objects {
                let eligible_records =
                    self.superseded_source_records_for_chain(chain, config.source_delete_grace_ms)?;
                for (record_key, record) in eligible_records
                    .into_iter()
                    .take(config.max_deletes_per_tick)
                {
                    checkpoint()?;
                    if self.compaction_source_is_current(chain, &record.object_key)? {
                        log::info!(
                            "storage compaction reconciliation source delete skipped current manifest object chain_key={} object_key={} record_key={}",
                            chain.key_prefix(),
                            record.object_key,
                            record_key
                        );
                        match self.object_store().delete(&record_key) {
                            Ok(()) => report.deleted_stale_cleanup_records += 1,
                            Err(error) => {
                                report.delete_failures += 1;
                                log::warn!(
                                    "storage compaction reconciliation source cleanup record delete failed chain_key={} object_key={} record_key={} kind={:?} message={}",
                                    chain.key_prefix(),
                                    record.object_key,
                                    record_key,
                                    error.kind,
                                    error.message
                                );
                            }
                        }
                        continue;
                    }
                    match self.object_store().delete(&record.object_key) {
                        Ok(()) => report.deleted_stale_source_objects += 1,
                        Err(error) => {
                            report.delete_failures += 1;
                            log::warn!(
                                "storage compaction reconciliation source delete failed chain_key={} object_key={} kind={:?} message={}",
                                chain.key_prefix(),
                                record.object_key,
                                error.kind,
                                error.message
                            );
                            continue;
                        }
                    }
                    checkpoint()?;
                    match self.object_store().delete(&record_key) {
                        Ok(()) => report.deleted_stale_cleanup_records += 1,
                        Err(error) => {
                            report.delete_failures += 1;
                            log::warn!(
                                "storage compaction reconciliation source cleanup record delete failed chain_key={} object_key={} record_key={} kind={:?} message={}",
                                chain.key_prefix(),
                                record.object_key,
                                record_key,
                                error.kind,
                                error.message
                            );
                        }
                    }
                }
            }
            for object_key in report.stale_cleanup_records.clone() {
                checkpoint()?;
                match self.object_store().delete(&object_key) {
                    Ok(()) => report.deleted_stale_cleanup_records += 1,
                    Err(error) => {
                        report.delete_failures += 1;
                        log::warn!(
                            "storage compaction reconciliation cleanup record delete failed chain_key={} object_key={} kind={:?} message={}",
                            chain.key_prefix(),
                            object_key,
                            error.kind,
                            error.message
                        );
                    }
                }
            }
        }
        Ok(report)
    }

    pub fn cleanup_superseded_sources_for_chain(
        &self,
        chain: &ChainIdentity,
        config: MaintenanceCompactionConfig,
    ) -> Result<MaintenanceCompactionReconciliationReport, DatalensError> {
        self.cleanup_superseded_sources_for_chain_with_checkpoint(chain, config, || Ok(()))
    }

    pub fn cleanup_superseded_sources_for_chain_with_checkpoint(
        &self,
        chain: &ChainIdentity,
        config: MaintenanceCompactionConfig,
        checkpoint: impl Fn() -> Result<(), DatalensError>,
    ) -> Result<MaintenanceCompactionReconciliationReport, DatalensError> {
        let mut report = MaintenanceCompactionReconciliationReport {
            read_only: false,
            orphan_compacted_objects: Vec::new(),
            stale_source_objects: Vec::new(),
            stale_cleanup_records: Vec::new(),
            deleted_orphan_compacted_objects: 0,
            deleted_stale_source_objects: 0,
            deleted_stale_cleanup_records: 0,
            delete_failures: 0,
        };
        if !config.cleanup_enabled || !config.delete_source_objects {
            return Ok(report);
        }
        let eligible_records =
            self.superseded_source_records_for_chain(chain, config.source_delete_grace_ms)?;
        for (record_key, record) in eligible_records
            .into_iter()
            .take(config.max_deletes_per_tick)
        {
            checkpoint()?;
            if self.compaction_source_is_current(chain, &record.object_key)? {
                log::info!(
                    "storage compaction source delete skipped current manifest object chain_key={} object_key={} record_key={}",
                    chain.key_prefix(),
                    record.object_key,
                    record_key
                );
                match self.object_store().delete(&record_key) {
                    Ok(()) => report.deleted_stale_cleanup_records += 1,
                    Err(error) => {
                        report.delete_failures += 1;
                        log::warn!(
                            "storage compaction source cleanup record delete failed chain_key={} object_key={} record_key={} kind={:?} message={}",
                            chain.key_prefix(),
                            record.object_key,
                            record_key,
                            error.kind,
                            error.message
                        );
                    }
                }
                continue;
            }
            report.stale_source_objects.push(record.object_key.clone());
            match self.object_store().delete(&record.object_key) {
                Ok(()) => report.deleted_stale_source_objects += 1,
                Err(error) => {
                    report.delete_failures += 1;
                    log::warn!(
                        "storage compaction source delete failed chain_key={} object_key={} kind={:?} message={}",
                        chain.key_prefix(),
                        record.object_key,
                        error.kind,
                        error.message
                    );
                    continue;
                }
            }
            checkpoint()?;
            match self.object_store().delete(&record_key) {
                Ok(()) => report.deleted_stale_cleanup_records += 1,
                Err(error) => {
                    report.delete_failures += 1;
                    log::warn!(
                        "storage compaction source cleanup record delete failed chain_key={} object_key={} record_key={} kind={:?} message={}",
                        chain.key_prefix(),
                        record.object_key,
                        record_key,
                        error.kind,
                        error.message
                    );
                }
            }
        }
        Ok(report)
    }

    fn compaction_source_is_current(
        &self,
        chain: &ChainIdentity,
        object_key: &str,
    ) -> Result<bool, DatalensError> {
        Ok(
            current_object_keys(&self.manifest_for_chain(chain)?.entries)
                .iter()
                .any(|current_key| current_key == object_key),
        )
    }

    fn compact_selected_manifest_entries(
        &self,
        args: CompactSelectedManifestEntriesArgs<'_>,
    ) -> Result<MaintenanceCompactionReport, DatalensError> {
        let CompactSelectedManifestEntriesArgs {
            entries,
            stale_queue_entry_keys,
            config,
            started,
            cursor,
            scan_partial,
            chain,
            checkpoint,
            coverage_index_v2_checked,
            operation_budget,
        } = args;
        let build_started = Instant::now();
        let manifest_entries = entries
            .iter()
            .map(|entry| entry.entry.clone())
            .collect::<Vec<_>>();
        let candidates = compaction_candidates(&manifest_entries, config);
        let pause_reason = compaction_pressure_pause_reason(config);
        log::info!(
            "storage compaction candidate build candidate_count={} entry_count={} duration_ms={}",
            candidates.len(),
            manifest_entries.len(),
            build_started.elapsed().as_millis()
        );
        if let Some(reason) = pause_reason {
            let duration_ms = duration_millis(started);
            log::info!(
                "storage compaction tick summary status=paused pause_reason={} input_objects=0 output_objects=0 deleted_source_objects=0 deleted_manifest_segments=0 duration_ms={}",
                reason,
                duration_ms
            );
            return Ok(MaintenanceCompactionReport {
                read_only: false,
                candidate_count: candidates.len(),
                candidate_backlog: candidates.len(),
                backlog: compaction_backlog_scopes(&candidates),
                candidates,
                processed_candidates: 0,
                duration_ms,
                tick_status: MaintenanceCompactionTickStatus::Paused,
                pause_reason: Some(reason.to_owned()),
                tick_summary: MaintenanceCompactionTickSummary {
                    duration_ms,
                    ..MaintenanceCompactionTickSummary::default()
                },
                compacted_objects: 0,
                compacted_rows: 0,
                deleted_source_objects: 0,
                source_delete_failures: 0,
                get_operations: 0,
                put_operations: 0,
                delete_operations: 0,
                coverage_index_v2_compacted_buckets: 0,
                coverage_index_v2_compacted_deltas: 0,
                coverage_index_v2_input_delta_bytes: 0,
                coverage_index_v2_cleanup_records: 0,
                coverage_index_v2_deleted_deltas: 0,
                coverage_index_v2_delta_delete_failures: 0,
            });
        }
        let mut compacted_objects = 0usize;
        let mut compacted_rows = 0usize;
        let mut processed_candidates = 0usize;
        let mut input_objects = 0usize;
        let mut deleted_manifest_segments = BTreeSet::new();
        let deleted_source_objects = 0usize;
        let mut source_delete_failures = 0usize;
        let mut cursor_advance_key = None;
        let max_candidates = config
            .max_candidates_per_tick
            .max(1)
            .min(config.max_concurrent_candidates.max(1));
        let max_duration = Duration::from_millis(config.max_tick_duration_ms.max(1));
        let mut partial = scan_partial;
        let mut cleanup_incomplete = false;
        let mut operation_budget =
            operation_budget.unwrap_or_else(|| CompactionOperationBudget::new(config));
        let mut coverage_index_v2_report = CoverageIndexV2CompactionReport::default();

        for candidate in &candidates {
            if processed_candidates >= max_candidates || started.elapsed() >= max_duration {
                partial = true;
                break;
            }
            let candidate_started = Instant::now();
            let selected_entries = candidate_selected_entries(&entries, candidate);
            if selected_entries.len() != candidate.entry_count {
                continue;
            }
            let manifest_segment_deletes = selected_entries
                .iter()
                .filter_map(|entry| entry.segment_key.clone())
                .collect::<BTreeSet<_>>()
                .len();
            if !operation_budget.can_process_candidate(
                candidate,
                selected_entries.len(),
                manifest_segment_deletes,
            ) {
                partial = true;
                break;
            }
            let candidate_entries = selected_entries
                .iter()
                .map(|entry| entry.entry.clone())
                .collect::<Vec<_>>();
            checkpoint()?;
            let compacted = self.write_compacted_object(candidate, &candidate_entries)?;
            operation_budget.record_gets(candidate_entries.len());
            operation_budget.record_puts(1);
            let publish_started = Instant::now();
            checkpoint()?;
            let manifest_publish = self.try_write_compaction_manifest_entries(
                &candidate.chain,
                &compacted.entries,
                &candidate_entries,
                config.validate_coverage_index_sources,
                checkpoint,
            )?;
            if !manifest_publish.completed {
                operation_budget.record_puts(manifest_publish.published_entries);
                continue;
            }
            operation_budget.record_puts(manifest_publish.published_entries);
            compacted_rows += compacted.row_count;
            compacted_objects += 1;
            processed_candidates += 1;
            input_objects += candidate_entries.len();
            deleted_manifest_segments.extend(
                selected_entries
                    .iter()
                    .filter_map(|entry| entry.segment_key.clone()),
            );
            operation_budget.record_deletes(manifest_segment_deletes);
            cursor_advance_key = selected_entries
                .iter()
                .filter_map(|entry| entry.cursor_key.clone())
                .max()
                .or(cursor_advance_key);
            log::info!(
                "storage compaction manifest publish chain_key={} duration_ms={}",
                candidate.chain.key_prefix(),
                publish_started.elapsed().as_millis()
            );
            if config.delete_source_objects {
                checkpoint()?;
                self.record_superseded_compaction_sources(candidate, config)?;
            }
            if config.cleanup_enabled {
                let queue_entry_keys = selected_entries
                    .iter()
                    .filter_map(|entry| entry.cursor_key.as_deref())
                    .filter(|key| key.contains("/metadata/compaction-queue/"))
                    .collect::<BTreeSet<_>>();
                if operation_budget.remaining_deletes() >= queue_entry_keys.len() {
                    checkpoint()?;
                    let cleanup = self.delete_compaction_queue_entries(
                        &candidate.chain,
                        queue_entry_keys.into_iter(),
                    );
                    operation_budget.record_deletes(cleanup.deleted_objects);
                    source_delete_failures =
                        source_delete_failures.saturating_add(cleanup.delete_failures);
                    if cleanup.delete_failures > 0 {
                        partial = true;
                        cleanup_incomplete = true;
                    }
                } else if !queue_entry_keys.is_empty() {
                    partial = true;
                    cleanup_incomplete = true;
                }
            }
            log::info!(
                "storage compaction candidate compacted chain_key={} range_kind={} range={}-{} processed_candidates={} duration_ms={}",
                candidate.chain.key_prefix(),
                candidate.range_kind,
                candidate.range.start(),
                candidate.range.end(),
                processed_candidates,
                candidate_started.elapsed().as_millis()
            );
        }

        if config.cleanup_enabled
            && let Some(chain) = chain
            && !stale_queue_entry_keys.is_empty()
        {
            let remaining_deletes = operation_budget.remaining_deletes();
            if remaining_deletes == 0 {
                partial = true;
                cleanup_incomplete = true;
            } else {
                let attempted_deletes = stale_queue_entry_keys.len().min(remaining_deletes);
                let queue_entry_keys = stale_queue_entry_keys
                    .iter()
                    .take(attempted_deletes)
                    .map(String::as_str);
                checkpoint()?;
                let cleanup = self.delete_compaction_queue_entries(chain, queue_entry_keys);
                operation_budget.record_deletes(cleanup.deleted_objects);
                source_delete_failures =
                    source_delete_failures.saturating_add(cleanup.delete_failures);
                if attempted_deletes < stale_queue_entry_keys.len() || cleanup.delete_failures > 0 {
                    partial = true;
                    cleanup_incomplete = true;
                }
            }
        }

        if !coverage_index_v2_checked
            && (config.cleanup_enabled || config.coverage_index_v2_delta_count_threshold > 0)
        {
            if started.elapsed() >= max_duration {
                partial = true;
            } else {
                let (report, v2_partial) = self.compact_coverage_index_v2_for_chain_inner(
                    chain,
                    config,
                    started,
                    checkpoint,
                    &mut operation_budget,
                )?;
                coverage_index_v2_report = report;
                partial |= v2_partial;
            }
        }

        if let Some(cursor) = cursor {
            let legacy_next_key = legacy_cursor_after_rewritten_manifest(
                cursor.scope_cursor_advance.clone(),
                processed_candidates,
            );
            let processed_scope_cursor = cursor_advance_key.clone().map(segment_compaction_cursor);
            let legacy_scan_cursor = cursor
                .scope_cursor_advance
                .clone()
                .filter(|cursor| cursor.legacy_entry_offset.is_some());
            let scope_next_key = if cleanup_incomplete {
                cursor.scope_cursor_current
            } else if partial {
                processed_scope_cursor
                    .or(legacy_next_key)
                    .or(legacy_scan_cursor)
                    .or(cursor.scope_cursor_overlap)
                    .or(cursor.scope_cursor_current)
            } else if processed_candidates < candidates.len() {
                cursor_advance_key
                    .map(segment_compaction_cursor)
                    .or(legacy_next_key)
                    .or(cursor.scope_cursor_advance)
            } else {
                cursor.scope_cursor_advance
            };
            self.write_compaction_cursor_key(&cursor.scope_cursor_key, scope_next_key)?;
            if !cleanup_incomplete
                && (!cursor.scope_partial || cursor.queue_cursor_advance.is_some())
                && processed_candidates >= candidates.len()
            {
                self.write_compaction_cursor_key(
                    &cursor.queue_cursor_key,
                    cursor.queue_cursor_advance,
                )?;
            }
        }
        let tick_status = if partial {
            MaintenanceCompactionTickStatus::Partial
        } else {
            MaintenanceCompactionTickStatus::Completed
        };
        let duration_ms = duration_millis(started);
        let remaining_candidates = candidates
            .iter()
            .skip(processed_candidates)
            .cloned()
            .collect::<Vec<_>>();
        let tick_summary = MaintenanceCompactionTickSummary {
            input_objects,
            output_objects: compacted_objects,
            deleted_source_objects,
            deleted_manifest_segments: deleted_manifest_segments.len(),
            duration_ms,
        };
        log::info!(
            "storage compaction tick summary status={} pause_reason=none input_objects={} output_objects={} deleted_source_objects={} deleted_manifest_segments={} coverage_index_v2_compacted_buckets={} coverage_index_v2_compacted_deltas={} coverage_index_v2_input_delta_bytes={} coverage_index_v2_cleanup_records={} coverage_index_v2_deleted_deltas={} coverage_index_v2_delta_delete_failures={} duration_ms={}",
            tick_status.as_str(),
            tick_summary.input_objects,
            tick_summary.output_objects,
            tick_summary.deleted_source_objects,
            tick_summary.deleted_manifest_segments,
            coverage_index_v2_report.compacted_buckets,
            coverage_index_v2_report.compacted_deltas,
            coverage_index_v2_report.input_delta_bytes,
            coverage_index_v2_report.cleanup_records,
            coverage_index_v2_report.deleted_deltas,
            coverage_index_v2_report.delete_failures,
            tick_summary.duration_ms
        );

        Ok(MaintenanceCompactionReport {
            read_only: false,
            candidate_count: candidates.len(),
            candidate_backlog: remaining_candidates.len(),
            backlog: compaction_backlog_scopes(&remaining_candidates),
            candidates,
            processed_candidates,
            duration_ms,
            tick_status,
            pause_reason: None,
            tick_summary,
            compacted_objects,
            compacted_rows,
            deleted_source_objects,
            source_delete_failures,
            get_operations: operation_budget.used_gets,
            put_operations: operation_budget.used_puts,
            delete_operations: operation_budget.used_deletes,
            coverage_index_v2_compacted_buckets: coverage_index_v2_report.compacted_buckets,
            coverage_index_v2_compacted_deltas: coverage_index_v2_report.compacted_deltas,
            coverage_index_v2_input_delta_bytes: coverage_index_v2_report.input_delta_bytes,
            coverage_index_v2_cleanup_records: coverage_index_v2_report.cleanup_records,
            coverage_index_v2_deleted_deltas: coverage_index_v2_report.deleted_deltas,
            coverage_index_v2_delta_delete_failures: coverage_index_v2_report.delete_failures,
        })
    }

    fn check_entries(
        &self,
        entries: &[ManifestEntry],
    ) -> Result<Vec<MaintenanceIssue>, DatalensError> {
        let mut issues = Vec::new();
        for entry in entries {
            let Some(object_key) = entry.object_key.as_deref() else {
                continue;
            };
            if !self.object_store().exists(object_key)? {
                if entry_shadowed_by_live_data_object(entry, entries, self.object_store())? {
                    continue;
                }
                issues.push(entry_issue(
                    entry,
                    MaintenanceIssueKind::MissingObject,
                    format!("manifest entry object not found {object_key}"),
                ));
                continue;
            }
            let bytes = self.object_store().get(object_key)?;
            issues.extend(metadata_issues(entry, object_key, &bytes));
            let Some(encoding) = entry.object_encoding else {
                issues.push(entry_issue(
                    entry,
                    MaintenanceIssueKind::ObjectDecodeFailure,
                    format!("manifest entry object {object_key} missing object_encoding"),
                ));
                continue;
            };
            if let Err(error) = decode_object_rows(encoding, entry.dataset_key.clone(), &bytes) {
                issues.push(entry_issue(
                    entry,
                    MaintenanceIssueKind::ObjectDecodeFailure,
                    format!("decode object {object_key}: {}", error.message),
                ));
            }
        }
        Ok(issues)
    }

    fn raw_manifest_entries_for_chain(
        &self,
        chain: &ChainIdentity,
    ) -> Result<Vec<ManifestEntry>, DatalensError> {
        let mut entries = Vec::new();
        let key = manifest_key(chain);
        if self.object_store().exists(&key)? {
            let bytes = self.object_store().get(&key)?;
            entries.extend(decode_manifest_object(&key, &bytes)?.entries);
        }
        for object in self.object_store().list(&manifest_segment_prefix(chain))? {
            if !is_manifest_segment_object(&object.key) {
                continue;
            }
            let bytes = self.object_store().get(&object.key)?;
            entries.extend(decode_manifest_object(&object.key, &bytes)?.entries);
        }
        Ok(entries)
    }

    fn compaction_reconciliation_report(
        &self,
        current_entries: &[ManifestEntry],
        raw_entries: &[ManifestEntry],
        config: MaintenanceCompactionConfig,
        read_only: bool,
    ) -> Result<MaintenanceCompactionReconciliationReport, DatalensError> {
        self.compaction_reconciliation_report_with_prefix(
            current_entries,
            raw_entries,
            config,
            read_only,
            "chains",
            "chains",
        )
    }

    fn compaction_reconciliation_report_for_chain(
        &self,
        chain: &ChainIdentity,
        current_entries: &[ManifestEntry],
        raw_entries: &[ManifestEntry],
        config: MaintenanceCompactionConfig,
        read_only: bool,
    ) -> Result<MaintenanceCompactionReconciliationReport, DatalensError> {
        self.compaction_reconciliation_report_with_prefix(
            current_entries,
            raw_entries,
            config,
            read_only,
            &format!("chains/{}/datasets", chain.key_prefix()),
            &superseded_source_record_prefix(chain),
        )
    }

    fn compaction_reconciliation_report_with_prefix(
        &self,
        current_entries: &[ManifestEntry],
        raw_entries: &[ManifestEntry],
        config: MaintenanceCompactionConfig,
        read_only: bool,
        data_object_prefix: &str,
        superseded_source_prefix: &str,
    ) -> Result<MaintenanceCompactionReconciliationReport, DatalensError> {
        let current_objects = current_object_keys(current_entries)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut orphan_compacted_objects = self
            .object_store()
            .list(data_object_prefix)?
            .into_iter()
            .map(|object| object.key)
            .filter(|key| is_data_object(key))
            .filter(|key| key.contains("/compacted/"))
            .filter(|key| !current_objects.contains(key))
            .collect::<Vec<_>>();
        orphan_compacted_objects.sort();
        orphan_compacted_objects.dedup();

        let mut stale_source_objects = self
            .superseded_source_records_from_prefix(
                superseded_source_prefix,
                config.source_delete_grace_ms,
                false,
            )?
            .into_iter()
            .filter(|(_, record)| !current_objects.contains(&record.object_key))
            .map(|(_, record)| record.object_key)
            .collect::<Vec<_>>();
        stale_source_objects.sort();
        stale_source_objects.dedup();

        let stale_cleanup_records =
            self.stale_compaction_cleanup_records(current_entries, raw_entries)?;

        Ok(MaintenanceCompactionReconciliationReport {
            read_only,
            orphan_compacted_objects,
            stale_source_objects,
            stale_cleanup_records,
            deleted_orphan_compacted_objects: 0,
            deleted_stale_source_objects: 0,
            deleted_stale_cleanup_records: 0,
            delete_failures: 0,
        })
    }

    fn stale_compaction_cleanup_records(
        &self,
        current_entries: &[ManifestEntry],
        raw_entries: &[ManifestEntry],
    ) -> Result<Vec<String>, DatalensError> {
        let mut chains = current_entries
            .iter()
            .chain(raw_entries.iter())
            .map(|entry| entry.chain.clone())
            .collect::<Vec<_>>();
        chains.sort_by_key(|chain| chain.key_prefix());
        chains.dedup_by_key(|chain| chain.key_prefix());

        let mut records = Vec::new();
        for chain in chains {
            let key = compaction_cursor_key(&chain);
            if !self.object_store().exists(&key)? {
                continue;
            }
            let candidates = compaction_candidates(
                &current_entries
                    .iter()
                    .filter(|entry| entry.chain == chain)
                    .cloned()
                    .collect::<Vec<_>>(),
                MaintenanceCompactionConfig::default(),
            );
            if candidates.is_empty() {
                records.push(key);
            }
        }
        records.sort();
        Ok(records)
    }

    fn record_superseded_compaction_sources(
        &self,
        candidate: &CompactionCandidate,
        config: MaintenanceCompactionConfig,
    ) -> Result<(), DatalensError> {
        let now_ms = unix_millis_now()?;
        for object_key in &candidate.object_keys {
            let record = SupersededCompactionSource {
                schema_version: 1,
                object_key: object_key.clone(),
                superseded_at_unix_ms: now_ms,
                delete_after_unix_ms: now_ms.saturating_add(config.source_delete_grace_ms),
            };
            let record_key = superseded_source_record_key(&candidate.chain, object_key);
            let bytes = serde_json::to_vec_pretty(&record).map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::Internal,
                    format!("encode compaction superseded source record: {error}"),
                )
            })?;
            self.object_store().put(&record_key, &bytes)?;
            log::info!(
                "storage compaction source object superseded chain_key={} object_key={} delete_after_unix_ms={}",
                candidate.chain.key_prefix(),
                object_key,
                record.delete_after_unix_ms
            );
        }
        Ok(())
    }

    fn superseded_source_records_for_chain(
        &self,
        chain: &ChainIdentity,
        grace_ms: u64,
    ) -> Result<Vec<(String, SupersededCompactionSource)>, DatalensError> {
        self.superseded_source_records_from_prefix(
            &superseded_source_record_prefix(chain),
            grace_ms,
            true,
        )
    }

    fn superseded_source_records_from_prefix(
        &self,
        prefix: &str,
        grace_ms: u64,
        eligible_only: bool,
    ) -> Result<Vec<(String, SupersededCompactionSource)>, DatalensError> {
        let now_ms = unix_millis_now()?;
        let mut records = Vec::new();
        for object in self.object_store().list(prefix)? {
            if !object
                .key
                .contains("/metadata/compaction-superseded-sources/")
                || !object.key.ends_with(".json")
            {
                continue;
            }
            let bytes = self.object_store().get(&object.key)?;
            let mut record =
                serde_json::from_slice::<SupersededCompactionSource>(&bytes).map_err(|error| {
                    DatalensError::new(
                        DatalensErrorKind::StorageReadFailure,
                        format!(
                            "decode compaction superseded source record {}: {error}",
                            object.key
                        ),
                    )
                })?;
            record.delete_after_unix_ms = record
                .delete_after_unix_ms
                .max(record.superseded_at_unix_ms.saturating_add(grace_ms));
            if !eligible_only || record.delete_after_unix_ms <= now_ms {
                records.push((object.key, record));
            }
        }
        records.sort_by(|left, right| {
            left.1
                .delete_after_unix_ms
                .cmp(&right.1.delete_after_unix_ms)
                .then_with(|| left.1.object_key.cmp(&right.1.object_key))
        });
        Ok(records)
    }

    fn delete_compaction_queue_entries<'a>(
        &self,
        chain: &ChainIdentity,
        queue_entry_keys: impl Iterator<Item = &'a str>,
    ) -> CompactionSourceCleanup {
        let mut deleted_objects = 0usize;
        let mut delete_failures = 0usize;
        for object_key in queue_entry_keys {
            match self.object_store().delete(object_key) {
                Ok(()) => {
                    deleted_objects += 1;
                    log::info!(
                        "storage compaction queue entry deleted chain_key={} object_key={}",
                        chain.key_prefix(),
                        object_key
                    );
                }
                Err(error) => {
                    delete_failures += 1;
                    log::warn!(
                        "storage compaction queue entry delete failed chain_key={} object_key={} kind={:?} message={}",
                        chain.key_prefix(),
                        object_key,
                        error.kind,
                        error.message
                    );
                }
            }
        }
        CompactionSourceCleanup {
            deleted_objects,
            delete_failures,
        }
    }

    fn compact_coverage_index_v2_for_chain_inner(
        &self,
        chain: Option<&ChainIdentity>,
        config: MaintenanceCompactionConfig,
        tick_started: Instant,
        checkpoint: &dyn Fn() -> Result<(), DatalensError>,
        operation_budget: &mut CompactionOperationBudget,
    ) -> Result<(CoverageIndexV2CompactionReport, bool), DatalensError> {
        let Some(chain) = chain else {
            return Ok((CoverageIndexV2CompactionReport::default(), false));
        };
        let max_duration = Duration::from_millis(config.max_tick_duration_ms.max(1));
        let mut report = CoverageIndexV2CompactionReport::default();
        let reserved_compaction_gets = config.coverage_index_v2_delta_count_threshold.max(1);
        let reserve_compaction_budget =
            operation_budget.remaining_gets() > reserved_compaction_gets;
        let mut cleanup_scan = if config.cleanup_enabled {
            let remaining_gets = operation_budget.remaining_gets();
            let max_records = if remaining_gets > reserved_compaction_gets {
                remaining_gets - reserved_compaction_gets
            } else {
                remaining_gets
            }
            .min(COVERAGE_INDEX_V2_CLEANUP_RECORDS_PER_TICK);
            if max_records == 0 {
                None
            } else {
                let cursor_key = coverage_index_v2_cleanup_cursor_key(chain);
                let cursor = self.read_compaction_cursor_key(&cursor_key)?;
                let cleanup_scan = self.scan_coverage_index_v2_cleanup_records(
                    chain,
                    config,
                    &cursor,
                    max_records,
                )?;
                operation_budget.record_gets(cleanup_scan.get_operations);
                Some(cleanup_scan)
            }
        } else {
            None
        };
        let mut cleanup_snapshot_keys = cleanup_scan
            .as_ref()
            .into_iter()
            .flat_map(|scan| scan.records.iter())
            .map(|object| object.record.snapshot_key.clone())
            .collect::<BTreeSet<_>>();
        let mut cleanup_delta_keys = cleanup_scan
            .as_ref()
            .into_iter()
            .flat_map(|scan| scan.records.iter())
            .flat_map(|object| object.record.compacted_delta_keys.iter().cloned())
            .collect::<BTreeSet<_>>();
        let mut cleanup_partial = false;
        if config.cleanup_enabled
            && !reserve_compaction_budget
            && cleanup_scan
                .as_ref()
                .is_some_and(|scan| !scan.records.is_empty())
        {
            cleanup_partial =
                self.cleanup_coverage_index_v2_for_chain(CoverageIndexV2CleanupArgs {
                    chain,
                    deadline: CoverageIndexV2CleanupDeadline {
                        tick_started,
                        max_duration,
                    },
                    checkpoint,
                    operation_budget,
                    cleanup_scan: cleanup_scan.clone(),
                    max_delta_deletes: reserve_compaction_budget
                        .then_some(reserved_compaction_gets),
                    report: &mut report,
                })?;
            cleanup_scan = None;
        }
        let priority_cursor_key = coverage_index_v2_compaction_priority_cursor_key(chain);
        let priority_cursor = self.read_compaction_cursor_key(&priority_cursor_key)?;
        let next_priority = match priority_cursor.next_segment_key.as_deref() {
            Some(COVERAGE_INDEX_V2_COMPACTION_PRIORITY_ROOT) => CoverageIndexV2ScanPriority::Root,
            _ => CoverageIndexV2ScanPriority::SemanticEvmLogs,
        };
        let bucket_scan = self.scan_coverage_index_v2_delta_buckets(
            chain,
            next_priority,
            config.coverage_index_v2_delta_count_threshold,
            CoverageIndexV2CleanupDeadline {
                tick_started,
                max_duration,
            },
        )?;
        let mut cursor_advances = BTreeMap::<String, String>::new();
        let mut partial = cleanup_partial || bucket_scan.partial;
        let mut processed_v2_bucket = false;

        for bucket_scan_item in bucket_scan.buckets {
            if tick_started.elapsed() >= max_duration {
                partial = true;
                break;
            }
            let bucket = bucket_scan_item.bucket;
            if config.cleanup_enabled && operation_budget.remaining_puts() > 0 {
                for mut record in
                    v2_snapshot_cleanup_records_for_bucket(self.object_store(), &bucket)?
                {
                    if tick_started.elapsed() >= max_duration {
                        partial = true;
                        break;
                    }
                    record
                        .compacted_delta_keys
                        .retain(|key| !cleanup_delta_keys.contains(key));
                    if record.compacted_delta_keys.is_empty()
                        || cleanup_snapshot_keys.contains(&record.snapshot_key)
                        || operation_budget.remaining_puts() == 0
                    {
                        continue;
                    }
                    checkpoint()?;
                    let snapshot_key = record.snapshot_key.clone();
                    cleanup_delta_keys.extend(record.compacted_delta_keys.iter().cloned());
                    let record_key =
                        write_v2_cleanup_record(self.object_store(), chain, record.clone())?;
                    operation_budget.record_puts(1);
                    cleanup_snapshot_keys.insert(snapshot_key);
                    if let Some(cleanup_scan) = cleanup_scan.as_mut() {
                        cleanup_scan
                            .records
                            .push(CoverageIndexV2CleanupRecordObject {
                                key: record_key,
                                record,
                            });
                    }
                    report.cleanup_records += 1;
                }
            }
            if tick_started.elapsed() >= max_duration {
                partial = true;
                break;
            }
            if operation_budget.remaining_gets()
                < config.coverage_index_v2_delta_count_threshold.max(1)
            {
                if processed_v2_bucket {
                    partial = true;
                }
                break;
            }
            if operation_budget.remaining_puts() < 2 {
                if processed_v2_bucket {
                    partial = true;
                }
                break;
            }
            if let Some(compaction) = prepare_v2_bucket_compaction(
                self.object_store(),
                &bucket,
                config.coverage_index_v2_delta_count_threshold,
                operation_budget.remaining_gets(),
            )? {
                checkpoint()?;
                let snapshot_key = write_v2_snapshot(
                    self.object_store(),
                    chain,
                    &compaction.bucket.scope,
                    compaction.bucket.bucket_start,
                    compaction.bucket.bucket_end,
                    compaction.entries,
                    compaction.compacted_delta_keys.clone(),
                )?;
                operation_budget.record_puts(1);
                operation_budget.record_gets(compaction.newly_compacted_delta_keys.len());
                checkpoint()?;
                write_v2_snapshot_head(
                    self.object_store(),
                    chain,
                    &compaction.bucket.scope,
                    compaction.bucket.bucket_start,
                    compaction.bucket.bucket_end,
                    snapshot_key.clone(),
                    compaction.included_delta_high_watermark,
                )?;
                operation_budget.record_puts(1);
                report.compacted_buckets += 1;
                report.compacted_deltas += compaction.newly_compacted_delta_keys.len();
                report.input_delta_bytes = report
                    .input_delta_bytes
                    .saturating_add(compaction.input_delta_bytes);
                processed_v2_bucket = true;
                if config.cleanup_enabled && operation_budget.remaining_puts() > 0 {
                    let mut compacted_delta_keys = compaction.compacted_delta_keys;
                    compacted_delta_keys.retain(|key| !cleanup_delta_keys.contains(key));
                    if compacted_delta_keys.is_empty() {
                        continue;
                    }
                    checkpoint()?;
                    cleanup_delta_keys.extend(compacted_delta_keys.iter().cloned());
                    let record = CoverageIndexV2CleanupRecord {
                        schema_version: 1,
                        created_at_unix_ms: coverage_index_v2_unix_ms_now()?,
                        scope: compaction.bucket.scope,
                        bucket_start: compaction.bucket.bucket_start,
                        bucket_end: compaction.bucket.bucket_end,
                        compaction_id: snapshot_key
                            .rsplit('/')
                            .next()
                            .unwrap_or("")
                            .trim_end_matches(".json")
                            .to_owned(),
                        snapshot_key: snapshot_key.clone(),
                        compacted_delta_keys,
                    };
                    let record_key =
                        write_v2_cleanup_record(self.object_store(), chain, record.clone())?;
                    operation_budget.record_puts(1);
                    cleanup_snapshot_keys.insert(snapshot_key);
                    if let Some(cleanup_scan) = cleanup_scan.as_mut() {
                        cleanup_scan
                            .records
                            .push(CoverageIndexV2CleanupRecordObject {
                                key: record_key,
                                record,
                            });
                    }
                    report.cleanup_records += 1;
                }
            }
            if let Some(last_delta_key) = bucket_scan_item.last_delta_key {
                cursor_advances.insert(bucket_scan_item.cursor_key, last_delta_key);
            }
        }
        for cursor_advance in bucket_scan.empty_cursor_advances {
            cursor_advances
                .entry(cursor_advance.cursor_key)
                .or_insert(cursor_advance.next_segment_key);
        }
        for (cursor_key, cursor_advance_key) in cursor_advances {
            self.write_compaction_cursor_key(
                &cursor_key,
                Some(CompactionCursor {
                    schema_version: 1,
                    next_segment_key: Some(cursor_advance_key),
                    legacy_entry_offset: None,
                }),
            )?;
        }
        if processed_v2_bucket {
            self.write_compaction_cursor_key(
                &priority_cursor_key,
                Some(CompactionCursor {
                    schema_version: 1,
                    next_segment_key: Some(
                        bucket_scan
                            .priority
                            .next_priority()
                            .cursor_value()
                            .to_owned(),
                    ),
                    legacy_entry_offset: None,
                }),
            )?;
        }

        if config.cleanup_enabled && tick_started.elapsed() < max_duration {
            let cleanup_partial =
                self.cleanup_coverage_index_v2_for_chain(CoverageIndexV2CleanupArgs {
                    chain,
                    deadline: CoverageIndexV2CleanupDeadline {
                        tick_started,
                        max_duration,
                    },
                    checkpoint,
                    operation_budget,
                    cleanup_scan,
                    max_delta_deletes: None,
                    report: &mut report,
                })?;
            partial |= cleanup_partial;
        } else if config.cleanup_enabled {
            partial = true;
        }
        Ok((report, partial))
    }

    fn scan_coverage_index_v2_delta_buckets(
        &self,
        chain: &ChainIdentity,
        priority: CoverageIndexV2ScanPriority,
        delta_count_threshold: usize,
        deadline: CoverageIndexV2CleanupDeadline,
    ) -> Result<CoverageIndexV2BucketScan, DatalensError> {
        let prefix = format!("chains/{}/coverage-index-v2/deltas", chain.key_prefix());
        let semantic_evm_logs_prefix = format!("{prefix}/semantic/evm.logs");
        let semantic_scan = (
            semantic_evm_logs_prefix.as_str(),
            coverage_index_v2_semantic_evm_logs_compaction_cursor_key(chain),
            false,
        );
        let root_scan = (
            prefix.as_str(),
            coverage_index_v2_compaction_cursor_key(chain),
            true,
        );
        let scan_prefixes = match priority {
            CoverageIndexV2ScanPriority::SemanticEvmLogs => [semantic_scan, root_scan],
            CoverageIndexV2ScanPriority::Root => [root_scan, semantic_scan],
        };
        let mut buckets = Vec::new();
        let mut seen_buckets = BTreeSet::<CoverageIndexV2Bucket>::new();
        let mut empty_cursor_advances = Vec::new();
        let mut partial = false;
        for (scan_prefix, cursor_key, skip_semantic_evm_logs) in scan_prefixes {
            if deadline.expired() {
                partial = true;
                break;
            }
            let cursor = self.read_compaction_cursor_key(&cursor_key)?;
            let strict_prefix = format!("{scan_prefix}/");
            let mut list_page = self.object_store().list_page(
                scan_prefix,
                cursor.next_segment_key.as_deref(),
                COVERAGE_INDEX_V2_LIST_PAGE_SIZE,
            )?;
            if deadline.expired() {
                partial = true;
                break;
            }
            if list_page.objects.is_empty()
                && cursor.next_segment_key.is_some()
                && !list_page.has_more
            {
                list_page = self.object_store().list_page(
                    scan_prefix,
                    None,
                    COVERAGE_INDEX_V2_LIST_PAGE_SIZE,
                )?;
                if deadline.expired() {
                    partial = true;
                    break;
                }
            }
            let mut had_buckets = false;
            let mut bucket_delta_counts = BTreeMap::<CoverageIndexV2Bucket, usize>::new();
            let mut bucket_last_delta_keys = BTreeMap::<CoverageIndexV2Bucket, String>::new();
            for object in &list_page.objects {
                if !object.key.starts_with(&strict_prefix) || !object.key.ends_with(".json") {
                    continue;
                }
                let Some(bucket) = parse_v2_bucket_from_object_key(&prefix, &object.key)? else {
                    continue;
                };
                if skip_semantic_evm_logs && bucket.scope.starts_with("semantic/evm.logs/") {
                    continue;
                }
                had_buckets = true;
                *bucket_delta_counts.entry(bucket.clone()).or_default() += 1;
                bucket_last_delta_keys.insert(bucket, object.key.clone());
            }
            let mut emitted_buckets = false;
            let skip_sparse_page_buckets = list_page.has_more;
            for (bucket, last_delta_key) in bucket_last_delta_keys {
                let page_delta_count = bucket_delta_counts.get(&bucket).copied().unwrap_or(0);
                if (!skip_sparse_page_buckets || page_delta_count >= delta_count_threshold.max(1))
                    && seen_buckets.insert(bucket.clone())
                {
                    emitted_buckets = true;
                    buckets.push(CoverageIndexV2BucketScanItem {
                        cursor_key: cursor_key.clone(),
                        bucket,
                        last_delta_key: Some(last_delta_key),
                    });
                }
            }
            if (!had_buckets || !emitted_buckets)
                && let Some(next_segment_key) =
                    list_page.objects.last().map(|object| object.key.clone())
            {
                empty_cursor_advances.push(CoverageIndexV2CursorAdvance {
                    cursor_key,
                    next_segment_key,
                });
            }
            partial |= list_page.has_more;
        }
        Ok(CoverageIndexV2BucketScan {
            buckets,
            empty_cursor_advances,
            partial,
            priority,
        })
    }

    fn cleanup_coverage_index_v2_for_chain(
        &self,
        args: CoverageIndexV2CleanupArgs<'_>,
    ) -> Result<bool, DatalensError> {
        let CoverageIndexV2CleanupArgs {
            chain,
            deadline,
            checkpoint,
            operation_budget,
            cleanup_scan,
            max_delta_deletes,
            report,
        } = args;
        let cursor_key = coverage_index_v2_cleanup_cursor_key(chain);
        let Some(cleanup_scan) = cleanup_scan else {
            return Ok(true);
        };
        let had_records = !cleanup_scan.records.is_empty();
        let mut partial = cleanup_scan.partial;
        let mut cursor_advance_key = None;
        let mut remaining_delta_deletes = max_delta_deletes.unwrap_or(usize::MAX);
        let mut latest_delta_key_cache = BTreeMap::new();
        for record in cleanup_scan.records {
            if deadline.expired() {
                partial = true;
                break;
            }
            if remaining_delta_deletes == 0 {
                partial = true;
                break;
            }
            let required_deletes = record.record.compacted_delta_keys.len().saturating_add(1);
            let remaining_deletes = operation_budget.remaining_deletes();
            if remaining_deletes == 0 {
                partial = true;
                break;
            }
            if !v2_cleanup_record_is_safe_to_delete_with_cache(
                self.object_store(),
                chain,
                &record,
                &mut latest_delta_key_cache,
            )? {
                checkpoint()?;
                self.object_store().delete(&record.key)?;
                operation_budget.record_deletes(1);
                cursor_advance_key = Some(record.key);
                continue;
            }
            let can_finish_record = remaining_deletes >= required_deletes;
            if !can_finish_record && operation_budget.remaining_puts() == 0 {
                partial = true;
                break;
            }
            let delta_delete_budget = if can_finish_record {
                record.record.compacted_delta_keys.len()
            } else {
                remaining_deletes
            }
            .min(remaining_delta_deletes);
            let cleanup = self.delete_coverage_index_v2_cleanup_deltas(
                chain,
                &record,
                checkpoint,
                delta_delete_budget,
                deadline,
            );
            operation_budget.record_deletes(cleanup.deleted_objects);
            remaining_delta_deletes =
                remaining_delta_deletes.saturating_sub(cleanup.deleted_objects);
            report.deleted_deltas = report
                .deleted_deltas
                .saturating_add(cleanup.deleted_objects);
            report.delete_failures = report
                .delete_failures
                .saturating_add(cleanup.delete_failures);
            if cleanup.partial {
                partial = true;
            }
            if cleanup.delete_failures == 0 {
                if cleanup.deleted_objects >= record.record.compacted_delta_keys.len() {
                    if deadline.expired() {
                        partial = true;
                        break;
                    }
                    checkpoint()?;
                    self.object_store().delete(&record.key)?;
                    operation_budget.record_deletes(1);
                    cursor_advance_key = Some(record.key);
                } else {
                    let deleted_keys = cleanup.deleted_keys.into_iter().collect::<BTreeSet<_>>();
                    let mut updated_record = record.record;
                    updated_record
                        .compacted_delta_keys
                        .retain(|key| !deleted_keys.contains(key));
                    let bytes = serde_json::to_vec_pretty(&updated_record).map_err(|error| {
                        DatalensError::new(
                            DatalensErrorKind::Internal,
                            format!("encode coverage index v2 cleanup record: {error}"),
                        )
                    })?;
                    checkpoint()?;
                    self.object_store().put(&record.key, &bytes)?;
                    operation_budget.record_puts(1);
                    partial = true;
                    break;
                }
            } else {
                partial = true;
                break;
            }
        }
        if cursor_advance_key.is_none() && !had_records {
            cursor_advance_key = cleanup_scan.page_last_key;
        }
        if let Some(cursor_advance_key) = cursor_advance_key {
            self.write_compaction_cursor_key(
                &cursor_key,
                Some(CompactionCursor {
                    schema_version: 1,
                    next_segment_key: Some(cursor_advance_key),
                    legacy_entry_offset: None,
                }),
            )?;
        }
        Ok(partial)
    }

    fn scan_coverage_index_v2_cleanup_records(
        &self,
        chain: &ChainIdentity,
        config: MaintenanceCompactionConfig,
        cursor: &CompactionCursor,
        max_records: usize,
    ) -> Result<CoverageIndexV2CleanupScan, DatalensError> {
        let now_ms = coverage_index_v2_unix_ms_now()?;
        let prefix = format!("chains/{}/coverage-index-v2/cleanup", chain.key_prefix());
        let strict_prefix = format!("{prefix}/");
        let mut list_page = self.object_store().list_page(
            &prefix,
            cursor.next_segment_key.as_deref(),
            COVERAGE_INDEX_V2_LIST_PAGE_SIZE,
        )?;
        if list_page.objects.is_empty() && cursor.next_segment_key.is_some() && !list_page.has_more
        {
            list_page =
                self.object_store()
                    .list_page(&prefix, None, COVERAGE_INDEX_V2_LIST_PAGE_SIZE)?;
        }
        let mut records = Vec::new();
        let mut get_operations = 0usize;
        let mut partial = list_page.has_more;
        let page_objects = list_page.objects.len();
        for object in &list_page.objects {
            if !object.key.starts_with(&strict_prefix) || !object.key.ends_with(".json") {
                continue;
            }
            if records.len() >= max_records {
                partial = true;
                break;
            }
            let bytes = self.object_store().get(&object.key)?;
            get_operations = get_operations.saturating_add(1);
            let record: CoverageIndexV2CleanupRecord = match serde_json::from_slice(&bytes) {
                Ok(record) => record,
                Err(error) => {
                    log::warn!(
                        "storage coverage index v2 cleanup record skipped object_key={} reason=decode_failed message={}",
                        object.key,
                        error
                    );
                    continue;
                }
            };
            if record
                .created_at_unix_ms
                .saturating_add(config.coverage_index_v2_delete_grace_ms)
                <= now_ms
            {
                records.push(CoverageIndexV2CleanupRecordObject {
                    key: object.key.clone(),
                    record,
                });
            }
        }
        records.sort_by(|left, right| {
            left.record
                .created_at_unix_ms
                .cmp(&right.record.created_at_unix_ms)
                .then_with(|| left.key.cmp(&right.key))
        });
        log::info!(
            "storage coverage index v2 cleanup scan chain_key={} page_objects={} eligible_records={} max_records={} has_more={} get_operations={} cursor_present={} page_last_key_present={}",
            chain.key_prefix(),
            page_objects,
            records.len(),
            max_records,
            list_page.has_more,
            get_operations,
            cursor.next_segment_key.is_some(),
            list_page.objects.last().is_some()
        );
        Ok(CoverageIndexV2CleanupScan {
            records,
            partial,
            page_last_key: list_page.objects.last().map(|object| object.key.clone()),
            get_operations,
        })
    }

    fn delete_coverage_index_v2_cleanup_deltas(
        &self,
        chain: &ChainIdentity,
        cleanup: &CoverageIndexV2CleanupRecordObject,
        checkpoint: &dyn Fn() -> Result<(), DatalensError>,
        max_deletes: usize,
        deadline: CoverageIndexV2CleanupDeadline,
    ) -> CoverageIndexV2DeltaCleanup {
        let mut deleted_objects = 0usize;
        let mut delete_failures = 0usize;
        let mut deleted_keys = Vec::new();
        let mut partial = false;
        for object_key in cleanup.record.compacted_delta_keys.iter().take(max_deletes) {
            if deadline.expired() {
                partial = true;
                break;
            }
            if let Err(error) = checkpoint() {
                delete_failures += 1;
                log::warn!(
                    "storage coverage index v2 cleanup checkpoint failed chain_key={} object_key={} kind={:?} message={}",
                    chain.key_prefix(),
                    object_key,
                    error.kind,
                    error.message
                );
                break;
            }
            match self.object_store().delete(object_key) {
                Ok(()) => {
                    deleted_objects += 1;
                    deleted_keys.push(object_key.clone());
                    log::info!(
                        "storage coverage index v2 delta deleted chain_key={} object_key={}",
                        chain.key_prefix(),
                        object_key
                    );
                }
                Err(error) => {
                    delete_failures += 1;
                    log::warn!(
                        "storage coverage index v2 delta delete failed chain_key={} object_key={} kind={:?} message={}",
                        chain.key_prefix(),
                        object_key,
                        error.kind,
                        error.message
                    );
                }
            }
        }
        CoverageIndexV2DeltaCleanup {
            deleted_objects,
            delete_failures,
            deleted_keys,
            partial,
        }
    }

    fn try_write_compaction_manifest_entries(
        &self,
        chain: &ChainIdentity,
        compacted_entries: &[ManifestEntry],
        source_entries: &[ManifestEntry],
        validate_coverage_index_sources: bool,
        checkpoint: &dyn Fn() -> Result<(), DatalensError>,
    ) -> Result<CompactionManifestPublishResult, DatalensError> {
        let mut published_entries = 0usize;
        for compacted_entry in compacted_entries {
            let matching_source_entries = source_entries
                .iter()
                .filter(|source_entry| {
                    source_entry
                        .range
                        .intersection(&compacted_entry.range)
                        .is_some()
                })
                .cloned()
                .collect::<Vec<_>>();
            if matching_source_entries.is_empty() {
                return Ok(CompactionManifestPublishResult {
                    completed: false,
                    published_entries,
                });
            }
            if !self.try_write_compaction_manifest_entry(
                chain,
                compacted_entry.clone(),
                &matching_source_entries,
                validate_coverage_index_sources,
                checkpoint,
            )? {
                return Ok(CompactionManifestPublishResult {
                    completed: false,
                    published_entries,
                });
            }
            published_entries = published_entries.saturating_add(1);
        }
        Ok(CompactionManifestPublishResult {
            completed: true,
            published_entries,
        })
    }

    fn retention_delete_candidates(
        &self,
        protected_current_objects: &[String],
    ) -> Result<Vec<String>, DatalensError> {
        let protected = protected_current_objects
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let mut candidates = self
            .object_store()
            .list("chains")?
            .into_iter()
            .filter(|object| is_data_object(&object.key))
            .filter(|object| !protected.contains(object.key.as_str()))
            .map(|object| object.key)
            .collect::<Vec<_>>();
        candidates.sort();
        Ok(candidates)
    }

    fn write_compacted_object(
        &self,
        candidate: &CompactionCandidate,
        entries: &[ManifestEntry],
    ) -> Result<CompactedObject, DatalensError> {
        let mut rows = crate::empty_rows(candidate.dataset_key.clone())?.into_rows();
        for entry in entries {
            let Some(object_key) = entry.object_key.as_deref() else {
                return Err(DatalensError::new(
                    DatalensErrorKind::Internal,
                    "compaction candidate includes empty coverage",
                ));
            };
            let read_started = Instant::now();
            let bytes = self.object_store().get(object_key)?;
            log::info!(
                "storage compaction object read chain_key={} object_key={} object_bytes={} duration_ms={}",
                candidate.chain.key_prefix(),
                object_key,
                bytes.len(),
                read_started.elapsed().as_millis()
            );
            verify_manifest_object_metadata(entry, object_key, &bytes)?;
            let mut object_rows = decode_object_rows(
                candidate.object_encoding,
                candidate.dataset_key.clone(),
                &bytes,
            )
            .map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!(
                        "decode compacted source object {object_key}: {}",
                        error.message
                    ),
                )
            })?;
            object_rows = crate::filter_rows(object_rows, entry.range.clone());
            rows.try_append(object_rows.into_rows())?;
        }
        rows.sort();
        let rows = DatasetRows::new(candidate.dataset_key.clone(), rows)?;
        let object_compression = match candidate.object_encoding {
            ObjectEncoding::ParquetV1 => Some(self.parquet_compression()),
            ObjectEncoding::Json => None,
        };
        let bytes = encode_object_rows(
            candidate.object_encoding,
            &rows,
            object_compression.unwrap_or(ParquetCompression::None),
        )?;
        if bytes.len() as u64 > candidate.max_output_object_bytes {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!(
                    "compaction output object {} bytes exceeds max_output_object_bytes {}",
                    bytes.len(),
                    candidate.max_output_object_bytes
                ),
            ));
        }
        let checksum = checksum_hex(&bytes);
        let object_key = compacted_object_key(candidate, &checksum);
        let data_object = StorageDataObject {
            object_key: object_key.clone(),
            object_encoding: candidate.object_encoding,
            object_compression,
            row_count: rows.row_count(),
            object_size_bytes: bytes.len() as u64,
            checksum,
            checksum_algorithm: "sha256".to_owned(),
            written_at_unix_seconds: unix_seconds_now()?,
        };
        let write_started = Instant::now();
        match self.object_store().get_optional(&object_key)? {
            Some(existing) => {
                verify_existing_compacted_object(candidate, &data_object, &existing)?;
                log::info!(
                    "storage compaction object reuse chain_key={} object_key={} object_bytes={} rows={} duration_ms={}",
                    candidate.chain.key_prefix(),
                    object_key,
                    bytes.len(),
                    rows.row_count(),
                    write_started.elapsed().as_millis()
                );
            }
            None => match self.object_store().put_if_absent(&object_key, &bytes)? {
                ObjectPutIfAbsentResult::Created => {
                    log::info!(
                        "storage compaction object write chain_key={} object_key={} object_bytes={} rows={} duration_ms={}",
                        candidate.chain.key_prefix(),
                        object_key,
                        bytes.len(),
                        rows.row_count(),
                        write_started.elapsed().as_millis()
                    );
                }
                ObjectPutIfAbsentResult::AlreadyExists => {
                    let existing = self.object_store().get(&object_key)?;
                    verify_existing_compacted_object(candidate, &data_object, &existing)?;
                    log::info!(
                        "storage compaction object reuse chain_key={} object_key={} object_bytes={} rows={} duration_ms={}",
                        candidate.chain.key_prefix(),
                        object_key,
                        bytes.len(),
                        rows.row_count(),
                        write_started.elapsed().as_millis()
                    );
                }
            },
        }
        Ok(CompactedObject {
            row_count: rows.row_count(),
            entries: compacted_manifest_entries(candidate, entries, data_object),
        })
    }

    fn scan_compaction_manifest_entries(
        &self,
        chain: &ChainIdentity,
        config: MaintenanceCompactionConfig,
        tick_started: Instant,
    ) -> Result<CompactionManifestScan, DatalensError> {
        let load_started = Instant::now();
        let prefix = manifest_segment_prefix(chain);
        let queue_cursor_key = compaction_queue_cursor_key(chain);
        let queue_cursor = self.read_compaction_cursor_key(&queue_cursor_key)?;
        let max_entries = config.max_manifest_entries_per_tick.max(1);
        let legacy_cursor = self.read_compaction_legacy_cursor(chain)?;
        if legacy_cursor != CompactionCursor::default() {
            return self.scan_legacy_compaction_manifest_entries(
                chain,
                &legacy_cursor,
                max_entries,
                load_started,
            );
        }
        let queue_scan = self.scan_compaction_queue_entries(
            chain,
            &queue_cursor,
            config,
            max_entries.max(2),
            load_started,
        )?;
        if !queue_scan.entries.is_empty()
            || queue_scan.partial
            || (config.cleanup_enabled && !queue_scan.stale_queue_entry_keys.is_empty())
        {
            return Ok(queue_scan);
        }
        let mut queue_page =
            self.object_store()
                .list_page(&prefix, queue_cursor.next_segment_key.as_deref(), 1)?;
        if queue_page.objects.is_empty()
            && queue_cursor.next_segment_key.is_some()
            && !queue_page.has_more
        {
            queue_page = self.object_store().list_page(&prefix, None, 1)?;
        }
        let mut first_segment_objects = queue_page
            .objects
            .into_iter()
            .filter(|object| is_manifest_segment_object(&object.key))
            .collect::<Vec<_>>();
        first_segment_objects.sort_by(|left, right| left.key.cmp(&right.key));

        let Some(active_scope_prefix) = first_segment_objects
            .first()
            .and_then(|object| manifest_segment_scope_prefix(&object.key))
        else {
            return self.scan_legacy_compaction_manifest_entries(
                chain,
                &legacy_cursor,
                max_entries,
                load_started,
            );
        };
        let scope_cursor_key = compaction_scope_cursor_key(&active_scope_prefix);
        let mut scope_cursor = self.read_compaction_cursor_key(&scope_cursor_key)?;
        if scope_cursor == CompactionCursor::default()
            && let Ok(legacy_cursor) = self.read_compaction_legacy_cursor(chain)
            && legacy_cursor != CompactionCursor::default()
        {
            scope_cursor = legacy_cursor;
        }
        let mut list_page = self.object_store().list_page(
            &active_scope_prefix,
            scope_cursor.next_segment_key.as_deref(),
            max_entries,
        )?;
        if list_page.objects.is_empty()
            && scope_cursor.next_segment_key.is_some()
            && !list_page.has_more
        {
            list_page = self
                .object_store()
                .list_page(&active_scope_prefix, None, max_entries)?;
        }
        let mut segment_objects = list_page
            .objects
            .into_iter()
            .filter(|object| is_manifest_segment_object(&object.key))
            .collect::<Vec<_>>();
        segment_objects.sort_by(|left, right| left.key.cmp(&right.key));
        let mut entries = Vec::new();
        let mut scanned_objects = 0usize;
        let mut scanned_entries = 0usize;
        let mut cursor_advance_key = None;

        if segment_objects.is_empty() {
            return self.scan_legacy_compaction_manifest_entries(
                chain,
                &legacy_cursor,
                max_entries,
                load_started,
            );
        }

        let base_entries = if self.object_store().exists(&manifest_key(chain))? {
            let key = manifest_key(chain);
            let bytes = self.object_store().get(&key)?;
            decode_manifest_object(&key, &bytes)?.entries
        } else {
            Vec::new()
        };
        for object in &segment_objects {
            if entries.len() >= max_entries
                || tick_started.elapsed().as_millis() >= config.max_tick_duration_ms.max(1) as u128
            {
                break;
            }
            let bytes = self.object_store().get(&object.key)?;
            let manifest = decode_manifest_object(&object.key, &bytes)?;
            scanned_objects += 1;
            scanned_entries += manifest.entries.len();
            cursor_advance_key = Some(object.key.clone());
            for entry in manifest.entries {
                if base_entries
                    .iter()
                    .any(|base_entry| base_entry.shadows_segment(&entry))
                {
                    continue;
                }
                entries.push(SelectedManifestEntry {
                    segment_key: Some(object.key.clone()),
                    cursor_key: Some(object.key.clone()),
                    entry,
                });
                if entries.len() >= max_entries {
                    break;
                }
            }
        }
        let scope_partial = list_page.has_more || scanned_objects < segment_objects.len();
        let partial = scope_partial || queue_page.has_more;
        let queue_cursor_advance = if scope_partial {
            None
        } else {
            cursor_advance_key.clone().map(segment_compaction_cursor)
        };
        log::info!(
            "storage compaction manifest load chain_key={} source=manifest_segments listed_object_count={} scanned_object_count={} scanned_entry_count={} selected_entry_count={} scope_prefix={} partial={} duration_ms={}",
            chain.key_prefix(),
            segment_objects.len(),
            scanned_objects,
            scanned_entries,
            entries.len(),
            active_scope_prefix,
            partial,
            load_started.elapsed().as_millis()
        );
        Ok(CompactionManifestScan {
            entries,
            stale_queue_entry_keys: Vec::new(),
            partial,
            cursor_update: CompactionCursorUpdate {
                scope_cursor_key,
                scope_cursor_current: Some(scope_cursor),
                scope_cursor_overlap: None,
                scope_cursor_advance: cursor_advance_key.map(segment_compaction_cursor),
                scope_partial,
                queue_cursor_key,
                queue_cursor_advance,
            },
        })
    }

    fn scan_legacy_compaction_manifest_entries(
        &self,
        chain: &ChainIdentity,
        cursor: &CompactionCursor,
        max_entries: usize,
        load_started: Instant,
    ) -> Result<CompactionManifestScan, DatalensError> {
        let key = manifest_key(chain);
        if !self.object_store().exists(&key)? {
            log::info!(
                "storage compaction manifest load chain_key={} source=legacy_manifest object_count=0 entry_count=0 selected_entry_count=0 legacy_entry_offset=0 partial=false duration_ms={}",
                chain.key_prefix(),
                load_started.elapsed().as_millis()
            );
            return Ok(CompactionManifestScan {
                entries: Vec::new(),
                stale_queue_entry_keys: Vec::new(),
                partial: false,
                cursor_update: CompactionCursorUpdate {
                    scope_cursor_key: compaction_cursor_key(chain),
                    scope_cursor_current: Some(cursor.clone()),
                    scope_cursor_overlap: None,
                    scope_cursor_advance: None,
                    scope_partial: false,
                    queue_cursor_key: compaction_queue_cursor_key(chain),
                    queue_cursor_advance: None,
                },
            });
        }
        let bytes = self.object_store().get(&key)?;
        let manifest = decode_manifest_object(&key, &bytes)?;
        let entry_count = manifest.entries.len();
        let offset = cursor
            .legacy_entry_offset
            .unwrap_or_default()
            .min(entry_count);
        let entries = manifest
            .entries
            .into_iter()
            .skip(offset)
            .take(max_entries)
            .map(|entry| SelectedManifestEntry {
                segment_key: None,
                cursor_key: None,
                entry,
            })
            .collect::<Vec<_>>();
        let next_offset = offset.saturating_add(entries.len());
        let partial = next_offset < entry_count;
        log::info!(
            "storage compaction manifest load chain_key={} source=legacy_manifest object_count=1 entry_count={} selected_entry_count={} legacy_entry_offset={} partial={} duration_ms={}",
            chain.key_prefix(),
            entry_count,
            entries.len(),
            offset,
            partial,
            load_started.elapsed().as_millis()
        );
        Ok(CompactionManifestScan {
            entries,
            stale_queue_entry_keys: Vec::new(),
            partial,
            cursor_update: CompactionCursorUpdate {
                scope_cursor_key: compaction_cursor_key(chain),
                scope_cursor_current: Some(cursor.clone()),
                scope_cursor_overlap: None,
                scope_cursor_advance: partial.then_some(CompactionCursor {
                    schema_version: 1,
                    next_segment_key: None,
                    legacy_entry_offset: Some(next_offset),
                }),
                scope_partial: partial,
                queue_cursor_key: compaction_queue_cursor_key(chain),
                queue_cursor_advance: None,
            },
        })
    }

    fn read_compaction_legacy_cursor(
        &self,
        chain: &ChainIdentity,
    ) -> Result<CompactionCursor, DatalensError> {
        self.read_compaction_cursor_key(&compaction_cursor_key(chain))
    }

    fn read_compaction_cursor_key(&self, key: &str) -> Result<CompactionCursor, DatalensError> {
        if !self.object_store().exists(key)? {
            return Ok(CompactionCursor::default());
        }
        let bytes = self.object_store().get(key)?;
        serde_json::from_slice(&bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode compaction cursor {key}: {error}"),
            )
        })
    }

    fn write_compaction_cursor_key(
        &self,
        key: &str,
        cursor: Option<CompactionCursor>,
    ) -> Result<(), DatalensError> {
        let Some(cursor) = cursor else {
            self.object_store().delete(key)?;
            return Ok(());
        };
        let bytes = serde_json::to_vec_pretty(&cursor).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode compaction cursor: {error}"),
            )
        })?;
        self.object_store().put(key, &bytes)
    }

    fn scan_compaction_queue_entries(
        &self,
        chain: &ChainIdentity,
        cursor: &CompactionCursor,
        config: MaintenanceCompactionConfig,
        max_entries: usize,
        load_started: Instant,
    ) -> Result<CompactionManifestScan, DatalensError> {
        let prefix = compaction_queue::queue_prefix(chain);
        let queue_cursor = cursor
            .next_segment_key
            .as_deref()
            .filter(|key| key.starts_with(&prefix));
        let mut list_page = self
            .object_store()
            .list_page(&prefix, queue_cursor, max_entries)?;
        if list_page.objects.is_empty() && queue_cursor.is_some() && !list_page.has_more {
            list_page = self.object_store().list_page(&prefix, None, max_entries)?;
        }
        let mut queue_objects = list_page
            .objects
            .into_iter()
            .filter(|object| object.key.ends_with(".json"))
            .collect::<Vec<_>>();
        queue_objects.sort_by(|left, right| left.key.cmp(&right.key));
        if queue_objects.is_empty() {
            log::info!(
                "storage compaction manifest load chain_key={} source=compaction_queue listed_object_count=0 scanned_object_count=0 scanned_entry_count=0 selected_entry_count=0 partial=false duration_ms={}",
                chain.key_prefix(),
                load_started.elapsed().as_millis()
            );
            return Ok(CompactionManifestScan {
                entries: Vec::new(),
                stale_queue_entry_keys: Vec::new(),
                partial: false,
                cursor_update: CompactionCursorUpdate {
                    scope_cursor_key: compaction_queue_cursor_key(chain),
                    scope_cursor_current: None,
                    scope_cursor_overlap: None,
                    scope_cursor_advance: None,
                    scope_partial: false,
                    queue_cursor_key: compaction_queue_cursor_key(chain),
                    queue_cursor_advance: None,
                },
            });
        }
        let mut cursor_advance_key = None;
        let mut active_scope_prefix = None;
        let mut active_scope_cursor = None;
        for object in &queue_objects {
            let Some(bytes) = self.object_store().get_optional(&object.key)? else {
                cursor_advance_key = Some(object.key.clone());
                continue;
            };
            let queue_entry = compaction_queue::decode_entry(&object.key, &bytes)?;
            active_scope_prefix = manifest_segment_scope_prefix(&queue_entry.segment_key);
            break;
        }
        if let Some(active_scope_prefix) = active_scope_prefix.as_deref() {
            let scope_cursor_key = compaction_scope_cursor_key(active_scope_prefix);
            let scope_cursor = self.read_compaction_cursor_key(&scope_cursor_key)?;
            let scope_queue_cursor = scope_cursor
                .next_segment_key
                .as_deref()
                .filter(|key| key.starts_with(&prefix));
            active_scope_cursor = Some(scope_cursor.clone());
            let should_resume_scope = match (scope_queue_cursor, queue_cursor) {
                (Some(scope_key), Some(queue_key)) => scope_key > queue_key,
                (Some(_), None) => true,
                _ => false,
            };
            if should_resume_scope && let Some(scope_queue_cursor) = scope_queue_cursor {
                cursor_advance_key = Some(scope_queue_cursor.to_owned());
                list_page = self.object_store().list_page(
                    &prefix,
                    Some(scope_queue_cursor),
                    max_entries,
                )?;
                queue_objects = list_page
                    .objects
                    .into_iter()
                    .filter(|object| object.key.ends_with(".json"))
                    .collect::<Vec<_>>();
                queue_objects.sort_by(|left, right| left.key.cmp(&right.key));
            }
        }
        let base_entries = if self.object_store().exists(&manifest_key(chain))? {
            let key = manifest_key(chain);
            let bytes = self.object_store().get(&key)?;
            decode_manifest_object(&key, &bytes)?.entries
        } else {
            Vec::new()
        };
        let mut entries = Vec::new();
        let mut scope_entries = Vec::new();
        let mut scanned_objects = 0usize;
        let mut scanned_entries = 0usize;
        let mut scope_cursor_overlap_key = None;
        let mut scope_cursor_advance_key = None;
        let mut drained_queue_cursor_advance_key = None;
        let mut stopped_at_next_scope = false;
        let mut stale_queue_entry_keys = Vec::new();
        for object in &queue_objects {
            if entries.len() + scope_entries.len() >= max_entries {
                break;
            }
            if load_started.elapsed().as_millis() >= config.max_tick_duration_ms.max(1) as u128 {
                break;
            }
            let Some(bytes) = self.object_store().get_optional(&object.key)? else {
                cursor_advance_key = Some(object.key.clone());
                continue;
            };
            let queue_entry = compaction_queue::decode_entry(&object.key, &bytes)?;
            let object_scope_prefix = manifest_segment_scope_prefix(&queue_entry.segment_key);
            if active_scope_prefix.is_none() {
                active_scope_prefix = object_scope_prefix.clone();
            } else if active_scope_prefix != object_scope_prefix {
                if compaction_candidates(
                    &scope_entries
                        .iter()
                        .map(|entry: &SelectedManifestEntry| entry.entry.clone())
                        .collect::<Vec<_>>(),
                    config,
                )
                .is_empty()
                {
                    drained_queue_cursor_advance_key = cursor_advance_key.clone();
                    scope_entries.clear();
                    active_scope_prefix = object_scope_prefix.clone();
                    active_scope_cursor = active_scope_prefix
                        .as_deref()
                        .map(compaction_scope_cursor_key)
                        .map(|key| self.read_compaction_cursor_key(&key))
                        .transpose()?;
                } else {
                    stopped_at_next_scope = true;
                    break;
                }
            }
            scanned_objects += 1;
            scope_cursor_overlap_key = cursor_advance_key.clone();
            cursor_advance_key = Some(object.key.clone());
            scope_cursor_advance_key = Some(object.key.clone());
            let Some(segment_bytes) = self.object_store().get_optional(&queue_entry.segment_key)?
            else {
                stale_queue_entry_keys.push(object.key.clone());
                continue;
            };
            let manifest = decode_manifest_object(&queue_entry.segment_key, &segment_bytes)?;
            scanned_entries += manifest.entries.len();
            for entry in manifest.entries {
                if entry.object_key.is_none() {
                    stale_queue_entry_keys.push(object.key.clone());
                    continue;
                }
                if base_entries
                    .iter()
                    .any(|base_entry| base_entry.shadows_segment(&entry))
                {
                    continue;
                }
                scope_entries.push(SelectedManifestEntry {
                    segment_key: Some(queue_entry.segment_key.clone()),
                    cursor_key: Some(object.key.clone()),
                    entry,
                });
                if entries.len() + scope_entries.len() >= max_entries {
                    break;
                }
            }
        }
        entries.extend(scope_entries);
        let scope_partial =
            !stopped_at_next_scope && (scanned_objects < queue_objects.len() || list_page.has_more);
        let partial = !entries.is_empty() && (scope_partial || stopped_at_next_scope);
        let cursor_advance = scope_cursor_advance_key
            .or(cursor_advance_key)
            .map(segment_compaction_cursor);
        let scope_cursor_key = active_scope_prefix
            .as_deref()
            .map(compaction_scope_cursor_key)
            .unwrap_or_else(|| compaction_queue_cursor_key(chain));
        let cursor_overlap = scope_cursor_overlap_key.map(segment_compaction_cursor);
        let queue_cursor_advance = if scope_partial {
            drained_queue_cursor_advance_key.map(segment_compaction_cursor)
        } else {
            cursor_advance.clone()
        };
        log::info!(
            "storage compaction manifest load chain_key={} source=compaction_queue listed_object_count={} scanned_object_count={} scanned_entry_count={} selected_entry_count={} scope_prefix={} partial={} duration_ms={}",
            chain.key_prefix(),
            queue_objects.len(),
            scanned_objects,
            scanned_entries,
            entries.len(),
            active_scope_prefix.as_deref().unwrap_or("none"),
            partial,
            load_started.elapsed().as_millis()
        );
        Ok(CompactionManifestScan {
            entries,
            stale_queue_entry_keys,
            partial,
            cursor_update: CompactionCursorUpdate {
                scope_cursor_key,
                scope_cursor_current: active_scope_cursor,
                scope_cursor_overlap: cursor_overlap,
                scope_cursor_advance: cursor_advance.clone(),
                scope_partial,
                queue_cursor_key: compaction_queue_cursor_key(chain),
                queue_cursor_advance,
            },
        })
    }
}

#[derive(Clone, Debug)]
struct SelectedManifestEntry {
    segment_key: Option<String>,
    cursor_key: Option<String>,
    entry: ManifestEntry,
}

struct CompactSelectedManifestEntriesArgs<'a> {
    entries: Vec<SelectedManifestEntry>,
    stale_queue_entry_keys: Vec<String>,
    config: MaintenanceCompactionConfig,
    started: Instant,
    cursor: Option<CompactionCursorUpdate>,
    scan_partial: bool,
    chain: Option<&'a ChainIdentity>,
    checkpoint: &'a dyn Fn() -> Result<(), DatalensError>,
    coverage_index_v2_checked: bool,
    operation_budget: Option<CompactionOperationBudget>,
}

#[derive(Clone, Debug)]
struct CompactionManifestScan {
    entries: Vec<SelectedManifestEntry>,
    stale_queue_entry_keys: Vec<String>,
    partial: bool,
    cursor_update: CompactionCursorUpdate,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CompactionManifestPublishResult {
    completed: bool,
    published_entries: usize,
}

#[derive(Clone, Debug)]
struct CoverageIndexV2BucketScan {
    buckets: Vec<CoverageIndexV2BucketScanItem>,
    empty_cursor_advances: Vec<CoverageIndexV2CursorAdvance>,
    partial: bool,
    priority: CoverageIndexV2ScanPriority,
}

#[derive(Clone, Debug)]
struct CoverageIndexV2BucketScanItem {
    cursor_key: String,
    bucket: CoverageIndexV2Bucket,
    last_delta_key: Option<String>,
}

#[derive(Clone, Debug)]
struct CoverageIndexV2CursorAdvance {
    cursor_key: String,
    next_segment_key: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CoverageIndexV2ScanPriority {
    SemanticEvmLogs,
    Root,
}

const COVERAGE_INDEX_V2_COMPACTION_PRIORITY_SEMANTIC_EVM_LOGS: &str = "semantic-evm-logs";
const COVERAGE_INDEX_V2_COMPACTION_PRIORITY_ROOT: &str = "root";

impl CoverageIndexV2ScanPriority {
    fn next_priority(self) -> Self {
        match self {
            Self::SemanticEvmLogs => Self::Root,
            Self::Root => Self::SemanticEvmLogs,
        }
    }

    fn cursor_value(self) -> &'static str {
        match self {
            Self::SemanticEvmLogs => COVERAGE_INDEX_V2_COMPACTION_PRIORITY_SEMANTIC_EVM_LOGS,
            Self::Root => COVERAGE_INDEX_V2_COMPACTION_PRIORITY_ROOT,
        }
    }
}

fn coverage_index_v2_report_has_work(report: &CoverageIndexV2CompactionReport) -> bool {
    report.compacted_buckets > 0
        || report.compacted_deltas > 0
        || report.cleanup_records > 0
        || report.deleted_deltas > 0
        || report.delete_failures > 0
}

fn coverage_index_v2_only_compaction_report(
    started: Instant,
    partial: bool,
    operation_budget: CompactionOperationBudget,
    coverage_index_v2_report: CoverageIndexV2CompactionReport,
) -> MaintenanceCompactionReport {
    let duration_ms = duration_millis(started);
    let tick_status = if partial {
        MaintenanceCompactionTickStatus::Partial
    } else {
        MaintenanceCompactionTickStatus::Completed
    };
    log::info!(
        "storage compaction tick summary status={} pause_reason=none input_objects=0 output_objects=0 deleted_source_objects=0 deleted_manifest_segments=0 coverage_index_v2_compacted_buckets={} coverage_index_v2_compacted_deltas={} coverage_index_v2_input_delta_bytes={} coverage_index_v2_cleanup_records={} coverage_index_v2_deleted_deltas={} coverage_index_v2_delta_delete_failures={} duration_ms={}",
        tick_status.as_str(),
        coverage_index_v2_report.compacted_buckets,
        coverage_index_v2_report.compacted_deltas,
        coverage_index_v2_report.input_delta_bytes,
        coverage_index_v2_report.cleanup_records,
        coverage_index_v2_report.deleted_deltas,
        coverage_index_v2_report.delete_failures,
        duration_ms
    );
    MaintenanceCompactionReport {
        read_only: false,
        candidates: Vec::new(),
        candidate_count: 0,
        candidate_backlog: 0,
        backlog: Vec::new(),
        processed_candidates: 0,
        duration_ms,
        tick_status,
        pause_reason: None,
        tick_summary: MaintenanceCompactionTickSummary {
            duration_ms,
            ..MaintenanceCompactionTickSummary::default()
        },
        compacted_objects: 0,
        compacted_rows: 0,
        deleted_source_objects: 0,
        source_delete_failures: 0,
        get_operations: operation_budget.used_gets,
        put_operations: operation_budget.used_puts,
        delete_operations: operation_budget.used_deletes,
        coverage_index_v2_compacted_buckets: coverage_index_v2_report.compacted_buckets,
        coverage_index_v2_compacted_deltas: coverage_index_v2_report.compacted_deltas,
        coverage_index_v2_input_delta_bytes: coverage_index_v2_report.input_delta_bytes,
        coverage_index_v2_cleanup_records: coverage_index_v2_report.cleanup_records,
        coverage_index_v2_deleted_deltas: coverage_index_v2_report.deleted_deltas,
        coverage_index_v2_delta_delete_failures: coverage_index_v2_report.delete_failures,
    }
}

fn merge_coverage_index_v2_report(
    report: &mut MaintenanceCompactionReport,
    coverage_index_v2_report: CoverageIndexV2CompactionReport,
) {
    report.coverage_index_v2_compacted_buckets = report
        .coverage_index_v2_compacted_buckets
        .saturating_add(coverage_index_v2_report.compacted_buckets);
    report.coverage_index_v2_compacted_deltas = report
        .coverage_index_v2_compacted_deltas
        .saturating_add(coverage_index_v2_report.compacted_deltas);
    report.coverage_index_v2_input_delta_bytes = report
        .coverage_index_v2_input_delta_bytes
        .saturating_add(coverage_index_v2_report.input_delta_bytes);
    report.coverage_index_v2_cleanup_records = report
        .coverage_index_v2_cleanup_records
        .saturating_add(coverage_index_v2_report.cleanup_records);
    report.coverage_index_v2_deleted_deltas = report
        .coverage_index_v2_deleted_deltas
        .saturating_add(coverage_index_v2_report.deleted_deltas);
    report.coverage_index_v2_delta_delete_failures = report
        .coverage_index_v2_delta_delete_failures
        .saturating_add(coverage_index_v2_report.delete_failures);
}

#[derive(Clone, Debug)]
struct CoverageIndexV2CleanupScan {
    records: Vec<CoverageIndexV2CleanupRecordObject>,
    partial: bool,
    page_last_key: Option<String>,
    get_operations: usize,
}

struct CoverageIndexV2CleanupArgs<'a> {
    chain: &'a ChainIdentity,
    deadline: CoverageIndexV2CleanupDeadline,
    checkpoint: &'a dyn Fn() -> Result<(), DatalensError>,
    operation_budget: &'a mut CompactionOperationBudget,
    cleanup_scan: Option<CoverageIndexV2CleanupScan>,
    max_delta_deletes: Option<usize>,
    report: &'a mut CoverageIndexV2CompactionReport,
}

#[derive(Clone, Copy)]
struct CoverageIndexV2CleanupDeadline {
    tick_started: Instant,
    max_duration: Duration,
}

impl CoverageIndexV2CleanupDeadline {
    fn expired(self) -> bool {
        self.tick_started.elapsed() >= self.max_duration
    }
}

#[derive(Clone, Debug)]
struct CompactionCursorUpdate {
    scope_cursor_key: String,
    scope_cursor_current: Option<CompactionCursor>,
    scope_cursor_overlap: Option<CompactionCursor>,
    scope_cursor_advance: Option<CompactionCursor>,
    scope_partial: bool,
    queue_cursor_key: String,
    queue_cursor_advance: Option<CompactionCursor>,
}

#[derive(Clone, Copy, Debug)]
struct CompactionOperationBudget {
    max_gets: usize,
    max_puts: usize,
    max_deletes: usize,
    used_gets: usize,
    used_puts: usize,
    used_deletes: usize,
}

impl CompactionOperationBudget {
    fn new(config: MaintenanceCompactionConfig) -> Self {
        Self {
            max_gets: config.max_gets_per_tick,
            max_puts: config.max_puts_per_tick,
            max_deletes: config.max_deletes_per_tick,
            used_gets: 0,
            used_puts: 0,
            used_deletes: 0,
        }
    }

    fn can_process_candidate(
        &self,
        candidate: &CompactionCandidate,
        source_gets: usize,
        manifest_segment_deletes: usize,
    ) -> bool {
        self.remaining_gets() >= source_gets
            && self.remaining_puts() >= candidate.output_entry_count().saturating_add(1)
            && self.remaining_deletes() >= manifest_segment_deletes
    }

    fn record_gets(&mut self, count: usize) {
        self.used_gets = self.used_gets.saturating_add(count);
    }

    fn record_puts(&mut self, count: usize) {
        self.used_puts = self.used_puts.saturating_add(count);
    }

    fn record_deletes(&mut self, count: usize) {
        self.used_deletes = self.used_deletes.saturating_add(count);
    }

    fn remaining_gets(&self) -> usize {
        self.max_gets.saturating_sub(self.used_gets)
    }

    fn remaining_puts(&self) -> usize {
        self.max_puts.saturating_sub(self.used_puts)
    }

    fn remaining_deletes(&self) -> usize {
        self.max_deletes.saturating_sub(self.used_deletes)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
struct CompactionCursor {
    #[serde(default)]
    schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_segment_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    legacy_entry_offset: Option<usize>,
}

fn compaction_cursor_key(chain: &ChainIdentity) -> String {
    format!(
        "chains/{}/metadata/compaction-cursor.json",
        chain.key_prefix()
    )
}

fn compaction_queue_cursor_key(chain: &ChainIdentity) -> String {
    format!(
        "chains/{}/metadata/compaction-scope-queue-cursor.json",
        chain.key_prefix()
    )
}

fn coverage_index_v2_compaction_cursor_key(chain: &ChainIdentity) -> String {
    format!(
        "chains/{}/metadata/coverage-index-v2-compaction-cursor.json",
        chain.key_prefix()
    )
}

fn coverage_index_v2_semantic_evm_logs_compaction_cursor_key(chain: &ChainIdentity) -> String {
    format!(
        "chains/{}/metadata/coverage-index-v2-compaction-semantic-evm-logs-cursor.json",
        chain.key_prefix()
    )
}

fn coverage_index_v2_compaction_priority_cursor_key(chain: &ChainIdentity) -> String {
    format!(
        "chains/{}/metadata/coverage-index-v2-compaction-priority-cursor.json",
        chain.key_prefix()
    )
}

fn coverage_index_v2_cleanup_cursor_key(chain: &ChainIdentity) -> String {
    format!(
        "chains/{}/metadata/coverage-index-v2-cleanup-cursor.json",
        chain.key_prefix()
    )
}

fn compaction_scope_cursor_key(scope_prefix: &str) -> String {
    format!("{scope_prefix}/_metadata/compaction-cursor.json")
}

fn superseded_source_record_prefix(chain: &ChainIdentity) -> String {
    format!(
        "chains/{}/metadata/compaction-superseded-sources",
        chain.key_prefix()
    )
}

fn superseded_source_record_key(chain: &ChainIdentity, object_key: &str) -> String {
    format!(
        "{}/{}.json",
        superseded_source_record_prefix(chain),
        checksum_hex(object_key.as_bytes())
    )
}

fn segment_compaction_cursor(next_segment_key: String) -> CompactionCursor {
    CompactionCursor {
        schema_version: 1,
        next_segment_key: Some(next_segment_key),
        legacy_entry_offset: None,
    }
}

fn legacy_cursor_after_rewritten_manifest(
    cursor: Option<CompactionCursor>,
    processed_candidates: usize,
) -> Option<CompactionCursor> {
    let cursor = cursor?;
    if processed_candidates == 0 || cursor.legacy_entry_offset.is_none() {
        return None;
    }
    Some(CompactionCursor {
        schema_version: 1,
        next_segment_key: None,
        legacy_entry_offset: Some(0),
    })
}

fn decode_manifest_object(key: &str, bytes: &[u8]) -> Result<Manifest, DatalensError> {
    serde_json::from_slice(bytes).map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::StorageReadFailure,
            format!("decode manifest segment {key}: {error}"),
        )
    })
}

fn manifest_segment_scope_prefix(key: &str) -> Option<String> {
    key.rsplit_once('/').map(|(prefix, _)| prefix.to_owned())
}

fn duration_millis(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

fn duration_millis_value(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX - 1)
}

fn unix_millis_now() -> Result<u64, DatalensError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("system clock before unix epoch: {error}"),
            )
        })?;
    Ok(duration.as_millis().try_into().unwrap_or(u64::MAX))
}

fn optional_latency(value: u64) -> Option<u64> {
    (value != u64::MAX).then_some(value)
}

fn compaction_pressure_pause_reason(config: MaintenanceCompactionConfig) -> Option<&'static str> {
    if config.query_latency_pause_threshold_ms > 0
        && config
            .pressure
            .query_latency_ms
            .is_some_and(|latency| latency >= config.query_latency_pause_threshold_ms)
    {
        return Some("query_latency");
    }
    if config.write_latency_pause_threshold_ms > 0
        && config
            .pressure
            .write_latency_ms
            .is_some_and(|latency| latency >= config.write_latency_pause_threshold_ms)
    {
        return Some("write_latency");
    }
    None
}

fn metadata_issues(entry: &ManifestEntry, object_key: &str, bytes: &[u8]) -> Vec<MaintenanceIssue> {
    let mut issues = Vec::new();
    if let Some(expected_size) = entry.object_size_bytes {
        let actual_size = bytes.len() as u64;
        if actual_size != expected_size {
            issues.push(entry_issue(
                entry,
                MaintenanceIssueKind::ObjectSizeMismatch,
                format!(
                    "object {object_key} size mismatch: expected {expected_size} bytes, got {actual_size} bytes"
                ),
            ));
        }
    }

    match (
        entry.checksum.as_deref(),
        entry.checksum_algorithm.as_deref(),
    ) {
        (Some(expected_checksum), Some("sha256")) => {
            let actual_checksum = checksum_hex(bytes);
            if actual_checksum != expected_checksum {
                issues.push(entry_issue(
                    entry,
                    MaintenanceIssueKind::ObjectChecksumMismatch,
                    format!("object {object_key} checksum mismatch for sha256"),
                ));
            }
        }
        (Some(_), Some(algorithm)) => issues.push(entry_issue(
            entry,
            MaintenanceIssueKind::UnknownChecksumAlgorithm,
            format!("object {object_key} unknown checksum algorithm {algorithm}"),
        )),
        (Some(_), None) | (None, Some(_)) | (None, None) => {}
    }

    if issues.is_empty()
        && let Err(error) = verify_manifest_object_metadata(entry, object_key, bytes)
    {
        issues.push(entry_issue(
            entry,
            MaintenanceIssueKind::ObjectDecodeFailure,
            error.message,
        ));
    }
    issues
}

fn contradictory_coverage_issues(entries: &[ManifestEntry]) -> Vec<MaintenanceIssue> {
    let mut issues = Vec::new();
    for (index, left) in entries.iter().enumerate() {
        for right in entries.iter().skip(index + 1) {
            if left.chain == right.chain
                && left.dataset_key == right.dataset_key
                && left.selector_fingerprint == right.selector_fingerprint
                && left.range.kind() == right.range.kind()
                && left.range.intersection(&right.range).is_some()
                && (left.finality_level != right.finality_level
                    || left.object_encoding != right.object_encoding)
            {
                issues.push(entry_issue(
                    left,
                    MaintenanceIssueKind::ContradictoryCoverage,
                    "overlapping coverage has incompatible finality or object encoding".to_owned(),
                ));
            }
        }
    }
    issues
}

fn compaction_candidates(
    entries: &[ManifestEntry],
    config: MaintenanceCompactionConfig,
) -> Vec<CompactionCandidate> {
    let mut groups: Vec<(CompactionKey, Vec<&ManifestEntry>)> = Vec::new();
    for entry in entries {
        let (Some(object_key), Some(object_encoding)) = (&entry.object_key, entry.object_encoding)
        else {
            continue;
        };
        if object_key.is_empty() || is_compacted_object_key(object_key) {
            continue;
        }
        if entry
            .object_size_bytes
            .is_some_and(|size| size >= config.min_object_bytes)
        {
            continue;
        }
        let key = CompactionKey::from_entry(entry, object_encoding);
        if let Some((_, entries)) = groups.iter_mut().find(|(existing, _)| existing == &key) {
            entries.push(entry);
        } else {
            groups.push((key, vec![entry]));
        }
    }

    let mut candidates = Vec::new();
    for (key, mut entries) in groups {
        entries.sort_by_key(|entry| (entry.range.start(), entry.range.end()));
        let mut run = Vec::new();
        let mut run_bytes = 0u64;
        for entry in entries {
            let entry_bytes = entry.object_size_bytes.unwrap_or(0);
            if !run.is_empty()
                && candidate_input_limit_reached(&run, run_bytes, entry_bytes, config)
            {
                push_candidate(&mut candidates, &key, &run, config);
                run.clear();
                run_bytes = 0;
            }
            run.push(entry);
            run_bytes = run_bytes.saturating_add(entry_bytes);
            if run.len() >= 2 && run_bytes >= config.target_object_bytes {
                push_candidate(&mut candidates, &key, &run, config);
                run.clear();
                run_bytes = 0;
            }
        }
        push_candidate(&mut candidates, &key, &run, config);
    }
    candidates
}

fn compaction_backlog_report(
    entries: &[ManifestEntry],
    raw_entries: &[ManifestEntry],
    manifest_objects: &[ObjectMetadata],
    config: MaintenanceCompactionConfig,
) -> MaintenanceCompactionBacklogReport {
    let mut chains = Vec::<(ChainIdentity, BacklogChainAccumulator)>::new();
    for entry in raw_entries {
        if chains.iter().all(|(chain, _)| chain != &entry.chain) {
            chains.push((entry.chain.clone(), BacklogChainAccumulator::default()));
        }
    }
    for entry in entries {
        let Some(object_key) = entry.object_key.as_ref() else {
            continue;
        };
        if object_key.is_empty()
            || entry
                .object_size_bytes
                .is_some_and(|size| size >= config.min_object_bytes)
        {
            continue;
        }
        if let Some((_, accumulator)) = chains.iter_mut().find(|(chain, _)| chain == &entry.chain) {
            accumulator.add_entry(entry);
        } else {
            let mut accumulator = BacklogChainAccumulator::default();
            accumulator.add_entry(entry);
            chains.push((entry.chain.clone(), accumulator));
        }
    }

    for object in manifest_objects {
        if !is_manifest_segment_object(&object.key) {
            continue;
        }
        for (chain, accumulator) in &mut chains {
            if object
                .key
                .starts_with(&format!("chains/{}/manifest-segments/", chain.key_prefix()))
            {
                accumulator.manifest_segment_count += 1;
                break;
            }
        }
    }

    let mut chain_reports = chains
        .into_iter()
        .map(|(chain, accumulator)| accumulator.into_report(chain))
        .collect::<Vec<_>>();
    chain_reports.sort_by_key(|chain| chain.chain.key_prefix());

    MaintenanceCompactionBacklogReport {
        min_object_bytes: config.min_object_bytes,
        small_object_count: chain_reports
            .iter()
            .map(|chain| chain.small_object_count)
            .sum(),
        small_object_bytes: chain_reports
            .iter()
            .map(|chain| chain.small_object_bytes)
            .sum(),
        manifest_segment_count: chain_reports
            .iter()
            .map(|chain| chain.manifest_segment_count)
            .sum(),
        chains: chain_reports,
    }
}

#[derive(Clone, Debug, Default)]
struct BacklogChainAccumulator {
    small_object_count: usize,
    small_object_bytes: u64,
    manifest_segment_count: usize,
    datasets: Vec<(DatasetKey, BacklogDatasetAccumulator)>,
}

impl BacklogChainAccumulator {
    fn add_entry(&mut self, entry: &ManifestEntry) {
        let object_size_bytes = entry.object_size_bytes.unwrap_or(0);
        self.small_object_count += 1;
        self.small_object_bytes = self.small_object_bytes.saturating_add(object_size_bytes);
        if let Some((_, accumulator)) = self
            .datasets
            .iter_mut()
            .find(|(dataset_key, _)| dataset_key == &entry.dataset_key)
        {
            accumulator.add_entry(entry, object_size_bytes);
        } else {
            let mut accumulator = BacklogDatasetAccumulator::default();
            accumulator.add_entry(entry, object_size_bytes);
            self.datasets.push((entry.dataset_key.clone(), accumulator));
        }
    }

    fn into_report(self, chain: ChainIdentity) -> MaintenanceCompactionBacklogChain {
        MaintenanceCompactionBacklogChain {
            chain,
            small_object_count: self.small_object_count,
            small_object_bytes: self.small_object_bytes,
            manifest_segment_count: self.manifest_segment_count,
            datasets: sorted_backlog_datasets(self.datasets),
        }
    }
}

fn sorted_backlog_datasets(
    mut datasets: Vec<(DatasetKey, BacklogDatasetAccumulator)>,
) -> Vec<MaintenanceCompactionBacklogDataset> {
    datasets.sort_by_key(|(dataset_key, _)| dataset_key.as_str().to_owned());
    datasets
        .into_iter()
        .map(|(dataset_key, accumulator)| accumulator.into_report(dataset_key))
        .collect()
}

#[derive(Clone, Debug, Default)]
struct BacklogDatasetAccumulator {
    small_object_count: usize,
    small_object_bytes: u64,
    selectors: BTreeMap<(String, String), BacklogSelectorAccumulator>,
}

impl BacklogDatasetAccumulator {
    fn add_entry(&mut self, entry: &ManifestEntry, object_size_bytes: u64) {
        self.small_object_count += 1;
        self.small_object_bytes = self.small_object_bytes.saturating_add(object_size_bytes);
        self.selectors
            .entry((
                entry.selector_fingerprint.clone(),
                entry.selector_canonical_key.clone(),
            ))
            .or_default()
            .add_entry(object_size_bytes);
    }

    fn into_report(self, dataset_key: DatasetKey) -> MaintenanceCompactionBacklogDataset {
        MaintenanceCompactionBacklogDataset {
            dataset_key,
            small_object_count: self.small_object_count,
            small_object_bytes: self.small_object_bytes,
            selectors: self
                .selectors
                .into_iter()
                .map(
                    |((selector_fingerprint, selector_canonical_key), accumulator)| {
                        accumulator.into_report(selector_fingerprint, selector_canonical_key)
                    },
                )
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct BacklogSelectorAccumulator {
    small_object_count: usize,
    small_object_bytes: u64,
}

impl BacklogSelectorAccumulator {
    fn add_entry(&mut self, object_size_bytes: u64) {
        self.small_object_count += 1;
        self.small_object_bytes = self.small_object_bytes.saturating_add(object_size_bytes);
    }

    fn into_report(
        self,
        selector_fingerprint: String,
        selector_canonical_key: String,
    ) -> MaintenanceCompactionBacklogSelector {
        MaintenanceCompactionBacklogSelector {
            selector_fingerprint,
            selector_canonical_key,
            small_object_count: self.small_object_count,
            small_object_bytes: self.small_object_bytes,
        }
    }
}

fn candidate_input_limit_reached(
    run: &[&ManifestEntry],
    run_bytes: u64,
    entry_bytes: u64,
    config: MaintenanceCompactionConfig,
) -> bool {
    let max_objects = config.max_input_objects_per_candidate.max(2);
    if run.len() >= max_objects {
        return true;
    }
    let next_bytes = run_bytes.saturating_add(entry_bytes);
    let max_input_bytes = config.max_input_bytes_per_candidate.max(1);
    let max_output_bytes = config.max_output_object_bytes.max(1);
    run.len() >= 2 && (next_bytes > max_input_bytes || next_bytes > max_output_bytes)
}

fn compaction_backlog_scopes(candidates: &[CompactionCandidate]) -> Vec<CompactionBacklogScope> {
    let mut scopes =
        BTreeMap::<(String, String, String, String, String), CompactionBacklogScope>::new();
    for candidate in candidates {
        let selector_kind = compaction_selector_kind(candidate);
        let key = (
            candidate.chain.key_prefix(),
            candidate.dataset_key.as_str().to_owned(),
            candidate.selector_fingerprint.clone(),
            candidate.selector_canonical_key.clone(),
            selector_kind.clone(),
        );
        let scope = scopes.entry(key).or_insert_with(|| CompactionBacklogScope {
            chain: candidate.chain.clone(),
            dataset_key: candidate.dataset_key.clone(),
            selector_fingerprint: candidate.selector_fingerprint.clone(),
            selector_canonical_key: candidate.selector_canonical_key.clone(),
            selector_kind,
            small_objects: 0,
            manifest_segments: 0,
            candidate_backlog: 0,
        });
        scope.small_objects = scope.small_objects.saturating_add(candidate.entry_count);
        scope.manifest_segments = scope
            .manifest_segments
            .saturating_add(candidate.entry_count);
        scope.candidate_backlog = scope.candidate_backlog.saturating_add(1);
    }
    scopes.into_values().collect()
}

fn compaction_selector_kind(candidate: &CompactionCandidate) -> String {
    if candidate.selector_fingerprint == "all" || candidate.selector_canonical_key == "all" {
        return "all".to_owned();
    }
    candidate
        .selector_canonical_key
        .split(':')
        .next()
        .filter(|kind| !kind.is_empty())
        .unwrap_or("unknown")
        .to_owned()
}

#[derive(Clone, Debug)]
struct CompactedObject {
    row_count: usize,
    entries: Vec<ManifestEntry>,
}

#[derive(Clone, Debug, Default)]
struct CoverageIndexV2CompactionReport {
    compacted_buckets: usize,
    compacted_deltas: usize,
    input_delta_bytes: u64,
    cleanup_records: usize,
    deleted_deltas: usize,
    delete_failures: usize,
}

#[derive(Clone, Debug, Default)]
struct CoverageIndexV2FragmentationReport {
    coverage_delta_count: usize,
    coverage_delta_bytes: u64,
    coverage_snapshot_count: usize,
    coverage_snapshot_age_ms_max: u64,
    coverage_cleanup_record_count: usize,
    coverage_delta_backlog_top: Vec<CoverageDeltaBacklogScope>,
}

#[derive(Clone, Debug, Default)]
struct CoverageDeltaBacklogAccumulator {
    object_count: usize,
    bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CompactionSourceCleanup {
    deleted_objects: usize,
    delete_failures: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CoverageIndexV2DeltaCleanup {
    deleted_objects: usize,
    delete_failures: usize,
    deleted_keys: Vec<String>,
    partial: bool,
}

fn coverage_index_v2_fragmentation_report<S>(
    object_store: &S,
) -> Result<CoverageIndexV2FragmentationReport, DatalensError>
where
    S: ObjectStore,
{
    let now_ms = unix_millis_now()?;
    let mut report = CoverageIndexV2FragmentationReport::default();
    let mut backlog =
        BTreeMap::<String, (CoverageDeltaBacklogScope, CoverageDeltaBacklogAccumulator)>::new();

    for object in object_store.list("chains")? {
        if is_coverage_index_v2_delta_object(&object.key) {
            report.coverage_delta_count += 1;
            report.coverage_delta_bytes = report.coverage_delta_bytes.saturating_add(object.size);
            if let Some(scope) = coverage_delta_backlog_scope_from_key(&object.key) {
                let key = coverage_delta_backlog_scope_key(&scope);
                let (_, accumulator) = backlog
                    .entry(key)
                    .or_insert_with(|| (scope, CoverageDeltaBacklogAccumulator::default()));
                accumulator.object_count = accumulator.object_count.saturating_add(1);
                accumulator.bytes = accumulator.bytes.saturating_add(object.size);
            }
            continue;
        }
        if is_coverage_index_v2_snapshot_object(&object.key) {
            report.coverage_snapshot_count += 1;
            let bytes = object_store.get(&object.key)?;
            if let Ok(created_at) = serde_json::from_slice::<CoverageIndexV2CreatedAt>(&bytes)
                && created_at.created_at_unix_ms > 0
            {
                report.coverage_snapshot_age_ms_max = report
                    .coverage_snapshot_age_ms_max
                    .max(now_ms.saturating_sub(created_at.created_at_unix_ms));
            }
            continue;
        }
        if is_coverage_index_v2_cleanup_object(&object.key) {
            report.coverage_cleanup_record_count += 1;
        }
    }

    let mut top = backlog
        .into_values()
        .map(|(mut scope, accumulator)| {
            scope.object_count = accumulator.object_count;
            scope.bytes = accumulator.bytes;
            scope
        })
        .collect::<Vec<_>>();
    top.sort_by(|left, right| {
        right
            .bytes
            .cmp(&left.bytes)
            .then_with(|| left.chain.key_prefix().cmp(&right.chain.key_prefix()))
            .then_with(|| left.dataset_key.as_str().cmp(right.dataset_key.as_str()))
            .then_with(|| left.bucket_start.cmp(&right.bucket_start))
            .then_with(|| left.bucket_end.cmp(&right.bucket_end))
    });
    top.truncate(10);
    report.coverage_delta_backlog_top = top;
    Ok(report)
}

fn is_coverage_index_v2_delta_object(object_key: &str) -> bool {
    object_key.contains("/coverage-index-v2/deltas/") && object_key.ends_with(".json")
}

fn is_coverage_index_v2_snapshot_object(object_key: &str) -> bool {
    object_key.contains("/coverage-index-v2/snapshots/") && object_key.ends_with(".json")
}

fn is_coverage_index_v2_cleanup_object(object_key: &str) -> bool {
    object_key.contains("/coverage-index-v2/cleanup/") && object_key.ends_with(".json")
}

fn coverage_delta_backlog_scope_from_key(object_key: &str) -> Option<CoverageDeltaBacklogScope> {
    let parts = object_key.split('/').collect::<Vec<_>>();
    let index_root = parts.iter().position(|part| *part == "coverage-index-v2")?;
    if parts.first() != Some(&"chains")
        || parts.get(index_root + 1) != Some(&"deltas")
        || parts.len() < index_root + 5
    {
        return None;
    }
    let chain = chain_from_key_prefix(&parts[1..index_root].join("/"))?;
    let bucket = parts.get(parts.len().saturating_sub(2))?;
    let (bucket_start, bucket_end) = parse_bucket_range(bucket)?;
    let scope_parts = &parts[index_root + 2..parts.len().saturating_sub(2)];
    let dataset_key = scope_parts
        .get(1)
        .and_then(|value| DatasetKey::parse(value).ok())?;
    let (scope_kind, scope_class, selector_fingerprint) = match scope_parts.first().copied() {
        Some("exact") => {
            let raw_selector_fingerprint = exact_coverage_delta_backlog_selector(scope_parts);
            let selector_fingerprint = raw_selector_fingerprint
                .as_deref()
                .filter(|value| is_safe_exact_coverage_delta_backlog_selector(value))
                .map(str::to_owned);
            (
                "exact".to_owned(),
                exact_coverage_delta_backlog_scope_class(raw_selector_fingerprint.as_deref()),
                selector_fingerprint,
            )
        }
        Some("semantic") => (
            "semantic".to_owned(),
            semantic_coverage_delta_backlog_scope_class(scope_parts.get(5..).unwrap_or(&[])),
            None,
        ),
        _ => ("unknown".to_owned(), "unknown".to_owned(), None),
    };
    Some(CoverageDeltaBacklogScope {
        chain,
        dataset_key,
        scope_kind,
        scope_class,
        selector_fingerprint,
        bucket_start,
        bucket_end,
        object_count: 0,
        bytes: 0,
    })
}

fn coverage_delta_backlog_scope_key(scope: &CoverageDeltaBacklogScope) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        scope.chain.key_prefix(),
        scope.dataset_key.as_str(),
        scope.scope_kind.as_str(),
        scope.scope_class.as_str(),
        scope.selector_fingerprint.as_deref().unwrap_or(""),
        scope.bucket_start,
        scope.bucket_end
    )
}

fn exact_coverage_delta_backlog_selector(scope_parts: &[&str]) -> Option<String> {
    if scope_parts.len() < 5 {
        return None;
    }
    let selector_parts = &scope_parts[3..scope_parts.len().saturating_sub(1)];
    if selector_parts.is_empty() {
        None
    } else {
        Some(selector_parts.join("/"))
    }
}

fn exact_coverage_delta_backlog_scope_class(selector_fingerprint: Option<&str>) -> String {
    match selector_fingerprint {
        Some("all") => "all".to_owned(),
        Some(_) => "selector".to_owned(),
        None => "unknown".to_owned(),
    }
}

fn is_safe_exact_coverage_delta_backlog_selector(value: &str) -> bool {
    value == "all"
        || (value.len() <= 128
            && !value.contains('=')
            && !value.contains("0x")
            && value
                .chars()
                .all(|char| char.is_ascii_alphanumeric() || matches!(char, '/' | '-' | '_' | '.')))
}

fn semantic_coverage_delta_backlog_scope_class(scope_parts: &[&str]) -> String {
    match scope_parts {
        ["addr", "*"] => "addr_wildcard".to_owned(),
        ["addr", _] => "addr_value".to_owned(),
        ["topic", "*"] => "topic_wildcard".to_owned(),
        ["topic", _, "*"] => "topic_slot_wildcard".to_owned(),
        ["topic", _, "[]"] => "topic_empty".to_owned(),
        ["topic", _, "_large-any-of"] => "topic_large_any_of".to_owned(),
        ["topic", _, _] => "topic_value".to_owned(),
        _ => "other".to_owned(),
    }
}

fn chain_from_key_prefix(prefix: &str) -> Option<ChainIdentity> {
    let parts = prefix.split('/').collect::<Vec<_>>();
    let family = match parts.first().copied()? {
        "evm" => ChainFamily::Evm,
        other => ChainFamily::try_other(other.to_owned()).ok()?,
    };
    let configured_name = parts.get(1)?;
    let network_id = match parts.get(2) {
        Some(value) => Some(
            value
                .parse::<u64>()
                .map(NetworkId::numeric)
                .or_else(|_| NetworkId::textual(*value))
                .ok()?,
        ),
        None => None,
    };
    if parts.len() > 3 {
        return None;
    }
    ChainIdentity::try_new(family, *configured_name, network_id).ok()
}

fn parse_bucket_range(value: &str) -> Option<(u64, u64)> {
    let (start, end) = value.split_once('-')?;
    Some((start.parse().ok()?, end.parse().ok()?))
}

fn candidate_selected_entries(
    entries: &[SelectedManifestEntry],
    candidate: &CompactionCandidate,
) -> Vec<SelectedManifestEntry> {
    let mut entries = entries
        .iter()
        .filter(|selected| candidate_entry_matches(&selected.entry, candidate))
        .cloned()
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| (entry.entry.range.start(), entry.entry.range.end()));
    entries
}

fn candidate_entry_matches(entry: &ManifestEntry, candidate: &CompactionCandidate) -> bool {
    entry.chain == candidate.chain
        && entry.dataset_key == candidate.dataset_key
        && entry.selector_fingerprint == candidate.selector_fingerprint
        && entry.selector_canonical_key == candidate.selector_canonical_key
        && entry.range.kind() == candidate.range.kind()
        && entry.finality_level == candidate.finality_level
        && entry.object_encoding == Some(candidate.object_encoding)
        && entry.object_key.as_ref().is_some_and(|object_key| {
            candidate
                .object_keys
                .iter()
                .any(|candidate_key| candidate_key == object_key)
        })
}

fn push_candidate(
    candidates: &mut Vec<CompactionCandidate>,
    key: &CompactionKey,
    entries: &[&ManifestEntry],
    config: MaintenanceCompactionConfig,
) {
    if entries.len() < 2 {
        return;
    }
    let source_ranges = entries
        .iter()
        .map(|entry| entry.range.clone())
        .collect::<Vec<_>>();
    if !source_ranges_are_contiguous(&source_ranges) {
        if !sparse_source_ranges_are_compactable(&source_ranges) {
            return;
        }
        let max_sparse_source_ranges = config
            .max_puts_per_tick
            .saturating_sub(1)
            .min(MAX_SPARSE_SOURCE_RANGES_PER_CANDIDATE);
        if source_ranges.len() > max_sparse_source_ranges {
            push_sparse_candidates_within_limits(
                candidates,
                key,
                entries,
                config,
                max_sparse_source_ranges,
            );
            return;
        }
    }
    let input_object_bytes = entries
        .iter()
        .filter_map(|entry| entry.object_size_bytes)
        .fold(0u64, u64::saturating_add);
    if input_object_bytes > config.max_output_object_bytes.max(1) {
        return;
    }
    let start = entries
        .iter()
        .map(|entry| entry.range.start())
        .min()
        .expect("start");
    let end = entries
        .iter()
        .map(|entry| entry.range.end())
        .max()
        .expect("end");
    if !source_ranges_are_contiguous(&source_ranges)
        && range_span_blocks(start, end) > MAX_SPARSE_CANDIDATE_RANGE_SPAN_BLOCKS
    {
        let max_sparse_source_ranges = config
            .max_puts_per_tick
            .saturating_sub(1)
            .min(MAX_SPARSE_SOURCE_RANGES_PER_CANDIDATE);
        push_sparse_candidates_within_limits(
            candidates,
            key,
            entries,
            config,
            max_sparse_source_ranges,
        );
        return;
    }
    let range = LedgerRange::try_new(key.range_kind.clone(), start, end).expect("valid range");
    candidates.push(CompactionCandidate {
        chain: key.chain.clone(),
        dataset_key: key.dataset_key.clone(),
        selector_fingerprint: key.selector_fingerprint.clone(),
        selector_canonical_key: key.selector_canonical_key.clone(),
        range_kind: range_kind_key(key.range_kind.clone()),
        finality_level: key.finality_level,
        object_encoding: key.object_encoding,
        range,
        entry_count: entries.len(),
        input_object_bytes,
        target_object_bytes: config.target_object_bytes,
        max_output_object_bytes: config.max_output_object_bytes,
        object_keys: entries
            .iter()
            .filter_map(|entry| entry.object_key.clone())
            .collect(),
        source_ranges,
    });
}

fn push_sparse_candidates_within_limits(
    candidates: &mut Vec<CompactionCandidate>,
    key: &CompactionKey,
    entries: &[&ManifestEntry],
    config: MaintenanceCompactionConfig,
    max_sparse_source_ranges: usize,
) {
    let min_sparse_source_ranges = if entries
        .iter()
        .any(|entry| entry.range.start() < entry.range.end())
    {
        2
    } else {
        3
    };
    if max_sparse_source_ranges < min_sparse_source_ranges {
        return;
    }
    let mut offset = 0usize;
    while offset < entries.len() {
        let remaining = entries.len() - offset;
        let mut chunk_len = 0usize;
        while chunk_len < remaining && chunk_len < max_sparse_source_ranges {
            let next_len = chunk_len + 1;
            if entries_range_span_blocks(&entries[offset..offset + next_len])
                > MAX_SPARSE_CANDIDATE_RANGE_SPAN_BLOCKS
            {
                break;
            }
            chunk_len = next_len;
        }
        if chunk_len < min_sparse_source_ranges {
            offset += 1;
            continue;
        }
        let trailing = remaining.saturating_sub(chunk_len);
        if trailing > 0 && trailing < min_sparse_source_ranges {
            let borrow = min_sparse_source_ranges - trailing;
            if chunk_len <= min_sparse_source_ranges.saturating_add(borrow) {
                return;
            }
            chunk_len -= borrow;
        }
        push_candidate(
            candidates,
            key,
            &entries[offset..offset + chunk_len],
            config,
        );
        offset += chunk_len;
    }
}

fn entries_range_span_blocks(entries: &[&ManifestEntry]) -> u64 {
    let start = entries
        .iter()
        .map(|entry| entry.range.start())
        .min()
        .unwrap_or(0);
    let end = entries
        .iter()
        .map(|entry| entry.range.end())
        .max()
        .unwrap_or(0);
    range_span_blocks(start, end)
}

fn range_span_blocks(start: u64, end: u64) -> u64 {
    end.saturating_sub(start).saturating_add(1)
}

impl CompactionCandidate {
    fn output_entry_count(&self) -> usize {
        if source_ranges_are_contiguous(&self.source_ranges) {
            1
        } else {
            self.source_ranges.len()
        }
    }
}

fn is_compacted_object_key(object_key: &str) -> bool {
    object_key.contains("/compacted/")
}

fn source_ranges_are_contiguous(ranges: &[LedgerRange]) -> bool {
    ranges
        .windows(2)
        .all(|window| window[1].start() == window[0].end().saturating_add(1))
}

fn sparse_source_ranges_are_compactable(ranges: &[LedgerRange]) -> bool {
    ranges.len() >= 3 || ranges.iter().any(|range| range.start() < range.end())
}

fn compacted_manifest_entries(
    candidate: &CompactionCandidate,
    source_entries: &[ManifestEntry],
    data_object: StorageDataObject,
) -> Vec<ManifestEntry> {
    let ranges = if source_ranges_are_contiguous(&candidate.source_ranges) {
        vec![candidate.range.clone()]
    } else {
        candidate.source_ranges.clone()
    };
    ranges
        .into_iter()
        .map(|range| {
            let row_count = if range == candidate.range {
                data_object.row_count
            } else {
                source_entries
                    .iter()
                    .filter(|entry| entry.range == range)
                    .map(|entry| entry.row_count)
                    .sum()
            };
            ManifestEntry {
                chain: candidate.chain.clone(),
                dataset_key: candidate.dataset_key.clone(),
                range,
                selector_fingerprint: candidate.selector_fingerprint.clone(),
                selector_canonical_key: candidate.selector_canonical_key.clone(),
                finality_level: candidate.finality_level,
                object_key: Some(data_object.object_key.clone()),
                object_encoding: Some(data_object.object_encoding),
                object_compression: data_object.object_compression,
                row_count,
                object_size_bytes: Some(data_object.object_size_bytes),
                checksum: Some(data_object.checksum.clone()),
                checksum_algorithm: Some(data_object.checksum_algorithm.clone()),
                written_at_unix_seconds: Some(data_object.written_at_unix_seconds),
            }
        })
        .collect()
}

fn compacted_object_key(candidate: &CompactionCandidate, checksum: &str) -> String {
    let checksum_prefix = checksum.get(..16).unwrap_or(checksum);
    format!(
        "chains/{}/datasets/{}/{}/{}/{}/compacted/{:020}-{:020}-{}{}",
        candidate.chain.key_prefix(),
        candidate.dataset_key.as_str(),
        candidate.object_encoding.as_str(),
        range_kind_key(candidate.range.kind()),
        candidate.selector_fingerprint,
        candidate.range.start(),
        candidate.range.end(),
        checksum_prefix,
        candidate.object_encoding.extension(),
    )
}

fn verify_existing_compacted_object(
    candidate: &CompactionCandidate,
    data_object: &StorageDataObject,
    bytes: &[u8],
) -> Result<(), DatalensError> {
    if data_object.object_encoding != candidate.object_encoding {
        return Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            format!(
                "existing compacted object {} encoding metadata differs",
                data_object.object_key
            ),
        ));
    }
    match (data_object.object_encoding, data_object.object_compression) {
        (ObjectEncoding::Json, None) | (ObjectEncoding::ParquetV1, Some(_)) => {}
        _ => {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!(
                    "existing compacted object {} compression metadata is incompatible",
                    data_object.object_key
                ),
            ));
        }
    }
    if data_object.checksum_algorithm != "sha256" {
        return Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            format!(
                "existing compacted object {} checksum algorithm {} is unsupported",
                data_object.object_key, data_object.checksum_algorithm
            ),
        ));
    }
    let actual_size = bytes.len() as u64;
    if actual_size != data_object.object_size_bytes {
        return Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            format!(
                "existing compacted object {} size mismatch: expected {} bytes, got {} bytes",
                data_object.object_key, data_object.object_size_bytes, actual_size
            ),
        ));
    }
    let actual_checksum = checksum_hex(bytes);
    if actual_checksum != data_object.checksum {
        return Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            format!(
                "existing compacted object {} checksum mismatch for sha256",
                data_object.object_key
            ),
        ));
    }
    let decoded = decode_object_rows(
        data_object.object_encoding,
        candidate.dataset_key.clone(),
        bytes,
    )
    .map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            format!(
                "decode existing compacted object {}: {}",
                data_object.object_key, error.message
            ),
        )
    })?;
    if decoded.dataset_key() != &candidate.dataset_key {
        return Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            format!(
                "existing compacted object {} dataset key mismatch",
                data_object.object_key
            ),
        ));
    }
    if decoded.row_count() != data_object.row_count {
        return Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            format!(
                "existing compacted object {} row count mismatch: expected {}, got {}",
                data_object.object_key,
                data_object.row_count,
                decoded.row_count()
            ),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CompactionKey {
    chain: ChainIdentity,
    dataset_key: DatasetKey,
    selector_fingerprint: String,
    selector_canonical_key: String,
    range_kind: LedgerRangeKind,
    finality_level: ManifestFinalityLevel,
    object_encoding: ObjectEncoding,
}

impl CompactionKey {
    fn from_entry(entry: &ManifestEntry, object_encoding: ObjectEncoding) -> Self {
        Self {
            chain: entry.chain.clone(),
            dataset_key: entry.dataset_key.clone(),
            selector_fingerprint: entry.selector_fingerprint.clone(),
            selector_canonical_key: entry.selector_canonical_key.clone(),
            range_kind: entry.range.kind(),
            finality_level: entry.finality_level,
            object_encoding,
        }
    }
}

fn current_object_keys(entries: &[ManifestEntry]) -> Vec<String> {
    let mut keys = entries
        .iter()
        .filter_map(|entry| entry.object_key.clone())
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    keys
}

fn entry_shadowed_by_live_data_object<S>(
    entry: &ManifestEntry,
    entries: &[ManifestEntry],
    object_store: &S,
) -> Result<bool, DatalensError>
where
    S: ObjectStore,
{
    for candidate in entries {
        if candidate.object_key == entry.object_key {
            continue;
        }
        let Some(object_key) = candidate.object_key.as_deref() else {
            continue;
        };
        if candidate.shadows_segment(entry) && object_store.exists(object_key)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn is_data_object(object_key: &str) -> bool {
    object_key != manifest_key_from_object_key(object_key)
        && !object_key.contains("/coverage-index/")
        && !object_key.contains("/coverage-index-v2/")
        && !object_key.contains("/metadata/compaction-queue/")
        && !object_key.contains("/manifest-segments/")
        && (object_key.ends_with(".json") || object_key.ends_with(".parquet"))
}

fn is_manifest_object(object_key: &str) -> bool {
    object_key.ends_with("/manifest.json") || is_manifest_segment_object(object_key)
}

fn is_manifest_segment_object(object_key: &str) -> bool {
    object_key.contains("/manifest-segments/")
        && object_key.ends_with(".json")
        && !object_key.contains("/_metadata/")
}

fn manifest_key_from_object_key(object_key: &str) -> String {
    let mut parts = object_key.split('/').take(4).collect::<Vec<_>>();
    if parts.len() == 4 && parts.first() == Some(&"chains") {
        parts.push("manifest.json");
        parts.join("/")
    } else {
        String::new()
    }
}

fn entry_issue(
    entry: &ManifestEntry,
    issue_kind: MaintenanceIssueKind,
    message: String,
) -> MaintenanceIssue {
    MaintenanceIssue {
        issue_kind,
        chain: Some(entry.chain.clone()),
        dataset_key: Some(entry.dataset_key.clone()),
        selector_fingerprint: Some(entry.selector_fingerprint.clone()),
        range: Some(entry.range.clone()),
        object_key: entry.object_key.clone(),
        message,
    }
}
