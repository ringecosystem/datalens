use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
    LedgerRangeKind,
};
use serde::{Deserialize, Serialize};

use crate::{
    DurableStorage, Manifest, ManifestEntry, ManifestFinalityLevel, ObjectEncoding, ObjectMetadata,
    ObjectStore, ParquetCompression, StorageDataObject, checksum_hex, decode_object_rows,
    encode_object_rows, manifest_key, manifest_segment_prefix, range_kind_key, unix_seconds_now,
    verify_manifest_object_metadata,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceReport {
    pub read_only: bool,
    pub mode: MaintenanceOperationMode,
    pub operations: Vec<MaintenanceOperation>,
    pub check: MaintenanceCheckReport,
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
    pub processed_candidates: usize,
    pub duration_ms: u64,
    pub tick_status: MaintenanceCompactionTickStatus,
    pub compacted_objects: usize,
    pub compacted_rows: usize,
    pub deleted_source_objects: usize,
    pub source_delete_failures: usize,
    pub get_operations: usize,
    pub put_operations: usize,
    pub delete_operations: usize,
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
    pub max_merge_ranges: usize,
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
    pub delete_source_objects: bool,
}

impl Default for MaintenanceCompactionConfig {
    fn default() -> Self {
        Self {
            min_object_bytes: u64::MAX,
            max_merge_ranges: 32,
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
            delete_source_objects: false,
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
    pub object_keys: Vec<String>,
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
        let compaction_reconciliation =
            self.compaction_reconciliation_report(&entries, &raw_entries, true)?;
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
            compaction_backlog,
            compaction: MaintenanceCompactionReport {
                read_only: true,
                candidate_count: candidates.len(),
                candidates,
                processed_candidates: 0,
                duration_ms: 0,
                tick_status: MaintenanceCompactionTickStatus::Completed,
                compacted_objects: 0,
                compacted_rows: 0,
                deleted_source_objects: 0,
                source_delete_failures: 0,
                get_operations: 0,
                put_operations: 0,
                delete_operations: 0,
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
                entry,
            })
            .collect::<Vec<_>>();
        self.compact_selected_manifest_entries(entries, config, started, None, false)
    }

    pub fn compact_small_objects_for_chain(
        &self,
        chain: &ChainIdentity,
        config: MaintenanceCompactionConfig,
    ) -> Result<MaintenanceCompactionReport, DatalensError> {
        let started = Instant::now();
        log::info!(
            "storage compaction tick started chain_key={} max_tick_duration_ms={} max_candidates_per_tick={} max_manifest_entries_per_tick={}",
            chain.key_prefix(),
            config.max_tick_duration_ms,
            config.max_candidates_per_tick,
            config.max_manifest_entries_per_tick
        );
        let scan = self.scan_compaction_manifest_entries(chain, config, started)?;
        let report = self.compact_selected_manifest_entries(
            scan.entries,
            config,
            started,
            Some((chain, scan.cursor_advance)),
            scan.partial,
        )?;
        Ok(report)
    }

    pub fn reconcile_compaction_for_chain(
        &self,
        chain: &ChainIdentity,
        config: MaintenanceCompactionConfig,
    ) -> Result<MaintenanceCompactionReconciliationReport, DatalensError> {
        let current_entries = self.manifest_for_chain(chain)?.entries;
        let raw_entries = self.raw_manifest_entries_for_chain(chain)?;
        let mut report =
            self.compaction_reconciliation_report(&current_entries, &raw_entries, false)?;
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

        for object_key in report.orphan_compacted_objects.clone() {
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
            for object_key in report.stale_source_objects.clone() {
                match self.object_store().delete(&object_key) {
                    Ok(()) => report.deleted_stale_source_objects += 1,
                    Err(error) => {
                        report.delete_failures += 1;
                        log::warn!(
                            "storage compaction reconciliation source delete failed chain_key={} object_key={} kind={:?} message={}",
                            chain.key_prefix(),
                            object_key,
                            error.kind,
                            error.message
                        );
                    }
                }
            }
        }
        for object_key in report.stale_cleanup_records.clone() {
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
        Ok(report)
    }

    fn compact_selected_manifest_entries(
        &self,
        entries: Vec<SelectedManifestEntry>,
        config: MaintenanceCompactionConfig,
        started: Instant,
        cursor: Option<(&ChainIdentity, Option<CompactionCursor>)>,
        scan_partial: bool,
    ) -> Result<MaintenanceCompactionReport, DatalensError> {
        if compaction_pressure_pause_reason(config).is_some() {
            return Ok(MaintenanceCompactionReport {
                read_only: false,
                candidate_count: 0,
                candidates: Vec::new(),
                processed_candidates: 0,
                duration_ms: duration_millis(started),
                tick_status: MaintenanceCompactionTickStatus::Paused,
                compacted_objects: 0,
                compacted_rows: 0,
                deleted_source_objects: 0,
                source_delete_failures: 0,
                get_operations: 0,
                put_operations: 0,
                delete_operations: 0,
            });
        }
        let build_started = Instant::now();
        let manifest_entries = entries
            .iter()
            .map(|entry| entry.entry.clone())
            .collect::<Vec<_>>();
        let candidates = compaction_candidates(&manifest_entries, config);
        log::info!(
            "storage compaction candidate build candidate_count={} entry_count={} duration_ms={}",
            candidates.len(),
            manifest_entries.len(),
            build_started.elapsed().as_millis()
        );
        let mut compacted_objects = 0usize;
        let mut compacted_rows = 0usize;
        let mut processed_candidates = 0usize;
        let mut deleted_source_objects = 0usize;
        let mut source_delete_failures = 0usize;
        let mut cursor_advance_key = None;
        let max_candidates = config
            .max_candidates_per_tick
            .max(1)
            .min(config.max_concurrent_candidates.max(1));
        let max_duration = Duration::from_millis(config.max_tick_duration_ms.max(1));
        let mut partial = scan_partial;
        let mut operation_budget = CompactionOperationBudget::new(config);

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
            if !operation_budget.can_process_candidate(candidate, selected_entries.len()) {
                partial = true;
                break;
            }
            let candidate_entries = selected_entries
                .iter()
                .map(|entry| entry.entry.clone())
                .collect::<Vec<_>>();
            let compacted = self.write_compacted_object(candidate, &candidate_entries)?;
            operation_budget.record_gets(candidate_entries.len());
            operation_budget.record_puts(1);
            let publish_started = Instant::now();
            if !self.try_write_compaction_manifest_entry(
                &candidate.chain,
                compacted.entry,
                &candidate_entries,
            )? {
                continue;
            }
            operation_budget.record_puts(1);
            compacted_rows += compacted.row_count;
            compacted_objects += 1;
            processed_candidates += 1;
            cursor_advance_key = selected_entries
                .iter()
                .filter_map(|entry| entry.segment_key.clone())
                .max()
                .or(cursor_advance_key);
            log::info!(
                "storage compaction manifest publish chain_key={} duration_ms={}",
                candidate.chain.key_prefix(),
                publish_started.elapsed().as_millis()
            );
            if config.delete_source_objects
                && operation_budget.can_delete_sources(candidate.object_keys.len())
            {
                let cleanup = self.delete_compacted_source_objects(candidate);
                operation_budget.record_deletes(candidate.object_keys.len());
                deleted_source_objects += cleanup.deleted_objects;
                source_delete_failures += cleanup.delete_failures;
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

        if let Some((chain, scan_cursor_advance_key)) = cursor {
            let next_key = if partial && processed_candidates < candidates.len() {
                cursor_advance_key
                    .map(segment_compaction_cursor)
                    .or(scan_cursor_advance_key)
            } else if partial {
                scan_cursor_advance_key
            } else {
                None
            };
            self.write_compaction_cursor(chain, next_key)?;
        }
        let tick_status = if partial {
            MaintenanceCompactionTickStatus::Partial
        } else {
            MaintenanceCompactionTickStatus::Completed
        };

        Ok(MaintenanceCompactionReport {
            read_only: false,
            candidate_count: candidates.len(),
            candidates,
            processed_candidates,
            duration_ms: duration_millis(started),
            tick_status,
            compacted_objects,
            compacted_rows,
            deleted_source_objects,
            source_delete_failures,
            get_operations: operation_budget.used_gets,
            put_operations: operation_budget.used_puts,
            delete_operations: operation_budget.used_deletes,
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
            if !object.key.ends_with(".json") {
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
        read_only: bool,
    ) -> Result<MaintenanceCompactionReconciliationReport, DatalensError> {
        let current_objects = current_object_keys(current_entries)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut orphan_compacted_objects = self
            .object_store()
            .list("chains")?
            .into_iter()
            .map(|object| object.key)
            .filter(|key| is_data_object(key))
            .filter(|key| key.contains("/compacted/"))
            .filter(|key| !current_objects.contains(key))
            .collect::<Vec<_>>();
        orphan_compacted_objects.sort();
        orphan_compacted_objects.dedup();

        let mut stale_source_objects = Vec::new();
        for entry in raw_entries {
            let Some(object_key) = entry.object_key.as_ref() else {
                continue;
            };
            if object_key.contains("/compacted/") || current_objects.contains(object_key) {
                continue;
            }
            if stale_source_object_is_safe(entry, current_entries, self.object_store())? {
                stale_source_objects.push(object_key.clone());
            }
        }
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

    fn delete_compacted_source_objects(
        &self,
        candidate: &CompactionCandidate,
    ) -> CompactionSourceCleanup {
        let mut deleted_objects = 0usize;
        let mut delete_failures = 0usize;
        for object_key in &candidate.object_keys {
            match self.object_store().delete(object_key) {
                Ok(()) => {
                    deleted_objects += 1;
                    log::info!(
                        "storage compaction source object deleted chain_key={} object_key={}",
                        candidate.chain.key_prefix(),
                        object_key
                    );
                }
                Err(error) => {
                    delete_failures += 1;
                    log::warn!(
                        "storage compaction source object delete failed chain_key={} object_key={} kind={:?} message={}",
                        candidate.chain.key_prefix(),
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
        self.object_store().put(&object_key, &bytes)?;
        log::info!(
            "storage compaction object write chain_key={} object_key={} object_bytes={} rows={} duration_ms={}",
            candidate.chain.key_prefix(),
            object_key,
            bytes.len(),
            rows.row_count(),
            write_started.elapsed().as_millis()
        );
        Ok(CompactedObject {
            row_count: rows.row_count(),
            entry: ManifestEntry {
                chain: candidate.chain.clone(),
                dataset_key: candidate.dataset_key.clone(),
                range: candidate.range.clone(),
                selector_fingerprint: candidate.selector_fingerprint.clone(),
                selector_canonical_key: candidate.selector_canonical_key.clone(),
                finality_level: candidate.finality_level,
                object_key: Some(data_object.object_key),
                object_encoding: Some(data_object.object_encoding),
                object_compression: data_object.object_compression,
                row_count: data_object.row_count,
                object_size_bytes: Some(data_object.object_size_bytes),
                checksum: Some(data_object.checksum),
                checksum_algorithm: Some(data_object.checksum_algorithm),
                written_at_unix_seconds: Some(data_object.written_at_unix_seconds),
            },
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
        let cursor = self.read_compaction_cursor(chain)?;
        let max_entries = config.max_manifest_entries_per_tick.max(1);
        if cursor.legacy_entry_offset.is_some() {
            return self.scan_legacy_compaction_manifest_entries(
                chain,
                &cursor,
                max_entries,
                load_started,
            );
        }
        let mut list_page = self.object_store().list_page(
            &prefix,
            cursor.next_segment_key.as_deref(),
            max_entries,
        )?;
        if list_page.objects.is_empty() && cursor.next_segment_key.is_some() && !list_page.has_more
        {
            list_page = self.object_store().list_page(&prefix, None, max_entries)?;
        }
        let mut segment_objects = list_page
            .objects
            .into_iter()
            .filter(|object| object.key.ends_with(".json"))
            .collect::<Vec<_>>();
        segment_objects.sort_by(|left, right| left.key.cmp(&right.key));
        let mut entries = Vec::new();
        let mut scanned_objects = 0usize;
        let mut scanned_entries = 0usize;
        let mut cursor_advance_key = None;
        let mut active_scope_prefix = None;

        if segment_objects.is_empty() {
            return self.scan_legacy_compaction_manifest_entries(
                chain,
                &cursor,
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
            let object_scope_prefix = manifest_segment_scope_prefix(&object.key);
            if active_scope_prefix.is_none() {
                active_scope_prefix = object_scope_prefix.clone();
            } else if active_scope_prefix != object_scope_prefix {
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
                    entry,
                });
                if entries.len() >= max_entries {
                    break;
                }
            }
        }
        let partial = list_page.has_more || scanned_objects < segment_objects.len();
        log::info!(
            "storage compaction manifest load chain_key={} source=manifest_segments listed_object_count={} scanned_object_count={} scanned_entry_count={} selected_entry_count={} scope_prefix={} partial={} duration_ms={}",
            chain.key_prefix(),
            segment_objects.len(),
            scanned_objects,
            scanned_entries,
            entries.len(),
            active_scope_prefix.as_deref().unwrap_or("none"),
            partial,
            load_started.elapsed().as_millis()
        );
        Ok(CompactionManifestScan {
            entries,
            partial,
            cursor_advance: cursor_advance_key.map(segment_compaction_cursor),
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
                partial: false,
                cursor_advance: None,
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
            partial,
            cursor_advance: partial.then_some(CompactionCursor {
                schema_version: 1,
                next_segment_key: None,
                legacy_entry_offset: Some(next_offset),
            }),
        })
    }

    fn read_compaction_cursor(
        &self,
        chain: &ChainIdentity,
    ) -> Result<CompactionCursor, DatalensError> {
        let key = compaction_cursor_key(chain);
        if !self.object_store().exists(&key)? {
            return Ok(CompactionCursor::default());
        }
        let bytes = self.object_store().get(&key)?;
        serde_json::from_slice(&bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode compaction cursor {key}: {error}"),
            )
        })
    }

    fn write_compaction_cursor(
        &self,
        chain: &ChainIdentity,
        cursor: Option<CompactionCursor>,
    ) -> Result<(), DatalensError> {
        let key = compaction_cursor_key(chain);
        let Some(cursor) = cursor else {
            self.object_store().delete(&key)?;
            return Ok(());
        };
        let bytes = serde_json::to_vec_pretty(&cursor).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode compaction cursor: {error}"),
            )
        })?;
        self.object_store().put(&key, &bytes)
    }
}

#[derive(Clone, Debug)]
struct SelectedManifestEntry {
    segment_key: Option<String>,
    entry: ManifestEntry,
}

#[derive(Clone, Debug)]
struct CompactionManifestScan {
    entries: Vec<SelectedManifestEntry>,
    partial: bool,
    cursor_advance: Option<CompactionCursor>,
}

#[derive(Clone, Copy, Debug)]
struct CompactionOperationBudget {
    max_gets: usize,
    max_puts: usize,
    max_deletes: usize,
    used_gets: usize,
    used_puts: usize,
    used_deletes: usize,
    delete_source_objects: bool,
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
            delete_source_objects: config.delete_source_objects,
        }
    }

    fn can_process_candidate(&self, candidate: &CompactionCandidate, source_gets: usize) -> bool {
        self.remaining_gets() >= source_gets
            && self.remaining_puts() >= 2
            && (!self.delete_source_objects
                || self.max_deletes == 0
                || self.remaining_deletes() >= candidate.object_keys.len())
    }

    fn can_delete_sources(&self, source_deletes: usize) -> bool {
        self.remaining_deletes() >= source_deletes
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

fn segment_compaction_cursor(next_segment_key: String) -> CompactionCursor {
    CompactionCursor {
        schema_version: 1,
        next_segment_key: Some(next_segment_key),
        legacy_entry_offset: None,
    }
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
        if object_key.is_empty() {
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
        for entry in entries {
            if run.last().is_none_or(|last: &&ManifestEntry| {
                entry.range.start() == last.range.end().saturating_add(1)
            }) && run.len() < config.max_merge_ranges.max(2)
            {
                run.push(entry);
            } else {
                push_candidate(&mut candidates, &key, &run);
                run.clear();
                run.push(entry);
            }
        }
        push_candidate(&mut candidates, &key, &run);
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

#[derive(Clone, Debug)]
struct CompactedObject {
    row_count: usize,
    entry: ManifestEntry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CompactionSourceCleanup {
    deleted_objects: usize,
    delete_failures: usize,
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
) {
    if entries.len() < 2 {
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
        object_keys: entries
            .iter()
            .filter_map(|entry| entry.object_key.clone())
            .collect(),
    });
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

fn stale_source_object_is_safe<S>(
    entry: &ManifestEntry,
    current_entries: &[ManifestEntry],
    object_store: &S,
) -> Result<bool, DatalensError>
where
    S: ObjectStore,
{
    let Some(source_object_key) = entry.object_key.as_deref() else {
        return Ok(false);
    };
    if !object_store.exists(source_object_key)? {
        return Ok(false);
    }
    for current_entry in current_entries {
        let Some(current_object_key) = current_entry.object_key.as_deref() else {
            continue;
        };
        if current_object_key == source_object_key {
            return Ok(false);
        }
        if current_entry.shadows_segment(entry) && object_store.exists(current_object_key)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn is_data_object(object_key: &str) -> bool {
    object_key != manifest_key_from_object_key(object_key)
        && !object_key.contains("/coverage-index/")
        && !object_key.contains("/manifest-segments/")
        && (object_key.ends_with(".json") || object_key.ends_with(".parquet"))
}

fn is_manifest_object(object_key: &str) -> bool {
    object_key.ends_with("/manifest.json")
        || (object_key.contains("/manifest-segments/") && object_key.ends_with(".json"))
}

fn is_manifest_segment_object(object_key: &str) -> bool {
    object_key.contains("/manifest-segments/") && object_key.ends_with(".json")
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
