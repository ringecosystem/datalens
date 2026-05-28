use std::collections::BTreeSet;

use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
    LedgerRangeKind,
};
use serde::{Deserialize, Serialize};

use crate::{
    DurableStorage, Manifest, ManifestEntry, ManifestFinalityLevel, ObjectEncoding, ObjectStore,
    StorageDataObject, checksum_hex, decode_object_rows, encode_object_rows, object_key,
    range_kind_key, unix_seconds_now, verify_manifest_object_metadata,
};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceReport {
    pub read_only: bool,
    pub mode: MaintenanceOperationMode,
    pub operations: Vec<MaintenanceOperation>,
    pub check: MaintenanceCheckReport,
    pub compaction: MaintenanceCompactionReport,
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
pub struct MaintenanceCompactionReport {
    pub read_only: bool,
    pub candidates: Vec<CompactionCandidate>,
    pub compacted_objects: usize,
    pub compacted_rows: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceCompactionConfig {
    pub min_object_bytes: u64,
    pub max_merge_ranges: usize,
}

impl Default for MaintenanceCompactionConfig {
    fn default() -> Self {
        Self {
            min_object_bytes: u64::MAX,
            max_merge_ranges: 32,
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
        let manifest_keys = manifest_objects
            .iter()
            .filter(|object| object.key.ends_with("/manifest.json"))
            .map(|object| object.key.clone())
            .collect::<Vec<_>>();

        let mut issues = Vec::new();
        let mut entries = Vec::new();
        for manifest_key in manifest_keys {
            let bytes = self.object_store().get(&manifest_key)?;
            match serde_json::from_slice::<Manifest>(&bytes) {
                Ok(mut manifest) => entries.append(&mut manifest.entries),
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

        issues.extend(self.check_entries(&entries)?);
        issues.extend(contradictory_coverage_issues(&entries));

        let candidates = compaction_candidates(&entries, MaintenanceCompactionConfig::default());
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
            compaction: MaintenanceCompactionReport {
                read_only: true,
                candidates,
                compacted_objects: 0,
                compacted_rows: 0,
            },
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
        let mut manifest = Manifest::default();
        for object in self.object_store().list("chains")? {
            if object.key.ends_with("/manifest.json") {
                let bytes = self.object_store().get(&object.key)?;
                let mut chain_manifest: Manifest =
                    serde_json::from_slice(&bytes).map_err(|error| {
                        DatalensError::new(
                            DatalensErrorKind::StorageReadFailure,
                            format!("decode manifest {}: {error}", object.key),
                        )
                    })?;
                manifest.entries.append(&mut chain_manifest.entries);
            }
        }

        let candidates = compaction_candidates(&manifest.entries, config);
        let mut compacted_objects = 0usize;
        let mut compacted_rows = 0usize;

        for candidate in &candidates {
            let mut chain_manifest = self.manifest_for_chain(&candidate.chain)?;
            let entries = candidate_entries(&chain_manifest.entries, candidate);
            if entries.len() != candidate.entry_count {
                continue;
            }
            let compacted = self.write_compacted_object(candidate, &entries)?;
            compacted_rows += compacted.row_count;
            compacted_objects += 1;
            replace_compacted_entries(&mut chain_manifest, &entries, compacted.entry);
            self.write_manifest(&candidate.chain, &chain_manifest)?;
        }

        Ok(MaintenanceCompactionReport {
            read_only: false,
            candidates,
            compacted_objects,
            compacted_rows,
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
                issues.push(entry_issue(
                    entry,
                    MaintenanceIssueKind::MissingObject,
                    format!("manifest entry object not found {object_key}"),
                ));
                continue;
            }
            let bytes = self.object_store().get(object_key)?;
            issues.extend(metadata_issues(entry, object_key, &bytes));
            let encoding = entry.object_encoding.unwrap_or_else(|| {
                ObjectEncoding::from_object_key(object_key).unwrap_or(ObjectEncoding::Json)
            });
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
            let bytes = self.object_store().get(object_key)?;
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
        let bytes = encode_object_rows(candidate.object_encoding, &rows)?;
        let object_key = object_key(
            &candidate.chain,
            &candidate.dataset_key,
            candidate.range.clone(),
            &candidate.selector_fingerprint,
            candidate.object_encoding,
        );
        let data_object = StorageDataObject {
            object_key: object_key.clone(),
            object_encoding: candidate.object_encoding,
            row_count: rows.row_count(),
            object_size_bytes: bytes.len() as u64,
            checksum: checksum_hex(&bytes),
            checksum_algorithm: "sha256".to_owned(),
            written_at_unix_seconds: unix_seconds_now()?,
        };
        self.object_store().put(&object_key, &bytes)?;
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
                row_count: data_object.row_count,
                object_size_bytes: Some(data_object.object_size_bytes),
                checksum: Some(data_object.checksum),
                checksum_algorithm: Some(data_object.checksum_algorithm),
                written_at_unix_seconds: Some(data_object.written_at_unix_seconds),
            },
        })
    }
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

#[derive(Clone, Debug)]
struct CompactedObject {
    row_count: usize,
    entry: ManifestEntry,
}

fn candidate_entries(
    entries: &[ManifestEntry],
    candidate: &CompactionCandidate,
) -> Vec<ManifestEntry> {
    let mut entries = entries
        .iter()
        .filter(|entry| {
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
        })
        .cloned()
        .collect::<Vec<_>>();
    entries.sort_by_key(|entry| (entry.range.start(), entry.range.end()));
    entries
}

fn replace_compacted_entries(
    manifest: &mut Manifest,
    compacted_entries: &[ManifestEntry],
    compacted_entry: ManifestEntry,
) {
    manifest.entries.retain(|entry| {
        !compacted_entries.iter().any(|compacted| {
            entry.chain == compacted.chain
                && entry.dataset_key == compacted.dataset_key
                && entry.selector_fingerprint == compacted.selector_fingerprint
                && entry.range == compacted.range
                && entry.finality_level == compacted.finality_level
                && entry.object_key == compacted.object_key
        })
    });
    manifest.upsert(compacted_entry);
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

fn is_data_object(object_key: &str) -> bool {
    object_key != manifest_key_from_object_key(object_key)
        && (object_key.ends_with(".json") || object_key.ends_with(".parquet"))
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
