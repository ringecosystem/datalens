use datalens_chain::{AdapterCapabilities, DatasetSelector, HeightRangeKind, ReorgSignal};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
    LedgerRangeKind, QueryRows,
};
use serde::{Deserialize, Serialize};

use crate::{ObjectStore, filter_rows, merge_ranges};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HotCacheConfig {
    pub reorg_window: u64,
}

impl Default for HotCacheConfig {
    fn default() -> Self {
        Self { reorg_window: 64 }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HotWriteRequest<'a> {
    pub chain: &'a ChainIdentity,
    pub dataset_key: DatasetKey,
    pub selector: &'a DatasetSelector,
    pub range: LedgerRange,
    pub rows: &'a DatasetRows,
    pub reorg_signals: &'a [ReorgSignal],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HotWriteOutcome {
    pub range: LedgerRange,
    pub row_count: usize,
    pub reorg: Option<HotReorgOutcome>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HotReorgOutcome {
    pub reason: HotReorgReason,
    pub rollback_height: u64,
    pub stale_entries: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HotReorgReason {
    ParentMismatch,
    SameHeightDifferentHash,
    ProviderCanonicalHashChanged,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct HotManifest {
    #[serde(default)]
    pub entries: Vec<HotManifestEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HotManifestEntry {
    pub chain: ChainIdentity,
    pub dataset_key: DatasetKey,
    pub range: LedgerRange,
    pub selector_fingerprint: String,
    pub selector_canonical_key: String,
    pub status: HotEntryStatus,
    pub object_key: String,
    pub row_count: usize,
    pub blocks: Vec<HotBlockMetadata>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HotEntryStatus {
    Active,
    Stale,
    InactiveCandidate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HotBlockMetadata {
    pub range_kind: LedgerRangeKind,
    pub height: u64,
    pub hash: String,
    pub parent_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
}

impl From<&ReorgSignal> for HotBlockMetadata {
    fn from(signal: &ReorgSignal) -> Self {
        Self {
            range_kind: signal.range_kind.clone(),
            height: signal.height,
            hash: signal.hash.clone(),
            parent_hash: signal.parent_hash.clone(),
            timestamp: signal.timestamp,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HotCache<S> {
    object_store: S,
    config: HotCacheConfig,
}

impl<S> HotCache<S>
where
    S: ObjectStore,
{
    pub fn new(object_store: S, config: HotCacheConfig) -> Self {
        Self {
            object_store,
            config,
        }
    }

    pub fn validate_adapter_support(
        capabilities: &AdapterCapabilities,
        dataset_key: &DatasetKey,
    ) -> Result<(), DatalensError> {
        let Some(dataset) = capabilities.dataset(dataset_key) else {
            return Err(DatalensError::unsupported(
                "adapter does not support dataset for hot cache reorg detection",
            ));
        };
        if !dataset.supports_reorg_signals()
            || !dataset.supports_canonical_block_lookup()
            || !dataset.supports_latest_reorg_signal()
        {
            return Err(DatalensError::unsupported(
                "adapter does not expose hot cache reorg detection signals",
            ));
        }
        Ok(())
    }

    pub fn manifest(&self, chain: &ChainIdentity) -> Result<HotManifest, DatalensError> {
        let key = hot_manifest_key(chain);
        if !self.object_store.exists(&key)? {
            return Ok(HotManifest::default());
        }
        let bytes = self.object_store.get(&key)?;
        serde_json::from_slice(&bytes).map_err(|error| {
            DatalensError::storage_read(format!("decode hot manifest {key}: {error}"))
        })
    }

    pub fn covered_ranges(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        let selector_fingerprint = selector.fingerprint();
        let ranges = self
            .manifest(chain)?
            .entries
            .into_iter()
            .filter(|entry| {
                entry.status == HotEntryStatus::Active
                    && entry.chain == *chain
                    && entry.dataset_key == *dataset_key
                    && entry.selector_fingerprint == selector_fingerprint
            })
            .filter_map(|entry| entry.range.intersection(&range))
            .collect::<Vec<_>>();
        Ok(merge_ranges(ranges))
    }

    pub fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        let selector_fingerprint = selector.fingerprint();
        let mut rows = empty_rows(dataset_key.clone())?.into_rows();
        for entry in self.manifest(chain)?.entries {
            if entry.status != HotEntryStatus::Active
                || entry.chain != *chain
                || entry.dataset_key != *dataset_key
                || entry.selector_fingerprint != selector_fingerprint
                || entry.range.intersection(&range).is_none()
            {
                continue;
            }
            if entry.blocks.is_empty() {
                return Err(DatalensError::storage_read(
                    "active hot cache entry is missing reorg metadata",
                ));
            }
            let bytes = self.object_store.get(&entry.object_key)?;
            let cached = serde_json::from_slice::<DatasetRows>(&bytes).map_err(|error| {
                DatalensError::storage_read(format!(
                    "decode hot cache rows {}: {error}",
                    entry.object_key
                ))
            })?;
            let cached = filter_rows(cached, range.clone());
            rows.try_append(cached.into_rows())?;
        }
        rows.sort();
        DatasetRows::new(dataset_key.clone(), rows)
    }

    pub fn write_rows(
        &self,
        request: HotWriteRequest<'_>,
    ) -> Result<HotWriteOutcome, DatalensError> {
        if request.rows.dataset_key() != &request.dataset_key {
            return Err(DatalensError::internal(
                "dataset rows key does not match hot cache dataset key",
            ));
        }
        validate_hot_signals(&request.range, request.reorg_signals)?;

        let mut manifest = self.manifest(request.chain)?;
        let reorg = detect_reorg(
            &manifest,
            request.chain,
            request.range.kind(),
            request.reorg_signals,
        );
        let reorg = match reorg {
            Some((reason, rollback_height)) => {
                validate_reorg_window(
                    &manifest,
                    request.chain,
                    rollback_height,
                    self.config.reorg_window,
                )?;
                let stale_entries = mark_stale_from(
                    &mut manifest,
                    request.chain,
                    request.range.kind(),
                    rollback_height,
                );
                Some(HotReorgOutcome {
                    reason,
                    rollback_height,
                    stale_entries,
                })
            }
            None => None,
        };

        let object_key = hot_object_key(
            request.chain,
            &request.dataset_key,
            &request.range,
            &request.selector.fingerprint(),
            request.reorg_signals,
        );
        let bytes = serde_json::to_vec(request.rows)
            .map_err(|error| DatalensError::internal(format!("encode hot rows: {error}")))?;
        self.object_store.put(&object_key, &bytes)?;

        manifest.entries.push(HotManifestEntry {
            chain: request.chain.clone(),
            dataset_key: request.dataset_key.clone(),
            range: request.range.clone(),
            selector_fingerprint: request.selector.fingerprint(),
            selector_canonical_key: request.selector.canonical_key(),
            status: HotEntryStatus::Active,
            object_key,
            row_count: request.rows.row_count(),
            blocks: request
                .reorg_signals
                .iter()
                .map(HotBlockMetadata::from)
                .collect(),
        });
        self.write_manifest(request.chain, &manifest)?;

        Ok(HotWriteOutcome {
            range: request.range,
            row_count: request.rows.row_count(),
            reorg,
        })
    }

    fn write_manifest(
        &self,
        chain: &ChainIdentity,
        manifest: &HotManifest,
    ) -> Result<(), DatalensError> {
        let key = hot_manifest_key(chain);
        let bytes = serde_json::to_vec_pretty(manifest)
            .map_err(|error| DatalensError::internal(format!("encode hot manifest: {error}")))?;
        self.object_store.put(&key, &bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                format!("write hot manifest {key}: {}", error.message),
            )
        })
    }
}

fn validate_hot_signals(range: &LedgerRange, signals: &[ReorgSignal]) -> Result<(), DatalensError> {
    if signals.len() != range.len() as usize {
        return Err(DatalensError::invalid_input(
            "hot cache writes require one reorg signal per height",
        ));
    }
    for (offset, signal) in signals.iter().enumerate() {
        let expected = range.start() + offset as u64;
        if signal.range_kind != range.kind() || signal.height != expected {
            return Err(DatalensError::invalid_input(
                "hot cache reorg signals must cover the written range in order",
            ));
        }
    }
    Ok(())
}

fn detect_reorg(
    manifest: &HotManifest,
    chain: &ChainIdentity,
    range_kind: HeightRangeKind,
    signals: &[ReorgSignal],
) -> Option<(HotReorgReason, u64)> {
    for signal in signals {
        if let Some(existing) = active_block(manifest, chain, range_kind.clone(), signal.height)
            && existing.hash != signal.hash
        {
            return Some((HotReorgReason::SameHeightDifferentHash, signal.height));
        }
    }

    for pair in signals.windows(2) {
        if pair[1].parent_hash != pair[0].hash {
            return Some((HotReorgReason::ParentMismatch, pair[1].height));
        }
    }

    if let Some(first) = signals.first()
        && let Some(previous) = first
            .height
            .checked_sub(1)
            .and_then(|height| active_block(manifest, chain, range_kind, height))
        && first.parent_hash != previous.hash
    {
        return Some((HotReorgReason::ParentMismatch, first.height));
    }

    None
}

fn active_block<'a>(
    manifest: &'a HotManifest,
    chain: &ChainIdentity,
    range_kind: HeightRangeKind,
    height: u64,
) -> Option<&'a HotBlockMetadata> {
    manifest
        .entries
        .iter()
        .rev()
        .filter(|entry| entry.status == HotEntryStatus::Active && entry.chain == *chain)
        .flat_map(|entry| entry.blocks.iter())
        .find(|block| block.range_kind == range_kind && block.height == height)
}

fn validate_reorg_window(
    manifest: &HotManifest,
    chain: &ChainIdentity,
    rollback_height: u64,
    reorg_window: u64,
) -> Result<(), DatalensError> {
    let Some(active_tip) = manifest
        .entries
        .iter()
        .filter(|entry| entry.status == HotEntryStatus::Active && entry.chain == *chain)
        .flat_map(|entry| entry.blocks.iter().map(|block| block.height))
        .max()
    else {
        return Ok(());
    };
    if rollback_height <= active_tip {
        let depth = active_tip - rollback_height + 1;
        if depth > reorg_window {
            return Err(DatalensError::provider_limit(format!(
                "hot reorg window exceeded: rollback depth {depth}, configured window {reorg_window}"
            )));
        }
    }
    Ok(())
}

fn mark_stale_from(
    manifest: &mut HotManifest,
    chain: &ChainIdentity,
    range_kind: HeightRangeKind,
    rollback_height: u64,
) -> usize {
    let mut stale_entries = 0usize;
    for entry in &mut manifest.entries {
        if entry.status != HotEntryStatus::Active || entry.chain != *chain {
            continue;
        }
        let affected = entry
            .blocks
            .iter()
            .any(|block| block.range_kind == range_kind && block.height >= rollback_height);
        if affected {
            entry.status = HotEntryStatus::Stale;
            stale_entries += 1;
        }
    }
    stale_entries
}

fn hot_manifest_key(chain: &ChainIdentity) -> String {
    format!("hot/chains/{}/manifest.json", chain.key_prefix())
}

fn hot_object_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: &LedgerRange,
    selector_fingerprint: &str,
    signals: &[ReorgSignal],
) -> String {
    let hash = signals
        .last()
        .map(|signal| signal.hash.as_str())
        .unwrap_or("empty");
    format!(
        "hot/chains/{}/datasets/{}/{}/{}/{}-{}-{}.json",
        chain.key_prefix(),
        dataset_key.as_str(),
        range_kind_key(range.kind()),
        selector_fingerprint,
        range.start(),
        range.end(),
        storage_key_segment(hash),
    )
}

fn range_kind_key(kind: LedgerRangeKind) -> String {
    match kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(value) => value,
    }
}

fn storage_key_segment(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

fn empty_rows(dataset_key: DatasetKey) -> Result<DatasetRows, DatalensError> {
    let rows = match dataset_key.legacy_dataset() {
        Some(datalens_core::Dataset::Blocks) => QueryRows::EvmBlocks(Vec::new()),
        Some(datalens_core::Dataset::Logs) => QueryRows::EvmLogs(Vec::new()),
        None => QueryRows::AdapterJson {
            dataset_key: dataset_key.clone(),
            rows: Vec::new(),
        },
    };
    DatasetRows::new(dataset_key, rows)
}
