use datalens_chain::{DatasetSelector, HeightRangeKind, ReorgSignal};
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
    QueryRows,
};

use crate::{ObjectEncoding, range_kind_key, validate_object_key, verify_manifest_object_metadata};

use super::{
    HOT_CACHE_PREFIX, HOT_CACHE_SCHEMA_VERSION, HotBlockMetadata, HotCacheEntryMetadata,
    HotEntryStatus, HotManifest, HotReorgReason,
};

pub(super) fn validate_hot_signals(
    range: &LedgerRange,
    signals: &[ReorgSignal],
) -> Result<(), DatalensError> {
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

pub(super) fn detect_reorg(
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

pub(super) fn validate_reorg_window(
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

pub(super) fn mark_stale_from(
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

pub(super) fn hot_manifest_key(chain: &ChainIdentity) -> String {
    format!("hot/chains/{}/manifest.json", chain.key_prefix())
}

pub(super) fn hot_object_key(
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

pub(super) fn hot_storage_object_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: &LedgerRange,
    selector: &DatasetSelector,
    height: u64,
    block_hash: &str,
    encoding: ObjectEncoding,
) -> Result<String, DatalensError> {
    let hash = hot_hash_key(block_hash)?;
    let key = format!(
        "{}/{}/{}.rows{}",
        hot_logical_prefix(chain, dataset_key, range, selector, height)?,
        hash,
        hash,
        encoding.extension()
    );
    validate_object_key(&key)?;
    Ok(key)
}

pub(super) fn hot_metadata_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: &LedgerRange,
    selector: &DatasetSelector,
    height: u64,
    block_hash: &str,
) -> Result<String, DatalensError> {
    let hash = hot_hash_key(block_hash)?;
    let key = format!(
        "{}/{}/{}.metadata.json",
        hot_logical_prefix(chain, dataset_key, range, selector, height)?,
        hash,
        hash
    );
    validate_object_key(&key)?;
    Ok(key)
}

pub(super) fn hot_logical_prefix(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: &LedgerRange,
    selector: &DatasetSelector,
    height: u64,
) -> Result<String, DatalensError> {
    let prefix = format!(
        "{HOT_CACHE_PREFIX}/chains/{}/ranges/{}/{:#020}-{:#020}/datasets/{}/{}/{}/height-{:#020}",
        chain.key_prefix(),
        range_kind_key(range.kind()),
        range.start(),
        range.end(),
        dataset_key.as_str(),
        HOT_CACHE_SCHEMA_VERSION,
        selector.fingerprint(),
        height
    )
    .replace("0x", "");
    validate_object_key(&prefix)?;
    Ok(prefix)
}

pub(super) fn hot_hash_key(block_hash: &str) -> Result<String, DatalensError> {
    let hash = block_hash
        .trim()
        .strip_prefix("0x")
        .unwrap_or(block_hash.trim());
    if hash.is_empty()
        || !hash.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '-' || character == '_'
        })
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "hot cache block hash must be a non-empty storage-safe value",
        ));
    }
    Ok(hash.to_owned())
}

pub(super) fn verify_hot_object_metadata(
    metadata: &HotCacheEntryMetadata,
    bytes: &[u8],
) -> Result<(), DatalensError> {
    let entry = crate::ManifestEntry {
        chain: metadata.chain.clone().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "hot cache metadata missing chain identity",
            )
        })?,
        dataset_key: metadata.dataset_key.clone().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "hot cache metadata missing dataset key",
            )
        })?,
        range: metadata.range.clone().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                "hot cache metadata missing range",
            )
        })?,
        selector_fingerprint: metadata.selector_fingerprint.clone(),
        selector_canonical_key: metadata.selector_canonical_key.clone(),
        finality_level: crate::ManifestFinalityLevel::Safe,
        object_key: Some(metadata.object_key.clone()),
        object_encoding: metadata.object_encoding,
        row_count: metadata.row_count,
        object_size_bytes: Some(metadata.object_size_bytes),
        checksum: Some(metadata.checksum.clone()),
        checksum_algorithm: Some(metadata.checksum_algorithm.clone()),
        written_at_unix_seconds: Some(metadata.observed_at_unix_seconds),
    };
    verify_manifest_object_metadata(&entry, &metadata.object_key, bytes)
}

pub(super) fn storage_key_segment(value: &str) -> String {
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

pub(super) fn empty_rows(dataset_key: DatasetKey) -> Result<DatasetRows, DatalensError> {
    let rows = match dataset_key.evm_dataset() {
        Some(datalens_core::Dataset::Blocks) => QueryRows::EvmBlocks(Vec::new()),
        Some(datalens_core::Dataset::Transactions) => QueryRows::EvmTransactions(Vec::new()),
        Some(datalens_core::Dataset::Receipts) => QueryRows::EvmReceipts(Vec::new()),
        Some(datalens_core::Dataset::Logs) => QueryRows::EvmLogs(Vec::new()),
        None => QueryRows::AdapterJson {
            dataset_key: dataset_key.clone(),
            rows: Vec::new(),
        },
    };
    DatasetRows::new(dataset_key, rows)
}
