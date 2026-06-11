use datalens_chain::DatasetSelector;
use datalens_core::{
    ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, DatasetRows, LedgerRange,
    LedgerRangeKind, QueryRows,
};
use sha2::{Digest, Sha256};

use crate::{ManifestEntry, ObjectEncoding, ParquetCompression, StorageDataObject, parquet_codec};

pub fn coverage_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range_kind: LedgerRangeKind,
    selector: &DatasetSelector,
) -> String {
    format!(
        "chains/{}/datasets/{}/{}/{}/{}",
        chain.key_prefix(),
        dataset_key.as_str(),
        object_encoding_for_dataset(dataset_key).as_str(),
        range_kind_key(range_kind),
        selector.fingerprint()
    )
}

pub(crate) fn object_key(
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: LedgerRange,
    selector_fingerprint: &str,
    encoding: ObjectEncoding,
) -> String {
    format!(
        "chains/{}/datasets/{}/{}/{}/{}",
        chain.key_prefix(),
        dataset_key.as_str(),
        encoding.as_str(),
        range_kind_key(range.kind()),
        selector_fingerprint,
    ) + &format!(
        "/{:#020}-{:#020}{}",
        range.start(),
        range.end(),
        encoding.extension()
    )
    .replace("0x", "")
}

pub(crate) fn manifest_key(chain: &ChainIdentity) -> String {
    format!("chains/{}/manifest.json", chain.key_prefix())
}

pub(crate) fn manifest_version_key(chain: &ChainIdentity) -> String {
    format!("chains/{}/manifest.version", chain.key_prefix())
}

pub(crate) fn manifest_segment_prefix(chain: &ChainIdentity) -> String {
    format!("chains/{}/manifest-segments", chain.key_prefix())
}

pub(crate) fn manifest_segment_key(chain: &ChainIdentity, entry: &ManifestEntry) -> String {
    format!(
        "{}/{}/{}/{}/{}/{:020}-{:020}.json",
        manifest_segment_prefix(chain),
        entry.dataset_key.as_str(),
        range_kind_key(entry.range.kind()),
        entry.selector_fingerprint,
        entry.finality_level.as_str(),
        entry.range.start(),
        entry.range.end(),
    )
}

pub(crate) fn checksum_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

pub(crate) fn unix_seconds_now() -> Result<u64, DatalensError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|error| {
            DatalensError::internal(format!("system clock before unix epoch: {error}"))
        })
}

pub(crate) fn validate_existing_data_object(
    existing: &ManifestEntry,
    data_object: &StorageDataObject,
) -> Result<(), DatalensError> {
    if existing.object_key.as_deref() != Some(data_object.object_key.as_str())
        || existing.object_encoding != Some(data_object.object_encoding)
        || existing.row_count != data_object.row_count
        || existing
            .object_size_bytes
            .is_some_and(|size| size != data_object.object_size_bytes)
        || existing
            .checksum
            .as_deref()
            .is_some_and(|checksum| checksum != data_object.checksum)
        || existing
            .checksum_algorithm
            .as_deref()
            .is_some_and(|algorithm| algorithm != data_object.checksum_algorithm)
        || existing
            .object_compression
            .is_some_and(|compression| Some(compression) != data_object.object_compression)
    {
        return Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            "existing manifest data object metadata differs for logical shard",
        ));
    }
    Ok(())
}

pub(crate) fn verify_manifest_object_metadata(
    entry: &ManifestEntry,
    object_key: &str,
    bytes: &[u8],
) -> Result<(), DatalensError> {
    if let Some(expected_size) = entry.object_size_bytes {
        let actual_size = bytes.len() as u64;
        if actual_size != expected_size {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!(
                    "cached object {object_key} size mismatch: expected {expected_size} bytes, got {actual_size} bytes"
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
                return Err(DatalensError::new(
                    DatalensErrorKind::StorageReadFailure,
                    format!("cached object {object_key} checksum mismatch for sha256"),
                ));
            }
        }
        (Some(_), Some(algorithm)) => {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("cached object {object_key} unknown checksum algorithm {algorithm}"),
            ));
        }
        (Some(_), None) | (None, Some(_)) => {
            log::debug!(
                "storage skipped incomplete cached object checksum metadata object_key={object_key}"
            );
        }
        (None, None) => {}
    }

    Ok(())
}

pub(crate) fn range_kind_key(kind: LedgerRangeKind) -> String {
    match kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(value) => value,
    }
}

pub(crate) fn merge_ranges(mut ranges: Vec<LedgerRange>) -> Vec<LedgerRange> {
    if ranges.is_empty() {
        return ranges;
    }
    ranges.sort_by_key(|range| range.start());
    let mut merged = vec![ranges[0].clone()];
    for range in ranges.into_iter().skip(1) {
        let last = merged.last_mut().expect("merged range");
        if range.kind() == last.kind() && range.start() <= last.end().saturating_add(1) {
            let end = last.end().max(range.end());
            *last = LedgerRange::try_new(last.kind(), last.start(), end).expect("valid range");
        } else {
            merged.push(range);
        }
    }
    merged
}

pub(crate) fn intersect(left: LedgerRange, right: LedgerRange) -> Option<LedgerRange> {
    left.intersection(&right)
}

pub(crate) fn object_encoding_for_dataset(dataset_key: &DatasetKey) -> ObjectEncoding {
    match dataset_key.evm_dataset() {
        Some(
            datalens_core::Dataset::Blocks
            | datalens_core::Dataset::BlockHeaders
            | datalens_core::Dataset::Transactions
            | datalens_core::Dataset::Receipts
            | datalens_core::Dataset::Logs,
        ) => ObjectEncoding::ParquetV1,
        None => ObjectEncoding::Json,
    }
}

pub(crate) fn encode_object_rows(
    encoding: ObjectEncoding,
    rows: &DatasetRows,
    parquet_compression: ParquetCompression,
) -> Result<Vec<u8>, DatalensError> {
    match encoding {
        ObjectEncoding::Json => serde_json::to_vec(rows).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!("encode cached rows: {error}"),
            )
        }),
        ObjectEncoding::ParquetV1 => parquet_codec::encode_rows(rows, parquet_compression),
    }
}

pub(crate) fn decode_object_rows(
    encoding: ObjectEncoding,
    dataset_key: DatasetKey,
    bytes: &[u8],
) -> Result<DatasetRows, DatalensError> {
    match encoding {
        ObjectEncoding::Json => serde_json::from_slice(bytes).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::StorageReadFailure,
                format!("decode json cached rows: {error}"),
            )
        }),
        ObjectEncoding::ParquetV1 => parquet_codec::decode_rows(dataset_key, bytes),
    }
}

pub(crate) fn empty_rows(dataset_key: DatasetKey) -> Result<DatasetRows, DatalensError> {
    let rows = match dataset_key.evm_dataset() {
        Some(datalens_core::Dataset::Blocks) => QueryRows::EvmBlocks(Vec::new()),
        Some(datalens_core::Dataset::BlockHeaders) => QueryRows::EvmBlockHeaders(Vec::new()),
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

pub(crate) fn filter_rows(rows: DatasetRows, range: LedgerRange) -> DatasetRows {
    let dataset_key = rows.dataset_key().clone();
    let Some(block_range) = range.block_range() else {
        return rows;
    };
    let rows = match rows.into_rows() {
        QueryRows::EvmBlocks(rows) => QueryRows::EvmBlocks(
            rows.into_iter()
                .filter(|row| block_range.contains(row.number))
                .collect(),
        ),
        QueryRows::EvmBlockHeaders(rows) => QueryRows::EvmBlockHeaders(
            rows.into_iter()
                .filter(|row| block_range.contains(row.block_number))
                .collect(),
        ),
        QueryRows::EvmTransactions(rows) => QueryRows::EvmTransactions(
            rows.into_iter()
                .filter(|row| block_range.contains(row.block_number))
                .collect(),
        ),
        QueryRows::EvmReceipts(rows) => QueryRows::EvmReceipts(
            rows.into_iter()
                .filter(|row| block_range.contains(row.block_number))
                .collect(),
        ),
        QueryRows::EvmLogs(rows) => QueryRows::EvmLogs(
            rows.into_iter()
                .filter(|row| block_range.contains(row.block_number))
                .collect(),
        ),
        QueryRows::AdapterJson { dataset_key, rows } => {
            QueryRows::AdapterJson { dataset_key, rows }
        }
    };
    DatasetRows::new(dataset_key, rows).expect("filtered rows keep dataset key")
}
