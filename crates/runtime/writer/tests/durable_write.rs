use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey,
    DatasetRows, LedgerRange, LogRecord, NetworkId, QueryRows,
};
use datalens_storage::{
    LocalStorage, Manifest, ManifestFinalityLevel, ObjectEncoding, StorageRepository,
    StorageWriteOutcome, StorageWriteRequest,
};
use datalens_writer::{
    DurableWriteRequest, DurableWriteSegment, DurableWriter, DurableWriterConfig, WriteFlushReason,
    WriteStagingConfig,
};

#[test]
fn test_writer_persists_single_non_empty_segment() {
    let storage = LocalStorage::new(temp_storage_root("single-non-empty"));
    let writer = DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    );
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let range = LedgerRange::blocks(10, 10).expect("valid range");
    let rows = block_rows(vec![block(10)]);

    let result = writer
        .write(DurableWriteRequest {
            chain: chain.clone(),
            dataset_key: DatasetKey::evm_blocks(),
            selector,
            finality_level: FinalityLevel::Safe,
            segments: vec![DurableWriteSegment {
                range: range.clone(),
                rows,
            }],
        })
        .expect("durable write");

    assert_eq!(result.data_objects.len(), 1);
    assert_eq!(result.empty_coverages, Vec::<LedgerRange>::new());
    assert_eq!(result.data_objects[0].range, range);
    assert_eq!(result.data_objects[0].row_count, 1);
    assert!(result.data_objects[0].object_size_bytes > 0);
    assert_eq!(result.data_objects[0].checksum_algorithm, "sha256");
    assert_eq!(result.data_objects[0].checksum.len(), 64);

    let manifest = storage.manifest().expect("manifest");
    let entry = manifest.entries.first().expect("manifest entry");
    assert_eq!(entry.object_encoding, Some(ObjectEncoding::ParquetV1));
    assert_eq!(
        entry.object_size_bytes,
        Some(result.data_objects[0].object_size_bytes)
    );
    assert_eq!(
        entry.checksum.as_deref(),
        Some(result.data_objects[0].checksum.as_str())
    );
    assert_eq!(entry.checksum_algorithm.as_deref(), Some("sha256"));
    assert!(entry.written_at_unix_seconds.is_some());
}

#[test]
fn test_writer_records_empty_coverage_without_data_object() {
    let storage = LocalStorage::new(temp_storage_root("empty-coverage"));
    let writer = DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    );
    let chain = test_chain();

    let result = writer
        .write(DurableWriteRequest {
            chain: chain.clone(),
            dataset_key: DatasetKey::evm_logs(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Finalized,
            segments: vec![DurableWriteSegment {
                range: LedgerRange::blocks(20, 21).expect("valid range"),
                rows: log_rows(Vec::new()),
            }],
        })
        .expect("durable write");

    assert_eq!(
        result.empty_coverages,
        vec![LedgerRange::blocks(20, 21).expect("valid range")]
    );
    assert_eq!(result.data_objects.len(), 0);

    let manifest = storage.manifest().expect("manifest");
    let entry = manifest.entries.first().expect("manifest entry");
    assert_eq!(entry.object_key, None);
    assert_eq!(entry.object_encoding, None);
    assert_eq!(entry.object_size_bytes, None);
    assert_eq!(entry.checksum, None);
    assert_eq!(entry.checksum_algorithm, None);
    assert_eq!(entry.written_at_unix_seconds, None);
    assert_eq!(entry.finality_level, ManifestFinalityLevel::Finalized);
}

#[test]
fn test_writer_merges_adjacent_sparse_segments_by_min_rows() {
    let storage = LocalStorage::new(temp_storage_root("merge-sparse"));
    let writer = DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024 * 1024,
            min_object_rows: 3,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    );

    let result = writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![
                DurableWriteSegment {
                    range: LedgerRange::blocks(1, 1).expect("valid range"),
                    rows: block_rows(vec![block(1)]),
                },
                DurableWriteSegment {
                    range: LedgerRange::blocks(2, 2).expect("valid range"),
                    rows: block_rows(vec![block(2)]),
                },
                DurableWriteSegment {
                    range: LedgerRange::blocks(3, 3).expect("valid range"),
                    rows: block_rows(vec![block(3)]),
                },
            ],
        })
        .expect("durable write");

    assert_eq!(result.data_objects.len(), 1);
    assert_eq!(
        result.data_objects[0].range,
        LedgerRange::blocks(1, 3).expect("valid range")
    );
    assert_eq!(result.data_objects[0].row_count, 3);

    let manifest = storage.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(
        manifest.entries[0].range,
        LedgerRange::blocks(1, 3).expect("valid range")
    );
}

#[test]
fn test_writer_continues_merging_after_min_rows_until_target_bytes() {
    let storage = LocalStorage::new(temp_storage_root("merge-past-min-rows"));
    let writer = DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024 * 1024,
            min_object_rows: 2,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    );

    let result = writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![
                DurableWriteSegment {
                    range: LedgerRange::blocks(1, 1).expect("valid range"),
                    rows: block_rows(vec![block(1)]),
                },
                DurableWriteSegment {
                    range: LedgerRange::blocks(2, 2).expect("valid range"),
                    rows: block_rows(vec![block(2)]),
                },
                DurableWriteSegment {
                    range: LedgerRange::blocks(3, 3).expect("valid range"),
                    rows: block_rows(vec![block(3)]),
                },
            ],
        })
        .expect("durable write");

    assert_eq!(result.data_objects.len(), 1);
    assert_eq!(
        result.data_objects[0].range,
        LedgerRange::blocks(1, 3).expect("valid range")
    );
    assert_eq!(result.data_objects[0].row_count, 3);
}

#[test]
fn test_writer_flushes_before_merge_when_target_bytes_reached() {
    let storage = LocalStorage::new(temp_storage_root("target-bytes"));
    let writer = DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1,
            min_object_rows: 3,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    );

    let result = writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![
                DurableWriteSegment {
                    range: LedgerRange::blocks(1, 1).expect("valid range"),
                    rows: block_rows(vec![block(1)]),
                },
                DurableWriteSegment {
                    range: LedgerRange::blocks(2, 2).expect("valid range"),
                    rows: block_rows(vec![block(2)]),
                },
            ],
        })
        .expect("durable write");

    assert_eq!(result.data_objects.len(), 2);
    assert_eq!(storage.manifest().expect("manifest").entries.len(), 2);
}

#[test]
fn test_writer_does_not_merge_non_adjacent_or_empty_segments_into_data_object() {
    let storage = LocalStorage::new(temp_storage_root("no-merge-incompatible"));
    let writer = DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024 * 1024,
            min_object_rows: 3,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    );

    let result = writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![
                DurableWriteSegment {
                    range: LedgerRange::blocks(1, 1).expect("valid range"),
                    rows: block_rows(vec![block(1)]),
                },
                DurableWriteSegment {
                    range: LedgerRange::blocks(3, 3).expect("valid range"),
                    rows: block_rows(vec![block(3)]),
                },
                DurableWriteSegment {
                    range: LedgerRange::blocks(4, 4).expect("valid range"),
                    rows: block_rows(Vec::new()),
                },
            ],
        })
        .expect("durable write");

    assert_eq!(result.data_objects.len(), 2);
    assert_eq!(
        result.empty_coverages,
        vec![LedgerRange::blocks(4, 4).expect("valid range")]
    );
}

#[test]
fn test_writer_does_not_merge_different_range_kinds() {
    let storage = LocalStorage::new(temp_storage_root("range-kind"));
    let writer = DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024 * 1024,
            min_object_rows: 3,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    );

    let result = writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![
                DurableWriteSegment {
                    range: LedgerRange::blocks(1, 1).expect("valid range"),
                    rows: block_rows(vec![block(1)]),
                },
                DurableWriteSegment {
                    range: LedgerRange::slots(2, 2).expect("valid range"),
                    rows: block_rows(vec![block(2)]),
                },
            ],
        })
        .expect("durable write");

    assert_eq!(result.data_objects.len(), 2);
    assert_eq!(storage.manifest().expect("manifest").entries.len(), 2);
}

#[test]
fn test_writer_repeated_logical_shard_is_idempotent() {
    let storage = LocalStorage::new(temp_storage_root("idempotent"));
    let writer = DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    );
    let request = DurableWriteRequest {
        chain: test_chain(),
        dataset_key: DatasetKey::evm_blocks(),
        selector: DatasetSelector::all(),
        finality_level: FinalityLevel::Safe,
        segments: vec![DurableWriteSegment {
            range: LedgerRange::blocks(1, 1).expect("valid range"),
            rows: block_rows(vec![block(1)]),
        }],
    };

    let first = writer.write(request.clone()).expect("first write");
    let second = writer.write(request).expect("second write");

    assert_eq!(first.data_objects, second.data_objects);
    assert_eq!(storage.manifest().expect("manifest").entries.len(), 1);
}

#[test]
fn test_writer_stages_non_empty_segment_below_min_rows_without_manifest_coverage() {
    let storage = LocalStorage::new(temp_storage_root("stages-below-min"));
    let writer = DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024 * 1024,
            min_object_rows: 3,
            record_empty_coverage: true,
            staging: WriteStagingConfig {
                enabled: true,
                ..Default::default()
            },
        },
    );
    let range = LedgerRange::blocks(1, 1).expect("valid range");

    let result = writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![DurableWriteSegment {
                range: range.clone(),
                rows: block_rows(vec![block(1)]),
            }],
        })
        .expect("stage write");

    assert_eq!(result.staged_ranges, vec![range]);
    assert!(result.data_objects.is_empty());
    assert!(storage.manifest().expect("manifest").entries.is_empty());
}

#[test]
fn test_writer_flush_persists_staged_segments_as_durable_coverage() {
    let storage = LocalStorage::new(temp_storage_root("flush-staged"));
    let writer = DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024 * 1024,
            min_object_rows: 3,
            record_empty_coverage: true,
            staging: WriteStagingConfig {
                enabled: true,
                ..Default::default()
            },
        },
    );

    writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![DurableWriteSegment {
                range: LedgerRange::blocks(1, 1).expect("valid range"),
                rows: block_rows(vec![block(1)]),
            }],
        })
        .expect("stage write");
    let result = writer.flush().expect("flush staged writes");

    assert_eq!(result.data_objects.len(), 1);
    assert_eq!(
        storage.manifest().expect("manifest").entries[0].range,
        LedgerRange::blocks(1, 1).expect("valid range")
    );
}

#[test]
fn test_writer_flushes_staged_segments_for_configured_rows_threshold() {
    let storage = LocalStorage::new(temp_storage_root("staging-rows-threshold"));
    let writer = DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024 * 1024,
            min_object_rows: 100,
            record_empty_coverage: true,
            staging: WriteStagingConfig {
                enabled: true,
                min_rows: Some(2),
                ..Default::default()
            },
        },
    );

    let result = writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![
                DurableWriteSegment {
                    range: LedgerRange::blocks(1, 1).expect("valid range"),
                    rows: block_rows(vec![block(1)]),
                },
                DurableWriteSegment {
                    range: LedgerRange::blocks(2, 2).expect("valid range"),
                    rows: block_rows(vec![block(2)]),
                },
            ],
        })
        .expect("flush by rows threshold");

    assert_eq!(result.flush_reason, Some(WriteFlushReason::RowsThreshold));
    assert_eq!(result.data_objects.len(), 1);
    assert_eq!(storage.manifest().expect("manifest").entries.len(), 1);
}

#[test]
fn test_writer_flushes_staged_segments_for_range_count_threshold() {
    let storage = LocalStorage::new(temp_storage_root("staging-range-threshold"));
    let writer = DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024 * 1024,
            min_object_rows: 100,
            record_empty_coverage: true,
            staging: WriteStagingConfig {
                enabled: true,
                max_staged_ranges: Some(2),
                ..Default::default()
            },
        },
    );

    writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![DurableWriteSegment {
                range: LedgerRange::blocks(1, 1).expect("valid range"),
                rows: block_rows(vec![block(1)]),
            }],
        })
        .expect("stage first range");
    let result = writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![DurableWriteSegment {
                range: LedgerRange::blocks(2, 2).expect("valid range"),
                rows: block_rows(vec![block(2)]),
            }],
        })
        .expect("flush by range count");

    assert_eq!(result.flush_reason, Some(WriteFlushReason::RangeThreshold));
    assert_eq!(result.data_objects.len(), 1);
}

#[test]
fn test_writer_flushes_staged_segments_for_bytes_threshold() {
    let storage = LocalStorage::new(temp_storage_root("staging-bytes-threshold"));
    let writer = DurableWriter::new(
        storage,
        DurableWriterConfig {
            target_object_bytes: 1024 * 1024,
            min_object_rows: 100,
            record_empty_coverage: true,
            staging: WriteStagingConfig {
                enabled: true,
                target_object_bytes: Some(1),
                ..Default::default()
            },
        },
    );

    let result = writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![DurableWriteSegment {
                range: LedgerRange::blocks(1, 1).expect("valid range"),
                rows: block_rows(vec![block(1)]),
            }],
        })
        .expect("flush by bytes");

    assert_eq!(result.flush_reason, Some(WriteFlushReason::BytesThreshold));
    assert_eq!(result.data_objects.len(), 1);
}

#[test]
fn test_writer_flushes_staged_segments_for_age_threshold() {
    let storage = LocalStorage::new(temp_storage_root("staging-age-threshold"));
    let writer = DurableWriter::new(
        storage,
        DurableWriterConfig {
            target_object_bytes: 1024 * 1024,
            min_object_rows: 100,
            record_empty_coverage: true,
            staging: WriteStagingConfig {
                enabled: true,
                max_staged_age_ms: Some(0),
                ..Default::default()
            },
        },
    );

    let result = writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![DurableWriteSegment {
                range: LedgerRange::blocks(1, 1).expect("valid range"),
                rows: block_rows(vec![block(1)]),
            }],
        })
        .expect("flush by age");

    assert_eq!(result.flush_reason, Some(WriteFlushReason::AgeThreshold));
    assert_eq!(result.data_objects.len(), 1);
}

#[test]
fn test_writer_flushes_staged_segments_for_shutdown_when_enabled() {
    let storage = LocalStorage::new(temp_storage_root("staging-shutdown"));
    let writer = DurableWriter::new(
        storage,
        DurableWriterConfig {
            target_object_bytes: 1024 * 1024,
            min_object_rows: 100,
            record_empty_coverage: true,
            staging: WriteStagingConfig {
                enabled: true,
                flush_on_shutdown: true,
                ..Default::default()
            },
        },
    );

    writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![DurableWriteSegment {
                range: LedgerRange::blocks(1, 1).expect("valid range"),
                rows: block_rows(vec![block(1)]),
            }],
        })
        .expect("stage write");
    let result = writer.flush_for_shutdown().expect("shutdown flush");

    assert_eq!(result.flush_reason, Some(WriteFlushReason::Shutdown));
    assert_eq!(result.data_objects.len(), 1);
}

#[test]
fn test_writer_retains_staged_segments_when_manual_flush_fails() {
    let storage = ToggleFailStorage::new(LocalStorage::new(temp_storage_root("staging-retry")));
    let writer = DurableWriter::new(
        storage.clone(),
        DurableWriterConfig {
            target_object_bytes: 1024 * 1024,
            min_object_rows: 100,
            record_empty_coverage: true,
            staging: WriteStagingConfig {
                enabled: true,
                ..Default::default()
            },
        },
    );

    writer
        .write(DurableWriteRequest {
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            finality_level: FinalityLevel::Safe,
            segments: vec![DurableWriteSegment {
                range: LedgerRange::blocks(1, 1).expect("valid range"),
                rows: block_rows(vec![block(1)]),
            }],
        })
        .expect("stage write");

    storage.set_fail_writes(true);
    let error = writer.flush().expect_err("flush fails");
    assert_eq!(error.kind, DatalensErrorKind::StorageWriteFailure);
    assert!(storage.manifest().expect("manifest").entries.is_empty());

    storage.set_fail_writes(false);
    let retry = writer.flush().expect("retry retained staged write");
    assert_eq!(retry.flush_reason, Some(WriteFlushReason::Manual));
    assert_eq!(retry.data_objects.len(), 1);
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-writer-{name}-{}",
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

fn block(number: u64) -> BlockHeader {
    BlockHeader {
        number,
        hash: format!("0xblock{number}"),
        parent_hash: format!("0xparent{number}"),
        timestamp: number,
    }
}

fn block_rows(rows: Vec<BlockHeader>) -> DatasetRows {
    DatasetRows::new(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(rows)).expect("dataset rows")
}

fn log_rows(rows: Vec<LogRecord>) -> DatasetRows {
    DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(rows)).expect("dataset rows")
}

#[derive(Clone)]
struct ToggleFailStorage {
    inner: LocalStorage,
    fail_writes: Arc<AtomicBool>,
}

impl ToggleFailStorage {
    fn new(inner: LocalStorage) -> Self {
        Self {
            inner,
            fail_writes: Arc::new(AtomicBool::new(false)),
        }
    }

    fn set_fail_writes(&self, fail: bool) {
        self.fail_writes.store(fail, Ordering::SeqCst);
    }
}

impl StorageRepository for ToggleFailStorage {
    fn manifest(&self) -> Result<Manifest, DatalensError> {
        self.inner.manifest()
    }

    fn covered_ranges(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        self.inner
            .covered_ranges(chain, dataset_key, selector, range)
    }

    fn read_rows(
        &self,
        chain: &ChainIdentity,
        dataset_key: &DatasetKey,
        selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        self.inner.read_rows(chain, dataset_key, selector, range)
    }

    fn write_rows(
        &self,
        request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        if self.fail_writes.load(Ordering::SeqCst) {
            return Err(DatalensError::new(
                DatalensErrorKind::StorageWriteFailure,
                "injected durable write failure",
            ));
        }
        self.inner.write_rows(request)
    }
}
