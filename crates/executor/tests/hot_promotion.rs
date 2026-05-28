use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use datalens_chain::{
    AdapterCapabilities, CanonicalBlock, CanonicalBlockRequest, ChainAdapter, ChainFetchRequest,
    ChainFetchResponse, ChainHeight, DatasetCapability, DatasetSelector, FinalityLevel,
    HeightRangeKind, SelectorKind,
};
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset, DatasetKey,
    DatasetRows, LedgerRange, NetworkId, QueryDataFinality, QueryRows,
};
use datalens_executor::{HotCachePromoter, HotPromotionRequest};
use datalens_metrics::{ApplicationIdentity, MetricsRecorder};
use datalens_storage::{
    FillOutcome, HotCacheCandidateStatus, HotCacheEntryMetadata, HotCacheFinalityStatus,
    LocalHotCacheStorage, LocalObjectStore, LocalStorage, ObjectStore, QueryOutcome,
    StorageWriteOutcome, UsageLedgerRepository, UsageLedgerStore,
};
use datalens_writer::DurableWriterConfig;

#[test]
fn test_unsafe_hot_data_is_skipped_and_not_written_to_durable_cache() {
    let root = temp_storage_root("unsafe-skip");
    let hot = LocalHotCacheStorage::new(root.join("hot"));
    let durable = LocalStorage::new(root.join("durable"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    write_hot_block(&hot, &chain, &selector, 10, "0x10", "0x09", |metadata| {
        metadata.finality_status = HotCacheFinalityStatus::Unsafe;
        metadata.eligible_for_promotion = false;
    });
    let source = PromotionSource::new(ChainHeight::block(9).with_finality(FinalityLevel::Safe));
    let promoter = promoter(hot, durable.clone(), source);

    let result = promoter
        .promote(HotPromotionRequest {
            chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector,
            range: blocks(10, 10),
            application: Some(ApplicationIdentity::named("system")),
        })
        .expect("promotion skips unsafe hot data");

    assert_eq!(result.attempted, 1);
    assert_eq!(result.promoted, 0);
    assert_eq!(result.skipped, 1);
    assert!(
        durable
            .manifest()
            .expect("durable manifest")
            .entries
            .is_empty()
    );
}

#[test]
fn test_safe_canonical_hot_data_promotes_through_durable_writer_and_records_outcomes() {
    let root = temp_storage_root("safe-promote");
    let hot = LocalHotCacheStorage::new(root.join("hot"));
    let durable = LocalStorage::new(root.join("durable"));
    let ledger = UsageLedgerStore::new(LocalObjectStore::new(root.join("ledger")));
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let chain = test_chain();
    let selector = DatasetSelector::all();
    write_hot_block(&hot, &chain, &selector, 10, "0x10", "0x09", |metadata| {
        metadata.finality_status = HotCacheFinalityStatus::Safe;
        metadata.eligible_for_promotion = true;
    });
    let source = PromotionSource::new(ChainHeight::block(10).with_finality(FinalityLevel::Safe))
        .with_canonical_block(10, "0x10", "0x09", FinalityLevel::Safe);
    let promoter = promoter(hot.clone(), durable.clone(), source)
        .with_metrics(recorder.clone(), ApplicationIdentity::named("system"))
        .with_usage_ledger(ledger.clone(), ApplicationIdentity::named("system"));

    let result = promoter
        .promote(HotPromotionRequest {
            chain: chain.clone(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: selector.clone(),
            range: blocks(10, 10),
            application: Some(ApplicationIdentity::named("system")),
        })
        .expect("promotion succeeds");

    assert_eq!(result.attempted, 1);
    assert_eq!(result.promoted, 1);
    assert_eq!(result.skipped, 0);
    assert_eq!(
        durable
            .covered_ranges(&chain, &DatasetKey::evm_blocks(), &selector, blocks(10, 10))
            .expect("durable coverage"),
        vec![blocks(10, 10)]
    );
    assert_eq!(
        block_numbers(
            &durable
                .read_rows(&chain, &DatasetKey::evm_blocks(), &selector, blocks(10, 10))
                .expect("read durable rows")
        ),
        vec![10]
    );
    let hot_entry = hot
        .list_entries(&chain, HeightRangeKind::Block, 10, 10)
        .expect("list hot entries")
        .pop()
        .expect("hot entry retained");
    assert!(hot_entry.promoted_at_unix_seconds.is_some());

    let events = ledger.read_application("system").expect("ledger events");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].range, blocks(10, 10));
    assert_eq!(events[0].query_outcome, QueryOutcome::PromotionCompleted);
    assert_eq!(events[0].fill_outcome, FillOutcome::PromotionWritten);

    let metrics = recorder.encode().expect("metrics output");
    assert!(metrics.contains(r#"datalens_hot_promotion_total{application="system",chain="ethereum",chain_kind="evm",dataset="blocks",outcome="attempted"} 1"#));
    assert!(metrics.contains(r#"datalens_hot_promotion_total{application="system",chain="ethereum",chain_kind="evm",dataset="blocks",outcome="promoted"} 1"#));
}

#[test]
fn test_stale_or_conflicting_hot_data_is_not_promoted() {
    let root = temp_storage_root("stale-skip");
    let hot = LocalHotCacheStorage::new(root.join("hot"));
    let durable = LocalStorage::new(root.join("durable"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    write_hot_block(
        &hot,
        &chain,
        &selector,
        20,
        "0x20-old",
        "0x19",
        |metadata| {
            metadata.finality_status = HotCacheFinalityStatus::Safe;
            metadata.eligible_for_promotion = true;
            metadata.candidate_status = HotCacheCandidateStatus::Stale;
        },
    );
    write_hot_block(&hot, &chain, &selector, 21, "0x21", "0x20", |metadata| {
        metadata.finality_status = HotCacheFinalityStatus::Safe;
        metadata.eligible_for_promotion = true;
    });
    let source = PromotionSource::new(ChainHeight::block(21).with_finality(FinalityLevel::Safe))
        .with_canonical_block(21, "0xcanonical", "0x20", FinalityLevel::Safe);
    let promoter = promoter(hot, durable.clone(), source);

    let result = promoter
        .promote(HotPromotionRequest {
            chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector,
            range: blocks(20, 21),
            application: None,
        })
        .expect("promotion skips ineligible data");

    assert_eq!(result.attempted, 2);
    assert_eq!(result.promoted, 0);
    assert_eq!(result.skipped, 2);
    assert!(
        durable
            .manifest()
            .expect("durable manifest")
            .entries
            .is_empty()
    );
}

#[test]
fn test_repeated_promotion_is_idempotent() {
    let root = temp_storage_root("idempotent");
    let hot = LocalHotCacheStorage::new(root.join("hot"));
    let durable = LocalStorage::new(root.join("durable"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    write_hot_block(&hot, &chain, &selector, 30, "0x30", "0x29", |metadata| {
        metadata.finality_status = HotCacheFinalityStatus::Finalized;
        metadata.eligible_for_promotion = true;
    });
    let source =
        PromotionSource::new(ChainHeight::block(30).with_finality(FinalityLevel::Finalized))
            .with_canonical_block(30, "0x30", "0x29", FinalityLevel::Finalized);
    let promoter = promoter(hot, durable.clone(), source);

    let request = HotPromotionRequest {
        chain: chain.clone(),
        dataset_key: DatasetKey::evm_blocks(),
        selector: selector.clone(),
        range: blocks(30, 30),
        application: None,
    };
    let first = promoter.promote(request.clone()).expect("first promotion");
    let second = promoter.promote(request).expect("second promotion");

    assert_eq!(first.promoted, 1);
    assert_eq!(second.promoted, 0);
    assert_eq!(second.skipped, 1);
    assert_eq!(durable.manifest().expect("manifest").entries.len(), 1);
}

#[test]
fn test_empty_hot_coverage_promotes_as_durable_empty_coverage() {
    let root = temp_storage_root("empty-coverage");
    let hot = LocalHotCacheStorage::new(root.join("hot"));
    let durable = LocalStorage::new(root.join("durable"));
    let chain = test_chain();
    let selector = DatasetSelector::all();
    let mut metadata = hot_metadata(35, "0x35", "0x34");
    metadata.finality_status = HotCacheFinalityStatus::Safe;
    metadata.eligible_for_promotion = true;
    hot.write_rows(datalens_storage::HotCacheWriteRequest {
        chain: &chain,
        dataset_key: DatasetKey::evm_blocks(),
        selector: &selector,
        range: blocks(35, 35),
        rows: &block_rows(&[]),
        metadata,
    })
    .expect("write empty hot coverage");
    let source = PromotionSource::new(ChainHeight::block(35).with_finality(FinalityLevel::Safe));
    let promoter = promoter(hot, durable.clone(), source);

    let result = promoter
        .promote(HotPromotionRequest {
            chain: chain.clone(),
            dataset_key: DatasetKey::evm_blocks(),
            selector,
            range: blocks(35, 35),
            application: None,
        })
        .expect("promote empty coverage");

    assert_eq!(result.promoted, 1);
    let manifest = durable.manifest().expect("manifest");
    assert_eq!(manifest.entries.len(), 1);
    assert_eq!(manifest.entries[0].range, blocks(35, 35));
    assert_eq!(manifest.entries[0].object_key, None);
    assert_eq!(manifest.entries[0].row_count, 0);
}

#[test]
fn test_promotion_failure_does_not_mark_or_delete_hot_data() {
    let hot = LocalHotCacheStorage::new(temp_storage_root("failure-hot"));
    let durable = FailingStorage;
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let chain = test_chain();
    let selector = DatasetSelector::all();
    write_hot_block(&hot, &chain, &selector, 40, "0x40", "0x39", |metadata| {
        metadata.finality_status = HotCacheFinalityStatus::Safe;
        metadata.eligible_for_promotion = true;
    });
    let source = PromotionSource::new(ChainHeight::block(40).with_finality(FinalityLevel::Safe))
        .with_canonical_block(40, "0x40", "0x39", FinalityLevel::Safe);
    let promoter = HotCachePromoter::new(
        hot.clone(),
        durable,
        source,
        DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
        },
    )
    .with_metrics(recorder.clone(), ApplicationIdentity::named("system"));

    let error = promoter
        .promote(HotPromotionRequest {
            chain: chain.clone(),
            dataset_key: DatasetKey::evm_blocks(),
            selector,
            range: blocks(40, 40),
            application: None,
        })
        .expect_err("durable write failure is returned");

    assert_eq!(error.kind, DatalensErrorKind::StorageWriteFailure);
    let entry = hot
        .list_entries(&chain, HeightRangeKind::Block, 40, 40)
        .expect("hot entries retained")
        .pop()
        .expect("hot entry");
    assert!(entry.promoted_at_unix_seconds.is_none());
    assert!(
        hot.object_store()
            .exists(&entry.object_key)
            .expect("hot object")
    );
    let metrics = recorder.encode().expect("metrics output");
    assert!(metrics.contains(r#"datalens_hot_promotion_total{application="system",chain="ethereum",chain_kind="evm",dataset="blocks",outcome="attempted"} 1"#));
    assert!(metrics.contains(r#"datalens_hot_promotion_total{application="system",chain="ethereum",chain_kind="evm",dataset="blocks",outcome="failed"} 1"#));
}

fn promoter<S>(
    hot: LocalHotCacheStorage,
    durable: S,
    source: PromotionSource,
) -> HotCachePromoter<LocalObjectStore, S, PromotionSource>
where
    S: datalens_storage::StorageRepository + Clone,
{
    HotCachePromoter::new(
        hot,
        durable,
        source,
        DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
        },
    )
}

fn write_hot_block(
    hot: &LocalHotCacheStorage,
    chain: &ChainIdentity,
    selector: &DatasetSelector,
    height: u64,
    hash: &str,
    parent_hash: &str,
    mutate: impl FnOnce(&mut HotCacheEntryMetadata),
) {
    let mut metadata = hot_metadata(height, hash, parent_hash);
    mutate(&mut metadata);
    hot.write_rows(datalens_storage::HotCacheWriteRequest {
        chain,
        dataset_key: DatasetKey::evm_blocks(),
        selector,
        range: blocks(height, height),
        rows: &block_rows(&[(height, hash, parent_hash)]),
        metadata,
    })
    .expect("write hot block");
}

fn hot_metadata(height: u64, block_hash: &str, parent_hash: &str) -> HotCacheEntryMetadata {
    HotCacheEntryMetadata {
        block_hash: block_hash.to_owned(),
        parent_hash: parent_hash.to_owned(),
        height,
        observed_at_unix_seconds: 1_700_000_000 + height,
        source_provider: "test-provider".to_owned(),
        finality_status: HotCacheFinalityStatus::Unsafe,
        row_count: 0,
        object_size_bytes: 0,
        checksum: String::new(),
        checksum_algorithm: String::new(),
        candidate_status: HotCacheCandidateStatus::Active,
        active_branch: Some("branch-a".to_owned()),
        eligible_for_promotion: false,
        promoted_at_unix_seconds: None,
        schema_version: String::new(),
        object_encoding: None,
        object_key: String::new(),
        metadata_key: String::new(),
        chain: None,
        dataset_key: None,
        selector_fingerprint: String::new(),
        selector_canonical_key: String::new(),
        range: None,
        source: datalens_core::QuerySegmentSource::HotCache,
        query_finality: QueryDataFinality::Unsafe,
    }
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-hot-promotion-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp storage root");
    root
}

fn test_chain() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain identity")
}

fn blocks(start: u64, end: u64) -> LedgerRange {
    LedgerRange::blocks(start, end).expect("valid block range")
}

fn block_rows(rows: &[(u64, &str, &str)]) -> DatasetRows {
    DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(
            rows.iter()
                .map(|(number, hash, parent_hash)| BlockHeader {
                    number: *number,
                    hash: (*hash).to_owned(),
                    parent_hash: (*parent_hash).to_owned(),
                    timestamp: *number,
                })
                .collect(),
        ),
    )
    .expect("dataset rows")
}

fn block_numbers(rows: &DatasetRows) -> Vec<u64> {
    match rows.rows() {
        QueryRows::EvmBlocks(blocks) => blocks.iter().map(|block| block.number).collect(),
        _ => panic!("expected block rows"),
    }
}

#[derive(Clone, Debug)]
struct PromotionSource {
    boundary: ChainHeight,
    canonical: Arc<Mutex<Vec<CanonicalBlock>>>,
}

impl PromotionSource {
    fn new(boundary: ChainHeight) -> Self {
        Self {
            boundary,
            canonical: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn with_canonical_block(
        self,
        height: u64,
        hash: &str,
        parent_hash: &str,
        finality: FinalityLevel,
    ) -> Self {
        self.canonical
            .lock()
            .expect("canonical lock")
            .push(CanonicalBlock {
                chain: test_chain(),
                height,
                hash: hash.to_owned(),
                parent_hash: parent_hash.to_owned(),
                finality,
            });
        self
    }
}

impl ChainAdapter for PromotionSource {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(test_chain()).with_dataset_capability(
            DatasetCapability::new(Dataset::Blocks)
                .with_selector(SelectorKind::All)
                .with_range(HeightRangeKind::Block)
                .with_safe_height(true)
                .with_finalized_height(true)
                .with_reorg_signals(true),
        )
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(100))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(self.boundary.clone())
    }

    fn canonical_block(
        &self,
        request: CanonicalBlockRequest,
    ) -> Result<CanonicalBlock, DatalensError> {
        self.canonical
            .lock()
            .expect("canonical lock")
            .iter()
            .find(|block| block.height == request.height && block.chain == request.chain)
            .cloned()
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    "missing canonical block",
                )
            })
    }

    fn fetch(&self, _request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        Err(DatalensError::unsupported(
            "promotion source does not fetch",
        ))
    }
}

#[derive(Clone, Default)]
struct FailingStorage;

impl datalens_storage::StorageRepository for FailingStorage {
    fn manifest(&self) -> Result<datalens_storage::Manifest, DatalensError> {
        Ok(datalens_storage::Manifest::default())
    }

    fn covered_ranges(
        &self,
        _chain: &ChainIdentity,
        _dataset_key: &DatasetKey,
        _selector: &DatasetSelector,
        _range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        Ok(Vec::new())
    }

    fn read_rows(
        &self,
        _chain: &ChainIdentity,
        _dataset_key: &DatasetKey,
        _selector: &DatasetSelector,
        _range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        Err(DatalensError::storage_read("not implemented"))
    }

    fn write_rows(
        &self,
        _request: datalens_storage::StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::StorageWriteFailure,
            "injected durable write failure",
        ))
    }
}
