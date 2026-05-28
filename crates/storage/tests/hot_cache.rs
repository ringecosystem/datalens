use std::path::PathBuf;

use datalens_chain::{AdapterCapabilities, DatasetCapability, DatasetSelector, ReorgSignal};
use datalens_core::{
    BlockHeader, ChainFamily, ChainIdentity, DatalensErrorKind, DatasetKey, DatasetRows,
    LedgerRange, LedgerRangeKind, NetworkId, QueryRows,
};
use datalens_storage::{
    HotCache, HotCacheConfig, HotEntryStatus, HotReorgReason, HotWriteRequest, LocalObjectStore,
    LocalStorage, StorageWriteRequest,
};

#[test]
fn test_hot_write_with_continuous_parent_hash_does_not_rollback() {
    let cache = hot_cache("continuous-parent");
    let chain = test_chain();

    let first = cache
        .write_rows(HotWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: blocks(10, 10),
            rows: &block_rows(&[(10, "0x0a", "0x09")]),
            reorg_signals: &[signal(10, "0x0a", "0x09")],
        })
        .expect("write first hot block");
    let second = cache
        .write_rows(HotWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: blocks(11, 11),
            rows: &block_rows(&[(11, "0x0b", "0x0a")]),
            reorg_signals: &[signal(11, "0x0b", "0x0a")],
        })
        .expect("write continuous hot block");

    assert!(first.reorg.is_none());
    assert!(second.reorg.is_none());
    assert_eq!(cache.manifest(&chain).expect("manifest").entries.len(), 2);
}

#[test]
fn test_parent_mismatch_triggers_reorg_detection_and_marks_old_entries_stale() {
    let cache = hot_cache("parent-mismatch");
    let chain = test_chain();

    cache
        .write_rows(HotWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: blocks(10, 11),
            rows: &block_rows(&[(10, "0x0a", "0x09"), (11, "0x0b", "0x0a")]),
            reorg_signals: &[signal(10, "0x0a", "0x09"), signal(11, "0x0b", "0x0a")],
        })
        .expect("seed hot range");

    let outcome = cache
        .write_rows(HotWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: blocks(12, 12),
            rows: &block_rows(&[(12, "0x0c", "0xdead")]),
            reorg_signals: &[signal(12, "0x0c", "0xdead")],
        })
        .expect("write reorged block");

    let reorg = outcome.reorg.expect("reorg detected");
    assert_eq!(reorg.reason, HotReorgReason::ParentMismatch);
    assert_eq!(reorg.rollback_height, 12);
    assert_eq!(reorg.stale_entries, 0);
}

#[test]
fn test_same_height_different_hash_triggers_conflict_and_hides_stale_hot_rows() {
    let cache = hot_cache("same-height-conflict");
    let chain = test_chain();

    cache
        .write_rows(HotWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: blocks(10, 10),
            rows: &block_rows(&[(10, "0xold", "0x09")]),
            reorg_signals: &[signal(10, "0xold", "0x09")],
        })
        .expect("seed old candidate");

    let outcome = cache
        .write_rows(HotWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: blocks(10, 10),
            rows: &block_rows(&[(10, "0xnew", "0x08")]),
            reorg_signals: &[signal(10, "0xnew", "0x08")],
        })
        .expect("write canonical replacement");

    let reorg = outcome.reorg.expect("conflict detected");
    assert_eq!(reorg.reason, HotReorgReason::SameHeightDifferentHash);
    assert_eq!(reorg.rollback_height, 10);
    assert_eq!(reorg.stale_entries, 1);

    let rows = cache
        .read_rows(
            &chain,
            &DatasetKey::evm_blocks(),
            &DatasetSelector::all(),
            blocks(10, 10),
        )
        .expect("read active hot rows");
    let QueryRows::EvmBlocks(rows) = rows.rows() else {
        panic!("expected block rows");
    };
    assert_eq!(
        rows.iter().map(|row| row.hash.as_str()).collect::<Vec<_>>(),
        ["0xnew"]
    );

    let manifest = cache.manifest(&chain).expect("manifest");
    assert_eq!(
        manifest
            .entries
            .iter()
            .filter(|entry| entry.status == HotEntryStatus::Stale)
            .count(),
        1
    );
}

#[test]
fn test_hot_rollback_does_not_modify_durable_manifest() {
    let root = temp_storage_root("durable-untouched");
    let durable = LocalStorage::new(&root);
    let cache = HotCache::new(LocalObjectStore::new(&root), HotCacheConfig::default());
    let chain = test_chain();

    durable
        .write_rows(StorageWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: blocks(1, 1),
            rows: &block_rows(&[(1, "0x01", "0x00")]),
            finality_level: datalens_chain::FinalityLevel::Safe,
            record_empty_coverage: true,
        })
        .expect("write durable row");
    let before = durable.manifest().expect("durable manifest before");

    cache
        .write_rows(HotWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: blocks(10, 10),
            rows: &block_rows(&[(10, "0xold", "0x09")]),
            reorg_signals: &[signal(10, "0xold", "0x09")],
        })
        .expect("seed hot");
    cache
        .write_rows(HotWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: blocks(10, 10),
            rows: &block_rows(&[(10, "0xnew", "0x08")]),
            reorg_signals: &[signal(10, "0xnew", "0x08")],
        })
        .expect("trigger hot rollback");

    assert_eq!(durable.manifest().expect("durable manifest after"), before);
}

#[test]
fn test_unsupported_adapter_returns_explicit_hot_query_error() {
    let capabilities = AdapterCapabilities::new(test_chain()).with_dataset_capability(
        DatasetCapability::new(DatasetKey::evm_blocks())
            .with_selector(datalens_chain::SelectorKind::All)
            .with_range(LedgerRangeKind::Block),
    );

    let error = HotCache::<LocalObjectStore>::validate_adapter_support(
        &capabilities,
        &DatasetKey::evm_blocks(),
    )
    .expect_err("unsupported hot query");

    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
    assert!(error.message.contains("hot cache reorg detection"));
}

#[test]
fn test_reorg_deeper_than_configured_window_returns_explicit_error() {
    let cache = HotCache::new(
        LocalObjectStore::new(temp_storage_root("window")),
        HotCacheConfig { reorg_window: 1 },
    );
    let chain = test_chain();

    cache
        .write_rows(HotWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: blocks(10, 12),
            rows: &block_rows(&[
                (10, "0x0a", "0x09"),
                (11, "0x0b", "0x0a"),
                (12, "0x0c", "0x0b"),
            ]),
            reorg_signals: &[
                signal(10, "0x0a", "0x09"),
                signal(11, "0x0b", "0x0a"),
                signal(12, "0x0c", "0x0b"),
            ],
        })
        .expect("seed hot range");

    let error = cache
        .write_rows(HotWriteRequest {
            chain: &chain,
            dataset_key: DatasetKey::evm_blocks(),
            selector: &DatasetSelector::all(),
            range: blocks(10, 10),
            rows: &block_rows(&[(10, "0xnew", "0x08")]),
            reorg_signals: &[signal(10, "0xnew", "0x08")],
        })
        .expect_err("reorg too deep");

    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);
    assert!(error.message.contains("hot reorg window"));
}

fn hot_cache(name: &str) -> HotCache<LocalObjectStore> {
    HotCache::new(
        LocalObjectStore::new(temp_storage_root(name)),
        HotCacheConfig::default(),
    )
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-hot-cache-{name}-{}",
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

fn signal(height: u64, hash: &str, parent_hash: &str) -> ReorgSignal {
    ReorgSignal::block(height, hash, parent_hash, None)
}

fn block_rows(blocks: &[(u64, &str, &str)]) -> DatasetRows {
    DatasetRows::new(
        DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(
            blocks
                .iter()
                .map(|(number, hash, parent_hash)| BlockHeader {
                    number: *number,
                    hash: (*hash).to_owned(),
                    parent_hash: (*parent_hash).to_owned(),
                    timestamp: 1,
                })
                .collect(),
        ),
    )
    .expect("dataset rows")
}
