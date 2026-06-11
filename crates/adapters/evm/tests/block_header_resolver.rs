use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use datalens_chain::FinalityLevel;
use datalens_core::{BlockRange, ChainFamily, ChainIdentity, DatalensError, EvmBlockHeader};
use datalens_evm::{
    DurableEvmBlockHeaderStore, EvmBlockHeaderFetch, EvmBlockHeaderFetcher,
    EvmBlockHeaderResolveRequest, EvmBlockHeaderResolver, EvmBlockHeaderStore,
};
use datalens_storage::LocalStorage;
use datalens_writer::DurableWriterConfig;

#[test]
fn test_block_header_resolver_reuses_stored_headers_and_fetches_only_missing_ranges() {
    let chain = ethereum();
    let store = MemoryHeaderStore::new(vec![header(10), header(11), header(13)]);
    let fetcher = MemoryHeaderFetcher::new(vec![header(12)]);
    let resolver = EvmBlockHeaderResolver::with_store(fetcher.clone(), store.clone());

    let resolved = resolver
        .resolve(EvmBlockHeaderResolveRequest {
            chain: chain.clone(),
            range: BlockRange::expect_new(10, 13),
            finality_level: FinalityLevel::Safe,
        })
        .expect("resolve headers");

    assert_eq!(numbers(&resolved), vec![10, 11, 12, 13]);
    assert_eq!(fetcher.requests(), vec![BlockRange::expect_new(12, 12)]);
    assert_eq!(store.persisted_numbers(), vec![12]);
}

#[test]
fn test_block_header_resolver_does_not_fetch_when_requested_range_is_durable() {
    let chain = ethereum();
    let store = MemoryHeaderStore::new(vec![header(20), header(21)]);
    let fetcher = MemoryHeaderFetcher::new(Vec::new());
    let resolver = EvmBlockHeaderResolver::with_store(fetcher.clone(), store.clone());

    let resolved = resolver
        .resolve(EvmBlockHeaderResolveRequest {
            chain,
            range: BlockRange::expect_new(20, 21),
            finality_level: FinalityLevel::Finalized,
        })
        .expect("resolve headers");

    assert_eq!(numbers(&resolved), vec![20, 21]);
    assert!(fetcher.requests().is_empty());
    assert!(store.persisted_numbers().is_empty());
}

#[test]
fn test_block_header_resolver_does_not_reuse_safe_headers_for_finalized_resolve() {
    let chain = ethereum();
    let storage = LocalStorage::new(temp_storage_root("finality-aware-header-resolve"));
    let store = DurableEvmBlockHeaderStore::new(storage, writer_config());
    store
        .persist_headers(
            &chain,
            BlockRange::expect_new(30, 30),
            FinalityLevel::Safe,
            vec![header(30)],
        )
        .expect("persist safe header");
    let fetcher = MemoryHeaderFetcher::new(vec![header(30)]);
    let resolver = EvmBlockHeaderResolver::with_store(fetcher.clone(), store);

    let resolved = resolver
        .resolve(EvmBlockHeaderResolveRequest {
            chain,
            range: BlockRange::expect_new(30, 30),
            finality_level: FinalityLevel::Finalized,
        })
        .expect("resolve finalized header");

    assert_eq!(numbers(&resolved), vec![30]);
    assert_eq!(fetcher.requests(), vec![BlockRange::expect_new(30, 30)]);
}

#[derive(Clone, Debug)]
struct MemoryHeaderStore {
    stored: Arc<Mutex<BTreeMap<u64, EvmBlockHeader>>>,
    persisted: Arc<Mutex<Vec<EvmBlockHeader>>>,
}

impl MemoryHeaderStore {
    fn new(headers: Vec<EvmBlockHeader>) -> Self {
        Self {
            stored: Arc::new(Mutex::new(
                headers
                    .into_iter()
                    .map(|header| (header.block_number, header))
                    .collect(),
            )),
            persisted: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn persisted_numbers(&self) -> Vec<u64> {
        self.persisted
            .lock()
            .expect("persisted headers")
            .iter()
            .map(|header| header.block_number)
            .collect()
    }
}

impl EvmBlockHeaderStore for MemoryHeaderStore {
    fn read_headers(
        &self,
        _chain: &ChainIdentity,
        range: BlockRange,
        _finality_level: FinalityLevel,
    ) -> Result<Vec<EvmBlockHeader>, DatalensError> {
        Ok(self
            .stored
            .lock()
            .expect("stored headers")
            .values()
            .filter(|header| range.contains(header.block_number))
            .cloned()
            .collect())
    }

    fn persist_headers(
        &self,
        _chain: &ChainIdentity,
        _range: BlockRange,
        _finality_level: FinalityLevel,
        headers: Vec<EvmBlockHeader>,
    ) -> Result<(), DatalensError> {
        let mut stored = self.stored.lock().expect("stored headers");
        let mut persisted = self.persisted.lock().expect("persisted headers");
        for header in headers {
            stored.insert(header.block_number, header.clone());
            persisted.push(header);
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct MemoryHeaderFetcher {
    headers: Arc<BTreeMap<u64, EvmBlockHeader>>,
    requests: Arc<Mutex<Vec<BlockRange>>>,
}

impl MemoryHeaderFetcher {
    fn new(headers: Vec<EvmBlockHeader>) -> Self {
        Self {
            headers: Arc::new(
                headers
                    .into_iter()
                    .map(|header| (header.block_number, header))
                    .collect(),
            ),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn requests(&self) -> Vec<BlockRange> {
        self.requests.lock().expect("fetch requests").clone()
    }
}

impl EvmBlockHeaderFetcher for MemoryHeaderFetcher {
    fn fetch_block_headers(&self, range: BlockRange) -> Result<EvmBlockHeaderFetch, DatalensError> {
        self.requests.lock().expect("fetch requests").push(range);
        Ok(EvmBlockHeaderFetch {
            range,
            headers: self
                .headers
                .values()
                .filter(|header| range.contains(header.block_number))
                .cloned()
                .collect(),
        })
    }
}

fn ethereum() -> ChainIdentity {
    ChainIdentity::expect_new(ChainFamily::Evm, "ethereum")
}

fn header(block_number: u64) -> EvmBlockHeader {
    EvmBlockHeader {
        block_number,
        block_hash: format!("0x{block_number:064x}"),
        parent_hash: format!("0x{:064x}", block_number - 1),
        timestamp: 1_700_000_000 + block_number,
        logs_bloom: format!("0x{}", "00".repeat(256)),
    }
}

fn numbers(headers: &[EvmBlockHeader]) -> Vec<u64> {
    headers.iter().map(|header| header.block_number).collect()
}

fn writer_config() -> DurableWriterConfig {
    DurableWriterConfig {
        target_object_bytes: 1024,
        min_object_rows: 1,
        record_empty_coverage: true,
        staging: Default::default(),
    }
}

fn temp_storage_root(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "datalens-evm-{name}-{}-{nanos}",
        std::process::id()
    ))
}
