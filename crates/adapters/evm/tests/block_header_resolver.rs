use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use datalens_chain::FinalityLevel;
use datalens_core::{BlockRange, ChainFamily, ChainIdentity, DatalensError, EvmBlockHeader};
use datalens_evm::{
    EvmBlockHeaderFetch, EvmBlockHeaderFetcher, EvmBlockHeaderResolveRequest,
    EvmBlockHeaderResolver, EvmBlockHeaderStore,
};

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
