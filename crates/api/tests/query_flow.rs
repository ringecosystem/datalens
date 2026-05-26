use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use datalens_api::{
    QueryService, Source,
    config::{ChainConfig, DatasetsConfig, LogsDatasetConfig, PlannerConfig, WriterConfig},
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    LogFilter, LogRecord, NetworkId, QueryRequest, QueryResponse, QueryRows,
};
use datalens_storage::LocalStorage;

#[test]
fn test_query_blocks_miss_persists_then_equivalent_hit_uses_cache() {
    let storage = LocalStorage::new(temp_storage_root("blocks-miss-hit"));
    let source = MockSource::default().with_blocks(vec![block(10, "0x10"), block(11, "0x11")]);
    let service = service(storage, source.clone());
    let request = blocks_request(10, 11);

    let first = service
        .query(request.clone())
        .expect("first query succeeds");
    let second = service.query(request).expect("second query succeeds");

    assert_eq!(
        first.cache.missing_ranges,
        vec![BlockRange::expect_new(10, 11)]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![BlockRange::expect_new(10, 11)]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(10, 11))]
    );
    assert_eq!(block_numbers(&second), vec![10, 11]);
}

#[test]
fn test_query_blocks_partial_hit_fetches_only_missing_range() {
    let storage = LocalStorage::new(temp_storage_root("blocks-partial"));
    let source = MockSource::default().with_blocks(vec![
        block(1, "0x01"),
        block(2, "0x02"),
        block(3, "0x03"),
        block(4, "0x04"),
    ]);
    let service = service(storage, source.clone());

    service.query(blocks_request(1, 2)).expect("seed cache");
    source.clear_calls();
    let response = service.query(blocks_request(1, 4)).expect("partial query");

    assert_eq!(
        response.cache.hit_ranges,
        vec![BlockRange::expect_new(1, 2)]
    );
    assert_eq!(
        response.cache.missing_ranges,
        vec![BlockRange::expect_new(3, 4)]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(3, 4))]
    );
    assert_eq!(block_numbers(&response), vec![1, 2, 3, 4]);
}

#[test]
fn test_query_empty_logs_records_empty_coverage_without_data_object() {
    let storage = LocalStorage::new(temp_storage_root("logs-empty"));
    let root = storage.root().to_path_buf();
    let source = MockSource::default();
    let service = service(storage, source.clone());
    let request = logs_request(50, 52, vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]);

    let first = service
        .query(request.clone())
        .expect("empty log query succeeds");
    let second = service.query(request).expect("empty log query hits cache");

    assert_eq!(
        first.cache.missing_ranges,
        vec![BlockRange::expect_new(50, 52)]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![BlockRange::expect_new(50, 52)]
    );
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Logs(BlockRange::expect_new(50, 51)),
            SourceCall::Logs(BlockRange::expect_new(52, 52)),
        ]
    );
    assert_eq!(log_indexes(&second), Vec::<u64>::new());
    assert!(root.join("manifest.json").exists());
    assert!(!root.join("objects").exists());
}

#[test]
fn test_query_logs_miss_persists_then_equivalent_hit_uses_cache() {
    let storage = LocalStorage::new(temp_storage_root("logs-miss-hit"));
    let source = MockSource::default().with_logs(vec![
        log(
            20,
            0,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![TOPIC_A],
        ),
        log(
            20,
            1,
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            vec![TOPIC_A],
        ),
        log(
            21,
            0,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![TOPIC_B],
        ),
    ]);
    let service = service(storage, source.clone());
    let request = logs_request_with_topics(
        20,
        21,
        vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        vec![Some(vec![TOPIC_A])],
    );

    let first = service
        .query(request.clone())
        .expect("first log query succeeds");
    let second = service.query(request).expect("second log query succeeds");

    assert_eq!(
        first.cache.missing_ranges,
        vec![BlockRange::expect_new(20, 21)]
    );
    assert_eq!(
        second.cache.hit_ranges,
        vec![BlockRange::expect_new(20, 21)]
    );
    assert_eq!(
        source.calls(),
        vec![SourceCall::Logs(BlockRange::expect_new(20, 21))]
    );
    assert_eq!(
        log_addresses(&second),
        vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
    );
    assert_eq!(log_indexes(&second), vec![0]);
}

#[test]
fn test_query_range_limit_rejection_returns_invalid_input() {
    let service = service(
        LocalStorage::new(temp_storage_root("range-limit")),
        MockSource::default(),
    );
    let error = service
        .query(blocks_request(1, 5))
        .expect_err("range is too large");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
}

#[test]
fn test_provider_limit_error_is_classified() {
    let source = MockSource::default().with_error(DatalensErrorKind::ProviderLimit);
    let root = temp_storage_root("provider-limit");
    let service = service(LocalStorage::new(&root), source);
    let error = service
        .query(logs_request(
            1,
            2,
            vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        ))
        .expect_err("provider limit");

    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);
    assert!(!root.join("manifest.json").exists());
}

fn service(storage: LocalStorage, source: MockSource) -> QueryService<MockSource> {
    QueryService::new(
        storage,
        source,
        PlannerConfig {
            max_query_range_blocks: 4,
            default_chunk_range_blocks: 2,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
        },
        ChainConfig {
            kind: "evm".to_owned(),
            chain_id: 1,
            rpc_urls: vec!["http://example.invalid".to_owned()],
            safe_height_lag_blocks: 0,
            datasets: DatasetsConfig {
                blocks: datalens_api::config::BlocksDatasetConfig {
                    enabled: true,
                    max_batch_blocks: 2,
                },
                logs: LogsDatasetConfig {
                    enabled: true,
                    max_get_logs_range_blocks: 2,
                    max_addresses_per_query: 2,
                },
            },
        },
    )
}

fn blocks_request(from_block: u64, to_block: u64) -> QueryRequest {
    QueryRequest {
        chain: ethereum_identity(),
        dataset: Dataset::Blocks,
        range: BlockRange::expect_new(from_block, to_block),
        filter: None,
        include_block: false,
    }
}

fn logs_request(from_block: u64, to_block: u64, addresses: Vec<&str>) -> QueryRequest {
    logs_request_with_topics(
        from_block,
        to_block,
        addresses,
        vec![None, None, None, None],
    )
}

fn logs_request_with_topics(
    from_block: u64,
    to_block: u64,
    addresses: Vec<&str>,
    topics: Vec<Option<Vec<&str>>>,
) -> QueryRequest {
    QueryRequest {
        chain: ethereum_identity(),
        dataset: Dataset::Logs,
        range: BlockRange::expect_new(from_block, to_block),
        filter: Some(LogFilter {
            addresses: addresses.into_iter().map(str::to_owned).collect(),
            topics: topics
                .into_iter()
                .map(|topic| topic.map(|values| values.into_iter().map(str::to_owned).collect()))
                .collect(),
        }),
        include_block: false,
    }
}

const TOPIC_A: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const TOPIC_B: &str = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain identity")
}

fn block(number: u64, hash: &str) -> BlockHeader {
    BlockHeader {
        number,
        hash: hash.to_owned(),
        parent_hash: format!("{hash}-parent"),
        timestamp: number * 10,
    }
}

fn log(block_number: u64, log_index: u64, address: &str, topics: Vec<&str>) -> LogRecord {
    LogRecord {
        block_number,
        block_hash: format!("0xblock-{block_number}"),
        transaction_hash: format!("0xtx-{block_number}-{log_index}"),
        transaction_index: 0,
        log_index,
        address: address.to_owned(),
        topics: topics.into_iter().map(str::to_owned).collect(),
        data: "0x".to_owned(),
        removed: false,
    }
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp storage root");
    root
}

fn block_numbers(response: &QueryResponse) -> Vec<u64> {
    match &response.rows {
        QueryRows::Blocks(rows) => rows.iter().map(|row| row.number).collect(),
        QueryRows::Logs(_) => panic!("expected blocks"),
    }
}

fn log_indexes(response: &QueryResponse) -> Vec<u64> {
    match &response.rows {
        QueryRows::Logs(rows) => rows.iter().map(|row| row.log_index).collect(),
        QueryRows::Blocks(_) => panic!("expected logs"),
    }
}

fn log_addresses(response: &QueryResponse) -> Vec<String> {
    match &response.rows {
        QueryRows::Logs(rows) => rows.iter().map(|row| row.address.clone()).collect(),
        QueryRows::Blocks(_) => panic!("expected logs"),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceCall {
    Blocks(BlockRange),
    Logs(BlockRange),
}

#[derive(Clone, Default)]
struct MockSource {
    blocks: Arc<Mutex<Vec<BlockHeader>>>,
    logs: Arc<Mutex<Vec<LogRecord>>>,
    calls: Arc<Mutex<Vec<SourceCall>>>,
    error: Arc<Mutex<Option<DatalensErrorKind>>>,
}

impl MockSource {
    fn with_blocks(self, blocks: Vec<BlockHeader>) -> Self {
        *self.blocks.lock().expect("blocks lock") = blocks;
        self
    }

    fn with_logs(self, logs: Vec<LogRecord>) -> Self {
        *self.logs.lock().expect("logs lock") = logs;
        self
    }

    fn with_error(self, kind: DatalensErrorKind) -> Self {
        *self.error.lock().expect("error lock") = Some(kind);
        self
    }

    fn calls(&self) -> Vec<SourceCall> {
        self.calls.lock().expect("calls lock").clone()
    }

    fn clear_calls(&self) {
        self.calls.lock().expect("calls lock").clear();
    }
}

impl Source for MockSource {
    fn fetch_blocks(&self, range: BlockRange) -> Result<Vec<BlockHeader>, DatalensError> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(SourceCall::Blocks(range));
        if let Some(kind) = self.error.lock().expect("error lock").clone() {
            return Err(DatalensError::new(kind, "mock provider error"));
        }
        Ok(self
            .blocks
            .lock()
            .expect("blocks lock")
            .iter()
            .filter(|block| range.contains(block.number))
            .cloned()
            .collect())
    }

    fn fetch_logs(
        &self,
        range: BlockRange,
        filter: &LogFilter,
    ) -> Result<Vec<LogRecord>, DatalensError> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(SourceCall::Logs(range));
        if let Some(kind) = self.error.lock().expect("error lock").clone() {
            return Err(DatalensError::new(kind, "mock provider error"));
        }
        Ok(self
            .logs
            .lock()
            .expect("logs lock")
            .iter()
            .filter(|log| range.contains(log.block_number))
            .filter(|log| log_matches_filter(log, filter))
            .cloned()
            .collect())
    }
}

fn log_matches_filter(log: &LogRecord, filter: &LogFilter) -> bool {
    if !filter.addresses.is_empty()
        && !filter
            .addresses
            .iter()
            .any(|address| address == &log.address)
    {
        return false;
    }

    filter
        .topics
        .iter()
        .enumerate()
        .all(|(index, expected)| match expected {
            Some(values) => log
                .topics
                .get(index)
                .is_some_and(|topic| values.iter().any(|value| value == topic)),
            None => true,
        })
}
