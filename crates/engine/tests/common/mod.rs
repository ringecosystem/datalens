#![allow(dead_code)]

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalens_core::wire::{LogRecord, QueryRequest};
use datalens_core::{
    Address, B256, BlockRange, Bytes, ChainId, ChainName, Dataset, Finality, Heads, LogRow,
    LogSelector, QueryFinality, TopicFilter,
};
use datalens_engine::{Caller, Chain, ChainSettings, EngineSettings, QueryEngine};
use datalens_evm::mock::{ItemOutcome, MockRpc};
use datalens_evm::{EvmClient, HeadMode};
use datalens_provider::{Gateway, ProviderConfig, ProviderMetrics};
use datalens_store::Store;
use prometheus::Registry;
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::sync::watch;

pub mod follow_chain;

pub const CHUNK: u64 = 10_000;
pub const SAFE: u64 = 99_000;
pub const FINALIZED: u64 = 98_000;
pub const LATEST: u64 = 99_064;

pub fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

pub fn topic(byte: u8) -> B256 {
    B256::repeat_byte(byte)
}

pub fn chain_name() -> ChainName {
    ChainName::new("ethereum").unwrap()
}

pub fn chain_id() -> ChainId {
    ChainId::new(1).unwrap()
}

pub fn range(start: u64, end: u64) -> BlockRange {
    BlockRange::new(start, end).unwrap()
}

pub fn selector(addresses: &[u8], topic0: &[u8]) -> LogSelector {
    let mut topics: [TopicFilter; 4] = Default::default();
    if !topic0.is_empty() {
        topics[0] = TopicFilter::one_of(topic0.iter().map(|byte| topic(*byte))).unwrap();
    }
    LogSelector {
        addresses: addresses.iter().map(|byte| addr(*byte)).collect(),
        topics,
    }
}

pub fn sender_of(hash: &B256) -> Address {
    Address::from_slice(&hash[12..])
}

/// Inverse of the transaction hashes in [`fixture`].
fn block_of_transaction(hash: &B256) -> u64 {
    let i = u64::from_be_bytes(hash[24..].try_into().unwrap()) - 1_000_000;
    i * 500 + 7
}

pub fn block_hash(number: u64) -> B256 {
    B256::left_padding_from(&number.to_be_bytes())
}

/// One log every 500 blocks below 30_000: addresses cycle 0xa1, 0xa2, 0xa3, topic0
/// alternates 0x11 and 0x22. 60 logs in total.
pub fn fixture() -> Vec<LogRow> {
    (0..60_u64)
        .map(|i| {
            let block = i * 500 + 7;
            LogRow {
                block_number: block,
                log_index: (i % 3) as u32,
                block_hash: block_hash(block),
                transaction_index: 0,
                transaction_hash: B256::left_padding_from(&(1_000_000 + i).to_be_bytes()),
                address: addr(0xa1 + (i % 3) as u8),
                topics: vec![topic(if i % 2 == 0 { 0x11 } else { 0x22 }), topic(0x33)],
                data: Bytes::from(vec![i as u8]),
                finality: Finality::Finalized,
            }
        })
        .collect()
}

pub fn expected(selector: &LogSelector, blocks: BlockRange) -> Vec<Value> {
    fixture()
        .into_iter()
        .filter(|row| blocks.contains(row.block_number) && selector.matches(row))
        .map(|row| serde_json::to_value(LogRecord::from(row)).unwrap())
        .collect()
}

pub fn hex(number: u64) -> String {
    format!("0x{number:x}")
}

fn parse_hex(value: &Value) -> u64 {
    u64::from_str_radix(value.as_str().unwrap().trim_start_matches("0x"), 16).unwrap()
}

fn log_json(row: &LogRow) -> Value {
    json!({
        "blockNumber": hex(row.block_number),
        "logIndex": hex(row.log_index as u64),
        "blockHash": format!("{:#x}", row.block_hash),
        "transactionIndex": hex(row.transaction_index as u64),
        "transactionHash": format!("{:#x}", row.transaction_hash),
        "address": format!("{:#x}", row.address),
        "topics": row.topics.iter().map(|t| format!("{t:#x}")).collect::<Vec<_>>(),
        "data": format!("0x{}", hex::encode(&row.data)),
        "removed": false
    })
}

fn block_json(number: u64) -> Value {
    json!({
        "number": hex(number),
        "hash": format!("{:#x}", block_hash(number)),
        "parentHash": format!("{:#x}", block_hash(number.saturating_sub(1))),
        "timestamp": hex(1_700_000_000 + number),
        "gasUsed": "0x0"
    })
}

fn filter_matches(filter: &Value, row: &LogRow) -> bool {
    let from = parse_hex(&filter["fromBlock"]);
    let to = parse_hex(&filter["toBlock"]);
    if row.block_number < from || row.block_number > to {
        return false;
    }
    let address = format!("{:#x}", row.address);
    let address_ok = match &filter["address"] {
        Value::Null => true,
        Value::String(one) => one == &address,
        Value::Array(many) => many.iter().any(|a| a.as_str() == Some(address.as_str())),
        _ => false,
    };
    let topics = filter["topics"].as_array().cloned().unwrap_or_default();
    address_ok
        && topics.iter().enumerate().all(|(position, wanted)| {
            let have = row.topics.get(position).map(|t| format!("{t:#x}"));
            match wanted {
                Value::Null => true,
                Value::String(one) => have.as_deref() == Some(one.as_str()),
                Value::Array(many) => many
                    .iter()
                    .any(|t| t.as_str().is_some() && t.as_str() == have.as_deref()),
                _ => false,
            }
        })
}

/// Mock JSON-RPC chain over [`fixture`]. `fail_from` makes `eth_getLogs` starting at that
/// block fail with a non-retryable error.
pub struct MockChain {
    pub rpc: MockRpc,
    fail_from: Arc<Mutex<Vec<u64>>>,
    senders: Arc<Mutex<SenderFaults>>,
}

/// Transactions, by the block of their log, whose sender lookup misbehaves.
#[derive(Default)]
struct SenderFaults {
    failing: Vec<u64>,
    misplaced: Vec<u64>,
}

impl MockChain {
    pub async fn spawn() -> Self {
        Self::with_delay(Duration::ZERO).await
    }

    /// `eth_getLogs` blocks for `delay`. The mock gets its own multi-thread runtime, so the
    /// blocking handler does not stall the single-threaded test runtime.
    pub fn spawn_slow(delay: Duration) -> Self {
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                sender.send(Self::with_delay(delay).await).unwrap();
                std::future::pending::<()>().await;
            });
        });
        receiver.recv().unwrap()
    }

    async fn with_delay(delay: Duration) -> Self {
        let rpc = MockRpc::spawn().await;
        let fail_from: Arc<Mutex<Vec<u64>>> = Arc::default();
        let failing = Arc::clone(&fail_from);
        let senders: Arc<Mutex<SenderFaults>> = Arc::default();
        let sender_faults = Arc::clone(&senders);
        let logs = fixture();
        rpc.set_handler(move |item| {
            let params = &item["params"];
            match item["method"].as_str().unwrap_or_default() {
                "eth_getLogs" => {
                    std::thread::sleep(delay);
                    let filter = &params[0];
                    if failing
                        .lock()
                        .unwrap()
                        .contains(&parse_hex(&filter["fromBlock"]))
                    {
                        return ItemOutcome::Error {
                            code: -32000,
                            message: "internal boom".to_owned(),
                        };
                    }
                    ItemOutcome::Result(Value::Array(
                        logs.iter()
                            .filter(|row| filter_matches(filter, row))
                            .map(log_json)
                            .collect(),
                    ))
                }
                "eth_getBlockByNumber" => {
                    let number = match params[0].as_str() {
                        Some("latest") => LATEST,
                        _ => parse_hex(&params[0]),
                    };
                    ItemOutcome::Result(block_json(number))
                }
                "eth_getTransactionByHash" => {
                    let hash: B256 = params[0].as_str().unwrap().parse().unwrap();
                    let mut block = block_of_transaction(&hash);
                    let faults = sender_faults.lock().unwrap();
                    if faults.failing.contains(&block) {
                        return ItemOutcome::Error {
                            code: -32000,
                            message: "internal boom".to_owned(),
                        };
                    }
                    if faults.misplaced.contains(&block) {
                        block += 1;
                    }
                    ItemOutcome::Result(json!({
                        "from": format!("{:#x}", sender_of(&hash)),
                        "blockNumber": hex(block),
                        "blockHash": format!("{:#x}", block_hash(block)),
                    }))
                }
                _ => ItemOutcome::Result(Value::Null),
            }
        });
        Self {
            rpc,
            fail_from,
            senders,
        }
    }

    pub fn fail_logs_from(&self, block: u64) {
        self.fail_from.lock().unwrap().push(block);
    }

    pub fn clear_log_failures(&self) {
        self.fail_from.lock().unwrap().clear();
    }

    /// The sender lookup for the transaction logged at `block` returns a JSON-RPC error.
    pub fn fail_sender_at(&self, block: u64) {
        self.senders.lock().unwrap().failing.push(block);
    }

    /// The sender lookup for the transaction logged at `block` places it one block later.
    pub fn misplace_sender_at(&self, block: u64) {
        self.senders.lock().unwrap().misplaced.push(block);
    }

    /// JSON-RPC items by method, counting each item of a batch.
    pub fn count(&self, method: &str) -> usize {
        self.rpc
            .calls()
            .iter()
            .flat_map(|body| match body {
                Value::Array(items) => items.clone(),
                other => vec![other.clone()],
            })
            .filter(|item| item["method"].as_str() == Some(method))
            .count()
    }

    pub fn total(&self) -> usize {
        [
            "eth_getLogs",
            "eth_getBlockByNumber",
            "eth_getTransactionByHash",
        ]
        .iter()
        .map(|method| self.count(method))
        .sum()
    }
}

pub fn settings() -> EngineSettings {
    EngineSettings {
        max_wait: Duration::from_secs(10),
        page_limit: NonZeroU32::new(1_000).unwrap(),
        lease: Duration::from_secs(30),
        token_wait: Duration::from_secs(5),
        heads_ttl: Duration::from_millis(500),
        idle_poll: Duration::from_millis(50),
        ..EngineSettings::default()
    }
}

pub fn chain(url: &str, tick: Duration) -> Chain {
    chain_with_rps(url, tick, 1_000)
}

/// `rps` is also the burst, so `1` admits one call per second.
pub fn chain_with_rps(url: &str, tick: Duration, rps: u32) -> Chain {
    chain_with(url, tick, rps, CHUNK)
}

pub fn chain_with(url: &str, tick: Duration, rps: u32, max_get_logs_range: u64) -> Chain {
    let metrics = ProviderMetrics::register(&Registry::new()).unwrap();
    let gateway = Gateway::new(
        chain_name(),
        ProviderConfig {
            url: url.to_owned(),
            rps: NonZeroU32::new(rps).unwrap(),
            burst: NonZeroU32::new(rps).unwrap(),
            daily_calls: None,
            max_in_flight: NonZeroU32::new(8).unwrap(),
            min_range_blocks: NonZeroU64::new(16).unwrap(),
            max_get_logs_range_blocks: NonZeroU64::new(max_get_logs_range).unwrap(),
        },
        metrics,
    )
    .unwrap();
    Chain::new(
        ChainSettings {
            name: chain_name(),
            id: chain_id(),
            chunk_blocks: NonZeroU64::new(CHUNK).unwrap(),
            header_chunk_blocks: NonZeroU64::new(100).unwrap(),
            max_addresses_per_query: NonZeroUsize::new(32).unwrap(),
            head_mode: HeadMode::Lag {
                safe_lag_blocks: 64,
                finalized_lag_blocks: 1_064,
            },
            tick,
            reorg_window_blocks: NonZeroU64::new(128).unwrap(),
        },
        EvmClient::new(gateway, NonZeroUsize::new(32).unwrap(), 20).unwrap(),
    )
}

/// Engine with heads seeded directly in `chain_heads`, so no test query needs a head RPC.
pub async fn engine(pool: &PgPool, mock: &MockChain) -> QueryEngine {
    engine_with(
        pool,
        chain(mock.rpc.url(), Duration::from_millis(100)),
        settings(),
    )
    .await
}

pub async fn engine_with(pool: &PgPool, chain: Chain, settings: EngineSettings) -> QueryEngine {
    datalens_store::migrate(pool).await.unwrap();
    let store = Store::new(pool.clone());
    store.ensure_chain(chain_id(), &chain_name()).await.unwrap();
    store
        .upsert_heads(
            chain_id(),
            &Heads {
                latest: LATEST,
                latest_hash: block_hash(LATEST),
                safe: SAFE,
                finalized: FINALIZED,
            },
        )
        .await
        .unwrap();
    QueryEngine::new(store, vec![chain], settings, &Registry::new()).unwrap()
}

pub struct Workers {
    _stop: watch::Sender<bool>,
}

pub fn spawn_workers(engine: &QueryEngine, count: usize) -> Workers {
    let (stop, rx) = watch::channel(false);
    for n in 0..count {
        let engine = engine.clone();
        let rx = rx.clone();
        tokio::spawn(async move { engine.run_fill_worker(format!("test-{n}"), rx).await });
    }
    Workers { _stop: stop }
}

pub fn caller() -> Caller<'static> {
    Caller {
        application: "degov",
        fill_budget: None,
        max_subscriptions: None,
    }
}

pub fn logs_request(selector: LogSelector, blocks: BlockRange) -> QueryRequest {
    QueryRequest {
        chain: chain_name(),
        dataset: Dataset::EvmLogs,
        selector: Some(selector),
        range: blocks,
        finality: QueryFinality::DurableOnly,
        fields: None,
        page: None,
        wait_ms: None,
    }
}
