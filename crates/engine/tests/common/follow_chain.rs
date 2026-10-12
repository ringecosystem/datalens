//! A mock chain whose head advances and whose hashes can be replaced from any height.
//! Every block `n` holds two logs: `0xb1` with topic0 `0x11` at index 0 and `0xb2` with
//! topic0 `0x22` at index 1.

use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalens_core::wire::QueryRequest;
use datalens_core::{
    B256, BlockRange, Bytes, ChainId, ChainName, Dataset, Finality, LogRow, LogSelector,
    QueryFinality,
};
use datalens_engine::{Chain, ChainSettings, EngineSettings, QueryEngine};
use datalens_evm::mock::{ItemOutcome, MockRpc};
use datalens_evm::{EvmClient, HeadMode};
use datalens_provider::{Gateway, ProviderConfig, ProviderMetrics};
use datalens_store::Store;
use prometheus::{Encoder, Registry};
use serde_json::{Value, json};
use sqlx::PgPool;

use super::{addr, filter_matches, hex, log_json, parse_hex, selector};

pub const SAFE_LAG: u64 = 8;
pub const FINALIZED_LAG: u64 = 16;
pub const LOG_CHUNK: u64 = 1_000;

#[derive(Default)]
struct State {
    latest: u64,
    /// `(from, salt)`: blocks at or above `from` carry `salt` in their hash.
    forks: Vec<(u64, u8)>,
}

impl State {
    fn hash(&self, number: u64) -> B256 {
        let salt = self
            .forks
            .iter()
            .rev()
            .find(|(from, _)| *from <= number)
            .map_or(0, |(_, salt)| *salt);
        let mut bytes = [0_u8; 32];
        bytes[0] = 0xcc;
        bytes[1] = salt;
        bytes[24..].copy_from_slice(&number.to_be_bytes());
        B256::from(bytes)
    }

    fn logs(&self, number: u64) -> [LogRow; 2] {
        let row = |index: u32, address: u8, topic0: u8| {
            let mut tx = [0_u8; 32];
            tx[0] = self.hash(number)[1];
            tx[23] = index as u8;
            tx[24..].copy_from_slice(&number.to_be_bytes());
            LogRow {
                block_number: number,
                log_index: index,
                block_hash: self.hash(number),
                transaction_index: index,
                transaction_hash: B256::from(tx),
                address: addr(address),
                topics: vec![super::topic(topic0)],
                data: Bytes::from(vec![index as u8]),
                finality: Finality::Unsafe,
            }
        };
        [row(0, 0xb1, 0x11), row(1, 0xb2, 0x22)]
    }
}

pub struct FollowChain {
    pub rpc: MockRpc,
    state: Arc<Mutex<State>>,
}

impl FollowChain {
    pub async fn spawn(latest: u64) -> Self {
        let rpc = MockRpc::spawn().await;
        let state = Arc::new(Mutex::new(State {
            latest,
            forks: Vec::new(),
        }));
        let shared = Arc::clone(&state);
        rpc.set_handler(move |item| {
            let state = shared.lock().unwrap();
            let params = &item["params"];
            match item["method"].as_str().unwrap_or_default() {
                "eth_getLogs" => {
                    let filter = &params[0];
                    let from = parse_hex(&filter["fromBlock"]);
                    let to = parse_hex(&filter["toBlock"]).min(state.latest);
                    ItemOutcome::Result(Value::Array(
                        (from..=to)
                            .flat_map(|number| state.logs(number))
                            .filter(|row| filter_matches(filter, row))
                            .map(|row| log_json(&row))
                            .collect(),
                    ))
                }
                "eth_getBlockByNumber" => {
                    let number = match params[0].as_str() {
                        Some("latest") => state.latest,
                        _ => parse_hex(&params[0]),
                    };
                    if number > state.latest {
                        return ItemOutcome::Result(Value::Null);
                    }
                    ItemOutcome::Result(json!({
                        "number": hex(number),
                        "hash": format!("{:#x}", state.hash(number)),
                        "parentHash": format!("{:#x}", state.hash(number.saturating_sub(1))),
                        "timestamp": hex(1_700_000_000 + number),
                        "gasUsed": "0x0"
                    }))
                }
                _ => ItemOutcome::Result(Value::Null),
            }
        });
        Self { rpc, state }
    }

    pub fn advance(&self, blocks: u64) {
        self.state.lock().unwrap().latest += blocks;
    }

    /// Replaces the hash of every block from `from` on, as a reorg would.
    pub fn reorg_from(&self, from: u64, salt: u8) {
        self.state.lock().unwrap().forks.push((from, salt));
    }

    pub fn latest(&self) -> u64 {
        self.state.lock().unwrap().latest
    }

    pub fn safe(&self) -> u64 {
        self.latest() - SAFE_LAG
    }

    pub fn hash(&self, number: u64) -> B256 {
        self.state.lock().unwrap().hash(number)
    }

    /// The logs `wanted` matches in `blocks`, as the chain holds them now.
    pub fn logs(&self, wanted: &LogSelector, blocks: BlockRange) -> Vec<LogRow> {
        let state = self.state.lock().unwrap();
        (blocks.start()..=blocks.end())
            .flat_map(|number| state.logs(number))
            .filter(|row| wanted.matches(row))
            .collect()
    }

    /// HTTP requests, so a batch counts once.
    pub fn requests(&self) -> usize {
        self.rpc.calls().len()
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
}

pub fn follow_chain(name: &str, id: i32, url: &str) -> Chain {
    let name = ChainName::new(name).unwrap();
    let metrics = ProviderMetrics::register(&Registry::new()).unwrap();
    let gateway = Gateway::new(
        name.clone(),
        ProviderConfig {
            url: url.to_owned(),
            rps: NonZeroU32::new(1_000).unwrap(),
            burst: NonZeroU32::new(1_000).unwrap(),
            daily_calls: None,
            max_in_flight: NonZeroU32::new(8).unwrap(),
            min_range_blocks: NonZeroU64::new(16).unwrap(),
            max_get_logs_range_blocks: NonZeroU64::new(LOG_CHUNK).unwrap(),
        },
        metrics,
    )
    .unwrap();
    Chain::new(
        ChainSettings {
            name,
            id: ChainId::new(id).unwrap(),
            chunk_blocks: NonZeroU64::new(LOG_CHUNK).unwrap(),
            header_chunk_blocks: NonZeroU64::new(100).unwrap(),
            max_addresses_per_query: NonZeroUsize::new(32).unwrap(),
            head_mode: HeadMode::Lag {
                safe_lag_blocks: SAFE_LAG,
                finalized_lag_blocks: FINALIZED_LAG,
            },
            tick: Duration::from_millis(100),
            reorg_window_blocks: NonZeroU64::new(64).unwrap(),
        },
        EvmClient::new(gateway, NonZeroUsize::new(32).unwrap(), 20).unwrap(),
    )
}

pub fn follow_settings() -> EngineSettings {
    EngineSettings {
        max_wait: Duration::from_secs(5),
        heads_ttl: Duration::ZERO,
        idle_poll: Duration::from_millis(50),
        retention_every: Duration::ZERO,
        ..EngineSettings::default()
    }
}

/// Engine over `chains` with heads written by one leader tick each.
pub async fn follow_engine(
    pool: &PgPool,
    chains: Vec<Chain>,
    settings: EngineSettings,
    registry: &Registry,
) -> QueryEngine {
    datalens_store::migrate(pool).await.unwrap();
    let store = Store::new(pool.clone());
    let names: Vec<ChainName> = chains.iter().map(|chain| chain.name().clone()).collect();
    for chain in &chains {
        store.ensure_chain(chain.id(), chain.name()).await.unwrap();
    }
    let engine = QueryEngine::new(store, chains, settings, registry).unwrap();
    for name in &names {
        engine.tick_once(name).await.unwrap();
    }
    engine
}

/// The registry in Prometheus text format.
pub fn metrics_text(registry: &Registry) -> String {
    let mut encoded = Vec::new();
    prometheus::TextEncoder::new()
        .encode(&registry.gather(), &mut encoded)
        .unwrap();
    String::from_utf8(encoded).unwrap()
}

pub fn usdc() -> LogSelector {
    selector(&[0xb1], &[0x11])
}

/// Any address with topic0 `0x22`. It does not batch with an address-bound selector.
pub fn any_transfer() -> LogSelector {
    selector(&[], &[0x22])
}

pub fn request(
    chain: &str,
    wanted: LogSelector,
    blocks: BlockRange,
    finality: QueryFinality,
) -> QueryRequest {
    QueryRequest {
        chain: ChainName::new(chain).unwrap(),
        dataset: Dataset::EvmLogs,
        selector: Some(wanted),
        range: blocks,
        finality,
        fields: None,
        page: None,
        wait_ms: Some(0),
    }
}
