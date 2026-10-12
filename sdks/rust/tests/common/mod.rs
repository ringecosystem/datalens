//! An in-process datalens server (api and worker roles, real PostgreSQL) over a mock chain
//! whose head advances and whose hashes can be replaced from any height. Every block holds
//! two logs: `0xb1` with topic0 `0x11` at index 0, and `0xb2` with topic0 `0x22` at index 1.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalens_evm::mock::{ItemOutcome, MockRpc};
use datalens_sdk::{
    Address, B256, BlockRange, ChainName, Client, ClientConfig, LogSelector, TopicFilter,
};
use datalens_server::config::Config;
use datalens_server::run::{RunError, run};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub const START: u64 = 2_000;
pub const SAFE_LAG: u64 = 8;
pub const APPLICATION: &str = "indexer";
pub const TOKEN: &str = "indexer-token";

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

    fn log(&self, number: u64, index: u64) -> Value {
        let (address, topic0) = [(0xb1, 0x11), (0xb2, 0x22)][index as usize];
        let mut tx = [0_u8; 32];
        tx[0] = self.hash(number)[1];
        tx[23] = index as u8;
        tx[24..].copy_from_slice(&number.to_be_bytes());
        json!({
            "blockNumber": hex(number),
            "logIndex": hex(index),
            "blockHash": format!("{:#x}", self.hash(number)),
            "transactionIndex": hex(index),
            "transactionHash": format!("{:#x}", B256::from(tx)),
            "address": format!("{:#x}", Address::repeat_byte(address)),
            "topics": [format!("{:#x}", B256::repeat_byte(topic0))],
            "data": format!("0x0{index}"),
            "removed": false
        })
    }
}

pub struct MockChain {
    rpc: MockRpc,
    state: Arc<Mutex<State>>,
}

impl MockChain {
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
                    let logs = (from..=to)
                        .flat_map(|number| [state.log(number, 0), state.log(number, 1)])
                        .filter(|log| matches(filter, log))
                        .collect();
                    ItemOutcome::Result(Value::Array(logs))
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

    pub fn url(&self) -> &str {
        self.rpc.url()
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

    pub fn hash(&self, number: u64) -> B256 {
        self.state.lock().unwrap().hash(number)
    }
}

fn matches(filter: &Value, log: &Value) -> bool {
    let address_ok = match &filter["address"] {
        Value::String(one) => *one == log["address"],
        Value::Array(many) => many.is_empty() || many.contains(&log["address"]),
        _ => true,
    };
    let topic_ok = match &filter["topics"][0] {
        Value::String(one) => *one == log["topics"][0],
        Value::Array(many) => many.contains(&log["topics"][0]),
        _ => true,
    };
    address_ok && topic_ok
}

fn hex(number: u64) -> String {
    format!("0x{number:x}")
}

fn parse_hex(value: &Value) -> u64 {
    u64::from_str_radix(value.as_str().unwrap().trim_start_matches("0x"), 16).unwrap()
}

pub fn ethereum() -> ChainName {
    ChainName::new("ethereum").unwrap()
}

pub fn range(start: u64, end: u64) -> BlockRange {
    BlockRange::new(start, end).unwrap()
}

/// Address `0xb1` with topic0 `0x11`: one log per block, at index 0.
pub fn usdc() -> LogSelector {
    let mut topics: [TopicFilter; 4] = Default::default();
    topics[0] = TopicFilter::one_of([B256::repeat_byte(0x11)]).unwrap();
    LogSelector {
        addresses: [Address::repeat_byte(0xb1)].into(),
        topics,
    }
}

/// Block numbers of the rows of a query, in order.
pub fn blocks_of(rows: &[Value]) -> Vec<u64> {
    rows.iter()
        .map(|row| row["block_number"].as_u64().unwrap())
        .collect()
}

fn config_text(rpc_url: &str) -> String {
    format!(
        r#"
[server]
bind = "127.0.0.1:0"
roles = ["api", "worker"]

[database]
url = "postgres://unused"

[query]
max_wait_ms = 5000

[fill]
workers = 2

[metrics]
bearer_token = "metrics-secret"

[chains.ethereum]
chain_id = 1
rpc_url = "{rpc_url}"
finality = {{ mode = "lag", safe_lag_blocks = {SAFE_LAG}, finalized_lag_blocks = 16 }}
tick_ms = 100
chunk_blocks = 100
max_get_logs_range_blocks = 1000
max_addresses_per_query = 32
provider = {{ rps = 1000, burst = 1000, max_in_flight = 8 }}

[[applications]]
id = "{APPLICATION}"
token = "{TOKEN}"
chains = ["ethereum"]
datasets = ["evm.logs", "evm.blocks"]
quota = {{ max_range_blocks = 5000, max_requests_per_minute = 60000, max_concurrent_requests = 16, max_subscriptions = 4 }}
"#
    )
}

/// A server running in-process until dropped.
pub struct Server {
    pub base: String,
    _stop: oneshot::Sender<()>,
    task: JoinHandle<Result<(), RunError>>,
}

impl Server {
    /// Migrates, boots on an ephemeral port, and waits until ethereum has a head.
    pub async fn boot(pool: PgPool, chain: &MockChain) -> Self {
        datalens_store::migrate(&pool).await.unwrap();
        let config = Config::from_toml(&config_text(chain.url()), &|_| None).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(run(config, pool, listener, async move {
            let _ = stopped.await;
        }));
        let server = Self {
            base,
            _stop: stop,
            task,
        };
        let client = server.client();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        while client.heads(&ethereum()).await.is_err() {
            assert!(tokio::time::Instant::now() < deadline, "no head");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        server
    }

    pub fn client(&self) -> Client {
        self.client_as(APPLICATION, TOKEN)
    }

    pub fn client_as(&self, application: &str, token: &str) -> Client {
        let mut config = ClientConfig::new(&self.base, application, token);
        config.retry.max_attempts = 1;
        Client::new(config).unwrap()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
