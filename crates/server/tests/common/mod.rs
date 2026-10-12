#![allow(dead_code)]

use std::time::Duration;

use datalens_client::Client;
use datalens_core::wire::QueryRequest;
use datalens_core::{
    Address, B256, BlockRange, ChainName, Dataset, LogSelector, QueryFinality, TopicFilter,
};
use datalens_evm::mock::{ItemOutcome, MockRpc};
use datalens_server::config::Config;
use datalens_server::run::{RunError, run};
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub const LATEST: u64 = 99_064;
pub const SAFE: u64 = 99_000;
pub const METRICS_TOKEN: &str = "metrics-secret";

pub fn addr(byte: u8) -> Address {
    Address::repeat_byte(byte)
}

pub fn topic(byte: u8) -> B256 {
    B256::repeat_byte(byte)
}

pub fn range(start: u64, end: u64) -> BlockRange {
    BlockRange::new(start, end).unwrap()
}

pub fn ethereum() -> ChainName {
    ChainName::new("ethereum").unwrap()
}

pub fn selector(addresses: &[u8]) -> LogSelector {
    let mut topics: [TopicFilter; 4] = Default::default();
    topics[0] = TopicFilter::one_of([topic(0x11)]).unwrap();
    LogSelector {
        addresses: addresses.iter().map(|byte| addr(*byte)).collect(),
        topics,
    }
}

pub fn logs_request(chain: ChainName, selector: LogSelector, blocks: BlockRange) -> QueryRequest {
    QueryRequest {
        chain,
        dataset: Dataset::EvmLogs,
        selector: Some(selector),
        range: blocks,
        finality: QueryFinality::DurableOnly,
        fields: None,
        page: None,
        wait_ms: None,
    }
}

fn hex(number: u64) -> String {
    format!("0x{number:x}")
}

fn parse_hex(value: &Value) -> u64 {
    u64::from_str_radix(value.as_str().unwrap().trim_start_matches("0x"), 16).unwrap()
}

/// Block of the `i`th fixture log: one every 500 blocks from block 7, 20 in total.
pub fn log_block(i: u64) -> u64 {
    i * 500 + 7
}

/// Mock chain: 20 logs alternating between 0xa1 and 0xa2, all with topic0 0x11.
pub async fn mock_chain() -> MockRpc {
    let rpc = MockRpc::spawn().await;
    rpc.set_handler(|item| {
        let params = &item["params"];
        match item["method"].as_str().unwrap_or_default() {
            "eth_getLogs" => {
                let filter = &params[0];
                let (from, to) = (
                    parse_hex(&filter["fromBlock"]),
                    parse_hex(&filter["toBlock"]),
                );
                let wanted: Vec<String> = match &filter["address"] {
                    Value::String(one) => vec![one.clone()],
                    Value::Array(many) => many
                        .iter()
                        .map(|a| a.as_str().unwrap().to_owned())
                        .collect(),
                    _ => Vec::new(),
                };
                let logs = (0..20_u64)
                    .filter_map(|i| {
                        let block = log_block(i);
                        let address = format!("{:#x}", addr(0xa1 + (i % 2) as u8));
                        let keep = (from..=to).contains(&block)
                            && (wanted.is_empty() || wanted.contains(&address));
                        keep.then(|| {
                            json!({
                                "blockNumber": hex(block),
                                "logIndex": "0x0",
                                "blockHash": format!("{:#x}", B256::left_padding_from(&block.to_be_bytes())),
                                "transactionIndex": "0x0",
                                "transactionHash": format!("{:#x}", B256::left_padding_from(&(1_000_000 + i).to_be_bytes())),
                                "address": address,
                                "topics": [format!("{:#x}", topic(0x11))],
                                "data": "0x",
                                "removed": false
                            })
                        })
                    })
                    .collect();
                ItemOutcome::Result(Value::Array(logs))
            }
            "eth_getBlockByNumber" => {
                let number = match params[0].as_str() {
                    Some("latest") => LATEST,
                    _ => parse_hex(&params[0]),
                };
                ItemOutcome::Result(json!({
                    "number": hex(number),
                    "hash": format!("{:#x}", B256::left_padding_from(&number.to_be_bytes())),
                    "parentHash": format!("{:#x}", B256::left_padding_from(&number.saturating_sub(1).to_be_bytes())),
                    "timestamp": hex(1_700_000_000 + number),
                }))
            }
            _ => ItemOutcome::Result(Value::Null),
        }
    });
    rpc
}

/// JSON-RPC items with `method`, counting each item of a batch.
pub fn count(rpc: &MockRpc, method: &str) -> usize {
    rpc.calls()
        .iter()
        .flat_map(|body| match body {
            Value::Array(items) => items.clone(),
            other => vec![other.clone()],
        })
        .filter(|item| item["method"].as_str() == Some(method))
        .count()
}

/// Two chains on the same mock RPC. `degov` may use ethereum with a 50_000 block range and
/// admin rights; `narrow` may only use base.
pub fn config_text(rpc_url: &str) -> String {
    format!(
        r#"
[server]
bind = "127.0.0.1:0"
roles = ["api", "worker"]

[database]
url = "postgres://unused"

[fill]
workers = 2

[metrics]
bearer_token = "{METRICS_TOKEN}"

[chains.ethereum]
chain_id = 1
rpc_url = "{rpc_url}"
finality = {{ mode = "lag", safe_lag_blocks = 64, finalized_lag_blocks = 1064 }}
tick_ms = 100
chunk_blocks = 10000
max_get_logs_range_blocks = 10000
max_addresses_per_query = 32
provider = {{ rps = 1000, burst = 1000, max_in_flight = 8 }}

[chains.base]
chain_id = 8453
rpc_url = "{rpc_url}"
finality = {{ mode = "lag", safe_lag_blocks = 64, finalized_lag_blocks = 1064 }}
tick_ms = 100
chunk_blocks = 10000
max_get_logs_range_blocks = 10000
max_addresses_per_query = 32
provider = {{ rps = 1000, burst = 1000, max_in_flight = 8 }}

[[applications]]
id = "degov"
token = "degov-token"
chains = ["ethereum"]
datasets = ["evm.logs", "evm.blocks"]
quota = {{ max_range_blocks = 50000, max_requests_per_minute = 600, max_concurrent_requests = 8, max_subscriptions = 1 }}
admin = true

[[applications]]
id = "narrow"
token = "narrow-token"
chains = ["base"]
datasets = ["evm.logs"]
quota = {{ max_range_blocks = 50000, max_requests_per_minute = 600, max_concurrent_requests = 8, max_subscriptions = 1 }}
"#
    )
}

/// A server running in-process until dropped or [`Booted::stop`] is called.
pub struct Booted {
    pub base: String,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), RunError>>>,
}

impl Booted {
    pub fn client(&self, application: &str, token: &str) -> Client {
        Client::new(&self.base, application, token)
    }

    pub fn degov(&self) -> Client {
        self.client("degov", "degov-token")
    }

    pub async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        self.task.take().unwrap().await.unwrap().unwrap();
    }
}

impl Drop for Booted {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

/// Migrates, boots `config` on an ephemeral port, and waits until every chain has a head.
pub async fn boot(pool: PgPool, config: Config) -> Booted {
    datalens_store::migrate(&pool).await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let chains: Vec<ChainName> = config.applications[0].chains.iter().cloned().collect();
    let token = config.applications[0].token.expose().to_owned();
    let application = config.applications[0].id.clone();
    let (stop, stopped) = oneshot::channel::<()>();
    let task = tokio::spawn(run(config, pool, listener, async move {
        let _ = stopped.await;
    }));
    let booted = Booted {
        base,
        stop: Some(stop),
        task: Some(task),
    };
    let client = booted.client(&application, &token);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    for chain in &chains {
        while client.chain_heads(chain).await.is_err() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "{chain} never got a head"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    booted
}

pub async fn boot_mock(pool: PgPool, rpc: &MockRpc) -> Booted {
    let config = Config::from_toml(&config_text(rpc.url()), &|_| None).unwrap();
    boot(pool, config).await
}
