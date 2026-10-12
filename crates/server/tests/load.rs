mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::boot;
use datalens_client::Client;
use datalens_core::wire::{BackfillRequest, QueryRequest, WatchRequest};
use datalens_core::{
    Address, B256, BlockRange, ChainName, Dataset, LogSelector, QueryFinality, TopicFilter,
};
use datalens_evm::mock::{ItemOutcome, MockRpc};
use datalens_server::config::Config;
use serde_json::{Value, json};
use sqlx::PgPool;

const LATEST: u64 = 20_000;
const COVERED: u64 = 9_999;
const WALL: Duration = Duration::from_secs(30);
const PRODUCTION_TICK_MS: f64 = 2_000.0;
const TEST_TICK_MS: f64 = 100.0;

struct ChainRpc {
    name: &'static str,
    id: u64,
    safe_lag: u64,
    finalized_lag: u64,
    max_logs: u64,
    rpc: MockRpc,
}

#[sqlx::test(migrations = false)]
async fn arbitrum_steady_rate_and_covered_query_p95(pool: PgPool) {
    let chains = spawn_chains().await;
    let logs = Arc::new(covered_logs());
    let eth = chains
        .iter()
        .find(|chain| chain.name == "ethereum")
        .unwrap();
    let logs_for_handler = Arc::clone(&logs);
    eth.rpc
        .set_handler(move |item| ethereum_handler(item, &logs_for_handler));
    for chain in chains.iter().filter(|chain| chain.name != "ethereum") {
        chain.rpc.set_handler(head_handler);
    }

    let config = Config::from_toml(&config_text(&chains), &|_| None).unwrap();
    let server = boot(pool, config).await;
    let client = server.client("degov-live", "degov-live-token");
    let arbitrum = ChainName::new("arbitrum").unwrap();
    client
        .watch(&WatchRequest {
            chain: arbitrum.clone(),
            dataset: Dataset::EvmLogs,
            selector: Some(contract_selector()),
        })
        .await
        .unwrap();
    wait_until_caught_up(&client).await;

    let arb = chains
        .iter()
        .find(|chain| chain.name == "arbitrum")
        .unwrap();
    let before = arb.rpc.calls().len();
    tokio::time::sleep(WALL).await;
    let calls = arb.rpc.calls().len() - before;
    let simulated_seconds = WALL.as_secs_f64() * (PRODUCTION_TICK_MS / TEST_TICK_MS);
    let steady_rps = calls as f64 / simulated_seconds;

    let p95_ms = covered_query_p95(&client).await;
    println!("arbitrum_calls={calls}");
    println!("arbitrum_simulated_seconds={simulated_seconds:.0}");
    println!("arbitrum_steady_rps={steady_rps:.2}");
    println!("covered_query_p95_ms={p95_ms:.2}");
    assert!(
        steady_rps <= 5.0,
        "arbitrum steady {steady_rps:.2} req/s exceeds 5"
    );
    // Debug builds spend the budget in unoptimized JSON encoding. The acceptance
    // number is the release server, which `loadtest/run.sh` measures.
    if !cfg!(debug_assertions) {
        assert!(
            p95_ms < 100.0,
            "covered 10k-block query p95 {p95_ms:.2} ms is not under 100"
        );
    }
    server.stop().await;
}

async fn wait_until_caught_up(client: &Client) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        assert!(
            Instant::now() < deadline,
            "arbitrum follow did not catch up"
        );
        let text = client.metrics("metrics-secret").await.unwrap();
        if text.lines().any(|line| {
            line.starts_with("datalens_follow_lag_blocks{")
                && line.contains("chain=\"arbitrum\"")
                && line.ends_with(" 0")
        }) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn covered_query_p95(client: &Client) -> f64 {
    let request = QueryRequest {
        chain: ChainName::new("ethereum").unwrap(),
        dataset: Dataset::EvmLogs,
        selector: Some(contract_selector()),
        range: BlockRange::new(0, COVERED).unwrap(),
        finality: QueryFinality::DurableOnly,
        fields: None,
        page: None,
        wait_ms: Some(30_000),
    };
    let backfill = client
        .backfill(&BackfillRequest {
            chain: request.chain.clone(),
            dataset: Dataset::EvmLogs,
            selector: request.selector.clone(),
            range: request.range,
        })
        .await
        .unwrap();
    assert_eq!(backfill.jobs.len(), 1, "{backfill:?}");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        assert!(Instant::now() < deadline, "covered fill did not finish");
        let jobs = client.jobs(Some("done")).await.unwrap();
        if jobs.jobs.iter().any(|job| job.id == backfill.jobs[0]) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let warm = client.query(&request).await.unwrap();
    assert_eq!(warm.rows.len(), 10_000);
    assert!(warm.incomplete_reason.is_none());

    let mut samples = Vec::with_capacity(20);
    for _ in 0..20 {
        let started = Instant::now();
        let response = client.query(&request).await.unwrap();
        assert_eq!(response.rows.len(), 10_000);
        samples.push(started.elapsed().as_secs_f64() * 1_000.0);
    }
    samples.sort_by(|left, right| left.partial_cmp(right).unwrap());
    println!(
        "covered_query_min_ms={:.2} covered_query_p50_ms={:.2}",
        samples[0], samples[10]
    );
    samples[18]
}

fn contract_selector() -> LogSelector {
    let mut topics: [TopicFilter; 4] = Default::default();
    topics[0] = TopicFilter::one_of([B256::repeat_byte(0x11)]).unwrap();
    LogSelector {
        addresses: [Address::repeat_byte(0xa1)].into_iter().collect(),
        topics,
    }
}

fn covered_logs() -> Vec<Value> {
    (0..=COVERED)
        .map(|block| {
            json!({
                "blockNumber": hex(block),
                "logIndex": "0x0",
                "blockHash": hash(block),
                "transactionIndex": "0x0",
                "transactionHash": hash(1_000_000 + block),
                "address": format!("{:#x}", Address::repeat_byte(0xa1)),
                "topics": [format!("{:#x}", B256::repeat_byte(0x11))],
                "data": "0x",
                "removed": false
            })
        })
        .collect()
}

fn ethereum_handler(item: &Value, logs: &[Value]) -> ItemOutcome {
    let params = &item["params"];
    match item["method"].as_str().unwrap_or_default() {
        "eth_getLogs" => {
            let filter = &params[0];
            let from = parse_hex(&filter["fromBlock"]);
            let to = parse_hex(&filter["toBlock"]);
            let start = from;
            let end = to.min(COVERED);
            let rows = if start > end {
                Vec::new()
            } else {
                logs[start as usize..=end as usize].to_vec()
            };
            ItemOutcome::Result(Value::Array(rows))
        }
        "eth_getBlockByNumber" => ItemOutcome::Result(block_json(block_number(&params[0]))),
        _ => ItemOutcome::Result(Value::Null),
    }
}

fn head_handler(item: &Value) -> ItemOutcome {
    let params = &item["params"];
    match item["method"].as_str().unwrap_or_default() {
        "eth_getBlockByNumber" => ItemOutcome::Result(block_json(block_number(&params[0]))),
        "eth_getLogs" => ItemOutcome::Result(Value::Array(Vec::new())),
        _ => ItemOutcome::Result(Value::Null),
    }
}

fn block_number(param: &Value) -> u64 {
    match param.as_str() {
        Some("latest") | Some("safe") | Some("finalized") => LATEST,
        _ => parse_hex(param),
    }
}

fn block_json(number: u64) -> Value {
    json!({
        "number": hex(number.min(LATEST)),
        "hash": hash(number.min(LATEST)),
        "parentHash": hash(number.min(LATEST).saturating_sub(1)),
        "timestamp": hex(1_700_000_000 + number.min(LATEST)),
    })
}

fn hash(number: u64) -> String {
    format!("{:#x}", B256::left_padding_from(&number.to_be_bytes()))
}

fn hex(number: u64) -> String {
    format!("0x{number:x}")
}

fn parse_hex(value: &Value) -> u64 {
    u64::from_str_radix(value.as_str().unwrap().trim_start_matches("0x"), 16).unwrap()
}

async fn spawn_chains() -> Vec<ChainRpc> {
    let spec = [
        ("ethereum", 1, 32, 96, 10_000),
        ("darwinia", 46, 64, 256, 1_000),
        ("base", 8453, 64, 256, 10_000),
        ("polygon", 137, 128, 512, 10_000),
        ("lisk", 1135, 64, 256, 10_000),
        ("optimism", 10, 64, 256, 10_000),
        ("bsc", 56, 128, 512, 10_000),
        ("arbitrum", 42161, 64, 256, 10_000),
    ];
    let mut chains = Vec::with_capacity(spec.len());
    for (name, id, safe_lag, finalized_lag, max_logs) in spec {
        chains.push(ChainRpc {
            name,
            id,
            safe_lag,
            finalized_lag,
            max_logs,
            rpc: MockRpc::spawn().await,
        });
    }
    chains
}

fn config_text(chains: &[ChainRpc]) -> String {
    let mut text = String::from(
        r#"
[server]
bind = "127.0.0.1:0"
roles = ["api", "worker"]

[database]
url = "postgres://unused"

[fill]
workers = 2

[metrics]
bearer_token = "metrics-secret"
"#,
    );
    for chain in chains {
        text.push_str(&format!(
            r#"
[chains.{name}]
chain_id = {id}
rpc_url = "{url}"
finality = {{ mode = "lag", safe_lag_blocks = {safe}, finalized_lag_blocks = {finalized} }}
tick_ms = 100
chunk_blocks = 10000
max_get_logs_range_blocks = {max_logs}
max_addresses_per_query = 32
provider = {{ rps = 1000, burst = 1000, max_in_flight = 8 }}
"#,
            name = chain.name,
            id = chain.id,
            url = chain.rpc.url(),
            safe = chain.safe_lag,
            finalized = chain.finalized_lag,
            max_logs = chain.max_logs,
        ));
    }
    text.push_str(
        r#"
[[applications]]
id = "degov-live"
token = "degov-live-token"
chains = ["ethereum", "darwinia", "base", "polygon", "lisk", "optimism", "bsc", "arbitrum"]
datasets = ["evm.logs"]
quota = { max_range_blocks = 20000, max_requests_per_minute = 60000, max_concurrent_requests = 8, max_subscriptions = 8 }
admin = true

[[applications]]
id = "degov-staging"
token = "degov-staging-token"
chains = ["ethereum", "darwinia", "base", "polygon", "lisk", "optimism", "bsc", "arbitrum"]
datasets = ["evm.logs"]
quota = { max_range_blocks = 20000, max_requests_per_minute = 60000, max_concurrent_requests = 8, max_subscriptions = 8 }

[[applications]]
id = "degov-realtime"
token = "degov-realtime-token"
chains = ["ethereum", "darwinia", "base", "polygon", "lisk", "optimism", "bsc", "arbitrum"]
datasets = ["evm.logs"]
quota = { max_range_blocks = 20000, max_requests_per_minute = 60000, max_concurrent_requests = 8, max_subscriptions = 8 }

[[applications]]
id = "ormp"
token = "ormp-token"
chains = ["ethereum", "darwinia", "base", "polygon", "lisk", "optimism", "bsc", "arbitrum"]
datasets = ["evm.logs"]
quota = { max_range_blocks = 20000, max_requests_per_minute = 60000, max_concurrent_requests = 8, max_subscriptions = 8 }
"#,
    );
    text
}
