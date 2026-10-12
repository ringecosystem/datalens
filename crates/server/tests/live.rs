mod common;

use common::*;
use datalens_core::wire::LogRecord;
use datalens_core::{Address, B256, LogSelector, TopicFilter};
use datalens_server::config::Config;
use sqlx::PgPool;

const RPC_URL: &str = "https://ethereum-rpc.publicnode.com";
const USDC: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

/// Provider calls made for anything but head tracking, summed from `/metrics`.
fn non_head_calls(metrics: &str) -> u64 {
    metrics
        .lines()
        .filter(|line| line.starts_with("datalens_provider_calls_total{"))
        .filter(|line| !line.contains(r#"purpose="head""#))
        .map(|line| line.rsplit(' ').next().unwrap().parse::<u64>().unwrap())
        .sum()
}

#[ignore = "live: calls ethereum-rpc.publicnode.com"]
#[sqlx::test(migrations = false)]
async fn live_usdc_transfers_fill_once_then_serve_from_postgres(pool: PgPool) {
    let text = config_text(RPC_URL)
        .replace(
            r#"finality = { mode = "lag", safe_lag_blocks = 64, finalized_lag_blocks = 1064 }"#,
            r#"finality = { mode = "rpc_tags" }"#,
        )
        .replace("tick_ms = 100", "tick_ms = 4000")
        .replace("chunk_blocks = 10000", "chunk_blocks = 10")
        .replace("rps = 1000, burst = 1000", "rps = 5, burst = 5");
    let config = Config::from_toml(&text, &|_| None).unwrap();
    let server = boot(pool, config).await;
    let client = server.degov();

    let safe = client.chain_heads(&ethereum()).await.unwrap().safe;
    let start = (safe - 500) / 10 * 10;
    let mut topics: [TopicFilter; 4] = Default::default();
    topics[0] = TopicFilter::one_of([TRANSFER.parse::<B256>().unwrap()]).unwrap();
    let selector = LogSelector {
        addresses: [USDC.parse::<Address>().unwrap()].into(),
        topics,
    };
    let request = logs_request(ethereum(), selector, range(start, start + 9));

    let cold = client.query(&request).await.unwrap();
    assert_eq!(cold.served, Some(range(start, start + 9)));
    assert_eq!(cold.incomplete_reason, None);
    assert!(!cold.rows.is_empty(), "USDC moves every few blocks");
    for row in &cold.rows {
        let log: LogRecord = serde_json::from_value(row.clone()).unwrap();
        assert_eq!(format!("{:#x}", log.address), USDC);
        assert!((start..=start + 9).contains(&log.block_number));
    }
    let after_cold = non_head_calls(&client.metrics(METRICS_TOKEN).await.unwrap());
    assert!(after_cold >= 1);

    let warm = client.query(&request).await.unwrap();
    assert_eq!(warm.rows, cold.rows);
    let after_warm = non_head_calls(&client.metrics(METRICS_TOKEN).await.unwrap());
    assert_eq!(after_warm, after_cold);
    println!(
        "live smoke: blocks {start}..={} rows {} provider calls {after_cold}",
        start + 9,
        cold.rows.len()
    );
    server.stop().await;
}
