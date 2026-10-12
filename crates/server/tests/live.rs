mod common;

use common::*;
use std::time::{Duration, Instant};

use datalens_core::wire::{IncompleteReason, LogRecord, SegmentSource};
use datalens_core::{Address, B256, Finality, LogSelector, QueryFinality, TopicFilter};
use datalens_server::config::Config;
use sqlx::PgPool;

const RPC_URL: &str = "https://ethereum-rpc.publicnode.com";
const USDC: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

/// Provider calls made for queries, summed from `/metrics`. Chain leader calls (heads, follow,
/// reorg checks) run on their own schedule and are left out.
fn query_calls(metrics: &str) -> u64 {
    metrics
        .lines()
        .filter(|line| line.starts_with("datalens_provider_calls_total{"))
        .filter(|line| line.contains(r#"purpose="query_fill""#))
        .map(|line| line.rsplit(' ').next().unwrap().parse::<u64>().unwrap())
        .sum()
}

fn live_config() -> Config {
    let text = config_text(RPC_URL)
        .replace(
            r#"finality = { mode = "lag", safe_lag_blocks = 64, finalized_lag_blocks = 1064 }"#,
            r#"finality = { mode = "rpc_tags" }"#,
        )
        .replace("tick_ms = 100", "tick_ms = 4000")
        .replace("chunk_blocks = 10000", "chunk_blocks = 10")
        .replace("rps = 1000, burst = 1000", "rps = 5, burst = 5");
    Config::from_toml(&text, &|_| None).unwrap()
}

fn usdc_transfers() -> LogSelector {
    let mut topics: [TopicFilter; 4] = Default::default();
    topics[0] = TopicFilter::one_of([TRANSFER.parse::<B256>().unwrap()]).unwrap();
    LogSelector {
        addresses: [USDC.parse::<Address>().unwrap()].into(),
        topics,
    }
}

#[ignore = "live: calls ethereum-rpc.publicnode.com"]
#[sqlx::test(migrations = false)]
async fn live_usdc_transfers_fill_once_then_serve_from_postgres(pool: PgPool) {
    let server = boot(pool, live_config()).await;
    let client = server.degov();

    let safe = client.chain_heads(&ethereum()).await.unwrap().safe;
    let start = (safe - 500) / 10 * 10;
    let request = logs_request(ethereum(), usdc_transfers(), range(start, start + 9));

    let cold = client.query(&request).await.unwrap();
    assert_eq!(cold.served, Some(range(start, start + 9)));
    assert_eq!(cold.incomplete_reason, None);
    assert!(!cold.rows.is_empty(), "USDC moves every few blocks");
    for row in &cold.rows {
        let log: LogRecord = serde_json::from_value(row.clone()).unwrap();
        assert_eq!(format!("{:#x}", log.address), USDC);
        assert!((start..=start + 9).contains(&log.block_number));
    }
    let after_cold = query_calls(&client.metrics(METRICS_TOKEN).await.unwrap());
    assert!(after_cold >= 1);

    let warm = client.query(&request).await.unwrap();
    assert_eq!(warm.rows, cold.rows);
    let after_warm = query_calls(&client.metrics(METRICS_TOKEN).await.unwrap());
    assert_eq!(after_warm, after_cold);
    println!(
        "live smoke: blocks {start}..={} rows {} provider calls {after_cold}",
        start + 9,
        cold.rows.len()
    );
    server.stop().await;
}

#[ignore = "live: calls ethereum-rpc.publicnode.com"]
#[sqlx::test(migrations = false)]
async fn live_latest_only_usdc_transfers_come_from_the_follower(pool: PgPool) {
    let server = boot(pool, live_config()).await;
    let client = server.degov();
    let heads = client.chain_heads(&ethereum()).await.unwrap();
    let mut request = logs_request(
        ethereum(),
        usdc_transfers(),
        range(heads.safe + 1, heads.latest),
    );
    request.finality = QueryFinality::LatestOnly;
    request.wait_ms = Some(0);

    let deadline = Instant::now() + Duration::from_secs(60);
    let mut polls = 0;
    let response = loop {
        polls += 1;
        let response = client.query(&request).await.unwrap();
        if response.served.is_some() {
            break response;
        }
        assert_eq!(
            response.incomplete_reason,
            Some(IncompleteReason::NotFollowedYet)
        );
        assert!(Instant::now() < deadline, "follower never reached the head");
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    let served = response.served.unwrap();
    assert!(served.start() > heads.safe, "{served:?}");
    assert_eq!(served.end(), heads.latest);
    let segment = response.segments.last().unwrap();
    assert_eq!(segment.source, SegmentSource::Hot);
    assert_eq!(segment.finality, Finality::Unsafe);
    assert!(segment.anchor.unwrap().number >= served.end());
    assert!(!response.rows.is_empty(), "USDC moves every few blocks");
    for row in &response.rows {
        let log: LogRecord = serde_json::from_value(row.clone()).unwrap();
        assert_eq!(format!("{:#x}", log.address), USDC);
        assert_eq!(log.finality, Finality::Unsafe);
        assert!(served.contains(log.block_number));
    }
    assert_eq!(
        query_calls(&client.metrics(METRICS_TOKEN).await.unwrap()),
        0
    );
    println!(
        "live follow: blocks {}..={} rows {} after {polls} polls",
        served.start(),
        served.end(),
        response.rows.len()
    );
    server.stop().await;
}
