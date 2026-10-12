#[path = "common/mod.rs"]
mod common;

use std::time::{Duration, Instant};

use datalens_core::wire::{
    Anchor, ChainHeads, CoverageSpan, FollowInfo, IncompleteReason, LogRecord, Segment,
    SegmentSource,
};
use datalens_core::{
    BlockRange, ChainName, Dataset, Error, Finality, Heads, LogRow, QueryFinality,
};
use datalens_engine::{Caller, EngineSettings, QueryEngine};
use datalens_store::HeadKind;
use prometheus::Registry;
use serde_json::Value;
use sqlx::PgPool;
use tokio::task::JoinSet;

use common::follow_chain::{
    FollowChain, any_transfer, follow_chain, follow_engine, follow_settings, metrics_text, request,
    usdc,
};
use common::{caller, chain_id, range, spawn_workers};

const START: u64 = 10_000;

fn records(rows: Vec<LogRow>, finality: Finality) -> Vec<Value> {
    rows.into_iter()
        .map(|row| {
            let mut record = LogRecord::from(row);
            record.finality = finality;
            serde_json::to_value(record).unwrap()
        })
        .collect()
}

fn hot(start: u64, end: u64, anchor: u64, mock: &FollowChain) -> Segment {
    Segment {
        start,
        end,
        source: SegmentSource::Hot,
        finality: Finality::Unsafe,
        anchor: Some(Anchor {
            number: anchor,
            hash: mock.hash(anchor),
        }),
    }
}

fn durable(start: u64, end: u64, finality: Finality) -> Segment {
    Segment {
        start,
        end,
        source: SegmentSource::Durable,
        finality,
        anchor: None,
    }
}

async fn setup(pool: &PgPool, settings: EngineSettings) -> (FollowChain, QueryEngine, Registry) {
    let mock = FollowChain::spawn(START).await;
    let registry = Registry::new();
    let engine = follow_engine(
        pool,
        vec![follow_chain("ethereum", 1, mock.rpc.url())],
        settings,
        &registry,
    )
    .await;
    (mock, engine, registry)
}

fn ethereum() -> ChainName {
    ChainName::new("ethereum").unwrap()
}

#[sqlx::test(migrations = false)]
async fn latest_only_serves_hot_rows_and_a_hundred_clients_cost_what_one_does(pool: PgPool) {
    let (mock, engine, registry) = setup(&pool, follow_settings()).await;
    let hot_range = range(9_993, START);
    let ask = request("ethereum", usdc(), hot_range, QueryFinality::LatestOnly);

    let first = engine.query(&caller(), ask.clone()).await.unwrap();
    assert_eq!(first.served, None);
    assert_eq!(first.next_start, Some(9_993));
    assert_eq!(
        first.incomplete_reason,
        Some(IncompleteReason::NotFollowedYet)
    );
    assert_eq!(mock.requests(), 1);

    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(mock.count("eth_getLogs"), 1);
    let after_tick = mock.requests();
    assert_eq!(after_tick, 3);

    let mut clients = JoinSet::new();
    for _ in 0..100 {
        let engine = engine.clone();
        let ask = ask.clone();
        clients.spawn(async move { engine.query(&caller(), ask).await.unwrap() });
    }
    let want_rows = records(mock.logs(&usdc(), hot_range), Finality::Unsafe);
    assert_eq!(want_rows.len(), 8);
    while let Some(response) = clients.join_next().await {
        let response = response.unwrap();
        assert_eq!(response.served, Some(hot_range));
        assert_eq!(response.next_start, None);
        assert_eq!(response.incomplete_reason, None);
        assert_eq!(response.segments, vec![hot(9_993, START, START, &mock)]);
        assert_eq!(response.rows, want_rows);
    }
    assert_eq!(mock.requests(), after_tick);
    assert!(
        metrics_text(&registry)
            .contains("datalens_subscriptions{chain=\"ethereum\",dataset=\"evm.logs\"} 1")
    );
}

#[sqlx::test(migrations = false)]
async fn safe_to_latest_joins_durable_and_hot_segments(pool: PgPool) {
    let (mock, engine, _registry) = setup(&pool, follow_settings()).await;
    let _workers = spawn_workers(&engine, 1);
    let mut ask = request(
        "ethereum",
        usdc(),
        range(9_990, START),
        QueryFinality::SafeToLatest,
    );
    ask.wait_ms = None;
    engine.query(&caller(), ask.clone()).await.unwrap();
    engine.tick_once(&ethereum()).await.unwrap();

    let response = engine.query(&caller(), ask).await.unwrap();
    assert_eq!(response.served, Some(range(9_990, START)));
    assert_eq!(response.next_start, None);
    assert_eq!(
        response.segments,
        vec![
            durable(9_990, 9_992, Finality::Safe),
            hot(9_993, START, START, &mock),
        ]
    );
    let mut want = records(mock.logs(&usdc(), range(9_990, 9_992)), Finality::Safe);
    want.extend(records(
        mock.logs(&usdc(), range(9_993, START)),
        Finality::Unsafe,
    ));
    assert_eq!(response.rows, want);
}

#[sqlx::test(migrations = false)]
async fn safe_advancing_promotes_followed_rows_without_refetching(pool: PgPool) {
    let (mock, engine, _registry) = setup(&pool, follow_settings()).await;
    let ask = request(
        "ethereum",
        usdc(),
        range(9_993, START),
        QueryFinality::LatestOnly,
    );
    engine.query(&caller(), ask).await.unwrap();
    engine.tick_once(&ethereum()).await.unwrap();

    mock.advance(8);
    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(mock.count("eth_getLogs"), 2);
    let last_get_logs = mock
        .rpc
        .calls()
        .into_iter()
        .flat_map(|body| match body {
            Value::Array(items) => items,
            other => vec![other],
        })
        .rfind(|item| item["method"] == "eth_getLogs")
        .unwrap();
    assert_eq!(last_get_logs["params"][0]["fromBlock"], "0x2711");

    let coverage = engine
        .coverage(
            &ethereum(),
            Dataset::EvmLogs,
            Some(usdc()),
            Some(range(9_993, 10_008)),
        )
        .await
        .unwrap();
    assert_eq!(
        coverage.spans,
        vec![CoverageSpan {
            start: 9_993,
            end: START,
            finality: Finality::Safe,
        }]
    );
    assert_eq!(
        coverage.follow,
        Some(FollowInfo {
            from_block: 9_993,
            head_block: Some(10_008),
            head_hash: Some(mock.hash(10_008)),
            promoted_to: Some(START),
        })
    );

    let before = mock.requests();
    let durable_ask = request(
        "ethereum",
        usdc(),
        range(9_993, START),
        QueryFinality::DurableOnly,
    );
    let response = engine.query(&caller(), durable_ask).await.unwrap();
    assert_eq!(response.served, Some(range(9_993, START)));
    assert_eq!(
        response.segments,
        vec![durable(9_993, START, Finality::Safe)]
    );
    assert_eq!(
        response.rows,
        records(mock.logs(&usdc(), range(9_993, START)), Finality::Safe)
    );
    assert_eq!(mock.requests(), before);
}

#[sqlx::test(migrations = false)]
async fn durable_only_chasing_safe_waits_for_promotion_instead_of_filling(pool: PgPool) {
    let (mock, engine, _registry) = setup(&pool, follow_settings()).await;
    let _workers = spawn_workers(&engine, 1);
    let mut first = request(
        "ethereum",
        usdc(),
        range(9_900, 9_992),
        QueryFinality::DurableOnly,
    );
    first.wait_ms = None;
    let response = engine.query(&caller(), first).await.unwrap();
    assert_eq!(response.served, Some(range(9_900, 9_992)));
    assert_eq!(mock.count("eth_getLogs"), 1);
    let jobs = engine.jobs(None, 100).await.unwrap().jobs;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].range, range(9_000, 9_992));

    mock.advance(8);
    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(mock.count("eth_getLogs"), 2);

    // Safe is 10_000 now but promotion runs one tick behind the fetch.
    let mut chase = request(
        "ethereum",
        usdc(),
        range(9_993, START),
        QueryFinality::DurableOnly,
    );
    chase.wait_ms = Some(5_000);
    let waiting = tokio::spawn({
        let engine = engine.clone();
        async move { engine.query(&caller(), chase).await.unwrap() }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!waiting.is_finished());
    mock.advance(1);
    engine.tick_once(&ethereum()).await.unwrap();
    let response = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(response.served, Some(range(9_993, START)));
    assert_eq!(response.incomplete_reason, None);
    // Finalized is 9_993 now, so that block is promoted straight to finalized.
    let mut expected = records(mock.logs(&usdc(), range(9_993, 9_993)), Finality::Finalized);
    expected.extend(records(
        mock.logs(&usdc(), range(9_994, START)),
        Finality::Safe,
    ));
    assert_eq!(response.rows, expected);

    let before = mock.requests();
    let whole = request(
        "ethereum",
        usdc(),
        range(9_000, START),
        QueryFinality::DurableOnly,
    );
    let response = engine.query(&caller(), whole).await.unwrap();
    assert_eq!(response.served, Some(range(9_000, START)));
    assert_eq!(mock.requests(), before);
    assert_eq!(mock.count("eth_getLogs"), 3);
    assert_eq!(engine.jobs(None, 100).await.unwrap().jobs.len(), 1);
}

#[sqlx::test(migrations = false)]
async fn heads_wait_returns_on_change_and_on_timeout(pool: PgPool) {
    let (mock, engine, _registry) = setup(&pool, follow_settings()).await;
    let waiting = tokio::spawn({
        let engine = engine.clone();
        async move {
            let started = Instant::now();
            let heads = engine
                .wait_heads(&ethereum(), START, HeadKind::Latest, Duration::from_secs(5))
                .await
                .unwrap();
            (heads, started.elapsed())
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    mock.advance(2);
    engine.tick_once(&ethereum()).await.unwrap();
    let (heads, waited) = waiting.await.unwrap();
    let now = ChainHeads::new(
        ethereum(),
        Heads {
            latest: 10_002,
            latest_hash: mock.hash(10_002),
            safe: 9_994,
            finalized: 9_986,
        },
    );
    assert_eq!(heads, now);
    assert!(waited < Duration::from_secs(2), "{waited:?}");

    let started = Instant::now();
    let heads = engine
        .wait_heads(
            &ethereum(),
            10_002,
            HeadKind::Latest,
            Duration::from_millis(200),
        )
        .await
        .unwrap();
    let waited = started.elapsed();
    assert_eq!(heads, now);
    assert!(waited >= Duration::from_millis(200), "{waited:?}");
    assert!(waited < Duration::from_secs(2), "{waited:?}");
}

#[sqlx::test(migrations = false)]
async fn expired_subscriptions_stop_follow_and_retention_drops_their_unsafe_rows(pool: PgPool) {
    let settings = EngineSettings {
        subscription_ttl: Duration::from_millis(500),
        unsafe_retention: Duration::ZERO,
        ..follow_settings()
    };
    let (mock, engine, registry) = setup(&pool, settings).await;
    let ask = request(
        "ethereum",
        usdc(),
        range(9_993, START),
        QueryFinality::LatestOnly,
    );
    engine.query(&caller(), ask).await.unwrap();
    engine.tick_once(&ethereum()).await.unwrap();
    let store = engine.store();
    assert_eq!(
        store
            .unsafe_hashes(chain_id(), range(0, 20_000))
            .await
            .unwrap()
            .len(),
        8
    );

    tokio::time::sleep(Duration::from_millis(700)).await;
    mock.advance(8);
    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(mock.count("eth_getLogs"), 1);
    assert!(store.list_follow(chain_id()).await.unwrap().is_empty());
    assert_eq!(
        store
            .unsafe_hashes(chain_id(), range(0, 20_000))
            .await
            .unwrap(),
        vec![]
    );
    assert!(
        metrics_text(&registry)
            .contains("datalens_subscriptions{chain=\"ethereum\",dataset=\"evm.logs\"} 0")
    );
}

#[sqlx::test(migrations = false)]
async fn max_subscriptions_rejects_a_new_head_selector(pool: PgPool) {
    let (_mock, engine, _registry) = setup(&pool, follow_settings()).await;
    let capped = Caller {
        application: "degov",
        fill_budget: None,
        max_subscriptions: Some(1),
    };
    let hot_range: BlockRange = range(9_993, START);
    engine
        .query(
            &capped,
            request("ethereum", usdc(), hot_range, QueryFinality::LatestOnly),
        )
        .await
        .unwrap();
    engine
        .query(
            &capped,
            request("ethereum", usdc(), hot_range, QueryFinality::LatestOnly),
        )
        .await
        .unwrap();
    let error = engine
        .query(
            &capped,
            request(
                "ethereum",
                any_transfer(),
                hot_range,
                QueryFinality::LatestOnly,
            ),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, Error::QuotaExceeded(_)), "{error:?}");
}
