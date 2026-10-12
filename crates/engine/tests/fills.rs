#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use datalens_core::B256;
use datalens_core::wire::IncompleteReason;
use datalens_engine::EngineSettings;
use datalens_store::JobStatus;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::task::JoinSet;

use common::{
    MockChain, caller, chain, chain_id, chain_with_rps, engine, engine_with, expected, fixture,
    logs_request, range, selector, settings, spawn_workers,
};

#[sqlx::test(migrations = false)]
async fn gaps_in_one_cell_plan_that_whole_cell_ended_at_safe(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    for blocks in [
        range(91_000, 91_500),
        range(95_000, 98_999),
        range(93_000, 99_000),
    ] {
        let mut request = logs_request(selector(&[0xa1], &[]), blocks);
        request.wait_ms = Some(0);
        engine.query(&caller(), request).await.unwrap();
    }
    let jobs = engine.jobs(None, 10).await.unwrap();
    assert_eq!(
        jobs.jobs.iter().map(|job| job.range).collect::<Vec<_>>(),
        vec![range(90_000, 99_000)]
    );
}

#[sqlx::test(migrations = false)]
async fn a_stored_row_on_another_fork_fails_the_fill_retryably_without_coverage(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let mut forked = fixture()[0].clone();
    forked.block_hash = B256::repeat_byte(0xee);
    let mut tx = pool.begin().await.unwrap();
    engine
        .store()
        .insert_logs(&mut tx, chain_id(), &[forked])
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut request = logs_request(selector(&[0xa1], &[]), range(0, 9_999));
    request.wait_ms = Some(0);
    engine.query(&caller(), request.clone()).await.unwrap();
    assert!(engine.fill_once("w").await.unwrap());
    assert_eq!(mock.count("eth_getLogs"), 1);

    let response = engine.query(&caller(), request).await.unwrap();
    assert_eq!(response.served, None);
    assert_eq!(response.next_start, Some(0));
    assert_eq!(response.incomplete_reason, Some(IncompleteReason::Pending));
    assert_eq!(response.incomplete_detail.as_deref(), Some("reorg"));
    let jobs = engine.jobs(Some(JobStatus::Pending), 10).await.unwrap();
    assert_eq!(jobs.jobs.len(), 1);
    assert_eq!(jobs.jobs[0].attempts, 1);
}

#[sqlx::test(migrations = false)]
async fn a_throttled_fill_reports_budget_exhausted(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine_with(
        &pool,
        chain_with_rps(mock.rpc.url(), Duration::from_millis(100), 1),
        EngineSettings {
            token_wait: Duration::ZERO,
            ..settings()
        },
    )
    .await;
    let wanted = selector(&[0xa1], &[]);
    let mut request = logs_request(wanted.clone(), range(0, 19_999));
    request.wait_ms = Some(0);
    engine.query(&caller(), request.clone()).await.unwrap();
    assert!(engine.fill_once("w").await.unwrap());
    assert!(engine.fill_once("w").await.unwrap());
    assert_eq!(mock.count("eth_getLogs"), 1);

    let response = engine.query(&caller(), request).await.unwrap();
    assert_eq!(response.served, Some(range(0, 9_999)));
    assert_eq!(response.next_start, Some(10_000));
    assert_eq!(
        response.incomplete_reason,
        Some(IncompleteReason::BudgetExhausted)
    );
    assert_eq!(response.incomplete_detail.as_deref(), Some("throttled"));
    assert_eq!(response.rows, expected(&wanted, range(0, 9_999)));
}

#[sqlx::test(migrations = false)]
async fn a_fill_slower_than_its_lease_is_fetched_once(pool: PgPool) {
    let mock = MockChain::spawn_slow(Duration::from_millis(1_500));
    let engine = engine_with(
        &pool,
        chain(mock.rpc.url(), Duration::from_millis(100)),
        EngineSettings {
            lease: Duration::from_millis(300),
            ..settings()
        },
    )
    .await;
    let _workers = spawn_workers(&engine, 3);
    let wanted = selector(&[0xa1], &[]);

    let response = engine
        .query(&caller(), logs_request(wanted.clone(), range(0, 9_999)))
        .await
        .unwrap();
    assert_eq!(response.served, Some(range(0, 9_999)));
    assert_eq!(response.incomplete_reason, None);
    assert_eq!(response.rows, expected(&wanted, range(0, 9_999)));
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(mock.count("eth_getLogs"), 1);
}

#[sqlx::test(migrations = false)]
async fn fifty_waiting_queries_complete_on_eight_connections(
    pool_options: PgPoolOptions,
    connect: PgConnectOptions,
) {
    let pool = pool_options
        .max_connections(8)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(connect)
        .await
        .unwrap();
    let mock = MockChain::spawn_slow(Duration::from_millis(500));
    let engine = engine(&pool, &mock).await;
    let _workers = spawn_workers(&engine, 2);

    let mut queries = JoinSet::new();
    for byte in 1..=50_u8 {
        let engine = engine.clone();
        queries.spawn(async move {
            engine
                .query(
                    &caller(),
                    logs_request(selector(&[byte], &[]), range(0, 9_999)),
                )
                .await
        });
    }
    let mut complete = 0;
    while let Some(joined) = queries.join_next().await {
        let response = joined.unwrap().unwrap();
        assert_eq!(response.served, Some(range(0, 9_999)));
        assert_eq!(response.incomplete_reason, None);
        complete += 1;
    }
    assert_eq!(complete, 50);
}
