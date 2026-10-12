#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use chrono::Utc;
use datalens_core::wire::{BackfillRequest, CoverageSpan, IncompleteReason};
use datalens_core::{Dataset, Finality, Heads};
use datalens_engine::{Caller, FillBudget};
use datalens_store::{JobStatus, UsageTotals};
use sqlx::PgPool;
use tokio::sync::watch;

use common::{
    LATEST, MockChain, block_hash, caller, chain_id, chain_name, engine, logs_request, range,
    selector, spawn_workers,
};

#[sqlx::test(migrations = false)]
async fn head_tracker_writes_lag_heads_from_one_latest_call(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let heads = engine.tick_once(&chain_name()).await.unwrap();
    let want = Heads {
        latest: LATEST,
        latest_hash: block_hash(LATEST),
        safe: LATEST - 64,
        finalized: LATEST - 1_064,
    };
    assert_eq!(heads, want);
    assert_eq!(engine.store().heads(chain_id()).await.unwrap(), Some(want));
    assert_eq!(mock.count("eth_getBlockByNumber"), 1);
}

#[sqlx::test(migrations = false)]
async fn head_tracker_waits_while_another_replica_holds_the_chain_lock(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let mut holder = pool.acquire().await.unwrap();
    sqlx::query("select pg_advisory_lock(1)")
        .execute(&mut *holder)
        .await
        .unwrap();

    let (stop, rx) = watch::channel(false);
    let tracker = tokio::spawn({
        let engine = engine.clone();
        async move { engine.run_chain_leader(chain_name(), rx).await }
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(mock.count("eth_getBlockByNumber"), 0);

    sqlx::query("select pg_advisory_unlock(1)")
        .execute(&mut *holder)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(mock.count("eth_getBlockByNumber") >= 1);
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), tracker)
        .await
        .unwrap()
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn usage_is_flushed_per_application(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let _workers = spawn_workers(&engine, 1);
    for _ in 0..2 {
        engine
            .query(
                &caller(),
                logs_request(selector(&[0xa1], &[]), range(0, 9_999)),
            )
            .await
            .unwrap();
    }
    engine.usage().flush(engine.store()).await.unwrap();
    let totals = engine
        .store()
        .usage(
            Utc::now().date_naive(),
            "degov",
            chain_id(),
            Dataset::EvmLogs,
        )
        .await
        .unwrap();
    assert_eq!(
        totals,
        UsageTotals {
            queries: 2,
            rows_returned: 14,
            blocks_served: 20_000,
            blocks_filled: 10_000,
            provider_calls: 1,
        }
    );
}

#[sqlx::test(migrations = false)]
async fn backfill_enqueues_grid_chunks_and_admin_reads_report_them(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let wanted = selector(&[0xa2], &[]);
    let backfill = engine
        .backfill(
            "ops",
            BackfillRequest {
                chain: chain_name(),
                dataset: Dataset::EvmLogs,
                selector: Some(wanted.clone()),
                range: range(5_000, 25_000),
            },
        )
        .await
        .unwrap();
    assert_eq!(backfill.jobs.len(), 3);
    let pending = engine.jobs(Some(JobStatus::Pending), 10).await.unwrap();
    assert_eq!(pending.jobs.len(), 3);
    assert!(pending.jobs.iter().all(|job| job.purpose == "backfill"));

    while engine.fill_once("backfiller").await.unwrap() {}
    assert_eq!(mock.count("eth_getLogs"), 3);
    let coverage = engine
        .coverage(
            &chain_name(),
            Dataset::EvmLogs,
            Some(wanted),
            Some(range(0, 40_000)),
        )
        .await
        .unwrap();
    assert_eq!(coverage.selector_id, backfill.selector_id);
    assert_eq!(
        coverage.spans,
        vec![CoverageSpan {
            start: 0,
            end: 29_999,
            finality: Finality::Finalized,
        }]
    );
    assert_eq!(coverage.covered, vec![range(0, 29_999)]);
    assert_eq!(coverage.missing, vec![range(30_000, 40_000)]);
    assert_eq!(
        engine
            .jobs(Some(JobStatus::Done), 10)
            .await
            .unwrap()
            .jobs
            .len(),
        3
    );
}

#[sqlx::test(migrations = false)]
async fn fill_budget_stops_enqueuing_and_reports_budget_exhausted(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let _workers = spawn_workers(&engine, 1);
    let budget = FillBudget::new(15_000);
    let caller = Caller {
        application: "degov",
        fill_budget: Some(&budget),
        max_subscriptions: None,
    };
    let response = engine
        .query(
            &caller,
            logs_request(selector(&[0xa1], &[]), range(0, 29_999)),
        )
        .await
        .unwrap();
    assert_eq!(response.served, Some(range(0, 9_999)));
    assert_eq!(response.next_start, Some(10_000));
    assert_eq!(
        response.incomplete_reason,
        Some(IncompleteReason::BudgetExhausted)
    );
    assert_eq!(mock.count("eth_getLogs"), 1);
}

#[sqlx::test(migrations = false)]
async fn a_poll_of_an_in_flight_fill_waits_on_it_without_spending_budget(pool: PgPool) {
    let mock = MockChain::spawn_slow(Duration::from_millis(500));
    let engine = engine(&pool, &mock).await;
    let budget = FillBudget::new(15_000);
    let caller = Caller {
        application: "degov",
        fill_budget: Some(&budget),
        max_subscriptions: None,
    };
    let mut request = logs_request(selector(&[0xa1], &[]), range(0, 9_999));
    request.wait_ms = Some(0);
    let first = engine.query(&caller, request.clone()).await.unwrap();
    assert_eq!(first.incomplete_reason, Some(IncompleteReason::Pending));

    let _workers = spawn_workers(&engine, 1);
    request.wait_ms = None;
    let second = engine.query(&caller, request).await.unwrap();
    assert_eq!(second.served, Some(range(0, 9_999)));
    assert_eq!(second.incomplete_reason, None);
    assert!(budget.try_take(5_000));
    assert_eq!(mock.count("eth_getLogs"), 1);
}
