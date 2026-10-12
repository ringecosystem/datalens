#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use chrono::Utc;
use datalens_core::wire::{CoverageSpan, IncompleteReason};
use datalens_core::{B256, ChainId, ChainName, Dataset, Finality, LogSelector, Selector};
use datalens_engine::{Caller, EngineSettings};
use datalens_store::{JobPurpose, JobStatus};
use serde_json::json;
use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tokio::task::JoinSet;

use common::{
    MockChain, caller, chain, chain_id, chain_name, chain_with, chain_with_rps, engine,
    engine_with, expected, fixture, logs_request, range, selector, settings, spawn_workers,
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
async fn a_cell_that_failed_midway_keeps_its_prefix_and_refetches_only_the_rest(pool: PgPool) {
    let mock = MockChain::spawn().await;
    mock.fail_logs_from(5_000);
    let engine = engine_with(
        &pool,
        chain_with(mock.rpc.url(), Duration::from_millis(100), 1_000, 1_000),
        settings(),
    )
    .await;
    let wanted = selector(&[0xa1], &[]);
    let mut request = logs_request(wanted.clone(), range(0, 9_999));
    request.wait_ms = Some(0);

    engine.query(&caller(), request.clone()).await.unwrap();
    assert!(engine.fill_once("w").await.unwrap());
    assert_eq!(mock.count("eth_getLogs"), 6);
    let partial = engine.query(&caller(), request.clone()).await.unwrap();
    assert_eq!(partial.served, Some(range(0, 4_999)));
    assert_eq!(partial.next_start, Some(5_000));
    assert_eq!(partial.incomplete_reason, Some(IncompleteReason::Pending));
    assert_eq!(partial.rows, expected(&wanted, range(0, 4_999)));

    mock.clear_log_failures();
    assert!(engine.fill_once("w").await.unwrap());
    assert_eq!(mock.count("eth_getLogs"), 11);
    let complete = engine.query(&caller(), request).await.unwrap();
    assert_eq!(complete.served, Some(range(0, 9_999)));
    assert_eq!(complete.incomplete_reason, None);
    assert_eq!(complete.rows, expected(&wanted, range(0, 9_999)));
}

#[sqlx::test(migrations = false)]
async fn a_batch_member_reclaimed_mid_fetch_gets_no_coverage_or_usage_from_the_old_holder(
    pool: PgPool,
) {
    let mock = MockChain::spawn_slow(Duration::from_millis(1_000));
    let engine = engine_with(
        &pool,
        chain(mock.rpc.url(), Duration::from_millis(100)),
        EngineSettings {
            lease: Duration::from_millis(300),
            ..settings()
        },
    )
    .await;
    let kept = selector(&[0xa1], &[]);
    let lost = selector(&[0xa2], &[]);
    for (application, wanted) in [("keeper", &kept), ("loser", &lost)] {
        let mut request = logs_request(wanted.clone(), range(0, 9_999));
        request.wait_ms = Some(0);
        let who = Caller {
            application,
            fill_budget: None,
            max_subscriptions: None,
        };
        engine.query(&who, request).await.unwrap();
    }

    let fill = tokio::spawn({
        let engine = engine.clone();
        async move { engine.fill_once("holder").await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let store = engine.store();
    let running = store.list_jobs(Some(JobStatus::Running), 10).await.unwrap();
    assert_eq!(running.len(), 2);
    let (stolen, _) = running
        .iter()
        .find(|(job, _)| job.application_id == "loser")
        .unwrap();
    assert!(
        store
            .fail_job(
                stolen.id,
                "holder",
                &json!({"kind": "test"}),
                true,
                Duration::ZERO
            )
            .await
            .unwrap()
    );
    let thief = store
        .claim("thief", Duration::from_secs(30), 1, None)
        .await
        .unwrap();
    assert_eq!(
        thief.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![stolen.id]
    );
    assert!(fill.await.unwrap().unwrap());

    let spans = |wanted: LogSelector| {
        let engine = engine.clone();
        async move {
            engine
                .coverage(&chain_name(), Dataset::EvmLogs, Some(wanted), None)
                .await
                .unwrap()
                .spans
        }
    };
    assert_eq!(
        spans(kept).await,
        vec![CoverageSpan {
            start: 0,
            end: 9_999,
            finality: Finality::Finalized,
        }]
    );
    assert_eq!(spans(lost).await, Vec::new());
    let running = store.list_jobs(Some(JobStatus::Running), 10).await.unwrap();
    assert_eq!(
        running
            .iter()
            .map(|(job, _)| (job.id, job.worker_id.as_deref()))
            .collect::<Vec<_>>(),
        vec![(stolen.id, Some("thief"))]
    );

    engine.usage().flush(store).await.unwrap();
    let usage = |application: &'static str| async move {
        store
            .usage(
                Utc::now().date_naive(),
                application,
                chain_id(),
                Dataset::EvmLogs,
            )
            .await
            .unwrap()
    };
    let keeper = usage("keeper").await;
    assert_eq!((keeper.blocks_filled, keeper.provider_calls), (10_000, 1));
    let loser = usage("loser").await;
    assert_eq!((loser.blocks_filled, loser.provider_calls), (0, 0));
}

#[sqlx::test(migrations = false)]
async fn batched_provider_calls_are_split_across_member_applications(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let wide: Vec<u8> = (1..=31).collect();
    let members = [
        ("a", selector(&wide, &[])),
        ("b", selector(&[0xa1, 0xa2], &[])),
        ("c", selector(&[0xa3], &[])),
    ];
    for (application, wanted) in &members {
        let mut request = logs_request(wanted.clone(), range(0, 9_999));
        request.wait_ms = Some(0);
        let who = Caller {
            application,
            fill_budget: None,
            max_subscriptions: None,
        };
        engine.query(&who, request).await.unwrap();
    }
    assert!(engine.fill_once("w").await.unwrap());
    assert!(!engine.fill_once("w").await.unwrap());
    assert_eq!(mock.count("eth_getLogs"), 2);

    engine.usage().flush(engine.store()).await.unwrap();
    let mut filled = Vec::new();
    for (application, _) in &members {
        let totals = engine
            .store()
            .usage(
                Utc::now().date_naive(),
                application,
                chain_id(),
                Dataset::EvmLogs,
            )
            .await
            .unwrap();
        filled.push((totals.blocks_filled, totals.provider_calls));
    }
    assert_eq!(filled, vec![(10_000, 1), (10_000, 1), (10_000, 0)]);
}

#[sqlx::test(migrations = false)]
async fn an_internal_fill_error_fails_the_job_after_three_attempts(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let store = engine.store();
    let orphan = ChainId::new(2).unwrap();
    store
        .ensure_chain(orphan, &ChainName::new("orphan").unwrap())
        .await
        .unwrap();
    let selector_id = store
        .upsert_selector(orphan, &Selector::Logs(selector(&[0xa1], &[])))
        .await
        .unwrap();
    store
        .enqueue(
            orphan,
            Dataset::EvmLogs,
            selector_id,
            range(0, 9_999),
            JobPurpose::Query,
            "degov",
        )
        .await
        .unwrap();

    for _ in 0..3 {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !engine.fill_once("w").await.unwrap() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "job never came back"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let failed = engine.jobs(Some(JobStatus::Failed), 10).await.unwrap();
    assert_eq!(failed.jobs.len(), 1);
    assert_eq!(failed.jobs[0].attempts, 3);
    assert_eq!(
        failed.jobs[0].last_error.as_ref().unwrap()["kind"],
        "internal"
    );
    assert!(!engine.fill_once("w").await.unwrap());
    assert_eq!(mock.total(), 0);
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
