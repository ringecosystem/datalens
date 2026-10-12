#[path = "common/mod.rs"]
mod common;

use datalens_core::wire::{CoverageSpan, IncompleteReason, LogRecord, WatchRequest};
use datalens_core::{ChainName, Dataset, Finality, LogRow, LogSelector, QueryFinality};
use datalens_engine::{Caller, EngineSettings, QueryEngine};
use prometheus::Registry;
use serde_json::Value;
use sqlx::PgPool;

use common::follow_chain::{
    FollowChain, follow_chain, follow_engine, follow_settings, metrics_text, request, usdc,
};
use common::{caller, chain_id, range, selector};

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

fn ethereum() -> ChainName {
    ChainName::new("ethereum").unwrap()
}

/// Address `0xb2` with topic0 `0x22`. It batches with `usdc()`.
fn second() -> LogSelector {
    selector(&[0xb2], &[0x22])
}

fn span(start: u64, end: u64, finality: Finality) -> CoverageSpan {
    CoverageSpan {
        start,
        end,
        finality,
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

async fn latest_only(engine: &QueryEngine, wanted: LogSelector, start: u64, end: u64) {
    engine
        .query(
            &caller(),
            request(
                "ethereum",
                wanted,
                range(start, end),
                QueryFinality::LatestOnly,
            ),
        )
        .await
        .unwrap();
}

async fn spans(engine: &QueryEngine, wanted: LogSelector) -> Vec<CoverageSpan> {
    engine
        .coverage(&ethereum(), Dataset::EvmLogs, Some(wanted), None)
        .await
        .unwrap()
        .spans
}

async fn stored(engine: &QueryEngine, wanted: &LogSelector, start: u64, end: u64) -> Vec<LogRow> {
    engine
        .store()
        .read_logs(chain_id(), wanted, range(start, end), None, 10_000)
        .await
        .unwrap()
        .0
}

#[sqlx::test(migrations = false)]
async fn a_fork_under_the_whole_reorg_window_is_rolled_back_before_follow_halts(pool: PgPool) {
    let (mock, engine, registry) = setup(&pool, follow_settings()).await;
    latest_only(&engine, usdc(), 9_993, START).await;
    // One catch-up tick leaves 208 unsafe blocks, more than the 64-block window.
    mock.advance(200);
    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(stored(&engine, &usdc(), 9_993, 10_200).await.len(), 208);

    mock.reorg_from(9_995, 5);
    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(stored(&engine, &usdc(), 9_995, 10_200).await, vec![]);
    assert_eq!(stored(&engine, &usdc(), 9_993, 9_994).await.len(), 2);
    let progress = engine.store().list_follow(chain_id()).await.unwrap();
    assert_eq!(progress[0].head_block, 9_994);
    assert!(
        engine
            .store()
            .follow_halt(chain_id())
            .await
            .unwrap()
            .is_some()
    );
    let text = metrics_text(&registry);
    assert!(text.contains("datalens_follow_halted{chain=\"ethereum\"} 1"));

    let response = engine
        .query(
            &caller(),
            request(
                "ethereum",
                usdc(),
                range(10_193, 10_200),
                QueryFinality::LatestOnly,
            ),
        )
        .await
        .unwrap();
    assert_eq!(response.served, None);
    assert_eq!(response.rows, Vec::<Value>::new());
}

#[sqlx::test(migrations = false)]
async fn promotion_splits_at_finalized_and_later_raises_safe_rows_to_finalized(pool: PgPool) {
    let (mock, engine, _registry) = setup(&pool, follow_settings()).await;
    latest_only(&engine, usdc(), 9_993, START).await;
    engine.tick_once(&ethereum()).await.unwrap();
    mock.advance(8);
    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(
        spans(&engine, usdc()).await,
        vec![span(9_993, 10_000, Finality::Safe)]
    );

    mock.advance(8);
    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(
        spans(&engine, usdc()).await,
        vec![
            span(9_993, 10_000, Finality::Finalized),
            span(10_001, 10_008, Finality::Safe),
        ]
    );
    let response = engine
        .query(
            &caller(),
            request(
                "ethereum",
                usdc(),
                range(9_993, 10_008),
                QueryFinality::DurableOnly,
            ),
        )
        .await
        .unwrap();
    let mut expected = records(
        mock.logs(&usdc(), range(9_993, 10_000)),
        Finality::Finalized,
    );
    expected.extend(records(
        mock.logs(&usdc(), range(10_001, 10_008)),
        Finality::Safe,
    ));
    assert_eq!(response.rows, expected);
}

#[sqlx::test(migrations = false)]
async fn one_promotion_straddling_finalized_records_both_finalities(pool: PgPool) {
    let (mock, engine, _registry) = setup(&pool, follow_settings()).await;
    latest_only(&engine, usdc(), 9_993, START).await;
    mock.advance(20);
    engine.tick_once(&ethereum()).await.unwrap();
    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(
        spans(&engine, usdc()).await,
        vec![
            span(9_993, 10_004, Finality::Finalized),
            span(10_005, 10_012, Finality::Safe),
        ]
    );
    let rows = stored(&engine, &usdc(), 9_993, 10_012).await;
    assert!(
        rows[..12]
            .iter()
            .all(|row| row.finality == Finality::Finalized)
    );
    assert!(rows[12..].iter().all(|row| row.finality == Finality::Safe));
}

#[sqlx::test(migrations = false)]
async fn a_caller_over_quota_does_not_fill_another_applications_follow_region(pool: PgPool) {
    let (mock, engine, _registry) = setup(&pool, follow_settings()).await;
    latest_only(&engine, usdc(), 9_993, START).await;
    mock.advance(8);
    engine.tick_once(&ethereum()).await.unwrap();

    let capped = Caller {
        application: "other",
        fill_budget: None,
        max_subscriptions: Some(0),
    };
    let response = engine
        .query(
            &capped,
            request(
                "ethereum",
                usdc(),
                range(9_900, START),
                QueryFinality::DurableOnly,
            ),
        )
        .await
        .unwrap();
    let jobs = engine.jobs(None, 100).await.unwrap().jobs;
    assert!(!jobs.is_empty());
    assert!(jobs.iter().all(|job| job.range.end() <= 9_992), "{jobs:?}");
    assert_eq!(response.next_start, Some(9_900));
    assert!(response.incomplete_reason.is_some());
}

#[sqlx::test(migrations = false)]
async fn latest_only_straddling_safe_reports_the_blocks_it_will_never_serve(pool: PgPool) {
    let (_mock, engine, _registry) = setup(&pool, follow_settings()).await;
    latest_only(&engine, usdc(), 9_993, START).await;
    engine.tick_once(&ethereum()).await.unwrap();

    for (end, served) in [(START, range(9_993, START)), (9_998, range(9_993, 9_998))] {
        let response = engine
            .query(
                &caller(),
                request(
                    "ethereum",
                    usdc(),
                    range(9_990, end),
                    QueryFinality::LatestOnly,
                ),
            )
            .await
            .unwrap();
        assert_eq!(response.served, Some(served));
        assert_eq!(response.next_start, None);
        assert_eq!(
            response.incomplete_reason,
            Some(IncompleteReason::BelowSafe)
        );
        assert!(
            response
                .incomplete_detail
                .as_deref()
                .is_some_and(|detail| detail.contains("9990..=9992")),
            "{response:?}"
        );
    }

    let response = engine
        .query(
            &caller(),
            request(
                "ethereum",
                usdc(),
                range(9_990, 10_005),
                QueryFinality::LatestOnly,
            ),
        )
        .await
        .unwrap();
    assert_eq!(response.served, Some(range(9_993, START)));
    assert_eq!(response.next_start, Some(10_001));
    assert_eq!(response.incomplete_reason, Some(IncompleteReason::Pending));

    let response = engine
        .query(
            &caller(),
            request(
                "ethereum",
                usdc(),
                range(9_993, START),
                QueryFinality::LatestOnly,
            ),
        )
        .await
        .unwrap();
    assert_eq!(response.next_start, None);
    assert_eq!(response.incomplete_reason, None);
}

#[sqlx::test(migrations = false)]
async fn batchable_selectors_with_different_heads_share_one_get_logs(pool: PgPool) {
    let (mock, engine, _registry) = setup(&pool, follow_settings()).await;
    latest_only(&engine, usdc(), 9_993, START).await;
    engine.tick_once(&ethereum()).await.unwrap();
    mock.advance(3);
    latest_only(&engine, second(), 9_996, 10_003).await;

    let before = mock.count("eth_getLogs");
    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(mock.count("eth_getLogs") - before, 1);
    for wanted in [usdc(), second()] {
        let response = engine
            .query(
                &caller(),
                request(
                    "ethereum",
                    wanted.clone(),
                    range(9_996, 10_003),
                    QueryFinality::LatestOnly,
                ),
            )
            .await
            .unwrap();
        assert_eq!(response.served, Some(range(9_996, 10_003)));
        assert_eq!(
            response.rows,
            records(mock.logs(&wanted, range(9_996, 10_003)), Finality::Unsafe)
        );
    }
}

#[sqlx::test(migrations = false)]
async fn a_result_limit_shrinks_the_follow_window_and_fails_only_at_the_minimum(pool: PgPool) {
    let (mock, engine, _registry) = setup(&pool, follow_settings()).await;
    latest_only(&engine, usdc(), 9_993, START).await;
    mock.limit_logs(Some(100));
    mock.advance(300);
    engine.tick_once(&ethereum()).await.unwrap();
    assert!(mock.count("eth_getLogs") > 1);
    assert_eq!(
        stored(&engine, &usdc(), 9_993, 10_300).await,
        mock.logs(&usdc(), range(9_993, 10_300))
            .into_iter()
            .map(|mut row| {
                row.finality = Finality::Unsafe;
                row
            })
            .collect::<Vec<_>>()
    );
    let head = engine.store().list_follow(chain_id()).await.unwrap()[0].head_block;
    assert_eq!(head, 10_300);

    // The provider's minimum window is 16 blocks.
    mock.limit_logs(Some(8));
    mock.advance(40);
    let before = mock.count("eth_getLogs");
    engine.tick_once(&ethereum()).await.unwrap();
    assert!(mock.count("eth_getLogs") > before);
    let progress = engine.store().list_follow(chain_id()).await.unwrap();
    assert_eq!(progress[0].head_block, 10_300);
    assert_eq!(stored(&engine, &usdc(), 10_301, 10_340).await, vec![]);
}

#[sqlx::test(migrations = false)]
async fn retention_drops_an_unwatched_selectors_rows_and_keeps_the_live_one(pool: PgPool) {
    let settings = EngineSettings {
        unsafe_retention: std::time::Duration::ZERO,
        ..follow_settings()
    };
    let (mock, engine, _registry) = setup(&pool, settings).await;
    latest_only(&engine, usdc(), 9_993, START).await;
    latest_only(&engine, second(), 9_993, START).await;
    engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(stored(&engine, &second(), 9_993, START).await.len(), 8);

    engine
        .unwatch(
            "degov",
            WatchRequest {
                chain: ethereum(),
                dataset: Dataset::EvmLogs,
                selector: Some(second()),
            },
        )
        .await
        .unwrap();
    mock.advance(8);
    engine.tick_once(&ethereum()).await.unwrap();

    assert_eq!(stored(&engine, &second(), 9_993, 10_008).await, vec![]);
    assert_eq!(stored(&engine, &usdc(), 9_993, 10_008).await.len(), 16);
    assert_eq!(
        spans(&engine, usdc()).await,
        vec![span(9_993, 10_000, Finality::Safe)]
    );
    let progress = engine.store().list_follow(chain_id()).await.unwrap();
    assert_eq!(progress.len(), 1);
    assert_eq!(progress[0].head_block, 10_008);
}

#[sqlx::test(migrations = false)]
async fn header_catch_up_past_the_batch_size_uses_several_batches(pool: PgPool) {
    let (mock, engine, _registry) = setup(&pool, follow_settings()).await;
    engine
        .watch(
            &caller(),
            WatchRequest {
                chain: ethereum(),
                dataset: Dataset::EvmBlocks,
                selector: None,
            },
        )
        .await
        .unwrap();
    engine.tick_once(&ethereum()).await.unwrap();
    mock.advance(30);
    let before = mock.requests();
    engine.tick_once(&ethereum()).await.unwrap();
    // The head call, the reorg check, and two header batches of at most 20.
    assert_eq!(mock.requests() - before, 4);
    let progress = engine.store().list_follow(chain_id()).await.unwrap();
    assert_eq!(progress[0].head_block, 10_030);
    assert_eq!(
        engine.store().hash_at(chain_id(), 10_030).await.unwrap(),
        Some(mock.hash(10_030))
    );
    assert_eq!(
        engine.store().hash_at(chain_id(), 10_011).await.unwrap(),
        Some(mock.hash(10_011))
    );
}

#[sqlx::test(migrations = false)]
async fn a_canonical_block_missing_on_the_provider_is_retried_without_moving_follow(pool: PgPool) {
    let (mock, engine, _registry) = setup(&pool, follow_settings()).await;
    latest_only(&engine, usdc(), 9_993, START).await;
    engine.tick_once(&ethereum()).await.unwrap();
    let before = engine.store().list_follow(chain_id()).await.unwrap();

    mock.advance(2);
    mock.hide_block(Some(START));
    // The follow step fails and is retried next tick. Heads still publish.
    let heads = engine.tick_once(&ethereum()).await.unwrap();
    assert_eq!(heads.latest, 10_002);
    assert_eq!(
        engine.store().list_follow(chain_id()).await.unwrap(),
        before
    );
    assert!(
        engine
            .store()
            .follow_halt(chain_id())
            .await
            .unwrap()
            .is_none()
    );

    mock.hide_block(None);
    engine.tick_once(&ethereum()).await.unwrap();
    let after = engine.store().list_follow(chain_id()).await.unwrap();
    assert_eq!(after[0].head_block, 10_002);
}
