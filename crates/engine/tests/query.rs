#[path = "common/mod.rs"]
mod common;

use datalens_core::wire::{IncompleteReason, PageRequest, QueryResponse, Segment, SegmentSource};
use datalens_core::{Dataset, Finality, QueryFinality};
use datalens_engine::QueryEngine;
use serde_json::Value;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

use common::{
    MockChain, caller, engine, expected, logs_request, range, selector, sender_of, spawn_workers,
};

fn finalized_segment(start: u64, end: u64) -> Vec<Segment> {
    vec![Segment {
        start,
        end,
        source: SegmentSource::Durable,
        finality: Finality::Finalized,
        anchor: None,
    }]
}

fn assert_complete(response: &QueryResponse, start: u64, end: u64) {
    assert_eq!(response.served, Some(range(start, end)));
    assert_eq!(response.next_start, None);
    assert_eq!(response.incomplete_reason, None);
    assert_eq!(response.segments, finalized_segment(start, end));
}

#[sqlx::test(migrations = false)]
async fn cold_query_fills_and_the_repeat_makes_no_rpc_call(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let _workers = spawn_workers(&engine, 2);
    let wanted = selector(&[0xa1], &[0x11]);

    let first = engine
        .query(&caller(), logs_request(wanted.clone(), range(0, 25_000)))
        .await
        .unwrap();
    assert_complete(&first, 0, 25_000);
    assert_eq!(first.rows.len(), 9);
    assert_eq!(first.rows, expected(&wanted, range(0, 25_000)));
    assert_eq!(mock.count("eth_getLogs"), 3);

    let before = mock.total();
    let second = engine
        .query(&caller(), logs_request(wanted.clone(), range(0, 25_000)))
        .await
        .unwrap();
    assert_eq!(mock.total() - before, 0);
    assert_eq!(second.rows, first.rows);
    assert_eq!(mock.count("eth_getBlockByNumber"), 0);
}

#[sqlx::test(migrations = false)]
async fn two_engines_on_one_database_fetch_each_chunk_once(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let left = engine(&pool, &mock).await;
    let other_pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with((*pool.connect_options()).clone())
        .await
        .unwrap();
    let right = engine(&other_pool, &mock).await;
    let _left_workers = spawn_workers(&left, 2);
    let _right_workers = spawn_workers(&right, 2);
    let wanted = selector(&[0xa2], &[]);

    let who = caller();
    let (a, b) = tokio::join!(
        left.query(&who, logs_request(wanted.clone(), range(0, 29_999))),
        right.query(&who, logs_request(wanted.clone(), range(0, 29_999))),
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(mock.count("eth_getLogs"), 3);
    assert_complete(&a, 0, 29_999);
    assert_eq!(a.rows.len(), 20);
    assert_eq!(a.rows, expected(&wanted, range(0, 29_999)));
    assert_eq!(b.rows, a.rows);
}

#[sqlx::test(migrations = false)]
async fn three_selectors_in_one_chunk_share_one_batched_get_logs(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let members = [
        selector(&[0xa1], &[0x11]),
        selector(&[0xa2], &[0x11]),
        selector(&[0xa3], &[0x22]),
    ];
    for member in &members {
        let mut request = logs_request(member.clone(), range(0, 9_999));
        request.wait_ms = Some(0);
        let pending = engine.query(&caller(), request).await.unwrap();
        assert_eq!(pending.served, None);
        assert_eq!(pending.next_start, Some(0));
        assert_eq!(pending.incomplete_reason, Some(IncompleteReason::Pending));
    }
    assert_eq!(mock.count("eth_getLogs"), 0);

    assert!(engine.fill_once("batcher").await.unwrap());
    assert!(!engine.fill_once("batcher").await.unwrap());
    assert_eq!(mock.count("eth_getLogs"), 1);

    let mut counts = Vec::new();
    for member in &members {
        let response = engine
            .query(&caller(), logs_request(member.clone(), range(0, 9_999)))
            .await
            .unwrap();
        assert_complete(&response, 0, 9_999);
        assert_eq!(response.rows, expected(member, range(0, 9_999)));
        counts.push(response.rows.len());
    }
    assert_eq!(counts, vec![4, 3, 3]);
    assert_eq!(mock.count("eth_getLogs"), 1);
}

#[sqlx::test(migrations = false)]
async fn narrower_selector_is_served_from_wider_coverage(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let _workers = spawn_workers(&engine, 1);
    let wide = selector(&[0xa1, 0xa2], &[]);
    engine
        .query(&caller(), logs_request(wide, range(0, 9_999)))
        .await
        .unwrap();
    let before = mock.total();

    let narrow = selector(&[0xa1], &[0x11]);
    let response = engine
        .query(&caller(), logs_request(narrow.clone(), range(100, 9_000)))
        .await
        .unwrap();
    assert_eq!(mock.total() - before, 0);
    assert_complete(&response, 100, 9_000);
    assert_eq!(response.rows.len(), 2);
    assert_eq!(response.rows, expected(&narrow, range(100, 9_000)));
}

#[sqlx::test(migrations = false)]
async fn failed_chunk_returns_the_prefix_and_where_to_resume(pool: PgPool) {
    let mock = MockChain::spawn().await;
    mock.fail_logs_from(10_000);
    let engine = engine(&pool, &mock).await;
    let _workers = spawn_workers(&engine, 2);
    let wanted = selector(&[0xa1, 0xa2, 0xa3], &[]);

    let response = engine
        .query(&caller(), logs_request(wanted.clone(), range(0, 19_999)))
        .await
        .unwrap();
    assert_eq!(response.served, Some(range(0, 9_999)));
    assert_eq!(response.next_start, Some(10_000));
    assert_eq!(
        response.incomplete_reason,
        Some(IncompleteReason::ProviderError)
    );
    assert_eq!(response.incomplete_detail.as_deref(), Some("fatal"));
    assert_eq!(response.segments, finalized_segment(0, 9_999));
    assert_eq!(response.rows.len(), 20);
    assert_eq!(response.rows, expected(&wanted, range(0, 9_999)));
}

#[sqlx::test(migrations = false)]
async fn durable_query_above_safe_is_invalid_input(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let error = engine
        .query(
            &caller(),
            logs_request(selector(&[0xa1], &[]), range(98_000, 99_001)),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), "invalid_input");
    assert_eq!(
        error.to_string(),
        "invalid input: block range end 99001 is above the safe head 99000"
    );
    assert_eq!(mock.total(), 0);
}

#[sqlx::test(migrations = false)]
async fn safe_to_latest_serves_the_durable_part_and_flags_the_tail(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let _workers = spawn_workers(&engine, 1);
    let mut request = logs_request(selector(&[0xa1], &[]), range(98_500, 99_050));
    request.finality = QueryFinality::SafeToLatest;
    let response = engine.query(&caller(), request).await.unwrap();
    assert_eq!(response.served, Some(range(98_500, 99_000)));
    assert_eq!(response.next_start, Some(99_001));
    assert_eq!(
        response.incomplete_reason,
        Some(IncompleteReason::NotFollowedYet)
    );
    assert_eq!(
        response.segments,
        vec![Segment {
            start: 98_500,
            end: 99_000,
            source: SegmentSource::Durable,
            finality: Finality::Safe,
            anchor: None,
        }]
    );
}

#[sqlx::test(migrations = false)]
async fn transaction_from_is_looked_up_once(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let _workers = spawn_workers(&engine, 1);
    let mut request = logs_request(selector(&[0xa1], &[]), range(0, 9_999));
    request.fields = Some(vec![
        "block_number".to_owned(),
        "transaction_hash".to_owned(),
        "transaction_from".to_owned(),
        "block_timestamp".to_owned(),
    ]);

    let first = engine.query(&caller(), request.clone()).await.unwrap();
    assert_eq!(first.rows.len(), 7);
    assert_eq!(mock.count("eth_getTransactionByHash"), 7);
    assert_eq!(mock.count("eth_getBlockByNumber"), 7);
    for row in &first.rows {
        let hash = row["transaction_hash"].as_str().unwrap().parse().unwrap();
        let block = row["block_number"].as_u64().unwrap();
        let mut keys: Vec<&str> = row
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                "block_number",
                "block_timestamp",
                "transaction_from",
                "transaction_hash"
            ]
        );
        assert_eq!(
            row["transaction_from"],
            Value::String(format!("{:#x}", sender_of(&hash)))
        );
        assert_eq!(row["block_timestamp"], Value::from(1_700_000_000 + block));
    }

    let before = mock.total();
    let second = engine.query(&caller(), request).await.unwrap();
    assert_eq!(mock.total() - before, 0);
    assert_eq!(second.rows, first.rows);
}

#[sqlx::test(migrations = false)]
async fn unresolved_senders_fail_the_query_but_resolved_ones_are_kept(pool: PgPool) {
    let mock = MockChain::spawn().await;
    mock.fail_sender_at(1_507);
    mock.misplace_sender_at(3_007);
    let engine = engine(&pool, &mock).await;
    let _workers = spawn_workers(&engine, 1);
    let mut request = logs_request(selector(&[0xa1], &[]), range(0, 9_999));
    request.fields = Some(vec![
        "transaction_hash".to_owned(),
        "transaction_from".to_owned(),
    ]);

    let error = engine.query(&caller(), request.clone()).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "internal: no canonical sender for 2 of 7 transactions"
    );
    assert_eq!(mock.count("eth_getTransactionByHash"), 8);

    let error = engine.query(&caller(), request).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "internal: no canonical sender for 2 of 2 transactions"
    );
    assert_eq!(mock.count("eth_getTransactionByHash"), 11);
}

#[sqlx::test(migrations = false)]
async fn blocks_query_fills_headers_once(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let _workers = spawn_workers(&engine, 1);
    let mut request = logs_request(selector(&[], &[]), range(10, 29));
    request.dataset = Dataset::EvmBlocks;
    request.selector = None;
    request.fields = Some(vec!["number".to_owned(), "hash".to_owned()]);

    let first = engine.query(&caller(), request.clone()).await.unwrap();
    assert_complete(&first, 10, 29);
    assert_eq!(first.rows.len(), 20);
    assert_eq!(
        first.rows[0],
        serde_json::json!({"number": 10, "hash": format!("{:#x}", common::block_hash(10))})
    );
    let headers = mock.count("eth_getBlockByNumber");
    assert_eq!(headers, 100);

    let second = engine.query(&caller(), request).await.unwrap();
    assert_eq!(mock.count("eth_getBlockByNumber"), headers);
    assert_eq!(second.rows, first.rows);
}

#[sqlx::test(migrations = false)]
async fn pages_walk_the_served_prefix_with_cursors(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let _workers = spawn_workers(&engine, 1);
    let wanted = selector(&[0xa1, 0xa2, 0xa3], &[]);
    let mut collected = Vec::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let mut request = logs_request(wanted.clone(), range(0, 29_999));
        request.page = Some(PageRequest {
            limit: Some(25),
            cursor: cursor.clone(),
        });
        let response = engine.query(&caller(), request).await.unwrap();
        pages += 1;
        collected.extend(response.rows);
        cursor = response.page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(pages, 3);
    assert_eq!(collected, expected(&wanted, range(0, 29_999)));
}

#[sqlx::test(migrations = false)]
async fn pages_stop_at_the_served_end_while_the_next_cell_is_failed(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let _workers = spawn_workers(&engine, 1);
    let stored_past_served = engine
        .query(
            &caller(),
            logs_request(selector(&[0xa1], &[]), range(0, 19_999)),
        )
        .await
        .unwrap();
    assert_eq!(stored_past_served.served, Some(range(0, 19_999)));
    mock.fail_logs_from(10_000);
    let wanted = selector(&[0xa1, 0xa2, 0xa3], &[]);
    let mut collected = Vec::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let mut request = logs_request(wanted.clone(), range(0, 19_999));
        request.page = Some(PageRequest {
            limit: Some(6),
            cursor: cursor.clone(),
        });
        let response = engine.query(&caller(), request).await.unwrap();
        pages += 1;
        assert_eq!(response.served, Some(range(0, 9_999)));
        assert_eq!(response.next_start, Some(10_000));
        collected.extend(response.rows);
        cursor = response.page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(pages, 4);
    assert_eq!(collected, expected(&wanted, range(0, 9_999)));
}

#[sqlx::test(migrations = false)]
async fn latest_only_serves_nothing_and_enqueues_nothing_until_followed(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine = engine(&pool, &mock).await;
    let mut request = logs_request(selector(&[0xa1], &[]), range(98_500, 99_050));
    request.finality = QueryFinality::LatestOnly;
    let response = engine.query(&caller(), request).await.unwrap();
    assert_eq!(response.served, None);
    assert_eq!(response.next_start, Some(98_500));
    assert_eq!(
        response.incomplete_reason,
        Some(IncompleteReason::NotFollowedYet)
    );
    assert_eq!(response.segments, Vec::new());
    assert_eq!(response.rows, Vec::<Value>::new());
    assert_eq!(engine.jobs(None, 10).await.unwrap().jobs.len(), 0);
    assert_eq!(mock.total(), 0);
}

#[sqlx::test(migrations = false)]
async fn query_handling_never_asks_for_heights(pool: PgPool) {
    let mock = MockChain::spawn().await;
    let engine: QueryEngine = engine(&pool, &mock).await;
    let _workers = spawn_workers(&engine, 1);
    for _ in 0..5 {
        engine
            .query(
                &caller(),
                logs_request(selector(&[0xa1], &[]), range(0, 1_000)),
            )
            .await
            .unwrap();
    }
    assert_eq!(mock.count("eth_getBlockByNumber"), 0);
    assert_eq!(mock.count("eth_blockNumber"), 0);
    assert_eq!(mock.count("eth_getLogs"), 1);
}
