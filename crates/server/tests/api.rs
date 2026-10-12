mod common;

use common::*;
use datalens_core::wire::{BackfillRequest, IncompleteReason, LogRecord, SegmentSource};
use datalens_core::{ChainName, Dataset, Finality};
use serde_json::{Value, json};
use sqlx::PgPool;

#[sqlx::test(migrations = false)]
async fn cold_query_fills_through_the_server_and_repeat_is_served_from_postgres(pool: PgPool) {
    let rpc = mock_chain().await;
    let server = boot_mock(pool, &rpc).await;
    let client = server.degov();
    let request = logs_request(ethereum(), selector(&[0xa1]), range(0, 9_999));
    let get_logs_before = count(&rpc, "eth_getLogs");

    let cold = client.query(&request).await.unwrap();
    assert_eq!(cold.served, Some(range(0, 9_999)));
    assert_eq!(cold.next_start, None);
    assert_eq!(cold.incomplete_reason, None);
    assert_eq!(cold.segments.len(), 1);
    assert_eq!(cold.segments[0].source, SegmentSource::Durable);
    assert_eq!(cold.segments[0].finality, Finality::Finalized);
    let blocks: Vec<u64> = cold
        .rows
        .iter()
        .map(|row| {
            serde_json::from_value::<LogRecord>(row.clone())
                .unwrap()
                .block_number
        })
        .collect();
    assert_eq!(
        blocks,
        [
            7, 1_007, 2_007, 3_007, 4_007, 5_007, 6_007, 7_007, 8_007, 9_007
        ]
    );
    assert_eq!(count(&rpc, "eth_getLogs") - get_logs_before, 1);

    let calls_before = rpc.calls().len();
    let warm = client.query(&request).await.unwrap();
    assert_eq!(warm.rows, cold.rows);
    assert_eq!(
        rpc.calls().len() - calls_before,
        head_calls_only(&rpc, calls_before)
    );
    assert_eq!(count(&rpc, "eth_getLogs") - get_logs_before, 1);

    server.stop().await;
}

/// Head tracker ticks keep polling the mock; everything else after `from` must be absent.
fn head_calls_only(rpc: &datalens_evm::mock::MockRpc, from: usize) -> usize {
    rpc.calls()[from..]
        .iter()
        .filter(|body| {
            let items = match body {
                Value::Array(items) => items.clone(),
                other => vec![Value::clone(other)],
            };
            items
                .iter()
                .all(|item| item["params"][0].as_str() == Some("latest"))
        })
        .count()
}

#[sqlx::test(migrations = false)]
async fn errors_map_to_stable_kinds_and_statuses(pool: PgPool) {
    let rpc = mock_chain().await;
    let server = boot_mock(pool, &rpc).await;
    let degov = server.degov();

    let beyond_safe = logs_request(ethereum(), selector(&[0xa1]), range(SAFE - 10, SAFE + 1));
    let error = degov.query(&beyond_safe).await.unwrap_err();
    assert_eq!(
        (error.status(), error.kind()),
        (Some(400), Some("invalid_input"))
    );

    let wrong_token = server.client("degov", "nope");
    let error = wrong_token
        .query(&logs_request(ethereum(), selector(&[0xa1]), range(0, 9)))
        .await
        .unwrap_err();
    assert_eq!(
        (error.status(), error.kind()),
        (Some(401), Some("unauthorized"))
    );

    let narrow = server.client("narrow", "narrow-token");
    let error = narrow
        .query(&logs_request(ethereum(), selector(&[0xa1]), range(0, 9)))
        .await
        .unwrap_err();
    assert_eq!(
        (error.status(), error.kind()),
        (Some(403), Some("forbidden"))
    );

    let over_quota = logs_request(ethereum(), selector(&[0xa1]), range(0, 50_000));
    let error = degov.query(&over_quota).await.unwrap_err();
    assert_eq!(
        (error.status(), error.kind()),
        (Some(429), Some("quota_exceeded"))
    );

    let error = narrow.jobs(None).await.unwrap_err();
    assert_eq!(
        (error.status(), error.kind()),
        (Some(403), Some("forbidden"))
    );

    let error = degov.metrics("wrong").await.unwrap_err();
    assert_eq!(error.status(), Some(401));

    assert_eq!(count(&rpc, "eth_getLogs"), 0);
    server.stop().await;
}

#[sqlx::test(migrations = false)]
async fn graphql_returns_the_same_rows_as_rest(pool: PgPool) {
    let rpc = mock_chain().await;
    let server = boot_mock(pool, &rpc).await;
    let client = server.degov();
    let request = logs_request(ethereum(), selector(&[0xa2]), range(0, 9_999));

    let rest = client.query(&request).await.unwrap();
    let data = client
        .graphql_query(
            &request,
            "rows served { start end } nextStart incompleteReason segments { source finality }",
        )
        .await
        .unwrap();
    assert_eq!(data["query"]["rows"], Value::Array(rest.rows.clone()));
    assert_eq!(rest.rows.len(), 10);
    assert_eq!(data["query"]["served"], json!({"start": 0, "end": 9_999}));
    assert_eq!(data["query"]["nextStart"], Value::Null);
    assert_eq!(data["query"]["incompleteReason"], Value::Null);
    assert_eq!(
        data["query"]["segments"],
        json!([{"source": "durable", "finality": "finalized"}])
    );

    let error = client
        .graphql_query(
            &logs_request(ethereum(), selector(&[0xa2]), range(0, 50_000)),
            "nextStart",
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), Some("quota_exceeded"));
    server.stop().await;
}

#[sqlx::test(migrations = false)]
async fn heads_admin_and_metrics_round_trip(pool: PgPool) {
    let rpc = mock_chain().await;
    let server = boot_mock(pool, &rpc).await;
    let client = server.degov();

    let heads = client.heads().await.unwrap();
    let chains: Vec<(String, u64, u64, u64)> = heads
        .chains
        .iter()
        .map(|h| (h.chain.to_string(), h.latest, h.safe, h.finalized))
        .collect();
    assert_eq!(chains, [("ethereum".to_owned(), LATEST, SAFE, 98_000)]);
    let narrow = server.client("narrow", "narrow-token");
    let base = ChainName::new("base").unwrap();
    assert_eq!(narrow.chain_heads(&base).await.unwrap().safe, SAFE);
    assert_eq!(
        narrow.chain_heads(&ethereum()).await.unwrap_err().status(),
        Some(403)
    );

    let backfill = client
        .backfill(&BackfillRequest {
            chain: ethereum(),
            dataset: Dataset::EvmLogs,
            selector: Some(selector(&[0xa1, 0xa2])),
            range: range(0, 19_999),
        })
        .await
        .unwrap();
    assert_eq!(backfill.jobs.len(), 2);

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    while client.jobs(Some("done")).await.unwrap().jobs.len() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "backfill never finished"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let calls_before = count(&rpc, "eth_getLogs");
    let warm = client
        .query(&logs_request(
            ethereum(),
            selector(&[0xa1, 0xa2]),
            range(0, 19_999),
        ))
        .await
        .unwrap();
    assert_eq!(warm.rows.len(), 20);
    assert_eq!(warm.incomplete_reason, None::<IncompleteReason>);
    assert_eq!(count(&rpc, "eth_getLogs"), calls_before);

    let text = client.metrics(METRICS_TOKEN).await.unwrap();
    assert!(text.contains(
        r#"datalens_queries_total{application="degov",chain="ethereum",dataset="evm.logs",outcome="complete"} 1"#
    ));
    assert!(text.contains(r#"datalens_fill_jobs{status="done"} 2"#));
    assert!(text.contains("datalens_provider_calls_total"));
    assert!(text.contains("datalens_query_duration_seconds_bucket"));
    assert!(text.contains("datalens_fill_job_duration_seconds_bucket"));
    server.stop().await;
}
