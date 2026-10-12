use std::sync::Arc;
use std::time::Duration;

use datalens_core::Dataset;
use datalens_provider::{ManualClock, ProviderError, Purpose, TokenWait};
use serde_json::json;

mod common;

use common::{gateway, rpc_error, rpc_ok, spawn};

#[tokio::test]
async fn header_not_found_is_not_ready_and_does_not_shrink_the_range() {
    let (url, calls) = spawn(vec![rpc_error(-32_000, "header not found")]).await;
    let (gateway, _) = gateway(url, 20, 20, None, 64, 16);
    let error = gateway
        .call(
            "eth_getLogs",
            json!([]),
            Purpose::Follow,
            None,
            TokenWait::UntilToken,
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::NotReady);
    assert_eq!(gateway.range_blocks(Dataset::EvmLogs), 64);
    assert_eq!(calls.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn oversized_phrase_is_limit_and_record_limit_halves_until_the_floor() {
    let (url, _) = spawn(vec![rpc_error(
        -32_000,
        "query returned more than 10000 results",
    )])
    .await;
    let (gateway, _) = gateway(url, 20, 20, None, 64, 16);
    let error = gateway
        .call(
            "eth_getLogs",
            json!([]),
            Purpose::QueryFill,
            Some("degov-live"),
            TokenWait::UntilToken,
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::Limit);
    assert_eq!(gateway.range_blocks(Dataset::EvmLogs), 64);

    assert_eq!(gateway.record_limit(Dataset::EvmLogs, 64).unwrap(), 32);
    assert_eq!(gateway.record_limit(Dataset::EvmLogs, 32).unwrap(), 16);
    assert_eq!(
        gateway.record_limit(Dataset::EvmLogs, 16).unwrap_err(),
        ProviderError::Limit
    );
    assert_eq!(gateway.range_blocks(Dataset::EvmLogs), 16);
}

#[tokio::test]
async fn eight_successes_double_the_range_and_ttl_restores_the_max() {
    let (url, _) = spawn(vec![rpc_ok(json!("0x1"))]).await;
    let (gateway, _) = gateway(url, 20, 20, None, 64, 16);
    let clock = ManualClock::new();
    gateway.set_clock(Arc::new(clock.clone()));

    assert_eq!(gateway.record_limit(Dataset::EvmLogs, 64).unwrap(), 32);
    for _ in 0..7 {
        gateway.record_success(Dataset::EvmLogs);
    }
    assert_eq!(gateway.range_blocks(Dataset::EvmLogs), 32);
    gateway.record_success(Dataset::EvmLogs);
    assert_eq!(gateway.range_blocks(Dataset::EvmLogs), 64);

    assert_eq!(gateway.record_limit(Dataset::EvmBlocks, 64).unwrap(), 32);
    clock.advance(10 * 60 * 1000 - 1);
    assert_eq!(gateway.range_blocks(Dataset::EvmBlocks), 32);
    clock.advance(1);
    assert_eq!(gateway.range_blocks(Dataset::EvmBlocks), 64);
    assert_eq!(gateway.range_blocks(Dataset::EvmLogs), 64);
}

#[tokio::test]
async fn two_limits_at_the_same_window_halve_once() {
    let (url, _) = spawn(vec![]).await;
    let (gateway, _) = gateway(url, 20, 20, None, 10_000, 16);
    let first = gateway.clone();
    let second = gateway.clone();
    let (left, right) = tokio::join!(
        async move { first.record_limit(Dataset::EvmLogs, 10_000) },
        async move { second.record_limit(Dataset::EvmLogs, 10_000) },
    );
    assert_eq!(left.unwrap(), 5_000);
    assert_eq!(right.unwrap(), 5_000);
    assert_eq!(gateway.range_blocks(Dataset::EvmLogs), 5_000);
}

#[tokio::test]
async fn retry_after_holds_every_caller_on_the_chain() {
    let (url, calls) = spawn(vec![
        common::Reply {
            status: 429,
            retry_after: Some("2".to_owned()),
            body: json!({"error": "slow down"}),
        },
        rpc_ok(json!("0x1")),
        rpc_ok(json!("0x2")),
    ])
    .await;
    let (gateway, _) = gateway(url, 50, 50, None, 64, 16);
    let lead_gateway = gateway.clone();
    let lead = tokio::spawn(async move {
        lead_gateway
            .call(
                "eth_blockNumber",
                json!([]),
                Purpose::Head,
                None,
                TokenWait::UntilToken,
            )
            .await
            .unwrap()
    });
    while calls.calls.lock().unwrap().is_empty() {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    let follow_gateway = gateway.clone();
    let follow = tokio::spawn(async move {
        follow_gateway
            .call(
                "eth_blockNumber",
                json!([]),
                Purpose::Follow,
                None,
                TokenWait::UntilToken,
            )
            .await
            .unwrap()
    });
    let (lead, follow) = tokio::join!(lead, follow);
    lead.unwrap();
    follow.unwrap();
    let arrived = calls.arrived.lock().unwrap().clone();
    assert_eq!(arrived.len(), 3);
    assert!(
        arrived[1].saturating_duration_since(arrived[0]) >= Duration::from_millis(1_500),
        "{arrived:?}"
    );
    assert!(
        arrived[2].saturating_duration_since(arrived[0]) >= Duration::from_millis(1_500),
        "{arrived:?}"
    );
}

#[tokio::test]
async fn retry_after_over_five_seconds_returns_without_parking_an_hour() {
    let (url, calls) = spawn(vec![
        common::Reply {
            status: 429,
            retry_after: Some("3600".to_owned()),
            body: json!({"error": "slow down"}),
        },
        rpc_ok(json!("0x1")),
    ])
    .await;
    let (gateway, _) = gateway(url, 20, 20, None, 64, 16);
    let error = gateway
        .call(
            "eth_blockNumber",
            json!([]),
            Purpose::Head,
            None,
            TokenWait::UntilToken,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ProviderError::RateLimited {
            retry_after: Duration::from_secs(3600)
        }
    );
    assert_eq!(calls.calls.lock().unwrap().len(), 1);
    let held = tokio::time::timeout(Duration::from_millis(200), async {
        gateway
            .call(
                "eth_blockNumber",
                json!([]),
                Purpose::Follow,
                None,
                TokenWait::UntilToken,
            )
            .await
    })
    .await;
    assert!(held.is_err());
    assert_eq!(calls.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn http_date_retry_after_is_not_treated_as_zero() {
    let (url, calls) = spawn(vec![
        common::Reply {
            status: 429,
            retry_after: Some("Tue, 01 Jan 2030 00:00:00 GMT".to_owned()),
            body: json!({"error": "slow down"}),
        },
        rpc_ok(json!("0x1")),
    ])
    .await;
    let (gateway, _) = gateway(url, 20, 20, None, 64, 16);
    let error = gateway
        .call(
            "eth_blockNumber",
            json!([]),
            Purpose::Head,
            None,
            TokenWait::UntilToken,
        )
        .await
        .unwrap_err();
    let ProviderError::RateLimited { retry_after } = error else {
        panic!("expected rate limited, got {error:?}");
    };
    assert!(retry_after > Duration::from_secs(5));
    assert_eq!(calls.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn transient_status_is_tried_exactly_three_times() {
    let (url, calls) = spawn(vec![
        common::Reply {
            status: 503,
            retry_after: None,
            body: json!({"error": "down"}),
        },
        common::Reply {
            status: 503,
            retry_after: None,
            body: json!({"error": "down"}),
        },
        common::Reply {
            status: 503,
            retry_after: None,
            body: json!({"error": "down"}),
        },
    ])
    .await;
    let (gateway, _) = gateway(url, 20, 20, None, 64, 16);
    let error = gateway
        .call(
            "eth_blockNumber",
            json!([]),
            Purpose::Head,
            None,
            TokenWait::UntilToken,
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::Transient { status: Some(503) });
    assert_eq!(calls.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn json_rpc_32005_retries_with_backoff() {
    let (url, calls) = spawn(vec![
        rpc_error(-32_005, "upstream busy"),
        rpc_error(-32_005, "upstream busy"),
        rpc_error(-32_005, "upstream busy"),
    ])
    .await;
    let (gateway, _) = gateway(url, 20, 20, None, 64, 16);
    let started = tokio::time::Instant::now();
    let error = gateway
        .call(
            "eth_blockNumber",
            json!([]),
            Purpose::Follow,
            None,
            TokenWait::UntilToken,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ProviderError::RateLimited {
            retry_after: Duration::ZERO
        }
    );
    assert_eq!(calls.calls.lock().unwrap().len(), 3);
    assert!(started.elapsed() >= Duration::from_millis(300));
}

#[tokio::test]
async fn duplicate_batch_ids_are_fatal() {
    let (url, _) = spawn(vec![common::Reply {
        status: 200,
        retry_after: None,
        body: json!([
            {"jsonrpc": "2.0", "id": 1, "result": "0x1"},
            {"jsonrpc": "2.0", "id": 1, "result": "0x2"}
        ]),
    }])
    .await;
    let (gateway, _) = gateway(url, 20, 20, None, 64, 16);
    let error = gateway
        .batch(
            vec![
                ("eth_blockNumber".to_owned(), json!([])),
                ("eth_blockNumber".to_owned(), json!([])),
            ],
            Purpose::Head,
            None,
            TokenWait::UntilToken,
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::fatal("duplicate batch response id"));
}

#[tokio::test]
async fn retry_after_one_second_then_success() {
    let (url, calls) = spawn(vec![
        common::Reply {
            status: 429,
            retry_after: Some("1".to_owned()),
            body: json!({"error": "slow down"}),
        },
        rpc_ok(json!("0x10")),
    ])
    .await;
    let (gateway, _) = gateway(url, 50, 50, None, 64, 16);
    let started = tokio::time::Instant::now();
    let result = gateway
        .call(
            "eth_blockNumber",
            json!([]),
            Purpose::Head,
            None,
            TokenWait::UntilToken,
        )
        .await
        .unwrap();
    assert_eq!(result, json!("0x10"));
    assert_eq!(calls.calls.lock().unwrap().len(), 2);
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert!(started.elapsed() < Duration::from_secs(4));
}
