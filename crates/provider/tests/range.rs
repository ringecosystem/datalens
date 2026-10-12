use std::sync::Arc;
use std::time::Duration;

use datalens_core::Dataset;
use datalens_provider::{ManualClock, ProviderError, Purpose};
use serde_json::json;

mod common;

use common::{gateway, rpc_error, rpc_ok, spawn};

#[tokio::test]
async fn header_not_found_is_not_ready_and_does_not_shrink_the_range() {
    let (url, calls) = spawn(vec![rpc_error(-32_000, "header not found")]).await;
    let (gateway, _) = gateway(url, 20, 20, None, 64, 16);
    let error = gateway
        .call("eth_getLogs", json!([]), Purpose::Follow, None)
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
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::Limit);
    assert_eq!(gateway.range_blocks(Dataset::EvmLogs), 64);

    assert_eq!(gateway.record_limit(Dataset::EvmLogs).unwrap(), 32);
    assert_eq!(gateway.record_limit(Dataset::EvmLogs).unwrap(), 16);
    assert_eq!(
        gateway.record_limit(Dataset::EvmLogs).unwrap_err(),
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

    assert_eq!(gateway.record_limit(Dataset::EvmLogs).unwrap(), 32);
    for _ in 0..7 {
        gateway.record_success(Dataset::EvmLogs);
    }
    assert_eq!(gateway.range_blocks(Dataset::EvmLogs), 32);
    gateway.record_success(Dataset::EvmLogs);
    assert_eq!(gateway.range_blocks(Dataset::EvmLogs), 64);

    assert_eq!(gateway.record_limit(Dataset::EvmBlocks).unwrap(), 32);
    clock.advance(10 * 60 * 1000 - 1);
    assert_eq!(gateway.range_blocks(Dataset::EvmBlocks), 32);
    clock.advance(1);
    assert_eq!(gateway.range_blocks(Dataset::EvmBlocks), 64);
    assert_eq!(gateway.range_blocks(Dataset::EvmLogs), 64);
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
    let started = std::time::Instant::now();
    let result = gateway
        .call("eth_blockNumber", json!([]), Purpose::Head, None)
        .await
        .unwrap();
    assert_eq!(result, json!("0x10"));
    assert_eq!(calls.calls.lock().unwrap().len(), 2);
    assert!(started.elapsed() >= Duration::from_secs(1));
}
