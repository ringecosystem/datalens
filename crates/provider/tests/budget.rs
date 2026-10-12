use std::sync::Arc;
use std::time::{Duration, Instant};

use datalens_provider::{ManualClock, ProviderError, Purpose};
use serde_json::json;

mod common;

use common::{gateway, rpc_ok, spawn};

#[tokio::test]
async fn backfill_fails_fast_while_head_waits_for_a_token() {
    let (url, calls) = spawn(vec![rpc_ok(json!("0x1")), rpc_ok(json!("0x2"))]).await;
    let (gateway, _) = gateway(url, 1, 1, None, 64, 16);
    gateway
        .call("eth_blockNumber", json!([]), Purpose::Head, None)
        .await
        .unwrap();

    let head = gateway.clone();
    let backfill = gateway.clone();
    let head_task = tokio::spawn(async move {
        let started = Instant::now();
        let result = head
            .call("eth_blockNumber", json!([]), Purpose::Head, None)
            .await;
        (result, started.elapsed())
    });
    let backfill_task = tokio::spawn(async move {
        let started = Instant::now();
        let result = backfill
            .call("eth_blockNumber", json!([]), Purpose::Backfill, None)
            .await;
        (result, started.elapsed())
    });
    let (backfill, head) = tokio::join!(backfill_task, head_task);
    let (backfill, backfill_elapsed) = backfill.unwrap();
    let (head, head_elapsed) = head.unwrap();

    assert_eq!(backfill.unwrap_err(), ProviderError::BudgetExhausted);
    assert!(backfill_elapsed < Duration::from_millis(500));
    assert_eq!(head.unwrap(), json!("0x2"));
    assert!(head_elapsed >= Duration::from_millis(800));
    assert!(head_elapsed < Duration::from_millis(1_800));
    assert_eq!(calls.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn head_reorg_and_head_waiters_each_receive_a_token() {
    let (url, calls) = spawn(vec![
        rpc_ok(json!("0x1")),
        rpc_ok(json!("0x2")),
        rpc_ok(json!("0x3")),
        rpc_ok(json!("0x4")),
    ])
    .await;
    let (gateway, _) = gateway(url, 2, 1, None, 64, 16);
    gateway
        .call("eth_blockNumber", json!([]), Purpose::Head, None)
        .await
        .unwrap();

    let first = gateway.clone();
    let second = gateway.clone();
    let third = gateway.clone();
    let head = tokio::spawn(async move {
        first
            .call("eth_blockNumber", json!([]), Purpose::Head, None)
            .await
    });
    let reorg = tokio::spawn(async move {
        second
            .call("eth_blockNumber", json!([]), Purpose::Reorg, None)
            .await
    });
    let later = tokio::spawn(async move {
        third
            .call("eth_blockNumber", json!([]), Purpose::Head, None)
            .await
    });
    let finished = tokio::time::timeout(Duration::from_secs(3), async {
        let (head, reorg, later) = tokio::join!(head, reorg, later);
        (
            head.unwrap().unwrap(),
            reorg.unwrap().unwrap(),
            later.unwrap().unwrap(),
        )
    })
    .await;

    let (head, reorg, later) = finished.expect("waiters still parked after 3s");
    assert_eq!(head, json!("0x2"));
    assert_eq!(reorg, json!("0x3"));
    assert_eq!(later, json!("0x4"));
    assert_eq!(calls.calls.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn daily_budget_resets_on_the_next_utc_day() {
    let (url, calls) = spawn(vec![]).await;
    let (gateway, _) = gateway(url, 20, 20, Some(1), 64, 16);
    let clock = ManualClock::new();
    gateway.set_clock(Arc::new(clock.clone()));

    gateway
        .call("eth_blockNumber", json!([]), Purpose::Head, None)
        .await
        .unwrap();
    let started = Instant::now();
    let error = gateway
        .call("eth_blockNumber", json!([]), Purpose::Head, None)
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::BudgetExhausted);
    assert!(started.elapsed() < Duration::from_millis(400));
    assert_eq!(calls.calls.lock().unwrap().len(), 1);

    clock.advance(86_400_000);
    gateway
        .call("eth_blockNumber", json!([]), Purpose::Head, None)
        .await
        .unwrap();
    assert_eq!(calls.calls.lock().unwrap().len(), 2);
}
