use std::sync::Arc;
use std::time::Duration;

use datalens_provider::{ManualClock, ProviderError, Purpose, TokenWait};
use prometheus::{Encoder, TextEncoder};
use serde_json::json;
use tokio::time::Instant;

mod common;

use common::{gateway, rpc_ok, spawn};

const UNTIL: TokenWait = TokenWait::UntilToken;

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
        .call("eth_blockNumber", json!([]), Purpose::Head, None, UNTIL)
        .await
        .unwrap();

    let mut tasks = Vec::new();
    for purpose in [Purpose::Head, Purpose::Reorg, Purpose::Head] {
        let gateway = gateway.clone();
        tasks.push(tokio::spawn(async move {
            let started = Instant::now();
            let value = gateway
                .call("eth_blockNumber", json!([]), purpose, None, UNTIL)
                .await
                .unwrap();
            (value, started.elapsed())
        }));
    }
    let finished = tokio::time::timeout(Duration::from_secs(4), async {
        let mut out = Vec::new();
        for task in tasks {
            out.push(task.await.unwrap());
        }
        out
    })
    .await
    .expect("waiters still parked after 4s");
    let (head, head_elapsed) = finished[0].clone();
    let (reorg, reorg_elapsed) = finished[1].clone();
    let (later, later_elapsed) = finished[2].clone();

    let mut values = vec![head, reorg, later];
    values.sort_by_key(|value| value.to_string());
    assert_eq!(values, vec![json!("0x2"), json!("0x3"), json!("0x4")]);
    let mut elapsed = vec![head_elapsed, reorg_elapsed, later_elapsed];
    elapsed.sort();
    assert!(elapsed[0] >= Duration::from_millis(300), "{elapsed:?}");
    assert!(elapsed[2] >= Duration::from_millis(1_200), "{elapsed:?}");
    assert!(elapsed[2] <= Duration::from_millis(3_000), "{elapsed:?}");
    assert!(
        elapsed[1].saturating_sub(elapsed[0]) >= Duration::from_millis(300),
        "{elapsed:?}"
    );
    assert_eq!(calls.calls.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn follow_acquires_while_backfill_is_waiting() {
    let (url, calls) = spawn(vec![
        rpc_ok(json!("0x1")),
        rpc_ok(json!("0x2")),
        rpc_ok(json!("0x3")),
    ])
    .await;
    let (gateway, _) = gateway(url, 5, 1, None, 64, 16);
    gateway
        .call("eth_blockNumber", json!([]), Purpose::Backfill, None, UNTIL)
        .await
        .unwrap();

    let backfill = gateway.clone();
    let backfill_task = tokio::spawn(async move {
        backfill
            .call("eth_blockNumber", json!([]), Purpose::Backfill, None, UNTIL)
            .await
            .unwrap()
    });
    tokio::task::yield_now().await;
    let follow = gateway.clone();
    let follow_task = tokio::spawn(async move {
        let started = Instant::now();
        let value = follow
            .call("eth_blockNumber", json!([]), Purpose::Follow, None, UNTIL)
            .await
            .unwrap();
        (value, started.elapsed())
    });
    let (follow, elapsed) = tokio::time::timeout(Duration::from_secs(2), follow_task)
        .await
        .expect("follow did not get a token")
        .unwrap();
    assert_eq!(follow, json!("0x2"));
    assert!(elapsed >= Duration::from_millis(100), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(600), "{elapsed:?}");
    backfill_task.await.unwrap();
    assert_eq!(calls.calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn fill_deadline_returns_throttled_and_counts_a_rejection() {
    let (url, calls) = spawn(vec![rpc_ok(json!("0x1"))]).await;
    let (gateway, registry) = gateway(url, 1, 1, None, 64, 16);
    gateway
        .call("eth_blockNumber", json!([]), Purpose::Head, None, UNTIL)
        .await
        .unwrap();
    let error = gateway
        .call(
            "eth_blockNumber",
            json!([]),
            Purpose::Backfill,
            None,
            TokenWait::For(Duration::ZERO),
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::Throttled);
    assert_eq!(calls.calls.lock().unwrap().len(), 1);
    let text = metric_text(&registry);
    assert!(
        text.contains(
            "datalens_provider_budget_rejections_total{chain=\"arbitrum\",purpose=\"backfill\"} 1"
        ),
        "{text}"
    );
    assert!(!text.contains("outcome=\"throttled\""), "{text}");
}

#[tokio::test]
async fn daily_budget_resets_on_the_next_utc_day() {
    let (url, calls) = spawn(vec![]).await;
    let (gateway, _) = gateway(url, 20, 20, Some(1), 64, 16);
    let clock = ManualClock::new();
    gateway.set_clock(Arc::new(clock.clone()));

    gateway
        .call("eth_blockNumber", json!([]), Purpose::Head, None, UNTIL)
        .await
        .unwrap();
    let error = gateway
        .call("eth_blockNumber", json!([]), Purpose::Head, None, UNTIL)
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::BudgetExhausted);
    assert_eq!(calls.calls.lock().unwrap().len(), 1);

    clock.advance(86_400_000);
    gateway
        .call("eth_blockNumber", json!([]), Purpose::Head, None, UNTIL)
        .await
        .unwrap();
    assert_eq!(calls.calls.lock().unwrap().len(), 2);
}

fn metric_text(registry: &prometheus::Registry) -> String {
    let mut encoded = Vec::new();
    TextEncoder::new()
        .encode(&registry.gather(), &mut encoded)
        .unwrap();
    String::from_utf8(encoded).unwrap()
}
