use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datalens_core::{Address, B256, BlockRange, Bytes, Dataset, Finality, LogRow, LogSelector};
use datalens_evm::mock::{ItemOutcome, MockRpc};
use datalens_provider::{ManualClock, ProviderError, Purpose};
use serde_json::{Value, json};

mod common;

use common::{block_span, client};

const TOPIC: &str = "0x00000000000000000000000000000000000000000000000000000000000000aa";
const TX: &str = "0x00000000000000000000000000000000000000000000000000000000000000bb";
const BLOCK: &str = "0x00000000000000000000000000000000000000000000000000000000000000cc";

fn log_json(address: &str, index: u64, removed: bool) -> Value {
    json!({
        "address": address,
        "topics": [TOPIC],
        "data": "0x01",
        "blockNumber": "0xa",
        "transactionHash": TX,
        "transactionIndex": "0x1",
        "blockHash": BLOCK,
        "logIndex": format!("0x{index:x}"),
        "removed": removed
    })
}

fn expected_row(address: &str, index: u32) -> LogRow {
    LogRow {
        block_number: 10,
        log_index: index,
        block_hash: BLOCK.parse().unwrap(),
        transaction_index: 1,
        transaction_hash: TX.parse().unwrap(),
        address: address.parse().unwrap(),
        topics: vec![TOPIC.parse::<B256>().unwrap()],
        data: Bytes::from(vec![0x01]),
        finality: Finality::Safe,
    }
}

#[tokio::test]
async fn get_logs_normalizes_rows_and_drops_removed() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|item| {
        assert_eq!(item["method"], "eth_getLogs");
        ItemOutcome::Result(json!([
            log_json("0x1111111111111111111111111111111111111111", 0, false),
            log_json("0x1111111111111111111111111111111111111111", 1, true),
        ]))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let rows = evm
        .get_logs(
            &LogSelector::default(),
            BlockRange::new(10, 10).unwrap(),
            Purpose::Follow,
            Some("degov-live"),
            Finality::Safe,
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![expected_row(
            "0x1111111111111111111111111111111111111111",
            0
        )]
    );
    let call = &mock.calls()[0];
    assert!(call["params"][0].get("address").is_none());
    assert!(call["params"][0].get("topics").is_none());
    assert_eq!(call["params"][0]["fromBlock"], "0xa");
    assert_eq!(call["params"][0]["toBlock"], "0xa");
}

#[tokio::test]
async fn get_logs_splits_by_max_addresses() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|item| {
        let filter = &item["params"][0];
        let addresses: Vec<String> = match &filter["address"] {
            Value::String(one) => vec![one.clone()],
            Value::Array(many) => many
                .iter()
                .map(|value| value.as_str().unwrap().to_owned())
                .collect(),
            _ => Vec::new(),
        };
        let logs: Vec<Value> = addresses
            .iter()
            .enumerate()
            .map(|(index, address)| log_json(address, index as u64, false))
            .collect();
        ItemOutcome::Result(Value::Array(logs))
    });
    let evm = client(mock.url(), 2, 20, 1000, 16);
    let addresses = [
        "0x1111111111111111111111111111111111111111",
        "0x2222222222222222222222222222222222222222",
        "0x3333333333333333333333333333333333333333",
    ];
    let selector = LogSelector {
        addresses: addresses
            .iter()
            .map(|address| address.parse::<Address>().unwrap())
            .collect::<BTreeSet<_>>(),
        topics: Default::default(),
    };
    let rows = evm
        .get_logs(
            &selector,
            BlockRange::new(10, 10).unwrap(),
            Purpose::QueryFill,
            None,
            Finality::Safe,
        )
        .await
        .unwrap();
    assert_eq!(mock.calls().len(), 2);
    let first = mock.calls()[0]["params"][0]["address"].clone();
    let second = mock.calls()[1]["params"][0]["address"].clone();
    assert_eq!(
        first,
        json!([
            "0x1111111111111111111111111111111111111111",
            "0x2222222222222222222222222222222222222222"
        ])
    );
    assert_eq!(second, json!("0x3333333333333333333333333333333333333333"));
    assert_eq!(
        rows,
        vec![
            expected_row("0x1111111111111111111111111111111111111111", 0),
            expected_row("0x2222222222222222222222222222222222222222", 1),
            expected_row("0x3333333333333333333333333333333333333333", 0),
        ]
    );
}

#[tokio::test]
async fn header_not_found_does_not_shrink_the_log_range() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|_| ItemOutcome::Error {
        code: -32_000,
        message: "header not found".to_owned(),
    });
    let evm = client(mock.url(), 32, 20, 32, 16);
    let error = evm
        .get_logs(
            &LogSelector::default(),
            BlockRange::new(0, 31).unwrap(),
            Purpose::Follow,
            None,
            Finality::Unsafe,
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::NotReady);
    assert_eq!(evm.gateway().range_blocks(Dataset::EvmLogs), 32);
    assert_eq!(mock.calls().len(), 1);
}

#[tokio::test]
async fn limit_halves_and_eight_successes_double_back() {
    let mock = MockRpc::spawn().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    mock.set_handler(move |_| {
        let seen = counter.fetch_add(1, Ordering::SeqCst);
        if seen == 0 {
            ItemOutcome::Error {
                code: -32_000,
                message: "query returned more than 10000 results".to_owned(),
            }
        } else {
            ItemOutcome::Result(json!([]))
        }
    });
    let evm = client(mock.url(), 32, 20, 32, 16);
    let rows = evm
        .get_logs(
            &LogSelector::default(),
            BlockRange::new(0, 127).unwrap(),
            Purpose::Backfill,
            None,
            Finality::Finalized,
        )
        .await
        .unwrap();
    assert_eq!(rows, Vec::<LogRow>::new());
    let spans: Vec<u64> = mock.calls().iter().map(block_span).collect();
    assert_eq!(spans, vec![32, 16, 16, 16, 16, 16, 16, 16, 16]);
    assert_eq!(evm.gateway().range_blocks(Dataset::EvmLogs), 32);
}

#[tokio::test]
async fn limit_at_the_floor_is_not_split_further() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|_| ItemOutcome::Error {
        code: -32_005,
        message: "query returned more than 10000 results".to_owned(),
    });
    let evm = client(mock.url(), 32, 20, 32, 16);
    let error = evm
        .get_logs(
            &LogSelector::default(),
            BlockRange::new(0, 31).unwrap(),
            Purpose::Backfill,
            None,
            Finality::Finalized,
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::Limit);
    let spans: Vec<u64> = mock.calls().iter().map(block_span).collect();
    assert_eq!(spans, vec![32, 16]);
    assert_eq!(evm.gateway().range_blocks(Dataset::EvmLogs), 16);
}

#[tokio::test]
async fn adaptive_range_returns_to_max_after_ttl() {
    let mock = MockRpc::spawn().await;
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    mock.set_handler(move |_| {
        let seen = counter.fetch_add(1, Ordering::SeqCst);
        if seen == 0 {
            ItemOutcome::Error {
                code: -32_000,
                message: "block range too large".to_owned(),
            }
        } else {
            ItemOutcome::Result(json!([]))
        }
    });
    let evm = client(mock.url(), 32, 20, 32, 16);
    let clock = ManualClock::new();
    evm.gateway().set_clock(Arc::new(clock.clone()));
    evm.get_logs(
        &LogSelector::default(),
        BlockRange::new(0, 31).unwrap(),
        Purpose::Follow,
        None,
        Finality::Safe,
    )
    .await
    .unwrap();
    assert_eq!(evm.gateway().range_blocks(Dataset::EvmLogs), 16);
    clock.advance(10 * 60 * 1000);
    assert_eq!(evm.gateway().range_blocks(Dataset::EvmLogs), 32);
}
