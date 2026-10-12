use std::collections::HashMap;

use datalens_core::{Address, B256, BlockRange, BlockRow, Finality};
use datalens_evm::BlockRef;
use datalens_evm::mock::{ItemOutcome, MockRpc};
use datalens_provider::Purpose;
use serde_json::{Value, json};

mod common;

use common::client;

fn header(number: u64) -> Value {
    json!({
        "number": format!("0x{number:x}"),
        "hash": format!("0x{number:064x}"),
        "parentHash": format!("0x{0:064x}", number.saturating_sub(1)),
        "timestamp": format!("0x{:x}", 1_700_000_000 + number),
        "miner": "0x00000000000000000000000000000000000000aa",
    })
}

fn expected_header(number: u64, finality: Finality) -> BlockRow {
    BlockRow {
        number,
        hash: format!("0x{number:064x}").parse().unwrap(),
        parent_hash: format!("0x{:064x}", number.saturating_sub(1))
            .parse()
            .unwrap(),
        timestamp: 1_700_000_000 + number,
        finality,
        header: json!({"miner": "0x00000000000000000000000000000000000000aa"}),
    }
}

fn number_from_tag(tag: &str) -> u64 {
    u64::from_str_radix(tag.trim_start_matches("0x"), 16).unwrap()
}

fn batch_len(body: &Value) -> usize {
    body.as_array().map(Vec::len).unwrap_or(1)
}

#[tokio::test]
async fn block_headers_batches_one_hundred_blocks() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|item| {
        let tag = item["params"][0].as_str().unwrap();
        ItemOutcome::Result(header(number_from_tag(tag)))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let rows = evm
        .block_headers(
            BlockRange::new(1, 100).unwrap(),
            Purpose::Follow,
            None,
            Finality::Safe,
        )
        .await
        .unwrap();
    assert_eq!(mock.calls().len(), 5);
    assert!(mock.calls().iter().all(|call| batch_len(call) == 20));
    assert_eq!(rows.len(), 100);
    assert_eq!(rows[0], expected_header(1, Finality::Safe));
    assert_eq!(rows[99], expected_header(100, Finality::Safe));
}

#[tokio::test]
async fn http_502_halves_a_header_batch() {
    let mock = MockRpc::spawn().await;
    mock.fail_next_http(3, 502, None);
    mock.set_handler(|item| {
        let tag = item["params"][0].as_str().unwrap();
        ItemOutcome::Result(header(number_from_tag(tag)))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let rows = evm
        .block_headers(
            BlockRange::new(1, 100).unwrap(),
            Purpose::Follow,
            None,
            Finality::Unsafe,
        )
        .await
        .unwrap();
    let lens: Vec<usize> = mock.calls().iter().map(batch_len).collect();
    assert_eq!(lens, vec![20, 20, 20, 10, 10, 20, 20, 20, 20]);
    assert_eq!(rows.len(), 100);
    assert_eq!(rows[0].number, 1);
    assert_eq!(rows[9].number, 10);
    assert_eq!(rows[99].number, 100);
}

#[tokio::test]
async fn block_by_number_returns_a_header_or_none() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|item| {
        let tag = item["params"][0].as_str().unwrap();
        if tag == "latest" {
            ItemOutcome::Result(Value::Null)
        } else {
            ItemOutcome::Result(header(number_from_tag(tag)))
        }
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let missing = evm
        .block_by_number(BlockRef::Latest, Purpose::Reorg, None, Finality::Unsafe)
        .await
        .unwrap();
    let row = evm
        .block_by_number(BlockRef::Number(7), Purpose::Reorg, None, Finality::Unsafe)
        .await
        .unwrap();
    assert_eq!(missing, None);
    assert_eq!(row, Some(expected_header(7, Finality::Unsafe)));
    assert_eq!(mock.calls().len(), 2);
    assert_eq!(mock.calls()[0]["params"], json!(["latest", false]));
    assert_eq!(mock.calls()[1]["params"], json!(["0x7", false]));
}

#[tokio::test]
async fn transaction_senders_maps_hash_to_from() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|item| {
        let hash = item["params"][0].as_str().unwrap();
        let from = match hash.chars().last() {
            Some('1') => "0x1111111111111111111111111111111111111111",
            Some('2') => "0x2222222222222222222222222222222222222222",
            _ => return ItemOutcome::Result(Value::Null),
        };
        ItemOutcome::Result(json!({"from": from}))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let left: B256 = format!("0x{:064x}", 1u64).parse().unwrap();
    let right: B256 = format!("0x{:064x}", 2u64).parse().unwrap();
    let missing: B256 = format!("0x{:064x}", 3u64).parse().unwrap();
    let senders = evm
        .transaction_senders(&[left, right, missing], Purpose::QueryFill, Some("ormp"))
        .await
        .unwrap();
    let address = |text: &str| text.parse::<Address>().unwrap();
    assert_eq!(
        senders,
        HashMap::from([
            (left, address("0x1111111111111111111111111111111111111111")),
            (right, address("0x2222222222222222222222222222222222222222")),
        ])
    );
    assert_eq!(mock.calls().len(), 1);
    assert_eq!(batch_len(&mock.calls()[0]), 3);
}
