use std::collections::HashMap;

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

use datalens_core::{Address, B256, BlockRange, BlockRow, Finality};
use datalens_evm::{BlockRef, EvmClient, HeaderBatchSizeError, TxSender};
use datalens_provider::{ProviderError, Purpose, TokenWait};
use serde_json::{Value, json};

mod common;

use common::client;
use common::mock::{ItemOutcome, MockRpc};

const UNTIL: TokenWait = TokenWait::UntilToken;

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
            UNTIL,
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
async fn http_403_is_retried_and_then_accepted() {
    let mock = MockRpc::spawn().await;
    mock.fail_next_http(1, 403, None);
    mock.set_handler(|item| {
        let tag = item["params"][0].as_str().unwrap();
        ItemOutcome::Result(header(number_from_tag(tag)))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let rows = evm
        .block_headers(
            BlockRange::new(1, 20).unwrap(),
            Purpose::Follow,
            None,
            UNTIL,
            Finality::Unsafe,
        )
        .await
        .unwrap();
    let lens: Vec<usize> = mock.calls().iter().map(batch_len).collect();
    assert_eq!(lens, vec![20, 20]);
    assert_eq!(rows.len(), 20);
}

#[tokio::test]
async fn http_403_three_times_stays_transient_and_does_not_halve() {
    let mock = MockRpc::spawn().await;
    mock.fail_next_http(3, 403, None);
    mock.set_handler(|item| {
        let tag = item["params"][0].as_str().unwrap();
        ItemOutcome::Result(header(number_from_tag(tag)))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let error = evm
        .block_headers(
            BlockRange::new(1, 20).unwrap(),
            Purpose::Follow,
            None,
            UNTIL,
            Finality::Unsafe,
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::Transient { status: Some(403) });
    let lens: Vec<usize> = mock.calls().iter().map(batch_len).collect();
    assert_eq!(lens, vec![20, 20, 20]);
}

#[tokio::test]
async fn http_503_retries_three_times_and_does_not_halve() {
    let mock = MockRpc::spawn().await;
    mock.fail_next_http(3, 503, None);
    mock.set_handler(|item| {
        let tag = item["params"][0].as_str().unwrap();
        ItemOutcome::Result(header(number_from_tag(tag)))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let error = evm
        .block_headers(
            BlockRange::new(1, 20).unwrap(),
            Purpose::Follow,
            None,
            UNTIL,
            Finality::Unsafe,
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::Transient { status: Some(503) });
    let lens: Vec<usize> = mock.calls().iter().map(batch_len).collect();
    assert_eq!(lens, vec![20, 20, 20]);
}

#[tokio::test]
async fn http_413_halves_until_the_batch_is_accepted() {
    let mock = MockRpc::spawn().await;
    mock.fail_next_http(1, 413, None);
    mock.set_handler(|item| {
        let tag = item["params"][0].as_str().unwrap();
        ItemOutcome::Result(header(number_from_tag(tag)))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let rows = evm
        .block_headers(
            BlockRange::new(1, 20).unwrap(),
            Purpose::Follow,
            None,
            UNTIL,
            Finality::Unsafe,
        )
        .await
        .unwrap();
    let lens: Vec<usize> = mock.calls().iter().map(batch_len).collect();
    assert_eq!(lens, vec![20, 10, 10]);
    assert_eq!(rows.len(), 20);
    assert_eq!(rows[0].number, 1);
    assert_eq!(rows[19].number, 20);
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
        .block_by_number(
            BlockRef::Latest,
            Purpose::Reorg,
            None,
            UNTIL,
            Finality::Unsafe,
        )
        .await
        .unwrap();
    let row = evm
        .block_by_number(
            BlockRef::Number(7),
            Purpose::Reorg,
            None,
            UNTIL,
            Finality::Unsafe,
        )
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
        ItemOutcome::Result(json!({
            "from": from,
            "blockNumber": "0x10",
            "blockHash": "0x00000000000000000000000000000000000000000000000000000000000000aa",
        }))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let left: B256 = format!("0x{:064x}", 1u64).parse().unwrap();
    let right: B256 = format!("0x{:064x}", 2u64).parse().unwrap();
    let missing: B256 = format!("0x{:064x}", 3u64).parse().unwrap();
    let senders = evm
        .transaction_senders(
            &[left, right, missing],
            Purpose::QueryFill,
            Some("ormp"),
            UNTIL,
        )
        .await
        .unwrap();
    let sender = |text: &str| TxSender {
        address: text.parse::<Address>().unwrap(),
        block_number: 16,
        block_hash: "0x00000000000000000000000000000000000000000000000000000000000000aa"
            .parse()
            .unwrap(),
    };
    assert_eq!(senders.failed, Vec::<B256>::new());
    assert_eq!(
        senders.senders,
        HashMap::from([
            (left, sender("0x1111111111111111111111111111111111111111")),
            (right, sender("0x2222222222222222222222222222222222222222")),
        ])
    );
    assert_eq!(mock.calls().len(), 1);
    assert_eq!(batch_len(&mock.calls()[0]), 3);
}

#[tokio::test]
async fn transaction_senders_dedupes_hashes_and_skips_pending() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|item| {
        let hash = item["params"][0].as_str().unwrap();
        if hash.ends_with('2') {
            return ItemOutcome::Result(json!({
                "from": "0x2222222222222222222222222222222222222222",
                "blockNumber": Value::Null,
                "blockHash": Value::Null,
            }));
        }
        ItemOutcome::Result(json!({
            "from": "0x1111111111111111111111111111111111111111",
            "blockNumber": "0x10",
            "blockHash": "0x00000000000000000000000000000000000000000000000000000000000000aa",
        }))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let left: B256 = format!("0x{:064x}", 1u64).parse().unwrap();
    let pending: B256 = format!("0x{:064x}", 2u64).parse().unwrap();
    let senders = evm
        .transaction_senders(&[left, left, pending], Purpose::Reorg, None, UNTIL)
        .await
        .unwrap();
    assert_eq!(batch_len(&mock.calls()[0]), 2);
    assert_eq!(senders.failed, Vec::<B256>::new());
    assert_eq!(senders.senders.len(), 1);
    assert_eq!(
        senders.senders[&left].address,
        "0x1111111111111111111111111111111111111111"
            .parse::<Address>()
            .unwrap()
    );
    assert!(!senders.senders.contains_key(&pending));
}

#[tokio::test]
async fn a_failed_sender_item_is_retried_and_kept_apart_from_successes() {
    let mock = MockRpc::spawn().await;
    let hits = AtomicUsize::new(0);
    mock.set_handler(move |item| {
        let hash = item["params"][0].as_str().unwrap();
        if hash.ends_with('2') && hits.fetch_add(1, Ordering::SeqCst) == 0 {
            return ItemOutcome::Error {
                code: -32_000,
                message: "temporary".to_owned(),
            };
        }
        ItemOutcome::Result(json!({
            "from": "0x1111111111111111111111111111111111111111",
            "blockNumber": "0x4",
            "blockHash": "0x00000000000000000000000000000000000000000000000000000000000000bb",
        }))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let left: B256 = format!("0x{:064x}", 1u64).parse().unwrap();
    let right: B256 = format!("0x{:064x}", 2u64).parse().unwrap();
    let senders = evm
        .transaction_senders(&[left, right], Purpose::Reorg, None, UNTIL)
        .await
        .unwrap();
    assert_eq!(mock.calls().len(), 2);
    assert_eq!(batch_len(&mock.calls()[1]), 1);
    assert!(senders.failed.is_empty());
    assert!(senders.senders.contains_key(&left));
    assert!(senders.senders.contains_key(&right));
}

#[tokio::test]
async fn a_mismatched_block_number_is_fatal() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|_| ItemOutcome::Result(header(9)));
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let error = evm
        .block_headers(
            BlockRange::new(7, 7).unwrap(),
            Purpose::Reorg,
            None,
            UNTIL,
            Finality::Unsafe,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ProviderError::fatal("block number does not match the request")
    );
}

#[tokio::test]
async fn header_json_drops_transactions_uncles_and_withdrawals() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|_| {
        let mut value = header(4);
        value["transactions"] = json!(["0xabc"]);
        value["uncles"] = json!(["0xdef"]);
        value["withdrawals"] = json!([{"index": "0x1"}]);
        ItemOutcome::Result(value)
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let rows = evm
        .block_headers(
            BlockRange::new(4, 4).unwrap(),
            Purpose::Follow,
            None,
            UNTIL,
            Finality::Safe,
        )
        .await
        .unwrap();
    assert_eq!(
        rows[0].header,
        json!({"miner": "0x00000000000000000000000000000000000000aa"})
    );
}

#[tokio::test]
async fn an_empty_quantity_is_rejected() {
    let mock = MockRpc::spawn().await;
    mock.set_handler(|_| {
        let mut value = header(1);
        value["number"] = json!("0x");
        ItemOutcome::Result(value)
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let error = evm
        .block_by_number(
            BlockRef::Number(1),
            Purpose::Reorg,
            None,
            UNTIL,
            Finality::Unsafe,
        )
        .await
        .unwrap_err();
    assert_eq!(error, ProviderError::fatal("invalid number"));
}

#[tokio::test]
async fn header_batch_size_outside_20_to_50_is_rejected() {
    let evm = client("http://127.0.0.1:1", 32, 20, 1000, 16);
    let built = EvmClient::new(evm.gateway().clone(), NonZeroUsize::new(1).unwrap(), 10);
    assert_eq!(built.err(), Some(HeaderBatchSizeError(10)));
}

#[tokio::test]
async fn a_batch_level_limit_error_halves_the_header_batch() {
    let mock = MockRpc::spawn().await;
    mock.fail_next_batch(-32_000, "batch limit exceeded");
    mock.set_handler(|item| {
        let tag = item["params"][0].as_str().unwrap();
        ItemOutcome::Result(header(number_from_tag(tag)))
    });
    let evm = client(mock.url(), 32, 20, 1000, 16);
    let rows = evm
        .block_headers(
            BlockRange::new(1, 20).unwrap(),
            Purpose::Follow,
            None,
            UNTIL,
            Finality::Safe,
        )
        .await
        .unwrap();
    let lens: Vec<usize> = mock.calls().iter().map(batch_len).collect();
    assert_eq!(lens, vec![20, 10, 10]);
    assert_eq!(rows.len(), 20);
    assert_eq!(rows[0].number, 1);
    assert_eq!(rows[19].number, 20);
}
