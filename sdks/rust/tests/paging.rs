mod fake;

use std::sync::atomic::Ordering;

use datalens_sdk::{BlockRange, Dataset, FollowEvent, FollowRequest, QueryFinality, QueryRequest};
use futures::StreamExt;
use serde_json::{Value, json};

use fake::{client, ethereum, fake, http};

const HEADS: &str = r#"{"chain":"ethereum","latest":17,"latest_hash":"0x00000000000000000000000000000000000000000000000000000000000000aa","safe":9,"finalized":4}"#;
const HEADS_LATER: &str = r#"{"chain":"ethereum","latest":20,"latest_hash":"0x00000000000000000000000000000000000000000000000000000000000000bb","safe":13,"finalized":4}"#;

fn hash(salt: u8) -> String {
    format!("0x{}", format!("{salt:02x}").repeat(32))
}

fn log(block: u64, index: u64, salt: u8) -> Value {
    json!({"block_number": block, "log_index": index, "block_hash": hash(salt)})
}

fn body(
    requested: (u64, u64),
    served: Option<(u64, u64)>,
    reason: Option<&str>,
    rows: Vec<Value>,
    cursor: Option<&str>,
) -> String {
    let segments: Vec<Value> = served
        .iter()
        .map(|(start, end)| {
            json!({"start": start, "end": end, "source": "hot", "finality": "unsafe",
                   "anchor": {"number": end, "hash": hash(0xee)}})
        })
        .collect();
    let next_start = match (served, reason) {
        (Some((_, end)), _) if end < requested.1 => json!(end + 1),
        _ => Value::Null,
    };
    json!({
        "chain": "ethereum",
        "dataset": "evm.logs",
        "requested": {"start": requested.0, "end": requested.1},
        "served": served.map(|(start, end)| json!({"start": start, "end": end})),
        "next_start": next_start,
        "incomplete_reason": reason,
        "segments": segments,
        "rows": rows,
        "page": {"next_cursor": cursor},
    })
    .to_string()
}

fn ok(body: String) -> String {
    http("200 OK", "", &body)
}

#[tokio::test]
async fn query_all_reads_again_when_a_later_page_disagrees_on_a_block_hash() {
    let (base, count) = fake(|n| match n {
        0 | 2 => ok(body(
            (10, 11),
            Some((10, 11)),
            None,
            vec![log(10, 0, 1)],
            Some("c"),
        )),
        1 => ok(body(
            (10, 11),
            Some((10, 11)),
            None,
            vec![log(10, 1, 2)],
            None,
        )),
        _ => ok(body(
            (10, 11),
            Some((10, 11)),
            None,
            vec![log(10, 1, 1)],
            None,
        )),
    })
    .await;
    let request = QueryRequest {
        chain: ethereum(),
        dataset: Dataset::EvmLogs,
        selector: None,
        range: BlockRange::new(10, 11).unwrap(),
        finality: QueryFinality::LatestOnly,
        fields: None,
        page: None,
        wait_ms: None,
    };
    let merged = client(&base).query_all(&request).await.unwrap();
    assert_eq!(merged.rows, vec![log(10, 0, 1), log(10, 1, 1)]);
    assert_eq!(count.load(Ordering::SeqCst), 4);
}

#[tokio::test]
async fn query_all_gives_up_after_max_attempts_of_disagreeing_pages() {
    let (base, count) = fake(|n| {
        let salt = (n % 2) as u8 + 1;
        let cursor = (n % 2 == 0).then_some("c");
        ok(body(
            (10, 11),
            Some((10, 11)),
            None,
            vec![log(10, n as u64 % 2, salt)],
            cursor,
        ))
    })
    .await;
    let request = QueryRequest {
        chain: ethereum(),
        dataset: Dataset::EvmLogs,
        selector: None,
        range: BlockRange::new(10, 11).unwrap(),
        finality: QueryFinality::LatestOnly,
        fields: None,
        page: None,
        wait_ms: None,
    };
    let error = client(&base).query_all(&request).await.unwrap_err();
    assert!(
        matches!(error, datalens_sdk::Error::Inconsistent(_)),
        "{error:?}"
    );
    assert!(error.is_retryable());
    assert_eq!(count.load(Ordering::SeqCst), 6);
}

fn summary(event: FollowEvent) -> String {
    match event {
        FollowEvent::Batch(batch) => format!("batch {:?}", batch.served.unwrap()),
        FollowEvent::Skipped { blocks } => format!("skipped {blocks:?}"),
        FollowEvent::CaughtUp { head } => format!("caught_up {head}"),
        FollowEvent::Stalled {
            next_start, reason, ..
        } => format!("stalled {next_start} {reason:?}"),
    }
}

fn latest_only(start: u64, end: u64) -> FollowRequest {
    let mut request = FollowRequest::logs(ethereum(), Default::default(), start);
    request.finality = QueryFinality::LatestOnly;
    request.end = Some(end);
    request
}

#[tokio::test]
async fn latest_only_follow_reports_the_blocks_the_server_left_below_a_moved_safe_head() {
    let (base, count) = fake(|n| match n {
        0 => ok(HEADS.to_owned()),
        _ => ok(body(
            (10, 14),
            Some((12, 14)),
            Some("below_safe"),
            vec![],
            None,
        )),
    })
    .await;
    let events: Vec<_> = client(&base)
        .follow(latest_only(10, 14))
        .map(|event| summary(event.unwrap()))
        .collect()
        .await;
    assert_eq!(
        events,
        vec![
            format!("skipped {:?}", BlockRange::new(10, 11).unwrap()),
            format!("batch {:?}", BlockRange::new(12, 14).unwrap()),
        ]
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn latest_only_follow_from_below_safe_names_the_skipped_span_and_refreshes_heads() {
    let (base, count) = fake(|n| match n {
        0 => ok(HEADS.to_owned()),
        1 => ok(body((10, 12), Some((10, 12)), None, vec![], None)),
        2 => ok(HEADS_LATER.to_owned()),
        _ => ok(body((14, 14), Some((14, 14)), None, vec![], None)),
    })
    .await;
    let mut request = latest_only(0, 14);
    request.chunk_blocks = 3;
    let events: Vec<_> = client(&base)
        .follow(request)
        .map(|event| summary(event.unwrap()))
        .collect()
        .await;
    assert_eq!(
        events,
        vec![
            format!("skipped {:?}", BlockRange::new(0, 9).unwrap()),
            format!("batch {:?}", BlockRange::new(10, 12).unwrap()),
            format!("skipped {:?}", BlockRange::new(13, 13).unwrap()),
            format!("batch {:?}", BlockRange::new(14, 14).unwrap()),
        ]
    );
    assert_eq!(count.load(Ordering::SeqCst), 4);
}
