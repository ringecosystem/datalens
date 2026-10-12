use datalens_core::wire::{
    Anchor, BlockRecord, ErrorBody, FollowInfo, FollowResumeRequest, IncompleteReason, LogRecord,
    PageInfo, QueryRequest, QueryResponse, Segment, SegmentSource, WatchRequest,
};
use datalens_core::{
    Address, B256, BlockRange, BlockRow, Bytes, ChainName, Dataset, Error, Finality, LogRow,
    QueryFinality, TopicFilter,
};
use serde_json::{Value, json};

#[test]
fn query_request_parses_the_plan_example_with_defaults() {
    let request: QueryRequest = serde_json::from_value(json!({
        "chain": "arbitrum",
        "dataset": "evm.logs",
        "selector": {
            "addresses": ["0x00000000000000000000000000000000000000aa"],
            "topics": [["0x1111111111111111111111111111111111111111111111111111111111111111"]]
        },
        "range": { "start": 100, "end": 5000 }
    }))
    .unwrap();
    assert_eq!(request.chain.as_str(), "arbitrum");
    assert_eq!(request.dataset, Dataset::EvmLogs);
    assert_eq!(request.range, BlockRange::new(100, 5000).unwrap());
    assert_eq!(request.finality, QueryFinality::DurableOnly);
    assert_eq!(request.fields, None);
    assert_eq!(request.page, None);
    assert_eq!(request.wait_ms, None);
    let selector = request.selector.unwrap();
    assert_eq!(
        selector.addresses.into_iter().collect::<Vec<_>>(),
        vec![Address::with_last_byte(0xaa)]
    );
    assert_eq!(
        selector.topics[0],
        TopicFilter::one_of([B256::repeat_byte(0x11)]).unwrap()
    );
    assert_eq!(selector.topics[1], TopicFilter::Any);
}

#[test]
fn query_request_rejects_unknown_fields_and_reversed_ranges() {
    let unknown = serde_json::from_value::<QueryRequest>(json!({
        "chain": "base", "dataset": "evm.blocks", "range": {"start": 1, "end": 2}, "kind": "x"
    }));
    assert!(unknown.is_err());
    let reversed = serde_json::from_value::<QueryRequest>(json!({
        "chain": "base", "dataset": "evm.blocks", "range": {"start": 3, "end": 2}
    }));
    assert!(reversed.is_err());
}

#[test]
fn query_response_serializes_with_nulls_and_hex() {
    let row = LogRecord::from(LogRow {
        block_number: 4200,
        log_index: 3,
        block_hash: B256::repeat_byte(0xbb),
        transaction_index: 1,
        transaction_hash: B256::repeat_byte(0xcc),
        address: Address::repeat_byte(0xaa),
        topics: vec![B256::repeat_byte(0x11)],
        data: Bytes::from(vec![0x01, 0x02]),
        finality: Finality::Safe,
    });
    let response = QueryResponse {
        chain: ChainName::new("arbitrum").unwrap(),
        dataset: Dataset::EvmLogs,
        requested: BlockRange::new(100, 5000).unwrap(),
        served: Some(BlockRange::new(100, 4200).unwrap()),
        next_start: Some(4201),
        incomplete_reason: Some(IncompleteReason::Pending),
        incomplete_detail: None,
        segments: vec![Segment {
            start: 100,
            end: 4200,
            source: SegmentSource::Durable,
            finality: Finality::Finalized,
            anchor: None,
        }],
        rows: vec![serde_json::to_value(&row).unwrap()],
        page: PageInfo { next_cursor: None },
    };
    assert_eq!(
        serde_json::to_value(&response).unwrap(),
        json!({
            "chain": "arbitrum",
            "dataset": "evm.logs",
            "requested": {"start": 100, "end": 5000},
            "served": {"start": 100, "end": 4200},
            "next_start": 4201,
            "incomplete_reason": "pending",
            "incomplete_detail": null,
            "segments": [
                {"start": 100, "end": 4200, "source": "durable", "finality": "finalized"}
            ],
            "rows": [{
                "block_number": 4200,
                "block_hash": format!("0x{}", "bb".repeat(32)),
                "transaction_hash": format!("0x{}", "cc".repeat(32)),
                "transaction_index": 1,
                "log_index": 3,
                "address": format!("0x{}", "aa".repeat(20)),
                "topics": [format!("0x{}", "11".repeat(32))],
                "data": "0x0102",
                "finality": "safe"
            }],
            "page": {"next_cursor": null}
        })
    );
    let back: QueryResponse = serde_json::to_value(&response)
        .and_then(serde_json::from_value)
        .unwrap();
    assert_eq!(back, response);
}

#[test]
fn segment_anchor_and_block_record_flatten_header() {
    let segment = Segment {
        start: 4000,
        end: 4200,
        source: SegmentSource::Hot,
        finality: Finality::Unsafe,
        anchor: Some(Anchor {
            number: 4200,
            hash: B256::repeat_byte(0x01),
        }),
    };
    assert_eq!(
        serde_json::to_value(segment).unwrap(),
        json!({
            "start": 4000, "end": 4200, "source": "hot", "finality": "unsafe",
            "anchor": {"number": 4200, "hash": format!("0x{}", "01".repeat(32))}
        })
    );
    let block = BlockRecord::from(BlockRow {
        number: 7,
        hash: B256::repeat_byte(0x07),
        parent_hash: B256::repeat_byte(0x06),
        timestamp: 1_700_000_000,
        finality: Finality::Finalized,
        header: json!({"gasUsed": "0x5208"}),
    });
    assert_eq!(
        serde_json::to_value(block).unwrap(),
        json!({
            "number": 7,
            "hash": format!("0x{}", "07".repeat(32)),
            "parent_hash": format!("0x{}", "06".repeat(32)),
            "timestamp": 1_700_000_000_u64,
            "finality": "finalized",
            "gasUsed": "0x5208"
        })
    );
}

#[test]
fn error_body_uses_the_stable_kind() {
    let body = ErrorBody::from(&Error::QuotaExceeded("too many".into()));
    assert_eq!(
        serde_json::to_value(body).unwrap(),
        json!({"error": {"kind": "quota_exceeded", "message": "too many"}})
    );
    let parsed: ErrorBody =
        serde_json::from_value(json!({"error": {"kind": "forbidden", "message": "no"}})).unwrap();
    assert_eq!(parsed.error.kind, "forbidden");
    let _: Value = serde_json::to_value(parsed).unwrap();
}

#[test]
fn watch_request_takes_a_selector_and_rejects_unknown_fields() {
    let request: WatchRequest = serde_json::from_value(json!({
        "chain": "arbitrum",
        "dataset": "evm.logs",
        "selector": {"addresses": ["0x00000000000000000000000000000000000000aa"]}
    }))
    .unwrap();
    assert_eq!(request.chain.as_str(), "arbitrum");
    assert_eq!(
        request
            .selector
            .unwrap()
            .addresses
            .into_iter()
            .collect::<Vec<_>>(),
        vec![Address::with_last_byte(0xaa)]
    );
    let unknown = serde_json::from_value::<WatchRequest>(json!({
        "chain": "base", "dataset": "evm.blocks", "range": {"start": 1, "end": 2}
    }));
    assert!(unknown.is_err());
}

#[test]
fn follow_info_before_the_first_fetch_has_null_heads() {
    let info = FollowInfo {
        from_block: 101,
        head_block: None,
        head_hash: None,
        promoted_to: None,
    };
    assert_eq!(
        serde_json::to_value(&info).unwrap(),
        json!({"from_block": 101, "head_block": null, "head_hash": null, "promoted_to": null})
    );
}

#[test]
fn below_safe_and_follow_resume_round_trip() {
    assert_eq!(
        serde_json::to_value(IncompleteReason::BelowSafe).unwrap(),
        json!("below_safe")
    );
    assert_eq!(IncompleteReason::BelowSafe.as_str(), "below_safe");
    let request: FollowResumeRequest =
        serde_json::from_value(json!({"chain": "ethereum"})).unwrap();
    assert_eq!(request.chain.as_str(), "ethereum");
    assert!(
        serde_json::from_value::<FollowResumeRequest>(json!({"chain": "ethereum", "x": 1}))
            .is_err()
    );
}
