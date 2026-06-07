use datalens_sdk::{
    native::QueryResponse,
    safety::{
        BlockAnchor, DataFinality, DataRange, FinalizedCursor, PromotionDecision,
        ProvisionalCursor, extract_cache_segments, plan_promotion,
    },
};
use serde_json::json;

#[test]
fn test_finalized_cursor_rejects_latest_or_provisional_advancement() {
    let mut cursor = FinalizedCursor::new("block", 10);

    let latest = anchor("block", 11, DataFinality::Latest);
    let provisional = anchor("block", 12, DataFinality::Provisional);

    assert!(cursor.advance(latest).is_err());
    assert!(cursor.advance(provisional).is_err());
    assert_eq!(cursor.height(), 10);
}

#[test]
fn test_finalized_cursor_accepts_safe_or_finalized_advancement() {
    let mut cursor = FinalizedCursor::new("block", 10);

    cursor
        .advance(anchor("block", 11, DataFinality::Safe))
        .expect("safe cursor advancement");
    cursor
        .advance(anchor("block", 12, DataFinality::Finalized))
        .expect("finalized cursor advancement");

    assert_eq!(cursor.height(), 12);
}

#[test]
fn test_promotion_planning_below_provisional_range_keeps_provisional() {
    let durable_head = anchor("block", 9, DataFinality::Finalized);
    let provisional_range = DataRange::new("block", 10, 20);
    let provisional_anchor = anchor("block", 20, DataFinality::Latest);

    let plan = plan_promotion(
        Some(&durable_head),
        Some(&provisional_range),
        Some(&provisional_anchor),
    );

    assert!(matches!(
        plan.decision,
        PromotionDecision::KeepProvisional { .. }
    ));
}

#[test]
fn test_promotion_planning_covering_durable_head_promotes() {
    let durable_head = anchor("block", 20, DataFinality::Safe);
    let provisional_range = DataRange::new("block", 10, 20);
    let provisional_anchor = anchor("block", 20, DataFinality::Latest);

    let plan = plan_promotion(
        Some(&durable_head),
        Some(&provisional_range),
        Some(&provisional_anchor),
    );

    assert_eq!(
        plan.decision,
        PromotionDecision::Promote {
            range: provisional_range
        }
    );
}

#[test]
fn test_promotion_planning_missing_anchor_or_unknown_finality_rechecks() {
    let durable_head = anchor("block", 20, DataFinality::Finalized);
    let provisional_range = DataRange::new("block", 10, 20);
    let unknown_anchor = anchor("block", 20, DataFinality::Unknown);

    let missing_anchor_plan = plan_promotion(Some(&durable_head), Some(&provisional_range), None);
    let unknown_finality_plan = plan_promotion(
        Some(&durable_head),
        Some(&provisional_range),
        Some(&unknown_anchor),
    );

    assert!(matches!(
        missing_anchor_plan.decision,
        PromotionDecision::Recheck { .. }
    ));
    assert!(matches!(
        unknown_finality_plan.decision,
        PromotionDecision::Recheck { .. }
    ));
}

#[test]
fn test_query_response_cache_segment_extraction_preserves_metadata() {
    let response = QueryResponse {
        chain: json!({"configuredName": "ethereum"}),
        dataset_key: "evm.logs".to_owned(),
        range: json!({"kind": "block", "start": 10, "end": 12}),
        cache: json!({
            "segments": [{
                "range": {"kind": "block", "start": 10, "end": 12},
                "source": "provider",
                "finality": "latest",
                "anchor": {
                    "range_kind": "block",
                    "height": 12,
                    "block_hash": "0xabc",
                    "parent_hash": "0xdef",
                    "timestamp": 123
                }
            }]
        }),
        rows: json!({"rows": []}),
    };

    let segments = extract_cache_segments(&response);

    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].source.as_deref(), Some("provider"));
    assert_eq!(segments[0].finality, DataFinality::Latest);
    assert_eq!(segments[0].range, Some(DataRange::new("block", 10, 12)));
    assert_eq!(
        segments[0].anchor,
        Some(BlockAnchor {
            range_kind: "block".to_owned(),
            height: 12,
            block_hash: Some("0xabc".to_owned()),
            parent_hash: Some("0xdef".to_owned()),
            timestamp: Some(123),
            finality: DataFinality::Latest,
        })
    );
}

#[test]
fn test_provisional_cursor_accepts_latest_advancement() {
    let mut cursor = ProvisionalCursor::new("slot", 10);

    cursor
        .advance(anchor("slot", 11, DataFinality::Latest))
        .expect("latest cursor advancement");

    assert_eq!(cursor.height(), 11);
}

fn anchor(range_kind: &str, height: u64, finality: DataFinality) -> BlockAnchor {
    BlockAnchor {
        range_kind: range_kind.to_owned(),
        height,
        block_hash: None,
        parent_hash: None,
        timestamp: None,
        finality,
    }
}
