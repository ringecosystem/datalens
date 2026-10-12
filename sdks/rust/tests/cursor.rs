use datalens_sdk::{
    Anchor, B256, BlockRange, ChainName, Dataset, Finality, FinalizedCursor, Observation, PageInfo,
    ProvisionalCursor, QueryResponse, Segment, SegmentSource,
};
use serde_json::{Value, json};

fn hash(number: u64, salt: u8) -> B256 {
    let mut bytes = [0_u8; 32];
    bytes[1] = salt;
    bytes[24..].copy_from_slice(&number.to_be_bytes());
    B256::from(bytes)
}

fn durable(start: u64, end: u64, finality: Finality) -> Segment {
    Segment {
        start,
        end,
        source: SegmentSource::Durable,
        finality,
        anchor: None,
    }
}

fn hot(start: u64, end: u64, anchor: u64, salt: u8) -> Segment {
    Segment {
        start,
        end,
        source: SegmentSource::Hot,
        finality: Finality::Unsafe,
        anchor: Some(Anchor {
            number: anchor,
            hash: hash(anchor, salt),
        }),
    }
}

fn row(number: u64, salt: u8) -> Value {
    json!({"block_number": number, "block_hash": format!("{:#x}", hash(number, salt))})
}

fn response(start: u64, end: u64, segments: Vec<Segment>, rows: Vec<Value>) -> QueryResponse {
    let served = segments
        .first()
        .zip(segments.last())
        .map(|(first, last)| BlockRange::new(first.start, last.end).unwrap());
    QueryResponse {
        chain: ChainName::new("ethereum").unwrap(),
        dataset: Dataset::EvmLogs,
        requested: BlockRange::new(start, end).unwrap(),
        served,
        next_start: None,
        incomplete_reason: None,
        incomplete_detail: None,
        segments,
        rows,
        page: PageInfo::default(),
    }
}

#[test]
fn finalized_cursor_moves_over_contiguous_durable_segments_and_stops_at_hot_data() {
    let mut cursor = FinalizedCursor::new(100);
    let mixed = response(
        100,
        300,
        vec![
            durable(100, 149, Finality::Finalized),
            durable(150, 199, Finality::Safe),
            hot(200, 300, 300, 0),
        ],
        Vec::new(),
    );
    assert_eq!(cursor.advance(&mixed), 200);

    let gap = response(
        250,
        260,
        vec![durable(250, 260, Finality::Safe)],
        Vec::new(),
    );
    assert_eq!(cursor.advance(&gap), 200);

    let reread = response(
        150,
        220,
        vec![durable(150, 220, Finality::Safe)],
        Vec::new(),
    );
    assert_eq!(cursor.advance(&reread), 221);
}

#[test]
fn provisional_cursor_rolls_back_above_the_highest_block_whose_hash_still_matches() {
    let mut cursor = ProvisionalCursor::new(10);
    let first = response(
        10,
        14,
        vec![hot(10, 14, 14, 0)],
        (10..=14).map(|n| row(n, 0)).collect(),
    );
    assert_eq!(cursor.observe(&first), Observation::Consistent);
    assert_eq!((cursor.settled(), cursor.next()), (10, 15));

    let forked = response(
        10,
        14,
        vec![hot(10, 14, 14, 9)],
        (10..=11)
            .map(|n| row(n, 0))
            .chain((12..=14).map(|n| row(n, 9)))
            .collect(),
    );
    assert_eq!(cursor.observe(&forked), Observation::Rollback { from: 12 });
    assert_eq!(cursor.next(), 12);

    let refetched = response(
        12,
        14,
        vec![hot(12, 14, 14, 9)],
        (12..=14).map(|n| row(n, 9)).collect(),
    );
    assert_eq!(cursor.observe(&refetched), Observation::Consistent);
    assert_eq!(cursor.next(), 15);
}

#[test]
fn provisional_cursor_falls_back_to_settled_when_only_the_anchor_changed() {
    let mut cursor = ProvisionalCursor::new(10);
    let empty = response(10, 14, vec![hot(10, 14, 14, 0)], Vec::new());
    assert_eq!(cursor.observe(&empty), Observation::Consistent);
    let forked = response(10, 14, vec![hot(10, 14, 14, 3)], Vec::new());
    assert_eq!(cursor.observe(&forked), Observation::Rollback { from: 10 });
}

#[test]
fn provisional_cursor_settles_blocks_served_as_durable_and_forgets_their_hashes() {
    let mut cursor = ProvisionalCursor::new(10);
    let hot_only = response(
        10,
        14,
        vec![hot(10, 14, 14, 0)],
        (10..=14).map(|n| row(n, 0)).collect(),
    );
    cursor.observe(&hot_only);

    let promoted = response(
        10,
        16,
        vec![durable(10, 12, Finality::Safe), hot(13, 16, 16, 0)],
        (10..=16).map(|n| row(n, 0)).collect(),
    );
    assert_eq!(cursor.observe(&promoted), Observation::Consistent);
    assert_eq!((cursor.settled(), cursor.next()), (13, 17));

    let latest_only_after_safe_moved = response(
        13,
        18,
        vec![hot(15, 18, 18, 0)],
        (15..=18).map(|n| row(n, 0)).collect(),
    );
    assert_eq!(
        cursor.observe(&latest_only_after_safe_moved),
        Observation::Consistent
    );
    assert_eq!(cursor.settled(), 15);

    let reorged_at_15 = response(
        15,
        18,
        vec![hot(15, 18, 18, 4)],
        (15..=18).map(|n| row(n, 4)).collect(),
    );
    assert_eq!(
        cursor.observe(&reorged_at_15),
        Observation::Rollback { from: 15 }
    );
}

#[test]
fn provisional_cursor_rolls_back_when_a_reorg_removes_a_log_while_the_head_moves() {
    let mut cursor = ProvisionalCursor::new(10);
    let first = response(
        10,
        14,
        vec![hot(10, 14, 14, 0)],
        (10..=12).map(|n| row(n, 0)).collect(),
    );
    assert_eq!(cursor.observe(&first), Observation::Consistent);

    let mut removed = response(
        10,
        16,
        vec![hot(10, 16, 16, 5)],
        (10..=11).map(|n| row(n, 0)).collect(),
    );
    removed.page.next_cursor = Some("more".to_owned());
    assert_eq!(
        cursor.observe(&removed),
        Observation::Consistent,
        "a partial page cannot prove a log is gone"
    );

    removed.page.next_cursor = None;
    assert_eq!(cursor.observe(&removed), Observation::Rollback { from: 12 });
}

#[test]
fn provisional_cursor_does_not_flag_an_old_anchor_height_without_rows() {
    let mut cursor = ProvisionalCursor::new(10);
    let first = response(10, 14, vec![hot(10, 14, 14, 0)], vec![row(10, 0)]);
    assert_eq!(cursor.observe(&first), Observation::Consistent);
    let moved = response(10, 16, vec![hot(10, 16, 16, 0)], vec![row(10, 0)]);
    assert_eq!(cursor.observe(&moved), Observation::Consistent);
}
