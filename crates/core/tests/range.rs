use std::num::NonZeroU64;

use datalens_core::BlockRange;

fn range(start: u64, end: u64) -> BlockRange {
    BlockRange::new(start, end).unwrap()
}

fn pairs(ranges: &[BlockRange]) -> Vec<(u64, u64)> {
    ranges.iter().map(|r| (r.start(), r.end())).collect()
}

fn chunk(blocks: u64) -> NonZeroU64 {
    NonZeroU64::new(blocks).unwrap()
}

#[test]
fn new_rejects_start_after_end() {
    let error = BlockRange::new(5, 4).unwrap_err();
    assert_eq!(error.kind(), "invalid_input");
    assert_eq!(
        error.to_string(),
        "invalid input: block range start 5 is after end 4"
    );
}

#[test]
fn new_rejects_end_beyond_bigint_half_open_bound() {
    assert!(BlockRange::new(0, BlockRange::MAX_END).is_ok());
    assert!(BlockRange::new(0, BlockRange::MAX_END + 1).is_err());
}

#[test]
fn single_block_range_has_len_one() {
    assert_eq!(range(7, 7).len(), 1);
    assert_eq!(range(100, 5000).len(), 4901);
}

#[test]
fn contains_is_inclusive() {
    let r = range(10, 20);
    assert!(!r.contains(9));
    assert!(r.contains(10));
    assert!(r.contains(20));
    assert!(!r.contains(21));
}

#[test]
fn intersect_overlapping_touching_and_disjoint() {
    assert_eq!(range(0, 10).intersect(&range(5, 15)), Some(range(5, 10)));
    assert_eq!(range(0, 10).intersect(&range(10, 15)), Some(range(10, 10)));
    assert_eq!(range(0, 10).intersect(&range(11, 15)), None);
    assert_eq!(range(3, 4).intersect(&range(0, 100)), Some(range(3, 4)));
}

#[test]
fn align_chunks_returns_every_full_cell_the_range_overlaps() {
    assert_eq!(
        pairs(&range(95, 312).align_chunks(chunk(100))),
        vec![(0, 99), (100, 199), (200, 299), (300, 399)]
    );
    assert_eq!(
        pairs(&range(100, 100).align_chunks(chunk(100))),
        vec![(100, 199)]
    );
}

#[test]
fn align_chunks_exact_grid_range_is_one_chunk() {
    assert_eq!(
        pairs(&range(10_000, 19_999).align_chunks(chunk(10_000))),
        vec![(10_000, 19_999)]
    );
}

#[test]
fn align_chunks_inside_one_cell_returns_that_whole_cell() {
    assert_eq!(
        pairs(&range(10_005, 10_010).align_chunks(chunk(10_000))),
        vec![(10_000, 19_999)]
    );
    assert_eq!(
        pairs(&range(10_020, 10_030).align_chunks(chunk(10_000))),
        vec![(10_000, 19_999)]
    );
}

#[test]
fn align_chunks_of_size_one_yields_every_block() {
    assert_eq!(
        pairs(&range(3, 5).align_chunks(chunk(1))),
        vec![(3, 3), (4, 4), (5, 5)]
    );
}

#[test]
fn align_chunks_near_max_end_does_not_overflow() {
    let end = BlockRange::MAX_END;
    assert_eq!(
        pairs(&range(end - 2, end).align_chunks(chunk(u64::MAX))),
        vec![(0, end)]
    );
}

#[test]
fn serde_round_trip_and_validation() {
    let r: BlockRange = serde_json::from_str(r#"{"start":100,"end":5000}"#).unwrap();
    assert_eq!(r, range(100, 5000));
    assert_eq!(
        serde_json::to_string(&r).unwrap(),
        r#"{"start":100,"end":5000}"#
    );
    assert!(serde_json::from_str::<BlockRange>(r#"{"start":2,"end":1}"#).is_err());
    assert!(serde_json::from_str::<BlockRange>(r#"{"start":1,"end":2,"x":0}"#).is_err());
}
