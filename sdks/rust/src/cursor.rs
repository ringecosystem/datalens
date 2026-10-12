use std::collections::BTreeMap;

use datalens_core::wire::{QueryResponse, SegmentSource};
use datalens_core::{B256, Finality};
use serde_json::Value;

/// Checkpoint that only moves over durable data: `durable` segments at `safe` or
/// `finalized`, contiguous from [`FinalizedCursor::next`]. Rows below `next` will not change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedCursor {
    next: u64,
}

impl FinalizedCursor {
    pub fn new(next: u64) -> Self {
        Self { next }
    }

    /// First block not yet durable.
    pub fn next(&self) -> u64 {
        self.next
    }

    /// Moves past the durable segments that continue from `next` and returns the new `next`.
    /// It stops at a hot segment or a gap.
    pub fn advance(&mut self, response: &QueryResponse) -> u64 {
        for segment in &response.segments {
            let durable =
                segment.source == SegmentSource::Durable && segment.finality >= Finality::Safe;
            if !durable || segment.start > self.next {
                break;
            }
            self.next = self.next.max(segment.end + 1);
        }
        self.next
    }
}

/// What a re-read of hot data says about the data read before.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observation {
    /// Every block hash in the response matches what was seen before.
    Consistent,
    /// A block changed hash, so a reorg replaced blocks from `from` on. Drop provisional
    /// data at or above `from`, discard this response, and query again from `from`.
    Rollback { from: u64 },
}

/// Tracks hot data across repeated reads of the chain tail. It remembers the hash of every
/// hot block it saw, from segment anchors and from row `block_hash`, and reports a rollback
/// when a later response disagrees.
///
/// A hot segment's anchor is the follow head, often above the rows. A reorg shows as a new
/// hash at a height seen before, so re-read the tail from [`ProvisionalCursor::settled`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvisionalCursor {
    settled: u64,
    next: u64,
    seen: BTreeMap<u64, B256>,
}

impl ProvisionalCursor {
    pub fn new(start: u64) -> Self {
        Self {
            settled: start,
            next: start,
            seen: BTreeMap::new(),
        }
    }

    /// First block that may still change. Everything below it was served as durable.
    pub fn settled(&self) -> u64 {
        self.settled
    }

    /// First block not yet served.
    pub fn next(&self) -> u64 {
        self.next
    }

    pub fn observe(&mut self, response: &QueryResponse) -> Observation {
        let hashes = hot_hashes(response);
        if let Some(from) = self.fork_point(&hashes) {
            self.seen.split_off(&from);
            self.next = from;
            return Observation::Rollback { from };
        }
        self.seen.extend(hashes);
        if let Some(served) = response.served {
            self.next = self.next.max(served.end() + 1);
        }
        let mut settled = FinalizedCursor::new(response.requested.start());
        settled.advance(response);
        let mut floor = settled.next();
        // `latest_only` clips the hot segment at safe + 1, so blocks below it are durable.
        if floor == response.requested.start()
            && let Some(hot) = response
                .segments
                .iter()
                .find(|segment| segment.source == SegmentSource::Hot)
        {
            floor = floor.max(hot.start);
        }
        if floor > self.settled && response.requested.start() <= self.settled {
            self.settled = floor;
            self.seen = self.seen.split_off(&floor);
        }
        Observation::Consistent
    }

    /// Lowest block that may have changed: one above the highest height whose hash still
    /// matches below the lowest mismatch, since a matching hash commits to its ancestors.
    fn fork_point(&self, hashes: &BTreeMap<u64, B256>) -> Option<u64> {
        let mismatch = hashes
            .iter()
            .filter(|(number, hash)| self.seen.get(number).is_some_and(|seen| seen != *hash))
            .map(|(number, _)| *number)
            .next()?;
        let matched = hashes
            .range(..mismatch)
            .rev()
            .find(|(number, hash)| self.seen.get(number) == Some(*hash))
            .map(|(number, _)| number + 1);
        Some(matched.unwrap_or(self.settled).max(self.settled))
    }
}

/// Heights and hashes of the hot part of `response`: anchors, then rows inside hot segments.
fn hot_hashes(response: &QueryResponse) -> BTreeMap<u64, B256> {
    let hot: Vec<_> = response
        .segments
        .iter()
        .filter(|segment| segment.source == SegmentSource::Hot)
        .collect();
    let mut hashes: BTreeMap<u64, B256> = hot
        .iter()
        .filter_map(|segment| segment.anchor)
        .map(|anchor| (anchor.number, anchor.hash))
        .collect();
    for (number, hash) in response.rows.iter().filter_map(row_block) {
        if hot
            .iter()
            .any(|segment| segment.start <= number && number <= segment.end)
        {
            hashes.insert(number, hash);
        }
    }
    hashes
}

/// `block_number` and `block_hash` of a log row, or `number` and `hash` of a block row.
fn row_block(row: &Value) -> Option<(u64, B256)> {
    let (number, hash) = match row.get("block_number") {
        Some(number) => (number, row.get("block_hash")?),
        None => (row.get("number")?, row.get("hash")?),
    };
    Some((number.as_u64()?, hash.as_str()?.parse().ok()?))
}
