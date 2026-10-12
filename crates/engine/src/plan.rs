use std::num::NonZeroU64;

use datalens_core::wire::{Segment, SegmentSource};
use datalens_core::{BlockRange, Error, Finality, Heads};

/// `range` widened to whole grid chunks and clipped to `safe`, so overlapping requests
/// from different callers plan identical chunks. `None` when the range starts above `safe`.
pub(crate) fn grid_span(range: BlockRange, chunk: NonZeroU64, safe: u64) -> Option<BlockRange> {
    let chunk = chunk.get();
    let start = range.start() / chunk * chunk;
    let end = (range.end() / chunk)
        .saturating_add(1)
        .saturating_mul(chunk)
        .saturating_sub(1)
        .min(safe)
        .min(BlockRange::MAX_END);
    BlockRange::new(start, end).ok()
}

/// Parts of `range` not in `covered`. `covered` is sorted and disjoint.
pub(crate) fn uncovered(range: BlockRange, covered: &[BlockRange]) -> Vec<BlockRange> {
    let mut gaps = Vec::new();
    let mut next = range.start();
    for span in covered {
        let Some(hit) = span.intersect(&range) else {
            continue;
        };
        if hit.start() > next {
            gaps.extend(BlockRange::new(next, hit.start() - 1).ok());
        }
        next = next.max(hit.end().saturating_add(1));
    }
    if next <= range.end() {
        gaps.extend(BlockRange::new(next, range.end()).ok());
    }
    gaps
}

/// Whole grid cells holding an uncovered block of `span`, in block order, each ended at `safe`.
/// Two gaps in one cell plan one job, so `fill_jobs_active` dedupes them across callers.
pub(crate) fn fill_chunks(
    span: BlockRange,
    covered: &[BlockRange],
    chunk: NonZeroU64,
    safe: u64,
) -> Vec<BlockRange> {
    let mut cells: Vec<BlockRange> = Vec::new();
    for cell in uncovered(span, covered)
        .into_iter()
        .flat_map(|gap| gap.align_chunks(chunk))
    {
        let Ok(cell) = BlockRange::new(cell.start(), cell.end().min(safe)) else {
            continue;
        };
        if cells.last() != Some(&cell) {
            cells.push(cell);
        }
    }
    cells
}

/// The part of a request the durable path serves: everything up to `safe`.
pub(crate) fn durable_part(range: BlockRange, safe: u64) -> Option<BlockRange> {
    BlockRange::new(range.start(), range.end().min(safe)).ok()
}

/// Durable segments of `served`, split at the finalized head.
pub(crate) fn durable_segments(served: BlockRange, heads: &Heads) -> Vec<Segment> {
    let segment = |start: u64, end: u64, finality: Finality| Segment {
        start,
        end,
        source: SegmentSource::Durable,
        finality,
        anchor: None,
    };
    let mut segments = Vec::with_capacity(2);
    if served.start() <= heads.finalized {
        segments.push(segment(
            served.start(),
            served.end().min(heads.finalized),
            Finality::Finalized,
        ));
    }
    if served.end() > heads.finalized {
        segments.push(segment(
            served.start().max(heads.finalized + 1),
            served.end(),
            Finality::Safe,
        ));
    }
    segments
}

/// Finality a fill writes for `blocks` given the current heads.
pub(crate) fn fill_finality(blocks: BlockRange, heads: &Heads) -> Finality {
    if blocks.end() <= heads.finalized {
        Finality::Finalized
    } else {
        Finality::Safe
    }
}

/// Opaque page position. Logs order by `(block_number, log_index)`, blocks by number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cursor {
    Log { block_number: u64, log_index: u32 },
    Block { number: u64 },
}

impl Cursor {
    pub(crate) fn encode(self) -> String {
        match self {
            Self::Log {
                block_number,
                log_index,
            } => format!("l{block_number}.{log_index}"),
            Self::Block { number } => format!("b{number}"),
        }
    }

    pub(crate) fn decode(text: &str) -> Result<Self, Error> {
        let bad = || Error::InvalidInput(format!("invalid page cursor {text:?}"));
        if let Some(rest) = text.strip_prefix('l') {
            let (block, index) = rest.split_once('.').ok_or_else(bad)?;
            return Ok(Self::Log {
                block_number: block.parse().map_err(|_| bad())?,
                log_index: index.parse().map_err(|_| bad())?,
            });
        }
        if let Some(rest) = text.strip_prefix('b') {
            return Ok(Self::Block {
                number: rest.parse().map_err(|_| bad())?,
            });
        }
        Err(bad())
    }
}
