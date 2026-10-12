use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::Error;

/// Inclusive block range `[start, end]`.
///
/// `end` stays below `i64::MAX` so the half-open PostgreSQL form `int8range(start, end + 1)`
/// is always representable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "RawRange")]
pub struct BlockRange {
    start: u64,
    end: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRange {
    start: u64,
    end: u64,
}

impl TryFrom<RawRange> for BlockRange {
    type Error = Error;

    fn try_from(raw: RawRange) -> Result<Self, Error> {
        Self::new(raw.start, raw.end)
    }
}

impl BlockRange {
    pub const MAX_END: u64 = i64::MAX as u64 - 1;

    pub fn new(start: u64, end: u64) -> Result<Self, Error> {
        if start > end {
            return Err(Error::invalid(format!(
                "block range start {start} is after end {end}"
            )));
        }
        if end > Self::MAX_END {
            return Err(Error::invalid(format!(
                "block range end {end} exceeds {}",
                Self::MAX_END
            )));
        }
        Ok(Self { start, end })
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> u64 {
        self.end
    }

    /// Number of blocks in the range. Never zero.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }

    pub fn contains(&self, block: u64) -> bool {
        self.start <= block && block <= self.end
    }

    pub fn intersect(&self, other: &BlockRange) -> Option<BlockRange> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        (start <= end).then_some(BlockRange { start, end })
    }

    /// Grid cells that overlap this range. Cell `k` is `[k * chunk, (k + 1) * chunk - 1]`,
    /// with the cell end capped at [`BlockRange::MAX_END`].
    ///
    /// The cells are not clipped to this range. `fill_jobs` dedupes on the exact span, so two
    /// gaps in one cell must name that cell. The caller clips the last cell to safe.
    pub fn align_chunks(&self, chunk_blocks: NonZeroU64) -> Vec<BlockRange> {
        let chunk = chunk_blocks.get();
        let mut chunks = Vec::new();
        let mut cell = self.start / chunk;
        loop {
            let start = cell.saturating_mul(chunk);
            let end = cell
                .saturating_add(1)
                .saturating_mul(chunk)
                .saturating_sub(1)
                .min(Self::MAX_END);
            chunks.push(BlockRange { start, end });
            if end >= self.end {
                return chunks;
            }
            cell += 1;
        }
    }
}
