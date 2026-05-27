use serde::{Deserialize, Serialize};

use crate::{DatalensError, DatalensErrorKind};

#[derive(Deserialize)]
struct RawTimeRange {
    start: u64,
    end: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawTimeRange")]
pub struct TimeRange {
    start: u64,
    end: u64,
}

impl TryFrom<RawTimeRange> for TimeRange {
    type Error = DatalensError;

    fn try_from(raw: RawTimeRange) -> Result<Self, Self::Error> {
        Self::try_blocks(raw.start, raw.end)
    }
}

impl TimeRange {
    pub fn expect_blocks(start: u64, end: u64) -> Self {
        Self::try_blocks(start, end).expect("valid time range")
    }

    pub fn try_blocks(start: u64, end: u64) -> Result<Self, DatalensError> {
        if start > end {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "time range start must be less than or equal to end",
            ));
        }
        Ok(Self { start, end })
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> u64 {
        self.end
    }
}

#[derive(Deserialize)]
struct RawBlockRange {
    from_block: u64,
    to_block: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawBlockRange")]
pub struct BlockRange {
    pub from_block: u64,
    pub to_block: u64,
}

impl TryFrom<RawBlockRange> for BlockRange {
    type Error = DatalensError;

    fn try_from(raw: RawBlockRange) -> Result<Self, Self::Error> {
        Self::try_new(raw.from_block, raw.to_block)
    }
}

impl BlockRange {
    pub fn expect_new(from_block: u64, to_block: u64) -> Self {
        Self::try_new(from_block, to_block).expect("valid block range")
    }

    pub fn try_new(from_block: u64, to_block: u64) -> Result<Self, DatalensError> {
        if from_block > to_block {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "from_block must be less than or equal to to_block",
            ));
        }
        Ok(Self {
            from_block,
            to_block,
        })
    }

    pub fn len(&self) -> u128 {
        u128::from(self.to_block) - u128::from(self.from_block) + 1
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn contains(&self, block_number: u64) -> bool {
        self.from_block <= block_number && block_number <= self.to_block
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.from_block <= other.to_block && other.from_block <= self.to_block
    }

    pub fn intersection(&self, other: &Self) -> Option<Self> {
        let from_block = self.from_block.max(other.from_block);
        let to_block = self.to_block.min(other.to_block);
        Self::try_new(from_block, to_block).ok()
    }

    pub fn difference(&self, covered: &Self) -> Vec<Self> {
        let Some(overlap) = self.intersection(covered) else {
            return vec![*self];
        };
        let mut ranges = Vec::new();
        if self.from_block < overlap.from_block {
            ranges.push(Self::expect_new(self.from_block, overlap.from_block - 1));
        }
        if overlap.to_block < self.to_block {
            ranges.push(Self::expect_new(overlap.to_block + 1, self.to_block));
        }
        ranges
    }

    pub fn split(&self, max_blocks: u64) -> Result<Vec<Self>, DatalensError> {
        if max_blocks == 0 {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "max_blocks must be greater than zero",
            ));
        }

        let mut ranges = Vec::new();
        let mut from_block = self.from_block;
        loop {
            let offset = max_blocks - 1;
            let chunk_end = from_block.saturating_add(offset);
            let to_block = self.to_block.min(chunk_end);
            ranges.push(Self::expect_new(from_block, to_block));
            if to_block == self.to_block || to_block == u64::MAX {
                break;
            }
            from_block = to_block + 1;
        }
        Ok(ranges)
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum RawLedgerRangeKind {
    Block,
    Slot,
    Height,
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
#[serde(try_from = "RawLedgerRangeKind")]
pub enum LedgerRangeKind {
    Block,
    Slot,
    Height,
    Other(String),
}

impl TryFrom<RawLedgerRangeKind> for LedgerRangeKind {
    type Error = DatalensError;

    fn try_from(value: RawLedgerRangeKind) -> Result<Self, Self::Error> {
        match value {
            RawLedgerRangeKind::Block => Ok(Self::Block),
            RawLedgerRangeKind::Slot => Ok(Self::Slot),
            RawLedgerRangeKind::Height => Ok(Self::Height),
            RawLedgerRangeKind::Other(value) => Ok(Self::Other(crate::chain::validate_identifier(
                "ledger range kind",
                value,
            )?)),
        }
    }
}

#[derive(Deserialize)]
struct RawLedgerRange {
    kind: LedgerRangeKind,
    start: u64,
    end: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawLedgerRange")]
pub struct LedgerRange {
    kind: LedgerRangeKind,
    start: u64,
    end: u64,
}

impl TryFrom<RawLedgerRange> for LedgerRange {
    type Error = DatalensError;

    fn try_from(raw: RawLedgerRange) -> Result<Self, Self::Error> {
        Self::try_new(raw.kind, raw.start, raw.end)
    }
}

impl LedgerRange {
    pub fn try_new(kind: LedgerRangeKind, start: u64, end: u64) -> Result<Self, DatalensError> {
        let kind = match kind {
            LedgerRangeKind::Other(value) => LedgerRangeKind::Other(
                crate::chain::validate_identifier("ledger range kind", value)?,
            ),
            kind => kind,
        };
        if start > end {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "ledger range start must be less than or equal to end",
            ));
        }
        Ok(Self { kind, start, end })
    }

    pub fn blocks(start: u64, end: u64) -> Result<Self, DatalensError> {
        Self::try_new(LedgerRangeKind::Block, start, end)
    }

    pub fn slots(start: u64, end: u64) -> Result<Self, DatalensError> {
        Self::try_new(LedgerRangeKind::Slot, start, end)
    }

    pub fn heights(start: u64, end: u64) -> Result<Self, DatalensError> {
        Self::try_new(LedgerRangeKind::Height, start, end)
    }

    pub fn from_block_range(range: BlockRange) -> Self {
        Self {
            kind: LedgerRangeKind::Block,
            start: range.from_block,
            end: range.to_block,
        }
    }

    pub fn block_range(&self) -> Option<BlockRange> {
        if self.kind == LedgerRangeKind::Block {
            Some(BlockRange::expect_new(self.start, self.end))
        } else {
            None
        }
    }

    pub fn kind(&self) -> LedgerRangeKind {
        self.kind.clone()
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> u64 {
        self.end
    }

    pub fn len(&self) -> u128 {
        u128::from(self.end) - u128::from(self.start) + 1
    }

    pub fn is_empty(&self) -> bool {
        false
    }

    pub fn contains(&self, position: u64) -> bool {
        self.start <= position && position <= self.end
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.kind == other.kind && self.start <= other.end && other.start <= self.end
    }

    pub fn intersection(&self, other: &Self) -> Option<Self> {
        if self.kind != other.kind {
            return None;
        }
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        Self::try_new(self.kind.clone(), start, end).ok()
    }

    pub fn difference(&self, covered: &Self) -> Vec<Self> {
        let Some(overlap) = self.intersection(covered) else {
            return vec![self.clone()];
        };
        let mut ranges = Vec::new();
        if self.start < overlap.start {
            ranges.push(Self::try_new(self.kind.clone(), self.start, overlap.start - 1).unwrap());
        }
        if overlap.end < self.end {
            ranges.push(Self::try_new(self.kind.clone(), overlap.end + 1, self.end).unwrap());
        }
        ranges
    }

    pub fn split(&self, max_len: u64) -> Result<Vec<Self>, DatalensError> {
        if max_len == 0 {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "max_len must be greater than zero",
            ));
        }

        let mut ranges = Vec::new();
        let mut start = self.start;
        loop {
            let chunk_end = start.saturating_add(max_len - 1);
            let end = self.end.min(chunk_end);
            ranges.push(Self::try_new(self.kind.clone(), start, end).unwrap());
            if end == self.end || end == u64::MAX {
                break;
            }
            start = end + 1;
        }
        Ok(ranges)
    }
}
