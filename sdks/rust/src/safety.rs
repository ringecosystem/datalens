use serde::{Deserialize, Serialize};

use crate::native::QueryResponse;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataFinality {
    Finalized,
    Safe,
    Latest,
    Provisional,
    Unknown,
}

pub type SegmentFinality = DataFinality;

impl DataFinality {
    pub fn is_durable(self) -> bool {
        matches!(self, Self::Finalized | Self::Safe)
    }

    pub fn is_provisional(self) -> bool {
        matches!(self, Self::Latest | Self::Provisional)
    }
}

impl From<&str> for DataFinality {
    fn from(value: &str) -> Self {
        match value {
            "finalized" | "final" | "durable" => Self::Finalized,
            "safe" => Self::Safe,
            "latest" | "latest_only" => Self::Latest,
            "provisional" | "safe_to_latest" | "hot" => Self::Provisional,
            _ => Self::Unknown,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataRange {
    pub kind: String,
    pub start: u64,
    pub end: u64,
}

impl DataRange {
    pub fn new(kind: impl Into<String>, start: u64, end: u64) -> Self {
        Self {
            kind: kind.into(),
            start,
            end,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockAnchor {
    pub range_kind: String,
    pub height: u64,
    pub block_hash: Option<String>,
    pub parent_hash: Option<String>,
    pub timestamp: Option<u64>,
    pub finality: DataFinality,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SafetyConcern {
    NonDurableFinality { finality: DataFinality },
    UnknownFinality,
    RangeKindMismatch { cursor: String, anchor: String },
    HeightRegression { cursor: u64, anchor: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalizedCursor {
    range_kind: String,
    height: u64,
    anchor: Option<BlockAnchor>,
}

impl FinalizedCursor {
    pub fn new(range_kind: impl Into<String>, height: u64) -> Self {
        Self {
            range_kind: range_kind.into(),
            height,
            anchor: None,
        }
    }

    pub fn height(&self) -> u64 {
        self.height
    }

    pub fn anchor(&self) -> Option<&BlockAnchor> {
        self.anchor.as_ref()
    }

    pub fn advance(&mut self, anchor: BlockAnchor) -> Result<(), SafetyConcern> {
        if anchor.finality == DataFinality::Unknown {
            return Err(SafetyConcern::UnknownFinality);
        }
        if !anchor.finality.is_durable() {
            return Err(SafetyConcern::NonDurableFinality {
                finality: anchor.finality,
            });
        }
        advance_cursor(&self.range_kind, self.height, &anchor)?;
        self.height = anchor.height;
        self.anchor = Some(anchor);
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProvisionalCursor {
    range_kind: String,
    height: u64,
    anchor: Option<BlockAnchor>,
}

impl ProvisionalCursor {
    pub fn new(range_kind: impl Into<String>, height: u64) -> Self {
        Self {
            range_kind: range_kind.into(),
            height,
            anchor: None,
        }
    }

    pub fn height(&self) -> u64 {
        self.height
    }

    pub fn anchor(&self) -> Option<&BlockAnchor> {
        self.anchor.as_ref()
    }

    pub fn advance(&mut self, anchor: BlockAnchor) -> Result<(), SafetyConcern> {
        if anchor.finality == DataFinality::Unknown {
            return Err(SafetyConcern::UnknownFinality);
        }
        advance_cursor(&self.range_kind, self.height, &anchor)?;
        self.height = anchor.height;
        self.anchor = Some(anchor);
        Ok(())
    }
}

fn advance_cursor(
    cursor_range_kind: &str,
    cursor_height: u64,
    anchor: &BlockAnchor,
) -> Result<(), SafetyConcern> {
    if cursor_range_kind != anchor.range_kind {
        return Err(SafetyConcern::RangeKindMismatch {
            cursor: cursor_range_kind.to_owned(),
            anchor: anchor.range_kind.clone(),
        });
    }
    if anchor.height < cursor_height {
        return Err(SafetyConcern::HeightRegression {
            cursor: cursor_height,
            anchor: anchor.height,
        });
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromotionPlan {
    pub decision: PromotionDecision,
    pub durable_head: Option<BlockAnchor>,
    pub provisional_range: Option<DataRange>,
    pub provisional_anchor: Option<BlockAnchor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromotionDecision {
    Promote { range: DataRange },
    KeepProvisional { reason: String },
    Recheck { reason: String },
    Rollback { reason: String },
}

pub fn plan_promotion(
    durable_head: Option<&BlockAnchor>,
    provisional_range: Option<&DataRange>,
    provisional_anchor: Option<&BlockAnchor>,
) -> PromotionPlan {
    let decision = promotion_decision(durable_head, provisional_range, provisional_anchor);
    PromotionPlan {
        decision,
        durable_head: durable_head.cloned(),
        provisional_range: provisional_range.cloned(),
        provisional_anchor: provisional_anchor.cloned(),
    }
}

fn promotion_decision(
    durable_head: Option<&BlockAnchor>,
    provisional_range: Option<&DataRange>,
    provisional_anchor: Option<&BlockAnchor>,
) -> PromotionDecision {
    let Some(durable_head) = durable_head else {
        return PromotionDecision::Recheck {
            reason: "missing durable head anchor".to_owned(),
        };
    };
    let Some(provisional_range) = provisional_range else {
        return PromotionDecision::Recheck {
            reason: "missing provisional range".to_owned(),
        };
    };
    let Some(provisional_anchor) = provisional_anchor else {
        return PromotionDecision::Recheck {
            reason: "missing provisional anchor".to_owned(),
        };
    };
    if !durable_head.finality.is_durable() {
        return PromotionDecision::Recheck {
            reason: "durable head finality is not safe or finalized".to_owned(),
        };
    }
    if provisional_anchor.finality == DataFinality::Unknown {
        return PromotionDecision::Recheck {
            reason: "provisional anchor finality is unknown".to_owned(),
        };
    }
    if durable_head.range_kind != provisional_range.kind
        || provisional_anchor.range_kind != provisional_range.kind
    {
        return PromotionDecision::Rollback {
            reason: "anchor range kind does not match provisional range".to_owned(),
        };
    }
    if durable_head.height >= provisional_range.end {
        if !provisional_anchor.finality.is_durable() {
            return PromotionDecision::Recheck {
                reason: "provisional anchor requires canonical match proof before promotion"
                    .to_owned(),
            };
        }
        return PromotionDecision::Promote {
            range: provisional_range.clone(),
        };
    }
    if durable_head.height < provisional_range.start {
        return PromotionDecision::KeepProvisional {
            reason: "durable head is below provisional range".to_owned(),
        };
    }
    PromotionDecision::Recheck {
        reason: "durable head only partially covers provisional range".to_owned(),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheSegment {
    pub source: Option<String>,
    pub finality: DataFinality,
    pub range: Option<DataRange>,
    pub anchor: Option<BlockAnchor>,
}

pub fn extract_cache_segments(response: &QueryResponse) -> Vec<CacheSegment> {
    response
        .cache
        .get("segments")
        .and_then(serde_json::Value::as_array)
        .map(|segments| segments.iter().map(extract_cache_segment).collect())
        .unwrap_or_default()
}

fn extract_cache_segment(segment: &serde_json::Value) -> CacheSegment {
    let finality = read_string(segment, &["finality"])
        .as_deref()
        .map(DataFinality::from)
        .unwrap_or(DataFinality::Unknown);
    CacheSegment {
        source: read_string(segment, &["source"]),
        finality,
        range: segment.get("range").and_then(extract_range),
        anchor: extract_anchor(segment, finality),
    }
}

fn extract_range(value: &serde_json::Value) -> Option<DataRange> {
    Some(DataRange {
        kind: read_string(value, &["kind", "rangeKind", "range_kind"])?,
        start: read_u64(value, &["start"])?,
        end: read_u64(value, &["end"])?,
    })
}

fn extract_anchor(
    segment: &serde_json::Value,
    segment_finality: DataFinality,
) -> Option<BlockAnchor> {
    let anchor = segment.get("anchor").unwrap_or(segment);
    let range_kind = read_string(anchor, &["rangeKind", "range_kind", "kind"]).or_else(|| {
        segment
            .get("range")
            .and_then(|range| read_string(range, &["kind", "rangeKind", "range_kind"]))
    })?;
    let finality = read_string(anchor, &["finality"])
        .as_deref()
        .map(DataFinality::from)
        .unwrap_or(segment_finality);
    Some(BlockAnchor {
        range_kind,
        height: read_u64(anchor, &["height", "slot"])?,
        block_hash: read_string(anchor, &["blockHash", "block_hash", "hash"]),
        parent_hash: read_string(anchor, &["parentHash", "parent_hash"]),
        timestamp: read_u64(anchor, &["timestamp"]),
        finality,
    })
}

fn read_string(value: &serde_json::Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(serde_json::Value::as_str))
        .map(ToOwned::to_owned)
}

fn read_u64(value: &serde_json::Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(serde_json::Value::as_u64))
}
