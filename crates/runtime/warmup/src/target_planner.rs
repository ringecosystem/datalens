use crate::WarmupTaskMode;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WarmupTargetPlanInput {
    pub mode: WarmupTaskMode,
    pub fixed_end: Option<u64>,
    pub cursor_next: u64,
    pub query_watermark: Option<u64>,
    pub safe_head: u64,
    pub lookahead_blocks: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlannedWarmupTarget {
    Range { start: u64, end: u64 },
    Noop(&'static str),
}

pub struct WarmupTargetPlanner;

impl WarmupTargetPlanner {
    pub fn plan(input: WarmupTargetPlanInput) -> PlannedWarmupTarget {
        let target_end = match input.mode {
            WarmupTaskMode::FixedRange => input
                .fixed_end
                .unwrap_or(input.cursor_next)
                .min(input.safe_head),
            WarmupTaskMode::FollowSafeHeight => input.safe_head,
            WarmupTaskMode::FollowQuery => {
                let Some(query_watermark) = input.query_watermark else {
                    return PlannedWarmupTarget::Noop("query_watermark_missing");
                };
                if input.cursor_next > query_watermark {
                    input
                        .cursor_next
                        .saturating_add(input.lookahead_blocks.saturating_sub(1))
                        .min(input.safe_head)
                } else {
                    query_watermark
                        .saturating_add(input.lookahead_blocks)
                        .min(input.safe_head)
                }
            }
        };
        if input.cursor_next > target_end {
            return PlannedWarmupTarget::Noop("cursor_at_or_beyond_target");
        }
        PlannedWarmupTarget::Range {
            start: input.cursor_next,
            end: target_end,
        }
    }
}
