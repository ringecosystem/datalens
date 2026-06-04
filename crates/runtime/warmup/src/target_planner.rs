use crate::WarmupTaskMode;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WarmupTargetPlanInput {
    pub mode: WarmupTaskMode,
    pub fixed_end: Option<u64>,
    pub cursor_next: u64,
    pub query_watermark: Option<u64>,
    pub safe_head: u64,
    pub lookahead_blocks: u64,
    pub start_offset_blocks: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlannedWarmupTarget {
    Range { start: u64, end: u64 },
    Noop(&'static str),
}

pub struct WarmupTargetPlanner;

impl WarmupTargetPlanner {
    pub fn plan(input: WarmupTargetPlanInput) -> PlannedWarmupTarget {
        let (target_start, target_end) = match input.mode {
            WarmupTaskMode::FixedRange => (
                input.cursor_next,
                input
                    .fixed_end
                    .unwrap_or(input.cursor_next)
                    .min(input.safe_head),
            ),
            WarmupTaskMode::FollowSafeHeight => (input.cursor_next, input.safe_head),
            WarmupTaskMode::FollowQuery => {
                let Some(query_watermark) = input.query_watermark else {
                    return PlannedWarmupTarget::Noop("query_watermark_missing");
                };

                let desired_start = query_watermark.saturating_add(
                    input
                        .start_offset_blocks
                        .map(|offset| offset.max(1))
                        .unwrap_or_else(|| {
                            adaptive_start_offset_blocks(query_watermark, input.safe_head)
                        }),
                );
                let start = input.cursor_next.max(desired_start);
                if start > input.safe_head {
                    return PlannedWarmupTarget::Noop("start_after_safe_head");
                }
                let end = if input.lookahead_blocks == 0 {
                    input.safe_head
                } else {
                    start
                        .saturating_add(input.lookahead_blocks.saturating_sub(1))
                        .min(input.safe_head)
                };
                (start, end)
            }
        };
        if target_start > target_end {
            return PlannedWarmupTarget::Noop("cursor_at_or_beyond_target");
        }
        PlannedWarmupTarget::Range {
            start: target_start,
            end: target_end,
        }
    }
}

fn adaptive_start_offset_blocks(query_watermark: u64, safe_head: u64) -> u64 {
    [5_000, 4_000, 3_000, 1_000, 500, 100, 50, 10, 1]
        .into_iter()
        .find(|offset| query_watermark.saturating_add(*offset) <= safe_head)
        .unwrap_or(1)
}
