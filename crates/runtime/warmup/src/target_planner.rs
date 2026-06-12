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
    pub start_offset_tiers_blocks: Option<Vec<u64>>,
    pub catchup_threshold_blocks: u64,
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

                if input.safe_head <= query_watermark {
                    return PlannedWarmupTarget::Noop("start_after_safe_head");
                }
                let start = follow_query_start(&input, query_watermark);
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

fn follow_query_start(input: &WarmupTargetPlanInput, query_watermark: u64) -> u64 {
    let safe_delta = input.safe_head.saturating_sub(query_watermark);
    let target_offset = smallest_fitting_offset(input, 0, safe_delta).unwrap_or(1);
    let Some(cursor_distance) = input.cursor_next.checked_sub(query_watermark) else {
        return query_watermark.saturating_add(target_offset);
    };

    if cursor_distance <= input.catchup_threshold_blocks
        && let Some(offset) = smallest_fitting_offset(input, cursor_distance, safe_delta)
    {
        return query_watermark.saturating_add(offset);
    }

    if max_healthy_cursor_distance(input, safe_delta)
        .is_some_and(|max_distance| cursor_distance > max_distance)
    {
        return query_watermark.saturating_add(target_offset);
    }

    input.cursor_next
}

fn smallest_fitting_offset(
    input: &WarmupTargetPlanInput,
    greater_than: u64,
    safe_delta: u64,
) -> Option<u64> {
    let lead_floor = lookahead_lead_floor(input).filter(|offset| *offset <= safe_delta);
    let greater_than = lead_floor
        .map(|offset| greater_than.max(offset.saturating_sub(1)))
        .unwrap_or(greater_than);
    let mut configured = configured_offset_tiers(input);
    configured.sort_unstable();
    configured.dedup();
    configured
        .into_iter()
        .find(|offset| *offset > greater_than && *offset <= safe_delta)
        .or_else(|| adaptive_fallback_offset(greater_than, safe_delta))
        .or(lead_floor)
}

fn configured_offset_tiers(input: &WarmupTargetPlanInput) -> Vec<u64> {
    if let Some(offset) = input.start_offset_blocks {
        return vec![offset.max(1)];
    }
    input
        .start_offset_tiers_blocks
        .clone()
        .unwrap_or_else(|| vec![5_000, 3_000, 1_000])
        .into_iter()
        .filter(|offset| *offset > 0)
        .collect()
}

fn largest_fitting_offset(input: &WarmupTargetPlanInput, safe_delta: u64) -> Option<u64> {
    configured_offset_tiers(input)
        .into_iter()
        .chain(lookahead_lead_floor(input))
        .chain([500, 100, 50, 10, 1])
        .filter(|offset| *offset <= safe_delta)
        .max()
}

fn max_healthy_cursor_distance(input: &WarmupTargetPlanInput, safe_delta: u64) -> Option<u64> {
    largest_fitting_offset(input, safe_delta)
        .map(|offset| offset.saturating_add(input.lookahead_blocks.saturating_sub(1)))
}

fn adaptive_fallback_offset(greater_than: u64, safe_delta: u64) -> Option<u64> {
    [500, 100, 50, 10, 1]
        .into_iter()
        .filter(|offset| *offset > greater_than && *offset <= safe_delta)
        .max()
}

fn lookahead_lead_floor(input: &WarmupTargetPlanInput) -> Option<u64> {
    (input.lookahead_blocks >= 10_000).then_some(input.lookahead_blocks / 10 + 1)
}
