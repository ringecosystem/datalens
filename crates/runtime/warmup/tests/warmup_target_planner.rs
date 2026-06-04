use datalens_warmup::{
    PlannedWarmupTarget, WarmupTargetPlanInput, WarmupTargetPlanner, WarmupTaskMode,
};

#[test]
fn test_follow_query_without_query_watermark_has_no_target() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 10,
        query_watermark: None,
        safe_head: 20,
        lookahead_blocks: 5,
    });

    assert_eq!(plan, PlannedWarmupTarget::Noop("query_watermark_missing"));
}

#[test]
fn test_follow_query_targets_query_watermark_plus_lookahead() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 1,
        query_watermark: Some(10),
        safe_head: 30,
        lookahead_blocks: 5,
    });

    assert_eq!(plan, PlannedWarmupTarget::Range { start: 1, end: 15 });
}

#[test]
fn test_follow_query_cursor_ahead_of_query_makes_bounded_background_progress() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 20,
        query_watermark: Some(10),
        safe_head: 50,
        lookahead_blocks: 3,
    });

    assert_eq!(plan, PlannedWarmupTarget::Range { start: 20, end: 22 });
}

#[test]
fn test_follow_query_query_at_safe_head_does_not_plan_beyond_safe_head() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 15,
        query_watermark: Some(20),
        safe_head: 20,
        lookahead_blocks: 10,
    });

    assert_eq!(plan, PlannedWarmupTarget::Range { start: 15, end: 20 });
}

#[test]
fn test_follow_query_caps_query_lookahead_at_safe_head() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 1,
        query_watermark: Some(25),
        safe_head: 28,
        lookahead_blocks: 10,
    });

    assert_eq!(plan, PlannedWarmupTarget::Range { start: 1, end: 28 });
}

#[test]
fn test_follow_safe_height_still_targets_safe_head() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowSafeHeight,
        fixed_end: None,
        cursor_next: 7,
        query_watermark: None,
        safe_head: 9,
        lookahead_blocks: 100,
    });

    assert_eq!(plan, PlannedWarmupTarget::Range { start: 7, end: 9 });
}
