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
        start_offset_blocks: None,
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
        start_offset_blocks: Some(0),
    });

    assert_eq!(plan, PlannedWarmupTarget::Range { start: 11, end: 15 });
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
        start_offset_blocks: Some(0),
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
        start_offset_blocks: Some(0),
    });

    assert_eq!(plan, PlannedWarmupTarget::Noop("start_after_safe_head"));
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
        start_offset_blocks: Some(1),
    });

    assert_eq!(plan, PlannedWarmupTarget::Range { start: 26, end: 28 });
}

#[test]
fn test_follow_query_explicit_zero_offset_still_starts_after_watermark() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 1,
        query_watermark: Some(25),
        safe_head: 28,
        lookahead_blocks: 10,
        start_offset_blocks: Some(0),
    });

    assert_eq!(plan, PlannedWarmupTarget::Range { start: 26, end: 28 });
}

#[test]
fn test_follow_query_uses_adaptive_start_offset_after_watermark() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 100,
        query_watermark: Some(100_000),
        safe_head: 110_000,
        lookahead_blocks: 3,
        start_offset_blocks: None,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 105_000,
            end: 105_002
        }
    );
}

#[test]
fn test_follow_query_adaptive_offset_decays_near_safe_head() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 1,
        query_watermark: Some(100_000),
        safe_head: 100_700,
        lookahead_blocks: 5,
        start_offset_blocks: None,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 100_500,
            end: 100_504
        }
    );
}

#[test]
fn test_follow_query_adaptive_offset_decays_to_positive_offset_near_safe_head() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 1,
        query_watermark: Some(100_000),
        safe_head: 100_005,
        lookahead_blocks: 5,
        start_offset_blocks: None,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 100_001,
            end: 100_005
        }
    );
}

#[test]
fn test_follow_query_noops_when_safe_head_equals_query_watermark() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 1,
        query_watermark: Some(100_000),
        safe_head: 100_000,
        lookahead_blocks: 5,
        start_offset_blocks: None,
    });

    assert_eq!(plan, PlannedWarmupTarget::Noop("start_after_safe_head"));
}

#[test]
fn test_follow_query_zero_lookahead_targets_safe_head_from_planned_start() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 1,
        query_watermark: Some(100_000),
        safe_head: 100_700,
        lookahead_blocks: 0,
        start_offset_blocks: None,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 100_500,
            end: 100_700
        }
    );
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
        start_offset_blocks: None,
    });

    assert_eq!(plan, PlannedWarmupTarget::Range { start: 7, end: 9 });
}
