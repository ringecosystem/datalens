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
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(plan, PlannedWarmupTarget::Noop("query_watermark_missing"));
}

#[test]
fn test_follow_query_cursor_behind_watermark_reanchors_to_offset() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 20_248_000,
        query_watermark: Some(20_248_609),
        safe_head: 20_500_000,
        lookahead_blocks: 10_000,
        start_offset_blocks: Some(1),
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 20_249_610,
            end: 20_259_609,
        }
    );
}

#[test]
fn test_follow_query_targets_query_watermark_plus_lookahead() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 10,
        query_watermark: Some(10),
        safe_head: 30,
        lookahead_blocks: 5,
        start_offset_blocks: Some(0),
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
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
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
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
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(plan, PlannedWarmupTarget::Noop("start_after_safe_head"));
}

#[test]
fn test_follow_query_caps_query_lookahead_at_safe_head() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 25,
        query_watermark: Some(25),
        safe_head: 28,
        lookahead_blocks: 10,
        start_offset_blocks: Some(1),
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(plan, PlannedWarmupTarget::Range { start: 26, end: 28 });
}

#[test]
fn test_follow_query_explicit_zero_offset_still_starts_after_watermark() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 25,
        query_watermark: Some(25),
        safe_head: 28,
        lookahead_blocks: 10,
        start_offset_blocks: Some(0),
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(plan, PlannedWarmupTarget::Range { start: 26, end: 28 });
}

#[test]
fn test_follow_query_large_lookahead_builds_lead_past_next_runner_batch() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 221_992_620,
        query_watermark: Some(221_992_619),
        safe_head: 222_500_000,
        lookahead_blocks: 100_000,
        start_offset_blocks: Some(1),
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 222_002_620,
            end: 222_102_619,
        }
    );
}

#[test]
fn test_follow_query_large_lookahead_reanchors_cursor_below_lead_floor() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 222_272_619,
        query_watermark: Some(222_262_619),
        safe_head: 222_500_000,
        lookahead_blocks: 100_000,
        start_offset_blocks: Some(1),
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 222_272_620,
            end: 222_372_619,
        }
    );
}

#[test]
fn test_follow_query_reanchors_cursor_below_configured_start_offset() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 222_272_620,
        query_watermark: Some(222_262_619),
        safe_head: 222_500_000,
        lookahead_blocks: 100_000,
        start_offset_blocks: Some(50_000),
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 222_312_619,
            end: 222_412_618,
        }
    );
}

#[test]
fn test_follow_query_uses_adaptive_start_offset_after_watermark() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 100_000,
        query_watermark: Some(100_000),
        safe_head: 110_000,
        lookahead_blocks: 3,
        start_offset_blocks: None,
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 101_000,
            end: 101_002
        }
    );
}

#[test]
fn test_follow_query_adaptive_offset_decays_near_safe_head() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 100_000,
        query_watermark: Some(100_000),
        safe_head: 100_700,
        lookahead_blocks: 5,
        start_offset_blocks: None,
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
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
fn test_follow_query_uses_configured_offset_tiers() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 100_000,
        query_watermark: Some(100_000),
        safe_head: 110_000,
        lookahead_blocks: 3,
        start_offset_blocks: None,
        start_offset_tiers_blocks: Some(vec![5_000, 2_000, 750]),
        catchup_threshold_blocks: 200,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 100_750,
            end: 100_752
        }
    );
}

#[test]
fn test_follow_query_jumps_forward_when_query_catches_current_window() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 101_000,
        query_watermark: Some(100_800),
        safe_head: 110_000,
        lookahead_blocks: 3,
        start_offset_blocks: None,
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 101_800,
            end: 101_802
        }
    );
}

#[test]
fn test_follow_query_keeps_healthy_cursor_ahead_when_query_is_outside_catchup_threshold() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 102_000,
        query_watermark: Some(100_800),
        safe_head: 110_000,
        lookahead_blocks: 3,
        start_offset_blocks: None,
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 102_000,
            end: 102_002
        }
    );
}

#[test]
fn test_follow_query_keeps_cursor_one_block_beyond_largest_offset_inside_lookahead() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 105_001,
        query_watermark: Some(100_000),
        safe_head: 110_000,
        lookahead_blocks: 100,
        start_offset_blocks: None,
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 105_001,
            end: 105_100
        }
    );
}

#[test]
fn test_follow_query_reanchors_cursor_beyond_largest_offset_and_lookahead() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 105_100,
        query_watermark: Some(100_000),
        safe_head: 110_000,
        lookahead_blocks: 100,
        start_offset_blocks: None,
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 101_000,
            end: 101_099
        }
    );
}

#[test]
fn test_follow_query_reanchors_production_shaped_far_ahead_cursor() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 387_254_544,
        query_watermark: Some(357_257_044),
        safe_head: 388_000_000,
        lookahead_blocks: 100,
        start_offset_blocks: None,
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 357_258_044,
            end: 357_258_143
        }
    );
}

#[test]
fn test_follow_query_reanchors_far_ahead_cursor_to_adaptive_offset() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 130_797_500,
        query_watermark: Some(100_800),
        safe_head: 140_000_000,
        lookahead_blocks: 3,
        start_offset_blocks: None,
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(
        plan,
        PlannedWarmupTarget::Range {
            start: 101_800,
            end: 101_802
        }
    );
}

#[test]
fn test_follow_query_adaptive_offset_decays_to_positive_offset_near_safe_head() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 100_000,
        query_watermark: Some(100_000),
        safe_head: 100_005,
        lookahead_blocks: 5,
        start_offset_blocks: None,
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
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
        cursor_next: 100_000,
        query_watermark: Some(100_000),
        safe_head: 100_000,
        lookahead_blocks: 5,
        start_offset_blocks: None,
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(plan, PlannedWarmupTarget::Noop("start_after_safe_head"));
}

#[test]
fn test_follow_query_zero_lookahead_targets_safe_head_from_planned_start() {
    let plan = WarmupTargetPlanner::plan(WarmupTargetPlanInput {
        mode: WarmupTaskMode::FollowQuery,
        fixed_end: None,
        cursor_next: 100_000,
        query_watermark: Some(100_000),
        safe_head: 100_700,
        lookahead_blocks: 0,
        start_offset_blocks: None,
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
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
        start_offset_tiers_blocks: None,
        catchup_threshold_blocks: 200,
    });

    assert_eq!(plan, PlannedWarmupTarget::Range { start: 7, end: 9 });
}
