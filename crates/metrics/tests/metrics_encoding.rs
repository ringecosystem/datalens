use datalens_core::{ChainFamily, ChainIdentity, DatalensErrorKind, DatasetKey};
use datalens_metrics::{
    ApplicationIdentity, CacheCoverageOutcome, CompactionBacklogLabels, CompactionTickMetrics,
    DurableIntentClaimOutcome, DurableIntentOutcome, DurableWriteOutcome, ErrorLabels, FillOutcome,
    HotReorgOutcome, MetricsLabels, MetricsRecorder, QueryMetadataEnqueueOutcome,
    QueryMetadataWriteOutcome, QueryOutcome, WarmupFetchOutcome, WarmupTaskOutcome,
    WarmupWriteOutcome,
};

#[test]
fn test_record_metrics_renders_prometheus_text_with_expected_labels() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let labels = labels(Some("indexer"));

    recorder.record_query(&labels, QueryOutcome::Miss);
    recorder.observe_query_duration(&labels, 0.25);
    recorder.record_cache_coverage(&labels, CacheCoverageOutcome::PartialHit);
    recorder.record_fill(&labels, FillOutcome::Filled);
    recorder.record_durable_write(&labels, DurableWriteOutcome::Staged);
    recorder.record_durable_intent(&labels, "query", DurableIntentOutcome::Submitted);
    recorder.observe_durable_intent_duration(
        &labels,
        "query",
        DurableIntentOutcome::Completed,
        2.0,
    );
    recorder.set_durable_intent_backlog_for_scope(&chain(), "query", 3, 15);
    recorder.observe_durable_intent_claim_duration(
        &chain(),
        "query",
        DurableIntentClaimOutcome::Claimed,
        0.125,
    );
    recorder.record_durable_intent_claim(&chain(), "query", DurableIntentClaimOutcome::Claimed);
    recorder.observe_fill_duration(&labels, 1.5);
    recorder.set_latest_requested_block(&labels, 42);
    recorder.set_latest_filled_block(&labels, 40);
    recorder.record_provider_error(&ErrorLabels::from_labels(
        &labels,
        DatalensErrorKind::ProviderTimeout,
    ));
    recorder.record_storage_error(&ErrorLabels::from_labels(
        &labels,
        DatalensErrorKind::StorageReadFailure,
    ));

    let output = recorder.encode().expect("prometheus text");

    assert!(output.contains("datalens_query_total"));
    assert!(output.contains(
        r#"datalens_query_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="miss"} 1"#
    ));
    assert!(output.contains("datalens_query_duration_seconds"));
    assert!(output.contains(
        r#"datalens_cache_coverage_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="partial_hit"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_fill_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="filled"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_durable_write_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="staged"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_durable_intent_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="submitted",source="query"} 1"#
    ));
    assert!(output.contains("datalens_durable_intent_duration_seconds"));
    assert!(output.contains(
        r#"datalens_durable_intent_pending_total{chain="ethereum",chain_kind="evm",source="query"} 3"#
    ));
    assert!(output.contains(
        r#"datalens_durable_intent_oldest_pending_age_seconds{chain="ethereum",chain_kind="evm",source="query"} 15"#
    ));
    assert!(output.contains(
        r#"datalens_durable_intent_claim_total{chain="ethereum",chain_kind="evm",outcome="claimed",source="query"} 1"#
    ));
    assert!(output.contains("datalens_durable_intent_claim_duration_seconds"));
    assert!(output.contains("datalens_fill_duration_seconds"));
    assert!(output.contains(
        r#"datalens_provider_error_total{chain="ethereum",chain_kind="evm",dataset="evm.logs",error_kind="provider_timeout"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_storage_error_total{chain="ethereum",chain_kind="evm",dataset="evm.logs",error_kind="storage_read_failure"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_application_chain_latest_requested_block{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs"} 42"#
    ));
    assert!(output.contains(
        r#"datalens_application_chain_latest_filled_block{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs"} 40"#
    ));
}

#[test]
fn test_warmup_metrics_have_distinct_series_and_outcomes() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let labels = labels(Some("warmup-app"));

    recorder.record_warmup_task(&labels, WarmupTaskOutcome::Completed);
    recorder.record_warmup_fetch(&labels, "evm_logs", WarmupFetchOutcome::Fetched);
    recorder.record_warmup_write(&labels, WarmupWriteOutcome::Written);
    recorder.record_warmup_rows(&labels, 7);
    recorder.record_warmup_provider_error(&labels, "evm_logs", DatalensErrorKind::ProviderLimit);
    recorder.set_warmup_current_height(&labels, 123);

    let output = recorder.encode().expect("prometheus text");

    assert!(output.contains(
        r#"datalens_warmup_task_total{application="warmup-app",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="completed"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_warmup_fetch_total{application="warmup-app",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="fetched",selector_kind="evm_logs"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_warmup_write_total{application="warmup-app",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="written"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_warmup_rows_total{application="warmup-app",chain="ethereum",chain_kind="evm",dataset="evm.logs"} 7"#
    ));
    assert!(output.contains(
        r#"datalens_warmup_provider_error_total{application="warmup-app",chain="ethereum",chain_kind="evm",dataset="evm.logs",error_kind="provider_limit",selector_kind="evm_logs"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_warmup_current_height{application="warmup-app",chain="ethereum",chain_kind="evm",dataset="evm.logs"} 123"#
    ));
}

#[test]
fn test_query_metadata_metrics_have_kind_and_outcome_labels() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let labels = labels(Some("query-api"));

    recorder.record_query_metadata_enqueue(
        &labels,
        "query_watermark",
        QueryMetadataEnqueueOutcome::Coalesced,
    );
    recorder.record_query_metadata_enqueue(
        &labels,
        "usage_ledger",
        QueryMetadataEnqueueOutcome::Dropped,
    );
    recorder.record_query_metadata_enqueue(
        &labels,
        "query_activity",
        QueryMetadataEnqueueOutcome::CoalesceFull,
    );
    recorder.record_query_metadata_write(
        &labels,
        "query_activity",
        QueryMetadataWriteOutcome::Completed,
    );
    recorder.observe_query_metadata_write_duration(&labels, "query_activity", 0.25);

    let output = recorder.encode().expect("prometheus text");

    assert!(output.contains(
        r#"datalens_query_metadata_enqueue_total{application="query-api",chain="ethereum",chain_kind="evm",dataset="evm.logs",metadata_kind="query_watermark",outcome="coalesced"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_query_metadata_enqueue_total{application="query-api",chain="ethereum",chain_kind="evm",dataset="evm.logs",metadata_kind="usage_ledger",outcome="dropped"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_query_metadata_enqueue_total{application="query-api",chain="ethereum",chain_kind="evm",dataset="evm.logs",metadata_kind="query_activity",outcome="coalesce_full"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_query_metadata_write_total{application="query-api",chain="ethereum",chain_kind="evm",dataset="evm.logs",metadata_kind="query_activity",outcome="completed"} 1"#
    ));
    assert!(output.contains("datalens_query_metadata_write_duration_seconds"));
}

#[test]
fn test_compaction_metrics_expose_backlog_progress_and_backpressure() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let labels = CompactionBacklogLabels::new(chain(), DatasetKey::evm_logs(), "evm_logs", "0xabc");

    recorder.set_compaction_backlog(&labels, 6, 3, 2);
    recorder.record_compaction_tick(
        &chain(),
        CompactionTickMetrics {
            status: "partial",
            pause_reason: "none",
            input_objects: 4,
            output_objects: 1,
            deleted_source_objects: 3,
            deleted_manifest_segments: 2,
            duration_seconds: 0.75,
        },
    );
    recorder.record_compaction_tick(
        &chain(),
        CompactionTickMetrics {
            status: "paused",
            pause_reason: "query_latency",
            input_objects: 0,
            output_objects: 0,
            deleted_source_objects: 0,
            deleted_manifest_segments: 0,
            duration_seconds: 0.01,
        },
    );

    let output = recorder.encode().expect("prometheus text");

    assert!(output.contains(
        r#"datalens_compaction_small_objects{chain="ethereum",chain_kind="evm",dataset="evm.logs",selector="0xabc",selector_kind="evm_logs"} 6"#
    ));
    assert!(output.contains(
        r#"datalens_compaction_manifest_segments{chain="ethereum",chain_kind="evm",dataset="evm.logs",selector="0xabc",selector_kind="evm_logs"} 3"#
    ));
    assert!(output.contains(
        r#"datalens_compaction_candidate_backlog{chain="ethereum",chain_kind="evm",dataset="evm.logs",selector="0xabc",selector_kind="evm_logs"} 2"#
    ));
    assert!(output.contains(
        r#"datalens_compaction_input_objects_total{chain="ethereum",chain_kind="evm",pause_reason="none",status="partial"} 4"#
    ));
    assert!(output.contains(
        r#"datalens_compaction_output_objects_total{chain="ethereum",chain_kind="evm",pause_reason="none",status="partial"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_compaction_deleted_source_objects_total{chain="ethereum",chain_kind="evm",pause_reason="none",status="partial"} 3"#
    ));
    assert!(output.contains(
        r#"datalens_compaction_deleted_manifest_segments_total{chain="ethereum",chain_kind="evm",pause_reason="none",status="partial"} 2"#
    ));
    assert!(output.contains(
        r#"datalens_compaction_paused{chain="ethereum",chain_kind="evm",reason="query_latency"} 1"#
    ));
    assert!(output.contains("datalens_compaction_tick_duration_seconds"));
}

#[test]
fn test_unknown_application_fallback_is_stable() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let labels = labels(None);

    recorder.record_query(&labels, QueryOutcome::Hit);

    let output = recorder.encode().expect("prometheus text");
    assert!(output.contains(r#"application="unknown""#));
}

#[test]
fn test_metrics_labels_do_not_include_filter_values() {
    let labels = MetricsLabels::from_dataset_key(
        ApplicationIdentity::named("wallet-search"),
        ChainIdentity::expect_new(ChainFamily::Evm, "ethereum"),
        DatasetKey::evm_logs(),
    );

    assert_eq!(
        labels.label_values(),
        ["wallet-search", "ethereum", "evm", "evm.logs"]
    );

    let recorder = MetricsRecorder::new().expect("metrics recorder");
    recorder.record_query(&labels, QueryOutcome::Miss);
    let output = recorder.encode().expect("prometheus text");

    assert!(!output.contains("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    assert!(!output.contains("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
}

#[test]
fn test_hot_cache_metrics_have_distinct_outcome_labels() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let labels = labels(Some("indexer"));

    recorder.record_query(&labels, QueryOutcome::HotHit);
    recorder.record_cache_coverage(&labels, CacheCoverageOutcome::HotMiss);
    recorder.record_fill(&labels, FillOutcome::LiveFetch);
    recorder.record_fill(&labels, FillOutcome::PromotionWritten);

    let output = recorder.encode().expect("prometheus text");

    assert!(output.contains(
        r#"datalens_query_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="hot_hit"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_cache_coverage_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="hot_miss"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_fill_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="live_fetch"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_fill_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="promotion_written"} 1"#
    ));
}

#[test]
fn test_hot_reorg_metrics_record_detection_rollback_stale_and_refetch_outcomes() {
    let recorder = MetricsRecorder::new().expect("metrics recorder");
    let labels = labels(Some("indexer"));

    recorder.record_hot_reorg(&labels, HotReorgOutcome::Detected, 1);
    recorder.record_hot_reorg(&labels, HotReorgOutcome::RollbackApplied, 1);
    recorder.record_hot_reorg(&labels, HotReorgOutcome::StaleEntry, 3);
    recorder.record_hot_reorg(&labels, HotReorgOutcome::RefetchSucceeded, 1);

    let output = recorder.encode().expect("prometheus text");

    assert!(output.contains(
        r#"datalens_hot_reorg_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="detected"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_hot_reorg_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="rollback_applied"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_hot_reorg_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="stale_entry"} 3"#
    ));
    assert!(output.contains(
        r#"datalens_hot_reorg_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="evm.logs",outcome="refetch_succeeded"} 1"#
    ));
}

#[test]
fn test_metrics_labels_preserve_native_dataset_keys() {
    assert_eq!(
        labels_with(
            ChainIdentity::expect_new(ChainFamily::Evm, "ethereum"),
            DatasetKey::evm_logs(),
        )
        .label_values(),
        ["indexer", "ethereum", "evm", "evm.logs"]
    );
    assert_eq!(
        labels_with(
            ChainIdentity::expect_new(
                ChainFamily::try_other("solana").expect("solana family"),
                "solana-mainnet-beta",
            ),
            DatasetKey::solana_slots(),
        )
        .label_values(),
        ["indexer", "solana-mainnet-beta", "solana", "solana.slots"]
    );
    assert_eq!(
        labels_with(
            ChainIdentity::expect_new(
                ChainFamily::try_other("tron").expect("tron family"),
                "tron-mainnet",
            ),
            DatasetKey::tron_blocks(),
        )
        .label_values(),
        ["indexer", "tron-mainnet", "tron", "tron.blocks"]
    );
}

fn labels(application: Option<&str>) -> MetricsLabels {
    MetricsLabels::from_dataset_key(
        ApplicationIdentity::from_optional(application),
        chain(),
        DatasetKey::evm_logs(),
    )
}

fn chain() -> ChainIdentity {
    ChainIdentity::expect_new(ChainFamily::Evm, "ethereum")
}

fn labels_with(chain: ChainIdentity, dataset_key: DatasetKey) -> MetricsLabels {
    MetricsLabels::from_dataset_key(ApplicationIdentity::named("indexer"), chain, dataset_key)
}
