use datalens_core::{ChainFamily, ChainIdentity, DatalensErrorKind, Dataset};
use datalens_metrics::{
    ApplicationIdentity, CacheCoverageOutcome, DurableWriteOutcome, ErrorLabels, FillOutcome,
    HotReorgOutcome, MetricsLabels, MetricsRecorder, QueryOutcome, WarmupFetchOutcome,
    WarmupTaskOutcome, WarmupWriteOutcome,
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
        r#"datalens_query_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs",outcome="miss"} 1"#
    ));
    assert!(output.contains("datalens_query_duration_seconds"));
    assert!(output.contains(
        r#"datalens_cache_coverage_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs",outcome="partial_hit"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_fill_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs",outcome="filled"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_durable_write_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs",outcome="staged"} 1"#
    ));
    assert!(output.contains("datalens_fill_duration_seconds"));
    assert!(output.contains(
        r#"datalens_provider_error_total{chain="ethereum",chain_kind="evm",dataset="logs",error_kind="provider_timeout"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_storage_error_total{chain="ethereum",chain_kind="evm",dataset="logs",error_kind="storage_read_failure"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_application_chain_latest_requested_block{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs"} 42"#
    ));
    assert!(output.contains(
        r#"datalens_application_chain_latest_filled_block{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs"} 40"#
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
        r#"datalens_warmup_task_total{application="warmup-app",chain="ethereum",chain_kind="evm",dataset="logs",outcome="completed"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_warmup_fetch_total{application="warmup-app",chain="ethereum",chain_kind="evm",dataset="logs",outcome="fetched",selector_kind="evm_logs"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_warmup_write_total{application="warmup-app",chain="ethereum",chain_kind="evm",dataset="logs",outcome="written"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_warmup_rows_total{application="warmup-app",chain="ethereum",chain_kind="evm",dataset="logs"} 7"#
    ));
    assert!(output.contains(
        r#"datalens_warmup_provider_error_total{application="warmup-app",chain="ethereum",chain_kind="evm",dataset="logs",error_kind="provider_limit",selector_kind="evm_logs"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_warmup_current_height{application="warmup-app",chain="ethereum",chain_kind="evm",dataset="logs"} 123"#
    ));
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
    let labels = MetricsLabels::new(
        ApplicationIdentity::named("wallet-search"),
        ChainIdentity::expect_new(ChainFamily::Evm, "ethereum"),
        Dataset::Logs,
    );

    assert_eq!(
        labels.label_values(),
        ["wallet-search", "ethereum", "evm", "logs"]
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
        r#"datalens_query_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs",outcome="hot_hit"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_cache_coverage_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs",outcome="hot_miss"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_fill_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs",outcome="live_fetch"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_fill_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs",outcome="promotion_written"} 1"#
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
        r#"datalens_hot_reorg_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs",outcome="detected"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_hot_reorg_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs",outcome="rollback_applied"} 1"#
    ));
    assert!(output.contains(
        r#"datalens_hot_reorg_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs",outcome="stale_entry"} 3"#
    ));
    assert!(output.contains(
        r#"datalens_hot_reorg_total{application="indexer",chain="ethereum",chain_kind="evm",dataset="logs",outcome="refetch_succeeded"} 1"#
    ));
}

fn labels(application: Option<&str>) -> MetricsLabels {
    MetricsLabels::new(
        ApplicationIdentity::from_optional(application),
        ChainIdentity::expect_new(ChainFamily::Evm, "ethereum"),
        Dataset::Logs,
    )
}
