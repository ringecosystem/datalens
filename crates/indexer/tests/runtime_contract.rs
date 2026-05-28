use datalens_chain::{ChainHeight, DatasetSelector, FinalityLevel, HeightRangeKind};
use datalens_core::{ChainFamily, ChainIdentity, DatasetKey, LedgerRange, NetworkId};
use datalens_indexer::*;
use datalens_metrics::ApplicationIdentity;

#[test]
fn test_index_job_contract_captures_chain_datasets_range_and_run_mode() {
    let job = IndexJob {
        id: IndexJobId::new("job-ethereum-logs").expect("valid job id"),
        application: ApplicationIdentity::named("backfill-worker"),
        chain: ethereum_identity(),
        range: LedgerRange::blocks(100, 199).expect("valid range"),
        dataset_selection: IndexDatasetSelection::Selected(vec![IndexDatasetRequest {
            dataset_key: DatasetKey::evm_logs(),
            selector: DatasetSelector::all(),
        }]),
        finality_requirement: IndexFinalityRequirement::Safe,
        runtime_config: IndexRuntimeConfig::default(),
        run_mode: IndexRunMode::Backfill,
        retry_policy: IndexRetryPolicy::default(),
    };

    assert_eq!(job.id.as_str(), "job-ethereum-logs");
    assert_eq!(job.application.as_str(), "backfill-worker");
    assert_eq!(job.chain, ethereum_identity());
    assert_eq!(
        job.range,
        LedgerRange::blocks(100, 199).expect("valid range")
    );
    assert_eq!(job.run_mode, IndexRunMode::Backfill);
    assert!(job.run_mode.writes_durable_data());
}

#[test]
fn test_index_plan_splits_dataset_ranges_by_provider_safe_chunk_limit() {
    let job = IndexJob {
        id: IndexJobId::new("job-ethereum-blocks").expect("valid job id"),
        application: ApplicationIdentity::named("indexer"),
        chain: ethereum_identity(),
        range: LedgerRange::blocks(1, 5).expect("valid range"),
        dataset_selection: IndexDatasetSelection::Selected(vec![IndexDatasetRequest {
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
        }]),
        finality_requirement: IndexFinalityRequirement::Safe,
        runtime_config: IndexRuntimeConfig::default(),
        run_mode: IndexRunMode::Backfill,
        retry_policy: IndexRetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 5_000,
        },
    };
    let dataset_limit = IndexDatasetProviderLimit {
        dataset_key: DatasetKey::evm_blocks(),
        range_kind: HeightRangeKind::Block,
        max_range_len: 2,
    };

    let plan = IndexPlan::try_new(
        job,
        ChainHeight::block(5).with_finality(FinalityLevel::Finalized),
        vec![dataset_limit],
        vec![LedgerRange::blocks(1, 2).expect("covered range")],
    )
    .expect("index plan");

    assert_eq!(plan.finality_boundary.value, 5);
    assert_eq!(plan.durable_finality, FinalityLevel::Finalized);
    assert_eq!(
        plan.skipped_ranges,
        vec![IndexSkippedRange {
            dataset_key: DatasetKey::evm_blocks(),
            range: LedgerRange::blocks(1, 2).expect("covered range"),
            reason: IndexSkipReason::CoveredByManifest,
        }]
    );
    assert_eq!(
        plan.chunks,
        vec![
            IndexChunk {
                ordinal: 0,
                dataset_key: DatasetKey::evm_blocks(),
                selector: DatasetSelector::all(),
                range: LedgerRange::blocks(3, 4).expect("chunk range"),
                retry_policy: plan.retry_policy.clone(),
            },
            IndexChunk {
                ordinal: 1,
                dataset_key: DatasetKey::evm_blocks(),
                selector: DatasetSelector::all(),
                range: LedgerRange::blocks(5, 5).expect("chunk range"),
                retry_policy: plan.retry_policy.clone(),
            },
        ]
    );
}

#[test]
fn test_index_plan_rejects_latest_or_out_of_boundary_durable_ranges() {
    let job = block_job(1, 10, IndexRunMode::Backfill);

    let latest_error = IndexPlan::try_new(
        job.clone(),
        ChainHeight::block(10).with_finality(FinalityLevel::Latest),
        vec![IndexDatasetProviderLimit {
            dataset_key: DatasetKey::evm_blocks(),
            range_kind: HeightRangeKind::Block,
            max_range_len: 5,
        }],
        Vec::new(),
    )
    .expect_err("latest boundary is rejected");

    assert!(latest_error.message.contains("safe or finalized"));

    let capped_plan = IndexPlan::try_new(
        job,
        ChainHeight::block(9).with_finality(FinalityLevel::Safe),
        vec![IndexDatasetProviderLimit {
            dataset_key: DatasetKey::evm_blocks(),
            range_kind: HeightRangeKind::Block,
            max_range_len: 5,
        }],
        Vec::new(),
    )
    .expect("range is capped at finality boundary");

    assert_eq!(
        capped_plan.planned_range,
        LedgerRange::blocks(1, 9).expect("capped range")
    );
}

#[test]
fn test_verify_mode_plans_coverage_checks_without_write_chunks() {
    let plan = IndexPlan::try_new(
        block_job(1, 3, IndexRunMode::Verify),
        ChainHeight::block(3).with_finality(FinalityLevel::Safe),
        vec![IndexDatasetProviderLimit {
            dataset_key: DatasetKey::evm_blocks(),
            range_kind: HeightRangeKind::Block,
            max_range_len: 2,
        }],
        vec![LedgerRange::blocks(1, 3).expect("covered range")],
    )
    .expect("verify plan");

    assert!(plan.chunks.is_empty());
    assert_eq!(
        plan.verification_ranges,
        vec![IndexVerificationRange {
            dataset_key: DatasetKey::evm_blocks(),
            range: LedgerRange::blocks(1, 3).expect("covered range"),
        }]
    );
    assert!(!plan.job.run_mode.writes_durable_data());
}

#[test]
fn test_cursor_checkpoint_and_run_result_are_accounting_not_coverage() {
    let checkpoint = IndexCheckpoint {
        job_id: IndexJobId::new("job-1").expect("valid job id"),
        chain: ethereum_identity(),
        chunk: IndexChunk {
            ordinal: 7,
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            range: LedgerRange::blocks(40, 49).expect("chunk range"),
            retry_policy: IndexRetryPolicy::default(),
        },
        durable_write: Some(IndexDurableWriteSummary {
            finality_level: FinalityLevel::Safe,
            data_objects: 1,
            empty_coverages: 0,
            rows_written: 10,
        }),
        provider_calls: 2,
        attempts: 1,
    };
    let cursor = IndexCursor::from_checkpoint(&checkpoint);

    assert_eq!(cursor.job_id, checkpoint.job_id);
    assert_eq!(cursor.next_chunk_ordinal, 8);
    assert_eq!(
        cursor.last_checkpointed_range,
        Some(LedgerRange::blocks(40, 49).expect("chunk range"))
    );
    assert!(!cursor.is_durable_coverage());

    let result = IndexRunResult {
        job_id: checkpoint.job_id.clone(),
        mode: IndexRunMode::Resume,
        status: IndexRunStatus::Completed,
        checkpoints: vec![checkpoint],
        accounting: IndexAccounting {
            provider_calls: 2,
            rows_written: 10,
            skipped_ranges: 1,
            retries: 0,
            failures: 0,
            ..IndexAccounting::default()
        },
    };

    assert_eq!(result.accounting.provider_calls, 2);
    assert_eq!(result.accounting.rows_written, 10);
    assert_eq!(result.status, IndexRunStatus::Completed);
}

fn block_job(start: u64, end: u64, run_mode: IndexRunMode) -> IndexJob {
    IndexJob {
        id: IndexJobId::new("job-ethereum-blocks").expect("valid job id"),
        application: ApplicationIdentity::named("indexer"),
        chain: ethereum_identity(),
        range: LedgerRange::blocks(start, end).expect("valid range"),
        dataset_selection: IndexDatasetSelection::Selected(vec![IndexDatasetRequest {
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
        }]),
        finality_requirement: IndexFinalityRequirement::Safe,
        runtime_config: IndexRuntimeConfig::default(),
        run_mode,
        retry_policy: IndexRetryPolicy::default(),
    }
}

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain")
}
