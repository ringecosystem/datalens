use datalens_chain::{
    AdapterCapabilities, ChainAdapter, ChainFetchRequest, ChainFetchResponse, ChainHeight,
    DatasetCapability, DatasetSelector, HeightRangeKind, SelectorKind,
};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset, DatasetKey, DatasetRows,
    LedgerRange, NetworkId,
};
use datalens_metrics::ApplicationIdentity;
use datalens_runtime_indexer::*;
use datalens_storage::{Manifest, StorageRepository, StorageWriteOutcome, StorageWriteRequest};
use datalens_writer::DurableWriterConfig;

#[test]
fn test_verify_classifies_read_path_context_canceled_as_storage_failure() {
    let source = VerifyAdapter;

    let error = IndexRuntime::new(
        source.clone(),
        FailingReadStorage,
        InMemoryIndexCursorStore::default(),
        DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
    )
    .run(block_job(1, 2))
    .expect_err("storage read cancellation fails");

    assert_eq!(error.kind, DatalensErrorKind::StorageReadFailure);
    assert!(error.message.contains("verify storage read failed"));
    assert!(error.message.contains("context canceled"));
    assert!(error.message.contains("retryable=true"));
}

fn block_job(start: u64, end: u64) -> IndexJob {
    IndexJob {
        id: IndexJobId::new("verify-failure").expect("job id"),
        application: ApplicationIdentity::named("indexer"),
        chain: ethereum_identity(),
        range: LedgerRange::blocks(start, end).expect("range"),
        dataset_selection: IndexDatasetSelection::Selected(vec![IndexDatasetRequest {
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
        }]),
        finality_requirement: IndexFinalityRequirement::Safe,
        runtime_config: IndexRuntimeConfig { max_chunk_len: 2 },
        run_mode: IndexRunMode::Verify,
        retry_policy: IndexRetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        },
    }
}

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("chain")
}

#[derive(Clone)]
struct VerifyAdapter;

impl ChainAdapter for VerifyAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(ethereum_identity()).with_dataset_capability(
            DatasetCapability::new(Dataset::Blocks)
                .with_selector(SelectorKind::All)
                .with_range(HeightRangeKind::Block)
                .with_max_range_len(2)
                .with_empty_coverage(true)
                .with_safe_height(true)
                .with_finalized_height(true)
                .with_range_split(true),
        )
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(100))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::ProviderFailure,
            "verify should not call provider finality",
        ))
    }

    fn finalized_height(&self) -> Result<ChainHeight, DatalensError> {
        self.cache_safe_height()
    }

    fn fetch(&self, _request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::ProviderFailure,
            "verify should not fetch provider rows",
        ))
    }
}

#[derive(Clone)]
struct FailingReadStorage;

impl StorageRepository for FailingReadStorage {
    fn manifest(&self) -> Result<Manifest, DatalensError> {
        Ok(Manifest::default())
    }

    fn covered_ranges(
        &self,
        _chain: &ChainIdentity,
        _dataset_key: &DatasetKey,
        _selector: &DatasetSelector,
        range: LedgerRange,
    ) -> Result<Vec<LedgerRange>, DatalensError> {
        Ok(vec![range])
    }

    fn read_rows(
        &self,
        _chain: &ChainIdentity,
        _dataset_key: &DatasetKey,
        _selector: &DatasetSelector,
        _range: LedgerRange,
    ) -> Result<DatasetRows, DatalensError> {
        Err(DatalensError::new(
            DatalensErrorKind::ProviderFailure,
            "context canceled",
        ))
    }

    fn write_rows(
        &self,
        _request: StorageWriteRequest<'_>,
    ) -> Result<StorageWriteOutcome, DatalensError> {
        unreachable!("verify must not write rows")
    }
}
