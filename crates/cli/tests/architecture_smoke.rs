use datalens_api::{auth::AuthenticationHook, compatibility::CompatibilityAdapter};
use datalens_chain::{AdapterCapabilities, ChainAdapter};
use datalens_core::{
    ChainFamily, ChainIdentity, CoverageLevel, DatasetId, DatasetKey, DatasetRows, LedgerRange,
    QueryRows, ResultEnvelope, TimeRange,
};
use datalens_evm::EvmAdapterMetadata;
use datalens_planner::{PlanRequest, PlanStatus};
use datalens_writer::{
    DurableWriteRequest, DurableWriteResult, DurableWriteSegment, DurableWriter,
    DurableWriterConfig,
};

#[test]
fn workspace_exposes_architecture_boundaries() {
    let chain = ChainIdentity::expect_new(ChainFamily::Evm, "ethereum-mainnet");
    let dataset = DatasetId::expect_new("logs");
    let range = TimeRange::expect_blocks(1, 2);
    let envelope = ResultEnvelope::ok(dataset.clone(), range, Vec::<u8>::new());
    let capabilities = AdapterCapabilities::new(chain.clone()).with_dataset(DatasetKey::evm_logs());

    assert_eq!(chain.family(), ChainFamily::Evm);
    assert_eq!(range.start(), 1);
    assert_eq!(range.end(), 2);
    assert_eq!(envelope.dataset(), &dataset);
    assert_eq!(capabilities.chain(), &chain);
    assert_eq!(CoverageLevel::Missing, PlanStatus::Missing.coverage_level());

    fn assert_chain_adapter<T: ChainAdapter>() {}
    fn assert_storage_repository<T: datalens_storage::StorageRepository>() {}
    fn assert_auth_hook<T: AuthenticationHook>() {}
    fn assert_compatibility<T: CompatibilityAdapter>() {}

    let _ = EvmAdapterMetadata::default();
    let _ = PlanRequest::new(chain.clone(), dataset.clone(), range);
    let _writer = DurableWriter::new(
        datalens_storage::LocalStorage::new(std::env::temp_dir().join("datalens-smoke")),
        DurableWriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
        },
    );
    let _write_request = DurableWriteRequest {
        chain,
        dataset_key: DatasetKey::evm_logs(),
        selector: datalens_chain::DatasetSelector::all(),
        finality_level: datalens_chain::FinalityLevel::Safe,
        segments: vec![DurableWriteSegment {
            range: LedgerRange::blocks(1, 2).expect("valid range"),
            rows: DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
                .expect("dataset rows"),
        }],
    };
    let _write_result = DurableWriteResult::default();

    let _chain_adapter_type_check = assert_chain_adapter::<datalens_evm::EvmAdapter>;
    let _storage_type_check = assert_storage_repository::<datalens_storage::LocalStorage>;
    let _auth_hook_type_check = assert_auth_hook::<datalens_api::auth::NoAuthentication>;
    let _compatibility_type_check =
        assert_compatibility::<datalens_api::compatibility::NativeCompatibility>;
}
