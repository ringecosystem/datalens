use datalens_api::{auth::AuthenticationHook, compatibility::CompatibilityAdapter};
use datalens_chain::{AdapterCapabilities, ChainAdapter};
use datalens_core::{
    ChainFamily, ChainIdentity, CoverageLevel, DatalensErrorKind, DatasetId, ResultEnvelope,
    TimeRange,
};
use datalens_evm::EvmAdapterMetadata;
use datalens_planner::{PlanRequest, PlanStatus};
use datalens_storage::Storage;
use datalens_writer::{WriteRequest, WriteStatus};

#[test]
fn workspace_exposes_architecture_boundaries() {
    let chain = ChainIdentity::expect_new(ChainFamily::Evm, "ethereum-mainnet");
    let dataset = DatasetId::expect_new("logs");
    let range = TimeRange::expect_blocks(1, 2);
    let envelope = ResultEnvelope::ok(dataset.clone(), range, Vec::<u8>::new());
    let capabilities = AdapterCapabilities::new(chain.clone()).with_dataset(dataset.clone());

    assert_eq!(chain.family(), ChainFamily::Evm);
    assert_eq!(range.start(), 1);
    assert_eq!(range.end(), 2);
    assert_eq!(envelope.dataset(), &dataset);
    assert_eq!(capabilities.chain(), &chain);
    assert_eq!(CoverageLevel::Missing, PlanStatus::Missing.coverage_level());
    assert_eq!(
        DatalensErrorKind::UnsupportedDataset,
        WriteStatus::Deferred.error_kind()
    );

    fn assert_chain_adapter<T: ChainAdapter>() {}
    fn assert_storage<T: Storage>() {}
    fn assert_auth_hook<T: AuthenticationHook>() {}
    fn assert_compatibility<T: CompatibilityAdapter>() {}

    let _ = EvmAdapterMetadata::default();
    let _ = PlanRequest::new(chain.clone(), dataset.clone(), range);
    let _ = WriteRequest::new(chain, dataset, range);

    let _chain_adapter_type_check = assert_chain_adapter::<datalens_evm::EvmAdapter>;
    let _storage_type_check = assert_storage::<datalens_storage::InMemoryStorage>;
    let _auth_hook_type_check = assert_auth_hook::<datalens_api::auth::NoAuthentication>;
    let _compatibility_type_check =
        assert_compatibility::<datalens_api::compatibility::NativeCompatibility>;
}
