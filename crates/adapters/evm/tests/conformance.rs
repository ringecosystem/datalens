use datalens_chain_conformance::{
    FixtureProvider, assert_capability_conformance, assert_fetch_conformance,
    assert_finality_conformance, assert_metadata_conformance, assert_reorg_signal_conformance,
};
use datalens_evm::{EvmFinalityPolicy, EvmRpcClient};

#[test]
fn test_evm_adapter_passes_chain_conformance_suite() {
    let provider = FixtureProvider::evm();
    let adapter = EvmRpcClient::with_chain(
        vec![provider.url()],
        provider.chain(),
        EvmFinalityPolicy::Lag {
            safe_lag_blocks: Some(2),
            finalized_lag_blocks: Some(4),
        },
        2,
        3,
        3,
        1,
    );

    assert_capability_conformance(&adapter, provider.chain());
    assert_fetch_conformance(&adapter, provider.evm_log_selector());
    assert_finality_conformance(&adapter);
    assert_reorg_signal_conformance(&adapter);
    assert_metadata_conformance(&adapter, provider.evm_log_selector());
}
