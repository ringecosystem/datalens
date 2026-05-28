use datalens_chain_conformance::{
    TronFixtureProvider, assert_tron_capability_conformance, assert_tron_fetch_conformance,
    assert_tron_finality_conformance, assert_tron_metadata_conformance,
    assert_tron_reorg_signal_conformance,
};
use datalens_tron::{TronAdapter, TronFixtureProviderRpc};

#[test]
fn test_tron_adapter_passes_chain_conformance_suite() {
    let fixture = TronFixtureProvider::tron();
    let adapter = TronAdapter::with_provider(fixture.chain(), TronFixtureProviderRpc);

    assert_tron_capability_conformance(&adapter, fixture.chain());
    assert_tron_fetch_conformance(&adapter, fixture.all_selector());
    assert_tron_finality_conformance(&adapter);
    assert_tron_reorg_signal_conformance(&adapter);
    assert_tron_metadata_conformance(&adapter, fixture.all_selector());
}
