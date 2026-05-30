use datalens_chain::{ChainAdapter, ChainFetchRequest};
use datalens_chain_conformance::{
    TronFixtureProvider, assert_tron_capability_conformance, assert_tron_fetch_conformance,
    assert_tron_finality_conformance, assert_tron_metadata_conformance,
    assert_tron_reorg_signal_conformance,
};
use datalens_core::{DatasetKey, LedgerRange};
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

    let request = ChainFetchRequest::new(
        fixture.chain(),
        DatasetKey::tron_events(),
        LedgerRange::blocks(10, 10).expect("range"),
        fixture.event_selector(),
    );
    let response = adapter.fetch(request).expect("filtered events");
    assert_eq!(response.rows.row_count(), 1);
}
