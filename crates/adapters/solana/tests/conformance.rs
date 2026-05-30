use datalens_chain_conformance::{
    SolanaFixtureProvider, assert_solana_capability_conformance, assert_solana_fetch_conformance,
    assert_solana_finality_conformance, assert_solana_metadata_conformance,
    assert_solana_reorg_signal_conformance,
};
use datalens_solana::{SolanaAdapter, SolanaFixtureRpc};

#[test]
fn test_solana_adapter_passes_chain_conformance_suite() {
    let fixture = SolanaFixtureProvider::solana();
    let adapter = SolanaAdapter::with_provider(fixture.chain(), SolanaFixtureRpc);

    assert_solana_capability_conformance(&adapter, fixture.chain());
    assert_solana_fetch_conformance(&adapter, fixture.program_selector());
    assert_solana_finality_conformance(&adapter);
    assert_solana_reorg_signal_conformance(&adapter);
    assert_solana_metadata_conformance(&adapter, fixture.program_selector());
}
