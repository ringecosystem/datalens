use datalens_chain::{
    ChainAdapter, ChainFetchRequest, DatasetSelector, FinalityKind, HeightRangeKind,
};
use datalens_core::{DatalensErrorKind, DatasetKey, LedgerRange, QueryRows};
use datalens_tron::{TronAdapter, TronFixtureProviderRpc, tron_all_selector};

#[test]
fn test_blocks_fetch_returns_ordered_adapter_json_rows() {
    let adapter = TronAdapter::with_fixture_defaults();
    let request = ChainFetchRequest::new(
        adapter.capabilities().chain().clone(),
        DatasetKey::tron_blocks(),
        LedgerRange::blocks(10, 12).expect("valid range"),
        tron_all_selector().expect("selector"),
    );

    let response = adapter.fetch(request.clone()).expect("fetch blocks");
    response
        .validate_for_request(&request)
        .expect("response matches request");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(
        rows.iter()
            .map(|row| row["number"].as_u64().expect("number"))
            .collect::<Vec<_>>(),
        vec![10, 11, 12]
    );
    assert_eq!(rows[0]["range_kind"], "block");
    assert_eq!(rows[0]["hash"], "000000000000000a-tron-hash");
    assert_eq!(rows[0]["parent_hash"], "0000000000000009-tron-hash");
    assert_eq!(rows[0]["timestamp"], 1_700_000_010_u64);
    assert_eq!(rows[0]["transaction_count"], 1);
    assert_eq!(
        rows[0]["witness_address"],
        "TTronWitness111111111111111111111111111"
    );
}

#[test]
fn test_all_selector_uses_storage_safe_fingerprint() {
    let selector = tron_all_selector().expect("selector");

    assert_eq!(selector.canonical_key(), "all");
    assert_eq!(selector.fingerprint(), "tron-all/all");
}

#[test]
fn test_finality_boundaries_are_block_based_and_finalized_is_durable() {
    let adapter = TronAdapter::with_fixture_defaults();
    let latest = adapter.latest_height().expect("latest block");
    let safe = adapter.cache_safe_height().expect("safe block");
    let finalized = adapter.finalized_height().expect("finalized block");

    assert_eq!(latest.range_kind, HeightRangeKind::Block);
    assert_eq!(latest.value, 14);
    assert_eq!(latest.finality, FinalityKind::Latest);
    assert_eq!(safe.range_kind, HeightRangeKind::Block);
    assert_eq!(safe.value, 12);
    assert_eq!(safe.finality, FinalityKind::Finalized);
    assert_eq!(finalized, safe);
}

#[test]
fn test_unsupported_events_and_evm_selectors_are_stable_errors() {
    let adapter = TronAdapter::with_fixture_defaults();
    let chain = adapter.capabilities().chain().clone();
    let error = adapter
        .fetch(ChainFetchRequest::new(
            chain.clone(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(10, 10).expect("valid range"),
            tron_all_selector().expect("selector"),
        ))
        .expect_err("events unsupported");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);

    let error = adapter
        .fetch(ChainFetchRequest::new(
            chain,
            DatasetKey::tron_blocks(),
            LedgerRange::blocks(10, 10).expect("valid range"),
            DatasetSelector::all(),
        ))
        .expect_err("plain all selector unsupported");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
}

#[test]
fn test_provider_limit_is_classified_for_oversized_block_ranges() {
    let adapter = TronAdapter::with_provider_limits(TronFixtureProviderRpc, 2);
    let error = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::tron_blocks(),
            LedgerRange::blocks(10, 13).expect("valid range"),
            tron_all_selector().expect("selector"),
        ))
        .expect_err("range too large");

    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);
}
