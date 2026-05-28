use datalens_chain::{
    ChainAdapter, ChainFetchRequest, DatasetSelector, FinalityKind, HeightRangeKind,
};
use datalens_core::{DatalensErrorKind, DatasetKey, LedgerRange, QueryRows};
use datalens_solana::{
    SolanaAdapter, SolanaFixtureRpc, solana_address_selector, solana_all_selector,
    solana_program_selector,
};

#[test]
fn test_slots_fetch_skips_missing_slots_and_keeps_ordered_adapter_json_rows() {
    let adapter = SolanaAdapter::with_fixture_defaults();
    let request = ChainFetchRequest::new(
        adapter.capabilities().chain().clone(),
        DatasetKey::solana_slots(),
        LedgerRange::slots(10, 12).expect("valid range"),
        solana_all_selector().expect("selector"),
    );

    let response = adapter.fetch(request.clone()).expect("fetch slots");
    response
        .validate_for_request(&request)
        .expect("response matches request");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(
        rows.iter()
            .map(|row| row["slot"].as_u64().expect("slot"))
            .collect::<Vec<_>>(),
        vec![10, 12]
    );
    assert_eq!(rows[0]["commitment"], "finalized");
    assert_eq!(rows[0]["blockhash"], "slot-10-hash");
    assert_eq!(rows[0]["previous_blockhash"], "slot-9-hash");
    assert_eq!(rows[0]["parent_slot"], 9);
    assert_eq!(rows[0]["transaction_count"], 1);
}

#[test]
fn test_program_selector_fetches_transactions_and_instructions() {
    let adapter = SolanaAdapter::with_fixture_defaults();
    let selector = solana_program_selector("program1111111111111111111111111111111111")
        .expect("program selector");
    let chain = adapter.capabilities().chain().clone();

    let transactions = adapter
        .fetch(ChainFetchRequest::new(
            chain.clone(),
            DatasetKey::solana_transactions(),
            LedgerRange::slots(10, 12).expect("valid range"),
            selector.clone(),
        ))
        .expect("transactions");
    let QueryRows::AdapterJson { rows, .. } = transactions.rows.rows() else {
        panic!("expected transaction JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["signature"], "sig-slot-10");
    assert_eq!(rows[0]["selector_kind"], "solana_program");

    let instructions = adapter
        .fetch(ChainFetchRequest::new(
            chain,
            DatasetKey::solana_instructions(),
            LedgerRange::slots(10, 12).expect("valid range"),
            selector,
        ))
        .expect("instructions");
    let QueryRows::AdapterJson { rows, .. } = instructions.rows.rows() else {
        panic!("expected instruction JSON rows");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0]["program_id"],
        "program1111111111111111111111111111111111"
    );
    assert_eq!(rows[0]["instruction_path"], "0");
    assert_eq!(rows[1]["instruction_path"], "0/0");
}

#[test]
fn test_address_selector_uses_stable_storage_safe_fingerprint() {
    let first =
        solana_address_selector(" Account111111111111111111111111111111111 ").expect("selector");
    let second =
        solana_address_selector("Account111111111111111111111111111111111").expect("selector");

    assert_eq!(first, second);
    assert_eq!(
        first.canonical_key(),
        "address/Account111111111111111111111111111111111"
    );
    assert!(first.fingerprint().starts_with("solana-address/"));
    assert!(!first.fingerprint().contains("Account111"));
}

#[test]
fn test_finality_boundaries_are_slot_based_and_finalized_is_durable() {
    let adapter = SolanaAdapter::with_fixture_defaults();
    let latest = adapter.latest_height().expect("latest slot");
    let safe = adapter.cache_safe_height().expect("safe slot");
    let finalized = adapter.finalized_height().expect("finalized slot");

    assert_eq!(latest.range_kind, HeightRangeKind::Slot);
    assert_eq!(latest.value, 14);
    assert_eq!(latest.finality, FinalityKind::Latest);
    assert_eq!(safe.range_kind, HeightRangeKind::Slot);
    assert_eq!(safe.value, 12);
    assert_eq!(safe.finality, FinalityKind::Finalized);
    assert_eq!(finalized, safe);
}

#[test]
fn test_unsupported_evm_and_block_requests_are_stable_errors() {
    let adapter = SolanaAdapter::with_fixture_defaults();
    let chain = adapter.capabilities().chain().clone();
    let error = adapter
        .fetch(ChainFetchRequest::new(
            chain.clone(),
            DatasetKey::evm_logs(),
            LedgerRange::slots(10, 10).expect("valid range"),
            DatasetSelector::all(),
        ))
        .expect_err("evm logs unsupported");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);

    let error = adapter
        .fetch(ChainFetchRequest::new(
            chain,
            DatasetKey::solana_slots(),
            LedgerRange::blocks(10, 10).expect("valid range"),
            solana_all_selector().expect("selector"),
        ))
        .expect_err("block ranges unsupported");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
}

#[test]
fn test_provider_limit_is_classified_for_oversized_slot_ranges() {
    let adapter = SolanaAdapter::with_provider_limits(SolanaFixtureRpc, 2);
    let error = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_slots(),
            LedgerRange::slots(10, 13).expect("valid range"),
            solana_all_selector().expect("selector"),
        ))
        .expect_err("range too large");

    assert_eq!(error.kind, DatalensErrorKind::ProviderLimit);
}
