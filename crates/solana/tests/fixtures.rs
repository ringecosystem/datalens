use datalens_chain::{
    ChainAdapter, ChainFetchRequest, DatasetSelector, FinalityKind, HeightRangeKind,
};
use datalens_core::{DatalensErrorKind, DatasetKey, LedgerRange, QueryRows};
use datalens_solana::{
    SolanaAdapter, SolanaFixtureRpc, SolanaSignatureInfo, solana_address_selector,
    solana_all_selector, solana_program_selector, solana_signature_selector,
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
fn test_all_selector_fetches_account_balance_updates() {
    let adapter = SolanaAdapter::with_fixture_defaults();
    let response = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_account_updates(),
            LedgerRange::slots(10, 12).expect("valid range"),
            solana_all_selector().expect("selector"),
        ))
        .expect("account updates");
    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected account update JSON rows");
    };

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0]["slot"], 10);
    assert_eq!(rows[0]["signature"], "sig-slot-10");
    assert_eq!(
        rows[0]["account"],
        "Account111111111111111111111111111111111"
    );
    assert_eq!(rows[0]["account_index"], 0);
    assert_eq!(rows[0]["update_kind"], "lamports");
    assert_eq!(rows[0]["lamports_before"], 1_000_000);
    assert_eq!(rows[0]["lamports_after"], 900_000);
    assert_eq!(rows[0]["lamports_delta"], -100_000);
    assert_eq!(rows[0]["source"], "getBlock.transaction.meta");
    assert_eq!(rows[0]["selector_kind"], "solana_all");
    assert_eq!(rows[0]["commitment"], "finalized");
    assert_eq!(rows[1]["update_kind"], "spl_token");
    assert_eq!(rows[1]["mint"], "TokenMint11111111111111111111111111111111");
    assert_eq!(rows[1]["token_amount_before"], "10");
    assert_eq!(rows[1]["token_amount_after"], "7");
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

#[test]
fn test_signature_selector_uses_get_transaction_when_available() {
    let provider = OptimizedSolanaRpc::default();
    let adapter = SolanaAdapter::with_provider(default_chain(), provider.clone());
    let response = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_transactions(),
            LedgerRange::slots(10, 12).expect("valid range"),
            solana_signature_selector("sigslot10").expect("selector"),
        ))
        .expect("transactions");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["signature"], "sigslot10");
    assert_eq!(provider.transaction_calls(), vec!["sigslot10"]);
    assert_eq!(provider.blocks_with_limit_calls(), 0);
    assert_eq!(provider.block_calls(), Vec::<u64>::new());
}

#[test]
fn test_address_selector_discovers_signatures_before_fetching_transactions() {
    let provider = OptimizedSolanaRpc::default();
    let adapter = SolanaAdapter::with_provider(default_chain(), provider.clone());
    let response = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_account_updates(),
            LedgerRange::slots(10, 12).expect("valid range"),
            solana_address_selector("Account111111111111111111111111111111111").expect("selector"),
        ))
        .expect("account updates");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(
        provider.signature_address_calls(),
        vec![
            "Account111111111111111111111111111111111",
            "Account111111111111111111111111111111111",
        ]
    );
    assert_eq!(provider.transaction_calls(), vec!["sigslot10"]);
    assert_eq!(provider.blocks_with_limit_calls(), 0);
}

#[test]
fn test_optimized_selector_fetch_falls_back_to_slot_scan_on_provider_limit() {
    let provider = OptimizedSolanaRpc::with_signature_discovery_error(
        DatalensErrorKind::ProviderLimit,
        "provider limit",
    );
    let adapter = SolanaAdapter::with_provider(default_chain(), provider.clone());
    let response = adapter
        .fetch(ChainFetchRequest::new(
            adapter.capabilities().chain().clone(),
            DatasetKey::solana_transactions(),
            LedgerRange::slots(10, 12).expect("valid range"),
            solana_address_selector("Account111111111111111111111111111111111").expect("selector"),
        ))
        .expect("transactions");

    let QueryRows::AdapterJson { rows, .. } = response.rows.rows() else {
        panic!("expected adapter JSON rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(provider.signature_address_calls().len(), 1);
    assert_eq!(provider.blocks_with_limit_calls(), 1);
    assert_eq!(provider.block_calls(), vec![10, 12]);
    assert!(
        response
            .provider_diagnostics
            .warnings
            .contains(&"solana optimized selector fetch failed; fell back to slot scan".to_owned())
    );
}

#[derive(Clone, Default)]
struct OptimizedSolanaRpc {
    state: std::sync::Arc<std::sync::Mutex<OptimizedState>>,
}

#[derive(Default)]
struct OptimizedState {
    signature_address_calls: Vec<String>,
    transaction_calls: Vec<String>,
    blocks_with_limit_calls: u64,
    block_calls: Vec<u64>,
    signature_discovery_error: Option<DatalensErrorKind>,
    signature_discovery_message: String,
}

impl OptimizedSolanaRpc {
    fn with_signature_discovery_error(kind: DatalensErrorKind, message: &str) -> Self {
        let provider = Self::default();
        {
            let mut state = provider.state.lock().expect("state");
            state.signature_discovery_error = Some(kind);
            state.signature_discovery_message = message.to_owned();
        }
        provider
    }

    fn signature_address_calls(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("state")
            .signature_address_calls
            .clone()
    }

    fn transaction_calls(&self) -> Vec<String> {
        self.state.lock().expect("state").transaction_calls.clone()
    }

    fn blocks_with_limit_calls(&self) -> u64 {
        self.state.lock().expect("state").blocks_with_limit_calls
    }

    fn block_calls(&self) -> Vec<u64> {
        self.state.lock().expect("state").block_calls.clone()
    }
}

impl datalens_solana::SolanaRpc for OptimizedSolanaRpc {
    fn get_slot(
        &self,
        commitment: datalens_solana::SolanaCommitment,
    ) -> Result<u64, datalens_core::DatalensError> {
        SolanaFixtureRpc.get_slot(commitment)
    }

    fn get_blocks_with_limit(
        &self,
        start_slot: u64,
        limit: u64,
        commitment: datalens_solana::SolanaCommitment,
    ) -> Result<Vec<u64>, datalens_core::DatalensError> {
        self.state.lock().expect("state").blocks_with_limit_calls += 1;
        SolanaFixtureRpc.get_blocks_with_limit(start_slot, limit, commitment)
    }

    fn get_block(
        &self,
        slot: u64,
        commitment: datalens_solana::SolanaCommitment,
    ) -> Result<Option<datalens_solana::SolanaBlock>, datalens_core::DatalensError> {
        self.state.lock().expect("state").block_calls.push(slot);
        SolanaFixtureRpc.get_block(slot, commitment)
    }

    fn get_signatures_for_address(
        &self,
        address: &str,
        before: Option<&str>,
        _until: Option<&str>,
        _limit: usize,
        _commitment: datalens_solana::SolanaCommitment,
    ) -> Result<Vec<SolanaSignatureInfo>, datalens_core::DatalensError> {
        let mut state = self.state.lock().expect("state");
        state.signature_address_calls.push(address.to_owned());
        if let Some(kind) = state.signature_discovery_error.clone() {
            return Err(datalens_core::DatalensError::new(
                kind,
                state.signature_discovery_message.clone(),
            ));
        }
        if before.is_some() {
            return Ok(Vec::new());
        }
        Ok(vec![SolanaSignatureInfo {
            signature: "sigslot10".to_owned(),
            slot: 10,
        }])
    }

    fn get_transaction(
        &self,
        signature: &str,
        commitment: datalens_solana::SolanaCommitment,
    ) -> Result<Option<datalens_solana::SolanaTransactionWithSlot>, datalens_core::DatalensError>
    {
        self.state
            .lock()
            .expect("state")
            .transaction_calls
            .push(signature.to_owned());
        let block = SolanaFixtureRpc
            .get_block(10, commitment)?
            .expect("fixture block");
        Ok(block
            .transactions
            .into_iter()
            .next()
            .map(|mut transaction| {
                transaction.signature = signature.to_owned();
                datalens_solana::SolanaTransactionWithSlot {
                    slot: block.slot,
                    block_time: block.block_time,
                    blockhash: block.blockhash,
                    transaction,
                    raw: block.raw,
                }
            }))
    }

    fn provider_name(&self) -> &'static str {
        "optimized-solana-fixture"
    }
}

fn default_chain() -> datalens_core::ChainIdentity {
    datalens_core::ChainIdentity::try_new(
        datalens_core::ChainFamily::Other("solana".to_owned()),
        "solana-mainnet-beta",
        Some(datalens_core::NetworkId::textual("mainnet-beta").expect("network id")),
    )
    .expect("chain")
}
