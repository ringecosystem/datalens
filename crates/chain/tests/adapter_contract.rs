use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset, DatasetKey, DatasetRows,
    LedgerRange, LedgerRangeKind, LogFilter, NetworkId, QueryRows,
};

use datalens_chain::*;

#[derive(Clone)]
struct EmptyAdapter {
    chain: ChainIdentity,
}

impl ChainAdapter for EmptyAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(self.chain.clone()).with_dataset_capability(
            DatasetCapability::new(Dataset::Logs)
                .with_selector(SelectorKind::EvmLogs)
                .with_range(HeightRangeKind::Block)
                .with_max_range_len(2)
                .with_max_addresses_per_query(100)
                .with_max_topics_per_query(4)
                .with_empty_coverage(true)
                .with_safe_height(true)
                .with_finalized_height(true)
                .with_provider_native_finality_tags(true)
                .with_range_split(true),
        )
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(12))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(10).with_finality(FinalityKind::Safe))
    }

    fn canonical_block(
        &self,
        request: CanonicalBlockRequest,
    ) -> Result<CanonicalBlock, DatalensError> {
        Ok(CanonicalBlock {
            chain: self.chain.clone(),
            height: request.height,
            hash: format!("0x{:064x}", request.height),
            parent_hash: format!("0x{:064x}", request.height - 1),
            finality: FinalityKind::Latest,
        })
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        Ok(ChainFetchResponse::try_empty(
            request.chain,
            request.dataset_key,
            request.range,
            request.selector,
        )?
        .with_source_metadata(SourceMetadata {
            provider: "mock".to_owned(),
            request_id: Some("req-1".to_owned()),
        })
        .with_provider_diagnostics(ProviderDiagnostics {
            calls: 1,
            rows_scanned: 0,
            warnings: Vec::new(),
        }))
    }
}

#[test]
fn test_dataset_selector_fingerprint_is_stable_and_storage_safe() {
    let first = DatasetSelector::try_evm_logs(LogFilter {
        addresses: vec!["0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned()],
        topics: vec![None],
    })
    .expect("valid selector");
    let second = DatasetSelector::try_evm_logs(LogFilter {
        addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        topics: vec![None],
    })
    .expect("valid selector");

    assert_eq!(first.kind(), SelectorKind::EvmLogs);
    assert_eq!(first.fingerprint(), second.fingerprint());
    assert!(first.fingerprint().starts_with("evm-logs/addr-topic-"));
    assert!(!first.fingerprint().contains("0xaaaaaaaa"));
    assert_ne!(first.canonical_key(), first.fingerprint());
}

#[test]
fn test_dataset_selector_evm_logs_delegates_coverage() {
    let stored = DatasetSelector::try_evm_logs(LogFilter {
        addresses: vec![
            "0x1111111111111111111111111111111111111111".to_owned(),
            "0x2222222222222222222222222222222222222222".to_owned(),
        ],
        topics: vec![Some(vec![
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        ])],
    })
    .expect("valid selector");
    let query = DatasetSelector::try_evm_logs(LogFilter {
        addresses: vec!["0x1111111111111111111111111111111111111111".to_owned()],
        topics: vec![Some(vec![
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        ])],
    })
    .expect("valid selector");
    let broader_query = DatasetSelector::try_evm_logs(LogFilter {
        addresses: vec![
            "0x1111111111111111111111111111111111111111".to_owned(),
            "0x3333333333333333333333333333333333333333".to_owned(),
        ],
        topics: vec![Some(vec![
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        ])],
    })
    .expect("valid selector");

    assert!(stored.covers(&query));
    assert!(!stored.covers(&broader_query));
}

#[test]
fn test_dataset_selector_other_uses_exact_semantics_only() {
    let kind = AdapterKey::try_new("custom").expect("valid key");
    let stored = DatasetSelector::try_other(kind.clone(), "same-fingerprint", "same/canonical-key")
        .expect("valid selector");
    let same = DatasetSelector::try_other(kind.clone(), "same-fingerprint", "same/canonical-key")
        .expect("valid selector");
    let different_canonical =
        DatasetSelector::try_other(kind.clone(), "same-fingerprint", "different/canonical-key")
            .expect("valid selector");
    let different_fingerprint =
        DatasetSelector::try_other(kind.clone(), "different-fingerprint", "same/canonical-key")
            .expect("valid selector");
    let different_kind = DatasetSelector::try_other(
        AdapterKey::try_new("other-custom").expect("valid key"),
        "same-fingerprint",
        "same/canonical-key",
    )
    .expect("valid selector");
    let all = DatasetSelector::all();

    assert!(stored.covers(&same));
    assert!(!stored.covers(&different_canonical));
    assert!(!stored.covers(&different_fingerprint));
    assert!(!stored.covers(&different_kind));
    assert!(all.covers(&DatasetSelector::all()));
    assert!(!all.covers(&stored));
    assert!(!stored.covers(&all));
}

#[test]
fn test_fetch_request_response_and_capabilities_cover_query_cache_contract() {
    let chain = ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain");
    let adapter = EmptyAdapter {
        chain: chain.clone(),
    };
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.chain(), &chain);
    let logs = capabilities
        .dataset(&DatasetKey::evm_logs())
        .expect("logs capability");
    assert!(logs.supports_selector(SelectorKind::EvmLogs));
    assert_eq!(logs.max_range_len(), Some(2));
    assert_eq!(logs.max_topics_per_query(), Some(4));
    assert!(logs.supports_empty_coverage());
    assert!(logs.supports_safe_height());
    assert!(logs.supports_finalized_height());
    assert!(logs.supports_provider_native_finality_tags());
    assert!(logs.supports_range_split());

    let request = ChainFetchRequest::new(
        chain.clone(),
        DatasetKey::evm_logs(),
        LedgerRange::blocks(10, 11).expect("valid range"),
        DatasetSelector::try_evm_logs(LogFilter {
            addresses: Vec::new(),
            topics: Vec::new(),
        })
        .expect("valid selector"),
    )
    .with_limit(FetchLimit::max_rows(100))
    .with_context(FetchContext {
        request_id: Some("query-1".to_owned()),
        cache_write: true,
    });
    let response = adapter.fetch(request.clone()).expect("fetch response");

    assert_eq!(adapter.latest_height().unwrap(), ChainHeight::block(12));
    assert_eq!(
        adapter.cache_safe_height().unwrap(),
        ChainHeight::block(10).with_finality(FinalityKind::Safe)
    );
    response
        .validate_for_request(&request)
        .expect("response matches request");
    assert_eq!(response.dataset_key, DatasetKey::evm_logs());
    assert_eq!(
        response.range,
        LedgerRange::blocks(10, 11).expect("valid range")
    );
    assert_eq!(
        response.rows,
        DatasetRows::new(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new())).unwrap()
    );
    assert_eq!(response.coverage_selector.kind(), SelectorKind::EvmLogs);
    assert_eq!(response.source_metadata.provider, "mock");
    assert_eq!(response.provider_diagnostics.calls, 1);
}

#[test]
fn test_canonical_block_lookup_contract_exposes_reorg_signals() {
    let chain = ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain");
    let adapter = EmptyAdapter {
        chain: chain.clone(),
    };

    let block = adapter
        .canonical_block(CanonicalBlockRequest {
            chain: chain.clone(),
            range_kind: LedgerRangeKind::Block,
            height: 12,
        })
        .expect("canonical block");

    assert_eq!(block.chain, chain);
    assert_eq!(block.height, 12);
    assert_eq!(
        block.hash,
        "0x000000000000000000000000000000000000000000000000000000000000000c"
    );
    assert_eq!(
        block.parent_hash,
        "0x000000000000000000000000000000000000000000000000000000000000000b"
    );
    assert_eq!(block.finality, FinalityKind::Latest);
}

#[test]
fn test_canonical_block_lookup_defaults_to_unsupported() {
    let chain = ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain");
    let adapter = DefaultUnsupportedCanonicalAdapter {
        chain: chain.clone(),
    };

    let error = adapter
        .canonical_block(CanonicalBlockRequest {
            chain,
            range_kind: LedgerRangeKind::Block,
            height: 12,
        })
        .expect_err("canonical block lookup is unsupported by default");

    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
    assert!(error.message.contains("canonical block lookup"));
}

#[derive(Clone)]
struct DefaultUnsupportedCanonicalAdapter {
    chain: ChainIdentity,
}

impl ChainAdapter for DefaultUnsupportedCanonicalAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        AdapterCapabilities::new(self.chain.clone())
    }

    fn latest_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(12))
    }

    fn cache_safe_height(&self) -> Result<ChainHeight, DatalensError> {
        Ok(ChainHeight::block(10).with_finality(FinalityKind::Safe))
    }

    fn fetch(&self, request: ChainFetchRequest) -> Result<ChainFetchResponse, DatalensError> {
        ChainFetchResponse::try_empty(
            request.chain,
            request.dataset_key,
            request.range,
            request.selector,
        )
    }
}

#[test]
fn test_fetch_response_validate_for_request_rejects_contract_mismatch() {
    let chain = ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain");
    let selector = DatasetSelector::all();
    let request = ChainFetchRequest::new(
        chain.clone(),
        DatasetKey::evm_blocks(),
        LedgerRange::blocks(1, 2).expect("valid range"),
        selector.clone(),
    );
    let response = ChainFetchResponse::try_new(
        chain,
        DatasetKey::evm_logs(),
        LedgerRange::blocks(1, 2).expect("valid range"),
        selector,
        QueryRows::EvmLogs(Vec::new()),
    )
    .expect("valid response");

    let error = response
        .validate_for_request(&request)
        .expect_err("dataset mismatch rejected");

    assert_eq!(error.kind, DatalensErrorKind::Internal);
}

#[test]
fn test_fetch_response_validate_for_request_rejects_unconfirmed_empty_response() {
    let chain = ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain");
    let request = ChainFetchRequest::new(
        chain.clone(),
        DatasetKey::evm_logs(),
        LedgerRange::blocks(1, 2).expect("valid range"),
        DatasetSelector::try_evm_logs(LogFilter {
            addresses: Vec::new(),
            topics: Vec::new(),
        })
        .expect("valid selector"),
    );
    let response = ChainFetchResponse::try_empty(
        chain,
        DatasetKey::evm_logs(),
        LedgerRange::blocks(1, 2).expect("valid range"),
        request.selector.clone(),
    )
    .expect("valid response");

    let error = response
        .validate_for_request(&request)
        .expect_err("unconfirmed empty response rejected");

    assert_eq!(error.kind, DatalensErrorKind::Internal);
}

#[test]
fn test_durable_range_requires_safe_or_finalized_matching_height_kind() {
    let range = LedgerRange::blocks(1, 10).expect("valid range");
    assert!(
        validate_durable_range(
            &range,
            &ChainHeight::block(10).with_finality(FinalityKind::Safe),
        )
        .is_ok()
    );
    assert!(
        validate_durable_range(
            &range,
            &ChainHeight::block(10).with_finality(FinalityKind::Finalized),
        )
        .is_ok()
    );

    let latest_error =
        validate_durable_range(&range, &ChainHeight::block(10)).expect_err("latest is not durable");
    assert_eq!(latest_error.kind, DatalensErrorKind::InvalidInput);

    let too_high_error = validate_durable_range(
        &range,
        &ChainHeight::block(9).with_finality(FinalityKind::Safe),
    )
    .expect_err("range above safe height is rejected");
    assert_eq!(too_high_error.kind, DatalensErrorKind::InvalidInput);

    let other_height = ChainHeight {
        range_kind: LedgerRangeKind::Slot,
        value: 10,
        finality: FinalityLevel::Safe,
    };
    let kind_error =
        validate_durable_range(&range, &other_height).expect_err("kind mismatch rejected");
    assert_eq!(kind_error.kind, DatalensErrorKind::InvalidInput);
}

#[test]
fn test_other_finality_cannot_authorize_durable_cache_write() {
    let height = ChainHeight::block(10).with_finality(FinalityLevel::ChainSpecific("checkpoint"));

    assert!(!height.finality.is_durable_writable());
    assert!(height.validate_durable_writable().is_err());
}

#[test]
fn test_default_unsupported_finality_and_reorg_methods_are_stable() {
    let chain = ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain");
    let adapter = EmptyAdapter { chain };

    let finalized = adapter
        .finalized_height()
        .expect_err("finalized unsupported by default");
    assert_eq!(finalized.kind, DatalensErrorKind::UnsupportedDataset);

    let historical_signal = adapter
        .reorg_signal(HeightRangeKind::Block, 10)
        .expect_err("historical signal unsupported by default");
    assert_eq!(
        historical_signal.kind,
        DatalensErrorKind::UnsupportedDataset
    );

    let latest_signal = adapter
        .latest_reorg_signal()
        .expect_err("latest signal unsupported by default");
    assert_eq!(latest_signal.kind, DatalensErrorKind::UnsupportedDataset);
}

#[test]
fn test_other_selector_and_range_kinds_are_owned_stable_and_storage_safe() {
    let first = AdapterKey::try_new("solana-accounts").expect("valid key");
    let second = AdapterKey::try_new("solana-accounts").expect("valid key");

    assert_eq!(
        SelectorKind::Other(first.clone()),
        SelectorKind::Other(second.clone())
    );
    assert_eq!(
        HeightRangeKind::Other(first.as_str().to_owned()),
        HeightRangeKind::Other(second.as_str().to_owned())
    );
    assert_eq!(first.as_str(), "solana-accounts");
    assert!(AdapterKey::try_new("").is_err());
    assert!(AdapterKey::try_new("bad/key").is_err());
    assert!(AdapterKey::try_new("bad\\key").is_err());

    let selector = DatasetSelector::try_other(
        first.clone(),
        "accounts-fingerprint",
        "accounts/canonical-key",
    )
    .expect("valid selector");
    let range = HeightRange::try_new(HeightRangeKind::Other(first.as_str().to_owned()), 1, 2)
        .expect("valid range");

    assert_eq!(selector.kind(), SelectorKind::Other(second.clone()));
    assert_eq!(
        range.kind(),
        HeightRangeKind::Other(second.as_str().to_owned())
    );
    assert_eq!(selector.fingerprint(), "accounts-fingerprint");
    assert_eq!(selector.canonical_key(), "accounts/canonical-key");
    assert!(
        DatasetSelector::try_other(
            AdapterKey::try_new("bad-selector").expect("valid key"),
            "bad\\fingerprint",
            "canonical",
        )
        .is_err()
    );
    assert!(HeightRange::try_new(HeightRangeKind::Other("bad-range".to_owned()), 2, 1,).is_err());
}

#[test]
fn test_other_selector_rejects_dot_path_segments() {
    let kind = AdapterKey::try_new("other-selector").expect("valid key");

    for key in ["../x", "a/../b", ".", "a/./b"] {
        assert!(
            DatasetSelector::try_other(kind.clone(), key, "accounts/fingerprint").is_err(),
            "fingerprint {key:?} should be rejected"
        );
        assert!(
            DatasetSelector::try_other(kind.clone(), "accounts/fingerprint", key).is_err(),
            "canonical key {key:?} should be rejected"
        );
    }
}
