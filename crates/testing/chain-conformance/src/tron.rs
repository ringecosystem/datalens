use datalens_chain::{
    AdapterKey, ChainAdapter, ChainFetchRequest, DatasetSelector, FetchContext, FinalityKind,
    HeightRangeKind, SelectorKind, validate_durable_range,
};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensErrorKind, DatasetKey, LedgerRange, NetworkId, QueryRows,
};

#[derive(Clone)]
pub struct TronFixtureProvider {
    chain: ChainIdentity,
}

impl TronFixtureProvider {
    pub fn tron() -> Self {
        Self {
            chain: ChainIdentity::try_new(
                ChainFamily::Other("tron".to_owned()),
                "tron-mainnet",
                Some(NetworkId::textual("mainnet").expect("valid network id")),
            )
            .expect("valid chain"),
        }
    }

    pub fn chain(&self) -> ChainIdentity {
        self.chain.clone()
    }

    pub fn all_selector(&self) -> DatasetSelector {
        DatasetSelector::try_other(
            AdapterKey::try_new("tron_all").expect("adapter key"),
            "tron-all/all",
            "all",
        )
        .expect("valid selector")
    }

    pub fn event_selector(&self) -> DatasetSelector {
        DatasetSelector::try_other(
            AdapterKey::try_new("tron_events").expect("adapter key"),
            "tron-events/conformance",
            "contracts/41abcdefabcdefabcdefabcdefabcdefabcdefabcd/events/Transfer",
        )
        .expect("valid selector")
    }
}

pub fn assert_tron_capability_conformance<A>(adapter: &A, expected_chain: ChainIdentity)
where
    A: ChainAdapter,
{
    let capabilities = adapter.capabilities();
    assert_eq!(capabilities.chain(), &expected_chain);
    assert!(capabilities.datasets().contains(&DatasetKey::tron_blocks()));
    assert!(
        capabilities
            .datasets()
            .contains(&DatasetKey::tron_transactions())
    );
    assert!(
        capabilities
            .datasets()
            .contains(&DatasetKey::tron_transaction_infos())
    );
    assert!(capabilities.datasets().contains(&DatasetKey::tron_events()));

    for dataset_key in [
        DatasetKey::tron_blocks(),
        DatasetKey::tron_transactions(),
        DatasetKey::tron_transaction_infos(),
        DatasetKey::tron_events(),
    ] {
        let capability = capabilities
            .dataset(&dataset_key)
            .expect("Tron dataset capability");
        assert!(capability.supports_selector(SelectorKind::Other(
            AdapterKey::try_new("tron_all").expect("adapter key")
        )));
        assert!(capability.ranges().contains(&HeightRangeKind::Block));
        assert_eq!(capability.max_range_len(), Some(64));
        assert!(capability.supports_finalized_height());
        assert!(!capability.supports_safe_height());
        assert!(capability.supports_empty_coverage());
        assert!(capability.supports_range_split());
        assert!(capability.supports_reorg_signals());
    }

    let events = capabilities
        .dataset(&DatasetKey::tron_events())
        .expect("Tron events capability");
    assert!(events.supports_selector(SelectorKind::Other(
        AdapterKey::try_new("tron_events").expect("adapter key")
    )));
    for dataset_key in [
        DatasetKey::tron_blocks(),
        DatasetKey::tron_transactions(),
        DatasetKey::tron_transaction_infos(),
    ] {
        assert!(
            !capabilities
                .dataset(&dataset_key)
                .expect("Tron dataset capability")
                .supports_selector(SelectorKind::Other(
                    AdapterKey::try_new("tron_events").expect("adapter key")
                ))
        );
    }
}

pub fn assert_tron_fetch_conformance<A>(adapter: &A, selector: DatasetSelector)
where
    A: ChainAdapter,
{
    let chain = adapter.capabilities().chain().clone();
    let blocks_request = ChainFetchRequest::new(
        chain.clone(),
        DatasetKey::tron_blocks(),
        LedgerRange::blocks(10, 12).expect("valid range"),
        selector.clone(),
    );
    let blocks = adapter.fetch(blocks_request.clone()).expect("blocks");
    blocks
        .validate_for_request(&blocks_request)
        .expect("response matches request");
    let QueryRows::AdapterJson { rows, .. } = blocks.rows.rows() else {
        panic!("expected Tron adapter JSON rows");
    };
    assert_eq!(
        rows.iter()
            .map(|row| row["number"].as_u64().expect("number"))
            .collect::<Vec<_>>(),
        [10, 11, 12]
    );
    assert!(rows.iter().all(|row| row["range_kind"] == "block"));
    assert!(rows.iter().all(|row| row["finality"] == "finalized"));
    assert!(rows.iter().all(|row| row["hash"].is_string()));
    assert!(rows.iter().all(|row| row["parent_hash"].is_string()));

    let events_request = ChainFetchRequest::new(
        chain.clone(),
        DatasetKey::tron_events(),
        LedgerRange::blocks(10, 10).expect("valid range"),
        selector.clone(),
    );
    let events = adapter.fetch(events_request.clone()).expect("events");
    events
        .validate_for_request(&events_request)
        .expect("events response matches request");
    assert_eq!(events.rows.row_count(), 1);

    let error = adapter
        .fetch(ChainFetchRequest::new(
            chain,
            DatasetKey::tron_blocks(),
            LedgerRange::slots(10, 10).expect("valid range"),
            selector,
        ))
        .expect_err("unsupported range");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
}

pub fn assert_tron_finality_conformance<A>(adapter: &A)
where
    A: ChainAdapter,
{
    let latest = adapter.latest_height().expect("latest block");
    let safe = adapter.cache_safe_height().expect("cache-safe block");
    let finalized = adapter.finalized_height().expect("finalized block");

    assert_eq!(latest.range_kind, HeightRangeKind::Block);
    assert_eq!(latest.value, 14);
    assert_eq!(latest.finality, FinalityKind::Latest);
    assert_eq!(safe.range_kind, HeightRangeKind::Block);
    assert_eq!(safe.value, 12);
    assert_eq!(safe.finality, FinalityKind::Finalized);
    assert_eq!(finalized, safe);
    validate_durable_range(&LedgerRange::blocks(10, safe.value).unwrap(), &safe)
        .expect("finalized block can authorize durable writes");
}

pub fn assert_tron_reorg_signal_conformance<A>(adapter: &A)
where
    A: ChainAdapter,
{
    let signal = adapter
        .reorg_signal(HeightRangeKind::Block, 12)
        .expect("block signal");
    assert_eq!(signal.range_kind, HeightRangeKind::Block);
    assert_eq!(signal.height, 12);
    assert_eq!(signal.hash, "000000000000000c-tron-hash");
    assert_eq!(signal.parent_hash, "000000000000000b-tron-hash");
    assert!(signal.timestamp.is_some());

    let latest = adapter.latest_reorg_signal().expect("latest signal");
    assert_eq!(latest.range_kind, HeightRangeKind::Block);
    assert_eq!(latest.height, 14);
    assert_eq!(latest.hash, "000000000000000e-tron-hash");

    let error = adapter
        .reorg_signal(HeightRangeKind::Slot, 12)
        .expect_err("unsupported slot signal");
    assert_eq!(error.kind, DatalensErrorKind::UnsupportedDataset);
}

pub fn assert_tron_metadata_conformance<A>(adapter: &A, selector: DatasetSelector)
where
    A: ChainAdapter,
{
    let chain = adapter.capabilities().chain().clone();
    let request = ChainFetchRequest::new(
        chain,
        DatasetKey::tron_blocks(),
        LedgerRange::blocks(10, 12).expect("valid range"),
        selector,
    )
    .with_context(FetchContext {
        request_id: Some("conformance-request".to_owned()),
        cache_write: true,
    });
    let response = adapter.fetch(request.clone()).expect("metadata response");

    response
        .validate_for_request(&request)
        .expect("response matches request");
    assert_eq!(response.source_metadata.provider, "tron-fixture");
    assert_eq!(
        response.source_metadata.request_id.as_deref(),
        Some("conformance-request")
    );
    assert!(response.provider_diagnostics.calls <= 3);
    assert_eq!(response.dataset_key, DatasetKey::tron_blocks());
    assert_eq!(
        response.coverage_selector.canonical_key(),
        request.selector.canonical_key()
    );
}
