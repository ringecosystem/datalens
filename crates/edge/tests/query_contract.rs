mod support;

use support::query_contract::*;

#[test]
fn test_client_query_request_json_matches_api_request_contract() {
    let request = datalens_client::QueryRequest {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_logs(),
        selector: datalens_client::QuerySelector::EvmLogs(LogFilter {
            addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
            topics: vec![None, Some(vec![TOPIC_A.to_owned()])],
        }),
        range: LedgerRange::blocks(20, 21).expect("range"),
        finality: QueryFinalityRequirement::DurableOnly,
        fields: datalens_client::FieldSelection::All,
    };

    let api_request: QueryApiRequest =
        serde_json::from_value(serde_json::to_value(request).expect("client request json"))
            .expect("api request json");

    assert_eq!(api_request.chain, ethereum_identity());
    assert_eq!(api_request.dataset_key, "evm.logs");
    assert_eq!(
        api_request.range,
        QueryRangeApi::Block { start: 20, end: 21 }
    );
    assert!(matches!(api_request.selector, QuerySelectorApi::EvmLogs(_)));
    assert_eq!(api_request.finality, QueryFinalityRequirement::DurableOnly);
}

#[test]
fn test_client_query_response_json_decodes_api_response_contract() {
    let api_response = QueryApiResponse {
        chain: ethereum_identity(),
        dataset_key: "evm.blocks".to_owned(),
        range: QueryRangeApi::Block { start: 10, end: 10 },
        cache: datalens_edge::QueryCacheApi {
            hit_ranges: vec![QueryRangeApi::Block { start: 10, end: 10 }],
            missing_ranges: Vec::new(),
            durable_hit_ranges: vec![QueryRangeApi::Block { start: 10, end: 10 }],
            hot_hit_ranges: Vec::new(),
            provider_fill_ranges: Vec::new(),
            promotion_pending_ranges: Vec::new(),
            segments: vec![datalens_edge::QuerySegmentApi {
                range: QueryRangeApi::Block { start: 10, end: 10 },
                source: QuerySegmentSource::Durable,
                finality: QueryDataFinality::Safe,
            }],
        },
        rows: datalens_core::DatasetRows::new(
            DatasetKey::evm_blocks(),
            QueryRows::EvmBlocks(vec![block(10, "0x10")]),
        )
        .expect("rows"),
    };

    let client_response: datalens_client::QueryResponse =
        serde_json::from_value(serde_json::to_value(api_response).expect("api response json"))
            .expect("client response json");

    assert_eq!(
        client_response.cache.outcome(),
        datalens_client::CacheOutcome::FullHit
    );
    assert_eq!(
        query_row_block_numbers(client_response.rows.rows()),
        vec![10]
    );
}

#[test]
fn test_native_api_evm_blocks_request_maps_to_native_input() {
    let request = QueryApiRequest {
        chain: ethereum_identity(),
        dataset_key: "evm.blocks".to_owned(),
        selector: QuerySelectorApi::All,
        range: QueryRangeApi::Block { start: 10, end: 12 },
        finality: QueryFinalityRequirement::DurableOnly,
        fields: FieldSelectionApi::All,
    };

    let input = request.into_native_input().expect("native request maps");

    assert_eq!(input.chain, ethereum_identity());
    assert_eq!(input.dataset_key, DatasetKey::evm_blocks());
    assert_eq!(
        input.ledger_range,
        LedgerRange::blocks(10, 12).expect("range")
    );
    assert_eq!(input.selector, DatasetSelector::all());
    assert_eq!(input.field_selection, FieldSelection::All);
    assert_eq!(input.finality, QueryFinalityRequirement::DurableOnly);
}

#[test]
fn test_native_api_evm_logs_request_maps_to_native_input() {
    let request: QueryApiRequest = serde_json::from_value(serde_json::json!({
        "chain": ethereum_identity(),
        "dataset_key": "evm.logs",
        "selector": {
            "kind": "evm_logs",
            "value": {
                "addresses": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                "topics": [null, [TOPIC_A]]
            }
        },
        "range": { "kind": "block", "start": 20, "end": 21 },
        "finality": "safe_to_latest",
        "fields": "all"
    }))
    .expect("request json");

    let input = request.into_native_input().expect("native request maps");

    assert_eq!(input.dataset_key, DatasetKey::evm_logs());
    assert_eq!(
        input.ledger_range,
        LedgerRange::blocks(20, 21).expect("range")
    );
    assert!(matches!(input.selector, DatasetSelector::EvmLogs(_)));
    assert_eq!(input.finality, QueryFinalityRequirement::SafeToLatest);
}

#[test]
fn test_native_api_solana_slots_request_maps_to_native_input() {
    let request = QueryApiRequest {
        chain: ChainIdentity::try_new(
            ChainFamily::Other("solana".to_owned()),
            "mainnet",
            Some(NetworkId::textual("mainnet-beta").expect("network id")),
        )
        .expect("valid chain"),
        dataset_key: "solana.slots".to_owned(),
        selector: QuerySelectorApi::Other {
            kind: "solana_all".to_owned(),
            fingerprint: "solana-all/all".to_owned(),
            canonical_key: "all".to_owned(),
        },
        range: QueryRangeApi::Slot {
            start: 100,
            end: 102,
        },
        finality: QueryFinalityRequirement::DurableOnly,
        fields: FieldSelectionApi::All,
    };

    let input = request.into_native_input().expect("native request maps");

    assert_eq!(input.dataset_key, DatasetKey::solana_slots());
    assert_eq!(
        input.ledger_range,
        LedgerRange::slots(100, 102).expect("range")
    );
    assert!(matches!(input.selector, DatasetSelector::Other { .. }));
}

#[test]
fn test_native_api_tron_blocks_request_maps_to_native_input() {
    let request = QueryApiRequest {
        chain: ChainIdentity::try_new(
            ChainFamily::Other("tron".to_owned()),
            "mainnet",
            Some(NetworkId::numeric(728126428)),
        )
        .expect("valid chain"),
        dataset_key: "tron.blocks".to_owned(),
        selector: QuerySelectorApi::Other {
            kind: "tron_all".to_owned(),
            fingerprint: "tron-all/all".to_owned(),
            canonical_key: "all".to_owned(),
        },
        range: QueryRangeApi::Block { start: 30, end: 31 },
        finality: QueryFinalityRequirement::LatestOnly,
        fields: FieldSelectionApi::All,
    };

    let input = request.into_native_input().expect("native request maps");

    assert_eq!(input.dataset_key, DatasetKey::tron_blocks());
    assert_eq!(
        input.ledger_range,
        LedgerRange::blocks(30, 31).expect("range")
    );
    assert_eq!(input.finality, QueryFinalityRequirement::LatestOnly);
}

#[test]
fn test_native_api_request_rejects_invalid_dataset_key() {
    let request = QueryApiRequest {
        chain: ethereum_identity(),
        dataset_key: "logs".to_owned(),
        selector: QuerySelectorApi::All,
        range: QueryRangeApi::Block { start: 1, end: 1 },
        finality: QueryFinalityRequirement::DurableOnly,
        fields: FieldSelectionApi::All,
    };

    let error = request
        .into_native_input()
        .expect_err("invalid dataset key");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("dataset_key"));
}

#[test]
fn test_native_api_request_rejects_invalid_range_kind() {
    let error = serde_json::from_value::<QueryApiRequest>(serde_json::json!({
        "chain": ethereum_identity(),
        "dataset_key": "evm.blocks",
        "selector": { "kind": "all" },
        "range": { "kind": "epoch", "start": 1, "end": 1 },
        "finality": "durable_only",
        "fields": "all"
    }))
    .expect_err("invalid range kind");

    assert!(error.to_string().contains("unknown variant"));
}

#[test]
fn test_native_api_request_rejects_unsupported_selector() {
    let error = serde_json::from_value::<QueryApiRequest>(serde_json::json!({
        "chain": ethereum_identity(),
        "dataset_key": "evm.blocks",
        "selector": { "kind": "logs" },
        "range": { "kind": "block", "start": 1, "end": 1 },
        "finality": "durable_only",
        "fields": "all"
    }))
    .expect_err("unsupported selector kind");

    assert!(error.to_string().contains("unknown variant"));
}

#[test]
fn test_native_api_finality_values_map_without_allow_hot() {
    for (value, expected) in [
        ("durable_only", QueryFinalityRequirement::DurableOnly),
        ("safe_to_latest", QueryFinalityRequirement::SafeToLatest),
        ("latest_only", QueryFinalityRequirement::LatestOnly),
    ] {
        let request: QueryApiRequest = serde_json::from_value(serde_json::json!({
            "chain": ethereum_identity(),
            "dataset_key": "evm.blocks",
            "selector": { "kind": "all" },
            "range": { "kind": "block", "start": 1, "end": 1 },
            "finality": value,
            "fields": "all"
        }))
        .expect("request json");

        assert_eq!(
            request
                .into_native_input()
                .expect("native request maps")
                .finality,
            expected
        );
    }
}

#[test]
fn test_native_api_response_uses_ledger_ranges_and_dataset_rows() {
    let response = QueryApiResponse::from(NativeQueryResponse {
        chain: ethereum_identity(),
        dataset_key: DatasetKey::evm_blocks(),
        ledger_range: LedgerRange::blocks(10, 10).expect("range"),
        cache: datalens_edge::NativeCacheSummary {
            hit_ranges: vec![LedgerRange::blocks(10, 10).expect("range")],
            missing_ranges: Vec::new(),
            durable_hit_ranges: vec![LedgerRange::blocks(10, 10).expect("range")],
            hot_hit_ranges: Vec::new(),
            provider_fill_ranges: Vec::new(),
            promotion_pending_ranges: Vec::new(),
            segments: Vec::new(),
        },
        rows: datalens_core::DatasetRows::new(
            DatasetKey::evm_blocks(),
            QueryRows::EvmBlocks(vec![block(10, "0x10")]),
        )
        .expect("rows"),
    });

    let json = serde_json::to_value(response).expect("response json");

    assert_eq!(json["dataset_key"], "evm.blocks");
    assert_eq!(
        json["range"],
        serde_json::json!({ "kind": "block", "start": 10, "end": 10 })
    );
    assert_eq!(json["rows"]["dataset_key"]["name"], "blocks");
}

#[test]
fn test_query_service_supports_latest_only_read_through_without_durable_cache_write() {
    let root = temp_storage_root("hot-read-through");
    let source = MockSource::default().with_blocks(vec![block(100, "0x64")]);
    let service = service(LocalStorage::new(&root), source.clone());
    let mut request = blocks_request(100, 100);
    request.finality = QueryFinalityRequirement::LatestOnly;

    let response = service.query_native(request).expect("hot query succeeds");

    assert_eq!(
        response.cache.provider_fill_ranges,
        vec![LedgerRange::blocks(100, 100).expect("range")]
    );
    assert_eq!(
        response.cache.missing_ranges,
        vec![LedgerRange::blocks(100, 100).expect("range")]
    );
    assert_eq!(response.cache.durable_hit_ranges, Vec::<LedgerRange>::new());
    assert_eq!(response.cache.hot_hit_ranges, Vec::<LedgerRange>::new());
    assert_eq!(response.cache.segments.len(), 1);
    assert_eq!(
        response.cache.segments[0].source,
        QuerySegmentSource::Provider
    );
    assert_eq!(
        response.cache.segments[0].finality,
        QueryDataFinality::Latest
    );
    assert_eq!(query_row_block_numbers(response.rows.rows()), vec![100]);
    assert_eq!(
        source.calls(),
        vec![SourceCall::Blocks(BlockRange::expect_new(100, 100))]
    );
    assert!(!root.join("chains/evm/ethereum/1/manifest.json").exists());
    assert!(
        !root
            .join("chains/evm/ethereum/1/manifest-segments")
            .exists()
    );
    assert!(!root.join("chains/evm/ethereum/1/datasets").exists());
}

#[test]
fn test_api_error_mapping_uses_stable_response_codes() {
    let body = api_error_body(DatalensError::new(
        DatalensErrorKind::ProviderLimit,
        "mock provider limit",
    ));

    assert_eq!(
        api_error_status(&DatalensErrorKind::ProviderLimit),
        axum::http::StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(body.error.kind, "provider_limit");
    assert_eq!(body.error.message, "mock provider limit");

    let hot_body = api_error_body(DatalensError::new(
        DatalensErrorKind::UnsupportedHotQuery,
        "adapter cannot safely serve the requested hot/latest contract",
    ));
    assert_eq!(
        api_error_status(&DatalensErrorKind::UnsupportedHotQuery),
        axum::http::StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(hot_body.error.kind, "unsupported_hot_query");
}

#[test]
fn test_query_response_marks_provider_fill_source_and_finality() {
    let source = MockSource::default().with_blocks(vec![block(1, "0x01"), block(2, "0x02")]);
    let service = service(
        LocalStorage::new(temp_storage_root("response-provider-segment")),
        source,
    );

    let response = service
        .query_native(blocks_request(1, 2))
        .expect("query succeeds");

    assert_eq!(
        response.cache.provider_fill_ranges,
        vec![LedgerRange::blocks(1, 2).expect("range")]
    );
    assert_eq!(response.cache.hot_hit_ranges, Vec::<LedgerRange>::new());
    assert_eq!(response.cache.segments.len(), 1);
    assert_eq!(
        response.cache.segments[0].source,
        QuerySegmentSource::Provider
    );
    assert_eq!(response.cache.segments[0].finality, QueryDataFinality::Safe);
}

#[test]
fn test_query_rejects_fetch_response_chain_mismatch_without_cache_write() {
    assert_contract_violation_not_cached(
        "contract-chain",
        ResponseMutation::Chain(
            ChainIdentity::try_new(ChainFamily::Evm, "polygon", Some(NetworkId::numeric(137)))
                .expect("valid chain"),
        ),
    );
}

#[test]
fn test_query_rejects_fetch_response_dataset_mismatch_without_cache_write() {
    assert_contract_violation_not_cached(
        "contract-dataset",
        ResponseMutation::Dataset(DatasetKey::evm_logs()),
    );
}

#[test]
fn test_query_rejects_fetch_response_range_mismatch_without_cache_write() {
    assert_contract_violation_not_cached(
        "contract-range",
        ResponseMutation::Range(LedgerRange::from_block_range(BlockRange::expect_new(2, 3))),
    );
}

#[test]
fn test_query_rejects_fetch_response_selector_mismatch_without_cache_write() {
    assert_contract_violation_not_cached(
        "contract-selector",
        ResponseMutation::Selector(
            DatasetSelector::try_evm_logs(LogFilter {
                addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
                topics: Vec::new(),
            })
            .expect("valid selector"),
        ),
    );
}

#[test]
fn test_query_rejects_fetch_response_rows_mismatch_without_cache_write() {
    assert_contract_violation_not_cached(
        "contract-rows",
        ResponseMutation::Rows(QueryRows::EvmLogs(vec![log(
            1,
            0,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![TOPIC_A],
        )])),
    );
}

#[test]
fn test_query_uses_adapter_range_limit_when_smaller_than_config() {
    let source = MockSource::default()
        .with_blocks(vec![
            block(1, "0x01"),
            block(2, "0x02"),
            block(3, "0x03"),
            block(4, "0x04"),
        ])
        .with_blocks_max_range_len(1);
    let service = service(
        LocalStorage::new(temp_storage_root("adapter-range-limit")),
        source.clone(),
    );

    let response = service
        .query_native(blocks_request(1, 4))
        .expect("query succeeds");

    assert_eq!(block_numbers(&response), vec![1, 2, 3, 4]);
    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Blocks(BlockRange::expect_new(1, 1)),
            SourceCall::Blocks(BlockRange::expect_new(2, 2)),
            SourceCall::Blocks(BlockRange::expect_new(3, 3)),
            SourceCall::Blocks(BlockRange::expect_new(4, 4)),
        ]
    );
}

#[test]
fn test_query_uses_adapter_log_range_limit_when_smaller_than_config() {
    let source = MockSource::default().with_logs_max_range_len(1);
    let service = service(
        LocalStorage::new(temp_storage_root("adapter-log-range-limit")),
        source.clone(),
    );

    service
        .query_native(logs_request(
            1,
            3,
            vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        ))
        .expect("query succeeds");

    assert_eq!(
        source.calls(),
        vec![
            SourceCall::Logs(BlockRange::expect_new(1, 1)),
            SourceCall::Logs(BlockRange::expect_new(2, 2)),
            SourceCall::Logs(BlockRange::expect_new(3, 3)),
        ]
    );
}

#[test]
fn test_query_rejects_log_addresses_above_adapter_capability() {
    let source = MockSource::default().with_max_addresses_per_query(1);
    let service = service(
        LocalStorage::new(temp_storage_root("adapter-address-limit")),
        source,
    );

    let error = service
        .query_native(logs_request(
            1,
            1,
            vec![
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ],
        ))
        .expect_err("too many addresses for adapter");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
}

#[test]
fn test_query_delegates_log_address_limit_to_planner_capabilities() {
    let source = MockSource::default().with_max_addresses_per_query(2);
    let service = QueryService::new(
        LocalStorage::new(temp_storage_root("planner-address-limit")),
        source.clone(),
        PlannerConfig {
            max_query_range_blocks: 4,
            default_chunk_range_blocks: 2,
        },
        WriterConfig {
            target_object_bytes: 1024,
            min_object_rows: 1,
            record_empty_coverage: true,
            staging: Default::default(),
        },
        chain_config(1),
    );

    service
        .query_native(logs_request(
            1,
            1,
            vec![
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            ],
        ))
        .expect("planner capability allows two addresses");

    assert_eq!(
        source.calls(),
        vec![SourceCall::Logs(BlockRange::expect_new(1, 1))]
    );
}
