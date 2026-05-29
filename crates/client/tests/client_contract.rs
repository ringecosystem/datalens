use std::sync::{Arc, Mutex};

use datalens_client::{
    APPLICATION_IDENTITY_HEADER, AUTHORIZATION_HEADER, ApiErrorKind, CacheOutcome, ChainDiscovery,
    ClientError, DatalensClient, DatalensClientConfig, FallbackMode, HttpRequest, HttpResponse,
    HttpTransport, QueryOptions, QueryRequest, QueryResponse, QuerySelector, TronEventSelector,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, Dataset, DatasetKey, DatasetRows,
    LedgerRange, LogFilter, NetworkId, QueryDataFinality, QueryFinalityRequirement, QueryRows,
    QuerySegmentSource,
};

#[test]
fn test_query_serializes_native_request_and_application_header() {
    let transport = RecordingTransport::new(HttpResponse::json(
        200,
        serde_json::json!({
            "chain": ethereum_identity(),
            "dataset_key": "evm.blocks",
            "range": { "kind": "block", "start": 10, "end": 12 },
            "cache": { "hit_ranges": [], "missing_ranges": [{ "kind": "block", "start": 10, "end": 12 }] },
            "rows": dataset_rows_json(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new()))
        }),
    ));
    let client = client(transport.clone(), Some("wallet-search"));

    let response = client
        .query(
            QueryRequest::new(
                ethereum_identity(),
                DatasetKey::evm_blocks(),
                LedgerRange::blocks(10, 12).expect("range"),
            )
            .with_finality(QueryFinalityRequirement::DurableOnly)
            .with_fields(datalens_client::FieldSelection::All),
        )
        .expect("blocks query decodes");

    assert_eq!(response.cache.outcome(), CacheOutcome::Miss);
    assert_eq!(response.dataset_key, DatasetKey::evm_blocks());
    assert_eq!(
        response.range,
        LedgerRange::blocks(10, 12).expect("ledger range")
    );
    let request = transport.only_request();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/v1/query");
    assert_eq!(
        request.header(APPLICATION_IDENTITY_HEADER),
        Some("wallet-search")
    );
    assert_eq!(
        request.body,
        serde_json::json!({
            "chain": ethereum_identity(),
            "dataset_key": "evm.blocks",
            "selector": { "kind": "all" },
            "range": { "kind": "block", "start": 10, "end": 12 },
            "finality": "durable_only",
            "fields": "all"
        })
    );
}

#[test]
fn test_client_sends_bearer_token_when_configured() {
    let transport = RecordingTransport::new(HttpResponse::json(
        200,
        serde_json::json!({
            "chain": ethereum_identity(),
            "dataset_key": "evm.blocks",
            "range": { "kind": "block", "start": 10, "end": 10 },
            "cache": { "hit_ranges": [], "missing_ranges": [{ "kind": "block", "start": 10, "end": 10 }] },
            "rows": dataset_rows_json(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new()))
        }),
    ));
    let client = DatalensClient::with_transport(
        DatalensClientConfig {
            endpoint: "http://datalens.invalid".to_owned(),
            application: Some("wallet-search".to_owned()),
            bearer_token: Some(" secret-token ".to_owned()),
        },
        transport.clone(),
    )
    .expect("client config");

    client
        .query(QueryRequest::new(
            ethereum_identity(),
            DatasetKey::evm_blocks(),
            LedgerRange::blocks(10, 10).expect("range"),
        ))
        .expect("query response");

    let request = transport.only_request();
    assert_eq!(
        request.header(APPLICATION_IDENTITY_HEADER),
        Some("wallet-search")
    );
    assert_eq!(
        request.header(AUTHORIZATION_HEADER),
        Some("Bearer secret-token")
    );
}

#[test]
fn test_query_blocks_with_hot_options_serializes_explicit_hot_contract() {
    let transport = RecordingTransport::new(HttpResponse::json(
        200,
        serde_json::json!({
            "chain": ethereum_identity(),
            "dataset_key": "evm.blocks",
            "range": { "kind": "block", "start": 100, "end": 101 },
            "cache": {
                "hit_ranges": [],
                "missing_ranges": [{ "kind": "block", "start": 100, "end": 101 }],
                "durable_hit_ranges": [],
                "hot_hit_ranges": [],
                "provider_fill_ranges": [{ "kind": "block", "start": 100, "end": 101 }],
                "promotion_pending_ranges": [],
                "segments": [{
                    "range": { "kind": "block", "start": 100, "end": 101 },
                    "source": "provider",
                    "finality": "latest"
                }]
            },
            "rows": dataset_rows_json(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new()))
        }),
    ));
    let client = client(transport.clone(), Some("wallet-search"));

    let response = client
        .query_blocks_with_options(
            ethereum_identity(),
            BlockRange::expect_new(100, 101),
            QueryOptions {
                finality: QueryFinalityRequirement::SafeToLatest,
            },
        )
        .expect("hot query decodes");

    assert_eq!(
        response.cache.provider_fill_ranges,
        vec![LedgerRange::blocks(100, 101).expect("range")]
    );
    assert_eq!(
        response.cache.segments[0].source,
        QuerySegmentSource::Provider
    );
    assert_eq!(
        response.cache.segments[0].finality,
        QueryDataFinality::Latest
    );
    let request = transport.only_request();
    assert_eq!(
        request.body,
        serde_json::json!({
            "chain": ethereum_identity(),
            "dataset_key": "evm.blocks",
            "selector": { "kind": "all" },
            "range": { "kind": "block", "start": 100, "end": 101 },
            "finality": "safe_to_latest",
            "fields": "all"
        })
    );
}

#[test]
fn test_evm_query_helpers_reject_non_evm_chains_before_sending_request() {
    let transport = RecordingTransport::new(HttpResponse::json(
        200,
        serde_json::json!({ "unexpected": true }),
    ));
    let client = client(transport.clone(), Some("indexer"));

    let error = client
        .query_blocks(solana_identity(), BlockRange::expect_new(10, 12))
        .expect_err("non-EVM blocks helper is invalid");

    assert!(matches!(error, ClientError::InvalidInput(message) if message.contains("EVM")));
    assert!(transport.requests().is_empty());

    let error = client
        .query_logs(
            tron_identity(),
            BlockRange::expect_new(10, 12),
            LogFilter {
                addresses: Vec::new(),
                topics: Vec::new(),
            },
        )
        .expect_err("non-EVM logs helper is invalid");

    assert!(matches!(error, ClientError::InvalidInput(message) if message.contains("EVM")));
    assert!(transport.requests().is_empty());
}

#[test]
fn test_query_dataset_serializes_explicit_dataset_range_and_selector() {
    let transport = RecordingTransport::new(HttpResponse::json(
        200,
        serde_json::json!({
            "chain": tron_identity(),
            "dataset_key": "tron.events",
            "range": { "kind": "block", "start": 1000, "end": 1001 },
            "cache": {
                "hit_ranges": [],
                "missing_ranges": [{ "kind": "block", "start": 1000, "end": 1001 }]
            },
            "rows": dataset_rows_json(
                DatasetKey::tron_events(),
                QueryRows::AdapterJson {
                    dataset_key: DatasetKey::tron_events(),
                    rows: Vec::new(),
                }
            )
        }),
    ));
    let client = client(transport.clone(), Some("ormp"));

    client
        .query_dataset(
            tron_identity(),
            DatasetKey::tron_events(),
            LedgerRange::blocks(1000, 1001).expect("range"),
            QuerySelector::tron_contract_event(
                "0x0000000000000000000000000000000000000000",
                "Transfer",
            )
            .expect("selector"),
        )
        .expect("tron query decodes");

    assert_eq!(
        transport.only_request().body,
        serde_json::json!({
            "chain": tron_identity(),
            "dataset_key": "tron.events",
            "selector": {
                "kind": "other",
                "value": {
                    "kind": "tron_events",
                    "fingerprint": "tron-events/6fc306e8d91c62c5c6efcf12",
                    "canonical_key": "contracts/410000000000000000000000000000000000000000/events/Transfer"
                }
            },
            "range": { "kind": "block", "start": 1000, "end": 1001 },
            "finality": "durable_only",
            "fields": "all"
        })
    );
}

#[test]
fn test_solana_selector_helpers_match_adapter_contract() {
    assert_eq!(
        QuerySelector::solana_all(),
        QuerySelector::other("solana_all", "solana-all/all", "all")
    );
    assert_eq!(
        QuerySelector::solana_address(" 11111111111111111111111111111111 ").expect("address"),
        QuerySelector::other(
            "solana_address",
            "solana-address/8a83665f3798727f",
            "address/11111111111111111111111111111111"
        )
    );
    assert_eq!(
        QuerySelector::solana_program("So11111111111111111111111111111111111111112")
            .expect("program"),
        QuerySelector::other(
            "solana_program",
            "solana-program/d77cf5c8a1c79d6b",
            "program/So11111111111111111111111111111111111111112"
        )
    );
    assert_eq!(
        QuerySelector::solana_signature("5Nf6zKzExample1111111111111111111111111111111111")
            .expect("signature"),
        QuerySelector::other(
            "solana_signature",
            "solana-signature/47c263cfea835908",
            "signature/5Nf6zKzExample1111111111111111111111111111111111"
        )
    );
}

#[test]
fn test_tron_selector_helpers_match_adapter_contract() {
    assert_eq!(
        QuerySelector::tron_all(),
        QuerySelector::other("tron_all", "tron-all/all", "all")
    );
    assert_eq!(
        QuerySelector::tron_contract("0x0000000000000000000000000000000000000000")
            .expect("contract"),
        QuerySelector::other(
            "tron_events",
            "tron-events/501adaf4d004cec3d56317e1",
            "contracts/410000000000000000000000000000000000000000/events/all"
        )
    );
    assert_eq!(
        QuerySelector::tron_event(TronEventSelector {
            contract_addresses: vec![
                "0x0000000000000000000000000000000000000000".to_owned(),
                "410000000000000000000000000000000000000000".to_owned(),
            ],
            event_names: vec!["Transfer".to_owned(), "Transfer".to_owned()],
        })
        .expect("event selector"),
        QuerySelector::other(
            "tron_events",
            "tron-events/6fc306e8d91c62c5c6efcf12",
            "contracts/410000000000000000000000000000000000000000/events/Transfer"
        )
    );
}

#[test]
fn test_query_serializes_solana_slot_request_with_native_selector_and_fields() {
    let transport = RecordingTransport::new(HttpResponse::json(
        200,
        serde_json::json!({
            "chain": solana_identity(),
            "dataset_key": "solana.slots",
            "range": { "kind": "slot", "start": 500, "end": 501 },
            "cache": {
                "hit_ranges": [{ "kind": "slot", "start": 500, "end": 501 }],
                "missing_ranges": []
            },
            "rows": dataset_rows_json(
                DatasetKey::solana_slots(),
                QueryRows::AdapterJson {
                    dataset_key: DatasetKey::solana_slots(),
                    rows: vec![serde_json::json!({ "slot": 500, "block_hash": "hash-500" })],
                }
            )
        }),
    ));
    let client = client(transport.clone(), Some("indexer"));

    let response = client
        .query(
            QueryRequest::new(
                solana_identity(),
                DatasetKey::solana_slots(),
                LedgerRange::slots(500, 501).expect("range"),
            )
            .with_selector(QuerySelector::other(
                "solana_program",
                "program-11111111",
                "program=11111111111111111111111111111111",
            ))
            .with_fields(datalens_client::FieldSelection::Include(vec![
                "slot".to_owned(),
                "block_hash".to_owned(),
            ])),
        )
        .expect("solana query decodes");

    assert_eq!(response.dataset_key, DatasetKey::solana_slots());
    assert_eq!(
        response.range,
        LedgerRange::slots(500, 501).expect("ledger range")
    );
    assert_eq!(response.rows.dataset_key(), &DatasetKey::solana_slots());
    let request = transport.only_request();
    assert_eq!(
        request.body,
        serde_json::json!({
            "chain": solana_identity(),
            "dataset_key": "solana.slots",
            "selector": {
                "kind": "other",
                "value": {
                    "kind": "solana_program",
                    "fingerprint": "program-11111111",
                    "canonical_key": "program=11111111111111111111111111111111"
                }
            },
            "range": { "kind": "slot", "start": 500, "end": 501 },
            "finality": "durable_only",
            "fields": { "include": ["slot", "block_hash"] }
        })
    );
}

#[test]
fn test_query_decodes_tron_block_response_with_native_dataset_and_range() {
    let response: QueryResponse = serde_json::from_value(serde_json::json!({
        "chain": tron_identity(),
        "dataset_key": "tron.blocks",
        "range": { "kind": "block", "start": 1000, "end": 1000 },
        "cache": {
            "hit_ranges": [{ "kind": "block", "start": 1000, "end": 1000 }],
            "missing_ranges": [],
            "durable_hit_ranges": [{ "kind": "block", "start": 1000, "end": 1000 }],
            "hot_hit_ranges": [],
            "provider_fill_ranges": [],
            "promotion_pending_ranges": [],
            "segments": [{
                "range": { "kind": "block", "start": 1000, "end": 1000 },
                "source": "durable",
                "finality": "finalized"
            }]
        },
        "rows": dataset_rows_json(
            DatasetKey::tron_blocks(),
            QueryRows::AdapterJson {
                dataset_key: DatasetKey::tron_blocks(),
                rows: vec![serde_json::json!({ "number": 1000, "block_id": "000003e8" })],
            }
        )
    }))
    .expect("tron response json");

    assert_eq!(response.dataset_key, DatasetKey::tron_blocks());
    assert_eq!(
        response.range,
        LedgerRange::blocks(1000, 1000).expect("range")
    );
    assert_eq!(
        response.cache.durable_hit_ranges,
        vec![LedgerRange::blocks(1000, 1000).expect("range")]
    );
    assert_eq!(response.rows.dataset_key(), &DatasetKey::tron_blocks());
}

#[test]
fn test_query_logs_serializes_filter_topic_wildcards_and_empty_topic_sets() {
    let transport = RecordingTransport::new(HttpResponse::json(
        200,
        serde_json::json!({
            "chain": ethereum_identity(),
            "dataset_key": "evm.logs",
            "range": { "kind": "block", "start": 20, "end": 21 },
            "cache": {
                "hit_ranges": [{ "kind": "block", "start": 20, "end": 20 }],
                "missing_ranges": [{ "kind": "block", "start": 21, "end": 21 }],
                "durable_hit_ranges": [{ "kind": "block", "start": 20, "end": 20 }],
                "hot_hit_ranges": [],
                "provider_fill_ranges": [{ "kind": "block", "start": 21, "end": 21 }],
                "promotion_pending_ranges": [],
                "segments": [
                    {
                        "range": { "kind": "block", "start": 20, "end": 20 },
                        "source": "durable",
                        "finality": "safe"
                    },
                    {
                        "range": { "kind": "block", "start": 21, "end": 21 },
                        "source": "provider",
                        "finality": "safe"
                    }
                ]
            },
            "rows": dataset_rows_json(DatasetKey::evm_logs(), QueryRows::EvmLogs(Vec::new()))
        }),
    ));
    let client = client(transport.clone(), None);

    let response = client
        .query_logs(
            ethereum_identity(),
            BlockRange::expect_new(20, 21),
            LogFilter {
                addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
                topics: vec![None, Some(Vec::new()), Some(vec![topic_a()])],
            },
        )
        .expect("logs query decodes");

    assert_eq!(response.cache.outcome(), CacheOutcome::PartialHit);
    assert_eq!(
        response.cache.durable_hit_ranges,
        vec![LedgerRange::blocks(20, 20).expect("range")]
    );
    assert_eq!(
        response.cache.provider_fill_ranges,
        vec![LedgerRange::blocks(21, 21).expect("range")]
    );
    assert_eq!(
        response.cache.segments[0].source,
        QuerySegmentSource::Durable
    );
    assert_eq!(
        response.cache.segments[1].source,
        QuerySegmentSource::Provider
    );
    let request = transport.only_request();
    assert_eq!(request.header(APPLICATION_IDENTITY_HEADER), Some("unknown"));
    assert_eq!(
        request.body["selector"]["value"],
        serde_json::json!({
            "addresses": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
            "topics": [null, [], [topic_a()]]
        })
    );
}

#[test]
fn test_discovery_decodes_chain_identity_and_dataset_capabilities() {
    let transport = RecordingTransport::new(HttpResponse::json(
        200,
        serde_json::json!({
            "chains": [{
                "identity": ethereum_identity(),
                "datasets": ["blocks", "logs"]
            }]
        }),
    ));
    let client = client(transport, Some("indexer"));

    let discovery = client.discover().expect("discovery decodes");

    assert_eq!(
        discovery.chains,
        vec![ChainDiscovery {
            identity: ethereum_identity(),
            datasets: vec![Dataset::Blocks, Dataset::Logs],
        }]
    );
}

#[test]
fn test_response_rows_and_cache_summary_decode_with_outcome_helpers() {
    let response: QueryResponse = serde_json::from_value(serde_json::json!({
        "chain": ethereum_identity(),
            "dataset_key": "evm.blocks",
            "range": { "kind": "block", "start": 1, "end": 2 },
            "cache": {
                "hit_ranges": [{ "kind": "block", "start": 1, "end": 2 }],
                "missing_ranges": [],
                "durable_hit_ranges": [{ "kind": "block", "start": 1, "end": 2 }],
                "hot_hit_ranges": [],
                "provider_fill_ranges": [],
                "promotion_pending_ranges": [],
                "segments": [{
                    "range": { "kind": "block", "start": 1, "end": 2 },
                    "source": "durable",
                    "finality": "finalized"
                }]
            },
            "rows": dataset_rows_json(
                DatasetKey::evm_blocks(),
                QueryRows::EvmBlocks(vec![BlockHeader {
                    number: 1,
                    hash: "0x01".to_owned(),
                    parent_hash: "0x00".to_owned(),
                    timestamp: 100,
                }])
            )
    }))
    .expect("response json");

    assert_eq!(response.cache.outcome(), CacheOutcome::FullHit);
    assert_eq!(
        response.cache.segments[0].source,
        QuerySegmentSource::Durable
    );
    assert_eq!(
        response.cache.segments[0].finality,
        QueryDataFinality::Finalized
    );
    assert_eq!(response.rows.dataset_key(), &DatasetKey::evm_blocks());
    assert_eq!(
        response.rows.rows(),
        &QueryRows::EvmBlocks(vec![BlockHeader {
            number: 1,
            hash: "0x01".to_owned(),
            parent_hash: "0x00".to_owned(),
            timestamp: 100,
        }])
    );
}

#[test]
fn test_hot_response_source_and_finality_decode_without_rpc_fallback() {
    let response: QueryResponse = serde_json::from_value(serde_json::json!({
        "chain": ethereum_identity(),
        "dataset_key": "evm.blocks",
        "range": { "kind": "block", "start": 100, "end": 101 },
        "cache": {
            "hit_ranges": [],
            "missing_ranges": [],
            "durable_hit_ranges": [],
            "hot_hit_ranges": [{ "kind": "block", "start": 100, "end": 100 }],
            "provider_fill_ranges": [{ "kind": "block", "start": 101, "end": 101 }],
            "promotion_pending_ranges": [{ "kind": "block", "start": 100, "end": 101 }],
            "segments": [
                {
                    "range": { "kind": "block", "start": 100, "end": 100 },
                    "source": "hot",
                    "finality": "unsafe"
                },
                {
                    "range": { "kind": "block", "start": 101, "end": 101 },
                    "source": "provider",
                    "finality": "latest"
                }
            ]
        },
        "rows": dataset_rows_json(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new()))
    }))
    .expect("hot response json");

    assert_eq!(
        response.cache.hot_hit_ranges,
        vec![LedgerRange::blocks(100, 100).expect("range")]
    );
    assert_eq!(
        response.cache.provider_fill_ranges,
        vec![LedgerRange::blocks(101, 101).expect("range")]
    );
    assert_eq!(response.cache.segments[0].source, QuerySegmentSource::Hot);
    assert_eq!(
        response.cache.segments[0].finality,
        QueryDataFinality::Unsafe
    );
    assert_eq!(
        response.cache.segments[1].source,
        QuerySegmentSource::Provider
    );
    assert_eq!(
        response.cache.segments[1].finality,
        QueryDataFinality::Latest
    );
}

#[test]
fn test_error_response_maps_to_typed_client_error_without_matching_message_text() {
    let transport = RecordingTransport::new(HttpResponse::json(
        429,
        serde_json::json!({
            "error": {
                "kind": "provider_limit",
                "message": "provider-specific detail"
            }
        }),
    ));
    let client = client(transport, Some("indexer"));

    let error = client
        .query_blocks(ethereum_identity(), BlockRange::expect_new(1, 1))
        .expect_err("api error");

    assert_eq!(error.api_kind(), Some(ApiErrorKind::ProviderLimit));
    assert_eq!(error.status(), Some(429));
}

#[test]
fn test_rpc_fallback_boundary_is_explicitly_unsupported_and_cache_is_not_written() {
    let transport =
        RecordingTransport::new(HttpResponse::json(200, serde_json::json!({ "chains": [] })));
    let client = client(transport.clone(), Some("indexer"));

    let error = client
        .query_blocks_with_fallback(
            ethereum_identity(),
            BlockRange::expect_new(1, 1),
            FallbackMode::Rpc,
        )
        .expect_err("fallback is unsupported");

    assert!(error.is_unsupported_fallback());
    assert!(transport.requests().is_empty());
}

fn client(
    transport: RecordingTransport,
    application: Option<&str>,
) -> DatalensClient<RecordingTransport> {
    DatalensClient::with_transport(
        DatalensClientConfig {
            endpoint: "http://datalens.invalid".to_owned(),
            application: application.map(str::to_owned),
            bearer_token: None,
        },
        transport,
    )
    .expect("client config")
}

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain")
}

fn solana_identity() -> ChainIdentity {
    ChainIdentity::try_new(
        ChainFamily::try_other("solana").expect("family"),
        "mainnet",
        None,
    )
    .expect("valid chain")
}

fn tron_identity() -> ChainIdentity {
    ChainIdentity::try_new(
        ChainFamily::try_other("tron").expect("family"),
        "mainnet",
        None,
    )
    .expect("valid chain")
}

fn topic_a() -> String {
    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()
}

fn dataset_rows_json(dataset_key: DatasetKey, rows: QueryRows) -> serde_json::Value {
    serde_json::to_value(DatasetRows::new(dataset_key, rows).expect("dataset rows"))
        .expect("dataset rows json")
}

#[derive(Clone)]
struct RecordingTransport {
    response: HttpResponse,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
}

impl RecordingTransport {
    fn new(response: HttpResponse) -> Self {
        Self {
            response,
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn requests(&self) -> Vec<HttpRequest> {
        self.requests.lock().expect("requests lock").clone()
    }

    fn only_request(&self) -> HttpRequest {
        let requests = self.requests();
        assert_eq!(requests.len(), 1);
        requests[0].clone()
    }
}

impl HttpTransport for RecordingTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, datalens_client::ClientError> {
        self.requests.lock().expect("requests lock").push(request);
        Ok(self.response.clone())
    }
}
