use std::sync::{Arc, Mutex};

use datalens_client::{
    APPLICATION_IDENTITY_HEADER, ApiErrorKind, CacheOutcome, ChainDiscovery, DatalensClient,
    DatalensClientConfig, FallbackMode, HttpRequest, HttpResponse, HttpTransport, QueryOptions,
    QueryResponse,
};
use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, Dataset, LogFilter, NetworkId,
    QueryDataFinality, QueryFinalityRequirement, QueryRows, QuerySegmentSource,
};

#[test]
fn test_query_blocks_serializes_api_request_and_application_header() {
    let transport = RecordingTransport::new(HttpResponse::json(
        200,
        serde_json::json!({
            "chain": ethereum_identity(),
            "range": { "from_block": 10, "to_block": 12 },
            "cache": { "hit_ranges": [], "missing_ranges": [{ "from_block": 10, "to_block": 12 }] },
            "rows": { "dataset": "blocks", "rows": [] }
        }),
    ));
    let client = client(transport.clone(), Some("wallet-search"));

    let response = client
        .query_blocks(ethereum_identity(), BlockRange::expect_new(10, 12))
        .expect("blocks query decodes");

    assert_eq!(response.cache.outcome(), CacheOutcome::Miss);
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
            "dataset": "blocks",
            "range": { "from_block": 10, "to_block": 12 },
            "filter": null,
            "include_block": false,
            "allow_hot": false,
            "finality": "durable_only"
        })
    );
}

#[test]
fn test_query_blocks_with_hot_options_serializes_explicit_hot_contract() {
    let transport = RecordingTransport::new(HttpResponse::json(
        422,
        serde_json::json!({
            "error": {
                "kind": "unsupported_hot_query",
                "message": "hot query is not supported"
            }
        }),
    ));
    let client = client(transport.clone(), Some("wallet-search"));

    let error = client
        .query_blocks_with_options(
            ethereum_identity(),
            BlockRange::expect_new(100, 101),
            QueryOptions {
                allow_hot: true,
                finality: QueryFinalityRequirement::SafeToLatest,
            },
        )
        .expect_err("hot query is unsupported by this server");

    assert_eq!(error.api_kind(), Some(ApiErrorKind::UnsupportedHotQuery));
    let request = transport.only_request();
    assert_eq!(
        request.body,
        serde_json::json!({
            "chain": ethereum_identity(),
            "dataset": "blocks",
            "range": { "from_block": 100, "to_block": 101 },
            "filter": null,
            "include_block": false,
            "allow_hot": true,
            "finality": "safe_to_latest"
        })
    );
}

#[test]
fn test_query_logs_serializes_filter_topic_wildcards_and_empty_topic_sets() {
    let transport = RecordingTransport::new(HttpResponse::json(
        200,
        serde_json::json!({
            "chain": ethereum_identity(),
            "range": { "from_block": 20, "to_block": 21 },
            "cache": {
                "hit_ranges": [{ "from_block": 20, "to_block": 20 }],
                "missing_ranges": [{ "from_block": 21, "to_block": 21 }],
                "durable_hit_ranges": [{ "from_block": 20, "to_block": 20 }],
                "hot_hit_ranges": [],
                "provider_fill_ranges": [{ "from_block": 21, "to_block": 21 }],
                "promotion_pending_ranges": [],
                "segments": [
                    {
                        "range": { "from_block": 20, "to_block": 20 },
                        "source": "durable",
                        "finality": "safe"
                    },
                    {
                        "range": { "from_block": 21, "to_block": 21 },
                        "source": "provider",
                        "finality": "safe"
                    }
                ]
            },
            "rows": { "dataset": "logs", "rows": [] }
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
        vec![BlockRange::expect_new(20, 20)]
    );
    assert_eq!(
        response.cache.provider_fill_ranges,
        vec![BlockRange::expect_new(21, 21)]
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
        request.body["filter"],
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
            "range": { "from_block": 1, "to_block": 2 },
            "cache": {
                "hit_ranges": [{ "from_block": 1, "to_block": 2 }],
                "missing_ranges": [],
                "durable_hit_ranges": [{ "from_block": 1, "to_block": 2 }],
                "hot_hit_ranges": [],
                "provider_fill_ranges": [],
                "promotion_pending_ranges": [],
                "segments": [{
                    "range": { "from_block": 1, "to_block": 2 },
                    "source": "durable",
                    "finality": "finalized"
                }]
            },
            "rows": {
                "dataset": "blocks",
                "rows": [{
                "number": 1,
                "hash": "0x01",
                "parent_hash": "0x00",
                "timestamp": 100
            }]
        }
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
    assert_eq!(
        response.rows,
        QueryRows::EvmBlocks(vec![BlockHeader {
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
        "range": { "from_block": 100, "to_block": 101 },
        "cache": {
            "hit_ranges": [],
            "missing_ranges": [],
            "durable_hit_ranges": [],
            "hot_hit_ranges": [{ "from_block": 100, "to_block": 100 }],
            "provider_fill_ranges": [{ "from_block": 101, "to_block": 101 }],
            "promotion_pending_ranges": [{ "from_block": 100, "to_block": 101 }],
            "segments": [
                {
                    "range": { "from_block": 100, "to_block": 100 },
                    "source": "hot",
                    "finality": "unsafe"
                },
                {
                    "range": { "from_block": 101, "to_block": 101 },
                    "source": "provider",
                    "finality": "latest"
                }
            ]
        },
        "rows": { "dataset": "blocks", "rows": [] }
    }))
    .expect("hot response json");

    assert_eq!(
        response.cache.hot_hit_ranges,
        vec![BlockRange::expect_new(100, 100)]
    );
    assert_eq!(
        response.cache.provider_fill_ranges,
        vec![BlockRange::expect_new(101, 101)]
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
        },
        transport,
    )
    .expect("client config")
}

fn ethereum_identity() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain")
}

fn topic_a() -> String {
    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()
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
