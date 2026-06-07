use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use datalens_sdk::{
    ApiErrorKind, ClientConfig, DatalensClient, QuotaErrorKind, RetryConfig,
    native::{
        ChainFamilyInput, ChainFamilyKindInput, ChainHeadFinalityInput, ChainIdentityInput,
        DatasetKeyInput, EvmLogsSelectorInput, FieldSelectionInput, NetworkIdInput, QueryInput,
        QueryRangeInput, QueryRangeKindInput, QuerySelectorInput, SelectorKindInput,
    },
};
use serde_json::{Value, json};

#[test]
fn test_native_query_posts_rest_v1_query_by_default_with_auth_headers() {
    let server = MockRestServer::new(vec![json!({
        "chain": {"configuredName": "ethereum"},
        "dataset_key": "evm.logs",
        "range": {"kind": "block", "start": 1, "end": 2},
        "cache": {
            "hit_ranges": [{"kind": "block", "start": 1, "end": 2}],
            "missing_ranges": [],
            "durable_hit_ranges": [{"kind": "block", "start": 1, "end": 2}],
            "hot_hit_ranges": [],
            "provider_fill_ranges": [],
            "promotion_pending_ranges": [],
            "segments": []
        },
        "rows": {
            "dataset_key": "evm.logs",
            "rows": [{"blockNumber": 1}]
        }
    })]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: Some("secret-token".to_owned()),
        application: Some("query-app".to_owned()),
        timeout: Some(Duration::from_secs(5)),
        user_agent: Some("datalens-sdk-tests".to_owned()),
    })
    .expect("client config");

    let response = client.native().query(query_input()).expect("native query");

    assert_eq!(response.dataset_key, "evm.logs");
    assert_eq!(response.cache["hit_ranges"][0]["kind"], "block");
    assert_eq!(response.rows["rows"][0]["blockNumber"], 1);

    let request = server.only_request();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/v1/query");
    assert_eq!(
        request.headers.authorization.as_deref(),
        Some("Bearer secret-token")
    );
    assert_eq!(request.headers.application.as_deref(), Some("query-app"));
    assert_eq!(
        request.headers.user_agent.as_deref(),
        Some("datalens-sdk-tests")
    );
}

#[test]
fn test_native_query_rest_request_json_uses_tagged_shapes_and_fields() {
    let server = MockRestServer::new(vec![json!({
        "chain": {"configuredName": "ethereum"},
        "dataset_key": "evm.logs",
        "range": {"kind": "block", "start": 1, "end": 2},
        "cache": {
            "hit_ranges": [],
            "missing_ranges": [],
            "durable_hit_ranges": [],
            "hot_hit_ranges": [],
            "provider_fill_ranges": [],
            "promotion_pending_ranges": [],
            "segments": []
        },
        "rows": {"rows": []}
    })]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: None,
        user_agent: None,
    })
    .expect("client config");

    client.native().query(query_input()).expect("native query");

    let body = server.only_request().body;
    assert_eq!(
        body["chain"],
        json!({
            "family": "Evm",
            "configured_name": "ethereum",
            "network_id": {
                "kind": "numeric",
                "value": 1
            }
        })
    );
    assert!(body["chain"]["family"]["kind"].is_null());
    assert!(body["chain"]["configuredName"].is_null());
    assert!(body["chain"]["networkId"]["numeric"].is_null());
    assert_eq!(body["dataset_key"], "evm.logs");
    assert_eq!(body["selector"]["kind"], "evm_logs");
    assert_eq!(body["selector"]["value"]["addresses"][0], "0xaddr");
    assert_eq!(body["selector"]["value"]["topics"][0][0], "0xtopic0");
    assert_eq!(body["range"]["kind"], "block");
    assert_eq!(body["range"]["start"], 1);
    assert_eq!(body["range"]["end"], 2);
    assert_eq!(body["finality"], "durable_only");
    assert_eq!(body["fields"]["include"], json!(["block_number", "topics"]));
}

#[test]
fn test_native_query_rest_request_json_preserves_large_u64_range() {
    let server = MockRestServer::new(vec![query_response_body()]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: None,
        user_agent: None,
    })
    .expect("client config");
    let mut input = query_input();
    input.range.start = 2_147_483_648;
    input.range.end = 2_147_483_649;

    client.native().query(input).expect("native query");

    let body = server.only_request().body;
    assert_eq!(body["range"]["start"], json!(2_147_483_648_u64));
    assert_eq!(body["range"]["end"], json!(2_147_483_649_u64));
}

#[test]
fn test_native_query_without_finality_defaults_to_durable_boundary() {
    let server = MockRestServer::new(vec![
        json!({
            "chain": {"configured_name": "ethereum"},
            "height": 2,
            "finality": "finalized",
            "range_kind": "block"
        }),
        query_response_body(),
    ]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");
    let mut input = query_input();
    input.finality = None;

    let response = client.native().query(input).expect("native query");

    assert_eq!(response.dataset_key, "evm.logs");
    let requests = server.requests();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_eq!(
        requests[0].path,
        "/v1/chains/ethereum/head?finality=finalized"
    );
    assert_eq!(requests[1].method, "POST");
    assert_eq!(requests[1].path, "/v1/query");
    assert_eq!(requests[1].body["finality"], "durable_only");
}

#[test]
fn test_native_query_without_finality_accepts_safe_head_fallback() {
    let server = MockRestServer::new(vec![
        json!({
            "chain": {"configured_name": "ethereum"},
            "height": 2,
            "finality": "safe",
            "range_kind": "block"
        }),
        query_response_body(),
    ]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");
    let mut input = query_input();
    input.finality = None;

    client.native().query(input).expect("native query");

    assert_eq!(server.requests().len(), 2);
}

#[test]
fn test_native_query_without_finality_rejects_range_above_durable_boundary() {
    let server = MockRestServer::new(vec![json!({
        "chain": {"configured_name": "ethereum"},
        "height": 1,
        "finality": "finalized",
        "range_kind": "block"
    })]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");
    let mut input = query_input();
    input.finality = None;

    let error = client
        .native()
        .query(input)
        .expect_err("range should exceed durable boundary");

    assert!(matches!(error, datalens_sdk::Error::Safety(_)));
    assert!(
        error.to_string().contains("exceeds finalized head"),
        "{error}"
    );
    assert_eq!(server.requests().len(), 1);
}

#[test]
fn test_native_query_without_finality_falls_back_to_safe_head_when_finalized_unavailable() {
    let server = MockRestServer::with_responses(vec![
        MockRestResponse {
            status: 422,
            body: json!({
                "error": {
                    "kind": "unavailable_head",
                    "message": "finalized head is unavailable"
                }
            }),
        },
        MockRestResponse::ok(json!({
            "chain": {"configured_name": "ethereum"},
            "height": 2,
            "finality": "safe",
            "range_kind": "block"
        })),
        MockRestResponse::ok(query_response_body()),
    ]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");
    let mut input = query_input();
    input.finality = None;

    client.native().query(input).expect("native query");

    let requests = server.requests();
    assert_eq!(requests.len(), 3, "{requests:?}");
    assert_eq!(
        requests[0].path,
        "/v1/chains/ethereum/head?finality=finalized"
    );
    assert_eq!(requests[1].path, "/v1/chains/ethereum/head?finality=safe");
    assert_eq!(requests[2].path, "/v1/query");
}

#[test]
fn test_native_query_rejects_latest_only_finality_without_explicit_opt_in() {
    let server = MockRestServer::new(Vec::new());
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");
    let mut input = query_input();
    input.finality = Some("latest_only".to_owned());

    let error = client
        .native()
        .query(input)
        .expect_err("latest query should require explicit opt-in");

    assert!(matches!(error, datalens_sdk::Error::Safety(_)));
    assert!(
        error.to_string().contains("requires query_provisional"),
        "{error}"
    );
    assert_eq!(server.requests().len(), 0);
}

#[test]
fn test_native_query_rejects_safe_to_latest_finality_without_explicit_opt_in() {
    let server = MockRestServer::new(Vec::new());
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");
    let mut input = query_input();
    input.finality = Some("safe_to_latest".to_owned());

    let error = client
        .native()
        .query(input)
        .expect_err("safe-to-latest query should require explicit opt-in");

    assert!(matches!(error, datalens_sdk::Error::Safety(_)));
    assert!(
        error.to_string().contains("requires query_provisional"),
        "{error}"
    );
    assert_eq!(server.requests().len(), 0);
}

#[test]
fn test_native_query_provisional_sends_latest_only_finality() {
    let server = MockRestServer::new(vec![query_response_body_with_provider_segment("latest")]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");
    let mut input = query_input();
    input.finality = Some("latest_only".to_owned());

    let response = client
        .native()
        .query_provisional(input)
        .expect("provisional query");

    assert_eq!(response.cache["segments"][0]["finality"], json!("latest"));
    let request = server.only_request();
    assert_eq!(request.body["finality"], "latest_only");
}

#[test]
fn test_native_query_provisional_sends_safe_to_latest_finality() {
    let server = MockRestServer::new(vec![query_response_body_with_provider_segment("latest")]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");
    let mut input = query_input();
    input.finality = Some("safe_to_latest".to_owned());

    client
        .native()
        .query_provisional(input)
        .expect("provisional query");

    assert_eq!(server.only_request().body["finality"], "safe_to_latest");
}

#[test]
fn test_native_query_rejects_latest_segment_for_durable_query() {
    let server = MockRestServer::new(vec![query_response_body_with_provider_segment("latest")]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");

    let error = client
        .native()
        .query(query_input())
        .expect_err("durable query should reject latest segment");

    assert!(matches!(error, datalens_sdk::Error::Safety(_)));
    assert!(
        error
            .to_string()
            .contains("durable query returned non-durable segment"),
        "{error}"
    );
}

#[test]
fn test_native_query_accepts_provider_finalized_segment_for_durable_query() {
    let server = MockRestServer::new(vec![query_response_body_with_provider_segment("finalized")]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");

    let response = client.native().query(query_input()).expect("native query");

    assert_eq!(response.cache["segments"][0]["source"], json!("provider"));
    assert_eq!(
        response.cache["segments"][0]["finality"],
        json!("finalized")
    );
}

#[test]
fn test_native_query_accepts_provider_safe_segment_for_durable_query() {
    let server = MockRestServer::new(vec![query_response_body_with_provider_segment("safe")]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");

    let response = client.native().query(query_input()).expect("native query");

    assert_eq!(response.cache["segments"][0]["source"], json!("provider"));
    assert_eq!(response.cache["segments"][0]["finality"], json!("safe"));
}

#[test]
fn test_native_chain_head_gets_rest_head_with_finality_and_auth_headers() {
    let server = MockRestServer::new(vec![json!({
        "chain": {"configured_name": "ethereum"},
        "height": 18_500_123,
        "finality": "finalized",
        "range_kind": "block",
        "timestamp": 1_700_000_000_u64
    })]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: Some("secret-token".to_owned()),
        application: Some("query-app".to_owned()),
        timeout: Some(Duration::from_secs(5)),
        user_agent: Some("datalens-sdk-tests".to_owned()),
    })
    .expect("client config");

    let response = client
        .native()
        .chain_head("ethereum", Some(ChainHeadFinalityInput::Finalized))
        .expect("chain head");

    assert_eq!(response.chain["configured_name"], "ethereum");
    assert_eq!(response.height, 18_500_123);
    assert_eq!(response.finality, "finalized");
    assert_eq!(response.range_kind, "block");
    assert_eq!(response.timestamp, Some(1_700_000_000));

    let request = server.only_request();
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, "/v1/chains/ethereum/head?finality=finalized");
    assert_eq!(
        request.headers.authorization.as_deref(),
        Some("Bearer secret-token")
    );
    assert_eq!(request.headers.application.as_deref(), Some("query-app"));
    assert_eq!(
        request.headers.user_agent.as_deref(),
        Some("datalens-sdk-tests")
    );
    assert!(request.body.is_null());
}

#[test]
fn test_native_chain_head_helpers_use_typed_finality() {
    let server = MockRestServer::new(vec![
        json!({
            "chain": {"configured_name": "ethereum"},
            "height": 18_500_001,
            "finality": "latest",
            "range_kind": "block"
        }),
        json!({
            "chain": {"configured_name": "ethereum"},
            "height": 18_499_900,
            "finality": "safe",
            "range_kind": "block"
        }),
        json!({
            "chain": {"configured_name": "ethereum"},
            "height": 18_499_800,
            "finality": "finalized",
            "range_kind": "block"
        }),
    ]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");

    assert_eq!(
        client.native().latest_head("ethereum").unwrap().finality,
        "latest"
    );
    assert_eq!(
        client.native().safe_head("ethereum").unwrap().finality,
        "safe"
    );
    assert_eq!(
        client.native().finalized_head("ethereum").unwrap().finality,
        "finalized"
    );

    let requests = server.requests();
    assert_eq!(requests[0].path, "/v1/chains/ethereum/head?finality=latest");
    assert_eq!(requests[1].path, "/v1/chains/ethereum/head?finality=safe");
    assert_eq!(
        requests[2].path,
        "/v1/chains/ethereum/head?finality=finalized"
    );
}

#[test]
fn test_native_chain_head_supports_numeric_chain_locator_without_finality_query() {
    let server = MockRestServer::new(vec![json!({
        "chain": {"configured_name": "ethereum"},
        "height": 18_500_456,
        "finality": "latest",
        "range_kind": "block"
    })]);
    let client = DatalensClient::new(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");

    let response = client.native().chain_head("1", None).expect("chain head");

    assert_eq!(response.height, 18_500_456);
    assert_eq!(response.timestamp, None);
    assert_eq!(server.only_request().path, "/v1/chains/1/head");
}

#[test]
fn test_native_chain_head_rejects_graphql_endpoint_clients() {
    let client = DatalensClient::with_graphql_endpoint(ClientConfig {
        endpoint: "http://127.0.0.1:1/native/graphql".to_owned(),
        bearer_token: None,
        application: None,
        timeout: Some(Duration::from_secs(5)),
        user_agent: None,
    })
    .expect("client config");

    let error = client
        .native()
        .chain_head("ethereum", Some(ChainHeadFinalityInput::Safe))
        .expect_err("chain head should require REST");

    assert!(
        error
            .to_string()
            .contains("requires a REST datalens endpoint"),
        "{error}"
    );
}

#[test]
fn test_native_query_retries_request_rate_limit_and_preserves_headers() {
    let server = MockRestServer::with_responses(vec![
        MockRestResponse::too_many_requests(request_rate_limit_body(Some(0))),
        MockRestResponse::too_many_requests(request_rate_limit_body(Some(0))),
        MockRestResponse::ok(query_response_body()),
    ]);
    let client = DatalensClient::new_with_retry_config(
        ClientConfig {
            endpoint: server.endpoint(),
            bearer_token: Some("secret-token".to_owned()),
            application: Some("query-app".to_owned()),
            timeout: Some(Duration::from_secs(5)),
            user_agent: Some("datalens-sdk-tests".to_owned()),
        },
        test_retry_config(3),
    )
    .expect("client config");

    let response = client.native().query(query_input()).expect("native query");

    assert_eq!(response.dataset_key, "evm.logs");
    assert_eq!(response.rows["rows"][0]["blockNumber"], 1);
    let requests = server.requests();
    assert_eq!(requests.len(), 3, "{requests:?}");
    for request in requests {
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/v1/query");
        assert_eq!(
            request.headers.authorization.as_deref(),
            Some("Bearer secret-token")
        );
        assert_eq!(request.headers.application.as_deref(), Some("query-app"));
        assert_eq!(
            request.headers.user_agent.as_deref(),
            Some("datalens-sdk-tests")
        );
    }
}

#[test]
fn test_native_query_range_limit_is_typed_and_not_retried() {
    let server = MockRestServer::with_responses(vec![MockRestResponse::too_many_requests(
        range_limit_body(),
    )]);
    let client = DatalensClient::new_with_retry_config(
        ClientConfig {
            endpoint: server.endpoint(),
            bearer_token: None,
            application: None,
            timeout: Some(Duration::from_secs(5)),
            user_agent: None,
        },
        test_retry_config(3),
    )
    .expect("client config");

    let error = client
        .native()
        .query(query_input())
        .expect_err("range limit should fail");

    let api_error = error.api_error().expect("typed api error");
    assert_eq!(api_error.kind, ApiErrorKind::RateLimited);
    let quota = api_error.quota.expect("quota metadata");
    assert_eq!(quota.kind, QuotaErrorKind::RangeLimit);
    assert_eq!(quota.retry_after_seconds, None);
    assert!(!error.is_retryable());
    assert_eq!(server.requests().len(), 1);
}

#[test]
fn test_native_query_stable_non_retryable_error_is_typed() {
    let server = MockRestServer::with_responses(vec![MockRestResponse {
        status: 422,
        body: json!({
            "error": {
                "kind": "unsupported_dataset",
                "message": "unsupported dataset evm.receipts"
            }
        }),
    }]);
    let client = DatalensClient::new_with_retry_config(
        ClientConfig {
            endpoint: server.endpoint(),
            bearer_token: None,
            application: None,
            timeout: Some(Duration::from_secs(5)),
            user_agent: None,
        },
        test_retry_config(3),
    )
    .expect("client config");

    let error = client
        .native()
        .query(query_input())
        .expect_err("unsupported dataset should fail");

    let api_error = error.api_error().expect("typed api error");
    assert_eq!(api_error.kind, ApiErrorKind::UnsupportedDataset);
    assert_eq!(api_error.message, "unsupported dataset evm.receipts");
    assert!(api_error.quota.is_none());
    assert!(!error.is_retryable());
    assert_eq!(server.requests().len(), 1);
}

#[test]
fn test_native_query_respects_retry_max_attempts() {
    let server = MockRestServer::with_responses(vec![
        MockRestResponse::too_many_requests(request_rate_limit_body(None)),
        MockRestResponse::too_many_requests(request_rate_limit_body(None)),
    ]);
    let client = DatalensClient::new_with_retry_config(
        ClientConfig {
            endpoint: server.endpoint(),
            bearer_token: None,
            application: None,
            timeout: Some(Duration::from_secs(5)),
            user_agent: None,
        },
        test_retry_config(2),
    )
    .expect("client config");

    let error = client
        .native()
        .query(query_input())
        .expect_err("rate limit should exhaust retries");

    assert!(error.is_retryable());
    assert_eq!(error.retry_after_seconds(), None);
    assert_eq!(server.requests().len(), 2);
}

#[test]
fn test_native_chain_head_retries_request_rate_limit() {
    let server = MockRestServer::with_responses(vec![
        MockRestResponse::too_many_requests(request_rate_limit_body(Some(0))),
        MockRestResponse::ok(json!({
            "chain": {"configured_name": "ethereum"},
            "height": 18_500_789,
            "finality": "safe",
            "range_kind": "block"
        })),
    ]);
    let client = DatalensClient::new_with_retry_config(
        ClientConfig {
            endpoint: server.endpoint(),
            bearer_token: None,
            application: Some("head-app".to_owned()),
            timeout: Some(Duration::from_secs(5)),
            user_agent: None,
        },
        test_retry_config(2),
    )
    .expect("client config");

    let response = client
        .native()
        .chain_head("ethereum", Some(ChainHeadFinalityInput::Safe))
        .expect("chain head");

    assert_eq!(response.height, 18_500_789);
    let requests = server.requests();
    assert_eq!(requests.len(), 2, "{requests:?}");
    assert_eq!(requests[0].path, "/v1/chains/ethereum/head?finality=safe");
    assert_eq!(requests[1].headers.application.as_deref(), Some("head-app"));
}

#[test]
fn test_retry_config_delay_prefers_service_retry_after() {
    let retry = RetryConfig {
        max_attempts: 4,
        initial_delay: Duration::from_millis(2),
        max_delay: Duration::from_millis(5),
        max_elapsed: Some(Duration::from_millis(100)),
        jitter: false,
        jitter_factor: 0.0,
    };

    assert_eq!(
        retry.delay_for_attempt(1, None),
        Some(Duration::from_millis(2))
    );
    assert_eq!(
        retry.delay_for_attempt(2, None),
        Some(Duration::from_millis(4))
    );
    assert_eq!(
        retry.delay_for_attempt(3, None),
        Some(Duration::from_millis(5))
    );
    assert_eq!(
        retry.delay_for_attempt(3, Some(Duration::from_secs(7))),
        Some(Duration::from_secs(7))
    );
}

#[test]
fn test_retry_config_jitter_delay_respects_max_delay() {
    let retry = RetryConfig {
        max_attempts: 4,
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(10),
        max_elapsed: Some(Duration::from_millis(100)),
        jitter: true,
        jitter_factor: 1.0,
    };

    for _ in 0..10_000 {
        let delay = retry
            .delay_for_attempt(1, None)
            .expect("retry delay should be configured");
        assert!(
            delay <= retry.max_delay,
            "jittered delay {delay:?} exceeded max_delay {:?}",
            retry.max_delay
        );
    }
}

fn query_input() -> QueryInput {
    QueryInput {
        chain: ChainIdentityInput {
            family: ChainFamilyInput {
                kind: ChainFamilyKindInput::Evm,
                other: None,
            },
            configured_name: "ethereum".to_owned(),
            network_id: Some(NetworkIdInput {
                numeric: Some(1),
                textual: None,
            }),
        },
        dataset_key: DatasetKeyInput {
            family: "evm".to_owned(),
            name: "logs".to_owned(),
        },
        selector: QuerySelectorInput {
            kind: SelectorKindInput::EvmLogs,
            evm_logs: Some(EvmLogsSelectorInput {
                addresses: vec!["0xaddr".to_owned()],
                topics: vec![vec!["0xtopic0".to_owned()]],
            }),
            other: None,
        },
        range: QueryRangeInput {
            kind: QueryRangeKindInput::Block,
            start: 1,
            end: 2,
        },
        finality: Some("durable_only".to_owned()),
        fields: Some(FieldSelectionInput {
            include: vec!["block_number".to_owned(), "topics".to_owned()],
        }),
    }
}

fn query_response_body() -> Value {
    json!({
        "chain": {"configuredName": "ethereum"},
        "dataset_key": "evm.logs",
        "range": {"kind": "block", "start": 1, "end": 2},
        "cache": {
            "hit_ranges": [{"kind": "block", "start": 1, "end": 2}],
            "missing_ranges": [],
            "durable_hit_ranges": [{"kind": "block", "start": 1, "end": 2}],
            "hot_hit_ranges": [],
            "provider_fill_ranges": [],
            "promotion_pending_ranges": [],
            "segments": []
        },
        "rows": {
            "dataset_key": "evm.logs",
            "rows": [{"blockNumber": 1}]
        }
    })
}

fn query_response_body_with_provider_segment(finality: &str) -> Value {
    json!({
        "chain": {"configuredName": "ethereum"},
        "dataset_key": "evm.logs",
        "range": {"kind": "block", "start": 1, "end": 2},
        "cache": {
            "hit_ranges": [],
            "missing_ranges": [{"kind": "block", "start": 1, "end": 2}],
            "durable_hit_ranges": [],
            "hot_hit_ranges": [],
            "provider_fill_ranges": [{"kind": "block", "start": 1, "end": 2}],
            "promotion_pending_ranges": [],
            "segments": [{
                "range": {"kind": "block", "start": 1, "end": 2},
                "source": "provider",
                "finality": finality
            }]
        },
        "rows": {"rows": []}
    })
}

fn request_rate_limit_body(retry_after_seconds: Option<u64>) -> Value {
    json!({
        "error": {
            "kind": "rate_limited",
            "message": "application request rate quota exceeded",
            "quota": {
                "kind": "request_rate_limit",
                "scope": "application",
                "limit": 1,
                "requested": null,
                "observed": 1,
                "retry_after_seconds": retry_after_seconds
            }
        }
    })
}

fn range_limit_body() -> Value {
    json!({
        "error": {
            "kind": "rate_limited",
            "message": "application query range quota exceeded",
            "quota": {
                "kind": "range_limit",
                "scope": "application",
                "limit": 1,
                "requested": 2,
                "observed": null,
                "retry_after_seconds": null
            }
        }
    })
}

fn test_retry_config(max_attempts: u32) -> RetryConfig {
    RetryConfig {
        max_attempts,
        initial_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(1),
        max_elapsed: Some(Duration::from_millis(100)),
        jitter: false,
        jitter_factor: 0.0,
    }
}

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: String,
    path: String,
    headers: RecordedHeaders,
    body: Value,
}

#[derive(Clone, Debug, Default)]
struct RecordedHeaders {
    authorization: Option<String>,
    application: Option<String>,
    user_agent: Option<String>,
}

#[derive(Clone, Debug)]
struct MockRestResponse {
    status: u16,
    body: Value,
}

impl MockRestResponse {
    fn ok(body: Value) -> Self {
        Self { status: 200, body }
    }

    fn too_many_requests(body: Value) -> Self {
        Self { status: 429, body }
    }
}

struct MockRestServer {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MockRestServer {
    fn new(responses: Vec<Value>) -> Self {
        Self::with_responses(responses.into_iter().map(MockRestResponse::ok).collect())
    }

    fn with_responses(responses: Vec<MockRestResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let address = listener.local_addr().expect("mock address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_requests = Arc::clone(&requests);
        let handle = thread::spawn(move || {
            for response in responses {
                let (stream, _) = listener.accept().expect("accept request");
                handle_connection(stream, response, &server_requests);
            }
        });
        Self {
            address,
            requests,
            handle: Some(handle),
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    fn only_request(&self) -> RecordedRequest {
        let requests = self.requests.lock().expect("requests");
        assert_eq!(requests.len(), 1, "{requests:?}");
        requests[0].clone()
    }

    fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().expect("requests").clone()
    }
}

impl Drop for MockRestServer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.join().expect("mock server thread");
        }
    }
}

fn handle_connection(
    mut stream: TcpStream,
    response: MockRestResponse,
    requests: &Arc<Mutex<Vec<RecordedRequest>>>,
) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
    let mut content_length = 0usize;
    let mut headers = RecordedHeaders::default();
    let mut line = String::new();
    reader.read_line(&mut line).expect("request line");
    let mut request_parts = line.split_whitespace();
    let method = request_parts.next().expect("method").to_owned();
    let path = request_parts.next().expect("path").to_owned();
    loop {
        line.clear();
        reader.read_line(&mut line).expect("header line");
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            break;
        }
        let lower = trimmed.to_ascii_lowercase();
        if let Some(value) = lower.strip_prefix("content-length: ") {
            content_length = value.parse().expect("content length");
        } else if lower.starts_with("authorization: ") {
            headers.authorization = Some(trimmed["authorization: ".len()..].to_owned());
        } else if lower.starts_with("x-datalens-application: ") {
            headers.application = Some(trimmed["x-datalens-application: ".len()..].to_owned());
        } else if lower.starts_with("user-agent: ") {
            headers.user_agent = Some(trimmed["user-agent: ".len()..].to_owned());
        }
    }
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body).expect("request body");
    let body: Value = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&body).expect("rest body")
    };
    requests.lock().expect("requests").push(RecordedRequest {
        method,
        path,
        headers,
        body,
    });

    let response_body = serde_json::to_vec(&response.body).expect("response json");
    let status = response.status;
    write!(
        stream,
        "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        response_body.len()
    )
    .expect("response headers");
    stream.write_all(&response_body).expect("response body");
}
