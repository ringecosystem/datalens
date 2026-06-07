use std::{
    io::ErrorKind,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use datalens_sdk::{
    ApiErrorKind, ClientConfig, DatalensClient, Error, QuotaErrorKind, RetryConfig,
    index::{EventFilter, PageRequest},
    native::{
        ChainFamilyInput, ChainFamilyKindInput, ChainIdentityInput, DatasetKeyInput,
        EvmLogsSelectorInput, NetworkIdInput, QueryInput, QueryRangeInput, QueryRangeKindInput,
        QuerySelectorInput, SelectorKindInput,
    },
};
use serde_json::{Value, json};

#[test]
fn test_index_raw_events_sends_bearer_token_and_decodes_nodes() {
    let server = MockGraphqlServer::new(vec![json!({
        "data": {
            "events": [{
                "indexName": "ormp",
                "chain": "ethereum",
                "chainId": 1,
                "dataset": "evm.logs",
                "blockNumber": 10,
                "blockHash": "0xblock",
                "transactionHash": "0xtx",
                "transactionIndex": 1,
                "eventIndex": 2,
                "address": "0xaddr",
                "selector": "Transfer",
                "topics": ["0xtopic0"],
                "topic0": "0xtopic0",
                "signature": "Transfer(address,address,uint256)",
                "eventName": "Transfer",
                "decoded": {"from": "0xfrom"},
                "data": "0xdata",
                "payload": {"raw": true},
                "createdAt": "2026-05-31T00:00:00Z"
            }]
        }
    })]);
    let client = client(&server, Some("secret-token"));

    let events = client
        .index()
        .raw_events(
            EventFilter::new("evm.logs").with_index_name("ormp"),
            Some(25),
            None,
        )
        .expect("raw events");

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].index_name.as_deref(), Some("ormp"));
    assert_eq!(events[0].decoded["from"], "0xfrom");

    let request = server.only_request();
    assert_eq!(
        request.headers.authorization.as_deref(),
        Some("Bearer secret-token")
    );
    assert_eq!(
        request.headers.user_agent.as_deref(),
        Some("datalens-sdk-tests")
    );
    assert_eq!(request.headers.application.as_deref(), Some("query-app"));
    assert!(request.query.contains("events("), "{}", request.query);
    assert_eq!(request.variables["dataset"], "evm.logs");
    assert_eq!(request.variables["indexName"], "ormp");
    assert_eq!(request.variables["limit"], 25);
}

#[test]
fn test_index_decoded_events_connection_sends_cursor_and_decodes_page_info() {
    let server = MockGraphqlServer::new(vec![json!({
        "data": {
            "decodedEventsConnection": {
                "edges": [{
                    "cursor": "cursor-2",
                    "node": {
                        "indexName": "ormp",
                        "chain": "ethereum",
                        "chainId": 1,
                        "dataset": "evm.logs",
                        "blockNumber": 11,
                        "blockHash": "0xblock",
                        "transactionHash": "0xtx",
                        "transactionIndex": 0,
                        "logIndex": 7,
                        "address": "0xaddr",
                        "eventName": "MessageAccepted",
                        "signature": "MessageAccepted(bytes32)",
                        "topic0": "0xtopic0",
                        "decodedArgs": {"messageHash": "0xhash"},
                        "decodeStatus": "decoded",
                        "decodeError": null,
                        "payload": {"decoded": true},
                        "createdAt": "2026-05-31T00:00:00Z"
                    }
                }],
                "nodes": [],
                "pageInfo": {
                    "endCursor": "cursor-2",
                    "hasNextPage": true
                }
            }
        }
    })]);
    let client = client(&server, None);

    let connection = client
        .index()
        .decoded_events_connection(
            EventFilter::new("evm.logs").with_event_name("MessageAccepted"),
            PageRequest::first(2).after("cursor-1"),
        )
        .expect("decoded connection");

    assert_eq!(connection.edges.len(), 1);
    assert_eq!(connection.edges[0].cursor, "cursor-2");
    assert_eq!(
        connection.edges[0].node.decoded_args["messageHash"],
        "0xhash"
    );
    assert_eq!(connection.page_info.end_cursor.as_deref(), Some("cursor-2"));
    assert!(connection.page_info.has_next_page);

    let request = server.only_request();
    assert!(request.query.contains("decodedEventsConnection("));
    assert_eq!(request.variables["eventName"], "MessageAccepted");
    assert_eq!(request.variables["first"], 2);
    assert_eq!(request.variables["after"], "cursor-1");
}

#[test]
fn test_native_query_sends_graphql_input_and_decodes_sdk_owned_response() {
    let server = MockGraphqlServer::new(vec![json!({
        "data": {
            "query": {
                "chain": {"configuredName": "ethereum"},
                "datasetKey": "evm.logs",
                "range": {"kind": "block", "start": 1, "end": 2},
                "cache": {"hitRanges": [], "missingRanges": []},
                "rows": [{"blockNumber": 1}]
            }
        }
    })]);
    let client = client(&server, None);

    let response = client
        .native()
        .query(QueryInput {
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
                    topics: Vec::new(),
                }),
                other: None,
            },
            range: QueryRangeInput {
                kind: QueryRangeKindInput::Block,
                start: 1,
                end: 2,
            },
            finality: Some("durable_only".to_owned()),
            fields: None,
        })
        .expect("native query");

    assert_eq!(response.dataset_key, "evm.logs");
    assert_eq!(response.rows[0]["blockNumber"], 1);

    let request = server.only_request();
    assert!(request.query.contains("query($input: QueryInput!)"));
    assert_eq!(
        request.variables["input"]["chain"]["configuredName"],
        "ethereum"
    );
    assert_eq!(request.variables["input"]["datasetKey"]["family"], "evm");
    assert_eq!(request.variables["input"]["selector"]["kind"], "evm_logs");
}

#[test]
fn test_native_query_graphql_defaults_missing_finality_to_durable_only() {
    let server = MockGraphqlServer::new(vec![json!({
        "data": {
            "query": {
                "chain": {"configuredName": "ethereum"},
                "datasetKey": "evm.logs",
                "range": {"kind": "block", "start": 1, "end": 2},
                "cache": {
                    "hitRanges": [{"kind": "block", "start": 1, "end": 2}],
                    "missingRanges": [],
                    "hotHitRanges": [],
                    "providerFillRanges": [],
                    "segments": [{
                        "range": {"kind": "block", "start": 1, "end": 2},
                        "source": "durable",
                        "finality": "safe"
                    }]
                },
                "rows": [{"blockNumber": 1}]
            }
        }
    })]);
    let client = client(&server, None);
    let mut input = native_query_input();
    input.finality = None;

    let response = client.native().query(input).expect("native query");

    assert_eq!(response.dataset_key, "evm.logs");
    let request = server.only_request();
    assert_eq!(
        request.variables["input"]["finality"],
        json!("durable_only")
    );
}

#[test]
fn test_native_query_graphql_rejects_latest_segment_for_durable_query() {
    let server = MockGraphqlServer::new(vec![json!({
        "data": {
            "query": {
                "chain": {"configuredName": "ethereum"},
                "datasetKey": "evm.logs",
                "range": {"kind": "block", "start": 1, "end": 2},
                "cache": {
                    "hitRanges": [],
                    "missingRanges": [{"kind": "block", "start": 1, "end": 2}],
                    "hotHitRanges": [],
                    "providerFillRanges": [{"kind": "block", "start": 1, "end": 2}],
                    "segments": [{
                        "range": {"kind": "block", "start": 1, "end": 2},
                        "source": "provider",
                        "finality": "latest"
                    }]
                },
                "rows": []
            }
        }
    })]);
    let client = client(&server, None);

    let error = client
        .native()
        .query(native_query_input())
        .expect_err("durable query should reject latest segment");

    assert!(matches!(error, Error::Safety(_)));
    assert!(
        error
            .to_string()
            .contains("durable query returned non-durable segment"),
        "{error}"
    );
}

#[test]
fn test_graphql_errors_are_returned_without_requiring_live_datalens() {
    let server = MockGraphqlServer::new(vec![json!({
        "errors": [{
            "message": "not authorized",
            "extensions": {"code": "UNAUTHENTICATED"}
        }]
    })]);
    let client = client(&server, None);

    let error = client
        .index()
        .raw_events(EventFilter::new("evm.logs"), None, None)
        .expect_err("graphql error");

    assert!(matches!(error, Error::Graphql(errors) if errors[0].message == "not authorized"));
}

fn native_query_input() -> QueryInput {
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
                topics: Vec::new(),
            }),
            other: None,
        },
        range: QueryRangeInput {
            kind: QueryRangeKindInput::Block,
            start: 1,
            end: 2,
        },
        finality: Some("durable_only".to_owned()),
        fields: None,
    }
}

#[test]
fn test_graphql_error_extensions_are_typed_for_sdk_inspection() {
    let server = MockGraphqlServer::new(vec![json!({
        "errors": [{
            "message": "application query range quota exceeded",
            "extensions": {
                "kind": "rate_limited",
                "status": 429,
                "quota": {
                    "kind": "range_limit",
                    "scope": "application",
                    "limit": 1,
                    "requested": 2,
                    "observed": null,
                    "retry_after_seconds": null
                }
            }
        }]
    })]);
    let client = client(&server, None);

    let error = client
        .native()
        .query(QueryInput {
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
                    topics: Vec::new(),
                }),
                other: None,
            },
            range: QueryRangeInput {
                kind: QueryRangeKindInput::Block,
                start: 1,
                end: 2,
            },
            finality: Some("durable_only".to_owned()),
            fields: None,
        })
        .expect_err("graphql quota error");

    let api_error = error.api_error().expect("typed graphql error");
    assert_eq!(api_error.kind, ApiErrorKind::RateLimited);
    assert_eq!(api_error.status, Some(429));
    let quota = api_error.quota.expect("quota metadata");
    assert_eq!(quota.kind, QuotaErrorKind::RangeLimit);
    assert!(!error.is_retryable());
}

#[test]
fn test_graphql_request_rate_limit_is_retried_and_preserves_headers() {
    let server = MockGraphqlServer::new(vec![
        graphql_quota_error("request_rate_limit", Some(0)),
        json!({
            "data": {
                "events": [{
                    "indexName": "ormp",
                    "chain": "ethereum",
                    "chainId": 1,
                    "dataset": "evm.logs",
                    "blockNumber": 10,
                    "blockHash": "0xblock",
                    "transactionHash": "0xtx",
                    "transactionIndex": 1,
                    "eventIndex": 2,
                    "address": "0xaddr",
                    "selector": "Transfer",
                    "topics": [],
                    "topic0": null,
                    "signature": null,
                    "eventName": null,
                    "decoded": null,
                    "data": "0xdata",
                    "payload": {},
                    "createdAt": "2026-05-31T00:00:00Z"
                }]
            }
        }),
    ]);
    let client = graphql_client_with_retry(&server, test_retry_config(2));

    let events = client
        .index()
        .raw_events(EventFilter::new("evm.logs"), None, None)
        .expect("raw events");

    assert_eq!(events.len(), 1);
    let requests = server.requests();
    assert_eq!(requests.len(), 2, "{requests:?}");
    for request in requests {
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
fn test_graphql_range_limit_is_typed_and_not_retried() {
    let server = MockGraphqlServer::new(vec![graphql_quota_error("range_limit", None)]);
    let client = graphql_client_with_retry(&server, test_retry_config(3));

    let error = client
        .index()
        .raw_events(EventFilter::new("evm.logs"), None, None)
        .expect_err("range limit should fail");

    let api_error = error.api_error().expect("typed graphql error");
    assert_eq!(api_error.kind, ApiErrorKind::RateLimited);
    let quota = api_error.quota.expect("quota metadata");
    assert_eq!(quota.kind, QuotaErrorKind::RangeLimit);
    assert!(!error.is_retryable());
    assert_eq!(server.requests().len(), 1);
}

#[test]
fn test_http_unauthorized_is_auth_error() {
    let server = MockGraphqlServer::with_status(
        401,
        vec![json!({
            "error": "missing bearer token"
        })],
    );
    let client = client(&server, None);

    let error = client
        .native()
        .discovery()
        .expect_err("unauthorized response");

    assert!(matches!(error, Error::Unauthorized { status: 401, .. }));
}

fn client(server: &MockGraphqlServer, bearer_token: Option<&str>) -> DatalensClient {
    DatalensClient::with_graphql_endpoint(ClientConfig {
        endpoint: server.endpoint(),
        bearer_token: bearer_token.map(str::to_owned),
        application: Some("query-app".to_owned()),
        timeout: Some(Duration::from_secs(5)),
        user_agent: Some("datalens-sdk-tests".to_owned()),
    })
    .expect("client config")
}

fn graphql_client_with_retry(
    server: &MockGraphqlServer,
    retry_config: RetryConfig,
) -> DatalensClient {
    DatalensClient::with_graphql_endpoint_with_retry_config(
        ClientConfig {
            endpoint: server.endpoint(),
            bearer_token: Some("secret-token".to_owned()),
            application: Some("query-app".to_owned()),
            timeout: Some(Duration::from_secs(5)),
            user_agent: Some("datalens-sdk-tests".to_owned()),
        },
        retry_config,
    )
    .expect("client config")
}

fn graphql_quota_error(kind: &str, retry_after_seconds: Option<u64>) -> Value {
    json!({
        "errors": [{
            "message": "application quota exceeded",
            "extensions": {
                "kind": "rate_limited",
                "status": 429,
                "quota": {
                    "kind": kind,
                    "scope": "application",
                    "limit": 1,
                    "requested": null,
                    "observed": 1,
                    "retry_after_seconds": retry_after_seconds
                }
            }
        }]
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
    headers: RecordedHeaders,
    query: String,
    variables: Value,
}

#[derive(Clone, Debug, Default)]
struct RecordedHeaders {
    authorization: Option<String>,
    application: Option<String>,
    user_agent: Option<String>,
}

struct MockGraphqlServer {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MockGraphqlServer {
    fn new(responses: Vec<Value>) -> Self {
        Self::with_status(200, responses)
    }

    fn with_status(status: u16, responses: Vec<Value>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        listener
            .set_nonblocking(true)
            .expect("nonblocking mock server");
        let address = listener.local_addr().expect("mock address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_requests = Arc::clone(&requests);
        let handle = thread::spawn(move || {
            for response in responses {
                let started_at = Instant::now();
                let stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error)
                            if error.kind() == ErrorKind::WouldBlock
                                && started_at.elapsed() < Duration::from_millis(250) =>
                        {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) if error.kind() == ErrorKind::WouldBlock => return,
                        Err(error) => panic!("accept request: {error}"),
                    }
                };
                handle_connection(stream, status, response, &server_requests);
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

impl Drop for MockGraphqlServer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.join().expect("mock server thread");
        }
    }
}

fn handle_connection(
    mut stream: TcpStream,
    status: u16,
    response: Value,
    requests: &Arc<Mutex<Vec<RecordedRequest>>>,
) {
    let mut reader = BufReader::new(stream.try_clone().expect("clone stream"));
    let mut content_length = 0usize;
    let mut headers = RecordedHeaders::default();
    let mut line = String::new();
    reader.read_line(&mut line).expect("request line");
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
    let body: Value = serde_json::from_slice(&body).expect("graphql body");
    requests.lock().expect("requests").push(RecordedRequest {
        headers,
        query: body["query"].as_str().expect("query string").to_owned(),
        variables: body["variables"].clone(),
    });

    let response_body = serde_json::to_vec(&response).expect("response json");
    write!(
        stream,
        "HTTP/1.1 {status} OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        response_body.len()
    )
    .expect("response headers");
    stream.write_all(&response_body).expect("response body");
}
