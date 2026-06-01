use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use datalens_sdk::{
    ClientConfig, DatalensClient,
    native::{
        ChainFamilyInput, ChainFamilyKindInput, ChainIdentityInput, DatasetKeyInput,
        EvmLogsSelectorInput, FieldSelectionInput, NetworkIdInput, QueryInput, QueryRangeInput,
        QueryRangeKindInput, QuerySelectorInput, SelectorKindInput,
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
    assert_eq!(body["chain"]["configuredName"], "ethereum");
    assert_eq!(body["chain"]["family"]["kind"], "evm");
    assert_eq!(body["chain"]["networkId"]["numeric"], 1);
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

struct MockRestServer {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MockRestServer {
    fn new(responses: Vec<Value>) -> Self {
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
    response: Value,
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
    let body: Value = serde_json::from_slice(&body).expect("rest body");
    requests.lock().expect("requests").push(RecordedRequest {
        method,
        path,
        headers,
        body,
    });

    let response_body = serde_json::to_vec(&response).expect("response json");
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        response_body.len()
    )
    .expect("response headers");
    stream.write_all(&response_body).expect("response body");
}
