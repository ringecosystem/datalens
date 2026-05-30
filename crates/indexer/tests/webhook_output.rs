use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    thread,
    time::Duration,
};

use datalens_indexer::{
    IndexedRecord, OutputSinkConfig, OutputWriteSink, WebhookHeaderConfig, WebhookOutboxConfig,
    WebhookOutputConfig, WebhookRetryConfig,
};
use serde_json::Value;
use sqlx::{Row, sqlite::SqliteConnectOptions};
use tokio::runtime::Runtime;

fn record(block_number: u64, log_index: u64) -> IndexedRecord {
    IndexedRecord {
        index: "ormp".to_owned(),
        chain: "ethereum".to_owned(),
        chain_id: 1,
        dataset: "evm.logs".to_owned(),
        payload: serde_json::json!({
            "block_number": block_number,
            "block_hash": format!("0xblock{block_number:064x}"),
            "transaction_hash": format!("0xtx{block_number:064x}"),
            "transaction_index": 2,
            "log_index": log_index,
            "address": "0x0000000000000000000000000000000000000001",
            "topics": [
                "0x0000000000000000000000000000000000000000000000000000000000000000"
            ],
            "data": "0x010203",
            "removed": false,
        }),
    }
}

#[test]
fn test_webhook_output_posts_stable_payload_and_idempotency_header() {
    let server = MockWebhookServer::start(vec![MockResponse::ok()]);
    let sink = webhook_sink(&server.endpoint(), 100, Some("Idempotency-Key"));
    let result = sink
        .write_records(&[record(100, 0), record(101, 1)])
        .expect("webhook write");
    let requests = server.join();

    assert_eq!(result.written_rows, 2);
    let receipt = result.receipt.expect("receipt");
    assert_eq!(receipt.accepted_rows, 2);
    assert_eq!(receipt.inserted_rows, 2);
    assert_eq!(receipt.batches_attempted, 1);
    assert_eq!(receipt.batches_delivered, 1);
    assert_eq!(
        receipt.highest_position,
        Some("ethereum:101:2:1".to_owned())
    );

    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/indexed-events");
    assert_eq!(request.header("x-datalens-source"), Some("indexer"));
    assert_eq!(
        request.header("idempotency-key"),
        Some("ormp:evm:ethereum:1:evm.logs:ethereum:101:2:1:0")
    );

    let body: Value = serde_json::from_slice(&request.body).expect("json payload");
    assert_eq!(body["index"], "ormp");
    assert_eq!(body["chain"]["family"], "evm");
    assert_eq!(body["chain"]["name"], "ethereum");
    assert_eq!(body["chain"]["id"], 1);
    assert_eq!(body["chain"]["identity"], "evm:ethereum:1");
    assert_eq!(body["dataset"], "evm.logs");
    assert_eq!(body["batch"]["sequence"], 0);
    assert_eq!(body["batch"]["row_count"], 2);
    assert_eq!(body["batch"]["highest_position"], "ethereum:101:2:1");
    assert_eq!(
        body["batch"]["id"],
        "ormp:evm:ethereum:1:evm.logs:ethereum:101:2:1:0"
    );
    assert_eq!(body["rows"].as_array().expect("rows").len(), 2);
    assert_eq!(body["rows"][0]["payload"]["block_number"], 100);
}

#[test]
fn test_webhook_output_batches_by_row_count() {
    let server = MockWebhookServer::start(vec![
        MockResponse::ok(),
        MockResponse::ok(),
        MockResponse::ok(),
    ]);
    let sink = webhook_sink(&server.endpoint(), 2, None);

    sink.write_records(&[
        record(100, 0),
        record(101, 1),
        record(102, 2),
        record(103, 3),
        record(104, 4),
    ])
    .expect("webhook write");
    let requests = server.join();

    assert_eq!(requests.len(), 3);
    let row_counts: Vec<u64> = requests
        .iter()
        .map(|request| {
            let body: Value = serde_json::from_slice(&request.body).expect("json payload");
            body["batch"]["row_count"].as_u64().expect("row_count")
        })
        .collect();
    assert_eq!(row_counts, vec![2, 2, 1]);
}

#[test]
fn test_webhook_output_retries_5xx_and_returns_delivered_receipt() {
    let server = MockWebhookServer::start(vec![
        MockResponse::new(503, "temporary"),
        MockResponse::ok(),
    ]);
    let mut sink = webhook_sink(&server.endpoint(), 100, None);
    if let OutputSinkConfig::Webhook { webhook } = &mut sink {
        webhook.retry.max_attempts = 2;
        webhook.retry.initial_backoff_ms = 1;
        webhook.retry.max_backoff_ms = 1;
    }

    let result = sink
        .write_records(&[record(100, 0)])
        .expect("webhook write");
    let requests = server.join();

    assert_eq!(requests.len(), 2);
    let receipt = result.receipt.expect("receipt");
    assert_eq!(receipt.batches_attempted, 2);
    assert_eq!(receipt.batches_delivered, 1);
}

#[test]
fn test_webhook_output_classifies_timeout_as_retryable_failure() {
    let server = MockWebhookServer::start(vec![
        MockResponse::ok().with_delay_ms(100),
        MockResponse::ok(),
    ]);
    let mut sink = webhook_sink(&server.endpoint(), 100, None);
    if let OutputSinkConfig::Webhook { webhook } = &mut sink {
        webhook.timeout_ms = 20;
        webhook.retry.max_attempts = 2;
        webhook.retry.initial_backoff_ms = 1;
        webhook.retry.max_backoff_ms = 1;
    }

    let result = sink
        .write_records(&[record(100, 0)])
        .expect("webhook write");
    let requests = server.join();

    assert_eq!(requests.len(), 2);
    let receipt = result.receipt.expect("receipt");
    assert_eq!(receipt.batches_attempted, 2);
    assert_eq!(receipt.batches_delivered, 1);
}

#[test]
fn test_webhook_output_does_not_retry_permanent_4xx_and_redacts_secrets() {
    let server = MockWebhookServer::start(vec![MockResponse::new(
        400,
        "bad secret top-secret-token bearer abc123",
    )]);
    let mut sink = webhook_sink(&server.endpoint(), 100, None);
    if let OutputSinkConfig::Webhook { webhook } = &mut sink {
        webhook.headers.push(WebhookHeaderConfig {
            name: "Authorization".to_owned(),
            value: "Bearer top-secret-token".to_owned(),
            secret: true,
        });
        webhook.retry.max_attempts = 3;
    }

    let error = sink
        .write_records(&[record(100, 0)])
        .expect_err("permanent error")
        .to_string();
    let requests = server.join();

    assert_eq!(requests.len(), 1);
    assert!(error.contains("status 400"), "{error}");
    assert!(!error.contains("top-secret-token"), "{error}");
    assert!(!error.contains("abc123"), "{error}");
}

#[test]
fn test_webhook_outbox_persists_pending_batch_and_retries_after_reopen() {
    let outbox_path = temp_path("webhook-outbox-reopen").join("outbox.sqlite");
    let first_server = MockWebhookServer::start(vec![MockResponse::new(503, "temporary")]);
    let mut first_sink = webhook_sink(&first_server.endpoint(), 100, Some("Idempotency-Key"));
    enable_outbox(&mut first_sink, outbox_path.clone(), 2);

    let first = first_sink
        .write_records(&[record(100, 0)])
        .expect("persist failed webhook batch");
    let first_requests = first_server.join();
    drop(first_sink);

    assert_eq!(first.receipt.expect("receipt").batches_delivered, 0);
    assert_eq!(
        outbox_status_counts(&outbox_path),
        vec![("pending".to_owned(), 1)]
    );

    let second_server = MockWebhookServer::start(vec![MockResponse::ok()]);
    let mut second_sink = webhook_sink(&second_server.endpoint(), 100, Some("Idempotency-Key"));
    enable_outbox(&mut second_sink, outbox_path.clone(), 2);

    let second = second_sink.flush().expect("retry pending webhook batch");
    assert_eq!(second.receipt.expect("receipt").batches_delivered, 1);
    let second_requests = second_server.join();

    assert_eq!(
        outbox_status_counts(&outbox_path),
        Vec::<(String, i64)>::new()
    );
    assert_eq!(first_requests.len(), 1);
    assert_eq!(second_requests.len(), 1);
    assert_eq!(
        first_requests[0].header("idempotency-key"),
        second_requests[0].header("idempotency-key")
    );
}

#[test]
fn test_webhook_outbox_removes_successfully_delivered_batch() {
    let outbox_path = temp_path("webhook-outbox-success").join("outbox.sqlite");
    let server = MockWebhookServer::start(vec![MockResponse::ok()]);
    let mut sink = webhook_sink(&server.endpoint(), 100, Some("Idempotency-Key"));
    enable_outbox(&mut sink, outbox_path.clone(), 3);

    sink.write_records(&[record(100, 0)])
        .expect("deliver webhook batch");
    let requests = server.join();

    assert_eq!(requests.len(), 1);
    assert_eq!(
        outbox_status_counts(&outbox_path),
        Vec::<(String, i64)>::new()
    );
}

#[test]
fn test_webhook_outbox_dead_letters_permanent_failure_with_redacted_error() {
    let outbox_path = temp_path("webhook-outbox-dead-letter").join("outbox.sqlite");
    let server = MockWebhookServer::start(vec![MockResponse::new(
        400,
        "bad secret top-secret-token bearer abc123",
    )]);
    let mut sink = webhook_sink(&server.endpoint(), 100, None);
    if let OutputSinkConfig::Webhook { webhook } = &mut sink {
        webhook.headers.push(WebhookHeaderConfig {
            name: "Authorization".to_owned(),
            value: "Bearer top-secret-token".to_owned(),
            secret: true,
        });
    }
    enable_outbox(&mut sink, outbox_path.clone(), 3);

    let result = sink
        .write_records(&[record(100, 0)])
        .expect("dead-letter permanent webhook failure");
    let requests = server.join();
    let dead_letter = outbox_record(&outbox_path).expect("dead-letter record");

    assert_eq!(requests.len(), 1);
    assert_eq!(result.receipt.expect("receipt").batches_delivered, 0);
    assert_eq!(dead_letter.status, "dead_letter");
    assert!(dead_letter.last_error.contains("status 400"));
    assert!(!dead_letter.last_error.contains("top-secret-token"));
    assert!(!dead_letter.last_error.contains("abc123"));
}

fn webhook_sink(
    url: &str,
    max_rows_per_request: usize,
    idempotency_header: Option<&str>,
) -> OutputSinkConfig {
    OutputSinkConfig::Webhook {
        webhook: WebhookOutputConfig {
            url: url.to_owned(),
            headers: vec![WebhookHeaderConfig {
                name: "X-Datalens-Source".to_owned(),
                value: "indexer".to_owned(),
                secret: false,
            }],
            timeout_ms: 5_000,
            max_rows_per_request,
            max_bytes_per_request: 1_000_000,
            retry: WebhookRetryConfig::default(),
            idempotency_key_header: idempotency_header.map(str::to_owned),
            outbox: WebhookOutboxConfig::default(),
        },
    }
}

fn enable_outbox(sink: &mut OutputSinkConfig, path: PathBuf, max_attempts: usize) {
    if let OutputSinkConfig::Webhook { webhook } = sink {
        webhook.retry.initial_backoff_ms = 1;
        webhook.retry.max_backoff_ms = 1;
        webhook.retry.max_attempts = 1;
        webhook.outbox.enabled = true;
        webhook.outbox.path = Some(path);
        webhook.outbox.max_attempts = max_attempts;
    }
}

fn temp_path(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("datalens-{name}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn outbox_status_counts(path: &std::path::Path) -> Vec<(String, i64)> {
    with_outbox_runtime(path, |runtime, pool| {
        runtime.block_on(async {
            sqlx::query("SELECT status, COUNT(*) AS count FROM webhook_outbox GROUP BY status ORDER BY status")
                .fetch_all(pool)
                .await
                .expect("query outbox")
                .into_iter()
                .map(|row| {
                    (
                        row.get::<String, _>("status"),
                        row.get::<i64, _>("count"),
                    )
                })
                .collect()
        })
    })
}

fn outbox_record(path: &std::path::Path) -> Option<OutboxRecord> {
    with_outbox_runtime(path, |runtime, pool| {
        runtime.block_on(async {
            sqlx::query("SELECT status, last_error FROM webhook_outbox LIMIT 1")
                .fetch_optional(pool)
                .await
                .expect("query outbox")
                .map(|row| OutboxRecord {
                    status: row.get("status"),
                    last_error: row.get("last_error"),
                })
        })
    })
}

fn with_outbox_runtime<T>(
    path: &std::path::Path,
    f: impl FnOnce(&Runtime, &sqlx::SqlitePool) -> T,
) -> T {
    let runtime = Runtime::new().expect("runtime");
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false);
    let pool = runtime
        .block_on(sqlx::sqlite::SqlitePoolOptions::new().connect_with(options))
        .expect("connect outbox");
    f(&runtime, &pool)
}

struct OutboxRecord {
    status: String,
    last_error: String,
}

struct MockWebhookServer {
    addr: std::net::SocketAddr,
    handle: thread::JoinHandle<Vec<RecordedRequest>>,
}

impl MockWebhookServer {
    fn start(responses: Vec<MockResponse>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock webhook");
        let addr = listener.local_addr().expect("mock webhook addr");
        let handle = thread::spawn(move || {
            let mut handles = Vec::new();
            for response in responses {
                let (mut stream, _) = listener.accept().expect("accept webhook request");
                handles.push(thread::spawn(move || {
                    let request = read_request(&mut stream);
                    write_response(&mut stream, response);
                    request
                }));
            }
            handles
                .into_iter()
                .map(|handle| handle.join().expect("mock webhook request"))
                .collect()
        });

        Self { addr, handle }
    }

    fn endpoint(&self) -> String {
        format!("http://{}/indexed-events", self.addr)
    }

    fn join(self) -> Vec<RecordedRequest> {
        self.handle.join().expect("mock webhook server")
    }
}

struct MockResponse {
    status: u16,
    body: String,
    delay_ms: u64,
}

impl MockResponse {
    fn ok() -> Self {
        Self::new(200, "{}")
    }

    fn new(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.to_owned(),
            delay_ms: 0,
        }
    }

    fn with_delay_ms(mut self, delay_ms: u64) -> Self {
        self.delay_ms = delay_ms;
        self
    }
}

#[derive(Debug)]
struct RecordedRequest {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl RecordedRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

fn read_request(stream: &mut TcpStream) -> RecordedRequest {
    let mut buffer = Vec::new();
    let mut temp = [0; 1024];
    loop {
        let read = stream.read(&mut temp).expect("read request");
        assert_ne!(read, 0, "request ended before headers");
        buffer.extend_from_slice(&temp[..read]);
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    let header_end = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("headers end")
        + 4;
    let headers_text = String::from_utf8(buffer[..header_end].to_vec()).expect("utf8 headers");
    let mut lines = headers_text.split("\r\n");
    let request_line = lines.next().expect("request line");
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().expect("method").to_owned();
    let path = request_parts.next().expect("path").to_owned();
    let headers: Vec<(String, String)> = lines
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (name, value) = line.split_once(':').expect("header separator");
            (name.to_owned(), value.trim().to_owned())
        })
        .collect();
    let content_length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or_default();
    let mut body = buffer[header_end..].to_vec();
    while body.len() < content_length {
        let read = stream.read(&mut temp).expect("read body");
        assert_ne!(read, 0, "request ended before body");
        body.extend_from_slice(&temp[..read]);
    }
    body.truncate(content_length);

    RecordedRequest {
        method,
        path,
        headers,
        body,
    }
}

fn write_response(stream: &mut TcpStream, response: MockResponse) {
    if response.delay_ms > 0 {
        thread::sleep(Duration::from_millis(response.delay_ms));
    }
    let status_text = match response.status {
        200 => "OK",
        400 => "Bad Request",
        503 => "Service Unavailable",
        _ => "Status",
    };
    let body = response.body.as_bytes();
    let _ = write!(
        stream,
        "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        response.status,
        status_text,
        body.len()
    );
    let _ = stream.write_all(body);
}
