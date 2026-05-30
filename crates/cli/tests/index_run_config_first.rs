use std::{
    collections::VecDeque,
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    process::Command as ProcessCommand,
    thread,
};

use datalens_core::{DatasetKey, DatasetRows, QueryRows};

#[test]
fn test_index_run_cli_executes_checkpoint_skip_and_durable_hit_rerun_with_mock_datalens() {
    let root = temp_storage_root("index-run-cli-e2e");
    let output_path = root.join("events.jsonl");
    let checkpoint_path = root.join("checkpoint.json");
    unsafe {
        std::env::set_var("DATALENS_INDEX_RUN_E2E_TOKEN", "runner-token");
    }

    let first_server = MockDatalensServer::start(vec![
        response_json(10, 11, 2, false),
        response_json(12, 12, 1, false),
    ]);
    let config = write_index_config(
        &root,
        first_server.endpoint(),
        &output_path,
        &checkpoint_path,
    );

    let doctor_output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["index", "doctor", "--config"])
        .arg(&config)
        .output()
        .expect("run index doctor");
    assert!(
        doctor_output.status.success(),
        "doctor stderr: {}",
        String::from_utf8_lossy(&doctor_output.stderr)
    );

    let plan_output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["index", "plan", "--config"])
        .arg(&config)
        .output()
        .expect("run index plan");
    assert!(
        plan_output.status.success(),
        "plan stderr: {}",
        String::from_utf8_lossy(&plan_output.stderr)
    );
    let plan: serde_json::Value = serde_json::from_slice(&plan_output.stdout).expect("plan JSON");
    assert_eq!(plan["tasks"].as_array().expect("tasks").len(), 2);

    let first_report = run_index_config(&config);
    first_server.join();
    assert_eq!(first_report["summary"]["planned_queries"], 2);
    assert_eq!(first_report["summary"]["executed_queries"], 2);
    assert_eq!(first_report["summary"]["rows_written"], 3);
    assert_eq!(first_report["summary"]["provider_fill_range_count"], 2);
    assert_eq!(first_report["chains"][0]["chain"], "ethereum");
    assert_eq!(
        first_report["chains"][0]["provider_fill_ranges"]
            .as_array()
            .expect("ranges")
            .len(),
        2
    );
    assert_eq!(jsonl_line_count(&output_path), 3);

    let second_report = run_index_config(&config);
    assert_eq!(second_report["summary"]["executed_queries"], 0);
    assert_eq!(second_report["summary"]["checkpoint_skipped_ranges"], 2);
    assert_eq!(jsonl_line_count(&output_path), 3);

    fs::remove_file(&checkpoint_path).expect("delete checkpoint");
    let cached_server = MockDatalensServer::start(vec![
        response_json(10, 11, 2, true),
        response_json(12, 12, 1, true),
    ]);
    write_index_config(
        &root,
        cached_server.endpoint(),
        &output_path,
        &checkpoint_path,
    );

    let cached_report = run_index_config(&config);
    cached_server.join();
    assert_eq!(cached_report["summary"]["executed_queries"], 2);
    assert_eq!(cached_report["summary"]["full_durable_hit_count"], 2);
    assert_eq!(cached_report["summary"]["provider_fill_range_count"], 0);
    assert_eq!(cached_report["chains"][0]["full_durable_hit_count"], 2);
    assert_eq!(jsonl_line_count(&output_path), 6);
}

fn temp_storage_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-cli-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    fs::create_dir_all(&root).expect("create temp dir");
    root
}

fn write_index_config(
    root: &std::path::Path,
    endpoint: String,
    output_path: &std::path::Path,
    checkpoint_path: &std::path::Path,
) -> String {
    let config_path = root.join("mock.index.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[client]
endpoint = "{endpoint}"
application = "ormp"
token_env = "DATALENS_INDEX_RUN_E2E_TOKEN"

[index]
name = "ormp"
dataset = "evm.logs"
finality = "durable"
chunk_blocks = 2

[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 10
to_block = 12
addresses = ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
topics = []

[output.jsonl]
path = "{}"

[checkpoint]
path = "{}"
"#,
            output_path.display(),
            checkpoint_path.display()
        ),
    )
    .expect("write index config");
    config_path.to_string_lossy().into_owned()
}

fn run_index_config(config: &str) -> serde_json::Value {
    let output = ProcessCommand::new(env!("CARGO_BIN_EXE_datalens"))
        .args(["index", "run", "--config", config])
        .env("DATALENS_INDEX_RUN_E2E_TOKEN", "runner-token")
        .output()
        .expect("run index config");

    assert!(
        output.status.success(),
        "index run failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("run report JSON")
}

fn jsonl_line_count(path: &std::path::Path) -> usize {
    fs::read_to_string(path)
        .expect("jsonl output")
        .lines()
        .count()
}

fn response_json(start: u64, end: u64, row_count: usize, durable_hit: bool) -> serde_json::Value {
    let range = serde_json::json!({ "kind": "block", "start": start, "end": end });
    let (hit_ranges, missing_ranges, provider_fill_ranges) = if durable_hit {
        (vec![range.clone()], Vec::new(), Vec::new())
    } else {
        (Vec::new(), vec![range.clone()], vec![range.clone()])
    };
    serde_json::json!({
        "chain": {
            "family": "Evm",
            "configured_name": "ethereum",
            "network_id": { "kind": "numeric", "value": 1 }
        },
        "dataset_key": "evm.logs",
        "range": range,
        "cache": {
            "hit_ranges": hit_ranges,
            "missing_ranges": missing_ranges,
            "durable_hit_ranges": if durable_hit { vec![range] } else { Vec::<serde_json::Value>::new() },
            "hot_hit_ranges": [],
            "provider_fill_ranges": provider_fill_ranges,
            "promotion_pending_ranges": [],
            "segments": []
        },
        "rows": serde_json::to_value(DatasetRows::new(
            DatasetKey::evm_logs(),
            QueryRows::EvmLogs((0..row_count).map(|index| {
                datalens_core::LogRecord::try_new(
                    start + index as u64,
                    format!("0x{:064x}", start + index as u64),
                    format!("0x{index:064x}"),
                    0,
                    index as u64,
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    Vec::new(),
                    "0x".to_owned(),
                    false,
                )
                .expect("log record")
            }).collect()),
        ).expect("rows")).expect("rows json")
    })
}

struct MockDatalensServer {
    addr: SocketAddr,
    handle: thread::JoinHandle<()>,
}

impl MockDatalensServer {
    fn start(responses: Vec<serde_json::Value>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock datalens");
        let addr = listener.local_addr().expect("mock datalens addr");
        let handle = thread::spawn(move || {
            let mut responses = VecDeque::from(responses);
            while let Some(response) = responses.pop_front() {
                let (mut stream, _) = listener.accept().expect("accept mock request");
                let request = read_http_request(&mut stream);
                assert!(request.contains("POST /v1/query"), "{request}");
                write_http_json_response(&mut stream, response);
            }
        });
        Self { addr, handle }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn join(self) {
        self.handle.join().expect("mock datalens server");
    }
}

fn read_http_request(stream: &mut TcpStream) -> String {
    let mut buffer = Vec::new();
    let mut chunk = [0; 1024];
    loop {
        let read = stream.read(&mut chunk).expect("read mock request");
        assert!(read > 0, "connection closed before headers");
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    let headers = String::from_utf8_lossy(&buffer).into_owned();
    let content_length = headers
        .lines()
        .find_map(|line| {
            line.strip_prefix("content-length:")
                .or_else(|| line.strip_prefix("Content-Length:"))
                .and_then(|value| value.trim().parse::<usize>().ok())
        })
        .unwrap_or(0);
    let header_end = buffer
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
        .expect("header terminator");
    let current_body_len = buffer.len().saturating_sub(header_end);
    if current_body_len < content_length {
        let mut remaining = vec![0; content_length - current_body_len];
        stream
            .read_exact(&mut remaining)
            .expect("read mock request body");
        buffer.extend_from_slice(&remaining);
    }
    String::from_utf8_lossy(&buffer).into_owned()
}

fn write_http_json_response(stream: &mut TcpStream, body: serde_json::Value) {
    let body = serde_json::to_vec(&body).expect("response JSON");
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .expect("write mock response headers");
    stream.write_all(&body).expect("write mock response body");
}
