use std::{
    collections::VecDeque,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use datalens_client::{DatalensClient, HttpRequest, HttpResponse, HttpTransport};
use datalens_core::{DatasetKey, DatasetRows, QueryRows};
use datalens_indexer::{
    DatalensIndexConfig, IndexPlanBuilder, IndexRunner, IndexRunnerOptions, OutputSinkConfig,
    ParquetOutputConfig, QueryableStore, SqliteOutputStore, StoreQuery,
};

#[test]
fn test_chain_fetch_failure_preserves_last_durable_checkpoint_and_resume_indexes_remaining_range() {
    let root = temp_path("chain-fetch-resume");
    let sqlite_url = format!("sqlite:{}", root.join("index.db").display());
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(10, 13, 2);
    let output = OutputSinkConfig::DatabaseSqlite {
        url: sqlite_url.clone(),
    };
    let options = IndexRunnerOptions::default().with_checkpoint_path(&checkpoint_path);
    let first_transport = QueueTransport::new(vec![
        Ok(HttpResponse::json(200, response_json(10, 11))),
        Ok(HttpResponse::json(
            503,
            serde_json::json!({
                "error": {
                    "kind": "provider_failure",
                    "message": "provider unavailable"
                }
            }),
        )),
    ]);
    let first_client = client(first_transport.clone());

    let error = IndexRunner::new(plan.clone(), output.clone())
        .with_options(options.clone())
        .run(&first_client)
        .expect_err("second chunk fails");

    assert!(
        error.to_string().contains("provider unavailable"),
        "{error}"
    );
    assert_eq!(checkpoint_completed_block(&checkpoint_path), Some(11));
    assert_eq!(sqlite_blocks(&sqlite_url), vec![10, 11]);
    assert_eq!(request_ranges(&first_transport), vec![(10, 11), (12, 13)]);

    let second_transport =
        QueueTransport::new(vec![Ok(HttpResponse::json(200, response_json(12, 13)))]);
    let second_client = client(second_transport.clone());

    let report = IndexRunner::new(plan, output)
        .with_options(options)
        .run(&second_client)
        .expect("resume succeeds");

    assert_eq!(report.summary.executed_queries, 1);
    assert_eq!(report.summary.checkpoint_skipped_ranges, 1);
    assert_eq!(checkpoint_completed_block(&checkpoint_path), Some(13));
    assert_eq!(sqlite_blocks(&sqlite_url), vec![10, 11, 12, 13]);
    assert_eq!(request_ranges(&second_transport), vec![(12, 13)]);
}

#[test]
fn test_sqlite_write_failure_does_not_create_checkpoint_or_visible_rows() {
    let root = temp_path("sqlite-write-failure");
    let database_path = root.join("index.db");
    fs::write(&database_path, b"not a sqlite database").expect("write invalid database");
    let sqlite_url = format!("sqlite:{}", database_path.display());
    let checkpoint_path = root.join("checkpoint.json");
    let transport = QueueTransport::new(vec![Ok(HttpResponse::json(200, response_json(20, 21)))]);
    let client = client(transport);

    let error = IndexRunner::new(
        plan(20, 21, 2),
        OutputSinkConfig::DatabaseSqlite { url: sqlite_url },
    )
    .with_options(IndexRunnerOptions::default().with_checkpoint_path(&checkpoint_path))
    .run(&client)
    .expect_err("sqlite write fails");

    assert!(
        error.to_string().contains("sqlite")
            || error.to_string().contains("file is not a database"),
        "{error}"
    );
    assert!(!checkpoint_path.exists(), "checkpoint should not advance");
}

#[test]
fn test_parquet_flush_failure_does_not_advance_checkpoint_until_flush_succeeds_after_restart() {
    let root = temp_path("parquet-flush-restart");
    let blocked_output_path = root.join("blocked-output");
    fs::write(&blocked_output_path, b"file blocks output directory").expect("block output path");
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(30, 31, 2);
    let failing_transport =
        QueueTransport::new(vec![Ok(HttpResponse::json(200, response_json(30, 31)))]);
    let failing_client = client(failing_transport);

    let error = IndexRunner::new(
        plan.clone(),
        OutputSinkConfig::Parquet {
            config: parquet_config(blocked_output_path),
        },
    )
    .with_options(IndexRunnerOptions::default().with_checkpoint_path(&checkpoint_path))
    .run(&failing_client)
    .expect_err("parquet flush fails");

    assert!(error.to_string().contains("parquet"), "{error}");
    assert!(!checkpoint_path.exists(), "buffered rows were not durable");

    let output_path = root.join("parquet-output");
    let recovery_transport =
        QueueTransport::new(vec![Ok(HttpResponse::json(200, response_json(30, 31)))]);
    let recovery_client = client(recovery_transport);

    IndexRunner::new(
        plan,
        OutputSinkConfig::Parquet {
            config: parquet_config(output_path.clone()),
        },
    )
    .with_options(IndexRunnerOptions::default().with_checkpoint_path(&checkpoint_path))
    .run(&recovery_client)
    .expect("parquet recovery succeeds");

    assert_eq!(checkpoint_completed_block(&checkpoint_path), Some(31));
    assert_eq!(parquet_file_count(&output_path), 1);
}

#[test]
fn test_daemon_style_restart_with_checkpointed_sqlite_output_skips_duplicate_visible_records() {
    let root = temp_path("daemon-restart");
    let sqlite_url = format!("sqlite:{}", root.join("index.db").display());
    let checkpoint_path = root.join("checkpoint.json");
    let plan = plan(40, 43, 2);
    let output = OutputSinkConfig::DatabaseSqlite {
        url: sqlite_url.clone(),
    };
    let options = IndexRunnerOptions::default().with_checkpoint_path(&checkpoint_path);
    let first_transport = QueueTransport::new(vec![
        Ok(HttpResponse::json(200, response_json(40, 41))),
        Err(datalens_client::ClientError::Transport(
            "cache server unavailable".to_owned(),
        )),
    ]);
    let first_client = client(first_transport);

    IndexRunner::new(plan.clone(), output.clone())
        .with_options(options.clone())
        .run(&first_client)
        .expect_err("first daemon cycle stops after partial progress");

    let second_transport =
        QueueTransport::new(vec![Ok(HttpResponse::json(200, response_json(42, 43)))]);
    let second_client = client(second_transport);

    IndexRunner::new(plan, output)
        .with_options(options)
        .run(&second_client)
        .expect("second daemon cycle resumes");

    assert_eq!(sqlite_blocks(&sqlite_url), vec![40, 41, 42, 43]);
    assert_eq!(checkpoint_completed_block(&checkpoint_path), Some(43));
}

fn plan(start: u64, end: u64, chunk_blocks: u64) -> datalens_indexer::IndexPlan {
    let config = DatalensIndexConfig::from_toml_str(&format!(
        r#"
[client]
endpoint = "http://127.0.0.1:3000"
application = "ormp"
token_env = "PATH"

[index]
name = "ormp"
dataset = "evm.logs"
finality = "durable"
chunk_blocks = {chunk_blocks}

[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = {start}
to_block = {end}
addresses = ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
topics = []

[output.jsonl]
path = ".data/indexes/ormp/events.jsonl"

[checkpoint]
path = ".data/indexes/ormp/checkpoint.json"
"#
    ))
    .expect("index config");
    IndexPlanBuilder::new().build(&config).expect("plan")
}

fn client(transport: QueueTransport) -> DatalensClient<QueueTransport> {
    DatalensClient::with_transport(
        datalens_client::DatalensClientConfig {
            endpoint: "http://127.0.0.1:3000".to_owned(),
            application: Some("ormp".to_owned()),
            bearer_token: None,
        },
        transport,
    )
    .expect("client")
}

fn response_json(start: u64, end: u64) -> serde_json::Value {
    let range = serde_json::json!({ "kind": "block", "start": start, "end": end });
    serde_json::json!({
        "chain": {
            "family": "Evm",
            "configured_name": "ethereum",
            "network_id": { "kind": "numeric", "value": 1 }
        },
        "dataset_key": "evm.logs",
        "range": range,
        "cache": {
            "hit_ranges": [range],
            "missing_ranges": [],
            "durable_hit_ranges": [range],
            "hot_hit_ranges": [],
            "provider_fill_ranges": [],
            "promotion_pending_ranges": [],
            "segments": []
        },
        "rows": serde_json::to_value(DatasetRows::new(
            DatasetKey::evm_logs(),
            QueryRows::EvmLogs((start..=end).map(|block_number| {
                datalens_core::LogRecord::try_new(
                    block_number,
                    format!("0x{block_number:064x}"),
                    format!("0x{:064x}", block_number + 10_000),
                    0,
                    block_number - start,
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

fn parquet_config(path: PathBuf) -> ParquetOutputConfig {
    ParquetOutputConfig {
        path,
        max_rows_per_file: Some(100),
        max_bytes_per_file: None,
        partition_by: vec![],
        compression: None,
    }
}

fn checkpoint_completed_block(path: &Path) -> Option<u64> {
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(path).ok()?).expect("checkpoint json");
    value["entries"]
        .as_object()
        .and_then(|entries| entries.values().next())
        .and_then(|entry| entry["last_completed_block"].as_u64())
}

fn sqlite_blocks(url: &str) -> Vec<u64> {
    let store = SqliteOutputStore::connect(url).expect("sqlite store");
    let mut blocks = store
        .query(StoreQuery {
            dataset: "evm.logs".to_owned(),
            filter: serde_json::json!({ "chain": "ethereum", "from_block": 0, "to_block": 100 }),
        })
        .expect("query sqlite")
        .rows
        .into_iter()
        .map(|row| row["block_number"].as_u64().expect("block number"))
        .collect::<Vec<_>>();
    blocks.sort_unstable();
    blocks
}

fn request_ranges(transport: &QueueTransport) -> Vec<(u64, u64)> {
    transport
        .requests()
        .iter()
        .map(|request| {
            let range = &request.body["range"];
            (
                range["start"].as_u64().expect("range start"),
                range["end"].as_u64().expect("range end"),
            )
        })
        .collect()
}

fn parquet_file_count(root: &Path) -> usize {
    if !root.exists() {
        return 0;
    }
    let mut count = 0;
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).expect("read parquet dir") {
            let path = entry.expect("parquet entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some("parquet") {
                count += 1;
            }
        }
    }
    count
}

fn temp_path(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-indexer-fault-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("create temp dir");
    root
}

type QueuedResponse = Result<HttpResponse, datalens_client::ClientError>;

#[derive(Clone)]
struct QueueTransport {
    responses: Arc<Mutex<VecDeque<QueuedResponse>>>,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
}

impl QueueTransport {
    fn new(responses: Vec<QueuedResponse>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(VecDeque::from(responses))),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn requests(&self) -> Vec<HttpRequest> {
        self.requests.lock().expect("requests").clone()
    }
}

impl HttpTransport for QueueTransport {
    fn send(&self, request: HttpRequest) -> Result<HttpResponse, datalens_client::ClientError> {
        self.requests.lock().expect("requests").push(request);
        self.responses
            .lock()
            .expect("responses")
            .pop_front()
            .unwrap_or_else(|| {
                Err(datalens_client::ClientError::Transport(
                    "missing response".to_owned(),
                ))
            })
    }
}
