use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Duration,
};

use datalens_client::{DatalensClient, HttpRequest, HttpResponse, HttpTransport};
use datalens_core::{DatasetKey, DatasetRows, QueryRows};
use datalens_indexer::{
    DatalensIndexConfig, IndexDaemon, IndexDaemonOptions, QueryableStore, SqliteOutputStore,
    StoreQuery,
};

#[tokio::test]
async fn test_daemon_runs_index_cycle_starts_graphql_and_shuts_down() {
    let root = temp_path("daemon-sqlite-query");
    let sqlite_url = format!("sqlite:{}", root.join("index.db").display());
    let checkpoint_path = root.join("checkpoint.json");
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
chunk_blocks = 2

[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 10
to_block = 10
addresses = ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
topics = []

[output]
kind = "database"

[output.database]
driver = "sqlite"
url = "{sqlite_url}"

[query]
enabled = true
protocol = "graphql"
bind = "127.0.0.1:0"

[checkpoint]
path = "{}"
"#,
        checkpoint_path.display()
    ))
    .expect("daemon config");
    let transport = QueueTransport::new(vec![HttpResponse::json(200, response_json(10, 10, 1))]);
    let client = DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        transport.clone(),
    )
    .expect("client");
    let daemon = IndexDaemon::new(config, client).with_options(
        IndexDaemonOptions::default()
            .with_poll_interval(Duration::from_millis(20))
            .with_run_once(true),
    );

    let report = daemon
        .run_until_shutdown(async {})
        .await
        .expect("daemon run");

    assert_eq!(report.index_runs, 1);
    assert_eq!(
        report
            .query_service
            .as_ref()
            .expect("query service")
            .graphql_path,
        "/graphql"
    );
    assert_eq!(transport.requests().len(), 1);

    let rows = tokio::task::spawn_blocking(move || {
        let store = SqliteOutputStore::connect(&sqlite_url).expect("sqlite store");
        store
            .query(StoreQuery {
                dataset: "evm.logs".to_owned(),
                filter: serde_json::json!({
                    "chain": "ethereum",
                    "from_block": 10,
                    "to_block": 10
                }),
            })
            .expect("query sqlite")
    })
    .await
    .expect("query task");
    assert_eq!(rows.rows.len(), 1);
}

#[tokio::test]
async fn test_daemon_retries_provider_timeout_and_completes_without_restart() {
    let root = temp_path("daemon-provider-timeout-retry");
    let sqlite_url = format!("sqlite:{}", root.join("index.db").display());
    let checkpoint_path = root.join("checkpoint.json");
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
chunk_blocks = 1

[index.retry]
max_attempts = 2
initial_backoff_ms = 1
max_backoff_ms = 1

[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 20
to_block = 20
addresses = ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
topics = []

[output]
kind = "database"

[output.database]
driver = "sqlite"
url = "{sqlite_url}"

[checkpoint]
path = "{}"
"#,
        checkpoint_path.display()
    ))
    .expect("daemon config");
    let transport = QueueTransport::new(vec![
        HttpResponse::json(
            504,
            serde_json::json!({
                "error": {
                    "kind": "provider_timeout",
                    "message": "provider transport failed method=eth_getLogs kind=ProviderTimeout"
                }
            }),
        ),
        HttpResponse::json(200, response_json(20, 20, 1)),
    ]);
    let client = DatalensClient::with_transport(
        config.client.to_datalens_client_config(),
        transport.clone(),
    )
    .expect("client");
    let daemon = IndexDaemon::new(config, client).with_options(
        IndexDaemonOptions::default()
            .with_poll_interval(Duration::from_millis(20))
            .with_run_once(true),
    );

    let report = daemon
        .run_until_shutdown(async {})
        .await
        .expect("daemon retry run");

    assert_eq!(report.index_runs, 1);
    assert_eq!(transport.requests().len(), 2);
    assert_eq!(checkpoint_completed_block(&checkpoint_path), Some(20));

    let rows = tokio::task::spawn_blocking(move || {
        let store = SqliteOutputStore::connect(&sqlite_url).expect("sqlite store");
        store
            .query(StoreQuery {
                dataset: "evm.logs".to_owned(),
                filter: serde_json::json!({
                    "chain": "ethereum",
                    "from_block": 20,
                    "to_block": 20
                }),
            })
            .expect("query sqlite")
    })
    .await
    .expect("query task");
    assert_eq!(rows.rows.len(), 1);
}

fn response_json(start: u64, end: u64, row_count: usize) -> serde_json::Value {
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
            "durable_hit_ranges": [{
                "kind": "block",
                "start": start,
                "end": end
            }],
            "hot_hit_ranges": [],
            "provider_fill_ranges": [],
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

fn checkpoint_completed_block(path: &std::path::Path) -> Option<u64> {
    let value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path).ok()?).expect("checkpoint json");
    value["entries"]
        .as_object()
        .and_then(|entries| entries.values().next())
        .and_then(|entry| entry["last_completed_block"].as_u64())
}

fn temp_path(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-indexer-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp dir");
    root
}

#[derive(Clone)]
struct QueueTransport {
    responses: Arc<Mutex<VecDeque<HttpResponse>>>,
    requests: Arc<Mutex<Vec<HttpRequest>>>,
}

impl QueueTransport {
    fn new(responses: Vec<HttpResponse>) -> Self {
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
            .ok_or_else(|| datalens_client::ClientError::Transport("missing response".to_owned()))
    }
}
