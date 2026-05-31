use std::sync::{Arc, Mutex};

use datalens_client::{DatalensClient, HttpRequest, HttpResponse, HttpTransport};
use datalens_core::{DatasetKey, DatasetRows, QueryRows};
use datalens_indexer::{
    DatalensIndexConfig, IndexPlanBuilder, IndexRunner, OutputSinkConfig, PlannedDecodeEvent,
    PlannedDecodeEventInput,
};

const TRANSFER_TOPIC0: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const FROM_TOPIC: &str = "0x0000000000000000000000001111111111111111111111111111111111111111";
const TO_TOPIC: &str = "0x0000000000000000000000002222222222222222222222222222222222222222";
const VALUE_DATA: &str = "0x000000000000000000000000000000000000000000000000000000000000007b";
const ORMP_MESSAGE_ACCEPTED_TOPIC0: &str =
    "0xcfb9b3466878aff0c7df17da215fd57d59eb245a5d03f5a7b57294d54581eb18";
const ORMP_MESSAGE_ACCEPTED_MSG_HASH: &str =
    "0x60f5743a8b3bbe4e4bd99607b19985203a9310f4859e03912ed086f4d32bdff8";
const ORMP_MESSAGE_ACCEPTED_DATA: &str = "0x000000000000000000000000000000000000000000000000000000000000002000000000000000000000000013b2211a7ca45db2808f6db05557ce5347e3634e000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000002cd1867fb8016f93710b6386f7f9f1d540a60812000000000000000000000000000000000000000000000000000000000000002e0000000000000000000000002cd1867fb8016f93710b6386f7f9f1d540a60812000000000000000000000000000000000000000000000000000000000001d874000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000a4394d1bca0000000000000000000000009f33a4809aa708d7a399fedba514e0a0d15efa850000000000000000000000009f33a4809aa708d7a399fedba514e0a0d15efa8500000000000000000000000000000000000000000000000000000000000000600000000000000000000000000000000000000000000000000000000000000008844866883501484100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000";

#[test]
fn test_index_runner_decodes_evm_logs_from_inline_abi() {
    let config = index_config(true);
    let plan = IndexPlanBuilder::new().build(&config).expect("plan builds");
    assert_eq!(plan.decode_events()[0].topic0, TRANSFER_TOPIC0);
    assert_eq!(plan.decode_events()[0].index.as_deref(), Some("erc20"));
    assert_eq!(plan.decode_events()[0].chain.as_deref(), Some("ethereum"));
    assert_eq!(plan.decode_events()[0].dataset.as_deref(), Some("evm.logs"));
    let direct_decode = datalens_indexer::decode_evm_log(
        plan.decode_events(),
        "erc20",
        "ethereum",
        "evm.logs",
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        &[
            TRANSFER_TOPIC0.to_owned(),
            FROM_TOPIC.to_owned(),
            TO_TOPIC.to_owned(),
        ],
        VALUE_DATA,
    );
    assert_eq!(direct_decode.status.as_str(), "decoded");
    let output_path = temp_path("success").join("events.jsonl");
    let transport = QueueTransport::new(vec![HttpResponse::json(
        200,
        response_json(vec![log_record(
            vec![TRANSFER_TOPIC0, FROM_TOPIC, TO_TOPIC],
            VALUE_DATA,
        )]),
    )]);
    let client =
        DatalensClient::with_transport(config.client.to_datalens_client_config(), transport)
            .expect("client config");
    let runner = IndexRunner::new(
        plan,
        OutputSinkConfig::FileJson {
            path: output_path.clone(),
        },
    );

    runner.run(&client).expect("decode should not fail run");

    let row = read_first_jsonl_row(&output_path);
    assert_eq!(row["topic0"], TRANSFER_TOPIC0);
    assert_eq!(row["decode_status"], "decoded");
    assert_eq!(row["event_name"], "Transfer");
    assert_eq!(row["signature"], "Transfer(address,address,uint256)");
    assert_eq!(
        row["decoded"],
        serde_json::json!({
            "from": "0x1111111111111111111111111111111111111111",
            "to": "0x2222222222222222222222222222222222222222",
            "value": "123",
        })
    );
    assert_eq!(row["address"], "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    assert_eq!(row["raw"]["topics"][0], TRANSFER_TOPIC0);
}

#[test]
fn test_index_runner_marks_unknown_topic0_without_dropping_evm_log() {
    let config = index_config(true);
    let plan = IndexPlanBuilder::new().build(&config).expect("plan builds");
    let output_path = temp_path("unknown").join("events.jsonl");
    let transport = QueueTransport::new(vec![HttpResponse::json(
        200,
        response_json(vec![log_record(
            vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
            "0x",
        )]),
    )]);
    let client =
        DatalensClient::with_transport(config.client.to_datalens_client_config(), transport)
            .expect("client config");
    let runner = IndexRunner::new(
        plan,
        OutputSinkConfig::FileJson {
            path: output_path.clone(),
        },
    );

    runner
        .run(&client)
        .expect("unknown topic should not fail run");

    let row = read_first_jsonl_row(&output_path);
    assert_eq!(row["decode_status"], "unknown_event");
    assert_eq!(
        row["decode_error"],
        "no configured ABI event matched topic0"
    );
    assert_eq!(row["raw"]["data"], "0x");
}

#[test]
fn test_index_runner_records_evm_decode_error_without_stopping_run() {
    let config = index_config(true);
    let plan = IndexPlanBuilder::new().build(&config).expect("plan builds");
    let output_path = temp_path("malformed").join("events.jsonl");
    let transport = QueueTransport::new(vec![HttpResponse::json(
        200,
        response_json(vec![log_record(
            vec![TRANSFER_TOPIC0, FROM_TOPIC, TO_TOPIC],
            "0x1234",
        )]),
    )]);
    let client =
        DatalensClient::with_transport(config.client.to_datalens_client_config(), transport)
            .expect("client config");
    let runner = IndexRunner::new(
        plan,
        OutputSinkConfig::FileJson {
            path: output_path.clone(),
        },
    );

    runner
        .run(&client)
        .expect("decode error should not fail run");

    let row = read_first_jsonl_row(&output_path);
    assert_eq!(row["decode_status"], "failed");
    assert!(
        row["decode_error"]
            .as_str()
            .expect("decode error")
            .contains("decode")
    );
    assert_eq!(row["event_name"], "Transfer");
    assert_eq!(row["raw"]["data"], "0x1234");
}

#[test]
fn test_index_runner_omits_decode_metadata_when_decoder_disabled() {
    let config = index_config(false);
    let plan = IndexPlanBuilder::new().build(&config).expect("plan builds");
    let output_path = temp_path("disabled").join("events.jsonl");
    let transport = QueueTransport::new(vec![HttpResponse::json(
        200,
        response_json(vec![log_record(
            vec![TRANSFER_TOPIC0, FROM_TOPIC, TO_TOPIC],
            VALUE_DATA,
        )]),
    )]);
    let client =
        DatalensClient::with_transport(config.client.to_datalens_client_config(), transport)
            .expect("client config");
    let runner = IndexRunner::new(
        plan,
        OutputSinkConfig::FileJson {
            path: output_path.clone(),
        },
    );

    runner.run(&client).expect("index run");

    let row = read_first_jsonl_row(&output_path);
    assert!(row.get("decode_status").is_none());
    assert!(row.get("decoded").is_none());
    assert_eq!(row["topics"][0], TRANSFER_TOPIC0);
}

#[test]
fn test_ormp_message_accepted_fragment_decodes_live_smoke_log() {
    let events = vec![PlannedDecodeEvent {
        name: "MessageAccepted".to_owned(),
        signature:
            "MessageAccepted(bytes32,(address,uint256,uint256,address,uint256,address,uint256,bytes))"
                .to_owned(),
        topic0: ORMP_MESSAGE_ACCEPTED_TOPIC0.to_owned(),
        chain: Some("ethereum".to_owned()),
        index: Some("ormp".to_owned()),
        dataset: Some("evm.logs".to_owned()),
        contract: Some("0x13b2211a7ca45db2808f6db05557ce5347e3634e".to_owned()),
        inputs: vec![
            PlannedDecodeEventInput {
                name: "msgHash".to_owned(),
                kind: "bytes32".to_owned(),
                indexed: true,
            },
            PlannedDecodeEventInput {
                name: "message".to_owned(),
                kind: "(address,uint256,uint256,address,uint256,address,uint256,bytes)"
                    .to_owned(),
                indexed: false,
            },
        ],
    }];

    let decoded = datalens_indexer::decode_evm_log(
        &events,
        "ormp",
        "ethereum",
        "evm.logs",
        "0x13b2211a7ca45db2808f6db05557ce5347e3634e",
        &[
            ORMP_MESSAGE_ACCEPTED_TOPIC0.to_owned(),
            ORMP_MESSAGE_ACCEPTED_MSG_HASH.to_owned(),
        ],
        ORMP_MESSAGE_ACCEPTED_DATA,
    );

    assert_eq!(decoded.status.as_str(), "decoded");
    assert_eq!(decoded.event_name.as_deref(), Some("MessageAccepted"));
    assert_eq!(
        decoded.signature.as_deref(),
        Some(
            "MessageAccepted(bytes32,(address,uint256,uint256,address,uint256,address,uint256,bytes))"
        )
    );
    let arguments = decoded.arguments.expect("decoded arguments");
    assert_eq!(arguments["msgHash"], ORMP_MESSAGE_ACCEPTED_MSG_HASH);
    assert_eq!(
        arguments["message"][0],
        "0x13b2211a7ca45db2808f6db05557ce5347e3634e"
    );
    assert_eq!(arguments["message"][2], "1");
    assert_eq!(
        arguments["message"][3],
        "0x2cd1867fb8016f93710b6386f7f9f1d540a60812"
    );
    assert_eq!(arguments["message"][4], "46");
}

fn index_config(decode_enabled: bool) -> DatalensIndexConfig {
    unsafe {
        std::env::set_var("DATALENS_INDEX_TEST_TOKEN", "runner-token");
    }
    DatalensIndexConfig::from_toml_str(&format!(
        r#"
[client]
endpoint = "http://127.0.0.1:3000"
application = "abi-test"
token_env = "DATALENS_INDEX_TEST_TOKEN"

[index]
name = "erc20"
dataset = "evm.logs"
finality = "durable"
chunk_blocks = 10

[[sources]]
chain = "ethereum"
family = "evm"
chain_id = 1
from_block = 10
to_block = 10
addresses = []
topics = []

[decode]
enabled = {decode_enabled}

[[decode.abis]]
chain = "ethereum"
index = "erc20"
dataset = "evm.logs"
json = '''
[
  {{
    "type": "event",
    "name": "Transfer",
    "anonymous": false,
    "inputs": [
      {{ "name": "from", "type": "address", "indexed": true }},
      {{ "name": "to", "type": "address", "indexed": true }},
      {{ "name": "value", "type": "uint256", "indexed": false }}
    ]
  }}
]
'''

[output.jsonl]
path = ".data/indexes/erc20/events.jsonl"

[checkpoint]
path = ".data/indexes/erc20/checkpoint.json"
"#
    ))
    .expect("valid config")
}

fn log_record(topics: Vec<&str>, data: &str) -> datalens_core::LogRecord {
    datalens_core::LogRecord::try_new(
        10,
        "0x01".to_owned(),
        "0x0000000000000000000000000000000000000000000000000000000000000000".to_owned(),
        0,
        0,
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        topics.into_iter().map(str::to_owned).collect(),
        data.to_owned(),
        false,
    )
    .expect("log record")
}

fn response_json(rows: Vec<datalens_core::LogRecord>) -> serde_json::Value {
    let range = serde_json::json!({ "kind": "block", "start": 10, "end": 10 });
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
            QueryRows::EvmLogs(rows),
        ).expect("rows")).expect("rows json")
    })
}

fn read_first_jsonl_row(path: &std::path::Path) -> serde_json::Value {
    let rows = std::fs::read_to_string(path).expect("jsonl output");
    serde_json::from_str(rows.lines().next().expect("first row")).expect("json row")
}

fn temp_path(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-indexer-evm-abi-{name}-{}",
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
    responses: Arc<Mutex<Vec<HttpResponse>>>,
}

impl QueueTransport {
    fn new(responses: Vec<HttpResponse>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into_iter().rev().collect())),
        }
    }
}

impl HttpTransport for QueueTransport {
    fn send(&self, _request: HttpRequest) -> Result<HttpResponse, datalens_client::ClientError> {
        self.responses
            .lock()
            .expect("responses lock")
            .pop()
            .ok_or_else(|| datalens_client::ClientError::Transport("missing response".to_owned()))
    }
}
