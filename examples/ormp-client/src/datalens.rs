use alloy_dyn_abi::{DynSolEvent, DynSolType, DynSolValue};
use alloy_primitives::{
    B256,
    hex::{self, FromHex},
};
use datalens_sdk::{
    DatalensClient,
    index::DecodedEvent,
    native::{
        ChainFamilyInput, ChainFamilyKindInput, ChainIdentityInput, DatasetKeyInput,
        EvmLogsSelectorInput, NetworkIdInput, QueryInput, QueryRangeInput, QueryRangeKindInput,
        QuerySelectorInput, SelectorKindInput,
    },
};
use serde_json::Value;

use crate::{AppError, AppResult, config::AppConfig};

pub const DATASET: &str = "evm.logs";
pub const INDEX_NAME: &str = "ormp";
pub const MESSAGE_ACCEPTED: &str = "MessageAccepted";
pub const MESSAGE_ACCEPTED_SIGNATURE: &str =
    "MessageAccepted(bytes32,(address,uint256,uint256,address,uint256,address,uint256,bytes))";

pub struct DatalensOrmpClient {
    client: DatalensClient,
}

impl DatalensOrmpClient {
    pub fn new(client: DatalensClient) -> Self {
        Self { client }
    }

    pub fn fetch_message_accepted_page(
        &self,
        config: &AppConfig,
        start_block: u64,
        end_block: u64,
    ) -> AppResult<MessageAcceptedPage> {
        fetch_message_accepted_page(&self.client, config, start_block, end_block)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MessageAcceptedPage {
    pub events: Vec<MessageAcceptedEvent>,
    pub next_cursor: Option<String>,
    pub has_next_page: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MessageAcceptedEvent {
    pub cursor: String,
    pub event: DecodedEvent,
}

pub fn fetch_message_accepted_page(
    client: &DatalensClient,
    config: &AppConfig,
    start_block: u64,
    end_block: u64,
) -> AppResult<MessageAcceptedPage> {
    let response = client
        .native()
        .query(query_input(config, start_block, end_block))?;
    let events = rows_from(&response.rows)?
        .into_iter()
        .map(|row| row_to_message_accepted_event(row, config))
        .collect::<AppResult<Vec<_>>>()?;

    Ok(MessageAcceptedPage {
        events,
        next_cursor: Some((end_block + 1).to_string()),
        has_next_page: config.end_block.is_some_and(|target| end_block < target),
    })
}

pub fn query_input(config: &AppConfig, start_block: u64, end_block: u64) -> QueryInput {
    QueryInput {
        chain: ChainIdentityInput {
            family: ChainFamilyInput {
                kind: ChainFamilyKindInput::Evm,
                other: None,
            },
            configured_name: config.chain_name.clone(),
            network_id: Some(NetworkIdInput {
                numeric: Some(config.chain_id),
                textual: None,
            }),
        },
        dataset_key: DatasetKeyInput {
            family: config.dataset_family.clone(),
            name: config.dataset_name.clone(),
        },
        selector: QuerySelectorInput {
            kind: SelectorKindInput::EvmLogs,
            evm_logs: Some(EvmLogsSelectorInput {
                addresses: vec![config.contract_address.clone()],
                topics: vec![vec![config.event_topic0.clone()]],
            }),
            other: None,
        },
        range: QueryRangeInput {
            kind: QueryRangeKindInput::Block,
            start: start_block,
            end: end_block,
        },
        finality: Some("durable_only".to_owned()),
        fields: None,
    }
}

fn rows_from(rows: &Value) -> AppResult<Vec<&Value>> {
    if let Some(rows) = rows.as_array() {
        return Ok(rows.iter().collect());
    }
    if let Some(rows) = rows.get("rows").and_then(Value::as_array) {
        return Ok(rows.iter().collect());
    }
    if let Some(rows) = rows
        .get("rows")
        .and_then(|rows| rows.get("rows"))
        .and_then(Value::as_array)
    {
        return Ok(rows.iter().collect());
    }
    if let Some(rows) = rows
        .get("rows")
        .and_then(|rows| rows.get("rows"))
        .and_then(|rows| rows.get("rows"))
        .and_then(Value::as_array)
    {
        return Ok(rows.iter().collect());
    }
    Err(AppError::Handler(
        "native Datalens response did not contain log rows".to_owned(),
    ))
}

fn topic_matches(row: &Value, topic0: &str) -> bool {
    row.get("topics")
        .and_then(Value::as_array)
        .and_then(|topics| topics.first())
        .and_then(Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case(topic0))
}

fn row_to_message_accepted_event(
    row: &Value,
    config: &AppConfig,
) -> AppResult<MessageAcceptedEvent> {
    let block_number = i32_field(row, "block_number")?;
    let transaction_index = i32_field(row, "transaction_index")?;
    let log_index = i32_field(row, "log_index")?;
    let transaction_hash = string_field(row, "transaction_hash");
    let cursor = format!(
        "{}:{}:{}",
        block_number,
        transaction_hash.as_deref().unwrap_or("<missing-tx>"),
        log_index
    );
    let decoded = decode_message_accepted(row, &config.event_topic0);

    Ok(MessageAcceptedEvent {
        cursor,
        event: DecodedEvent {
            index_name: Some(INDEX_NAME.to_owned()),
            chain: Some(config.chain_name.clone()),
            chain_id: Some(config.chain_id),
            dataset: Some(DATASET.to_owned()),
            block_number: Some(block_number),
            block_hash: string_field(row, "block_hash"),
            transaction_hash,
            transaction_index: Some(transaction_index),
            log_index: Some(log_index),
            address: string_field(row, "address"),
            event_name: Some(MESSAGE_ACCEPTED.to_owned()),
            signature: Some(config.event_signature.clone()),
            topic0: row
                .get("topics")
                .and_then(Value::as_array)
                .and_then(|topics| topics.first())
                .and_then(Value::as_str)
                .map(str::to_owned),
            decoded_args: decoded.args,
            decode_status: Some(decoded.status.to_owned()),
            decode_error: decoded.error,
            payload: row.clone(),
            created_at: None,
        },
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LocalDecode {
    status: &'static str,
    args: Value,
    error: Option<String>,
}

fn decode_message_accepted(row: &Value, topic0: &str) -> LocalDecode {
    if !topic_matches(row, topic0) {
        return LocalDecode {
            status: "unsupported",
            args: Value::Null,
            error: Some("log topic0 does not match MessageAccepted".to_owned()),
        };
    }

    match decode_message_accepted_args(row, topic0) {
        Ok(args) => LocalDecode {
            status: "decoded",
            args,
            error: None,
        },
        Err(error) => LocalDecode {
            status: "failed",
            args: Value::Null,
            error: Some(error),
        },
    }
}

fn decode_message_accepted_args(row: &Value, topic0: &str) -> Result<Value, String> {
    let topics = topics_from(row)?;
    let data = data_from(row)?;
    let event = DynSolEvent::new(
        Some(parse_topic(topic0)?),
        vec![
            "bytes32"
                .parse::<DynSolType>()
                .map_err(|error| error.to_string())?,
        ],
        DynSolType::Tuple(vec![
            "(address,uint256,uint256,address,uint256,address,uint256,bytes)"
                .parse::<DynSolType>()
                .map_err(|error| error.to_string())?,
        ]),
    )
    .ok_or_else(|| "invalid MessageAccepted event ABI".to_owned())?;
    let decoded = event
        .decode_log_parts(topics, &data)
        .map_err(|error| error.to_string())?;
    let msg_hash = decoded
        .indexed
        .first()
        .map(sol_value_to_json)
        .ok_or_else(|| "decoded MessageAccepted log is missing msgHash".to_owned())?;
    let message = decoded
        .body
        .first()
        .ok_or_else(|| "decoded MessageAccepted log is missing message tuple".to_owned())?;
    let DynSolValue::Tuple(fields) = message else {
        return Err("decoded MessageAccepted message is not a tuple".to_owned());
    };

    let mut object = serde_json::Map::new();
    object.insert("msgHash".to_owned(), msg_hash.clone());
    object.insert("messageHash".to_owned(), msg_hash);
    if let Some(value) = fields.first() {
        object.insert("sender".to_owned(), sol_value_to_json(value));
    }
    if let Some(value) = fields.get(1) {
        object.insert("sourceChainId".to_owned(), sol_value_to_json(value));
    }
    if let Some(value) = fields.get(2) {
        object.insert("targetChainId".to_owned(), sol_value_to_json(value));
    }
    if let Some(value) = fields.get(3) {
        object.insert("receiver".to_owned(), sol_value_to_json(value));
    }
    Ok(Value::Object(object))
}

fn topics_from(row: &Value) -> Result<Vec<B256>, String> {
    row.get("topics")
        .and_then(Value::as_array)
        .ok_or_else(|| "native log row is missing topics".to_owned())?
        .iter()
        .map(|topic| {
            topic
                .as_str()
                .ok_or_else(|| "native log topic is not a string".to_owned())
                .and_then(parse_topic)
        })
        .collect()
}

fn data_from(row: &Value) -> Result<Vec<u8>, String> {
    let data = row
        .get("data")
        .and_then(Value::as_str)
        .ok_or_else(|| "native log row is missing data".to_owned())?;
    let data = data
        .strip_prefix("0x")
        .ok_or_else(|| "native log data must start with 0x".to_owned())?;
    Vec::from_hex(data).map_err(|error| format!("invalid native log data: {error}"))
}

fn parse_topic(value: &str) -> Result<B256, String> {
    value
        .parse::<B256>()
        .map_err(|error| format!("invalid native log topic: {error}"))
}

fn sol_value_to_json(value: &DynSolValue) -> Value {
    match value {
        DynSolValue::Bool(value) => Value::Bool(*value),
        DynSolValue::Int(value, _) => Value::String(value.to_string()),
        DynSolValue::Uint(value, _) => Value::String(value.to_string()),
        DynSolValue::FixedBytes(value, size) => {
            Value::String(format!("0x{}", hex::encode(&value[..*size])))
        }
        DynSolValue::Address(value) => Value::String(format!("{value:#x}")),
        DynSolValue::Function(value) => Value::String(format!("0x{}", hex::encode(value))),
        DynSolValue::Bytes(value) => Value::String(format!("0x{}", hex::encode(value))),
        DynSolValue::String(value) => Value::String(value.clone()),
        DynSolValue::Array(values)
        | DynSolValue::FixedArray(values)
        | DynSolValue::Tuple(values) => {
            Value::Array(values.iter().map(sol_value_to_json).collect())
        }
    }
}

fn string_field(row: &Value, name: &str) -> Option<String> {
    row.get(name).and_then(Value::as_str).map(str::to_owned)
}

fn i32_field(row: &Value, name: &str) -> AppResult<i32> {
    row.get(name)
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
                .and_then(|value| i32::try_from(value).ok())
        })
        .ok_or_else(|| AppError::Handler(format!("native log row is missing {name}")))
}
