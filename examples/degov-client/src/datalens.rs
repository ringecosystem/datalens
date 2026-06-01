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
pub const INDEX_NAME: &str = "degov";
pub const VOTE_CAST: &str = "VoteCast";
pub const VOTE_CAST_SIGNATURE: &str = "VoteCast(address,uint256,uint8,uint256,string)";

pub struct DatalensDegovClient {
    client: DatalensClient,
}

impl DatalensDegovClient {
    pub fn new(client: DatalensClient) -> Self {
        Self { client }
    }

    pub fn fetch_vote_cast_page(
        &self,
        config: &AppConfig,
        start_block: i32,
        end_block: i32,
    ) -> AppResult<VoteCastPage> {
        fetch_vote_cast_page(&self.client, config, start_block, end_block)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct VoteCastPage {
    pub events: Vec<VoteCastEvent>,
    pub next_cursor: Option<String>,
    pub has_next_page: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VoteCastEvent {
    pub cursor: String,
    pub event: DecodedEvent,
}

pub fn fetch_vote_cast_page(
    client: &DatalensClient,
    config: &AppConfig,
    start_block: i32,
    end_block: i32,
) -> AppResult<VoteCastPage> {
    let response = client
        .native()
        .query(query_input(config, start_block, end_block))?;
    let events = rows_from(&response.rows)?
        .into_iter()
        .filter(|row| topic_matches(row, &config.event_topic0))
        .map(|row| row_to_vote_cast_event(row, config))
        .collect::<AppResult<Vec<_>>>()?;

    Ok(VoteCastPage {
        events,
        next_cursor: Some((end_block + 1).to_string()),
        has_next_page: config.end_block.is_some_and(|target| end_block < target),
    })
}

pub fn query_input(config: &AppConfig, start_block: i32, end_block: i32) -> QueryInput {
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

fn row_to_vote_cast_event(row: &Value, config: &AppConfig) -> AppResult<VoteCastEvent> {
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

    Ok(VoteCastEvent {
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
            event_name: Some(VOTE_CAST.to_owned()),
            signature: Some(config.event_signature.clone()),
            topic0: row
                .get("topics")
                .and_then(Value::as_array)
                .and_then(|topics| topics.first())
                .and_then(Value::as_str)
                .map(str::to_owned),
            decoded_args: row
                .get("decodedArgs")
                .or_else(|| row.get("decoded_args"))
                .cloned()
                .unwrap_or(Value::Null),
            decode_status: Some("decoded".to_owned()),
            decode_error: None,
            payload: row.clone(),
            created_at: None,
        },
    })
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
