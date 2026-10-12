use datalens_core::{Address, B256, BlockRow, Bytes, Finality, LogRow};
use datalens_provider::ProviderError;
use serde_json::Value;

pub(crate) fn parse_log(
    value: &Value,
    finality: Finality,
) -> Result<Option<LogRow>, ProviderError> {
    if value
        .get("removed")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(None);
    }
    let topics = value
        .get("topics")
        .and_then(Value::as_array)
        .ok_or_else(|| ProviderError::fatal("log is missing topics"))?;
    if topics.len() > 4 {
        return Err(ProviderError::fatal("log has more than 4 topics"));
    }
    let mut parsed_topics = Vec::with_capacity(topics.len());
    for topic in topics {
        let text = topic
            .as_str()
            .ok_or_else(|| ProviderError::fatal("log topic is not a string"))?;
        parsed_topics.push(parse_b256(text, "topic")?);
    }
    Ok(Some(LogRow {
        block_number: parse_quantity(required_str(value, "blockNumber")?, "blockNumber")?,
        log_index: parse_u32(required_str(value, "logIndex")?, "logIndex")?,
        block_hash: parse_b256(required_str(value, "blockHash")?, "blockHash")?,
        transaction_index: parse_u32(required_str(value, "transactionIndex")?, "transactionIndex")?,
        transaction_hash: parse_b256(required_str(value, "transactionHash")?, "transactionHash")?,
        address: parse_address(required_str(value, "address")?)?,
        topics: parsed_topics,
        data: parse_bytes(required_str(value, "data")?)?,
        finality,
    }))
}

pub(crate) fn parse_block(value: &Value, finality: Finality) -> Result<BlockRow, ProviderError> {
    if value.is_null() {
        return Err(ProviderError::NotReady);
    }
    let Value::Object(object) = value else {
        return Err(ProviderError::fatal("block is not an object"));
    };
    let number = parse_quantity(required_str(value, "number")?, "number")?;
    let hash = parse_b256(required_str(value, "hash")?, "hash")?;
    let parent_hash = parse_b256(required_str(value, "parentHash")?, "parentHash")?;
    let timestamp = parse_quantity(required_str(value, "timestamp")?, "timestamp")?;
    let mut header = object.clone();
    for field in ["number", "hash", "parentHash", "timestamp"] {
        header.remove(field);
    }
    Ok(BlockRow {
        number,
        hash,
        parent_hash,
        timestamp,
        finality,
        header: Value::Object(header),
    })
}

pub(crate) fn parse_sender(value: &Value) -> Result<Option<Address>, ProviderError> {
    if value.is_null() {
        return Ok(None);
    }
    Ok(Some(parse_address(required_str(value, "from")?)?))
}

fn required_str<'a>(value: &'a Value, field: &str) -> Result<&'a str, ProviderError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| ProviderError::fatal(format!("missing {field}")))
}

fn parse_quantity(text: &str, field: &str) -> Result<u64, ProviderError> {
    let hex = text.strip_prefix("0x").unwrap_or(text);
    if hex.is_empty() {
        return Ok(0);
    }
    u64::from_str_radix(hex, 16).map_err(|_| ProviderError::fatal(format!("invalid {field}")))
}

fn parse_u32(text: &str, field: &str) -> Result<u32, ProviderError> {
    u32::try_from(parse_quantity(text, field)?)
        .map_err(|_| ProviderError::fatal(format!("{field} does not fit u32")))
}

fn parse_b256(text: &str, field: &str) -> Result<B256, ProviderError> {
    text.parse()
        .map_err(|_| ProviderError::fatal(format!("invalid {field}")))
}

fn parse_address(text: &str) -> Result<Address, ProviderError> {
    text.parse()
        .map_err(|_| ProviderError::fatal("invalid address"))
}

fn parse_bytes(text: &str) -> Result<Bytes, ProviderError> {
    let hex_text = text.strip_prefix("0x").unwrap_or(text);
    let raw = hex::decode(hex_text).map_err(|_| ProviderError::fatal("invalid data"))?;
    Ok(Bytes::from(raw))
}
