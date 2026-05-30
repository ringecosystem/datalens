use alloy_dyn_abi::{DynSolEvent, DynSolType, DynSolValue, Specifier};
use alloy_json_abi::JsonAbi;
use alloy_primitives::{
    B256,
    hex::{self, FromHex},
};
use serde_json::Value;

use crate::{IndexerError, PlannedDecodeEvent};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedEvmLog {
    pub status: EvmDecodeStatus,
    pub event_name: Option<String>,
    pub signature: Option<String>,
    pub topic0: Option<String>,
    pub arguments: Option<Value>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvmDecodeStatus {
    Decoded,
    UnknownEvent,
    Failed,
}

impl EvmDecodeStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Decoded => "decoded",
            Self::UnknownEvent => "unknown_event",
            Self::Failed => "failed",
        }
    }
}

pub fn decode_evm_log(
    events: &[PlannedDecodeEvent],
    index: &str,
    chain: &str,
    dataset: &str,
    address: &str,
    topics: &[String],
    data: &str,
) -> DecodedEvmLog {
    let topic0 = topics.first().cloned();
    let Some(topic0) = topic0 else {
        return DecodedEvmLog {
            status: EvmDecodeStatus::UnknownEvent,
            event_name: None,
            signature: None,
            topic0: None,
            arguments: None,
            error: Some("log has no topic0".to_owned()),
        };
    };
    let Some(event) = events.iter().find(|event| {
        event.applies_to(index, chain, dataset, address)
            && event.topic0.eq_ignore_ascii_case(&topic0)
    }) else {
        return DecodedEvmLog {
            status: EvmDecodeStatus::UnknownEvent,
            event_name: None,
            signature: None,
            topic0: Some(topic0),
            arguments: None,
            error: Some("no configured ABI event matched topic0".to_owned()),
        };
    };

    match decode_event(event, topics, data) {
        Ok(arguments) => DecodedEvmLog {
            status: EvmDecodeStatus::Decoded,
            event_name: Some(event.name.clone()),
            signature: Some(event.signature.clone()),
            topic0: Some(event.topic0.clone()),
            arguments: Some(arguments),
            error: None,
        },
        Err(error) => DecodedEvmLog {
            status: EvmDecodeStatus::Failed,
            event_name: Some(event.name.clone()),
            signature: Some(event.signature.clone()),
            topic0: Some(event.topic0.clone()),
            arguments: None,
            error: Some(format!("decode failed: {error}")),
        },
    }
}

pub(crate) fn planned_events_from_abi_json(
    scope: DecodeScope,
    json: &str,
) -> Result<Vec<PlannedDecodeEvent>, IndexerError> {
    let abi = serde_json::from_str::<JsonAbi>(json)
        .map_err(|error| IndexerError::Config(format!("decode ABI JSON: {error}")))?;
    let mut events = Vec::new();
    for event in abi.events.values().flatten() {
        if event.anonymous {
            continue;
        }
        events.push(PlannedDecodeEvent {
            name: event.name.clone(),
            signature: event.signature(),
            topic0: format!("{:#x}", event.selector()),
            chain: Some(scope.chain.clone()),
            index: Some(scope.index.clone()),
            dataset: Some(scope.dataset.clone()),
            contract: None,
            inputs: event
                .inputs
                .iter()
                .map(|input| crate::PlannedDecodeEventInput {
                    name: input.name.clone(),
                    kind: input.ty.to_string(),
                    indexed: input.indexed,
                })
                .collect(),
        });
    }
    Ok(events)
}

#[derive(Clone, Debug)]
pub(crate) struct DecodeScope {
    pub chain: String,
    pub index: String,
    pub dataset: String,
}

fn decode_event(
    event: &PlannedDecodeEvent,
    topics: &[String],
    data: &str,
) -> Result<Value, String> {
    let json_event = event.to_alloy_event()?;
    let dyn_event = json_event.resolve().map_err(|error| error.to_string())?;
    let topics = topics
        .iter()
        .map(|topic| parse_topic(topic))
        .collect::<Result<Vec<_>, _>>()?;
    let data = parse_data(data)?;
    let decoded = dyn_event
        .decode_log_parts(topics, &data)
        .map_err(|error| error.to_string())?;
    Ok(decoded_arguments(event, &decoded.indexed, &decoded.body))
}

fn decoded_arguments(
    event: &PlannedDecodeEvent,
    indexed_values: &[DynSolValue],
    body_values: &[DynSolValue],
) -> Value {
    let mut object = serde_json::Map::new();
    let mut indexed = indexed_values.iter();
    let mut body = body_values.iter();
    for (position, input) in event.inputs.iter().enumerate() {
        let value = if input.indexed {
            indexed.next()
        } else {
            body.next()
        };
        let key = if input.name.is_empty() {
            format!("arg{position}")
        } else {
            input.name.clone()
        };
        if let Some(value) = value {
            object.insert(key, sol_value_to_json(value));
        }
    }
    Value::Object(object)
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

fn parse_topic(value: &str) -> Result<B256, String> {
    value
        .parse::<B256>()
        .map_err(|error| format!("invalid topic: {error}"))
}

fn parse_data(value: &str) -> Result<Vec<u8>, String> {
    let hex = value
        .strip_prefix("0x")
        .ok_or_else(|| "data must start with 0x".to_owned())?;
    Vec::from_hex(hex).map_err(|error| format!("invalid data: {error}"))
}

trait PlannedDecodeEventExt {
    fn applies_to(&self, index: &str, chain: &str, dataset: &str, address: &str) -> bool;
    fn to_alloy_event(&self) -> Result<alloy_json_abi::Event, String>;
}

impl PlannedDecodeEventExt for PlannedDecodeEvent {
    fn applies_to(&self, index: &str, chain: &str, dataset: &str, address: &str) -> bool {
        optional_matches(self.index.as_deref(), index)
            && optional_matches(self.chain.as_deref(), chain)
            && optional_matches(self.dataset.as_deref(), dataset)
            && self
                .contract
                .as_deref()
                .is_none_or(|value| value.eq_ignore_ascii_case(address))
    }

    fn to_alloy_event(&self) -> Result<alloy_json_abi::Event, String> {
        let mut inputs = Vec::new();
        for input in &self.inputs {
            inputs.push(alloy_json_abi::EventParam {
                name: input.name.clone(),
                ty: input.kind.clone(),
                indexed: input.indexed,
                components: Vec::new(),
                internal_type: None,
            });
        }
        let event = alloy_json_abi::Event {
            name: self.name.clone(),
            inputs,
            anonymous: false,
        };
        DynSolEvent::new(
            Some(parse_topic(&self.topic0)?),
            event
                .inputs
                .iter()
                .filter(|input| input.indexed)
                .map(|input| {
                    input
                        .ty
                        .parse::<DynSolType>()
                        .map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, _>>()?,
            DynSolType::Tuple(
                event
                    .inputs
                    .iter()
                    .filter(|input| !input.indexed)
                    .map(|input| {
                        input
                            .ty
                            .parse::<DynSolType>()
                            .map_err(|error| error.to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        )
        .ok_or_else(|| "invalid event ABI".to_owned())?;
        Ok(event)
    }
}

fn optional_matches(scope: Option<&str>, value: &str) -> bool {
    scope.is_none_or(|scope| scope == value)
}
