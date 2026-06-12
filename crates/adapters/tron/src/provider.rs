use datalens_core::{DatalensError, DatalensErrorKind, redact_url, redact_urls_in_text};
use reqwest::blocking::Client;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    TronBlock, TronContractEvent, TronContractEventPage, TronContractEventRequest, TronFinality,
    TronProvider, normalize_tron_contract_address,
};

#[derive(Clone, Debug)]
pub struct TronHttpProvider {
    url: String,
    trongrid: Option<TronGridConfig>,
    client: Client,
}

#[derive(Clone, Debug)]
struct TronGridConfig {
    base_url: String,
    api_key: Option<String>,
}

impl TronHttpProvider {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            trongrid: None,
            client: Client::new(),
        }
    }

    pub fn with_trongrid(mut self, base_url: impl Into<String>, api_key: Option<String>) -> Self {
        self.trongrid = Some(TronGridConfig {
            base_url: base_url.into(),
            api_key: api_key.filter(|value| !value.trim().is_empty()),
        });
        self
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}/{}", self.url.trim_end_matches('/'), path)
    }

    fn trongrid_endpoint(&self, base_url: &str, path: &str) -> String {
        format!("{}/{}", base_url.trim_end_matches('/'), path)
    }

    fn post(&self, path: &str, body: Value) -> Result<Value, DatalensError> {
        let endpoint = self.endpoint(path);
        let response = self
            .client
            .post(&endpoint)
            .json(&body)
            .send()
            .map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!(
                        "Tron provider request failed endpoint={}: {}",
                        redact_url(&endpoint),
                        redact_urls_in_text(&error.to_string())
                    ),
                )
            })?;
        let status = response.status();
        let body: Value = response.json().map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!(
                    "decode Tron provider response: {}",
                    redact_urls_in_text(&error.to_string())
                ),
            )
        })?;
        if !status.is_success() {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!("Tron provider HTTP error {}", status.as_u16()),
            ));
        }
        if body.get("Error").or_else(|| body.get("error")).is_some() {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "Tron provider returned an error response",
            ));
        }
        Ok(body)
    }

    fn path(finality: TronFinality, method: &str) -> String {
        let prefix = match finality {
            TronFinality::Latest => "wallet",
            TronFinality::Finalized => "walletsolidity",
        };
        format!("{prefix}/{method}")
    }
}

impl TronProvider for TronHttpProvider {
    fn latest_block(&self, finality: TronFinality) -> Result<TronBlock, DatalensError> {
        let value = self.post(&Self::path(finality, "getnowblock"), json!({}))?;
        parse_block(&value)
    }

    fn get_block_by_number(
        &self,
        number: u64,
        finality: TronFinality,
    ) -> Result<Option<TronBlock>, DatalensError> {
        let value = self.post(
            &Self::path(finality, "getblockbynum"),
            json!({ "num": number }),
        )?;
        if value.as_object().is_some_and(serde_json::Map::is_empty) {
            return Ok(None);
        }
        parse_block(&value).map(Some)
    }

    fn get_transaction_info_by_id(&self, tx_id: &str) -> Result<Option<Value>, DatalensError> {
        let value = self.post(
            &Self::path(TronFinality::Finalized, "gettransactioninfobyid"),
            json!({ "value": tx_id }),
        )?;
        if value.as_object().is_some_and(serde_json::Map::is_empty) {
            return Ok(None);
        }
        Ok(Some(value))
    }

    fn supports_contract_event_query(&self) -> bool {
        self.trongrid
            .as_ref()
            .and_then(|config| config.api_key.as_ref())
            .is_some()
    }

    fn get_contract_events(
        &self,
        request: TronContractEventRequest,
    ) -> Result<TronContractEventPage, DatalensError> {
        let config = self.trongrid.as_ref().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "TronGrid contract events are not configured",
            )
        })?;
        let api_key = config.api_key.as_ref().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::AuthenticationFailed,
                "TronGrid API key is not configured",
            )
        })?;
        if request.range.start() != request.range.end() {
            return Err(DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                "TronGrid contract events provider requires a single block_number query",
            ));
        }
        let contract_address = trongrid_contract_address(&request.contract_address)?;
        let path = format!("v1/contracts/{contract_address}/events");
        let mut query = vec![
            ("block_number".to_owned(), request.range.start().to_string()),
            (
                "only_confirmed".to_owned(),
                request.only_confirmed.to_string(),
            ),
            ("limit".to_owned(), request.limit.to_string()),
        ];
        if let Some(event_name) = &request.event_name {
            query.push(("event_name".to_owned(), event_name.clone()));
        }
        if let Some(fingerprint) = &request.fingerprint {
            query.push(("fingerprint".to_owned(), fingerprint.clone()));
        }
        let url = format!(
            "{}?{}",
            self.trongrid_endpoint(&config.base_url, &path),
            encode_query(&query)
        );
        let response = self
            .client
            .get(&url)
            .header("TRON-PRO-API-KEY", api_key)
            .header(reqwest::header::ACCEPT_ENCODING, "identity")
            .send()
            .map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!(
                        "TronGrid contract events request failed endpoint={}: {}",
                        redact_url(&url),
                        redact_urls_in_text(&error.to_string())
                    ),
                )
            })?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let raw_body = response.text().map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!(
                    "read TronGrid contract events response failed endpoint={}: {}",
                    redact_url(&url),
                    redact_urls_in_text(&error.to_string())
                ),
            )
        })?;
        let body: Value = serde_json::from_str(&raw_body).map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!(
                    "decode TronGrid contract events response failed status={} content_type={} body_prefix={}: {}",
                    status.as_u16(),
                    content_type.as_deref().unwrap_or("<none>"),
                    redact_body_prefix(&raw_body),
                    redact_urls_in_text(&error.to_string())
                ),
            )
        })?;
        if status.as_u16() == 401 || status.as_u16() == 403 {
            return Err(DatalensError::new(
                DatalensErrorKind::AuthenticationFailed,
                format!("TronGrid contract events HTTP error {}", status.as_u16()),
            ));
        }
        if status.as_u16() == 429 {
            return Err(DatalensError::new(
                DatalensErrorKind::RateLimited,
                "TronGrid contract events rate limited",
            ));
        }
        if !status.is_success() {
            return Err(trongrid_contract_events_http_error(status.as_u16(), &body));
        }
        parse_contract_event_page(&body)
    }

    fn provider_name(&self) -> &'static str {
        "tron-http"
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TronFixtureProviderRpc;

impl TronProvider for TronFixtureProviderRpc {
    fn latest_block(&self, finality: TronFinality) -> Result<TronBlock, DatalensError> {
        Ok(match finality {
            TronFinality::Latest => fixture_block(14),
            TronFinality::Finalized => fixture_block(12),
        })
    }

    fn get_block_by_number(
        &self,
        number: u64,
        _finality: TronFinality,
    ) -> Result<Option<TronBlock>, DatalensError> {
        Ok(match number {
            10..=14 => Some(fixture_block(number)),
            _ => None,
        })
    }

    fn get_transaction_info_by_id(&self, tx_id: &str) -> Result<Option<Value>, DatalensError> {
        Ok(match tx_id {
            "tron-tx-10" => Some(json!({
                "id": "tron-tx-10",
                "blockNumber": 10,
                "blockTimeStamp": 1_700_000_010_u64,
                "fee": 1_000_u64,
                "receipt": {
                    "result": "SUCCESS",
                    "energy_usage_total": 12,
                    "net_usage": 345,
                },
                "contractResult": ["00"],
                "log": [{
                    "address": "41abcdefabcdefabcdefabcdefabcdefabcdefabcd",
                    "topics": [
                        "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                        "0000000000000000000000001111111111111111111111111111111111111111",
                        "0000000000000000000000002222222222222222222222222222222222222222"
                    ],
                    "data": "0000000000000000000000000000000000000000000000000000000000000001"
                }],
            })),
            _ => None,
        })
    }

    fn provider_name(&self) -> &'static str {
        "tron-fixture"
    }
}

fn fixture_block(number: u64) -> TronBlock {
    let hash = format!("{number:016x}-tron-hash");
    let parent_hash = format!("{:016x}-tron-hash", number.saturating_sub(1));
    TronBlock {
        number,
        hash: hash.clone(),
        parent_hash: parent_hash.clone(),
        timestamp: 1_700_000_000 + number,
        witness_address: Some("TTronWitness111111111111111111111111111".to_owned()),
        transaction_count: if number == 10 { 1 } else { 0 },
        raw: json!({
            "blockID": hash,
            "block_header": {
                "raw_data": {
                    "number": number,
                    "parentHash": parent_hash,
                    "timestamp": 1_700_000_000 + number,
                    "witness_address": "TTronWitness111111111111111111111111111",
                }
            },
            "transactions": if number == 10 {
                json!([{
                    "txID": "tron-tx-10",
                    "ret": [{ "contractRet": "SUCCESS" }],
                    "raw_data": {
                        "contract": [{
                            "type": "TransferContract",
                            "parameter": {
                                "value": {
                                    "owner_address": "TTronOwner11111111111111111111111111111",
                                    "to_address": "TTronRecipient111111111111111111111111",
                                    "amount": 1
                                }
                            }
                        }]
                    }
                }])
            } else {
                json!([])
            },
        }),
    }
}

fn parse_block(value: &Value) -> Result<TronBlock, DatalensError> {
    let raw_data = value
        .get("block_header")
        .and_then(|header| header.get("raw_data"))
        .ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "Tron block missing block_header.raw_data",
            )
        })?;
    let number = raw_data
        .get("number")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "Tron block missing number",
            )
        })?;
    let hash = string_field(value, "blockID", "Tron block missing blockID")?;
    let parent_hash = string_field(raw_data, "parentHash", "Tron block missing parentHash")?;
    let timestamp = raw_data
        .get("timestamp")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "Tron block missing timestamp",
            )
        })?;
    let witness_address = raw_data
        .get("witness_address")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let transaction_count = value
        .get("transactions")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);

    Ok(TronBlock {
        number,
        hash,
        parent_hash,
        timestamp,
        witness_address,
        transaction_count,
        raw: value.clone(),
    })
}

fn encode_query(query: &[(String, String)]) -> String {
    query
        .iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                encode_query_component(key),
                encode_query_component(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn encode_query_component(value: &str) -> String {
    let mut output = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            output.push(byte as char);
        } else {
            output.push_str(&format!("%{byte:02X}"));
        }
    }
    output
}

fn trongrid_contract_address(address: &str) -> Result<String, DatalensError> {
    let normalized = normalize_tron_contract_address(address)?;
    if normalized.starts_with('T') {
        return Ok(normalized);
    }
    let bytes = decode_hex(&normalized)?;
    Ok(base58check_encode(&bytes))
}

fn decode_hex(value: &str) -> Result<Vec<u8>, DatalensError> {
    if !value.len().is_multiple_of(2) {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "Tron hex address must contain an even number of digits",
        ));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for chunk in value.as_bytes().chunks_exact(2) {
        let hex = std::str::from_utf8(chunk).map_err(|_| {
            DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "Tron hex address must be valid UTF-8",
            )
        })?;
        let byte = u8::from_str_radix(hex, 16).map_err(|_| {
            DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "Tron hex address contains an invalid digit",
            )
        })?;
        bytes.push(byte);
    }
    Ok(bytes)
}

fn base58check_encode(payload: &[u8]) -> String {
    let first = Sha256::digest(payload);
    let second = Sha256::digest(first);
    let mut bytes = Vec::with_capacity(payload.len() + 4);
    bytes.extend_from_slice(payload);
    bytes.extend_from_slice(&second[..4]);
    base58_encode(&bytes)
}

fn base58_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    let mut digits = vec![0_u8];
    for byte in bytes {
        let mut carry = u32::from(*byte);
        for digit in &mut digits {
            let value = u32::from(*digit) * 256 + carry;
            *digit = (value % 58) as u8;
            carry = value / 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }

    let leading_zeroes = bytes.iter().take_while(|byte| **byte == 0).count();
    let mut output = String::with_capacity(leading_zeroes + digits.len());
    for _ in 0..leading_zeroes {
        output.push('1');
    }
    for digit in digits.iter().rev() {
        output.push(ALPHABET[*digit as usize] as char);
    }
    output
}

fn trongrid_contract_events_http_error(status: u16, body: &Value) -> DatalensError {
    let message = body
        .get("error")
        .or_else(|| body.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("TronGrid contract events error");
    let normalized = message.to_ascii_lowercase();
    let kind = if normalized.contains("valid contract address") {
        DatalensErrorKind::InvalidInput
    } else if normalized.contains("page limit") || normalized.contains("limit 100") {
        DatalensErrorKind::ProviderLimit
    } else if status == 429 || normalized.contains("rate limit") {
        DatalensErrorKind::RateLimited
    } else {
        DatalensErrorKind::ProviderFailure
    };
    DatalensError::new(
        kind,
        format!(
            "TronGrid contract events HTTP error {status}: {}",
            redact_urls_in_text(message)
        ),
    )
}

fn redact_body_prefix(body: &str) -> String {
    let prefix: String = body.chars().take(256).collect();
    redact_urls_in_text(&prefix.replace(['\n', '\r'], " "))
}

fn parse_contract_event_page(value: &Value) -> Result<TronContractEventPage, DatalensError> {
    let data = value.get("data").and_then(Value::as_array).ok_or_else(|| {
        DatalensError::new(
            DatalensErrorKind::InvalidRequest,
            "TronGrid contract events response missing data array",
        )
    })?;
    let events = data
        .iter()
        .map(parse_contract_event)
        .collect::<Result<Vec<_>, _>>()?;
    let next_fingerprint = value
        .get("meta")
        .and_then(|meta| meta.get("fingerprint"))
        .and_then(Value::as_str)
        .filter(|fingerprint| !fingerprint.is_empty())
        .map(str::to_owned);
    Ok(TronContractEventPage {
        events,
        next_fingerprint,
        provider_calls: 1,
    })
}

fn parse_contract_event(value: &Value) -> Result<TronContractEvent, DatalensError> {
    let contract_address = value
        .get("contract_address")
        .or_else(|| value.get("contractAddress"))
        .and_then(Value::as_str)
        .map(normalize_tron_contract_address)
        .transpose()?
        .ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::InvalidRequest,
                "TronGrid contract event missing contract_address",
            )
        })?;
    let block_number = value
        .get("block_number")
        .or_else(|| value.get("blockNumber"))
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::InvalidRequest,
                "TronGrid contract event missing block_number",
            )
        })?;
    Ok(TronContractEvent {
        contract_address,
        event_name: value
            .get("event_name")
            .or_else(|| value.get("eventName"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        event_signature: value
            .get("event")
            .or_else(|| value.get("event_signature"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        indexed_fields: value
            .get("result_type")
            .cloned()
            .map(|field| vec![field])
            .unwrap_or_default(),
        non_indexed_fields: value.get("result").cloned().unwrap_or(Value::Null),
        transaction_id: value
            .get("transaction_id")
            .or_else(|| value.get("transaction"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        block_number,
        block_hash: value
            .get("block_hash")
            .or_else(|| value.get("blockHash"))
            .and_then(Value::as_str)
            .map(str::to_owned),
        transaction_index: value
            .get("transaction_index")
            .or_else(|| value.get("transactionIndex"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        event_index: value
            .get("event_index")
            .or_else(|| value.get("eventIndex"))
            .and_then(Value::as_u64)
            .unwrap_or(0),
        confirmed: value
            .get("confirmed")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        raw: value.clone(),
    })
}

fn string_field(value: &Value, name: &str, message: &str) -> Result<String, DatalensError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| DatalensError::new(DatalensErrorKind::ProviderFailure, message))
}
