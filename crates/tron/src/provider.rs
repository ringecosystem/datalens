use datalens_core::{DatalensError, DatalensErrorKind};
use reqwest::blocking::Client;
use serde_json::{Value, json};

use crate::{TronBlock, TronFinality, TronProvider};

#[derive(Clone, Debug)]
pub struct TronHttpProvider {
    url: String,
    client: Client,
}

impl TronHttpProvider {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            client: Client::new(),
        }
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}/{}", self.url.trim_end_matches('/'), path)
    }

    fn post(&self, path: &str, body: Value) -> Result<Value, DatalensError> {
        let response = self
            .client
            .post(self.endpoint(path))
            .json(&body)
            .send()
            .map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!("Tron provider request failed: {error}"),
                )
            })?;
        let status = response.status();
        let body: Value = response.json().map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!("decode Tron provider response: {error}"),
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

fn string_field(value: &Value, name: &str, message: &str) -> Result<String, DatalensError> {
    value
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| DatalensError::new(DatalensErrorKind::ProviderFailure, message))
}
