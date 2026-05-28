use datalens_core::{DatalensError, DatalensErrorKind};
use reqwest::blocking::Client;
use serde_json::{Value, json};

use crate::{
    SolanaBlock, SolanaCommitment, SolanaInnerInstructionGroup, SolanaInstruction, SolanaRpc,
    SolanaTransaction,
};

#[derive(Clone, Debug)]
pub struct SolanaHttpRpc {
    url: String,
    client: Client,
}

impl SolanaHttpRpc {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            client: Client::new(),
        }
    }

    fn call(&self, method: &str, params: Value) -> Result<Value, DatalensError> {
        let response = self
            .client
            .post(&self.url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))
            .send()
            .map_err(|error| {
                DatalensError::new(
                    DatalensErrorKind::ProviderFailure,
                    format!("Solana provider request failed: {error}"),
                )
            })?;
        let status = response.status();
        let body: Value = response.json().map_err(|error| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!("decode Solana JSON-RPC response: {error}"),
            )
        })?;
        if !status.is_success() {
            return Err(DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!("Solana provider HTTP error {}", status.as_u16()),
            ));
        }
        if let Some(error) = body.get("error") {
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("Solana provider error");
            let lower = message.to_ascii_lowercase();
            let kind = if lower.contains("rate") {
                DatalensErrorKind::RateLimited
            } else if lower.contains("limit") || lower.contains("too many") {
                DatalensErrorKind::ProviderLimit
            } else {
                DatalensErrorKind::ProviderFailure
            };
            return Err(DatalensError::new(kind, message));
        }
        body.get("result").cloned().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "Solana JSON-RPC response missing result",
            )
        })
    }
}

impl SolanaRpc for SolanaHttpRpc {
    fn get_slot(&self, commitment: SolanaCommitment) -> Result<u64, DatalensError> {
        self.call("getSlot", json!([{ "commitment": commitment.as_str() }]))?
            .as_u64()
            .ok_or_else(|| {
                DatalensError::new(DatalensErrorKind::ProviderFailure, "invalid getSlot result")
            })
    }

    fn get_blocks_with_limit(
        &self,
        start_slot: u64,
        limit: u64,
        commitment: SolanaCommitment,
    ) -> Result<Vec<u64>, DatalensError> {
        let result = self.call(
            "getBlocksWithLimit",
            json!([start_slot, limit, { "commitment": commitment.as_str() }]),
        )?;
        let slots = result.as_array().ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                "invalid getBlocksWithLimit result",
            )
        })?;
        slots
            .iter()
            .map(|slot| {
                slot.as_u64().ok_or_else(|| {
                    DatalensError::new(
                        DatalensErrorKind::ProviderFailure,
                        "invalid getBlocksWithLimit slot",
                    )
                })
            })
            .collect()
    }

    fn get_block(
        &self,
        slot: u64,
        commitment: SolanaCommitment,
    ) -> Result<Option<SolanaBlock>, DatalensError> {
        let result = self.call(
            "getBlock",
            json!([slot, {
                "commitment": commitment.as_str(),
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "transactionDetails": "full"
            }]),
        )?;
        if result.is_null() {
            return Ok(None);
        }
        parse_block(slot, &result).map(Some)
    }

    fn provider_name(&self) -> &'static str {
        "solana-rpc"
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SolanaFixtureRpc;

impl SolanaRpc for SolanaFixtureRpc {
    fn get_slot(&self, commitment: SolanaCommitment) -> Result<u64, DatalensError> {
        Ok(match commitment {
            SolanaCommitment::Processed | SolanaCommitment::Confirmed => 14,
            SolanaCommitment::Finalized => 12,
        })
    }

    fn get_blocks_with_limit(
        &self,
        start_slot: u64,
        limit: u64,
        _commitment: SolanaCommitment,
    ) -> Result<Vec<u64>, DatalensError> {
        Ok([10, 12, 14]
            .into_iter()
            .filter(|slot| *slot >= start_slot && *slot < start_slot.saturating_add(limit))
            .collect())
    }

    fn get_block(
        &self,
        slot: u64,
        commitment: SolanaCommitment,
    ) -> Result<Option<SolanaBlock>, DatalensError> {
        Ok(fixture_block(slot, commitment))
    }

    fn provider_name(&self) -> &'static str {
        "solana-fixture"
    }
}

fn fixture_block(slot: u64, commitment: SolanaCommitment) -> Option<SolanaBlock> {
    let suffix = if slot == 14 && commitment != SolanaCommitment::Finalized {
        "latest"
    } else {
        "hash"
    };
    match slot {
        10 => Some(SolanaBlock {
            slot,
            block_height: Some(1_000),
            blockhash: "slot-10-hash".to_owned(),
            previous_blockhash: "slot-9-hash".to_owned(),
            parent_slot: 9,
            block_time: Some(1_700_000_010),
            transactions: vec![fixture_transaction(
                "sig-slot-10",
                vec![
                    "Account111111111111111111111111111111111".to_owned(),
                    "program1111111111111111111111111111111111".to_owned(),
                ],
                "program1111111111111111111111111111111111",
            )],
            raw: json!({ "fixture_slot": slot }),
        }),
        12 => Some(SolanaBlock {
            slot,
            block_height: Some(1_001),
            blockhash: "slot-12-hash".to_owned(),
            previous_blockhash: "slot-10-hash".to_owned(),
            parent_slot: 10,
            block_time: Some(1_700_000_012),
            transactions: vec![fixture_transaction(
                "sig-slot-12",
                vec!["Other11111111111111111111111111111111111".to_owned()],
                "otherprogram111111111111111111111111111111",
            )],
            raw: json!({ "fixture_slot": slot }),
        }),
        14 => Some(SolanaBlock {
            slot,
            block_height: Some(1_002),
            blockhash: format!("slot-14-{suffix}"),
            previous_blockhash: "slot-12-hash".to_owned(),
            parent_slot: 12,
            block_time: Some(1_700_000_014),
            transactions: Vec::new(),
            raw: json!({ "fixture_slot": slot }),
        }),
        _ => None,
    }
}

fn fixture_transaction(
    signature: &str,
    account_keys: Vec<String>,
    program_id: &str,
) -> SolanaTransaction {
    SolanaTransaction {
        signature: signature.to_owned(),
        fee: 5_000,
        err: None,
        account_keys,
        loaded_addresses: Vec::new(),
        instructions: vec![SolanaInstruction {
            program_id: program_id.to_owned(),
            accounts: vec!["Account111111111111111111111111111111111".to_owned()],
            data: Some("3Bxs".to_owned()),
            parsed: None,
        }],
        inner_instructions: vec![SolanaInnerInstructionGroup {
            index: 0,
            instructions: vec![SolanaInstruction {
                program_id: program_id.to_owned(),
                accounts: vec!["Account111111111111111111111111111111111".to_owned()],
                data: Some("inner".to_owned()),
                parsed: None,
            }],
        }],
        raw: json!({ "fixture": signature }),
    }
}

fn parse_block(slot: u64, value: &Value) -> Result<SolanaBlock, DatalensError> {
    let transactions = value
        .get("transactions")
        .and_then(Value::as_array)
        .map(|transactions| {
            transactions
                .iter()
                .map(parse_transaction)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    Ok(SolanaBlock {
        slot,
        block_height: value.get("blockHeight").and_then(Value::as_u64),
        blockhash: string_field(value, "blockhash")?,
        previous_blockhash: string_field(value, "previousBlockhash")?,
        parent_slot: value
            .get("parentSlot")
            .and_then(Value::as_u64)
            .ok_or_else(|| {
                DatalensError::new(DatalensErrorKind::ProviderFailure, "missing parentSlot")
            })?,
        block_time: value.get("blockTime").and_then(Value::as_u64),
        transactions,
        raw: value.clone(),
    })
}

fn parse_transaction(value: &Value) -> Result<SolanaTransaction, DatalensError> {
    let transaction = value.get("transaction").unwrap_or(value);
    let meta = value.get("meta").unwrap_or(&Value::Null);
    let message = transaction.get("message").unwrap_or(transaction);
    let signatures = transaction
        .get("signatures")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            DatalensError::new(DatalensErrorKind::ProviderFailure, "missing signatures")
        })?;
    let signature = signatures
        .first()
        .and_then(Value::as_str)
        .ok_or_else(|| DatalensError::new(DatalensErrorKind::ProviderFailure, "missing signature"))?
        .to_owned();
    let account_keys = message
        .get("accountKeys")
        .and_then(Value::as_array)
        .map(|keys| keys.iter().filter_map(account_key_value).collect())
        .unwrap_or_default();
    let instructions = message
        .get("instructions")
        .and_then(Value::as_array)
        .map(|instructions| instructions.iter().map(parse_instruction).collect())
        .transpose()?
        .unwrap_or_default();
    let inner_instructions = meta
        .get("innerInstructions")
        .and_then(Value::as_array)
        .map(|groups| {
            groups
                .iter()
                .map(parse_inner_instruction_group)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    let loaded_addresses = meta
        .get("loadedAddresses")
        .map(loaded_addresses)
        .unwrap_or_default();
    Ok(SolanaTransaction {
        signature,
        fee: meta.get("fee").and_then(Value::as_u64).unwrap_or_default(),
        err: meta.get("err").filter(|value| !value.is_null()).cloned(),
        account_keys,
        loaded_addresses,
        instructions,
        inner_instructions,
        raw: value.clone(),
    })
}

fn parse_inner_instruction_group(
    value: &Value,
) -> Result<SolanaInnerInstructionGroup, DatalensError> {
    let index = value
        .get("index")
        .and_then(Value::as_u64)
        .unwrap_or_default() as usize;
    let instructions = value
        .get("instructions")
        .and_then(Value::as_array)
        .map(|instructions| instructions.iter().map(parse_instruction).collect())
        .transpose()?
        .unwrap_or_default();
    Ok(SolanaInnerInstructionGroup {
        index,
        instructions,
    })
}

fn loaded_addresses(value: &Value) -> Vec<String> {
    ["writable", "readonly"]
        .into_iter()
        .filter_map(|field| value.get(field).and_then(Value::as_array))
        .flat_map(|addresses| addresses.iter())
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn parse_instruction(value: &Value) -> Result<SolanaInstruction, DatalensError> {
    Ok(SolanaInstruction {
        program_id: value
            .get("programId")
            .or_else(|| value.get("program_id"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        accounts: value
            .get("accounts")
            .and_then(Value::as_array)
            .map(|accounts| {
                accounts
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
        data: value.get("data").and_then(Value::as_str).map(str::to_owned),
        parsed: value.get("parsed").cloned(),
    })
}

fn account_key_value(value: &Value) -> Option<String> {
    value.as_str().map(str::to_owned).or_else(|| {
        value
            .get("pubkey")
            .and_then(Value::as_str)
            .map(str::to_owned)
    })
}

fn string_field(value: &Value, field: &str) -> Result<String, DatalensError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::ProviderFailure,
                format!("missing or invalid {field}"),
            )
        })
}
