use sha2::{Digest, Sha256};

use crate::{ClientError, QuerySelector};

const SOLANA_ALL_KIND: &str = "solana_all";
const SOLANA_ADDRESS_KIND: &str = "solana_address";
const SOLANA_PROGRAM_KIND: &str = "solana_program";
const SOLANA_SIGNATURE_KIND: &str = "solana_signature";
const TRON_ALL_KIND: &str = "tron_all";
const TRON_EVENTS_KIND: &str = "tron_events";

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TronEventSelector {
    pub contract_addresses: Vec<String>,
    pub event_names: Vec<String>,
}

impl QuerySelector {
    pub fn solana_all() -> Self {
        Self::other(SOLANA_ALL_KIND, "solana-all/all", "all")
    }

    pub fn solana_address(address: &str) -> Result<Self, ClientError> {
        let address = normalize_solana_key("address", address)?;
        Ok(Self::other(
            SOLANA_ADDRESS_KIND,
            format!("solana-address/{}", digest_prefix(&address, 8)),
            format!("address/{address}"),
        ))
    }

    pub fn solana_account(address: &str) -> Result<Self, ClientError> {
        Self::solana_address(address)
    }

    pub fn solana_program(program_id: &str) -> Result<Self, ClientError> {
        let program_id = normalize_solana_key("program id", program_id)?;
        Ok(Self::other(
            SOLANA_PROGRAM_KIND,
            format!("solana-program/{}", digest_prefix(&program_id, 8)),
            format!("program/{program_id}"),
        ))
    }

    pub fn solana_signature(signature: &str) -> Result<Self, ClientError> {
        let signature = normalize_solana_key("signature", signature)?;
        Ok(Self::other(
            SOLANA_SIGNATURE_KIND,
            format!("solana-signature/{}", digest_prefix(&signature, 8)),
            format!("signature/{signature}"),
        ))
    }

    pub fn tron_all() -> Self {
        Self::other(TRON_ALL_KIND, "tron-all/all", "all")
    }

    pub fn tron_contract(address: impl AsRef<str>) -> Result<Self, ClientError> {
        Self::tron_event(TronEventSelector {
            contract_addresses: vec![address.as_ref().to_owned()],
            event_names: Vec::new(),
        })
    }

    pub fn tron_contract_event(
        address: impl AsRef<str>,
        event_name: impl AsRef<str>,
    ) -> Result<Self, ClientError> {
        Self::tron_event(TronEventSelector {
            contract_addresses: vec![address.as_ref().to_owned()],
            event_names: vec![event_name.as_ref().to_owned()],
        })
    }

    pub fn tron_event(filter: TronEventSelector) -> Result<Self, ClientError> {
        let mut contract_addresses = filter
            .contract_addresses
            .iter()
            .map(|address| normalize_tron_contract_address(address))
            .collect::<Result<Vec<_>, _>>()?;
        contract_addresses.sort();
        contract_addresses.dedup();
        if contract_addresses.is_empty() {
            return Err(ClientError::InvalidInput(
                "Tron event selector requires at least one contract address".to_owned(),
            ));
        }

        let mut event_names = filter
            .event_names
            .iter()
            .map(|name| normalize_tron_event_name(name))
            .collect::<Result<Vec<_>, _>>()?;
        event_names.sort();
        event_names.dedup();

        let canonical_key = format!(
            "contracts/{}/events/{}",
            contract_addresses.join("+"),
            if event_names.is_empty() {
                "all".to_owned()
            } else {
                event_names.join("+")
            }
        );
        Ok(Self::other(
            TRON_EVENTS_KIND,
            format!("tron-events/{}", digest_prefix(&canonical_key, 12)),
            canonical_key,
        ))
    }
}

fn normalize_solana_key(kind: &str, value: &str) -> Result<String, ClientError> {
    let value = value.trim();
    if value.is_empty()
        || value.contains('/')
        || value.contains('\\')
        || !value.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(ClientError::InvalidInput(format!(
            "Solana {kind} must be a non-empty base58-like key"
        )));
    }
    Ok(value.to_owned())
}

fn normalize_tron_contract_address(address: &str) -> Result<String, ClientError> {
    let address = address.trim();
    let hex = address.strip_prefix("0x").unwrap_or(address);
    if hex.len() == 40 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(format!("41{}", hex.to_ascii_lowercase()));
    }
    if hex.len() == 42 && hex.starts_with("41") && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Ok(hex.to_ascii_lowercase());
    }
    if address.len() == 34
        && address.starts_with('T')
        && address.bytes().all(|byte| byte.is_ascii_alphanumeric())
    {
        return Ok(address.to_owned());
    }
    Err(ClientError::InvalidInput(
        "Tron contract address must be 20-byte hex, 41-prefixed hex, or base58".to_owned(),
    ))
}

fn normalize_tron_event_name(name: &str) -> Result<String, ClientError> {
    let name = name.trim();
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(ClientError::InvalidInput(
            "Tron event name must contain only letters, numbers, or underscores".to_owned(),
        ));
    }
    Ok(name.to_owned())
}

fn digest_prefix(value: &str, len: usize) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest
        .iter()
        .take(len)
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
