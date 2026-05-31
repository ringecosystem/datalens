use std::{env, path::PathBuf};

use crate::IndexerError;

use super::{
    ClientConfig, ClientToken, FinalityRequirement, IndexConfig, IndexDataset, SecretString,
};

pub(crate) fn parse_dataset(
    field: &str,
    value: &str,
    errors: &mut Vec<String>,
) -> Option<IndexDataset> {
    match value {
        "evm.logs" => Some(IndexDataset::EvmLogs),
        "solana.transactions" => Some(IndexDataset::SolanaTransactions),
        "solana.instructions" => Some(IndexDataset::SolanaInstructions),
        "solana.account_updates" => Some(IndexDataset::SolanaAccountUpdates),
        "tron.events" => Some(IndexDataset::TronEvents),
        value => {
            errors.push(format!(
                "{field}: unsupported dataset {value}; supported values are evm.logs, solana.transactions, solana.instructions, solana.account_updates, and tron.events"
            ));
            None
        }
    }
}

pub(super) fn optional_non_empty(
    field: &str,
    value: Option<String>,
    errors: &mut Vec<String>,
) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim();
        if value.is_empty() {
            errors.push(format!("{field}: must not be empty"));
            None
        } else {
            Some(value.to_owned())
        }
    })
}

pub(crate) fn required_u64(
    field: &str,
    value: Option<u64>,
    errors: &mut Vec<String>,
) -> Option<u64> {
    match value {
        Some(value) => Some(value),
        None => {
            errors.push(format!("{field}: missing required field"));
            None
        }
    }
}

pub(super) fn required_path(
    field: &str,
    value: Option<PathBuf>,
    errors: &mut Vec<String>,
) -> Option<PathBuf> {
    match value {
        Some(value) if !value.as_os_str().is_empty() => Some(value),
        Some(_) => {
            errors.push(format!("{field}: must not be empty"));
            None
        }
        None => {
            errors.push(format!("{field}: missing required field"));
            None
        }
    }
}

pub(super) fn normalize_application_id(
    field: &str,
    value: &str,
    errors: &mut Vec<String>,
) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.starts_with('.')
        || normalized.ends_with('.')
        || normalized.contains('/')
        || normalized.contains('\\')
        || normalized.len() > 64
        || !normalized.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        errors.push(format!(
            "{field}: application id must be 1-64 characters using lowercase letters, digits, dot, underscore, or hyphen"
        ));
        return None;
    }
    Some(normalized)
}

pub(super) fn validate_graphql_identifier(
    field: &str,
    value: String,
    errors: &mut Vec<String>,
) -> Option<String> {
    let mut bytes = value.bytes();
    let valid_start = bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_');
    let valid_rest = bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    if !valid_start || !valid_rest || value.starts_with("__") {
        errors.push(format!(
            "{field}: must be a GraphQL identifier matching [_A-Za-z][_0-9A-Za-z]* and must not start with __"
        ));
        return None;
    }
    Some(value)
}

pub(super) fn validate_optional_positive_u64(
    field: &str,
    value: Option<u64>,
    errors: &mut Vec<String>,
) {
    if value == Some(0) {
        errors.push(format!("{field}: must be greater than 0"));
    }
}

pub(super) fn validate_optional_positive_usize(
    field: &str,
    value: Option<usize>,
    errors: &mut Vec<String>,
) {
    if value == Some(0) {
        errors.push(format!("{field}: must be greater than 0"));
    }
}

pub(super) fn expand_env_vars(text: &str) -> Result<String, IndexerError> {
    let mut expanded = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        expanded.push_str(&rest[..start]);
        let tail = &rest[start + 2..];
        let Some(end) = tail.find('}') else {
            return Err(IndexerError::Config(
                "unterminated environment variable placeholder".to_owned(),
            ));
        };
        let name = &tail[..end];
        let value = env::var(name).unwrap_or_default();
        expanded.push_str(&value);
        rest = &tail[end + 1..];
    }
    expanded.push_str(rest);
    Ok(expanded)
}

pub(super) fn validate_parquet_partitions(values: &[String], errors: &mut Vec<String>) {
    for (index, value) in values.iter().enumerate() {
        match value.as_str() {
            "index" | "chain_family" | "chain_id" | "chain" | "dataset" => {}
            _ => errors.push(format!(
                "output.parquet.partition_by[{index}]: unsupported partition field {value}; supported values are index, chain_family, chain_id, chain, and dataset"
            )),
        }
    }
}

pub(super) fn validate_parquet_compression(value: Option<&str>, errors: &mut Vec<String>) {
    match value {
        Some("uncompressed" | "snappy" | "zstd") | None => {}
        Some(value) => errors.push(format!(
            "output.parquet.compression: unsupported compression {value}; supported values are uncompressed, snappy, and zstd"
        )),
    }
}

pub(super) fn empty_client() -> ClientConfig {
    ClientConfig {
        endpoint: String::new(),
        application: String::new(),
        token: ClientToken {
            env: String::new(),
            value: SecretString::new(String::new()),
        },
    }
}

pub(super) fn empty_index() -> IndexConfig {
    IndexConfig {
        name: String::new(),
        dataset: IndexDataset::EvmLogs,
        finality: FinalityRequirement::Durable,
        chunk_blocks: 1,
    }
}
