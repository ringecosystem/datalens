use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BlockRange, ChainIdentity, DatalensError, DatalensErrorKind, Dataset, DatasetKey};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockHeader {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
    pub timestamp: u64,
}

#[derive(Deserialize)]
struct RawLogRecord {
    block_number: u64,
    block_hash: String,
    transaction_hash: String,
    transaction_index: u64,
    log_index: u64,
    address: String,
    topics: Vec<String>,
    data: String,
    removed: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawLogRecord")]
pub struct LogRecord {
    pub block_number: u64,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub log_index: u64,
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub removed: bool,
}

impl TryFrom<RawLogRecord> for LogRecord {
    type Error = DatalensError;

    fn try_from(raw: RawLogRecord) -> Result<Self, Self::Error> {
        Self::try_new(
            raw.block_number,
            raw.block_hash,
            raw.transaction_hash,
            raw.transaction_index,
            raw.log_index,
            raw.address,
            raw.topics,
            raw.data,
            raw.removed,
        )
    }
}

impl LogRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        block_number: u64,
        block_hash: String,
        transaction_hash: String,
        transaction_index: u64,
        log_index: u64,
        address: impl AsRef<str>,
        topics: Vec<String>,
        data: String,
        removed: bool,
    ) -> Result<Self, DatalensError> {
        validate_hex_data("data", &data)?;
        Ok(Self {
            block_number,
            block_hash,
            transaction_hash,
            transaction_index,
            log_index,
            address: normalize_hex("address", address.as_ref(), 20)?,
            topics: normalize_values("topic", topics, 32)?,
            data,
            removed,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LogFilter {
    #[serde(default)]
    pub addresses: Vec<String>,
    #[serde(default)]
    pub topics: Vec<Option<Vec<String>>>,
}

#[derive(Deserialize)]
struct RawEvmLogFilter {
    #[serde(default)]
    addresses: Vec<String>,
    #[serde(default)]
    topics: Vec<TopicFilter>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawEvmLogFilter")]
pub struct EvmLogFilter {
    addresses: Vec<String>,
    topics: Vec<TopicFilter>,
}

impl TryFrom<RawEvmLogFilter> for EvmLogFilter {
    type Error = DatalensError;

    fn try_from(raw: RawEvmLogFilter) -> Result<Self, Self::Error> {
        Ok(Self {
            addresses: normalize_values("address", raw.addresses, 20)?,
            topics: raw.topics,
        })
    }
}

impl EvmLogFilter {
    pub fn addresses(&self) -> &[String] {
        &self.addresses
    }

    pub fn topics(&self) -> &[TopicFilter] {
        &self.topics
    }

    pub fn canonical_key(&self) -> String {
        let addresses = if self.addresses.is_empty() {
            "addr=*".to_owned()
        } else {
            format!("addr={}", self.addresses.join(","))
        };
        let topics = if self.topics.is_empty() {
            "topics=*".to_owned()
        } else {
            let slots = self
                .topics
                .iter()
                .map(TopicFilter::canonical_key)
                .collect::<Vec<_>>()
                .join(";");
            format!("topics={slots}")
        };
        format!("{addresses}/{topics}")
    }

    pub fn compact_key(&self) -> String {
        format!("addr-topic-{}", stable_digest_prefix(&self.canonical_key()))
    }
}

impl TryFrom<LogFilter> for EvmLogFilter {
    type Error = DatalensError;

    fn try_from(filter: LogFilter) -> Result<Self, Self::Error> {
        let addresses = normalize_values("address", filter.addresses, 20)?;
        let topics = filter
            .topics
            .into_iter()
            .map(|slot| match slot {
                None => Ok(TopicFilter::Wildcard),
                Some(values) => normalize_values("topic", values, 32).map(TopicFilter::AnyOf),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { addresses, topics })
    }
}

impl TryFrom<&LogFilter> for EvmLogFilter {
    type Error = DatalensError;

    fn try_from(filter: &LogFilter) -> Result<Self, Self::Error> {
        filter.clone().try_into()
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", content = "values", rename_all = "snake_case")]
enum RawTopicFilter {
    Wildcard,
    AnyOf(Vec<String>),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "values", rename_all = "snake_case")]
#[serde(try_from = "RawTopicFilter")]
pub enum TopicFilter {
    Wildcard,
    AnyOf(Vec<String>),
}

impl TryFrom<RawTopicFilter> for TopicFilter {
    type Error = DatalensError;

    fn try_from(value: RawTopicFilter) -> Result<Self, Self::Error> {
        match value {
            RawTopicFilter::Wildcard => Ok(Self::Wildcard),
            RawTopicFilter::AnyOf(values) => normalize_values("topic", values, 32).map(Self::AnyOf),
        }
    }
}

impl TopicFilter {
    fn canonical_key(&self) -> String {
        match self {
            Self::Wildcard => "*".to_owned(),
            Self::AnyOf(values) if values.is_empty() => "[]".to_owned(),
            Self::AnyOf(values) => values.join(","),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LegacyEvmQueryRequest {
    pub chain: ChainIdentity,
    pub dataset: Dataset,
    pub range: BlockRange,
    pub filter: Option<LogFilter>,
    #[serde(default)]
    pub include_block: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CacheSummary {
    pub hit_ranges: Vec<BlockRange>,
    pub missing_ranges: Vec<BlockRange>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "dataset", content = "rows", rename_all = "snake_case")]
pub enum QueryRows {
    #[serde(rename = "blocks", alias = "evm_blocks")]
    EvmBlocks(Vec<BlockHeader>),
    #[serde(rename = "logs", alias = "evm_logs")]
    EvmLogs(Vec<LogRecord>),
    TronEvents(Vec<serde_json::Value>),
    SolanaTransactions(Vec<serde_json::Value>),
    SolanaInstructions(Vec<serde_json::Value>),
    OtherJson(Vec<serde_json::Value>),
}

impl QueryRows {
    pub fn dataset_key(&self) -> DatasetKey {
        match self {
            Self::EvmBlocks(_) => DatasetKey::evm_blocks(),
            Self::EvmLogs(_) => DatasetKey::evm_logs(),
            Self::TronEvents(_) => DatasetKey::tron_events(),
            Self::SolanaTransactions(_) => DatasetKey::solana_transactions(),
            Self::SolanaInstructions(_) => DatasetKey::solana_instructions(),
            Self::OtherJson(_) => {
                DatasetKey::try_new(crate::ChainFamily::Other("other".to_owned()), "json").unwrap()
            }
        }
    }

    pub fn row_count(&self) -> usize {
        match self {
            Self::EvmBlocks(rows) => rows.len(),
            Self::EvmLogs(rows) => rows.len(),
            Self::TronEvents(rows) => rows.len(),
            Self::SolanaTransactions(rows) => rows.len(),
            Self::SolanaInstructions(rows) => rows.len(),
            Self::OtherJson(rows) => rows.len(),
        }
    }

    pub fn try_append(&mut self, other: QueryRows) -> Result<(), DatalensError> {
        match (self, other) {
            (Self::EvmBlocks(left), Self::EvmBlocks(mut right)) => {
                left.append(&mut right);
                Ok(())
            }
            (Self::EvmLogs(left), Self::EvmLogs(mut right)) => {
                left.append(&mut right);
                Ok(())
            }
            (Self::TronEvents(left), Self::TronEvents(mut right)) => {
                left.append(&mut right);
                Ok(())
            }
            (Self::SolanaTransactions(left), Self::SolanaTransactions(mut right)) => {
                left.append(&mut right);
                Ok(())
            }
            (Self::SolanaInstructions(left), Self::SolanaInstructions(mut right)) => {
                left.append(&mut right);
                Ok(())
            }
            (Self::OtherJson(left), Self::OtherJson(mut right)) => {
                left.append(&mut right);
                Ok(())
            }
            _ => Err(DatalensError::new(
                DatalensErrorKind::Internal,
                "cannot append rows from a different dataset",
            )),
        }
    }

    pub fn sort(&mut self) {
        match self {
            Self::EvmBlocks(rows) => rows.sort_by_key(|row| row.number),
            Self::EvmLogs(rows) => rows.sort_by_key(|row| (row.block_number, row.log_index)),
            Self::TronEvents(_)
            | Self::SolanaTransactions(_)
            | Self::SolanaInstructions(_)
            | Self::OtherJson(_) => {}
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DatasetRows {
    dataset_key: DatasetKey,
    rows: QueryRows,
}

impl DatasetRows {
    pub fn new(dataset_key: DatasetKey, rows: QueryRows) -> Result<Self, DatalensError> {
        if dataset_key != rows.dataset_key() && !matches!(rows, QueryRows::OtherJson(_)) {
            return Err(DatalensError::new(
                DatalensErrorKind::Internal,
                "dataset rows key does not match typed rows",
            ));
        }
        Ok(Self { dataset_key, rows })
    }

    pub fn dataset_key(&self) -> &DatasetKey {
        &self.dataset_key
    }

    pub fn rows(&self) -> &QueryRows {
        &self.rows
    }

    pub fn into_rows(self) -> QueryRows {
        self.rows
    }

    pub fn row_count(&self) -> usize {
        self.rows.row_count()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LegacyEvmQueryResponse {
    pub chain: ChainIdentity,
    pub range: BlockRange,
    pub cache: CacheSummary,
    pub rows: QueryRows,
}

fn normalize_values(
    kind: &str,
    values: Vec<String>,
    byte_len: usize,
) -> Result<Vec<String>, DatalensError> {
    let mut normalized = BTreeSet::new();
    for value in values {
        normalized.insert(normalize_hex(kind, &value, byte_len)?);
    }
    Ok(normalized.into_iter().collect())
}

fn normalize_hex(kind: &str, value: &str, byte_len: usize) -> Result<String, DatalensError> {
    let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    else {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{kind} must be 0x-prefixed hex"),
        ));
    };
    if hex.len() != byte_len * 2 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{kind} must be {byte_len} bytes"),
        ));
    }
    if !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{kind} must contain only hex digits"),
        ));
    }
    Ok(format!("0x{}", hex.to_ascii_lowercase()))
}

fn validate_hex_data(kind: &str, value: &str) -> Result<(), DatalensError> {
    let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    else {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{kind} must be 0x-prefixed hex"),
        ));
    };
    if hex.len() % 2 != 0 {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{kind} must have an even number of hex digits"),
        ));
    }
    if !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{kind} must contain only hex digits"),
        ));
    }
    Ok(())
}

fn stable_digest_prefix(value: &str) -> String {
    const PREFIX_BYTES: usize = 16;

    let digest = Sha256::digest(value.as_bytes());
    digest[..PREFIX_BYTES]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
