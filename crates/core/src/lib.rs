//! Chain-neutral datalens vocabulary shared across workspace crates.

pub mod chain {
    use serde::{Deserialize, Serialize};

    use crate::{DatalensError, DatalensErrorKind};

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum ChainFamily {
        Evm,
        Other(String),
    }

    impl ChainFamily {
        pub fn try_other(value: impl Into<String>) -> Result<Self, DatalensError> {
            let value = validate_identifier("chain family", value.into())?;
            Ok(Self::Other(value))
        }

        pub fn key(&self) -> &str {
            match self {
                Self::Evm => "evm",
                Self::Other(value) => value,
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
    #[serde(tag = "kind", content = "value", rename_all = "snake_case")]
    pub enum NetworkId {
        Numeric(u64),
        Textual(String),
    }

    impl NetworkId {
        pub fn numeric(value: u64) -> Self {
            Self::Numeric(value)
        }

        pub fn textual(value: impl Into<String>) -> Result<Self, DatalensError> {
            Ok(Self::Textual(validate_identifier(
                "network id",
                value.into(),
            )?))
        }

        pub fn key(&self) -> String {
            match self {
                Self::Numeric(value) => value.to_string(),
                Self::Textual(value) => value.clone(),
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct ChainIdentity {
        family: ChainFamily,
        configured_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        network_id: Option<NetworkId>,
    }

    impl ChainIdentity {
        pub fn new(family: ChainFamily, id: impl Into<String>) -> Self {
            Self::try_new(family, id, None).expect("valid chain identity")
        }

        pub fn with_network_id(
            family: ChainFamily,
            configured_name: impl Into<String>,
            network_id: NetworkId,
        ) -> Self {
            Self::try_new(family, configured_name, Some(network_id)).expect("valid chain identity")
        }

        pub fn try_new(
            family: ChainFamily,
            configured_name: impl Into<String>,
            network_id: Option<NetworkId>,
        ) -> Result<Self, DatalensError> {
            if matches!(&family, ChainFamily::Other(value) if value.trim().is_empty()) {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "chain family must not be empty",
                ));
            }
            Ok(Self {
                family,
                configured_name: validate_identifier(
                    "configured chain name",
                    configured_name.into(),
                )?,
                network_id,
            })
        }

        pub fn family(&self) -> ChainFamily {
            self.family.clone()
        }

        pub fn family_ref(&self) -> &ChainFamily {
            &self.family
        }

        pub fn configured_name(&self) -> &str {
            &self.configured_name
        }

        pub fn network_id(&self) -> Option<&NetworkId> {
            self.network_id.as_ref()
        }

        pub fn id(&self) -> &str {
            &self.configured_name
        }

        pub fn key_prefix(&self) -> String {
            match &self.network_id {
                Some(network_id) => format!(
                    "{}/{}/{}",
                    self.family.key(),
                    self.configured_name,
                    network_id.key()
                ),
                None => format!("{}/{}", self.family.key(), self.configured_name),
            }
        }
    }

    fn validate_identifier(kind: &str, value: String) -> Result<String, DatalensError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("{kind} must not be empty"),
            ));
        }
        if value.contains('/') || value.contains('\\') {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("{kind} must not contain path separators"),
            ));
        }
        Ok(value.to_owned())
    }
}

pub mod range {
    use serde::{Deserialize, Serialize};

    use crate::{DatalensError, DatalensErrorKind};

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct TimeRange {
        start: u64,
        end: u64,
    }

    impl TimeRange {
        pub fn blocks(start: u64, end: u64) -> Self {
            Self { start, end }
        }

        pub fn start(&self) -> u64 {
            self.start
        }

        pub fn end(&self) -> u64 {
            self.end
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct BlockRange {
        pub from_block: u64,
        pub to_block: u64,
    }

    impl BlockRange {
        pub fn new(from_block: u64, to_block: u64) -> Self {
            Self::try_new(from_block, to_block).expect("valid block range")
        }

        pub fn try_new(from_block: u64, to_block: u64) -> Result<Self, DatalensError> {
            if from_block > to_block {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "from_block must be less than or equal to to_block",
                ));
            }
            Ok(Self {
                from_block,
                to_block,
            })
        }

        pub fn len(&self) -> u128 {
            u128::from(self.to_block) - u128::from(self.from_block) + 1
        }

        pub fn is_empty(&self) -> bool {
            false
        }

        pub fn contains(&self, block_number: u64) -> bool {
            self.from_block <= block_number && block_number <= self.to_block
        }

        pub fn overlaps(&self, other: &Self) -> bool {
            self.from_block <= other.to_block && other.from_block <= self.to_block
        }

        pub fn intersection(&self, other: &Self) -> Option<Self> {
            let from_block = self.from_block.max(other.from_block);
            let to_block = self.to_block.min(other.to_block);
            Self::try_new(from_block, to_block).ok()
        }

        pub fn difference(&self, covered: &Self) -> Vec<Self> {
            let Some(overlap) = self.intersection(covered) else {
                return vec![*self];
            };
            let mut ranges = Vec::new();
            if self.from_block < overlap.from_block {
                ranges.push(Self::new(self.from_block, overlap.from_block - 1));
            }
            if overlap.to_block < self.to_block {
                ranges.push(Self::new(overlap.to_block + 1, self.to_block));
            }
            ranges
        }

        pub fn split(&self, max_blocks: u64) -> Result<Vec<Self>, DatalensError> {
            if max_blocks == 0 {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "max_blocks must be greater than zero",
                ));
            }

            let mut ranges = Vec::new();
            let mut from_block = self.from_block;
            loop {
                let offset = max_blocks - 1;
                let chunk_end = from_block.saturating_add(offset);
                let to_block = self.to_block.min(chunk_end);
                ranges.push(Self::new(from_block, to_block));
                if to_block == self.to_block || to_block == u64::MAX {
                    break;
                }
                from_block = to_block + 1;
            }
            Ok(ranges)
        }
    }
}

pub mod dataset {
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct DatasetId(String);

    impl DatasetId {
        pub fn new(id: impl Into<String>) -> Self {
            Self(id.into())
        }

        pub fn as_str(&self) -> &str {
            &self.0
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Dataset {
        Blocks,
        Logs,
    }

    impl Dataset {
        pub fn as_str(&self) -> &'static str {
            match self {
                Self::Blocks => "blocks",
                Self::Logs => "logs",
            }
        }
    }
}

pub mod coverage {
    use serde::{Deserialize, Serialize};

    use crate::{BlockRange, ChainIdentity, Dataset, EvmLogFilter};

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum CoverageLevel {
        Covered,
        Partial,
        Missing,
    }

    pub const COVERAGE_SCHEMA_VERSION: u16 = 1;

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct CoverageKey {
        chain: ChainIdentity,
        dataset: Dataset,
        schema_version: u16,
        coverage: CoverageShape,
    }

    impl CoverageKey {
        pub fn full_blocks(chain: ChainIdentity) -> Self {
            Self {
                chain,
                dataset: Dataset::Blocks,
                schema_version: COVERAGE_SCHEMA_VERSION,
                coverage: CoverageShape::All,
            }
        }

        pub fn evm_logs(chain: ChainIdentity, filter: EvmLogFilter) -> Self {
            Self {
                chain,
                dataset: Dataset::Logs,
                schema_version: COVERAGE_SCHEMA_VERSION,
                coverage: CoverageShape::EvmLogs(filter),
            }
        }

        pub fn chain(&self) -> &ChainIdentity {
            &self.chain
        }

        pub fn dataset(&self) -> Dataset {
            self.dataset
        }

        pub fn schema_version(&self) -> u16 {
            self.schema_version
        }

        pub fn coverage_key(&self) -> String {
            match &self.coverage {
                CoverageShape::All => "all".to_owned(),
                CoverageShape::EvmLogs(filter) => format!("evm-logs/{}", filter.canonical_key()),
            }
        }

        pub fn object_prefix(&self) -> String {
            format!(
                "chains/{}/datasets/{}/v{}/{}",
                self.chain.key_prefix(),
                self.dataset.as_str(),
                self.schema_version,
                self.coverage_key()
            )
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "shape", content = "filter", rename_all = "snake_case")]
    pub enum CoverageShape {
        All,
        EvmLogs(EvmLogFilter),
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum CoverageValue {
        DataObject,
        Empty,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct CoverageRecord {
        key: CoverageKey,
        range: BlockRange,
        row_count: usize,
        object_key: Option<String>,
    }

    impl CoverageRecord {
        pub fn data_object(
            key: CoverageKey,
            range: BlockRange,
            row_count: usize,
            object_key: impl Into<String>,
        ) -> Self {
            Self {
                key,
                range,
                row_count,
                object_key: Some(object_key.into()),
            }
        }

        pub fn empty(key: CoverageKey, range: BlockRange) -> Self {
            Self {
                key,
                range,
                row_count: 0,
                object_key: None,
            }
        }

        pub fn key(&self) -> &CoverageKey {
            &self.key
        }

        pub fn range(&self) -> BlockRange {
            self.range
        }

        pub fn row_count(&self) -> usize {
            self.row_count
        }

        pub fn object_key(&self) -> Option<&str> {
            self.object_key.as_deref()
        }

        pub fn value(&self) -> CoverageValue {
            if self.object_key.is_some() {
                CoverageValue::DataObject
            } else {
                CoverageValue::Empty
            }
        }

        pub fn covers(&self, key: &CoverageKey, range: &BlockRange) -> bool {
            &self.key == key
                && self.range.from_block <= range.from_block
                && range.to_block <= self.range.to_block
        }
    }
}

pub mod error {
    use std::{error::Error, fmt};

    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum DatalensErrorKind {
        InvalidInput,
        InvalidRequest,
        Unsupported,
        UnsupportedDataset,
        UnsupportedFilter,
        ProviderFailure,
        ProviderLimit,
        ProviderTimeout,
        RateLimited,
        Unavailable,
        ProviderUnavailable,
        Persistence,
        StorageFailure,
        StorageReadFailure,
        StorageWriteFailure,
        ManifestUpdateFailure,
        Internal,
    }

    impl DatalensErrorKind {
        pub fn is_retryable(&self) -> bool {
            matches!(
                self,
                Self::ProviderFailure
                    | Self::ProviderTimeout
                    | Self::RateLimited
                    | Self::Unavailable
                    | Self::ProviderUnavailable
                    | Self::StorageFailure
                    | Self::StorageReadFailure
                    | Self::StorageWriteFailure
                    | Self::ManifestUpdateFailure
            )
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct DatalensError {
        pub kind: DatalensErrorKind,
        pub message: String,
    }

    impl DatalensError {
        pub fn new(kind: DatalensErrorKind, message: impl Into<String>) -> Self {
            Self {
                kind,
                message: message.into(),
            }
        }

        pub fn invalid_input(message: impl Into<String>) -> Self {
            Self::new(DatalensErrorKind::InvalidInput, message)
        }

        pub fn unsupported(message: impl Into<String>) -> Self {
            Self::new(DatalensErrorKind::UnsupportedDataset, message)
        }

        pub fn provider_limit(message: impl Into<String>) -> Self {
            Self::new(DatalensErrorKind::ProviderLimit, message)
        }

        pub fn provider_timeout(message: impl Into<String>) -> Self {
            Self::new(DatalensErrorKind::ProviderTimeout, message)
        }

        pub fn rate_limited(message: impl Into<String>) -> Self {
            Self::new(DatalensErrorKind::RateLimited, message)
        }

        pub fn storage_read(message: impl Into<String>) -> Self {
            Self::new(DatalensErrorKind::StorageReadFailure, message)
        }

        pub fn storage_write(message: impl Into<String>) -> Self {
            Self::new(DatalensErrorKind::StorageWriteFailure, message)
        }

        pub fn manifest_update(message: impl Into<String>) -> Self {
            Self::new(DatalensErrorKind::ManifestUpdateFailure, message)
        }

        pub fn internal(message: impl Into<String>) -> Self {
            Self::new(DatalensErrorKind::Internal, message)
        }

        pub fn is_retryable(&self) -> bool {
            self.kind.is_retryable()
        }
    }

    impl fmt::Display for DatalensError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{:?}: {}", self.kind, self.message)
        }
    }

    impl Error for DatalensError {}
}

pub mod result {
    use serde::{Deserialize, Serialize};

    use crate::{DatasetId, TimeRange};

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct ResultEnvelope<T> {
        dataset: DatasetId,
        range: TimeRange,
        payload: T,
    }

    impl<T> ResultEnvelope<T> {
        pub fn ok(dataset: DatasetId, range: TimeRange, payload: T) -> Self {
            Self {
                dataset,
                range,
                payload,
            }
        }

        pub fn dataset(&self) -> &DatasetId {
            &self.dataset
        }

        pub fn range(&self) -> &TimeRange {
            &self.range
        }

        pub fn payload(&self) -> &T {
            &self.payload
        }
    }
}

pub mod query {
    use std::collections::BTreeSet;

    use serde::{Deserialize, Serialize};

    use crate::{BlockRange, ChainIdentity, DatalensError, DatalensErrorKind, Dataset};

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct BlockHeader {
        pub number: u64,
        pub hash: String,
        pub parent_hash: String,
        pub timestamp: u64,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct LogFilter {
        #[serde(default)]
        pub addresses: Vec<String>,
        #[serde(default)]
        pub topics: Vec<Option<Vec<String>>>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct EvmLogFilter {
        addresses: Vec<String>,
        topics: Vec<TopicFilter>,
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

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "kind", content = "values", rename_all = "snake_case")]
    pub enum TopicFilter {
        Wildcard,
        AnyOf(Vec<String>),
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
    pub struct QueryRequest {
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
        Blocks(Vec<BlockHeader>),
        Logs(Vec<LogRecord>),
    }

    impl QueryRows {
        pub fn row_count(&self) -> usize {
            match self {
                Self::Blocks(rows) => rows.len(),
                Self::Logs(rows) => rows.len(),
            }
        }

        pub fn append(&mut self, other: QueryRows) {
            match (self, other) {
                (Self::Blocks(left), Self::Blocks(mut right)) => left.append(&mut right),
                (Self::Logs(left), Self::Logs(mut right)) => left.append(&mut right),
                _ => {}
            }
        }

        pub fn sort(&mut self) {
            match self {
                Self::Blocks(rows) => rows.sort_by_key(|row| row.number),
                Self::Logs(rows) => rows.sort_by_key(|row| (row.block_number, row.log_index)),
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct QueryResponse {
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
}

pub use chain::{ChainFamily, ChainIdentity, NetworkId};
pub use coverage::{CoverageKey, CoverageLevel, CoverageRecord, CoverageShape, CoverageValue};
pub use dataset::{Dataset, DatasetId};
pub use error::{DatalensError, DatalensErrorKind};
pub use query::{
    BlockHeader, CacheSummary, EvmLogFilter, LogFilter, LogRecord, QueryRequest, QueryResponse,
    QueryRows, TopicFilter,
};
pub use range::{BlockRange, TimeRange};
pub use result::ResultEnvelope;

#[cfg(test)]
mod tests {
    use crate::{
        BlockRange, ChainFamily, ChainIdentity, CoverageKey, CoverageRecord, CoverageValue,
        DatalensError, DatalensErrorKind, EvmLogFilter, LogFilter, NetworkId, TopicFilter,
    };

    #[test]
    fn test_chain_identity_validates_configured_name_and_network_id() {
        let identity = ChainIdentity::try_new(
            ChainFamily::Evm,
            "ethereum-mainnet",
            Some(NetworkId::numeric(1)),
        )
        .expect("valid chain identity");

        assert_eq!(identity.family(), ChainFamily::Evm);
        assert_eq!(identity.configured_name(), "ethereum-mainnet");
        assert_eq!(identity.network_id(), Some(&NetworkId::numeric(1)));

        assert!(ChainIdentity::try_new(ChainFamily::Evm, " ", None).is_err());
        assert!(ChainIdentity::try_new(ChainFamily::Other(" ".to_owned()), "chain", None).is_err());
        assert!(NetworkId::textual(" ").is_err());
    }

    #[test]
    fn test_block_range_inclusive_math_handles_edges() {
        let single = BlockRange::try_new(7, 7).expect("single block");
        assert_eq!(single.len(), 1);
        assert!(single.contains(7));

        let range = BlockRange::try_new(10, 14).expect("multi block");
        assert_eq!(range.len(), 5);
        assert_eq!(
            range.intersection(&BlockRange::try_new(12, 20).unwrap()),
            Some(BlockRange::try_new(12, 14).unwrap())
        );
        assert!(range.overlaps(&BlockRange::try_new(14, 20).unwrap()));
        assert!(!range.overlaps(&BlockRange::try_new(15, 20).unwrap()));
        assert_eq!(
            range.difference(&BlockRange::try_new(12, 13).unwrap()),
            vec![
                BlockRange::try_new(10, 11).unwrap(),
                BlockRange::try_new(14, 14).unwrap()
            ]
        );
        assert_eq!(
            range.split(2).expect("split"),
            vec![
                BlockRange::try_new(10, 11).unwrap(),
                BlockRange::try_new(12, 13).unwrap(),
                BlockRange::try_new(14, 14).unwrap()
            ]
        );

        let max = BlockRange::try_new(u64::MAX, u64::MAX).expect("max block");
        assert_eq!(max.len(), 1);
        assert_eq!(max.split(10).unwrap(), vec![max]);
        assert_eq!(
            BlockRange::try_new(0, u64::MAX).unwrap().len(),
            u128::from(u64::MAX) + 1
        );
        assert!(BlockRange::try_new(2, 1).is_err());
        assert!(range.split(0).is_err());
    }

    #[test]
    fn test_evm_log_filter_normalization_is_canonical() {
        let left = LogFilter {
            addresses: vec![
                "0x2222222222222222222222222222222222222222".to_owned(),
                "0x1111111111111111111111111111111111111111".to_owned(),
                "0x1111111111111111111111111111111111111111".to_owned(),
            ],
            topics: vec![
                Some(vec![
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                ]),
                None,
                Some(vec![]),
            ],
        };
        let right = LogFilter {
            addresses: vec![
                "0X1111111111111111111111111111111111111111".to_owned(),
                "0X2222222222222222222222222222222222222222".to_owned(),
            ],
            topics: vec![
                Some(vec![
                    "0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned(),
                    "0XBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".to_owned(),
                ]),
                None,
                Some(vec![]),
            ],
        };

        let normalized_left = EvmLogFilter::try_from(left).expect("valid filter");
        let normalized_right = EvmLogFilter::try_from(right).expect("equivalent filter");

        assert_eq!(normalized_left, normalized_right);
        assert_eq!(
            normalized_left.topics()[1],
            TopicFilter::Wildcard,
            "wildcard slot is preserved"
        );
        assert_eq!(
            normalized_left.topics()[2],
            TopicFilter::AnyOf(Vec::new()),
            "empty alternatives are distinct from wildcard"
        );
        assert!(
            EvmLogFilter::try_from(LogFilter {
                addresses: vec!["0xabc".to_owned()],
                topics: vec![],
            })
            .is_err()
        );
        assert!(
            EvmLogFilter::try_from(LogFilter {
                addresses: vec![],
                topics: vec![Some(vec!["0xabc".to_owned()])],
            })
            .is_err()
        );
    }

    #[test]
    fn test_coverage_key_is_deterministic_for_equivalent_inputs() {
        let chain = ChainIdentity::try_new(
            ChainFamily::Evm,
            "ethereum-mainnet",
            Some(NetworkId::numeric(1)),
        )
        .unwrap();
        let first = EvmLogFilter::try_from(LogFilter {
            addresses: vec![
                "0x2222222222222222222222222222222222222222".to_owned(),
                "0x1111111111111111111111111111111111111111".to_owned(),
            ],
            topics: vec![None],
        })
        .unwrap();
        let second = EvmLogFilter::try_from(LogFilter {
            addresses: vec![
                "0X1111111111111111111111111111111111111111".to_owned(),
                "0X2222222222222222222222222222222222222222".to_owned(),
            ],
            topics: vec![None],
        })
        .unwrap();

        let block_key = CoverageKey::full_blocks(chain.clone());
        let log_key = CoverageKey::evm_logs(chain.clone(), first);
        let equivalent_log_key = CoverageKey::evm_logs(chain.clone(), second);
        let other_log_key = CoverageKey::evm_logs(
            chain,
            EvmLogFilter::try_from(LogFilter {
                addresses: vec!["0x3333333333333333333333333333333333333333".to_owned()],
                topics: vec![None],
            })
            .unwrap(),
        );

        assert_eq!(block_key.coverage_key(), "all");
        assert_eq!(log_key, equivalent_log_key);
        assert_eq!(log_key.coverage_key(), equivalent_log_key.coverage_key());
        assert_ne!(log_key.coverage_key(), other_log_key.coverage_key());
        assert!(log_key.object_prefix().contains("logs/v1/evm-logs/"));
    }

    #[test]
    fn test_empty_coverage_record_is_distinct_from_missing_and_satisfies_same_key_range() {
        let chain =
            ChainIdentity::try_new(ChainFamily::Evm, "darwinia", Some(NetworkId::numeric(46)))
                .unwrap();
        let key = CoverageKey::full_blocks(chain);
        let range = BlockRange::try_new(100, 110).unwrap();
        let record = CoverageRecord::empty(key.clone(), range);

        assert_eq!(record.row_count(), 0);
        assert_eq!(record.object_key(), None);
        assert_eq!(record.value(), CoverageValue::Empty);
        assert!(record.covers(&key, &BlockRange::try_new(102, 103).unwrap()));
        assert!(!record.covers(&key, &BlockRange::try_new(90, 103).unwrap()));

        let other_key = CoverageKey::full_blocks(
            ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
                .unwrap(),
        );
        assert!(!record.covers(&other_key, &BlockRange::try_new(102, 103).unwrap()));
    }

    #[test]
    fn test_error_retryability_and_constructors() {
        assert!(!DatalensError::invalid_input("bad input").is_retryable());
        assert!(!DatalensError::unsupported("unsupported").is_retryable());
        assert!(!DatalensError::provider_limit("too wide").is_retryable());
        assert!(DatalensError::provider_timeout("timeout").is_retryable());
        assert!(DatalensError::rate_limited("rate limited").is_retryable());
        assert!(DatalensError::storage_write("write failed").is_retryable());
        assert!(!DatalensError::internal("broken invariant").is_retryable());

        assert_eq!(
            DatalensError::manifest_update("manifest").kind,
            DatalensErrorKind::ManifestUpdateFailure
        );
    }
}
