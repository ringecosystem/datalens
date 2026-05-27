//! Chain-neutral datalens vocabulary shared across workspace crates.

pub mod chain {
    use serde::{Deserialize, Serialize};

    use crate::{DatalensError, DatalensErrorKind};

    #[derive(Deserialize)]
    enum RawChainFamily {
        Evm,
        Other(String),
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(try_from = "RawChainFamily")]
    pub enum ChainFamily {
        Evm,
        Other(String),
    }

    impl TryFrom<RawChainFamily> for ChainFamily {
        type Error = DatalensError;

        fn try_from(value: RawChainFamily) -> Result<Self, Self::Error> {
            match value {
                RawChainFamily::Evm => Ok(Self::Evm),
                RawChainFamily::Other(value) => Self::try_other(value),
            }
        }
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

    #[derive(Deserialize)]
    #[serde(tag = "kind", content = "value", rename_all = "snake_case")]
    enum RawNetworkId {
        Numeric(u64),
        Textual(String),
    }

    #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
    #[serde(tag = "kind", content = "value", rename_all = "snake_case")]
    #[serde(try_from = "RawNetworkId")]
    pub enum NetworkId {
        Numeric(u64),
        Textual(String),
    }

    impl TryFrom<RawNetworkId> for NetworkId {
        type Error = DatalensError;

        fn try_from(value: RawNetworkId) -> Result<Self, Self::Error> {
            match value {
                RawNetworkId::Numeric(value) => Ok(Self::Numeric(value)),
                RawNetworkId::Textual(value) => Self::textual(value),
            }
        }
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

    #[derive(Deserialize)]
    struct RawChainIdentity {
        family: ChainFamily,
        configured_name: String,
        #[serde(default)]
        network_id: Option<NetworkId>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(try_from = "RawChainIdentity")]
    pub struct ChainIdentity {
        family: ChainFamily,
        configured_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        network_id: Option<NetworkId>,
    }

    impl TryFrom<RawChainIdentity> for ChainIdentity {
        type Error = DatalensError;

        fn try_from(raw: RawChainIdentity) -> Result<Self, Self::Error> {
            Self::try_new(raw.family, raw.configured_name, raw.network_id)
        }
    }

    impl ChainIdentity {
        pub fn expect_new(family: ChainFamily, id: impl Into<String>) -> Self {
            Self::try_new(family, id, None).expect("valid chain identity")
        }

        pub fn expect_with_network_id(
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

    pub(crate) fn validate_identifier(kind: &str, value: String) -> Result<String, DatalensError> {
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

    #[derive(Deserialize)]
    struct RawTimeRange {
        start: u64,
        end: u64,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(try_from = "RawTimeRange")]
    pub struct TimeRange {
        start: u64,
        end: u64,
    }

    impl TryFrom<RawTimeRange> for TimeRange {
        type Error = DatalensError;

        fn try_from(raw: RawTimeRange) -> Result<Self, Self::Error> {
            Self::try_blocks(raw.start, raw.end)
        }
    }

    impl TimeRange {
        pub fn expect_blocks(start: u64, end: u64) -> Self {
            Self::try_blocks(start, end).expect("valid time range")
        }

        pub fn try_blocks(start: u64, end: u64) -> Result<Self, DatalensError> {
            if start > end {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "time range start must be less than or equal to end",
                ));
            }
            Ok(Self { start, end })
        }

        pub fn start(&self) -> u64 {
            self.start
        }

        pub fn end(&self) -> u64 {
            self.end
        }
    }

    #[derive(Deserialize)]
    struct RawBlockRange {
        from_block: u64,
        to_block: u64,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(try_from = "RawBlockRange")]
    pub struct BlockRange {
        pub from_block: u64,
        pub to_block: u64,
    }

    impl TryFrom<RawBlockRange> for BlockRange {
        type Error = DatalensError;

        fn try_from(raw: RawBlockRange) -> Result<Self, Self::Error> {
            Self::try_new(raw.from_block, raw.to_block)
        }
    }

    impl BlockRange {
        pub fn expect_new(from_block: u64, to_block: u64) -> Self {
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
                ranges.push(Self::expect_new(self.from_block, overlap.from_block - 1));
            }
            if overlap.to_block < self.to_block {
                ranges.push(Self::expect_new(overlap.to_block + 1, self.to_block));
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
                ranges.push(Self::expect_new(from_block, to_block));
                if to_block == self.to_block || to_block == u64::MAX {
                    break;
                }
                from_block = to_block + 1;
            }
            Ok(ranges)
        }
    }

    #[derive(Deserialize)]
    #[serde(tag = "kind", content = "value", rename_all = "snake_case")]
    enum RawLedgerRangeKind {
        Block,
        Slot,
        Height,
        Other(String),
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(tag = "kind", content = "value", rename_all = "snake_case")]
    #[serde(try_from = "RawLedgerRangeKind")]
    pub enum LedgerRangeKind {
        Block,
        Slot,
        Height,
        Other(String),
    }

    impl TryFrom<RawLedgerRangeKind> for LedgerRangeKind {
        type Error = DatalensError;

        fn try_from(value: RawLedgerRangeKind) -> Result<Self, Self::Error> {
            match value {
                RawLedgerRangeKind::Block => Ok(Self::Block),
                RawLedgerRangeKind::Slot => Ok(Self::Slot),
                RawLedgerRangeKind::Height => Ok(Self::Height),
                RawLedgerRangeKind::Other(value) => Ok(Self::Other(
                    crate::chain::validate_identifier("ledger range kind", value)?,
                )),
            }
        }
    }

    #[derive(Deserialize)]
    struct RawLedgerRange {
        kind: LedgerRangeKind,
        start: u64,
        end: u64,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(try_from = "RawLedgerRange")]
    pub struct LedgerRange {
        kind: LedgerRangeKind,
        start: u64,
        end: u64,
    }

    impl TryFrom<RawLedgerRange> for LedgerRange {
        type Error = DatalensError;

        fn try_from(raw: RawLedgerRange) -> Result<Self, Self::Error> {
            Self::try_new(raw.kind, raw.start, raw.end)
        }
    }

    impl LedgerRange {
        pub fn try_new(kind: LedgerRangeKind, start: u64, end: u64) -> Result<Self, DatalensError> {
            let kind = match kind {
                LedgerRangeKind::Other(value) => LedgerRangeKind::Other(
                    crate::chain::validate_identifier("ledger range kind", value)?,
                ),
                kind => kind,
            };
            if start > end {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "ledger range start must be less than or equal to end",
                ));
            }
            Ok(Self { kind, start, end })
        }

        pub fn blocks(start: u64, end: u64) -> Result<Self, DatalensError> {
            Self::try_new(LedgerRangeKind::Block, start, end)
        }

        pub fn slots(start: u64, end: u64) -> Result<Self, DatalensError> {
            Self::try_new(LedgerRangeKind::Slot, start, end)
        }

        pub fn heights(start: u64, end: u64) -> Result<Self, DatalensError> {
            Self::try_new(LedgerRangeKind::Height, start, end)
        }

        pub fn from_block_range(range: BlockRange) -> Self {
            Self {
                kind: LedgerRangeKind::Block,
                start: range.from_block,
                end: range.to_block,
            }
        }

        pub fn block_range(&self) -> Option<BlockRange> {
            if self.kind == LedgerRangeKind::Block {
                Some(BlockRange::expect_new(self.start, self.end))
            } else {
                None
            }
        }

        pub fn kind(&self) -> LedgerRangeKind {
            self.kind.clone()
        }

        pub fn start(&self) -> u64 {
            self.start
        }

        pub fn end(&self) -> u64 {
            self.end
        }

        pub fn len(&self) -> u128 {
            u128::from(self.end) - u128::from(self.start) + 1
        }

        pub fn is_empty(&self) -> bool {
            false
        }

        pub fn contains(&self, position: u64) -> bool {
            self.start <= position && position <= self.end
        }

        pub fn overlaps(&self, other: &Self) -> bool {
            self.kind == other.kind && self.start <= other.end && other.start <= self.end
        }

        pub fn intersection(&self, other: &Self) -> Option<Self> {
            if self.kind != other.kind {
                return None;
            }
            let start = self.start.max(other.start);
            let end = self.end.min(other.end);
            Self::try_new(self.kind.clone(), start, end).ok()
        }

        pub fn difference(&self, covered: &Self) -> Vec<Self> {
            let Some(overlap) = self.intersection(covered) else {
                return vec![self.clone()];
            };
            let mut ranges = Vec::new();
            if self.start < overlap.start {
                ranges
                    .push(Self::try_new(self.kind.clone(), self.start, overlap.start - 1).unwrap());
            }
            if overlap.end < self.end {
                ranges.push(Self::try_new(self.kind.clone(), overlap.end + 1, self.end).unwrap());
            }
            ranges
        }

        pub fn split(&self, max_len: u64) -> Result<Vec<Self>, DatalensError> {
            if max_len == 0 {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "max_len must be greater than zero",
                ));
            }

            let mut ranges = Vec::new();
            let mut start = self.start;
            loop {
                let chunk_end = start.saturating_add(max_len - 1);
                let end = self.end.min(chunk_end);
                ranges.push(Self::try_new(self.kind.clone(), start, end).unwrap());
                if end == self.end || end == u64::MAX {
                    break;
                }
                start = end + 1;
            }
            Ok(ranges)
        }
    }
}

pub mod dataset {
    use serde::{Deserialize, Serialize};

    use crate::{ChainFamily, DatalensError, chain::validate_identifier};

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(try_from = "String")]
    pub struct DatasetId(String);

    impl TryFrom<String> for DatasetId {
        type Error = DatalensError;

        fn try_from(value: String) -> Result<Self, Self::Error> {
            Self::try_new(value)
        }
    }

    impl DatasetId {
        pub fn expect_new(id: impl Into<String>) -> Self {
            Self::try_new(id).expect("valid dataset id")
        }

        pub fn try_new(id: impl Into<String>) -> Result<Self, DatalensError> {
            Ok(Self(validate_identifier("dataset id", id.into())?))
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

    #[derive(Deserialize)]
    struct RawDatasetKey {
        family: ChainFamily,
        name: DatasetId,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(try_from = "RawDatasetKey")]
    pub struct DatasetKey {
        family: ChainFamily,
        name: DatasetId,
        #[serde(skip)]
        key: String,
    }

    impl TryFrom<RawDatasetKey> for DatasetKey {
        type Error = DatalensError;

        fn try_from(raw: RawDatasetKey) -> Result<Self, Self::Error> {
            Self::try_new(raw.family, raw.name.as_str())
        }
    }

    impl DatasetKey {
        pub fn try_new(
            family: ChainFamily,
            name: impl Into<String>,
        ) -> Result<Self, DatalensError> {
            let family = match family {
                ChainFamily::Evm => ChainFamily::Evm,
                ChainFamily::Other(value) => ChainFamily::try_other(value)?,
            };
            let name = DatasetId::try_new(name)?;
            let key = format!("{}.{}", family.key(), name.as_str());
            Ok(Self { family, name, key })
        }

        pub fn evm_blocks() -> Self {
            Self::from(Dataset::Blocks)
        }

        pub fn evm_logs() -> Self {
            Self::from(Dataset::Logs)
        }

        pub fn tron_blocks() -> Self {
            Self::try_new(ChainFamily::Other("tron".to_owned()), "blocks").unwrap()
        }

        pub fn tron_events() -> Self {
            Self::try_new(ChainFamily::Other("tron".to_owned()), "events").unwrap()
        }

        pub fn solana_slots() -> Self {
            Self::try_new(ChainFamily::Other("solana".to_owned()), "slots").unwrap()
        }

        pub fn solana_transactions() -> Self {
            Self::try_new(ChainFamily::Other("solana".to_owned()), "transactions").unwrap()
        }

        pub fn solana_instructions() -> Self {
            Self::try_new(ChainFamily::Other("solana".to_owned()), "instructions").unwrap()
        }

        pub fn solana_account_updates() -> Self {
            Self::try_new(ChainFamily::Other("solana".to_owned()), "account_updates").unwrap()
        }

        pub fn family(&self) -> &ChainFamily {
            &self.family
        }

        pub fn name(&self) -> &DatasetId {
            &self.name
        }

        pub fn as_str(&self) -> &str {
            &self.key
        }

        pub fn legacy_dataset(&self) -> Option<Dataset> {
            match (self.family(), self.name().as_str()) {
                (ChainFamily::Evm, "blocks") => Some(Dataset::Blocks),
                (ChainFamily::Evm, "logs") => Some(Dataset::Logs),
                _ => None,
            }
        }
    }

    impl From<Dataset> for DatasetKey {
        fn from(dataset: Dataset) -> Self {
            match dataset {
                Dataset::Blocks => Self::try_new(ChainFamily::Evm, "blocks").unwrap(),
                Dataset::Logs => Self::try_new(ChainFamily::Evm, "logs").unwrap(),
            }
        }
    }
}

pub mod coverage {
    use serde::{Deserialize, Deserializer, Serialize, de::Error};

    use crate::{
        BlockRange, ChainIdentity, DatalensError, DatalensErrorKind, Dataset, EvmLogFilter,
    };

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum CoverageLevel {
        Covered,
        Partial,
        Missing,
    }

    pub const COVERAGE_SCHEMA_VERSION: u16 = 1;

    #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
    pub struct CoverageKey {
        chain: ChainIdentity,
        dataset: Dataset,
        schema_version: u16,
        coverage: CoverageShape,
    }

    impl<'de> Deserialize<'de> for CoverageKey {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            #[derive(Deserialize)]
            struct RawCoverageKey {
                chain: ChainIdentity,
                dataset: Dataset,
                schema_version: u16,
                coverage: CoverageShape,
            }

            let raw = RawCoverageKey::deserialize(deserializer)?;
            if raw.schema_version != COVERAGE_SCHEMA_VERSION {
                return Err(D::Error::custom(format!(
                    "unsupported coverage schema_version {}; only {COVERAGE_SCHEMA_VERSION} is supported",
                    raw.schema_version
                )));
            }
            Ok(Self {
                chain: raw.chain,
                dataset: raw.dataset,
                schema_version: raw.schema_version,
                coverage: raw.coverage,
            })
        }
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
                CoverageShape::EvmLogs(filter) => format!("evm-logs/{}", filter.compact_key()),
            }
        }

        pub fn canonical_coverage_key(&self) -> String {
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

    #[derive(Deserialize)]
    struct RawCoverageRecord {
        key: CoverageKey,
        range: BlockRange,
        row_count: usize,
        object_key: Option<String>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    #[serde(try_from = "RawCoverageRecord")]
    pub struct CoverageRecord {
        key: CoverageKey,
        range: BlockRange,
        row_count: usize,
        object_key: Option<String>,
    }

    impl TryFrom<RawCoverageRecord> for CoverageRecord {
        type Error = DatalensError;

        fn try_from(raw: RawCoverageRecord) -> Result<Self, Self::Error> {
            Self::try_from_parts(raw.key, raw.range, raw.row_count, raw.object_key)
        }
    }

    impl CoverageRecord {
        pub fn try_data_object(
            key: CoverageKey,
            range: BlockRange,
            row_count: usize,
            object_key: impl Into<String>,
        ) -> Result<Self, DatalensError> {
            Self::try_from_parts(key, range, row_count, Some(object_key.into()))
        }

        pub fn try_empty(
            key: CoverageKey,
            range: BlockRange,
            row_count: usize,
            object_key: Option<String>,
        ) -> Result<Self, DatalensError> {
            Self::try_from_parts(key, range, row_count, object_key)
        }

        fn try_from_parts(
            key: CoverageKey,
            range: BlockRange,
            row_count: usize,
            object_key: Option<String>,
        ) -> Result<Self, DatalensError> {
            match object_key {
                Some(object_key) => {
                    if row_count == 0 {
                        return Err(DatalensError::new(
                            DatalensErrorKind::InvalidInput,
                            "data object coverage must have row_count greater than zero",
                        ));
                    }
                    if object_key.trim().is_empty() {
                        return Err(DatalensError::new(
                            DatalensErrorKind::InvalidInput,
                            "data object coverage must have a non-empty object key",
                        ));
                    }
                    Ok(Self {
                        key,
                        range,
                        row_count,
                        object_key: Some(object_key),
                    })
                }
                None => {
                    if row_count != 0 {
                        return Err(DatalensError::new(
                            DatalensErrorKind::InvalidInput,
                            "empty coverage must have row_count zero",
                        ));
                    }
                    Ok(Self {
                        key,
                        range,
                        row_count,
                        object_key: None,
                    })
                }
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
            if self.row_count > 0 && self.object_key.is_some() {
                CoverageValue::DataObject
            } else {
                CoverageValue::Empty
            }
        }

        /// First-stage coverage matching is exact by key; broader filters do not satisfy narrower filters.
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
        UnsupportedDataset,
        ProviderFailure,
        ProviderLimit,
        ProviderTimeout,
        RateLimited,
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
                RawTopicFilter::AnyOf(values) => {
                    normalize_values("topic", values, 32).map(Self::AnyOf)
                }
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
        pub fn dataset(&self) -> Dataset {
            match self {
                Self::EvmBlocks(_) => Dataset::Blocks,
                Self::EvmLogs(_) => Dataset::Logs,
                Self::TronEvents(_)
                | Self::SolanaTransactions(_)
                | Self::SolanaInstructions(_)
                | Self::OtherJson(_) => {
                    panic!("non-EVM rows do not have a legacy Dataset")
                }
            }
        }

        pub fn dataset_key(&self) -> DatasetKey {
            match self {
                Self::EvmBlocks(_) => DatasetKey::evm_blocks(),
                Self::EvmLogs(_) => DatasetKey::evm_logs(),
                Self::TronEvents(_) => DatasetKey::tron_events(),
                Self::SolanaTransactions(_) => DatasetKey::solana_transactions(),
                Self::SolanaInstructions(_) => DatasetKey::solana_instructions(),
                Self::OtherJson(_) => {
                    DatasetKey::try_new(crate::ChainFamily::Other("other".to_owned()), "json")
                        .unwrap()
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
}

pub use chain::{ChainFamily, ChainIdentity, NetworkId};
pub use coverage::{CoverageKey, CoverageLevel, CoverageRecord, CoverageShape, CoverageValue};
pub use dataset::{Dataset, DatasetId, DatasetKey};
pub use error::{DatalensError, DatalensErrorKind};
pub use query::{
    BlockHeader, CacheSummary, DatasetRows, EvmLogFilter, LogFilter, LogRecord, QueryRequest,
    QueryResponse, QueryRows, TopicFilter,
};
pub use range::{BlockRange, LedgerRange, LedgerRangeKind, TimeRange};
pub use result::ResultEnvelope;

#[cfg(test)]
mod tests {
    use crate::{
        BlockRange, ChainFamily, ChainIdentity, CoverageKey, CoverageRecord, CoverageValue,
        DatalensError, DatalensErrorKind, Dataset, DatasetId, EvmLogFilter, LogFilter, LogRecord,
        NetworkId, QueryRows, TimeRange, TopicFilter,
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
    fn test_dataset_key_has_builtin_chain_neutral_ids() {
        assert_eq!(crate::DatasetKey::evm_blocks().as_str(), "evm.blocks");
        assert_eq!(crate::DatasetKey::evm_logs().as_str(), "evm.logs");
        assert_eq!(crate::DatasetKey::tron_blocks().as_str(), "tron.blocks");
        assert_eq!(crate::DatasetKey::tron_events().as_str(), "tron.events");
        assert_eq!(crate::DatasetKey::solana_slots().as_str(), "solana.slots");
        assert_eq!(
            crate::DatasetKey::solana_transactions().as_str(),
            "solana.transactions"
        );
        assert_eq!(
            crate::DatasetKey::solana_instructions().as_str(),
            "solana.instructions"
        );
        assert_eq!(
            crate::DatasetKey::solana_account_updates().as_str(),
            "solana.account_updates"
        );
        assert_eq!(
            crate::DatasetKey::from(Dataset::Logs),
            crate::DatasetKey::evm_logs()
        );
        assert!(crate::DatasetKey::try_new(ChainFamily::Evm, "bad/path").is_err());
    }

    #[test]
    fn test_ledger_range_supports_block_slot_and_height_math() {
        let range = crate::LedgerRange::blocks(10, 14).expect("valid range");
        assert_eq!(range.kind(), crate::LedgerRangeKind::Block);
        assert_eq!(range.len(), 5);
        assert!(range.contains(12));
        assert_eq!(
            range.intersection(&crate::LedgerRange::blocks(12, 20).unwrap()),
            Some(crate::LedgerRange::blocks(12, 14).unwrap())
        );
        assert!(range.overlaps(&crate::LedgerRange::blocks(14, 20).unwrap()));
        assert!(!range.overlaps(&crate::LedgerRange::slots(14, 20).unwrap()));
        assert_eq!(
            range.difference(&crate::LedgerRange::blocks(12, 13).unwrap()),
            vec![
                crate::LedgerRange::blocks(10, 11).unwrap(),
                crate::LedgerRange::blocks(14, 14).unwrap()
            ]
        );
        assert_eq!(
            range.split(2).expect("split"),
            vec![
                crate::LedgerRange::blocks(10, 11).unwrap(),
                crate::LedgerRange::blocks(12, 13).unwrap(),
                crate::LedgerRange::blocks(14, 14).unwrap()
            ]
        );

        let slot = crate::LedgerRange::slots(1, 1).expect("slot range");
        assert_eq!(slot.kind(), crate::LedgerRangeKind::Slot);
        assert_eq!(slot.start(), 1);
        assert_eq!(slot.end(), 1);
        assert!(crate::LedgerRange::heights(2, 1).is_err());
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
    fn test_log_record_deserialization_canonicalizes_hex_values() {
        let json = r#"{
            "block_number":10,
            "block_hash":"0xblock",
            "transaction_hash":"0xtx",
            "transaction_index":0,
            "log_index":1,
            "address":"0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "topics":["0XBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"],
            "data":"0xCAFE",
            "removed":false
        }"#;

        let record: LogRecord = serde_json::from_str(json).expect("valid log record");

        assert_eq!(record.address, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(
            record.topics,
            vec!["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
        );
    }

    #[test]
    fn test_log_record_deserialization_rejects_invalid_hex_values() {
        let invalid_address = r#"{
            "block_number":10,
            "block_hash":"0xblock",
            "transaction_hash":"0xtx",
            "transaction_index":0,
            "log_index":1,
            "address":"0xabc",
            "topics":[],
            "data":"0x",
            "removed":false
        }"#;
        assert!(serde_json::from_str::<LogRecord>(invalid_address).is_err());

        let invalid_topic = r#"{
            "block_number":10,
            "block_hash":"0xblock",
            "transaction_hash":"0xtx",
            "transaction_index":0,
            "log_index":1,
            "address":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "topics":["0xabc"],
            "data":"0x",
            "removed":false
        }"#;
        assert!(serde_json::from_str::<LogRecord>(invalid_topic).is_err());

        let invalid_data = r#"{
            "block_number":10,
            "block_hash":"0xblock",
            "transaction_hash":"0xtx",
            "transaction_index":0,
            "log_index":1,
            "address":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "topics":[],
            "data":"0x0",
            "removed":false
        }"#;
        assert!(serde_json::from_str::<LogRecord>(invalid_data).is_err());
    }

    #[test]
    fn test_empty_coverage_record_is_distinct_from_missing_and_satisfies_same_key_range() {
        let chain =
            ChainIdentity::try_new(ChainFamily::Evm, "darwinia", Some(NetworkId::numeric(46)))
                .unwrap();
        let key = CoverageKey::full_blocks(chain);
        let range = BlockRange::try_new(100, 110).unwrap();
        let record = CoverageRecord::try_empty(key.clone(), range, 0, None).unwrap();

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

    #[test]
    fn test_deserialization_rejects_invalid_domain_values() {
        assert!(
            serde_json::from_str::<ChainIdentity>(
                r#"{"family":{"Other":" "},"configured_name":"ethereum"}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<ChainIdentity>(
                r#"{"family":"Evm","configured_name":"eth/mainnet"}"#
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<NetworkId>(r#"{"kind":"textual","value":"eth/mainnet"}"#)
                .is_err()
        );
        assert!(serde_json::from_str::<BlockRange>(r#"{"from_block":10,"to_block":9}"#).is_err());
        assert!(
            serde_json::from_str::<TopicFilter>(r#"{"kind":"any_of","values":["0xabc"]}"#).is_err()
        );
        assert!(
            serde_json::from_str::<EvmLogFilter>(r#"{"addresses":["0xabc"],"topics":[]}"#).is_err()
        );

        let toml_text = r#"
            family = "Evm"
            configured_name = " "
        "#;
        assert!(toml::from_str::<ChainIdentity>(toml_text).is_err());
    }

    #[test]
    fn test_deserialized_equivalent_filters_keep_same_coverage_key() {
        let chain =
            ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
                .unwrap();
        let filter = EvmLogFilter::try_from(LogFilter {
            addresses: vec!["0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned()],
            topics: vec![Some(vec![
                "0XBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".to_owned(),
            ])],
        })
        .unwrap();
        let encoded = serde_json::to_string(&filter).unwrap();
        let decoded: EvmLogFilter = serde_json::from_str(&encoded).unwrap();

        assert_eq!(
            CoverageKey::evm_logs(chain.clone(), filter).coverage_key(),
            CoverageKey::evm_logs(chain, decoded).coverage_key()
        );
    }

    #[test]
    fn test_coverage_record_checked_constructors_reject_invalid_semantics() {
        let key = CoverageKey::full_blocks(
            ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
                .unwrap(),
        );
        let range = BlockRange::try_new(1, 2).unwrap();

        assert!(CoverageRecord::try_data_object(key.clone(), range, 0, "obj.json").is_err());
        assert!(CoverageRecord::try_data_object(key.clone(), range, 1, " ").is_err());
        assert!(CoverageRecord::try_empty(key.clone(), range, 1, None).is_err());
        assert!(
            CoverageRecord::try_empty(key.clone(), range, 0, Some("obj.json".to_owned())).is_err()
        );

        let record = CoverageRecord::try_empty(key.clone(), range, 0, None).unwrap();
        assert_eq!(record.value(), CoverageValue::Empty);
        assert!(record.covers(&key, &range));
    }

    #[test]
    fn test_coverage_record_deserialization_rejects_invalid_semantics() {
        let json = r#"{
            "key":{"chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},"dataset":"blocks","schema_version":1,"coverage":{"shape":"all"}},
            "range":{"from_block":1,"to_block":2},
            "row_count":0,
            "object_key":"objects/blocks/all/1-2.json"
        }"#;
        assert!(serde_json::from_str::<CoverageRecord>(json).is_err());

        let json = r#"{
            "key":{"chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},"dataset":"blocks","schema_version":1,"coverage":{"shape":"all"}},
            "range":{"from_block":1,"to_block":2},
            "row_count":1,
            "object_key":null
        }"#;
        assert!(serde_json::from_str::<CoverageRecord>(json).is_err());
    }

    #[test]
    fn test_query_rows_try_append_checks_dataset_mismatch() {
        let mut blocks = QueryRows::EvmBlocks(vec![crate::BlockHeader {
            number: 1,
            hash: "0x1".to_owned(),
            parent_hash: "0x0".to_owned(),
            timestamp: 10,
        }]);

        blocks
            .try_append(QueryRows::EvmBlocks(vec![crate::BlockHeader {
                number: 2,
                hash: "0x2".to_owned(),
                parent_hash: "0x1".to_owned(),
                timestamp: 20,
            }]))
            .unwrap();
        assert_eq!(blocks.row_count(), 2);

        let error = blocks
            .try_append(QueryRows::EvmLogs(Vec::new()))
            .expect_err("dataset mismatch");
        assert_eq!(error.kind, DatalensErrorKind::Internal);
    }

    #[test]
    fn test_dataset_rows_envelope_keeps_dataset_key_with_typed_rows() {
        let rows = crate::DatasetRows::new(
            crate::DatasetKey::evm_blocks(),
            QueryRows::EvmBlocks(vec![crate::BlockHeader {
                number: 1,
                hash: "0x1".to_owned(),
                parent_hash: "0x0".to_owned(),
                timestamp: 10,
            }]),
        )
        .expect("matching dataset rows");

        assert_eq!(rows.dataset_key(), &crate::DatasetKey::evm_blocks());
        assert_eq!(rows.rows().row_count(), 1);

        let error = crate::DatasetRows::new(
            crate::DatasetKey::evm_logs(),
            QueryRows::EvmBlocks(Vec::new()),
        )
        .expect_err("dataset key mismatch");
        assert_eq!(error.kind, DatalensErrorKind::Internal);
    }

    #[test]
    fn test_compact_coverage_key_is_deterministic_and_storage_safe() {
        let first = EvmLogFilter::try_from(LogFilter {
            addresses: vec!["0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned()],
            topics: vec![None],
        })
        .unwrap();
        let second = EvmLogFilter::try_from(LogFilter {
            addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
            topics: vec![None],
        })
        .unwrap();
        let third = EvmLogFilter::try_from(LogFilter {
            addresses: vec!["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned()],
            topics: vec![None],
        })
        .unwrap();

        assert_eq!(first.canonical_key(), second.canonical_key());
        assert_eq!(first.compact_key(), second.compact_key());
        assert_ne!(first.compact_key(), third.compact_key());
        assert!(first.compact_key().starts_with("addr-topic-"));
        assert!(!first.compact_key().contains('/'));
    }

    #[test]
    fn test_compact_coverage_key_uses_sha256_prefix() {
        let filter = EvmLogFilter::try_from(LogFilter {
            addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
            topics: vec![None],
        })
        .unwrap();
        let key = filter.compact_key();
        let digest = key.strip_prefix("addr-topic-").expect("compact key prefix");

        assert_eq!(digest.len(), 32, "128-bit SHA-256 prefix");
        assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(!key.contains("0xaaaaaaaa"));
    }

    #[test]
    fn test_coverage_key_deserialization_rejects_unsupported_schema_version() {
        let supported = r#"{
            "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
            "dataset":"blocks",
            "schema_version":1,
            "coverage":{"shape":"all"}
        }"#;
        assert!(serde_json::from_str::<CoverageKey>(supported).is_ok());

        let unsupported = r#"{
            "chain":{"family":"Evm","configured_name":"ethereum","network_id":{"kind":"numeric","value":1}},
            "dataset":"blocks",
            "schema_version":2,
            "coverage":{"shape":"all"}
        }"#;
        assert!(serde_json::from_str::<CoverageKey>(unsupported).is_err());
    }

    #[test]
    fn test_log_record_checked_constructor_canonicalizes_hex_values() {
        let record = LogRecord::try_new(
            10,
            "0xblock".to_owned(),
            "0xtx".to_owned(),
            0,
            1,
            "0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            vec!["0XBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".to_owned()],
            "0x".to_owned(),
            false,
        )
        .unwrap();

        assert_eq!(record.address, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(
            record.topics,
            vec!["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"]
        );

        let json = r#"{
            "block_number":10,
            "block_hash":"0xblock",
            "transaction_hash":"0xtx",
            "transaction_index":0,
            "log_index":1,
            "address":"0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "topics":["0XBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"],
            "data":"0x",
            "removed":false
        }"#;
        let decoded: LogRecord = serde_json::from_str(json).unwrap();
        assert_eq!(decoded.address, record.address);
        assert_eq!(decoded.topics, record.topics);

        let json = r#"{
            "block_number":10,
            "block_hash":"0xblock",
            "transaction_hash":"0xtx",
            "transaction_index":0,
            "log_index":1,
            "address":"0xabc",
            "topics":[],
            "data":"0x",
            "removed":false
        }"#;
        assert!(serde_json::from_str::<LogRecord>(json).is_err());

        let json = r#"{
            "block_number":10,
            "block_hash":"0xblock",
            "transaction_hash":"0xtx",
            "transaction_index":0,
            "log_index":1,
            "address":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "topics":[],
            "data":"0x0",
            "removed":false
        }"#;
        assert!(serde_json::from_str::<LogRecord>(json).is_err());

        assert!(
            LogRecord::try_new(
                10,
                "0xblock".to_owned(),
                "0xtx".to_owned(),
                0,
                1,
                "0xabc",
                Vec::new(),
                "0x".to_owned(),
                false,
            )
            .is_err()
        );
    }

    #[test]
    fn test_error_retryability_is_explicit_for_every_variant() {
        let cases = [
            (DatalensErrorKind::InvalidInput, false),
            (DatalensErrorKind::InvalidRequest, false),
            (DatalensErrorKind::UnsupportedDataset, false),
            (DatalensErrorKind::ProviderFailure, true),
            (DatalensErrorKind::ProviderLimit, false),
            (DatalensErrorKind::ProviderTimeout, true),
            (DatalensErrorKind::RateLimited, true),
            (DatalensErrorKind::StorageReadFailure, true),
            (DatalensErrorKind::StorageWriteFailure, true),
            (DatalensErrorKind::ManifestUpdateFailure, true),
            (DatalensErrorKind::Internal, false),
        ];

        for (kind, retryable) in cases {
            assert_eq!(kind.is_retryable(), retryable, "{kind:?}");
        }
    }

    #[test]
    fn test_dataset_id_and_time_range_have_checked_semantics() {
        assert!(DatasetId::try_new("logs").is_ok());
        assert!(DatasetId::try_new(" ").is_err());
        assert!(DatasetId::try_new("bad/path").is_err());
        assert_eq!(
            DatasetId::try_from(" logs ".to_owned()).unwrap().as_str(),
            "logs"
        );
        assert!(DatasetId::try_from("bad/path".to_owned()).is_err());
        assert!(TimeRange::try_blocks(1, 2).is_ok());
        assert!(TimeRange::try_blocks(2, 1).is_err());
    }

    #[test]
    fn test_coverage_matching_is_exact_by_key() {
        let chain =
            ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
                .unwrap();
        let range = BlockRange::try_new(1, 10).unwrap();
        let all_logs = CoverageKey::evm_logs(
            chain.clone(),
            EvmLogFilter::try_from(LogFilter {
                addresses: Vec::new(),
                topics: Vec::new(),
            })
            .unwrap(),
        );
        let address_logs = CoverageKey::evm_logs(
            chain,
            EvmLogFilter::try_from(LogFilter {
                addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
                topics: Vec::new(),
            })
            .unwrap(),
        );
        let record = CoverageRecord::try_empty(all_logs, range, 0, None).unwrap();

        assert!(!record.covers(&address_logs, &range));
    }
}
