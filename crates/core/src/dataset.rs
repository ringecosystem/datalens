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
    Transactions,
    Receipts,
    Logs,
}

impl Dataset {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Blocks => "blocks",
            Self::Transactions => "transactions",
            Self::Receipts => "receipts",
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
    pub fn try_new(family: ChainFamily, name: impl Into<String>) -> Result<Self, DatalensError> {
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

    pub fn evm_transactions() -> Self {
        Self::from(Dataset::Transactions)
    }

    pub fn evm_receipts() -> Self {
        Self::from(Dataset::Receipts)
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
            (ChainFamily::Evm, "transactions") => Some(Dataset::Transactions),
            (ChainFamily::Evm, "receipts") => Some(Dataset::Receipts),
            (ChainFamily::Evm, "logs") => Some(Dataset::Logs),
            _ => None,
        }
    }
}

impl From<Dataset> for DatasetKey {
    fn from(dataset: Dataset) -> Self {
        match dataset {
            Dataset::Blocks => Self::try_new(ChainFamily::Evm, "blocks").unwrap(),
            Dataset::Transactions => Self::try_new(ChainFamily::Evm, "transactions").unwrap(),
            Dataset::Receipts => Self::try_new(ChainFamily::Evm, "receipts").unwrap(),
            Dataset::Logs => Self::try_new(ChainFamily::Evm, "logs").unwrap(),
        }
    }
}
