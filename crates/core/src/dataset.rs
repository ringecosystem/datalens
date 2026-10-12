use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Dataset {
    #[serde(rename = "evm.logs")]
    EvmLogs,
    #[serde(rename = "evm.blocks")]
    EvmBlocks,
}

impl Dataset {
    pub const ALL: [Dataset; 2] = [Dataset::EvmLogs, Dataset::EvmBlocks];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::EvmLogs => "evm.logs",
            Self::EvmBlocks => "evm.blocks",
        }
    }
}

impl FromStr for Dataset {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        Self::ALL
            .into_iter()
            .find(|dataset| dataset.as_str() == s)
            .ok_or_else(|| Error::invalid(format!("unknown dataset {s:?}")))
    }
}

impl fmt::Display for Dataset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
