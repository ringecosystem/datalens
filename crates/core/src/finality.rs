use serde::{Deserialize, Serialize};

/// Finality of a stored row or coverage range. Ordered from weakest to strongest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Finality {
    Unsafe = 1,
    Safe = 2,
    Finalized = 3,
}

impl Finality {
    pub fn as_i16(self) -> i16 {
        self as i16
    }

    pub fn from_i16(value: i16) -> Option<Self> {
        match value {
            1 => Some(Self::Unsafe),
            2 => Some(Self::Safe),
            3 => Some(Self::Finalized),
            _ => None,
        }
    }
}

/// Which part of the chain a query is allowed to read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryFinality {
    #[default]
    DurableOnly,
    SafeToLatest,
    LatestOnly,
}
