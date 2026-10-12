use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::Error;

/// Configured chain name such as `arbitrum`: non-empty, `[a-z0-9_-]`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ChainName(String);

impl ChainName {
    pub fn new(name: impl Into<String>) -> Result<Self, Error> {
        let name = name.into();
        let valid = !name.is_empty()
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_');
        if valid {
            Ok(Self(name))
        } else {
            Err(Error::invalid(format!(
                "chain name {name:?} must be non-empty lowercase [a-z0-9_-]"
            )))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for ChainName {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Error> {
        Self::new(value)
    }
}

impl From<ChainName> for String {
    fn from(value: ChainName) -> Self {
        value.0
    }
}

impl FromStr for ChainName {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        Self::new(s)
    }
}

impl fmt::Display for ChainName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// EVM chain id. Positive, because it names per-chain partitions such as `evm_logs_42161`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "i32", into = "i32")]
pub struct ChainId(i32);

impl ChainId {
    pub fn new(id: i32) -> Result<Self, Error> {
        if id > 0 {
            Ok(Self(id))
        } else {
            Err(Error::invalid(format!("chain id {id} must be positive")))
        }
    }

    pub fn get(self) -> i32 {
        self.0
    }
}

impl TryFrom<i32> for ChainId {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Error> {
        Self::new(value)
    }
}

impl From<ChainId> for i32 {
    fn from(value: ChainId) -> Self {
        value.0
    }
}

impl fmt::Display for ChainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
