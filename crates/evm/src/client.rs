use std::fmt;
use std::num::NonZeroUsize;

use datalens_provider::Gateway;

/// `header_batch_size` was outside 20..=50.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeaderBatchSizeError(pub usize);

impl fmt::Display for HeaderBatchSizeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "header_batch_size {} is outside 20..=50", self.0)
    }
}

impl std::error::Error for HeaderBatchSizeError {}

/// Inclusive block number, or one of the provider's named tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockRef {
    Number(u64),
    Latest,
    Safe,
    Finalized,
}

impl BlockRef {
    pub(crate) fn param(self) -> String {
        match self {
            Self::Number(number) => format!("0x{number:x}"),
            Self::Latest => "latest".to_owned(),
            Self::Safe => "safe".to_owned(),
            Self::Finalized => "finalized".to_owned(),
        }
    }
}

/// How `heads` derives safe and finalized. Matches the chain config modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadMode {
    /// `safe = latest - safe_lag_blocks`, `finalized = latest - finalized_lag_blocks`.
    Lag {
        safe_lag_blocks: u64,
        finalized_lag_blocks: u64,
    },
    /// One batch of `latest`, `safe`, and `finalized` block tags.
    Tags,
}

pub struct EvmClient {
    pub(crate) gateway: Gateway,
    pub(crate) max_addresses: usize,
    pub(crate) rpc_batch: usize,
}

impl EvmClient {
    /// `header_batch_size` must be 20..=50. Values outside that range are rejected.
    pub fn new(
        gateway: Gateway,
        max_addresses_per_query: NonZeroUsize,
        header_batch_size: usize,
    ) -> Result<Self, HeaderBatchSizeError> {
        if !(20..=50).contains(&header_batch_size) {
            return Err(HeaderBatchSizeError(header_batch_size));
        }
        Ok(Self {
            gateway,
            max_addresses: max_addresses_per_query.get(),
            rpc_batch: header_batch_size,
        })
    }

    pub fn gateway(&self) -> &Gateway {
        &self.gateway
    }
}
