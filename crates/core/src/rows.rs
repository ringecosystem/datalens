use alloy_primitives::{Address, B256, Bytes};
use serde::{Deserialize, Serialize};

use crate::Finality;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRow {
    pub block_number: u64,
    pub log_index: u32,
    pub block_hash: B256,
    pub transaction_index: u32,
    pub transaction_hash: B256,
    pub address: Address,
    /// Zero to four topics, in log order.
    pub topics: Vec<B256>,
    pub data: Bytes,
    pub finality: Finality,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRow {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
    pub timestamp: u64,
    pub finality: Finality,
    /// Remaining header fields, projected by the query's `fields`.
    pub header: serde_json::Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Heads {
    pub latest: u64,
    pub latest_hash: B256,
    pub safe: u64,
    pub finalized: u64,
}
