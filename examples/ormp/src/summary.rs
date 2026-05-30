use datalens_client::QueryResponse;
use datalens_core::{DatasetKey, LedgerRange, QueryRows, missing_ranges};
use serde::Serialize;

use crate::{MSGPORT_ADDRESS, ORMP_ADDRESS, OrmpExampleError};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OrmpSummary {
    pub requested_range: RangeSummary,
    pub row_count: usize,
    pub hit_ranges: Vec<RangeSummary>,
    pub missing_ranges: Vec<RangeSummary>,
    pub durable_hit_ranges: Vec<RangeSummary>,
    pub provider_fill_ranges: Vec<RangeSummary>,
    pub first_log_block: Option<u64>,
    pub last_log_block: Option<u64>,
    pub contract_addresses: Vec<String>,
    pub full_durable_cache_hit: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RangeSummary {
    Block { start: u64, end: u64 },
    Slot { start: u64, end: u64 },
    Height { start: u64, end: u64 },
    Other { name: String, start: u64, end: u64 },
}

pub fn summarize_response(response: &QueryResponse) -> Result<OrmpSummary, OrmpExampleError> {
    if response.dataset_key != DatasetKey::evm_logs() {
        return Err(OrmpExampleError::InvalidResponse(
            "expected evm.logs response".to_owned(),
        ));
    }

    let logs = match response.rows.rows() {
        QueryRows::EvmLogs(logs) => logs,
        _ => {
            return Err(OrmpExampleError::InvalidResponse(
                "expected EVM log rows".to_owned(),
            ));
        }
    };
    let first_log_block = logs.iter().map(|log| log.block_number).min();
    let last_log_block = logs.iter().map(|log| log.block_number).max();
    let full_durable_cache_hit = response.cache.missing_ranges.is_empty()
        && response.cache.provider_fill_ranges.is_empty()
        && missing_ranges(response.range.clone(), &response.cache.durable_hit_ranges).is_empty();

    Ok(OrmpSummary {
        requested_range: RangeSummary::from_ledger_range(&response.range),
        row_count: response.rows.row_count(),
        hit_ranges: summarize_ranges(&response.cache.hit_ranges),
        missing_ranges: summarize_ranges(&response.cache.missing_ranges),
        durable_hit_ranges: summarize_ranges(&response.cache.durable_hit_ranges),
        provider_fill_ranges: summarize_ranges(&response.cache.provider_fill_ranges),
        first_log_block,
        last_log_block,
        contract_addresses: vec![MSGPORT_ADDRESS.to_owned(), ORMP_ADDRESS.to_owned()],
        full_durable_cache_hit,
    })
}

impl RangeSummary {
    pub fn from_ledger_range(range: &LedgerRange) -> Self {
        match range.kind() {
            datalens_core::LedgerRangeKind::Block => Self::Block {
                start: range.start(),
                end: range.end(),
            },
            datalens_core::LedgerRangeKind::Slot => Self::Slot {
                start: range.start(),
                end: range.end(),
            },
            datalens_core::LedgerRangeKind::Height => Self::Height {
                start: range.start(),
                end: range.end(),
            },
            datalens_core::LedgerRangeKind::Other(kind) => Self::Other {
                name: kind.to_owned(),
                start: range.start(),
                end: range.end(),
            },
        }
    }
}

fn summarize_ranges(ranges: &[LedgerRange]) -> Vec<RangeSummary> {
    ranges.iter().map(RangeSummary::from_ledger_range).collect()
}
