use std::collections::BTreeSet;

use datalens_core::{Address, BlockRange, Dataset, Finality, LogRow, LogSelector};
use datalens_provider::{ProviderError, Purpose, TokenWait};
use serde_json::{Value, json};

use crate::client::EvmClient;
use crate::decode::parse_log;
use crate::encode::log_filter;

/// Logs from a contiguous prefix of the requested range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogsPage {
    pub rows: Vec<LogRow>,
    /// Last block included in `rows`. `None` when the first window failed.
    pub fetched_to: Option<u64>,
}

/// `page` is the prefix fetched before `error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFetchError {
    pub page: LogsPage,
    pub error: ProviderError,
}

struct Window {
    rows: Vec<LogRow>,
    end: u64,
}

struct LogCall<'a> {
    selector: &'a LogSelector,
    groups: &'a [BTreeSet<Address>],
    purpose: Purpose,
    application: Option<&'a str>,
    wait: TokenWait,
    finality: Finality,
}

impl EvmClient {
    pub async fn get_logs(
        &self,
        selector: &LogSelector,
        range: BlockRange,
        purpose: Purpose,
        application: Option<&str>,
        wait: TokenWait,
        finality: Finality,
    ) -> Result<LogsPage, LogFetchError> {
        let groups = address_groups(selector, self.max_addresses);
        let call = LogCall {
            selector,
            groups: &groups,
            purpose,
            application,
            wait,
            finality,
        };
        let mut rows = Vec::new();
        let mut cursor = range.start();
        let mut fetched_to = None;
        while cursor <= range.end() {
            match self.fetch_window(&call, cursor, range.end()).await {
                Ok(window) => {
                    rows.extend(window.rows);
                    fetched_to = Some(window.end);
                    if window.end == range.end() {
                        break;
                    }
                    cursor = window.end + 1;
                }
                Err(error) => {
                    return Err(LogFetchError {
                        page: LogsPage { rows, fetched_to },
                        error,
                    });
                }
            }
        }
        Ok(LogsPage { rows, fetched_to })
    }

    async fn fetch_window(
        &self,
        call: &LogCall<'_>,
        cursor: u64,
        range_end: u64,
    ) -> Result<Window, ProviderError> {
        loop {
            let window = self.gateway.range_blocks(Dataset::EvmLogs);
            let end = cursor
                .saturating_add(window.saturating_sub(1))
                .min(range_end);
            match self.fetch_groups(call, cursor, end).await {
                Ok(rows) => {
                    self.gateway.record_success(Dataset::EvmLogs);
                    return Ok(Window { rows, end });
                }
                Err(ProviderError::Limit) => {
                    let failed_len = end - cursor + 1;
                    self.gateway.record_limit(Dataset::EvmLogs, failed_len)?;
                    let shrunk = self.gateway.range_blocks(Dataset::EvmLogs);
                    let next_end = cursor
                        .saturating_add(shrunk.saturating_sub(1))
                        .min(range_end);
                    if next_end >= end {
                        return Err(ProviderError::Limit);
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn fetch_groups(
        &self,
        call: &LogCall<'_>,
        start: u64,
        end: u64,
    ) -> Result<Vec<LogRow>, ProviderError> {
        let mut rows = Vec::new();
        for addresses in call.groups {
            let mut narrowed = call.selector.clone();
            narrowed.addresses = addresses.clone();
            let value = self
                .gateway
                .call(
                    "eth_getLogs",
                    json!([log_filter(&narrowed, start, end)]),
                    call.purpose,
                    call.application,
                    call.wait,
                )
                .await?;
            rows.extend(parse_log_list(
                &value,
                call.finality,
                start,
                end,
                &narrowed,
            )?);
        }
        Ok(rows)
    }
}

fn address_groups(selector: &LogSelector, max_addresses: usize) -> Vec<BTreeSet<Address>> {
    if selector.addresses.is_empty() {
        return vec![BTreeSet::new()];
    }
    selector
        .addresses
        .iter()
        .copied()
        .collect::<Vec<_>>()
        .chunks(max_addresses)
        .map(|chunk| chunk.iter().copied().collect())
        .collect()
}

fn parse_log_list(
    value: &Value,
    finality: Finality,
    start: u64,
    end: u64,
    selector: &LogSelector,
) -> Result<Vec<LogRow>, ProviderError> {
    let logs = value
        .as_array()
        .ok_or_else(|| ProviderError::fatal("eth_getLogs result is not an array"))?;
    let mut rows = Vec::new();
    for log in logs {
        if let Some(row) = parse_log(log, finality)? {
            if row.block_number < start || row.block_number > end {
                return Err(ProviderError::fatal(
                    "log block is outside the requested range",
                ));
            }
            if !selector.addresses.is_empty() && !selector.addresses.contains(&row.address) {
                return Err(ProviderError::fatal(
                    "log address does not match the filter",
                ));
            }
            for (index, filter) in selector.topics.iter().enumerate() {
                if !filter.matches(row.topics.get(index)) {
                    return Err(ProviderError::fatal("log topics do not match the filter"));
                }
            }
            rows.push(row);
        }
    }
    Ok(rows)
}
