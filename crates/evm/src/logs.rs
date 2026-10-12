use std::collections::BTreeSet;

use datalens_core::{Address, BlockRange, Dataset, Finality, LogRow, LogSelector};
use datalens_provider::{ProviderError, Purpose};
use serde_json::{Value, json};

use crate::client::EvmClient;
use crate::decode::parse_log;
use crate::encode::log_filter;

impl EvmClient {
    pub async fn get_logs(
        &self,
        selector: &LogSelector,
        range: BlockRange,
        purpose: Purpose,
        application: Option<&str>,
        finality: Finality,
    ) -> Result<Vec<LogRow>, ProviderError> {
        let mut rows = Vec::new();
        for addresses in address_groups(selector, self.max_addresses) {
            let mut narrowed = selector.clone();
            narrowed.addresses = addresses;
            rows.extend(
                self.logs_over_range(&narrowed, range, purpose, application, finality)
                    .await?,
            );
        }
        Ok(rows)
    }

    async fn logs_over_range(
        &self,
        selector: &LogSelector,
        range: BlockRange,
        purpose: Purpose,
        application: Option<&str>,
        finality: Finality,
    ) -> Result<Vec<LogRow>, ProviderError> {
        let mut rows = Vec::new();
        let mut cursor = range.start();
        while cursor <= range.end() {
            let window = self.gateway.range_blocks(Dataset::EvmLogs);
            let end = cursor
                .saturating_add(window.saturating_sub(1))
                .min(range.end());
            let params = json!([log_filter(selector, cursor, end)]);
            match self
                .gateway
                .call("eth_getLogs", params, purpose, application)
                .await
            {
                Ok(value) => {
                    rows.extend(parse_log_list(&value, finality)?);
                    self.gateway.record_success(Dataset::EvmLogs);
                    if end == range.end() {
                        break;
                    }
                    cursor = end + 1;
                }
                Err(ProviderError::Limit) => {
                    let failed_len = end - cursor + 1;
                    let shrunk = self.gateway.record_limit(Dataset::EvmLogs)?;
                    if failed_len <= shrunk {
                        return Err(ProviderError::Limit);
                    }
                }
                Err(error) => return Err(error),
            }
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

fn parse_log_list(value: &Value, finality: Finality) -> Result<Vec<LogRow>, ProviderError> {
    let logs = value
        .as_array()
        .ok_or_else(|| ProviderError::fatal("eth_getLogs result is not an array"))?;
    let mut rows = Vec::new();
    for log in logs {
        if let Some(row) = parse_log(log, finality)? {
            rows.push(row);
        }
    }
    Ok(rows)
}
