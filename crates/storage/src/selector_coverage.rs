use datalens_chain::DatasetSelector;
use datalens_core::{DatasetKey, DatasetRows, EvmLogFilter, LogFilter, QueryRows};

use crate::ManifestEntry;

pub(crate) fn entry_covers_selector(
    entry: &ManifestEntry,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    selector_fingerprint: &str,
) -> bool {
    if entry.selector_fingerprint == selector_fingerprint {
        return true;
    }
    if *dataset_key != DatasetKey::evm_logs() {
        return false;
    }
    let DatasetSelector::EvmLogs(query_filter) = selector else {
        return false;
    };
    let Some(stored_filter) = parse_evm_log_canonical_key(&entry.selector_canonical_key) else {
        return false;
    };
    stored_filter.covers(query_filter)
}

pub(crate) fn filter_evm_log_rows_for_selector(
    rows: DatasetRows,
    selector: &DatasetSelector,
) -> DatasetRows {
    let DatasetSelector::EvmLogs(filter) = selector else {
        return rows;
    };
    if rows.dataset_key() != &DatasetKey::evm_logs() {
        return rows;
    }
    match rows.into_rows() {
        QueryRows::EvmLogs(rows) => DatasetRows::new(
            DatasetKey::evm_logs(),
            QueryRows::EvmLogs(
                rows.into_iter()
                    .filter(|row| filter.matches_log(row))
                    .collect(),
            ),
        )
        .expect("filtered rows keep dataset key"),
        rows => DatasetRows::new(DatasetKey::evm_logs(), rows).expect("evm logs dataset rows"),
    }
}

fn parse_evm_log_canonical_key(canonical_key: &str) -> Option<EvmLogFilter> {
    let rest = canonical_key.strip_prefix("evm-logs/")?;
    let mut parts = rest.split('/');
    let addresses = parts.next()?.strip_prefix("addr=")?;
    let topics = parts.next()?.strip_prefix("topics=")?;
    if parts.next().is_some() {
        return None;
    }

    EvmLogFilter::try_from(LogFilter {
        addresses: parse_values(addresses, "*")?,
        topics: parse_topic_slots(topics)?,
    })
    .ok()
}

fn parse_values(value: &str, wildcard: &str) -> Option<Vec<String>> {
    if value == wildcard {
        return Some(Vec::new());
    }
    if value.is_empty() {
        return None;
    }
    Some(value.split(',').map(str::to_owned).collect())
}

fn parse_topic_slots(value: &str) -> Option<Vec<Option<Vec<String>>>> {
    if value == "*" {
        return Some(Vec::new());
    }
    if value.is_empty() {
        return None;
    }
    value
        .split(';')
        .map(|slot| {
            if slot == "*" {
                Some(None)
            } else if slot == "[]" {
                Some(Some(Vec::new()))
            } else {
                parse_values(slot, "*").map(Some)
            }
        })
        .collect()
}
