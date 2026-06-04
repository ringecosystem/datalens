use datalens_chain::DatasetSelector;
use datalens_core::{
    ChainIdentity, DatasetKey, DatasetRows, EvmLogFilter, LedgerRange, LogFilter, QueryRows,
    missing_ranges,
};

use crate::{ManifestEntry, intersect, merge_ranges};

pub(crate) struct SelectorCoverageCandidate<'a> {
    pub(crate) entry: &'a ManifestEntry,
    pub(crate) ranges: Vec<LedgerRange>,
}

pub(crate) fn selector_coverage_candidates<'a>(
    entries: &'a [ManifestEntry],
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    range: &LedgerRange,
) -> Vec<SelectorCoverageCandidate<'a>> {
    let selector_fingerprint = selector.fingerprint();
    let mut candidates =
        exact_selector_candidates(entries, chain, dataset_key, &selector_fingerprint, range);
    let exact_ranges = merge_ranges(
        candidates
            .iter()
            .flat_map(|candidate| candidate.ranges.clone())
            .collect(),
    );
    candidates.extend(semantic_selector_candidates(
        entries,
        chain,
        dataset_key,
        selector,
        &selector_fingerprint,
        range,
        &exact_ranges,
    ));
    candidates
}

fn exact_selector_candidates<'a>(
    entries: &'a [ManifestEntry],
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector_fingerprint: &str,
    range: &LedgerRange,
) -> Vec<SelectorCoverageCandidate<'a>> {
    entries
        .iter()
        .filter(|entry| entry_matches_context(entry, chain, dataset_key, range))
        .filter(|entry| entry.selector_fingerprint == selector_fingerprint)
        .filter_map(|entry| {
            intersect(entry.range.clone(), range.clone()).map(|range| SelectorCoverageCandidate {
                entry,
                ranges: vec![range],
            })
        })
        .collect()
}

fn semantic_selector_candidates<'a>(
    entries: &'a [ManifestEntry],
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
    selector_fingerprint: &str,
    range: &LedgerRange,
    exact_ranges: &[LedgerRange],
) -> Vec<SelectorCoverageCandidate<'a>> {
    entries
        .iter()
        .filter(|entry| entry_matches_context(entry, chain, dataset_key, range))
        .filter(|entry| entry.selector_fingerprint != selector_fingerprint)
        .filter(|entry| entry_semantically_covers_selector(entry, dataset_key, selector))
        .filter_map(|entry| {
            let intersection = intersect(entry.range.clone(), range.clone())?;
            let ranges = missing_ranges(intersection.clone(), exact_ranges);
            if ranges.is_empty() {
                None
            } else {
                Some(SelectorCoverageCandidate { entry, ranges })
            }
        })
        .collect()
}

fn entry_matches_context(
    entry: &ManifestEntry,
    chain: &ChainIdentity,
    dataset_key: &DatasetKey,
    range: &LedgerRange,
) -> bool {
    entry.chain == *chain && entry.dataset_key == *dataset_key && entry.range.kind() == range.kind()
}

fn entry_semantically_covers_selector(
    entry: &ManifestEntry,
    dataset_key: &DatasetKey,
    selector: &DatasetSelector,
) -> bool {
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
