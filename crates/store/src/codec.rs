use std::collections::BTreeSet;
use std::ops::Bound;
use std::time::Duration;

use datalens_core::{Address, B256, BlockRange, ChainId, Dataset, Finality, TopicFilter};
use sqlx::postgres::types::PgRange;

use crate::StoreError;

pub(crate) fn u64_to_i64(value: u64, what: &str) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::invalid(format!("{what} does not fit int8")))
}

pub(crate) fn i64_to_u64(value: i64, what: &str) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::data(format!("negative {what}")))
}

pub(crate) fn u32_to_i32(value: u32, what: &str) -> Result<i32, StoreError> {
    i32::try_from(value).map_err(|_| StoreError::invalid(format!("{what} does not fit int4")))
}

pub(crate) fn i32_to_u32(value: i32, what: &str) -> Result<u32, StoreError> {
    u32::try_from(value).map_err(|_| StoreError::data(format!("negative {what}")))
}

pub(crate) fn duration_millis(duration: Duration) -> Result<i64, StoreError> {
    i64::try_from(duration.as_millis())
        .map_err(|_| StoreError::invalid("duration does not fit int8 milliseconds"))
}

pub(crate) fn chain_from_db(id: i32) -> Result<ChainId, StoreError> {
    ChainId::new(id).map_err(|err| StoreError::data(err.to_string()))
}

pub(crate) fn dataset_from_db(value: &str) -> Result<Dataset, StoreError> {
    value
        .parse()
        .map_err(|err: datalens_core::Error| StoreError::data(err.to_string()))
}

pub(crate) fn finality_from_db(value: i16) -> Result<Finality, StoreError> {
    Finality::from_i16(value).ok_or_else(|| StoreError::data(format!("finality {value}")))
}

pub(crate) fn address_from_db(bytes: &[u8]) -> Result<Address, StoreError> {
    let bytes: [u8; 20] = bytes
        .try_into()
        .map_err(|_| StoreError::data(format!("address has {} bytes", bytes.len())))?;
    Ok(Address::from(bytes))
}

pub(crate) fn b256_from_db(bytes: &[u8]) -> Result<B256, StoreError> {
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| StoreError::data(format!("hash has {} bytes", bytes.len())))?;
    Ok(B256::from(bytes))
}

pub(crate) fn block_range_to_pg(range: BlockRange) -> PgRange<i64> {
    let start = range.start() as i64;
    let end = range.end() as i64 + 1;
    PgRange::from(start..end)
}

pub(crate) fn block_range_from_pg(range: PgRange<i64>) -> Result<BlockRange, StoreError> {
    let start = match range.start {
        Bound::Included(start) => start,
        Bound::Excluded(start) => start
            .checked_add(1)
            .ok_or_else(|| StoreError::data("range start overflow"))?,
        Bound::Unbounded => return Err(StoreError::data("range has no start")),
    };
    let end_exclusive = match range.end {
        Bound::Excluded(end) => end,
        Bound::Included(end) => end
            .checked_add(1)
            .ok_or_else(|| StoreError::data("range end overflow"))?,
        Bound::Unbounded => return Err(StoreError::data("range has no end")),
    };
    if start < 0 || end_exclusive <= start {
        return Err(StoreError::data("range is empty or negative"));
    }
    BlockRange::new(start as u64, (end_exclusive - 1) as u64)
        .map_err(|err| StoreError::data(err.to_string()))
}

pub(crate) fn addresses_to_db(addresses: &BTreeSet<Address>) -> Vec<Vec<u8>> {
    addresses.iter().map(|address| address.to_vec()).collect()
}

pub(crate) fn addresses_from_db(raw: Vec<Vec<u8>>) -> Result<BTreeSet<Address>, StoreError> {
    raw.into_iter()
        .map(|bytes| address_from_db(&bytes))
        .collect()
}

pub(crate) fn topics_to_db(topics: &[TopicFilter; 4]) -> Result<serde_json::Value, StoreError> {
    serde_json::to_value(topics).map_err(|err| StoreError::data(format!("selector topics: {err}")))
}

pub(crate) fn topics_from_db(value: serde_json::Value) -> Result<[TopicFilter; 4], StoreError> {
    if value.as_array().is_some_and(Vec::is_empty) {
        return Ok(Default::default());
    }
    let parsed: Vec<TopicFilter> = serde_json::from_value(value)
        .map_err(|err| StoreError::data(format!("selector topics: {err}")))?;
    if parsed.len() > 4 {
        return Err(StoreError::data("selector has more than 4 topic positions"));
    }
    let mut topics = <[TopicFilter; 4]>::default();
    for (slot, filter) in topics.iter_mut().zip(parsed) {
        *slot = filter;
    }
    Ok(topics)
}

/// EVM logs never contain a hole, and `LogRow.topics` is a dense prefix of the four positions.
pub(crate) fn topics_from_columns(columns: [Option<Vec<u8>>; 4]) -> Result<Vec<B256>, StoreError> {
    let mut topics = Vec::new();
    let mut ended = false;
    for column in columns {
        match column {
            Some(bytes) if !ended => topics.push(b256_from_db(&bytes)?),
            Some(_) => return Err(StoreError::data("log topics have a hole")),
            None => ended = true,
        }
    }
    Ok(topics)
}

pub(crate) fn topic_columns(topics: &[B256]) -> Result<[Option<Vec<u8>>; 4], StoreError> {
    if topics.len() > 4 {
        return Err(StoreError::invalid("log has more than 4 topics"));
    }
    let mut columns = [None, None, None, None];
    for (column, topic) in columns.iter_mut().zip(topics) {
        *column = Some(topic.to_vec());
    }
    Ok(columns)
}
