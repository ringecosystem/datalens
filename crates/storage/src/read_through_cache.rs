use std::{
    collections::{HashMap, VecDeque},
    mem::{size_of, size_of_val},
    sync::{Arc, Mutex},
};

use datalens_core::{
    BlockHeader, DatasetRows, EvmBlockHeader, EvmReceipt, EvmTransaction, LogRecord, QueryRows,
};

use crate::{ManifestEntry, ObjectEncoding};

const DEFAULT_READ_THROUGH_CACHE_MAX_BYTES: usize = 512 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadThroughCacheConfig {
    pub enabled: bool,
    pub max_entries: usize,
    pub max_bytes: usize,
}

impl Default for ReadThroughCacheConfig {
    fn default() -> Self {
        Self::enabled_with_byte_budget(128, DEFAULT_READ_THROUGH_CACHE_MAX_BYTES)
    }
}

impl ReadThroughCacheConfig {
    pub fn enabled(max_entries: usize) -> Self {
        Self::enabled_with_byte_budget(max_entries, DEFAULT_READ_THROUGH_CACHE_MAX_BYTES)
    }

    pub fn enabled_with_byte_budget(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            enabled: true,
            max_entries: max_entries.max(1),
            max_bytes: max_bytes.max(1),
        }
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            max_entries: 0,
            max_bytes: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReadThroughCache {
    inner: Option<Arc<Mutex<ReadThroughCacheInner>>>,
}

impl ReadThroughCache {
    pub(crate) fn new(config: ReadThroughCacheConfig) -> Self {
        if !config.enabled {
            return Self { inner: None };
        }
        Self {
            inner: Some(Arc::new(Mutex::new(ReadThroughCacheInner {
                max_entries: config.max_entries.max(1),
                max_bytes: config.max_bytes.max(1),
                current_bytes: 0,
                entries: HashMap::new(),
                lru: VecDeque::new(),
            }))),
        }
    }

    pub(crate) fn get(
        &self,
        object_key: &str,
        entry: &ManifestEntry,
        encoding: ObjectEncoding,
    ) -> Option<DatasetRows> {
        let key = cache_key(object_key, entry, encoding)?;
        let Some(inner) = &self.inner else {
            return None;
        };
        inner.lock().expect("read-through cache").get(&key)
    }

    pub(crate) fn put(
        &self,
        object_key: &str,
        entry: &ManifestEntry,
        encoding: ObjectEncoding,
        rows: DatasetRows,
    ) {
        let Some(key) = cache_key(object_key, entry, encoding) else {
            return;
        };
        let Some(object_byte_len) = entry
            .object_size_bytes
            .and_then(|value| usize::try_from(value).ok())
        else {
            return;
        };
        let byte_len = estimated_cached_rows_bytes(&rows, object_byte_len);
        let Some(inner) = &self.inner else {
            return;
        };
        inner
            .lock()
            .expect("read-through cache")
            .put(key, rows, byte_len);
    }
}

#[derive(Debug)]
struct ReadThroughCacheInner {
    max_entries: usize,
    max_bytes: usize,
    current_bytes: usize,
    entries: HashMap<ReadThroughCacheKey, ReadThroughCacheEntry>,
    lru: VecDeque<ReadThroughCacheKey>,
}

impl ReadThroughCacheInner {
    fn get(&mut self, key: &ReadThroughCacheKey) -> Option<DatasetRows> {
        let rows = self.entries.get(key)?.rows.clone();
        self.touch(key);
        Some(rows)
    }

    fn put(&mut self, key: ReadThroughCacheKey, rows: DatasetRows, byte_len: usize) {
        if byte_len > self.max_bytes {
            return;
        }
        if let Some(existing) = self.entries.remove(&key) {
            self.current_bytes = self.current_bytes.saturating_sub(existing.byte_len);
        }
        self.entries
            .insert(key.clone(), ReadThroughCacheEntry { rows, byte_len });
        self.current_bytes = self.current_bytes.saturating_add(byte_len);
        self.touch(&key);
        while self.entries.len() > self.max_entries || self.current_bytes > self.max_bytes {
            let Some(evicted) = self.lru.pop_front() else {
                break;
            };
            if let Some(entry) = self.entries.remove(&evicted) {
                self.current_bytes = self.current_bytes.saturating_sub(entry.byte_len);
            }
        }
    }

    fn touch(&mut self, key: &ReadThroughCacheKey) {
        self.lru.retain(|existing| existing != key);
        self.lru.push_back(key.clone());
    }
}

#[derive(Clone, Debug)]
struct ReadThroughCacheEntry {
    rows: DatasetRows,
    byte_len: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ReadThroughCacheKey {
    object_key: String,
    object_encoding: ObjectEncoding,
    object_size_bytes: Option<u64>,
    checksum: String,
    checksum_algorithm: String,
    written_at_unix_seconds: Option<u64>,
}

fn cache_key(
    object_key: &str,
    entry: &ManifestEntry,
    encoding: ObjectEncoding,
) -> Option<ReadThroughCacheKey> {
    let checksum = entry.checksum.clone()?;
    let checksum_algorithm = entry.checksum_algorithm.clone()?;
    Some(ReadThroughCacheKey {
        object_key: object_key.to_owned(),
        object_encoding: encoding,
        object_size_bytes: entry.object_size_bytes,
        checksum,
        checksum_algorithm,
        written_at_unix_seconds: entry.written_at_unix_seconds,
    })
}

fn estimated_cached_rows_bytes(rows: &DatasetRows, object_byte_len: usize) -> usize {
    (size_of::<DatasetRows>() + estimated_query_rows_bytes(rows.rows())).max(object_byte_len)
}

fn estimated_query_rows_bytes(rows: &QueryRows) -> usize {
    size_of::<QueryRows>()
        + match rows {
            QueryRows::EvmBlocks(rows) => estimated_vec_bytes(rows, estimated_block_header_bytes),
            QueryRows::EvmBlockHeaders(rows) => {
                estimated_vec_bytes(rows, estimated_evm_block_header_bytes)
            }
            QueryRows::EvmTransactions(rows) => {
                estimated_vec_bytes(rows, estimated_evm_transaction_bytes)
            }
            QueryRows::EvmReceipts(rows) => estimated_vec_bytes(rows, estimated_evm_receipt_bytes),
            QueryRows::EvmLogs(rows) => estimated_vec_bytes(rows, estimated_log_record_bytes),
            QueryRows::AdapterJson { dataset_key, rows } => {
                size_of_val(dataset_key)
                    + rows.capacity() * size_of::<serde_json::Value>()
                    + rows.iter().map(estimated_json_value_bytes).sum::<usize>()
            }
        }
}

fn estimated_vec_bytes<T>(rows: &[T], estimate_row: fn(&T) -> usize) -> usize {
    size_of::<Vec<T>>() + size_of_val(rows) + rows.iter().map(estimate_row).sum::<usize>()
}

fn estimated_block_header_bytes(row: &BlockHeader) -> usize {
    row.hash.capacity() + row.parent_hash.capacity()
}

fn estimated_evm_block_header_bytes(row: &EvmBlockHeader) -> usize {
    row.block_hash.capacity() + row.parent_hash.capacity() + row.logs_bloom.capacity()
}

fn estimated_evm_transaction_bytes(row: &EvmTransaction) -> usize {
    row.hash.capacity()
        + row.block_hash.capacity()
        + row.from.capacity()
        + estimated_option_string_bytes(&row.to)
        + row.value.capacity()
        + row.input.capacity()
        + estimated_option_string_bytes(&row.gas_price)
        + estimated_option_string_bytes(&row.max_fee_per_gas)
        + estimated_option_string_bytes(&row.max_priority_fee_per_gas)
        + estimated_option_string_bytes(&row.transaction_type)
}

fn estimated_evm_receipt_bytes(row: &EvmReceipt) -> usize {
    row.transaction_hash.capacity()
        + row.block_hash.capacity()
        + estimated_option_string_bytes(&row.effective_gas_price)
        + estimated_option_string_bytes(&row.contract_address)
        + estimated_option_string_bytes(&row.logs_bloom)
}

fn estimated_log_record_bytes(row: &LogRecord) -> usize {
    row.block_hash.capacity()
        + estimated_option_string_bytes(&row.parent_hash)
        + row.transaction_hash.capacity()
        + row.address.capacity()
        + row.topics.capacity() * size_of::<String>()
        + row
            .topics
            .iter()
            .map(|topic| topic.capacity())
            .sum::<usize>()
        + row.data.capacity()
}

fn estimated_option_string_bytes(value: &Option<String>) -> usize {
    value.as_ref().map(|value| value.capacity()).unwrap_or(0)
}

fn estimated_json_value_bytes(value: &serde_json::Value) -> usize {
    size_of::<serde_json::Value>()
        + match value {
            serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {
                0
            }
            serde_json::Value::String(value) => value.capacity(),
            serde_json::Value::Array(values) => {
                values.capacity() * size_of::<serde_json::Value>()
                    + values.iter().map(estimated_json_value_bytes).sum::<usize>()
            }
            serde_json::Value::Object(values) => values
                .iter()
                .map(|(key, value)| key.capacity() + estimated_json_value_bytes(value))
                .sum(),
        }
}

#[cfg(test)]
mod tests {
    use datalens_core::{BlockHeader, DatasetKey, LogRecord, QueryRows};

    use super::*;

    #[test]
    fn test_estimated_cached_rows_bytes_counts_decoded_rows_without_serializing() {
        let rows = DatasetRows::new(
            DatasetKey::evm_blocks(),
            QueryRows::EvmBlocks(vec![BlockHeader {
                number: 1,
                hash: "0xblock".repeat(32),
                parent_hash: "0xparent".repeat(32),
                timestamp: 1,
            }]),
        )
        .expect("dataset rows");

        assert!(estimated_cached_rows_bytes(&rows, 1) > 1);
    }

    #[test]
    fn test_estimated_cached_rows_bytes_uses_object_size_as_floor() {
        let rows = DatasetRows::new(DatasetKey::evm_blocks(), QueryRows::EvmBlocks(Vec::new()))
            .expect("dataset rows");

        assert_eq!(estimated_cached_rows_bytes(&rows, 8192), 8192);
    }

    #[test]
    fn test_estimated_cached_rows_bytes_accounts_for_log_payload_heap() {
        let small_rows = DatasetRows::new(
            DatasetKey::evm_logs(),
            QueryRows::EvmLogs(vec![log_record("0x01")]),
        )
        .expect("dataset rows");
        let large_rows = DatasetRows::new(
            DatasetKey::evm_logs(),
            QueryRows::EvmLogs(vec![log_record(&format!("0x{}", "ab".repeat(4096)))]),
        )
        .expect("dataset rows");

        assert!(
            estimated_cached_rows_bytes(&large_rows, 1)
                > estimated_cached_rows_bytes(&small_rows, 1) + 4096
        );
    }

    fn log_record(data: &str) -> LogRecord {
        LogRecord::try_new(
            1,
            "0xblock".repeat(8),
            "0xtx".repeat(16),
            0,
            0,
            "0x0000000000000000000000000000000000000001",
            vec![format!("0x{}", "11".repeat(32))],
            data.to_owned(),
            false,
        )
        .expect("log record")
    }
}
