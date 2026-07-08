use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use datalens_core::DatasetRows;

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
        let Some(byte_len) = entry
            .object_size_bytes
            .and_then(|value| usize::try_from(value).ok())
        else {
            return;
        };
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
