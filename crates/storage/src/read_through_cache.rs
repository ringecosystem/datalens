use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
};

use datalens_core::DatasetRows;

use crate::{ManifestEntry, ObjectEncoding};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadThroughCacheConfig {
    pub enabled: bool,
    pub max_entries: usize,
}

impl Default for ReadThroughCacheConfig {
    fn default() -> Self {
        Self::enabled(128)
    }
}

impl ReadThroughCacheConfig {
    pub fn enabled(max_entries: usize) -> Self {
        Self {
            enabled: true,
            max_entries: max_entries.max(1),
        }
    }

    pub fn disabled() -> Self {
        Self {
            enabled: false,
            max_entries: 0,
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
        let Some(inner) = &self.inner else {
            return;
        };
        inner.lock().expect("read-through cache").put(key, rows);
    }
}

#[derive(Debug)]
struct ReadThroughCacheInner {
    max_entries: usize,
    entries: HashMap<ReadThroughCacheKey, DatasetRows>,
    lru: VecDeque<ReadThroughCacheKey>,
}

impl ReadThroughCacheInner {
    fn get(&mut self, key: &ReadThroughCacheKey) -> Option<DatasetRows> {
        let rows = self.entries.get(key)?.clone();
        self.touch(key);
        Some(rows)
    }

    fn put(&mut self, key: ReadThroughCacheKey, rows: DatasetRows) {
        self.entries.insert(key.clone(), rows);
        self.touch(&key);
        while self.entries.len() > self.max_entries {
            let Some(evicted) = self.lru.pop_front() else {
                break;
            };
            self.entries.remove(&evicted);
        }
    }

    fn touch(&mut self, key: &ReadThroughCacheKey) {
        self.lru.retain(|existing| existing != key);
        self.lru.push_back(key.clone());
    }
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
