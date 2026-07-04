use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use datalens_core::DatalensError;
use datalens_storage::{ObjectListPage, ObjectMetadata, ObjectStore};

#[derive(Clone, Debug)]
pub struct CountingObjectStore<S> {
    inner: S,
    counts: Arc<Mutex<ObjectStoreCounts>>,
}

#[derive(Clone, Debug, Default)]
struct ObjectStoreCounts {
    gets: BTreeMap<String, usize>,
    puts: BTreeMap<String, usize>,
    lists: BTreeMap<String, usize>,
    list_pages: BTreeMap<String, usize>,
    deletes: BTreeMap<String, usize>,
}

impl<S> CountingObjectStore<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            counts: Arc::new(Mutex::new(ObjectStoreCounts::default())),
        }
    }

    pub fn get_count(&self, key: &str) -> usize {
        self.count_for(|counts| &counts.gets, key)
    }

    pub fn put_count(&self, key: &str) -> usize {
        self.count_for(|counts| &counts.puts, key)
    }

    pub fn list_count(&self, prefix: &str) -> usize {
        self.count_for(|counts| &counts.lists, prefix)
    }

    pub fn list_page_count(&self, prefix: &str) -> usize {
        self.count_for(|counts| &counts.list_pages, prefix)
    }

    pub fn delete_count(&self, key: &str) -> usize {
        self.count_for(|counts| &counts.deletes, key)
    }

    pub fn assert_no_overwrite(&self, prefix: &str) {
        let violations = self.overwrite_budget_violations_for_prefix(prefix);
        assert!(
            violations.is_empty(),
            "object store overwrites under prefix {prefix}: {violations:?}"
        );
    }

    pub fn assert_put_count_at_most(&self, key: &str, max_count: usize) {
        let put_count = self.put_count(key);
        assert!(
            put_count <= max_count,
            "object store put count for {key} exceeded budget {max_count}: {put_count}"
        );
    }

    pub fn overwrite_budget_violations(&self) -> Vec<(String, usize)> {
        self.overwrite_budget_violations_matching(|_| true)
    }

    pub fn overwrite_budget_violations_for_prefix(&self, prefix: &str) -> Vec<(String, usize)> {
        self.overwrite_budget_violations_matching(|key| key.starts_with(prefix))
    }

    fn count_for(
        &self,
        counts: impl FnOnce(&ObjectStoreCounts) -> &BTreeMap<String, usize>,
        key: &str,
    ) -> usize {
        counts(&self.counts.lock().expect("object store counts"))
            .get(key)
            .copied()
            .unwrap_or_default()
    }

    fn overwrite_budget_violations_matching(
        &self,
        matches: impl Fn(&str) -> bool,
    ) -> Vec<(String, usize)> {
        self.counts
            .lock()
            .expect("object store counts")
            .puts
            .iter()
            .filter(|(key, count)| matches(key) && **count > 1)
            .map(|(key, count)| (key.clone(), *count))
            .collect()
    }

    fn record(
        &self,
        operation: impl FnOnce(&mut ObjectStoreCounts) -> &mut BTreeMap<String, usize>,
        key: &str,
    ) {
        *operation(&mut self.counts.lock().expect("object store counts"))
            .entry(key.to_owned())
            .or_default() += 1;
    }
}

impl<S: ObjectStore> ObjectStore for CountingObjectStore<S> {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.record(|counts| &mut counts.gets, key);
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.record(|counts| &mut counts.puts, key);
        self.inner.put(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.record(|counts| &mut counts.lists, prefix);
        self.inner.list(prefix)
    }

    fn list_page(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<ObjectListPage, DatalensError> {
        self.record(|counts| &mut counts.list_pages, prefix);
        self.inner.list_page(prefix, start_after, limit)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.record(|counts| &mut counts.deletes, key);
        self.inner.delete(key)
    }

    fn lock_namespace(&self) -> String {
        self.inner.lock_namespace()
    }
}
