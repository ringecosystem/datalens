use std::collections::HashMap;
use std::mem;
use std::sync::Mutex;

use chrono::{NaiveDate, Utc};
use datalens_core::{ChainId, Dataset};
use datalens_store::{Store, StoreError, UsageDelta};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UsageCounts {
    pub queries: i64,
    pub rows_returned: i64,
    pub blocks_served: i64,
    pub blocks_filled: i64,
    pub provider_calls: i64,
}

impl UsageCounts {
    fn add(&mut self, other: &UsageCounts) {
        self.queries += other.queries;
        self.rows_returned += other.rows_returned;
        self.blocks_served += other.blocks_served;
        self.blocks_filled += other.blocks_filled;
        self.provider_calls += other.provider_calls;
    }
}

type Key = (NaiveDate, String, ChainId, Dataset);

/// Per-process usage totals, summed in memory and flushed in one upsert.
#[derive(Default)]
pub struct UsageBook {
    pending: Mutex<HashMap<Key, UsageCounts>>,
}

impl UsageBook {
    pub fn record(&self, application: &str, chain: ChainId, dataset: Dataset, add: UsageCounts) {
        let key = (
            Utc::now().date_naive(),
            application.to_owned(),
            chain,
            dataset,
        );
        lock(&self.pending).entry(key).or_default().add(&add);
    }

    /// Writes everything recorded so far. On failure the totals are kept for the next flush.
    pub async fn flush(&self, store: &Store) -> Result<(), StoreError> {
        let taken = mem::take(&mut *lock(&self.pending));
        if taken.is_empty() {
            return Ok(());
        }
        let deltas: Vec<UsageDelta> = taken
            .iter()
            .map(|((day, application, chain, dataset), counts)| UsageDelta {
                day: *day,
                application_id: application.clone(),
                chain_id: *chain,
                dataset: *dataset,
                queries: counts.queries,
                rows_returned: counts.rows_returned,
                blocks_served: counts.blocks_served,
                blocks_filled: counts.blocks_filled,
                provider_calls: counts.provider_calls,
            })
            .collect();
        if let Err(error) = store.add_usage(&deltas).await {
            let mut pending = lock(&self.pending);
            for (key, counts) in taken {
                pending.entry(key).or_default().add(&counts);
            }
            return Err(error);
        }
        Ok(())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
