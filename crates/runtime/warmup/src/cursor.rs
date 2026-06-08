use datalens_core::{LedgerRange, LedgerRangeKind};
use serde::{Deserialize, Serialize};

use crate::WarmupTaskId;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
/// Warmup cursor for resumable task execution. It records scheduler progress
/// and failure position, while manifest coverage remains the durable authority
/// used to decide what data already exists.
pub struct WarmupCursor {
    pub task_id: WarmupTaskId,
    pub next: u64,
    pub last_committed: Option<u64>,
    pub current_attempt: u32,
    pub last_processed_range: Option<LedgerRange>,
    pub last_error: Option<String>,
    pub updated_at: u64,
}

impl WarmupCursor {
    pub fn new(task_id: WarmupTaskId, next: u64, now: u64) -> Self {
        Self {
            task_id,
            next,
            last_committed: None,
            current_attempt: 0,
            last_processed_range: None,
            last_error: None,
            updated_at: now,
        }
    }

    pub(crate) fn mark_committed(&mut self, range: LedgerRange, now: u64) {
        self.next = range.end().saturating_add(1);
        self.last_committed = Some(range.end());
        self.current_attempt = 0;
        self.last_processed_range = Some(range);
        self.last_error = None;
        self.updated_at = now;
    }

    pub(crate) fn realign(&mut self, next: u64, now: u64) {
        self.next = next;
        self.last_committed = None;
        self.current_attempt = 0;
        self.last_processed_range = None;
        self.last_error = None;
        self.updated_at = now;
    }

    pub(crate) fn mark_failure(
        &mut self,
        range: LedgerRange,
        attempts: u32,
        error: String,
        now: u64,
    ) {
        self.next = range.start();
        self.current_attempt = attempts;
        self.last_processed_range = Some(range);
        self.last_error = Some(error);
        self.updated_at = now;
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WarmupCheckpoint {
    pub task_id: WarmupTaskId,
    pub range_kind: LedgerRangeKind,
    pub committed_range: LedgerRange,
    pub rows_written: usize,
    pub provider_calls: u64,
}
