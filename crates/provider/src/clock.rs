use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch. Range TTL and the daily counter read this
/// so tests can move time without sleeping.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

/// Test clock. Starts at 0.
#[derive(Clone, Debug, Default)]
pub struct ManualClock {
    ms: Arc<AtomicU64>,
}

impl ManualClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn advance(&self, ms: u64) {
        self.ms.fetch_add(ms, Ordering::Relaxed);
    }

    pub fn set(&self, ms: u64) {
        self.ms.store(ms, Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.ms.load(Ordering::Relaxed)
    }
}
