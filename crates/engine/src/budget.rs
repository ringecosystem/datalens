use std::sync::Mutex;
use std::time::{Duration, Instant};

const WINDOW: Duration = Duration::from_secs(60);

/// An application's `max_fill_blocks_per_minute`, counted per replica in a fixed window.
pub struct FillBudget {
    per_minute: u64,
    window: Mutex<(Instant, u64)>,
}

impl FillBudget {
    pub fn new(per_minute: u64) -> Self {
        Self {
            per_minute,
            window: Mutex::new((Instant::now(), 0)),
        }
    }

    /// Takes `blocks` from this minute's allowance. The first chunk of a window is always
    /// allowed, so a chunk larger than the whole allowance still makes progress.
    pub fn try_take(&self, blocks: u64) -> bool {
        let mut window = self
            .window
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = Instant::now();
        if now.duration_since(window.0) >= WINDOW {
            *window = (now, 0);
        }
        if window.1 > 0 && window.1.saturating_add(blocks) > self.per_minute {
            return false;
        }
        window.1 = window.1.saturating_add(blocks);
        true
    }
}
