use std::collections::HashMap;
use std::sync::Arc;

use datalens_core::Dataset;

use crate::{Clock, ProviderError};

const SUCCESS_STREAK: u8 = 8;
pub(crate) const RANGE_TTL_MS: u64 = 10 * 60 * 1000;

#[derive(Debug, Clone)]
struct RangeState {
    current: u64,
    successes: u8,
    updated_ms: u64,
}

pub(crate) struct RangeBook {
    max: u64,
    min: u64,
    states: HashMap<Dataset, RangeState>,
    pub(crate) clock: Arc<dyn Clock>,
}

impl RangeBook {
    pub(crate) fn new(max: u64, min: u64, clock: Arc<dyn Clock>) -> Self {
        Self {
            max,
            min,
            states: HashMap::new(),
            clock,
        }
    }

    pub(crate) fn current(&mut self, dataset: Dataset) -> u64 {
        let now = self.clock.now_ms();
        self.state(dataset, now).current
    }

    pub(crate) fn record_success(&mut self, dataset: Dataset) {
        let now = self.clock.now_ms();
        let max = self.max;
        let state = self.state(dataset, now);
        state.successes = state.successes.saturating_add(1);
        if state.successes >= SUCCESS_STREAK {
            state.current = state.current.saturating_mul(2).min(max);
            state.successes = 0;
        }
        state.updated_ms = now;
    }

    /// Halves the window. At the floor the error is returned unchanged so the
    /// caller does not keep splitting down to a single block.
    pub(crate) fn record_limit(&mut self, dataset: Dataset) -> Result<u64, ProviderError> {
        let now = self.clock.now_ms();
        let min = self.min;
        let state = self.state(dataset, now);
        state.successes = 0;
        state.updated_ms = now;
        if state.current <= min {
            return Err(ProviderError::Limit);
        }
        state.current = (state.current / 2).max(min);
        Ok(state.current)
    }

    fn state(&mut self, dataset: Dataset, now: u64) -> &mut RangeState {
        let max = self.max;
        let state = self.states.entry(dataset).or_insert(RangeState {
            current: max,
            successes: 0,
            updated_ms: now,
        });
        if now.saturating_sub(state.updated_ms) >= RANGE_TTL_MS {
            state.current = max;
            state.successes = 0;
            state.updated_ms = now;
        }
        state
    }
}
