use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use datalens_chain::ChainFetchRequest;

const PROBE_SUCCESS_THRESHOLD: u32 = 8;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ProviderRangeKey {
    chain_key: String,
    dataset_key: String,
    selector_key: String,
}

impl ProviderRangeKey {
    pub(crate) fn from_request(request: &ChainFetchRequest) -> Self {
        Self {
            chain_key: request.chain.key_prefix(),
            dataset_key: request.dataset_key.as_str().to_owned(),
            selector_key: request.selector.canonical_key(),
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct ProviderRangeController {
    state: Arc<Mutex<HashMap<ProviderRangeKey, ProviderRangeState>>>,
}

impl ProviderRangeController {
    pub(crate) fn effective_limit(
        &self,
        key: &ProviderRangeKey,
        configured_ceiling: Option<u64>,
    ) -> Option<u64> {
        let mut states = self.state.lock().expect("provider range state lock");
        let state = states.entry(key.clone()).or_default();
        state.update_ceiling(configured_ceiling);
        state.effective_limit()
    }

    pub(crate) fn record_provider_limit(
        &self,
        key: &ProviderRangeKey,
        configured_ceiling: Option<u64>,
        attempted_len: u128,
        hint_max_len: Option<u64>,
    ) -> Option<u64> {
        let mut states = self.state.lock().expect("provider range state lock");
        let state = states.entry(key.clone()).or_default();
        state.update_ceiling(configured_ceiling);
        state.record_provider_limit(attempted_len, hint_max_len);
        state.effective_limit()
    }

    pub(crate) fn record_success(
        &self,
        key: &ProviderRangeKey,
        configured_ceiling: Option<u64>,
        fetched_len: u128,
    ) {
        let Ok(fetched_len) = u64::try_from(fetched_len) else {
            return;
        };
        let mut states = self.state.lock().expect("provider range state lock");
        let state = states.entry(key.clone()).or_default();
        state.update_ceiling(configured_ceiling);
        state.record_success(fetched_len);
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ProviderRangeState {
    configured_ceiling: Option<u64>,
    current_max: Option<u64>,
    known_success_max: Option<u64>,
    failing_upper_bound: Option<u64>,
    hint_authoritative_max: Option<u64>,
    consecutive_successes: u32,
}

impl ProviderRangeState {
    fn update_ceiling(&mut self, configured_ceiling: Option<u64>) {
        self.configured_ceiling = configured_ceiling.filter(|value| *value > 0);
        if let (Some(current), Some(ceiling)) = (self.current_max, self.configured_ceiling) {
            self.current_max = Some(current.min(ceiling).max(1));
        }
        if let (Some(success), Some(ceiling)) = (self.known_success_max, self.configured_ceiling) {
            self.known_success_max = Some(success.min(ceiling).max(1));
        }
        if let (Some(failing), Some(ceiling)) = (self.failing_upper_bound, self.configured_ceiling)
        {
            self.failing_upper_bound = Some(failing.min(ceiling).max(1));
        }
        if let (Some(hint), Some(ceiling)) = (self.hint_authoritative_max, self.configured_ceiling)
        {
            self.hint_authoritative_max = Some(hint.min(ceiling).max(1));
        }
    }

    fn effective_limit(&self) -> Option<u64> {
        cap_to_authoritative_hint(
            cap_to_ceiling(
                self.current_max.or(self.configured_ceiling),
                self.configured_ceiling,
            ),
            self.hint_authoritative_max,
        )
    }

    fn configured_ceiling(&self) -> Option<u64> {
        cap_to_authoritative_hint(self.configured_ceiling, self.hint_authoritative_max)
    }

    fn cap_limit(&self, value: u64) -> Option<u64> {
        cap_to_authoritative_hint(
            cap_to_ceiling(Some(value), self.configured_ceiling),
            self.hint_authoritative_max,
        )
    }

    fn cap_failure_bound(&self, value: u64) -> Option<u64> {
        cap_to_ceiling(
            Some(value),
            self.configured_ceiling.or(self.hint_authoritative_max),
        )
    }

    fn cap_success_bound(&self, value: u64) -> Option<u64> {
        cap_to_authoritative_hint(
            cap_to_ceiling(Some(value), self.configured_ceiling),
            self.hint_authoritative_max,
        )
    }

    fn record_provider_limit(&mut self, attempted_len: u128, hint_max_len: Option<u64>) {
        self.consecutive_successes = 0;
        let attempted_len = u64::try_from(attempted_len).unwrap_or(u64::MAX).max(1);
        let next = if let Some(hint_max_len) = hint_max_len {
            let hinted =
                cap_to_ceiling(Some(hint_max_len.max(1)), self.configured_ceiling).unwrap_or(1);
            self.hint_authoritative_max = Some(hinted);
            self.known_success_max = self.known_success_max.map(|success| success.min(hinted));
            self.record_failing_upper_bound(hinted);
            hinted
        } else if let Some(success) = self.known_success_max {
            self.record_failing_upper_bound(attempted_len);
            let next = success.min(attempted_len.saturating_sub(1).max(1));
            self.known_success_max = Some(next);
            next
        } else {
            self.record_failing_upper_bound(attempted_len);
            (attempted_len / 2).max(1)
        };
        self.current_max = self.cap_limit(next);
    }

    fn record_success(&mut self, fetched_len: u64) {
        let Some(current) = self.current_max else {
            return;
        };
        if fetched_len < current {
            return;
        }
        self.known_success_max = self.cap_success_bound(
            self.known_success_max
                .map(|last| last.max(fetched_len))
                .unwrap_or(fetched_len),
        );
        let Some(ceiling) = self.configured_ceiling() else {
            return;
        };
        if current >= ceiling {
            return;
        }
        self.consecutive_successes = self.consecutive_successes.saturating_add(1);
        if self.consecutive_successes < PROBE_SUCCESS_THRESHOLD {
            return;
        }
        self.consecutive_successes = 0;
        let upper = self.failing_upper_bound.unwrap_or(ceiling).min(ceiling);
        if upper <= current {
            return;
        }
        let probed = midpoint(current, upper);
        let next = if self.failing_upper_bound.is_some() {
            probed
        } else {
            probed.max(current + 1)
        };
        self.current_max = Some(next.min(ceiling));
    }

    fn record_failing_upper_bound(&mut self, attempted_len: u64) {
        let Some(attempted_len) = self.cap_failure_bound(attempted_len) else {
            return;
        };
        self.failing_upper_bound = Some(match self.failing_upper_bound {
            Some(upper) if upper <= attempted_len => upper,
            _ => attempted_len,
        });
    }
}

fn midpoint(lower: u64, upper: u64) -> u64 {
    lower + ((upper - lower) / 2)
}

fn cap_to_ceiling(value: Option<u64>, ceiling: Option<u64>) -> Option<u64> {
    match (value, ceiling) {
        (Some(value), Some(ceiling)) => Some(value.min(ceiling).max(1)),
        (Some(value), None) => Some(value.max(1)),
        (None, Some(ceiling)) => Some(ceiling.max(1)),
        (None, None) => None,
    }
}

fn cap_to_authoritative_hint(value: Option<u64>, hint: Option<u64>) -> Option<u64> {
    match (value, hint) {
        (Some(value), Some(hint)) => Some(value.min(hint).max(1)),
        (None, Some(hint)) => Some(hint.max(1)),
        (Some(value), None) => Some(value.max(1)),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ProviderRangeKey {
        ProviderRangeKey {
            chain_key: "evm/ethereum/1".to_owned(),
            dataset_key: "evm.blocks".to_owned(),
            selector_key: "all".to_owned(),
        }
    }

    #[test]
    fn test_provider_range_controller_uses_hint_directly() {
        let controller = ProviderRangeController::default();
        let key = key();

        assert_eq!(controller.effective_limit(&key, Some(5_000)), Some(5_000));
        assert_eq!(
            controller.record_provider_limit(&key, Some(5_000), 5_000, Some(1_000)),
            Some(1_000)
        );
        assert_eq!(controller.effective_limit(&key, Some(5_000)), Some(1_000));
    }

    #[test]
    fn test_provider_range_controller_bisects_without_hint_then_probes_upward() {
        let controller = ProviderRangeController::default();
        let key = key();

        assert_eq!(
            controller.record_provider_limit(&key, Some(5_000), 5_000, None),
            Some(2_500)
        );
        for _ in 0..PROBE_SUCCESS_THRESHOLD {
            controller.record_success(&key, Some(5_000), 2_500);
        }
        assert_eq!(controller.effective_limit(&key, Some(5_000)), Some(3_750));
    }

    #[test]
    fn test_provider_range_controller_ignores_incidental_success_before_no_hint_failure() {
        let controller = ProviderRangeController::default();
        let key = key();

        controller.record_success(&key, Some(10_000), 100);
        assert_eq!(
            controller.record_provider_limit(&key, Some(10_000), 10_000, None),
            Some(5_000)
        );
    }

    #[test]
    fn test_provider_range_controller_keeps_known_success_after_no_hint_probe_failure() {
        let controller = ProviderRangeController::default();
        let key = key();

        assert_eq!(
            controller.record_provider_limit(&key, Some(5_000), 5_000, None),
            Some(2_500)
        );
        controller.record_success(&key, Some(5_000), 2_500);
        assert_eq!(
            controller.record_provider_limit(&key, Some(5_000), 3_750, None),
            Some(2_500)
        );
    }

    #[test]
    fn test_provider_range_controller_probe_failure_returns_to_known_success() {
        let controller = ProviderRangeController::default();
        let key = key();

        assert_eq!(
            controller.record_provider_limit(&key, Some(10_000), 10_000, None),
            Some(5_000)
        );
        for _ in 0..PROBE_SUCCESS_THRESHOLD {
            controller.record_success(&key, Some(10_000), 5_000);
        }
        assert_eq!(controller.effective_limit(&key, Some(10_000)), Some(7_500));
        assert_eq!(
            controller.record_provider_limit(&key, Some(10_000), 7_500, None),
            Some(5_000)
        );
        for _ in 0..PROBE_SUCCESS_THRESHOLD {
            controller.record_success(&key, Some(10_000), 5_000);
        }
        assert_eq!(controller.effective_limit(&key, Some(10_000)), Some(6_250));
    }

    #[test]
    fn test_provider_range_controller_authoritative_hint_does_not_probe_upward() {
        let controller = ProviderRangeController::default();
        let key = key();

        assert_eq!(
            controller.record_provider_limit(&key, Some(10_000), 10_000, None),
            Some(5_000)
        );
        controller.record_success(&key, Some(10_000), 5_000);
        assert_eq!(
            controller.record_provider_limit(&key, Some(10_000), 5_000, Some(1_000)),
            Some(1_000)
        );
        for _ in 0..PROBE_SUCCESS_THRESHOLD {
            controller.record_success(&key, Some(10_000), 1_000);
        }
        assert_eq!(controller.effective_limit(&key, Some(10_000)), Some(1_000));
    }
}
