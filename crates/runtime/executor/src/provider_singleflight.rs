use std::{
    collections::BTreeMap,
    sync::{Arc, Condvar, Mutex},
};

use datalens_chain::{ChainFetchRequest, ChainFetchResponse};
use datalens_core::{DatalensError, LedgerRangeKind};

#[derive(Clone, Debug, Default)]
pub(crate) struct ProviderSingleflight {
    in_flight: Arc<Mutex<BTreeMap<String, Arc<ProviderFetchSlot>>>>,
}

#[derive(Debug)]
struct ProviderFetchSlot {
    state: Mutex<ProviderFetchState>,
    completed: Condvar,
}

#[derive(Debug, Default)]
struct ProviderFetchState {
    result: Option<Result<ChainFetchResponse, DatalensError>>,
}

struct ProviderFetchLeaderCleanup {
    in_flight: Arc<Mutex<BTreeMap<String, Arc<ProviderFetchSlot>>>>,
    slot: Arc<ProviderFetchSlot>,
    key: String,
    active: bool,
}

impl ProviderFetchLeaderCleanup {
    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for ProviderFetchLeaderCleanup {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = self.slot.state.lock()
            && state.result.is_none()
        {
            state.result = Some(Err(DatalensError::internal("provider fetch abandoned")));
            self.slot.completed.notify_all();
        }
        if let Ok(mut in_flight) = self.in_flight.lock() {
            in_flight.remove(&self.key);
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ProviderSingleflightResult {
    pub(crate) response: ChainFetchResponse,
    pub(crate) shared: bool,
}

impl ProviderSingleflight {
    pub(crate) fn fetch(
        &self,
        request: &ChainFetchRequest,
        fetch: impl FnOnce() -> Result<ChainFetchResponse, DatalensError>,
    ) -> Result<ProviderSingleflightResult, DatalensError> {
        let key = provider_fetch_key(request);
        let (slot, leader) = {
            let mut in_flight = self
                .in_flight
                .lock()
                .map_err(|_| DatalensError::internal("provider singleflight lock poisoned"))?;
            if let Some(slot) = in_flight.get(&key) {
                (slot.clone(), false)
            } else {
                let slot = Arc::new(ProviderFetchSlot {
                    state: Mutex::new(ProviderFetchState::default()),
                    completed: Condvar::new(),
                });
                in_flight.insert(key.clone(), slot.clone());
                (slot, true)
            }
        };

        if leader {
            let mut cleanup = ProviderFetchLeaderCleanup {
                in_flight: self.in_flight.clone(),
                slot: slot.clone(),
                key: key.clone(),
                active: true,
            };
            let result = fetch();
            {
                let mut state = slot
                    .state
                    .lock()
                    .map_err(|_| DatalensError::internal("provider singleflight slot poisoned"))?;
                state.result = Some(result.clone());
                slot.completed.notify_all();
            }
            let mut in_flight = self
                .in_flight
                .lock()
                .map_err(|_| DatalensError::internal("provider singleflight lock poisoned"))?;
            in_flight.remove(&key);
            cleanup.disarm();
            return result.map(|response| ProviderSingleflightResult {
                response,
                shared: false,
            });
        }

        let mut state = slot
            .state
            .lock()
            .map_err(|_| DatalensError::internal("provider singleflight slot poisoned"))?;
        loop {
            if let Some(result) = state.result.clone() {
                return result.map(|response| ProviderSingleflightResult {
                    response,
                    shared: true,
                });
            }
            state = slot
                .completed
                .wait(state)
                .map_err(|_| DatalensError::internal("provider singleflight slot poisoned"))?;
        }
    }
}

fn provider_fetch_key(request: &ChainFetchRequest) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}|cache_write={}",
        request.chain.key_prefix(),
        request.dataset_key.as_str(),
        request.selector.fingerprint(),
        request.selector.canonical_key(),
        range_kind_key(request.range.kind()),
        request.range.start(),
        request.range.end(),
        request.context.cache_write,
    )
}

fn range_kind_key(kind: LedgerRangeKind) -> String {
    match kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(value) => value,
    }
}
