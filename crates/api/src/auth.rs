use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::http::HeaderMap;
use datalens_core::wire::QueryRequest;
use datalens_core::{ChainName, Dataset, Error};
use datalens_engine::FillBudget;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub const APPLICATION_HEADER: &str = "x-datalens-application";

#[derive(Debug, Clone)]
pub struct Quota {
    pub max_range_blocks: u64,
    pub max_requests_per_minute: u32,
    pub max_concurrent_requests: u32,
    pub max_fill_blocks_per_minute: Option<u64>,
    pub allow_unfiltered_logs: bool,
}

#[derive(Clone)]
pub struct Application {
    pub id: String,
    pub token: String,
    pub chains: BTreeSet<ChainName>,
    pub datasets: BTreeSet<Dataset>,
    pub quota: Quota,
    /// May call `/v1/admin/*`.
    pub admin: bool,
}

/// An application plus its per-replica quota counters.
pub(crate) struct Tenant {
    pub(crate) app: Application,
    requests: Mutex<RequestBucket>,
    concurrent: Arc<Semaphore>,
    pub(crate) fill: Option<FillBudget>,
}

pub(crate) struct Tenants(HashMap<String, Arc<Tenant>>);

impl Tenants {
    pub(crate) fn new(applications: Vec<Application>) -> Self {
        Self(
            applications
                .into_iter()
                .map(|app| {
                    let tenant = Tenant {
                        requests: Mutex::new(RequestBucket::new(app.quota.max_requests_per_minute)),
                        concurrent: Arc::new(Semaphore::new(
                            app.quota.max_concurrent_requests as usize,
                        )),
                        fill: app.quota.max_fill_blocks_per_minute.map(FillBudget::new),
                        app,
                    };
                    (tenant.app.id.clone(), Arc::new(tenant))
                })
                .collect(),
        )
    }

    /// `x-datalens-application` names the application. `Authorization: Bearer` must carry its token.
    pub(crate) fn authenticate(&self, headers: &HeaderMap) -> Result<Arc<Tenant>, Error> {
        let unauthorized = || Error::Unauthorized("unknown application or token".to_owned());
        let id = headers
            .get(APPLICATION_HEADER)
            .and_then(|value| value.to_str().ok())
            .ok_or_else(unauthorized)?;
        let token = bearer(headers).ok_or_else(unauthorized)?;
        let tenant = self.0.get(id).ok_or_else(unauthorized)?;
        if constant_time_eq(token.as_bytes(), tenant.app.token.as_bytes()) {
            Ok(Arc::clone(tenant))
        } else {
            Err(unauthorized())
        }
    }
}

impl Tenant {
    pub(crate) fn allow_chain(&self, chain: &ChainName) -> Result<(), Error> {
        if self.app.chains.contains(chain) {
            Ok(())
        } else {
            Err(Error::Forbidden(format!(
                "application {} may not read chain {chain}",
                self.app.id
            )))
        }
    }

    pub(crate) fn require_admin(&self) -> Result<(), Error> {
        if self.app.admin {
            Ok(())
        } else {
            Err(Error::Forbidden(format!(
                "application {} is not an admin",
                self.app.id
            )))
        }
    }

    /// Allowlist and quota checks. The permit holds a concurrency slot until dropped.
    pub(crate) fn admit(&self, request: &QueryRequest) -> Result<OwnedSemaphorePermit, Error> {
        let quota = &self.app.quota;
        self.allow_chain(&request.chain)?;
        if !self.app.datasets.contains(&request.dataset) {
            return Err(Error::Forbidden(format!(
                "application {} may not read {}",
                self.app.id, request.dataset
            )));
        }
        let unfiltered = request.dataset == Dataset::EvmLogs
            && request
                .selector
                .as_ref()
                .is_none_or(|selector| selector.addresses.is_empty());
        if unfiltered && !quota.allow_unfiltered_logs {
            return Err(Error::Forbidden(
                "evm.logs queries must filter by address".to_owned(),
            ));
        }
        if request.range.len() > quota.max_range_blocks {
            return Err(Error::QuotaExceeded(format!(
                "range of {} blocks exceeds max_range_blocks {}",
                request.range.len(),
                quota.max_range_blocks
            )));
        }
        if !self
            .requests
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .try_take()
        {
            return Err(Error::QuotaExceeded(format!(
                "more than {} requests per minute",
                quota.max_requests_per_minute
            )));
        }
        Arc::clone(&self.concurrent)
            .try_acquire_owned()
            .map_err(|_| {
                Error::QuotaExceeded(format!(
                    "more than {} concurrent requests",
                    quota.max_concurrent_requests
                ))
            })
    }
}

/// Token bucket holding one minute of requests, refilled continuously.
struct RequestBucket {
    capacity: f64,
    tokens: f64,
    at: Instant,
}

impl RequestBucket {
    fn new(per_minute: u32) -> Self {
        Self {
            capacity: f64::from(per_minute),
            tokens: f64::from(per_minute),
            at: Instant::now(),
        }
    }

    fn try_take(&mut self) -> bool {
        let now = Instant::now();
        let refill = now.duration_since(self.at).as_secs_f64() * self.capacity / 60.0;
        self.tokens = (self.tokens + refill).min(self.capacity);
        self.at = now;
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub(crate) fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
}

/// Compares without an early exit, so timing reveals only the length.
pub(crate) fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0_u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}
