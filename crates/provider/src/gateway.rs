use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datalens_core::{ChainName, Dataset};
use serde_json::{Value, json};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;

use crate::ProviderError;
use crate::budget::{AcquireFail, Budget, Parked};
use crate::clock::{Clock, SystemClock};
use crate::error::{BuildError, RETRY_AFTER_CAP, classify_rpc_message, parse_retry_after};
use crate::metrics::ProviderMetrics;
use crate::purpose::{Purpose, TokenWait};
use crate::range::RangeBook;

const ATTEMPTS: u32 = 3;

/// Per-chain gateway settings.
///
/// `rps`, `burst`, and `daily_calls` apply to this process only. Each replica
/// and each role process spends the full configured budget, and `daily_calls`
/// resets when the process restarts. `url` may embed an API key, so [`Debug`] redacts it.
pub struct ProviderConfig {
    pub url: String,
    pub rps: NonZeroU32,
    pub burst: NonZeroU32,
    pub daily_calls: Option<NonZeroU64>,
    pub max_in_flight: NonZeroU32,
    pub min_range_blocks: NonZeroU64,
    pub max_get_logs_range_blocks: NonZeroU64,
}

impl fmt::Debug for ProviderConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderConfig")
            .field("url", &"<redacted>")
            .field("rps", &self.rps)
            .field("burst", &self.burst)
            .field("daily_calls", &self.daily_calls)
            .field("max_in_flight", &self.max_in_flight)
            .field("min_range_blocks", &self.min_range_blocks)
            .field("max_get_logs_range_blocks", &self.max_get_logs_range_blocks)
            .finish()
    }
}

struct Inner {
    chain: ChainName,
    url: String,
    http: reqwest::Client,
    budget: Mutex<Budget>,
    in_flight: Arc<Semaphore>,
    ranges: Mutex<RangeBook>,
    metrics: ProviderMetrics,
}

/// One chain's JSON-RPC gateway. Clone shares the budget, range state, and client.
#[derive(Clone)]
pub struct Gateway {
    inner: Arc<Inner>,
}

impl Gateway {
    pub fn new(
        chain: ChainName,
        config: ProviderConfig,
        metrics: ProviderMetrics,
    ) -> Result<Self, BuildError> {
        if !is_http_url(&config.url) {
            return Err(BuildError::BadUrl);
        }
        let max = config.max_get_logs_range_blocks.get();
        let min = config.min_range_blocks.get();
        if min > max {
            return Err(BuildError::Range { min, max });
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| BuildError::Client(error.to_string()))?;
        let budget = Budget::new(
            config.rps.get(),
            config.burst.get(),
            config.daily_calls.map(NonZeroU64::get),
            tokio::time::Instant::now(),
        );
        metrics.set_tokens(chain.as_str(), budget.tokens());
        let inner = Arc::new(Inner {
            chain,
            url: config.url,
            http,
            budget: Mutex::new(budget),
            in_flight: Arc::new(Semaphore::new(config.max_in_flight.get() as usize)),
            ranges: Mutex::new(RangeBook::new(max, min, Arc::new(SystemClock))),
            metrics,
        });
        Ok(Self { inner })
    }

    pub fn chain(&self) -> &ChainName {
        &self.inner.chain
    }

    pub fn set_clock(&self, clock: Arc<dyn Clock>) {
        self.lock_ranges().clock = clock;
    }

    pub fn range_blocks(&self, dataset: Dataset) -> u64 {
        self.lock_ranges().current(dataset)
    }

    pub fn record_success(&self, dataset: Dataset) {
        self.lock_ranges().record_success(dataset);
    }

    pub fn record_limit(&self, dataset: Dataset, failed_blocks: u64) -> Result<u64, ProviderError> {
        self.lock_ranges().record_limit(dataset, failed_blocks)
    }

    pub async fn call(
        &self,
        method: &str,
        params: Value,
        purpose: Purpose,
        application: Option<&str>,
        wait: TokenWait,
    ) -> Result<Value, ProviderError> {
        let app = application_label(application);
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        for attempt in 1..=ATTEMPTS {
            self.acquire(purpose, wait).await?;
            match self.dispatch(method, &body, purpose).await {
                Ok(value) => match interpret_object(value) {
                    Ok(result) => {
                        self.count(method, purpose, app, "ok");
                        return Ok(result);
                    }
                    Err(error) => {
                        self.count(method, purpose, app, error.kind());
                        if !self.consider_retry(&error, attempt).await {
                            return Err(error);
                        }
                    }
                },
                Err(error) => {
                    self.count(method, purpose, app, error.kind());
                    if !self.consider_retry(&error, attempt).await {
                        return Err(error);
                    }
                }
            }
        }
        Err(ProviderError::fatal("retry loop ended without a result"))
    }

    pub async fn batch(
        &self,
        requests: Vec<(String, Value)>,
        purpose: Purpose,
        application: Option<&str>,
        wait: TokenWait,
    ) -> Result<Vec<Result<Value, ProviderError>>, ProviderError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let app = application_label(application);
        let methods: Vec<String> = requests.iter().map(|(method, _)| method.clone()).collect();
        let duration_method = duration_method(&methods);
        let body = Value::Array(
            requests
                .into_iter()
                .enumerate()
                .map(|(index, (method, params))| {
                    json!({
                        "jsonrpc": "2.0",
                        "id": index + 1,
                        "method": method,
                        "params": params,
                    })
                })
                .collect(),
        );
        for attempt in 1..=ATTEMPTS {
            self.acquire(purpose, wait).await?;
            match self.dispatch(duration_method, &body, purpose).await {
                Ok(value) => {
                    let results = interpret_batch(value, methods.len())?;
                    for (method, result) in methods.iter().zip(&results) {
                        let outcome = match result {
                            Ok(_) => "ok",
                            Err(error) => error.kind(),
                        };
                        self.count(method, purpose, app, outcome);
                    }
                    return Ok(results);
                }
                Err(error) => {
                    for method in &methods {
                        self.count(method, purpose, app, error.kind());
                    }
                    if !self.consider_retry(&error, attempt).await {
                        return Err(error);
                    }
                }
            }
        }
        Err(ProviderError::fatal("retry loop ended without a result"))
    }

    async fn acquire(&self, purpose: Purpose, wait: TokenWait) -> Result<(), ProviderError> {
        let started = Instant::now();
        let deadline = match wait {
            TokenWait::UntilToken => None,
            TokenWait::For(limit) => Some(started + limit),
        };
        let mut prefer = false;
        loop {
            let now = Instant::now();
            let hold = self.lock_budget().hold_delay(now);
            if let Some(delay) = hold {
                tokio::time::sleep(delay).await;
                continue;
            }
            let use_prefer = prefer;
            let parked = {
                let today = self.today();
                let mut budget = self.lock_budget();
                budget.refill(Instant::now());
                match budget.try_acquire(purpose, deadline, Instant::now(), today, use_prefer) {
                    Err(fail) => {
                        let tokens = budget.tokens();
                        drop(budget);
                        self.reject(purpose, tokens);
                        return Err(match fail {
                            AcquireFail::Daily => ProviderError::BudgetExhausted,
                            AcquireFail::Throttled => ProviderError::Throttled,
                        });
                    }
                    Ok(None) => {
                        let delay = budget.arm_delay();
                        let tokens = budget.tokens();
                        drop(budget);
                        self.inner
                            .metrics
                            .set_tokens(self.inner.chain.as_str(), tokens);
                        if let Some(delay) = delay {
                            self.spawn_refill(delay);
                        }
                        return Ok(());
                    }
                    Ok(Some(parked)) => {
                        let delay = budget.arm_delay();
                        drop(budget);
                        (parked, delay)
                    }
                }
            };
            if let Some(delay) = parked.1 {
                self.spawn_refill(delay);
            }
            if self.wait_turn(purpose, parked.0).await {
                let tokens = self.lock_budget().tokens();
                self.reject(purpose, tokens);
                return Err(ProviderError::Throttled);
            }
            prefer = true;
        }
    }

    /// `true` when the deadline elapsed.
    async fn wait_turn(&self, purpose: Purpose, parked: Parked) -> bool {
        let Parked { id, rx, deadline } = parked;
        let timed_out = async {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            biased;
            received = rx => {
                let _ = received;
                false
            }
            _ = timed_out => {
                let still_queued = self.lock_budget().remove(purpose, id);
                if !still_queued {
                    let mut budget = self.lock_budget();
                    budget.wake_highest();
                    let delay = budget.arm_delay();
                    drop(budget);
                    if let Some(delay) = delay {
                        self.spawn_refill(delay);
                    }
                }
                true
            }
        }
    }

    /// `true` when the attempt should be repeated.
    async fn consider_retry(&self, error: &ProviderError, attempt: u32) -> bool {
        if let ProviderError::RateLimited { retry_after } = error {
            self.hold_for(*retry_after);
            if *retry_after > RETRY_AFTER_CAP {
                return false;
            }
        }
        if error.retryable() && attempt < ATTEMPTS {
            tokio::time::sleep(error.retry_delay(attempt)).await;
            true
        } else {
            false
        }
    }

    fn hold_for(&self, delay: Duration) {
        if delay.is_zero() {
            return;
        }
        self.lock_budget().hold_until(Instant::now() + delay);
    }

    fn reject(&self, purpose: Purpose, tokens: f64) {
        self.inner
            .metrics
            .set_tokens(self.inner.chain.as_str(), tokens);
        self.inner
            .metrics
            .record_rejection(self.inner.chain.as_str(), purpose.as_str());
    }

    fn spawn_refill(&self, delay: Duration) {
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let mut next = {
                let mut budget = lock_mutex(&inner.budget);
                budget.refill_fired(tokio::time::Instant::now())
            };
            inner
                .metrics
                .set_tokens(inner.chain.as_str(), lock_mutex(&inner.budget).tokens());
            while let Some(delay) = next {
                tokio::time::sleep(delay).await;
                next = {
                    let mut budget = lock_mutex(&inner.budget);
                    let delay = budget.refill_fired(tokio::time::Instant::now());
                    inner
                        .metrics
                        .set_tokens(inner.chain.as_str(), budget.tokens());
                    delay
                };
            }
        });
    }

    async fn dispatch(
        &self,
        method: &str,
        body: &Value,
        purpose: Purpose,
    ) -> Result<Value, ProviderError> {
        let permit = self.permit().await;
        let started = std::time::Instant::now();
        let result = self.post(body).await;
        drop(permit);
        self.inner.metrics.observe_duration(
            self.inner.chain.as_str(),
            method,
            purpose.as_str(),
            started.elapsed().as_secs_f64(),
        );
        result
    }

    async fn permit(&self) -> OwnedSemaphorePermit {
        self.inner
            .in_flight
            .clone()
            .acquire_owned()
            .await
            .expect("in-flight semaphore is closed")
    }

    async fn post(&self, body: &Value) -> Result<Value, ProviderError> {
        let response = self
            .inner
            .http
            .post(&self.inner.url)
            .json(body)
            .send()
            .await
            .map_err(|_| ProviderError::Transient { status: None })?;
        let status = response.status();
        if status.as_u16() == 413 {
            return Err(ProviderError::BatchTooLarge);
        }
        if status.as_u16() == 429 {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .map(parse_retry_after)
                .unwrap_or(Duration::ZERO);
            return Err(ProviderError::RateLimited { retry_after });
        }
        if status.is_server_error() {
            return Err(ProviderError::Transient {
                status: Some(status.as_u16()),
            });
        }
        if !status.is_success() {
            return Err(ProviderError::fatal(format!("http {}", status.as_u16())));
        }
        response
            .json()
            .await
            .map_err(|_| ProviderError::fatal("provider response is not json"))
    }

    fn count(&self, method: &str, purpose: Purpose, application: &str, outcome: &str) {
        self.inner.metrics.record_call(
            self.inner.chain.as_str(),
            method,
            purpose.as_str(),
            application,
            outcome,
        );
    }

    fn today(&self) -> u64 {
        self.lock_ranges().clock.now_ms() / 86_400_000
    }

    fn lock_budget(&self) -> std::sync::MutexGuard<'_, Budget> {
        lock_mutex(&self.inner.budget)
    }

    fn lock_ranges(&self) -> std::sync::MutexGuard<'_, RangeBook> {
        lock_mutex(&self.inner.ranges)
    }
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn application_label(application: Option<&str>) -> &str {
    application
        .filter(|value| !value.is_empty())
        .unwrap_or("none")
}

fn duration_method(methods: &[String]) -> &str {
    match methods {
        [first, rest @ ..] if rest.iter().all(|method| method == first) => first,
        _ => "mixed",
    }
}

fn is_http_url(url: &str) -> bool {
    let Some((scheme, rest)) = url.split_once("://") else {
        return false;
    };
    matches!(scheme, "http" | "https") && !rest.is_empty()
}

fn interpret_object(value: Value) -> Result<Value, ProviderError> {
    let Value::Object(object) = value else {
        return Err(ProviderError::fatal("json-rpc response is not an object"));
    };
    if let Some(error) = object.get("error").filter(|error| !error.is_null()) {
        return Err(classify_error_value(error));
    }
    object
        .get("result")
        .cloned()
        .ok_or_else(|| ProviderError::fatal("json-rpc response is missing result"))
}

fn interpret_batch(
    value: Value,
    expected: usize,
) -> Result<Vec<Result<Value, ProviderError>>, ProviderError> {
    if value.get("error").is_some() && !value.is_array() {
        return Err(classify_error_value(&value["error"]));
    }
    let Value::Array(items) = value else {
        return Err(ProviderError::fatal(
            "json-rpc batch response is not an array",
        ));
    };
    let mut ordered = vec![None; expected];
    for item in items {
        let id = item
            .get("id")
            .and_then(Value::as_i64)
            .and_then(|id| usize::try_from(id).ok())
            .filter(|id| *id >= 1 && *id <= expected);
        let Some(id) = id else {
            return Err(ProviderError::fatal("batch response id is out of range"));
        };
        if ordered[id - 1].is_some() {
            return Err(ProviderError::fatal("duplicate batch response id"));
        }
        ordered[id - 1] = Some(interpret_object(item));
    }
    Ok(ordered
        .into_iter()
        .map(|slot| slot.unwrap_or_else(|| Err(ProviderError::fatal("missing batch item"))))
        .collect())
}

fn classify_error_value(error: &Value) -> ProviderError {
    let code = error.get("code").and_then(Value::as_i64).unwrap_or(0);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("provider error");
    classify_rpc_message(code, message)
}
