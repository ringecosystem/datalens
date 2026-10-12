use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

use datalens_core::ChainName;
use datalens_core::wire::{
    ChainHeads, ErrorBody, HeadsResponse, PageRequest, QueryRequest, QueryResponse, WatchRequest,
    WatchResponse,
};
use futures::stream::BoxStream;
use reqwest::header::RETRY_AFTER;
use reqwest::{RequestBuilder, Response};
use serde::de::DeserializeOwned;

use crate::error::{ApiErrorKind, Error, Result};
use crate::follow::{FollowEvent, FollowRequest, follow};

/// Retries of one call after a retryable [`Error`]. The delay doubles from `initial_backoff`
/// up to `max_backoff`, scaled by a random factor in [0.5, 1). `Retry-After` overrides it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryConfig {
    /// Total tries, the first included. 1 disables retries.
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            initial_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(10),
        }
    }
}

impl RetryConfig {
    /// Jittered delay before retry number `failures`, counting from 1.
    pub fn backoff(&self, failures: u32) -> Duration {
        let doublings = failures.saturating_sub(1).min(20);
        let delay = self
            .initial_backoff
            .saturating_mul(1 << doublings)
            .min(self.max_backoff);
        let random = RandomState::new().build_hasher().finish();
        delay.mul_f64(0.5 + (random as f64 / u64::MAX as f64) / 2.0)
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Base URL such as `https://datalens.example.com`.
    pub endpoint: String,
    pub application: String,
    pub token: String,
    /// Per request. A heads long-poll adds its own wait on top.
    pub timeout: Duration,
    pub retry: RetryConfig,
}

impl ClientConfig {
    pub fn new(
        endpoint: impl Into<String>,
        application: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            application: application.into(),
            token: token.into(),
            timeout: Duration::from_secs(30),
            retry: RetryConfig::default(),
        }
    }
}

/// The head a [`Client::wait_heads`] call waits on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HeadKind {
    Latest,
    Safe,
    Finalized,
}

impl HeadKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Latest => "latest",
            Self::Safe => "safe",
            Self::Finalized => "finalized",
        }
    }

    pub fn of(self, heads: &ChainHeads) -> u64 {
        match self {
            Self::Latest => heads.latest,
            Self::Safe => heads.safe,
            Self::Finalized => heads.finalized,
        }
    }
}

/// One application's view of a datalens server. Cloning shares the connection pool.
#[derive(Debug, Clone)]
pub struct Client {
    http: reqwest::Client,
    endpoint: String,
    application: String,
    token: String,
    timeout: Duration,
    retry: RetryConfig,
}

impl Client {
    pub fn new(config: ClientConfig) -> Result<Self> {
        Ok(Self {
            http: reqwest::Client::builder().build()?,
            endpoint: config.endpoint.trim_end_matches('/').to_owned(),
            application: config.application,
            token: config.token,
            timeout: config.timeout,
            retry: config.retry,
        })
    }

    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry
    }

    /// One page. `page.next_cursor` names the next page of the same request.
    pub async fn query(&self, request: &QueryRequest) -> Result<QueryResponse> {
        self.call(|| self.post("/v1/query").json(request)).await
    }

    /// Every page of `request`, rows concatenated. The other fields come from the last page,
    /// whose `served` contains the rows of every earlier page.
    pub async fn query_all(&self, request: &QueryRequest) -> Result<QueryResponse> {
        let mut merged = self.query(request).await?;
        let mut paged = request.clone();
        while let Some(cursor) = merged.page.next_cursor.take() {
            paged.page = Some(PageRequest {
                limit: request.page.as_ref().and_then(|page| page.limit),
                cursor: Some(cursor),
            });
            let mut next = self.query(&paged).await?;
            merged.rows.append(&mut next.rows);
            next.rows = merged.rows;
            merged = next;
        }
        Ok(merged)
    }

    pub async fn heads(&self, chain: &ChainName) -> Result<ChainHeads> {
        self.call(|| self.get(&format!("/v1/heads/{chain}"))).await
    }

    /// Heads of every chain this application may read.
    pub async fn all_heads(&self) -> Result<Vec<ChainHeads>> {
        let response: HeadsResponse = self.call(|| self.get("/v1/heads")).await?;
        Ok(response.chains)
    }

    /// Returns once head `kind` passes `after`, or with the unchanged heads after `timeout`.
    /// The server caps `timeout` at 60 s.
    pub async fn wait_heads(
        &self,
        chain: &ChainName,
        after: u64,
        kind: HeadKind,
        timeout: Duration,
    ) -> Result<ChainHeads> {
        let params = [
            ("after", after.to_string()),
            ("kind", kind.as_str().to_owned()),
            ("timeout_ms", timeout.as_millis().to_string()),
        ];
        self.call(|| {
            self.get(&format!("/v1/heads/{chain}/wait"))
                .query(&params)
                .timeout(self.timeout + timeout)
        })
        .await
    }

    /// Subscribes the selector for the server's TTL, or renews it.
    pub async fn watch(&self, request: &WatchRequest) -> Result<WatchResponse> {
        self.call(|| self.post("/v1/watch").json(request)).await
    }

    pub async fn unwatch(&self, request: &WatchRequest) -> Result<()> {
        self.call_text(|| {
            self.authed(self.http.delete(format!("{}/v1/watch", self.endpoint)))
                .json(request)
        })
        .await
        .map(drop)
    }

    /// Reads `request.start..` chunk by chunk, then follows the head. See [`FollowEvent`].
    pub fn follow(&self, request: FollowRequest) -> BoxStream<'static, Result<FollowEvent>> {
        follow(self.clone(), request)
    }

    fn get(&self, path: &str) -> RequestBuilder {
        self.authed(self.http.get(format!("{}{path}", self.endpoint)))
    }

    fn post(&self, path: &str) -> RequestBuilder {
        self.authed(self.http.post(format!("{}{path}", self.endpoint)))
    }

    fn authed(&self, request: RequestBuilder) -> RequestBuilder {
        request
            .header("x-datalens-application", &self.application)
            .bearer_auth(&self.token)
            .timeout(self.timeout)
    }

    async fn call<T: DeserializeOwned>(&self, build: impl Fn() -> RequestBuilder) -> Result<T> {
        let text = self.call_text(build).await?;
        serde_json::from_str(&text).map_err(|error| Error::Decode(error.to_string()))
    }

    async fn call_text(&self, build: impl Fn() -> RequestBuilder) -> Result<String> {
        let mut failures = 0;
        loop {
            let error = match build().send().await {
                Ok(response) => match checked(response).await {
                    Ok(text) => return Ok(text),
                    Err(error) => error,
                },
                Err(error) => Error::Http(error),
            };
            failures += 1;
            if !error.is_retryable() || failures >= self.retry.max_attempts {
                return Err(error);
            }
            let delay = error
                .retry_after()
                .unwrap_or_else(|| self.retry.backoff(failures));
            tokio::time::sleep(delay).await;
        }
    }
}

async fn checked(response: Response) -> Result<String> {
    let status = response.status();
    let retry_after = response
        .headers()
        .get(RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
    let text = response.text().await?;
    if status.is_success() {
        return Ok(text);
    }
    let (kind, message) = match serde_json::from_str::<ErrorBody>(&text) {
        Ok(body) => (ApiErrorKind::parse(&body.error.kind), body.error.message),
        Err(_) => (ApiErrorKind::Other("http".to_owned()), text),
    };
    Err(Error::Api {
        kind,
        message,
        status: status.as_u16(),
        retry_after,
    })
}
