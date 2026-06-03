use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{Error, GraphqlError, index::IndexClient, native::NativeClient};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientConfig {
    pub endpoint: String,
    pub bearer_token: Option<String>,
    pub application: Option<String>,
    pub timeout: Option<Duration>,
    pub user_agent: Option<String>,
}

#[derive(Clone)]
pub struct DatalensClient {
    base_url: String,
    graphql_endpoint: String,
    native_transport: NativeTransport,
    bearer_token: Option<String>,
    application: Option<String>,
    user_agent: Option<String>,
    retry_config: RetryConfig,
    http: reqwest::blocking::Client,
}

impl DatalensClient {
    pub fn new(config: ClientConfig) -> Result<Self, Error> {
        Self::new_with_retry_config(config, RetryConfig::default())
    }

    pub fn new_with_retry_config(
        config: ClientConfig,
        retry_config: RetryConfig,
    ) -> Result<Self, Error> {
        let endpoint = config.endpoint.trim().trim_end_matches('/').to_owned();
        if endpoint.is_empty() {
            return Err(Error::InvalidConfig(
                "datalens endpoint must not be empty".to_owned(),
            ));
        }

        Self::build(
            endpoint.clone(),
            format!("{endpoint}/native/graphql"),
            NativeTransport::Rest,
            config,
            retry_config,
        )
    }

    pub fn with_graphql_endpoint(config: ClientConfig) -> Result<Self, Error> {
        Self::with_graphql_endpoint_with_retry_config(config, RetryConfig::default())
    }

    pub fn with_graphql_endpoint_with_retry_config(
        config: ClientConfig,
        retry_config: RetryConfig,
    ) -> Result<Self, Error> {
        let endpoint = config.endpoint.trim().trim_end_matches('/').to_owned();
        if endpoint.is_empty() {
            return Err(Error::InvalidConfig(
                "datalens GraphQL endpoint must not be empty".to_owned(),
            ));
        }

        Self::build(
            endpoint.clone(),
            endpoint,
            NativeTransport::Graphql,
            config,
            retry_config,
        )
    }

    fn build(
        base_url: String,
        graphql_endpoint: String,
        native_transport: NativeTransport,
        config: ClientConfig,
        retry_config: RetryConfig,
    ) -> Result<Self, Error> {
        retry_config.validate()?;
        let mut builder = reqwest::blocking::Client::builder();
        if let Some(timeout) = config.timeout {
            builder = builder.timeout(timeout);
        }
        if let Some(user_agent) = normalize_optional(config.user_agent) {
            builder = builder.user_agent(user_agent.clone());
            Ok(Self {
                base_url,
                graphql_endpoint,
                native_transport,
                bearer_token: normalize_optional(config.bearer_token),
                application: normalize_optional(config.application),
                user_agent: Some(user_agent),
                retry_config,
                http: builder
                    .build()
                    .map_err(|error| Error::InvalidConfig(error.to_string()))?,
            })
        } else {
            Ok(Self {
                base_url,
                graphql_endpoint,
                native_transport,
                bearer_token: normalize_optional(config.bearer_token),
                application: normalize_optional(config.application),
                user_agent: None,
                retry_config,
                http: builder
                    .build()
                    .map_err(|error| Error::InvalidConfig(error.to_string()))?,
            })
        }
    }

    pub fn native(&self) -> NativeClient<'_> {
        NativeClient::new(self)
    }

    pub fn index(&self) -> IndexClient<'_> {
        IndexClient::new(self)
    }

    pub(crate) fn native_transport(&self) -> NativeTransport {
        self.native_transport
    }

    pub(crate) fn execute<T, V>(&self, query: &'static str, variables: V) -> Result<T, Error>
    where
        T: DeserializeOwned,
        V: Serialize,
    {
        let request = GraphqlRequest { query, variables };
        let started_at = Instant::now();
        let mut attempt = 1;
        loop {
            let result = self.execute_once(&request);
            match result {
                Ok(data) => return Ok(data),
                Err(error) => {
                    let Some(delay) = self.retry_delay(&error, attempt, started_at) else {
                        return Err(error);
                    };
                    std::thread::sleep(delay);
                    attempt += 1;
                }
            }
        }
    }

    fn execute_once<T, V>(&self, request: &GraphqlRequest<V>) -> Result<T, Error>
    where
        T: DeserializeOwned,
        V: Serialize,
    {
        let mut builder = self.http.post(&self.graphql_endpoint).json(request);
        builder = self.apply_headers(builder);

        let response = builder
            .send()
            .map_err(|error| Error::Transport(format!("send datalens GraphQL request: {error}")))?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|error| {
            Error::Transport(format!("read datalens GraphQL response: {error}"))
        })?;

        if !(200..300).contains(&status) {
            return Err(http_status_error(status, body));
        }

        let response: GraphqlResponse<T> = serde_json::from_str(&body)
            .map_err(|error| Error::Decode(format!("decode datalens GraphQL response: {error}")))?;
        if !response.errors.is_empty() {
            return Err(Error::Graphql(response.errors));
        }
        response.data.ok_or_else(|| {
            Error::Decode("datalens GraphQL response did not include data".to_owned())
        })
    }

    pub(crate) fn post_json<T, V>(&self, path: &str, request: &V) -> Result<T, Error>
    where
        T: DeserializeOwned,
        V: Serialize,
    {
        let body = self.send_retryable_rest(|| {
            let mut builder = self
                .http
                .post(format!("{}{}", self.base_url, path))
                .json(request);
            builder = self.apply_headers(builder);
            builder
                .send()
                .map_err(|error| Error::Transport(format!("send datalens REST request: {error}")))
        })?;

        serde_json::from_str(&body)
            .map_err(|error| Error::Decode(format!("decode datalens REST response: {error}")))
    }

    pub(crate) fn get_json<T>(
        &self,
        path_segments: &[&str],
        query: &[(&str, &str)],
    ) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        let mut url = reqwest::Url::parse(&self.base_url).map_err(|error| {
            Error::InvalidConfig(format!(
                "datalens REST endpoint is not a valid URL: {error}"
            ))
        })?;
        {
            let mut segments = url.path_segments_mut().map_err(|_| {
                Error::InvalidConfig(
                    "datalens REST endpoint cannot be used as a base URL".to_owned(),
                )
            })?;
            segments.pop_if_empty();
            for segment in path_segments {
                segments.push(segment);
            }
        }
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query.iter().copied());
        }

        let body = self.send_retryable_rest(|| {
            let mut builder = self.http.get(url.clone());
            builder = self.apply_headers(builder);
            builder
                .send()
                .map_err(|error| Error::Transport(format!("send datalens REST request: {error}")))
        })?;

        serde_json::from_str(&body)
            .map_err(|error| Error::Decode(format!("decode datalens REST response: {error}")))
    }

    fn apply_headers(
        &self,
        mut builder: reqwest::blocking::RequestBuilder,
    ) -> reqwest::blocking::RequestBuilder {
        if let Some(token) = &self.bearer_token {
            builder = builder.bearer_auth(token);
        }
        if let Some(application) = &self.application {
            builder = builder.header("x-datalens-application", application);
        }
        if let Some(user_agent) = &self.user_agent {
            builder = builder.header(reqwest::header::USER_AGENT, user_agent);
        }
        builder
    }

    fn send_retryable_rest(
        &self,
        mut send: impl FnMut() -> Result<reqwest::blocking::Response, Error>,
    ) -> Result<String, Error> {
        let started_at = Instant::now();
        let mut attempt = 1;
        loop {
            let result = send().and_then(|response| {
                let status = response.status().as_u16();
                let body = response.text().map_err(|error| {
                    Error::Transport(format!("read datalens REST response: {error}"))
                })?;

                if (200..300).contains(&status) {
                    Ok(body)
                } else {
                    Err(http_status_error(status, body))
                }
            });

            match result {
                Ok(body) => return Ok(body),
                Err(error) => {
                    let Some(delay) = self.retry_delay(&error, attempt, started_at) else {
                        return Err(error);
                    };
                    std::thread::sleep(delay);
                    attempt += 1;
                }
            }
        }
    }

    fn retry_delay(&self, error: &Error, attempt: u32, started_at: Instant) -> Option<Duration> {
        if !error.is_retryable() {
            return None;
        }
        let retry_after = error.retry_after_seconds().map(Duration::from_secs);
        let delay = self.retry_config.delay_for_attempt(attempt, retry_after)?;
        if let Some(max_elapsed) = self.retry_config.max_elapsed
            && started_at.elapsed().saturating_add(delay) > max_elapsed
        {
            return None;
        }
        Some(delay)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub max_elapsed: Option<Duration>,
    pub jitter: bool,
    pub jitter_factor: f64,
}

impl RetryConfig {
    pub fn disabled() -> Self {
        Self {
            max_attempts: 1,
            initial_delay: Duration::from_millis(0),
            max_delay: Duration::from_millis(0),
            max_elapsed: None,
            jitter: false,
            jitter_factor: 0.0,
        }
    }

    pub fn delay_for_attempt(
        &self,
        failed_attempt: u32,
        retry_after: Option<Duration>,
    ) -> Option<Duration> {
        if failed_attempt == 0 || failed_attempt >= self.max_attempts {
            return None;
        }
        let delay = retry_after.unwrap_or_else(|| self.exponential_delay(failed_attempt));
        if self.jitter && retry_after.is_none() {
            Some(self.jitter_delay(delay, failed_attempt))
        } else {
            Some(delay)
        }
    }

    fn validate(&self) -> Result<(), Error> {
        if self.max_attempts == 0 {
            return Err(Error::InvalidConfig(
                "datalens retry max_attempts must be at least 1".to_owned(),
            ));
        }
        if !self.jitter_factor.is_finite() || !(0.0..=1.0).contains(&self.jitter_factor) {
            return Err(Error::InvalidConfig(
                "datalens retry jitter_factor must be between 0.0 and 1.0".to_owned(),
            ));
        }
        Ok(())
    }

    fn exponential_delay(&self, failed_attempt: u32) -> Duration {
        let exponent = failed_attempt.saturating_sub(1).min(31);
        let multiplier = 2_u32.pow(exponent);
        self.initial_delay
            .saturating_mul(multiplier)
            .min(self.max_delay)
    }

    fn jitter_delay(&self, delay: Duration, failed_attempt: u32) -> Duration {
        if delay.is_zero() || self.jitter_factor <= 0.0 {
            return delay;
        }
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.subsec_nanos())
            .unwrap_or(0);
        let sample = (nanos ^ failed_attempt).wrapping_mul(1_103_515_245) % 1_000;
        let unit = f64::from(sample) / 999.0;
        let factor = 1.0 - self.jitter_factor + (2.0 * self.jitter_factor * unit);
        Duration::from_secs_f64(delay.as_secs_f64() * factor)
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(2),
            max_elapsed: Some(Duration::from_secs(10)),
            jitter: true,
            jitter_factor: 0.2,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NativeTransport {
    Rest,
    Graphql,
}

#[derive(Serialize)]
struct GraphqlRequest<V> {
    query: &'static str,
    variables: V,
}

#[derive(Deserialize)]
struct GraphqlResponse<T> {
    data: Option<T>,
    #[serde(default)]
    errors: Vec<GraphqlError>,
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then(|| value.to_owned())
    })
}

fn http_status_error(status: u16, body: String) -> Error {
    if status == 401 || status == 403 {
        Error::Unauthorized { status, body }
    } else {
        Error::HttpStatus { status, body }
    }
}
