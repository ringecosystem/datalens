use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::http::{HeaderMap, header};
use datalens_core::{DatalensError, DatalensErrorKind};

use crate::{QueryAuthApplicationConfig, QueryAuthConfig};

pub(super) fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    value.strip_prefix("Bearer ")
}

#[derive(Clone, Debug)]
pub(super) struct QueryAuthRegistry {
    enabled: bool,
    applications: Arc<BTreeMap<String, QueryAuthApplicationConfig>>,
    quota_state: Arc<Mutex<BTreeMap<String, QueryAuthState>>>,
}

impl QueryAuthRegistry {
    pub(super) fn new(config: QueryAuthConfig) -> Self {
        Self {
            enabled: config.enabled,
            applications: Arc::new(
                config
                    .applications
                    .into_iter()
                    .map(|mut application| {
                        application.id = normalize_application_id(&application.id);
                        (application.id.clone(), application)
                    })
                    .collect(),
            ),
            quota_state: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub(super) fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<QueryApplicationPermit>, DatalensError> {
        if !self.enabled {
            return Ok(None);
        }
        let token = bearer_token(headers).ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::AuthenticationFailed,
                "application credentials are required",
            )
        })?;
        let application = self
            .applications
            .values()
            .find(|application| application.token == token)
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::AuthenticationFailed,
                    "application credentials are invalid",
                )
            })?;
        if !application.enabled {
            return Err(DatalensError::new(
                DatalensErrorKind::Unauthorized,
                "application is disabled",
            ));
        }
        self.acquire_permit(application).map(Some)
    }

    fn acquire_permit(
        &self,
        application: &QueryAuthApplicationConfig,
    ) -> Result<QueryApplicationPermit, DatalensError> {
        let Some(quota) = &application.quota else {
            return Ok(QueryApplicationPermit::noop(application.id.clone()));
        };
        let mut states = self.quota_state.lock().expect("query auth quota state");
        let state = states.entry(application.id.clone()).or_default();
        let now = Instant::now();
        if now.duration_since(state.window_started_at) >= Duration::from_secs(60) {
            state.window_started_at = now;
            state.requests_in_window = 0;
        }
        if let Some(limit) = quota.max_requests_per_minute
            && state.requests_in_window >= limit
        {
            return Err(DatalensError::new(
                DatalensErrorKind::RateLimited,
                "application request rate quota exceeded",
            ));
        }
        if let Some(limit) = quota.max_concurrent_requests
            && state.in_flight >= limit
        {
            return Err(DatalensError::new(
                DatalensErrorKind::RateLimited,
                "application concurrent request quota exceeded",
            ));
        }
        if quota.max_requests_per_minute.is_some() {
            state.requests_in_window += 1;
        }
        let release = quota.max_concurrent_requests.is_some();
        if release {
            state.in_flight += 1;
        }
        Ok(QueryApplicationPermit {
            application_id: application.id.clone(),
            quota_state: self.quota_state.clone(),
            release,
        })
    }
}

#[derive(Debug)]
pub(super) struct QueryApplicationPermit {
    application_id: String,
    quota_state: Arc<Mutex<BTreeMap<String, QueryAuthState>>>,
    release: bool,
}

impl QueryApplicationPermit {
    fn noop(application_id: String) -> Self {
        Self {
            application_id,
            quota_state: Arc::new(Mutex::new(BTreeMap::new())),
            release: false,
        }
    }

    pub(super) fn application_id(&self) -> &str {
        &self.application_id
    }
}

impl Drop for QueryApplicationPermit {
    fn drop(&mut self) {
        if !self.release {
            return;
        }
        let mut states = self.quota_state.lock().expect("query auth quota state");
        if let Some(state) = states.get_mut(&self.application_id) {
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }
}

#[derive(Debug)]
struct QueryAuthState {
    window_started_at: Instant,
    requests_in_window: u64,
    in_flight: u64,
}

impl Default for QueryAuthState {
    fn default() -> Self {
        Self {
            window_started_at: Instant::now(),
            requests_in_window: 0,
            in_flight: 0,
        }
    }
}

fn normalize_application_id(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}
