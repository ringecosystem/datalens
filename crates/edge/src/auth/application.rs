use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::http::{HeaderMap, header};
use datalens_core::{DatalensError, DatalensErrorKind, DatasetKey, QueryFinalityRequirement};
use datalens_metrics::ApplicationIdentity;

use crate::{config, contract::query::QueryApiRequest};

pub const APPLICATION_HEADER: &str = "x-datalens-application";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthContext {
    pub subject: Option<String>,
}

/// Authenticated application context carried from edge routes into metrics,
/// usage ledger attribution, and warmup ownership checks. Authentication proves
/// the token; authorization and quota checks are route-specific.
#[derive(Debug)]
pub struct ApplicationContext {
    pub id: String,
    pub name: String,
    pub display_name: Option<String>,
    pub quota: Option<config::ApplicationQuotaConfig>,
    _permit: Option<ApplicationRequestPermit>,
}

impl ApplicationContext {
    pub fn metrics_identity(&self) -> ApplicationIdentity {
        ApplicationIdentity::named(self.id.clone())
    }
}

pub trait AuthenticationHook {
    fn authenticate(&self) -> AuthContext;
}

#[derive(Clone, Debug, Default)]
pub struct NoAuthentication;

impl AuthenticationHook for NoAuthentication {
    fn authenticate(&self) -> AuthContext {
        AuthContext { subject: None }
    }
}

#[derive(Clone, Debug, Default)]
/// Registry-backed application authentication and authorization boundary for
/// edge APIs. It normalizes application ids before lookup so headers, metrics,
/// and warmup task ownership use the same stable identity.
pub struct ApplicationRegistry {
    required: bool,
    applications: BTreeMap<String, config::ApplicationConfig>,
    quota_state: Arc<Mutex<BTreeMap<String, ApplicationQuotaState>>>,
}

impl ApplicationRegistry {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn from_config(config: config::ApplicationRegistryConfig) -> Result<Self, DatalensError> {
        let mut applications = BTreeMap::new();
        for mut application in config.applications {
            application.id = normalize_application_id(&application.id)?;
            application.name = normalize_application_id(&application.name)?;
            if application.token.trim().is_empty() {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!("application {} token must not be empty", application.id),
                ));
            }
            if config.required && application.operations.is_empty() {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    format!(
                        "application {} must declare at least one operation when application auth is required",
                        application.id
                    ),
                ));
            }
            for dataset in &mut application.datasets {
                *dataset = normalize_application_dataset_key(dataset).map_err(|_| {
                    DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        format!(
                            "application {} references unknown dataset {dataset}",
                            application.id
                        ),
                    )
                })?;
            }
            if applications
                .insert(application.id.clone(), application)
                .is_some()
            {
                return Err(DatalensError::new(
                    DatalensErrorKind::InvalidInput,
                    "application id is registered more than once",
                ));
            }
        }
        if config.required && applications.is_empty() {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "applications registry must contain at least one application when required",
            ));
        }
        Ok(Self {
            required: config.required,
            applications,
            quota_state: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn required(&self) -> bool {
        self.required
    }

    pub fn authenticate_headers(
        &self,
        headers: &HeaderMap,
        chain: &str,
        dataset: &str,
        range_len: u128,
        finality: QueryFinalityRequirement,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        if !self.required {
            return Ok(None);
        }
        let result = (|| {
            let application = self.authenticate_application_headers(headers)?;
            authorize_application_operation(
                application,
                config::ApplicationOperationConfig::Query,
            )?;
            authorize_application_dataset(application, chain, dataset)?;
            enforce_quota(application, range_len, finality)?;
            self.application_context(application)
        })();
        self.record_auth_result(headers, &result);
        result
    }

    pub fn authenticate_warmup_headers(
        &self,
        headers: &HeaderMap,
        chain: &str,
        dataset: &str,
        operation: config::ApplicationOperationConfig,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        if !self.required {
            return Ok(None);
        }
        let result = (|| {
            let application = self.authenticate_application_headers(headers)?;
            authorize_application_operation(application, operation)?;
            authorize_application_dataset(application, chain, dataset)?;
            self.application_context(application)
        })();
        self.record_auth_result(headers, &result);
        result
    }

    pub fn authenticate_native_query_headers(
        &self,
        headers: &HeaderMap,
        request: &QueryApiRequest,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        if !self.required {
            return Ok(None);
        }
        let result = (|| {
            let application = self.authenticate_application_headers(headers)?;
            authorize_application_operation(
                application,
                config::ApplicationOperationConfig::Query,
            )?;
            authorize_application_dataset(
                application,
                request.chain.configured_name(),
                &request.dataset_key,
            )?;
            enforce_native_query_quota(application, request)?;
            self.application_context(application)
        })();
        self.record_auth_result(headers, &result);
        result
    }

    pub fn authenticate_task_headers(
        &self,
        headers: &HeaderMap,
        operation: config::ApplicationOperationConfig,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        if !self.required {
            return Ok(None);
        }
        let result = (|| {
            let application = self.authenticate_application_headers(headers)?;
            authorize_application_operation(application, operation)?;
            self.application_context(application)
        })();
        self.record_auth_result(headers, &result);
        result
    }

    pub fn authenticate_chain_head_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        if !self.required {
            return Ok(None);
        }
        let result = (|| {
            let application = self.authenticate_application_headers(headers)?;
            authorize_application_operation(
                application,
                config::ApplicationOperationConfig::Discovery,
            )?;
            self.application_context(application)
        })();
        self.record_auth_result(headers, &result);
        result
    }

    pub fn authorize_chain_head_application(
        &self,
        application_context: &Option<ApplicationContext>,
        chain: &str,
    ) -> Result<(), DatalensError> {
        if !self.required {
            return Ok(());
        }
        let Some(application_context) = application_context else {
            return Err(DatalensError::new(
                DatalensErrorKind::AuthenticationFailed,
                "application identity is required",
            ));
        };
        let application = self
            .applications
            .get(&application_context.id)
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::AuthenticationFailed,
                    "application credentials are invalid",
                )
            })?;
        authorize_application_chain(application, chain)
    }

    pub fn authenticate_discovery_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        if !self.required {
            return Ok(None);
        }
        let result = (|| {
            let application = self.authenticate_application_headers(headers)?;
            authorize_application_operation(
                application,
                config::ApplicationOperationConfig::Discovery,
            )?;
            self.application_context(application)
        })();
        self.record_auth_result(headers, &result);
        result
    }

    fn authenticate_application_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<&config::ApplicationConfig, DatalensError> {
        let raw_application = headers
            .get(APPLICATION_HEADER)
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::AuthenticationFailed,
                    "application identity is required",
                )
            })?
            .to_str()
            .map_err(|_| {
                DatalensError::new(
                    DatalensErrorKind::AuthenticationFailed,
                    "application identity is invalid",
                )
            })?;
        let application_id = normalize_application_id(raw_application).map_err(|_| {
            DatalensError::new(
                DatalensErrorKind::AuthenticationFailed,
                "application identity is invalid",
            )
        })?;
        let application = self.applications.get(&application_id).ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::AuthenticationFailed,
                "application credentials are invalid",
            )
        })?;
        let token = bearer_token(headers).ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::AuthenticationFailed,
                "application credentials are required",
            )
        })?;
        if token != application.token {
            return Err(DatalensError::new(
                DatalensErrorKind::AuthenticationFailed,
                "application credentials are invalid",
            ));
        }
        if !application.enabled {
            return Err(DatalensError::new(
                DatalensErrorKind::Unauthorized,
                "application is disabled",
            ));
        }
        Ok(application)
    }

    fn application_context(
        &self,
        application: &config::ApplicationConfig,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        Ok(Some(ApplicationContext {
            id: application.id.clone(),
            name: application.name.clone(),
            display_name: application.display_name.clone(),
            quota: application.quota.clone(),
            _permit: Some(self.acquire_quota_permit(application)?),
        }))
    }

    fn acquire_quota_permit(
        &self,
        application: &config::ApplicationConfig,
    ) -> Result<ApplicationRequestPermit, DatalensError> {
        let Some(quota) = &application.quota else {
            return Ok(ApplicationRequestPermit::noop());
        };
        let mut states = self.quota_state.lock().expect("application quota state");
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
        let release = if quota.max_concurrent_requests.is_some() {
            state.in_flight += 1;
            true
        } else {
            false
        };
        Ok(ApplicationRequestPermit {
            application_id: application.id.clone(),
            quota_state: self.quota_state.clone(),
            release,
        })
    }

    fn record_auth_result(
        &self,
        headers: &HeaderMap,
        result: &Result<Option<ApplicationContext>, DatalensError>,
    ) {
        let mut states = self.quota_state.lock().expect("application quota state");
        match result {
            Ok(Some(application)) => {
                states.entry(application.id.clone()).or_default().accepted += 1;
            }
            Ok(None) => {}
            Err(error) => {
                let reason = match error.kind {
                    DatalensErrorKind::AuthenticationFailed => "rejected_authentication",
                    DatalensErrorKind::Unauthorized => "rejected_authorization",
                    DatalensErrorKind::RateLimited => "rejected_quota",
                    _ => "rejected_other",
                };
                let state = states.entry(metrics_application_id(headers)).or_default();
                *state.rejected.entry(reason.to_owned()).or_default() += 1;
            }
        }
    }

    pub(crate) fn metrics_text(&self) -> Option<String> {
        let states = self.quota_state.lock().expect("application quota state");
        if states.is_empty() {
            return None;
        }
        let mut lines = Vec::new();
        for (application_id, state) in states.iter() {
            if state.accepted > 0 {
                lines.push(format!(
                    r#"datalens_edge_request_total{{application="{application_id}",outcome="accepted"}} {}"#,
                    state.accepted
                ));
            }
            for (reason, count) in &state.rejected {
                lines.push(format!(
                    r#"datalens_edge_request_rejected_total{{application="{application_id}",reason="{reason}"}} {count}"#
                ));
            }
            lines.push(format!(
                r#"datalens_edge_application_in_flight{{application="{application_id}"}} {}"#,
                state.in_flight
            ));
        }
        Some(lines.join("\n"))
    }
}

#[derive(Debug)]
struct ApplicationRequestPermit {
    application_id: String,
    quota_state: Arc<Mutex<BTreeMap<String, ApplicationQuotaState>>>,
    release: bool,
}

impl ApplicationRequestPermit {
    fn noop() -> Self {
        Self {
            application_id: String::new(),
            quota_state: Arc::new(Mutex::new(BTreeMap::new())),
            release: false,
        }
    }
}

impl Drop for ApplicationRequestPermit {
    fn drop(&mut self) {
        if !self.release {
            return;
        }
        let mut states = self.quota_state.lock().expect("application quota state");
        if let Some(state) = states.get_mut(&self.application_id) {
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }
}

#[derive(Debug)]
struct ApplicationQuotaState {
    window_started_at: Instant,
    requests_in_window: u64,
    in_flight: u64,
    accepted: u64,
    rejected: BTreeMap<String, u64>,
}

impl Default for ApplicationQuotaState {
    fn default() -> Self {
        Self {
            window_started_at: Instant::now(),
            requests_in_window: 0,
            in_flight: 0,
            accepted: 0,
            rejected: BTreeMap::new(),
        }
    }
}

fn metrics_application_id(headers: &HeaderMap) -> String {
    headers
        .get(APPLICATION_HEADER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| normalize_application_id(value).ok())
        .unwrap_or_else(|| "unknown".to_owned())
}

pub(crate) fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    value
        .strip_prefix("Bearer ")
        .filter(|token| !token.trim().is_empty())
}

fn authorize_application_dataset(
    application: &config::ApplicationConfig,
    chain: &str,
    dataset: &str,
) -> Result<(), DatalensError> {
    authorize_application_chain(application, chain)?;
    if !application
        .datasets
        .iter()
        .any(|allowed| allowed == dataset)
    {
        return Err(DatalensError::new(
            DatalensErrorKind::Unauthorized,
            "application is not allowed to access this dataset",
        ));
    }
    Ok(())
}

fn authorize_application_chain(
    application: &config::ApplicationConfig,
    chain: &str,
) -> Result<(), DatalensError> {
    if !application.chains.iter().any(|allowed| allowed == chain) {
        return Err(DatalensError::new(
            DatalensErrorKind::Unauthorized,
            "application is not allowed to access this chain",
        ));
    }
    Ok(())
}

fn authorize_application_operation(
    application: &config::ApplicationConfig,
    operation: config::ApplicationOperationConfig,
) -> Result<(), DatalensError> {
    if application.operations.contains(&operation) {
        return Ok(());
    }
    Err(DatalensError::new(
        DatalensErrorKind::Unauthorized,
        "application is not allowed to perform this operation",
    ))
}

fn enforce_quota(
    application: &config::ApplicationConfig,
    range_len: u128,
    finality: QueryFinalityRequirement,
) -> Result<(), DatalensError> {
    let Some(quota) = &application.quota else {
        return Ok(());
    };
    if let Some(limit) = quota.max_query_range_blocks
        && range_len > u128::from(limit)
    {
        return Err(DatalensError::new(
            DatalensErrorKind::RateLimited,
            "application query range quota exceeded",
        ));
    }
    if finality.allows_hot()
        && let Some(limit) = quota.max_hot_query_range_blocks
        && range_len > u128::from(limit)
    {
        return Err(DatalensError::new(
            DatalensErrorKind::RateLimited,
            "application hot query range quota exceeded",
        ));
    }
    Ok(())
}

fn enforce_native_query_quota(
    application: &config::ApplicationConfig,
    request: &QueryApiRequest,
) -> Result<(), DatalensError> {
    let Some(quota) = &application.quota else {
        return Ok(());
    };
    let range = request.range.into_ledger_range()?;
    if let Some(limit) = quota.max_query_range_blocks {
        let requested = range.len();
        if requested > u128::from(limit) {
            return Err(DatalensError::new(
                DatalensErrorKind::RateLimited,
                "application query range quota exceeded",
            ));
        }
    }
    if request.finality.allows_hot()
        && let Some(limit) = quota.max_hot_query_range_blocks
    {
        let requested = range.len();
        if requested > u128::from(limit) {
            return Err(DatalensError::new(
                DatalensErrorKind::RateLimited,
                "application hot query range quota exceeded",
            ));
        }
    }
    Ok(())
}

pub fn normalize_application_id(value: &str) -> Result<String, DatalensError> {
    // Application ids are used as metrics labels and warmup ownership keys, so
    // normalization is intentionally stricter than display names.
    let normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.starts_with('.')
        || normalized.ends_with('.')
        || normalized.contains('/')
        || normalized.contains('\\')
        || normalized.len() > 64
    {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "application id must be 1-64 characters using lowercase letters, digits, dot, underscore, or hyphen",
        ));
    }
    if !normalized.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    }) {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "application id must be 1-64 characters using lowercase letters, digits, dot, underscore, or hyphen",
        ));
    }
    Ok(normalized)
}

pub fn normalize_application_dataset_key(value: &str) -> Result<String, DatalensError> {
    let key = DatasetKey::parse(value)?;
    let value = key.as_str();
    if !supported_application_dataset_keys().contains(&value) {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            "application dataset must be a supported canonical dataset key",
        ));
    }
    Ok(value.to_owned())
}

pub fn supported_application_dataset_keys() -> &'static [&'static str] {
    &[
        "evm.blocks",
        "evm.transactions",
        "evm.receipts",
        "evm.logs",
        "solana.slots",
        "solana.blocks",
        "solana.transactions",
        "solana.instructions",
        "solana.account_updates",
        "tron.blocks",
        "tron.transactions",
        "tron.transaction_infos",
        "tron.events",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_authorize_application_operation_rejects_empty_allowlist() {
        let application = config::ApplicationConfig {
            id: "empty-ops".to_owned(),
            name: "empty-ops".to_owned(),
            enabled: true,
            display_name: None,
            token: "secret-token".to_owned(),
            chains: vec!["ethereum".to_owned()],
            datasets: vec!["evm.blocks".to_owned()],
            operations: Vec::new(),
            quota: None,
        };

        let error = authorize_application_operation(
            &application,
            config::ApplicationOperationConfig::Query,
        )
        .expect_err("empty operation allowlist rejected");

        assert_eq!(error.kind, DatalensErrorKind::Unauthorized);
    }
}
