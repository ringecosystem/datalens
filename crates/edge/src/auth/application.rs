use std::collections::BTreeMap;

use axum::http::{HeaderMap, header};
use datalens_core::{DatalensError, DatalensErrorKind, QueryFinalityRequirement};
use datalens_metrics::ApplicationIdentity;

use crate::{config, contract::query::QueryApiRequest};

pub const APPLICATION_HEADER: &str = "x-datalens-application";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthContext {
    pub subject: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Authenticated application context carried from edge routes into metrics,
/// usage ledger attribution, and warmup ownership checks. Authentication proves
/// the token; authorization and quota checks are route-specific.
pub struct ApplicationContext {
    pub id: String,
    pub name: String,
    pub display_name: Option<String>,
    pub quota: Option<config::ApplicationQuotaConfig>,
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
        authorize_application_dataset(application, chain, dataset)?;
        enforce_quota(application, range_len, finality)?;
        Ok(Some(ApplicationContext {
            id: application.id.clone(),
            name: application.name.clone(),
            display_name: application.display_name.clone(),
            quota: application.quota.clone(),
        }))
    }

    pub fn authenticate_warmup_headers(
        &self,
        headers: &HeaderMap,
        chain: &str,
        dataset: &str,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        if !self.required {
            return Ok(None);
        }
        let application = self.authenticate_application_headers(headers)?;
        authorize_application_dataset(application, chain, dataset)?;
        Ok(Some(ApplicationContext {
            id: application.id.clone(),
            name: application.name.clone(),
            display_name: application.display_name.clone(),
            quota: application.quota.clone(),
        }))
    }

    pub fn authenticate_native_query_headers(
        &self,
        headers: &HeaderMap,
        request: &QueryApiRequest,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        if !self.required {
            return Ok(None);
        }
        let application = self.authenticate_application_headers(headers)?;
        authorize_application_dataset(
            application,
            request.chain.configured_name(),
            &request.dataset_key,
        )?;
        enforce_native_query_quota(application, request)?;
        Ok(Some(ApplicationContext {
            id: application.id.clone(),
            name: application.name.clone(),
            display_name: application.display_name.clone(),
            quota: application.quota.clone(),
        }))
    }

    pub fn authenticate_task_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        if !self.required {
            return Ok(None);
        }
        let application = self.authenticate_application_headers(headers)?;
        Ok(Some(ApplicationContext {
            id: application.id.clone(),
            name: application.name.clone(),
            display_name: application.display_name.clone(),
            quota: application.quota.clone(),
        }))
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
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
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
    if !application.chains.iter().any(|allowed| allowed == chain) {
        return Err(DatalensError::new(
            DatalensErrorKind::Unauthorized,
            "application is not allowed to access this chain",
        ));
    }
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
