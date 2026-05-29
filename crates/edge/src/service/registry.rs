use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use axum::http::HeaderMap;
use datalens_chain::ChainAdapter;
use datalens_core::{DatalensError, DatalensErrorKind, QueryFinalityRequirement};
use datalens_metrics::ApplicationIdentity;
use datalens_planner::NativeQueryInput;
use datalens_warmup::{
    WarmupRunResult, WarmupSubmitOutcome, WarmupSubmitRequest, WarmupTask, WarmupTaskFilter,
    WarmupTaskId,
};
use datalens_writer::DurableWriteResult;

use crate::{
    auth::application::{ApplicationContext, ApplicationRegistry},
    config,
    contract::{discovery::DiscoveryResponse, query::QueryApiRequest},
    service::{
        lifecycle::WarmupSchedulerHandle,
        query_service::{
            NativeQueryResponse, QueryService, RegisteredQueryService, RegisteredWarmupService,
        },
    },
};

#[derive(Clone, Default)]
pub struct QueryServiceRegistry {
    services: BTreeMap<String, Arc<dyn RegisteredQueryService>>,
    application_registry: ApplicationRegistry,
}

impl QueryServiceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_service<S>(mut self, service: QueryService<S>) -> Result<Self, DatalensError>
    where
        S: ChainAdapter + 'static,
    {
        let chain_name = service.chain_name().to_owned();
        if self.services.contains_key(&chain_name) {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("chain {chain_name} is already registered"),
            ));
        }
        self.services.insert(chain_name, Arc::new(service));
        Ok(self)
    }

    pub fn with_application_registry(
        mut self,
        config: config::ApplicationRegistryConfig,
    ) -> Result<Self, DatalensError> {
        self.application_registry = ApplicationRegistry::from_config(config)?;
        Ok(self)
    }

    pub fn chain_names(&self) -> Vec<String> {
        self.services.keys().cloned().collect()
    }

    pub fn query_native(
        &self,
        request: NativeQueryInput,
    ) -> Result<NativeQueryResponse, DatalensError> {
        self.query_native_with_application(request, None)
    }

    pub fn query_native_with_application(
        &self,
        request: NativeQueryInput,
        application: Option<ApplicationIdentity>,
    ) -> Result<NativeQueryResponse, DatalensError> {
        let chain_name = request.chain.configured_name();
        let service = self.services.get(chain_name).ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                format!("chain {chain_name} is not configured"),
            )
        })?;
        service.query_native(request, application)
    }

    pub fn discovery(&self) -> Result<DiscoveryResponse, DatalensError> {
        let chains = self
            .services
            .values()
            .map(|service| service.discovery())
            .collect::<Result<Vec<_>, _>>()?;
        Ok(DiscoveryResponse { chains })
    }

    pub(crate) fn authenticate_headers(
        &self,
        headers: &HeaderMap,
        chain: &str,
        dataset: &str,
        range_len: u128,
        finality: QueryFinalityRequirement,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        self.application_registry
            .authenticate_headers(headers, chain, dataset, range_len, finality)
    }

    pub(crate) fn authenticate_warmup_headers(
        &self,
        headers: &HeaderMap,
        chain: &str,
        dataset: &str,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        self.application_registry
            .authenticate_warmup_headers(headers, chain, dataset)
    }

    pub(crate) fn authenticate_native_query_headers(
        &self,
        headers: &HeaderMap,
        request: &QueryApiRequest,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        self.application_registry
            .authenticate_native_query_headers(headers, request)
    }

    pub(crate) fn authenticate_task_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<ApplicationContext>, DatalensError> {
        self.application_registry.authenticate_task_headers(headers)
    }

    pub fn submit_warmup_task(
        &self,
        request: WarmupSubmitRequest,
    ) -> Result<WarmupSubmitOutcome, DatalensError> {
        let service = self.warmup_service_for_chain(request.chain.configured_name())?;
        service.submit(request)
    }

    pub fn get_warmup_task(
        &self,
        task_id: &WarmupTaskId,
    ) -> Result<Option<WarmupTask>, DatalensError> {
        for service in self.services.values() {
            let Some(warmup) = service.warmup() else {
                continue;
            };
            if let Some(task) = warmup.get(task_id)? {
                return Ok(Some(task));
            }
        }
        Ok(None)
    }

    pub fn list_warmup_tasks(
        &self,
        filter: WarmupTaskFilter,
    ) -> Result<Vec<WarmupTask>, DatalensError> {
        let mut tasks = Vec::new();
        for service in self.services.values() {
            let Some(warmup) = service.warmup() else {
                continue;
            };
            tasks.extend(warmup.list(filter.clone())?);
        }
        tasks.sort_by(|left, right| left.task_id.as_str().cmp(right.task_id.as_str()));
        Ok(tasks)
    }

    pub fn pause_warmup_task(&self, task_id: &WarmupTaskId) -> Result<WarmupTask, DatalensError> {
        self.mutate_warmup_task(task_id, WarmupMutation::Pause)
    }

    pub fn cancel_warmup_task(&self, task_id: &WarmupTaskId) -> Result<WarmupTask, DatalensError> {
        self.mutate_warmup_task(task_id, WarmupMutation::Cancel)
    }

    pub fn retry_warmup_task(&self, task_id: &WarmupTaskId) -> Result<WarmupTask, DatalensError> {
        self.mutate_warmup_task(task_id, WarmupMutation::Retry)
    }

    pub fn run_warmup_once(&self) -> Result<Vec<WarmupRunResult>, DatalensError> {
        let mut results = Vec::new();
        for service in self.services.values() {
            let Some(warmup) = service.warmup() else {
                continue;
            };
            results.extend(warmup.run_available_once()?);
        }
        Ok(results)
    }

    pub fn start_warmup_scheduler(&self, interval: Duration) -> WarmupSchedulerHandle {
        let registry = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = stop.clone();
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                if let Err(error) = registry.run_warmup_once() {
                    log::warn!("warmup scheduler tick failed kind={:?}", error.kind);
                }
                thread::sleep(interval);
            }
        });
        WarmupSchedulerHandle {
            stop,
            handle: Some(handle),
        }
    }

    fn warmup_service_for_chain(
        &self,
        chain_name: &str,
    ) -> Result<Arc<dyn RegisteredWarmupService>, DatalensError> {
        self.services
            .get(chain_name)
            .and_then(|service| service.warmup())
            .ok_or_else(|| {
                DatalensError::new(
                    DatalensErrorKind::UnsupportedDataset,
                    format!("warmup is not configured for chain {chain_name}"),
                )
            })
    }

    fn mutate_warmup_task(
        &self,
        task_id: &WarmupTaskId,
        mutation: WarmupMutation,
    ) -> Result<WarmupTask, DatalensError> {
        let task = self.get_warmup_task(task_id)?.ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("warmup task {} not found", task_id.as_str()),
            )
        })?;
        let service = self.warmup_service_for_chain(task.chain.configured_name())?;
        match mutation {
            WarmupMutation::Pause => service.pause(task_id)?,
            WarmupMutation::Cancel => service.cancel(task_id)?,
            WarmupMutation::Retry => service.retry_failed(task_id)?,
        }
        service.get(task_id)?.ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::Internal,
                format!(
                    "warmup task {} disappeared after mutation",
                    task_id.as_str()
                ),
            )
        })
    }

    pub fn metrics_text(&self) -> Option<Result<String, DatalensError>> {
        let mut texts = Vec::new();
        for service in self.services.values() {
            match service.metrics_text() {
                Some(Ok(text)) => texts.push(text),
                Some(Err(error)) => return Some(Err(error)),
                None => {}
            }
        }
        if texts.is_empty() {
            None
        } else {
            Some(Ok(texts.join("\n")))
        }
    }

    pub fn flush_staged_writes_for_shutdown(
        &self,
    ) -> Result<Vec<DurableWriteResult>, DatalensError> {
        self.services
            .values()
            .map(|service| service.flush_staged_writes_for_shutdown())
            .collect()
    }
}

#[derive(Clone, Copy)]
pub(crate) enum WarmupMutation {
    Pause,
    Cancel,
    Retry,
}
