use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use axum::Router;

use crate::{
    config::EdgeConfig, http::router::router_with_edge_config,
    service::registry::QueryServiceRegistry,
};

pub struct WarmupSchedulerHandle {
    pub(crate) stop: Arc<AtomicBool>,
    pub(crate) handle: Option<thread::JoinHandle<()>>,
}

pub trait LifecycleShutdown: Send + 'static {
    fn shutdown(self);
}

impl WarmupSchedulerHandle {
    pub fn shutdown(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take()
            && let Err(error) = handle.join()
        {
            log::warn!("warmup scheduler thread join failed: {error:?}");
        }
    }
}

impl LifecycleShutdown for WarmupSchedulerHandle {
    fn shutdown(self) {
        WarmupSchedulerHandle::shutdown(self);
    }
}

impl Drop for WarmupSchedulerHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take()
            && let Err(error) = handle.join()
        {
            log::warn!("warmup scheduler thread join failed: {error:?}");
        }
    }
}

pub struct ServiceLifecycle<S = NoopLifecycleShutdown> {
    registry: QueryServiceRegistry,
    edge: EdgeConfig,
    extra_router: Option<Router>,
    warmup_scheduler: Option<S>,
}

pub struct NoopLifecycleShutdown;

impl LifecycleShutdown for NoopLifecycleShutdown {
    fn shutdown(self) {}
}

impl ServiceLifecycle<NoopLifecycleShutdown> {
    pub fn new(registry: QueryServiceRegistry) -> Self {
        Self {
            registry,
            edge: EdgeConfig::default(),
            extra_router: None,
            warmup_scheduler: None,
        }
    }

    pub fn new_with_edge_config(registry: QueryServiceRegistry, edge: EdgeConfig) -> Self {
        Self {
            registry,
            edge,
            extra_router: None,
            warmup_scheduler: None,
        }
    }
}

impl<S> ServiceLifecycle<S> {
    pub fn with_warmup_scheduler<T>(self, scheduler: T) -> ServiceLifecycle<T>
    where
        T: LifecycleShutdown,
    {
        ServiceLifecycle {
            registry: self.registry,
            edge: self.edge,
            extra_router: self.extra_router,
            warmup_scheduler: Some(scheduler),
        }
    }

    pub fn with_extra_router(mut self, router: Router) -> Self {
        self.extra_router = Some(router);
        self
    }

    fn registry(&self) -> QueryServiceRegistry {
        self.registry.clone()
    }

    fn edge_config(&self) -> EdgeConfig {
        self.edge.clone()
    }

    fn router(&self) -> Router {
        let router = router_with_edge_config(self.registry(), self.edge_config());
        match &self.extra_router {
            Some(extra) => router.merge(extra.clone()),
            None => router,
        }
    }
}

impl<S> ServiceLifecycle<S>
where
    S: LifecycleShutdown,
{
    pub fn shutdown(self) -> Result<(), std::io::Error> {
        if let Some(scheduler) = self.warmup_scheduler {
            scheduler.shutdown();
        }
        flush_registry_staged_writes_for_shutdown(&self.registry)
    }
}

pub async fn serve(bind: SocketAddr, registry: QueryServiceRegistry) -> Result<(), std::io::Error> {
    serve_lifecycle(bind, ServiceLifecycle::new(registry)).await
}

pub async fn serve_lifecycle<S>(
    bind: SocketAddr,
    lifecycle: ServiceLifecycle<S>,
) -> Result<(), std::io::Error>
where
    S: LifecycleShutdown,
{
    let listener = tokio::net::TcpListener::bind(bind).await?;
    log::info!("api listener bound to {bind}");
    let router = lifecycle.router();
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                log::error!("failed to listen for shutdown signal: {error}");
            }
        })
        .await?;
    lifecycle.shutdown()
}

fn flush_registry_staged_writes_for_shutdown(
    registry: &QueryServiceRegistry,
) -> Result<(), std::io::Error> {
    registry
        .flush_staged_writes_for_shutdown()
        .map(|results| {
            let flushed_objects = results
                .iter()
                .map(|result| result.data_objects.len())
                .sum::<usize>();
            if flushed_objects > 0 {
                log::info!("flushed {flushed_objects} staged durable objects during shutdown");
            }
        })
        .map_err(|error| std::io::Error::other(error.to_string()))
}
