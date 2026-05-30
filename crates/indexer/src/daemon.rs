use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};

use datalens_client::{DatalensClient, HttpTransport};
use serde::Serialize;
use tokio::{sync::oneshot, task::JoinHandle};

use crate::{
    DatabaseDriver, DatalensIndexConfig, IndexPlanBuilder, IndexRunner, IndexRunnerOptions,
    IndexerError, OutputConfig, OutputSinkConfig, QueryProtocol, QueryableStore, SqliteOutputStore,
    StoreQuery, StoreQueryResult, graphql,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DaemonQueryMode {
    Disabled,
    Graphql,
}

pub fn validate_daemon_config(
    config: &DatalensIndexConfig,
) -> Result<DaemonQueryMode, IndexerError> {
    if !config.query.enabled {
        return Ok(DaemonQueryMode::Disabled);
    }
    if config.query.protocol != QueryProtocol::Graphql {
        return Err(IndexerError::Config(
            "query.protocol: daemon query service requires graphql".to_owned(),
        ));
    }
    let mode = match &config.output {
        OutputConfig::Database { database } if database.driver == DatabaseDriver::Sqlite => {
            DaemonQueryMode::Graphql
        }
        OutputConfig::Database { database } => {
            return Err(IndexerError::Config(format!(
                "query.enabled: daemon query service currently supports sqlite output, not {}",
                database.driver.as_str()
            )));
        }
        OutputConfig::Jsonl { .. } => {
            return Err(IndexerError::Config(
                "query.enabled: output kind jsonl does not support query service mode".to_owned(),
            ));
        }
        OutputConfig::Webhook { .. } => {
            return Err(IndexerError::Config(
                "query.enabled: output kind webhook does not support query service mode".to_owned(),
            ));
        }
    };
    config.query.bind.parse::<SocketAddr>().map_err(|error| {
        IndexerError::Config(format!("query.bind: invalid socket address: {error}"))
    })?;
    Ok(mode)
}

#[derive(Clone, Debug)]
pub struct IndexDaemonOptions {
    pub poll_interval: Duration,
    pub run_once: bool,
}

impl Default for IndexDaemonOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(30),
            run_once: false,
        }
    }
}

impl IndexDaemonOptions {
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    pub fn with_run_once(mut self, run_once: bool) -> Self {
        self.run_once = run_once;
        self
    }
}

pub struct IndexDaemon<T> {
    config: DatalensIndexConfig,
    client: DatalensClient<T>,
    options: IndexDaemonOptions,
}

impl<T> IndexDaemon<T>
where
    T: HttpTransport + Send + 'static,
{
    pub fn new(config: DatalensIndexConfig, client: DatalensClient<T>) -> Self {
        Self {
            config,
            client,
            options: IndexDaemonOptions::default(),
        }
    }

    pub fn with_options(mut self, options: IndexDaemonOptions) -> Self {
        self.options = options;
        self
    }

    pub async fn run_until_shutdown<S>(self, shutdown: S) -> Result<IndexDaemonReport, IndexerError>
    where
        S: Future<Output = ()> + Send,
    {
        let query_mode = validate_daemon_config(&self.config)?;
        let query_service = match query_mode {
            DaemonQueryMode::Disabled => None,
            DaemonQueryMode::Graphql => Some(start_graphql_service(&self.config).await?),
        };
        let mut report = IndexDaemonReport {
            index_runs: 0,
            query_service: query_service.as_ref().map(|service| service.report.clone()),
        };

        if self.options.run_once {
            self.run_index_cycle().await?;
            report.index_runs += 1;
            shutdown_query_service(query_service).await?;
            return Ok(report);
        }

        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => {
                    log::info!("index daemon shutdown requested");
                    break;
                }
                result = self.run_index_cycle() => {
                    result?;
                    report.index_runs += 1;
                }
            }
            tokio::select! {
                () = &mut shutdown => break,
                () = tokio::time::sleep(self.options.poll_interval) => {}
            }
        }

        shutdown_query_service(query_service).await?;
        Ok(report)
    }

    async fn run_index_cycle(&self) -> Result<(), IndexerError> {
        let config = self.config.clone();
        let client = self.client.clone();
        tokio::task::spawn_blocking(move || {
            log::info!(
                "index daemon cycle start index={} output={} query_enabled={}",
                config.index.name,
                config.output.capability().kind.as_str(),
                config.query.enabled
            );
            let plan = IndexPlanBuilder::new().build(&config)?;
            let options = IndexRunnerOptions::default().with_checkpoint_policy(config.checkpoint);
            let runner =
                IndexRunner::new(plan, output_sink_config(&config.output)).with_options(options);
            let report = runner.run(&client)?;
            log::info!(
                "index daemon cycle complete planned={} executed={} rows={}",
                report.summary.planned_queries,
                report.summary.executed_queries,
                report.summary.rows_written
            );
            Ok(())
        })
        .await
        .map_err(|error| IndexerError::Runner(format!("index daemon task failed: {error}")))?
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct IndexDaemonReport {
    pub index_runs: usize,
    pub query_service: Option<QueryServiceReport>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct QueryServiceReport {
    pub bind: SocketAddr,
    pub graphql_path: String,
}

struct RunningQueryService {
    report: QueryServiceReport,
    shutdown: oneshot::Sender<()>,
    handle: JoinHandle<Result<(), IndexerError>>,
}

struct DaemonSqliteQueryStore {
    url: String,
}

impl QueryableStore for DaemonSqliteQueryStore {
    fn query(&self, query: StoreQuery) -> Result<StoreQueryResult, IndexerError> {
        let store = SqliteOutputStore::connect(&self.url)
            .map_err(|error| IndexerError::Runner(format!("open sqlite query output: {error}")))?;
        store.query(query)
    }
}

async fn start_graphql_service(
    config: &DatalensIndexConfig,
) -> Result<RunningQueryService, IndexerError> {
    let OutputConfig::Database { database } = &config.output else {
        return Err(IndexerError::Config(
            "query.enabled: daemon GraphQL requires database output".to_owned(),
        ));
    };
    let listener = tokio::net::TcpListener::bind(&config.query.bind)
        .await
        .map_err(|error| IndexerError::Runner(format!("bind query service: {error}")))?;
    let bind = listener
        .local_addr()
        .map_err(|error| IndexerError::Runner(format!("read query service bind: {error}")))?;
    let url = database.url.clone();
    tokio::task::spawn_blocking(move || SqliteOutputStore::connect(&url).map(drop))
        .await
        .map_err(|error| IndexerError::Runner(format!("open sqlite query output task: {error}")))?
        .map_err(|error| IndexerError::Runner(format!("open sqlite query output: {error}")))?;
    let store = Arc::new(DaemonSqliteQueryStore {
        url: database.url.clone(),
    });
    let app = graphql::graphql_router(store, &config.query.path, config.query.playground);
    let graphql_path = config.query.path.clone();
    let (shutdown, shutdown_receiver) = oneshot::channel();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_receiver.await;
            })
            .await
            .map_err(|error| IndexerError::Runner(format!("query service failed: {error}")))
    });
    log::info!("index daemon GraphQL query service listening on {bind}");
    Ok(RunningQueryService {
        report: QueryServiceReport { bind, graphql_path },
        shutdown,
        handle,
    })
}

async fn shutdown_query_service(service: Option<RunningQueryService>) -> Result<(), IndexerError> {
    let Some(service) = service else {
        return Ok(());
    };
    let _ = service.shutdown.send(());
    service
        .handle
        .await
        .map_err(|error| IndexerError::Runner(format!("query service join failed: {error}")))??;
    log::info!("index daemon query service stopped");
    Ok(())
}

fn output_sink_config(output: &OutputConfig) -> OutputSinkConfig {
    match output {
        OutputConfig::Jsonl { path } => OutputSinkConfig::FileJson { path: path.clone() },
        OutputConfig::Database { database } => match database.driver {
            DatabaseDriver::Sqlite => OutputSinkConfig::DatabaseSqlite {
                url: database.url.clone(),
            },
            DatabaseDriver::Postgres => OutputSinkConfig::DatabasePostgres {
                url: database.url.clone(),
            },
        },
        OutputConfig::Webhook { webhook } => OutputSinkConfig::Webhook {
            webhook: webhook.clone(),
        },
    }
}
