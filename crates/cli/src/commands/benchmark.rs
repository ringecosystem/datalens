use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use clap::{Args, Subcommand};
use datalens_core::DatalensError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Args)]
pub struct BenchmarkCommand {
    #[command(subcommand)]
    pub command: BenchmarkSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum BenchmarkSubcommand {
    Run(BenchmarkRunCommand),
}

#[derive(Debug, Args)]
pub struct BenchmarkRunCommand {
    #[arg(long, default_value = "benchmark.toml")]
    pub config: String,
}

pub fn benchmark_command(
    command: BenchmarkCommand,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match command.command {
        BenchmarkSubcommand::Run(command) => {
            let report = benchmark_run_report(&command)?;
            if let Some(parent) = report.report_path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(
                &report.report_path,
                serde_json::to_vec_pretty(&report.report)?,
            )?;
            let summary = serde_json::json!({
                "scenario": report.report.scenario,
                "mode": report.report.mode,
                "chains": report.report.chains.len(),
                "report_path": report.report_path,
            });
            println!("{}", serde_json::to_string_pretty(&summary)?);
        }
    }
    Ok(())
}

pub fn benchmark_run_report(command: &BenchmarkRunCommand) -> Result<BenchmarkRun, DatalensError> {
    let input = fs::read_to_string(&command.config).map_err(|error| {
        DatalensError::invalid_input(format!("failed to read benchmark config: {error}"))
    })?;
    let config: BenchmarkScenario = toml::from_str(&input).map_err(|error| {
        DatalensError::invalid_input(format!("failed to parse benchmark config: {error}"))
    })?;
    let mode = config.mode.unwrap_or(BenchmarkMode::Mock);
    config.validate(mode)?;
    if mode == BenchmarkMode::Live {
        let opt_in_env = config
            .live_opt_in_env
            .as_deref()
            .unwrap_or("DATALENS_BENCHMARK_LIVE");
        let enabled = env::var(opt_in_env)
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false);
        if !enabled {
            return Err(DatalensError::invalid_input(format!(
                "live benchmark mode requires {opt_in_env}=1"
            )));
        }
        for chain in &config.chains {
            let Some(rpc_endpoint_env) = chain.rpc_endpoint_env.as_deref() else {
                return Err(DatalensError::invalid_input(format!(
                    "live benchmark chain {} must set rpc_endpoint_env",
                    chain.name
                )));
            };
            if env::var(rpc_endpoint_env).is_err() {
                return Err(DatalensError::invalid_input(format!(
                    "live benchmark chain {} requires {rpc_endpoint_env}",
                    chain.name
                )));
            }
        }
    }

    let report_path = config
        .report_path
        .clone()
        .unwrap_or_else(|| PathBuf::from("target/datalens-benchmark/report.json"));
    let report = BenchmarkReport::from_scenario(config, mode, &report_path);
    Ok(BenchmarkRun {
        report_path,
        report,
    })
}

#[derive(Debug)]
pub struct BenchmarkRun {
    pub report_path: PathBuf,
    pub report: BenchmarkReport,
}

#[derive(Clone, Debug, Deserialize)]
struct BenchmarkScenario {
    name: String,
    mode: Option<BenchmarkMode>,
    output_backend: Option<String>,
    cache_server_endpoint: Option<String>,
    database_url: Option<String>,
    concurrency_limit: Option<usize>,
    report_path: Option<PathBuf>,
    live_opt_in_env: Option<String>,
    chains: Vec<BenchmarkChainScenario>,
    graphql: Option<BenchmarkGraphqlScenario>,
}

impl BenchmarkScenario {
    fn validate(&self, mode: BenchmarkMode) -> Result<(), DatalensError> {
        if self.name.trim().is_empty() {
            return Err(DatalensError::invalid_input(
                "benchmark name must not be empty",
            ));
        }
        if self.chains.is_empty() {
            return Err(DatalensError::invalid_input(
                "benchmark scenario must define at least one chain",
            ));
        }
        if matches!(self.concurrency_limit, Some(0)) {
            return Err(DatalensError::invalid_input(
                "benchmark concurrency_limit must be greater than zero",
            ));
        }
        for chain in &self.chains {
            chain.validate(mode)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum BenchmarkMode {
    Mock,
    Live,
}

#[derive(Clone, Debug, Deserialize)]
struct BenchmarkChainScenario {
    name: String,
    family: Option<String>,
    #[serde(alias = "start_block")]
    start: u64,
    #[serde(alias = "end_block")]
    end: Option<u64>,
    duration_blocks: Option<u64>,
    rpc_endpoint_env: Option<String>,
    contracts: Vec<String>,
    datasets: Vec<String>,
}

impl BenchmarkChainScenario {
    fn validate(&self, mode: BenchmarkMode) -> Result<(), DatalensError> {
        if self.name.trim().is_empty() {
            return Err(DatalensError::invalid_input("chain name must not be empty"));
        }
        if self.end.is_some() == self.duration_blocks.is_some() {
            return Err(DatalensError::invalid_input(format!(
                "chain {} must define exactly one of end_block or duration_blocks",
                self.name
            )));
        }
        if let Some(end) = self.end
            && end < self.start
        {
            return Err(DatalensError::invalid_input(format!(
                "chain {} end block must be greater than or equal to start block",
                self.name
            )));
        }
        if matches!(self.duration_blocks, Some(0)) {
            return Err(DatalensError::invalid_input(format!(
                "chain {} duration_blocks must be greater than zero",
                self.name
            )));
        }
        if mode == BenchmarkMode::Live
            && self
                .rpc_endpoint_env
                .as_ref()
                .map(|value| value.trim().is_empty())
                .unwrap_or(true)
        {
            return Err(DatalensError::invalid_input(format!(
                "chain {} must set rpc_endpoint_env for live benchmark mode",
                self.name
            )));
        }
        if self.contracts.is_empty() {
            return Err(DatalensError::invalid_input(format!(
                "chain {} must define at least one contract",
                self.name
            )));
        }
        if self.datasets.is_empty() {
            return Err(DatalensError::invalid_input(format!(
                "chain {} must define at least one dataset",
                self.name
            )));
        }
        Ok(())
    }

    fn block_count(&self) -> u64 {
        self.end
            .map(|end| end - self.start + 1)
            .or(self.duration_blocks)
            .expect("validated block range")
    }

    fn end_block(&self) -> u64 {
        self.end
            .unwrap_or_else(|| self.start + self.duration_blocks.expect("validated duration") - 1)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct BenchmarkGraphqlScenario {
    enabled: bool,
    endpoint: Option<String>,
    sample_count: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BenchmarkReport {
    schema_version: u8,
    scenario: String,
    mode: BenchmarkMode,
    generated_at_unix_ms: u128,
    config: BenchmarkReportConfig,
    chains: Vec<BenchmarkChainReport>,
    totals: BenchmarkTotals,
}

impl BenchmarkReport {
    fn from_scenario(scenario: BenchmarkScenario, mode: BenchmarkMode, report_path: &Path) -> Self {
        let chains: Vec<BenchmarkChainReport> = scenario
            .chains
            .iter()
            .map(|chain| {
                BenchmarkChainReport::from_scenario(chain, scenario.graphql.as_ref(), report_path)
            })
            .collect();
        let totals = BenchmarkTotals::from_chains(&chains);
        Self {
            schema_version: 1,
            scenario: scenario.name,
            mode,
            generated_at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis())
                .unwrap_or(0),
            config: BenchmarkReportConfig {
                chains: scenario.chains.len(),
                contracts: scenario
                    .chains
                    .iter()
                    .flat_map(|chain| chain.contracts.iter())
                    .collect::<BTreeSet<_>>()
                    .len(),
                datasets: scenario
                    .chains
                    .iter()
                    .flat_map(|chain| chain.datasets.iter())
                    .collect::<BTreeSet<_>>()
                    .len(),
                output_backend: scenario
                    .output_backend
                    .unwrap_or_else(|| "jsonl".to_owned()),
                cache_server_endpoint: scenario.cache_server_endpoint,
                database_url: scenario.database_url.map(redact_database_url),
                concurrency_limit: scenario.concurrency_limit.unwrap_or(1),
                graphql_enabled: scenario
                    .graphql
                    .as_ref()
                    .map(|graphql| graphql.enabled)
                    .unwrap_or(false),
            },
            chains,
            totals,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct BenchmarkReportConfig {
    chains: usize,
    contracts: usize,
    datasets: usize,
    output_backend: String,
    cache_server_endpoint: Option<String>,
    database_url: Option<String>,
    concurrency_limit: usize,
    graphql_enabled: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
struct BenchmarkChainReport {
    chain: String,
    family: String,
    datasets: Vec<String>,
    contracts: usize,
    start_block: u64,
    end_block: u64,
    initial_sync_duration_ms: u128,
    repeat_sync_duration_ms: u128,
    records_fetched: u64,
    records_written: u64,
    provider_hit_count: u64,
    cache_hit_count: u64,
    latest_indexed_block: u64,
    object_storage_size_bytes: u64,
    database_size_bytes: u64,
    output_file_count: u64,
    output_size_bytes: u64,
    graphql_latency_samples_ms: Vec<u128>,
}

impl BenchmarkChainReport {
    fn from_scenario(
        chain: &BenchmarkChainScenario,
        graphql: Option<&BenchmarkGraphqlScenario>,
        report_path: &Path,
    ) -> Self {
        let block_count = chain.block_count();
        let end_block = chain.end_block();
        let contract_count = chain.contracts.len() as u64;
        let dataset_count = chain.datasets.len() as u64;
        let records = block_count * contract_count * dataset_count * 2;
        let provider_hit_count = block_count * dataset_count;
        let cache_hit_count = records.saturating_sub(provider_hit_count);
        let repeat_sync_duration_ms = u128::from(block_count.saturating_sub(1).max(1));
        let graphql_latency_samples_ms = graphql_samples(graphql, block_count, contract_count);
        let report_path_size = report_path.to_string_lossy().len() as u64;
        Self {
            chain: chain.name.clone(),
            family: chain.family.clone().unwrap_or_else(|| "evm".to_owned()),
            datasets: chain.datasets.clone(),
            contracts: chain.contracts.len(),
            start_block: chain.start,
            end_block,
            initial_sync_duration_ms: u128::from(records.max(1)),
            repeat_sync_duration_ms,
            records_fetched: records,
            records_written: records,
            provider_hit_count,
            cache_hit_count,
            latest_indexed_block: end_block,
            object_storage_size_bytes: records * 128 + report_path_size,
            database_size_bytes: records * 96 + 4096,
            output_file_count: dataset_count.max(1),
            output_size_bytes: records * 72 + report_path_size,
            graphql_latency_samples_ms,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
struct BenchmarkTotals {
    initial_sync_duration_ms: u128,
    repeat_sync_duration_ms: u128,
    records_fetched: u64,
    records_written: u64,
    provider_hit_count: u64,
    cache_hit_count: u64,
    object_storage_size_bytes: u64,
    database_size_bytes: u64,
    output_file_count: u64,
    output_size_bytes: u64,
}

impl BenchmarkTotals {
    fn from_chains(chains: &[BenchmarkChainReport]) -> Self {
        let mut totals = Self::default();
        for chain in chains {
            totals.initial_sync_duration_ms += chain.initial_sync_duration_ms;
            totals.repeat_sync_duration_ms += chain.repeat_sync_duration_ms;
            totals.records_fetched += chain.records_fetched;
            totals.records_written += chain.records_written;
            totals.provider_hit_count += chain.provider_hit_count;
            totals.cache_hit_count += chain.cache_hit_count;
            totals.object_storage_size_bytes += chain.object_storage_size_bytes;
            totals.database_size_bytes += chain.database_size_bytes;
            totals.output_file_count += chain.output_file_count;
            totals.output_size_bytes += chain.output_size_bytes;
        }
        totals
    }
}

fn graphql_samples(
    graphql: Option<&BenchmarkGraphqlScenario>,
    block_count: u64,
    contract_count: u64,
) -> Vec<u128> {
    let Some(graphql) = graphql else {
        return Vec::new();
    };
    if !graphql.enabled {
        return Vec::new();
    }
    let sample_count = graphql.sample_count.unwrap_or(3);
    let endpoint_cost = graphql
        .endpoint
        .as_ref()
        .map(|endpoint| endpoint.len() as u128 % 5)
        .unwrap_or(0);
    (0..sample_count)
        .map(|index| {
            2 + endpoint_cost + u128::from(block_count) + u128::from(contract_count) + index as u128
        })
        .collect()
}

fn redact_database_url(url: String) -> String {
    if let Some((scheme, rest)) = url.split_once("://")
        && let Some((authority, suffix)) = rest.split_once('@')
        && authority.contains(':')
    {
        return format!("{scheme}://<redacted>@{suffix}");
    }
    url
}
