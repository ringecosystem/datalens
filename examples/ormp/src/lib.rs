use std::{env, error::Error, fmt};

use datalens_client::{
    DatalensClient, DatalensClientConfig, HttpTransport, QueryRequest, QueryResponse, QuerySelector,
};
use datalens_core::{
    BlockRange, ChainFamily, ChainIdentity, DatasetKey, LedgerRange, LogFilter, NetworkId,
    QueryFinalityRequirement, QueryRows, missing_ranges,
};
use serde::Serialize;

pub const ETHEREUM_CHAIN_ID: u64 = 1;
pub const ORMP_START_BLOCK: u64 = 20009590;
pub const MSGPORT_ADDRESS: &str = "0x2cd1867fb8016f93710b6386f7f9f1d540a60812";
pub const ORMP_ADDRESS: &str = "0x13b2211a7ca45db2808f6db05557ce5347e3634e";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrmpConfig {
    pub endpoint: String,
    pub application: String,
    pub bearer_token: Option<String>,
    pub from_block: u64,
    pub to_block: u64,
}

impl OrmpConfig {
    pub fn from_env() -> Result<Self, OrmpExampleError> {
        let from_block = read_u64_env("ORMP_FROM_BLOCK")?;
        let to_block = read_u64_env("ORMP_TO_BLOCK")?;
        Ok(Self {
            endpoint: read_required_env("DATALENS_ENDPOINT")?,
            application: read_required_env("DATALENS_APPLICATION")?,
            bearer_token: read_optional_env("DATALENS_PUBLIC_APP_TOKEN"),
            from_block,
            to_block,
        })
    }

    pub fn client_config(&self) -> DatalensClientConfig {
        DatalensClientConfig {
            endpoint: self.endpoint.clone(),
            application: Some(self.application.clone()),
            bearer_token: self.bearer_token.clone(),
        }
    }
}

pub fn build_query_request(
    from_block: u64,
    to_block: u64,
) -> Result<QueryRequest, OrmpExampleError> {
    Ok(QueryRequest::new(
        ethereum_chain(),
        DatasetKey::evm_logs(),
        LedgerRange::from_block_range(BlockRange::try_new(from_block, to_block)?),
    )
    .with_selector(QuerySelector::EvmLogs(LogFilter {
        addresses: vec![MSGPORT_ADDRESS.to_owned(), ORMP_ADDRESS.to_owned()],
        topics: Vec::new(),
    }))
    .with_finality(QueryFinalityRequirement::DurableOnly))
}

pub fn query_with_client<T>(
    client: &DatalensClient<T>,
    config: &OrmpConfig,
) -> Result<QueryResponse, datalens_client::ClientError>
where
    T: HttpTransport,
{
    let request = build_query_request(config.from_block, config.to_block)
        .map_err(|error| datalens_client::ClientError::InvalidInput(error.to_string()))?;
    client.query(request)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OrmpSummary {
    pub requested_range: RangeSummary,
    pub row_count: usize,
    pub hit_ranges: Vec<RangeSummary>,
    pub missing_ranges: Vec<RangeSummary>,
    pub durable_hit_ranges: Vec<RangeSummary>,
    pub provider_fill_ranges: Vec<RangeSummary>,
    pub first_log_block: Option<u64>,
    pub last_log_block: Option<u64>,
    pub contract_addresses: Vec<String>,
    pub full_durable_cache_hit: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RangeSummary {
    Block { start: u64, end: u64 },
    Slot { start: u64, end: u64 },
    Height { start: u64, end: u64 },
    Other { name: String, start: u64, end: u64 },
}

pub fn summarize_response(response: &QueryResponse) -> Result<OrmpSummary, OrmpExampleError> {
    if response.dataset_key != DatasetKey::evm_logs() {
        return Err(OrmpExampleError::InvalidResponse(
            "expected evm.logs response".to_owned(),
        ));
    }

    let logs = match response.rows.rows() {
        QueryRows::EvmLogs(logs) => logs,
        _ => {
            return Err(OrmpExampleError::InvalidResponse(
                "expected EVM log rows".to_owned(),
            ));
        }
    };
    let first_log_block = logs.iter().map(|log| log.block_number).min();
    let last_log_block = logs.iter().map(|log| log.block_number).max();
    let full_durable_cache_hit = response.cache.missing_ranges.is_empty()
        && response.cache.provider_fill_ranges.is_empty()
        && missing_ranges(response.range.clone(), &response.cache.durable_hit_ranges).is_empty();

    Ok(OrmpSummary {
        requested_range: RangeSummary::from_ledger_range(&response.range),
        row_count: response.rows.row_count(),
        hit_ranges: summarize_ranges(&response.cache.hit_ranges),
        missing_ranges: summarize_ranges(&response.cache.missing_ranges),
        durable_hit_ranges: summarize_ranges(&response.cache.durable_hit_ranges),
        provider_fill_ranges: summarize_ranges(&response.cache.provider_fill_ranges),
        first_log_block,
        last_log_block,
        contract_addresses: vec![MSGPORT_ADDRESS.to_owned(), ORMP_ADDRESS.to_owned()],
        full_durable_cache_hit,
    })
}

impl RangeSummary {
    fn from_ledger_range(range: &LedgerRange) -> Self {
        match range.kind() {
            datalens_core::LedgerRangeKind::Block => Self::Block {
                start: range.start(),
                end: range.end(),
            },
            datalens_core::LedgerRangeKind::Slot => Self::Slot {
                start: range.start(),
                end: range.end(),
            },
            datalens_core::LedgerRangeKind::Height => Self::Height {
                start: range.start(),
                end: range.end(),
            },
            datalens_core::LedgerRangeKind::Other(kind) => Self::Other {
                name: kind.to_owned(),
                start: range.start(),
                end: range.end(),
            },
        }
    }
}

#[derive(Debug)]
pub enum OrmpExampleError {
    MissingEnv(&'static str),
    InvalidEnv { name: &'static str, message: String },
    InvalidResponse(String),
    Datalens(datalens_core::DatalensError),
}

impl fmt::Display for OrmpExampleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnv(name) => write!(f, "missing required environment variable {name}"),
            Self::InvalidEnv { name, message } => {
                write!(f, "invalid environment variable {name}: {message}")
            }
            Self::InvalidResponse(message) => f.write_str(message),
            Self::Datalens(error) => write!(f, "{error}"),
        }
    }
}

impl Error for OrmpExampleError {}

impl From<datalens_core::DatalensError> for OrmpExampleError {
    fn from(error: datalens_core::DatalensError) -> Self {
        Self::Datalens(error)
    }
}

fn ethereum_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(
        ChainFamily::Evm,
        "ethereum",
        NetworkId::numeric(ETHEREUM_CHAIN_ID),
    )
}

fn summarize_ranges(ranges: &[LedgerRange]) -> Vec<RangeSummary> {
    ranges.iter().map(RangeSummary::from_ledger_range).collect()
}

fn read_required_env(name: &'static str) -> Result<String, OrmpExampleError> {
    read_optional_env(name).ok_or(OrmpExampleError::MissingEnv(name))
}

fn read_optional_env(name: &'static str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

fn read_u64_env(name: &'static str) -> Result<u64, OrmpExampleError> {
    read_required_env(name)?
        .parse::<u64>()
        .map_err(|error| OrmpExampleError::InvalidEnv {
            name,
            message: error.to_string(),
        })
}
