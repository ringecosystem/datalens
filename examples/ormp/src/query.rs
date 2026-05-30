use datalens_client::{DatalensClient, HttpTransport, QueryRequest, QueryResponse, QuerySelector};
use datalens_core::{
    BlockRange, ChainFamily, ChainIdentity, DatasetKey, LedgerRange, LogFilter, NetworkId,
    QueryFinalityRequirement,
};

use crate::{
    ETHEREUM_CHAIN_ID, MSGPORT_ADDRESS, ORMP_ADDRESS, OrmpConfig, OrmpExampleError, OrmpPlanJob,
};

pub fn build_query_request(
    from_block: u64,
    to_block: u64,
) -> Result<QueryRequest, OrmpExampleError> {
    build_evm_logs_request(
        "ethereum",
        ETHEREUM_CHAIN_ID,
        from_block,
        to_block,
        vec![MSGPORT_ADDRESS.to_owned(), ORMP_ADDRESS.to_owned()],
    )
}

pub fn build_job_query_request(job: &OrmpPlanJob) -> Result<QueryRequest, OrmpExampleError> {
    build_evm_logs_request(
        &job.chain,
        job.chain_id,
        job.from_block,
        job.to_block,
        job.addresses.clone(),
    )
}

fn build_evm_logs_request(
    chain: &str,
    chain_id: u64,
    from_block: u64,
    to_block: u64,
    addresses: Vec<String>,
) -> Result<QueryRequest, OrmpExampleError> {
    Ok(QueryRequest::new(
        evm_chain(chain, chain_id)?,
        DatasetKey::evm_logs(),
        LedgerRange::from_block_range(BlockRange::try_new(from_block, to_block)?),
    )
    .with_selector(QuerySelector::EvmLogs(LogFilter {
        addresses,
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

fn evm_chain(configured_name: &str, chain_id: u64) -> Result<ChainIdentity, OrmpExampleError> {
    Ok(ChainIdentity::try_new(
        ChainFamily::Evm,
        configured_name,
        Some(NetworkId::numeric(chain_id)),
    )?)
}
