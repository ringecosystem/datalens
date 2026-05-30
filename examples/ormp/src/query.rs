use datalens_client::{DatalensClient, HttpTransport, QueryRequest, QueryResponse, QuerySelector};
use datalens_core::{
    BlockRange, ChainFamily, ChainIdentity, DatasetKey, LedgerRange, LogFilter, NetworkId,
    QueryFinalityRequirement,
};

use crate::{ETHEREUM_CHAIN_ID, MSGPORT_ADDRESS, ORMP_ADDRESS, OrmpConfig, OrmpExampleError};

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

fn ethereum_chain() -> ChainIdentity {
    ChainIdentity::expect_with_network_id(
        ChainFamily::Evm,
        "ethereum",
        NetworkId::numeric(ETHEREUM_CHAIN_ID),
    )
}
