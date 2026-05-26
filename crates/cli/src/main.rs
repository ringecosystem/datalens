use clap::Parser;
use datalens_api::{Source, config::DatalensConfig};
use datalens_chain::ChainAdapter;
use datalens_core::{BlockHeader, BlockRange, DatalensError, LogFilter, LogRecord};
use datalens_evm::{EvmAdapter, EvmAdapterMetadata, EvmRpcClient};
use datalens_storage::LocalStorage;

#[derive(Debug, Parser)]
#[command(name = "datalens")]
struct Cli {
    #[arg(long, default_value = "datalens.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let config = DatalensConfig::from_file(&cli.config)?;
    let (chain_name, chain) = config
        .chains
        .iter()
        .next()
        .ok_or("config must define at least one chain")?;
    let bind = config.server.bind.parse()?;
    let storage = LocalStorage::new(&config.storage.root);
    let source = EvmSource(EvmRpcClient::new(chain.rpc_urls.clone()));
    let service = datalens_api::QueryService::new_named(
        storage,
        source,
        config.planner.clone(),
        config.writer.clone(),
        chain_name.clone(),
        chain.clone(),
    );

    let adapter = EvmAdapter::new(EvmAdapterMetadata::default());
    let _capabilities = adapter.capabilities();

    datalens_api::serve(bind, service, vec![chain_name.clone()]).await?;
    Ok(())
}

#[derive(Clone)]
struct EvmSource(EvmRpcClient);

impl Source for EvmSource {
    fn fetch_blocks(&self, range: BlockRange) -> Result<Vec<BlockHeader>, DatalensError> {
        self.0.fetch_blocks(range)
    }

    fn fetch_logs(
        &self,
        range: BlockRange,
        filter: &LogFilter,
    ) -> Result<Vec<LogRecord>, DatalensError> {
        self.0.fetch_logs(range, filter)
    }
}
