use clap::Parser;
use datalens_api::{auth::NoAuthentication, compatibility::NativeCompatibility};
use datalens_chain::ChainAdapter;
use datalens_core::{ChainFamily, ChainIdentity, DatasetId, TimeRange};
use datalens_evm::{EvmAdapter, EvmAdapterMetadata};
use datalens_planner::PlanRequest;
use datalens_storage::InMemoryStorage;
use datalens_writer::WriteRequest;

#[derive(Debug, Parser)]
#[command(name = "datalens")]
struct Cli {}

fn main() {
    let _cli = Cli::parse();
    let chain = ChainIdentity::new(ChainFamily::Evm, "ethereum-mainnet");
    let dataset = DatasetId::new("logs");
    let range = TimeRange::blocks(0, 0);
    let adapter = EvmAdapter::new(EvmAdapterMetadata::default());

    let _auth = NoAuthentication;
    let _compatibility = NativeCompatibility;
    let _storage = InMemoryStorage;
    let _plan = PlanRequest::new(chain.clone(), dataset.clone(), range.clone());
    let _write = WriteRequest::new(chain, dataset, range);
    let _capabilities = adapter.capabilities();
}
