use std::{fs, path::PathBuf, sync::Arc};

use axum::Router;
use clap::Parser;
use datalens_client::DatalensClient;
use datalens_event_counter_processor::{
    EventCounterExampleConfig, EventCounterGraphqlSchema, EventCounterProcessor,
    EventCounterSchemaInitializer,
};
use datalens_indexer::{
    ApplicationGraphqlSchemaContext, ApplicationGraphqlSchemaHook, DatalensIndexConfig,
    IndexPlanBuilder, ProcessorRuntime, ProcessorRuntimeOptions,
    graphql::graphql_application_router_with_auth,
};

#[derive(Debug, Parser)]
struct Args {
    #[arg(
        long,
        default_value = "examples/event-counter-processor/event-counter.index.toml"
    )]
    config: PathBuf,
    #[arg(long)]
    from_start: bool,
    #[arg(long)]
    no_checkpoint: bool,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    serve_query: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    let input = fs::read_to_string(&args.config)?;
    let index_config = DatalensIndexConfig::from_toml_str(&input)?;
    let example_config = EventCounterExampleConfig::from_toml_str(&input)?;
    let plan = IndexPlanBuilder::new().build(&index_config)?;
    let client = DatalensClient::new(index_config.client.to_datalens_client_config())?;
    let store = example_config.connect_store().await?;
    let options = ProcessorRuntimeOptions::default()
        .with_checkpoint_policy(index_config.checkpoint.clone())
        .with_no_checkpoint(args.no_checkpoint)
        .with_from_start(args.from_start)
        .with_dry_run(args.dry_run);

    let report = ProcessorRuntime::new(plan, EventCounterProcessor::default(), store.clone())
        .with_schema_initializer(EventCounterSchemaInitializer)
        .with_options(options)
        .run(&client)
        .await?;
    println!("{}", serde_json::to_string_pretty(&report)?);

    if args.serve_query && index_config.query.enabled {
        let schema = EventCounterGraphqlSchema
            .build_schema(ApplicationGraphqlSchemaContext::new(Arc::new(store)))?;
        let app: Router = graphql_application_router_with_auth(
            schema,
            &index_config.query.path,
            index_config.query.playground,
            index_config.query.auth.clone(),
            None,
        );
        let listener = tokio::net::TcpListener::bind(&index_config.query.bind).await?;
        axum::serve(listener, app).await?;
    }

    Ok(())
}
