use std::{env, time::Duration};

use datalens_example_degov_client::{ProposalMaterializer, ProposalProjection, consume_vote_page};
use datalens_sdk::{ClientConfig, DatalensClient};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = env::var("DATALENS_INDEX_GRAPHQL_URL")
        .unwrap_or_else(|_| "http://127.0.0.1:8080/graphql".to_owned());
    let bearer_token = env::var("DATALENS_TOKEN").ok();
    let after = env::var("DATALENS_AFTER_CURSOR").ok();
    let client = DatalensClient::new(ClientConfig {
        endpoint,
        bearer_token,
        timeout: Some(Duration::from_secs(10)),
        user_agent: Some("datalens-degov-client-example".to_owned()),
    })?;
    let mut materializer = ProposalMaterializer::default();
    let mut projection = ProposalProjection::default();

    let checkpoint = consume_vote_page(&client, &mut materializer, &mut projection, after, 25)?;
    for vote in materializer.consumed() {
        println!(
            "{} proposal={} support={} weight={}",
            vote.cursor,
            vote.proposal_id,
            vote.support.unwrap_or_default(),
            vote.weight
        );
    }
    if checkpoint.has_next_page
        && let Some(cursor) = checkpoint.cursor
    {
        eprintln!("next cursor: {cursor}");
    }

    Ok(())
}
