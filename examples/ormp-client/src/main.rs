use std::{env, time::Duration};

use datalens_example_ormp_client::fetch_message_accepted_page;
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
        user_agent: Some("datalens-ormp-client-example".to_owned()),
    })?;

    let page = fetch_message_accepted_page(&client, after, 25)?;
    for event in page.events {
        println!(
            "{} {} {}",
            event.cursor,
            event.block_number.unwrap_or_default(),
            event.message_hash.unwrap_or_default()
        );
    }
    if page.has_next_page
        && let Some(cursor) = page.next_cursor
    {
        eprintln!("next cursor: {cursor}");
    }

    Ok(())
}
