use datalens_core::{Finality, Heads};
use datalens_provider::{ProviderError, Purpose};
use serde_json::{Value, json};

use crate::client::{EvmClient, HeadMode};
use crate::decode::parse_block;

impl EvmClient {
    pub async fn heads(
        &self,
        mode: HeadMode,
        purpose: Purpose,
        application: Option<&str>,
    ) -> Result<Heads, ProviderError> {
        match mode {
            HeadMode::Lag {
                safe_lag_blocks,
                finalized_lag_blocks,
            } => {
                let value = self
                    .gateway
                    .call(
                        "eth_getBlockByNumber",
                        json!(["latest", false]),
                        purpose,
                        application,
                    )
                    .await?;
                let latest = parse_block(&value, Finality::Unsafe)?;
                Ok(Heads {
                    latest: latest.number,
                    latest_hash: latest.hash,
                    safe: latest.number.saturating_sub(safe_lag_blocks),
                    finalized: latest.number.saturating_sub(finalized_lag_blocks),
                })
            }
            HeadMode::Tags => {
                let results = self
                    .gateway
                    .batch(
                        vec![
                            ("eth_getBlockByNumber".to_owned(), json!(["latest", false])),
                            ("eth_getBlockByNumber".to_owned(), json!(["safe", false])),
                            (
                                "eth_getBlockByNumber".to_owned(),
                                json!(["finalized", false]),
                            ),
                        ],
                        purpose,
                        application,
                    )
                    .await?;
                let mut results = results.into_iter();
                let latest = parse_block(&next_item(&mut results)?, Finality::Unsafe)?;
                let safe = parse_block(&next_item(&mut results)?, Finality::Safe)?;
                let finalized = parse_block(&next_item(&mut results)?, Finality::Finalized)?;
                Ok(Heads {
                    latest: latest.number,
                    latest_hash: latest.hash,
                    safe: safe.number,
                    finalized: finalized.number,
                })
            }
        }
    }
}

fn next_item(
    results: &mut impl Iterator<Item = Result<Value, ProviderError>>,
) -> Result<Value, ProviderError> {
    results
        .next()
        .unwrap_or_else(|| Err(ProviderError::fatal("short head batch")))
}
