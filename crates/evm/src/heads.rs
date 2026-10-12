use datalens_core::{BlockRow, Finality, Heads};
use datalens_provider::{ProviderError, Purpose, TokenWait};
use serde_json::{Value, json};

use crate::client::{EvmClient, HeadMode};
use crate::decode::parse_block;

const UNSUPPORTED_SAFE: &str = "chain does not support safe tag; use lag mode";

impl EvmClient {
    pub async fn heads(
        &self,
        mode: HeadMode,
        purpose: Purpose,
        application: Option<&str>,
        wait: TokenWait,
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
                        wait,
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
                        wait,
                    )
                    .await?;
                let mut results = results.into_iter();
                let latest = parse_block(&next_item(&mut results)?, Finality::Unsafe)?;
                let safe = parse_safe_tag(next_item(&mut results))?;
                let finalized = parse_block(&next_item(&mut results)?, Finality::Finalized)?;
                if finalized.number > safe.number || safe.number > latest.number {
                    return Err(ProviderError::fatal("head tags are out of order"));
                }
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

fn parse_safe_tag(value: Result<Value, ProviderError>) -> Result<BlockRow, ProviderError> {
    let value = value.map_err(|_| ProviderError::fatal(UNSUPPORTED_SAFE))?;
    parse_block(&value, Finality::Safe).map_err(|error| match error {
        ProviderError::NotReady => ProviderError::fatal(UNSUPPORTED_SAFE),
        other => other,
    })
}

fn next_item(
    results: &mut impl Iterator<Item = Result<Value, ProviderError>>,
) -> Result<Value, ProviderError> {
    results
        .next()
        .unwrap_or_else(|| Err(ProviderError::fatal("short head batch")))
}
