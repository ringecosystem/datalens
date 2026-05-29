use async_graphql::{Enum, Error, InputObject};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, DatasetKey, LedgerRangeKind,
    LogFilter, NetworkId,
};
use datalens_warmup::{WarmupChunkPolicy, WarmupRetryPolicy, WarmupTaskFilter};
use serde::Deserialize;

use crate::contract::{
    query::{FieldSelectionApi, QueryApiRequest, QueryRangeApi, QuerySelectorApi},
    warmup::{WarmupDatasetKeyApi, WarmupSelectorApiRequest, WarmupSubmitApiRequest},
};

use super::graphql_error;

#[derive(InputObject)]
pub(crate) struct QueryInput {
    chain: ChainIdentityInput,
    dataset_key: DatasetKeyInput,
    selector: QuerySelectorInput,
    range: QueryRangeInput,
    finality: Option<String>,
    fields: Option<FieldSelectionInput>,
}

impl QueryInput {
    pub(crate) fn into_request(self) -> async_graphql::Result<QueryApiRequest> {
        Ok(QueryApiRequest {
            chain: self.chain.into_chain_identity()?,
            dataset_key: self.dataset_key.into_dataset_key()?.as_str().to_owned(),
            selector: self.selector.into_query_selector()?,
            range: self.range.into_query_range(),
            finality: parse_optional_json_value(self.finality, "durable_only")?,
            fields: self
                .fields
                .map(FieldSelectionInput::into_field_selection)
                .unwrap_or_default(),
        })
    }
}

#[derive(InputObject)]
pub(crate) struct WarmupSubmitInput {
    chain: ChainIdentityInput,
    dataset_key: WarmupDatasetKeyInput,
    selector: WarmupSelectorInput,
    range_kind: RangeKindInput,
    start: u64,
    end: Option<u64>,
    mode: Option<String>,
    chunk_policy: Option<WarmupChunkPolicyInput>,
    retry_policy: Option<WarmupRetryPolicyInput>,
}

impl WarmupSubmitInput {
    pub(crate) fn into_request(self) -> async_graphql::Result<WarmupSubmitApiRequest> {
        Ok(WarmupSubmitApiRequest {
            chain: self.chain.into_chain_identity()?,
            dataset_key: WarmupDatasetKeyApi::Structured(self.dataset_key.into_dataset_key()?),
            selector: self.selector.into_warmup_selector()?,
            range_kind: self.range_kind.into_range_kind()?,
            start: self.start,
            end: self.end,
            mode: parse_optional_json_value(self.mode, "fixed_range")?,
            chunk_policy: self
                .chunk_policy
                .map(WarmupChunkPolicyInput::into_chunk_policy)
                .unwrap_or_default(),
            retry_policy: self
                .retry_policy
                .map(WarmupRetryPolicyInput::into_retry_policy)
                .unwrap_or_default(),
        })
    }
}

#[derive(InputObject)]
pub(crate) struct ChainIdentityInput {
    family: ChainFamilyInput,
    configured_name: String,
    network_id: Option<NetworkIdInput>,
}

impl ChainIdentityInput {
    fn into_chain_identity(self) -> async_graphql::Result<ChainIdentity> {
        ChainIdentity::try_new(
            self.family.into_chain_family()?,
            self.configured_name,
            self.network_id
                .map(NetworkIdInput::into_network_id)
                .transpose()?,
        )
        .map_err(graphql_error)
    }
}

#[derive(InputObject)]
pub(crate) struct ChainFamilyInput {
    kind: ChainFamilyKindInput,
    other: Option<String>,
}

impl ChainFamilyInput {
    fn into_chain_family(self) -> async_graphql::Result<ChainFamily> {
        match self.kind {
            ChainFamilyKindInput::Evm => Ok(ChainFamily::Evm),
            ChainFamilyKindInput::Other => {
                let Some(other) = self.other else {
                    return Err(invalid_input("chain family other value is required"));
                };
                ChainFamily::try_other(other).map_err(graphql_error)
            }
        }
    }
}

#[derive(Enum, Clone, Copy, Eq, PartialEq)]
#[graphql(rename_items = "snake_case")]
pub(crate) enum ChainFamilyKindInput {
    Evm,
    Other,
}

#[derive(InputObject)]
pub(crate) struct NetworkIdInput {
    numeric: Option<u64>,
    textual: Option<String>,
}

impl NetworkIdInput {
    fn into_network_id(self) -> async_graphql::Result<NetworkId> {
        match (self.numeric, self.textual) {
            (Some(value), None) => Ok(NetworkId::numeric(value)),
            (None, Some(value)) => NetworkId::textual(value).map_err(graphql_error),
            (None, None) => Err(invalid_input(
                "network id numeric or textual value is required",
            )),
            (Some(_), Some(_)) => Err(invalid_input(
                "network id must provide either numeric or textual, not both",
            )),
        }
    }
}

#[derive(InputObject)]
pub(crate) struct DatasetKeyInput {
    family: String,
    name: String,
}

impl DatasetKeyInput {
    fn into_dataset_key(self) -> async_graphql::Result<DatasetKey> {
        dataset_key_from_parts(self.family, self.name)
    }
}

#[derive(InputObject)]
pub(crate) struct WarmupDatasetKeyInput {
    family: String,
    name: String,
}

impl WarmupDatasetKeyInput {
    fn into_dataset_key(self) -> async_graphql::Result<DatasetKey> {
        dataset_key_from_parts(self.family, self.name)
    }
}

#[derive(InputObject)]
pub(crate) struct QueryRangeInput {
    kind: QueryRangeKindInput,
    start: u64,
    end: u64,
}

impl QueryRangeInput {
    fn into_query_range(self) -> QueryRangeApi {
        match self.kind {
            QueryRangeKindInput::Block => QueryRangeApi::Block {
                start: self.start,
                end: self.end,
            },
            QueryRangeKindInput::Slot => QueryRangeApi::Slot {
                start: self.start,
                end: self.end,
            },
            QueryRangeKindInput::Height => QueryRangeApi::Height {
                start: self.start,
                end: self.end,
            },
        }
    }
}

#[derive(Enum, Clone, Copy, Eq, PartialEq)]
#[graphql(rename_items = "snake_case")]
pub(crate) enum QueryRangeKindInput {
    Block,
    Slot,
    Height,
}

#[derive(InputObject)]
pub(crate) struct RangeKindInput {
    kind: RangeKindValueInput,
    other: Option<String>,
}

impl RangeKindInput {
    fn into_range_kind(self) -> async_graphql::Result<LedgerRangeKind> {
        match self.kind {
            RangeKindValueInput::Block => Ok(LedgerRangeKind::Block),
            RangeKindValueInput::Slot => Ok(LedgerRangeKind::Slot),
            RangeKindValueInput::Height => Ok(LedgerRangeKind::Height),
            RangeKindValueInput::Other => {
                let Some(other) = self.other else {
                    return Err(invalid_input("range kind other value is required"));
                };
                serde_json::from_value(serde_json::json!({
                    "kind": "other",
                    "value": other
                }))
                .map_err(|error| {
                    graphql_error(DatalensError::new(
                        DatalensErrorKind::InvalidInput,
                        format!("invalid range kind: {error}"),
                    ))
                })
            }
        }
    }
}

#[derive(Enum, Clone, Copy, Eq, PartialEq)]
#[graphql(rename_items = "snake_case")]
pub(crate) enum RangeKindValueInput {
    Block,
    Slot,
    Height,
    Other,
}

#[derive(InputObject)]
pub(crate) struct QuerySelectorInput {
    kind: SelectorKindInput,
    evm_logs: Option<EvmLogsSelectorInput>,
    other: Option<OtherSelectorInput>,
}

impl QuerySelectorInput {
    fn into_query_selector(self) -> async_graphql::Result<QuerySelectorApi> {
        match self.kind {
            SelectorKindInput::All => Ok(QuerySelectorApi::All),
            SelectorKindInput::EvmLogs => {
                let Some(filter) = self.evm_logs else {
                    return Err(invalid_input("evmLogs selector value is required"));
                };
                Ok(QuerySelectorApi::EvmLogs(filter.into_log_filter()))
            }
            SelectorKindInput::Other => {
                let Some(other) = self.other else {
                    return Err(invalid_input("other selector value is required"));
                };
                Ok(QuerySelectorApi::Other {
                    kind: other.kind,
                    fingerprint: other.fingerprint,
                    canonical_key: other.canonical_key,
                })
            }
        }
    }
}

#[derive(InputObject)]
pub(crate) struct WarmupSelectorInput {
    kind: SelectorKindInput,
    evm_logs: Option<EvmLogsSelectorInput>,
    other: Option<OtherSelectorInput>,
}

impl WarmupSelectorInput {
    fn into_warmup_selector(self) -> async_graphql::Result<WarmupSelectorApiRequest> {
        match self.kind {
            SelectorKindInput::All => Ok(WarmupSelectorApiRequest::All),
            SelectorKindInput::EvmLogs => {
                let Some(filter) = self.evm_logs else {
                    return Err(invalid_input("evmLogs selector value is required"));
                };
                Ok(WarmupSelectorApiRequest::EvmLogs(filter.into_log_filter()))
            }
            SelectorKindInput::Other => {
                let Some(other) = self.other else {
                    return Err(invalid_input("other selector value is required"));
                };
                Ok(WarmupSelectorApiRequest::Other {
                    kind: other.kind,
                    fingerprint: other.fingerprint,
                    canonical_key: other.canonical_key,
                })
            }
        }
    }
}

#[derive(Enum, Clone, Copy, Eq, PartialEq)]
#[graphql(rename_items = "snake_case")]
pub(crate) enum SelectorKindInput {
    All,
    EvmLogs,
    Other,
}

#[derive(InputObject)]
pub(crate) struct EvmLogsSelectorInput {
    addresses: Option<Vec<String>>,
    topics: Option<Vec<Option<Vec<String>>>>,
}

impl EvmLogsSelectorInput {
    fn into_log_filter(self) -> LogFilter {
        LogFilter {
            addresses: self.addresses.unwrap_or_default(),
            topics: self.topics.unwrap_or_default(),
        }
    }
}

#[derive(InputObject)]
pub(crate) struct OtherSelectorInput {
    kind: String,
    fingerprint: String,
    canonical_key: String,
}

#[derive(InputObject)]
pub(crate) struct FieldSelectionInput {
    include: Option<Vec<String>>,
}

impl FieldSelectionInput {
    fn into_field_selection(self) -> FieldSelectionApi {
        match self.include {
            Some(fields) => FieldSelectionApi::Include(fields),
            None => FieldSelectionApi::All,
        }
    }
}

#[derive(InputObject)]
pub(crate) struct WarmupChunkPolicyInput {
    max_range_len: Option<u64>,
    target_rows_hint: Option<usize>,
}

impl WarmupChunkPolicyInput {
    fn into_chunk_policy(self) -> WarmupChunkPolicy {
        WarmupChunkPolicy {
            max_range_len: self.max_range_len.unwrap_or(1_000),
            target_rows_hint: self.target_rows_hint,
        }
    }
}

#[derive(InputObject)]
pub(crate) struct WarmupRetryPolicyInput {
    max_attempts: Option<u32>,
    initial_backoff_ms: Option<u64>,
    max_backoff_ms: Option<u64>,
}

impl WarmupRetryPolicyInput {
    fn into_retry_policy(self) -> WarmupRetryPolicy {
        let default = WarmupRetryPolicy::default();
        WarmupRetryPolicy {
            max_attempts: self.max_attempts.unwrap_or(default.max_attempts),
            initial_backoff_ms: self
                .initial_backoff_ms
                .unwrap_or(default.initial_backoff_ms),
            max_backoff_ms: self.max_backoff_ms.unwrap_or(default.max_backoff_ms),
        }
    }
}

#[derive(InputObject)]
pub(crate) struct WarmupTaskFilterInput {
    application_id: Option<String>,
    chain: Option<String>,
    state: Option<String>,
}

impl WarmupTaskFilterInput {
    pub(crate) fn into_filter(self) -> async_graphql::Result<WarmupTaskFilter> {
        Ok(WarmupTaskFilter {
            application_id: self.application_id,
            chain_key: self.chain,
            state: self.state.map(parse_json_value).transpose()?,
        })
    }
}

fn invalid_input(message: impl Into<String>) -> Error {
    graphql_error(DatalensError::new(DatalensErrorKind::InvalidInput, message))
}

fn dataset_key_from_parts(family: String, name: String) -> async_graphql::Result<DatasetKey> {
    let family = if family == "evm" {
        ChainFamily::Evm
    } else {
        ChainFamily::try_other(family).map_err(graphql_error)?
    };
    DatasetKey::try_new(family, name).map_err(graphql_error)
}

fn parse_optional_json_value<T>(value: Option<String>, default: &str) -> async_graphql::Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    parse_json_value(value.unwrap_or_else(|| default.to_owned()))
}

fn parse_json_value<T>(value: String) -> async_graphql::Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(serde_json::Value::String(value)).map_err(|error| {
        graphql_error(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("invalid enum value: {error}"),
        ))
    })
}
