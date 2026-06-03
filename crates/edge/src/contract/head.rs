use datalens_chain::{ChainHeight, FinalityKind};
use datalens_core::{ChainIdentity, DatalensError, DatalensErrorKind, LedgerRangeKind};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainHeadFinalityApi {
    #[default]
    Latest,
    Safe,
    Finalized,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ChainHeadApiResponse {
    pub chain: ChainIdentity,
    pub height: u64,
    pub finality: &'static str,
    pub range_kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<u64>,
}

impl ChainHeadFinalityApi {
    pub(crate) fn parse(value: Option<&str>) -> Result<Self, DatalensError> {
        match value.unwrap_or("latest") {
            "latest" => Ok(Self::Latest),
            "safe" => Ok(Self::Safe),
            "finalized" => Ok(Self::Finalized),
            finality => Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                format!("chain head finality {finality} is not supported"),
            )),
        }
    }
}

impl ChainHeadApiResponse {
    pub(crate) fn from_head(chain: ChainIdentity, head: ChainHeight) -> Self {
        Self {
            chain,
            height: head.value,
            finality: finality_name(head.finality),
            range_kind: range_kind_name(&head.range_kind),
            timestamp: None,
        }
    }
}

fn finality_name(finality: FinalityKind) -> &'static str {
    match finality {
        FinalityKind::Latest => "latest",
        FinalityKind::Safe => "safe",
        FinalityKind::Finalized => "finalized",
        FinalityKind::ChainSpecific(name) => name,
    }
}

fn range_kind_name(range_kind: &LedgerRangeKind) -> String {
    match range_kind {
        LedgerRangeKind::Block => "block".to_owned(),
        LedgerRangeKind::Slot => "slot".to_owned(),
        LedgerRangeKind::Height => "height".to_owned(),
        LedgerRangeKind::Other(kind) => kind.to_string(),
    }
}
