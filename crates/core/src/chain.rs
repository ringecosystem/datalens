use serde::{Deserialize, Serialize};

use crate::{DatalensError, DatalensErrorKind};

#[derive(Deserialize)]
enum RawChainFamily {
    Evm,
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawChainFamily")]
pub enum ChainFamily {
    Evm,
    Other(String),
}

impl TryFrom<RawChainFamily> for ChainFamily {
    type Error = DatalensError;

    fn try_from(value: RawChainFamily) -> Result<Self, Self::Error> {
        match value {
            RawChainFamily::Evm => Ok(Self::Evm),
            RawChainFamily::Other(value) => Self::try_other(value),
        }
    }
}

impl ChainFamily {
    pub fn try_other(value: impl Into<String>) -> Result<Self, DatalensError> {
        let value = validate_identifier("chain family", value.into())?;
        Ok(Self::Other(value))
    }

    pub fn key(&self) -> &str {
        match self {
            Self::Evm => "evm",
            Self::Other(value) => value,
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum RawNetworkId {
    Numeric(u64),
    Textual(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
#[serde(try_from = "RawNetworkId")]
pub enum NetworkId {
    Numeric(u64),
    Textual(String),
}

impl TryFrom<RawNetworkId> for NetworkId {
    type Error = DatalensError;

    fn try_from(value: RawNetworkId) -> Result<Self, Self::Error> {
        match value {
            RawNetworkId::Numeric(value) => Ok(Self::Numeric(value)),
            RawNetworkId::Textual(value) => Self::textual(value),
        }
    }
}

impl NetworkId {
    pub fn numeric(value: u64) -> Self {
        Self::Numeric(value)
    }

    pub fn textual(value: impl Into<String>) -> Result<Self, DatalensError> {
        Ok(Self::Textual(validate_identifier(
            "network id",
            value.into(),
        )?))
    }

    pub fn key(&self) -> String {
        match self {
            Self::Numeric(value) => value.to_string(),
            Self::Textual(value) => value.clone(),
        }
    }
}

#[derive(Deserialize)]
struct RawChainIdentity {
    family: ChainFamily,
    configured_name: String,
    #[serde(default)]
    network_id: Option<NetworkId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawChainIdentity")]
/// Stable identity for a configured chain as it appears in storage keys,
/// metrics labels, and API contracts. The configured name and optional network
/// id are validated to stay path-safe because durable object keys derive from
/// this value.
pub struct ChainIdentity {
    family: ChainFamily,
    configured_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    network_id: Option<NetworkId>,
}

impl TryFrom<RawChainIdentity> for ChainIdentity {
    type Error = DatalensError;

    fn try_from(raw: RawChainIdentity) -> Result<Self, Self::Error> {
        Self::try_new(raw.family, raw.configured_name, raw.network_id)
    }
}

impl ChainIdentity {
    pub fn expect_new(family: ChainFamily, id: impl Into<String>) -> Self {
        Self::try_new(family, id, None).expect("valid chain identity")
    }

    pub fn expect_with_network_id(
        family: ChainFamily,
        configured_name: impl Into<String>,
        network_id: NetworkId,
    ) -> Self {
        Self::try_new(family, configured_name, Some(network_id)).expect("valid chain identity")
    }

    pub fn try_new(
        family: ChainFamily,
        configured_name: impl Into<String>,
        network_id: Option<NetworkId>,
    ) -> Result<Self, DatalensError> {
        if matches!(&family, ChainFamily::Other(value) if value.trim().is_empty()) {
            return Err(DatalensError::new(
                DatalensErrorKind::InvalidInput,
                "chain family must not be empty",
            ));
        }
        Ok(Self {
            family,
            configured_name: validate_identifier("configured chain name", configured_name.into())?,
            network_id,
        })
    }

    pub fn family(&self) -> ChainFamily {
        self.family.clone()
    }

    pub fn family_ref(&self) -> &ChainFamily {
        &self.family
    }

    pub fn configured_name(&self) -> &str {
        &self.configured_name
    }

    pub fn network_id(&self) -> Option<&NetworkId> {
        self.network_id.as_ref()
    }

    pub fn id(&self) -> &str {
        &self.configured_name
    }

    pub fn key_prefix(&self) -> String {
        match &self.network_id {
            Some(network_id) => format!(
                "{}/{}/{}",
                self.family.key(),
                self.configured_name,
                network_id.key()
            ),
            None => format!("{}/{}", self.family.key(), self.configured_name),
        }
    }
}

pub(crate) fn validate_identifier(kind: &str, value: String) -> Result<String, DatalensError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{kind} must not be empty"),
        ));
    }
    if value.contains('/') || value.contains('\\') {
        return Err(DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("{kind} must not contain path separators"),
        ));
    }
    Ok(value.to_owned())
}
