use std::net::SocketAddr;

use datalens_core::{ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, NetworkId};
use datalens_edge::config::{ChainConfig, DatalensConfig};

pub use datalens_core::redact_url;

pub(crate) fn configured_chain<'a>(
    config: &'a DatalensConfig,
    name: &str,
) -> Result<(&'a str, &'a ChainConfig), DatalensError> {
    config
        .chains
        .get_key_value(name)
        .map(|(name, chain)| (name.as_str(), chain))
        .ok_or_else(|| {
            DatalensError::new(
                DatalensErrorKind::UnsupportedDataset,
                format!("chain {name} is not configured"),
            )
        })
}

pub(crate) fn chain_identity(
    name: &str,
    chain: &ChainConfig,
) -> Result<ChainIdentity, DatalensError> {
    let family = match chain.kind.as_str() {
        "evm" => ChainFamily::Evm,
        value => ChainFamily::try_other(value.to_owned())?,
    };
    ChainIdentity::try_new(family, name, Some(NetworkId::numeric(chain.chain_id)))
}

pub(crate) fn parse_bind(value: &str) -> Result<SocketAddr, DatalensError> {
    value.parse().map_err(|error| {
        DatalensError::new(
            DatalensErrorKind::InvalidInput,
            format!("server.bind must be a socket address: {error}"),
        )
    })
}
