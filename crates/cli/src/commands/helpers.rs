use std::net::SocketAddr;

use datalens_core::{ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, NetworkId};
use datalens_edge::config::{ChainConfig, DatalensConfig};

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

pub fn redact_url(value: &str) -> String {
    let Some((scheme, rest)) = value.split_once("://") else {
        return "<redacted>".to_owned();
    };
    let (without_fragment, fragment) = rest
        .split_once('#')
        .map(|(value, fragment)| (value, Some(fragment)))
        .unwrap_or((rest, None));
    let (without_query, query) = without_fragment
        .split_once('?')
        .map(|(value, query)| (value, Some(query)))
        .unwrap_or((without_fragment, None));
    let (authority, path) = without_query
        .split_once('/')
        .map(|(authority, path)| (authority, Some(path)))
        .unwrap_or((without_query, None));
    let authority = authority
        .rsplit_once('@')
        .map(|(_, host)| format!("<redacted>@{host}"))
        .unwrap_or_else(|| authority.to_owned());

    let mut output = format!("{scheme}://{authority}");
    if let Some(path) = path {
        output.push('/');
        output.push_str(path);
    }
    if let Some(query) = query {
        output.push('?');
        output.push_str(&redact_query(query));
    }
    if let Some(fragment) = fragment {
        output.push('#');
        output.push_str(fragment);
    }
    output
}

fn redact_query(query: &str) -> String {
    query
        .split('&')
        .map(|parameter| {
            let (name, separator, value) = parameter
                .split_once('=')
                .map(|(name, value)| (name, "=", value))
                .unwrap_or((parameter, "", ""));
            if is_sensitive_query_parameter(name) {
                format!("{name}=<redacted>")
            } else {
                format!("{name}{separator}{value}")
            }
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn is_sensitive_query_parameter(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "token"
            | "access_token"
            | "api_key"
            | "apikey"
            | "key"
            | "secret"
            | "password"
            | "signature"
    )
}
