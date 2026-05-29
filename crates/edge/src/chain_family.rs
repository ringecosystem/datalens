pub(crate) fn chain_family(
    kind: &str,
) -> Result<datalens_core::ChainFamily, datalens_core::DatalensError> {
    match kind {
        "evm" => Ok(datalens_core::ChainFamily::Evm),
        value => datalens_core::ChainFamily::try_other(value.to_owned()),
    }
}
