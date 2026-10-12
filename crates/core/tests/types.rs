use datalens_core::{ChainId, ChainName, Dataset, Error, Finality, QueryFinality};

#[test]
fn chain_name_accepts_lowercase_and_rejects_others() {
    assert_eq!(ChainName::new("arbitrum").unwrap().as_str(), "arbitrum");
    assert_eq!(ChainName::new("bsc-test_2").unwrap().as_str(), "bsc-test_2");
    for bad in ["", "Arbitrum", "arb itrum", "arb.one"] {
        assert_eq!(
            ChainName::new(bad).unwrap_err().kind(),
            "invalid_input",
            "{bad:?}"
        );
    }
}

#[test]
fn chain_name_serde_validates() {
    let name: ChainName = serde_json::from_str(r#""base""#).unwrap();
    assert_eq!(name.to_string(), "base");
    assert!(serde_json::from_str::<ChainName>(r#""BASE""#).is_err());
}

#[test]
fn chain_id_must_be_positive() {
    assert_eq!(ChainId::new(42161).unwrap().get(), 42161);
    assert!(ChainId::new(0).is_err());
    assert!(ChainId::new(-1).is_err());
    assert!(serde_json::from_str::<ChainId>("0").is_err());
    assert_eq!(serde_json::from_str::<ChainId>("46").unwrap().get(), 46);
}

#[test]
fn finality_codes_round_trip() {
    assert_eq!(Finality::Unsafe.as_i16(), 1);
    assert_eq!(Finality::Safe.as_i16(), 2);
    assert_eq!(Finality::Finalized.as_i16(), 3);
    assert_eq!(Finality::from_i16(2), Some(Finality::Safe));
    assert_eq!(Finality::from_i16(0), None);
    assert_eq!(Finality::from_i16(4), None);
    assert!(Finality::Unsafe < Finality::Safe && Finality::Safe < Finality::Finalized);
    assert_eq!(
        serde_json::to_string(&Finality::Finalized).unwrap(),
        r#""finalized""#
    );
}

#[test]
fn query_finality_defaults_to_durable_only_and_is_snake_case() {
    assert_eq!(QueryFinality::default(), QueryFinality::DurableOnly);
    assert_eq!(
        serde_json::to_string(&QueryFinality::SafeToLatest).unwrap(),
        r#""safe_to_latest""#
    );
    assert_eq!(
        serde_json::from_str::<QueryFinality>(r#""latest_only""#).unwrap(),
        QueryFinality::LatestOnly
    );
    assert_eq!(
        serde_json::from_str::<QueryFinality>(r#""durable_only""#).unwrap(),
        QueryFinality::DurableOnly
    );
}

#[test]
fn dataset_names() {
    assert_eq!(Dataset::EvmLogs.as_str(), "evm.logs");
    assert_eq!(Dataset::EvmBlocks.as_str(), "evm.blocks");
    assert_eq!("evm.blocks".parse::<Dataset>().unwrap(), Dataset::EvmBlocks);
    assert!("evm.transactions".parse::<Dataset>().is_err());
    assert_eq!(
        serde_json::to_string(&Dataset::EvmLogs).unwrap(),
        r#""evm.logs""#
    );
    assert_eq!(
        serde_json::from_str::<Dataset>(r#""evm.logs""#).unwrap(),
        Dataset::EvmLogs
    );
}

#[test]
fn error_kinds_are_stable() {
    let kinds: Vec<&str> = [
        Error::Unauthorized(String::new()),
        Error::Forbidden(String::new()),
        Error::InvalidInput(String::new()),
        Error::QuotaExceeded(String::new()),
        Error::Unsupported(String::new()),
        Error::Internal(String::new()),
    ]
    .iter()
    .map(Error::kind)
    .collect();
    assert_eq!(
        kinds,
        [
            "unauthorized",
            "forbidden",
            "invalid_input",
            "quota_exceeded",
            "unsupported",
            "internal"
        ]
    );
}
