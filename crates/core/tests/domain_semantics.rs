use datalens_core::{
    BlockHeader, BlockRange, ChainFamily, ChainIdentity, DatalensError, DatalensErrorKind, Dataset,
    DatasetId, EvmLogFilter, EvmReceipt, EvmTransaction, LogFilter, LogRecord, NetworkId,
    QueryDataFinality, QueryRows, QuerySegmentMetadata, QuerySegmentSource, TimeRange, TopicFilter,
};

#[test]
fn test_chain_identity_validates_configured_name_and_network_id() {
    let identity = ChainIdentity::try_new(
        ChainFamily::Evm,
        "ethereum-mainnet",
        Some(NetworkId::numeric(1)),
    )
    .expect("valid chain identity");

    assert_eq!(identity.family(), ChainFamily::Evm);
    assert_eq!(identity.configured_name(), "ethereum-mainnet");
    assert_eq!(identity.network_id(), Some(&NetworkId::numeric(1)));

    assert!(ChainIdentity::try_new(ChainFamily::Evm, " ", None).is_err());
    assert!(ChainIdentity::try_new(ChainFamily::Other(" ".to_owned()), "chain", None).is_err());
    assert!(NetworkId::textual(" ").is_err());
}

#[test]
fn test_block_range_inclusive_math_handles_edges() {
    let single = BlockRange::try_new(7, 7).expect("single block");
    assert_eq!(single.len(), 1);
    assert!(single.contains(7));

    let range = BlockRange::try_new(10, 14).expect("multi block");
    assert_eq!(range.len(), 5);
    assert_eq!(
        range.intersection(&BlockRange::try_new(12, 20).unwrap()),
        Some(BlockRange::try_new(12, 14).unwrap())
    );
    assert!(range.overlaps(&BlockRange::try_new(14, 20).unwrap()));
    assert!(!range.overlaps(&BlockRange::try_new(15, 20).unwrap()));
    assert_eq!(
        range.difference(&BlockRange::try_new(12, 13).unwrap()),
        vec![
            BlockRange::try_new(10, 11).unwrap(),
            BlockRange::try_new(14, 14).unwrap()
        ]
    );
    assert_eq!(
        range.split(2).expect("split"),
        vec![
            BlockRange::try_new(10, 11).unwrap(),
            BlockRange::try_new(12, 13).unwrap(),
            BlockRange::try_new(14, 14).unwrap()
        ]
    );

    let max = BlockRange::try_new(u64::MAX, u64::MAX).expect("max block");
    assert_eq!(max.len(), 1);
    assert_eq!(max.split(10).unwrap(), vec![max]);
    assert_eq!(
        BlockRange::try_new(0, u64::MAX).unwrap().len(),
        u128::from(u64::MAX) + 1
    );
    assert!(BlockRange::try_new(2, 1).is_err());
    assert!(range.split(0).is_err());
}

#[test]
fn test_dataset_key_has_builtin_chain_neutral_ids() {
    assert_eq!(
        datalens_core::DatasetKey::evm_blocks().as_str(),
        "evm.blocks"
    );
    assert_eq!(datalens_core::DatasetKey::evm_logs().as_str(), "evm.logs");
    assert_eq!(
        datalens_core::DatasetKey::evm_transactions().as_str(),
        "evm.transactions"
    );
    assert_eq!(
        datalens_core::DatasetKey::evm_receipts().as_str(),
        "evm.receipts"
    );
    assert_eq!(
        datalens_core::DatasetKey::tron_blocks().as_str(),
        "tron.blocks"
    );
    assert_eq!(
        datalens_core::DatasetKey::tron_events().as_str(),
        "tron.events"
    );
    assert_eq!(
        datalens_core::DatasetKey::solana_slots().as_str(),
        "solana.slots"
    );
    assert_eq!(
        datalens_core::DatasetKey::solana_transactions().as_str(),
        "solana.transactions"
    );
    assert_eq!(
        datalens_core::DatasetKey::solana_instructions().as_str(),
        "solana.instructions"
    );
    assert_eq!(
        datalens_core::DatasetKey::solana_account_updates().as_str(),
        "solana.account_updates"
    );
    assert_eq!(
        datalens_core::DatasetKey::from(Dataset::Logs),
        datalens_core::DatasetKey::evm_logs()
    );
    assert_eq!(
        datalens_core::DatasetKey::parse("evm.blocks").expect("parsed key"),
        datalens_core::DatasetKey::evm_blocks()
    );
    assert_eq!(
        datalens_core::DatasetKey::parse("solana.slots").expect("parsed key"),
        datalens_core::DatasetKey::solana_slots()
    );
    assert!(datalens_core::DatasetKey::parse("blocks").is_err());
    assert!(datalens_core::DatasetKey::try_new(ChainFamily::Evm, "bad/path").is_err());
}

#[test]
fn test_ledger_range_supports_block_slot_and_height_math() {
    let range = datalens_core::LedgerRange::blocks(10, 14).expect("valid range");
    assert_eq!(range.kind(), datalens_core::LedgerRangeKind::Block);
    assert_eq!(range.len(), 5);
    assert!(range.contains(12));
    assert_eq!(
        range.intersection(&datalens_core::LedgerRange::blocks(12, 20).unwrap()),
        Some(datalens_core::LedgerRange::blocks(12, 14).unwrap())
    );
    assert!(range.overlaps(&datalens_core::LedgerRange::blocks(14, 20).unwrap()));
    assert!(!range.overlaps(&datalens_core::LedgerRange::slots(14, 20).unwrap()));
    assert_eq!(
        range.difference(&datalens_core::LedgerRange::blocks(12, 13).unwrap()),
        vec![
            datalens_core::LedgerRange::blocks(10, 11).unwrap(),
            datalens_core::LedgerRange::blocks(14, 14).unwrap()
        ]
    );
    assert_eq!(
        range.split(2).expect("split"),
        vec![
            datalens_core::LedgerRange::blocks(10, 11).unwrap(),
            datalens_core::LedgerRange::blocks(12, 13).unwrap(),
            datalens_core::LedgerRange::blocks(14, 14).unwrap()
        ]
    );

    let slot = datalens_core::LedgerRange::slots(1, 1).expect("slot range");
    assert_eq!(slot.kind(), datalens_core::LedgerRangeKind::Slot);
    assert_eq!(slot.start(), 1);
    assert_eq!(slot.end(), 1);
    assert!(datalens_core::LedgerRange::heights(2, 1).is_err());
    assert!(range.split(0).is_err());
}

#[test]
fn test_missing_ranges_handles_unsorted_and_mixed_kind_coverage() {
    let missing = datalens_core::missing_ranges(
        datalens_core::LedgerRange::blocks(4, 8).expect("valid range"),
        &[
            datalens_core::LedgerRange::slots(4, 8).expect("other kind ignored"),
            datalens_core::LedgerRange::blocks(7, 7).expect("valid range"),
            datalens_core::LedgerRange::blocks(5, 6).expect("valid range"),
        ],
    );

    assert_eq!(
        missing,
        vec![
            datalens_core::LedgerRange::blocks(4, 4).expect("valid range"),
            datalens_core::LedgerRange::blocks(8, 8).expect("valid range"),
        ]
    );
}

#[test]
fn test_query_segment_metadata_marks_source_and_finality() {
    let durable = QuerySegmentMetadata::new(
        datalens_core::LedgerRange::blocks(1, 10).expect("valid range"),
        QuerySegmentSource::Durable,
        QueryDataFinality::Finalized,
    );
    let hot = QuerySegmentMetadata::new(
        datalens_core::LedgerRange::blocks(11, 12).expect("valid range"),
        QuerySegmentSource::Hot,
        QueryDataFinality::Unsafe,
    );
    let live = QuerySegmentMetadata::new(
        datalens_core::LedgerRange::blocks(13, 13).expect("valid range"),
        QuerySegmentSource::Provider,
        QueryDataFinality::Latest,
    );

    assert_eq!(durable.source.as_str(), "durable");
    assert_eq!(durable.finality.as_str(), "finalized");
    assert_eq!(hot.source.as_str(), "hot");
    assert_eq!(hot.finality.as_str(), "unsafe");
    assert_eq!(live.source.as_str(), "provider");
    assert_eq!(live.finality.as_str(), "latest");
}

#[test]
fn test_query_rows_sort_deduplicates_blocks_and_logs_stably() {
    let mut blocks =
        QueryRows::EvmBlocks(vec![block(2, "0x02"), block(1, "0x01"), block(2, "0x02")]);
    blocks.sort();

    assert_eq!(
        blocks,
        QueryRows::EvmBlocks(vec![block(1, "0x01"), block(2, "0x02")])
    );

    let mut logs = QueryRows::EvmLogs(vec![log(2, 1), log(1, 0), log(2, 1)]);
    logs.sort();

    assert_eq!(logs, QueryRows::EvmLogs(vec![log(1, 0), log(2, 1)]));

    let mut transactions = QueryRows::EvmTransactions(vec![
        transaction(2, 1),
        transaction(1, 0),
        transaction(2, 1),
    ]);
    transactions.sort();

    assert_eq!(
        transactions,
        QueryRows::EvmTransactions(vec![transaction(1, 0), transaction(2, 1)])
    );

    let mut receipts = QueryRows::EvmReceipts(vec![receipt(2, 1), receipt(1, 0), receipt(2, 1)]);
    receipts.sort();

    assert_eq!(
        receipts,
        QueryRows::EvmReceipts(vec![receipt(1, 0), receipt(2, 1)])
    );
}

#[test]
fn test_evm_log_filter_normalization_is_canonical() {
    let left = LogFilter {
        addresses: vec![
            "0x2222222222222222222222222222222222222222".to_owned(),
            "0x1111111111111111111111111111111111111111".to_owned(),
            "0x1111111111111111111111111111111111111111".to_owned(),
        ],
        topics: vec![
            Some(vec![
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ]),
            None,
            Some(vec![]),
        ],
    };
    let right = LogFilter {
        addresses: vec![
            "0X1111111111111111111111111111111111111111".to_owned(),
            "0X2222222222222222222222222222222222222222".to_owned(),
        ],
        topics: vec![
            Some(vec![
                "0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned(),
                "0XBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".to_owned(),
            ]),
            None,
            Some(vec![]),
        ],
    };

    let normalized_left = EvmLogFilter::try_from(left).expect("valid filter");
    let normalized_right = EvmLogFilter::try_from(right).expect("equivalent filter");

    assert_eq!(normalized_left, normalized_right);
    assert_eq!(
        normalized_left.topics()[1],
        TopicFilter::Wildcard,
        "wildcard slot is preserved"
    );
    assert_eq!(
        normalized_left.topics()[2],
        TopicFilter::AnyOf(Vec::new()),
        "empty alternatives are distinct from wildcard"
    );
    assert!(
        EvmLogFilter::try_from(LogFilter {
            addresses: vec!["0xabc".to_owned()],
            topics: vec![],
        })
        .is_err()
    );
    assert!(
        EvmLogFilter::try_from(LogFilter {
            addresses: vec![],
            topics: vec![Some(vec!["0xabc".to_owned()])],
        })
        .is_err()
    );
}

#[test]
fn test_log_record_deserialization_canonicalizes_hex_values() {
    let json = r#"{
        "block_number":10,
        "block_hash":"0xblock",
        "transaction_hash":"0xtx",
        "transaction_index":0,
        "log_index":1,
        "address":"0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "topics":[
            "0XBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
            "0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        ],
        "data":"0xCAFE",
        "removed":false
    }"#;

    let record: LogRecord = serde_json::from_str(json).expect("valid log record");

    assert_eq!(record.address, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    assert_eq!(
        record.topics,
        vec![
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ]
    );
}

#[test]
fn test_log_record_deserialization_rejects_invalid_hex_values() {
    let invalid_address = r#"{
        "block_number":10,
        "block_hash":"0xblock",
        "transaction_hash":"0xtx",
        "transaction_index":0,
        "log_index":1,
        "address":"0xabc",
        "topics":[],
        "data":"0x",
        "removed":false
    }"#;
    assert!(serde_json::from_str::<LogRecord>(invalid_address).is_err());

    let invalid_topic = r#"{
        "block_number":10,
        "block_hash":"0xblock",
        "transaction_hash":"0xtx",
        "transaction_index":0,
        "log_index":1,
        "address":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "topics":["0xabc"],
        "data":"0x",
        "removed":false
    }"#;
    assert!(serde_json::from_str::<LogRecord>(invalid_topic).is_err());

    let invalid_data = r#"{
        "block_number":10,
        "block_hash":"0xblock",
        "transaction_hash":"0xtx",
        "transaction_index":0,
        "log_index":1,
        "address":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "topics":[],
        "data":"0x0",
        "removed":false
    }"#;
    assert!(serde_json::from_str::<LogRecord>(invalid_data).is_err());
}

#[test]
fn test_error_retryability_and_constructors() {
    assert!(!DatalensError::invalid_input("bad input").is_retryable());
    assert!(!DatalensError::unsupported("unsupported").is_retryable());
    assert!(!DatalensError::provider_limit("too wide").is_retryable());
    assert!(DatalensError::provider_timeout("timeout").is_retryable());
    assert!(DatalensError::rate_limited("rate limited").is_retryable());
    assert!(DatalensError::storage_write("write failed").is_retryable());
    assert!(!DatalensError::internal("broken invariant").is_retryable());

    assert_eq!(
        DatalensError::manifest_update("manifest").kind,
        DatalensErrorKind::ManifestUpdateFailure
    );
}

#[test]
fn test_deserialization_rejects_invalid_domain_values() {
    assert!(
        serde_json::from_str::<ChainIdentity>(
            r#"{"family":{"Other":" "},"configured_name":"ethereum"}"#
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<ChainIdentity>(
            r#"{"family":"Evm","configured_name":"eth/mainnet"}"#
        )
        .is_err()
    );
    assert!(
        serde_json::from_str::<NetworkId>(r#"{"kind":"textual","value":"eth/mainnet"}"#).is_err()
    );
    assert!(serde_json::from_str::<BlockRange>(r#"{"from_block":10,"to_block":9}"#).is_err());
    assert!(
        serde_json::from_str::<TopicFilter>(r#"{"kind":"any_of","values":["0xabc"]}"#).is_err()
    );
    assert!(
        serde_json::from_str::<EvmLogFilter>(r#"{"addresses":["0xabc"],"topics":[]}"#).is_err()
    );

    let toml_text = r#"
        family = "Evm"
        configured_name = " "
    "#;
    assert!(toml::from_str::<ChainIdentity>(toml_text).is_err());
}

#[test]
fn test_query_rows_try_append_checks_dataset_mismatch() {
    let mut blocks = QueryRows::EvmBlocks(vec![datalens_core::BlockHeader {
        number: 1,
        hash: "0x1".to_owned(),
        parent_hash: "0x0".to_owned(),
        timestamp: 10,
    }]);

    blocks
        .try_append(QueryRows::EvmBlocks(vec![datalens_core::BlockHeader {
            number: 2,
            hash: "0x2".to_owned(),
            parent_hash: "0x1".to_owned(),
            timestamp: 20,
        }]))
        .unwrap();
    assert_eq!(blocks.row_count(), 2);

    let error = blocks
        .try_append(QueryRows::EvmLogs(Vec::new()))
        .expect_err("dataset mismatch");
    assert_eq!(error.kind, DatalensErrorKind::Internal);
}

#[test]
fn test_dataset_rows_envelope_keeps_dataset_key_with_typed_rows() {
    let rows = datalens_core::DatasetRows::new(
        datalens_core::DatasetKey::evm_blocks(),
        QueryRows::EvmBlocks(vec![datalens_core::BlockHeader {
            number: 1,
            hash: "0x1".to_owned(),
            parent_hash: "0x0".to_owned(),
            timestamp: 10,
        }]),
    )
    .expect("matching dataset rows");

    assert_eq!(rows.dataset_key(), &datalens_core::DatasetKey::evm_blocks());
    assert_eq!(rows.rows().row_count(), 1);

    let error = datalens_core::DatasetRows::new(
        datalens_core::DatasetKey::evm_logs(),
        QueryRows::EvmBlocks(Vec::new()),
    )
    .expect_err("dataset key mismatch");
    assert_eq!(error.kind, DatalensErrorKind::Internal);
}

#[test]
fn test_dataset_rows_envelope_checks_adapter_json_dataset_key() {
    let dataset_key = datalens_core::DatasetKey::tron_events();
    let rows = datalens_core::DatasetRows::new(
        dataset_key.clone(),
        QueryRows::AdapterJson {
            dataset_key: dataset_key.clone(),
            rows: vec![serde_json::json!({"event": "Transfer"})],
        },
    )
    .expect("matching adapter json rows");

    assert_eq!(rows.dataset_key(), &dataset_key);
    assert_eq!(rows.rows().row_count(), 1);

    let error = datalens_core::DatasetRows::new(
        datalens_core::DatasetKey::solana_transactions(),
        QueryRows::AdapterJson {
            dataset_key,
            rows: Vec::new(),
        },
    )
    .expect_err("adapter json dataset key mismatch");
    assert_eq!(error.kind, DatalensErrorKind::Internal);
}

#[test]
fn test_compact_coverage_key_is_deterministic_and_storage_safe() {
    let first = EvmLogFilter::try_from(LogFilter {
        addresses: vec!["0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned()],
        topics: vec![None],
    })
    .unwrap();
    let second = EvmLogFilter::try_from(LogFilter {
        addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        topics: vec![None],
    })
    .unwrap();
    let third = EvmLogFilter::try_from(LogFilter {
        addresses: vec!["0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned()],
        topics: vec![None],
    })
    .unwrap();

    assert_eq!(first.canonical_key(), second.canonical_key());
    assert_eq!(first.compact_key(), second.compact_key());
    assert_ne!(first.compact_key(), third.compact_key());
    assert!(first.compact_key().starts_with("addr-topic-"));
    assert!(!first.compact_key().contains('/'));
}

#[test]
fn test_compact_coverage_key_uses_sha256_prefix() {
    let filter = EvmLogFilter::try_from(LogFilter {
        addresses: vec!["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned()],
        topics: vec![None],
    })
    .unwrap();
    let key = filter.compact_key();
    let digest = key.strip_prefix("addr-topic-").expect("compact key prefix");

    assert_eq!(digest.len(), 32, "128-bit SHA-256 prefix");
    assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert!(!key.contains("0xaaaaaaaa"));
}

#[test]
fn test_log_record_checked_constructor_canonicalizes_hex_values() {
    let record = LogRecord::try_new(
        10,
        "0xblock".to_owned(),
        "0xtx".to_owned(),
        0,
        1,
        "0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        vec![
            "0XBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB".to_owned(),
            "0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned(),
        ],
        "0x".to_owned(),
        false,
    )
    .unwrap();

    assert_eq!(record.address, "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    assert_eq!(
        record.topics,
        vec![
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ]
    );

    let json = r#"{
        "block_number":10,
        "block_hash":"0xblock",
        "transaction_hash":"0xtx",
        "transaction_index":0,
        "log_index":1,
        "address":"0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "topics":[
            "0XBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
            "0XAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        ],
        "data":"0x",
        "removed":false
    }"#;
    let decoded: LogRecord = serde_json::from_str(json).unwrap();
    assert_eq!(decoded.address, record.address);
    assert_eq!(decoded.topics, record.topics);

    let json = r#"{
        "block_number":10,
        "block_hash":"0xblock",
        "transaction_hash":"0xtx",
        "transaction_index":0,
        "log_index":1,
        "address":"0xabc",
        "topics":[],
        "data":"0x",
        "removed":false
    }"#;
    assert!(serde_json::from_str::<LogRecord>(json).is_err());

    let json = r#"{
        "block_number":10,
        "block_hash":"0xblock",
        "transaction_hash":"0xtx",
        "transaction_index":0,
        "log_index":1,
        "address":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "topics":[],
        "data":"0x0",
        "removed":false
    }"#;
    assert!(serde_json::from_str::<LogRecord>(json).is_err());

    assert!(
        LogRecord::try_new(
            10,
            "0xblock".to_owned(),
            "0xtx".to_owned(),
            0,
            1,
            "0xabc",
            Vec::new(),
            "0x".to_owned(),
            false,
        )
        .is_err()
    );
}

#[test]
fn test_error_retryability_is_explicit_for_every_variant() {
    let cases = [
        (DatalensErrorKind::InvalidInput, false),
        (DatalensErrorKind::InvalidRequest, false),
        (DatalensErrorKind::UnsupportedDataset, false),
        (DatalensErrorKind::UnsupportedHotQuery, false),
        (DatalensErrorKind::ProviderFailure, true),
        (DatalensErrorKind::ProviderLimit, false),
        (DatalensErrorKind::ProviderTimeout, true),
        (DatalensErrorKind::RateLimited, true),
        (DatalensErrorKind::StorageReadFailure, true),
        (DatalensErrorKind::StorageWriteFailure, true),
        (DatalensErrorKind::ManifestUpdateFailure, true),
        (DatalensErrorKind::Internal, false),
    ];

    for (kind, retryable) in cases {
        assert_eq!(kind.is_retryable(), retryable, "{kind:?}");
    }
}

fn block(number: u64, hash: &str) -> BlockHeader {
    BlockHeader {
        number,
        hash: hash.to_owned(),
        parent_hash: format!("{hash}-parent"),
        timestamp: number,
    }
}

fn log(block_number: u64, log_index: u64) -> LogRecord {
    LogRecord {
        block_number,
        block_hash: format!("0xblock-{block_number}"),
        transaction_hash: format!("0xtx-{block_number}-{log_index}"),
        transaction_index: 0,
        log_index,
        address: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        topics: Vec::new(),
        data: "0x".to_owned(),
        removed: false,
    }
}

fn transaction(block_number: u64, transaction_index: u64) -> EvmTransaction {
    EvmTransaction {
        hash: format!("0xtx-{block_number}-{transaction_index}"),
        block_number,
        block_hash: format!("0xblock-{block_number}"),
        transaction_index,
        from: "0x1111111111111111111111111111111111111111".to_owned(),
        to: Some("0x2222222222222222222222222222222222222222".to_owned()),
        value: "0x1".to_owned(),
        input: "0x".to_owned(),
        nonce: 7,
        gas: 21_000,
        gas_price: Some("0x3b9aca00".to_owned()),
        max_fee_per_gas: None,
        max_priority_fee_per_gas: None,
        transaction_type: Some("0x2".to_owned()),
    }
}

fn receipt(block_number: u64, transaction_index: u64) -> EvmReceipt {
    EvmReceipt {
        transaction_hash: format!("0xtx-{block_number}-{transaction_index}"),
        block_number,
        block_hash: format!("0xblock-{block_number}"),
        transaction_index,
        status: Some(1),
        gas_used: 21_000,
        cumulative_gas_used: 21_000,
        effective_gas_price: Some("0x3b9aca00".to_owned()),
        contract_address: None,
        logs_bloom: Some(format!("0x{}", "0".repeat(512))),
    }
}

#[test]
fn test_dataset_id_and_time_range_have_checked_semantics() {
    assert!(DatasetId::try_new("logs").is_ok());
    assert!(DatasetId::try_new(" ").is_err());
    assert!(DatasetId::try_new("bad/path").is_err());
    assert_eq!(
        DatasetId::try_from(" logs ".to_owned()).unwrap().as_str(),
        "logs"
    );
    assert!(DatasetId::try_from("bad/path".to_owned()).is_err());
    assert!(TimeRange::try_blocks(1, 2).is_ok());
    assert!(TimeRange::try_blocks(2, 1).is_err());
}
