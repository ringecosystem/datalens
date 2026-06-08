use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use datalens_chain::{DatasetSelector, FinalityLevel};
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensError, DatasetKey, LedgerRange, LedgerRangeKind, NetworkId,
};
use datalens_storage::{
    DurableIntentSubmissionOutcome, DurableIntentSubmissionRequest, DurableIntentSubmissionService,
    DurablePromotionIntentCreateOutcome, DurablePromotionIntentRepository,
    DurablePromotionIntentSource, DurablePromotionIntentStatus, DurablePromotionIntentStore,
    LocalObjectStore, ObjectMetadata, ObjectStore,
};

#[test]
fn test_repository_persists_intent_after_recreating_with_same_storage_root() {
    let root = temp_storage_root("persist");
    let store = DurablePromotionIntentStore::new(LocalObjectStore::new(root.clone()));
    let created = create_intent(
        &store,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        100,
    );
    let intent_id = created.intent_id.clone();

    let recreated = DurablePromotionIntentStore::new(LocalObjectStore::new(root));
    let loaded = recreated
        .get(&intent_id)
        .expect("read intent")
        .expect("intent exists");

    assert_eq!(loaded, created);
    assert_eq!(loaded.status, DurablePromotionIntentStatus::Pending);
    assert_eq!(loaded.source, DurablePromotionIntentSource::Query);
    assert_eq!(loaded.application, "analytics-api");
}

#[test]
fn test_duplicate_submission_is_idempotent_by_durable_coverage() {
    let store =
        DurablePromotionIntentStore::new(LocalObjectStore::new(temp_storage_root("dedupe")));
    let service = DurableIntentSubmissionService::new(store);

    let first = submit_request(
        &service,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        Some("request-1"),
        None,
        100,
    );
    let second = submit_request(
        &service,
        DurablePromotionIntentSource::Warmup,
        "warmup-api",
        None,
        Some("task-1"),
        101,
    );

    let DurableIntentSubmissionOutcome::Submitted(first) = first else {
        panic!("first submission should create an intent");
    };
    let DurableIntentSubmissionOutcome::AlreadyPending(second) = second else {
        panic!("duplicate coverage should reuse pending intent");
    };
    assert_eq!(first.intent_id, second.intent_id);
    assert_eq!(second.source, DurablePromotionIntentSource::Query);
    assert_eq!(second.application, "analytics-api");
}

#[test]
fn test_equivalent_contiguous_ranges_dedupe_to_same_coverage_intent() {
    let store = DurablePromotionIntentStore::new(LocalObjectStore::new(temp_storage_root(
        "normalized-dedupe",
    )));
    let split = store
        .create_or_get(datalens_storage::CreateDurablePromotionIntent {
            source: DurablePromotionIntentSource::Query,
            application: "analytics-api".to_owned(),
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            selector_fingerprint: "all".to_owned(),
            selector_canonical_key: "all".to_owned(),
            finality: "safe".to_owned(),
            ranges: vec![
                LedgerRange::blocks(16, 20).expect("valid range"),
                LedgerRange::blocks(10, 15).expect("valid range"),
            ],
            request_id: Some("request-1".to_owned()),
            task_id: None,
            now_unix_seconds: 100,
        })
        .expect("create split intent");
    let DurablePromotionIntentCreateOutcome::Created(split) = split else {
        panic!("split range should create intent");
    };
    assert_eq!(
        split.ranges,
        vec![LedgerRange::blocks(10, 20).expect("valid range")]
    );

    let single = store
        .create_or_get(datalens_storage::CreateDurablePromotionIntent {
            source: DurablePromotionIntentSource::Warmup,
            application: "warmup-api".to_owned(),
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            selector_fingerprint: "all".to_owned(),
            selector_canonical_key: "all".to_owned(),
            finality: "safe".to_owned(),
            ranges: vec![LedgerRange::blocks(10, 20).expect("valid range")],
            request_id: None,
            task_id: Some("task-1".to_owned()),
            now_unix_seconds: 101,
        })
        .expect("create equivalent single intent");
    let DurablePromotionIntentCreateOutcome::Existing(single) = single else {
        panic!("equivalent coverage should reuse intent");
    };
    assert_eq!(split.intent_id, single.intent_id);
}

#[test]
fn test_equivalent_overlapping_ranges_dedupe_to_same_coverage_intent() {
    let store = DurablePromotionIntentStore::new(LocalObjectStore::new(temp_storage_root(
        "overlap-dedupe",
    )));
    let overlapping = store
        .create_or_get(datalens_storage::CreateDurablePromotionIntent {
            source: DurablePromotionIntentSource::Query,
            application: "analytics-api".to_owned(),
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            selector_fingerprint: "all".to_owned(),
            selector_canonical_key: "all".to_owned(),
            finality: "safe".to_owned(),
            ranges: vec![
                LedgerRange::blocks(10, 18).expect("valid range"),
                LedgerRange::blocks(15, 20).expect("valid range"),
            ],
            request_id: Some("request-1".to_owned()),
            task_id: None,
            now_unix_seconds: 100,
        })
        .expect("create overlapping intent");
    let DurablePromotionIntentCreateOutcome::Created(overlapping) = overlapping else {
        panic!("overlapping range should create intent");
    };
    assert_eq!(
        overlapping.ranges,
        vec![LedgerRange::blocks(10, 20).expect("valid range")]
    );
}

#[test]
fn test_status_transitions_pending_running_completed() {
    let store =
        DurablePromotionIntentStore::new(LocalObjectStore::new(temp_storage_root("transitions")));
    let created = create_intent(
        &store,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        100,
    );

    let running = store
        .mark_running(&created.intent_id, 110)
        .expect("mark running")
        .expect("intent exists");
    assert_eq!(running.status, DurablePromotionIntentStatus::Running);
    assert_eq!(running.updated_at_unix_seconds, 110);

    let completed = store
        .mark_completed(&created.intent_id, 120)
        .expect("mark completed")
        .expect("intent exists");
    assert_eq!(completed.status, DurablePromotionIntentStatus::Completed);
    assert_eq!(completed.updated_at_unix_seconds, 120);
    assert!(completed.next_retry_at_unix_seconds.is_none());
    assert!(completed.last_error.is_none());
}

#[test]
fn test_pending_index_tracks_create_running_completed_lifecycle() {
    let root = temp_storage_root("pending-index-lifecycle");
    let object_store = LocalObjectStore::new(root);
    let store = DurablePromotionIntentStore::new(object_store.clone());
    let created = create_intent(
        &store,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        100,
    );

    assert_eq!(
        pending_index_keys(&object_store, &test_chain(), "query").len(),
        1
    );

    store
        .mark_running(&created.intent_id, 110)
        .expect("mark running");
    assert!(pending_index_keys(&object_store, &test_chain(), "query").is_empty());

    store
        .mark_completed(&created.intent_id, 120)
        .expect("mark completed");
    assert!(pending_index_keys(&object_store, &test_chain(), "query").is_empty());
}

#[test]
fn test_repository_rejects_non_durable_finality() {
    let store = DurablePromotionIntentStore::new(LocalObjectStore::new(temp_storage_root(
        "repository-finality",
    )));

    for finality in ["latest", "checkpoint"] {
        let error = store
            .create_or_get(datalens_storage::CreateDurablePromotionIntent {
                source: DurablePromotionIntentSource::Query,
                application: "analytics-api".to_owned(),
                chain: test_chain(),
                dataset_key: DatasetKey::evm_blocks(),
                selector: DatasetSelector::all(),
                selector_fingerprint: "all".to_owned(),
                selector_canonical_key: "all".to_owned(),
                finality: finality.to_owned(),
                ranges: vec![LedgerRange::blocks(10, 11).expect("valid range")],
                request_id: Some("request-1".to_owned()),
                task_id: None,
                now_unix_seconds: 100,
            })
            .expect_err("non-durable finality should fail");
        assert!(error.message.contains("safe or finalized"));
    }

    assert!(
        store
            .list_pending(100, 10)
            .expect("list pending")
            .is_empty()
    );
}

#[test]
fn test_list_pending_for_chain_filters_before_applying_limit() {
    let store = DurablePromotionIntentStore::new(LocalObjectStore::new(temp_storage_root(
        "chain-pending-limit",
    )));
    let ethereum = test_chain();
    let lisk = lisk_chain();
    for index in 0..16 {
        store
            .create_or_get(datalens_storage::CreateDurablePromotionIntent {
                source: DurablePromotionIntentSource::Query,
                application: "analytics-api".to_owned(),
                chain: lisk.clone(),
                dataset_key: DatasetKey::evm_blocks(),
                selector: DatasetSelector::all(),
                selector_fingerprint: "all".to_owned(),
                selector_canonical_key: "all".to_owned(),
                finality: "safe".to_owned(),
                ranges: vec![LedgerRange::blocks(100 + index, 101 + index).expect("valid range")],
                request_id: None,
                task_id: None,
                now_unix_seconds: 100 + index,
            })
            .expect("create lisk intent");
    }
    let ethereum_intent = store
        .create_or_get(datalens_storage::CreateDurablePromotionIntent {
            source: DurablePromotionIntentSource::Query,
            application: "analytics-api".to_owned(),
            chain: ethereum.clone(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            selector_fingerprint: "all".to_owned(),
            selector_canonical_key: "all".to_owned(),
            finality: "safe".to_owned(),
            ranges: vec![LedgerRange::blocks(200, 201).expect("valid range")],
            request_id: None,
            task_id: None,
            now_unix_seconds: 116,
        })
        .expect("create ethereum intent");
    let DurablePromotionIntentCreateOutcome::Created(ethereum_intent) = ethereum_intent else {
        panic!("ethereum intent should be created");
    };

    let global = store.list_pending(200, 1).expect("list global pending");
    assert_eq!(global.len(), 1);
    assert_eq!(global[0].chain, lisk);

    let scoped = store
        .list_pending_for_chain(&ethereum, 200, 1)
        .expect("list chain pending");
    assert_eq!(
        scoped.first().map(|intent| intent.intent_id.as_str()),
        Some(ethereum_intent.intent_id.as_str())
    );
}

#[test]
fn test_retryable_failure_stores_error_increments_attempt_and_sets_retry_time() {
    let store =
        DurablePromotionIntentStore::new(LocalObjectStore::new(temp_storage_root("retryable")));
    let created = create_intent(
        &store,
        DurablePromotionIntentSource::Warmup,
        "warmup-api",
        100,
    );
    store
        .mark_running(&created.intent_id, 110)
        .expect("mark running");

    let failed = store
        .mark_retryable_failure(&created.intent_id, "temporary storage error", 130, 160)
        .expect("mark retryable failure")
        .expect("intent exists");

    assert_eq!(failed.status, DurablePromotionIntentStatus::FailedRetryable);
    assert_eq!(failed.attempt_count, 1);
    assert_eq!(failed.next_retry_at_unix_seconds, Some(160));
    assert_eq!(
        failed.last_error.as_deref(),
        Some("temporary storage error")
    );
    assert!(
        store
            .list_pending(159, 10)
            .expect("list pending")
            .is_empty()
    );
    assert_eq!(
        store
            .list_pending(160, 10)
            .expect("list pending")
            .first()
            .map(|intent| intent.intent_id.as_str()),
        Some(failed.intent_id.as_str())
    );
}

#[test]
fn test_retryable_pending_index_is_only_eligible_at_retry_time() {
    let root = temp_storage_root("retryable-index");
    let object_store = LocalObjectStore::new(root);
    let store = DurablePromotionIntentStore::new(object_store.clone());
    let created = create_intent(
        &store,
        DurablePromotionIntentSource::Warmup,
        "warmup-api",
        100,
    );
    store
        .mark_running(&created.intent_id, 110)
        .expect("mark running");

    store
        .mark_retryable_failure(&created.intent_id, "temporary storage error", 130, 160)
        .expect("mark retryable");

    let index_keys = pending_index_keys(&object_store, &test_chain(), "warmup");
    assert_eq!(index_keys.len(), 1);
    assert!(
        index_keys[0].contains("/created=0000000000000000160/"),
        "{:?}",
        index_keys
    );
    assert!(
        store
            .list_pending_for_chain(&test_chain(), 159, 10)
            .expect("list pending")
            .is_empty()
    );
    assert_eq!(
        store
            .list_pending_for_chain(&test_chain(), 160, 10)
            .expect("list pending")
            .first()
            .map(|intent| intent.intent_id.as_str()),
        Some(created.intent_id.as_str())
    );
}

#[test]
fn test_mark_running_only_claims_eligible_pending_or_due_retryable_intents() {
    let store = DurablePromotionIntentStore::new(LocalObjectStore::new(temp_storage_root(
        "claim-eligible-intents",
    )));
    let created = create_intent(
        &store,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        100,
    );

    let running = store
        .mark_running(&created.intent_id, 110)
        .expect("mark running")
        .expect("intent exists");
    assert_eq!(running.status, DurablePromotionIntentStatus::Running);
    assert!(
        store
            .mark_running(&created.intent_id, 111)
            .expect("second claim is rejected")
            .is_none()
    );

    store
        .mark_retryable_failure(&created.intent_id, "retry later", 120, 200)
        .expect("mark retryable");
    assert!(
        store
            .mark_running(&created.intent_id, 199)
            .expect("early retry claim is rejected")
            .is_none()
    );
    assert!(
        store
            .mark_running(&created.intent_id, 200)
            .expect("due retry claim is accepted")
            .is_some()
    );
    store
        .mark_completed(&created.intent_id, 210)
        .expect("mark completed");
    assert!(
        store
            .mark_running(&created.intent_id, 211)
            .expect("completed claim is rejected")
            .is_none()
    );
}

#[test]
fn test_stale_running_reset_changes_old_running_back_to_pending() {
    let store = DurablePromotionIntentStore::new(LocalObjectStore::new(temp_storage_root("stale")));
    let stale = create_intent(
        &store,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        100,
    );
    let fresh = create_intent_with_range(
        &store,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        LedgerRange::blocks(30, 31).expect("valid range"),
        100,
    );
    store
        .mark_running(&stale.intent_id, 120)
        .expect("mark stale running");
    store
        .mark_running(&fresh.intent_id, 180)
        .expect("mark fresh running");

    let reset = store
        .reset_stale_running(150, 200)
        .expect("reset stale running");

    assert_eq!(reset.len(), 1);
    assert_eq!(reset[0].intent_id, stale.intent_id);
    assert_eq!(reset[0].status, DurablePromotionIntentStatus::Pending);
    assert_eq!(reset[0].updated_at_unix_seconds, 200);
    assert_eq!(
        store
            .get(&fresh.intent_id)
            .expect("read fresh")
            .expect("fresh exists")
            .status,
        DurablePromotionIntentStatus::Running
    );
}

#[test]
fn test_reset_stale_running_restores_pending_index() {
    let root = temp_storage_root("stale-index");
    let object_store = LocalObjectStore::new(root);
    let store = DurablePromotionIntentStore::new(object_store.clone());
    let stale = create_intent(
        &store,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        100,
    );
    store
        .mark_running(&stale.intent_id, 120)
        .expect("mark stale running");
    assert!(pending_index_keys(&object_store, &test_chain(), "query").is_empty());

    store
        .reset_stale_running(150, 200)
        .expect("reset stale running");

    let index_keys = pending_index_keys(&object_store, &test_chain(), "query");
    assert_eq!(index_keys.len(), 1);
    assert!(
        index_keys[0].contains("/created=0000000000000000100/"),
        "{:?}",
        index_keys
    );
    assert_eq!(
        store
            .list_pending_for_chain(&test_chain(), 200, 10)
            .expect("list pending")
            .first()
            .map(|intent| intent.intent_id.as_str()),
        Some(stale.intent_id.as_str())
    );
}

#[test]
fn test_list_pending_for_chain_does_not_read_unrelated_chain_intent_json() {
    let root = temp_storage_root("chain-scoped-index-reads");
    let counting_store = CountingObjectStore::new(root);
    let store = DurablePromotionIntentStore::new(counting_store.clone());
    for index in 0..8 {
        store
            .create_or_get(datalens_storage::CreateDurablePromotionIntent {
                source: DurablePromotionIntentSource::Query,
                application: "analytics-api".to_owned(),
                chain: lisk_chain(),
                dataset_key: DatasetKey::evm_blocks(),
                selector: DatasetSelector::all(),
                selector_fingerprint: "all".to_owned(),
                selector_canonical_key: "all".to_owned(),
                finality: "safe".to_owned(),
                ranges: vec![LedgerRange::blocks(100 + index, 101 + index).expect("valid range")],
                request_id: None,
                task_id: None,
                now_unix_seconds: 100 + index,
            })
            .expect("create lisk intent");
    }
    let ethereum = store
        .create_or_get(datalens_storage::CreateDurablePromotionIntent {
            source: DurablePromotionIntentSource::Warmup,
            application: "warmup-api".to_owned(),
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            selector_fingerprint: "all".to_owned(),
            selector_canonical_key: "all".to_owned(),
            finality: "safe".to_owned(),
            ranges: vec![LedgerRange::blocks(200, 201).expect("valid range")],
            request_id: None,
            task_id: None,
            now_unix_seconds: 200,
        })
        .expect("create ethereum intent");
    let DurablePromotionIntentCreateOutcome::Created(ethereum) = ethereum else {
        panic!("ethereum intent should be created");
    };
    counting_store.clear_reads();

    let scoped = store
        .list_pending_for_chain(&test_chain(), 300, 1)
        .expect("list scoped pending");

    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0].intent_id, ethereum.intent_id);
    assert!(
        counting_store.read_keys().into_iter().all(|key| key
            == format!(
                "durable-promotion-intents/v1/intents/{}.json",
                ethereum.intent_id
            )),
        "unexpected canonical reads: {:?}",
        counting_store.read_keys()
    );
}

#[test]
fn test_list_pending_for_chain_with_missing_index_does_not_scan_canonical_intents() {
    let root = temp_storage_root("missing-index-no-hot-scan");
    let object_store = CountingObjectStore::new(root);
    let store = DurablePromotionIntentStore::new(object_store.clone());
    let created = create_intent(
        &store,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        100,
    );
    for key in pending_index_keys(object_store.inner(), &test_chain(), "query") {
        object_store.delete(&key).expect("remove pending index");
    }
    object_store.clear_reads();
    object_store.clear_lists();

    let pending = store
        .list_pending_for_chain(&test_chain(), 200, 10)
        .expect("list pending");

    assert!(pending.is_empty(), "{created:?} should remain index-only");
    assert!(object_store.read_keys().is_empty());
    assert!(
        object_store
            .list_prefixes()
            .into_iter()
            .all(|prefix| !prefix.starts_with("durable-promotion-intents/v1/intents")),
        "unexpected canonical lists: {:?}",
        object_store.list_prefixes()
    );
}

#[test]
fn test_list_pending_for_chain_with_insufficient_index_does_not_scan_missing_canonical_intents() {
    let root = temp_storage_root("insufficient-index-no-hot-scan");
    let object_store = CountingObjectStore::new(root);
    let store = DurablePromotionIntentStore::new(object_store.clone());
    let indexed = create_intent(
        &store,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        100,
    );
    let missing_index = create_intent_with_range(
        &store,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        LedgerRange::blocks(20, 21).expect("valid range"),
        101,
    );
    for key in pending_index_keys(object_store.inner(), &test_chain(), "query") {
        if key.contains(&missing_index.intent_id) {
            object_store.delete(&key).expect("remove pending index");
        }
    }
    object_store.clear_reads();
    object_store.clear_lists();

    let pending = store
        .list_pending_for_chain(&test_chain(), 200, 10)
        .expect("list pending");

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].intent_id, indexed.intent_id);
    assert_eq!(
        object_store.read_keys(),
        vec![format!(
            "durable-promotion-intents/v1/intents/{}.json",
            indexed.intent_id
        )]
    );
    assert!(
        object_store
            .list_prefixes()
            .into_iter()
            .all(|prefix| !prefix.starts_with("durable-promotion-intents/v1/intents")),
        "unexpected canonical lists: {:?}",
        object_store.list_prefixes()
    );
}

#[test]
fn test_rebuild_pending_indexes_heals_legacy_pending_beyond_first_128_intents() {
    let root = temp_storage_root("legacy-pending-index-rebuild");
    let object_store = LocalObjectStore::new(root);
    let store = DurablePromotionIntentStore::new(object_store.clone());
    for index in 0..130 {
        store
            .create_or_get(datalens_storage::CreateDurablePromotionIntent {
                source: DurablePromotionIntentSource::Query,
                application: "analytics-api".to_owned(),
                chain: lisk_chain(),
                dataset_key: DatasetKey::evm_blocks(),
                selector: DatasetSelector::all(),
                selector_fingerprint: "all".to_owned(),
                selector_canonical_key: "all".to_owned(),
                finality: "safe".to_owned(),
                ranges: vec![LedgerRange::blocks(1000 + index, 1001 + index).expect("valid range")],
                request_id: None,
                task_id: None,
                now_unix_seconds: 100 + index,
            })
            .expect("create lisk intent");
    }
    let created = create_intent_with_range(
        &store,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        LedgerRange::blocks(5000, 5001).expect("valid range"),
        500,
    );
    for key in object_store
        .list("durable-promotion-intents/v1/index/status=pending")
        .expect("list pending indexes")
        .into_iter()
        .map(|object| object.key)
    {
        object_store.delete(&key).expect("remove pending index");
    }
    assert!(
        store
            .list_pending_for_chain(&test_chain(), 600, 10)
            .expect("list pending before rebuild")
            .is_empty()
    );

    let rebuilt = store
        .rebuild_pending_indexes(600)
        .expect("rebuild pending indexes");
    let pending = store
        .list_pending_for_chain(&test_chain(), 600, 10)
        .expect("list pending after rebuild");

    assert!(
        rebuilt > 128,
        "expected rebuild to scan all canonical intents"
    );
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].intent_id, created.intent_id);
}

#[test]
fn test_rebuild_pending_indexes_heals_due_retryable_when_index_write_failed() {
    let root = temp_storage_root("retryable-index-write-failure");
    let object_store = FailOnceIndexPutObjectStore::new(root);
    let store = DurablePromotionIntentStore::new(object_store.clone());
    let created = create_intent(
        &store,
        DurablePromotionIntentSource::Warmup,
        "warmup-api",
        100,
    );
    store
        .mark_running(&created.intent_id, 110)
        .expect("mark running");
    object_store.fail_next_index_put();

    store
        .mark_retryable_failure(&created.intent_id, "temporary storage error", 130, 160)
        .expect_err("index write should fail after canonical retryable update");
    assert!(
        store
            .list_pending_for_chain(&test_chain(), 160, 10)
            .expect("list pending before rebuild")
            .is_empty()
    );

    store
        .rebuild_pending_indexes(160)
        .expect("rebuild pending indexes");

    let pending = store
        .list_pending_for_chain(&test_chain(), 160, 10)
        .expect("list pending");

    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].intent_id, created.intent_id);
    assert_eq!(
        pending_index_keys(object_store.inner(), &test_chain(), "warmup").len(),
        1
    );
}

#[test]
fn test_rebuild_pending_indexes_heals_future_retryable_before_it_is_due() {
    let root = temp_storage_root("future-retryable-index-rebuild");
    let object_store = LocalObjectStore::new(root);
    let store = DurablePromotionIntentStore::new(object_store.clone());
    let created = create_intent(
        &store,
        DurablePromotionIntentSource::Warmup,
        "warmup-api",
        100,
    );
    store
        .mark_running(&created.intent_id, 110)
        .expect("mark running");
    store
        .mark_retryable_failure(&created.intent_id, "retry later", 120, 500)
        .expect("mark retryable");
    for key in pending_index_keys(&object_store, &test_chain(), "warmup") {
        object_store.delete(&key).expect("remove retry index");
    }

    store
        .rebuild_pending_indexes(200)
        .expect("rebuild pending indexes");

    assert!(
        store
            .list_pending_for_chain(&test_chain(), 499, 10)
            .expect("list before retry")
            .is_empty()
    );
    assert_eq!(
        store
            .list_pending_for_chain(&test_chain(), 500, 10)
            .expect("list at retry")
            .first()
            .map(|intent| intent.intent_id.as_str()),
        Some(created.intent_id.as_str())
    );
}

#[test]
fn test_submission_service_handles_query_and_warmup_sources() {
    let store =
        DurablePromotionIntentStore::new(LocalObjectStore::new(temp_storage_root("sources")));
    let service = DurableIntentSubmissionService::new(store);

    let query = submit_request(
        &service,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        Some("request-1"),
        None,
        100,
    );
    let warmup = service.submit(DurableIntentSubmissionRequest {
        source: DurablePromotionIntentSource::Warmup,
        application: "warmup-api".to_owned(),
        chain: test_chain(),
        dataset_key: DatasetKey::evm_blocks(),
        selector: DatasetSelector::all(),
        finality: FinalityLevel::Finalized,
        ranges: vec![LedgerRange::blocks(40, 41).expect("valid range")],
        request_id: None,
        task_id: Some("task-1".to_owned()),
        now_unix_seconds: 101,
    });

    let DurableIntentSubmissionOutcome::Submitted(query) = query else {
        panic!("query source should submit");
    };
    let DurableIntentSubmissionOutcome::Submitted(warmup) = warmup else {
        panic!("warmup source should submit");
    };
    assert_eq!(query.source, DurablePromotionIntentSource::Query);
    assert_eq!(query.request_id.as_deref(), Some("request-1"));
    assert_eq!(warmup.source, DurablePromotionIntentSource::Warmup);
    assert_eq!(warmup.task_id.as_deref(), Some("task-1"));
}

#[test]
fn test_submission_service_rejects_non_durable_finality_without_writing_intent() {
    let store =
        DurablePromotionIntentStore::new(LocalObjectStore::new(temp_storage_root("non-durable")));
    let service = DurableIntentSubmissionService::new(store.clone());

    let outcome = service.submit(DurableIntentSubmissionRequest {
        source: DurablePromotionIntentSource::Query,
        application: "analytics-api".to_owned(),
        chain: test_chain(),
        dataset_key: DatasetKey::evm_blocks(),
        selector: DatasetSelector::all(),
        finality: FinalityLevel::Latest,
        ranges: vec![LedgerRange::blocks(10, 11).expect("valid range")],
        request_id: Some("request-1".to_owned()),
        task_id: None,
        now_unix_seconds: 100,
    });

    let DurableIntentSubmissionOutcome::Failed(error) = outcome else {
        panic!("latest finality should not create durable intent");
    };
    assert!(error.message.contains("safe or finalized"));
    assert!(
        store
            .list_pending(100, 10)
            .expect("list pending")
            .is_empty()
    );
}

fn create_intent(
    store: &impl DurablePromotionIntentRepository,
    source: DurablePromotionIntentSource,
    application: &str,
    now_unix_seconds: u64,
) -> datalens_storage::DurablePromotionIntent {
    create_intent_with_range(
        store,
        source,
        application,
        LedgerRange::blocks(10, 11).expect("valid range"),
        now_unix_seconds,
    )
}

fn create_intent_with_range(
    store: &impl DurablePromotionIntentRepository,
    source: DurablePromotionIntentSource,
    application: &str,
    range: LedgerRange,
    now_unix_seconds: u64,
) -> datalens_storage::DurablePromotionIntent {
    let outcome = store
        .create_or_get(datalens_storage::CreateDurablePromotionIntent {
            source,
            application: application.to_owned(),
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            selector_fingerprint: "all".to_owned(),
            selector_canonical_key: "all".to_owned(),
            finality: "safe".to_owned(),
            ranges: vec![range],
            request_id: None,
            task_id: None,
            now_unix_seconds,
        })
        .expect("create intent");
    match outcome {
        DurablePromotionIntentCreateOutcome::Created(intent) => intent,
        DurablePromotionIntentCreateOutcome::Existing(intent) => intent,
    }
}

fn submit_request(
    service: &DurableIntentSubmissionService<DurablePromotionIntentStore<LocalObjectStore>>,
    source: DurablePromotionIntentSource,
    application: &str,
    request_id: Option<&str>,
    task_id: Option<&str>,
    now_unix_seconds: u64,
) -> DurableIntentSubmissionOutcome {
    service.submit(DurableIntentSubmissionRequest {
        source,
        application: application.to_owned(),
        chain: test_chain(),
        dataset_key: DatasetKey::evm_blocks(),
        selector: DatasetSelector::all(),
        finality: FinalityLevel::Safe,
        ranges: vec![LedgerRange::blocks(10, 11).expect("valid range")],
        request_id: request_id.map(str::to_owned),
        task_id: task_id.map(str::to_owned),
        now_unix_seconds,
    })
}

fn temp_storage_root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-durable-intent-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create temp storage root");
    root
}

fn pending_index_keys(
    object_store: &LocalObjectStore,
    chain: &ChainIdentity,
    source: &str,
) -> Vec<String> {
    object_store
        .list(&format!(
            "durable-promotion-intents/v1/index/status=pending/chain={}/source={}",
            chain.key_prefix(),
            source
        ))
        .expect("list pending index")
        .into_iter()
        .map(|object| object.key)
        .collect()
}

fn test_chain() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
        .expect("valid chain identity")
}

fn lisk_chain() -> ChainIdentity {
    ChainIdentity::try_new(ChainFamily::Evm, "lisk", Some(NetworkId::numeric(1135)))
        .expect("valid chain identity")
}

#[derive(Clone, Debug)]
struct CountingObjectStore {
    inner: LocalObjectStore,
    reads: Arc<Mutex<BTreeMap<String, usize>>>,
    lists: Arc<Mutex<Vec<String>>>,
}

impl CountingObjectStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            reads: Arc::new(Mutex::new(BTreeMap::new())),
            lists: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn inner(&self) -> &LocalObjectStore {
        &self.inner
    }

    fn clear_reads(&self) {
        self.reads.lock().expect("read counts").clear();
    }

    fn clear_lists(&self) {
        self.lists.lock().expect("list prefixes").clear();
    }

    fn read_keys(&self) -> Vec<String> {
        self.reads
            .lock()
            .expect("read counts")
            .keys()
            .cloned()
            .collect()
    }

    fn list_prefixes(&self) -> Vec<String> {
        self.lists.lock().expect("list prefixes").clone()
    }
}

impl ObjectStore for CountingObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        *self
            .reads
            .lock()
            .expect("read counts")
            .entry(key.to_owned())
            .or_default() += 1;
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        self.inner.put(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.lists
            .lock()
            .expect("list prefixes")
            .push(prefix.to_owned());
        self.inner.list(prefix)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }
}

#[derive(Clone, Debug)]
struct FailOnceIndexPutObjectStore {
    inner: LocalObjectStore,
    fail_next_index_put: Arc<AtomicBool>,
}

impl FailOnceIndexPutObjectStore {
    fn new(root: PathBuf) -> Self {
        Self {
            inner: LocalObjectStore::new(root),
            fail_next_index_put: Arc::new(AtomicBool::new(false)),
        }
    }

    fn inner(&self) -> &LocalObjectStore {
        &self.inner
    }

    fn fail_next_index_put(&self) {
        self.fail_next_index_put.store(true, Ordering::SeqCst);
    }
}

impl ObjectStore for FailOnceIndexPutObjectStore {
    fn get(&self, key: &str) -> Result<Vec<u8>, DatalensError> {
        self.inner.get(key)
    }

    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), DatalensError> {
        if key.starts_with("durable-promotion-intents/v1/index/status=pending/")
            && self.fail_next_index_put.swap(false, Ordering::SeqCst)
        {
            return Err(DatalensError::new(
                datalens_core::DatalensErrorKind::StorageWriteFailure,
                "injected pending index put failure",
            ));
        }
        self.inner.put(key, bytes)
    }

    fn exists(&self, key: &str) -> Result<bool, DatalensError> {
        self.inner.exists(key)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, DatalensError> {
        self.inner.list(prefix)
    }

    fn delete(&self, key: &str) -> Result<(), DatalensError> {
        self.inner.delete(key)
    }
}

#[test]
fn test_coverage_dedupe_key_includes_range_kind_start_end_and_finality() {
    let store = DurablePromotionIntentStore::new(LocalObjectStore::new(temp_storage_root("keys")));
    let block = create_intent_with_range(
        &store,
        DurablePromotionIntentSource::Query,
        "analytics-api",
        LedgerRange::blocks(10, 11).expect("valid range"),
        100,
    );
    let slot_outcome = store
        .create_or_get(datalens_storage::CreateDurablePromotionIntent {
            source: DurablePromotionIntentSource::Warmup,
            application: "warmup-api".to_owned(),
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            selector_fingerprint: "all".to_owned(),
            selector_canonical_key: "all".to_owned(),
            finality: "safe".to_owned(),
            ranges: vec![LedgerRange::try_new(LedgerRangeKind::Slot, 10, 11).expect("valid range")],
            request_id: None,
            task_id: Some("task-1".to_owned()),
            now_unix_seconds: 100,
        })
        .expect("create slot intent");
    let DurablePromotionIntentCreateOutcome::Created(slot) = slot_outcome else {
        panic!("different range kind should create a distinct intent");
    };
    let finalized_outcome = store
        .create_or_get(datalens_storage::CreateDurablePromotionIntent {
            source: DurablePromotionIntentSource::Warmup,
            application: "warmup-api".to_owned(),
            chain: test_chain(),
            dataset_key: DatasetKey::evm_blocks(),
            selector: DatasetSelector::all(),
            selector_fingerprint: "all".to_owned(),
            selector_canonical_key: "all".to_owned(),
            finality: "finalized".to_owned(),
            ranges: vec![LedgerRange::blocks(10, 11).expect("valid range")],
            request_id: None,
            task_id: Some("task-2".to_owned()),
            now_unix_seconds: 100,
        })
        .expect("create finalized intent");
    let DurablePromotionIntentCreateOutcome::Created(finalized) = finalized_outcome else {
        panic!("different finality should create a distinct intent");
    };

    assert_ne!(block.intent_id, slot.intent_id);
    assert_ne!(block.intent_id, finalized.intent_id);
}
