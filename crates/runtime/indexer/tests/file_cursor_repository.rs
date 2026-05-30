use datalens_chain::DatasetSelector;
use datalens_core::{
    ChainFamily, ChainIdentity, DatalensErrorKind, DatasetKey, LedgerRange, NetworkId,
};
use datalens_indexer::{
    FileIndexCursorStore, IndexChunk, IndexCursor, IndexCursorRepository, IndexFailureState,
    IndexJobId, IndexRetryPolicy,
};

#[test]
fn test_file_cursor_store_persists_and_loads_cursor() {
    let root = temp_cursor_root("roundtrip");
    let store = FileIndexCursorStore::new(&root);
    let mut cursor = cursor("fixture-job");
    cursor.failure_state = Some(IndexFailureState {
        chunk: chunk(3, 4, 1),
        error_kind: DatalensErrorKind::ProviderFailure,
        message: "provider failed".to_owned(),
    });

    store.save(&cursor).expect("save cursor");
    let loaded = store
        .load(&cursor.job_id)
        .expect("load cursor")
        .expect("cursor exists");

    assert_eq!(loaded, cursor);
    assert_eq!(root.read_dir().expect("cursor dir").count(), 1);
}

#[test]
fn test_file_cursor_store_rejects_malformed_json() {
    let root = temp_cursor_root("malformed");
    let store = FileIndexCursorStore::new(&root);
    std::fs::create_dir_all(&root).expect("cursor dir");
    std::fs::write(root.join("fixture-job.json"), b"{not-json").expect("write malformed cursor");

    let error = store
        .load(&IndexJobId::new("fixture-job").expect("job id"))
        .expect_err("malformed cursor is an error");

    assert_eq!(error.kind, DatalensErrorKind::InvalidInput);
    assert!(error.message.contains("decode index cursor"));
}

#[test]
fn test_file_cursor_store_encodes_job_id_as_path_safe_filename() {
    let root = temp_cursor_root("path-safe");
    let store = FileIndexCursorStore::new(&root);
    let cursor = cursor(".. hidden job:*?");

    store.save(&cursor).expect("save cursor");

    let files = root
        .read_dir()
        .expect("cursor dir")
        .map(|entry| entry.expect("entry").path())
        .collect::<Vec<_>>();
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].parent(), Some(root.as_path()));
    assert_eq!(
        store
            .load(&cursor.job_id)
            .expect("load cursor")
            .expect("cursor exists"),
        cursor
    );
}

fn cursor(job_id: &str) -> IndexCursor {
    IndexCursor {
        job_id: IndexJobId::new(job_id).expect("job id"),
        chain: ChainIdentity::try_new(ChainFamily::Evm, "ethereum", Some(NetworkId::numeric(1)))
            .expect("chain"),
        dataset_key: DatasetKey::evm_blocks(),
        selector: DatasetSelector::all(),
        range_kind: datalens_chain::HeightRangeKind::Block,
        next_height: 5,
        completed_chunks: vec![0, 1],
        completed_ranges: vec![
            LedgerRange::blocks(1, 2).expect("range"),
            LedgerRange::blocks(3, 4).expect("range"),
        ],
        failure_state: None,
        next_chunk_ordinal: 2,
        last_checkpointed_range: Some(LedgerRange::blocks(3, 4).expect("range")),
    }
}

fn chunk(start: u64, end: u64, ordinal: u64) -> IndexChunk {
    IndexChunk {
        ordinal,
        dataset_key: DatasetKey::evm_blocks(),
        selector: DatasetSelector::all(),
        range: LedgerRange::blocks(start, end).expect("range"),
        retry_policy: IndexRetryPolicy {
            max_attempts: 1,
            initial_backoff_ms: 0,
            max_backoff_ms: 0,
        },
    }
}

fn temp_cursor_root(name: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "datalens-indexer-file-cursor-{name}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).expect("create cursor root");
    root
}
