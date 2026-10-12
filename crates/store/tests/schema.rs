use datalens_core::{ChainId, ChainName};
use sqlx::PgPool;

async fn partitions(pool: &PgPool) -> Vec<(String, String)> {
    sqlx::query_as(
        "select parent.relname::text, child.relname::text
         from pg_inherits
         join pg_class parent on parent.oid = pg_inherits.inhparent
         join pg_class child on child.oid = pg_inherits.inhrelid
         where parent.relname in ('evm_logs', 'evm_blocks')
         order by 1, 2",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

#[sqlx::test(migrations = false)]
async fn migrate_is_rerunnable_and_creates_schema(pool: PgPool) {
    datalens_store::migrate(&pool).await.unwrap();
    datalens_store::migrate(&pool).await.unwrap();

    let tables: Vec<String> = sqlx::query_scalar(
        "select tablename::text from pg_tables
         where schemaname = 'public' and tablename <> '_sqlx_migrations'
         order by 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        tables,
        [
            "chain_heads",
            "chains",
            "coverage",
            "evm_blocks",
            "evm_logs",
            "evm_tx_senders",
            "fill_jobs",
            "follow_progress",
            "selectors",
            "subscriptions",
            "usage_daily",
        ]
    );
}

#[sqlx::test(migrations = false)]
async fn ensure_chain_is_idempotent_and_creates_partitions(pool: PgPool) {
    datalens_store::migrate(&pool).await.unwrap();
    let arbitrum = ChainName::new("arbitrum").unwrap();
    let darwinia = ChainName::new("darwinia").unwrap();

    for _ in 0..2 {
        datalens_store::ensure_chain(&pool, ChainId::new(42161).unwrap(), &arbitrum)
            .await
            .unwrap();
    }
    datalens_store::ensure_chain(&pool, ChainId::new(46).unwrap(), &darwinia)
        .await
        .unwrap();

    let chains: Vec<(i32, String)> =
        sqlx::query_as("select chain_id, name from chains order by chain_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        chains,
        [(46, "darwinia".to_owned()), (42161, "arbitrum".to_owned())]
    );

    let expected: Vec<(String, String)> = [
        ("evm_blocks", "evm_blocks_42161"),
        ("evm_blocks", "evm_blocks_46"),
        ("evm_logs", "evm_logs_42161"),
        ("evm_logs", "evm_logs_46"),
    ]
    .into_iter()
    .map(|(parent, child)| (parent.to_owned(), child.to_owned()))
    .collect();
    assert_eq!(partitions(&pool).await, expected);
}

#[sqlx::test(migrations = false)]
async fn concurrent_ensure_chain_from_replicas_succeeds(pool: PgPool) {
    datalens_store::migrate(&pool).await.unwrap();
    let base = ChainName::new("base").unwrap();
    let id = ChainId::new(8453).unwrap();
    let calls = (0..4).map(|_| datalens_store::ensure_chain(&pool, id, &base));
    for result in futures::future::join_all(calls).await {
        result.unwrap();
    }
    assert_eq!(
        partitions(&pool).await,
        [
            ("evm_blocks".to_owned(), "evm_blocks_8453".to_owned()),
            ("evm_logs".to_owned(), "evm_logs_8453".to_owned()),
        ]
    );
}

#[sqlx::test(migrations = false)]
async fn partition_routes_rows_by_chain(pool: PgPool) {
    datalens_store::migrate(&pool).await.unwrap();
    datalens_store::ensure_chain(
        &pool,
        ChainId::new(1).unwrap(),
        &"ethereum".parse().unwrap(),
    )
    .await
    .unwrap();

    sqlx::query(
        "insert into evm_logs (chain_id, block_number, log_index, block_hash, transaction_index,
           transaction_hash, address, data, finality)
         values (1, 10, 0, '\\x00', 0, '\\x00', '\\x0a', '\\x', 3)",
    )
    .execute(&pool)
    .await
    .unwrap();
    let in_partition: i64 = sqlx::query_scalar("select count(*) from evm_logs_1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(in_partition, 1);

    let unknown_chain = sqlx::query(
        "insert into evm_logs (chain_id, block_number, log_index, block_hash, transaction_index,
           transaction_hash, address, data, finality)
         values (2, 10, 0, '\\x00', 0, '\\x00', '\\x0a', '\\x', 3)",
    )
    .execute(&pool)
    .await;
    assert!(unknown_chain.is_err());
}

#[sqlx::test(migrations = false)]
async fn coverage_rejects_overlapping_ranges_for_one_selector(pool: PgPool) {
    datalens_store::migrate(&pool).await.unwrap();
    datalens_store::ensure_chain(
        &pool,
        ChainId::new(1).unwrap(),
        &"ethereum".parse().unwrap(),
    )
    .await
    .unwrap();
    let selector_id: i64 = sqlx::query_scalar(
        "insert into selectors (chain_id, dataset, fingerprint, canonical_key)
         values (1, 'evm.blocks', 'f', 'all') returning id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let insert = |range: &'static str| {
        sqlx::query(
            "insert into coverage (chain_id, dataset, selector_id, blocks, finality)
             values (1, 'evm.blocks', $1, $2::int8range, 3)",
        )
        .bind(selector_id)
        .bind(range)
    };
    insert("[0,100)").execute(&pool).await.unwrap();
    insert("[100,200)").execute(&pool).await.unwrap();
    assert!(insert("[150,250)").execute(&pool).await.is_err());
}

#[sqlx::test(migrations = false)]
async fn coverage_rejects_finality_other_than_safe_or_finalized(pool: PgPool) {
    datalens_store::migrate(&pool).await.unwrap();
    datalens_store::ensure_chain(
        &pool,
        ChainId::new(1).unwrap(),
        &"ethereum".parse().unwrap(),
    )
    .await
    .unwrap();
    let selector_id: i64 = sqlx::query_scalar(
        "insert into selectors (chain_id, dataset, fingerprint, canonical_key)
         values (1, 'evm.blocks', 'f', 'all') returning id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let insert = |range: &'static str, finality: i16| {
        sqlx::query(
            "insert into coverage (chain_id, dataset, selector_id, blocks, finality)
             values (1, 'evm.blocks', $1, $2::int8range, $3)",
        )
        .bind(selector_id)
        .bind(range)
        .bind(finality)
    };
    for finality in [0, 1, 4] {
        assert!(
            insert("[0,10)", finality).execute(&pool).await.is_err(),
            "finality {finality}"
        );
    }
    insert("[0,10)", 2).execute(&pool).await.unwrap();
    insert("[10,20)", 3).execute(&pool).await.unwrap();
}

#[sqlx::test(migrations = false)]
async fn planner_indexes_keep_their_definitions(pool: PgPool) {
    datalens_store::migrate(&pool).await.unwrap();
    let rows: Vec<(String, String)> = sqlx::query_as(
        "select indexname::text, indexdef
         from pg_indexes
         where schemaname = 'public'
           and indexname in (
             'evm_logs_address_idx',
             'evm_tx_senders_block_idx',
             'fill_jobs_active',
             'selectors_addresses_idx'
           )
         order by 1",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            (
                "evm_logs_address_idx".to_owned(),
                "CREATE INDEX evm_logs_address_idx ON ONLY public.evm_logs USING btree (chain_id, address, block_number, log_index)".to_owned(),
            ),
            (
                "evm_tx_senders_block_idx".to_owned(),
                "CREATE INDEX evm_tx_senders_block_idx ON public.evm_tx_senders USING btree (chain_id, block_number)".to_owned(),
            ),
            (
                "fill_jobs_active".to_owned(),
                "CREATE UNIQUE INDEX fill_jobs_active ON public.fill_jobs USING btree (chain_id, dataset, selector_id, blocks) WHERE (status = ANY (ARRAY['pending'::text, 'running'::text]))".to_owned(),
            ),
            (
                "selectors_addresses_idx".to_owned(),
                "CREATE INDEX selectors_addresses_idx ON public.selectors USING gin (addresses)".to_owned(),
            ),
        ]
    );
}
