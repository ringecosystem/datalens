mod common;

use sqlx::PgPool;

/// A covered single-address query with a topic filter must use the address index.
/// The table also holds other addresses, so a sequential scan is not the cheap plan.
#[sqlx::test(migrations = false)]
async fn read_logs_uses_the_address_index(pool: PgPool) {
    datalens_store::migrate(&pool).await.unwrap();
    let store = common::store(&pool).await;
    let _ = store;

    sqlx::query(
        "insert into evm_logs (
            chain_id, block_number, log_index, block_hash, transaction_index, transaction_hash,
            address, topic0, data, finality
         )
         select 1, g, 0,
                decode(lpad(to_hex(g), 64, '0'), 'hex'),
                0,
                decode(lpad(to_hex(g), 64, '0'), 'hex'),
                case when g <= 10000
                     then '\\x1111111111111111111111111111111111111111'::bytea
                     else decode(lpad(to_hex(g), 40, '0'), 'hex')
                end,
                '\\x2222222222222222222222222222222222222222222222222222222222222222'::bytea,
                '\\x'::bytea,
                3
         from generate_series(1, 100000) g",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("analyze evm_logs")
        .execute(&pool)
        .await
        .unwrap();

    let plan: serde_json::Value = sqlx::query_scalar(
        "explain (format json)
         select block_number, log_index
         from evm_logs
         where chain_id = 1
           and block_number between 1 and 10000
           and address = '\\x1111111111111111111111111111111111111111'::bytea
           and topic0 = '\\x2222222222222222222222222222222222222222222222222222222222222222'::bytea
         order by block_number, log_index
         limit 10001",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let text = plan.to_string();
    assert!(
        text.contains("Index") && text.contains("address"),
        "expected the address index, got {text}"
    );
}
