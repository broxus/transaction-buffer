use crate::models::RawTransactionFromDb;
use anyhow::Result;
use itertools::Itertools;
use sqlx::postgres::PgArguments;
use sqlx::{Arguments, Pool};
use sqlx::{Postgres, Row, Transaction};
use std::cmp::Ordering;

const INSERT_RAW_TRANSACTION_QUERY: &str = "INSERT INTO raw_transactions (transaction, transaction_hash, timestamp_block, timestamp_lt, processed) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING";
const INSERT_RAW_TRANSACTIONS_QUERY: &str = "INSERT INTO raw_transactions (transaction, transaction_hash, timestamp_block, timestamp_lt, processed) \
SELECT * FROM UNNEST ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING";

const COUNT_RAW_TRANSACTION_QUERY: &str = "SELECT count(*) FROM raw_transactions";

const COUNT_RAW_NOT_PROCESSED_TRANSACTION_QUERY: &str =
    "SELECT count(*) FROM raw_transactions WHERE processed = false";

const GET_AND_UPDATE_RAW_TRANSACTIONS_QUERY: &str = "UPDATE raw_transactions
SET processed = true
WHERE (transaction_hash) IN (SELECT transaction_hash
                                          FROM raw_transactions
                                          WHERE timestamp_block < $1 AND processed = false
                                          ORDER BY timestamp_block, timestamp_lt
                                          LIMIT $2)
returning *;";

const GET_ALL_RAW_TRANSACTIONS_QUERY: &str = "SELECT * FROM raw_transactions WHERE processed = false ORDER BY (timestamp_block, timestamp_lt);";

const CREATE_TABLE_RAW_TRANSACTIONS_QUERY: &str = "CREATE TABLE IF NOT EXISTS raw_transactions
(
    transaction      BYTEA   NOT NULL,
    transaction_hash BYTEA   NOT NULL,
    timestamp_block  INTEGER NOT NULL,
    timestamp_lt     BIGINT  NOT NULL,
    created_at       BIGINT  NOT NULL DEFAULT extract(epoch from (CURRENT_TIMESTAMP(3) at time zone 'utc')) * 1000,
    processed        BOOL    NOT NULL,
    PRIMARY KEY (transaction_hash)
);";

const CREATE_TABLE_DROP_BASE_INDEX_QUERY: &str = "CREATE TABLE IF NOT EXISTS drop_base_index
(
    name  VARCHAR NOT NULL UNIQUE,
    value INTEGER NOT NULL
);";

const INSERT_DROP_BASE_INDEX_QUERY: &str =
    "INSERT INTO drop_base_index (name, value) values ('index', $1);";

const SELECT_DROP_BASE_INDEX_QUERY: &str = "SELECT value FROM drop_base_index;";

const DROP_TABLES_QUERY: &str = "DO
$$
    DECLARE
        r RECORD;
    BEGIN
        FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema())
            LOOP
                EXECUTE 'DROP TABLE ' || quote_ident(r.tablename) || ' CASCADE';
            END LOOP;
    END
$$;";

const DROP_FUNCTIONS_QUERY: &str = "DO
$$
    DECLARE
        r RECORD;
    BEGIN
        FOR r IN (SELECT p.proname as fun
                  FROM pg_catalog.pg_namespace n
                           JOIN
                       pg_catalog.pg_proc p ON
                           p.pronamespace = n.oid
                  WHERE p.prokind = 'f'
                    AND n.nspname = 'public')
            LOOP
                EXECUTE 'DROP FUNCTION ' || quote_ident(r.fun) || ' CASCADE';
            END LOOP;
    END
$$;";

const CREATE_INDEX_RAW_TRANSACTIONS_QUERY: &str = "CREATE INDEX IF NOT EXISTS raw_transactions_ix_ttp ON raw_transactions (timestamp_block, timestamp_lt, processed);";

const UPDATE_RAW_TRANSACTIONS_PROCESSED_TRUE_QUERY: &str = "UPDATE raw_transactions SET processed = true WHERE (timestamp_block, timestamp_lt) IN (SELECT * FROM UNNEST ($1, $2));";

pub async fn insert_raw_transaction(
    raw_transaction: RawTransactionFromDb,
    pg_pool: &Pool<Postgres>,
) -> Result<()> {
    let mut args = PgArguments::default();
    args.add(raw_transaction.transaction);
    args.add(raw_transaction.transaction_hash);
    args.add(raw_transaction.timestamp_block);
    args.add(raw_transaction.timestamp_lt);
    args.add(raw_transaction.processed);

    sqlx::query_with(INSERT_RAW_TRANSACTION_QUERY, args)
        .execute(pg_pool)
        .await?;

    Ok(())
}

pub async fn get_raw_transactions(
    limit: i64,
    timestamp_block_lt: i32,
    begin: &mut Transaction<'_, Postgres>,
) -> Result<Vec<RawTransactionFromDb>> {
    let mut args = PgArguments::default();
    args.add(timestamp_block_lt);
    args.add(limit);

    sqlx::query_with(GET_AND_UPDATE_RAW_TRANSACTIONS_QUERY, args)
        .fetch_all(&mut **begin)
        .await
        .map(|y| {
            y.into_iter()
                .map(RawTransactionFromDb::from)
                .sorted_by(|x, y| match x.timestamp_block.cmp(&y.timestamp_block) {
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => x.timestamp_lt.cmp(&y.timestamp_lt),
                    Ordering::Greater => Ordering::Greater,
                })
                .collect::<Vec<_>>()
        })
        .map_err(anyhow::Error::new)
}

pub async fn get_count_raw_transactions(pg_pool: &Pool<Postgres>) -> i64 {
    let count: i64 = sqlx::query(COUNT_RAW_TRANSACTION_QUERY)
        .fetch_one(pg_pool)
        .await
        .map(|x| x.get(0))
        .unwrap_or_default();

    count
}

pub async fn get_count_not_processed_raw_transactions(pg_pool: &Pool<Postgres>) -> i64 {
    let count: i64 = sqlx::query(COUNT_RAW_NOT_PROCESSED_TRANSACTION_QUERY)
        .fetch_one(pg_pool)
        .await
        .map(|x| x.get(0))
        .unwrap_or_default();

    count
}

pub async fn create_table_raw_transactions(pg_pool: &Pool<Postgres>) {
    if let Err(e) = sqlx::query(CREATE_TABLE_RAW_TRANSACTIONS_QUERY)
        .execute(pg_pool)
        .await
    {
        log::error!("create table raw_transactions ERROR {}", e);
    }

    if let Err(e) = sqlx::query(CREATE_INDEX_RAW_TRANSACTIONS_QUERY)
        .execute(pg_pool)
        .await
    {
        log::error!("create index raw_transactions ERROR {}", e);
    }
}

pub async fn insert_raw_transactions(
    raw_transactions: &mut Vec<RawTransactionFromDb>,
    pg_pool: &Pool<Postgres>,
) -> Result<()> {
    let mut transaction: Vec<Vec<u8>> = Vec::new();
    let mut transaction_hash: Vec<Vec<u8>> = Vec::new();
    let mut timestamp_block: Vec<i32> = Vec::new();
    let mut timestamp_lt: Vec<i64> = Vec::new();
    let mut processed: Vec<bool> = Vec::new();

    raw_transactions.drain(..).for_each(|row| {
        transaction.push(row.transaction);
        transaction_hash.push(row.transaction_hash);
        timestamp_block.push(row.timestamp_block);
        timestamp_lt.push(row.timestamp_lt);
        processed.push(row.processed);
    });

    sqlx::query(INSERT_RAW_TRANSACTIONS_QUERY)
        .bind(transaction)
        .bind(transaction_hash)
        .bind(timestamp_block)
        .bind(timestamp_lt)
        .bind(processed)
        .execute(pg_pool)
        .await?;

    Ok(())
}

pub async fn create_drop_index_table(pg_pool: &Pool<Postgres>) {
    if let Err(e) = sqlx::query(CREATE_TABLE_DROP_BASE_INDEX_QUERY)
        .execute(pg_pool)
        .await
    {
        log::error!("create table drop_index ERROR {}", e);
    }
}

pub async fn get_drop_index(pg_pool: &Pool<Postgres>) -> Result<i32, anyhow::Error> {
    let index: i32 = sqlx::query(SELECT_DROP_BASE_INDEX_QUERY)
        .fetch_one(pg_pool)
        .await
        .map(|x| x.get(0))?;
    Ok(index)
}

pub async fn insert_drop_index(pg_pool: &Pool<Postgres>, index: i32) {
    if let Err(e) = sqlx::query(INSERT_DROP_BASE_INDEX_QUERY)
        .bind(index)
        .execute(pg_pool)
        .await
    {
        log::error!("insert index drop ERROR {}", e);
    }
}

pub async fn drop_tables(pg_pool: &Pool<Postgres>) {
    if let Err(e) = sqlx::query(DROP_TABLES_QUERY).execute(pg_pool).await {
        log::error!("drop tables ERROR {}", e);
    }
}

pub async fn drop_functions(pg_pool: &Pool<Postgres>) {
    if let Err(e) = sqlx::query(DROP_FUNCTIONS_QUERY).execute(pg_pool).await {
        log::error!("drop functions ERROR {}", e);
    }
}

pub async fn get_all_raw_transactions(
    pg_pool: &Pool<Postgres>,
) -> Result<Vec<RawTransactionFromDb>, anyhow::Error> {
    sqlx::query(GET_ALL_RAW_TRANSACTIONS_QUERY)
        .fetch_all(pg_pool)
        .await
        .map(|x| x.into_iter().map(RawTransactionFromDb::from).collect_vec())
        .map_err(anyhow::Error::new)
}

pub async fn update_raw_transactions_set_processed_true(
    pg_pool: &Pool<Postgres>,
    times: Vec<(i32, i64)>,
) {
    let (timestamp_blocks, timestamp_lts) =
        times
            .into_iter()
            .fold((vec![], vec![]), |(mut block, mut lt), x| {
                block.push(x.0);
                lt.push(x.1);
                (block, lt)
            });

    let mut args = PgArguments::default();
    args.add(timestamp_blocks);
    args.add(timestamp_lts);

    if let Err(e) = sqlx::query_with(UPDATE_RAW_TRANSACTIONS_PROCESSED_TRUE_QUERY, args)
        .execute(pg_pool)
        .await
    {
        log::error!("update raw transactions processed true ERROR {}", e);
    }
}

#[cfg(test)]
mod test {
    use crate::update_raw_transactions_set_processed_true;
    use sqlx::PgPool;

    #[tokio::test]
    async fn test_insert() {
        let pg_pool = PgPool::connect("postgresql://postgres:postgres@localhost:5432/test_base")
            .await
            .unwrap();

        let times = vec![
            (1656071372, 27915771000001_i64),
            (1656070201, 27915328000006),
        ];
        update_raw_transactions_set_processed_true(&pg_pool, times).await;
    }
}
