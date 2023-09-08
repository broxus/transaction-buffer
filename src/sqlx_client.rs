use anyhow::Result;
use sqlx::Pool;
use sqlx::{Postgres, Row};

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
