use crate::sqlx_client::{
    create_drop_index_table, drop_functions, drop_tables, get_drop_index, insert_drop_index,
};
use sqlx::{Pool, Postgres};

// return is dropped
pub async fn check_base(pg_pool: &Pool<Postgres>, drop_index: i32) -> bool {
    if let Ok(x) = get_drop_index(pg_pool).await {
        if x != drop_index {
            drop_tables(pg_pool).await;
            drop_functions(pg_pool).await;
            create_drop_index_table(pg_pool).await;
            insert_drop_index(pg_pool, drop_index).await;
            true
        } else {
            false
        }
    } else {
        create_drop_index_table(pg_pool).await;
        insert_drop_index(pg_pool, drop_index).await;
        true
    }
}

#[cfg(test)]
mod test {
    use crate::drop_base::check_base;
    use sqlx::PgPool;

    #[tokio::test]
    async fn test_drop_base() {
        let pg_pool = PgPool::connect("postgresql://postgres:postgres@localhost:5432/test_base")
            .await
            .unwrap();
        check_base(&pg_pool, 3).await;
    }
}
