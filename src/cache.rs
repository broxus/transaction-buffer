use crate::sqlx_client::get_all_raw_transactions;
use crate::RawTransaction;
use itertools::Itertools;
use sqlx::{Pool, Postgres};
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct RawCache(Arc<RwLock<Vec<RawTransaction>>>);

impl RawCache {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(vec![])))
    }

    pub async fn fill_raws(&self, pg_pool: &Pool<Postgres>) {
        let raw_transactions = get_all_raw_transactions(pg_pool)
            .await
            .map(|x| x.into_iter().map(RawTransaction::from).collect_vec())
            .unwrap_or_default();

        *self.0.write().await = raw_transactions;
    }

    pub async fn insert_raw(&self, raw: RawTransaction) {
        self.0.write().await.push(raw);
    }

    pub async fn get_raws(
        &self,
        last_timestamp_block: i32,
        timer: Arc<RwLock<i32>>,
        cache_timer: i32,
    ) -> (Vec<RawTransaction>, Vec<(i32, i64)>) {
        let mut lock = self.0.write().await;
        let time = *timer.read().await;
        let (res, cache) = lock
            .drain(..)
            .fold((vec![], vec![]), |(mut res, mut cache), x| {
                if (x.data.now as i32) < last_timestamp_block || time >= cache_timer {
                    res.push(x)
                } else {
                    cache.push(x)
                };
                (res, cache)
            });

        *lock = cache;

        res.into_iter()
            .sorted_by(|x, y| match x.data.now.cmp(&y.data.now) {
                Ordering::Less => Ordering::Less,
                Ordering::Equal => x.data.lt.cmp(&y.data.lt),
                Ordering::Greater => Ordering::Greater,
            })
            .fold((vec![], vec![]), |(mut raws, mut times), x| {
                times.push((x.data.now as i32, x.data.lt as i64));
                raws.push(x);
                (raws, times)
            })
    }
}
