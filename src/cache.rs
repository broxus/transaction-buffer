use crate::rocksdb_client::RocksdbClient;
use itertools::Itertools;
use std::cmp::Ordering;
use tokio::sync::RwLock;
use ton_block::Transaction;

#[derive(Default)]
pub struct RawCache(RwLock<Vec<Transaction>>);

impl RawCache {
    pub async fn fill_raws(&self, rocksdb: &RocksdbClient) {
        let raw_transactions = rocksdb
            .iterate_unprocessed_transactions([u8::MAX; 45])
            .collect_vec();

        *self.0.write().await = raw_transactions;
    }

    pub async fn insert_raw(&self, raw: Transaction) {
        self.0.write().await.push(raw);
    }

    pub async fn get_raws(
        &self,
        last_timestamp_block: i32,
        timer: &RwLock<i32>,
        cache_timer: i32,
    ) -> Vec<Transaction> {
        let mut lock = self.0.write().await;
        let time = *timer.read().await;
        let (res, cache) = lock
            .drain(..)
            .fold((vec![], vec![]), |(mut res, mut cache), x| {
                if (x.now as i32) < last_timestamp_block || time >= cache_timer {
                    res.push(x)
                } else {
                    cache.push(x)
                };
                (res, cache)
            });

        *lock = cache;

        res.into_iter()
            .sorted_by(|x, y| match x.now.cmp(&y.now) {
                Ordering::Less => Ordering::Less,
                Ordering::Equal => x.lt.cmp(&y.lt),
                Ordering::Greater => Ordering::Greater,
            })
            .fold(vec![], |mut raws, x| {
                raws.push(x);
                raws
            })
    }
}
