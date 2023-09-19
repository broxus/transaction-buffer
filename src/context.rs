use std::sync::Arc;

use nekoton_abi::TransactionParser;
use tokio::sync::{Notify, RwLock};

use crate::cache::RawCache;
use crate::models::BufferedConsumerConfig;
use crate::rocksdb_client::RocksdbClient;
use crate::utils::create_transaction_parser;

pub struct BufferContext {
    pub rocksdb: Arc<RocksdbClient>,
    pub raw_cache: RawCache,
    pub time: RwLock<i32>,
    pub parser: TransactionParser,
    pub config: BufferedConsumerConfig,
    pub timestamp_last_block: RwLock<i32>,
    pub notify_for_services: Arc<Notify>,
}

impl BufferContext {
    pub fn new(config: BufferedConsumerConfig, notify_for_services: Arc<Notify>, rocksdb: Arc<RocksdbClient>) -> Arc<Self> {
        let raw_cache = RawCache::default();
        let time = RwLock::new(0);
        let parser =
            create_transaction_parser(config.any_extractable.clone()).expect("cant create parser");

        let timestamp_last_block = RwLock::new(0_i32);

        Arc::new(Self {
            rocksdb,
            raw_cache,
            time,
            parser,
            config,
            timestamp_last_block,
            notify_for_services,
        })
    }
}
