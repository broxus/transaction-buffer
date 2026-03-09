use crate::rocksdb_client::RocksdbClient;
use futures::channel::mpsc::{Receiver, Sender};
use nekoton_abi::transaction_parser::ExtractedOwned;
use std::sync::Arc;
use tokio::sync::Notify;
use ton_block::Transaction;
use transaction_consumer::TransactionConsumer;

#[derive(Debug, Clone)]
pub struct RocksdbClientConstants {
    pub drop_base_index: u32,
    pub from_timestamp: u32,
    pub postgres_base_is_dropped: bool,
    // if true - kafka offset always beginner
    pub is_new_kafka: bool,
}

pub struct BufferedConsumerConfig {
    pub transaction_consumer: Arc<TransactionConsumer>,
    pub any_extractable: Vec<AnyExtractable>,
    pub buff_size: i64,
    pub commit_time_secs: i32,
    pub cache_timer: i32,
    pub rocksdb_path: String,
    pub rocksdb_drop_base_index: u32,
    pub parsing_from_timestamp: Option<u32>,
    pub postgres_base_is_dropped: Option<bool>,
    pub transactions_logger_counter: i32,
    pub first_iterate_delay: Option<i32>,
    pub is_new_kafka: Option<bool>,
    pub archive_node_config: Option<ArchiveNodeConfig>,
}

#[derive(Debug, Clone)]
pub enum AnyExtractable {
    Event(ton_abi::Event),
    Function(ton_abi::Function),
}

#[derive(Debug, Clone)]
pub struct ArchiveNodeConfig {
    pub access_key: String,
    pub secret_key: String,
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub from_timestamp: Option<u32>,
    pub max_parallel_workers: Option<usize>,
    pub archive_batch_size: Option<usize>,
}

impl BufferedConsumerConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        transaction_consumer: Arc<TransactionConsumer>,
        any_extractable: Vec<AnyExtractable>,
        buff_size: i64,
        commit_time_secs: i32,
        cache_timer: i32,
        rocksdb_path: String,
        rocksdb_drop_base_index: u32,
        parsing_from_timestamp: Option<u32>,
        postgres_base_is_dropped: Option<bool>,
        transactions_logger_counter: i32,
        first_iterate_delay: Option<i32>,
        is_new_kafka: Option<bool>,
        archive_node_config: Option<ArchiveNodeConfig>,
    ) -> Self {
        Self {
            transaction_consumer,
            any_extractable,
            buff_size,
            commit_time_secs,
            cache_timer,
            rocksdb_path,
            rocksdb_drop_base_index,
            parsing_from_timestamp,
            postgres_base_is_dropped,
            transactions_logger_counter,
            first_iterate_delay,
            is_new_kafka,
            archive_node_config,
        }
    }
}

pub struct BufferedConsumerChannels {
    pub rx_parsed_events: Receiver<Vec<(Vec<ExtractedOwned>, Transaction)>>,
    pub tx_commit: Sender<Vec<Transaction>>,
    pub notify_for_services: Arc<Notify>,
    pub rocksdb_client: Arc<RocksdbClient>,
}
