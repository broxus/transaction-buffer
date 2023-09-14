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
}

#[derive(Debug, Clone)]
pub enum AnyExtractable {
    Event(ton_abi::Event),
    Function(ton_abi::Function),
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
        }
    }
}

pub struct BufferedConsumerChannels {
    pub rx_parsed_events: Receiver<Vec<(Vec<ExtractedOwned>, Transaction)>>,
    pub tx_commit: Sender<Vec<Transaction>>,
    pub notify_for_services: Arc<Notify>,
}
