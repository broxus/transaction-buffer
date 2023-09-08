use futures::channel::mpsc::{Receiver, Sender};
use nekoton_abi::transaction_parser::ExtractedOwned;
use std::sync::Arc;
use tokio::sync::Notify;
use ton_block::Transaction;
use transaction_consumer::TransactionConsumer;

pub struct BufferedConsumerConfig {
    pub transaction_consumer: Arc<TransactionConsumer>,
    pub any_extractable: Vec<AnyExtractable>,
    pub buff_size: i64,
    pub commit_time_secs: i32,
    pub cache_timer: i32,
    pub rocksdb_path: String,
    pub rocksdb_drop_base_index: u32,
}

#[derive(Debug, Clone)]
pub enum AnyExtractable {
    Event(ton_abi::Event),
    Function(ton_abi::Function),
}

impl BufferedConsumerConfig {
    pub fn new(
        transaction_consumer: Arc<TransactionConsumer>,
        any_extractable: Vec<AnyExtractable>,
        buff_size: i64,
        commit_time_secs: i32,
        cache_timer: i32,
        rocksdb_path: String,
        rocksdb_drop_base_index: u32,
    ) -> Self {
        Self {
            transaction_consumer,
            any_extractable,
            buff_size,
            commit_time_secs,
            cache_timer,
            rocksdb_path,
            rocksdb_drop_base_index,
        }
    }
}

pub struct BufferedConsumerChannels {
    pub rx_parsed_events: Receiver<Vec<(Vec<ExtractedOwned>, Transaction)>>,
    pub tx_commit: Sender<Vec<Transaction>>,
    pub notify_for_services: Arc<Notify>,
}
