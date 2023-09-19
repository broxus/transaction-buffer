use crate::context::BufferContext;
use crate::models::{BufferedConsumerChannels, BufferedConsumerConfig, RocksdbClientConstants};
use crate::utils::{create_rocksdb, timer};
use crate::{commit_transactions, parse_transaction};
use std::sync::Arc;
use tokio::sync::Notify;

pub fn test_from_local_transactions(config: BufferedConsumerConfig) -> BufferedConsumerChannels {
    let (tx_parsed_events, rx_parsed_events) = futures::channel::mpsc::channel(1);
    let (tx_commit, rx_commit) = futures::channel::mpsc::channel(1);
    let notify_for_services = Arc::new(Notify::new());

    let rocksdb = Arc::new(create_rocksdb(
        &config.rocksdb_path,
        RocksdbClientConstants {
            drop_base_index: config.rocksdb_drop_base_index,
            from_timestamp: config.parsing_from_timestamp.unwrap_or_default(),
            postgres_base_is_dropped: config.postgres_base_is_dropped.unwrap_or_default(),
        },
    ));

    let context = BufferContext::new(config, notify_for_services.clone(), rocksdb.clone());

    {
        let context = context.clone();
        tokio::spawn(timer(context));
    }

    {
        let context = context.clone();
        tokio::spawn(commit_transactions(rx_commit, context));
    }

    tokio::spawn(parse_transaction(tx_parsed_events, context));

    BufferedConsumerChannels {
        rx_parsed_events,
        tx_commit,
        notify_for_services,
        rocksdb_client: rocksdb
    }
}
