use crate::context::BufferContext;
use crate::models::{BufferedConsumerChannels, BufferedConsumerConfig, RocksdbClientConstants};
use crate::utils::{create_rocksdb, timer};
use crate::{commit_transactions, parse_transaction};
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::Instrument;

pub fn test_from_local_transactions(config: BufferedConsumerConfig) -> BufferedConsumerChannels {
    let (tx_parsed_events, rx_parsed_events) = futures::channel::mpsc::channel(1);
    let (tx_commit, rx_commit) = futures::channel::mpsc::channel(1);
    let notify_for_services = Arc::new(Notify::new());
    #[cfg(feature = "compact-logs")]
    let pipeline_span = tracing::info_span!("transaction_buffer_local_test_pipeline");
    #[cfg(not(feature = "compact-logs"))]
    let pipeline_span = tracing::info_span!(
        "transaction_buffer_local_test_pipeline",
        rocksdb_path = %config.rocksdb_path,
        buff_size = config.buff_size,
        cache_timer = config.cache_timer,
        rocksdb_drop_base_index = config.rocksdb_drop_base_index,
        parsing_from_timestamp = config.parsing_from_timestamp.unwrap_or_default(),
        is_new_kafka = config.is_new_kafka.unwrap_or(false),
    );

    let rocksdb = Arc::new(create_rocksdb(
        &config.rocksdb_path,
        RocksdbClientConstants {
            drop_base_index: config.rocksdb_drop_base_index,
            from_timestamp: config.parsing_from_timestamp.unwrap_or_default(),
            postgres_base_is_dropped: config.postgres_base_is_dropped.unwrap_or_default(),
            is_new_kafka: config.is_new_kafka.unwrap_or(false),
        },
    ));

    let context = BufferContext::new(config, notify_for_services.clone(), rocksdb.clone());
    let current_span = pipeline_span;

    {
        let context = context.clone();
        let span = tracing::debug_span!(parent: &current_span, "timer_task");
        tokio::spawn(timer(context).instrument(span));
    }

    {
        let context = context.clone();
        let span = tracing::info_span!(parent: &current_span, "commit_transactions_task");
        tokio::spawn(commit_transactions(rx_commit, context).instrument(span));
    }

    let span = tracing::info_span!(parent: &current_span, "parse_transaction_task");
    tokio::spawn(parse_transaction(tx_parsed_events, context).instrument(span));

    BufferedConsumerChannels {
        rx_parsed_events,
        tx_commit,
        notify_for_services,
        rocksdb_client: rocksdb,
    }
}
