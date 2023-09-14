use crate::context::BufferContext;
use crate::models::{BufferedConsumerChannels, BufferedConsumerConfig};
use crate::utils::timer;
use crate::{commit_transactions, parse_transaction};
use std::sync::Arc;
use tokio::sync::Notify;

pub fn test_from_local_transactions(config: BufferedConsumerConfig) -> BufferedConsumerChannels {
    let (tx_parsed_events, rx_parsed_events) = futures::channel::mpsc::channel(1);
    let (tx_commit, rx_commit) = futures::channel::mpsc::channel(1);
    let notify_for_services = Arc::new(Notify::new());

    let context = BufferContext::new(config, notify_for_services.clone());

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
    }
}
