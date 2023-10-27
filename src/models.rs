use anyhow::Context;
use dashmap::DashSet;
use futures::channel::mpsc::{Receiver, Sender};
use nekoton_abi::transaction_parser::ExtractedOwned;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use std::sync::Arc;
use tokio::sync::Notify;
use ton_block::{Deserializable, GetRepresentationHash, MsgAddressInt, Serializable, Transaction};
use ton_types::UInt256;
use transaction_consumer::TransactionConsumer;

pub struct BufferedConsumerConfig {
    pub transaction_consumer: Arc<TransactionConsumer>,
    pub pg_pool: PgPool,
    pub any_extractable: Vec<AnyExtractable>,
    pub buff_size: i64,
    pub commit_time_secs: i32,
    pub cache_timer: i32,
    pub save_failed_transactions_for_accounts: Arc<DashSet<MsgAddressInt>>,
}

#[derive(Debug, Clone)]
pub enum AnyExtractable {
    Event(ton_abi::Event),
    Function(ton_abi::Function),
}

impl BufferedConsumerConfig {
    pub fn new(
        transaction_consumer: Arc<TransactionConsumer>,
        pg_pool: PgPool,
        any_extractable: Vec<AnyExtractable>,
        buff_size: i64,
        commit_time_secs: i32,
        cache_timer: i32,
        save_failed_transactions_for_accounts: Option<Arc<DashSet<MsgAddressInt>>>,
    ) -> Self {
        Self {
            transaction_consumer,
            pg_pool,
            any_extractable,
            buff_size,
            commit_time_secs,
            cache_timer,
            save_failed_transactions_for_accounts: save_failed_transactions_for_accounts
                .unwrap_or_default(),
        }
    }
}

pub struct BufferedConsumerChannels {
    pub rx_parsed_events: Receiver<Vec<(Vec<ExtractedOwned>, RawTransaction)>>,
    pub tx_commit: Sender<()>,
    pub notify_for_services: Arc<Notify>,
}

#[derive(Debug, Clone)]
pub struct RawTransactionFromDb {
    pub transaction: Vec<u8>,
    pub transaction_hash: Vec<u8>,
    pub timestamp_block: i32,
    pub timestamp_lt: i64,
    pub created_at: i64,
    pub processed: bool,
}

impl From<PgRow> for RawTransactionFromDb {
    fn from(x: PgRow) -> Self {
        RawTransactionFromDb {
            transaction: x.get(0),
            transaction_hash: x.get(1),
            timestamp_block: x.get(2),
            timestamp_lt: x.get(3),
            created_at: x.get(4),
            processed: x.get(5),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RawTransaction {
    pub hash: UInt256,
    pub data: Transaction,
}

impl From<RawTransactionFromDb> for RawTransaction {
    fn from(value: RawTransactionFromDb) -> Self {
        let transaction =
            ton_block::Transaction::construct_from_bytes(value.transaction.as_slice()).unwrap();

        RawTransaction {
            hash: UInt256::from_be_bytes(&value.transaction_hash),
            data: transaction,
        }
    }
}

impl From<Transaction> for RawTransactionFromDb {
    fn from(x: Transaction) -> Self {
        RawTransactionFromDb {
            transaction: x
                .write_to_bytes()
                .context("Failed serializing tx to bytes")
                .unwrap(),
            transaction_hash: x.hash().unwrap().as_slice().to_vec(),
            timestamp_block: x.now as i32,
            timestamp_lt: x.lt as i64,
            created_at: 0,
            processed: false,
        }
    }
}

impl From<Transaction> for RawTransaction {
    fn from(x: Transaction) -> Self {
        RawTransaction {
            hash: x.hash().unwrap(),
            data: x,
        }
    }
}
