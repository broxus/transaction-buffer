mod cache;
pub mod drop_base;
pub mod models;
mod sqlx_client;
mod storage;

use crate::cache::RawCache;
use crate::models::{
    AnyExtractable, BufferedConsumerChannels, BufferedConsumerConfig, RawTransaction,
};
use crate::sqlx_client::{
    create_table_raw_transactions, get_count_not_processed_raw_transactions,
    get_count_raw_transactions, get_raw_transactions, insert_raw_transaction,
    insert_raw_transactions, update_raw_transactions_set_processed_true,
};
use crate::storage::{PersistentStorage, PersistentStorageConfig};
use chrono::NaiveDateTime;
use futures::channel::mpsc::{Receiver, Sender};
use futures::SinkExt;
use futures::StreamExt;
use nekoton_abi::transaction_parser::{Extracted, ExtractedOwned, ParsedType};
use nekoton_abi::TransactionParser;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tokio::time::sleep;
use ton_block::{TrComputePhase, Transaction};
use transaction_consumer::StreamFrom;
use weedb::WeeDb;

pub fn split_any_extractable(
    any_extractable: Vec<AnyExtractable>,
) -> (Vec<ton_abi::Function>, Vec<ton_abi::Event>) {
    let mut functions = Vec::new();
    let mut events = Vec::new();
    for any_extractable in any_extractable {
        match any_extractable {
            AnyExtractable::Function(function) => functions.push(function),
            AnyExtractable::Event(event) => events.push(event),
        }
    }

    functions.sort_by_key(|x| x.get_function_id());
    functions.dedup_by(|x, y| x.get_function_id() == y.get_function_id());
    events.sort_by(|x, y| x.id.cmp(&y.id));
    events.dedup_by(|x, y| x.id == y.id);

    (functions, events)
}

pub fn create_transaction_parser(any_extractable: Vec<AnyExtractable>) -> TransactionParser {
    let (functions, events) = split_any_extractable(any_extractable);

    TransactionParser::builder()
        .function_in_list(functions.clone(), false)
        .functions_out_list(functions, false)
        .events_list(events)
        .build()
        .unwrap()
}

pub fn split_extracted_owned(
    extracted: Vec<ExtractedOwned>,
) -> (Vec<ExtractedOwned>, Vec<ExtractedOwned>) // functions, events
{
    let mut functions = Vec::new();
    let mut events = Vec::new();

    for extracted_owned in extracted {
        match extracted_owned.parsed_type {
            ParsedType::FunctionInput
            | ParsedType::FunctionOutput
            | ParsedType::BouncedFunction => functions.push(extracted_owned),
            ParsedType::Event => events.push(extracted_owned),
        }
    }

    (functions, events)
}

#[allow(clippy::type_complexity)]
pub fn start_parsing_and_get_channels(config: BufferedConsumerConfig) -> BufferedConsumerChannels {
    let (tx_parsed_events, rx_parsed_events) = futures::channel::mpsc::channel(1);
    let (tx_commit, rx_commit) = futures::channel::mpsc::channel(1);
    let notify_for_services = Arc::new(Notify::new());

    {
        let notify_for_services = notify_for_services.clone();
        tokio::spawn(parse_kafka_transactions(
            config,
            tx_parsed_events,
            notify_for_services,
            rx_commit,
        ));
    }
    BufferedConsumerChannels {
        rx_parsed_events,
        tx_commit,
        notify_for_services,
    }
}

pub fn test_from_raw_transactions(
    pg_pool: PgPool,
    any_extractable: Vec<AnyExtractable>,
) -> BufferedConsumerChannels {
    let (tx_parsed_events, rx_parsed_events) = futures::channel::mpsc::channel(1);
    let (tx_commit, rx_commit) = futures::channel::mpsc::channel(1);
    let notify_for_services = Arc::new(Notify::new());

    let (functions, events) = split_any_extractable(any_extractable);

    let parser = TransactionParser::builder()
        .function_in_list(functions.clone(), false)
        .functions_out_list(functions, false)
        .events_list(events)
        .build()
        .unwrap();

    let timestamp_last_block = Arc::new(RwLock::new(i32::MAX));
    let time = Arc::new(RwLock::new(0));
    {
        let time = time.clone();
        tokio::spawn(timer(time));
    }
    tokio::spawn(parse_raw_transaction(
        parser,
        tx_parsed_events,
        notify_for_services.clone(),
        pg_pool,
        rx_commit,
        RawCache::new(),
        timestamp_last_block,
        time,
        2,
    ));
    BufferedConsumerChannels {
        rx_parsed_events,
        tx_commit,
        notify_for_services,
    }
}

async fn timer(time: Arc<RwLock<i32>>) {
    loop {
        *time.write().await += 1;
        sleep(Duration::from_secs(1)).await;
    }
}

pub fn create_rocksdb(path: &str) -> PersistentStorage {
    let config = PersistentStorageConfig {
        persistent_db_path: path.parse().expect("wrong rocksdb path"),
        persistent_db_options: Default::default(),
    };
    
    PersistentStorage::new(&config).expect("cant create rocksdb")
}

pub fn insert_raw_transaction_to_rocksdb(
    transaction: Transaction,
    db: &WeeDb,
) -> Result<(), weedb::Error> {
    let key = (transaction.now, transaction.lt);
    let value = transaction;
    db.put(key, value)
}

#[allow(clippy::too_many_arguments)]
async fn parse_kafka_transactions(
    config: BufferedConsumerConfig,
    tx_parsed_events: Sender<Vec<(Vec<ExtractedOwned>, RawTransaction)>>,
    notify_for_services: Arc<Notify>,
    commit_rx: Receiver<()>,
) {
    create_table_raw_transactions(&config.pg_pool).await;
    let (functions, events) = split_any_extractable(config.any_extractable.clone());

    let parser = TransactionParser::builder()
        .function_in_list(functions.clone(), false)
        .functions_out_list(functions, false)
        .events_list(events)
        .build()
        .unwrap();

    let raw_cache = RawCache::new();

    let time = Arc::new(RwLock::new(0));
    {
        let time = time.clone();
        tokio::spawn(timer(time));
    }

    let stream_from = match get_count_raw_transactions(&config.pg_pool).await == 0 {
        true => StreamFrom::Beginning,
        false => StreamFrom::Stored,
    };

    let (mut stream_transactions, offsets) = config
        .transaction_consumer
        .stream_until_highest_offsets(stream_from)
        .await
        .expect("cant get highest offsets stream transactions");

    let timestamp_last_block = Arc::new(RwLock::new(0_i32));

    let mut count = 0;
    let mut raw_transactions = vec![];
    while let Some(produced_transaction) = stream_transactions.next().await {
        count += 1;
        let transaction: Transaction = produced_transaction.transaction.clone();
        let transaction_time = transaction.now() as i64;
        *timestamp_last_block.write().await = transaction_time as i32;

        if buff_extracted_events(&transaction, &parser).is_some() {
            raw_transactions.push(transaction.into());
        }

        if count >= config.buff_size || *time.read().await >= config.commit_time_secs {
            if !raw_transactions.is_empty() {
                insert_raw_transactions(&mut raw_transactions, &config.pg_pool)
                    .await
                    .expect("cant insert raw_transactions: rip db");
            }

            if let Err(e) = produced_transaction.commit() {
                log::error!("cant commit kafka, stream is down. ERROR {}", e);
            }

            log::info!(
                "COMMIT KAFKA {} transactions timestamp_block {} date: {}",
                count,
                transaction_time,
                NaiveDateTime::from_timestamp_opt(transaction_time, 0).unwrap()
            );
            count = 0;
            *time.write().await = 0;
        }
    }

    if !raw_transactions.is_empty() {
        insert_raw_transactions(&mut raw_transactions, &config.pg_pool)
            .await
            .expect("cant insert raw_transaction: rip db");
    }

    log::info!("kafka synced");

    {
        let pg_pool = config.pg_pool.clone();
        let parser = parser.clone();
        let raw_cache = raw_cache.clone();
        let timestamp_last_block = timestamp_last_block.clone();
        let timer = time.clone();
        tokio::spawn(parse_raw_transaction(
            parser,
            tx_parsed_events,
            notify_for_services,
            pg_pool,
            commit_rx,
            raw_cache,
            timestamp_last_block,
            timer,
            config.cache_timer,
        ));
    }

    let mut i = 0;
    let mut stream_transactions = config
        .transaction_consumer
        .stream_transactions(StreamFrom::Offsets(offsets))
        .await
        .expect("cant get stream transactions");

    while let Some(produced_transaction) = stream_transactions.next().await {
        i += 1;
        let transaction: Transaction = produced_transaction.transaction.clone();
        let transaction_timestamp = transaction.now;

        if buff_extracted_events(&transaction, &parser).is_some() {
            insert_raw_transaction(transaction.clone().into(), &config.pg_pool)
                .await
                .expect("cant insert raw_transaction to db");
            raw_cache.insert_raw(transaction.into()).await;
        }

        *timestamp_last_block.write().await = transaction_timestamp as i32;
        *time.write().await = 0;

        produced_transaction.commit().expect("dead stream kafka");

        if i >= 5_000 {
            log::info!(
                "KAFKA 5_000 transactions timestamp_block {} date: {}",
                transaction_timestamp,
                NaiveDateTime::from_timestamp_opt(transaction_timestamp as i64, 0).unwrap()
            );
            i = 0;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn parse_raw_transaction(
    parser: TransactionParser,
    mut tx: Sender<Vec<(Vec<ExtractedOwned>, RawTransaction)>>,
    notify: Arc<Notify>,
    pg_pool: PgPool,
    mut commit_rx: Receiver<()>,
    raw_cache: RawCache,
    timestamp_last_block: Arc<RwLock<i32>>,
    timer: Arc<RwLock<i32>>,
    cache_timer: i32,
) {
    let count_not_processed = get_count_not_processed_raw_transactions(&pg_pool).await;
    let mut i: i64 = 0;

    loop {
        let mut begin = pg_pool.begin().await.expect("cant get pg transaction");

        let raw_transactions_from_db =
            get_raw_transactions(1000, *timestamp_last_block.read().await, &mut begin)
                .await
                .unwrap_or_default();

        if raw_transactions_from_db.is_empty() {
            notify.notify_one();
            break;
        }

        let mut send_message = vec![];
        for raw_transaction_from_db in raw_transactions_from_db {
            i += 1;

            let raw_transaction = match RawTransaction::try_from(raw_transaction_from_db) {
                Ok(ok) => ok,
                Err(e) => {
                    log::error!("{}", e);
                    continue;
                }
            };

            if let Some(events) = buff_extracted_events(&raw_transaction.data, &parser) {
                send_message.push((events, raw_transaction));
            };
        }
        if !send_message.is_empty() {
            tx.send(send_message).await.expect("dead sender");
            commit_rx.next().await;
        }
        begin.commit().await.expect("cant commit db update");

        if i >= count_not_processed {
            log::info!("end parse. synced");
            notify.notify_one();
            break;
        } else {
            log::info!("parsing {}/{}", i, count_not_processed);
        }
    }

    raw_cache.fill_raws(&pg_pool).await;

    loop {
        let (raw_transactions, times) = raw_cache
            .get_raws(
                *timestamp_last_block.read().await,
                timer.clone(),
                cache_timer,
            )
            .await;

        if raw_transactions.is_empty() {
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let mut send_message = vec![];
        for raw_transaction in raw_transactions {
            if let Some(events) = buff_extracted_events(&raw_transaction.data, &parser) {
                send_message.push((events, raw_transaction));
            };
        }

        if !send_message.is_empty() {
            tx.send(send_message).await.expect("dead sender");
            commit_rx.next().await;
        }

        update_raw_transactions_set_processed_true(&pg_pool, times).await;
    }
}

pub fn extract_events(
    data: &Transaction,
    parser: &TransactionParser,
) -> Option<Vec<ExtractedOwned>> {
    // let parser = TransactionParser::builder()
    //     .function_in_list(functions.clone(), false)
    //     .functions_out_list(functions, false)
    //     .events_list(events)
    //     .build()
    //     .unwrap();

    if let Ok(extracted) = parser.parse(data) {
        if !extracted.is_empty() {
            return filter_extracted(extracted, data.clone());
        }
    }
    None
}

pub fn buff_extracted_events(
    data: &Transaction,
    parser: &TransactionParser,
) -> Option<Vec<ExtractedOwned>> {
    if let Ok(extracted) = parser.parse(data) {
        if !extracted.is_empty() {
            return filter_extracted(extracted, data.clone());
        }
    }
    None
}

pub fn filter_extracted(
    mut extracted: Vec<Extracted>,
    tx: Transaction,
) -> Option<Vec<ExtractedOwned>> {
    if extracted.is_empty() {
        return None;
    }

    if let Ok(true) = tx.read_description().map(|x| {
        if !x.is_aborted() {
            true
        } else {
            x.compute_phase_ref()
                .map(|x| match x {
                    TrComputePhase::Vm(x) => x.exit_code == 60,
                    TrComputePhase::Skipped(_) => false,
                })
                .unwrap_or(false)
        }
    }) {
    } else {
        return None;
    }

    #[allow(clippy::nonminimal_bool)]
    extracted.retain(|x| !(x.parsed_type != ParsedType::Event && !x.is_in_message));

    if extracted.is_empty() {
        return None;
    }
    Some(extracted.into_iter().map(|x| x.into_owned()).collect())
}

#[cfg(test)]
mod test {
    use crate::{extract_events, filter_extracted};
    use nekoton_abi::TransactionParser;
    use ton_block::{Deserializable, GetRepresentationHash};

    #[test]
    fn test_empty() {
        let tx = "te6ccgECCQEAAgMAA7d3xF4oX2oWEwg5M5aR8xwgw6PQR7NiQn3ingdzt6NLnlAAAYzbaqqQGGge6YdqXzRGzEt95+94zU1ZfCjSJTBeSEvV1TdfJArAAAGM2xx3CCYpu36QADSAISBBqAUEAQIZBHtJDuaygBiAIH1iEQMCAG/Jh6EgTBRYQAAAAAAABAACAAAAAlv8RbySHn7Zsc6jPU5HH77NJiEltjMklhaKitL0Hn58QFAWDACeSFFMPQkAAAAAAAAAAAFJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCctRvCOj0IytqL1T55yj5A4CiFWjMA24f39CflxqMd3B4ruUkaHcmjwepyx+Zq9yebin0HUaTR+nX46leelD4ZXUCAeAIBgEB3wcAsWgA+IvFC+1CwmEHJnLSPmOEGHR6CPZsSE+8U8DudvRpc8sAOoJ7mWzKNw9aN8zgsFlHCBKE/DKTYv4bm/dZb5x149AQ6h5ywAYUWGAAADGbbVVSBMU3b9JAALloAdQT3MtmUbh60b5nBYLKOECUJ+GUmxfw3N+6y3zjrx6BAB8ReKF9qFhMIOTOWkfMcIMOj0EezYkJ94p4Hc7ejS55UO5rKAAGFFhgAAAxm2z5xITFN2/CP/urf0A=";
        let tx = ton_block::Transaction::construct_from_base64(&tx).unwrap();
        let abi = r#"{"ABI version":2,"version":"2.2","header":["pubkey","time"],"functions":[{"name":"constructor","inputs":[],"outputs":[]},{"name":"acceptUpgrade","inputs":[{"name":"code","type":"cell"},{"name":"newVersion","type":"uint8"}],"outputs":[]},{"name":"receiveTokenDecimals","inputs":[{"name":"decimals","type":"uint8"}],"outputs":[]},{"name":"setManager","inputs":[{"name":"_manager","type":"address"}],"outputs":[]},{"name":"removeToken","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"addToken","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"setCanon","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"enableToken","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"enableAll","inputs":[],"outputs":[]},{"name":"disableToken","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"disableAll","inputs":[],"outputs":[]},{"name":"getCanon","inputs":[{"name":"answerId","type":"uint32"}],"outputs":[{"name":"value0","type":"address"},{"components":[{"name":"decimals","type":"uint8"},{"name":"enabled","type":"bool"}],"name":"value1","type":"tuple"}]},{"name":"getTokens","inputs":[{"name":"answerId","type":"uint32"}],"outputs":[{"components":[{"name":"decimals","type":"uint8"},{"name":"enabled","type":"bool"}],"name":"_tokens","type":"map(address,tuple)"},{"name":"_canon","type":"address"}]},{"name":"onAcceptTokensBurn","inputs":[{"name":"_amount","type":"uint128"},{"name":"walletOwner","type":"address"},{"name":"value2","type":"address"},{"name":"remainingGasTo","type":"address"},{"name":"payload","type":"cell"}],"outputs":[]},{"name":"transferOwnership","inputs":[{"name":"newOwner","type":"address"}],"outputs":[]},{"name":"renounceOwnership","inputs":[],"outputs":[]},{"name":"owner","inputs":[],"outputs":[{"name":"owner","type":"address"}]},{"name":"_randomNonce","inputs":[],"outputs":[{"name":"_randomNonce","type":"uint256"}]},{"name":"version","inputs":[],"outputs":[{"name":"version","type":"uint8"}]},{"name":"manager","inputs":[],"outputs":[{"name":"manager","type":"address"}]}],"data":[{"key":1,"name":"_randomNonce","type":"uint256"},{"key":2,"name":"proxy","type":"address"}],"events":[{"name":"OwnershipTransferred","inputs":[{"name":"previousOwner","type":"address"},{"name":"newOwner","type":"address"}],"outputs":[]}],"fields":[{"name":"_pubkey","type":"uint256"},{"name":"_timestamp","type":"uint64"},{"name":"_constructorFlag","type":"bool"},{"name":"owner","type":"address"},{"name":"_randomNonce","type":"uint256"},{"name":"proxy","type":"address"},{"name":"version","type":"uint8"},{"components":[{"name":"decimals","type":"uint8"},{"name":"enabled","type":"bool"}],"name":"tokens","type":"map(address,tuple)"},{"name":"manager","type":"address"},{"name":"canon","type":"address"}]}"#;

        let abi = ton_abi::Contract::load(abi).unwrap();

        let funs = abi.functions.values().cloned().collect::<Vec<_>>();
        let parser = TransactionParser::builder()
            .function_in_list(&funs, false)
            .functions_out_list(&funs, false)
            .build()
            .unwrap();
        let test = parser.parse(&tx).unwrap();
        let test1 = filter_extracted(test, tx.hash().unwrap(), tx.clone()).unwrap();
        dbg!(test1);
    }
}
