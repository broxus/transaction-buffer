mod cache;
pub mod drop_base;
pub mod models;
mod sqlx_client;
mod storage;

use crate::cache::RawCache;
use crate::models::{AnyExtractable, BufferedConsumerChannels, BufferedConsumerConfig};
use crate::storage::{PersistentStorage, PersistentStorageConfig};
use chrono::NaiveDateTime;
use futures::channel::mpsc::{Receiver, Sender};
use futures::SinkExt;
use futures::StreamExt;
use itertools::Itertools;
use nekoton_abi::transaction_parser::{Extracted, ExtractedOwned, ParsedType};
use nekoton_abi::TransactionParser;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};
use tokio::time::sleep;
use ton_block::{TrComputePhase, Transaction};
use transaction_consumer::StreamFrom;

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
    rocksdb_path: &str,
    any_extractable: Vec<AnyExtractable>,
) -> BufferedConsumerChannels {
    let rocksdb = Arc::new(create_rocksdb(rocksdb_path));
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

    {
        let rocksdb = rocksdb.clone();
        tokio::spawn(commit_transactions(rx_commit, rocksdb));
    }

    tokio::spawn(parse_raw_transaction(
        parser,
        tx_parsed_events,
        notify_for_services.clone(),
        RawCache::new(),
        timestamp_last_block,
        time,
        2,
        rocksdb,
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

async fn commit_transactions(
    mut commit_rx: Receiver<Vec<Transaction>>,
    rocksdb: Arc<PersistentStorage>,
) {
    while let Some(transactions) = commit_rx.next().await {
        for transaction in transactions {
            rocksdb.update_transaction_processed(transaction)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn parse_kafka_transactions(
    config: BufferedConsumerConfig,
    tx_parsed_events: Sender<Vec<(Vec<ExtractedOwned>, Transaction)>>,
    notify_for_services: Arc<Notify>,
    commit_rx: Receiver<Vec<Transaction>>,
) {
    let rocksdb = Arc::new(create_rocksdb(&config.rocksdb_path));
    let raw_cache = RawCache::new();
    let time = Arc::new(RwLock::new(0));

    {
        let rocksdb = rocksdb.clone();
        tokio::spawn(commit_transactions(commit_rx, rocksdb));
    }

    let (functions, events) = split_any_extractable(config.any_extractable.clone());

    let parser = TransactionParser::builder()
        .function_in_list(functions.clone(), false)
        .functions_out_list(functions, false)
        .events_list(events)
        .build()
        .unwrap();

    {
        let time = time.clone();
        tokio::spawn(timer(time));
    }

    let stream_from = match rocksdb.check_drop_base_index(config.rocksdb_drop_base_index) {
        false => StreamFrom::Beginning,
        true => StreamFrom::Stored,
    };

    let (mut stream_transactions, offsets) = config
        .transaction_consumer
        .stream_until_highest_offsets(stream_from)
        .await
        .expect("cant get highest offsets stream transactions");

    let timestamp_last_block = Arc::new(RwLock::new(0_i32));

    let mut count = 0;
    let mut transactions = vec![];
    while let Some(produced_transaction) = stream_transactions.next().await {
        count += 1;
        let transaction: Transaction = produced_transaction.transaction.clone();
        let transaction_time = transaction.now() as i64;
        *timestamp_last_block.write().await = transaction_time as i32;

        if buff_extracted_events(&transaction, &parser).is_some() {
            transactions.push(transaction);
        }

        if count >= config.buff_size || *time.read().await >= config.commit_time_secs {
            rocksdb.insert_transactions_with_drain(&mut transactions);
            if let Err(e) = produced_transaction.commit() {
                log::error!("cant commit kafka, stream is down. ERROR {}", e);
                panic!("cant commit kafka, stream is down. ERROR {}", e)
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

    rocksdb.insert_transactions_with_drain(&mut transactions);

    log::info!("kafka synced");

    {
        let parser = parser.clone();
        let raw_cache = raw_cache.clone();
        let timestamp_last_block = timestamp_last_block.clone();
        let timer = time.clone();
        let rocksdb = rocksdb.clone();

        tokio::spawn(parse_raw_transaction(
            parser,
            tx_parsed_events,
            notify_for_services,
            raw_cache,
            timestamp_last_block,
            timer,
            config.cache_timer,
            rocksdb,
        ));
    }

    let mut stream_transactions = config
        .transaction_consumer
        .stream_transactions(StreamFrom::Offsets(offsets))
        .await
        .expect("cant get stream transactions");

    let mut i = 0;
    while let Some(produced_transaction) = stream_transactions.next().await {
        i += 1;
        let transaction: Transaction = produced_transaction.transaction.clone();
        let transaction_timestamp = transaction.now;

        if buff_extracted_events(&transaction, &parser).is_some() {
            rocksdb.insert_transaction(&transaction);
            raw_cache.insert_raw(transaction).await;
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
    mut tx: Sender<Vec<(Vec<ExtractedOwned>, Transaction)>>,
    notify: Arc<Notify>,
    raw_cache: RawCache,
    timestamp_last_block: Arc<RwLock<i32>>,
    timer: Arc<RwLock<i32>>,
    cache_timer: i32,
    rocksdb: Arc<PersistentStorage>,
) {
    let count_not_processed = rocksdb.count_not_processed_transactions();
    let transactions_iter = rocksdb.iterate_unprocessed_transactions();

    let mut i: i64 = 0;
    for transaction in transactions_iter {
        i += 1;

        tx.send(vec![(
            buff_extracted_events(&transaction, &parser).unwrap_or_default(),
            transaction,
        )])
        .await
        .expect("dead sender");

        if i % 10_000 == 0 {
            log::info!("parsing {}/{}", i, count_not_processed);
        }
    }

    notify.notify_one();

    raw_cache.fill_raws(&rocksdb).await;

    loop {
        let transactions = raw_cache
            .get_raws(*timestamp_last_block.read().await, &timer, cache_timer)
            .await;

        if transactions.is_empty() {
            sleep(Duration::from_secs(1)).await;
            continue;
        }

        let send_message = transactions
            .into_iter()
            .map(|transaction| {
                (
                    buff_extracted_events(&transaction, &parser).unwrap_or_default(),
                    transaction,
                )
            })
            .collect_vec();

        tx.send(send_message).await.expect("dead sender");
    }
}

pub fn extract_events(
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
    use crate::{create_rocksdb, extract_events, filter_extracted};
    use chrono::Utc;
    use nekoton_abi::TransactionParser;
    use std::sync::Arc;
    use ton_block::{Deserializable, GetRepresentationHash};

    // #[test]
    // fn test_empty() {
    //     let tx = "te6ccgECCQEAAgMAA7d3xF4oX2oWEwg5M5aR8xwgw6PQR7NiQn3ingdzt6NLnlAAAYzbaqqQGGge6YdqXzRGzEt95+94zU1ZfCjSJTBeSEvV1TdfJArAAAGM2xx3CCYpu36QADSAISBBqAUEAQIZBHtJDuaygBiAIH1iEQMCAG/Jh6EgTBRYQAAAAAAABAACAAAAAlv8RbySHn7Zsc6jPU5HH77NJiEltjMklhaKitL0Hn58QFAWDACeSFFMPQkAAAAAAAAAAAFJAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCctRvCOj0IytqL1T55yj5A4CiFWjMA24f39CflxqMd3B4ruUkaHcmjwepyx+Zq9yebin0HUaTR+nX46leelD4ZXUCAeAIBgEB3wcAsWgA+IvFC+1CwmEHJnLSPmOEGHR6CPZsSE+8U8DudvRpc8sAOoJ7mWzKNw9aN8zgsFlHCBKE/DKTYv4bm/dZb5x149AQ6h5ywAYUWGAAADGbbVVSBMU3b9JAALloAdQT3MtmUbh60b5nBYLKOECUJ+GUmxfw3N+6y3zjrx6BAB8ReKF9qFhMIOTOWkfMcIMOj0EezYkJ94p4Hc7ejS55UO5rKAAGFFhgAAAxm2z5xITFN2/CP/urf0A=";
    //     let tx = ton_block::Transaction::construct_from_base64(&tx).unwrap();
    //     let abi = r#"{"ABI version":2,"version":"2.2","header":["pubkey","time"],"functions":[{"name":"constructor","inputs":[],"outputs":[]},{"name":"acceptUpgrade","inputs":[{"name":"code","type":"cell"},{"name":"newVersion","type":"uint8"}],"outputs":[]},{"name":"receiveTokenDecimals","inputs":[{"name":"decimals","type":"uint8"}],"outputs":[]},{"name":"setManager","inputs":[{"name":"_manager","type":"address"}],"outputs":[]},{"name":"removeToken","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"addToken","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"setCanon","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"enableToken","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"enableAll","inputs":[],"outputs":[]},{"name":"disableToken","inputs":[{"name":"token","type":"address"}],"outputs":[]},{"name":"disableAll","inputs":[],"outputs":[]},{"name":"getCanon","inputs":[{"name":"answerId","type":"uint32"}],"outputs":[{"name":"value0","type":"address"},{"components":[{"name":"decimals","type":"uint8"},{"name":"enabled","type":"bool"}],"name":"value1","type":"tuple"}]},{"name":"getTokens","inputs":[{"name":"answerId","type":"uint32"}],"outputs":[{"components":[{"name":"decimals","type":"uint8"},{"name":"enabled","type":"bool"}],"name":"_tokens","type":"map(address,tuple)"},{"name":"_canon","type":"address"}]},{"name":"onAcceptTokensBurn","inputs":[{"name":"_amount","type":"uint128"},{"name":"walletOwner","type":"address"},{"name":"value2","type":"address"},{"name":"remainingGasTo","type":"address"},{"name":"payload","type":"cell"}],"outputs":[]},{"name":"transferOwnership","inputs":[{"name":"newOwner","type":"address"}],"outputs":[]},{"name":"renounceOwnership","inputs":[],"outputs":[]},{"name":"owner","inputs":[],"outputs":[{"name":"owner","type":"address"}]},{"name":"_randomNonce","inputs":[],"outputs":[{"name":"_randomNonce","type":"uint256"}]},{"name":"version","inputs":[],"outputs":[{"name":"version","type":"uint8"}]},{"name":"manager","inputs":[],"outputs":[{"name":"manager","type":"address"}]}],"data":[{"key":1,"name":"_randomNonce","type":"uint256"},{"key":2,"name":"proxy","type":"address"}],"events":[{"name":"OwnershipTransferred","inputs":[{"name":"previousOwner","type":"address"},{"name":"newOwner","type":"address"}],"outputs":[]}],"fields":[{"name":"_pubkey","type":"uint256"},{"name":"_timestamp","type":"uint64"},{"name":"_constructorFlag","type":"bool"},{"name":"owner","type":"address"},{"name":"_randomNonce","type":"uint256"},{"name":"proxy","type":"address"},{"name":"version","type":"uint8"},{"components":[{"name":"decimals","type":"uint8"},{"name":"enabled","type":"bool"}],"name":"tokens","type":"map(address,tuple)"},{"name":"manager","type":"address"},{"name":"canon","type":"address"}]}"#;
    //
    //     let abi = ton_abi::Contract::load(abi).unwrap();
    //
    //     let funs = abi.functions.values().cloned().collect::<Vec<_>>();
    //     let parser = TransactionParser::builder()
    //         .function_in_list(&funs, false)
    //         .functions_out_list(&funs, false)
    //         .build()
    //         .unwrap();
    //     let test = parser.parse(&tx).unwrap();
    //     let test1 = filter_extracted(test, tx.clone()).unwrap();
    //     dbg!(test1);
    // }

    #[tokio::test]
    async fn local_test() {
        let mut transactions = vec![];
        let tx_1633727461_19509853000001 = "te6ccgECCQEAAg4AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn3bkUEHE2RFMaxmGCqXux36pcE9N7+Jg1kxeQWe/n44lh2HYwAAEb59cMGBYWCz5QADSANoeJaAUEAQIZBGDJDe1mMBiANJ8xEQMCAG/Jh6EgTD0JAAAAAAAAAgAAAAAAAwu+Ss8rtVvUqnu/kPfiN6aoISSENKFpCmpLlo4NR6C8QFAX5ACeTXisOQvgAAAAAAAAAAGXAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCcshMEB1nTna+yJhfgnCtml3/fBkOUWgyUrx2xSrVL5ND2vdSOtvkOTEsSD5Rzec6Y8UePq9NoMVb2Hav+QBB4KACAeAIBgEB3wcAv+ADuT5QnCz3HAxJdzUCGosumwKYxEgCqGbYFcn1NeQOA6gAACN8+7cihMLBZ8oWNmsvgAAAAbCwWfKwsatywALZYvCjWYzeGLgWFLOIJcpGujmwK7+Or1U9tc+JQK2cCADBaABbLF4UazGbwxcCwpZxBLlI10c2BXfx1eqntrnxKBWzgQAdyfKE4We44GJLuagQ1Fl02BTGIkAVQzbArk+pryBwHVDe1mMABhRYYAAAI3z7W5UEwsFnuDE93M+AAAABwA==";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727461_19509853000001).unwrap();
        transactions.push(tx);

        let tx_1633727472_19509858000003 = "te6ccgECCgEAAk8AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn4n3IMTgKEyC61j0tftU8v6Vgvj7VlXnVTnzE/BgOfi9RKXVgAAEb5925FBYWCz8AADSAKGzVCAUEAQIbBFQJQJT9osoYgCfKCREDAgBvyYehIEwUWEAAAAAAAAQAAgAAAAN5LIuWZs30moOznxFIdV1huIpx2o2/0nHt1S46YntCoEBQH0wAnkovrD0JAAAAAAAAAAABMQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgnLa91I62+Q5MSxIPlHN5zpjxR4+r02gxVvYdq/5AEHgoHEWRFEnlriY3bDJFR8wLgyE0RDXNbAcm1XFxhVmlnfhAgHgCAYBAd8HAPtoAO5PlCcLPccDEl3NQIaiy6bApjESAKoZtgVyfU15A4DrAB0Q6MuaXXxqfenbyZapD5N5cc2T9zJ7mTthKTTKLUy3FAlKo+KABhRYYAAAI3z8T7kIwsFn4BbeDvsAAAABgCjegAAAAAAAAAAAAAAteYg9IAAAAAAAgAAAAMABs2gA6IdGXNLr41PvTt5MtUh8m8uObJ+5k9zJ2wlJplFqZbkAHcnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1UCU/aLKAGHHaEAAAjfPxPuQTCwWfgwAkASwHyiPGAE/XkTnXGH34UZVQbk9sG0gQffBscbfOmWTeakvvEfUxQ";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727472_19509858000003).unwrap();
        transactions.push(tx);

        let tx_1633727479_19509861000001 = "te6ccgECCgEAAk8AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn5Vo0GFSk3J6VaPZGutffA4OD9t2B3ovGEtwXK6deXIZ8xLhQAAEb5+J9yDYWCz9wADSAKGzRaAUEAQIbBEzJQJT9osoYgCfKCREDAgBvyYehIEwUWEAAAAAAAAQAAgAAAAIu8tRgneg1Rmq1NM3QjzfWBXdZ34bFUrTbpx2WBVAxEEBQH0wAnkovrD0JAAAAAAAAAAABMQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgnJxFkRRJ5a4mN2wyRUfMC4MhNEQ1zWwHJtVxcYVZpZ34ZzQAqT3Zze2quYBoUl4E+S+D45EFdT5KMwhe/VU/WnNAgHgCAYBAd8HAPtoAO5PlCcLPccDEl3NQIaiy6bApjESAKoZtgVyfU15A4DrAARC1XVeQTbQVAC1OgdG8pVjUv5TW0wggQoHXrHFQtEaFAlKo+KABhRYYAAAI3z8q0aEwsFn7hbeDvsAAAABgCjegAAAAAAAAAAAAAAteYg9IAAAAAAAgAAAAMABs2gAIharqvIJtoKgBanQOjeUqxqX8praYQQIUDr1jioWiNEAHcnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1UCU/aLKAGHHaEAAAjfPwxNITCwWfewAkASwHyiPGAD1LgN9vP8Nfgc1dEAYN2hDRxF3aWCwF/XV9M3yEaVkjQ";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727479_19509861000001).unwrap();
        transactions.push(tx);

        let tx_1633727484_19509863000003 = "te6ccgECCgEAAk4AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn50J8Oujed9pPwnJ3SFnP/4tIYddkrJ0HLjDvfkhLJBFYZXZQAAEb5+dCfBYWCz/AADSAKGzLCAUEAQIZBAlAlP2iyhiAJ8oJEQMCAG/Jh6EgTBRYQAAAAAAABAACAAAAApAsUmMxadVpULBEuM+/us1LgHZ0GA5Coh/6cuxb4jOYQFAfTACeSi+sPQkAAAAAAAAAAAExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCcsGw/gDlOnYgmA6QfkyC5mF8Pf069qtFieh+D6TfJab+dM881d43QzfI/kTa2nMoV9Q0KnYsUcjyT2MvY/SW1mMCAeAIBgEB3wcA+2gA7k+UJws9xwMSXc1AhqLLpsCmMRIAqhm2BXJ9TXkDgOsAKyVCJENFkIo9iaG2UKqX8yM0cbAOI+StrnOLxW3WbibUCUqj4oAGFFhgAAAjfPzoT4jCwWf4Ft4O+wAAAAGAKN6AAAAAAAAAAAAAAC15iD0gAAAAAACAAAAAwAGzaAFZKhEiGiyEUexNDbKFVL+ZGaONgHEfJW1znF4rbrNxNwAdyfKE4We44GJLuagQ1Fl02BTGIkAVQzbArk+pryBwHVQJT9osoAYcdoQAACN8/E+5BMLBZ+LACQBLAfKI8YAVZrnkt8rcYXK8FcDhDJTEFK5f7G4t089MK01WrFl8a1A=";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727484_19509863000003).unwrap();
        transactions.push(tx);

        let tx_1633727484_19509863000001 = "te6ccgECCgEAAk8AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn50J8GeA3M0lJfVf6WtwjGDUmXgdHnpfdff88LaNeLPGjk+8AAAEb5+VaNDYWCz/AADSAKGzPqAUEAQIbBElJQJT9osoYgCfKCREDAgBvyYehIEwUWEAAAAAAAAQAAgAAAANxUtP2HNjh9y5k7HlbE47cD6IbIvybBDzxqzspI72NtkBQH0wAnkovrD0JAAAAAAAAAAABMQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgnLzhdt1fHYLMBLqtjeWXBg4flNHR48yURuFSBGGKVo24sGw/gDlOnYgmA6QfkyC5mF8Pf069qtFieh+D6TfJab+AgHgCAYBAd8HAPtoAO5PlCcLPccDEl3NQIaiy6bApjESAKoZtgVyfU15A4DrAD0vpHrslfMd6lzt6vJz52aOXgvK5FRXq7zkXg7CZHCa1AlKo+KABhRYYAAAI3z86E+EwsFn+BbeDvsAAAABgCjegAAAAAAAAAAAAAAteYg9IAAAAAAAgAAAAMABs2gB6X0j12SvmO9S529Xk587NHLwXlcior1d5yLwdhMjhNcAHcnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1UCU/aLKAGHHaEAAAjfPxPuQTCwWfiwAkASwHyiPGAB2CuL+0TOTmkSR5J4D65UgywcjVkkd79oCrzrQLymeHQ";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727484_19509863000001).unwrap();
        transactions.push(tx);

        let tx_1633727484_19509863000005 = "te6ccgECCgEAAk4AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn50J8VZBCRXSSwgKv1v1LYHopeCg7HTKTVedfGCnf1fhzCy/wAAEb5+dCfDYWCz/AADSAKGzLCAUEAQIZBAlAlP2iyhiAJ8oJEQMCAG/Jh6EgTBRYQAAAAAAABAACAAAAA4TQ83eryvHmqIvLUXqc2TqdzzyvGXtIzgDD7QOGQsuGQFAfTACeSi+sPQkAAAAAAAAAAAExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCcnTPPNXeN0M3yP5E2tpzKFfUNCp2LFHI8k9jL2P0ltZjZ0RSAFmgKH75tfx6NyqiovpeGj64AHLvIEaEgEUAK0QCAeAIBgEB3wcA+2gA7k+UJws9xwMSXc1AhqLLpsCmMRIAqhm2BXJ9TXkDgOsABaZhbAGWx5at61189UlkqKaXD8cpjJBuCQ7MCYw09WvUCUqj4oAGFFhgAAAjfPzoT4zCwWf4Ft4O+wAAAAGAKN6AAAAAAAAAAAAAAC15iD0gAAAAAACAAAAAwAGzaAAtMwtgDLY8tW9a6+eqSyVFNLh+OUxkg3BIdmBMYaerXwAdyfKE4We44GJLuagQ1Fl02BTGIkAVQzbArk+pryBwHVQJT9osoAYcdoQAACN8/E+5BMLBZ+LACQBLAfKI8YAP8LK14qp/MskjHZ/gjkTB1KlWT1jhb8pJna9Dz3ddNZA=";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727484_19509863000005).unwrap();
        transactions.push(tx);

        let tx_1633727484_19509863000007 = "te6ccgECCgEAAk4AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn50J8cShzNdkHwFUU88U2XZKBdpZBK9jQJPRVMlxPNMFOxJggAAEb5+dCfFYWCz/AADSAKGzLCAUEAQIZBAlAlP2iyhiAJ8oJEQMCAG/Jh6EgTBRYQAAAAAAABAACAAAAAuVjCOcjo3WvXbjVgsCoyWXgwp25hd3wbEesHyBzWb2mQFAfTACeSi+sPQkAAAAAAAAAAAExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCcmdEUgBZoCh++bX8ejcqoqL6Xho+uABy7yBGhIBFACtE+fl7bTw9LjL+mX05uHISsbsMTURTf36piBt9hkcDyfgCAeAIBgEB3wcA+2gA7k+UJws9xwMSXc1AhqLLpsCmMRIAqhm2BXJ9TXkDgOsAPs4qb8MdTJKZhA3qSu6AXYFNQnsE4eQW2PYu9cPbCYpUCUqj4oAGFFhgAAAjfPzoT5DCwWf4Ft4O+wAAAAGAKN6AAAAAAAAAAAAAAC15iD0gAAAAAACAAAAAwAGzaAH2cVN+GOpklMwgb1JXdALsCmoT2CcPILbHsXeuHthMUwAdyfKE4We44GJLuagQ1Fl02BTGIkAVQzbArk+pryBwHVQJT9osoAYcdoQAACN8/E+5BMLBZ+LACQBLAfKI8YAJ1xnpK+TkQnGDcz+PKTFPkHfpKDsM35JxTO49RmFXfxA=";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727484_19509863000007).unwrap();
        transactions.push(tx);

        let tx_1633727487_19509864000001 = "te6ccgECCgEAAk8AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn6DagHNjl1aCbg151rTBDZa3q1hOdw0QpMagmFqieZaocXgXgAAEb5+dCfJYWCz/wADSAKGzNyAUEAQIbBEWJQJT9osoYgCfKCREDAgBvyYehIEwUWEAAAAAAAAQAAgAAAAKQAJji6510IbN3PWh9kGLzKXIEL4DacVkz4UP45Ma4XEBQH0wAnkovrD0JAAAAAAAAAAABMQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgnLbgIcgTa5BUrTze8vClZ0KbeF8bEwTvEFd+o/lZ9ruk/jqgTwmiJmbrCNz/F9R+sU9mvaVwIIG9yl+ja3ss7pIAgHgCAYBAd8HAPtoAO5PlCcLPccDEl3NQIaiy6bApjESAKoZtgVyfU15A4DrADpzm2Y7LeYk2bHvsjUdgRGPRVoRmHPnq2zNLt81wRvFFAlKo+KABhRYYAAAI3z9BtQEwsFn/hbeDvsAAAABgCjegAAAAAAAAAAAAAAteYg9IAAAAAAAgAAAAMABs2gB05zbMdlvMSbNj32RqOwIjHoq0IzDnz1bZml2+a4I3ikAHcnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1UCU/aLKAGHHaEAAAjfPxuPYTCwWfmwAkASwHyiPGADz+RnRpkQjSYk1vvvDk4H00F/nQmJJ/gT+1yX1RhH/1w";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727487_19509864000001).unwrap();
        transactions.push(tx);

        let tx_1633727484_19509863000009 = "te6ccgECCgEAAk4AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn50J8mSmDJWZewgMBW18BSRtbSdkWffK29hcG+8HEsplYtzSQAAEb5+dCfHYWCz/AADSAKGzLCAUEAQIZBAlAlP2iyhiAJ8oJEQMCAG/Jh6EgTBRYQAAAAAAABAACAAAAA3gKmvjShCOTOsKxmp8HUIqmdrAKjvKd6gzy7fjIJt+KQFAfTACeSi+sPQkAAAAAAAAAAAExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCcvn5e208PS4y/pl9ObhyErG7DE1EU39+qYgbfYZHA8n424CHIE2uQVK083vLwpWdCm3hfGxME7xBXfqP5Wfa7pMCAeAIBgEB3wcA+2gA7k+UJws9xwMSXc1AhqLLpsCmMRIAqhm2BXJ9TXkDgOsANL2T9VP3eJZO33OEQ4S0x5PRuAViOOn//6inbRzI4tgUCUqj4oAGFFhgAAAjfPzoT5TCwWf4Ft4O+wAAAAGAKN6AAAAAAAAAAAAAAC15iD0gAAAAAACAAAAAwAGzaAGl7J+qn7vEsnb7nCIcJaY8no3AKxHHT//9RTto5kcWwQAdyfKE4We44GJLuagQ1Fl02BTGIkAVQzbArk+pryBwHVQJT9osoAYcdoQAACN8/G49hMLBZ+bACQBLAfKI8YAfb7H66aVZPpLjOtUKS0wsThrnzFlOQs2xP/Icd0qO2FA=";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727484_19509863000009).unwrap();
        transactions.push(tx);

        let tx_1633727487_19509864000003 = "te6ccgECCgEAAk4AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn6DagNUZTyPwefzTf9hW9MEjXyFjznjEF0FUrt0IsZRkVh4XwAAEb5+g2oBYWCz/wADSAKGzLCAUEAQIZBAlAlP2iyhiAJ8oJEQMCAG/Jh6EgTBRYQAAAAAAABAACAAAAArrfsmsAkl7AFnulxe1rXrgC4r24vbvMSd+2kY7+IlL0QFAfTACeSi+sPQkAAAAAAAAAAAExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCcvjqgTwmiJmbrCNz/F9R+sU9mvaVwIIG9yl+ja3ss7pI8pGixftmdUSpH7aTbQv9FvfmrUUyfwHiMm5AaNkGIvECAeAIBgEB3wcA+2gA7k+UJws9xwMSXc1AhqLLpsCmMRIAqhm2BXJ9TXkDgOsAO8bmn6oifAaJ2zn0VuTLaOktWmdIiea1QBuG6JmWjxZUCUqj4oAGFFhgAAAjfP0G1AjCwWf+Ft4O+wAAAAGAKN6AAAAAAAAAAAAAAC15iD0gAAAAAACAAAAAwAGzaAHeNzT9URPgNE7Zz6K3JltHSWrTOkRPNaoA3DdEzLR4swAdyfKE4We44GJLuagQ1Fl02BTGIkAVQzbArk+pryBwHVQJT9osoAYcdoQAACN8/G49hMLBZ+bACQBLAfKI8YASV91H9Od1P0SECIXXE4+C37cTPTqYIfCcaeJ+yjfWS/A=";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727487_19509864000003).unwrap();
        transactions.push(tx);

        let tx_1633727487_19509864000005 = "te6ccgECCgEAAk4AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn6DagUbdffSf89hBa1GexE81mLOKF9myf93BN/RPoQqx5Xr2gAAEb5+g2oDYWCz/wADSAKGzLCAUEAQIZBAlAlP2iyhiAJ8oJEQMCAG/Jh6EgTBRYQAAAAAAABAACAAAAAvHb1yUS+o0j1TZJ//orKwBTNMIkdWLNr/Mry100VltmQFAfTACeSi+sPQkAAAAAAAAAAAExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCcvKRosX7ZnVEqR+2k20L/Rb35q1FMn8B4jJuQGjZBiLxVehAVvn+rBXcbxWgkDXv/0Og3OuDRHQPeox1SF6eztICAeAIBgEB3wcA+2gA7k+UJws9xwMSXc1AhqLLpsCmMRIAqhm2BXJ9TXkDgOsAErOO7jLMgxq1ErDxjuh/+es18Gxl9DRMHfBMV7UmS1DUCUqj4oAGFFhgAAAjfP0G1AzCwWf+Ft4O+wAAAAGAKN6AAAAAAAAAAAAAAC15iD0gAAAAAACAAAAAwAGzaACVnHdxlmQY1aiVh4x3Q//PWa+DYy+homDvgmK9qTJahwAdyfKE4We44GJLuagQ1Fl02BTGIkAVQzbArk+pryBwHVQJT9osoAYcdoQAACN8/KtGhMLBZ+7ACQBLAfKI8YAYtpLfqBFpgTngRg/5FUbikxnj2tfr5JICQDqeu62mzvA=";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727487_19509864000005).unwrap();
        transactions.push(tx);

        let tx_1633727491_19509866000001 = "te6ccgECCgEAAk8AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn6h7oG1rdyemhvUA/p0JIRwJlTvMmOOSdIz6eeVBZSAlNVSggAAEb5+g2oFYWC0AwADSAKGzOqAUEAQIbBEdJQJT9osoYgCfKCREDAgBvyYehIEwUWEAAAAAAAAQAAgAAAALzNieYO9rvzc/WHX+rtHOp7kmHMkhC0P8JSDIpGA6i5EBQH0wAnkovrD0JAAAAAAAAAAABMQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgnJV6EBW+f6sFdxvFaCQNe//Q6Dc64NEdA96jHVIXp7O0ghdenSRK8+0C6Heq9+ddTsNtsmPJFo0Z/PXgWHrCzToAgHgCAYBAd8HAPtoAO5PlCcLPccDEl3NQIaiy6bApjESAKoZtgVyfU15A4DrADZeOquWyPdRGZuPzO9Q+0wMQkIfSeqFsYQTWo/agIMbFAlKo+KABhRYYAAAI3z9Q90EwsFoBhbeDvsAAAABgCjegAAAAAAAAAAAAAAteYg9IAAAAAAAgAAAAMABs2gBsvHVXLZHuojM3H5neofaYGISEPpPVC2MIJrUftQEGNkAHcnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1UCU/aLKAGHHaEAAAjfPyrRoTCwWf0wAkASwHyiPGAHe82lVgNhnVmhrKHoy29N1PckUfIrlkNUVp0V/CWnuzw";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727491_19509866000001).unwrap();
        transactions.push(tx);

        let rocksdb = Arc::new(create_rocksdb("./raw_transactions"));

        let index = Utc::now().timestamp() as u32;
        rocksdb.check_drop_base_index(index);

        rocksdb.insert_transactions_with_drain(&mut transactions);

        let iter = rocksdb.iterate_unprocessed_transactions();

        let mut buff = (0_u32, 0_u64);
        for transaction in iter {
            println!(
                "now pre/new {}/{}, lt pre/new {}/{}",
                buff.0, transaction.now, buff.1, transaction.lt
            );
            let (now, lt) = (transaction.now, transaction.lt);
            rocksdb.update_transaction_processed(transaction);
            if now > buff.0 || (now == buff.0 && lt > buff.1) {
                buff = (now, lt);
                continue;
            }
            panic!("not sorted");
        }

        let iter = rocksdb.iterate_unprocessed_transactions();
        if iter.count() != 0 {
            panic!("not empty");
        }

        let tx_1633727487_19509864000005 = "te6ccgECCgEAAk4AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn6DagUbdffSf89hBa1GexE81mLOKF9myf93BN/RPoQqx5Xr2gAAEb5+g2oDYWCz/wADSAKGzLCAUEAQIZBAlAlP2iyhiAJ8oJEQMCAG/Jh6EgTBRYQAAAAAAABAACAAAAAvHb1yUS+o0j1TZJ//orKwBTNMIkdWLNr/Mry100VltmQFAfTACeSi+sPQkAAAAAAAAAAAExAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACCcvKRosX7ZnVEqR+2k20L/Rb35q1FMn8B4jJuQGjZBiLxVehAVvn+rBXcbxWgkDXv/0Og3OuDRHQPeox1SF6eztICAeAIBgEB3wcA+2gA7k+UJws9xwMSXc1AhqLLpsCmMRIAqhm2BXJ9TXkDgOsAErOO7jLMgxq1ErDxjuh/+es18Gxl9DRMHfBMV7UmS1DUCUqj4oAGFFhgAAAjfP0G1AzCwWf+Ft4O+wAAAAGAKN6AAAAAAAAAAAAAAC15iD0gAAAAAACAAAAAwAGzaACVnHdxlmQY1aiVh4x3Q//PWa+DYy+homDvgmK9qTJahwAdyfKE4We44GJLuagQ1Fl02BTGIkAVQzbArk+pryBwHVQJT9osoAYcdoQAACN8/KtGhMLBZ+7ACQBLAfKI8YAYtpLfqBFpgTngRg/5FUbikxnj2tfr5JICQDqeu62mzvA=";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727487_19509864000005).unwrap();
        transactions.push(tx);

        let tx_1633727491_19509866000001 = "te6ccgECCgEAAk8AA7d3cnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1AAARvn6h7oG1rdyemhvUA/p0JIRwJlTvMmOOSdIz6eeVBZSAlNVSggAAEb5+g2oFYWC0AwADSAKGzOqAUEAQIbBEdJQJT9osoYgCfKCREDAgBvyYehIEwUWEAAAAAAAAQAAgAAAALzNieYO9rvzc/WHX+rtHOp7kmHMkhC0P8JSDIpGA6i5EBQH0wAnkovrD0JAAAAAAAAAAABMQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgnJV6EBW+f6sFdxvFaCQNe//Q6Dc64NEdA96jHVIXp7O0ghdenSRK8+0C6Heq9+ddTsNtsmPJFo0Z/PXgWHrCzToAgHgCAYBAd8HAPtoAO5PlCcLPccDEl3NQIaiy6bApjESAKoZtgVyfU15A4DrADZeOquWyPdRGZuPzO9Q+0wMQkIfSeqFsYQTWo/agIMbFAlKo+KABhRYYAAAI3z9Q90EwsFoBhbeDvsAAAABgCjegAAAAAAAAAAAAAAteYg9IAAAAAAAgAAAAMABs2gBsvHVXLZHuojM3H5neofaYGISEPpPVC2MIJrUftQEGNkAHcnyhOFnuOBiS7moENRZdNgUxiJAFUM2wK5Pqa8gcB1UCU/aLKAGHHaEAAAjfPyrRoTCwWf0wAkASwHyiPGAHe82lVgNhnVmhrKHoy29N1PckUfIrlkNUVp0V/CWnuzw";
        let tx =
            ton_block::Transaction::construct_from_base64(&tx_1633727491_19509866000001).unwrap();
        transactions.push(tx);

        rocksdb.insert_transactions_with_drain(&mut transactions);

        let iter = rocksdb.iterate_unprocessed_transactions();
        if iter.count() != 0 {
            panic!("not empty");
        }

        println!("sorted and empty");

        drop(rocksdb);
        std::fs::remove_dir_all("./raw_transactions").unwrap();
    }
}
