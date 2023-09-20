use itertools::Itertools;
use log::LevelFilter;
use ton_block::{Deserializable, Transaction};

use crate::rocksdb_client::RocksdbClient;

pub async fn load_from_api(
    rocksdb_client: &RocksdbClient,
    from_timestamp: u32,
    url_api: &str,
    api_key: &str,
) -> Result<(), anyhow::Error> {
    let client = reqwest::Client::new();
    let mut is_processed = false;

    log::info!("start load from api");
    let mut from_timestamp_request = from_timestamp;
    let mut last_timestamp_lt = 0;
    println!("start load from api");
    let mut count = 0;
    loop {
        let url =
            format!("{url_api}/raw_transactions/{from_timestamp_request}/{is_processed}/{api_key}");
        log::info!("url: {}", url);
        let response: Vec<String> = match client.get(&url).send().await {
            Ok(ok) => match ok.text().await {
                Ok(ok) => match serde_json::from_str(&ok) {
                    Ok(ok) => ok,
                    Err(e) => {
                        log::error!("error: {}, text {}", e, ok);
                        continue;
                    }
                },
                Err(err) => {
                    log::error!("error: {}", err);
                    println!("error: {}", err);
                    continue;
                }
            },
            Err(err) => {
                log::error!("error: {}", err);
                println!("error: {}", err);
                continue;
            }
        };

        let mut transactions = response
            .into_iter()
            .map(|x| Transaction::construct_from_bytes(&base64::decode(x).unwrap()).unwrap())
            .collect_vec();

        if transactions.is_empty() && is_processed {
            break;
        } else if transactions.is_empty() {
            is_processed = true;
            from_timestamp_request = from_timestamp;
        } else {
            let (last_timestamp, last_lt) = transactions.last().map(|x| (x.now, x.lt)).unwrap();

            if (last_timestamp, last_lt) == (from_timestamp_request, last_timestamp_lt) {
                break;
            } else {
                (from_timestamp_request, last_timestamp_lt) = (last_timestamp, last_lt);
            }
        }
        count += transactions.len();
        log::info!("count_transactions: {}", count);
        rocksdb_client.insert_transactions_with_drain(&mut transactions);
    }

    Ok(())
}

#[cfg(test)]
pub fn init_logger(level_filter: LevelFilter) {
    use std::io::Write;

    env_logger::Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "{} {}/{} {} [{}] - {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S"),
                record.module_path().unwrap_or_default(),
                record.file().unwrap_or_default(),
                record.line().unwrap_or_default(),
                record.level(),
                record.args(),
            )
        })
        .filter(None, level_filter)
        .init();
}

#[cfg(test)]
pub fn get_rocksdb_client_test() -> std::sync::Arc<RocksdbClient> {
    std::sync::Arc::new(crate::utils::create_rocksdb(
        "./raw_transactions",
        crate::models::RocksdbClientConstants {
            drop_base_index: 0,
            from_timestamp: 0,
            postgres_base_is_dropped: false,
        },
    ))
}

#[cfg(test)]
mod test {
    use log::LevelFilter;
    use rand::RngCore;

    use crate::load_from_api::{get_rocksdb_client_test, init_logger};

    #[tokio::test]
    async fn test_load_from_api() {
        init_logger(LevelFilter::Info);

        let rocksdb = get_rocksdb_client_test();

        super::load_from_api(
            &rocksdb,
            0,
            "",
            "",
        )
        .await
        .unwrap();
        println!("transactions loaded");
        println!("count: {}", rocksdb.count_not_processed_transactions());
        drop(rocksdb);
        std::fs::remove_dir_all("./raw_transactions").unwrap();
    }

    #[test]
    fn gen_random_transactions() {
        init_logger(LevelFilter::Info);
        std::fs::remove_dir_all("./raw_transactions").unwrap();

        let rocksdb_client = get_rocksdb_client_test();

        for index in 0..1_000_000 {
            if index % 100_000 == 0 {
                log::info!("index: {}", index);
            }

            let mut key_index = [0_u8; 4 + 8 + 32];

            rand::thread_rng().fill_bytes(&mut key_index);

            let mut key = [0_u8; 1 + 4 + 8 + 32];
            let mut value = [0_u8; 1000];
            rand::thread_rng().fill_bytes(&mut value);

            key[0] = false as u8;
            key[1..].copy_from_slice(&key_index);

            rocksdb_client
                .inner
                .raw()
                .put_cf(&rocksdb_client.transactions.cf(), key, value)
                .expect("cant insert transaction: rocksdb is dead");
        }
        log::info!(
            "total_count: {}",
            rocksdb_client.count_not_processed_transactions()
        );
    }
}
