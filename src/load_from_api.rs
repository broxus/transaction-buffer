use itertools::Itertools;
use ton_block::{Deserializable, Transaction};

use crate::rocksdb_client::RocksdbClient;

pub async fn load_from_api(
    rocksdb_client: &RocksdbClient,
    from_timestamp: u32,
    to_timestamp: u32,
    url_api: &str,
    api_key: &str,
) -> Result<(), anyhow::Error> {
    let client = reqwest::Client::new();
    let mut is_processed = false;

    log::info!("start load from api");
    let mut count_inserted = 0;
    let mut from_timestamp_request = from_timestamp;
    let mut last_timestamp_lt = 0;
    loop {
        let url =
            format!("{url_api}/raw_transactions/{from_timestamp_request}/{to_timestamp}/{is_processed}/{api_key}");
        let response: Vec<String> = client.get(&url).send().await?.json().await?;
        let mut transactions = response
            .into_iter()
            .map(|x| Transaction::construct_from_bytes(&base64::decode(x).unwrap()).unwrap())
            .collect_vec();

        if transactions.is_empty() && is_processed {
            break;
        } else if transactions.is_empty() {
            is_processed = true;
            from_timestamp_request = from_timestamp;
            last_timestamp_lt = 0;
        } else {
            let (last_timestamp, last_lt) = transactions.last().map(|x| (x.now, x.lt)).unwrap();

            if (last_timestamp, last_lt) == (from_timestamp_request, last_timestamp_lt) {
                if is_processed {
                    break;
                } else {
                    is_processed = true;
                    from_timestamp_request = from_timestamp;
                    last_timestamp_lt = 0;
                }
            } else {
                (from_timestamp_request, last_timestamp_lt) = (last_timestamp, last_lt);
            }
        }

        log::info!("insert transactions len {}", transactions.len());
        count_inserted += transactions.len();
        rocksdb_client.insert_transactions_with_drain(&mut transactions);
        log::info!("count inserted transactions {}", count_inserted);
    }

    Ok(())
}

#[cfg(test)]
fn prepare_test(level_filter: log::LevelFilter) -> () {
    use env_logger::Builder;
    use std::io::Write;
    use std::str::FromStr;

    Builder::new()
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
        .filter(Some("transaction_buffer"), level_filter)
        .init();
}

#[cfg(test)]
mod test {
    use crate::load_from_api::prepare_test;
    use std::sync::Arc;

    use crate::models::RocksdbClientConstants;
    use crate::utils::create_rocksdb;

    #[tokio::test]
    async fn test_load_from_api() {
        prepare_test(log::LevelFilter::Debug);
        println!("test_load_from_api");
        let rocksdb = Arc::new(create_rocksdb(
            "./raw_transactions",
            RocksdbClientConstants {
                drop_base_index: 0,
                from_timestamp: 0,
                postgres_base_is_dropped: false,
            },
        ));

        println!("load_from_api");
        super::load_from_api(
            &rocksdb,
            0,
            1706199511,
            "",
            "",
        )
        .await
        .unwrap();
        println!("{}", rocksdb.count_not_processed_transactions());
        drop(rocksdb);
        std::fs::remove_dir_all("./raw_transactions").unwrap();
    }
}
