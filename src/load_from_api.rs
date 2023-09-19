use itertools::Itertools;
use ton_block::{Deserializable, Transaction};

use crate::rocksdb_client::RocksdbClient;

pub async fn load_from_api(rocksdb_client: &RocksdbClient, from_timestamp: u32, url_api: &str) -> Result<(), anyhow::Error> {
    let client = reqwest::Client::new();
    let mut is_processed = false;

    log::info!("start load from api");
    let mut from_timestamp_request = from_timestamp;

    loop {
        let url = format!("{url_api}/raw_transactions?from_timestamp={from_timestamp}&is_processed={is_processed}");
        let response: Vec<String> = client.get(&url).send().await?.json().await?;
        let mut transactions = response.into_iter().map(|x| Transaction::construct_from_bytes(&base64::decode(x).unwrap()).unwrap()).collect_vec();

        if transactions.is_empty() && is_processed {
            break;
        } else if transactions.is_empty() {
            is_processed = true;
            from_timestamp_request = from_timestamp;
        } else {
            from_timestamp_request = transactions.last().unwrap().now();
        }

        rocksdb_client.insert_transactions_with_drain(&mut transactions);
    }

    Ok(())
}