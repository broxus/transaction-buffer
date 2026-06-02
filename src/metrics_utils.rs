pub const RAW_TXS_RECEIVED_COUNT: &str = "transaction_buffer_raw_txs_received_count";
pub const RAW_TXS_BUFFERED_COUNT: &str = "transaction_buffer_raw_txs_buffered_count";
pub const FAILED_TXS_BUFFERED_COUNT: &str = "transaction_buffer_failed_txs_buffered_count";
pub const RAW_TXS_PARSED_COUNT: &str = "transaction_buffer_raw_txs_parsed_count";
pub const RAW_TXS_PARSE_ERROR_COUNT: &str = "transaction_buffer_raw_txs_parse_error_count";
pub const FAILED_TXS_PARSED_COUNT: &str = "transaction_buffer_failed_txs_parsed_count";
pub const EXTRACTED_ITEMS_COUNT: &str = "transaction_buffer_extracted_items_count";
pub const RAW_TX_LAST_TIME: &str = "transaction_buffer_raw_tx_last_time";
pub const RAW_TX_LAG: &str = "transaction_buffer_raw_tx_lag";
pub const RAW_TX_BUFFERED_LAST_TIME: &str = "transaction_buffer_raw_tx_buffered_last_time";
pub const RAW_TX_BUFFERED_LAG: &str = "transaction_buffer_raw_tx_buffered_lag";
pub const RAW_TX_PARSED_LAST_TIME: &str = "transaction_buffer_raw_tx_parsed_last_time";
pub const RAW_TX_PARSED_LAG: &str = "transaction_buffer_raw_tx_parsed_lag";

#[cfg(feature = "metrics")]
pub fn describe_metrics() {
    use metrics::{describe_counter, describe_gauge, Unit};

    describe_counter!(
        RAW_TXS_RECEIVED_COUNT,
        Unit::Count,
        "Raw transactions received from Kafka by transaction-buffer"
    );
    describe_counter!(
        RAW_TXS_BUFFERED_COUNT,
        Unit::Count,
        "Raw transactions written to the transaction-buffer database"
    );
    describe_counter!(
        FAILED_TXS_BUFFERED_COUNT,
        Unit::Count,
        "Failed raw transactions written to the transaction-buffer database"
    );
    describe_counter!(
        RAW_TXS_PARSED_COUNT,
        Unit::Count,
        "Raw transaction parser calls completed successfully, including empty extracted results"
    );
    describe_counter!(
        RAW_TXS_PARSE_ERROR_COUNT,
        Unit::Count,
        "Raw transaction parser calls that returned an error"
    );
    describe_counter!(
        FAILED_TXS_PARSED_COUNT,
        Unit::Count,
        "Buffered failed raw transactions emitted without extractable items"
    );
    describe_counter!(
        EXTRACTED_ITEMS_COUNT,
        Unit::Count,
        "Extracted functions and events emitted by transaction-buffer"
    );
    describe_gauge!(
        RAW_TX_LAST_TIME,
        Unit::Seconds,
        "Unix timestamp in UTC of the last raw transaction received from Kafka before buffering"
    );
    describe_gauge!(
        RAW_TX_LAG,
        Unit::Seconds,
        "Seconds between current time and the last raw transaction received from Kafka before buffering"
    );
    describe_gauge!(
        RAW_TX_BUFFERED_LAST_TIME,
        Unit::Seconds,
        "Unix timestamp in UTC of the last raw transaction read from the buffer"
    );
    describe_gauge!(
        RAW_TX_BUFFERED_LAG,
        Unit::Seconds,
        "Seconds between current time and the last raw transaction read from the buffer"
    );
    describe_gauge!(
        RAW_TX_PARSED_LAST_TIME,
        Unit::Seconds,
        "Unix timestamp in UTC of the last raw transaction parsed successfully"
    );
    describe_gauge!(
        RAW_TX_PARSED_LAG,
        Unit::Seconds,
        "Seconds between current time and the last raw transaction parsed successfully"
    );

    metrics::counter!(RAW_TXS_RECEIVED_COUNT).increment(0);
    metrics::counter!(RAW_TXS_BUFFERED_COUNT).increment(0);
    metrics::counter!(FAILED_TXS_BUFFERED_COUNT).increment(0);
    metrics::counter!(RAW_TXS_PARSED_COUNT).increment(0);
    metrics::counter!(RAW_TXS_PARSE_ERROR_COUNT).increment(0);
    metrics::counter!(FAILED_TXS_PARSED_COUNT).increment(0);
    metrics::counter!(EXTRACTED_ITEMS_COUNT).increment(0);
    metrics::gauge!(RAW_TX_LAST_TIME).set(0);
    metrics::gauge!(RAW_TX_LAG).set(0);
    metrics::gauge!(RAW_TX_BUFFERED_LAST_TIME).set(0);
    metrics::gauge!(RAW_TX_BUFFERED_LAG).set(0);
    metrics::gauge!(RAW_TX_PARSED_LAST_TIME).set(0);
    metrics::gauge!(RAW_TX_PARSED_LAG).set(0);
}

#[cfg(not(feature = "metrics"))]
pub fn describe_metrics() {}

#[cfg(feature = "metrics")]
pub fn increment_raw_transactions_received_count(count: u64) {
    metrics::counter!(RAW_TXS_RECEIVED_COUNT).increment(count);
}

#[cfg(not(feature = "metrics"))]
pub fn increment_raw_transactions_received_count(_count: u64) {}

#[cfg(feature = "metrics")]
pub fn increment_raw_transactions_buffered_count(count: u64) {
    metrics::counter!(RAW_TXS_BUFFERED_COUNT).increment(count);
}

#[cfg(not(feature = "metrics"))]
pub fn increment_raw_transactions_buffered_count(_count: u64) {}

#[cfg(feature = "metrics")]
pub fn increment_failed_transactions_buffered_count(count: u64) {
    metrics::counter!(FAILED_TXS_BUFFERED_COUNT).increment(count);
}

#[cfg(not(feature = "metrics"))]
pub fn increment_failed_transactions_buffered_count(_count: u64) {}

#[cfg(feature = "metrics")]
pub fn increment_raw_transactions_parsed_count(count: u64) {
    metrics::counter!(RAW_TXS_PARSED_COUNT).increment(count);
}

#[cfg(not(feature = "metrics"))]
pub fn increment_raw_transactions_parsed_count(_count: u64) {}

#[cfg(feature = "metrics")]
pub fn increment_raw_transactions_parse_error_count(count: u64) {
    metrics::counter!(RAW_TXS_PARSE_ERROR_COUNT).increment(count);
}

#[cfg(not(feature = "metrics"))]
pub fn increment_raw_transactions_parse_error_count(_count: u64) {}

#[cfg(feature = "metrics")]
pub fn increment_failed_transactions_parsed_count(count: u64) {
    metrics::counter!(FAILED_TXS_PARSED_COUNT).increment(count);
}

#[cfg(not(feature = "metrics"))]
pub fn increment_failed_transactions_parsed_count(_count: u64) {}

#[cfg(feature = "metrics")]
pub fn increment_extracted_items_count(count: u64) {
    metrics::counter!(EXTRACTED_ITEMS_COUNT).increment(count);
}

#[cfg(not(feature = "metrics"))]
pub fn increment_extracted_items_count(_count: u64) {}

#[cfg(feature = "metrics")]
pub fn record_raw_transaction_received_timestamp(transaction_timestamp: i64) {
    record_timestamp_and_lag(RAW_TX_LAST_TIME, RAW_TX_LAG, transaction_timestamp);
}

#[cfg(not(feature = "metrics"))]
pub fn record_raw_transaction_received_timestamp(_transaction_timestamp: i64) {}

#[cfg(feature = "metrics")]
pub fn record_raw_transaction_buffered_timestamp(transaction_timestamp: i64) {
    record_timestamp_and_lag(
        RAW_TX_BUFFERED_LAST_TIME,
        RAW_TX_BUFFERED_LAG,
        transaction_timestamp,
    );
}

#[cfg(not(feature = "metrics"))]
pub fn record_raw_transaction_buffered_timestamp(_transaction_timestamp: i64) {}

#[cfg(feature = "metrics")]
pub fn record_raw_transaction_parsed_timestamp(transaction_timestamp: i64) {
    record_timestamp_and_lag(
        RAW_TX_PARSED_LAST_TIME,
        RAW_TX_PARSED_LAG,
        transaction_timestamp,
    );
}

#[cfg(not(feature = "metrics"))]
pub fn record_raw_transaction_parsed_timestamp(_transaction_timestamp: i64) {}

#[cfg(feature = "metrics")]
fn record_timestamp_and_lag(
    last_time_metric: &'static str,
    lag_metric: &'static str,
    timestamp: i64,
) {
    let lag = chrono::Utc::now().timestamp().saturating_sub(timestamp);

    metrics::gauge!(last_time_metric).set(timestamp as f64);
    metrics::gauge!(lag_metric).set(lag.max(0) as f64);
}
