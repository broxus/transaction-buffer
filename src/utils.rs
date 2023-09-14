use crate::models::{AnyExtractable, RocksdbClientConstants};
use nekoton_abi::transaction_parser::{Extracted, ExtractedOwned, ParsedType};
use nekoton_abi::TransactionParser;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use ton_block::{Transaction, TrComputePhase};
use crate::context::BufferContext;
use crate::rocksdb_client::{RocksdbClientConfig, RocksdbClient};

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

pub async fn timer(context: Arc<BufferContext>) {
    loop {
        *context.time.write().await += 1;
        sleep(Duration::from_secs(1)).await;
    }
}

pub fn create_transaction_parser(
    any_extractable: Vec<AnyExtractable>,
) -> Result<TransactionParser, anyhow::Error> {
    let (functions, events) = split_any_extractable(any_extractable);

    TransactionParser::builder()
        .function_in_list(functions.clone(), false)
        .functions_out_list(functions, false)
        .events_list(events)
        .build()
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

pub fn create_rocksdb(rocksdb_path: &str, constants: RocksdbClientConstants) -> RocksdbClient {
    let config = RocksdbClientConfig {
        persistent_db_path: rocksdb_path.parse().expect("wrong rocksdb path"),
        persistent_db_options: Default::default(),
        constants,
    };

    RocksdbClient::new(&config).expect("cant create rocksdb")
}
