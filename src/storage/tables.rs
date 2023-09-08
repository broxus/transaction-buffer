use weedb::rocksdb::{BlockBasedOptions, DBCompressionType, Options};
use weedb::{Caches, ColumnFamily};

/// - Key: `timestamp: u32, timestamp_lt: u64, processed: bool`
/// - Value: transaction: bytes`
pub struct Transactions;

impl Transactions {
    pub const KEY_LEN: usize = 4 + 8 + 1;
}

impl ColumnFamily for Transactions {
    const NAME: &'static str = "transactions";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);
    }
}

pub struct TransactionsIndex;

impl TransactionsIndex {
    pub const KEY_LEN: usize = 4 + 8;
}

impl ColumnFamily for TransactionsIndex {
    const NAME: &'static str = "transactions_index";

    fn options(opts: &mut Options, caches: &Caches) {
        default_block_based_table_factory(opts, caches);
    }
}

fn default_block_based_table_factory(opts: &mut Options, caches: &Caches) {
    let mut block_factory = BlockBasedOptions::default();
    block_factory.set_block_cache(&caches.block_cache);
    opts.set_block_based_table_factory(&block_factory);
    opts.set_compression_type(DBCompressionType::Zstd);
}