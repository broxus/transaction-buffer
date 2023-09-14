use anyhow::{Context, Result};
use std::path::PathBuf;
use ton_block::{Deserializable, Serializable, Transaction};
use weedb::rocksdb::{IteratorMode, WriteBatchWithTransaction};
use weedb::{rocksdb, Caches, Migrations, Semver, Table, WeeDb};

pub mod tables;

#[derive(Debug, Clone)]
pub struct DbOptions {
    pub max_memory_usage: usize,
    pub min_caches_capacity: usize,
    pub min_compaction_memory_budget: usize,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            max_memory_usage: 2 << 30,             // 2 GB
            min_caches_capacity: 64 << 20,         // 64 MB
            min_compaction_memory_budget: 1 << 30, // 1 GB
        }
    }
}

pub struct RocksdbClient {
    pub transactions: Table<tables::Transactions>,
    pub transactions_index: Table<tables::TransactionsIndex>,
    pub drop_base_index: Table<tables::DropBaseIndex>,
    pub inner: WeeDb,
}

#[derive(Debug, Clone)]
pub struct RocksdbClientConfig {
    pub persistent_db_path: PathBuf,
    pub persistent_db_options: DbOptions,
}

impl RocksdbClient {
    const DB_VERSION: Semver = [0, 1, 0];

    pub fn new(config: &RocksdbClientConfig) -> Result<Self> {
        let limit = match fdlimit::raise_fd_limit() {
            // New fd limit
            Some(limit) => limit,
            // Current soft limit
            None => {
                rlimit::getrlimit(rlimit::Resource::NOFILE)
                    .unwrap_or((256, 0))
                    .0
            }
        };
        let options = &config.persistent_db_options;

        let caches_capacity =
            std::cmp::max(options.max_memory_usage / 3, options.min_caches_capacity);
        let compaction_memory_budget = std::cmp::max(
            options.max_memory_usage - options.max_memory_usage / 3,
            options.min_compaction_memory_budget,
        );

        let caches = Caches::with_capacity(caches_capacity);

        let inner = WeeDb::builder(&config.persistent_db_path, caches)
            .options(|opts, _| {
                opts.set_level_compaction_dynamic_level_bytes(true);

                // compression opts
                opts.set_compression_type(rocksdb::DBCompressionType::Zstd);

                // io
                opts.set_max_open_files(limit as i32);

                // logging
                opts.set_log_level(rocksdb::LogLevel::Error);
                opts.set_keep_log_file_num(2);
                opts.set_recycle_log_file_num(2);

                // cf
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);

                // cpu
                opts.set_max_background_jobs(std::cmp::max(
                    (std::thread::available_parallelism()
                        .expect("cant get available_parallelism")
                        .get() as i32)
                        / 2,
                    2,
                ));
                opts.increase_parallelism(
                    std::thread::available_parallelism()
                        .expect("cant get available_parallelism")
                        .get() as i32 as i32,
                );

                opts.optimize_level_style_compaction(compaction_memory_budget);

                // debug
                // opts.enable_statistics();
                // opts.set_stats_dump_period_sec(30);
            })
            .with_table::<tables::Transactions>()
            .with_table::<tables::TransactionsIndex>()
            .with_table::<tables::DropBaseIndex>()
            .build()
            .context("Failed building db")?;

        let migrations = Migrations::with_target_version(Self::DB_VERSION);
        inner
            .apply(migrations)
            .context("Failed to apply migrations")?;

        let transactions: Table<tables::Transactions> = inner.instantiate_table();
        let transactions_index: Table<tables::TransactionsIndex> = inner.instantiate_table();
        let drop_base_index: Table<tables::DropBaseIndex> = inner.instantiate_table();

        Ok(Self {
            transactions,
            transactions_index,
            drop_base_index,
            inner,
        })
    }

    pub fn insert_transactions_with_drain(&self, transactions: &mut Vec<Transaction>) {
        while let Some(transaction) = transactions.pop() {
            self.insert_transaction(&transaction);
        }
    }

    pub fn insert_transaction(&self, transaction: &Transaction) {
        let mut key_index = [0_u8; 12];

        key_index[0..4].copy_from_slice(&transaction.now.to_be_bytes());
        key_index[4..].copy_from_slice(&transaction.lt.to_be_bytes());

        if !self
            .transactions_index
            .contains_key(key_index)
            .expect("cant check transaction_index: rocksdb is dead")
        {
            let mut key = [0_u8; 13];
            let value = transaction.write_to_bytes().expect("trust me");

            key[0] = false as u8;
            key[1..5].copy_from_slice(&transaction.now.to_be_bytes());
            key[5..].copy_from_slice(&transaction.lt.to_be_bytes());

            let mut batch = WriteBatchWithTransaction::<false>::default();

            batch.put_cf(&self.transactions.cf(), key, value);
            batch.put_cf(&self.transactions_index.cf(), key_index, []);

            self.inner
                .raw()
                .write(batch)
                .expect("cant insert transaction: rocksdb is dead");
        }
    }

    pub fn update_transaction_processed(&self, transaction: Transaction) {
        let mut key = [0_u8; 13];
        let value = transaction.write_to_bytes().expect("trust me");

        key[0] = false as u8;
        key[1..5].copy_from_slice(&transaction.now.to_be_bytes());
        key[5..].copy_from_slice(&transaction.lt.to_be_bytes());

        let mut batch = WriteBatchWithTransaction::<false>::default();
        batch.delete_cf(&self.transactions.cf(), key);

        key[0] = true as u8;
        batch.put_cf(&self.transactions.cf(), key, value);

        self.inner
            .raw()
            .write(batch)
            .expect("cant update transaction: rocksdb is dead");
    }

    pub fn iterate_unprocessed_transactions(&self) -> impl Iterator<Item = Transaction> + '_ {
        let mut key = [0_u8; 13];
        key[0] = false as u8;

        self.transactions
            .iterator(IteratorMode::From(&key, rocksdb::Direction::Forward))
            .filter_map(|key| {
                let (key, value) = key.ok()?;
                if key[0] != false as u8 {
                    return None;
                }
                Some(Transaction::construct_from_bytes(&value).expect("trust me"))
            })
            .fuse()
    }

    pub fn count_not_processed_transactions(&self) -> usize {
        let mut key = [0_u8; 13];
        key[0] = false as u8;

        self.transactions
            .iterator(IteratorMode::From(&key, rocksdb::Direction::Forward))
            .filter_map(|key| {
                let (key, _) = key.ok()?;
                if key[0] != false as u8 {
                    return None;
                }
                Some(())
            })
            .fuse()
            .count()
    }

    pub fn check_drop_base_index(&self, index: u32) -> bool {
        let mut key = [0_u8; 4];
        key.copy_from_slice(&index.to_be_bytes());

        match self
            .drop_base_index
            .contains_key(key)
            .expect("cant check transaction_index: rocksdb is dead")
        {
            true => true,
            false => {
                let mut batch = WriteBatchWithTransaction::<false>::default();

                batch.delete_range_cf(
                    &self.transactions.cf(),
                    [0_u8; 4 + 8 + 1],
                    [u8::MAX; 4 + 8 + 1],
                );
                batch.delete_range_cf(&self.drop_base_index.cf(), [0_u8; 4], [u8::MAX; 4]);
                batch.delete_range_cf(
                    &self.transactions_index.cf(),
                    [0_u8; 4 + 8],
                    [u8::MAX; 4 + 8],
                );

                self.inner
                    .raw()
                    .write(batch)
                    .expect("cant delete range: rocksdb is dead");

                self.inner
                    .raw()
                    .put_cf(&self.drop_base_index.cf(), key, [])
                    .expect("cant put drop_base_index: rocksdb is dead");

                false
            }
        }
    }
}

impl Drop for RocksdbClient {
    fn drop(&mut self) {
        self.inner.raw().cancel_all_background_work(true);
    }
}
