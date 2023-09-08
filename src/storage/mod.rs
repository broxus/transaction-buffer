use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::storage::tables::Transactions;
use anyhow::{Context, Result};
use tokio::sync::watch;
use ton_block::{Deserializable, HashmapAugType, Serializable, Transaction};
use ton_types::{HashmapType, UInt256};
use weedb::rocksdb::{IteratorMode, WriteBatchWithTransaction};
use weedb::{rocksdb, Caches, Migrations, Semver, Table, WeeDb};

pub mod tables;

pub struct RuntimeStorage {
    key_block: watch::Sender<Option<ton_block::Block>>,
    masterchain_accounts_cache: RwLock<Option<ShardAccounts>>,
    shard_accounts_cache: RwLock<FxHashMap<ton_block::ShardIdent, ShardAccounts>>,
}

impl Default for RuntimeStorage {
    fn default() -> Self {
        let (key_block, _) = watch::channel(None);
        Self {
            key_block,
            masterchain_accounts_cache: Default::default(),
            shard_accounts_cache: Default::default(),
        }
    }
}

impl RuntimeStorage {
    pub fn subscribe_to_key_blocks(&self) -> watch::Receiver<Option<ton_block::Block>> {
        self.key_block.subscribe()
    }

    pub fn update_key_block(&self, block: &ton_block::Block) {
        self.key_block.send_replace(Some(block.clone()));
    }

    pub fn update_contract_states(
        &self,
        block_id: &ton_block::BlockIdExt,
        block_info: &ton_block::BlockInfo,
        shard_state: &ShardStateStuff,
    ) -> Result<()> {
        let accounts = shard_state.state().read_accounts()?;
        let state_handle = shard_state.ref_mc_state_handle().clone();

        let shard_accounts = ShardAccounts {
            accounts,
            state_handle,
            gen_utime: block_info.gen_utime().as_u32(),
        };

        if block_id.shard_id.is_masterchain() {
            *self.masterchain_accounts_cache.write() = Some(shard_accounts);
        } else {
            let mut cache = self.shard_accounts_cache.write();

            cache.insert(*block_info.shard(), shard_accounts);
            if block_info.after_merge() || block_info.after_split() {
                tracing::debug!("Clearing shard states cache after shards merge/split");

                let block_ids = block_info.read_prev_ids()?;
                match block_ids.len() {
                    // Block after split
                    //       |
                    //       *  - block A
                    //      / \
                    //     *   *  - blocks B', B"
                    1 => {
                        // Find all split shards for the block A
                        let (left, right) = block_ids[0].shard_id.split()?;

                        // Remove parent shard of the block A
                        if cache.contains_key(&left) && cache.contains_key(&right) {
                            cache.remove(&block_ids[0].shard_id);
                        }
                    }

                    // Block after merge
                    //     *   *  - blocks A', A"
                    //      \ /
                    //       *  - block B
                    //       |
                    2 => {
                        // Find and remove all parent shards
                        for block_id in block_info.read_prev_ids()? {
                            cache.remove(&block_id.shard_id);
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
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

pub struct PersistentStorage {
    pub transactions: Table<tables::Transactions>,
    pub transactions_index: Table<tables::TransactionsIndex>,
    pub inner: WeeDb,
}

#[derive(Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct PersistentStorageConfig {
    pub persistent_db_path: PathBuf,
    #[serde(default)]
    pub persistent_db_options: DbOptions,
}

impl PersistentStorage {
    const DB_VERSION: Semver = [0, 1, 0];

    pub fn new(config: &PersistentStorageConfig) -> Result<Self> {
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
                    (std::thread::available_parallelism() as i32) / 2,
                    2,
                ));
                opts.increase_parallelism(std::thread::available_parallelism() as i32);

                opts.optimize_level_style_compaction(compaction_memory_budget);

                // debug
                // opts.enable_statistics();
                // opts.set_stats_dump_period_sec(30);
            })
            .with_table::<tables::Transactions>()
            .with_table::<tables::TransactionsIndex>()
            .build()
            .context("Failed building db")?;

        let migrations = Migrations::with_target_version(Self::DB_VERSION);
        inner
            .apply(migrations)
            .context("Failed to apply migrations")?;

        let transactions: Table<tables::Transactions> = inner.instantiate_table();
        let transactions_index: Table<tables::TransactionsIndex> = inner.instantiate_table();

        Ok(Self {
            transactions,
            transactions_index,
            inner,
        })
    }

    pub fn insert(&self, transaction: &Transaction) {
        let mut key_index = [0_u8; 12];

        key_index[0..4].copy_from_slice(&transaction.now.to_le_bytes());
        key_index[4..].copy_from_slice(&transaction.lt.to_le_bytes());

        if !self
            .transactions_index
            .contains_key(&key_index)
            .expect("cant check transaction_index: rocksdb is dead")
        {
            let mut key = [0_u8; 13];
            let value = transaction.write_to_bytes().expect("trust me");

            key[0] = false as u8;
            key[1..5].copy_from_slice(&transaction.now.to_le_bytes());
            key[5..].copy_from_slice(&transaction.lt.to_le_bytes());

            let mut batch = WriteBatchWithTransaction::<false>::default();

            batch.put_cf(&self.transactions.cf(), &key, &value);
            batch.put_cf(&self.transactions_index.cf(), &key_index, &[]);

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
        key[1..5].copy_from_slice(&transaction.now.to_le_bytes());
        key[5..].copy_from_slice(&transaction.lt.to_le_bytes());

        let mut batch = WriteBatchWithTransaction::<false>::default();
        batch.delete_cf(&self.transactions.cf(), &key);

        key[0] = true as u8;
        batch.put_cf(&self.transactions.cf(), &key, &value);

        self.inner
            .raw()
            .write(batch)
            .expect("cant update transaction: rocksdb is dead");
    }

    pub fn iterate_unprocessed_transactions(&self) -> impl Iterator<Item = Transaction> {
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
    }
}

impl Drop for PersistentStorage {
    fn drop(&mut self) {
        self.inner.raw().cancel_all_background_work(true);
    }
}
