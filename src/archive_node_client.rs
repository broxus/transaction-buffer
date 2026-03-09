use crate::models::ArchiveNodeConfig;
use crate::rocksdb_client::RocksdbClient;
use crate::utils::buff_extracted_events;
use anyhow::{anyhow, Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use nekoton_abi::TransactionParser;
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tl_proto::{TlPacket, TlRead, TlResult, TlWrite};
use tokio::task::JoinSet;
use ton_block::Transaction;
use tycho_types::boc::Boc;
use tycho_types::cell::HashBytes;
use tycho_types::models::ShardIdent;
use tycho_types::models::{Block, BlockId};
use zstd::stream::decode_all;

const ARCHIVE_PREFIX_ID: u32 = tl_proto::id!("archive.prefix", scheme = "proto.tl");
const ARCHIVE_PREFIX: [u8; 4] = u32::to_le_bytes(ARCHIVE_PREFIX_ID);

#[derive(Debug, Clone, TlRead, TlWrite)]
#[tl(boxed, id = "archive.entryHeader", scheme = "proto.tl")]
struct ArchiveEntryHeader {
    #[tl(with = "tl_shard_ident")]
    shard_ident: ShardIdent,
    seqno: u32,
    #[tl(with = "tl_hash_bytes")]
    root_hash: HashBytes,
    #[tl(with = "tl_hash_bytes")]
    file_hash: HashBytes,
    ty: ArchiveEntryType,
    data_len: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, TlRead, TlWrite)]
#[tl(boxed, scheme = "proto.tl")]
#[repr(u8)]
enum ArchiveEntryType {
    #[tl(id = "archive.entryType.block")]
    Block = 0,
    #[tl(id = "archive.entryType.proof")]
    Proof = 1,
    #[tl(id = "archive.entryType.queueDiff")]
    QueueDiff = 2,
}

impl ArchiveEntryHeader {
    fn block_id(&self) -> BlockId {
        BlockId {
            shard: self.shard_ident,
            seqno: self.seqno,
            root_hash: self.root_hash,
            file_hash: self.file_hash,
        }
    }
}

mod tl_shard_ident {
    use super::*;

    pub const SIZE_HINT: usize = 12;

    pub const fn size_hint(_: &ShardIdent) -> usize {
        SIZE_HINT
    }

    pub fn write<P: TlPacket>(shard_ident: &ShardIdent, packet: &mut P) {
        shard_ident.workchain().write_to(packet);
        shard_ident.prefix().write_to(packet);
    }

    pub fn read(packet: &mut &[u8]) -> TlResult<ShardIdent> {
        let workchain = <i32 as TlRead>::read_from(packet)?;
        let prefix = <u64 as TlRead>::read_from(packet)?;
        ShardIdent::new(workchain, prefix).ok_or(tl_proto::TlError::InvalidData)
    }
}

mod tl_hash_bytes {
    use super::*;

    pub const SIZE_HINT: usize = 32;

    pub const fn size_hint(_: &HashBytes) -> usize {
        SIZE_HINT
    }

    pub fn write<P: TlPacket>(hash_bytes: &HashBytes, packet: &mut P) {
        packet.write_raw_slice(hash_bytes.as_ref());
    }

    pub fn read(data: &mut &[u8]) -> TlResult<HashBytes> {
        <&[u8; 32]>::read_from(data).map(|bytes| HashBytes(*bytes))
    }
}

struct ParsedArchive {
    block_entries: HashMap<BlockId, Vec<u8>>,
    next_archive_id_hint: u32,
}

struct ArchiveProcessResult {
    transactions: Vec<Transaction>,
    blocks_in_archive: usize,
    last_master_seqno: u32,
    last_master_gen_utime: u32,
}

#[derive(Debug, Clone)]
struct ArchiveProbeResult {
    next_archive_id_hint: u32,
    last_master_gen_utime: u32,
}

#[derive(Debug, Clone)]
struct ArchiveProbePoint {
    archive_id: u32,
    probe: ArchiveProbeResult,
}

struct BatchWindowOutput {
    window_start: u32,
    window_end: u32,
    processed_archives: u64,
    total_tx: u64,
    total_blocks: u64,
    last_archive_id: Option<u32>,
    overflow_next_archive_id: Option<u32>,
    reached_to_timestamp: bool,
    max_inserted_timestamp: Option<u32>,
}

struct ParallelArchiveConfig {
    max_parallel_workers: usize,
    archive_batch_size: usize,
}

const DEFAULT_ARCHIVE_BATCH_SIZE: usize = 1000;
const WINDOW_DISCOVERY_CHUNK_SIZE: u32 = 32;
const SEARCH_EXISTENCE_CHUNK_SIZE: u32 = 128;
const SEARCH_FORWARD_LOOKAHEAD_IDS: u32 = 4096;
const START_ARCHIVE_LOOKBACK_SECONDS: u32 = 24 * 60 * 60;
const SEARCH_BRACKET_CONCURRENCY: usize = 4;
const SEARCH_MIN_BRACKET_GAP: u32 = 1024;

#[derive(Clone)]
struct ArchiveS3Client {
    client: std::sync::Arc<dyn ObjectStore>,
}

struct SyncSummary {
    window_started_at: Instant,
    window_archives: u64,
    window_tx: u64,
    window_blocks: u64,
    total_archives: u64,
    total_tx: u64,
    total_blocks: u64,
}

struct ArchiveWriteOutcome {
    reached_to_timestamp: bool,
    next_archive_id: u32,
    tx_count: usize,
    blocks_in_archive: usize,
    max_inserted_timestamp: Option<u32>,
}

struct SearchStats {
    started_at: Instant,
    probe_requests: u64,
    cache_hits: u64,
}

impl SyncSummary {
    fn new() -> Self {
        Self {
            window_started_at: Instant::now(),
            window_archives: 0,
            window_tx: 0,
            window_blocks: 0,
            total_archives: 0,
            total_tx: 0,
            total_blocks: 0,
        }
    }

    fn add_batch(&mut self, tx: usize, blocks: usize) {
        self.window_archives = self.window_archives.saturating_add(1);
        self.window_tx = self.window_tx.saturating_add(tx as u64);
        self.window_blocks = self.window_blocks.saturating_add(blocks as u64);
        self.total_archives = self.total_archives.saturating_add(1);
        self.total_tx = self.total_tx.saturating_add(tx as u64);
        self.total_blocks = self.total_blocks.saturating_add(blocks as u64);
    }

    fn maybe_log(&mut self, label: &'static str) {
        let elapsed = self.window_started_at.elapsed();
        if elapsed < Duration::from_secs(60) || self.window_archives == 0 {
            return;
        }

        log_archive_sync_summary(
            label,
            elapsed,
            self.window_archives,
            self.window_tx,
            self.window_blocks,
            self.total_archives,
            self.total_tx,
            self.total_blocks,
        );
        self.reset_window();
    }

    fn log_final(&self, label: &'static str) {
        if self.window_archives == 0 {
            return;
        }
        log_archive_sync_summary(
            label,
            self.window_started_at.elapsed(),
            self.window_archives,
            self.window_tx,
            self.window_blocks,
            self.total_archives,
            self.total_tx,
            self.total_blocks,
        );
    }

    fn reset_window(&mut self) {
        self.window_started_at = Instant::now();
        self.window_archives = 0;
        self.window_tx = 0;
        self.window_blocks = 0;
    }
}

#[tracing::instrument(level = "info", skip(rocksdb_client, parser, cfg))]
pub async fn sync_from_genesis_until_day_ago(
    rocksdb_client: &RocksdbClient,
    parser: &TransactionParser,
    cfg: &ArchiveNodeConfig,
) -> Result<Option<u32>> {
    let cutoff_timestamp = now_minus_one_day_unix();
    sync_from_s3_range(
        rocksdb_client,
        parser,
        cfg,
        cfg.from_timestamp.unwrap_or_default(),
        cutoff_timestamp,
    )
    .await
}

#[tracing::instrument(
    level = "info",
    skip(rocksdb_client, parser, cfg),
    fields(from_timestamp, to_timestamp)
)]
pub(crate) async fn sync_from_s3_range(
    rocksdb_client: &RocksdbClient,
    parser: &TransactionParser,
    cfg: &ArchiveNodeConfig,
    from_timestamp: u32,
    to_timestamp: u32,
) -> Result<Option<u32>> {
    if from_timestamp >= to_timestamp {
        tracing::warn!(
            from_timestamp,
            to_timestamp,
            "invalid S3 sync range, skip loading"
        );
        return Ok(None);
    }

    match resolve_parallel_archive_config(cfg)? {
        Some(parallel_cfg) => {
            sync_from_s3_range_parallel(
                rocksdb_client,
                parser,
                cfg,
                from_timestamp,
                to_timestamp,
                parallel_cfg,
            )
            .await
        }
        None => {
            sync_from_s3_range_prefetch_one(
                rocksdb_client,
                parser,
                cfg,
                from_timestamp,
                to_timestamp,
            )
            .await
        }
    }
}

async fn sync_from_s3_range_parallel(
    rocksdb_client: &RocksdbClient,
    parser: &TransactionParser,
    cfg: &ArchiveNodeConfig,
    from_timestamp: u32,
    to_timestamp: u32,
    parallel_cfg: ParallelArchiveConfig,
) -> Result<Option<u32>> {
    let s3_client = build_s3_client(cfg)?;
    let mut max_inserted_timestamp: Option<u32> = None;
    let summary = Arc::new(Mutex::new(SyncSummary::new()));
    let stop_requested = Arc::new(AtomicBool::new(false));
    let batch_size_u32 = u32::try_from(parallel_cfg.archive_batch_size)
        .context("archive_batch_size does not fit into u32")?;
    let initial_archive_id =
        resolve_start_archive_id(
            &s3_client,
            resolve_search_from_timestamp(cfg, from_timestamp),
            "parallel",
        )
            .await?;
    let initial_window_start = batch_window_start(initial_archive_id, batch_size_u32);
    let mut next_batch_start = initial_window_start;
    let mut window_start_hints: HashMap<u32, u32> = HashMap::new();
    window_start_hints.insert(initial_window_start, initial_archive_id);
    let mut workers = JoinSet::new();

    loop {
        while !stop_requested.load(Ordering::SeqCst)
            && workers.len() < parallel_cfg.max_parallel_workers
        {
            let window_start = next_batch_start;
            let window_end = batch_window_end(window_start, batch_size_u32);
            let preferred_start = window_start_hints.remove(&window_start);
            let rocksdb_client = rocksdb_client.clone();
            let parser = parser.clone();
            let s3_client = s3_client.clone();
            let summary = summary.clone();
            let stop_requested = stop_requested.clone();
            workers.spawn(async move {
                process_archive_batch_window(
                    &rocksdb_client,
                    &parser,
                    &s3_client,
                    summary,
                    stop_requested,
                    window_start,
                    window_end,
                    preferred_start,
                    from_timestamp,
                    to_timestamp,
                )
                .await
            });
            next_batch_start = next_batch_start
                .checked_add(batch_size_u32)
                .ok_or_else(|| anyhow!("archive batch start overflow"))?;
        }

        let Some(batch_output) = workers.join_next().await else {
            break;
        };
        let batch_output = batch_output.context("archive batch worker join failed")??;
        let next_archive_id = batch_output.overflow_next_archive_id;

        if let Some(current) = batch_output.max_inserted_timestamp {
            max_inserted_timestamp = Some(match max_inserted_timestamp {
                Some(existing) => existing.max(current),
                None => current,
            });
        }

        if let Some(overflow_next_archive_id) = batch_output.overflow_next_archive_id {
            let next_window_start = batch_output
                .window_start
                .checked_add(batch_size_u32)
                .ok_or_else(|| anyhow!("archive batch window start overflow"))?;
            if overflow_next_archive_id >= next_window_start
                && overflow_next_archive_id <= batch_window_end(next_window_start, batch_size_u32)
            {
                window_start_hints
                    .entry(next_window_start)
                    .or_insert(overflow_next_archive_id);
            }
        }

        tracing::debug!(
            window_start = batch_output.window_start,
            window_end = batch_output.window_end,
            processed_archives = batch_output.processed_archives,
            total_tx = batch_output.total_tx,
            total_blocks = batch_output.total_blocks,
            last_archive_id = batch_output.last_archive_id,
            next_archive_id,
            overflow_next_archive_id = batch_output.overflow_next_archive_id,
            reached_to_timestamp = batch_output.reached_to_timestamp,
            "archive batch completed"
        );

        if batch_output.reached_to_timestamp {
            stop_requested.store(true, Ordering::SeqCst);
        }
    }

    summary
        .lock()
        .expect("archive sync summary mutex poisoned")
        .log_final("archive sync summary (final)");
    Ok(max_inserted_timestamp)
}

async fn sync_from_s3_range_prefetch_one(
    rocksdb_client: &RocksdbClient,
    parser: &TransactionParser,
    cfg: &ArchiveNodeConfig,
    from_timestamp: u32,
    to_timestamp: u32,
) -> Result<Option<u32>> {
    let s3_client = build_s3_client(cfg)?;
    let mut max_inserted_timestamp: Option<u32> = None;
    let mut summary = SyncSummary::new();
    let mut current_archive_id =
        resolve_start_archive_id(
            &s3_client,
            resolve_search_from_timestamp(cfg, from_timestamp),
            "prefetch",
        )
            .await?;
    let mut current_archive_bytes = download_archive_bytes(&s3_client, current_archive_id).await?;

    loop {
        let parsed_archive = parse_archive_payload_blocking(current_archive_bytes).await?;
        let prefetched_next_archive_id = parsed_archive.next_archive_id_hint;
        if prefetched_next_archive_id <= current_archive_id {
            return Err(anyhow!(
                "non-increasing archive id sequence: current={} next={}",
                current_archive_id,
                prefetched_next_archive_id
            ));
        }

        let next_download = {
            let s3_client = s3_client.clone();
            tokio::spawn(async move {
                download_archive_bytes(&s3_client, prefetched_next_archive_id).await
            })
        };

        let archive_result = process_parsed_archive_with_range(
            parsed_archive,
            from_timestamp,
            to_timestamp,
        )?;
        let (reached_to_timestamp, parsed_next_archive_id) = process_archive_result(
            rocksdb_client,
            parser,
            archive_result,
            to_timestamp,
            &mut max_inserted_timestamp,
            &mut summary,
        );

        if reached_to_timestamp {
            next_download.abort();
            break;
        }

        if parsed_next_archive_id != prefetched_next_archive_id {
            tracing::debug!(
                prefetched_next_archive_id,
                parsed_next_archive_id,
                "prefetched archive id adjusted after full parse"
            );
            next_download.abort();
            current_archive_id = parsed_next_archive_id;
            current_archive_bytes = download_archive_bytes(&s3_client, current_archive_id).await?;
            continue;
        }

        current_archive_id = prefetched_next_archive_id;
        current_archive_bytes = next_download
            .await
            .context("prefetch task join failed")?
            .with_context(|| format!("failed to download archive {}", current_archive_id))?;
    }

    summary.log_final("archive sync summary (final)");
    Ok(max_inserted_timestamp)
}

fn write_archive_result(
    rocksdb_client: &RocksdbClient,
    parser: &TransactionParser,
    archive_result: ArchiveProcessResult,
    to_timestamp: u32,
) -> ArchiveWriteOutcome {
    let reached_to_timestamp = archive_result.last_master_gen_utime >= to_timestamp;
    let next_archive_id = archive_result.last_master_seqno.saturating_add(1);
    let tx_count = archive_result.transactions.len();
    let blocks_in_archive = archive_result.blocks_in_archive;
    let mut transactions_to_insert = Vec::with_capacity(tx_count);
    let mut max_inserted_timestamp: Option<u32> = None;

    for transaction in archive_result.transactions {
        if buff_extracted_events(&transaction, parser).is_some() {
            max_inserted_timestamp = Some(match max_inserted_timestamp {
                Some(current) => current.max(transaction.now),
                None => transaction.now,
            });
            transactions_to_insert.push(transaction);
        }
    }
    rocksdb_client.insert_transactions_with_drain(&mut transactions_to_insert);

    ArchiveWriteOutcome {
        reached_to_timestamp,
        next_archive_id,
        tx_count,
        blocks_in_archive,
        max_inserted_timestamp,
    }
}

fn process_archive_result(
    rocksdb_client: &RocksdbClient,
    parser: &TransactionParser,
    archive_result: ArchiveProcessResult,
    to_timestamp: u32,
    max_inserted_timestamp: &mut Option<u32>,
    summary: &mut SyncSummary,
) -> (bool, u32) {
    let outcome = write_archive_result(rocksdb_client, parser, archive_result, to_timestamp);
    if let Some(current) = outcome.max_inserted_timestamp {
        *max_inserted_timestamp = Some(match *max_inserted_timestamp {
            Some(existing) => existing.max(current),
            None => current,
        });
    }
    summary.add_batch(outcome.tx_count, outcome.blocks_in_archive);
    summary.maybe_log("archive sync summary");

    (outcome.reached_to_timestamp, outcome.next_archive_id)
}

async fn process_archive_batch_window(
    rocksdb_client: &RocksdbClient,
    parser: &TransactionParser,
    s3_client: &ArchiveS3Client,
    summary: Arc<Mutex<SyncSummary>>,
    stop_requested: Arc<AtomicBool>,
    window_start: u32,
    window_end: u32,
    preferred_start: Option<u32>,
    from_timestamp: u32,
    to_timestamp: u32,
) -> Result<BatchWindowOutput> {
    let mut processed_archives = 0u64;
    let mut total_tx = 0u64;
    let mut total_blocks = 0u64;
    let mut last_archive_id = None;
    let mut overflow_next_archive_id = None;
    let mut reached_to_timestamp = false;
    let mut max_inserted_timestamp: Option<u32> = None;
    let mut current_archive_id = find_first_existing_archive_id_in_window(
        s3_client,
        stop_requested.as_ref(),
        window_start,
        window_end,
        preferred_start,
    )
    .await?;

    while let Some(archive_id) = current_archive_id {
        if stop_requested.load(Ordering::SeqCst) {
            break;
        }

        let result = load_archive_process_result(s3_client, archive_id, from_timestamp, to_timestamp)
            .await
            .with_context(|| format!("failed to process archive {}", archive_id))?;
        let write_outcome = write_archive_result(rocksdb_client, parser, result, to_timestamp);
        let next_archive_id = write_outcome.next_archive_id;

        processed_archives = processed_archives.saturating_add(1);
        total_tx = total_tx.saturating_add(write_outcome.tx_count as u64);
        total_blocks = total_blocks.saturating_add(write_outcome.blocks_in_archive as u64);
        last_archive_id = Some(archive_id);
        if let Some(current) = write_outcome.max_inserted_timestamp {
            max_inserted_timestamp = Some(match max_inserted_timestamp {
                Some(existing) => existing.max(current),
                None => current,
            });
        }
        {
            let mut summary = summary.lock().expect("archive sync summary mutex poisoned");
            summary.add_batch(write_outcome.tx_count, write_outcome.blocks_in_archive);
            summary.maybe_log("archive sync summary");
        }

        if next_archive_id <= archive_id {
            break;
        }

        if write_outcome.reached_to_timestamp {
            stop_requested.store(true, Ordering::SeqCst);
            reached_to_timestamp = true;
            break;
        }

        if stop_requested.load(Ordering::SeqCst) {
            break;
        }

        if next_archive_id > window_end {
            overflow_next_archive_id = Some(next_archive_id);
            break;
        }

        current_archive_id = find_first_existing_archive_id_in_range(
            s3_client,
            next_archive_id,
            window_end,
            WINDOW_DISCOVERY_CHUNK_SIZE,
        )
        .await?;

        if current_archive_id != Some(next_archive_id) {
            tracing::debug!(
                window_start,
                window_end,
                expected_next_archive_id = next_archive_id,
                resolved_next_archive_id = current_archive_id,
                "archive batch worker skipped missing archive ids inside window"
            );
        }
    }

    Ok(BatchWindowOutput {
        window_start,
        window_end,
        processed_archives,
        total_tx,
        total_blocks,
        last_archive_id,
        overflow_next_archive_id,
        reached_to_timestamp,
        max_inserted_timestamp,
    })
}

async fn find_first_existing_archive_id_in_window(
    s3_client: &ArchiveS3Client,
    stop_requested: &AtomicBool,
    window_start: u32,
    window_end: u32,
    preferred_start: Option<u32>,
) -> Result<Option<u32>> {
    if stop_requested.load(Ordering::SeqCst) {
        return Ok(None);
    }

    if let Some(archive_id) = preferred_start.filter(|id| *id >= window_start && *id <= window_end)
    {
        if archive_exists(s3_client, archive_id).await? {
            return Ok(Some(archive_id));
        }
    }

    find_first_existing_archive_id_in_range(
        s3_client,
        window_start,
        window_end,
        WINDOW_DISCOVERY_CHUNK_SIZE,
    )
    .await
}

async fn archive_exists(s3_client: &ArchiveS3Client, archive_id: u32) -> Result<bool> {
    let path = Path::from(format!("{archive_id}"));
    match s3_client.client.head(&path).await {
        Ok(meta) => Ok(meta.size > 0),
        Err(object_store::Error::NotFound { .. }) => Ok(false),
        Err(err) => Err(err).context("failed to query archive object in S3"),
    }
}

async fn load_archive_process_result(
    s3_client: &ArchiveS3Client,
    archive_id: u32,
    from_timestamp: u32,
    to_timestamp: u32,
) -> Result<ArchiveProcessResult> {
    let archive_bytes = download_archive_bytes(s3_client, archive_id).await?;
    parse_transactions_from_archive_bytes_with_range_blocking(
        archive_bytes,
        from_timestamp,
        to_timestamp,
    )
    .await
}

async fn load_archive_probe_result(
    s3_client: &ArchiveS3Client,
    archive_id: u32,
) -> Result<ArchiveProbeResult> {
    let archive_bytes = download_archive_bytes(s3_client, archive_id).await?;
    parse_archive_probe_blocking(archive_bytes).await
}

async fn download_archive_bytes(s3_client: &ArchiveS3Client, archive_id: u32) -> Result<Vec<u8>> {
    let path = Path::from(format!("{archive_id}"));

    let compressed_archive = s3_client
        .client
        .get(&path)
        .await
        .context("failed to get archive object from S3")?;

    let compressed_archive = compressed_archive
        .bytes()
        .await
        .context("failed to read archive body from S3")?;
    if compressed_archive.is_empty() {
        return Err(anyhow!("archive {archive_id} is empty"));
    }

    decompress_archive_payload_blocking(compressed_archive.to_vec()).await
}

fn parse_transactions_from_archive_bytes_with_range(
    data: &[u8],
    from_timestamp: u32,
    to_timestamp: u32,
) -> Result<ArchiveProcessResult> {
    let parsed_archive = parse_archive_payload(data)?;
    process_parsed_archive_with_range(parsed_archive, from_timestamp, to_timestamp)
}

async fn decompress_archive_payload_blocking(compressed_archive: Vec<u8>) -> Result<Vec<u8>> {
    tokio::task::spawn_blocking(move || {
        decode_all(std::io::Cursor::new(compressed_archive))
            .context("failed to decompress archive payload")
    })
    .await
    .context("archive decompress task join failed")?
}

async fn parse_archive_payload_blocking(data: Vec<u8>) -> Result<ParsedArchive> {
    tokio::task::spawn_blocking(move || parse_archive_payload(&data))
        .await
        .context("archive header parse task join failed")?
}

async fn parse_archive_probe_blocking(data: Vec<u8>) -> Result<ArchiveProbeResult> {
    tokio::task::spawn_blocking(move || parse_archive_probe(&data))
        .await
        .context("archive probe parse task join failed")?
}

async fn parse_transactions_from_archive_bytes_with_range_blocking(
    archive_bytes: Vec<u8>,
    from_timestamp: u32,
    to_timestamp: u32,
) -> Result<ArchiveProcessResult> {
    tokio::task::spawn_blocking(move || {
        parse_transactions_from_archive_bytes_with_range(
            &archive_bytes,
            from_timestamp,
            to_timestamp,
        )
    })
    .await
    .context("archive transactions parse task join failed")?
}

fn process_parsed_archive_with_range(
    parsed_archive: ParsedArchive,
    from_timestamp: u32,
    to_timestamp: u32,
) -> Result<ArchiveProcessResult> {
    let ParsedArchive {
        block_entries,
        next_archive_id_hint: _,
    } = parsed_archive;
    let mut transactions = Vec::new();
    let mut last_master_seqno = None;
    let mut last_master_gen_utime = None;
    for (block_id, block_data) in &block_entries {
        let block = match parse_checked_block(block_id, block_data)
            .with_context(|| format!("failed to deserialize block {block_id}"))
        {
            Ok(block) => block,
            Err(e) => {
                tracing::warn!(block_id = %block_id, error = ?e, "skip invalid block entry");
                continue;
            }
        };
        let block_timestamp = match block
            .load_info()
            .with_context(|| format!("failed to load block info {block_id}"))
        {
            Ok(info) => info.gen_utime,
            Err(e) => {
                tracing::warn!(block_id = %block_id, error = ?e, "skip block with invalid info");
                continue;
            }
        };

        if block_id.is_masterchain() {
            match last_master_seqno {
                Some(seqno) if seqno > block_id.seqno => {}
                _ => {
                    last_master_seqno = Some(block_id.seqno);
                    last_master_gen_utime = Some(block_timestamp);
                }
            }
        }

        if block_timestamp < from_timestamp || block_timestamp >= to_timestamp {
            continue;
        }

        collect_block_transactions(block_id, &block, &mut transactions)
            .with_context(|| format!("failed to extract transactions from block {block_id}"))?;
    }

    transactions.sort_by_key(|tx| (tx.now, tx.lt));

    let (last_master_seqno, last_master_gen_utime) =
        match (last_master_seqno, last_master_gen_utime) {
            (Some(seqno), Some(gen_utime)) => (seqno, gen_utime),
            _ => {
                return Err(anyhow!(
                    "archive does not contain readable masterchain blocks"
                ))
            }
        };

    Ok(ArchiveProcessResult {
        transactions,
        blocks_in_archive: block_entries.len(),
        last_master_seqno,
        last_master_gen_utime,
    })
}

fn parse_archive_payload(data: &[u8]) -> Result<ParsedArchive> {
    let mut data = data;
    read_archive_prefix(&mut data)?;

    let mut block_entries = HashMap::new();
    let mut last_header_master_seqno: Option<u32> = None;

    while data.len() >= 8 {
        let header = <ArchiveEntryHeader as TlRead>::read_from(&mut data)
            .map_err(|e| anyhow!("invalid archive entry header: {e:?}"))?;
        let data_len = header.data_len as usize;
        let block_id = header.block_id();

        let Some((entry_data, tail)) = data.split_at_checked(data_len) else {
            return Err(anyhow!("unexpected entry eof"));
        };
        data = tail;

        if header.ty == ArchiveEntryType::Block {
            if block_id.is_masterchain() {
                last_header_master_seqno = Some(match last_header_master_seqno {
                    Some(current) => current.max(block_id.seqno),
                    None => block_id.seqno,
                });
            }
            block_entries.entry(block_id).or_insert(entry_data.to_vec());
        }
    }

    let next_archive_id_hint = last_header_master_seqno
        .ok_or_else(|| anyhow!("archive does not contain masterchain block headers"))?
        .saturating_add(1);

    Ok(ParsedArchive {
        block_entries,
        next_archive_id_hint,
    })
}

fn parse_archive_probe(data: &[u8]) -> Result<ArchiveProbeResult> {
    let mut data = data;
    read_archive_prefix(&mut data)?;

    let mut last_master_seqno: Option<u32> = None;
    let mut last_master_block_id: Option<BlockId> = None;
    let mut last_master_block_data: Option<Vec<u8>> = None;

    while data.len() >= 8 {
        let header = <ArchiveEntryHeader as TlRead>::read_from(&mut data)
            .map_err(|e| anyhow!("invalid archive entry header: {e:?}"))?;
        let data_len = header.data_len as usize;
        let block_id = header.block_id();

        let Some((entry_data, tail)) = data.split_at_checked(data_len) else {
            return Err(anyhow!("unexpected entry eof"));
        };
        data = tail;

        if header.ty != ArchiveEntryType::Block || !block_id.is_masterchain() {
            continue;
        }

        let should_replace = match last_master_seqno {
            Some(current) => block_id.seqno >= current,
            None => true,
        };
        if should_replace {
            last_master_seqno = Some(block_id.seqno);
            last_master_block_id = Some(block_id);
            last_master_block_data = Some(entry_data.to_vec());
        }
    }

    let last_master_seqno =
        last_master_seqno.ok_or_else(|| anyhow!("archive does not contain masterchain block headers"))?;
    let last_master_block_id =
        last_master_block_id.ok_or_else(|| anyhow!("archive does not contain last masterchain block id"))?;
    let last_master_block_data = last_master_block_data
        .ok_or_else(|| anyhow!("archive does not contain last masterchain block data"))?;
    let last_master_block = parse_checked_block(&last_master_block_id, &last_master_block_data)
        .with_context(|| format!("failed to deserialize block {last_master_block_id}"))?;
    let last_master_gen_utime = last_master_block
        .load_info()
        .with_context(|| format!("failed to load block info {last_master_block_id}"))?
        .gen_utime;

    Ok(ArchiveProbeResult {
        next_archive_id_hint: last_master_seqno.saturating_add(1),
        last_master_gen_utime,
    })
}

fn read_archive_prefix(buf: &mut &[u8]) -> Result<()> {
    match buf.split_first_chunk() {
        Some((header, tail)) if header == &ARCHIVE_PREFIX => {
            *buf = tail;
            Ok(())
        }
        _ => Err(anyhow!("invalid archive header")),
    }
}

fn parse_checked_block(block_id: &BlockId, block_data: &[u8]) -> Result<Block> {
    let file_hash = Boc::file_hash_blake(block_data);
    anyhow::ensure!(
        block_id.file_hash.as_slice() == file_hash.as_slice(),
        "file_hash mismatch for {block_id}"
    );

    let root = Boc::decode(block_data)?;
    anyhow::ensure!(
        &block_id.root_hash == root.repr_hash(),
        "root_hash mismatch for {block_id}"
    );

    root.parse::<Block>()
        .with_context(|| format!("failed to parse block {block_id}"))
}

fn collect_block_transactions(
    block_id: &BlockId,
    block: &Block,
    output: &mut Vec<Transaction>,
) -> Result<()> {
    let extra = block.load_extra().context("failed to load block extra")?;
    let account_blocks = extra
        .account_blocks
        .load()
        .context("failed to load account blocks")?;

    for item in account_blocks.iter() {
        let (_, _, account_block) = item.context("invalid account block entry")?;

        for tx_item in account_block.transactions.values() {
            let (_, tx_cell) = tx_item.context("invalid transaction entry")?;
            let tx_boc = Boc::encode(tx_cell.inner());
            let tx = <Transaction as ton_block::Deserializable>::construct_from_bytes(&tx_boc)
                .with_context(|| format!("failed to decode transaction from block {block_id}"))?;
            output.push(tx);
        }
    }

    Ok(())
}

fn now_minus_one_day_unix() -> u32 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    now.saturating_sub(24 * 60 * 60) as u32
}

fn resolve_search_from_timestamp(cfg: &ArchiveNodeConfig, from_timestamp: u32) -> u32 {
    cfg.from_timestamp.unwrap_or(from_timestamp)
}

async fn resolve_start_archive_id(
    s3_client: &ArchiveS3Client,
    search_from_timestamp: u32,
    mode: &'static str,
) -> Result<u32> {
    if search_from_timestamp == 0 {
        tracing::info!(mode, "S3 start archive search skipped because timestamp is zero");
        return Ok(1);
    }

    let search_anchor = search_from_timestamp.saturating_sub(START_ARCHIVE_LOOKBACK_SECONDS);
    tracing::info!(
        mode,
        search_from_timestamp,
        search_anchor,
        "starting S3 archive search by timestamp"
    );
    let (start_point, stats) = find_archive_id_for_timestamp(s3_client, search_anchor).await?;
    tracing::info!(
        mode,
        search_from_timestamp,
        search_anchor,
        start_archive_id = start_point.archive_id,
        start_archive_timestamp = start_point.probe.last_master_gen_utime,
        probe_requests = stats.probe_requests,
        cache_hits = stats.cache_hits,
        elapsed_ms = stats.started_at.elapsed().as_millis(),
        "resolved initial archive id for S3 sync"
    );
    Ok(start_point.archive_id)
}

async fn find_archive_id_for_timestamp(
    s3_client: &ArchiveS3Client,
    target_timestamp: u32,
) -> Result<(ArchiveProbePoint, SearchStats)> {
    let mut stats = SearchStats {
        started_at: Instant::now(),
        probe_requests: 0,
        cache_hits: 0,
    };
    let mut probe_cache = HashMap::new();
    let mut low = probe_archive_at_or_after(s3_client, &mut probe_cache, &mut stats, 1)
        .await?
        .ok_or_else(|| anyhow!("S3 archive bucket is empty"))?;
    tracing::info!(
        archive_id = low.archive_id,
        archive_timestamp = low.probe.last_master_gen_utime,
        "S3 archive search initial probe completed"
    );
    if low.probe.last_master_gen_utime >= target_timestamp {
        return Ok((low, stats));
    }

    let mut wave = 0u32;
    let mut previous_low: Option<ArchiveProbePoint> = None;
    let high = loop {
        wave = wave.saturating_add(1);
        let candidates =
            build_bracketing_wave_candidates(&low, previous_low.as_ref(), target_timestamp);
        tracing::info!(
            wave,
            ?candidates,
            low_archive_id = low.archive_id,
            low_archive_timestamp = low.probe.last_master_gen_utime,
            "S3 archive search bracketing wave started"
        );

        let wave_started_at = Instant::now();
        let mut points =
            probe_candidates_at_or_after(s3_client, &mut probe_cache, &mut stats, &candidates)
                .await?;
        if points.is_empty() {
            let fallback_candidates = build_fallback_bracketing_wave_candidates(&low);
            tracing::info!(
                wave,
                ?fallback_candidates,
                low_archive_id = low.archive_id,
                low_archive_timestamp = low.probe.last_master_gen_utime,
                "S3 archive search primary bracketing wave missed, retrying with fallback candidates"
            );
            points = probe_candidates_at_or_after(
                s3_client,
                &mut probe_cache,
                &mut stats,
                &fallback_candidates,
            )
            .await?;
        }

        if points.is_empty() {
            tracing::info!(
                wave,
                low_archive_id = low.archive_id,
                low_archive_timestamp = low.probe.last_master_gen_utime,
                elapsed_ms = wave_started_at.elapsed().as_millis(),
                "S3 archive search reached bucket tail before target timestamp"
            );
            return Ok((low, stats));
        }

        let mut wave_low = low.clone();
        let mut wave_high = None;
        for point in points {
            if point.probe.last_master_gen_utime < target_timestamp {
                if point.archive_id > wave_low.archive_id {
                    wave_low = point;
                }
            } else if wave_high
                .as_ref()
                .map(|current: &ArchiveProbePoint| point.archive_id < current.archive_id)
                .unwrap_or(true)
            {
                wave_high = Some(point);
            }
        }

        tracing::info!(
            wave,
            low_archive_id = wave_low.archive_id,
            low_archive_timestamp = wave_low.probe.last_master_gen_utime,
            high_archive_id = wave_high.as_ref().map(|point| point.archive_id),
            high_archive_timestamp = wave_high
                .as_ref()
                .map(|point| point.probe.last_master_gen_utime),
            elapsed_ms = wave_started_at.elapsed().as_millis(),
            "S3 archive search bracketing wave completed"
        );

        if let Some(high) = wave_high {
            low = wave_low;
            break high;
        }

        anyhow::ensure!(
            wave_low.archive_id > low.archive_id,
            "archive timestamp search wave did not advance"
        );
        previous_low = Some(low.clone());
        low = wave_low;
    };

    let mut best_below = low.clone();
    let mut low_bound = low
        .probe
        .next_archive_id_hint
        .max(low.archive_id.saturating_add(1));
    let mut high_bound = high.archive_id.saturating_sub(1);
    let mut high = high;
    let mut step = 0u32;

    while low_bound <= high_bound {
        step = step.saturating_add(1);
        let estimated_id =
            estimate_search_candidate(&low, &high, low_bound, high_bound, target_timestamp);
        let step_started_at = Instant::now();
        let Some(point) =
            probe_archive_at_or_after(s3_client, &mut probe_cache, &mut stats, estimated_id)
                .await?
        else {
            tracing::info!(
                step,
                low_archive_id = low.archive_id,
                low_archive_timestamp = low.probe.last_master_gen_utime,
                high_archive_id = high.archive_id,
                high_archive_timestamp = high.probe.last_master_gen_utime,
                estimated_id,
                "S3 archive search refinement stopped because no archive exists after candidate"
            );
            break;
        };

        if point.archive_id > high_bound {
            tracing::info!(
                step,
                low_archive_id = low.archive_id,
                low_archive_timestamp = low.probe.last_master_gen_utime,
                high_archive_id = high.archive_id,
                high_archive_timestamp = high.probe.last_master_gen_utime,
                estimated_id,
                resolved_archive_id = point.archive_id,
                elapsed_ms = step_started_at.elapsed().as_millis(),
                "S3 archive search step skipped because resolved archive is outside high bound"
            );
            high_bound = estimated_id.saturating_sub(1);
            continue;
        }

        tracing::info!(
            step,
            low_archive_id = low.archive_id,
            low_archive_timestamp = low.probe.last_master_gen_utime,
            high_archive_id = high.archive_id,
            high_archive_timestamp = high.probe.last_master_gen_utime,
            estimated_id,
            resolved_archive_id = point.archive_id,
            resolved_archive_timestamp = point.probe.last_master_gen_utime,
            elapsed_ms = step_started_at.elapsed().as_millis(),
            "S3 archive search refinement step completed"
        );

        if point.probe.last_master_gen_utime < target_timestamp {
            best_below = point.clone();
            low_bound = point
                .probe
                .next_archive_id_hint
                .max(point.archive_id.saturating_add(1));
            low = point;
        } else {
            high = point.clone();
            high_bound = point.archive_id.saturating_sub(1);
        }
    }

    Ok((best_below, stats))
}

async fn probe_archive_at_or_after(
    s3_client: &ArchiveS3Client,
    probe_cache: &mut HashMap<u32, ArchiveProbeResult>,
    stats: &mut SearchStats,
    start_archive_id: u32,
) -> Result<Option<ArchiveProbePoint>> {
    let Some(archive_id) =
        find_first_existing_archive_id_at_or_after(s3_client, start_archive_id, SEARCH_EXISTENCE_CHUNK_SIZE)
            .await?
    else {
        return Ok(None);
    };

    if let Some(probe) = probe_cache.get(&archive_id).cloned() {
        stats.cache_hits = stats.cache_hits.saturating_add(1);
        return Ok(Some(ArchiveProbePoint { archive_id, probe }));
    }

    stats.probe_requests = stats.probe_requests.saturating_add(1);
    let probe = load_archive_probe_result(s3_client, archive_id)
        .await
        .with_context(|| format!("failed to probe archive {}", archive_id))?;
    probe_cache.insert(archive_id, probe.clone());
    Ok(Some(ArchiveProbePoint { archive_id, probe }))
}

async fn find_first_existing_archive_id_at_or_after(
    s3_client: &ArchiveS3Client,
    start_archive_id: u32,
    chunk_size: u32,
) -> Result<Option<u32>> {
    let range_end = start_archive_id
        .saturating_add(SEARCH_FORWARD_LOOKAHEAD_IDS.saturating_sub(1));
    find_first_existing_archive_id_in_range(s3_client, start_archive_id, range_end, chunk_size).await
}

async fn find_first_existing_archive_id_in_range(
    s3_client: &ArchiveS3Client,
    range_start: u32,
    range_end: u32,
    chunk_size: u32,
) -> Result<Option<u32>> {
    let mut chunk_start = range_start;
    while chunk_start <= range_end {
        let chunk_end = chunk_start
            .saturating_add(chunk_size.saturating_sub(1))
            .min(range_end);
        let mut checks = FuturesUnordered::new();

        for archive_id in chunk_start..=chunk_end {
            let s3_client = s3_client.clone();
            checks.push(async move {
                archive_exists(&s3_client, archive_id)
                    .await
                    .map(|exists| (archive_id, exists))
            });
        }

        let mut first_existing_archive_id: Option<u32> = None;
        while let Some(found) = checks.next().await {
            let (archive_id, exists) = found?;
            if exists {
                first_existing_archive_id = Some(match first_existing_archive_id {
                    Some(current) => current.min(archive_id),
                    None => archive_id,
                });
            }
        }

        if first_existing_archive_id.is_some() {
            return Ok(first_existing_archive_id);
        }

        if chunk_end == range_end || chunk_end == u32::MAX {
            break;
        }
        chunk_start = chunk_end + 1;
    }

    Ok(None)
}

fn build_bracketing_wave_candidates(
    low: &ArchiveProbePoint,
    previous_low: Option<&ArchiveProbePoint>,
    target_timestamp: u32,
) -> Vec<u32> {
    let start = low.probe.next_archive_id_hint.max(low.archive_id.saturating_add(1));
    let mut gap = start.saturating_sub(low.archive_id).max(SEARCH_MIN_BRACKET_GAP);

    if let Some(previous_low) = previous_low {
        let low_ts = low.probe.last_master_gen_utime;
        let prev_ts = previous_low.probe.last_master_gen_utime;
        if low_ts > prev_ts && low.archive_id > previous_low.archive_id && target_timestamp > low_ts {
            let remaining_ts = u64::from(target_timestamp.saturating_sub(low_ts));
            let id_delta = u64::from(low.archive_id.saturating_sub(previous_low.archive_id));
            let ts_delta = u64::from(low_ts.saturating_sub(prev_ts));
            let estimated_gap = remaining_ts
                .saturating_mul(id_delta)
                .checked_div(ts_delta)
                .unwrap_or_default();
            if estimated_gap > 0 {
                gap = (estimated_gap / SEARCH_BRACKET_CONCURRENCY as u64)
                    .max(u64::from(gap))
                    .min(u64::from(u32::MAX)) as u32;
            }
        }
    }

    let mut candidates = Vec::with_capacity(SEARCH_BRACKET_CONCURRENCY);
    let mut candidate = start;
    for _ in 0..SEARCH_BRACKET_CONCURRENCY {
        if candidates.last().copied() == Some(candidate) {
            break;
        }
        candidates.push(candidate);
        let next_gap = gap.max(SEARCH_MIN_BRACKET_GAP);
        candidate = candidate.saturating_add(next_gap);
    }
    candidates
}

fn build_fallback_bracketing_wave_candidates(low: &ArchiveProbePoint) -> Vec<u32> {
    let start = low.probe.next_archive_id_hint.max(low.archive_id.saturating_add(1));
    let mut candidates = Vec::with_capacity(SEARCH_BRACKET_CONCURRENCY);
    let mut gap = start
        .saturating_sub(low.archive_id)
        .max(SEARCH_MIN_BRACKET_GAP);
    let mut candidate = start;

    for _ in 0..SEARCH_BRACKET_CONCURRENCY {
        if candidates.last().copied() == Some(candidate) {
            break;
        }
        candidates.push(candidate);
        candidate = candidate.saturating_add(gap);
        gap = gap.saturating_mul(2).max(SEARCH_MIN_BRACKET_GAP);
    }

    candidates
}

async fn probe_candidates_at_or_after(
    s3_client: &ArchiveS3Client,
    probe_cache: &mut HashMap<u32, ArchiveProbeResult>,
    stats: &mut SearchStats,
    candidates: &[u32],
) -> Result<Vec<ArchiveProbePoint>> {
    let mut resolution_tasks = FuturesUnordered::new();
    for &candidate in candidates {
        let s3_client = s3_client.clone();
        resolution_tasks.push(async move {
            let archive_id = find_first_existing_archive_id_at_or_after(
                &s3_client,
                candidate,
                SEARCH_EXISTENCE_CHUNK_SIZE,
            )
            .await?;
            Ok::<_, anyhow::Error>(archive_id)
        });
    }

    let mut archive_ids = Vec::new();
    while let Some(result) = resolution_tasks.next().await {
        if let Some(archive_id) = result? {
            archive_ids.push(archive_id);
        }
    }
    archive_ids.sort_unstable();
    archive_ids.dedup();

    let mut probe_tasks = FuturesUnordered::new();
    for &archive_id in &archive_ids {
        if probe_cache.contains_key(&archive_id) {
            stats.cache_hits = stats.cache_hits.saturating_add(1);
            continue;
        }

        let s3_client = s3_client.clone();
        probe_tasks.push(async move {
            let probe = load_archive_probe_result(&s3_client, archive_id)
                .await
                .with_context(|| format!("failed to probe archive {}", archive_id))?;
            Ok::<_, anyhow::Error>((archive_id, probe))
        });
    }

    while let Some(result) = probe_tasks.next().await {
        let (archive_id, probe) = result?;
        stats.probe_requests = stats.probe_requests.saturating_add(1);
        probe_cache.insert(archive_id, probe);
    }

    Ok(archive_ids
        .into_iter()
        .filter_map(|archive_id| {
            probe_cache
                .get(&archive_id)
                .cloned()
                .map(|probe| ArchiveProbePoint { archive_id, probe })
        })
        .collect())
}

fn estimate_search_candidate(
    low: &ArchiveProbePoint,
    high: &ArchiveProbePoint,
    low_bound: u32,
    high_bound: u32,
    target_timestamp: u32,
) -> u32 {
    if low_bound >= high_bound {
        return low_bound;
    }

    let low_ts = low.probe.last_master_gen_utime;
    let high_ts = high.probe.last_master_gen_utime;
    if high_ts <= low_ts || target_timestamp <= low_ts || target_timestamp >= high_ts {
        return low_bound + (high_bound - low_bound) / 2;
    }

    let id_span = u64::from(high.archive_id.saturating_sub(low.archive_id));
    let ts_span = u64::from(high_ts.saturating_sub(low_ts));
    if id_span == 0 || ts_span == 0 {
        return low_bound + (high_bound - low_bound) / 2;
    }

    let ts_offset = u64::from(target_timestamp.saturating_sub(low_ts));
    let estimated_offset = ts_offset.saturating_mul(id_span) / ts_span;
    let estimated_id = low.archive_id.saturating_add(estimated_offset as u32);
    estimated_id.clamp(low_bound, high_bound)
}

fn batch_window_start(archive_id: u32, batch_size: u32) -> u32 {
    let zero_based = archive_id.saturating_sub(1);
    zero_based - (zero_based % batch_size) + 1
}

fn batch_window_end(window_start: u32, batch_size: u32) -> u32 {
    window_start
        .saturating_add(batch_size.saturating_sub(1))
}

#[allow(clippy::too_many_arguments)]
fn log_archive_sync_summary(
    label: &'static str,
    elapsed: Duration,
    window_archives: u64,
    window_tx: u64,
    window_blocks: u64,
    total_archives: u64,
    total_tx: u64,
    total_blocks: u64,
) {
    let elapsed_secs = elapsed.as_secs_f64().max(0.001);
    let archives_per_sec = (window_archives as f64 / elapsed_secs) as f32;
    let tx_per_sec = (window_tx as f64 / elapsed_secs) as f32;
    let blocks_per_sec = (window_blocks as f64 / elapsed_secs) as f32;

    tracing::info!(
        tx_per_sec,
        blocks_per_sec,
        archives_per_sec,
        total_tx,
        total_blocks,
        total_archives,
        "{label}"
    );
}

fn build_s3_client(cfg: &ArchiveNodeConfig) -> Result<ArchiveS3Client> {
    let access_key = required_s3_value("AK", &cfg.access_key)?;
    let secret_key = required_s3_value("SK", &cfg.secret_key)?;
    let endpoint = required_s3_value("url", &cfg.endpoint)?;
    let region = required_s3_value("region", &cfg.region)?;
    let bucket = required_s3_value("bucket", &cfg.bucket)?;

    let client = object_store::aws::AmazonS3Builder::new()
        .with_region(region)
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_client_options(ClientOptions::new().with_allow_http(true))
        .build()
        .context("failed to build S3 client")?;

    Ok(ArchiveS3Client {
        client: std::sync::Arc::new(client),
    })
}

fn required_s3_value(key: &str, value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(anyhow!("empty S3 config key: {}", key));
    }

    Ok(value.to_string())
}

fn resolve_parallel_archive_config(cfg: &ArchiveNodeConfig) -> Result<Option<ParallelArchiveConfig>> {
    match cfg.max_parallel_workers {
        Some(0) => Err(anyhow!(
            "archive max_parallel_workers must be greater than zero"
        )),
        Some(max_parallel_workers) => {
            let archive_batch_size = cfg.archive_batch_size.unwrap_or(DEFAULT_ARCHIVE_BATCH_SIZE);
            anyhow::ensure!(
                archive_batch_size > 0,
                "archive_batch_size must be greater than zero"
            );
            tracing::info!(
                max_parallel_workers,
                archive_batch_size,
                s3_endpoint = cfg.endpoint,
                s3_bucket = cfg.bucket,
                "archive parallel workers configured"
            );
            Ok(Some(ParallelArchiveConfig {
                max_parallel_workers,
                archive_batch_size,
            }))
        }
        None => {
            tracing::info!(
                s3_endpoint = cfg.endpoint,
                s3_bucket = cfg.bucket,
                "archive max_parallel_workers is not set, using prefetch=1 mode"
            );
            Ok(None)
        }
    }
}
