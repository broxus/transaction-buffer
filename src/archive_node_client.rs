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
    sync_from_s3_range(rocksdb_client, parser, cfg, 0, cutoff_timestamp).await
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
    let mut next_batch_start = 1u32;
    let mut window_start_hints: HashMap<u32, u32> = HashMap::new();
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
    let mut current_archive_id = 1u32;
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

        current_archive_id = Some(next_archive_id);
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

    let mut chunk_start = window_start;
    while chunk_start <= window_end {
        if stop_requested.load(Ordering::SeqCst) {
            return Ok(None);
        }

        let chunk_end = chunk_start
            .saturating_add(WINDOW_DISCOVERY_CHUNK_SIZE.saturating_sub(1))
            .min(window_end);
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

        chunk_start = match chunk_end.checked_add(1) {
            Some(next) => next,
            None => break,
        };
    }

    Ok(None)
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
