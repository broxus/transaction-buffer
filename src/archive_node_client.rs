use crate::models::ArchiveNodeConfig;
use crate::rocksdb_client::RocksdbClient;
use crate::utils::buff_extracted_events;
use anyhow::{anyhow, Context, Result};
use nekoton_abi::TransactionParser;
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore};
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tl_proto::{TlPacket, TlRead, TlResult, TlWrite};
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
}

struct ArchiveProcessResult {
    transactions: Vec<Transaction>,
    last_master_seqno: u32,
    last_master_gen_utime: u32,
}

#[derive(Clone)]
struct ArchiveS3Client {
    client: std::sync::Arc<dyn ObjectStore>,
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

    let s3_client = build_s3_client(cfg)?;
    let mut current_archive_id = 1u32;
    let mut max_inserted_timestamp: Option<u32> = None;

    loop {
        let archive_result = process_archive_with_range(
            &s3_client,
            current_archive_id,
            from_timestamp,
            to_timestamp,
        )
        .await?;

        let reached_to_timestamp = archive_result.last_master_gen_utime >= to_timestamp;
        let next_archive_id = archive_result.last_master_seqno.saturating_add(1);

        let loaded_len = archive_result.transactions.len();
        let mut transactions_to_insert = Vec::with_capacity(loaded_len);
        for transaction in archive_result.transactions {
            if buff_extracted_events(&transaction, parser).is_some() {
                max_inserted_timestamp = Some(match max_inserted_timestamp {
                    Some(current) => current.max(transaction.now),
                    None => transaction.now,
                });
                transactions_to_insert.push(transaction);
            }
        }
        let inserted_len = transactions_to_insert.len();
        rocksdb_client.insert_transactions_with_drain(&mut transactions_to_insert);

        tracing::info!(
            archive_id = current_archive_id,
            loaded_len,
            inserted_len,
            reached_to_timestamp,
            max_inserted_timestamp = max_inserted_timestamp.unwrap_or_default(),
            "archive batch synced"
        );

        if reached_to_timestamp {
            break;
        }
        current_archive_id = next_archive_id;
    }

    Ok(max_inserted_timestamp)
}

async fn process_archive_with_range(
    s3_client: &ArchiveS3Client,
    archive_id: u32,
    from_timestamp: u32,
    to_timestamp: u32,
) -> Result<ArchiveProcessResult> {
    let archive_bytes = download_archive_bytes(s3_client, archive_id).await?;
    parse_transactions_from_archive_bytes_with_range(&archive_bytes, from_timestamp, to_timestamp)
}

async fn download_archive_bytes(s3_client: &ArchiveS3Client, archive_id: u32) -> Result<Vec<u8>> {
    let path = Path::from(format!("{archive_id}"));
    let archive_info = s3_client
        .client
        .head(&path)
        .await
        .context("failed to query archive in S3")?;
    if archive_info.size == 0 {
        return Err(anyhow!("archive {archive_id} not found in S3 bucket"));
    }

    let compressed_archive = s3_client
        .client
        .get(&path)
        .await
        .context("failed to get archive object from S3")?
        .bytes()
        .await
        .context("failed to read archive body from S3")?;

    decode_all(std::io::Cursor::new(compressed_archive))
        .context("failed to decompress archive payload")
}

fn parse_transactions_from_archive_bytes_with_range(
    data: &[u8],
    from_timestamp: u32,
    to_timestamp: u32,
) -> Result<ArchiveProcessResult> {
    let parsed_archive = parse_archive_payload(data)?;

    let mut transactions = Vec::new();
    let mut last_master_seqno = None;
    let mut last_master_gen_utime = None;
    for (block_id, block_data) in &parsed_archive.block_entries {
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
        last_master_seqno,
        last_master_gen_utime,
    })
}

fn parse_archive_payload(data: &[u8]) -> Result<ParsedArchive> {
    let mut data = data;
    read_archive_prefix(&mut data)?;

    let mut block_entries = HashMap::new();

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
            block_entries.entry(block_id).or_insert(entry_data.to_vec());
        }
    }

    Ok(ParsedArchive { block_entries })
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
