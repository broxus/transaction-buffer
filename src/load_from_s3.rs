use crate::archive_node_client::sync_from_s3_range;
use crate::models::ArchiveNodeConfig;
use crate::rocksdb_client::RocksdbClient;
use anyhow::Result;
use nekoton_abi::TransactionParser;

#[tracing::instrument(
    level = "info",
    skip(rocksdb_client, parser, cfg),
    fields(from_timestamp, to_timestamp, s3_endpoint = %cfg.endpoint, s3_bucket = %cfg.bucket)
)]
pub async fn load_from_s3(
    rocksdb_client: &RocksdbClient,
    parser: &TransactionParser,
    from_timestamp: u32,
    to_timestamp: u32,
    cfg: &ArchiveNodeConfig,
) -> Result<()> {
    let max_inserted_timestamp =
        sync_from_s3_range(rocksdb_client, parser, cfg, from_timestamp, to_timestamp).await?;

    tracing::info!(
        max_inserted_timestamp = max_inserted_timestamp.unwrap_or_default(),
        "load from s3 completed"
    );
    Ok(())
}
