use std::{
    collections::HashMap,
    future::Future,
    time::{Duration, Instant},
};

use anyhow::Context as _;
use aws_sdk_s3::Client as S3Client;
use clap::Parser;
use futures::StreamExt;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tracing::{info, warn};
use zksync_types::{
    debank::build_state_diff, AccountTreeId, Address, StorageKey, StorageValue, H256,
};

const MAX_RETRIES: u32 = 5;

/// Retry an async operation with exponential backoff (2s, 4s, 8s, 16s, 32s).
async fn retry_s3<F, Fut, T, E>(label: &str, f: F) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0u32;
    loop {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                attempt += 1;
                if attempt > MAX_RETRIES {
                    return Err(e);
                }
                let delay = 1u64 << attempt; // 2, 4, 8, 16, 32 seconds
                warn!("{label}: attempt {attempt}/{MAX_RETRIES} failed: {e}. Retrying in {delay}s...");
                tokio::time::sleep(Duration::from_secs(delay)).await;
            }
        }
    }
}

#[derive(Debug, Clone, Parser)]
pub struct ExportStateDiffsArgs {
    /// PostgreSQL connection URL (e.g. postgres://postgres@localhost:5432/mainnet2).
    /// Defaults to DATABASE_URL or EN_DATABASE_URL environment variable.
    #[arg(long, env = "DATABASE_URL")]
    pub db_url: Option<String>,

    /// Target S3 bucket for state diffs.
    #[arg(
        long,
        env = "STATE_DIFF_S3_BUCKET",
        default_value = "chaintable-nodex-pipeline--apne1-az4--x-s3"
    )]
    pub s3_bucket: String,

    /// S3 key prefix (chain id).
    #[arg(long, env = "STATE_DIFF_S3_PREFIX", default_value = "324")]
    pub s3_prefix: String,

    /// Optional version segment for S3 path (e.g. "d5140dd7").
    /// When specified, state diffs are uploaded to {s3_prefix}/{version}/{block_hash}/stateDiff.
    #[arg(long, env = "STATE_DIFF_VERSION")]
    pub version: Option<String>,

    /// Start miniblock number (inclusive).
    #[arg(long, default_value_t = 0)]
    pub from: u64,

    /// End miniblock number (inclusive). If not set, defaults to the highest miniblock in the DB.
    #[arg(long)]
    pub to: Option<u64>,

    /// Batch size for DB queries (number of miniblocks per query batch).
    #[arg(long, default_value_t = 1000)]
    pub batch_size: u64,

    /// Number of concurrent S3 uploads.
    #[arg(long, default_value_t = 32)]
    pub concurrency: usize,

    /// Skip blocks that already have stateDiff in S3 (checks via S3 HeadObject).
    #[arg(long)]
    pub skip_existing: bool,

    /// Dry run: query DB and generate state diffs without uploading to S3.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Debug, Clone)]
struct MiniblockHeader {
    number: u64,
    hash: H256,
    parent_hash: H256,
}

impl ExportStateDiffsArgs {
    pub fn full_s3_prefix(&self) -> String {
        match &self.version {
            Some(v) if !v.is_empty() => format!("{}/{}", self.s3_prefix, v),
            _ => self.s3_prefix.clone(),
        }
    }

    pub async fn run(&self) -> anyhow::Result<()> {
        let db_url = self
            .db_url
            .clone()
            .or_else(|| std::env::var("EN_DATABASE_URL").ok())
            .or_else(|| std::env::var("DATABASE_URL").ok())
            .context("Database URL must be provided via --db-url or DATABASE_URL/EN_DATABASE_URL env var")?;

        info!("Connecting to PostgreSQL database at {db_url}...");
        let pool = PgPoolOptions::new()
            .max_connections((self.concurrency as u32).clamp(10, 50))
            .acquire_timeout(Duration::from_secs(30))
            .connect(&db_url)
            .await
            .context("Failed to connect to PostgreSQL")?;

        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        let s3_client = S3Client::new(&aws_config);

        let latest_block: i64 = sqlx::query_scalar("SELECT number FROM miniblocks ORDER BY number DESC LIMIT 1")
            .fetch_one(&pool)
            .await
            .context("Failed to fetch latest miniblock from database")?;

        let to_block = self.to.unwrap_or(latest_block as u64);
        let from_block = self.from;

        anyhow::ensure!(
            from_block <= to_block,
            "Start block ({from_block}) must be <= end block ({to_block})"
        );

        let total_blocks = to_block - from_block + 1;
        let full_prefix = self.full_s3_prefix();
        info!(
            "Starting export-state-diffs: blocks {} to {} (total {} blocks) | bucket: {} | prefix: {} | batch_size: {} | concurrency: {} | dry_run: {}",
            from_block,
            to_block,
            total_blocks,
            self.s3_bucket,
            full_prefix,
            self.batch_size,
            self.concurrency,
            self.dry_run
        );

        let start_time = Instant::now();
        let mut processed_blocks = 0u64;
        let mut prev_block_hash_cache: Option<(u64, H256)> = None;

        let mut current_from = from_block;
        while current_from <= to_block {
            let current_to = std::cmp::min(current_from + self.batch_size - 1, to_block);
            let batch_start_time = Instant::now();

            self.process_batch(
                &pool,
                &s3_client,
                current_from,
                current_to,
                &mut prev_block_hash_cache,
            )
            .await
            .with_context(|| format!("Failed to process batch {current_from}..={current_to}"))?;

            let batch_count = current_to - current_from + 1;
            processed_blocks += batch_count;

            let elapsed_total = start_time.elapsed().as_secs_f64();
            let total_speed = if elapsed_total > 0.0 {
                processed_blocks as f64 / elapsed_total
            } else {
                0.0
            };
            let batch_elapsed = batch_start_time.elapsed().as_secs_f64();
            let batch_speed = if batch_elapsed > 0.0 {
                batch_count as f64 / batch_elapsed
            } else {
                0.0
            };
            let progress_pct = (processed_blocks as f64 / total_blocks as f64) * 100.0;

            info!(
                "[export-state-diffs] Blocks {}..={} ({} blks) in {:.2}s ({:.1} blk/s) | Total: {}/{} ({:.2}%) at {:.1} blk/s avg",
                current_from,
                current_to,
                batch_count,
                batch_elapsed,
                batch_speed,
                processed_blocks,
                total_blocks,
                progress_pct,
                total_speed
            );

            current_from = current_to + 1;
        }

        info!(
            "Successfully exported state diffs for {} blocks in {:.2}s!",
            processed_blocks,
            start_time.elapsed().as_secs_f64()
        );
        Ok(())
    }

    async fn process_batch(
        &self,
        pool: &PgPool,
        s3_client: &S3Client,
        batch_from: u64,
        batch_to: u64,
        prev_block_hash_cache: &mut Option<(u64, H256)>,
    ) -> anyhow::Result<()> {
        // 1. Fetch miniblocks metadata
        let miniblock_rows = sqlx::query(
            "SELECT number, hash FROM miniblocks WHERE number >= $1 AND number <= $2 ORDER BY number ASC",
        )
        .bind(batch_from as i64)
        .bind(batch_to as i64)
        .fetch_all(pool)
        .await
        .context("Failed to query miniblocks")?;

        if miniblock_rows.is_empty() {
            return Ok(());
        }

        let mut miniblocks = Vec::with_capacity(miniblock_rows.len());
        for row in miniblock_rows {
            let number: i64 = row.get("number");
            let hash_bytes: Vec<u8> = row.get("hash");
            let hash = H256::from_slice(&hash_bytes);
            let num = number as u64;

            let parent_hash = if num == 0 {
                H256::zero()
            } else if let Some((prev_num, prev_hash)) = prev_block_hash_cache {
                if *prev_num == num - 1 {
                    *prev_hash
                } else {
                    Self::fetch_single_block_hash(pool, num - 1).await?
                }
            } else {
                Self::fetch_single_block_hash(pool, num - 1).await?
            };

            *prev_block_hash_cache = Some((num, hash));
            miniblocks.push(MiniblockHeader {
                number: num,
                hash,
                parent_hash,
            });
        }

        // 2. Fetch storage logs for the batch range
        let storage_log_rows = sqlx::query(
            "SELECT address, key, value, miniblock_number \
             FROM storage_logs \
             WHERE miniblock_number >= $1 AND miniblock_number <= $2 \
             ORDER BY miniblock_number ASC, operation_number ASC",
        )
        .bind(batch_from as i64)
        .bind(batch_to as i64)
        .fetch_all(pool)
        .await
        .context("Failed to query storage logs")?;

        let mut miniblock_writes: HashMap<u64, Vec<(StorageKey, StorageValue)>> = HashMap::new();
        for row in storage_log_rows {
            let mb_num: i64 = row.get("miniblock_number");
            let addr_opt: Option<Vec<u8>> = row.get("address");
            let key_opt: Option<Vec<u8>> = row.get("key");
            let val_bytes: Vec<u8> = row.get("value");

            if let (Some(addr), Some(key)) = (addr_opt, key_opt) {
                if addr.len() == 20 && key.len() == 32 && val_bytes.len() == 32 {
                    let storage_key = StorageKey::new(
                        AccountTreeId::new(Address::from_slice(&addr)),
                        H256::from_slice(&key),
                    );
                    let storage_val = H256::from_slice(&val_bytes);
                    miniblock_writes
                        .entry(mb_num as u64)
                        .or_default()
                        .push((storage_key, storage_val));
                }
            }
        }

        // 3. Fetch factory deps for the batch range
        let factory_dep_rows = sqlx::query(
            "SELECT bytecode_hash, bytecode, miniblock_number \
             FROM factory_deps \
             WHERE miniblock_number >= $1 AND miniblock_number <= $2",
        )
        .bind(batch_from as i64)
        .bind(batch_to as i64)
        .fetch_all(pool)
        .await
        .context("Failed to query factory deps")?;

        let mut miniblock_deps: HashMap<u64, Vec<(H256, Vec<u8>)>> = HashMap::new();
        for row in factory_dep_rows {
            let mb_num: i64 = row.get("miniblock_number");
            let hash_bytes: Vec<u8> = row.get("bytecode_hash");
            let bytecode: Vec<u8> = row.get("bytecode");
            if hash_bytes.len() == 32 {
                let hash = H256::from_slice(&hash_bytes);
                miniblock_deps.entry(mb_num as u64).or_default().push((hash, bytecode));
            }
        }

        // 4. Build BlockStorageDiff and RLP-encode
        let mut upload_items = Vec::with_capacity(miniblocks.len());
        for mb in miniblocks {
            let writes = miniblock_writes.remove(&mb.number).unwrap_or_default();
            let deps = miniblock_deps.remove(&mb.number).unwrap_or_default();
            let deps_refs: Vec<(&H256, &[u8])> = deps.iter().map(|(h, c)| (h, c.as_slice())).collect();

            let diff = build_state_diff(mb.hash, mb.parent_hash, writes, deps_refs);
            let rlp_bytes = diff.encode_rlp();
            upload_items.push((mb.number, mb.hash, rlp_bytes));
        }

        // 5. Concurrently upload to S3
        let bucket = self.s3_bucket.clone();
        let prefix = self.full_s3_prefix();
        let skip_existing = self.skip_existing;
        let dry_run = self.dry_run;

        let results = futures::stream::iter(upload_items)
            .map(|(num, hash, rlp_bytes)| {
                let s3 = s3_client.clone();
                let bucket = bucket.clone();
                let prefix = prefix.clone();
                async move {
                    let key = format!("{prefix}/{hash:#x}/stateDiff");
                    if dry_run {
                        return Ok(());
                    }

                    if skip_existing {
                        let head_res = s3.head_object().bucket(&bucket).key(&key).send().await;
                        if head_res.is_ok() {
                            return Ok(());
                        }
                    }

                    retry_s3(&format!("Upload stateDiff for block {num}"), || {
                        let s3 = &s3;
                        let bucket = &bucket;
                        let key = &key;
                        let bytes = &rlp_bytes;
                        async move {
                            s3.put_object()
                                .bucket(bucket)
                                .key(key)
                                .body(bytes.clone().into())
                                .send()
                                .await
                        }
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to upload {key} for block {num}: {e}"))?;

                    Ok::<_, anyhow::Error>(())
                }
            })
            .buffer_unordered(self.concurrency)
            .collect::<Vec<anyhow::Result<()>>>()
            .await;

        for res in results {
            res?;
        }

        Ok(())
    }

    async fn fetch_single_block_hash(pool: &PgPool, block_number: u64) -> anyhow::Result<H256> {
        let hash_bytes: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT hash FROM miniblocks WHERE number = $1")
                .bind(block_number as i64)
                .fetch_optional(pool)
                .await
                .context("Failed to query block hash")?;

        match hash_bytes {
            Some(bytes) if bytes.len() == 32 => Ok(H256::from_slice(&bytes)),
            _ => {
                warn!("Could not find hash for block {block_number}, defaulting to zero hash");
                Ok(H256::zero())
            }
        }
    }
}
