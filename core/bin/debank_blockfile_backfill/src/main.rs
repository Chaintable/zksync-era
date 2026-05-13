//! Standalone binary to backfill DeBank BlockFile to S3 for a block range.
//!
//! Two data sources (selected by `--source`):
//! - `pg`: reads from local PG `call_traces`, produces field-equivalent output
//!   to EN realtime upload. Use for chains where EN PG has all needed blocks
//!   (e.g. Lens; ABS after snapshot height).
//! - `rpc`: reads from official chain RPC, with known field degradation
//!   (no `error_events`, no `storage_change`, events not tied to trace tree).
//!   Use for chains without local EN data (e.g. ABS before snapshot height).
//!
//! Output:
//! - `--output-dir <path>`: gzipped JSON files under `<path>/<chain_id>/[<version>/]`
//!   (subdirs `header/`, `blockfile/`, `validation/`). For diffing against S3.
//! - `--upload-s3`: actually upload to S3 (off by default).
//!
//! Concurrency & resumability:
//! - `--concurrency N`: process up to N blocks in parallel (default 1). PG
//!   pool size auto-sizes to match.
//! - `--progress-file <path>`: append each completed block_num to this file.
//!   On startup, load the file and skip already-completed blocks. Combined
//!   with abort-on-error this lets you resume a failed run from where it left off.

mod source;
mod upload;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use aws_sdk_s3::Client as S3Client;
use futures::stream::{self, StreamExt, TryStreamExt};
use structopt::StructOpt;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use zksync_types::{
    debank::{assemble_block_file, DebankOutPut},
    web3::Bytes,
};

use crate::source::{pg::PgSource, rpc::RpcSource, Source};
use crate::upload::{upload_to_s3, write_local};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceKind {
    Pg,
    Rpc,
}

impl FromStr for SourceKind {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s {
            "pg" => Ok(Self::Pg),
            "rpc" => Ok(Self::Rpc),
            other => anyhow::bail!("unknown --source: {} (expected pg|rpc)", other),
        }
    }
}

#[derive(StructOpt)]
#[structopt(
    name = "debank_blockfile_backfill",
    about = "Backfill DeBank BlockFile to S3 from PG or official RPC"
)]
struct Opt {
    #[structopt(long)]
    chain_id: u64,

    #[structopt(long)]
    source: SourceKind,

    #[structopt(long)]
    start_block: u32,

    /// Exclusive upper bound.
    #[structopt(long)]
    end_block: u32,

    /// PG mode only: postgres URL (e.g. postgres://postgres@host:5432/zksync).
    #[structopt(long)]
    pg_url: Option<String>,

    /// RPC mode only: official chain RPC URL.
    #[structopt(long)]
    rpc_url: Option<String>,

    /// Number of blocks to process in parallel. Default 1 (sequential).
    /// PG ConnectionPool size auto-sizes to this.
    #[structopt(long, default_value = "1")]
    concurrency: u32,

    /// If set, append each completed block_num to this file. On startup, load
    /// the file and skip already-completed blocks. Enables resume after abort.
    #[structopt(long, parse(from_os_str))]
    progress_file: Option<PathBuf>,

    /// If set, write each block's output as gzipped JSON under this directory
    /// (layout: `<dir>/<chain>/[<version>/]header,blockfile,validation/...`).
    /// Useful for diffing against S3 baseline before flipping `--upload-s3`.
    #[structopt(long, parse(from_os_str))]
    output_dir: Option<PathBuf>,

    /// Upload to S3. Off by default to avoid clobbering EN realtime data
    /// on first run.
    #[structopt(long)]
    upload_s3: bool,

    /// S3 path version segment (e.g. "b8becb11" for Lens). Must match EN
    /// realtime config if both are uploading to the same bucket.
    #[structopt(long)]
    s3_version: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let opt = Opt::from_args();

    if opt.start_block >= opt.end_block {
        anyhow::bail!(
            "--start-block ({}) must be < --end-block ({})",
            opt.start_block,
            opt.end_block
        );
    }
    if opt.concurrency == 0 {
        anyhow::bail!("--concurrency must be >= 1");
    }

    let source: Arc<dyn Source> = match opt.source {
        SourceKind::Pg => Arc::new(
            PgSource::new(
                opt.pg_url.context("--pg-url required for --source pg")?,
                opt.concurrency,
            )
            .await?,
        ),
        SourceKind::Rpc => Arc::new(
            RpcSource::new(
                opt.rpc_url
                    .context("--rpc-url required for --source rpc")?,
                opt.chain_id,
            )
            .await?,
        ),
    };

    let s3 = if opt.upload_s3 {
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        Some(Arc::new(S3Client::new(&aws_config)))
    } else {
        None
    };

    if opt.output_dir.is_none() && s3.is_none() {
        tracing::warn!(
            "Neither --output-dir nor --upload-s3 is set; running in dry-run mode (assembles output but writes nowhere)."
        );
    }

    let progress = match &opt.progress_file {
        Some(path) => Some(Arc::new(Progress::load(path).await?)),
        None => None,
    };

    let already_done = progress
        .as_ref()
        .map(|p| p.snapshot_done())
        .unwrap_or_default();
    let total = (opt.start_block..opt.end_block).count();
    let skipped = (opt.start_block..opt.end_block)
        .filter(|n| already_done.contains(n))
        .count();
    if skipped > 0 {
        tracing::info!(
            "Resuming: {} block(s) already in progress file, will skip those",
            skipped
        );
    }

    let chain_id = opt.chain_id;
    let s3_version = opt.s3_version.clone();
    let output_dir = opt.output_dir.clone();

    stream::iter(opt.start_block..opt.end_block)
        .filter(move |n| {
            let skip = already_done.contains(n);
            async move { !skip }
        })
        .map(Ok::<u32, anyhow::Error>)
        .try_for_each_concurrent(opt.concurrency as usize, |block_num| {
            let source = source.clone();
            let s3 = s3.clone();
            let progress = progress.clone();
            let s3_version = s3_version.clone();
            let output_dir = output_dir.clone();
            async move {
                process_block(
                    &*source,
                    block_num,
                    chain_id,
                    s3_version.as_deref(),
                    output_dir.as_deref(),
                    s3.as_deref(),
                )
                .await?;
                if let Some(p) = &progress {
                    p.record(block_num).await?;
                }
                Ok(())
            }
        })
        .await?;

    tracing::info!(
        "Backfill complete (total={}, skipped_from_progress={}, processed={})",
        total,
        skipped,
        total - skipped,
    );
    Ok(())
}

async fn process_block(
    source: &dyn Source,
    block_num: u32,
    chain_id: u64,
    s3_version: Option<&str>,
    output_dir: Option<&Path>,
    s3: Option<&S3Client>,
) -> anyhow::Result<()> {
    let (block_meta, tx_results) = source.get_block_data(block_num).await?;
    let (block_file, header) = assemble_block_file(block_meta, tx_results);
    let validation_hash = block_file.validation().validation_hash;
    let output = DebankOutPut {
        block_file,
        header,
        state_diff: Bytes::default(),
        validation_hash,
    };

    if let Some(dir) = output_dir {
        write_local(dir, chain_id, s3_version, &output)?;
    }
    if let Some(client) = s3 {
        upload_to_s3(client, chain_id, s3_version, &output).await?;
    }

    tracing::info!(
        "Block {} done: {} txs, {} traces, {} error_traces, {} events, {} error_events",
        block_num,
        output.block_file.transactions.len(),
        output.block_file.traces.len(),
        output.block_file.error_traces.len(),
        output.block_file.events.len(),
        output.block_file.error_events.len(),
    );
    Ok(())
}

/// Append-only progress log: each completed block_num written on its own line.
/// On startup, parsed into a HashSet so already-done blocks are skipped.
struct Progress {
    done: HashSet<u32>,
    file: Mutex<tokio::fs::File>,
}

impl Progress {
    async fn load(path: &Path) -> anyhow::Result<Self> {
        let done: HashSet<u32> = if path.exists() {
            let content = tokio::fs::read_to_string(path)
                .await
                .with_context(|| format!("read progress file {}", path.display()))?;
            content
                .lines()
                .filter_map(|l| l.trim().parse::<u32>().ok())
                .collect()
        } else {
            HashSet::new()
        };
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .with_context(|| format!("open progress file {}", path.display()))?;
        Ok(Self {
            done,
            file: Mutex::new(file),
        })
    }

    fn snapshot_done(&self) -> HashSet<u32> {
        self.done.clone()
    }

    async fn record(&self, block_num: u32) -> anyhow::Result<()> {
        let mut file = self.file.lock().await;
        file.write_all(format!("{}\n", block_num).as_bytes())
            .await
            .with_context(|| format!("append block {} to progress file", block_num))?;
        file.flush().await?;
        Ok(())
    }
}
