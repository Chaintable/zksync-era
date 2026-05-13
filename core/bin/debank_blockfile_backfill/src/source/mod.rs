//! Block data sources for backfill: PG (full fidelity) and RPC (degraded).

pub mod pg;
pub mod rpc;

use async_trait::async_trait;
use zksync_types::debank::{BlockMeta, TxBlockData};

/// Source of block-level data for backfill.
///
/// Two implementations:
/// - [`pg::PgSource`]: reads from local PG `call_traces` table, field-equivalent to EN realtime.
/// - [`rpc::RpcSource`]: reads from official chain RPC, with known field degradation
///   (`error_events=[]`, `storage_change=false`, events have no parent_trace_id).
///
/// Each `get_block_data` call is self-contained: the source looks up its own
/// `parent_hash` (from block N-1's header). This way the main loop can tolerate
/// per-block failures (`--skip-on-error`) without losing the parent_hash chain.
#[async_trait]
pub trait Source: Send + Sync {
    /// Fetch one block's metadata + per-tx data. Self-contained — no loop state.
    async fn get_block_data(
        &self,
        block_num: u32,
    ) -> anyhow::Result<(BlockMeta, Vec<TxBlockData>)>;
}
