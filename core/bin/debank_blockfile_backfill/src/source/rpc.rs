//! RPC-mode source: reads from official chain RPC.
//!
//! Known field degradation versus PG mode (see backfill-redesign.md §"字段完整度对比"):
//! - `error_events = []` (no `parent_failed` propagation from RPC)
//! - `storage_change = false` / `self_storage_change = false` (DebugCall protocol doesn't carry this)
//! - events `parent_trace_id = ""`, `pos_in_parent_trace = 0` (eth_getLogs is flat, not tied to trace tree)
//!
//! traces / error_traces ARE classified correctly by `revert_reason` (DebugCall has it),
//! unlike the legacy `zksync_s3_backfill` which puts everything in `traces`.

use async_trait::async_trait;
use zksync_types::debank::{BlockMeta, TxBlockData};

use super::Source;

pub struct RpcSource {
    // TODO: zksync_web3_decl Client<L2>, chain_id
}

impl RpcSource {
    pub async fn new(_rpc_url: String, _chain_id: u64) -> anyhow::Result<Self> {
        anyhow::bail!("RpcSource not yet implemented (Phase 2.5)")
    }
}

#[async_trait]
impl Source for RpcSource {
    async fn get_block_data(
        &self,
        _block_num: u32,
    ) -> anyhow::Result<(BlockMeta, Vec<TxBlockData>)> {
        unimplemented!("RpcSource::get_block_data (Phase 2.5)")
    }
}
