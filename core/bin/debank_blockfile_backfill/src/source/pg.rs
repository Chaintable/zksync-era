//! PG-mode source: reads from local PG `call_traces`, produces field-equivalent
//! output to EN realtime upload.
//!
//! Standardization flow (must match `debank_s3_persistence::build_debank_output`):
//!   1. `call.trace_id = to_hash(&[tx_hash, "", "0"])`
//!   2. `for (i, sub) in call.calls.iter_mut() { sub.pos_in_parent_trace = i; }`
//!   3. `set_parent_failed(&mut call, false)`
//!   4. root → traces or error_traces (by `revert_reason || parent_failed`,
//!      though for root `parent_failed` is always false)
//!   5. `add_trace_log(...)` recurses, populating traces/error_traces/events/error_events

use std::str::FromStr;

use anyhow::Context;
use async_trait::async_trait;
use zksync_basic_types::L2BlockNumber;
use zksync_dal::{ConnectionPool, Core, CoreDal};
use zksync_multivm::{interface::Call, tracers::debank};
use zksync_types::{
    api,
    debank::{BlockMeta, DebankEvent, DebankTrace, DebankTransaction, TxBlockData},
    l2::TransactionType,
    url::SensitiveUrl,
    utils::deployed_address_evm_create,
    Address, ExecuteTransactionCommon, H256, U256,
};

use super::Source;

pub struct PgSource {
    pool: ConnectionPool<Core>,
}

impl PgSource {
    /// Build a PgSource. `concurrency` is the caller's parallel worker count
    /// (number of in-flight `get_block_data` calls); pool size is sized to that
    /// so each worker has a dedicated connection.
    pub async fn new(pg_url: String, concurrency: u32) -> anyhow::Result<Self> {
        let url = SensitiveUrl::from_str(&pg_url)?;
        let pool = ConnectionPool::<Core>::builder(url, concurrency.max(1))
            .build()
            .await?;
        Ok(Self { pool })
    }

    /// Genesis (block 0) special path: lens v25 genesis was bulk-deployed via
    /// custom_genesis (not via tx), so PG `transactions` / `call_traces` /
    /// `events` for block 0 are all empty. We reconstruct one synthetic tx +
    /// trace per **application contract** (filtered out 1.29M user-related
    /// "wrapper" contracts via [`GENESIS_WRAPPER_SKIP_THRESHOLD`]).
    ///
    /// Synthetic tx/trace fields mirror `zksync_s3_backfill::process_genesis_block`
    /// (mainnet historical backfill): `from=0`, `to=contract_addr`,
    /// `input=output=bytecode`, `call_create_type="create"`, gas/value all 0,
    /// `events=vec![]`.
    async fn process_genesis_block(&self) -> anyhow::Result<(BlockMeta, Vec<TxBlockData>)> {
        let mut conn = self.pool.connection().await?;
        let header = conn
            .blocks_dal()
            .get_l2_block_header(L2BlockNumber(0))
            .await?
            .context("genesis block not found in PG")?;

        let contracts = conn
            .storage_logs_dal()
            .get_genesis_application_contracts(GENESIS_WRAPPER_SKIP_THRESHOLD)
            .await?;
        tracing::info!(
            "Block 0 genesis: {} application contracts (after filtering wrappers > {})",
            contracts.len(),
            GENESIS_WRAPPER_SKIP_THRESHOLD
        );

        let mut tx_results = Vec::with_capacity(contracts.len());
        for (idx, (contract_addr, bytecode)) in contracts.into_iter().enumerate() {
            // Synthetic tx id mirrors mainnet zksync_s3_backfill pattern.
            let addr_str = format!("{:#x}", contract_addr);
            let genesis_tx_id = format!("0xgenesis020000000000000{}", addr_str);

            let debank_tx = DebankTransaction {
                id: genesis_tx_id.clone(),
                from: Address::zero(),
                to: Some(contract_addr),
                gas_limit: 0,
                gas_price: 0,
                gas_used: 0,
                status: true,
                gas_fee_cap: 0,
                gas_tip_cap: 0,
                input: bytecode.clone().into(),
                nonce: 0,
                transaction_index: idx as u32,
                value: U256::zero(),
            };

            let trace = DebankTrace {
                id: debank::to_hash(&[&idx.to_string()]),
                from_addr: Address::zero(),
                gas_limit: 0,
                input: bytecode.clone().into(),
                to_addr: contract_addr,
                value: U256::zero(),
                gas_used: 0,
                output: bytecode.into(),
                call_create_type: "create".to_string(),
                call_type: String::new(),
                tx_id: genesis_tx_id,
                parent_trace_id: String::new(),
                pos_in_parent_trace: 0,
                self_storage_change: false,
                storage_change: false,
                subtraces: 0,
                trace_address: vec![],
                error: String::new(),
            };

            tx_results.push(TxBlockData {
                debank_tx,
                traces: vec![trace],
                error_traces: vec![],
                events: vec![],
                error_events: vec![],
            });
        }

        let block_meta = BlockMeta {
            hash: header.hash,
            parent_hash: H256::zero(),  // genesis has no parent
            number: 0,
            timestamp: header.timestamp,
            base_fee_per_gas: header.base_fee_per_gas,
            gas_limit: header.gas_limit,
            logs_bloom: header.logs_bloom,
        };

        Ok((block_meta, tx_results))
    }
}

/// `bytecode_hash` 出现次数 > 此阈值的合约被视为 "mass-deployed wrapper" 跳过
/// (lens v25 genesis 有 645k user wrapper 共 2 个 bytecode_hash，需要 filter)。
/// 1000 是经验值：lens 实测 39 ZKSync system contracts + ~400 app contracts 都 < 1000。
const GENESIS_WRAPPER_SKIP_THRESHOLD: i64 = 1000;

#[async_trait]
impl Source for PgSource {
    async fn get_block_data(
        &self,
        block_num: u32,
    ) -> anyhow::Result<(BlockMeta, Vec<TxBlockData>)> {
        if block_num == 0 {
            return self.process_genesis_block().await;
        }
        let mut conn = self.pool.connection().await?;
        let l2_block = L2BlockNumber(block_num);

        let header = conn
            .blocks_dal()
            .get_l2_block_header(l2_block)
            .await?
            .with_context(|| format!("block {} not found in PG", block_num))?;

        // parent_hash: block N-1's hash. block 0 is handled separately above so
        // we can unconditionally fetch N-1.
        let prev_block_hash = conn
            .blocks_dal()
            .get_l2_block_header(L2BlockNumber(block_num - 1))
            .await?
            .with_context(|| format!("parent block {} not found in PG", block_num - 1))?
            .hash;

        // tx-level data: (Transaction, refunded_gas, error) joined in one query
        let tx_rows = conn
            .transactions_web3_dal()
            .get_raw_l2_block_transactions_with_status(l2_block)
            .await?;

        // call_traces for the entire block, keyed by tx_hash
        let trace_rows = conn
            .blocks_web3_dal()
            .get_traces_for_l2_block(l2_block)
            .await?;
        let mut trace_by_hash: std::collections::HashMap<H256, Call> = trace_rows
            .into_iter()
            .map(|(call, meta)| (meta.tx_hash, call))
            .collect();

        let mut tx_results = Vec::with_capacity(tx_rows.len());
        for (idx, row) in tx_rows.iter().enumerate() {
            // Skip L1 / ProtocolUpgrade transactions to align with EN realtime
            let l2_data = match &row.tx.common_data {
                ExecuteTransactionCommon::L2(data) => data,
                _ => continue,
            };

            let tx_hash = row.tx.hash();
            let tx = &row.tx;

            let to_address = tx.execute.contract_address.or_else(|| {
                Some(deployed_address_evm_create(
                    l2_data.initiator_address,
                    (*l2_data.nonce).into(),
                ))
            });

            let gas_limit = l2_data.fee.gas_limit.as_u64();
            let gas_used = gas_limit.saturating_sub(row.refunded_gas);
            let gas_price = l2_data
                .fee
                .get_effective_gas_price(header.base_fee_per_gas.into())
                .as_u64();

            let (gas_fee_cap, gas_tip_cap) =
                if l2_data.transaction_type as u32 >= TransactionType::EIP1559Transaction as u32 {
                    (
                        l2_data.fee.max_fee_per_gas.as_u64(),
                        l2_data.fee.max_priority_fee_per_gas.as_u64(),
                    )
                } else {
                    (0, 0)
                };

            // status: true iff tx executed without error (PG `transactions.error` is NULL)
            let status = row.error.is_none();

            let debank_tx = DebankTransaction {
                id: format!("{:#x}", tx_hash),
                from: l2_data.initiator_address,
                to: to_address,
                gas_limit,
                gas_price,
                gas_used,
                status,
                gas_fee_cap,
                gas_tip_cap,
                input: tx.execute.calldata.clone().into(),
                nonce: (*l2_data.nonce) as u64,
                transaction_index: idx as u32,
                value: tx.execute.value,
            };

            let mut traces = Vec::new();
            let mut error_traces = Vec::new();
            let mut events = Vec::new();
            let mut error_events = Vec::new();

            if let Some(mut call) = trace_by_hash.remove(&tx_hash) {
                // Standardize: same sequence as EN realtime debank_s3_persistence.rs:354-401
                call.trace_id = debank::to_hash(&[tx_hash.to_string().as_str(), "", "0"]);
                for (i, sub) in call.calls.iter_mut().enumerate() {
                    sub.pos_in_parent_trace = i as u32;
                }
                debank::set_parent_failed(&mut call, false);

                let root = debank::to_debank_trace(&call, tx_hash, vec![]);
                // root call has no parent so parent_failed is always false;
                // the `|| ... parent_failed` is kept for symmetry with EN realtime.
                if call.revert_reason.is_some() || call.parent_failed {
                    error_traces.push(root);
                } else {
                    traces.push(root);
                }

                debank::add_trace_log(
                    tx_hash,
                    &mut traces,
                    &mut error_traces,
                    &mut events,
                    &mut error_events,
                    vec![],
                    &mut call,
                );
            }

            tx_results.push(TxBlockData {
                debank_tx,
                traces,
                error_traces,
                events,
                error_events,
            });
        }

        // Lens v25 events fallback: blocks written by EN images built before
        // commit f650e15cb have no `events` field on persisted `Call` structs,
        // so the loop above produces empty events for every tx. Detect that
        // case (all txs have empty events AND empty error_events) and refill
        // from the `events` table. Trace-association fields (parent_trace_id,
        // pos_in_parent_trace) remain default — same degradation as RPC mode.
        let all_call_events_empty = !tx_results.is_empty()
            && tx_results
                .iter()
                .all(|t| t.events.is_empty() && t.error_events.is_empty());
        if all_call_events_empty {
            let logs = conn.events_dal().get_logs_for_l2_block(l2_block).await?;
            if !logs.is_empty() {
                attach_v25_events(&mut tx_results, logs);
            }
        }

        let block_meta = BlockMeta {
            hash: header.hash,
            parent_hash: prev_block_hash,
            number: header.number.0 as u64,
            timestamp: header.timestamp,
            base_fee_per_gas: header.base_fee_per_gas,
            gas_limit: header.gas_limit,
            logs_bloom: header.logs_bloom,
        };

        Ok((block_meta, tx_results))
    }
}

/// v25 fallback: pour all logs from the `events` table into `tx_results[0].events`
/// in `event_index_in_block` order.
///
/// **Why not partition events back to their owning tx_block?**
/// In Lens v25, the `events` table contains events from `tx_hash` values
/// that do NOT appear in the `transactions` table (likely v25-era EN
/// internal/bootstrap operations — observed pattern: each user-tx block
/// has 1 user-tx events row plus a "phantom" tx_hash with many events,
/// e.g. block 100000 has 1 user event + 19 phantom events for a unique
/// tx_hash never written to `transactions`). To keep events complete
/// without inflating `BlockFile.txs` with phantom entries (which would
/// diverge from v26 layout), all events are flattened into the first
/// tx_block. Each event carries its own `tx_id`, so downstream consumers
/// can still group by tx.
///
/// Fictive events (`tx_hash = 0x000…0`) are skipped — same policy as v26
/// (see backfill-redesign.md §"不补 fictive").
///
/// Degraded fields (vs EN realtime / v26 PG path):
/// - `parent_trace_id = ""`, `pos_in_parent_trace = 0` (events table doesn't
///   store trace association, identical to RPC-mode degradation).
///
/// `log_index = 0` initially; `assemble_block_file` reassigns block-monotonic.
fn attach_v25_events(tx_results: &mut Vec<TxBlockData>, logs: Vec<api::Log>) {
    if tx_results.is_empty() {
        return; // no tx in this block — nowhere to attach events
    }

    let mut all_events = Vec::with_capacity(logs.len());
    for log in logs {
        // v25 includes fictive events (tx_hash=0x000…0) to match `eth_getLogs`
        // output. v26's Call.events path keeps the old EN-realtime behavior of
        // dropping fictive — they only show up here in the v25 fallback path.
        let tx_hash = log.transaction_hash.unwrap_or_default();
        let log_index_in_block = log.log_index.map(|i| i.as_u32()).unwrap_or(0);
        let tx_hash_hex = format!("{:#x}", tx_hash);
        let selector = log
            .topics
            .first()
            .map(|t| format!("{:#x}", t))
            .unwrap_or_default();
        let topics_rest: Vec<String> = log
            .topics
            .iter()
            .skip(1)
            .map(|t| format!("{:#x}", t))
            .collect();
        let id = debank::to_hash(&[&tx_hash_hex, &log_index_in_block.to_string()]);

        all_events.push(DebankEvent {
            id,
            contract_id: log.address,
            selector,
            topics: topics_rest,
            data: log.data,
            tx_id: tx_hash_hex,
            parent_trace_id: String::new(),
            pos_in_parent_trace: 0,
            log_index: 0,
        });
    }

    tx_results[0].events = all_events;
}
