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
use zksync_basic_types::{protocol_version::ProtocolVersionId, L2BlockNumber};
use zksync_dal::{ConnectionPool, Core, CoreDal};
use zksync_multivm::{
    interface::{Call, CallType},
    tracers::debank,
};
use zksync_types::{
    api,
    debank::{BlockMeta, DebankEvent, DebankTrace, DebankTransaction, TxBlockData},
    l2::TransactionType,
    url::SensitiveUrl,
    utils::deployed_address_evm_create,
    zk_evm_types::FarCallOpcode,
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

            // get_mut (not remove) keeps the processed Call in trace_by_hash so
            // attach_v25_events can later walk the tree by tx_hash to rebuild
            // event→trace association for v25 logs.
            if let Some(call) = trace_by_hash.get_mut(&tx_hash) {
                // Standardize: same sequence as EN realtime debank_s3_persistence.rs:354-401
                call.trace_id = debank::to_hash(&[tx_hash.to_string().as_str(), "", "0"]);
                for (i, sub) in call.calls.iter_mut().enumerate() {
                    sub.pos_in_parent_trace = i as u32;
                }
                debank::set_parent_failed(call, false);

                let root = debank::to_debank_trace(call, tx_hash, vec![]);
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
                    call,
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
        // so the loop above produces empty events for every tx. Detect by
        // protocol_version (v25 only) — v26 keeps its Call.events path
        // untouched to stay byte-equal with EN realtime baseline.
        //
        // Includes v25 fictive blocks (l1_tx_count=0 AND l2_tx_count=0,
        // `tx_results.is_empty()`): events table still has 1 bootloader-emit
        // fictive event (tx_hash=0x000…0) which `attach_v25_events` synthesizes
        // a phantom TxBlockData for.
        let is_v25 = header.protocol_version == Some(ProtocolVersionId::Version25);
        let all_call_events_empty = tx_results
            .iter()
            .all(|t| t.events.is_empty() && t.error_events.is_empty());
        if is_v25 && all_call_events_empty {
            let logs = conn.events_dal().get_logs_for_l2_block(l2_block).await?;
            if !logs.is_empty() {
                attach_v25_events(&mut tx_results, logs, &trace_by_hash);
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

/// v25 fallback: rebuild trace-event association from PG `events` table + Call tree.
///
/// **Why this fallback exists.** v25-era Lens EN images predate DeBank fork commit
/// `f650e15cb` which added `events / trace_id / parent_trace_id / ...` fields to
/// `Call`. v25 `call_traces` blobs deserialize via `LegacyCall25` (12 fields,
/// no events). So the main loop produces empty `.events` for every tx, and we
/// pull logs from the `events` table here.
///
/// **Algorithm — heuristic event→trace match.**
/// For each log, find the trace whose executing-context address equals
/// `log.address`. "Executing context" = `Call.to` for normal frames, or
/// `Call.from` for DELEGATECALL (storage-context semantics).
///
/// - If exactly one frame in the tx's Call tree matches → `parent_trace_id =
///   that frame's trace_id`.
/// - If ≥2 frames match (ambiguous) or 0 frames match → fall back to the tx
///   root trace_id.
///
/// `pos_in_parent_trace` uses a per-frame monotonic counter mimicking
/// `vm_event.position` semantics in `add_trace_log` (debank.rs:45).
///
/// **Phantom / fictive logs.** v25 `events` table contains rows whose `tx_hash`
/// is absent from `transactions` (and therefore `call_traces`, FK CASCADE). We
/// synthesize a `TxBlockData` per such tx_hash and append it to `tx_results`
/// so that `events[*].parent_trace_id` always references a trace that exists
/// in the final `BlockFile.traces`. Fictive logs (`tx_hash = 0x000…0`) go
/// through the same synthesis path.
fn attach_v25_events(
    tx_results: &mut Vec<TxBlockData>,
    logs: Vec<api::Log>,
    trace_by_hash: &std::collections::HashMap<H256, Call>,
) {
    if logs.is_empty() {
        return;
    }

    // Partition logs by tx_hash, preserving first-appearance order.
    let mut by_tx: Vec<(H256, Vec<api::Log>)> = Vec::new();
    let mut tx_pos: std::collections::HashMap<H256, usize> = std::collections::HashMap::new();
    for log in logs {
        let tx_hash = log.transaction_hash.unwrap_or_default();
        match tx_pos.get(&tx_hash) {
            Some(&i) => by_tx[i].1.push(log),
            None => {
                tx_pos.insert(tx_hash, by_tx.len());
                by_tx.push((tx_hash, vec![log]));
            }
        }
    }

    // tx_hash → tx_results index (only for user txs already produced by the main loop).
    let tx_index: std::collections::HashMap<H256, usize> = tx_results
        .iter()
        .enumerate()
        .filter_map(|(i, t)| H256::from_str(&t.debank_tx.id).ok().map(|h| (h, i)))
        .collect();

    for (tx_hash, tx_logs) in by_tx {
        // Same formula as `add_trace_log` (multivm/debank.rs:29) and the main
        // loop (pg.rs:249) — uses `H256::to_string()` (no `0x` prefix), NOT
        // `{:#x}`. Hash inputs must match byte-for-byte.
        let tx_root_id = debank::to_hash(&[tx_hash.to_string().as_str(), "", "0"]);

        match (trace_by_hash.get(&tx_hash), tx_index.get(&tx_hash)) {
            (Some(root_call), Some(&idx)) => {
                // User tx with Call tree: DFS walk + match by exec address.
                // `fallback_pos` is a tx-wide counter for events whose
                // `parent_trace_id == tx_root_id` (ambiguous match / no
                // match / leftover). All those events share one counter to
                // keep `(parent_trace_id, pos_in_parent_trace)` unique, so
                // event.id (`to_hash(parent + pos)`) doesn't collide.
                let mut iter = tx_logs.into_iter().peekable();
                let mut fallback_pos: u32 = 0;
                walk_and_emit(
                    root_call,
                    root_call,
                    &mut iter,
                    &tx_root_id,
                    tx_hash,
                    &mut fallback_pos,
                    &mut tx_results[idx].events,
                );
                // Leftover logs whose address didn't match any frame along
                // the DFS path (e.g. system 0x800a Transfer emitted from
                // bootloader, not in user-visible Call tree).
                for log in iter {
                    let event = build_debank_event(log, tx_root_id.clone(), fallback_pos, tx_hash);
                    tx_results[idx].events.push(event);
                    fallback_pos += 1;
                }
            }
            _ => {
                // Phantom (no Call tree) or fictive (tx_hash=0x0): synthesize a
                // TxBlockData with one synthetic root trace.
                let synthetic_idx = tx_results.len() as u32;
                tx_results.push(build_synthetic_tx_block(
                    tx_hash,
                    tx_logs,
                    synthetic_idx,
                    tx_root_id,
                ));
            }
        }
    }
}

/// DFS pre-order walk over `cf`. At each frame entry and after each child
/// returns, drain events whose `address` matches `cf`'s executing-context
/// address. Each consumed event gets:
///   - `parent_trace_id`: from `find_parent_trace_id(root, log.address,
///     tx_root_id)` — uniquely matched frame's trace_id, or `tx_root_id` if
///     ambiguous (≥2 matches) / no match.
///   - `pos_in_parent_trace`: frame-local counter (matches `vm_event.position`
///     semantics in `add_trace_log`).
fn walk_and_emit(
    cf: &Call,
    root: &Call,
    events_iter: &mut std::iter::Peekable<std::vec::IntoIter<api::Log>>,
    tx_root_id: &str,
    tx_hash: H256,
    fallback_pos: &mut u32,
    out: &mut Vec<DebankEvent>,
) {
    let mut local_pos: u32 = 0;
    drain_to_frame(
        cf, root, events_iter, tx_root_id, tx_hash, &mut local_pos, fallback_pos, out,
    );
    for child in &cf.calls {
        walk_and_emit(child, root, events_iter, tx_root_id, tx_hash, fallback_pos, out);
        drain_to_frame(
            cf, root, events_iter, tx_root_id, tx_hash, &mut local_pos, fallback_pos, out,
        );
    }
}

fn drain_to_frame(
    cf: &Call,
    root: &Call,
    events_iter: &mut std::iter::Peekable<std::vec::IntoIter<api::Log>>,
    tx_root_id: &str,
    tx_hash: H256,
    local_pos: &mut u32,
    fallback_pos: &mut u32,
    out: &mut Vec<DebankEvent>,
) {
    while events_iter
        .peek()
        .is_some_and(|l| matches_addr(cf, l.address))
    {
        let log = events_iter.next().unwrap();
        let parent_id = find_parent_trace_id(root, log.address, tx_root_id);
        // Unique-match events use a frame-local counter (mirrors
        // `vm_event.position` semantics in `add_trace_log`). Ambiguous /
        // no-match events all share one tx-wide `fallback_pos` so their
        // `(parent_trace_id, pos)` pair stays unique across the tx — required
        // to avoid event.id collisions (`id = to_hash(parent + pos)`).
        let pos = if parent_id == tx_root_id {
            let p = *fallback_pos;
            *fallback_pos += 1;
            p
        } else {
            let p = *local_pos;
            *local_pos += 1;
            p
        };
        out.push(build_debank_event(log, parent_id, pos, tx_hash));
    }
}

/// Match `addr` against the frame's executing-context address.
/// For DELEGATECALL the executing context is the caller's storage (`Call.from`).
/// For Normal/Mimic CALL and CREATE it's `Call.to`.
fn matches_addr(cf: &Call, addr: Address) -> bool {
    let exec_addr = match cf.r#type {
        CallType::Call(FarCallOpcode::Delegate) => cf.from,
        _ => cf.to,
    };
    exec_addr == addr
}

/// Find the trace_id of the unique frame in `root`'s Call tree whose
/// executing-context address equals `log_addr`. If 0 or ≥2 frames match, fall
/// back to `tx_root_id`.
fn find_parent_trace_id(root: &Call, log_addr: Address, tx_root_id: &str) -> String {
    fn walk(c: &Call, addr: Address, count: &mut u32, found: &mut Option<String>) {
        if matches_addr(c, addr) {
            *count += 1;
            if *count == 1 {
                *found = Some(c.trace_id.clone());
            }
        }
        for sub in &c.calls {
            walk(sub, addr, count, found);
        }
    }
    let mut count = 0u32;
    let mut found: Option<String> = None;
    walk(root, log_addr, &mut count, &mut found);
    if count == 1 {
        found.unwrap()
    } else {
        tx_root_id.to_string()
    }
}

/// Build a `DebankEvent` from an `api::Log` with the resolved trace
/// association. `log_index` stays 0; `assemble_block_file` reassigns
/// block-monotonic at output time.
fn build_debank_event(
    log: api::Log,
    parent_trace_id: String,
    pos_in_parent_trace: u32,
    tx_hash: H256,
) -> DebankEvent {
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
    let id = debank::to_hash(&[&parent_trace_id, &pos_in_parent_trace.to_string()]);
    DebankEvent {
        id,
        contract_id: log.address,
        selector,
        topics: topics_rest,
        data: log.data,
        tx_id: format!("{:#x}", tx_hash),
        parent_trace_id,
        pos_in_parent_trace,
        log_index: 0,
    }
}

/// For a phantom tx_hash (events table row without a `transactions` /
/// `call_traces` counterpart) or fictive (tx_hash=0x0), synthesize one
/// `TxBlockData` so its events can reference a real trace_id in
/// `BlockFile.traces`.
fn build_synthetic_tx_block(
    tx_hash: H256,
    logs: Vec<api::Log>,
    transaction_index: u32,
    tx_root_id: String,
) -> TxBlockData {
    let tx_id = format!("{:#x}", tx_hash);
    let synthetic_trace = DebankTrace {
        id: tx_root_id.clone(),
        from_addr: Address::zero(),
        gas_limit: 0,
        input: Default::default(),
        to_addr: Address::zero(),
        value: U256::zero(),
        gas_used: 0,
        output: Default::default(),
        call_create_type: "empty".to_string(),
        call_type: String::new(),
        tx_id: tx_id.clone(),
        parent_trace_id: String::new(),
        pos_in_parent_trace: 0,
        self_storage_change: false,
        storage_change: false,
        subtraces: 0,
        trace_address: vec![],
        error: String::new(),
    };
    let synthetic_tx = DebankTransaction {
        id: tx_id,
        from: Address::zero(),
        to: Some(Address::zero()),
        gas_limit: 0,
        gas_price: 0,
        gas_used: 0,
        status: true,
        gas_fee_cap: 0,
        gas_tip_cap: 0,
        input: Default::default(),
        nonce: 0,
        transaction_index,
        value: U256::zero(),
    };
    let events: Vec<DebankEvent> = logs
        .into_iter()
        .enumerate()
        .map(|(i, log)| build_debank_event(log, tx_root_id.clone(), i as u32, tx_hash))
        .collect();
    TxBlockData {
        debank_tx: synthetic_tx,
        traces: vec![synthetic_trace],
        error_traces: vec![],
        events,
        error_events: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zksync_multivm::interface::VmEvent;

    fn addr(n: u8) -> Address {
        let mut bytes = [0u8; 20];
        bytes[19] = n;
        Address::from(bytes)
    }

    fn tx_hash(n: u8) -> H256 {
        let mut bytes = [0u8; 32];
        bytes[31] = n;
        H256::from(bytes)
    }

    fn mk_call(to: Address, calls: Vec<Call>) -> Call {
        Call {
            to,
            calls,
            ..Default::default()
        }
    }

    fn mk_log(tx: H256, address: Address) -> api::Log {
        api::Log {
            address,
            topics: vec![],
            data: Default::default(),
            block_hash: None,
            block_number: None,
            l1_batch_number: None,
            transaction_hash: Some(tx),
            transaction_index: None,
            log_index: None,
            transaction_log_index: None,
            log_type: None,
            removed: None,
            block_timestamp: None,
        }
    }

    /// Set trace_id / parent_trace_id / pos_in_parent_trace for the whole Call
    /// tree using the same formula as `add_trace_log` (multivm/debank.rs:26-32).
    fn assign_trace_ids(root: &mut Call, tx: H256) {
        root.trace_id = debank::to_hash(&[tx.to_string().as_str(), "", "0"]);
        fn recurse(call: &mut Call, tx: H256) {
            for (i, sub) in call.calls.iter_mut().enumerate() {
                sub.pos_in_parent_trace = i as u32;
                sub.parent_trace_id = Some(call.trace_id.clone());
                sub.trace_id = debank::to_hash(&[
                    tx.to_string().as_str(),
                    &call.trace_id,
                    &sub.pos_in_parent_trace.to_string(),
                ]);
                recurse(sub, tx);
            }
        }
        recurse(root, tx);
    }

    fn mk_tx_block(tx: H256) -> TxBlockData {
        TxBlockData {
            debank_tx: DebankTransaction {
                id: format!("{:#x}", tx),
                ..Default::default()
            },
            traces: vec![],
            error_traces: vec![],
            events: vec![],
            error_events: vec![],
        }
    }

    /// T1: single Call frame with one matching log → event mounted on that frame.
    #[test]
    fn test_single_match_root() {
        let tx = tx_hash(1);
        let a = addr(0xAA);
        let mut root = mk_call(a, vec![]);
        assign_trace_ids(&mut root, tx);
        let expected_id = root.trace_id.clone();

        let mut tx_blocks = vec![mk_tx_block(tx)];
        let mut traces = std::collections::HashMap::new();
        traces.insert(tx, root);

        attach_v25_events(&mut tx_blocks, vec![mk_log(tx, a)], &traces);
        assert_eq!(tx_blocks[0].events.len(), 1);
        assert_eq!(tx_blocks[0].events[0].parent_trace_id, expected_id);
        assert_eq!(tx_blocks[0].events[0].pos_in_parent_trace, 0);
    }

    /// T2: nested sub-call with a uniquely matching address → event mounts on sub.
    #[test]
    fn test_single_match_nested() {
        let tx = tx_hash(2);
        let (a, b) = (addr(0xAA), addr(0xBB));
        let mut root = mk_call(a, vec![mk_call(b, vec![])]);
        assign_trace_ids(&mut root, tx);
        let sub_id = root.calls[0].trace_id.clone();

        let mut tx_blocks = vec![mk_tx_block(tx)];
        let mut traces = std::collections::HashMap::new();
        traces.insert(tx, root);

        attach_v25_events(&mut tx_blocks, vec![mk_log(tx, b)], &traces);
        assert_eq!(tx_blocks[0].events[0].parent_trace_id, sub_id);
    }

    /// T3: address appears in ≥2 frames → ambiguous, fallback to tx_root.
    #[test]
    fn test_ambiguous_fallback_to_tx_root() {
        let tx = tx_hash(3);
        let a = addr(0xAA);
        let mut root = mk_call(a, vec![mk_call(a, vec![])]);
        assign_trace_ids(&mut root, tx);
        let tx_root_id = root.trace_id.clone();

        let mut tx_blocks = vec![mk_tx_block(tx)];
        let mut traces = std::collections::HashMap::new();
        traces.insert(tx, root);

        attach_v25_events(&mut tx_blocks, vec![mk_log(tx, a)], &traces);
        assert_eq!(tx_blocks[0].events[0].parent_trace_id, tx_root_id);
    }

    /// T4: log address matches no frame at all → fallback to tx_root.
    #[test]
    fn test_no_match_fallback() {
        let tx = tx_hash(4);
        let (a, b, c) = (addr(0xAA), addr(0xBB), addr(0xCC));
        let mut root = mk_call(a, vec![mk_call(b, vec![])]);
        assign_trace_ids(&mut root, tx);
        let tx_root_id = root.trace_id.clone();

        let mut tx_blocks = vec![mk_tx_block(tx)];
        let mut traces = std::collections::HashMap::new();
        traces.insert(tx, root);

        attach_v25_events(&mut tx_blocks, vec![mk_log(tx, c)], &traces);
        assert_eq!(tx_blocks[0].events[0].parent_trace_id, tx_root_id);
    }

    /// T5: DELEGATECALL frame uses Call.from as exec_addr.
    #[test]
    fn test_delegatecall_uses_from() {
        let tx = tx_hash(5);
        let (a, b, c) = (addr(0xAA), addr(0xBB), addr(0xCC));
        // root.to=A; root.calls[0] is DELEGATECALL with from=B to=C.
        let mut root = Call {
            to: a,
            calls: vec![Call {
                r#type: CallType::Call(FarCallOpcode::Delegate),
                from: b,
                to: c,
                ..Default::default()
            }],
            ..Default::default()
        };
        assign_trace_ids(&mut root, tx);
        let delegate_id = root.calls[0].trace_id.clone();

        let mut tx_blocks = vec![mk_tx_block(tx)];
        let mut traces = std::collections::HashMap::new();
        traces.insert(tx, root);

        // log.address = B → only delegate frame matches (root.to=A, delegate.from=B).
        attach_v25_events(&mut tx_blocks, vec![mk_log(tx, b)], &traces);
        assert_eq!(tx_blocks[0].events[0].parent_trace_id, delegate_id);
    }

    /// T6: parent post-child emit (Case A). Events: [(B, log_in_B), (A, log_in_A)].
    /// Walk: enter A; B != A skip; enter B; consume B-log on B; return to A;
    /// consume A-log on A with frame-local pos=0.
    #[test]
    fn test_case_a_parent_post_child_emit() {
        let tx = tx_hash(6);
        let (a, b) = (addr(0xAA), addr(0xBB));
        let mut root = mk_call(a, vec![mk_call(b, vec![])]);
        assign_trace_ids(&mut root, tx);
        let root_id = root.trace_id.clone();
        let sub_id = root.calls[0].trace_id.clone();

        let mut tx_blocks = vec![mk_tx_block(tx)];
        let mut traces = std::collections::HashMap::new();
        traces.insert(tx, root);

        attach_v25_events(
            &mut tx_blocks,
            vec![mk_log(tx, b), mk_log(tx, a)],
            &traces,
        );
        assert_eq!(tx_blocks[0].events.len(), 2);
        assert_eq!(tx_blocks[0].events[0].parent_trace_id, sub_id);
        assert_eq!(tx_blocks[0].events[0].pos_in_parent_trace, 0);
        assert_eq!(tx_blocks[0].events[1].parent_trace_id, root_id);
        assert_eq!(tx_blocks[0].events[1].pos_in_parent_trace, 0);
    }

    /// T7: phantom tx_hash (no Call tree) → synthesize TxBlockData.
    #[test]
    fn test_phantom_synthetic_append() {
        let phantom = tx_hash(7);
        let a = addr(0xAA);

        let mut tx_blocks: Vec<TxBlockData> = vec![];
        let traces = std::collections::HashMap::new();

        attach_v25_events(
            &mut tx_blocks,
            vec![mk_log(phantom, a), mk_log(phantom, a)],
            &traces,
        );
        assert_eq!(tx_blocks.len(), 1);
        let syn = &tx_blocks[0];
        assert_eq!(syn.debank_tx.gas_used, 0);
        assert_eq!(syn.debank_tx.nonce, 0);
        assert_eq!(syn.debank_tx.from, Address::zero());
        assert_eq!(syn.traces.len(), 1);
        assert_eq!(syn.events.len(), 2);
        let expected_root = debank::to_hash(&[phantom.to_string().as_str(), "", "0"]);
        assert_eq!(syn.traces[0].id, expected_root);
        assert_eq!(syn.events[0].parent_trace_id, expected_root);
        assert_eq!(syn.events[0].pos_in_parent_trace, 0);
        assert_eq!(syn.events[1].pos_in_parent_trace, 1);
    }

    /// T8: fictive (tx_hash=0x0) follows the same synthesis path as phantom.
    #[test]
    fn test_fictive_synthetic_append() {
        let fictive = H256::zero();
        let a = addr(0xAA);

        let mut tx_blocks: Vec<TxBlockData> = vec![];
        let traces = std::collections::HashMap::new();

        attach_v25_events(&mut tx_blocks, vec![mk_log(fictive, a)], &traces);
        assert_eq!(tx_blocks.len(), 1);
        assert_eq!(tx_blocks[0].debank_tx.id, format!("{:#x}", fictive));
        assert_eq!(tx_blocks[0].events.len(), 1);
    }

    /// T9: byte-equal invariant — same Call tree, one path emits events via
    /// `add_trace_log` (with `vm_event` populated), the other via
    /// `attach_v25_events` walk. Must produce identical (parent_trace_id,
    /// pos_in_parent_trace, id, contract_id) for each event.
    #[test]
    fn test_byte_equal_with_add_trace_log() {
        let tx = tx_hash(9);
        let (a, b) = (addr(0xAA), addr(0xBB));

        // Build identical trees; one carries VmEvents inline, the other doesn't.
        let mk_tree_with_events = || Call {
            to: a,
            calls: vec![Call {
                to: b,
                events: vec![VmEvent {
                    location: Default::default(),
                    address: b,
                    indexed_topics: vec![],
                    value: vec![],
                    position: 0,
                }],
                ..Default::default()
            }],
            events: vec![VmEvent {
                location: Default::default(),
                address: a,
                indexed_topics: vec![],
                value: vec![],
                position: 0,
            }],
            ..Default::default()
        };

        // Path A: run add_trace_log as EN realtime would.
        let mut tree_atl = mk_tree_with_events();
        tree_atl.trace_id = debank::to_hash(&[tx.to_string().as_str(), "", "0"]);
        for (i, sub) in tree_atl.calls.iter_mut().enumerate() {
            sub.pos_in_parent_trace = i as u32;
        }
        let mut traces_atl = vec![debank::to_debank_trace(&tree_atl, tx, vec![])];
        let mut error_traces_atl = vec![];
        let mut events_atl = vec![];
        let mut error_events_atl = vec![];
        debank::add_trace_log(
            tx,
            &mut traces_atl,
            &mut error_traces_atl,
            &mut events_atl,
            &mut error_events_atl,
            vec![],
            &mut tree_atl,
        );

        // Path B: feed the same tree (sans VmEvents) into attach_v25_events.
        let mut tree_attach = mk_tree_with_events();
        // Standardize trace_ids same as the main loop does (pg.rs:249-251).
        tree_attach.trace_id = debank::to_hash(&[tx.to_string().as_str(), "", "0"]);
        for (i, sub) in tree_attach.calls.iter_mut().enumerate() {
            sub.pos_in_parent_trace = i as u32;
        }
        debank::set_parent_failed(&mut tree_attach, false);
        let mut dummy_traces = vec![];
        let mut dummy_errs = vec![];
        let mut dummy_events: Vec<DebankEvent> = vec![];
        let mut dummy_err_events = vec![];
        // run add_trace_log only to compute sub-call trace_id without emitting events;
        // strip events from tree_attach first
        let mut tree_attach_no_events = tree_attach.clone();
        fn strip_events(c: &mut Call) {
            c.events.clear();
            for sub in c.calls.iter_mut() {
                strip_events(sub);
            }
        }
        strip_events(&mut tree_attach_no_events);
        debank::add_trace_log(
            tx,
            &mut dummy_traces,
            &mut dummy_errs,
            &mut dummy_events,
            &mut dummy_err_events,
            vec![],
            &mut tree_attach_no_events,
        );
        // dummy_events must be empty (events stripped); now run attach_v25_events
        // with the *trace-id-populated* tree against external logs.
        assert!(dummy_events.is_empty(), "stripped tree should produce no events");

        let mut tx_blocks = vec![mk_tx_block(tx)];
        let mut traces = std::collections::HashMap::new();
        traces.insert(tx, tree_attach_no_events);

        // emit order: child first (events_atl pushes B's log first, then A's)
        attach_v25_events(
            &mut tx_blocks,
            vec![mk_log(tx, b), mk_log(tx, a)],
            &traces,
        );

        assert_eq!(events_atl.len(), 2);
        assert_eq!(tx_blocks[0].events.len(), 2);
        for (atl, attach) in events_atl.iter().zip(tx_blocks[0].events.iter()) {
            assert_eq!(atl.parent_trace_id, attach.parent_trace_id);
            assert_eq!(atl.pos_in_parent_trace, attach.pos_in_parent_trace);
            assert_eq!(atl.id, attach.id);
            assert_eq!(atl.contract_id, attach.contract_id);
            assert_eq!(atl.tx_id, attach.tx_id);
        }
    }

    /// T10: when Call.events is already populated (v26 path), the main loop
    /// produces non-empty events and `attach_v25_events` is never invoked.
    /// This test asserts the guard `all_call_events_empty` semantics — it's a
    /// no-op test on `attach_v25_events` itself: if tx_results already has
    /// events, calling attach with empty logs leaves things untouched.
    #[test]
    fn test_v26_path_unaffected() {
        let tx = tx_hash(10);
        let mut tx_blocks = vec![mk_tx_block(tx)];
        tx_blocks[0].events.push(DebankEvent {
            id: "preexisting".to_string(),
            parent_trace_id: "preexisting_parent".to_string(),
            ..Default::default()
        });
        let traces = std::collections::HashMap::new();
        // attach with empty logs is a no-op.
        attach_v25_events(&mut tx_blocks, vec![], &traces);
        assert_eq!(tx_blocks[0].events.len(), 1);
        assert_eq!(tx_blocks[0].events[0].id, "preexisting");
    }
}
