//! Read-only replay from a restored archive PG. No stored Call is used and no
//! database / S3 write is performed. Batches are committed locally only after
//! transaction, receipt, event and final-storage comparisons succeed.

#[path = "../rebuild_validation.rs"]
mod rebuild_validation;
#[path = "../rebuild_version.rs"]
mod rebuild_version;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{ensure, Context};
use once_cell::sync::OnceCell;
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::Row;
use structopt::StructOpt;
use tokio::runtime::Handle;
use zksync_dal::{ConnectionPool, Core, CoreDal};
use zksync_multivm::{
    interface::{Call, ExecutionResult, L2BlockEnv, VmFactory, VmInterface},
    pubdata_builders::pubdata_params_to_builder,
    tracers::{debank, CallTracer},
    vm_latest::{HistoryEnabled, ToTracerPointer, Vm},
};
use zksync_state::PostgresStorage;
use zksync_types::{
    api::GetLogsFilter,
    debank::{assemble_block_file, BlockMeta},
    url::SensitiveUrl,
    AccountTreeId, Address, L1BatchNumber, L2ChainId, ProtocolVersionId, StorageKey, H256, U256,
};
use zksync_vm_executor::storage::L1BatchParamsProvider;
use zksync_vm_interface::{
    storage::{ReadStorage, StorageView},
    VmEvent,
};

const FORMAT: &str = "lens-execution-replay-v1";

#[derive(StructOpt)]
struct Args {
    #[structopt(skip)]
    generator_sha256: String,
    #[structopt(long)]
    database_url: SensitiveUrl,
    #[structopt(long)]
    chain_id: u64,
    /// Inclusive L1 batch range. Genesis is a state projection, not a VM batch.
    #[structopt(long)]
    start_batch: u32,
    #[structopt(long)]
    end_batch: u32,
    #[structopt(long, parse(from_os_str))]
    output_dir: PathBuf,
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn write_json(path: &Path, value: &impl Serialize) -> anyhow::Result<String> {
    let bytes = serde_json::to_vec(value)?;
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(&bytes)?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    Ok(sha256(&bytes))
}

fn events_key(events: &[VmEvent]) -> Vec<(String, Vec<String>, Vec<u8>)> {
    events
        .iter()
        .map(|event| {
            (
                format!("{:#x}", event.address),
                event
                    .indexed_topics
                    .iter()
                    .map(|topic| format!("{topic:#x}"))
                    .collect(),
                event.value.clone(),
            )
        })
        .collect()
}

fn logs_key(logs: &[zksync_types::api::Log]) -> Vec<(String, Vec<String>, Vec<u8>)> {
    logs.iter()
        .map(|log| {
            (
                format!("{:#x}", log.address),
                log.topics
                    .iter()
                    .map(|topic| format!("{topic:#x}"))
                    .collect(),
                log.data.0.clone(),
            )
        })
        .collect()
}

fn replay(pool: ConnectionPool<Core>, args: &Args, batch: u32, rt: Handle) -> anyhow::Result<()> {
    let mut connection = rt.block_on(pool.connection())?;
    let mut db = rt.block_on(connection.start_transaction())?;
    rt.block_on(
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .execute(db.conn()),
    )?;
    let pruning = rt.block_on(db.pruning_dal().get_pruning_info())?;
    let batch_number = L1BatchNumber(batch);
    let provider = rt.block_on(L1BatchParamsProvider::new(&mut db))?;
    let mut first = rt
        .block_on(provider.load_first_l2_block_in_batch(&mut db, batch_number))?
        .context("batch is absent from archive PG")?;
    let batch_header = rt
        .block_on(db.blocks_dal().get_l1_batch_header(batch_number))?
        .context("sealed batch header is missing")?;
    let protocol = batch_header
        .protocol_version
        .context("batch protocol version is missing")?;
    ensure!(
        protocol >= ProtocolVersionId::Version25,
        "this collector requires protocol 25 or newer"
    );
    if !first.has_protocol_version() {
        first.set_protocol_version(protocol);
    }
    let previous = first.number() - 1;
    ensure!(
        pruning
            .last_hard_pruned
            .is_none_or(|pruned| pruned.l2_block < previous),
        "archive state before this batch has been pruned"
    );
    let restored = rt.block_on(provider.load_l1_batch_params(
        &mut db,
        &first,
        u32::MAX,
        L2ChainId::try_from(args.chain_id).map_err(anyhow::Error::msg)?,
    ))?;
    let blocks = rt.block_on(
        db.transactions_dal()
            .get_l2_blocks_to_execute_for_l1_batch(batch_number),
    )?;
    let (from, to) = rt
        .block_on(db.blocks_dal().get_l2_block_range_of_l1_batch(batch_number))?
        .context("batch has no L2 blocks")?;
    ensure!(
        blocks.len() == (to.0 - from.0 + 1) as usize,
        "L2 block coverage is incomplete"
    );
    let mut headers = vec![];
    for (index, block) in blocks.iter().enumerate() {
        ensure!(
            block.number.0 == from.0 + index as u32,
            "non-contiguous L2 blocks"
        );
        let header = rt
            .block_on(db.blocks_dal().get_l2_block_header(block.number))?
            .context("L2 header is missing")?;
        ensure!(
            block.txs.len() == (header.l1_tx_count + header.l2_tx_count) as usize,
            "raw transaction coverage is incomplete at block {}",
            block.number
        );
        if let Some(previous_header) = headers.last() {
            let previous_header: &zksync_types::block::L2BlockHeader = previous_header;
            ensure!(
                block.prev_block_hash == previous_header.hash,
                "canonical parent hash mismatch"
            );
        }
        headers.push(header);
    }
    ensure!(
        blocks.last().unwrap().txs.is_empty(),
        "batch has no fictive final block"
    );
    let tx_hashes: Vec<_> = blocks
        .iter()
        .flat_map(|block| block.txs.iter().map(|tx| tx.hash()))
        .collect();
    let receipts = rt.block_on(
        db.transactions_web3_dal()
            .get_transaction_receipts(&tx_hashes),
    )?;
    ensure!(
        receipts.len() == tx_hashes.len(),
        "receipt coverage is incomplete"
    );
    let receipts: BTreeMap<_, _> = receipts
        .into_iter()
        .map(|receipt| (receipt.inner.transaction_hash, receipt.inner))
        .collect();
    let logs = rt.block_on(db.events_web3_dal().get_logs(
        GetLogsFilter {
            from_block: from,
            to_block: to,
            addresses: vec![],
            topics: vec![],
        },
        usize::MAX.min(i32::MAX as usize),
    ))?;
    let rows = rt.block_on(sqlx::query("SELECT DISTINCT ON (hashed_key) hashed_key, address, key, value FROM storage_logs WHERE miniblock_number BETWEEN $1 AND $2 ORDER BY hashed_key, miniblock_number DESC, operation_number DESC")
        .bind(i64::from(from.0)).bind(i64::from(to.0)).fetch_all(db.conn()))?;
    let mut expected_writes = BTreeMap::new();
    for row in rows {
        let address: Vec<u8> = row.try_get("address")?;
        let key: Vec<u8> = row.try_get("key")?;
        let value: Vec<u8> = row.try_get("value")?;
        let hashed_key: Vec<u8> = row.try_get("hashed_key")?;
        let key = StorageKey::new(
            AccountTreeId::new(Address::from_slice(&address)),
            H256::from_slice(&key),
        );
        ensure!(
            key.hashed_key().as_bytes() == hashed_key.as_slice(),
            "storage key preimage mismatch"
        );
        expected_writes.insert(key.hashed_key(), (key, H256::from_slice(&value)));
    }
    let input = serde_json::json!({
        "format": FORMAT, "chain_id": args.chain_id, "generator_sha256": args.generator_sha256,
        "batch_env": restored.l1_batch_env, "system_env": restored.system_env,
        "pubdata_params": restored.pubdata_params, "blocks": blocks,
        "headers": headers.iter().map(|header| serde_json::json!({
            "number": header.number, "hash": header.hash, "timestamp": header.timestamp,
            "base_fee_per_gas": header.base_fee_per_gas, "gas_limit": header.gas_limit,
            "logs_bloom": header.logs_bloom, "protocol_version": header.protocol_version,
            "base_system_contracts_hashes": header.base_system_contracts_hashes,
        })).collect::<Vec<_>>(),
        "receipts": receipts.values().collect::<Vec<_>>(), "committed_logs": logs,
        "expected_writes": expected_writes.values().collect::<Vec<_>>(),
    });
    let input_hash = sha256(&serde_json::to_vec(&input)?);
    let destination = args.output_dir.join(batch.to_string());
    if destination.exists() {
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(destination.join("manifest.json"))?)?;
        ensure!(
            manifest["format"] == FORMAT && manifest["input_sha256"] == input_hash,
            "existing batch was generated with different inputs or rules"
        );
        ensure!(
            manifest["execution_verified"] == true && manifest["state_writes_verified"] == true,
            "existing batch has not passed execution verification"
        );
        let mut expected_files: BTreeSet<String> =
            ["input.json", "execution.json", "storage-input.json"]
                .into_iter()
                .map(str::to_owned)
                .collect();
        for block in &blocks {
            for suffix in ["blockfile.json", "header.json", "validation.json"] {
                expected_files.insert(format!("{}.{}", block.number.0, suffix));
            }
            for index in 0..block.txs.len() {
                expected_files.insert(format!("{}.tx-{index}.execution.json", block.number.0));
            }
        }
        ensure!(
            manifest["files"]
                .as_object()
                .context("invalid file manifest")?
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>()
                == expected_files,
            "committed manifest has incomplete output coverage"
        );
        for (name, hash) in manifest["files"]
            .as_object()
            .context("invalid file manifest")?
        {
            ensure!(
                sha256(&fs::read(destination.join(name))?) == hash.as_str().unwrap_or_default(),
                "committed file checksum mismatch: {name}"
            );
        }
        return Ok(());
    }
    let staging = args
        .output_dir
        .join(format!(".batch-{batch}-prepared-{}", std::process::id()));
    fs::create_dir(&staging)?;
    let mut files = BTreeMap::new();
    files.insert(
        "input.json".to_owned(),
        write_json(&staging.join("input.json"), &input)?,
    );
    // The snapshot ends at the previous batch. Its writes, including initial
    // writes in that batch, must count as already committed for the new batch.
    let mut storage = rt.block_on(PostgresStorage::new_async(rt.clone(), db, previous, true))?;
    let chain_id_key = StorageKey::new(
        AccountTreeId::new(zksync_system_constants::SYSTEM_CONTEXT_ADDRESS),
        H256::zero(),
    );
    ensure!(
        U256::from_big_endian(storage.read_value(&chain_id_key).as_bytes()) == args.chain_id.into(),
        "chain ID in the restored state differs from --chain-id"
    );
    let storage = StorageView::new(storage).to_rc_ptr();
    let pubdata_builder = pubdata_params_to_builder(restored.pubdata_params, protocol);
    let mut vm =
        Vm::<_, HistoryEnabled>::new(restored.l1_batch_env, restored.system_env, storage.clone());
    let mut raw_transactions = vec![];
    let mut outputs = vec![];
    for (block_index, (block, header)) in blocks.iter().zip(&headers).enumerate() {
        if block_index > 0 {
            vm.start_new_l2_block(L2BlockEnv::from_l2_block_data(block));
        }
        let mut tx_outputs = vec![];
        for (index, tx) in block.txs.iter().enumerate() {
            let traces = Arc::new(OnceCell::new());
            let tracer = CallTracer::new(traces.clone()).into_tracer_pointer();
            let (compression, result) = vm.inspect_transaction_with_bytecode_compression(
                &mut tracer.into(),
                tx.clone(),
                true,
            );
            let (status, output, revert) = match &result.result {
                ExecutionResult::Success { output } => (true, output.clone(), None),
                ExecutionResult::Revert { output } => {
                    (false, output.encoded_data(), Some(output.to_string()))
                }
                ExecutionResult::Halt { reason } => (false, vec![], Some(reason.to_string())),
            };
            let gas = tx
                .gas_limit()
                .as_u64()
                .checked_sub(result.refunds.gas_refunded)
                .context("refund exceeds transaction gas limit")?;
            let mut call = Call::new_high_level(
                tx.gas_limit().as_u64(),
                gas,
                tx.execute.value,
                tx.execute.calldata.clone(),
                output,
                revert,
                traces.get().context("tracer did not finish")?.clone(),
            );
            set_batch_number(&mut call, batch_number);
            let execution = serde_json::json!({
                "transaction": tx, "call": call, "result": format!("{:?}", result.result),
                "bytecode_compression_error": compression.as_ref().err().map(|error| format!("{error:?}")),
                "committed_events": result.logs.events,
                "storage_logs": result.logs.storage_logs.iter().map(|log| serde_json::json!({
                    "log": log.log, "previous_value": log.previous_value,
                })).collect::<Vec<_>>(),
                "dynamic_factory_deps": result.dynamic_factory_deps.iter().collect::<BTreeMap<_, _>>(),
            });
            let name = format!("{}.tx-{index}.execution.json", block.number.0);
            files.insert(name.clone(), write_json(&staging.join(&name), &execution)?);
            raw_transactions.push(name);
            compression.context("bytecode compression failed")?;
            ensure!(
                !matches!(result.result, ExecutionResult::Halt { .. }),
                "historical transaction halted"
            );
            let receipt = receipts.get(&tx.hash()).context("receipt missing")?;
            ensure!(
                receipt.block_hash == header.hash
                    && receipt.transaction_index.as_u32() == index as u32,
                "receipt location mismatch for {:?}",
                tx.hash()
            );
            ensure!(
                receipt.status.as_u64() == u64::from(status),
                "execution status mismatch"
            );
            ensure!(
                receipt.gas_used.map(|value| value.as_u64()) == Some(gas),
                "execution gas mismatch"
            );
            ensure!(
                events_key(&result.logs.events) == logs_key(&receipt.logs),
                "committed transaction logs differ from receipt"
            );
            ensure!(
                events_key(&captured_committed_events(&call.calls)?)
                    == events_key(&result.logs.events),
                "raw traced committed events differ from VM output for {:?}",
                tx.hash()
            );
            let data = debank::transaction_data(
                tx,
                index as u32,
                header.base_fee_per_gas,
                gas,
                status,
                Some(call),
            );
            ensure!(
                receipt.effective_gas_price == Some(data.debank_tx.gas_price.into()),
                "effective gas price differs from receipt"
            );
            tx_outputs.push(data);
        }
        outputs.push(assemble_block_file(
            BlockMeta {
                hash: header.hash,
                parent_hash: block.prev_block_hash,
                number: block.number.0 as u64,
                timestamp: header.timestamp,
                base_fee_per_gas: header.base_fee_per_gas,
                gas_limit: header.gas_limit,
                logs_bloom: header.logs_bloom,
            },
            tx_outputs,
        ));
    }
    let traces = Arc::new(OnceCell::new());
    let tracer = CallTracer::new(traces.clone()).into_tracer_pointer();
    let mut finished = vm.finish_batch_with_tracer(&mut tracer.into(), pubdata_builder);
    let mut tip_calls = traces.get().cloned().unwrap_or_default();
    for call in &mut tip_calls {
        set_batch_number(call, batch_number);
    }
    finished.block_tip_call_traces = tip_calls.clone();
    // Batch-tip execution has no ordinary transaction. Keep its actual frames
    // and events explicitly, rather than manufacturing a tx with zero hash.
    files.insert("execution.json".into(), write_json(&staging.join("execution.json"), &serde_json::json!({
        "transactions": raw_transactions, "batch_tip_calls": tip_calls,
        "batch_tip_result": format!("{:?}", finished.block_tip_execution_result.result),
        "batch_tip_trace_complete": traces.get().is_some(),
        "batch_tip_events": finished.block_tip_execution_result.logs.events,
        "final_storage_logs": finished.final_execution_state.deduplicated_storage_logs,
        "used_contract_hashes": finished.final_execution_state.used_contract_hashes,
        "final_bootloader_memory": finished.final_bootloader_memory,
        "batch_tip_dynamic_factory_deps": finished.block_tip_execution_result.dynamic_factory_deps.iter().collect::<BTreeMap<_, _>>(),
    }))?);
    ensure!(traces.get().is_some(), "batch-tip tracer did not finish");
    ensure!(
        !finished.block_tip_execution_result.result.is_failed(),
        "batch finalization failed"
    );
    debank::append_batch_tip(&mut outputs.last_mut().unwrap().0, &finished);
    ensure!(
        events_key(&captured_committed_events(&tip_calls)?)
            == events_key(&finished.block_tip_execution_result.logs.events),
        "batch-tip raw traced committed events differ from VM output"
    );
    let actual = events_key(&finished.final_execution_state.events);
    let expected = logs_key(&logs);
    drop(vm);
    let mut storage = storage.borrow_mut();
    let mut expected_changes = BTreeMap::new();
    for (hash, (key, value)) in expected_writes {
        if storage.inner_mut().read_value(&key) != value {
            expected_changes.insert(hash, value);
        }
    }
    let mut actual_changes = BTreeMap::new();
    for log in &finished.final_execution_state.deduplicated_storage_logs {
        if log.is_write() && storage.inner_mut().read_value(&log.key) != log.value {
            actual_changes.insert(log.key.hashed_key(), log.value);
        }
    }
    let mut slots = storage
        .cache()
        .read_storage_keys()
        .into_iter()
        .collect::<Vec<_>>();
    slots.sort_by_key(|(key, _)| key.hashed_key());
    let mut state_input = vec![];
    let mut complete_indices = true;
    for (key, value) in slots {
        let index = storage.inner_mut().get_enumeration_index(&key);
        complete_indices &= value.is_zero() || index.is_some();
        state_input
            .push(serde_json::json!({ "key": key, "value": value, "enumeration_index": index }));
    }
    files.insert(
        "storage-input.json".into(),
        write_json(&staging.join("storage-input.json"), &state_input)?,
    );
    drop(storage);
    ensure!(
        actual == expected,
        "committed event content or order mismatch"
    );
    ensure!(
        complete_indices,
        "nonzero historical slot has no enumeration index"
    );
    ensure!(
        actual_changes == expected_changes,
        "final storage changes differ from the archive"
    );
    for (block_file, header) in &outputs {
        rebuild_validation::validate(block_file)?;
        let prefix = block_file.block.height;
        for (name, value) in [
            (
                format!("{prefix}.blockfile.json"),
                serde_json::to_value(block_file)?,
            ),
            (
                format!("{prefix}.header.json"),
                serde_json::to_value(header)?,
            ),
            (
                format!("{prefix}.validation.json"),
                serde_json::to_value(block_file.validation())?,
            ),
        ] {
            files.insert(name.clone(), write_json(&staging.join(&name), &value)?);
        }
    }
    write_json(
        &staging.join("manifest.json"),
        &serde_json::json!({
            "format": FORMAT, "input_sha256": input_hash, "files": files,
            "generator_sha256": args.generator_sha256, "reference": "restored_archive_pg",
            "chain_id": args.chain_id, "protocol_version": protocol,
            "batch": batch, "from_block": from.0, "to_block": to.0,
            "execution_verified": true, "state_root_recomputed": false,
            "state_writes_verified": true,
            "canonical_publish_ready": false,
            "system_execution": "block-scoped traces and events with empty tx_id; no synthetic transaction",
        }),
    )?;
    fs::File::open(&staging)?.sync_all()?;
    fs::rename(&staging, &destination)?;
    fs::File::open(&args.output_dir)?.sync_all()?;
    Ok(())
}

fn set_batch_number(call: &mut Call, batch: L1BatchNumber) {
    for event in &mut call.events {
        event.location.0 = batch;
    }
    for child in &mut call.calls {
        set_batch_number(child, batch);
    }
}

/// Compare physical rollback with VM logs before applying the protocol's
/// transaction-summary / inherited-failure classification to exported events.
fn captured_committed_events(calls: &[Call]) -> anyhow::Result<Vec<VmEvent>> {
    fn collect(
        calls: &[Call],
        failed: bool,
        seen: &mut BTreeSet<u32>,
        events: &mut Vec<(u32, VmEvent)>,
    ) -> anyhow::Result<()> {
        for call in calls {
            ensure!(
                call.events.len() == call.event_metadata.len(),
                "incomplete execution event metadata"
            );
            let failed = failed || call.parent_failed || debank::call_failed(call);
            for (event, metadata) in call.events.iter().zip(&call.event_metadata) {
                ensure!(
                    metadata.timestamp > 0 && seen.insert(metadata.timestamp),
                    "duplicate or missing event emission identity"
                );
                if !failed && !metadata.reverted {
                    events.push((metadata.timestamp, event.clone()));
                }
            }
            collect(&call.calls, failed, seen, events)?;
        }
        Ok(())
    }
    let mut events = vec![];
    collect(calls, false, &mut BTreeSet::new(), &mut events)?;
    events.sort_by_key(|event| event.0);
    Ok(events.into_iter().map(|event| event.1).collect())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let mut args = Args::from_args();
    args.generator_sha256 = rebuild_version::executable_sha256()?;
    ensure!(
        args.start_batch > 0 && args.start_batch <= args.end_batch,
        "invalid batch range"
    );
    fs::create_dir_all(&args.output_dir)?;
    let pool = ConnectionPool::<Core>::builder(args.database_url.clone(), 1)
        .build()
        .await?;
    let rt = Handle::current();
    let replay_pool = pool.clone();
    let result = tokio::task::spawn_blocking(move || {
        for batch in args.start_batch..=args.end_batch {
            replay(replay_pool.clone(), &args, batch, rt.clone())
                .with_context(|| format!("batch {batch}"))?;
        }
        anyhow::Ok(())
    })
    .await;
    pool.close().await;
    result??;
    Ok(())
}
