use md5::Context as Md5;
use zksync_types::zk_evm_types::FarCallOpcode;
use zksync_types::{
    debank::{BlockFile, DebankEvent, DebankTrace, DebankTransaction, TxBlockData},
    l2::TransactionType,
    utils::deployed_address_evm_create,
    Address, ExecuteTransactionCommon, Transaction, H256,
};
use zksync_vm_interface::{Call, CallType, FinishedL1Batch};

const PARENT_CALL_FAILED_ERROR: &str = "parent call failed";

pub fn to_hash(args: &[&str]) -> String {
    let mut hasher = Md5::new();
    for arg in args {
        hasher.consume(arg.as_bytes());
    }
    format!("{:x}", hasher.compute())
}

pub fn root_trace_id(tx_hash: H256) -> String {
    to_hash(&[&format!("{tx_hash:#x}"), "", "0"])
}

/// Protocol genesis identity: kind byte, eleven zero bytes, account address.
/// Kinds 1/2/3 represent allocation balance, code and native-token projection.
pub fn genesis_tx_id(kind: u8, address: Address) -> H256 {
    let mut bytes = [0; 32];
    bytes[0] = kind;
    bytes[12..].copy_from_slice(address.as_bytes());
    H256(bytes)
}

/// The same conversion is used for freshly executed historical and live
/// transactions. Priority and protocol-upgrade transactions are real inputs too.
pub fn transaction_data(
    tx: &Transaction,
    index: u32,
    base_fee: u64,
    gas_used: u64,
    status: bool,
    call: Option<Call>,
) -> TxBlockData {
    let (to, gas_price, fee_cap, tip_cap) = match &tx.common_data {
        ExecuteTransactionCommon::L2(data) => {
            let to = tx.execute.contract_address.or_else(|| {
                Some(deployed_address_evm_create(
                    data.initiator_address,
                    (*data.nonce).into(),
                ))
            });
            let (fee_cap, tip_cap) =
                if data.transaction_type as u32 >= TransactionType::EIP1559Transaction as u32 {
                    (
                        data.fee.max_fee_per_gas.as_u64(),
                        data.fee.max_priority_fee_per_gas.as_u64(),
                    )
                } else {
                    (0, 0)
                };
            (
                to,
                data.fee.get_effective_gas_price(base_fee.into()).as_u64(),
                fee_cap,
                tip_cap,
            )
        }
        ExecuteTransactionCommon::L1(_) | ExecuteTransactionCommon::ProtocolUpgrade(_) => (
            tx.execute.contract_address,
            tx.max_fee_per_gas().as_u64(),
            tx.max_fee_per_gas().as_u64(),
            0,
        ),
    };
    let mut data = TxBlockData {
        debank_tx: DebankTransaction {
            id: format!("{:#x}", tx.hash()),
            from: tx.initiator_account(),
            to,
            gas_limit: tx.gas_limit().as_u64(),
            gas_price,
            gas_used,
            status,
            gas_fee_cap: fee_cap,
            gas_tip_cap: tip_cap,
            input: tx.execute.calldata.clone().into(),
            // Priority / upgrade transactions have no account nonce (the same
            // zero convention is used by the chain's Ethereum RPC projection).
            nonce: tx.nonce().map_or(0, |nonce| *nonce as u64),
            transaction_index: index,
            value: tx.execute.value,
        },
        traces: vec![],
        error_traces: vec![],
        events: vec![],
        error_events: vec![],
    };
    if let Some(mut call) = call {
        call.trace_id = root_trace_id(tx.hash());
        set_parent_failed(&mut call, false);
        let root = to_debank_trace(&call, tx.hash(), vec![]);
        if effective_failed(&call) {
            data.error_traces.push(root);
        } else {
            data.traces.push(root);
        }
        add_trace_log(
            tx.hash(),
            &mut data.traces,
            &mut data.error_traces,
            &mut data.events,
            &mut data.error_events,
            vec![],
            &mut call,
        );
    }
    data
}

/// Batch finalization is block-scoped execution, not a transaction. Its frames
/// have an empty tx_id and an identity rooted in the canonical block hash.
/// Events still reference their actual emitting frame.
pub fn append_batch_tip(block: &mut BlockFile, tip: &FinishedL1Batch) {
    if tip.block_tip_call_traces.is_empty() {
        return;
    }
    let stats = &tip.block_tip_execution_result.statistics;
    let mut root = Call::new_high_level(
        stats.gas_used.saturating_add(stats.gas_remaining.into()),
        stats.gas_used,
        Default::default(),
        vec![],
        vec![],
        None,
        tip.block_tip_call_traces.clone(),
    );
    root.trace_id = to_hash(&["batch_tip", &format!("{:#x}", block.block.id), "0"]);
    set_parent_failed(&mut root, false);
    let first_trace = block.traces.len();
    let first_error_trace = block.error_traces.len();
    let first_event = block.events.len();
    let first_error_event = block.error_events.len();
    block
        .traces
        .push(to_debank_trace(&root, block.block.id, vec![]));
    add_trace_log(
        block.block.id,
        &mut block.traces,
        &mut block.error_traces,
        &mut block.events,
        &mut block.error_events,
        vec![],
        &mut root,
    );
    for trace in block.traces[first_trace..]
        .iter_mut()
        .chain(&mut block.error_traces[first_error_trace..])
    {
        trace.tx_id.clear();
    }
    for event in block.events[first_event..]
        .iter_mut()
        .chain(&mut block.error_events[first_error_event..])
    {
        event.tx_id.clear();
    }
    for (index, event) in block.events.iter_mut().enumerate() {
        event.log_index = index as u32;
    }
    block.storage_contracts =
        zksync_types::debank::collect_storage_contracts(&block.traces, &block.error_traces);
}

pub fn add_trace_log(
    tx_hash: H256,
    outtraces: &mut Vec<DebankTrace>,
    outerrtraces: &mut Vec<DebankTrace>,
    outevents: &mut Vec<DebankEvent>,
    outerrevents: &mut Vec<DebankEvent>,
    trace_addresses: Vec<u32>,
    cf: &mut Call,
) {
    let first_event = outevents.len();
    let first_error_event = outerrevents.len();
    cf.assign_child_positions();
    for (i, subcall) in &mut cf.calls.iter_mut().enumerate() {
        // Legacy PG calls have no position metadata. Derive positions from the
        // tree at every depth instead of trusting their default zero values.
        subcall.parent_trace_id = Some(cf.trace_id.clone());
        subcall.trace_id = to_hash(&[
            &format!("{tx_hash:#x}"),
            &cf.trace_id,
            &subcall.pos_in_parent_trace.to_string(),
        ]);
        add_trace_log(
            tx_hash,
            outtraces,
            outerrtraces,
            outevents,
            outerrevents,
            child_trace_address(&trace_addresses, i as u32),
            subcall,
        );
    }
    for (index, vm_event) in cf.events.iter().enumerate() {
        let metadata = cf.event_metadata.get(index);
        let pos_in_parent_trace = vm_event.position;
        let e = DebankEvent {
            parent_trace_id: cf.trace_id.clone(),
            pos_in_parent_trace,
            id: to_hash(&[&cf.trace_id, &pos_in_parent_trace.to_string()]),
            tx_id: format!("{:#x}", tx_hash),
            contract_id: vm_event.address.as_fixed_bytes().into(),
            selector: vm_event
                .indexed_topics
                .get(0)
                .map(|topic| format!("{:#x}", topic))
                .unwrap_or_default(),
            topics: vm_event
                .indexed_topics
                .iter()
                .skip(1)
                .map(|topic| format!("{:#x}", topic))
                .collect(),
            data: vm_event.value.clone().into(),
            log_index: metadata.map_or(vm_event.position, |metadata| metadata.timestamp),
            ..Default::default()
        };

        if effective_failed(cf) || metadata.is_some_and(|metadata| metadata.reverted) {
            outerrevents.push(e);
        } else {
            outevents.push(e);
        }
    }
    for (i, subcall) in cf.calls.iter().enumerate() {
        if effective_failed(subcall) {
            outerrtraces.push(to_debank_trace(
                &subcall,
                tx_hash,
                child_trace_address(&trace_addresses, i as u32),
            ));
        } else {
            outtraces.push(to_debank_trace(
                &subcall,
                tx_hash,
                child_trace_address(&trace_addresses, i as u32),
            ));
        }
    }
    if trace_addresses.is_empty() {
        // Tree traversal order differs from emission order: a parent may emit
        // both before and after its child. Timestamps also distinguish duplicates.
        outevents[first_event..].sort_by_key(|event| event.log_index);
        outerrevents[first_error_event..].sort_by_key(|event| event.log_index);
        for (index, event) in outevents[first_event..].iter_mut().enumerate() {
            event.log_index = index as u32;
        }
        for event in &mut outerrevents[first_error_event..] {
            event.log_index = 0;
        }
    }
}

pub fn to_debank_trace(call: &Call, tx_hash: H256, trace_addresses: Vec<u32>) -> DebankTrace {
    let (call_create_type, call_type) = match call.r#type {
        CallType::Create => (String::from("create"), String::new()),
        CallType::Call(farcall) => match farcall {
            FarCallOpcode::Normal | FarCallOpcode::Mimic => {
                (String::from("call"), String::from("call"))
            }
            FarCallOpcode::Delegate => (String::from("call"), String::from("delegatecall")),
        },
        _ => ("empty".to_string(), String::new()),
    };

    DebankTrace {
        id: call.trace_id.clone(),
        from_addr: call.from.to_fixed_bytes().into(),
        gas_limit: call.gas.into(),
        input: call.input.clone().into(),
        to_addr: call.to.as_fixed_bytes().into(),
        value: call.value,
        gas_used: call.gas_used.into(),
        output: call.output.clone().into(),
        call_create_type,
        call_type,
        tx_id: format!("{:#x}", tx_hash),
        parent_trace_id: call.parent_trace_id.clone().unwrap_or_default(),
        pos_in_parent_trace: call.pos_in_parent_trace,
        self_storage_change: call.self_storage_change,
        storage_change: call.storage_change,
        subtraces: call.calls.len() as u32,
        trace_address: trace_addresses,
        error: trace_error(call),
    }
}

fn trace_error(call: &Call) -> String {
    if let Some(error) = &call.revert_reason {
        return error.clone();
    }
    if let Some(error) = &call.error {
        return error.clone();
    }
    if call.parent_failed {
        return PARENT_CALL_FAILED_ERROR.to_owned();
    }
    String::new()
}

pub fn call_failed(call: &Call) -> bool {
    call.revert_reason.is_some() || call.error.is_some()
}

pub fn effective_failed(call: &Call) -> bool {
    call_failed(call) || call.parent_failed
}

pub fn child_trace_address(a: &[u32], i: u32) -> Vec<u32> {
    let mut child = Vec::with_capacity(a.len() + 1);
    child.extend_from_slice(a);
    child.push(i);
    child
}

pub fn set_parent_failed(cf: &mut Call, parent_failed: bool) {
    let failed = call_failed(cf) || cf.parent_failed || parent_failed;
    for subcall in &mut cf.calls {
        subcall.parent_failed |= failed;
        set_parent_failed(subcall, failed);
    }
}
