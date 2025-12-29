use md5::Context as Md5;
use zksync_types::zk_evm_types::FarCallOpcode;
use zksync_types::{
    debank::{DebankEvent, DebankTrace},
    H256,
};
use zksync_vm_interface::{Call, CallType};

pub fn to_hash(args: &[&str]) -> String {
    let mut hasher = Md5::new();
    for arg in args {
        hasher.consume(arg.as_bytes());
    }
    format!("{:x}", hasher.compute())
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
    for (i, subcall) in &mut cf.calls.iter_mut().enumerate() {
        subcall.parent_trace_id = Some(cf.trace_id.clone());
        subcall.trace_id = to_hash(&[
            tx_hash.to_string().as_str(),
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
    for vm_event in &cf.events {
        //println!("Debank add_trace_log: vm_event = {:?}", vm_event);
        let pos_in_parent_trace = vm_event.position;
        let e = DebankEvent {
            parent_trace_id: cf.trace_id.clone(),
            pos_in_parent_trace,
            id: to_hash(&[&cf.trace_id, &pos_in_parent_trace.to_string()]),
            tx_id: Some(tx_hash.as_fixed_bytes().into()),
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
            ..Default::default()
        };

        if cf.revert_reason.is_some() || cf.parent_failed {
            outerrevents.push(e);
        } else {
            outevents.push(e);
        }
    }
    for (i, subcall) in cf.calls.iter().enumerate() {
        if subcall.revert_reason.is_some() || subcall.parent_failed {
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
        tx_id: Some(tx_hash),
        parent_trace_id: call.parent_trace_id.clone().unwrap_or_default(),
        pos_in_parent_trace: call.pos_in_parent_trace,
        self_storage_change: call.self_storage_change,
        storage_change: call.storage_change,
        sub_traces: call.calls.len() as u32,
        trace_address: trace_addresses,
        error: call.revert_reason.clone().unwrap_or_default(),
    }
}

pub fn child_trace_address(a: &[u32], i: u32) -> Vec<u32> {
    let mut child = Vec::with_capacity(a.len() + 1);
    child.extend_from_slice(a);
    child.push(i);
    child
}

pub fn set_parent_failed(cf: &mut Call, parent_failed: bool) {
    let failed = cf.revert_reason.is_some() || parent_failed;
    for subcall in &mut cf.calls {
        subcall.parent_failed = failed;
        set_parent_failed(subcall, failed);
    }
}
