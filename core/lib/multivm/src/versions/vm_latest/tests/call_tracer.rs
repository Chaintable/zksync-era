use std::sync::Arc;

use once_cell::sync::OnceCell;
use zksync_test_contracts::TestContract;
use zksync_types::{Address, Execute};

use super::TestedLatestVm;
use crate::{
    interface::{Call, InspectExecutionMode, TxExecutionMode, VmEvent, VmInterface},
    tracers::CallTracer,
    versions::testonly::{call_tracer, read_max_depth_contract, ContractToDeploy, VmTesterBuilder},
    vm_latest::{constants::BATCH_COMPUTATIONAL_GAS_LIMIT, ToTracerPointer, Vm},
};

#[test]
fn execution_events_and_storage_writes_are_captured_from_the_vm() {
    check_execution_capture(0);
}

#[test]
fn reverted_emissions_preserve_protocol_failure_classification() {
    check_execution_capture(1);
}

fn check_execution_capture(deploys: u32) {
    let contract = TestContract::load_test();
    let address = Address::repeat_byte(0xA5);
    let mut tester = VmTesterBuilder::new()
        .with_rich_accounts(1)
        .with_execution_mode(TxExecutionMode::VerifyExecute)
        .with_custom_contracts(vec![ContractToDeploy::new(
            contract.bytecode.to_vec(),
            address,
        )])
        .build::<TestedLatestVm>();
    let calldata = contract
        .function("execute")
        .encode_input(
            &[0u32, 2, 0, 0, 2, 0, deploys]
                .map(|value| zksync_types::ethabi::Token::Uint(value.into())),
        )
        .unwrap();
    let tx = tester.rich_accounts[0].get_l2_tx_for_execute(
        Execute {
            contract_address: Some(address),
            calldata,
            value: Default::default(),
            factory_deps: vec![],
        },
        None,
    );
    let traces = Arc::new(OnceCell::new());
    let tracer = CallTracer::new(traces.clone()).into_tracer_pointer();
    tester.vm.push_transaction(tx.clone());
    let result = tester
        .vm
        .inspect(&mut tracer.into(), InspectExecutionMode::OneTx);
    assert_eq!(
        result.result.is_failed(),
        deploys > 0,
        "{:?}",
        result.result
    );
    let output = match &result.result {
        crate::interface::ExecutionResult::Success { output } => output.clone(),
        crate::interface::ExecutionResult::Revert { output } => output.encoded_data(),
        other => panic!("unexpected result: {other:?}"),
    };
    let live = crate::interface::TransactionExecutionResult {
        hash: tx.hash(),
        transaction: tx.clone(),
        execution_info: result.get_execution_metrics(),
        execution_status: if deploys == 0 {
            crate::interface::TxExecutionStatus::Success
        } else {
            crate::interface::TxExecutionStatus::Failure
        },
        refunded_gas: result.refunds.gas_refunded,
        revert_reason: match &result.result {
            crate::interface::ExecutionResult::Revert { output } => Some(output.to_string()),
            _ => None,
        },
        call_traces: traces.get().unwrap().clone(),
        output: output.clone(),
    };
    let data =
        crate::tracers::debank::transaction_data(&tx, 0, 0, 0, deploys == 0, live.call_trace());
    assert_eq!(
        data.traces
            .iter()
            .chain(&data.error_traces)
            .find(|trace| trace.trace_address.is_empty())
            .unwrap()
            .output
            .0,
        output
    );

    fn collect(calls: &[Call], failed: bool, events: &mut Vec<(u32, VmEvent)>) {
        for call in calls {
            assert_eq!(call.events.len(), call.event_metadata.len());
            let failed = failed
                || call.parent_failed
                || call.error.is_some()
                || call.revert_reason.is_some();
            for (event, metadata) in call.events.iter().zip(&call.event_metadata) {
                if !failed && !metadata.reverted {
                    events.push((metadata.timestamp, event.clone()));
                }
            }
            collect(&call.calls, failed, events);
        }
    }
    let calls = traces.get().unwrap();
    let mut events = vec![];
    collect(calls, false, &mut events);
    events.sort_by_key(|event| event.0);
    let keys = |events: Vec<VmEvent>| {
        events
            .into_iter()
            .map(|event| (event.address, event.indexed_topics, event.value))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        keys(events.into_iter().map(|event| event.1).collect()),
        keys(result.logs.events.clone())
    );
    let projected = data
        .events
        .iter()
        .map(|event| {
            let topics = std::iter::once(&event.selector)
                .filter(|topic| !topic.is_empty())
                .chain(&event.topics)
                .map(|topic| topic.parse::<zksync_types::H256>().unwrap())
                .collect::<Vec<_>>();
            (event.contract_id, topics, event.data.0.clone())
        })
        .collect::<Vec<_>>();
    if deploys == 0 {
        assert_eq!(projected, keys(result.logs.events));
    } else {
        // The protocol propagates the transaction-summary failure even to
        // committed fee/refund calls. Raw rollback metadata is checked above.
        assert!(data.events.is_empty());
        assert!(
            data.error_events
                .iter()
                .filter(|event| event.contract_id == address)
                .count()
                >= 2
        );
    }
    fn has_write(calls: &[Call], address: Address) -> bool {
        calls.iter().any(|call| {
            (call.to == address && call.self_storage_change) || has_write(&call.calls, address)
        })
    }
    assert!(has_write(calls, address));

    let tip_traces = Arc::new(OnceCell::new());
    let tracer = CallTracer::new(tip_traces.clone()).into_tracer_pointer();
    let finished = tester.vm.finish_batch_with_tracer(
        &mut tracer.into(),
        crate::versions::testonly::default_pubdata_builder(),
    );
    assert!(!finished.block_tip_execution_result.result.is_failed());
    let tip_calls = tip_traces.get().unwrap();
    assert!(!tip_calls.is_empty());
    let mut tip_events = vec![];
    collect(tip_calls, false, &mut tip_events);
    tip_events.sort_by_key(|event| event.0);
    assert_eq!(
        keys(tip_events.into_iter().map(|event| event.1).collect()),
        keys(finished.block_tip_execution_result.logs.events),
    );
}

// This test is ultra slow, so it's ignored by default.
#[test]
#[ignore]
fn test_max_depth() {
    let contarct = read_max_depth_contract();
    let address = Address::random();
    let mut vm = VmTesterBuilder::new()
        .with_rich_accounts(1)
        .with_bootloader_gas_limit(BATCH_COMPUTATIONAL_GAS_LIMIT)
        .with_execution_mode(TxExecutionMode::VerifyExecute)
        .with_custom_contracts(vec![ContractToDeploy::account(contarct, address)])
        .build::<TestedLatestVm>();

    let account = &mut vm.rich_accounts[0];
    let tx = account.get_l2_tx_for_execute(
        Execute {
            contract_address: Some(address),
            calldata: vec![],
            value: Default::default(),
            factory_deps: vec![],
        },
        None,
    );

    let result = Arc::new(OnceCell::new());
    let call_tracer = CallTracer::new(result.clone()).into_tracer_pointer();
    vm.vm.push_transaction(tx);
    let res = vm
        .vm
        .inspect(&mut call_tracer.into(), InspectExecutionMode::OneTx);
    assert!(result.get().is_some());
    assert!(res.result.is_failed());
}

#[test]
fn basic_behavior() {
    call_tracer::test_basic_behavior::<Vm<_, _>>();
}

#[test]
fn transfer() {
    call_tracer::test_transfer::<Vm<_, _>>();
}

#[test]
fn reverted_tx() {
    call_tracer::test_reverted_tx::<Vm<_, _>>();
}

#[test]
fn reverted_deployment() {
    call_tracer::test_reverted_deployment_tx::<Vm<_, _>>();
}

#[test]
fn out_of_gas() {
    call_tracer::test_out_of_gas::<Vm<_, _>>();
}

#[test]
fn recursive_tx() {
    call_tracer::test_recursive_tx::<Vm<_, _>>();
}

#[test]
fn evm_to_eravm_call() {
    call_tracer::test_evm_to_eravm_call::<Vm<_, _>>();
}

#[test]
fn evm_deployment_tx() {
    call_tracer::test_evm_deployment_tx::<Vm<_, _>>();
}

#[test]
fn evm_deployment_from_contract() {
    call_tracer::test_evm_deployment_from_contract::<Vm<_, _>>();
}
