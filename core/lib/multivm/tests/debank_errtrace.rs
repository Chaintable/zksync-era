use zksync_multivm::tracers::debank::{add_trace_log, set_parent_failed, to_debank_trace};
use zksync_types::H256;
use zksync_vm_interface::{Call, VmEvent};

const PARENT_CALL_FAILED_ERROR: &str = "parent call failed";

fn call_with_error(error: Option<&str>, calls: Vec<Call>) -> Call {
    Call {
        revert_reason: error.map(str::to_owned),
        calls,
        ..Call::default()
    }
}

fn call_with_vm_error(error: &str, calls: Vec<Call>) -> Call {
    Call {
        error: Some(error.to_owned()),
        calls,
        ..Call::default()
    }
}

#[test]
fn failed_parent_routes_the_whole_subtree_to_error_traces() {
    let grandchild = call_with_error(Some("out of gas"), vec![]);
    let child = call_with_error(None, vec![grandchild]);
    let mut root = call_with_error(Some("execution reverted"), vec![child]);

    set_parent_failed(&mut root, false);
    assert!(root.calls[0].parent_failed);
    assert!(root.calls[0].calls[0].parent_failed);

    let tx_hash = H256::zero();
    let mut traces = vec![];
    let mut error_traces = vec![to_debank_trace(&root, tx_hash, vec![])];
    add_trace_log(
        tx_hash,
        &mut traces,
        &mut error_traces,
        &mut vec![],
        &mut vec![],
        vec![],
        &mut root,
    );

    assert!(traces.is_empty());
    assert_eq!(error_traces.len(), 3);
    let trace_at = |address: &[u32]| {
        error_traces
            .iter()
            .find(|trace| trace.trace_address == address)
            .unwrap()
    };
    assert_eq!(trace_at(&[]).error, "execution reverted");
    assert_eq!(trace_at(&[0]).error, PARENT_CALL_FAILED_ERROR);
    assert_eq!(trace_at(&[0, 0]).error, "out of gas");
}

#[test]
fn vm_error_is_preserved_and_propagated_to_descendants() {
    let grandchild = Call {
        events: vec![VmEvent::default()],
        ..Call::default()
    };
    let mut child = call_with_vm_error("Panic", vec![grandchild]);
    child.events.push(VmEvent::default());
    let mut root = call_with_error(None, vec![child]);

    set_parent_failed(&mut root, false);

    let tx_hash = H256::zero();
    let mut traces = vec![to_debank_trace(&root, tx_hash, vec![])];
    let mut error_traces = vec![];
    let mut events = vec![];
    let mut error_events = vec![];
    add_trace_log(
        tx_hash,
        &mut traces,
        &mut error_traces,
        &mut events,
        &mut error_events,
        vec![],
        &mut root,
    );

    assert_eq!(traces.len(), 1);
    assert_eq!(error_traces.len(), 2);
    assert_eq!(error_traces[0].error, "parent call failed");
    assert_eq!(error_traces[1].error, "Panic");
    assert!(events.is_empty());
    assert_eq!(error_events.len(), 2);
}

#[test]
fn successful_tree_remains_in_normal_outputs() {
    let child = Call {
        events: vec![VmEvent::default()],
        ..Call::default()
    };
    let mut root = Call {
        calls: vec![child],
        events: vec![VmEvent::default()],
        ..Call::default()
    };

    set_parent_failed(&mut root, false);

    let tx_hash = H256::zero();
    let mut traces = vec![to_debank_trace(&root, tx_hash, vec![])];
    let mut error_traces = vec![];
    let mut events = vec![];
    let mut error_events = vec![];
    add_trace_log(
        tx_hash,
        &mut traces,
        &mut error_traces,
        &mut events,
        &mut error_events,
        vec![],
        &mut root,
    );

    assert_eq!(traces.len(), 2);
    assert!(error_traces.is_empty());
    assert_eq!(events.len(), 2);
    assert!(error_events.is_empty());
}
