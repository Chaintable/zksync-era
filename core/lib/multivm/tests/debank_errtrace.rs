use zksync_multivm::tracers::debank::{add_trace_log, set_parent_failed, to_debank_trace};
use zksync_types::H256;
use zksync_vm_interface::Call;

const PARENT_CALL_FAILED_ERROR: &str = "parent call failed";

fn call_with_error(error: Option<&str>, calls: Vec<Call>) -> Call {
    Call {
        revert_reason: error.map(str::to_owned),
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
