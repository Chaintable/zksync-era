use std::collections::HashSet;

use zksync_multivm::tracers::debank::{
    add_trace_log, root_trace_id, set_parent_failed, to_debank_trace,
};
use zksync_types::H256;
use zksync_vm_interface::{Call, CallEventMetadata, VmEvent};

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

#[test]
fn legacy_positions_are_rebuilt_at_every_depth() {
    let leaves = vec![Call::default(), Call::default()];
    let mut root = Call {
        trace_id: root_trace_id(H256::zero()),
        calls: vec![
            Call {
                calls: leaves.clone(),
                ..Call::default()
            },
            Call {
                calls: leaves,
                ..Call::default()
            },
        ],
        ..Call::default()
    };
    let mut traces = vec![];
    add_trace_log(
        H256::zero(),
        &mut traces,
        &mut vec![],
        &mut vec![],
        &mut vec![],
        vec![],
        &mut root,
    );
    assert_eq!(traces.len(), 6);
    assert_eq!(
        traces
            .iter()
            .map(|trace| &trace.id)
            .collect::<HashSet<_>>()
            .len(),
        6
    );
    for trace in &traces {
        assert_eq!(
            trace.pos_in_parent_trace,
            *trace.trace_address.last().unwrap()
        );
        if trace.trace_address.len() > 1 {
            let parent = traces
                .iter()
                .find(|parent| {
                    parent.trace_address == trace.trace_address[..trace.trace_address.len() - 1]
                })
                .unwrap();
            assert_eq!(trace.parent_trace_id, parent.id);
        }
    }
}

#[test]
fn root_identity_uses_the_entire_transaction_hash() {
    let first = H256::repeat_byte(0x11);
    let mut second = first;
    second.0[10] = 0x22;
    assert_eq!(first.to_string(), second.to_string());
    assert_ne!(root_trace_id(first), root_trace_id(second));
}

#[test]
fn duplicate_logs_keep_their_frames_and_execution_order() {
    let event = VmEvent::default();
    let mut root = Call {
        trace_id: root_trace_id(H256::zero()),
        events: vec![event.clone(), event.clone()],
        event_metadata: vec![
            CallEventMetadata {
                timestamp: 10,
                reverted: false,
            },
            CallEventMetadata {
                timestamp: 30,
                reverted: false,
            },
        ],
        calls: vec![Call {
            call_start_timestamp: 15,
            events: vec![event],
            event_metadata: vec![CallEventMetadata {
                timestamp: 20,
                reverted: false,
            }],
            ..Call::default()
        }],
        ..Call::default()
    };
    let mut events = vec![];
    add_trace_log(
        H256::zero(),
        &mut vec![],
        &mut vec![],
        &mut events,
        &mut vec![],
        vec![],
        &mut root,
    );
    assert_eq!(events.len(), 3);
    assert_eq!(
        events
            .iter()
            .map(|event| event.log_index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(events[0].parent_trace_id, root.trace_id);
    assert_eq!(events[1].parent_trace_id, root.calls[0].trace_id);
    assert_eq!(events[2].parent_trace_id, root.trace_id);
    assert_eq!(events[0].pos_in_parent_trace, 0);
    assert_eq!(root.calls[0].pos_in_parent_trace, 1);
    assert_eq!(events[2].pos_in_parent_trace, 2);
    assert_eq!(
        events
            .iter()
            .map(|event| &event.id)
            .collect::<HashSet<_>>()
            .len(),
        3
    );
}

#[test]
fn near_call_rollback_survives_outer_success_and_failure_propagation() {
    let mut root = Call {
        events: vec![VmEvent::default()],
        event_metadata: vec![CallEventMetadata {
            timestamp: 10,
            reverted: true,
        }],
        calls: vec![Call {
            parent_failed: true,
            calls: vec![Call::default()],
            ..Call::default()
        }],
        ..Call::default()
    };
    set_parent_failed(&mut root, false);
    assert!(root.calls[0].parent_failed);
    assert!(root.calls[0].calls[0].parent_failed);
    let mut events = vec![];
    let mut error_events = vec![];
    add_trace_log(
        H256::zero(),
        &mut vec![],
        &mut vec![],
        &mut events,
        &mut error_events,
        vec![],
        &mut root,
    );
    assert!(events.is_empty());
    assert_eq!(error_events.len(), 1);
}

#[test]
fn batch_tip_has_block_scoped_references_without_a_synthetic_transaction() {
    use zksync_multivm::tracers::debank::append_batch_tip;
    use zksync_types::debank::{BlockFile, DebankBlock};
    use zksync_vm_interface::FinishedL1Batch;

    let mut tip = FinishedL1Batch::mock();
    tip.block_tip_call_traces = vec![Call {
        events: vec![VmEvent::default()],
        event_metadata: vec![CallEventMetadata {
            timestamp: 10,
            reverted: false,
        }],
        self_storage_change: true,
        storage_change: true,
        ..Call::default()
    }];
    let mut block = BlockFile {
        block: DebankBlock {
            id: H256::repeat_byte(1),
            ..Default::default()
        },
        ..Default::default()
    };
    append_batch_tip(&mut block, &tip);
    assert!(block.transactions.is_empty());
    assert_eq!(block.traces.len(), 2);
    assert!(block.traces.iter().all(|trace| trace.tx_id.is_empty()));
    assert_eq!(block.events.len(), 1);
    assert!(block.events[0].tx_id.is_empty());
    assert!(block
        .traces
        .iter()
        .any(|trace| trace.id == block.events[0].parent_trace_id));
    let first_id = block.events[0].id.clone();
    let mut other = BlockFile {
        block: DebankBlock {
            id: H256::repeat_byte(2),
            ..Default::default()
        },
        ..Default::default()
    };
    append_batch_tip(&mut other, &tip);
    assert_ne!(other.events[0].id, first_id);
}
