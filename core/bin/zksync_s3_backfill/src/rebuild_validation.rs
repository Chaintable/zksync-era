use std::collections::{BTreeMap, BTreeSet};

use anyhow::{ensure, Context};
use zksync_multivm::tracers::debank::to_hash;
use zksync_types::debank::BlockFile;

/// Validate the actual serialized graph, independently of the Call traversal.
pub fn validate(block: &BlockFile) -> anyhow::Result<()> {
    let tx_ids: BTreeSet<_> = block.transactions.iter().map(|tx| tx.id.as_str()).collect();
    ensure!(
        tx_ids.len() == block.transactions.len(),
        "duplicate transaction ID"
    );
    let mut ids = BTreeSet::new();
    let mut paths = BTreeSet::new();
    let mut traces = BTreeMap::new();
    let mut roots = BTreeSet::new();
    for trace in block.traces.iter().chain(&block.error_traces) {
        ensure!(
            !trace.id.is_empty() && ids.insert(trace.id.as_str()),
            "duplicate or empty trace ID"
        );
        ensure!(
            trace.tx_id.is_empty() || tx_ids.contains(trace.tx_id.as_str()),
            "trace references unknown transaction"
        );
        ensure!(
            paths.insert((trace.tx_id.as_str(), &trace.trace_address)),
            "duplicate trace path"
        );
        if trace.trace_address.is_empty() {
            ensure!(trace.parent_trace_id.is_empty(), "root has a parent");
            let expected = if trace.tx_id.is_empty() {
                to_hash(&["batch_tip", &format!("{:#x}", block.block.id), "0"])
            } else {
                to_hash(&[&trace.tx_id, "", "0"])
            };
            ensure!(
                trace.id == expected,
                "root identity disagrees with its scope"
            );
            roots.insert(trace.tx_id.as_str());
        }
        traces.insert(trace.id.as_str(), trace);
    }
    ensure!(tx_ids.is_subset(&roots), "transaction has no root trace");
    let mut positions: BTreeMap<&str, BTreeSet<u32>> = BTreeMap::new();
    let mut children: BTreeMap<&str, BTreeSet<u32>> = BTreeMap::new();
    for trace in traces.values() {
        if trace.trace_address.is_empty() {
            continue;
        }
        let parent = traces
            .get(trace.parent_trace_id.as_str())
            .context("trace parent is missing")?;
        ensure!(
            parent.tx_id == trace.tx_id
                && parent.trace_address == trace.trace_address[..trace.trace_address.len() - 1],
            "trace parent and path disagree"
        );
        let scope = if trace.tx_id.is_empty() {
            format!("{:#x}", block.block.id)
        } else {
            trace.tx_id.clone()
        };
        ensure!(
            trace.id == to_hash(&[&scope, &parent.id, &trace.pos_in_parent_trace.to_string()]),
            "child identity disagrees with its parent and position"
        );
        ensure!(
            positions
                .entry(parent.id.as_str())
                .or_default()
                .insert(trace.pos_in_parent_trace),
            "duplicate mixed child position"
        );
        children
            .entry(parent.id.as_str())
            .or_default()
            .insert(*trace.trace_address.last().unwrap());
    }
    for event in block.events.iter().chain(&block.error_events) {
        ensure!(
            !event.id.is_empty() && ids.insert(event.id.as_str()),
            "duplicate or empty event ID"
        );
        let parent = traces
            .get(event.parent_trace_id.as_str())
            .context("event parent is missing")?;
        ensure!(
            parent.tx_id == event.tx_id,
            "event and parent transactions disagree"
        );
        ensure!(
            event.id == to_hash(&[&parent.id, &event.pos_in_parent_trace.to_string()]),
            "event identity disagrees with its parent and position"
        );
        ensure!(
            positions
                .entry(parent.id.as_str())
                .or_default()
                .insert(event.pos_in_parent_trace),
            "duplicate mixed event position"
        );
    }
    for trace in traces.values() {
        let indices = children.get(trace.id.as_str()).cloned().unwrap_or_default();
        ensure!(
            indices.len() == trace.subtraces as usize
                && indices.iter().copied().eq(0..trace.subtraces),
            "child count or path indices disagree"
        );
    }
    for positions in positions.values() {
        ensure!(
            positions.iter().copied().eq(0..positions.len() as u32),
            "mixed positions are not contiguous"
        );
    }
    ensure!(
        block
            .events
            .iter()
            .enumerate()
            .all(|(index, event)| event.log_index == index as u32),
        "committed event indices are not contiguous"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use zksync_types::debank::{DebankEvent, DebankTrace, DebankTransaction};

    use super::*;

    #[test]
    fn rejects_corruption_of_an_otherwise_closed_graph() {
        let tx = format!("{:#x}", zksync_types::H256::repeat_byte(1));
        let root = to_hash(&[&tx, "", "0"]);
        let child = to_hash(&[&tx, &root, "0"]);
        let block = BlockFile {
            transactions: vec![DebankTransaction {
                id: tx.clone(),
                ..Default::default()
            }],
            traces: vec![
                DebankTrace {
                    id: root.clone(),
                    tx_id: tx.clone(),
                    subtraces: 1,
                    ..Default::default()
                },
                DebankTrace {
                    id: child,
                    tx_id: tx.clone(),
                    parent_trace_id: root.clone(),
                    trace_address: vec![0],
                    ..Default::default()
                },
            ],
            events: vec![DebankEvent {
                id: to_hash(&[&root, "1"]),
                tx_id: tx,
                parent_trace_id: root,
                pos_in_parent_trace: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        validate(&block).unwrap();
        let mut duplicate = block.clone();
        duplicate.error_traces.push(duplicate.traces[1].clone());
        assert!(validate(&duplicate).is_err());
        let mut missing_parent = block.clone();
        missing_parent.events[0].parent_trace_id = "missing".into();
        assert!(validate(&missing_parent).is_err());
        let mut wrong_path = block.clone();
        wrong_path.traces[1].trace_address = vec![1];
        assert!(validate(&wrong_path).is_err());
        let mut wrong_identity = block.clone();
        wrong_identity.events[0].id = "wrong".into();
        assert!(validate(&wrong_identity).is_err());
        let mut wrong_position = block;
        wrong_position.events[0].pos_in_parent_trace = 0;
        assert!(validate(&wrong_position).is_err());
    }
}
