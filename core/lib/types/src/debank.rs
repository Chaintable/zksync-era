use crate::{Address, H256, H64, U256};
use hex;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::collections::HashSet;
use std::str::FromStr;
use zksync_basic_types::{web3::Bytes, Bloom};

mod hex_u64 {
    use serde::{self, Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("0x{:x}", value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<u64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let s = s.strip_prefix("0x").unwrap_or(&s);
        u64::from_str_radix(s, 16).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Header {
    #[serde(with = "hex_u64")]
    pub number: u64,
    pub hash: H256,
    pub parent_hash: H256,
    pub nonce: H64,
    pub mix_hash: H256,
    pub sha3_uncles: H256,
    pub logs_bloom: Bloom,
    pub state_root: H256,
    pub miner: Address,
    pub difficulty: U256,
    pub extra_data: Bytes,
    #[serde(with = "hex_u64")]
    pub gas_limit: u64,
    #[serde(with = "hex_u64")]
    pub gas_used: u64,
    #[serde(with = "hex_u64")]
    pub timestamp: u64,
    pub transactions_root: H256,
    pub receipts_root: H256,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_fee_per_gas: Option<U256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withdrawals_root: Option<H256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_gas_used: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excess_blob_gas: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_beacon_block_root: Option<H256>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requests_root: Option<H256>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DebankBlock {
    pub id: H256,
    pub height: u64,
    pub parent_id: H256,
    pub base_fee_per_gas: Option<u64>,
    pub miner: Address,
    pub gas_limit: u64,
    pub gas_used: u64,
    pub timestamp: u64,
    pub process_start_timestamp: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DebankTransaction {
    pub id: String,
    #[serde(rename = "from_addr")]
    pub from: Address,
    #[serde(rename = "to_addr")]
    pub to: Option<Address>,
    pub gas_limit: u64,
    pub gas_price: u64,
    pub gas_used: u64,
    pub status: bool,
    #[serde(rename = "max_fee_per_gas")]
    pub gas_fee_cap: u64,
    #[serde(rename = "max_priority_fee_per_gas")]
    pub gas_tip_cap: u64,
    pub input: Bytes,
    pub nonce: u64,
    #[serde(rename = "idx")]
    pub transaction_index: u32,
    pub value: U256,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DebankTrace {
    pub id: String,
    pub from_addr: Address,
    pub gas_limit: u64,
    pub input: Bytes,
    pub to_addr: Address,
    pub value: U256,
    pub gas_used: u64,
    pub output: Bytes,
    #[serde(rename = "type")]
    pub call_create_type: String,
    pub call_type: String,
    pub tx_id: String,
    pub parent_trace_id: String,
    pub pos_in_parent_trace: u32,
    pub self_storage_change: bool,
    pub storage_change: bool,
    pub subtraces: u32,
    pub trace_address: Vec<u32>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DebankEvent {
    pub id: String,
    pub contract_id: Address,
    pub selector: String,
    pub topics: Vec<String>,
    pub data: Bytes,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub tx_id: String,
    pub parent_trace_id: String,
    pub pos_in_parent_trace: u32,
    #[serde(rename = "idx")]
    pub log_index: u32,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DebankSimulateResp {
    pub results: Vec<DebankSingleSimulateResult>,
    pub stats: DebankSimulateStats,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DebankSingleSimulateResult {
    pub traces: Vec<DebankTrace>,
    pub events: Vec<DebankEvent>,
    pub code: i32,
    pub err: String,
    pub gas_used: u64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DebankSimulateStats {
    /// blockNum
    pub block_num: u64,
    /// blockHash
    pub block_hash: H256,
    /// blockTime
    pub block_time: u64,
    /// success
    pub success: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct BlockValidation {
    pub validation_hash: i64,
    is_fork: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct BlockFile {
    pub block: DebankBlock,
    #[serde(rename = "txs")]
    pub transactions: Vec<DebankTransaction>,
    pub events: Vec<DebankEvent>,
    pub traces: Vec<DebankTrace>,
    pub error_events: Vec<DebankEvent>,
    pub error_traces: Vec<DebankTrace>,
    pub storage_contracts: Vec<String>,
}

impl BlockFile {
    pub fn validation(&self) -> BlockValidation {
        let mut ids = Vec::new();
        ids.push(self.block.id.to_string());
        for transaction in self.transactions.iter() {
            ids.push(transaction.id.to_string());
        }
        for event in self.events.iter() {
            ids.push(event.id.clone())
        }
        for trace in self.traces.iter() {
            ids.push(trace.id.clone())
        }
        BlockValidation {
            validation_hash: calc_validation_hash(&ids),
            is_fork: false,
        }
    }
}

pub fn calc_validation_hash(ids: &[String]) -> i64 {
    let mut sha1_sum = U256::from(0);
    for each in ids {
        let mut hasher = Sha1::new();
        hasher.update(each.as_bytes());
        let hash_int = U256::from_str_radix(&hex::encode(hasher.finalize()), 16)
            .unwrap_or_else(|_| panic!("Failed to convert id {} to U256", each));
        sha1_sum += hash_int;
    }
    let sha1_sum_str = sha1_sum.to_string();
    let last_6_digits = if sha1_sum_str.len() >= 6 {
        &sha1_sum_str[sha1_sum_str.len().saturating_sub(6)..]
    } else {
        &sha1_sum_str
    };

    i64::from_str(last_6_digits).unwrap_or(0)
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct DebankOutPut {
    pub block_file: BlockFile,
    pub header: Header,
    pub state_diff: Bytes,
    pub validation_hash: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaBlockContext {
    pub hash: H256,
    pub parent_hash: H256,
    pub block_number: u64,
    #[serde(default)]
    pub timestamp: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KafkaBlockChangeNotification {
    pub change_type: u64,
    #[serde(default)]
    pub new_blocks: Vec<KafkaBlockContext>,
    #[serde(default)]
    pub drop_blocks: Vec<KafkaBlockContext>,
}

/// Per-tx data ready for assembly into a [`BlockFile`].
///
/// Produced by both EN realtime upload (from VM `Call` via `set_parent_failed` +
/// `add_trace_log`) and backfill source layers (PG / RPC). Consumed by
/// [`assemble_block_file`], which is the single source of truth for
/// block-level field layout.
///
/// `events.log_index` is filled by [`assemble_block_file`] — sources should leave it at 0.
#[derive(Clone, Debug)]
pub struct TxBlockData {
    pub debank_tx: DebankTransaction,
    pub traces: Vec<DebankTrace>,
    pub error_traces: Vec<DebankTrace>,
    pub events: Vec<DebankEvent>,
    pub error_events: Vec<DebankEvent>,
}

/// Block-level metadata required by [`assemble_block_file`].
///
/// Only the subset of fields EN realtime can populate is exposed; the rest of
/// the [`Header`] is forced to `Default::default()` to keep backfill output
/// field-equivalent to EN realtime (which doesn't have state_root etc.).
#[derive(Clone, Debug)]
pub struct BlockMeta {
    pub hash: H256,
    pub parent_hash: H256,
    pub number: u64,
    pub timestamp: u64,
    pub base_fee_per_gas: u64,
    pub gas_limit: u64,
    pub logs_bloom: Bloom,
}

/// keccak256 of RLP-encoded empty list — standard Ethereum empty uncles hash.
fn empty_uncles_hash() -> H256 {
    H256::from_slice(
        &hex::decode("1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347")
            .expect("static empty uncles hash hex literal"),
    )
}

/// Assemble a [`BlockFile`] + [`Header`] from block-level metadata and per-tx data.
///
/// Single source of truth for BlockFile layout — invoked by both EN realtime
/// upload and backfill. Responsibilities:
/// - Assigns a block-monotonic `log_index` to events (across all txs, in tx order).
///   `error_events` keep `log_index=0` (they're dropped downstream).
/// - Aggregates `storage_contracts` from traces with `self_storage_change=true`,
///   walking `traces` then `error_traces` in order, dedupe by HashSet (first occurrence wins).
///   `delegatecall` uses `from_addr`, others use `to_addr`.
/// - Builds `DebankBlock` (block_file.block) with `gas_used = sum(tx.gas_used)`.
/// - Builds `Header` with only the fields EN realtime actually fills
///   (number/hash/parent_hash/sha3_uncles/logs_bloom/gas/timestamp/base_fee_per_gas).
pub fn assemble_block_file(
    block: BlockMeta,
    tx_results: Vec<TxBlockData>,
) -> (BlockFile, Header) {
    let mut transactions = Vec::with_capacity(tx_results.len());
    let mut all_traces = Vec::new();
    let mut all_error_traces = Vec::new();
    let mut all_events = Vec::new();
    let mut all_error_events = Vec::new();
    let mut global_log_index: u32 = 0;

    for tx in tx_results {
        transactions.push(tx.debank_tx);
        all_traces.extend(tx.traces);
        all_error_traces.extend(tx.error_traces);
        all_events.extend(tx.events);
        all_error_events.extend(tx.error_events);

        // Mirrors `debank_s3_persistence.rs:386-389` verbatim: after each tx, walk
        // the newly-appended tail of `all_events` (everything past the previous
        // `global_log_index`) and assign monotonically increasing log_index.
        // error_events are intentionally skipped — they're dropped downstream.
        for event in all_events.iter_mut().skip(global_log_index as usize) {
            event.log_index = global_log_index;
            global_log_index += 1;
        }
    }

    let mut seen = HashSet::new();
    let storage_contracts: Vec<String> = all_traces
        .iter()
        .chain(all_error_traces.iter())
        .filter(|trace| trace.self_storage_change)
        .filter_map(|trace| {
            let addr = if trace.call_type == "delegatecall" {
                format!("{:?}", trace.from_addr)
            } else {
                format!("{:?}", trace.to_addr)
            };
            if seen.insert(addr.clone()) {
                Some(addr)
            } else {
                None
            }
        })
        .collect();

    let gas_used: u64 = transactions.iter().map(|tx| tx.gas_used).sum();

    let block_file = BlockFile {
        block: DebankBlock {
            id: block.hash,
            height: block.number,
            parent_id: block.parent_hash,
            base_fee_per_gas: Some(block.base_fee_per_gas),
            gas_limit: block.gas_limit,
            gas_used,
            timestamp: block.timestamp,
            process_start_timestamp: block.timestamp,
            ..Default::default()
        },
        transactions,
        events: all_events,
        traces: all_traces,
        error_events: all_error_events,
        error_traces: all_error_traces,
        storage_contracts,
    };

    let header = Header {
        number: block.number,
        hash: block.hash,
        parent_hash: block.parent_hash,
        sha3_uncles: empty_uncles_hash(),
        logs_bloom: block.logs_bloom,
        gas_limit: block.gas_limit,
        gas_used,
        timestamp: block.timestamp,
        base_fee_per_gas: Some(U256::from(block.base_fee_per_gas)),
        miner: Address::default(),
        ..Default::default()
    };

    (block_file, header)
}

#[cfg(test)]
mod assemble_tests {
    use super::*;

    fn meta() -> BlockMeta {
        BlockMeta {
            hash: H256::repeat_byte(0x01),
            parent_hash: H256::repeat_byte(0x02),
            number: 100,
            timestamp: 12345,
            base_fee_per_gas: 1_000_000_000,
            gas_limit: 30_000_000,
            logs_bloom: Bloom::default(),
        }
    }

    fn ev(id: &str) -> DebankEvent {
        DebankEvent {
            id: id.to_string(),
            ..Default::default()
        }
    }

    fn tx(gas_used: u64, events: Vec<DebankEvent>, error_events: Vec<DebankEvent>) -> TxBlockData {
        TxBlockData {
            debank_tx: DebankTransaction {
                gas_used,
                ..Default::default()
            },
            traces: vec![],
            error_traces: vec![],
            events,
            error_events,
        }
    }

    #[test]
    fn empty_input_yields_empty_block() {
        let (bf, h) = assemble_block_file(meta(), vec![]);
        assert!(bf.transactions.is_empty());
        assert!(bf.traces.is_empty());
        assert!(bf.events.is_empty());
        assert!(bf.error_traces.is_empty());
        assert!(bf.error_events.is_empty());
        assert!(bf.storage_contracts.is_empty());
        assert_eq!(bf.block.gas_used, 0);
        assert_eq!(h.gas_used, 0);
        assert_eq!(h.number, 100);
        assert_eq!(h.hash, H256::repeat_byte(0x01));
        assert_eq!(h.parent_hash, H256::repeat_byte(0x02));
    }

    #[test]
    fn log_index_is_monotonic_across_txs() {
        let txs = vec![
            tx(100, vec![ev("a"), ev("b")], vec![]),
            tx(50, vec![ev("c"), ev("d"), ev("e")], vec![]),
            tx(25, vec![ev("f")], vec![]),
        ];
        let (bf, h) = assemble_block_file(meta(), txs);
        assert_eq!(
            bf.events.iter().map(|e| e.log_index).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5]
        );
        assert_eq!(h.gas_used, 175);
    }

    #[test]
    fn error_events_do_not_advance_log_index() {
        // tx1 emits 1 event + 1 error_event; tx2 emits 1 event.
        // expected: events log_index = [0, 1], error_events log_index = [0] (default).
        let txs = vec![
            tx(0, vec![ev("a")], vec![ev("err1")]),
            tx(0, vec![ev("b")], vec![]),
        ];
        let (bf, _) = assemble_block_file(meta(), txs);
        assert_eq!(
            bf.events.iter().map(|e| e.log_index).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            bf.error_events.iter().map(|e| e.log_index).collect::<Vec<_>>(),
            vec![0]
        );
    }

    #[test]
    fn storage_contracts_dedup_first_seen_order_and_delegatecall_picks_from() {
        let addr_a = Address::repeat_byte(0xaa);
        let addr_b = Address::repeat_byte(0xbb);
        let addr_c = Address::repeat_byte(0xcc);
        let addr_d = Address::repeat_byte(0xdd);

        let trace = |from: Address, to: Address, kind: &str, has_change: bool| DebankTrace {
            from_addr: from,
            to_addr: to,
            call_type: kind.to_string(),
            self_storage_change: has_change,
            ..Default::default()
        };

        // tx1.traces: call→to=addr_a (recorded), call→to=addr_b (recorded)
        // tx1.error_traces: delegatecall→from=addr_c (recorded under from)
        // tx2.traces: call→to=addr_a (dup, skipped), call→to=addr_d but storage_change=false (filtered)
        let tx1 = TxBlockData {
            debank_tx: DebankTransaction::default(),
            traces: vec![
                trace(addr_d, addr_a, "call", true),
                trace(addr_d, addr_b, "call", true),
            ],
            error_traces: vec![trace(addr_c, addr_a, "delegatecall", true)],
            events: vec![],
            error_events: vec![],
        };
        let tx2 = TxBlockData {
            debank_tx: DebankTransaction::default(),
            traces: vec![
                trace(addr_d, addr_a, "call", true),
                trace(addr_d, addr_d, "call", false),
            ],
            error_traces: vec![],
            events: vec![],
            error_events: vec![],
        };
        let (bf, _) = assemble_block_file(meta(), vec![tx1, tx2]);
        // Order: traces first then error_traces (within each tx, in declared order).
        // tx1 traces produce addr_a, addr_b; tx1 error_traces produce addr_c (delegatecall→from).
        // tx2 traces try addr_a (dup), addr_d (filtered out by self_storage_change=false).
        assert_eq!(
            bf.storage_contracts,
            vec![
                format!("{:?}", addr_a),
                format!("{:?}", addr_b),
                format!("{:?}", addr_c),
            ]
        );
    }

    #[test]
    fn header_zeroes_unfilled_fields_but_sets_empty_uncles_hash() {
        let (_, h) = assemble_block_file(meta(), vec![]);
        assert_eq!(h.state_root, H256::default());
        assert_eq!(h.transactions_root, H256::default());
        assert_eq!(h.receipts_root, H256::default());
        assert_eq!(h.miner, Address::default());
        assert_eq!(h.difficulty, U256::default());
        // sha3_uncles is intentionally the keccak256 of RLP-encoded empty list.
        assert_ne!(h.sha3_uncles, H256::default());
        assert_eq!(h.base_fee_per_gas, Some(U256::from(1_000_000_000u64)));
    }

    #[test]
    fn block_gas_used_sums_tx_gas_used() {
        let txs = vec![
            tx(100, vec![], vec![]),
            tx(200, vec![], vec![]),
            tx(300, vec![], vec![]),
        ];
        let (bf, h) = assemble_block_file(meta(), txs);
        assert_eq!(bf.block.gas_used, 600);
        assert_eq!(h.gas_used, 600);
    }
}
