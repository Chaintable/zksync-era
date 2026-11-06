use crate::{Address, H256, H64, U256};
use hex;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{collections::HashSet, str::FromStr};
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
    #[serde(default)]
    pub error_traces: Vec<DebankTrace>,
    #[serde(default)]
    pub error_events: Vec<DebankEvent>,
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
    pub txs_count: u64,
    pub events_count: u64,
    pub traces_count: u64,
    pub error_events_count: u64,
    pub error_traces_count: u64,
    pub storage_contracts_count: u64,
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
        ids.push(format!("{:#x}", self.block.id));
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
            txs_count: self.transactions.len() as u64,
            events_count: self.events.len() as u64,
            traces_count: self.traces.len() as u64,
            error_events_count: self.error_events.len() as u64,
            error_traces_count: self.error_traces.len() as u64,
            storage_contracts_count: self.storage_contracts.len() as u64,
        }
    }
}

pub fn collect_storage_contracts(
    traces: &[DebankTrace],
    error_traces: &[DebankTrace],
) -> Vec<String> {
    let mut seen = HashSet::new();
    traces
        .iter()
        .chain(error_traces)
        .filter(|trace| trace.self_storage_change)
        .filter_map(|trace| {
            let address = if trace.call_type == "delegatecall" {
                format!("{:?}", trace.from_addr)
            } else {
                format!("{:?}", trace.to_addr)
            };
            seen.insert(address.clone()).then_some(address)
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_matches_go_protocol_and_counts_every_array() {
        let block = BlockFile {
            block: DebankBlock {
                id: H256::repeat_byte(1),
                ..Default::default()
            },
            transactions: vec![DebankTransaction {
                id: "tx".into(),
                ..Default::default()
            }],
            events: vec![DebankEvent {
                id: "event".into(),
                ..Default::default()
            }],
            traces: vec![DebankTrace {
                id: "trace".into(),
                ..Default::default()
            }],
            error_events: vec![DebankEvent {
                id: "error_event".into(),
                ..Default::default()
            }],
            error_traces: vec![DebankTrace {
                id: "error_trace".into(),
                ..Default::default()
            }],
            storage_contracts: vec![format!("{:#x}", Address::repeat_byte(2))],
        };
        let validation = serde_json::to_value(block.validation()).unwrap();
        // Independently computed from complete block ID + tx/event/normal-trace
        // IDs using SHA1 integer addition modulo 1_000_000 (Go's protocol).
        assert_eq!(validation["validation_hash"], 72815);
        for field in [
            "txs_count",
            "events_count",
            "traces_count",
            "error_events_count",
            "error_traces_count",
            "storage_contracts_count",
        ] {
            assert_eq!(validation[field], 1, "{field}");
        }
    }

    #[test]
    fn storage_contracts_include_error_traces() {
        let normal_address = Address::from_low_u64_be(1);
        let error_address = Address::from_low_u64_be(2);
        let normal_trace = DebankTrace {
            to_addr: normal_address,
            self_storage_change: true,
            ..Default::default()
        };
        let error_trace = DebankTrace {
            from_addr: error_address,
            call_type: "delegatecall".to_owned(),
            self_storage_change: true,
            ..Default::default()
        };

        assert_eq!(
            collect_storage_contracts(&[normal_trace], &[error_trace]),
            vec![
                format!("{:?}", normal_address),
                format!("{:?}", error_address)
            ]
        );
    }

    #[test]
    fn simulate_result_serializes_error_outputs() {
        let result = DebankSingleSimulateResult {
            error_traces: vec![DebankTrace {
                error: "parent call failed".to_owned(),
                ..Default::default()
            }],
            error_events: vec![DebankEvent::default()],
            ..Default::default()
        };

        let json = serde_json::to_value(result).unwrap();
        assert_eq!(json["error_traces"].as_array().unwrap().len(), 1);
        assert_eq!(json["error_events"].as_array().unwrap().len(), 1);
    }
}

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
#[derive(Clone, Debug, Serialize, Deserialize)]
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
///   `error_events` keep `log_index=0`, outside the receipt log index space.
/// - Aggregates `storage_contracts` from traces with `self_storage_change=true`,
///   walking `traces` then `error_traces` in order, dedupe by HashSet (first occurrence wins).
///   `delegatecall` uses `from_addr`, others use `to_addr`.
/// - Builds `DebankBlock` (block_file.block) with `gas_used = sum(tx.gas_used)`.
/// - Builds `Header` with only the fields EN realtime actually fills
///   (number/hash/parent_hash/sha3_uncles/logs_bloom/gas/timestamp/base_fee_per_gas).
pub fn assemble_block_file(block: BlockMeta, tx_results: Vec<TxBlockData>) -> (BlockFile, Header) {
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

        // Receipt log indices count only committed events, across transactions.
        for event in all_events.iter_mut().skip(global_log_index as usize) {
            event.log_index = global_log_index;
            global_log_index += 1;
        }
    }

    let storage_contracts = collect_storage_contracts(&all_traces, &all_error_traces);

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
