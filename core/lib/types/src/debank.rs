use crate::{
    storage::{log::StorageLogWithPreviousValue, StorageKey, StorageValue},
    Address, H256, H64, U256,
};
use alloy_primitives::{keccak256, B256, Bytes as AlloyBytes, U256 as AlloyU256};
use alloy_rlp::Encodable;
use alloy_rlp_derive::{RlpDecodable, RlpEncodable};
use hex;
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};
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


#[derive(Debug, Clone, PartialEq, RlpDecodable, RlpEncodable, Default)]
pub struct BlockStorageDiff {
    /// Block root hash.
    pub hash: B256,
    /// Parent block root hash.
    pub parent_hash: B256,
    /// New accounts
    pub new_accounts: Vec<NewAccount>,
    /// Deleted accounts
    pub deleted_accounts: Vec<B256>,
    /// Account storage diff
    pub storage_diffs: Vec<AccountStorageDiff>,
    /// New codes
    pub new_codes: Vec<NewCode>,
}

impl BlockStorageDiff {
    pub fn encode_rlp(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode(&mut buf);
        buf
    }
}

/// Builds a [`BlockStorageDiff`] from deduplicated write logs and new factory dependencies.
/// Slot keys and account addresses are hashed with keccak256 to align with standard EVM
/// state storage indexing used across DeBank / leafage-evm.
pub fn build_state_diff<'a>(
    block_hash: H256,
    prev_block_hash: H256,
    writes: impl IntoIterator<Item = (StorageKey, StorageValue)>,
    new_factory_deps: impl IntoIterator<Item = (&'a H256, &'a [u8])>,
) -> BlockStorageDiff {
    let mut modified_slots: HashMap<Address, HashMap<H256, H256>> = HashMap::new();

    for (key, value) in writes {
        let addr = *key.address();
        let slot = *key.key();
        modified_slots.entry(addr).or_default().insert(slot, value);
    }

    let mut storage_diffs = Vec::with_capacity(modified_slots.len());
    for (addr, slots) in modified_slots {
        let mut diffs = Vec::with_capacity(slots.len());
        for (key, val) in slots {
            diffs.push(IndexValuePair {
                index: keccak256(key.as_bytes()),
                value: AlloyU256::from_be_bytes(val.to_fixed_bytes()),
            });
        }
        diffs.sort_by_key(|d| d.index);
        storage_diffs.push(AccountStorageDiff {
            address: keccak256(addr.as_bytes()),
            diffs,
        });
    }
    storage_diffs.sort_by_key(|s| s.address);

    let mut new_codes = Vec::new();
    for (hash, code) in new_factory_deps {
        new_codes.push(NewCode {
            code_hash: B256::from(hash.0),
            code: AlloyBytes::copy_from_slice(code),
        });
    }
    new_codes.sort_by_key(|c| c.code_hash);

    BlockStorageDiff {
        hash: B256::from(block_hash.0),
        parent_hash: B256::from(prev_block_hash.0),
        new_accounts: Vec::new(),
        deleted_accounts: Vec::new(),
        storage_diffs,
        new_codes,
    }
}

/// Convenience helper to build [`BlockStorageDiff`] from a block's `storage_logs` and `new_factory_deps`.
pub fn build_state_diff_from_logs<'a>(
    block_hash: H256,
    prev_block_hash: H256,
    storage_logs: impl IntoIterator<Item = &'a StorageLogWithPreviousValue>,
    new_factory_deps: &'a HashMap<H256, Vec<u8>>,
) -> BlockStorageDiff {
    let writes = storage_logs
        .into_iter()
        .filter(|log| log.log.is_write())
        .map(|log| (log.log.key, log.log.value));
    let factory_deps = new_factory_deps.iter().map(|(h, c)| (h, c.as_slice()));
    build_state_diff(block_hash, prev_block_hash, writes, factory_deps)
}

#[derive(Debug, Clone, PartialEq, RlpDecodable, RlpEncodable)]
pub struct NewCode {
    pub code_hash: B256,
    pub code: AlloyBytes,
}

#[derive(Debug, Clone, PartialEq, RlpDecodable, RlpEncodable)]
pub struct NewAccount {
    /// Account address
    pub address: B256,
    /// Account balance
    pub balance: AlloyU256,
    /// Account nonce
    pub nonce: u64,
    /// code hash
    pub code_hash: B256,
}

#[derive(Debug, Clone, PartialEq, RlpDecodable, RlpEncodable)]
pub struct AccountStorageDiff {
    pub address: B256,
    pub diffs: Vec<IndexValuePair>,
}

#[derive(Debug, Clone, PartialEq, RlpDecodable, RlpEncodable)]
pub struct IndexValuePair {
    pub index: B256,
    pub value: AlloyU256,
}

#[derive(Debug, Clone, PartialEq, RlpDecodable, RlpEncodable)]
pub struct SlimAccount {
    pub balance: AlloyU256,
    pub nonce: u64,
    pub code_hash: B256,
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rlp::Decodable;
    use crate::storage::log::StorageLog;

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

    #[test]
    fn test_block_storage_diff_rlp_roundtrip() {
        let diff = BlockStorageDiff {
            hash: B256::from([1u8; 32]),
            parent_hash: B256::from([2u8; 32]),
            new_accounts: vec![NewAccount {
                address: B256::from([3u8; 32]),
                balance: AlloyU256::from(1000u64),
                nonce: 42,
                code_hash: B256::from([4u8; 32]),
            }],
            deleted_accounts: vec![B256::from([5u8; 32])],
            storage_diffs: vec![AccountStorageDiff {
                address: B256::from([6u8; 32]),
                diffs: vec![IndexValuePair {
                    index: B256::from([7u8; 32]),
                    value: AlloyU256::from(9999u64),
                }],
            }],
            new_codes: vec![NewCode {
                code_hash: B256::from([8u8; 32]),
                code: AlloyBytes::from(vec![0x60, 0x00, 0x60, 0x00]),
            }],
        };

        let encoded = diff.encode_rlp();
        assert!(!encoded.is_empty());

        let mut slice = encoded.as_slice();
        let decoded = BlockStorageDiff::decode(&mut slice).expect("Failed to decode RLP");
        assert_eq!(diff, decoded);
        assert!(slice.is_empty());
    }

    #[test]
    fn test_build_state_diff_from_logs() {
        use crate::AccountTreeId;

        let contract = Address::from_low_u64_be(0x800a);
        let key1 = H256::from_low_u64_be(1);
        let val1 = H256::from_low_u64_be(100);
        let val2 = H256::from_low_u64_be(200);

        let storage_key1 = StorageKey::new(AccountTreeId::new(contract), key1);

        let logs = vec![
            StorageLogWithPreviousValue {
                log: StorageLog::new_write_log(storage_key1, val1),
                previous_value: H256::zero(),
            },
            // Overwrite in the same block
            StorageLogWithPreviousValue {
                log: StorageLog::new_write_log(storage_key1, val2),
                previous_value: val1,
            },
            // Read log should be ignored
            StorageLogWithPreviousValue {
                log: StorageLog::new_read_log(storage_key1, val2),
                previous_value: val2,
            },
        ];

        let mut factory_deps = HashMap::new();
        let code_hash = H256::from_low_u64_be(0xdead);
        let bytecode = vec![1, 2, 3, 4];
        factory_deps.insert(code_hash, bytecode.clone());

        let block_hash = H256::from_low_u64_be(10);
        let prev_hash = H256::from_low_u64_be(9);

        let diff = build_state_diff_from_logs(block_hash, prev_hash, &logs, &factory_deps);

        assert_eq!(diff.hash, B256::from(block_hash.0));
        assert_eq!(diff.parent_hash, B256::from(prev_hash.0));
        assert_eq!(diff.storage_diffs.len(), 1);
        assert_eq!(
            diff.storage_diffs[0].address,
            keccak256(contract.as_bytes())
        );
        assert_eq!(diff.storage_diffs[0].diffs.len(), 1);
        assert_eq!(diff.storage_diffs[0].diffs[0].index, keccak256(key1.as_bytes()));
        assert_eq!(
            diff.storage_diffs[0].diffs[0].value,
            AlloyU256::from(200u64)
        );

        assert_eq!(diff.new_codes.len(), 1);
        assert_eq!(diff.new_codes[0].code_hash, B256::from(code_hash.0));
        assert_eq!(diff.new_codes[0].code.as_ref(), bytecode.as_slice());

        // Test RLP encode roundtrip
        let rlp_bytes = diff.encode_rlp();
        assert!(!rlp_bytes.is_empty());
        let mut slice = rlp_bytes.as_slice();
        let decoded = BlockStorageDiff::decode(&mut slice).unwrap();
        assert_eq!(diff, decoded);
    }
}
