//! Genesis is a deterministic projection of trusted initial allocations, not a
//! replay of historical transactions. Every field below must be supplied;
//! unavailable balances / storage coverage must not silently become zero/false.

#[path = "../rebuild_validation.rs"]
mod rebuild_validation;
#[path = "../rebuild_version.rs"]
mod rebuild_version;

use std::{collections::BTreeMap, fs, io::Write, path::PathBuf};

use anyhow::ensure;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use structopt::StructOpt;
use zksync_multivm::tracers::debank::{genesis_tx_id, root_trace_id};
use zksync_types::{
    debank::{assemble_block_file, BlockMeta, DebankTrace, DebankTransaction, TxBlockData},
    web3::Bytes,
    Address, U256,
};

#[derive(StructOpt)]
struct Args {
    /// Frozen, independently verified genesis allocation (see README).
    #[structopt(long, parse(from_os_str))]
    input: PathBuf,
    /// A new directory; an existing result is never overwritten.
    #[structopt(long, parse(from_os_str))]
    output_dir: PathBuf,
}

#[derive(Deserialize)]
struct Input {
    chain_id: u64,
    source: String,
    block: BlockMeta,
    alloc: BTreeMap<Address, Account>,
}

#[derive(Deserialize)]
struct Account {
    balance: U256,
    code: Bytes,
    has_storage: bool,
}

fn projection(kind: u8, address: Address, value: U256, code: Bytes, index: u32) -> TxBlockData {
    let id = genesis_tx_id(kind, address);
    TxBlockData {
        debank_tx: DebankTransaction {
            id: format!("{id:#x}"),
            to: Some(address),
            value,
            input: code.clone(),
            status: true,
            transaction_index: index,
            ..Default::default()
        },
        traces: vec![DebankTrace {
            id: root_trace_id(id),
            tx_id: format!("{id:#x}"),
            to_addr: address,
            value,
            input: code.clone(),
            output: code,
            call_create_type: if kind == 1 { "call" } else { "create" }.into(),
            call_type: if kind == 1 { "call" } else { "" }.into(),
            ..Default::default()
        }],
        events: vec![],
        error_events: vec![],
        error_traces: vec![],
    }
}

fn main() -> anyhow::Result<()> {
    let args = Args::from_args();
    let bytes = fs::read(args.input)?;
    let input: Input = serde_json::from_slice(&bytes)?;
    ensure!(
        input.block.number == 0 && input.block.parent_hash.is_zero(),
        "expected a genesis header"
    );
    ensure!(
        !input.source.trim().is_empty() && !input.alloc.is_empty(),
        "genesis source / allocation is missing"
    );
    let mut txs = vec![];
    let mut storage_contracts = vec![];
    let native_address = Address::repeat_byte(0xee);
    let native_code = input
        .alloc
        .get(&native_address)
        .map(|account| account.code.clone())
        .unwrap_or_default();
    for (address, account) in input.alloc {
        if account.has_storage {
            storage_contracts.push(format!("{address:#x}"));
        }
        if !account.balance.is_zero() {
            txs.push(projection(
                1,
                address,
                account.balance,
                Bytes::default(),
                txs.len() as u32,
            ));
        }
        // Include every deployed code allocation, regardless of duplicate code
        // frequency. User wrappers are part of the genesis state as well.
        if !account.code.0.is_empty() {
            txs.push(projection(
                2,
                address,
                U256::zero(),
                account.code,
                txs.len() as u32,
            ));
        }
    }
    txs.push(projection(
        3,
        native_address,
        U256::zero(),
        native_code,
        txs.len() as u32,
    ));
    let (mut block, header) = assemble_block_file(input.block, txs);
    block.storage_contracts = storage_contracts;
    rebuild_validation::validate(&block)?;
    let validation = block.validation();
    fs::create_dir(&args.output_dir)?;
    let mut hashes = BTreeMap::new();
    for (name, value) in [
        ("blockfile.json", serde_json::to_value(&block)?),
        ("header.json", serde_json::to_value(header)?),
        ("validation.json", serde_json::to_value(validation)?),
    ] {
        let value = serde_json::to_vec(&value)?;
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(args.output_dir.join(name))?;
        file.write_all(&value)?;
        file.sync_all()?;
        hashes.insert(name, hex::encode(Sha256::digest(&value)));
    }
    let manifest = serde_json::to_vec(&serde_json::json!({
        "format": "lens-genesis-projection-v1", "chain_id": input.chain_id,
        "generator_sha256": rebuild_version::executable_sha256()?,
        "source": input.source, "input_sha256": hex::encode(Sha256::digest(&bytes)),
        "files": hashes, "projection": "balance/code/native-token; not real transactions",
        "state_diff_exported": false, "state_root_recomputed": false, "canonical_publish_ready": false,
    }))?;
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(args.output_dir.join("manifest.json"))?;
    file.write_all(&manifest)?;
    file.sync_all()?;
    fs::File::open(&args.output_dir)?.sync_all()?;
    Ok(())
}
