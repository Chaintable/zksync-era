//! Standalone binary to backfill DeBank block data to S3 for historical blocks (0 to 13,083,679).
//!
//! This binary processes blocks that predate the sandbox-based tracing support:
//! - Block 0 (genesis): Synthetic transactions/traces for all system contracts
//! - Blocks 1–2,219,806: Events only (no traces)
//! - Blocks 2,219,807–13,083,679: Events + traces from mainnet RPC
//!
//! All data is fetched from the mainnet RPC — no database required.
//! Events are fetched via `eth_getLogs`.
//! Traces are fetched via `debug_traceTransaction` with callTracer.
//!
//! Outputs are written to two S3 buckets:
//! - Header → `chaintable-nodex-pipeline--apne1-az4--x-s3` at `324/<blockHash>/block`
//! - BlockFile → `chaintable-pipeline--apne1-az4--x-s3` at `324/<blockHash>`
//! - Validation → `chaintable-pipeline--apne1-az4--x-s3` at `324/<blockNum>/<blockHash>`

use std::collections::HashSet;
use std::future::Future;
use std::str::FromStr;

use anyhow::Context as _;
use aws_sdk_s3::Client as S3Client;
use flate2::{write::GzEncoder, Compression};
use structopt::StructOpt;
use zksync_multivm::tracers::debank;
use zksync_system_constants::{
    ACCOUNT_CODE_STORAGE_ADDRESS, BOOTLOADER_ADDRESS, BOOTLOADER_UTILITIES_ADDRESS,
    CODE_ORACLE_ADDRESS, COMPLEX_UPGRADER_ADDRESS, COMPRESSOR_ADDRESS, CONTRACT_DEPLOYER_ADDRESS,
    CREATE2_FACTORY_ADDRESS, ECRECOVER_PRECOMPILE_ADDRESS, EC_ADD_PRECOMPILE_ADDRESS,
    EC_MUL_PRECOMPILE_ADDRESS, EC_PAIRING_PRECOMPILE_ADDRESS, EVENT_WRITER_ADDRESS,
    EVM_GAS_MANAGER_ADDRESS, EVM_HASHES_STORAGE_ADDRESS, EVM_PREDEPLOYS_MANAGER_ADDRESS,
    IDENTITY_ADDRESS, IMMUTABLE_SIMULATOR_STORAGE_ADDRESS, KECCAK256_PRECOMPILE_ADDRESS,
    KNOWN_CODES_STORAGE_ADDRESS, L1_MESSENGER_ADDRESS, L2_ASSET_ROUTER_ADDRESS,
    L2_BASE_TOKEN_ADDRESS, L2_BRIDGEHUB_ADDRESS, L2_CHAIN_ASSET_HANDLER_ADDRESS,
    L2_GENESIS_UPGRADE_ADDRESS, L2_INTEROP_ROOT_STORAGE_ADDRESS, L2_MESSAGE_ROOT_ADDRESS,
    L2_MESSAGE_VERIFICATION_ADDRESS, L2_NATIVE_TOKEN_VAULT_ADDRESS, L2_WRAPPED_BASE_TOKEN_IMPL,
    MODEXP_PRECOMPILE_ADDRESS, MSG_VALUE_SIMULATOR_ADDRESS, NONCE_HOLDER_ADDRESS,
    PUBDATA_CHUNK_PUBLISHER_ADDRESS, SECP256R1_VERIFY_PRECOMPILE_ADDRESS,
    SHA256_PRECOMPILE_ADDRESS, SLOAD_CONTRACT_ADDRESS, SYSTEM_CONTEXT_ADDRESS,
};
use zksync_types::{
    api::{
        BlockIdVariant, BlockNumber, CallTracerResult, DebugCall, DebugCallType, SupportedTracers,
        TracerConfig, TransactionVariant,
    },
    debank::{
        BlockFile, DebankBlock, DebankEvent, DebankOutPut, DebankTrace, DebankTransaction, Header,
    },
    Address, H256, U256,
};
use zksync_web3_decl::{
    client::{Client, L2},
    namespaces::{DebugNamespaceClient, EthNamespaceClient},
    types::FilterBuilder,
};

const CHAIN_ID: u64 = 324;
const MAX_BLOCK: u32 = 13_083_680;
const TRACES_START_BLOCK: u32 = 2_219_807;
const MAX_RETRIES: u32 = 5;

/// S3 bucket for headers.
const HEADER_BUCKET: &str = "chaintable-nodex-pipeline--apne1-az4--x-s3";
/// S3 bucket for block files and validation.
const BLOCK_FILE_BUCKET: &str = "chaintable-pipeline--apne1-az4--x-s3";

/// Retry an async operation with exponential backoff (2s, 4s, 8s, 16s, 32s).
async fn retry<F, Fut, T, E>(label: &str, f: F) -> Result<T, E>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0u32;
    loop {
        match f().await {
            Ok(val) => return Ok(val),
            Err(e) => {
                attempt += 1;
                if attempt > MAX_RETRIES {
                    tracing::error!("{}: failed after {} retries: {}", label, MAX_RETRIES, e);
                    return Err(e);
                }
                let delay = 1u64 << (attempt + 1); // 4, 8, 16, 32, 64 seconds
                tracing::warn!(
                    "{}: attempt {}/{} failed: {}. Retrying in {}s...",
                    label,
                    attempt,
                    MAX_RETRIES,
                    e,
                    delay
                );
                tokio::time::sleep(std::time::Duration::from_secs(delay)).await;
            }
        }
    }
}

#[derive(StructOpt)]
#[structopt(name = "ZKsync S3 backfill", author = "DeBank")]
struct Opt {
    /// Mainnet RPC URL for fetching block data, events, and traces.
    #[structopt(
        long,
        env = "DEBANK_MAINNET_RPC_URL",
        default_value = "https://mainnet.era.zksync.io"
    )]
    mainnet_rpc_url: String,

    /// Start block number (inclusive).
    #[structopt(long, default_value = "0")]
    start_block: u32,

    /// End block number (exclusive). Defaults to 13,083,680.
    #[structopt(long)]
    end_block: Option<u32>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let opt = Opt::from_args();

    // Build the mainnet RPC client.
    let mainnet_url = zksync_types::url::SensitiveUrl::from_str(&opt.mainnet_rpc_url)?;
    let mainnet_client = Client::<L2>::http(mainnet_url)?
        .for_network(L2::from(zksync_types::L2ChainId::from(324)))
        .build();

    // Build the S3 client using default AWS credentials (env vars, instance profile, etc.)
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .load()
        .await;
    let s3_client = S3Client::new(&aws_config);

    let end_block = opt.end_block.unwrap_or(MAX_BLOCK);
    tracing::info!(
        "Starting S3 backfill from block {} to {} (exclusive)",
        opt.start_block,
        end_block
    );

    for block_num in opt.start_block..end_block {
        tracing::info!("Processing block {}", block_num);

        let output = if block_num == 0 {
            process_genesis_block(&mainnet_client).await?
        } else {
            process_historical_block(&mainnet_client, block_num).await?
        };

        tracing::info!(
            "Block {} built: {} txs, {} events, {} traces. Uploading to S3...",
            block_num,
            output.block_file.transactions.len(),
            output.block_file.events.len(),
            output.block_file.traces.len(),
        );

        // Upload to S3.
        upload_to_s3(&s3_client, &output).await?;

        tracing::info!("Block {} uploaded to S3", block_num);
    }

    tracing::info!("S3 backfill complete.");
    Ok(())
}

/// Upload DebankOutPut to S3 with the DeBank-specific bucket/path layout.
async fn upload_to_s3(s3: &S3Client, output: &DebankOutPut) -> anyhow::Result<()> {
    let block_hash = format!("{:#x}", output.header.hash);
    let block_num = output.header.number;

    // 1. Header → chaintable-nodex-pipeline bucket at 324/<blockHash>/block
    let header_key = format!("{}/{}/block", CHAIN_ID, block_hash);
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    serde_json::to_writer(&mut gz, &output.header)?;
    let header_compressed = gz.finish()?;
    tracing::info!(
        "Uploading header for block {} ({} bytes gzipped)...",
        block_num,
        header_compressed.len()
    );
    s3.put_object()
        .bucket(HEADER_BUCKET)
        .key(&header_key)
        .body(header_compressed.into())
        .send()
        .await
        .context(format!("Failed to upload header for block {}", block_num))?;

    // 2. BlockFile → chaintable-pipeline bucket at 324/<blockHash>
    let block_file_key = format!("{}/{}", CHAIN_ID, block_hash);
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    serde_json::to_writer(&mut gz, &output.block_file)?;
    let block_file_compressed = gz.finish()?;
    tracing::info!(
        "Uploading block_file for block {} ({} bytes gzipped)...",
        block_num,
        block_file_compressed.len()
    );
    s3.put_object()
        .bucket(BLOCK_FILE_BUCKET)
        .key(&block_file_key)
        .body(block_file_compressed.into())
        .send()
        .await
        .context(format!(
            "Failed to upload block file for block {}",
            block_num
        ))?;

    // 3. Validation → chaintable-pipeline bucket at 324/<blockNum>/<blockHash>
    let validation = output.block_file.validation();
    let validation_key = format!("{}/{}/{}", CHAIN_ID, block_num, block_hash);
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    serde_json::to_writer(&mut gz, &validation)?;
    let validation_compressed = gz.finish()?;
    tracing::info!(
        "Uploading validation for block {} ({} bytes gzipped)...",
        block_num,
        validation_compressed.len()
    );
    s3.put_object()
        .bucket(BLOCK_FILE_BUCKET)
        .key(&validation_key)
        .body(validation_compressed.into())
        .send()
        .await
        .context(format!(
            "Failed to upload validation for block {}",
            block_num
        ))?;

    Ok(())
}

/// Build DebankOutPut for genesis block (block 0) with synthetic system contract deployments.
async fn process_genesis_block(client: &Client<L2>) -> anyhow::Result<DebankOutPut> {
    tracing::info!("Fetching genesis block...");
    let l2_block = retry("get_block_by_number(0)", || {
        client.get_block_by_number(BlockNumber::Number(0.into()), false)
    })
    .await?
    .context("genesis block not found")?;
    tracing::info!("Genesis block fetched: hash={:#x}", l2_block.hash);

    let system_contract_addresses: Vec<Address> = vec![
        ACCOUNT_CODE_STORAGE_ADDRESS,
        NONCE_HOLDER_ADDRESS,
        KNOWN_CODES_STORAGE_ADDRESS,
        IMMUTABLE_SIMULATOR_STORAGE_ADDRESS,
        CONTRACT_DEPLOYER_ADDRESS,
        L1_MESSENGER_ADDRESS,
        MSG_VALUE_SIMULATOR_ADDRESS,
        L2_BASE_TOKEN_ADDRESS,
        KECCAK256_PRECOMPILE_ADDRESS,
        SHA256_PRECOMPILE_ADDRESS,
        ECRECOVER_PRECOMPILE_ADDRESS,
        MODEXP_PRECOMPILE_ADDRESS,
        EC_ADD_PRECOMPILE_ADDRESS,
        EC_MUL_PRECOMPILE_ADDRESS,
        EC_PAIRING_PRECOMPILE_ADDRESS,
        SECP256R1_VERIFY_PRECOMPILE_ADDRESS,
        CODE_ORACLE_ADDRESS,
        IDENTITY_ADDRESS,
        SYSTEM_CONTEXT_ADDRESS,
        EVENT_WRITER_ADDRESS,
        BOOTLOADER_UTILITIES_ADDRESS,
        COMPRESSOR_ADDRESS,
        COMPLEX_UPGRADER_ADDRESS,
        EVM_GAS_MANAGER_ADDRESS,
        EVM_PREDEPLOYS_MANAGER_ADDRESS,
        EVM_HASHES_STORAGE_ADDRESS,
        BOOTLOADER_ADDRESS,
        PUBDATA_CHUNK_PUBLISHER_ADDRESS,
        CREATE2_FACTORY_ADDRESS,
        L2_GENESIS_UPGRADE_ADDRESS,
        L2_BRIDGEHUB_ADDRESS,
        L2_MESSAGE_ROOT_ADDRESS,
        L2_ASSET_ROUTER_ADDRESS,
        L2_NATIVE_TOKEN_VAULT_ADDRESS,
        SLOAD_CONTRACT_ADDRESS,
        L2_WRAPPED_BASE_TOKEN_IMPL,
        L2_INTEROP_ROOT_STORAGE_ADDRESS,
        L2_MESSAGE_VERIFICATION_ADDRESS,
        L2_CHAIN_ASSET_HANDLER_ADDRESS,
    ];

    let mut transactions = Vec::new();
    let mut traces = Vec::new();

    for (idx, addr) in system_contract_addresses.iter().enumerate() {
        let addr_str = format!("{:#x}", addr);
        let genesis_id = format!("0xgenesis020000000000000{}", addr_str);

        tracing::info!(
            "Fetching code for system contract {}/{}: {}",
            idx + 1,
            system_contract_addresses.len(),
            addr_str
        );
        let addr_copy = *addr;
        let input = retry(&format!("get_code({})", addr_str), || {
            client.get_code(
                addr_copy,
                Some(BlockIdVariant::BlockNumber(BlockNumber::Number(0.into()))),
            )
        })
        .await?;

        transactions.push(DebankTransaction {
            id: genesis_id.clone(),
            from: Address::zero(),
            to: Some(*addr),
            gas_limit: 0,
            gas_price: 0,
            gas_used: 0,
            status: true,
            gas_fee_cap: 0,
            gas_tip_cap: 0,
            input: input.clone(),
            nonce: 0,
            transaction_index: idx as u32,
            value: U256::zero(),
        });

        let trace_id = debank::to_hash(&[&idx.to_string()]);
        traces.push(DebankTrace {
            id: trace_id,
            from_addr: Address::zero(),
            gas_limit: 0,
            input: input.clone(),
            to_addr: *addr,
            value: U256::zero(),
            gas_used: 0,
            output: input,
            call_create_type: "create".to_string(),
            call_type: String::new(),
            tx_id: genesis_id,
            parent_trace_id: String::new(),
            pos_in_parent_trace: 0,
            self_storage_change: false,
            storage_change: false,
            sub_traces: 0,
            trace_address: vec![],
            error: String::new(),
        });
    }

    // Native coin address.
    {
        let native_coin_address: Address = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            .parse()
            .unwrap();
        let idx = system_contract_addresses.len();
        let addr_str = format!("{:#x}", native_coin_address);
        let genesis_id = format!("0xgenesis030000000000000{}", addr_str);

        tracing::info!("Fetching code for native coin address: {}", addr_str);
        let input = retry(&format!("get_code({})", addr_str), || {
            client.get_code(
                native_coin_address,
                Some(BlockIdVariant::BlockNumber(BlockNumber::Number(0.into()))),
            )
        })
        .await?;

        transactions.push(DebankTransaction {
            id: genesis_id.clone(),
            from: Address::zero(),
            to: Some(native_coin_address),
            gas_limit: 0,
            gas_price: 0,
            gas_used: 0,
            status: true,
            gas_fee_cap: 0,
            gas_tip_cap: 0,
            input: input.clone(),
            nonce: 0,
            transaction_index: idx as u32,
            value: U256::zero(),
        });

        let trace_id = debank::to_hash(&[&idx.to_string()]);
        traces.push(DebankTrace {
            id: trace_id,
            from_addr: Address::zero(),
            gas_limit: 0,
            input: input.clone(),
            to_addr: native_coin_address,
            value: U256::zero(),
            gas_used: 0,
            output: input,
            call_create_type: "create".to_string(),
            call_type: String::new(),
            tx_id: genesis_id,
            parent_trace_id: String::new(),
            pos_in_parent_trace: 0,
            self_storage_change: false,
            storage_change: false,
            sub_traces: 0,
            trace_address: vec![],
            error: String::new(),
        });
    }

    let block_file = BlockFile {
        block: DebankBlock {
            id: l2_block.hash,
            height: l2_block.number.as_u64(),
            parent_id: l2_block.parent_hash,
            base_fee_per_gas: Some(l2_block.base_fee_per_gas.as_u64()),
            gas_limit: l2_block.gas_limit.as_u64(),
            gas_used: l2_block.gas_used.as_u64(),
            timestamp: l2_block.timestamp.as_u64(),
            process_start_timestamp: l2_block.timestamp.as_u64(),
            ..Default::default()
        },
        transactions,
        events: vec![],
        traces,
        error_traces: vec![],
        error_events: vec![],
        storage_contracts: vec![],
    };

    let header = build_header(&l2_block);

    Ok(DebankOutPut {
        header,
        validation_hash: block_file.validation().validation_hash,
        block_file,
        state_diff: vec![].into(),
    })
}

/// Build DebankOutPut for historical blocks (1 to 13,083,679).
/// All data is fetched from the mainnet RPC.
async fn process_historical_block(
    client: &Client<L2>,
    block_num: u32,
) -> anyhow::Result<DebankOutPut> {
    // Fetch block with full transactions.
    tracing::info!("Fetching block {} with full transactions...", block_num);
    let l2_block = retry(&format!("get_block_by_number({})", block_num), || {
        client.get_block_by_number(BlockNumber::Number(block_num.into()), true)
    })
    .await?
    .context(format!("block {} not found", block_num))?;

    // Extract full transactions from block.
    let full_transactions: Vec<_> = l2_block
        .transactions
        .iter()
        .filter_map(|tv| match tv {
            TransactionVariant::Full(tx) => Some(tx),
            TransactionVariant::Hash(_) => None,
        })
        .collect();

    tracing::info!(
        "Block {} has {} transactions",
        block_num,
        full_transactions.len()
    );

    // Fetch receipts for each transaction.
    let mut receipt_map: std::collections::HashMap<H256, (Option<u64>, bool)> =
        std::collections::HashMap::new();
    for (i, tx) in full_transactions.iter().enumerate() {
        tracing::debug!(
            "Fetching receipt {}/{} for tx {:#x}",
            i + 1,
            full_transactions.len(),
            tx.hash
        );
        let tx_hash = tx.hash;
        let receipt = retry(&format!("get_transaction_receipt({:#x})", tx_hash), || {
            client.get_transaction_receipt(tx_hash)
        })
        .await?;
        if let Some(receipt) = receipt {
            let gas_used = receipt.gas_used.map(|g| g.as_u64());
            let status = receipt.status.as_u64() == 1;
            receipt_map.insert(tx.hash, (gas_used, status));
        }
    }

    // Build DebankTransactions.
    let mut debank_transactions = Vec::new();
    for (idx, tx) in full_transactions.iter().enumerate() {
        let (gas_used, status) = receipt_map
            .get(&tx.hash)
            .map(|(g, s)| (g.unwrap_or(0), *s))
            .unwrap_or((0, false));

        let gas_fee_cap = tx.max_fee_per_gas.map(|v| v.as_u64()).unwrap_or(0);
        let gas_tip_cap = tx
            .max_priority_fee_per_gas
            .map(|v| v.as_u64())
            .unwrap_or(0);
        let gas_price = tx.gas_price.map(|v| v.as_u64()).unwrap_or(0);

        debank_transactions.push(DebankTransaction {
            id: format!("{:#x}", tx.hash),
            from: tx.from.unwrap_or_default(),
            to: tx.to,
            gas_limit: tx.gas.as_u64(),
            gas_price,
            gas_used,
            status,
            gas_fee_cap,
            gas_tip_cap,
            input: tx.input.clone(),
            nonce: tx.nonce.as_u64(),
            transaction_index: idx as u32,
            value: tx.value,
        });
    }

    tracing::info!(
        "Block {} receipts fetched. Fetching events via eth_getLogs...",
        block_num
    );

    // Fetch events from mainnet RPC via eth_getLogs.
    let logs = retry(&format!("get_logs(block={})", block_num), || {
        let filter = FilterBuilder::default()
            .set_from_block(BlockNumber::Number(block_num.into()))
            .set_to_block(BlockNumber::Number(block_num.into()))
            .build();
        client.get_logs(filter)
    })
    .await?;
    tracing::info!("Block {} has {} logs", block_num, logs.len());

    let debank_events: Vec<DebankEvent> = logs
        .iter()
        .map(|log| {
            let tx_hash = log.transaction_hash.unwrap_or_default();
            let log_index = log.log_index.map(|i| i.as_u32()).unwrap_or(0);
            DebankEvent {
                id: debank::to_hash(&[&format!("{:#x}", tx_hash), &log_index.to_string()]),
                contract_id: log.address,
                selector: log
                    .topics
                    .first()
                    .map(|t| format!("{:#x}", t))
                    .unwrap_or_default(),
                topics: log
                    .topics
                    .iter()
                    .skip(1)
                    .map(|t| format!("{:#x}", t))
                    .collect(),
                data: log.data.clone(),
                tx_id: format!("{:#x}", tx_hash),
                parent_trace_id: String::new(),
                pos_in_parent_trace: 0,
                log_index,
            }
        })
        .collect();

    // For blocks >= 2,219,807, fetch traces from mainnet RPC via debug_traceTransaction.
    let mut debank_traces = Vec::new();
    if block_num >= TRACES_START_BLOCK {
        tracing::info!(
            "Block {} >= {}: fetching traces for {} txs...",
            block_num,
            TRACES_START_BLOCK,
            full_transactions.len()
        );
        let tracer_config = TracerConfig {
            tracer: SupportedTracers::CallTracer,
            tracer_config: Default::default(),
        };
        for (i, tx) in full_transactions.iter().enumerate() {
            tracing::debug!(
                "Tracing tx {}/{}: {:#x}",
                i + 1,
                full_transactions.len(),
                tx.hash
            );
            let tx_hash = tx.hash;
            let trace_result =
                retry(&format!("trace_transaction({:#x})", tx_hash), || {
                    client.trace_transaction(tx_hash, Some(tracer_config))
                })
                .await?;

            if let Some(CallTracerResult::CallTrace(debug_call)) = trace_result {
                let root_trace_id =
                    debank::to_hash(&[tx.hash.to_string().as_str(), "", "0"]);
                flatten_debug_call_to_debank_traces(
                    &debug_call,
                    tx.hash,
                    &root_trace_id,
                    None,
                    0,
                    vec![],
                    &mut debank_traces,
                );
            }
        }
    }

    // Collect storage_contracts from traces.
    let mut storage_contracts: Vec<String> = debank_traces
        .iter()
        .filter(|trace| trace.self_storage_change)
        .map(|trace| {
            if trace.call_type == "delegatecall" {
                format!("{:?}", trace.from_addr)
            } else {
                format!("{:?}", trace.to_addr)
            }
        })
        .collect();
    let mut seen = HashSet::new();
    storage_contracts.retain(|addr| seen.insert(addr.clone()));

    let block_file = BlockFile {
        block: DebankBlock {
            id: l2_block.hash,
            height: l2_block.number.as_u64(),
            parent_id: l2_block.parent_hash,
            base_fee_per_gas: Some(l2_block.base_fee_per_gas.as_u64()),
            gas_limit: l2_block.gas_limit.as_u64(),
            gas_used: l2_block.gas_used.as_u64(),
            timestamp: l2_block.timestamp.as_u64(),
            process_start_timestamp: l2_block.timestamp.as_u64(),
            ..Default::default()
        },
        transactions: debank_transactions,
        events: debank_events,
        traces: debank_traces,
        error_traces: vec![],
        error_events: vec![],
        storage_contracts,
    };

    let header = build_header(&l2_block);

    Ok(DebankOutPut {
        header,
        validation_hash: block_file.validation().validation_hash,
        block_file,
        state_diff: vec![].into(),
    })
}

fn build_header(l2_block: &zksync_types::api::Block<TransactionVariant>) -> Header {
    Header {
        number: l2_block.number.as_u64(),
        hash: l2_block.hash,
        parent_hash: l2_block.parent_hash,
        nonce: l2_block.nonce,
        mix_hash: l2_block.mix_hash,
        sha3_uncles: l2_block.uncles_hash,
        logs_bloom: l2_block.logs_bloom,
        state_root: l2_block.state_root,
        miner: l2_block.author,
        difficulty: l2_block.difficulty,
        extra_data: l2_block.extra_data.clone(),
        gas_limit: l2_block.gas_limit.as_u64(),
        gas_used: l2_block.gas_used.as_u64(),
        timestamp: l2_block.timestamp.as_u64(),
        transactions_root: l2_block.transactions_root,
        receipts_root: l2_block.receipts_root,
        base_fee_per_gas: Some(l2_block.base_fee_per_gas),
        withdrawals_root: None,
        blob_gas_used: None,
        excess_blob_gas: None,
        parent_beacon_block_root: None,
        requests_root: None,
        ..Default::default()
    }
}

/// Recursively converts a DebugCall tree into flat DebankTrace entries.
fn flatten_debug_call_to_debank_traces(
    debug_call: &DebugCall,
    tx_hash: H256,
    trace_id: &str,
    parent_trace_id: Option<&str>,
    pos_in_parent_trace: u32,
    trace_address: Vec<u32>,
    traces: &mut Vec<DebankTrace>,
) {
    let (call_create_type, call_type) = match debug_call.r#type {
        DebugCallType::Create => ("create".to_string(), String::new()),
        DebugCallType::Call => ("call".to_string(), "call".to_string()),
        DebugCallType::DelegateCall => ("call".to_string(), "delegatecall".to_string()),
    };

    traces.push(DebankTrace {
        id: trace_id.to_string(),
        from_addr: debug_call.from,
        gas_limit: debug_call.gas.as_u64(),
        input: debug_call.input.clone(),
        to_addr: debug_call.to,
        value: debug_call.value,
        gas_used: debug_call.gas_used.as_u64(),
        output: debug_call.output.clone(),
        call_create_type,
        call_type,
        tx_id: format!("{:#x}", tx_hash),
        parent_trace_id: parent_trace_id.unwrap_or_default().to_string(),
        pos_in_parent_trace,
        self_storage_change: false,
        storage_change: false,
        sub_traces: debug_call.calls.len() as u32,
        trace_address: trace_address.clone(),
        error: debug_call
            .revert_reason
            .clone()
            .or_else(|| debug_call.error.clone())
            .unwrap_or_default(),
    });

    for (i, subcall) in debug_call.calls.iter().enumerate() {
        let child_trace_id = debank::to_hash(&[
            tx_hash.to_string().as_str(),
            trace_id,
            &(i as u32).to_string(),
        ]);
        let child_trace_address = debank::child_trace_address(&trace_address, i as u32);
        flatten_debug_call_to_debank_traces(
            subcall,
            tx_hash,
            &child_trace_id,
            Some(trace_id),
            i as u32,
            child_trace_address,
            traces,
        );
    }
}
