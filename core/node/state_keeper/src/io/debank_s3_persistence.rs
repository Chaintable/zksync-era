use std::collections::HashSet;

use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use flate2::{write::GzEncoder, Compression};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use zksync_multivm::{interface::TxExecutionStatus, tracers::debank};
use zksync_types::{
    debank::{
        BlockFile, DebankBlock, DebankOutPut, DebankTransaction, Header,
        KafkaBlockChangeNotification, KafkaBlockContext,
    },
    l2::TransactionType,
    utils::deployed_address_evm_create,
    web3::Bytes,
    Address, ExecuteTransactionCommon, H256, U256,
};

use crate::{io::output_handler::StateKeeperOutputHandler, updates::UpdatesManager};

/// S3 bucket for headers.
const HEADER_BUCKET: &str = "chaintable-nodex-pipeline--apne1-az4--x-s3";
/// S3 bucket for block files and validation.
const BLOCK_FILE_BUCKET: &str = "chaintable-pipeline--apne1-az4--x-s3";

/// Output handler that assembles `DebankOutPut` from live block execution
/// and uploads it to S3 as blocks are sealed, then sends a Kafka notification.
pub struct DebankS3OutputHandler {
    s3_client: S3Client,
    chain_id: u64,
    kafka_producer: Option<FutureProducer>,
    kafka_topic: Option<String>,
}

impl std::fmt::Debug for DebankS3OutputHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DebankS3OutputHandler")
            .field("chain_id", &self.chain_id)
            .field("kafka_topic", &self.kafka_topic)
            .field("kafka_producer", &self.kafka_producer.as_ref().map(|_| "..."))
            .finish()
    }
}

impl DebankS3OutputHandler {
    pub async fn new(
        chain_id: u64,
        kafka_brokers: Option<String>,
        kafka_topic: Option<String>,
    ) -> Self {
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        tracing::info!(
            "DebankS3OutputHandler: AWS region={:?}",
            aws_config.region()
        );
        let s3_client = S3Client::new(&aws_config);

        let kafka_producer = kafka_brokers.as_ref().map(|brokers| {
            let producer: FutureProducer = ClientConfig::new()
                .set("bootstrap.servers", brokers)
                .set("message.timeout.ms", "5000")
                .set("enable.idempotence", "true")
                .create()
                .expect("Failed to create Kafka producer");
            tracing::info!(
                "DebankS3OutputHandler: Kafka producer created for brokers={}",
                brokers
            );
            producer
        });

        Self {
            s3_client,
            chain_id,
            kafka_producer,
            kafka_topic,
        }
    }

    fn build_debank_output(&self, updates_manager: &UpdatesManager) -> DebankOutPut {
        let block = updates_manager.last_pending_l2_block();
        let l2_header = updates_manager.header_for_first_pending_block();
        let base_fee = l2_header.base_fee_per_gas;

        let block_hash = l2_header.hash;
        let block_number = l2_header.number.0 as u64;
        let block_timestamp = l2_header.timestamp;

        let mut debank_transactions = Vec::new();
        let mut all_traces = Vec::new();
        let mut all_events = Vec::new();
        let mut all_error_traces = Vec::new();
        let mut all_error_events = Vec::new();
        let mut global_log_index: u32 = 0;

        for (idx, tx_result) in block.executed_transactions.iter().enumerate() {
            // Skip L1 and ProtocolUpgrade transactions
            let l2_data = match &tx_result.transaction.common_data {
                ExecuteTransactionCommon::L2(data) => data,
                _ => continue,
            };

            let tx_hash = tx_result.hash;
            let tx = &tx_result.transaction;

            // Build DebankTransaction
            let to_address = tx.execute.contract_address.or_else(|| {
                Some(deployed_address_evm_create(
                    l2_data.initiator_address,
                    (*l2_data.nonce).into(),
                ))
            });

            let gas_limit = l2_data.fee.gas_limit.as_u64();
            let gas_used = gas_limit.saturating_sub(tx_result.refunded_gas);
            let gas_price = l2_data
                .fee
                .get_effective_gas_price(base_fee.into())
                .as_u64();

            let (gas_fee_cap, gas_tip_cap) =
                if l2_data.transaction_type as u32 >= TransactionType::EIP1559Transaction as u32 {
                    (
                        l2_data.fee.max_fee_per_gas.as_u64(),
                        l2_data.fee.max_priority_fee_per_gas.as_u64(),
                    )
                } else {
                    (0, 0)
                };

            let status = tx_result.execution_status == TxExecutionStatus::Success;

            debank_transactions.push(DebankTransaction {
                id: format!("{:#x}", tx_hash),
                from: l2_data.initiator_address,
                to: to_address,
                gas_limit,
                gas_price,
                gas_used,
                status,
                gas_fee_cap,
                gas_tip_cap,
                input: tx.execute.calldata.clone().into(),
                nonce: (*l2_data.nonce) as u64,
                transaction_index: idx as u32,
                value: tx.execute.value,
            });

            // Build traces and events from call traces
            if !tx_result.call_traces.is_empty() {
                if let Some(mut first_call) = tx_result.call_trace() {
                    // Set trace_id on root call
                    first_call.trace_id =
                        debank::to_hash(&[tx_hash.to_string().as_str(), "", "0"]);

                    // Set pos_in_parent_trace for subcalls
                    for (i, subcall) in first_call.calls.iter_mut().enumerate() {
                        subcall.pos_in_parent_trace = i as u32;
                    }

                    // Mark parent_failed on subcalls
                    debank::set_parent_failed(&mut first_call, false);

                    // Push root trace
                    let root_trace =
                        debank::to_debank_trace(&first_call, tx_hash, vec![]);
                    if first_call.revert_reason.is_some() || first_call.parent_failed {
                        all_error_traces.push(root_trace);
                    } else {
                        all_traces.push(root_trace);
                    }

                    // Recursively convert subcalls to traces/events
                    debank::add_trace_log(
                        tx_hash,
                        &mut all_traces,
                        &mut all_error_traces,
                        &mut all_events,
                        &mut all_error_events,
                        vec![],
                        &mut first_call,
                    );

                    // Assign per-tx log_index to events
                    for event in all_events.iter_mut().skip(global_log_index as usize) {
                        event.log_index = global_log_index;
                        global_log_index += 1;
                    }
                    // Don't increment global_log_index for error events
                }
            }
        }

        // Build storage_contracts from traces with self_storage_change
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

        // Assemble BlockFile
        let block_file = BlockFile {
            block: DebankBlock {
                id: block_hash,
                height: block_number,
                parent_id: block.prev_block_hash,
                base_fee_per_gas: Some(base_fee),
                gas_limit: l2_header.gas_limit,
                gas_used: debank_transactions.iter().map(|tx| tx.gas_used).sum(),
                timestamp: block_timestamp,
                process_start_timestamp: block_timestamp,
                ..Default::default()
            },
            transactions: debank_transactions,
            events: all_events,
            traces: all_traces,
            error_traces: all_error_traces,
            error_events: all_error_events,
            storage_contracts,
        };

        // keccak256 of RLP-encoded empty list — standard Ethereum empty uncles hash
        let empty_uncles_hash = H256::from_slice(
            &hex::decode("1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347")
                .unwrap(),
        );

        // Build Header
        let header = Header {
            number: block_number,
            hash: block_hash,
            parent_hash: block.prev_block_hash,
            sha3_uncles: empty_uncles_hash,
            logs_bloom: l2_header.logs_bloom,
            gas_limit: l2_header.gas_limit,
            gas_used: block_file.block.gas_used,
            timestamp: block_timestamp,
            base_fee_per_gas: Some(U256::from(base_fee)),
            miner: Address::default(),
            ..Default::default()
        };

        let validation_hash = block_file.validation().validation_hash;

        DebankOutPut {
            block_file,
            header,
            state_diff: Bytes::default(),
            validation_hash,
        }
    }
}

#[async_trait]
impl StateKeeperOutputHandler for DebankS3OutputHandler {
    async fn handle_l2_block_data(
        &mut self,
        updates_manager: &UpdatesManager,
    ) -> anyhow::Result<()> {
        let output = self.build_debank_output(updates_manager);
        let block_number = output.header.number;

        let block_context = KafkaBlockContext {
            hash: output.header.hash,
            parent_hash: output.header.parent_hash,
            block_number: output.header.number,
            timestamp: output.header.timestamp,
        };

        // Spawn background upload so we don't block the state keeper pipeline
        let s3 = self.s3_client.clone();
        let chain_id = self.chain_id;
        let kafka_producer = self.kafka_producer.clone();
        let kafka_topic = self.kafka_topic.clone();
        tokio::spawn(async move {
            if let Err(e) = upload_to_s3(&s3, chain_id, &output).await {
                tracing::error!(
                    "Failed to upload debank data for block {} to S3: {:#}",
                    block_number,
                    e
                );
                return;
            }
            tracing::info!(
                "Uploaded debank data for block {} to S3 ({} txs, {} traces, {} events)",
                block_number,
                output.block_file.transactions.len(),
                output.block_file.traces.len(),
                output.block_file.events.len(),
            );

            // Send Kafka notification after successful S3 upload
            if let (Some(producer), Some(topic)) = (kafka_producer, kafka_topic) {
                if let Err(e) =
                    send_kafka_notification(&producer, &topic, block_context, block_number).await
                {
                    tracing::error!(
                        "Failed to send Kafka notification for block {}: {:#}",
                        block_number,
                        e
                    );
                }
            }
        });

        Ok(())
    }
}

/// Upload DebankOutPut to S3 with the DeBank-specific bucket/path layout.
async fn upload_to_s3(
    s3: &S3Client,
    chain_id: u64,
    output: &DebankOutPut,
) -> anyhow::Result<()> {
    let block_hash = format!("{:#x}", output.header.hash);
    let block_num = output.header.number;

    // 1. Header -> chaintable-nodex-pipeline bucket at {chain_id}/{blockHash}/block
    let header_key = format!("{}/{}/block", chain_id, block_hash);
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    serde_json::to_writer(&mut gz, &output.header)?;
    let header_compressed = gz.finish()?;
    s3.put_object()
        .bucket(HEADER_BUCKET)
        .key(&header_key)
        .body(header_compressed.into())
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to upload header for block {}: {:?}", block_num, e))?;

    // 2. BlockFile -> chaintable-pipeline bucket at {chain_id}/{blockHash}
    let block_file_key = format!("{}/{}", chain_id, block_hash);
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    serde_json::to_writer(&mut gz, &output.block_file)?;
    let block_file_compressed = gz.finish()?;
    s3.put_object()
        .bucket(BLOCK_FILE_BUCKET)
        .key(&block_file_key)
        .body(block_file_compressed.into())
        .send()
        .await
        .map_err(|e| {
            anyhow::anyhow!("Failed to upload block file for block {}: {:?}", block_num, e)
        })?;

    // 3. Validation -> chaintable-pipeline bucket at {chain_id}/{blockNum}/{blockHash}
    let validation = output.block_file.validation();
    let validation_key = format!("{}/{}/{}", chain_id, block_num, block_hash);
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    serde_json::to_writer(&mut gz, &validation)?;
    let validation_compressed = gz.finish()?;
    s3.put_object()
        .bucket(BLOCK_FILE_BUCKET)
        .key(&validation_key)
        .body(validation_compressed.into())
        .send()
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to upload validation for block {}: {:?}",
                block_num,
                e
            )
        })?;

    Ok(())
}

/// Send a block change notification to Kafka (gzip-compressed JSON, key="NewBlock").
async fn send_kafka_notification(
    producer: &FutureProducer,
    topic: &str,
    block_context: KafkaBlockContext,
    block_number: u64,
) -> anyhow::Result<()> {
    let notification = KafkaBlockChangeNotification {
        change_type: 1, // New block added
        new_blocks: vec![block_context],
        drop_blocks: vec![],
    };

    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    serde_json::to_writer(&mut gz, &notification)?;
    let payload = gz.finish()?;

    let record = FutureRecord::to(topic).key("NewBlock").payload(&payload);
    producer
        .send(record, Timeout::Never)
        .await
        .map_err(|(e, _)| {
            anyhow::anyhow!(
                "Failed to send Kafka notification for block {}: {}",
                block_number,
                e
            )
        })?;

    tracing::info!(
        "Sent Kafka block notification for block {} to topic '{}'",
        block_number,
        topic
    );
    Ok(())
}
