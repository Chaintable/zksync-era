use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use flate2::{write::GzEncoder, Compression};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use tokio::task::JoinHandle;
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
    /// Optional version segment inserted into S3 paths after chain_id.
    version: Option<String>,
    kafka_producer: Option<FutureProducer>,
    kafka_topic: Option<String>,
    /// Handle to the previous block's upload task. Awaited before spawning the
    /// next upload so that S3 writes and Kafka notifications are strictly ordered.
    pending_upload: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for DebankS3OutputHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DebankS3OutputHandler")
            .field("chain_id", &self.chain_id)
            .field("version", &self.version)
            .field("kafka_topic", &self.kafka_topic)
            .field("kafka_producer", &self.kafka_producer.as_ref().map(|_| "..."))
            .finish()
    }
}

impl DebankS3OutputHandler {
    pub async fn new(
        chain_id: u64,
        version: Option<String>,
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
            version,
            kafka_producer,
            kafka_topic,
            pending_upload: None,
        }
    }

    /// Wait for the previous block's upload to complete so notifications stay ordered.
    async fn wait_for_pending_upload(&mut self) {
        if let Some(handle) = self.pending_upload.take() {
            if let Err(e) = handle.await {
                tracing::error!("Previous debank upload task panicked: {:#}", e);
            }
        }
    }

    /// Build output, wait for the previous upload to finish, then spawn the next one.
    async fn upload_block(&mut self, updates_manager: &UpdatesManager) {
        let output = self.build_debank_output(updates_manager);
        let block_number = output.header.number;

        let block_context = KafkaBlockContext {
            hash: output.header.hash,
            parent_hash: output.header.parent_hash,
            block_number: output.header.number,
            timestamp: output.header.timestamp,
        };

        // Wait for the previous block's upload to complete before spawning the next one.
        self.wait_for_pending_upload().await;

        let s3 = self.s3_client.clone();
        let chain_id = self.chain_id;
        let version = self.version.clone();
        let kafka_producer = self.kafka_producer.clone();
        let kafka_topic = self.kafka_topic.clone();
        self.pending_upload = Some(tokio::spawn(async move {
            const MAX_RETRIES: u32 = 5;
            const INITIAL_BACKOFF: Duration = Duration::from_secs(2);

            let mut uploaded = false;
            for attempt in 0..MAX_RETRIES {
                match upload_to_s3(&s3, chain_id, version.as_deref(), &output).await {
                    Ok(()) => {
                        uploaded = true;
                        break;
                    }
                    Err(e) => {
                        let backoff = INITIAL_BACKOFF * 2u32.pow(attempt);
                        tracing::warn!(
                            "Failed to upload debank data for block {} to S3 (attempt {}/{}): {:#}. Retrying in {:?}",
                            block_number,
                            attempt + 1,
                            MAX_RETRIES,
                            e,
                            backoff,
                        );
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
            if !uploaded {
                tracing::error!(
                    "Failed to upload debank data for block {} to S3 after {} attempts, giving up",
                    block_number,
                    MAX_RETRIES,
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
        }));
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
        self.upload_block(updates_manager).await;
        Ok(())
    }

    async fn handle_l1_batch(
        &mut self,
        updates_manager: Arc<UpdatesManager>,
    ) -> anyhow::Result<()> {
        // The last pending L2 block at batch seal time is the fictive block.
        // It has 0 executed transactions so handle_l2_block_data is never called for it
        // (seal_last_pending_block_data skips empty blocks). Upload it here so every
        // L2 block in the chain is present in S3 / Kafka.
        self.upload_block(&updates_manager).await;
        Ok(())
    }
}

/// Upload DebankOutPut to S3 with the DeBank-specific bucket/path layout.
async fn upload_to_s3(
    s3: &S3Client,
    chain_id: u64,
    version: Option<&str>,
    output: &DebankOutPut,
) -> anyhow::Result<()> {
    let block_hash = format!("{:#x}", output.header.hash);
    let block_num = output.header.number;

    // Build the prefix: "{chain_id}" or "{chain_id}/{version}"
    let prefix = match version {
        Some(v) => format!("{}/{}", chain_id, v),
        None => chain_id.to_string(),
    };

    // 1. Header -> chaintable-nodex-pipeline bucket at {prefix}/{blockHash}/block
    let header_key = format!("{}/{}/block", prefix, block_hash);
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

    // 2. BlockFile -> chaintable-pipeline bucket at {prefix}/{blockHash}
    let block_file_key = format!("{}/{}", prefix, block_hash);
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

    // 3. Validation -> chaintable-pipeline bucket at {prefix}/{blockNum}/{blockHash}
    let validation = output.block_file.validation();
    let validation_key = format!("{}/{}/{}", prefix, block_num, block_hash);
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
