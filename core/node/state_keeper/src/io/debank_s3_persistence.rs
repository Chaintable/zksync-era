use std::io::Read as IoRead;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_s3::Client as S3Client;
use flate2::read::GzDecoder;
use flate2::{write::GzEncoder, Compression};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use tokio::task::JoinHandle;
use zksync_multivm::{interface::TxExecutionStatus, tracers::debank};
use zksync_types::{
    debank::{
        assemble_block_file, BlockMeta, DebankOutPut, DebankTransaction,
        KafkaBlockChangeNotification, KafkaBlockContext, TxBlockData,
    },
    l2::TransactionType,
    utils::deployed_address_evm_create,
    web3::Bytes,
    ExecuteTransactionCommon, H256,
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
    pending_upload: Option<JoinHandle<Option<KafkaBlockContext>>>,
    /// Tracks the last block successfully notified to Kafka, used for
    /// continuity checking, gap filling, and duplicate/stale detection.
    last_block_context: Option<KafkaBlockContext>,
    /// Backup mode: only upload to S3, skip all Kafka interactions
    /// (no resume on startup, no notification, no gap fill).
    is_backup: bool,
}

impl std::fmt::Debug for DebankS3OutputHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DebankS3OutputHandler")
            .field("chain_id", &self.chain_id)
            .field("version", &self.version)
            .field("kafka_topic", &self.kafka_topic)
            .field("kafka_producer", &self.kafka_producer.as_ref().map(|_| "..."))
            .field(
                "last_block_context",
                &self
                    .last_block_context
                    .as_ref()
                    .map(|c| c.block_number),
            )
            .finish()
    }
}

impl DebankS3OutputHandler {
    pub async fn new(
        chain_id: u64,
        version: Option<String>,
        kafka_brokers: Option<String>,
        kafka_topic: Option<String>,
        is_backup: bool,
    ) -> Self {
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        tracing::info!(
            "DebankS3OutputHandler: AWS region={:?}",
            aws_config.region()
        );
        let s3_client = S3Client::new(&aws_config);

        let kafka_producer = if is_backup {
            tracing::info!("DebankS3OutputHandler: backup mode — skipping Kafka producer");
            None
        } else {
            kafka_brokers.as_ref().map(|brokers| {
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
            })
        };

        let last_block_context = if is_backup {
            None
        } else {
            match (kafka_brokers.as_deref(), kafka_topic.as_deref()) {
                (Some(brokers), Some(topic)) => {
                    match resume_from_kafka(brokers, topic).await {
                        Ok(ctx) => {
                            tracing::info!(
                                "DebankS3OutputHandler: resumed from Kafka, last block = {:?}",
                                ctx.as_ref().map(|c| c.block_number),
                            );
                            ctx
                        }
                        Err(e) => {
                            tracing::warn!(
                                "DebankS3OutputHandler: failed to resume from Kafka: {:#}",
                                e
                            );
                            None
                        }
                    }
                }
                _ => None,
            }
        };

        Self {
            s3_client,
            chain_id,
            version,
            kafka_producer,
            kafka_topic,
            pending_upload: None,
            last_block_context,
            is_backup,
        }
    }

    /// Wait for the previous block's upload to complete and update
    /// `last_block_context` if the Kafka notification succeeded.
    async fn wait_for_pending_upload(&mut self) {
        if let Some(handle) = self.pending_upload.take() {
            match handle.await {
                Ok(Some(ctx)) => {
                    self.last_block_context = Some(ctx);
                }
                Ok(None) => {
                    // S3 or Kafka failed, or block was skipped — keep existing last_block_context
                }
                Err(e) => {
                    tracing::error!("Previous debank upload task panicked: {:#}", e);
                }
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
        let last_ctx = self.last_block_context.clone();
        let is_backup = self.is_backup;
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
                return None;
            }
            tracing::info!(
                "Uploaded debank data for block {} to S3 ({} txs, {} traces, {} events)",
                block_number,
                output.block_file.transactions.len(),
                output.block_file.traces.len(),
                output.block_file.events.len(),
            );

            if is_backup {
                return None;
            }

            let (producer, topic) = match (kafka_producer, kafka_topic) {
                (Some(p), Some(t)) => (p, t),
                _ => return None,
            };

            // Skip if behind or duplicate
            if let Some(ref last) = last_ctx {
                if block_context.block_number <= last.block_number {
                    tracing::info!(
                        "Skipping Kafka notification for block {} (Kafka already at {})",
                        block_context.block_number,
                        last.block_number,
                    );
                    return None;
                }

                // Fill gap if needed
                if block_context.block_number > last.block_number + 1 {
                    tracing::warn!(
                        "Kafka gap detected: last={}, current={}. Filling from S3...",
                        last.block_number,
                        block_context.block_number,
                    );
                    if let Err(e) = fill_kafka_gap(
                        &s3,
                        &producer,
                        &topic,
                        chain_id,
                        version.as_deref(),
                        last.block_number,
                        &block_context,
                    )
                    .await
                    {
                        tracing::error!(
                            "Failed to fill Kafka gap (blocks {}..{}): {:#}. \
                             Skipping current block to preserve continuity.",
                            last.block_number + 1,
                            block_context.block_number - 1,
                            e,
                        );
                        return None;
                    }
                }
            }

            // Send Kafka notification for current block
            match send_kafka_notification(
                &producer,
                &topic,
                block_context.clone(),
                block_number,
            )
            .await
            {
                Ok(()) => Some(block_context),
                Err(e) => {
                    tracing::error!(
                        "Failed to send Kafka notification for block {}: {:#}",
                        block_number,
                        e
                    );
                    None
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

        let mut tx_results = Vec::new();

        for (idx, tx_result) in block.executed_transactions.iter().enumerate() {
            // Skip L1 and ProtocolUpgrade transactions
            let l2_data = match &tx_result.transaction.common_data {
                ExecuteTransactionCommon::L2(data) => data,
                _ => continue,
            };

            let tx_hash = tx_result.hash;
            let tx = &tx_result.transaction;

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

            let debank_tx = DebankTransaction {
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
            };

            let mut traces = Vec::new();
            let mut error_traces = Vec::new();
            let mut events = Vec::new();
            let mut error_events = Vec::new();

            if !tx_result.call_traces.is_empty() {
                if let Some(mut first_call) = tx_result.call_trace() {
                    first_call.trace_id =
                        debank::to_hash(&[tx_hash.to_string().as_str(), "", "0"]);

                    for (i, subcall) in first_call.calls.iter_mut().enumerate() {
                        subcall.pos_in_parent_trace = i as u32;
                    }

                    debank::set_parent_failed(&mut first_call, false);

                    let root_trace =
                        debank::to_debank_trace(&first_call, tx_hash, vec![]);
                    // root call has no parent so parent_failed is always false;
                    // the `|| ... parent_failed` is kept for symmetry only.
                    if first_call.revert_reason.is_some() || first_call.parent_failed {
                        error_traces.push(root_trace);
                    } else {
                        traces.push(root_trace);
                    }

                    debank::add_trace_log(
                        tx_hash,
                        &mut traces,
                        &mut error_traces,
                        &mut events,
                        &mut error_events,
                        vec![],
                        &mut first_call,
                    );
                }
            }

            tx_results.push(TxBlockData {
                debank_tx,
                traces,
                error_traces,
                events,
                error_events,
            });
        }

        let block_meta = BlockMeta {
            hash: block_hash,
            parent_hash: block.prev_block_hash,
            number: block_number,
            timestamp: block_timestamp,
            base_fee_per_gas: base_fee,
            gas_limit: l2_header.gas_limit,
            logs_bloom: l2_header.logs_bloom,
        };
        let (block_file, header) = assemble_block_file(block_meta, tx_results);

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

/// Read the last Kafka message to recover `last_block_context` on startup.
async fn resume_from_kafka(
    brokers: &str,
    topic: &str,
) -> anyhow::Result<Option<KafkaBlockContext>> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "true")
        .set("session.timeout.ms", "6000")
        .set("group.id", "debank_s3_resume_group")
        .create()?;

    let partition = 0;
    let (low, high) =
        consumer.fetch_watermarks(topic, partition, Duration::from_secs(5))?;
    tracing::info!(
        "resume_from_kafka: watermarks for partition {}: low={}, high={}",
        partition,
        low,
        high,
    );

    if high <= low {
        return Ok(None);
    }

    // Seek to the last message
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, partition, Offset::Offset(high - 1))?;
    consumer.assign(&tpl)?;

    match consumer.recv().await {
        Ok(msg) => {
            let payload = msg
                .payload()
                .ok_or_else(|| anyhow::anyhow!("empty Kafka payload"))?;
            let decoded = decompress_gzip(payload)?;
            let notification: KafkaBlockChangeNotification =
                serde_json::from_slice(&decoded)?;
            Ok(notification.new_blocks.last().cloned())
        }
        Err(rdkafka::error::KafkaError::PartitionEOF(_)) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Fill missing Kafka notifications by reading block headers from S3.
/// Walks backwards from `current_block.parent_hash` to collect headers
/// for blocks `(last_block_number+1)..current_block.block_number`, then
/// sends notifications in ascending order.
async fn fill_kafka_gap(
    s3: &S3Client,
    producer: &FutureProducer,
    topic: &str,
    chain_id: u64,
    version: Option<&str>,
    last_block_number: u64,
    current_block: &KafkaBlockContext,
) -> anyhow::Result<()> {
    let gap_size = (current_block.block_number - last_block_number - 1) as usize;
    if gap_size == 0 {
        return Ok(());
    }

    let mut missing = Vec::with_capacity(gap_size);
    let mut hash = current_block.parent_hash;

    for _ in 0..gap_size {
        let header = read_header_from_s3(s3, chain_id, version, hash).await?;
        missing.push(KafkaBlockContext {
            hash: header.hash,
            parent_hash: header.parent_hash,
            block_number: header.number,
            timestamp: header.timestamp,
        });
        hash = header.parent_hash;
    }

    missing.reverse();

    for ctx in &missing {
        send_kafka_notification(producer, topic, ctx.clone(), ctx.block_number).await?;
        tracing::info!(
            "Filled Kafka gap: sent notification for block {}",
            ctx.block_number,
        );
    }

    Ok(())
}

/// Read a block header from the HEADER_BUCKET in S3.
async fn read_header_from_s3(
    s3: &S3Client,
    chain_id: u64,
    version: Option<&str>,
    block_hash: H256,
) -> anyhow::Result<Header> {
    let prefix = match version {
        Some(v) => format!("{}/{}", chain_id, v),
        None => chain_id.to_string(),
    };
    let block_hash = format!("{:#x}", block_hash);
    let key = format!("{}/{}/block", prefix, block_hash);

    let resp = s3
        .get_object()
        .bucket(HEADER_BUCKET)
        .key(&key)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get header from S3 key={}: {:?}", key, e))?;

    let body = resp.body.collect().await?.into_bytes();
    let decoded = decompress_gzip(&body)?;
    let header: Header = serde_json::from_slice(&decoded)?;
    Ok(header)
}

/// Decompress gzip-encoded bytes.
fn decompress_gzip(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut decoder = GzDecoder::new(data);
    let mut decoded = Vec::new();
    decoder.read_to_end(&mut decoded)?;
    Ok(decoded)
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
