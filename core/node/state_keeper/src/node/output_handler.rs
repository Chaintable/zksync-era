use anyhow::Context as _;
use zksync_dal::node::{MasterPool, PoolResource};
use zksync_node_framework::{
    resource::Unique,
    service::StopReceiver,
    task::{Task, TaskId},
    wiring_layer::{WiringError, WiringLayer},
    FromContext, IntoContext,
};
use zksync_shared_resources::{api::SyncState, contracts::L2ContractsResource};
use zksync_types::L2_ASSET_ROUTER_ADDRESS;

use super::resources::OutputHandlerResource;
use crate::{
    io::seal_logic::l2_block_seal_subtasks::L2BlockSealProcess, DebankS3OutputHandler,
    L2BlockSealerTask, OutputHandler, StateKeeperPersistence, TreeWritesPersistence,
};

/// Wiring layer for the state keeper output handler.
///
/// ## Requests resources
///
/// - `PoolResource<MasterPool>`
/// - `SyncStateResource` (optional)
///
/// ## Adds resources
///
/// - `OutputHandlerResource`
///
/// ## Adds tasks
///
/// - `L2BlockSealerTask`
#[derive(Debug)]
pub struct OutputHandlerLayer {
    l2_block_seal_queue_capacity: usize,
    /// Whether transactions should be pre-inserted to DB.
    /// Should be set to `true` for EN's IO as EN doesn't store transactions in DB
    /// before they are included into L2 blocks.
    pre_insert_txs: bool,
    /// Whether protective reads persistence is enabled.
    /// May be set to `false` for nodes that do not participate in the sequencing process (e.g. external nodes)
    /// or run `vm_runner_protective_reads` component.
    protective_reads_persistence_enabled: bool,
    /// Whether to upload Debank block data to S3 as blocks are sealed.
    debank_s3_enabled: bool,
    /// Chain ID used for S3 key paths.
    chain_id: u64,
    /// Kafka broker addresses for Debank block notifications.
    debank_kafka_brokers: Option<String>,
    /// Kafka topic for Debank block notifications.
    debank_kafka_topic: Option<String>,
    /// Optional version segment for Debank S3 paths.
    debank_version: Option<String>,
    /// When true, the Debank handler only uploads to S3 and skips all Kafka work.
    debank_is_backup: bool,
}

#[derive(Debug, FromContext)]
pub struct Input {
    master_pool: PoolResource<MasterPool>,
    sync_state: Option<SyncState>,
    l2_contracts: L2ContractsResource,
}

#[derive(Debug, IntoContext)]
pub struct Output {
    output_handler: OutputHandlerResource,
    #[context(task)]
    l2_block_sealer: L2BlockSealerTask,
}

impl OutputHandlerLayer {
    pub fn new(l2_block_seal_queue_capacity: usize) -> Self {
        Self {
            l2_block_seal_queue_capacity,
            pre_insert_txs: false,
            protective_reads_persistence_enabled: false,
            debank_s3_enabled: false,
            chain_id: 0,
            debank_kafka_brokers: None,
            debank_kafka_topic: None,
            debank_version: None,
            debank_is_backup: false,
        }
    }

    pub fn with_pre_insert_txs(mut self, pre_insert_txs: bool) -> Self {
        self.pre_insert_txs = pre_insert_txs;
        self
    }

    pub fn with_protective_reads_persistence_enabled(
        mut self,
        protective_reads_persistence_enabled: bool,
    ) -> Self {
        self.protective_reads_persistence_enabled = protective_reads_persistence_enabled;
        self
    }

    pub fn with_debank_s3(mut self, enabled: bool, chain_id: u64) -> Self {
        self.debank_s3_enabled = enabled;
        self.chain_id = chain_id;
        self
    }

    pub fn with_debank_kafka(
        mut self,
        brokers: Option<String>,
        topic: Option<String>,
    ) -> Self {
        self.debank_kafka_brokers = brokers;
        self.debank_kafka_topic = topic;
        self
    }

    pub fn with_debank_version(mut self, version: Option<String>) -> Self {
        self.debank_version = version;
        self
    }

    pub fn with_debank_is_backup(mut self, is_backup: bool) -> Self {
        self.debank_is_backup = is_backup;
        self
    }
}

#[async_trait::async_trait]
impl WiringLayer for OutputHandlerLayer {
    type Input = Input;
    type Output = Output;

    fn layer_name(&self) -> &'static str {
        "state_keeper_output_handler_layer"
    }

    async fn wire(self, input: Self::Input) -> Result<Self::Output, WiringError> {
        // Create L2 block sealer task and output handler.
        // L2 Block sealing process is parallelized, so we have to provide enough pooled connections.
        let persistence_pool = input
            .master_pool
            .get_custom(L2BlockSealProcess::subtasks_len())
            .await
            .context("Get master pool")?;

        let l2_contracts = &input.l2_contracts.0;
        let l2_shared_bridge_addr = l2_contracts.shared_bridge_addr;
        let l2_legacy_shared_bridge_addr = if l2_shared_bridge_addr == L2_ASSET_ROUTER_ADDRESS {
            // System has migrated to `L2_ASSET_ROUTER_ADDRESS`, use legacy shared bridge address from main node.
            l2_contracts.legacy_shared_bridge_addr
        } else {
            // System hasn't migrated on `L2_ASSET_ROUTER_ADDRESS`, we can safely use `l2_shared_bridge_addr`.
            Some(l2_shared_bridge_addr)
        };

        let (mut persistence, l2_block_sealer) = StateKeeperPersistence::new(
            persistence_pool.clone(),
            l2_legacy_shared_bridge_addr,
            self.l2_block_seal_queue_capacity,
        )
        .await?;
        if self.pre_insert_txs {
            persistence = persistence.with_tx_insertion();
        }
        if !self.protective_reads_persistence_enabled {
            persistence = persistence.without_protective_reads();
        }

        let tree_writes_persistence = TreeWritesPersistence::new(persistence_pool);
        let mut output_handler = OutputHandler::new(Box::new(persistence))
            .with_handler(Box::new(tree_writes_persistence));
        if let Some(sync_state) = input.sync_state {
            output_handler = output_handler.with_handler(Box::new(sync_state));
        }
        if self.debank_s3_enabled {
            let debank_handler = DebankS3OutputHandler::new(
                self.chain_id,
                self.debank_version.clone(),
                self.debank_kafka_brokers.clone(),
                self.debank_kafka_topic.clone(),
                self.debank_is_backup,
            )
            .await;
            output_handler = output_handler.with_handler(Box::new(debank_handler));
            tracing::info!(
                "Debank S3 output handler enabled for chain_id={}, version={:?}, is_backup={}",
                self.chain_id,
                self.debank_version,
                self.debank_is_backup,
            );
        }
        let output_handler = OutputHandlerResource(Unique::new(output_handler));

        Ok(Output {
            output_handler,
            l2_block_sealer,
        })
    }
}

#[async_trait::async_trait]
impl Task for L2BlockSealerTask {
    fn id(&self) -> TaskId {
        "state_keeper/l2_block_sealer".into()
    }

    async fn run(self: Box<Self>, _stop_receiver: StopReceiver) -> anyhow::Result<()> {
        // Miniblock sealer will exit itself once sender is dropped.
        (*self).run().await
    }
}
