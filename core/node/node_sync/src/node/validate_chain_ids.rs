use anyhow::Context as _;
use zksync_node_framework::{
    service::StopReceiver,
    task::{Task, TaskId, TaskKind},
    wiring_layer::{WiringError, WiringLayer},
    FromContext, IntoContext,
};
use zksync_types::{L1ChainId, L2ChainId};
use zksync_web3_decl::client::{DynClient, L1, L2};

use crate::validate_chain_ids_task::ValidateChainIdsTask;

/// Wiring layer for chain ID validation precondition for external node.
/// Ensures that chain IDs are consistent locally and on the main node. It can additionally check
/// the settlement layer client when constructed with [`Self::new`].
///
/// ## Requests resources
///
/// - `MainNodeClientResource`
/// - `EthInterfaceResource` (only when constructed with [`Self::new`])
///
/// ## Adds preconditions
///
/// - `ValidateChainIdsTask`
#[derive(Debug)]
pub struct ValidateChainIdsLayer {
    l1_chain_id: L1ChainId,
    l2_chain_id: L2ChainId,
    require_l1_client: bool,
}

#[derive(Debug, FromContext)]
pub struct Input {
    l1_client: Option<Box<DynClient<L1>>>,
    main_node_client: Box<DynClient<L2>>,
}

#[derive(Debug, IntoContext)]
pub struct Output {
    #[context(task)]
    task: ValidateChainIdsTask,
}

impl ValidateChainIdsLayer {
    pub fn new(l1_chain_id: L1ChainId, l2_chain_id: L2ChainId) -> Self {
        Self {
            l1_chain_id,
            l2_chain_id,
            require_l1_client: true,
        }
    }

    pub fn without_l1_client(l1_chain_id: L1ChainId, l2_chain_id: L2ChainId) -> Self {
        Self {
            l1_chain_id,
            l2_chain_id,
            require_l1_client: false,
        }
    }
}

#[async_trait::async_trait]
impl WiringLayer for ValidateChainIdsLayer {
    type Input = Input;
    type Output = Output;

    fn layer_name(&self) -> &'static str {
        "validate_chain_ids_layer"
    }

    async fn wire(self, input: Self::Input) -> Result<Self::Output, WiringError> {
        let task = if self.require_l1_client {
            ValidateChainIdsTask::new(
                self.l1_chain_id,
                self.l2_chain_id,
                input
                    .l1_client
                    .context("L1 client is required for chain ID validation")?,
                input.main_node_client,
            )
        } else {
            ValidateChainIdsTask::without_l1_client(
                self.l1_chain_id,
                self.l2_chain_id,
                input.main_node_client,
            )
        };
        Ok(Output { task })
    }
}

#[async_trait::async_trait]
impl Task for ValidateChainIdsTask {
    fn kind(&self) -> TaskKind {
        TaskKind::OneshotTask
    }

    fn id(&self) -> TaskId {
        "validate_chain_ids".into()
    }

    async fn run(self: Box<Self>, stop_receiver: StopReceiver) -> anyhow::Result<()> {
        (*self).run_once(stop_receiver.0).await
    }
}
