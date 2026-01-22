use crate::web3::state::RpcState;

use super::{DebugNamespace, EthNamespace};

#[derive(Debug)]
pub(crate) struct DebankNamespace {
    pub(crate) eth: EthNamespace,
    pub(crate) debug: DebugNamespace,
}

impl DebankNamespace {
    pub(crate) async fn new(state: RpcState) -> anyhow::Result<Self> {
        Ok(Self {
            eth: EthNamespace::new(state.clone()),
            debug: DebugNamespace::new(state).await?,
        })
    }
}


