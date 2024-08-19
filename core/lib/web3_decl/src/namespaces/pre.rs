#[cfg_attr(not(feature = "server"), allow(unused_imports))]
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use zksync_types::{
    api::{BlockId, PreResult},
    transaction_request::CallRequest,
};

use crate::client::{ForNetwork, L2};

#[cfg_attr(
    feature = "server",
    rpc(server, client, namespace = "pre", client_bounds(Self: ForNetwork<Net = L2>))
)]
#[cfg_attr(
    not(feature = "server"),
    rpc(client, namespace = "pre", client_bounds(Self: ForNetwork<Net = L2>))
)]
pub trait PreNamespace {
    #[method(name = "traceMany")]
    async fn pre_trace_many(
        &self,
        requests: Vec<CallRequest>,
        block: Option<BlockId>,
    ) -> RpcResult<Vec<PreResult>>;
}
