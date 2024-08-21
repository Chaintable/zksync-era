#[cfg_attr(not(feature = "server"), allow(unused_imports))]
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use zksync_types::api::OpenEthActionTrace;

use crate::{
    client::{ForWeb3Network, L2},
    types::H256,
};

#[cfg_attr(
    feature = "server",
    rpc(server, client, namespace = "trace", client_bounds(Self: ForWeb3Network<Net = L2>))
)]
#[cfg_attr(
    not(feature = "server"),
    rpc(client, namespace = "trace", client_bounds(Self: ForWeb3Network<Net = L2>))
)]
pub trait TraceNamespace {
    #[method(name = "transaction")]
    async fn trace_trace_transaction(&self, tx_hash: H256) -> RpcResult<Vec<OpenEthActionTrace>>;
}
