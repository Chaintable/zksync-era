use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use zksync_types::{
    api::{BlockId, OpenEthActionTrace, TransactionReceipt},
    transaction_request::CallRequest,
    H256,
};

#[rpc(server, client, namespace = "trace")]
#[async_trait::async_trait]
pub trait TraceApi {
    #[method(name = "transaction")]
    async fn trace_transaction(&self, tx_hash: H256) -> RpcResult<Vec<OpenEthActionTrace>>;
}

#[rpc(server, client, namespace = "pre")]
#[async_trait::async_trait]
pub trait PreApi {
    #[method(name = "traceTransaction")]
    async fn pre_trace_transaction(
        &self,
        request: CallRequest,
        block: Option<BlockId>,
    ) -> RpcResult<Vec<OpenEthActionTrace>>;

    #[method(name = "getLogs")]
    async fn pre_get_logs(
        &self,
        request: CallRequest,
        block: Option<BlockId>,
    ) -> RpcResult<TransactionReceipt>;
}
