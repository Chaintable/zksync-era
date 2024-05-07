use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use zksync_types::{
    api::{BlockId, DebugCall, PreResult, TracerConfig, TransactionReceipt},
    transaction_request::CallRequest,
    H256,
};

#[rpc(server, client, namespace = "debug")]
#[async_trait::async_trait]
pub trait DebugApi {
    #[method(name = "traceTransaction")]
    async fn debug_transaction_by_hash(&self, hash: H256) -> RpcResult<Option<DebugCall>>;

    #[method(name = "traceCall")]
    async fn trace_call(
        &self,
        request: CallRequest,
        block: Option<BlockId>,
        options: Option<TracerConfig>,
    ) -> RpcResult<DebugCall>;

    #[method(name = "getLog")]
    async fn trace_get_log(
        &self,
        request: CallRequest,
        block: Option<BlockId>,
    ) -> RpcResult<TransactionReceipt>;

    #[method(name = "traceMany")]
    async fn debug_trace_many(
        &self,
        requests: Vec<CallRequest>,
        block: Option<BlockId>,
    ) -> RpcResult<Vec<PreResult>>;
}
