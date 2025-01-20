use zksync_types::{
    api::{BlockId, BlockNumber, CallTracerBlockResult, CallTracerResult, TracerConfig, TransactionReceipt, PreResult, OpenEthActionTrace},
    transaction_request::CallRequest,
    H256,
};
use zksync_web3_decl::{
    jsonrpsee::core::{async_trait, RpcResult},
    namespaces::{DebugNamespaceServer, PreNamespaceServer, TraceNamespaceServer}
};

use crate::web3::namespaces::DebugNamespace;

#[async_trait]
impl DebugNamespaceServer for DebugNamespace {
    async fn trace_block_by_number(
        &self,
        block: BlockNumber,
        options: Option<TracerConfig>,
    ) -> RpcResult<CallTracerBlockResult> {
        self.debug_trace_block_impl(BlockId::Number(block), options)
            .await
            .map_err(|err| self.current_method().map_err(err))
    }

    async fn trace_block_by_hash(
        &self,
        hash: H256,
        options: Option<TracerConfig>,
    ) -> RpcResult<CallTracerBlockResult> {
        self.debug_trace_block_impl(BlockId::Hash(hash), options)
            .await
            .map_err(|err| self.current_method().map_err(err))
    }

    async fn trace_call(
        &self,
        request: CallRequest,
        block: Option<BlockId>,
        options: Option<TracerConfig>,
    ) -> RpcResult<CallTracerResult> {
        self.debug_trace_call_impl(request, block, options)
            .await
            .map_err(|err| self.current_method().map_err(err))
    }

    async fn trace_transaction(
        &self,
        tx_hash: H256,
        options: Option<TracerConfig>,
    ) -> RpcResult<Option<CallTracerResult>> {
        self.debug_trace_transaction_impl(tx_hash, options)
            .await
            .map_err(|err| self.current_method().map_err(err))
    }

    async fn trace_get_log(
        &self,
        request: CallRequest,
        block: Option<BlockId>,
    ) -> RpcResult<TransactionReceipt> {
        self.debug_trace_get_log_impl(request, block)
            .await
            .map_err(|err| self.current_method().map_err(err))
    }
    async fn debug_trace_many(
        &self,
        requests: Vec<CallRequest>,
        block: Option<BlockId>,
    ) -> RpcResult<Vec<PreResult>> {
        self.debug_pre_trace_many_impl(requests, block)
            .await
            .map_err(|err| self.current_method().map_err(err))
    }
}

#[async_trait]
impl PreNamespaceServer for DebugNamespace {
    async fn pre_trace_many(
        &self,
        requests: Vec<CallRequest>,
        block: Option<BlockId>,
    ) -> RpcResult<Vec<PreResult>> {
        self.debug_pre_trace_many_impl(requests, block)
            .await
            .map_err(|err| self.current_method().map_err(err))
    }
}
#[async_trait]
impl TraceNamespaceServer for DebugNamespace {
    async fn trace_trace_transaction(&self, tx_hash: H256) -> RpcResult<Vec<OpenEthActionTrace>> {
        self.trace_trace_transaction_impl(tx_hash)
            .await
            .map_err(|err| self.current_method().map_err(err))
    }
}