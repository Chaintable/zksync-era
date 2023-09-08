use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use zksync_types::{api::Transaction, H256, U64};

#[rpc(server, client, namespace = "eth")]
#[async_trait::async_trait]
pub trait EthApi {
    #[method(name = "getTransactionByHash")]
    async fn get_transaction_by_hash(&self, hash: H256) -> RpcResult<Transaction>;

    #[method(name = "blockNumber")]
    async fn get_block_number(&self) -> RpcResult<U64>;
}
