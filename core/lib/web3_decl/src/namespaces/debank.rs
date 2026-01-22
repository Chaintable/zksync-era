#[cfg_attr(not(feature = "server"), allow(unused_imports))]
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use alloy_primitives::U256 as AlloyU256;
use alloy_rpc_types::{state::StateOverride, BlockOverrides};
use zksync_types::{
    debank::DebankBlock,
    transaction_request::{CallRequest, MultiCallResp},
    Address, DebankSimulateResp, H256, U256,
};

use crate::{
    client::{ForWeb3Network, L2},
    types::{Bytes, DebankBlockContext},
};

#[cfg_attr(
    feature = "server",
    rpc(server, client, client_bounds(Self: ForWeb3Network<Net = L2>))
)]
#[cfg_attr(
    not(feature = "server"),
    rpc(client, client_bounds(Self: ForWeb3Network<Net = L2>))
)]
pub trait DebankNamespace {
    #[method(name = "getAddressNonce")]
    async fn get_address_nonce(
        &self,
        address: Address,
        block_context: Option<DebankBlockContext>,
    ) -> RpcResult<U256>;

    #[method(name = "getAddressBalance")]
    async fn get_address_balance(
        &self,
        address: Address,
        block_context: Option<DebankBlockContext>,
    ) -> RpcResult<U256>;

    #[method(name = "getAddressCode")]
    async fn get_address_code(
        &self,
        address: Address,
        block_context: Option<DebankBlockContext>,
    ) -> RpcResult<Bytes>;

    #[method(name = "getStorageAt")]
    async fn get_storage_at(
        &self,
        address: Address,
        position: U256,
        block_context: Option<DebankBlockContext>,
    ) -> RpcResult<H256>;

    #[method(name = "contractMultiCall")]
    async fn contract_multi_call(
        &self,
        requests: Vec<CallRequest>,
        block_ctx: Option<DebankBlockContext>,
        block_overrides: Option<BlockOverrides>,
        state_override: Option<StateOverride>,
        fast_fail: Option<bool>,
        use_parallel: Option<bool>,
        disable_cache: Option<bool>,
    ) -> RpcResult<MultiCallResp>;

    #[method(name = "simulateTransactions")]
    async fn simulate_transactions(
        &self,
        requests: Vec<CallRequest>,
        block_context: Option<DebankBlockContext>,
        block_overrides: Option<BlockOverrides>,
    ) -> RpcResult<DebankSimulateResp>;

    #[method(name = "estimateGas")]
    async fn estimate_gas(
        &self,
        request: CallRequest,
        block_context: Option<DebankBlockContext>,
        block_overrides: Option<BlockOverrides>,
    ) -> RpcResult<U256>;

    #[method(name = "getLatestBlock")]
    async fn get_latest_block(&self) -> RpcResult<DebankBlock>;

    #[method(name = "blockIsValid")]
    async fn block_is_valid(
        &self,
        block_hash: H256,
        block_context: Option<DebankBlockContext>,
    ) -> RpcResult<bool>;

    #[method(name = "getBlockByHeight")]
    async fn get_block_by_height(
        &self,
        block_number: AlloyU256,
    ) -> RpcResult<DebankBlock>;

    #[method(name = "getBlockById")]
    async fn get_block_by_id(
        &self,
        hash: H256,
    ) -> RpcResult<DebankBlock>;

    #[method(name = "version")]
    async fn version(&self, block_context: Option<DebankBlockContext>) -> RpcResult<String>;
}


