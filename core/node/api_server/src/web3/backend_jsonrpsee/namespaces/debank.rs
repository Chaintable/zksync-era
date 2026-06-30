use alloy_primitives::U256 as AlloyU256;
use anyhow::anyhow;
use alloy_rpc_types::{state::StateOverride, BlockOverrides};
use zksync_types::api::{BlockId, BlockIdVariant, BlockNumber};
use zksync_types::transaction_request::{CallRequest, MultiCallResp};
use zksync_types::{
    debank::{DebankBlock, DebankSimulateResp},
    Address, H256, U256,
};
use zksync_web3_decl::{
    error::Web3Error,
    jsonrpsee::core::{async_trait, RpcResult},
    jsonrpsee::types::ErrorObjectOwned,
    namespaces::DebankNamespaceServer,
    types::{Bytes, DebankBlockContext},
};

use crate::web3::{
    metrics::{
        leafage_rpc_summary_enabled, record_leafage_rpc_summary, LeafageStatusLabels,
        MethodNameLabel, LEAFAGE_RPC_COMMON_METRICS, LEAFAGE_RPC_HISTOGRAM_METRICS,
    },
    namespaces::DebankNamespace,
};

fn resolve_block_id_variant_from_ctx(
    block_context: Option<DebankBlockContext>,
) -> Option<BlockIdVariant> {
    block_context.map(|ctx| ctx.block_id)
}


fn resolve_block_id_from_ctx(block_context: Option<DebankBlockContext>) -> Option<BlockId> {
    block_context.map(|ctx| ctx.block_id.into())
}

fn observe_leafage_call<T>(
    method_name: &'static str,
    started_at: std::time::Instant,
    result: Result<T, ErrorObjectOwned>,
) -> RpcResult<T> {
    let elapsed = started_at.elapsed();
    LEAFAGE_RPC_COMMON_METRICS.time_count[&MethodNameLabel { method_name }].inc();
    if leafage_rpc_summary_enabled() {
        record_leafage_rpc_summary(method_name, elapsed);
    } else {
        LEAFAGE_RPC_HISTOGRAM_METRICS.time[&MethodNameLabel { method_name }].observe(elapsed);
    }
    let return_code = result
        .as_ref()
        .err()
        .map(|err| err.code())
        .unwrap_or(0);
    LEAFAGE_RPC_COMMON_METRICS.status[&LeafageStatusLabels {
        method_name,
        return_code,
    }]
    .inc();
    result
}

#[async_trait]
impl DebankNamespaceServer for DebankNamespace {
    async fn get_address_nonce(
        &self,
        address: Address,
        block_context: Option<DebankBlockContext>,
    ) -> RpcResult<U256> {
        let block = resolve_block_id_variant_from_ctx(block_context);
        let started_at = std::time::Instant::now();
        let result = self
            .eth
            .get_transaction_count_impl(address, block.map(Into::into))
            .await
            .map_err(|err| self.eth.current_method().map_err(err));
        observe_leafage_call("getAddressNonce", started_at, result)
    }

    async fn get_address_balance(
        &self,
        address: Address,
        block_context: Option<DebankBlockContext>,
    ) -> RpcResult<U256> {
        let block = resolve_block_id_variant_from_ctx(block_context);
        let started_at = std::time::Instant::now();
        let result = self
            .eth
            .get_balance_impl(address, block.map(Into::into))
            .await
            .map_err(|err| self.eth.current_method().map_err(err));
        observe_leafage_call("getAddressBalance", started_at, result)
    }

    async fn get_address_code(
        &self,
        address: Address,
        block_context: Option<DebankBlockContext>,
    ) -> RpcResult<Bytes> {
        let block = resolve_block_id_variant_from_ctx(block_context);
        let started_at = std::time::Instant::now();
        let result = self
            .eth
            .get_code_impl(address, block.map(Into::into))
            .await
            .map_err(|err| self.eth.current_method().map_err(err));
        observe_leafage_call("getAddressCode", started_at, result)
    }

    async fn get_storage_at(
        &self,
        address: Address,
        position: U256,
        block_context: Option<DebankBlockContext>,
    ) -> RpcResult<H256> {
        let block = resolve_block_id_variant_from_ctx(block_context);
        let started_at = std::time::Instant::now();
        let result = self
            .eth
            .get_storage_at_impl(address, position, block.map(Into::into))
            .await
            .map_err(|err| self.eth.current_method().map_err(err));
        observe_leafage_call("getStorageAt", started_at, result)
    }

    async fn contract_multi_call(
        &self,
        requests: Vec<CallRequest>,
        block_ctx: Option<DebankBlockContext>,
        _block_overrides: Option<BlockOverrides>,
        _state_override: Option<StateOverride>,
        fast_fail: Option<bool>,
        use_parallel: Option<bool>,
        disable_cache: Option<bool>,
    ) -> RpcResult<MultiCallResp> {
        let block = resolve_block_id_variant_from_ctx(block_ctx);
        let fast_fail = fast_fail.unwrap_or(true);
        let use_parallel = use_parallel.unwrap_or(true);
        let disable_cache = disable_cache.unwrap_or(false);
        let started_at = std::time::Instant::now();
        let result = self
            .eth
            .multi_call_impl(
                requests,
                block.map(Into::into),
                fast_fail,
                use_parallel,
                disable_cache,
            )
            .await
            .map_err(|err| self.eth.current_method().map_err(err));
        observe_leafage_call("contractMultiCall", started_at, result)
    }

    async fn simulate_transactions(
        &self,
        requests: Vec<CallRequest>,
        block_context: Option<DebankBlockContext>,
        _block_overrides: Option<BlockOverrides>,
    ) -> RpcResult<DebankSimulateResp> {
        let block = resolve_block_id_from_ctx(block_context);
        let started_at = std::time::Instant::now();
        let result = self
            .debug
            .debank_simulate_transactions_impl(requests, block)
            .await
            .map_err(|err| self.debug.current_method().map_err(err));
        observe_leafage_call("simulateTransactions", started_at, result)
    }

    async fn estimate_gas(
        &self,
        request: CallRequest,
        _block_context: Option<DebankBlockContext>,
        _block_overrides: Option<BlockOverrides>,
    ) -> RpcResult<U256> {
        let started_at = std::time::Instant::now();
        let result = self
            .eth
            .estimate_gas_impl(request, None, None)
            .await
            .map_err(|err| self.eth.current_method().map_err(err));
        observe_leafage_call("estimateGas", started_at, result)
    }

    async fn get_latest_block(
        &self,
    ) -> RpcResult<DebankBlock> {
        let started_at = std::time::Instant::now();
        let result = self
            .eth
            .get_block_impl(BlockId::Number(BlockNumber::Latest), true)
            .await
            .and_then(|block| block.ok_or(Web3Error::NoBlock))
            .map(|block| DebankBlock {
                id: block.hash,
                height: block.number.as_u64(),
                parent_id: block.parent_hash,
                base_fee_per_gas: Some(block.base_fee_per_gas.as_u64()),
                miner: block.author,
                gas_limit: block.gas_limit.as_u64(),
                gas_used: block.gas_used.as_u64(),
                timestamp: block.timestamp.as_u64(),
                process_start_timestamp: block.timestamp.as_u64(),
                ..Default::default()
            })
            .map_err(|err| self.eth.current_method().map_err(err));
        observe_leafage_call("getLatestBlock", started_at, result)
    }

    async fn block_is_valid(
        &self,
        _block_hash: H256,
        _block_context: Option<DebankBlockContext>,
    ) -> RpcResult<bool> {
        let started_at = std::time::Instant::now();
        observe_leafage_call("blockIsValid", started_at, Ok(true))
    }

    async fn get_block_by_height(
        &self,
        block_number: AlloyU256,
    ) -> RpcResult<DebankBlock> {
        let block_number = match u64::try_from(block_number) {
            Ok(number) => BlockNumber::from(number),
            Err(_) => {
                return Err(
                    self.eth
                        .current_method()
                        .map_err(Web3Error::InternalError(anyhow!("block_number too large"))),
                );
            }
        };
        let started_at = std::time::Instant::now();
        let result = self
            .eth
            .get_block_impl(BlockId::Number(block_number), true)
            .await
            .and_then(|block| block.ok_or(Web3Error::NoBlock))
            .map(|block| DebankBlock {
                id: block.hash,
                height: block.number.as_u64(),
                parent_id: block.parent_hash,
                base_fee_per_gas: Some(block.base_fee_per_gas.as_u64()),
                miner: block.author,
                gas_limit: block.gas_limit.as_u64(),
                gas_used: block.gas_used.as_u64(),
                timestamp: block.timestamp.as_u64(),
                process_start_timestamp: block.timestamp.as_u64(),
                ..Default::default()
            })
            .map_err(|err| self.eth.current_method().map_err(err));
        observe_leafage_call("getBlockByHeight", started_at, result)
    }

    async fn get_block_by_id(
        &self,
        hash: H256,
    ) -> RpcResult<DebankBlock> {
        let started_at = std::time::Instant::now();
        let result = self
            .eth
            .get_block_impl(BlockId::Hash(hash), true)
            .await
            .and_then(|block| block.ok_or(Web3Error::NoBlock))
            .map(|block| DebankBlock {
                id: block.hash,
                height: block.number.as_u64(),
                parent_id: block.parent_hash,
                base_fee_per_gas: Some(block.base_fee_per_gas.as_u64()),
                miner: block.author,
                gas_limit: block.gas_limit.as_u64(),
                gas_used: block.gas_used.as_u64(),
                timestamp: block.timestamp.as_u64(),
                process_start_timestamp: block.timestamp.as_u64(),
                ..Default::default()
            })
            .map_err(|err| self.eth.current_method().map_err(err));
        observe_leafage_call("getBlockById", started_at, result)
    }

    async fn version(&self, _block_context: Option<DebankBlockContext>) -> RpcResult<String> {
        let started_at = std::time::Instant::now();
        observe_leafage_call("version", started_at, Ok(self.eth.etcd_register_version().to_owned()))
    }
}
