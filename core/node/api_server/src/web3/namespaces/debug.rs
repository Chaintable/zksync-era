use std::sync::Arc;

use anyhow::Context as _;
use multivm::{interface::ExecutionResult, vm_latest::constants::BATCH_COMPUTATIONAL_GAS_LIMIT};
use once_cell::sync::OnceCell;
use zksync_dal::{CoreDal, DalError};
use zksync_system_constants::MAX_ENCODED_TX_SIZE;
use zksync_types::{
    api::{
        flat_call, BlockId, BlockNumber, DebugCall, Log, PreError, PreResult, ResultDebugCall,
        TracerConfig, TransactionReceipt,
    },
    debug_flat_call::{flatten_debug_calls, DebugCallFlat},
    fee_model::BatchFeeInput,
    l2::L2Tx,
    transaction_request::CallRequest,
    vm_trace::Call,
    AccountTreeId, H256,
};
use zksync_web3_decl::error::Web3Error;

use crate::{
    execution_sandbox::{ApiTracer, TxSharedArgs},
    tx_sender::{ApiContracts, TxSenderConfig},
    web3::{backend_jsonrpsee::MethodTracer, state::RpcState},
};

#[derive(Debug, Clone)]
pub(crate) struct DebugNamespace {
    batch_fee_input: BatchFeeInput,
    state: RpcState,
    api_contracts: ApiContracts,
}

impl DebugNamespace {
    pub async fn new(state: RpcState) -> anyhow::Result<Self> {
        let api_contracts = ApiContracts::load_from_disk();
        let fee_input_provider = &state.tx_sender.0.batch_fee_input_provider;
        let batch_fee_input = fee_input_provider
            .get_batch_fee_input_scaled(
                state.api_config.estimate_gas_scale_factor,
                state.api_config.estimate_gas_scale_factor,
            )
            .await
            .context("cannot get batch fee input")?;

        Ok(Self {
            // For now, the same scaling is used for both the L1 gas price and the pubdata price
            batch_fee_input,
            state,
            api_contracts,
        })
    }

    fn sender_config(&self) -> &TxSenderConfig {
        &self.state.tx_sender.0.sender_config
    }

    pub(crate) fn current_method(&self) -> &MethodTracer {
        &self.state.current_method
    }

    pub async fn debug_trace_block_impl(
        &self,
        block_id: BlockId,
        options: Option<TracerConfig>,
    ) -> Result<Vec<ResultDebugCall>, Web3Error> {
        self.current_method().set_block_id(block_id);

        let only_top_call = options
            .map(|options| options.tracer_config.only_top_call)
            .unwrap_or(false);
        let mut connection = self.state.acquire_connection().await?;
        let block_number = self.state.resolve_block(&mut connection, block_id).await?;
        self.current_method()
            .set_block_diff(self.state.last_sealed_l2_block.diff(block_number));

        let call_traces = connection
            .blocks_web3_dal()
            .get_traces_for_l2_block(block_number)
            .await
            .map_err(DalError::generalize)?;
        let call_trace = call_traces
            .into_iter()
            .map(|call_trace| {
                let mut result: DebugCall = call_trace.into();
                if only_top_call {
                    result.calls = vec![];
                }
                ResultDebugCall { result }
            })
            .collect();
        Ok(call_trace)
    }

    pub async fn debug_trace_block_flat_impl(
        &self,
        block_id: BlockId,
        options: Option<TracerConfig>,
    ) -> Result<Vec<DebugCallFlat>, Web3Error> {
        let call_trace = self.debug_trace_block_impl(block_id, options).await?;
        let call_trace_flat = flatten_debug_calls(call_trace);
        Ok(call_trace_flat)
    }

    pub async fn debug_trace_transaction_impl(
        &self,
        tx_hash: H256,
        options: Option<TracerConfig>,
    ) -> Result<Option<DebugCall>, Web3Error> {
        let only_top_call = options
            .map(|options| options.tracer_config.only_top_call)
            .unwrap_or(false);
        let mut connection = self.state.acquire_connection().await?;
        let call_trace = connection
            .transactions_dal()
            .get_call_trace(tx_hash)
            .await
            .map_err(DalError::generalize)?;
        Ok(call_trace.map(|call_trace| {
            let mut result: DebugCall = call_trace.into();
            if only_top_call {
                result.calls = vec![];
            }
            result
        }))
    }

    pub async fn debug_trace_call_impl(
        &self,
        mut request: CallRequest,
        block_id: Option<BlockId>,
        options: Option<TracerConfig>,
    ) -> Result<DebugCall, Web3Error> {
        let block_id = block_id.unwrap_or(BlockId::Number(BlockNumber::Pending));
        self.current_method().set_block_id(block_id);

        let only_top_call = options
            .map(|options| options.tracer_config.only_top_call)
            .unwrap_or(false);

        let mut connection = self.state.acquire_connection().await?;
        let block_args = self
            .state
            .resolve_block_args(&mut connection, block_id)
            .await?;
        drop(connection);

        self.current_method().set_block_diff(
            self.state
                .last_sealed_l2_block
                .diff_with_block_args(&block_args),
        );

        if request.gas.is_none() {
            request.gas = Some(
                self.state
                    .tx_sender
                    .get_default_eth_call_gas(block_args)
                    .await
                    .map_err(Web3Error::InternalError)?
                    .into(),
            )
        }

        let call_overrides = request.get_call_overrides()?;
        let tx = L2Tx::from_request(request.into(), MAX_ENCODED_TX_SIZE)?;

        let shared_args = self.shared_args().await;
        let vm_permit = self
            .state
            .tx_sender
            .vm_concurrency_limiter()
            .acquire()
            .await;
        let vm_permit = vm_permit.context("cannot acquire VM permit")?;

        // We don't need properly trace if we only need top call
        let call_tracer_result = Arc::new(OnceCell::default());
        let custom_tracers = if only_top_call {
            vec![]
        } else {
            vec![ApiTracer::CallTracer(call_tracer_result.clone())]
        };

        let executor = &self.state.tx_sender.0.executor;
        let result = executor
            .execute_tx_eth_call(
                vm_permit,
                shared_args,
                self.state.connection_pool.clone(),
                call_overrides,
                tx.clone(),
                block_args,
                self.sender_config().vm_execution_cache_misses_limit,
                custom_tracers,
            )
            .await?;

        let (output, revert_reason) = match result.result {
            ExecutionResult::Success { output, .. } => (output, None),
            ExecutionResult::Revert { output } => (vec![], Some(output.to_string())),
            ExecutionResult::Halt { reason } => {
                return Err(Web3Error::SubmitTransactionError(
                    reason.to_string(),
                    vec![],
                ))
            }
        };

        // We had only one copy of Arc this arc is already dropped it's safe to unwrap
        let trace = Arc::try_unwrap(call_tracer_result)
            .unwrap()
            .take()
            .unwrap_or_default();
        let call = Call::new_high_level(
            tx.common_data.fee.gas_limit.as_u64(),
            result.statistics.gas_used,
            tx.execute.value,
            tx.execute.calldata,
            output,
            revert_reason,
            trace,
        );
        Ok(call.into())
    }

    async fn shared_args(&self) -> TxSharedArgs {
        let sender_config = self.sender_config();
        TxSharedArgs {
            operator_account: AccountTreeId::default(),
            fee_input: self.batch_fee_input,
            base_system_contracts: self.api_contracts.eth_call.clone(),
            caches: self.state.tx_sender.storage_caches().clone(),
            validation_computational_gas_limit: BATCH_COMPUTATIONAL_GAS_LIMIT,
            chain_id: sender_config.chain_id,
            whitelisted_tokens_for_aa: self
                .state
                .tx_sender
                .read_whitelisted_tokens_for_aa_cache()
                .await,
        }
    }

    pub async fn debug_trace_get_log_impl(
        &self,
        request: CallRequest,
        block_id: Option<BlockId>,
    ) -> Result<TransactionReceipt, Web3Error> {
        let block_id = block_id.unwrap_or(BlockId::Number(BlockNumber::Pending));
        self.current_method().set_block_id(block_id);

        let mut connection = self.state.acquire_connection().await?;
        let block_args = self
            .state
            .resolve_block_args(&mut connection, block_id)
            .await?;
        let block_hash = connection
            .blocks_web3_dal()
            .get_l2_block_hash(block_args.resolved_block_number)
            .await
            .map_err(|_| Web3Error::NoBlock)?;
        drop(connection);

        self.current_method().set_block_diff(
            self.state
                .last_sealed_l2_block
                .diff_with_block_args(&block_args),
        );

        let tx = L2Tx::from_request(request.clone().into(), MAX_ENCODED_TX_SIZE)?;

        let shared_args = self.shared_args().await;
        let vm_permit = self
            .state
            .tx_sender
            .vm_concurrency_limiter()
            .acquire()
            .await;
        let vm_permit = vm_permit.context("cannot acquire VM permit")?;

        // We don't need properly trace if we only need top call
        let call_tracer_result = Arc::new(OnceCell::default());
        let custom_tracers = vec![ApiTracer::CallTracer(call_tracer_result.clone())];

        let executor = &self.state.tx_sender.0.executor;
        let result = executor
            .execute_tx_eth_call(
                vm_permit,
                shared_args,
                self.state.connection_pool.clone(),
                tx.clone(),
                block_args,
                self.sender_config().vm_execution_cache_misses_limit,
                custom_tracers,
            )
            .await?;

        let mut logs = vec![];
        let mut transaction_log_index: u32 = 0;

        let transaction_hash = H256::random();

        for log in result.logs.events {
            logs.push(Log {
                l1_batch_number: Some(log.location.0 .0.into()),
                address: log.address,
                topics: log.indexed_topics,
                data: log.value.into(),
                block_hash,
                block_number: Some(block_args.resolved_block_number.0.into()),
                transaction_hash: Some(transaction_hash),
                transaction_index: Some(Default::default()),
                log_index: Some(transaction_log_index.into()),
                transaction_log_index: Some(transaction_log_index.into()),
                log_type: None,
                removed: Some(false),
            });
            transaction_log_index += 1;
        }

        let from = request.from.clone().unwrap_or_default();
        let to = request.to.clone();

        let receipt = TransactionReceipt {
            transaction_hash,
            transaction_index: Default::default(),
            block_hash: block_hash.unwrap_or_default(),
            block_number: block_args.resolved_block_number.0.into(),
            from,
            to,
            gas_used: Some(result.statistics.gas_used.into()),
            cumulative_gas_used: result.statistics.gas_used.into(),
            contract_address: None,
            logs,
            logs_bloom: Default::default(),
            status: 1.into(),
            root: Default::default(),
            effective_gas_price: Some(0.into()),
            l1_batch_tx_index: Default::default(),
            l1_batch_number: Default::default(),
            l2_to_l1_logs: Default::default(),
            transaction_type: Some(0.into()),
        };

        Ok(receipt)
    }

    pub async fn debug_pre_trace_many_impl(
        &self,
        requests: Vec<CallRequest>,
        block_id: Option<BlockId>,
    ) -> Result<Vec<PreResult>, Web3Error> {
        let block_id = block_id.unwrap_or(BlockId::Number(BlockNumber::Latest));
        self.current_method().set_block_id(block_id);
        let mut connection = self.state.acquire_connection().await?;
        let block_args = self
            .state
            .resolve_block_args(&mut connection, block_id)
            .await?;
        let block_hash = connection
            .blocks_web3_dal()
            .get_l2_block_hash(block_args.resolved_block_number)
            .await
            .map_err(|_| Web3Error::NoBlock)?;
        drop(connection);

        self.current_method().set_block_diff(
            self.state
                .last_sealed_l2_block
                .diff_with_block_args(&block_args),
        );

        let txs = requests
            .into_iter()
            .map(|request| L2Tx::from_request(request.into(), MAX_ENCODED_TX_SIZE))
            .collect::<Result<Vec<_>, _>>()?;
        let shared_args = self.shared_args().await;
        let vm_permit = self
            .state
            .tx_sender
            .vm_concurrency_limiter()
            .acquire()
            .await;
        let vm_permit = vm_permit.context("cannot acquire VM permit")?;

        // We don't need properly trace if we only need top call
        let executor = &self.state.tx_sender.0.executor;
        let results = executor
            .execute_txs_eth_call(
                vm_permit,
                shared_args,
                self.state.connection_pool.clone(),
                txs.clone(),
                block_args,
                self.sender_config().vm_execution_cache_misses_limit,
            )
            .await?;
        let mut pre_res_vec = vec![];
        if results.len() != txs.len() {
            let pre_error = PreError {
                msg: "Results count does not match requests count".to_string(),
                code: 1000,
            };
            let pre_res = PreResult {
                error: pre_error,
                ..Default::default()
            };
            pre_res_vec.push(pre_res);
            return Ok(pre_res_vec);
        }
        let mut tx_index = 0;
        for ((result, calls), tx) in results.into_iter().zip(txs) {
            let (output, revert_reason) = match result.result {
                ExecutionResult::Success { output, .. } => (output, None),
                ExecutionResult::Revert { output } => {
                    let pre_error = PreError {
                        msg: output.to_string(),
                        code: 1002,
                    };
                    let pre_res = PreResult {
                        error: pre_error,
                        ..Default::default()
                    };
                    pre_res_vec.push(pre_res);
                    tx_index += 1;
                    continue;
                }
                ExecutionResult::Halt { reason } => {
                    let pre_error = PreError {
                        msg: reason.to_string(),
                        code: 1000,
                    };
                    let pre_res = PreResult {
                        error: pre_error,
                        ..Default::default()
                    };
                    pre_res_vec.push(pre_res);
                    tx_index += 1;
                    continue;
                }
            };
            let gas_used = result.statistics.gas_used;
            let mut logs = vec![];
            let mut transaction_log_index: u32 = 0;
            let transaction_hash = H256::random();
            for log in result.logs.events {
                logs.push(Log {
                    l1_batch_number: Some(log.location.0 .0.into()),
                    address: log.address,
                    topics: log.indexed_topics,
                    data: log.value.into(),
                    block_hash,
                    block_number: Some(block_args.resolved_block_number.0.into()),
                    transaction_hash: Some(transaction_hash),
                    transaction_index: Some(tx_index.into()),
                    log_index: Some(transaction_log_index.into()),
                    transaction_log_index: Some(transaction_log_index.into()),
                    log_type: None,
                    removed: Some(false),
                });
                transaction_log_index += 1;
            }
            let call = Call::new_high_level(
                tx.common_data.fee.gas_limit.as_u64(),
                result.statistics.gas_used,
                tx.execute.value,
                tx.execute.calldata,
                output,
                revert_reason,
                calls,
            );
            let res = flat_call(
                call.into(),
                tx_index,
                transaction_hash,
                block_args.resolved_block_number.0.into(),
                block_hash.unwrap_or_default(),
                &mut Vec::new(),
            );
            let pre_res = PreResult {
                trace: res,
                logs,
                gas_used: gas_used.into(),
                error: Default::default(),
            };
            tx_index += 1;
            pre_res_vec.push(pre_res);
        }
        Ok(pre_res_vec)
    }
}
