use std::sync::Arc;

use anyhow::Context as _;
use once_cell::sync::OnceCell;
use zksync_dal::{CoreDal, DalError};
use zksync_multivm::{
    interface::{Call, CallType, ExecutionResult, TxExecutionMode},
    vm_latest::constants::BATCH_COMPUTATIONAL_GAS_LIMIT,
};
use zksync_system_constants::MAX_ENCODED_TX_SIZE;
use zksync_types::{
    api::{
        flat_call, BlockId, BlockNumber, DebugCall, DebugCallType, Log, OpenEthActionTrace,
        PreError, PreResult, ResultDebugCall, TracerConfig, TransactionReceipt,
    },
    debug_flat_call::{flatten_debug_calls, DebugCallFlat},
    fee_model::BatchFeeInput,
    l2::L2Tx,
    transaction_request::CallRequest,
    web3, AccountTreeId, H256, U256,
};
use zksync_web3_decl::error::Web3Error;

use crate::{
    execution_sandbox::{ApiTracer, TxExecutionArgs, TxSetupArgs},
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
        let api_contracts = ApiContracts::load_from_disk().await?;
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

    pub(crate) fn map_call(call: Call, only_top_call: bool) -> DebugCall {
        let calls = if only_top_call {
            vec![]
        } else {
            call.calls
                .into_iter()
                .map(|call| Self::map_call(call, false))
                .collect()
        };
        let debug_type = match call.r#type {
            CallType::Call(_) => DebugCallType::Call,
            CallType::Create => DebugCallType::Create,
            CallType::NearCall => unreachable!("We have to filter our near calls before"),
        };
        DebugCall {
            r#type: debug_type,
            from: call.from,
            to: call.to,
            gas: U256::from(call.gas),
            gas_used: U256::from(call.gas_used),
            value: call.value,
            output: web3::Bytes::from(call.output),
            input: web3::Bytes::from(call.input),
            error: call.error,
            revert_reason: call.revert_reason,
            calls,
        }
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
        if matches!(block_id, BlockId::Number(BlockNumber::Pending)) {
            // See `EthNamespace::get_block_impl()` for an explanation why this check is needed.
            return Ok(vec![]);
        }

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
                let result = Self::map_call(call_trace, only_top_call);
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
        Ok(call_trace.map(|call_trace| Self::map_call(call_trace, only_top_call)))
    }

    pub async fn trace_trace_transaction_impl(
        &self,
        tx_hash: H256,
    ) -> Result<Vec<OpenEthActionTrace>, Web3Error> {
        let mut connection = self.state.acquire_connection().await?;
        let call_trace = connection
            .transactions_dal()
            .get_call_trace(tx_hash)
            .await
            .map_err(DalError::generalize)?;
        let chain_id = self.state.api_config.l2_chain_id;
        let tx = connection
            .transactions_web3_dal()
            .get_transaction_by_hash(tx_hash, chain_id)
            .await
            .map_err(DalError::generalize)?;
        if tx.is_none() {
            return Ok(vec![]);
        }
        if call_trace.is_none() {
            return Ok(vec![]);
        }
        let call_trace = Self::map_call(call_trace.unwrap(), false);
        let tx = tx.unwrap();
        let call_trace_flat = flat_call(
            call_trace,
            tx.transaction_index.unwrap().as_usize(),
            tx_hash,
            tx.block_number.unwrap().as_u64(),
            tx.block_hash.unwrap(),
            &mut Vec::new(),
        );
        Ok(call_trace_flat)
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
        self.current_method().set_block_diff(
            self.state
                .last_sealed_l2_block
                .diff_with_block_args(&block_args),
        );
        if request.gas.is_none() {
            request.gas = Some(block_args.default_eth_call_gas(&mut connection).await?);
        }
        drop(connection);

        let call_overrides = request.get_call_overrides()?;
        let tx = L2Tx::from_request(request.into(), MAX_ENCODED_TX_SIZE)?;

        let setup_args = self.call_args(call_overrides.enforced_base_fee).await;
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

        let connection = self.state.acquire_connection().await?;
        let executor = &self.state.tx_sender.0.executor;
        let result = executor
            .execute_tx_in_sandbox(
                vm_permit,
                setup_args,
                TxExecutionArgs::for_eth_call(tx.clone()),
                connection,
                block_args,
                None,
                custom_tracers,
            )
            .await?
            .vm;

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
        Ok(Self::map_call(call, false))
    }

    async fn call_args(&self, enforced_base_fee: Option<u64>) -> TxSetupArgs {
        let sender_config = self.sender_config();
        TxSetupArgs {
            execution_mode: TxExecutionMode::EthCall,
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
            enforced_base_fee,
        }
    }

    pub async fn debug_trace_get_log_impl(
        &self,
        mut request: CallRequest,
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
        self.current_method().set_block_diff(
            self.state
                .last_sealed_l2_block
                .diff_with_block_args(&block_args),
        );

        if request.gas.is_none() {
            request.gas = Some(block_args.default_eth_call_gas(&mut connection).await?);
        }
        drop(connection);

        let call_overrides = request.get_call_overrides()?;
        let tx = L2Tx::from_request(request.clone().into(), MAX_ENCODED_TX_SIZE)?;

        let setup_args = self.call_args(call_overrides.enforced_base_fee).await;
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

        let connection = self.state.acquire_connection().await?;
        let executor = &self.state.tx_sender.0.executor;
        let result = executor
            .execute_tx_in_sandbox(
                vm_permit,
                setup_args,
                TxExecutionArgs::for_eth_call(tx.clone()),
                connection,
                block_args,
                None,
                custom_tracers,
            )
            .await?;

        let mut logs = vec![];
        let mut transaction_log_index: u32 = 0;

        let transaction_hash = H256::random();

        for log in result.vm.logs.events {
            logs.push(Log {
                l1_batch_number: Some(log.location.0 .0.into()),
                address: log.address,
                topics: log.indexed_topics,
                data: log.value.into(),
                block_hash,
                block_number: Some(block_args.resolved_block_number.0.into()),
                block_timestamp: Some(0.into()),
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
            gas_used: Some(result.vm.statistics.gas_used.into()),
            cumulative_gas_used: result.vm.statistics.gas_used.into(),
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
        mut requests: Vec<CallRequest>,
        block_id: Option<BlockId>,
    ) -> Result<Vec<PreResult>, Web3Error> {
        let block_id = block_id.unwrap_or(BlockId::Number(BlockNumber::Latest));
        self.current_method().set_block_id(block_id);
        let mut connection = self.state.acquire_connection().await?;
        let call_overrides = requests[0].get_call_overrides()?;
        let block_args = self
            .state
            .resolve_block_args(&mut connection, block_id)
            .await?;
        let block_hash = connection
            .blocks_web3_dal()
            .get_l2_block_hash(block_args.resolved_block_number)
            .await
            .map_err(|_| Web3Error::NoBlock)?;
        self.current_method().set_block_diff(
            self.state
                .last_sealed_l2_block
                .diff_with_block_args(&block_args),
        );

        let gas = Some(block_args.default_eth_call_gas(&mut connection).await?);
        for request in requests.iter_mut() {
            if request.gas.is_none() {
                request.gas = gas.clone();
            }
        }
        drop(connection);

        let txs = requests
            .into_iter()
            .map(|request| L2Tx::from_request(request.into(), MAX_ENCODED_TX_SIZE))
            .collect::<Result<Vec<_>, _>>()?;
        let setup_args = self.call_args(call_overrides.enforced_base_fee).await;
        let vm_permit = self
            .state
            .tx_sender
            .vm_concurrency_limiter()
            .acquire()
            .await;
        let vm_permit = vm_permit.context("cannot acquire VM permit")?;

        let connection = self.state.acquire_connection().await?;
        let executor = &self.state.tx_sender.0.executor;
        let results = executor
            .execute_txs_in_sandbox(
                vm_permit,
                setup_args,
                txs.clone()
                    .into_iter()
                    .map(TxExecutionArgs::for_eth_call)
                    .collect(),
                connection,
                block_args,
                None,
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
                    block_timestamp: Some(0.into()),
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
                Self::map_call(call.into(), false),
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
