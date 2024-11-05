use anyhow::Context as _;
use zksync_dal::{CoreDal, DalError};
use zksync_multivm::interface::{Call, CallType, ExecutionResult, OneshotTracingParams, TxExecutionArgs};
use zksync_system_constants::MAX_ENCODED_TX_SIZE;
use zksync_types::{api::{
    flat_call, BlockId, BlockNumber, CallTracerBlockResult, CallTracerResult, DebugCall, DebugCallType, Log,
    ResultDebugCall, SupportedTracers, TracerConfig, OpenEthActionTrace, PreError, PreResult, TransactionReceipt,
}, debug_flat_call::{Action, CallResult, CallTraceMeta, DebugCallFlat, ResultDebugCallFlat}, l2::L2Tx, transaction_request::CallRequest, web3, H256, U256, U64};
use zksync_web3_decl::error::Web3Error;

use crate::{
    execution_sandbox::{SandboxAction},
    web3::{backend_jsonrpsee::MethodTracer, state::RpcState},
};

#[derive(Debug, Clone)]
pub(crate) struct DebugNamespace {
    state: RpcState,
}

impl DebugNamespace {
    pub async fn new(state: RpcState) -> anyhow::Result<Self> {
        Ok(Self { state })
    }

    pub(crate) fn map_call(
        call: Call,
        meta: CallTraceMeta,
        tracer_option: TracerConfig,
    ) -> CallTracerResult {
        match tracer_option.tracer {
            SupportedTracers::CallTracer => CallTracerResult::CallTrace(Self::map_default_call(
                call,
                tracer_option.tracer_config.only_top_call,
            )),
            SupportedTracers::FlatCallTracer => {
                let mut calls = vec![];
                let mut traces = vec![meta.index_in_block];
                Self::flatten_call(
                    call,
                    &mut calls,
                    &mut traces,
                    tracer_option.tracer_config.only_top_call,
                    &meta,
                );
                CallTracerResult::FlatCallTrace(calls)
            }
        }
    }
    pub(crate) fn map_default_call(call: Call, only_top_call: bool) -> DebugCall {
        let calls = if only_top_call {
            vec![]
        } else {
            call.calls
                .into_iter()
                .map(|call| Self::map_default_call(call, false))
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

    fn flatten_call(
        call: Call,
        calls: &mut Vec<DebugCallFlat>,
        trace_address: &mut Vec<usize>,
        only_top_call: bool,
        meta: &CallTraceMeta,
    ) {
        let subtraces = call.calls.len();
        let debug_type = match call.r#type {
            CallType::Call(_) => DebugCallType::Call,
            CallType::Create => DebugCallType::Create,
            CallType::NearCall => unreachable!("We have to filter our near calls before"),
        };

        let result = if call.error.is_none() {
            Some(CallResult {
                output: web3::Bytes::from(call.output),
                gas_used: U256::from(call.gas_used),
            })
        } else {
            None
        };

        calls.push(DebugCallFlat {
            action: Action {
                call_type: debug_type,
                from: call.from,
                to: call.to,
                gas: U256::from(call.gas),
                value: call.value,
                input: web3::Bytes::from(call.input),
            },
            result,
            subtraces,
            trace_address: trace_address.clone(), // Clone the current trace address
            transaction_position: meta.index_in_block,
            transaction_hash: meta.tx_hash,
            block_number: meta.block_number,
            block_hash: meta.block_hash,
            r#type: DebugCallType::Call,
        });

        if !only_top_call {
            for (number, call) in call.calls.into_iter().enumerate() {
                trace_address.push(number);
                Self::flatten_call(call, calls, trace_address, false, meta);
                trace_address.pop();
            }
        }
    }

    pub(crate) fn current_method(&self) -> &MethodTracer {
        &self.state.current_method
    }

    pub async fn debug_trace_block_impl(
        &self,
        block_id: BlockId,
        options: Option<TracerConfig>,
    ) -> Result<CallTracerBlockResult, Web3Error> {
        self.current_method().set_block_id(block_id);
        if matches!(block_id, BlockId::Number(BlockNumber::Pending)) {
            // See `EthNamespace::get_block_impl()` for an explanation why this check is needed.
            return Ok(CallTracerBlockResult::CallTrace(vec![]));
        }

        let mut connection = self.state.acquire_connection().await?;
        let block_number = self.state.resolve_block(&mut connection, block_id).await?;
        // let block_hash = block_hash self.state.
        self.current_method()
            .set_block_diff(self.state.last_sealed_l2_block.diff(block_number));

        let call_traces = connection
            .blocks_web3_dal()
            .get_traces_for_l2_block(block_number)
            .await
            .map_err(DalError::generalize)?;

        let options = options.unwrap_or_default();
        let result = match options.tracer {
            SupportedTracers::CallTracer => CallTracerBlockResult::CallTrace(
                call_traces
                    .into_iter()
                    .map(|(call, _)| ResultDebugCall {
                        result: Self::map_default_call(call, options.tracer_config.only_top_call),
                    })
                    .collect(),
            ),
            SupportedTracers::FlatCallTracer => {
                let res = call_traces
                    .into_iter()
                    .map(|(call, meta)| {
                        let mut traces = vec![meta.index_in_block];
                        let mut flat_calls = vec![];
                        Self::flatten_call(
                            call,
                            &mut flat_calls,
                            &mut traces,
                            options.tracer_config.only_top_call,
                            &meta,
                        );
                        ResultDebugCallFlat {
                            tx_hash: meta.tx_hash,
                            result: flat_calls,
                        }
                    })
                    .collect();
                CallTracerBlockResult::FlatCallTrace(res)
            }
        };
        Ok(result)
    }

    pub async fn debug_trace_transaction_impl(
        &self,
        tx_hash: H256,
        options: Option<TracerConfig>,
    ) -> Result<Option<CallTracerResult>, Web3Error> {
        let mut connection = self.state.acquire_connection().await?;
        let call_trace = connection
            .transactions_dal()
            .get_call_trace(tx_hash)
            .await
            .map_err(DalError::generalize)?;
        Ok(call_trace.map(|(call_trace, meta)| {
            Self::map_call(call_trace, meta, options.unwrap_or_default())
        }))
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
        let (call, call_meta) = call_trace.unwrap();
        let tx = tx.unwrap();
        let CallTracerResult::CallTrace(call_trace) = Self::map_call(call, call_meta, TracerConfig::default()) else { todo!() };
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
    ) -> Result<CallTracerResult, Web3Error> {
        let block_id = block_id.unwrap_or(BlockId::Number(BlockNumber::Pending));
        self.current_method().set_block_id(block_id);

        let options = options.unwrap_or_default();

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

        let fee_input = if block_args.resolves_to_latest_sealed_l2_block() {
            // It is important to drop a DB connection before calling the provider, since it acquires a connection internally
            // on the main node.
            drop(connection);
            let scale_factor = self.state.api_config.estimate_gas_scale_factor;
            let fee_input_provider = &self.state.tx_sender.0.batch_fee_input_provider;
            // For now, the same scaling is used for both the L1 gas price and the pubdata price
            fee_input_provider
                .get_batch_fee_input_scaled(scale_factor, scale_factor)
                .await?
        } else {
            let fee_input = block_args.historical_fee_input(&mut connection).await?;
            drop(connection);
            fee_input
        };

        let call_overrides = request.get_call_overrides()?;
        let call = L2Tx::from_request(
            request.into(),
            MAX_ENCODED_TX_SIZE,
            block_args.use_evm_emulator(),
        )?;

        let vm_permit = self
            .state
            .tx_sender
            .vm_concurrency_limiter()
            .acquire()
            .await;
        let vm_permit = vm_permit.context("cannot acquire VM permit")?;

        // We don't need properly trace if we only need top call
        let tracing_params = OneshotTracingParams {
            trace_calls: !options.tracer_config.only_top_call,
        };

        let connection = self.state.acquire_connection().await?;
        let executor = &self.state.tx_sender.0.executor;
        let result = executor
            .execute_in_sandbox(
                vm_permit,
                connection,
                SandboxAction::Call {
                    call: call.clone(),
                    fee_input,
                    enforced_base_fee: call_overrides.enforced_base_fee,
                    tracing_params,
                },
                &block_args,
                None,
            )
            .await?;

        let (output, revert_reason) = match result.vm.result {
            ExecutionResult::Success { output, .. } => (output, None),
            ExecutionResult::Revert { output } => (vec![], Some(output.to_string())),
            ExecutionResult::Halt { reason } => {
                return Err(Web3Error::SubmitTransactionError(
                    reason.to_string(),
                    vec![],
                ))
            }
        };
        let call = Call::new_high_level(
            call.common_data.fee.gas_limit.as_u64(),
            result.vm.statistics.gas_used,
            call.execute.value,
            call.execute.calldata,
            output,
            revert_reason,
            result.call_traces,
        );
        let number = block_args.resolved_block_number();
        let meta = CallTraceMeta {
            block_number: number.0,
            // It's a call request, it's safe to everything as default
            ..Default::default()
        };
        Ok(Self::map_call(call, meta, options))
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
            .get_l2_block_hash(block_args.resolved.state_l2_block_number())
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
        let fee_input = if block_args.resolves_to_latest_sealed_l2_block() {
            // It is important to drop a DB connection before calling the provider, since it acquires a connection internally
            // on the main node.
            let scale_factor = self.state.api_config.estimate_gas_scale_factor;
            let fee_input_provider = &self.state.tx_sender.0.batch_fee_input_provider;
            // For now, the same scaling is used for both the L1 gas price and the pubdata price
            fee_input_provider
                .get_batch_fee_input_scaled(scale_factor, scale_factor)
                .await?
        } else {
            let fee_input = block_args.historical_fee_input(&mut connection).await?;
            fee_input
        };
        let call_overrides = request.get_call_overrides()?;
        let tx = L2Tx::from_request(request.clone().into(), MAX_ENCODED_TX_SIZE, false)?;
        let vm_permit = self
            .state
            .tx_sender
            .vm_concurrency_limiter()
            .acquire()
            .await;
        let vm_permit = vm_permit.context("cannot acquire VM permit")?;
        // We don't need properly trace if we only need top call
        // let call_tracer_result = Arc::new(OnceCell::default());
        // let custom_tracers = vec![CallTracer::new(call_tracer_result.clone())];
        // let connection = self.state.acquire_connection().await?;
        let executor = &self.state.tx_sender.0.executor;
        let action = SandboxAction::Call {
            call: tx,
            fee_input,
            enforced_base_fee: call_overrides.enforced_base_fee,
            tracing_params: OneshotTracingParams::default(),
        };
        let result = executor
            .execute_in_sandbox(
                vm_permit,
                connection,
                action,
                &block_args,
                None,
            )
            .await?;
        let mut logs = vec![];
        let mut transaction_log_index: u32 = 0;
        let block_number = block_args.resolved.state_l2_block_number().0;
        let transaction_hash = H256::random();
        for log in result.vm.logs.events {
            logs.push(Log {
                l1_batch_number: Some(log.location.0.0.into()),
                address: log.address,
                topics: log.indexed_topics,
                data: log.value.into(),
                block_hash,
                block_number: Some(U64::from(block_number.clone() as u64)),
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
            block_number: U64::from(block_number.clone() as u64),
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
        let fee_input = if block_args.resolves_to_latest_sealed_l2_block() {
            // It is important to drop a DB connection before calling the provider, since it acquires a connection internally
            // on the main node.
            let scale_factor = self.state.api_config.estimate_gas_scale_factor;
            let fee_input_provider = &self.state.tx_sender.0.batch_fee_input_provider;
            // For now, the same scaling is used for both the L1 gas price and the pubdata price
            fee_input_provider
                .get_batch_fee_input_scaled(scale_factor, scale_factor)
                .await?
        } else {
            let fee_input = block_args.historical_fee_input(&mut connection).await?;
            fee_input
        };

        let block_hash = connection
            .blocks_web3_dal()
            .get_l2_block_hash(block_args.resolved_block_number())
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
        let txs = requests
            .into_iter()
            .map(|request| L2Tx::from_request(request.into(), MAX_ENCODED_TX_SIZE, false))
            .collect::<Result<Vec<_>, _>>()?;
        let vm_permit = self
            .state
            .tx_sender
            .vm_concurrency_limiter()
            .acquire()
            .await;
        let vm_permit = vm_permit.context("cannot acquire VM permit")?;
        let executor = &self.state.tx_sender.0.executor;
        let action = SandboxAction::Call {
            call: txs[0].clone(),
            fee_input: fee_input.clone(),
            enforced_base_fee: call_overrides.enforced_base_fee,
            tracing_params: OneshotTracingParams::default(),
        };
        let results = executor
            .execute_txs_in_sandbox(
                vm_permit,
                txs.clone()
                    .into_iter()
                    .map(TxExecutionArgs::for_eth_call)
                    .collect(),
                connection,
                action,
                block_args.clone(),
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
                    l1_batch_number: Some(log.location.0.0.into()),
                    address: log.address,
                    topics: log.indexed_topics,
                    data: log.value.into(),
                    block_hash,
                    block_timestamp: Some(0.into()),
                    block_number: Some((block_args.resolved_block_number().0 as u64).into()),
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
            let call_meta = CallTraceMeta {
                index_in_block: tx_index,
                tx_hash: transaction_hash,
                block_number: block_args.resolved_block_number().0,
                block_hash: block_hash.unwrap_or_default(),
            };
            let CallTracerResult::CallTrace(call_trace) = Self::map_call(call, call_meta, TracerConfig::default()) else { todo!() };
            let res = flat_call(
                call_trace,
                tx_index,
                transaction_hash,
                block_args.resolved.state_l2_block_number().0 as u64,
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
