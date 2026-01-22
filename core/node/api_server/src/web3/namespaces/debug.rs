use anyhow::Context as _;
use std::collections::{HashMap, HashSet};
use zksync_dal::{CoreDal, DalError};
use zksync_multivm::{
    interface::{Call, CallType, ExecutionResult, OneshotTracingParams},
    tracers::debank,
};
use zksync_system_constants::MAX_ENCODED_TX_SIZE;
use zksync_types::api::{flat_call, Log, OpenEthActionTrace, PreError, PreResult};
use zksync_types::{
    api::state_override::{OverrideAccount, OverrideState, StateOverride},
    StorageLog,
};
use zksync_types::{
    api::{
        BlockId, BlockNumber, CallTracerBlockResult, CallTracerResult, DebugCall, DebugCallType,
        ResultDebugCall, SupportedTracers, TracerConfig,
    },
    debank::{
        BlockFile, DebankBlock, DebankOutPut, DebankSimulateResp, DebankSimulateStats,
        DebankSingleSimulateResult, DebankTransaction, Header,
    },
    debug_flat_call::{Action, CallResult, CallTraceMeta, DebugCallFlat, ResultDebugCallFlat},
    l2::{L2Tx, TransactionType},
    transaction_request::CallRequest,
    web3,
    web3::Bytes,
    zk_evm_types::FarCallOpcode,
    Address, H256, U256,
};
use zksync_web3_decl::error::Web3Error;

use crate::{
    execution_sandbox::SandboxAction,
    web3::{backend_jsonrpsee::MethodTracer, namespaces::validate_gas_cap, state::RpcState},
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
        mut meta: CallTraceMeta,
        tracer_option: TracerConfig,
    ) -> CallTracerResult {
        match tracer_option.tracer {
            SupportedTracers::CallTracer => CallTracerResult::CallTrace(Self::map_default_call(
                call,
                tracer_option.tracer_config.only_top_call,
                meta.internal_error,
            )),
            SupportedTracers::FlatCallTracer => {
                let mut calls = vec![];
                let mut traces = vec![meta.index_in_block];
                if tracer_option.tracer_config.independent_tx_trace {
                    traces = vec![];
                }
                Self::flatten_call(
                    call,
                    &mut calls,
                    &mut traces,
                    tracer_option.tracer_config.only_top_call,
                    &mut meta,
                );
                CallTracerResult::FlatCallTrace(calls)
            }
        }
    }

    pub(crate) fn map_default_call(
        call: Call,
        only_top_call: bool,
        internal_error: Option<String>,
    ) -> DebugCall {
        let calls = if only_top_call {
            vec![]
        } else {
            // We don't need to propagate the internal error to the nested calls.
            call.calls
                .into_iter()
                .map(|call| Self::map_default_call(call, false, None))
                .collect()
        };
        let debug_type = match call.r#type {
            CallType::Call(FarCallOpcode::Normal) => DebugCallType::Call,
            CallType::Call(FarCallOpcode::Mimic) => DebugCallType::Call,
            CallType::Call(FarCallOpcode::Delegate) => DebugCallType::DelegateCall,
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
            error: call.error.or(internal_error),
            revert_reason: call.revert_reason,
            calls,
        }
    }

    fn flatten_call(
        call: Call,
        calls: &mut Vec<DebugCallFlat>,
        trace_address: &mut Vec<usize>,
        only_top_call: bool,
        meta: &mut CallTraceMeta,
    ) {
        let subtraces = call.calls.len();
        let debug_type = match call.r#type {
            CallType::Call(FarCallOpcode::Normal) => DebugCallType::Call,
            CallType::Call(FarCallOpcode::Mimic) => DebugCallType::Call,
            CallType::Call(FarCallOpcode::Delegate) => DebugCallType::DelegateCall,
            CallType::Create => DebugCallType::Create,
            CallType::NearCall => unreachable!("We have to filter our near calls before"),
        };

        // We only want to set the internal error for topmost call, so we take it.
        let internal_error = meta.internal_error.take();

        let (result, error) = match (call.revert_reason, call.error, internal_error) {
            (Some(revert_reason), _, _) => {
                // If revert_reason exists, it takes priority over VM error
                (None, Some(revert_reason))
            }
            (None, Some(vm_error), _) => {
                // If no revert_reason but VM error exists
                (None, Some(vm_error))
            }
            (None, None, Some(internal_error)) => {
                // No VM error, but there is an error in the sequencer DB.
                // Only to be set as a topmost error.
                (None, Some(internal_error))
            }
            (None, None, None) => (
                Some(CallResult {
                    output: web3::Bytes::from(call.output),
                    gas_used: U256::from(call.gas_used),
                }),
                None,
            ),
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
            error,
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
        self.state
            .start_info
            .ensure_not_pruned(block_id, &mut connection)
            .await?;

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
                    .map(|(call, meta)| ResultDebugCall {
                        result: Self::map_default_call(
                            call,
                            options.tracer_config.only_top_call,
                            meta.internal_error,
                        ),
                    })
                    .collect(),
            ),
            SupportedTracers::FlatCallTracer => {
                let res = call_traces
                    .into_iter()
                    .map(|(call, mut meta)| {
                        let mut traces = vec![meta.index_in_block];
                        let mut flat_calls = vec![];
                        Self::flatten_call(
                            call,
                            &mut flat_calls,
                            &mut traces,
                            options.tracer_config.only_top_call,
                            &mut meta,
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
        let CallTracerResult::CallTrace(call_trace) =
            Self::map_call(call, call_meta, TracerConfig::default())
        else {
            todo!()
        };
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
        self.state
            .start_info
            .ensure_not_pruned(block_id, &mut connection)
            .await?;

        let block_args = self
            .state
            .resolve_block_args(&mut connection, block_id)
            .await?;
        self.current_method().set_block_diff(
            self.state
                .last_sealed_l2_block
                .diff_with_block_args(&block_args),
        );

        // Validate user-provided gas against the cap
        validate_gas_cap(
            &request,
            block_id,
            &block_args,
            &mut connection,
            self.state.api_config.eth_call_gas_cap,
            self.current_method(),
        )
        .await?;

        if request.gas.is_none() {
            request.gas = Some(
                block_args
                    .default_eth_call_gas(&mut connection, self.state.api_config.eth_call_gas_cap)
                    .await?,
            );
        }

        let fee_input = if block_args.resolves_to_latest_sealed_l2_block() {
            // It is important to drop a DB connection before calling the provider, since it acquires a connection internally
            // on the main node.
            drop(connection);
            self.state.tx_sender.scaled_batch_fee_input().await?
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
        let call = Call::new_high_level(
            call.common_data.fee.gas_limit.as_u64(),
            result.metrics.vm.gas_used as u64,
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

    pub async fn debug_get_raw_transaction_impl(
        &self,
        hash: H256,
    ) -> Result<Option<Bytes>, Web3Error> {
        let mut connection = self.state.acquire_connection().await?;
        let raw_tx_bytes = connection
            .transactions_web3_dal()
            .get_raw_transaction_bytes(hash)
            .await
            .map_err(DalError::generalize)?;
        Ok(raw_tx_bytes.map(Bytes::from))
    }

    pub async fn debug_get_raw_transactions_impl(
        &self,
        block_id: BlockId,
    ) -> Result<Vec<Bytes>, Web3Error> {
        self.current_method().set_block_id(block_id);
        if matches!(block_id, BlockId::Number(BlockNumber::Pending)) {
            // See `EthNamespace::get_block_impl()` for an explanation why this check is needed.
            return Ok(vec![]);
        }

        let mut connection = self.state.acquire_connection().await?;
        self.state
            .start_info
            .ensure_not_pruned(block_id, &mut connection)
            .await?;

        let block_number = self.state.resolve_block(&mut connection, block_id).await?;
        let raw_txs_bytes = connection
            .transactions_web3_dal()
            .get_l2_block_raw_transactions_bytes(block_number)
            .await
            .map_err(DalError::generalize)?;
        Ok(raw_txs_bytes.into_iter().map(Bytes::from).collect())
    }

    pub fn state_override_from_write_logs(write_logs: &[StorageLog]) -> StateOverride {
        // address -> (slot -> value)
        let mut per_account: HashMap<Address, HashMap<H256, H256>> = HashMap::new();

        for log in write_logs {
            let address = *log.key.address();
            let slot = *log.key.key();
            let value = log.value;

            per_account.entry(address).or_default().insert(slot, value);
        }

        let accounts = per_account
            .into_iter()
            .map(|(addr, slots)| {
                let account = OverrideAccount {
                    state: Some(OverrideState::StateDiff(slots)),
                    ..OverrideAccount::default()
                };
                (addr, account)
            })
            .collect::<HashMap<_, _>>();

        StateOverride::new(accounts)
    }

    fn merge_state_overrides(acc: StateOverride, next: StateOverride) -> StateOverride {
        let mut map: HashMap<Address, OverrideAccount> = acc.into_iter().collect();
        for (addr, mut next_acc) in next.into_iter() {
            let mut merged = map.remove(&addr).unwrap_or_else(OverrideAccount::default);
            if let Some(balance) = next_acc.balance.take() {
                merged.balance = Some(balance);
            }
            if let Some(nonce) = next_acc.nonce.take() {
                merged.nonce = Some(nonce);
            }
            if let Some(code) = next_acc.code.take() {
                merged.code = Some(code);
            }
            match (merged.state.take(), next_acc.state.take()) {
                (Some(OverrideState::StateDiff(mut a)), Some(OverrideState::StateDiff(b))) => {
                    a.extend(b);
                    merged.state = Some(OverrideState::StateDiff(a));
                }
                (Some(OverrideState::State(mut a)), Some(OverrideState::StateDiff(b))) => {
                    a.extend(b);
                    merged.state = Some(OverrideState::State(a));
                }
                (Some(OverrideState::State(_)), Some(OverrideState::State(b))) => {
                    merged.state = Some(OverrideState::State(b));
                }
                (Some(OverrideState::StateDiff(_)), Some(OverrideState::State(b))) => {
                    merged.state = Some(OverrideState::State(b));
                }
                (None, Some(b)) => {
                    merged.state = Some(b);
                }
                (Some(a), None) => {
                    merged.state = Some(a);
                }
                (None, None) => {}
            }

            map.insert(addr, merged);
        }
        StateOverride::new(map)
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
        let gas = Some(
            block_args
                .default_eth_call_gas(&mut connection, self.state.api_config.eth_call_gas_cap)
                .await?,
        );
        for request in requests.iter_mut() {
            if request.gas.is_none() {
                request.gas = gas.clone();
            }
        }
        let txs = requests
            .into_iter()
            .map(|request| L2Tx::from_request(request.into(), MAX_ENCODED_TX_SIZE, true))
            .collect::<Result<Vec<_>, _>>()?;
        let executor = &self.state.tx_sender.0.executor;
        let mut pre_res_vec = Vec::with_capacity(txs.len());
        let mut accumulated_override: Option<StateOverride> = None;
        for (i, tx) in txs.iter().enumerate() {
            let vm_permit = self
                .state
                .tx_sender
                .vm_concurrency_limiter()
                .acquire()
                .await;
            let vm_permit = match vm_permit {
                Some(permit) => permit,
                None => {
                    let pre_error = PreError {
                        msg: format!("cannot acquire VM permit"),
                        code: 1000,
                    };
                    pre_res_vec.push(PreResult {
                        error: pre_error,
                        ..Default::default()
                    });
                    continue;
                }
            };
            let result = executor
                .execute_in_sandbox(
                    vm_permit,
                    self.state.acquire_connection().await?,
                    SandboxAction::Call {
                        call: tx.clone(),
                        fee_input: fee_input.clone(),
                        enforced_base_fee: call_overrides.enforced_base_fee,
                        tracing_params: OneshotTracingParams { trace_calls: true },
                    },
                    &block_args,
                    accumulated_override.clone(),
                )
                .await;

            match result {
                Ok(result) => {
                    let next_override = Self::state_override_from_write_logs(&result.write_logs);
                    accumulated_override = Some(match accumulated_override.take() {
                        None => next_override,
                        Some(acc) => Self::merge_state_overrides(acc, next_override),
                    });
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
                            continue;
                        }
                        ExecutionResult::Halt { reason } => {
                            let pre_error = PreError {
                                msg: reason.to_string(),
                                code: 1000,
                            };
                            pre_res_vec.push(PreResult {
                                error: pre_error,
                                ..Default::default()
                            });
                            continue;
                        }
                    };
                    let call = Call::new_high_level(
                        tx.common_data.fee.gas_limit.as_u64(),
                        result.metrics.vm.gas_used as u64,
                        tx.execute.value,
                        tx.execute.calldata.clone(),
                        output,
                        revert_reason,
                        result.call_traces,
                    );
                    let number = block_args.resolved_block_number();
                    let transaction_hash = H256::random();
                    let meta = CallTraceMeta {
                        block_number: number.0,
                        block_hash: block_hash.unwrap_or_default(),
                        tx_hash: transaction_hash,
                        index_in_block: i,
                        ..Default::default()
                    };

                    let CallTracerResult::CallTrace(call_trace) =
                        Self::map_call(call, meta, TracerConfig::default())
                    else {
                        todo!()
                    };
                    let flat_trace = flat_call(
                        call_trace,
                        i,
                        transaction_hash,
                        number.0 as u64,
                        block_hash.unwrap_or_default(),
                        &mut Vec::new(),
                    );

                    let gas_used = result.metrics.vm.gas_used as u64;
                    let mut logs = vec![];
                    let mut log_index: u32 = 0;
                    for log in result.events {
                        logs.push(Log {
                            l1_batch_number: Some(log.location.0 .0.into()),
                            address: log.address,
                            topics: log.indexed_topics,
                            data: log.value.into(),
                            block_hash,
                            block_timestamp: Some(0.into()),
                            block_number: Some(
                                (block_args.resolved_block_number().0 as u64).into(),
                            ),
                            transaction_hash: Some(transaction_hash),
                            transaction_index: Some(i.into()),
                            log_index: Some(log_index.into()),
                            transaction_log_index: Some(log_index.into()),
                            log_type: None,
                            removed: Some(false),
                        });
                        log_index += 1;
                    }

                    let pre_res = PreResult {
                        trace: flat_trace,
                        logs,
                        gas_used: gas_used.into(),
                        error: Default::default(),
                    };

                    pre_res_vec.push(pre_res);
                }
                Err(e) => {
                    let pre_error = PreError {
                        msg: format!("Sandbox execution error: {e}"),
                        code: 1000,
                    };
                    pre_res_vec.push(PreResult {
                        error: pre_error,
                        ..Default::default()
                    });
                }
            }
        }
        return Ok(pre_res_vec);
    }

    #[allow(dead_code)]
    pub async fn debank_simulate_transactions_impl(
        &self,
        mut requests: Vec<CallRequest>,
        block_id: Option<BlockId>,
    ) -> Result<DebankSimulateResp, Web3Error> {
        let block_id = block_id.unwrap_or(BlockId::Number(BlockNumber::Latest));
        self.current_method().set_block_id(block_id);

        let mut connection = self.state.acquire_connection().await?;
        let call_overrides = requests
            .first()
            .ok_or_else(|| Web3Error::InternalError(anyhow::anyhow!("empty request list")))?
            .get_call_overrides()?;
        let block_args = self
            .state
            .resolve_block_args(&mut connection, block_id)
            .await?;
        let fee_input = if block_args.resolves_to_latest_sealed_l2_block() {
            let scale_factor = self.state.api_config.estimate_gas_scale_factor;
            let fee_input_provider = &self.state.tx_sender.0.batch_fee_input_provider;
            fee_input_provider
                .get_batch_fee_input_scaled(scale_factor, scale_factor)
                .await?
        } else {
            block_args.historical_fee_input(&mut connection).await?
        };
        let block_hash = connection
            .blocks_web3_dal()
            .get_l2_block_hash(block_args.resolved_block_number())
            .await
            .map_err(|_| Web3Error::NoBlock)?;
        let block_time = connection
            .blocks_web3_dal()
            .get_api_block(block_args.resolved_block_number())
            .await
            .map_err(DalError::generalize)?
            .map(|block| block.timestamp.as_u64())
            .unwrap_or_default();
        self.current_method().set_block_diff(
            self.state
                .last_sealed_l2_block
                .diff_with_block_args(&block_args),
        );

        let gas = Some(
            block_args
                .default_eth_call_gas(&mut connection, self.state.api_config.eth_call_gas_cap)
                .await?,
        );
        for request in requests.iter_mut() {
            if request.gas.is_none() {
                request.gas = gas.clone();
            }
        }
        let txs = requests
            .into_iter()
            .map(|request| L2Tx::from_request(request.into(), MAX_ENCODED_TX_SIZE, true))
            .collect::<Result<Vec<_>, _>>()?;

        let executor = &self.state.tx_sender.0.executor;
        let mut results = Vec::with_capacity(txs.len());
        let mut accumulated_override: Option<StateOverride> = None;
        for tx in txs.iter() {
            let vm_permit = self
                .state
                .tx_sender
                .vm_concurrency_limiter()
                .acquire()
                .await;
            let vm_permit = match vm_permit {
                Some(permit) => permit,
                None => {
                    results.push(DebankSingleSimulateResult {
                        code: 1000,
                        err: "cannot acquire VM permit".to_string(),
                        ..Default::default()
                    });
                    continue;
                }
            };

            let result = executor
                .execute_in_sandbox(
                    vm_permit,
                    self.state.acquire_connection().await?,
                    SandboxAction::Call {
                        call: tx.clone(),
                        fee_input: fee_input.clone(),
                        enforced_base_fee: call_overrides.enforced_base_fee,
                        tracing_params: OneshotTracingParams { trace_calls: true },
                    },
                    &block_args,
                    accumulated_override.clone(),
                )
                .await;

            match result {
                Ok(result) => {
                    let gas_used = result.metrics.vm.gas_used as u64;
                    let (output, revert_reason, error) = match result.result {
                        ExecutionResult::Success { output, .. } => (output, None, None),
                        ExecutionResult::Revert { output } => (
                            vec![],
                            Some(output.to_string()),
                            Some((1002, output.to_string())),
                        ),
                        ExecutionResult::Halt { reason } => {
                            (vec![], None, Some((1000, reason.to_string())))
                        }
                    };

                    if let Some((code, err)) = error {
                        results.push(DebankSingleSimulateResult {
                            code,
                            err,
                            gas_used,
                            ..Default::default()
                        });
                        continue;
                    }

                    let next_override = Self::state_override_from_write_logs(&result.write_logs);
                    accumulated_override = Some(match accumulated_override.take() {
                        None => next_override,
                        Some(acc) => Self::merge_state_overrides(acc, next_override),
                    });

                    let call = Call::new_high_level(
                        tx.common_data.fee.gas_limit.as_u64(),
                        gas_used,
                        tx.execute.value,
                        tx.execute.calldata.clone(),
                        output,
                        revert_reason,
                        result.call_traces,
                    );
                    let transaction_hash = H256::random();
                    let mut first_call = call;
                    first_call.trace_id =
                        debank::to_hash(&[transaction_hash.to_string().as_str(), "", "0"]);
                    for (idx, subcall) in first_call.calls.iter_mut().enumerate() {
                        subcall.pos_in_parent_trace = idx as u32;
                    }

                    let mut traces = vec![debank::to_debank_trace(
                        &first_call,
                        transaction_hash,
                        vec![],
                    )];
                    let mut error_traces = Vec::new();
                    let mut events = Vec::new();
                    let mut error_events = Vec::new();
                    debank::add_trace_log(
                        transaction_hash,
                        &mut traces,
                        &mut error_traces,
                        &mut events,
                        &mut error_events,
                        vec![],
                        &mut first_call,
                    );
                    for (log_index, event) in events.iter_mut().enumerate() {
                        event.log_index = log_index as u32;
                    }

                    results.push(DebankSingleSimulateResult {
                        traces,
                        events,
                        gas_used,
                        ..Default::default()
                    });
                }
                Err(e) => {
                    results.push(DebankSingleSimulateResult {
                        code: 1000,
                        err: format!("Sandbox execution error: {e}"),
                        ..Default::default()
                    });
                }
            }
        }

        let success = results.iter().all(|result| result.code == 0);
        Ok(DebankSimulateResp {
            results,
            stats: DebankSimulateStats {
                block_num: block_args.resolved_block_number().0 as u64,
                block_hash: block_hash.unwrap_or_default(),
                block_time,
                success,
            },
        })
    }

    async fn trace_debank_genesis_block_impl(
        &self,
        block_id: BlockId,
    ) -> Result<DebankOutPut, Web3Error> {
        let mut connection = self.state.acquire_connection().await?;
        let block_number = self.state.resolve_block(&mut connection, block_id).await?;
        
        let l2_block = match connection
            .blocks_web3_dal()
            .get_api_block(block_number)
            .await
            .map_err(DalError::generalize)?
        {
            Some(block) => block,
            None => return Err(Web3Error::NoBlock),
        };

        let block_file = BlockFile {
            block: DebankBlock {
                id: l2_block.hash,
                height: l2_block.number.as_u64(),
                parent_id: l2_block.parent_hash,
                base_fee_per_gas: Some(l2_block.base_fee_per_gas.as_u64()),
                gas_limit: l2_block.gas_limit.as_u64(),
                gas_used: l2_block.gas_used.as_u64(),
                timestamp: l2_block.timestamp.as_u64(),
                process_start_timestamp: l2_block.timestamp.as_u64(),
                ..Default::default()
            },
            transactions: vec![],
            events: vec![],
            traces: vec![],
            error_traces: vec![],
            error_events: vec![],
            storage_contracts: vec![],
        };

        let header = Header {
            number: l2_block.number.as_u64(),
            hash: l2_block.hash,
            parent_hash: l2_block.parent_hash,
            nonce: l2_block.nonce,
            mix_hash: l2_block.mix_hash,
            sha3_uncles: l2_block.uncles_hash,
            logs_bloom: l2_block.logs_bloom,
            state_root: l2_block.state_root,
            miner: l2_block.author,
            difficulty: l2_block.difficulty,
            extra_data: l2_block.extra_data,
            gas_limit: l2_block.gas_limit.as_u64(),
            gas_used: l2_block.gas_used.as_u64(),
            timestamp: l2_block.timestamp.as_u64(),
            transactions_root: l2_block.transactions_root,
            receipts_root: l2_block.receipts_root,
            base_fee_per_gas: Some(l2_block.base_fee_per_gas),
            withdrawals_root: None,
            blob_gas_used: None,
            excess_blob_gas: None,
            parent_beacon_block_root: None,
            requests_root: None,
            ..Default::default()
        };

        Ok(DebankOutPut {
            header,
            validation_hash: block_file.validation().validation_hash,
            block_file,
            state_diff: vec![].into(),
        })
    }

    pub async fn trace_debank_block_impl(
        &self,
        block_id: BlockId,
    ) -> Result<DebankOutPut, Web3Error> {
        self.current_method().set_block_id(block_id);
        let mut connection = self.state.acquire_connection().await?;

        self.state
            .start_info
            .ensure_not_pruned(block_id, &mut connection)
            .await?;

        let block_args = self
            .state
            .resolve_block_args(&mut connection, block_id)
            .await?;
        self.current_method().set_block_diff(
            self.state
                .last_sealed_l2_block
                .diff_with_block_args(&block_args),
        );

        let block_number = self.state.resolve_block(&mut connection, block_id).await?;
        
        // Handle genesis block separately
        if block_number.0 == 0 {
            return self.trace_debank_genesis_block_impl(block_id).await;
        }
        
        let parent_block_args = self.state.resolve_block_args(&mut connection, BlockId::Number((block_number - 1).0.into())).await?;

        let l2_block = match connection
            .blocks_web3_dal()
            .get_api_block(block_number)
            .await
            .map_err(DalError::generalize)?
        {
            Some(block) => block,
            None => return Err(Web3Error::NoBlock),
        };

        let raw_transactions = connection
            .transactions_web3_dal()
            .get_raw_l2_block_transactions(block_number)
            .await
            .map_err(DalError::generalize)?;

        let l2_transactions: Vec<L2Tx> = raw_transactions
            .into_iter()
            .filter_map(|tx| match tx.common_data {
                zksync_types::ExecuteTransactionCommon::L2(_) => Some(tx.try_into().unwrap()),
                _ => None,
            })
            .collect();

        // Collect all transaction hashes and fetch receipts in one call
        let tx_hashes: Vec<H256> = l2_transactions.iter().map(|tx| tx.hash()).collect();
        let receipts = connection
            .transactions_web3_dal()
            .get_transaction_receipts(&tx_hashes)
            .await
            .map_err(DalError::generalize)?;
        // Create a map from tx_hash -> (gas_used, status)
        let receipt_map: HashMap<H256, (Option<u64>, bool)> = receipts
            .into_iter()
            .map(|receipt| {
                let tx_hash = receipt.inner.transaction_hash;
                let gas_used = receipt.inner.gas_used.map(|g| g.as_u64());
                let status = receipt.inner.status.as_u64() == 1;
                (tx_hash, (gas_used, status))
            })
            .collect();

        let fee_input = if block_args.resolves_to_latest_sealed_l2_block() {
            // It is important to drop a DB connection before calling the provider, since it acquires a connection internally
            // on the main node.
            drop(connection);
            self.state.tx_sender.scaled_batch_fee_input().await?
        } else {
            let fee_input = block_args.historical_fee_input(&mut connection).await?;
            drop(connection);
            fee_input
        };

        let mut debank_transactions = vec![];
        let mut debank_traces = vec![];
        let mut debank_errtraces = vec![];
        let mut debank_events = vec![];
        let mut debank_errevents = vec![];
        let mut accumulated_override: Option<StateOverride> = None;
        for (idx, l2_tx) in l2_transactions.iter().enumerate() {
            let vm_permit = self
                .state
                .tx_sender
                .vm_concurrency_limiter()
                .acquire()
                .await;
            let vm_permit = vm_permit.context("cannot acquire VM permit")?;

            let tracing_params = OneshotTracingParams { trace_calls: true };
            let result = self
                .state
                .tx_sender
                .0
                .executor
                .execute_in_sandbox(
                    vm_permit,
                    self.state.acquire_connection().await?,
                    SandboxAction::Call {
                        call: l2_tx.clone(),
                        fee_input: fee_input.clone(),
                        enforced_base_fee: None,
                        tracing_params,
                    },
                    &parent_block_args,
                    accumulated_override.clone(),
                )
                .await;

            match result {
                Ok(execution_result) => {
                    let mut tx_events = Vec::new();
                    let next_override = Self::state_override_from_write_logs(&execution_result.write_logs);
                    accumulated_override = Some(match accumulated_override.take() {
                        None => next_override,
                        Some(acc) => Self::merge_state_overrides(acc, next_override),
                    });
                    let (gas_fee_cap, gas_tip_cap) = if l2_tx.common_data.transaction_type as u32 >= TransactionType::EIP1559Transaction as u32 {
                        (
                            l2_tx.common_data.fee.max_fee_per_gas.as_u64(),
                            l2_tx.common_data.fee.max_priority_fee_per_gas.as_u64(),
                        )
                    } else {
                        (0, 0)
                    };
                    // Get gas_used and status from receipt map, with fallback to execution result
                    let tx_hash = l2_tx.hash();
                    let (gas_used, status) = receipt_map
                        .get(&tx_hash)
                        .map(|(receipt_gas_used, receipt_status)| {
                            (
                                receipt_gas_used.unwrap_or_else(|| {
                                    execution_result.metrics.vm.gas_used as u64
                                }),
                                *receipt_status,
                            )
                        })
                        .unwrap_or_else(|| {
                            // Fallback if receipt not found
                            (
                                execution_result.metrics.vm.gas_used as u64,
                                matches!(execution_result.result, ExecutionResult::Success { .. }),
                            )
                        });
                    let debank_transaction = DebankTransaction {
                        id: l2_tx.hash(),
                        from: l2_tx.common_data.initiator_address,
                        to: l2_tx.execute.contract_address,
                        gas_limit: l2_tx.common_data.fee.gas_limit.low_u64(),
                        gas_price: l2_tx
                            .common_data
                            .fee
                            .get_effective_gas_price(l2_block.base_fee_per_gas.into())
                            .low_u64(),
                        gas_used,
                        status,
                        gas_fee_cap,
                        gas_tip_cap,
                        input: l2_tx.execute.calldata.clone().into(),
                        nonce: (*l2_tx.common_data.nonce) as u64,
                        transaction_index: idx as u32,
                        value: l2_tx.execute.value,
                    };
                    debank_transactions.push(debank_transaction);

                    let (output, revert_reason) = match execution_result.result {
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
                        l2_tx.common_data.fee.gas_limit.low_u64(),
                        execution_result.metrics.vm.gas_used as u64,
                        l2_tx.execute.value,
                        l2_tx.execute.calldata.clone(),
                        output,
                        revert_reason,
                        execution_result.call_traces,
                    );

                    let mut first_call = call;
                    first_call.trace_id =
                        debank::to_hash(&[l2_tx.hash().to_string().as_str(), "", "0"]);

                    for (i, subcall) in first_call.calls.iter_mut().enumerate() {
                        subcall.pos_in_parent_trace = i as u32;
                    }
                    debank_traces.push(debank::to_debank_trace(&first_call, l2_tx.hash(), vec![]));

                    debank::add_trace_log(
                        l2_tx.hash(),
                        &mut debank_traces,
                        &mut debank_errtraces,
                        &mut tx_events,
                        &mut debank_errevents,
                        vec![],
                        &mut first_call,
                    );
                    for (log_index, event) in tx_events.iter_mut().enumerate() {
                        event.log_index = log_index as u32;
                    }
                    debank_events.extend(tx_events);
                }
                Err(_) => {
                    // Handle errors during execution
                    // For example, log the error or take appropriate action
                }
            }
        }

        // Collect to_addrs from traces where self_storage_change is true
        let mut storage_contracts: Vec<String> = debank_traces
            .iter()
            .filter(|trace| trace.self_storage_change)
            .map(|trace| {
                if trace.call_type == "delegatecall" {
                    format!("{:?}", trace.from_addr)
                } else {
                    format!("{:?}", trace.to_addr)
                }
            })
            .collect();
        // Deduplicate while preserving first-seen order.
        let mut seen = HashSet::new();
        storage_contracts.retain(|addr| seen.insert(addr.clone()));

        let block_file = BlockFile {
            block: DebankBlock {
                id: l2_block.hash,
                height: l2_block.number.as_u64(),
                parent_id: l2_block.parent_hash,
                base_fee_per_gas: Some(l2_block.base_fee_per_gas.as_u64()),
                gas_limit: l2_block.gas_limit.as_u64(),
                gas_used: l2_block.gas_used.as_u64(),
                timestamp: l2_block.timestamp.as_u64(),
                process_start_timestamp: l2_block.timestamp.as_u64(),
                ..Default::default()
            },
            transactions: debank_transactions,
            events: debank_events,
            traces: debank_traces,
            error_traces: debank_errtraces,
            error_events: debank_errevents,
            storage_contracts,
        };

        let header = Header {
            number: l2_block.number.as_u64(),
            hash: l2_block.hash,
            parent_hash: l2_block.parent_hash,
            nonce: l2_block.nonce,
            mix_hash: l2_block.mix_hash,
            sha3_uncles: l2_block.uncles_hash,
            logs_bloom: l2_block.logs_bloom,
            state_root: l2_block.state_root,
            miner: l2_block.author,
            difficulty: l2_block.difficulty,
            extra_data: l2_block.extra_data,
            gas_limit: l2_block.gas_limit.as_u64(),
            gas_used: l2_block.gas_used.as_u64(),
            timestamp: l2_block.timestamp.as_u64(),
            transactions_root: l2_block.transactions_root,
            receipts_root: l2_block.receipts_root,
            base_fee_per_gas: Some(l2_block.base_fee_per_gas),
            withdrawals_root: None, // Assuming withdrawals_root is not available in l2_block
            blob_gas_used: None,    // Assuming blob_gas_used is not available in l2_block
            excess_blob_gas: None,  // Assuming excess_blob_gas is not available in l2_block
            parent_beacon_block_root: None, // Assuming parent_beacon_block_root is not available in l2_block
            requests_root: None,            // Assuming requests_root is not available in l2_block
            ..Default::default()
        };

        Ok(DebankOutPut {
            header,
            validation_hash: block_file.validation().validation_hash,
            block_file,
            state_diff: vec![].into(),
        })
    }
}
