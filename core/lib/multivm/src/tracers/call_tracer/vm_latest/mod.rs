use zk_evm_1_5_2::{
    aux_structures::{LogQuery, Timestamp},
    reference_impls::event_sink::EventMessage,
};
use zk_evm_1_5_2::{
    tracing::{AfterExecutionData, BeforeExecutionData, VmLocalStateData},
    zkevm_opcode_defs::{
        FarCallABI, FatPointer, LogOpcode, Opcode, RetOpcode,
        CALL_IMPLICIT_CALLDATA_FAT_PTR_REGISTER, RET_IMPLICIT_RETURNDATA_PARAMS_REGISTER,
    },
};
use zksync_system_constants::CONTRACT_DEPLOYER_ADDRESS;
use zksync_types::{zk_evm_types::FarCallOpcode, L1BatchNumber, U256};
use zksync_vm_interface::{tracer::TracerExecutionStatus, CallEventMetadata};

use crate::{
    glue::GlueInto,
    interface::{
        storage::{StoragePtr, WriteStorage},
        tracer::VmExecutionStopReason,
        Call, CallType, VmRevertReason,
    },
    tracers::{dynamic::vm_1_5_2::DynTracer, CallTracer},
    vm_latest::{
        old_vm::events, BootloaderState, HistoryMode, SimpleMemory, VmTracer, ZkSyncVmState,
    },
};

#[derive(Debug, Clone, Default)]
pub(super) struct CallEvents {
    fragments: Vec<(LogQuery, bool)>,
    near_calls: Vec<(usize, usize)>,
}

impl CallEvents {
    fn into_events(self, call: &mut Call, batch_number: L1BatchNumber) {
        let fragments = self.fragments.into_iter().map(|(query, reverted)| {
            (
                EventMessage {
                    shard_id: query.shard_id,
                    is_first: query.is_service,
                    tx_number_in_block: query.tx_number_in_block,
                    address: query.address,
                    key: query.key,
                    value: query.written_value,
                },
                query.timestamp.0,
                reverted,
            )
        });
        for event in events::merge_events_with_metadata(fragments) {
            call.event_metadata.push(CallEventMetadata {
                timestamp: event.timestamp,
                reverted: event.reverted,
            });
            let mut event = event.into_vm_event(batch_number);
            event.position = (call.calls.len() + call.events.len()) as u32;
            call.events.push(event);
        }
    }
}

impl<S, H: HistoryMode> DynTracer<S, SimpleMemory<H>> for CallTracer {
    fn before_execution(
        &mut self,
        state: VmLocalStateData<'_>,
        data: BeforeExecutionData,
        _memory: &SimpleMemory<H>,
        _storage: StoragePtr<S>,
    ) {
        self.pending_event_timestamp =
            matches!(data.opcode.variant.opcode, Opcode::Log(LogOpcode::Event))
                .then_some(state.vm_local_state.timestamp + 1);
    }

    fn after_execution(
        &mut self,
        state: VmLocalStateData<'_>,
        data: AfterExecutionData,
        memory: &SimpleMemory<H>,
        _storage: StoragePtr<S>,
    ) {
        match data.opcode.variant.opcode {
            Opcode::NearCall(_) => {
                self.increase_near_call_count();
                if let Some(call) = self.stack.last_mut() {
                    call.latest_events
                        .near_calls
                        .push((call.latest_events.fragments.len(), call.farcall.calls.len()));
                }
            }
            Opcode::FarCall(far_call) => {
                // We use parent gas for properly calculating gas used in the trace.
                let current_ergs = state.vm_local_state.callstack.current.ergs_remaining;
                let parent_gas = state
                    .vm_local_state
                    .callstack
                    .inner
                    .last()
                    .map(|call| call.ergs_remaining + current_ergs)
                    .unwrap_or(current_ergs) as u64;

                let mut current_call = Call {
                    r#type: CallType::Call(far_call.glue_into()),
                    gas: 0,
                    parent_gas,
                    call_start_timestamp: state.vm_local_state.timestamp,
                    ..Default::default()
                };

                self.handle_far_call_op_code_latest(state, memory, &mut current_call);
                self.push_call_and_update_stats(current_call, 0);
            }
            Opcode::Log(log_opcode) => {
                match log_opcode {
                    LogOpcode::StorageWrite | LogOpcode::TransientStorageWrite => {
                        // We need to set the storage change here, because the log opcode
                        // is not a call and we need to set the storage change for the current call
                        if let Some(mut current_call) = self.stack.pop() {
                            current_call.farcall.storage_change = true;
                            current_call.farcall.self_storage_change = true;
                            self.stack.push(current_call);
                        };
                    }
                    _ => (),
                }
            }
            Opcode::Ret(ret_code) => {
                self.handle_ret_op_code_latest(state, memory, ret_code);
            }
            _ => {}
        };
    }
}

impl<S: WriteStorage, H: HistoryMode> VmTracer<S, H> for CallTracer {
    /// Run after each vm execution cycle
    fn finish_cycle(
        &mut self,
        state: &mut ZkSyncVmState<S, H>,
        _bootloader_state: &mut BootloaderState,
    ) -> TracerExecutionStatus {
        // Read the fragment immediately after EVENT, before a RET can append rollback
        // queries or discard a frame. The active far-call is the emission context.
        if let Some(timestamp) = self.pending_event_timestamp.take() {
            if let (Some(call), Some(query)) = (
                self.stack.last_mut(),
                state
                    .event_sink
                    .log_queries_after_timestamp(Timestamp(timestamp))
                    .last(),
            ) {
                if !query.rollback && query.timestamp.0 == timestamp {
                    call.latest_events.fragments.push((**query, false));
                }
            }
        }
        TracerExecutionStatus::Continue
    }

    fn after_vm_execution(
        &mut self,
        _state: &mut ZkSyncVmState<S, H>,
        _bootloader_state: &BootloaderState,
        _stop_reason: VmExecutionStopReason,
    ) {
        let result = std::mem::take(&mut self.finished_calls);
        let cell = self.result.as_ref();
        cell.set(result).unwrap();
    }
}

impl CallTracer {
    fn handle_far_call_op_code_latest<H: HistoryMode>(
        &mut self,
        state: VmLocalStateData<'_>,
        memory: &SimpleMemory<H>,
        current_call: &mut Call,
    ) {
        let current = state.vm_local_state.callstack.current;
        // All calls from the actual users are mimic calls,
        // so we need to check that the previous call was to the deployer.
        // Actually it's a call of the constructor.
        // And at this stage caller is user and callee is deployed contract.
        let call_type = if let CallType::Call(far_call) = current_call.r#type {
            if matches!(far_call, FarCallOpcode::Mimic) {
                let previous_caller = state
                    .vm_local_state
                    .callstack
                    .inner
                    .last()
                    .map(|call| call.this_address)
                    // Actually it's safe to just unwrap here, because we have at least one call in the stack
                    // But i want to be sure that we will not have any problems in the future
                    .unwrap_or(current.this_address);
                if previous_caller == CONTRACT_DEPLOYER_ADDRESS {
                    CallType::Create
                } else {
                    CallType::Call(far_call)
                }
            } else {
                CallType::Call(far_call)
            }
        } else {
            unreachable!()
        };
        let calldata = if current.code_page.0 == 0 || current.ergs_remaining == 0 {
            vec![]
        } else {
            let packed_abi =
                state.vm_local_state.registers[CALL_IMPLICIT_CALLDATA_FAT_PTR_REGISTER as usize];
            assert!(packed_abi.is_pointer);
            let far_call_abi = FarCallABI::from_u256(packed_abi.value);
            memory.read_unaligned_bytes(
                far_call_abi.memory_quasi_fat_pointer.memory_page as usize,
                far_call_abi.memory_quasi_fat_pointer.start as usize,
                far_call_abi.memory_quasi_fat_pointer.length as usize,
            )
        };

        current_call.input = calldata;
        current_call.r#type = call_type;
        if matches!(call_type, CallType::Call(FarCallOpcode::Delegate)) {
            // Delegate execution retains msg.sender and this_address, but the
            // trace calls the library from the caller's storage context. This
            // also makes `storage_contracts` resolve to that context, not to the
            // original external sender or the library's code address.
            current_call.from = state
                .vm_local_state
                .callstack
                .inner
                .last()
                .expect("delegate call has a parent frame")
                .this_address;
            current_call.to = current.code_address;
        } else {
            current_call.from = current.msg_sender;
            current_call.to = current.this_address;
        }
        current_call.value = U256::from(current.context_u128_value);
        current_call.gas = current.ergs_remaining as u64;
    }

    fn save_output_latest<H: HistoryMode>(
        &mut self,
        state: VmLocalStateData<'_>,
        memory: &SimpleMemory<H>,
        ret_opcode: RetOpcode,
        current_call: &mut Call,
    ) {
        let fat_data_pointer =
            state.vm_local_state.registers[RET_IMPLICIT_RETURNDATA_PARAMS_REGISTER as usize];

        // if `fat_data_pointer` is not a pointer then there is no output
        let output = if fat_data_pointer.is_pointer {
            let fat_data_pointer = FatPointer::from_u256(fat_data_pointer.value);
            if !fat_data_pointer.is_trivial() {
                Some(memory.read_unaligned_bytes(
                    fat_data_pointer.memory_page as usize,
                    fat_data_pointer.start as usize,
                    fat_data_pointer.length as usize,
                ))
            } else {
                None
            }
        } else {
            None
        };

        match ret_opcode {
            RetOpcode::Ok => {
                current_call.output = output.unwrap_or_default();
            }
            RetOpcode::Revert => {
                if let Some(output) = output {
                    current_call.revert_reason =
                        Some(VmRevertReason::from(output.as_slice()).to_string());
                    current_call.output = output;
                } else {
                    current_call.revert_reason = Some("Unknown revert reason".to_string());
                }
            }
            RetOpcode::Panic => {
                current_call.error = Some("Panic".to_string());
            }
        }
    }

    fn handle_ret_op_code_latest<H: HistoryMode>(
        &mut self,
        state: VmLocalStateData<'_>,
        memory: &SimpleMemory<H>,
        ret_opcode: RetOpcode,
    ) {
        let Some(current_call) = self.stack.last_mut() else {
            return;
        };

        if current_call.near_calls_after > 0 {
            current_call.near_calls_after -= 1;
            current_call.stack_depth_on_prefix -= 1;
            let (fragment_start, child_start) = current_call
                .latest_events
                .near_calls
                .pop()
                .expect("near-call event checkpoint is missing");
            if !matches!(ret_opcode, RetOpcode::Ok) {
                for (_, reverted) in &mut current_call.latest_events.fragments[fragment_start..] {
                    *reverted = true;
                }
                for child in &mut current_call.farcall.calls[child_start..] {
                    child.parent_failed = true;
                }
            }
            return;
        }

        let mut current_call = self.stack.pop().unwrap();
        // This frame owns only the fragments it executed. Descendant emissions are
        // retained on their own frames, including identical logs and reverted logs.
        current_call
            .latest_events
            .into_events(&mut current_call.farcall, L1BatchNumber(0));
        current_call.farcall.assign_child_positions();
        current_call.farcall.gas_used = current_call
            .farcall
            .parent_gas
            .saturating_sub(state.vm_local_state.callstack.current.ergs_remaining as u64);
        self.save_output_latest(state, memory, ret_opcode, &mut current_call.farcall);

        // If there is a parent call, push the current call to it
        // Otherwise, push the current call to the stack, because it's the top level call
        if let Some(parent_call) = self.stack.last_mut() {
            if current_call.farcall.storage_change {
                parent_call.farcall.storage_change = true;
            }
            parent_call.farcall.calls.push(current_call.farcall);
        } else {
            self.finished_calls.push(current_call.farcall);
        }
    }
}

#[cfg(test)]
mod tests {
    use zksync_types::{Address, EVENT_WRITER_ADDRESS};

    use super::*;

    fn fragments(timestamp: u32) -> CallEvents {
        let emitter = Address::repeat_byte(0x11);
        let head = LogQuery {
            timestamp: Timestamp(timestamp),
            tx_number_in_block: 1,
            shard_id: 0,
            aux_byte: 1,
            address: EVENT_WRITER_ADDRESS,
            key: U256::from(2),
            read_value: U256::zero(),
            written_value: U256::from_big_endian(emitter.as_bytes()),
            rw_flag: true,
            rollback: false,
            is_service: true,
        };
        let body = LogQuery {
            timestamp: Timestamp(timestamp + 4),
            is_service: false,
            key: U256::from(0x55),
            written_value: U256::zero(),
            ..head
        };
        CallEvents {
            fragments: vec![(head, false), (body, false)],
            near_calls: vec![],
        }
    }

    #[test]
    fn repeated_event_contents_retain_distinct_execution_identities() {
        let mut parent = Call::default();
        let mut child = Call::default();
        fragments(10).into_events(&mut parent, L1BatchNumber(2));
        fragments(20).into_events(&mut child, L1BatchNumber(2));
        assert_eq!(parent.events, child.events);
        assert_eq!(parent.event_metadata[0].timestamp, 10);
        assert_eq!(child.event_metadata[0].timestamp, 20);
        assert_eq!(parent.events[0].location.0, L1BatchNumber(2));
    }

    #[test]
    fn rollback_metadata_survives_fragment_merging() {
        let mut fragments = fragments(10);
        fragments.fragments[1].1 = true;
        let mut call = Call::default();
        fragments.into_events(&mut call, L1BatchNumber(2));
        assert_eq!(call.events.len(), 1);
        assert!(call.event_metadata[0].reverted);
    }
}
