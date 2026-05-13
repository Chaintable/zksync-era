//! Legacy VM call representations.

use serde::{Deserialize, Serialize};
use zksync_types::{Address, U256};
use zksync_vm_interface::{Call, CallType};

/// Represents a call in the VM trace.
/// This version of the call represents the call structure before the 1.5.0 protocol version, where
/// all the gas-related fields were represented as `u32` instead of `u64`.
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct LegacyCall {
    /// Type of the call.
    pub r#type: CallType,
    /// Address of the caller.
    pub from: Address,
    /// Address of the callee.
    pub to: Address,
    /// Gas from the parent call.
    pub parent_gas: u32,
    /// Gas provided for the call.
    pub gas: u32,
    /// Gas used by the call.
    pub gas_used: u32,
    /// Value transferred.
    pub value: U256,
    /// Input data.
    pub input: Vec<u8>,
    /// Output data.
    pub output: Vec<u8>,
    /// Error message provided by vm or some unexpected errors.
    pub error: Option<String>,
    /// Revert reason.
    pub revert_reason: Option<String>,
    /// Subcalls.
    pub calls: Vec<LegacyCall>,
}

/// Lens chain v25-era Call struct. Same layout as the current `Call` but without
/// the DeBank-specific fields that were appended in commit `f650e15cb`
/// (`feat: apply DeBankDeFi customizations to chain/croze`). Subcalls recurse
/// with the same struct.
///
/// Lens blocks 0..=207145 (protocol_version=25) were written before those
/// DeBank fields existed. Bincode-deserializing that data into the current
/// `Call` struct fails with `UnexpectedEof` because the trailing DeBank fields
/// aren't present. `parse_call_trace` falls back to this struct on that error.
///
/// On `Into<Call>` conversion all DeBank fields default to "no info" — same
/// values `LegacyCall`/`LegacyMixedCall` use. Downstream backfilled v25 blocks
/// will therefore have empty `events`, `storage_contracts`, etc. — the same
/// degradation set as RPC-mode backfill.
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct LegacyCall25 {
    pub r#type: CallType,
    pub from: Address,
    pub to: Address,
    pub parent_gas: u64,
    pub gas: u64,
    pub gas_used: u64,
    pub value: U256,
    pub input: Vec<u8>,
    pub output: Vec<u8>,
    pub error: Option<String>,
    pub revert_reason: Option<String>,
    pub calls: Vec<LegacyCall25>,
}

/// Represents a call in the VM trace.
/// This version has subcalls in the form of "new" calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct LegacyMixedCall {
    /// Type of the call.
    pub r#type: CallType,
    /// Address of the caller.
    pub from: Address,
    /// Address of the callee.
    pub to: Address,
    /// Gas from the parent call.
    pub parent_gas: u32,
    /// Gas provided for the call.
    pub gas: u32,
    /// Gas used by the call.
    pub gas_used: u32,
    /// Value transferred.
    pub value: U256,
    /// Input data.
    pub input: Vec<u8>,
    /// Output data.
    pub output: Vec<u8>,
    /// Error message provided by vm or some unexpected errors.
    pub error: Option<String>,
    /// Revert reason.
    pub revert_reason: Option<String>,
    /// Subcalls.
    pub calls: Vec<Call>,
}

impl From<LegacyCall> for Call {
    fn from(legacy_call: LegacyCall) -> Self {
        Self {
            r#type: legacy_call.r#type,
            from: legacy_call.from,
            to: legacy_call.to,
            parent_gas: legacy_call.parent_gas.into(),
            gas: legacy_call.gas.into(),
            gas_used: legacy_call.gas_used.into(),
            value: legacy_call.value,
            input: legacy_call.input,
            output: legacy_call.output,
            error: legacy_call.error,
            revert_reason: legacy_call.revert_reason,
            calls: legacy_call.calls.into_iter().map(Into::into).collect(),
            call_start_timestamp: 0,
            events: vec![],
            parent_trace_id: None,
            trace_id: String::from(""),
            pos_in_parent_trace: 0,
            self_storage_change: false,
            storage_change: false,
            parent_failed: false,
        }
    }
}

impl From<LegacyMixedCall> for Call {
    fn from(legacy_call: LegacyMixedCall) -> Self {
        Self {
            r#type: legacy_call.r#type,
            from: legacy_call.from,
            to: legacy_call.to,
            parent_gas: legacy_call.parent_gas.into(),
            gas: legacy_call.gas.into(),
            gas_used: legacy_call.gas_used.into(),
            value: legacy_call.value,
            input: legacy_call.input,
            output: legacy_call.output,
            error: legacy_call.error,
            revert_reason: legacy_call.revert_reason,
            calls: legacy_call.calls,
            call_start_timestamp: 0,
            events: vec![],
            parent_trace_id: None,
            trace_id: String::from(""),
            pos_in_parent_trace: 0,
            self_storage_change: false,
            storage_change: false,
            parent_failed: false,
        }
    }
}

impl From<LegacyCall25> for Call {
    fn from(c: LegacyCall25) -> Self {
        Self {
            r#type: c.r#type,
            from: c.from,
            to: c.to,
            parent_gas: c.parent_gas,
            gas: c.gas,
            gas_used: c.gas_used,
            value: c.value,
            input: c.input,
            output: c.output,
            error: c.error,
            revert_reason: c.revert_reason,
            calls: c.calls.into_iter().map(Into::into).collect(),
            // DeBank fields default — Lens v25 PG data predates these fields,
            // matching the degradation set of LegacyCall / LegacyMixedCall.
            call_start_timestamp: 0,
            events: vec![],
            parent_trace_id: None,
            trace_id: String::new(),
            pos_in_parent_trace: 0,
            self_storage_change: false,
            storage_change: false,
            parent_failed: false,
        }
    }
}

#[derive(Debug)]
pub(super) struct LegacyCallConversionOverflowError;

impl TryFrom<Call> for LegacyCall {
    type Error = LegacyCallConversionOverflowError;

    fn try_from(call: Call) -> Result<Self, LegacyCallConversionOverflowError> {
        let calls: Result<Vec<LegacyCall>, LegacyCallConversionOverflowError> =
            call.calls.into_iter().map(LegacyCall::try_from).collect();
        Ok(Self {
            r#type: call.r#type,
            from: call.from,
            to: call.to,
            parent_gas: call
                .parent_gas
                .try_into()
                .map_err(|_| LegacyCallConversionOverflowError)?,
            gas: call
                .gas
                .try_into()
                .map_err(|_| LegacyCallConversionOverflowError)?,
            gas_used: call
                .gas_used
                .try_into()
                .map_err(|_| LegacyCallConversionOverflowError)?,
            value: call.value,
            input: call.input,
            output: call.output,
            error: call.error,
            revert_reason: call.revert_reason,
            calls: calls?,
        })
    }
}

impl TryFrom<Call> for LegacyMixedCall {
    type Error = LegacyCallConversionOverflowError;

    fn try_from(call: Call) -> Result<Self, LegacyCallConversionOverflowError> {
        Ok(Self {
            r#type: call.r#type,
            from: call.from,
            to: call.to,
            parent_gas: call
                .parent_gas
                .try_into()
                .map_err(|_| LegacyCallConversionOverflowError)?,
            gas: call
                .gas
                .try_into()
                .map_err(|_| LegacyCallConversionOverflowError)?,
            gas_used: call
                .gas_used
                .try_into()
                .map_err(|_| LegacyCallConversionOverflowError)?,
            value: call.value,
            input: call.input,
            output: call.output,
            error: call.error,
            revert_reason: call.revert_reason,
            calls: call.calls,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_call_deserialization() {
        let b = hex::decode("00000000002a000000000000003078303030303030303030303030303030303030303030303030303030303030303030303030303030302a00000000000000307830303030303030303030303030303030303030303030303030303030303030303030303038303031fa590600fa59060057ed03000f00000000000000307833386437656134633638303030000000000000000000000000000000000000010000000000000000000000002a000000000000003078303030303030303030303030303030303030303030303030303030303030303030303030383030312a00000000000000307830303030303030303030303030303030303030303030303030303030303030303030303038303062bae978fbda058bf7510f00000300000000000000307830040000000000000030e5ccbd000000000000000000000000000000000000").unwrap();
        let _: LegacyCall = bincode::deserialize(&b).unwrap();
    }

    #[test]
    fn call_deserialization() {
        let b = hex::decode("00000000002a000000000000003078303030303030303030303030303030303030303030303030303030303030303030303030303030302a00000000000000307830303030303030303030303030303030303030303030303030303030303030303030303038303031fa59060000000000fa5906000000000057ed0300000000000f00000000000000307833386437656134633638303030000000000000000000000000000000000000010000000000000000000000002a000000000000003078303030303030303030303030303030303030303030303030303030303030303030303030383030312a00000000000000307830303030303030303030303030303030303030303030303030303030303030303030303038303062bae978fb00000000da058bf700000000510f0000000000000300000000000000307830040000000000000030e5ccbd000000000000000000000000000000000000").unwrap();
        let _: Call = bincode::deserialize(&b).unwrap();
    }
}
