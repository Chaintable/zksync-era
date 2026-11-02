use zksync_types::{ceil_div_u256, H256, U256};

use crate::{interface::L1BatchEnv, vm_latest::utils::fee::get_batch_base_fee};

pub(crate) fn compute_refund(
    l1_batch: &L1BatchEnv,
    bootloader_refund: u64,
    gas_spent_on_pubdata: u64,
    tx_gas_limit: u64,
    current_ergs_per_pubdata_byte: u32,
    pubdata_published: u32,
    tx_hash: H256,
) -> u64 {
    let total_gas_spent = tx_gas_limit - bootloader_refund;

    let gas_spent_on_computation = total_gas_spent
        .checked_sub(gas_spent_on_pubdata)
        .unwrap_or_else(|| {
            tracing::error!(
                "Gas spent on pubdata is greater than total gas spent. On pubdata: {}, total: {}",
                gas_spent_on_pubdata,
                total_gas_spent
            );
            0
        });

    // For now, bootloader charges only for base fee.
    let effective_gas_price = get_batch_base_fee(l1_batch);

    if effective_gas_price == 0 {
        // Lens's historical gasless batches pay no fee to convert back into
        // gas. Preserve the bootloader's refund, including its memory value.
        return bootloader_refund;
    }

    let bootloader_eth_price_per_pubdata_byte =
        U256::from(effective_gas_price) * U256::from(current_ergs_per_pubdata_byte);

    let fair_eth_price_per_pubdata_byte = U256::from(l1_batch.fee_input.fair_pubdata_price());

    // For now, L1 originated transactions are allowed to pay less than fair fee per pubdata,
    // so we should take it into account.
    let eth_price_per_pubdata_byte_for_calculation = std::cmp::min(
        bootloader_eth_price_per_pubdata_byte,
        fair_eth_price_per_pubdata_byte,
    );

    let fair_fee_eth = U256::from(gas_spent_on_computation)
        * U256::from(l1_batch.fee_input.fair_l2_gas_price())
        + U256::from(pubdata_published) * eth_price_per_pubdata_byte_for_calculation;
    let pre_paid_eth = U256::from(tx_gas_limit) * U256::from(effective_gas_price);
    let refund_eth = pre_paid_eth.checked_sub(fair_fee_eth).unwrap_or_else(|| {
        tracing::error!(
            "Fair fee is greater than pre paid. Fair fee: {} wei, pre paid: {} wei",
            fair_fee_eth,
            pre_paid_eth
        );
        U256::zero()
    });

    tracing::trace!(
        "Fee benchmark for transaction with hash {}",
        hex::encode(tx_hash.as_bytes())
    );
    tracing::trace!("Gas Limit: {}", tx_gas_limit);
    tracing::trace!("Gas spent on computation: {}", gas_spent_on_computation);
    tracing::trace!("Gas spent on pubdata: {}", gas_spent_on_pubdata);
    tracing::trace!("Pubdata published: {}", pubdata_published);

    ceil_div_u256(refund_eth, effective_gas_price.into()).as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::interface::L2BlockEnv;
    use zksync_types::{fee_model::BatchFeeInput, L1BatchNumber};

    #[test]
    fn gasless_batch_preserves_the_bootloader_refund() {
        let batch = L1BatchEnv {
            previous_batch_hash: None,
            number: L1BatchNumber(1),
            timestamp: 1,
            fee_input: BatchFeeInput::pubdata_independent(0, 0, 0),
            enforced_base_fee: Some(0),
            fee_account: Default::default(),
            first_l2_block: L2BlockEnv {
                number: 1,
                timestamp: 1,
                prev_block_hash: H256::zero(),
                max_virtual_blocks_to_create: 1,
                interop_roots: vec![],
            },
            settlement_layer: Default::default(),
            interop_fee: Default::default(),
        };
        assert_eq!(
            compute_refund(&batch, 333, 12, 1000, 1, 12, H256::zero()),
            333
        );
    }
}
