//! Read-only projections for offline execution replay of pre-interop v25 archives.
//! Normal node DAL queries deliberately keep requiring the current schema.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{ensure, Context};
use zksync_types::{
    block::{L1BatchHeader, L2BlockExecutionData, L2BlockHeader},
    l2_to_l1_log::UserL2ToL1Log,
    L1BatchNumber, L2BlockNumber, ProtocolVersionId, Transaction,
};

use crate::{
    models::{
        storage_block::{StorageL1BatchHeader, StorageL2BlockHeader},
        storage_transaction::StorageTransaction,
    },
    Connection, Core, CoreDal,
};

/// Schema capabilities resolved through the connection's search path. Fallbacks
/// are the defaults of the native migrations, never replacements for present data.
#[derive(Debug)]
pub struct ArchiveReplayReader {
    batch_columns: BTreeSet<String>,
    block_columns: BTreeSet<String>,
    has_interop_roots: bool,
}

fn projection(
    columns: &BTreeSet<String>,
    name: &str,
    v25_default: &str,
    protocol: ProtocolVersionId,
) -> anyhow::Result<String> {
    if columns.contains(name) {
        Ok(name.to_owned())
    } else {
        ensure!(
            protocol == ProtocolVersionId::Version25,
            "archive lacks {name}; its default is only defined for protocol 25"
        );
        Ok(format!("{v25_default} AS {name}"))
    }
}

impl ArchiveReplayReader {
    pub async fn new(db: &mut Connection<'_, Core>) -> anyhow::Result<Self> {
        async fn columns(
            db: &mut Connection<'_, Core>,
            relation: &str,
        ) -> anyhow::Result<BTreeSet<String>> {
            Ok(sqlx::query_scalar::<_, String>(
                "SELECT attname::text FROM pg_attribute \
                 WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped",
            )
            .bind(relation)
            .fetch_all(db.conn())
            .await?
            .into_iter()
            .collect())
        }
        Ok(Self {
            batch_columns: columns(db, "l1_batches").await?,
            block_columns: columns(db, "miniblocks").await?,
            has_interop_roots: sqlx::query_scalar(
                "SELECT to_regclass('interop_roots') IS NOT NULL",
            )
            .fetch_one(db.conn())
            .await?,
        })
    }

    pub async fn batch_header(
        &self,
        db: &mut Connection<'_, Core>,
        number: L1BatchNumber,
    ) -> anyhow::Result<L1BatchHeader> {
        let version: Option<i32> = sqlx::query_scalar(
            "SELECT protocol_version FROM l1_batches WHERE number = $1 AND is_sealed",
        )
        .bind(i64::from(number.0))
        .fetch_one(db.conn())
        .await?;
        let protocol = ProtocolVersionId::try_from(u16::try_from(
            version.context("batch protocol version is missing")?,
        )?)?;
        self.check_interop_schema(protocol)?;
        // Expressions and aliases come exclusively from these constants.
        let defaults = [
            ("interop_fee", "0::numeric"),
            ("pubdata_limit", "NULL::bigint"),
            ("settlement_layer_type", "'L1'::text"),
            ("settlement_layer_chain_id", "1::bigint"),
        ]
        .into_iter()
        .map(|(name, default)| projection(&self.batch_columns, name, default, protocol))
        .collect::<anyhow::Result<Vec<_>>>()?
        .join(", ");
        let query = format!(
            "SELECT number, timestamp, l1_tx_count, l2_tx_count, l2_to_l1_messages, bloom, \
             priority_ops_onchain_data, used_contract_hashes, bootloader_code_hash, \
             default_aa_code_hash, evm_emulator_code_hash, protocol_version, system_logs, \
             pubdata_input, fee_address, l1_gas_price, l2_fair_gas_price, fair_pubdata_price, \
             {defaults} FROM l1_batches WHERE number = $1 AND is_sealed"
        );
        let row = sqlx::query_as::<_, StorageL1BatchHeader>(&query)
            .bind(i64::from(number.0))
            .fetch_one(db.conn())
            .await?;
        let logs = db
            .blocks_dal()
            .get_l2_to_l1_logs_for_batch::<UserL2ToL1Log>(number)
            .await?;
        row.into_l1_batch_header_with_logs(logs)
    }

    fn check_interop_schema(&self, protocol: ProtocolVersionId) -> anyhow::Result<()> {
        ensure!(
            self.has_interop_roots || protocol == ProtocolVersionId::Version25,
            "archive lacks interop_roots; an empty input is only defined for protocol 25"
        );
        Ok(())
    }

    pub async fn block_header(
        &self,
        db: &mut Connection<'_, Core>,
        number: L2BlockNumber,
        protocol: ProtocolVersionId,
    ) -> anyhow::Result<L2BlockHeader> {
        let rolling = projection(
            &self.block_columns,
            "rolling_txs_hash",
            "NULL::bytea",
            protocol,
        )?;
        let da_scheme = projection(
            &self.block_columns,
            "l2_da_commitment_scheme",
            "NULL::integer",
            protocol,
        )?;
        let query = format!(
            "SELECT number, timestamp, hash, l1_tx_count, l2_tx_count, fee_account_address, \
             base_fee_per_gas, l1_gas_price, l2_fair_gas_price, gas_per_pubdata_limit, \
             bootloader_code_hash, default_aa_code_hash, evm_emulator_code_hash, \
             protocol_version, virtual_blocks, fair_pubdata_price, gas_limit, logs_bloom, \
             l2_da_validator_address, pubdata_type, {rolling}, {da_scheme} \
             FROM miniblocks WHERE number = $1"
        );
        let row = sqlx::query_as::<_, StorageL2BlockHeader>(&query)
            .bind(i64::from(number.0))
            .fetch_one(db.conn())
            .await?;
        ensure!(
            row.protocol_version == Some(i32::from(protocol as u16)),
            "block {number} protocol differs from its batch"
        );
        Ok(row.into())
    }

    /// Includes empty blocks; coverage is checked against every persisted header
    /// by the caller, rather than inferring block coverage from transactions.
    pub async fn execution_data(
        &self,
        db: &mut Connection<'_, Core>,
        headers: &[L2BlockHeader],
        protocol: ProtocolVersionId,
    ) -> anyhow::Result<Vec<L2BlockExecutionData>> {
        self.check_interop_schema(protocol)?;
        let first = headers.first().context("batch has no L2 blocks")?;
        let last = headers.last().unwrap();
        let transactions = sqlx::query_as::<_, StorageTransaction>(
            "SELECT * FROM transactions WHERE miniblock_number BETWEEN $1 AND $2 \
             ORDER BY miniblock_number, index_in_block",
        )
        .bind(i64::from(first.number.0))
        .bind(i64::from(last.number.0))
        .fetch_all(db.conn())
        .await?;
        let mut by_block: BTreeMap<_, Vec<Transaction>> = BTreeMap::new();
        for tx in transactions {
            by_block
                .entry(tx.miniblock_number.context("unmined archive transaction")?)
                .or_default()
                .push(tx.into());
        }
        let mut previous_hash = db
            .blocks_web3_dal()
            .get_l2_block_hash(first.number - 1)
            .await?
            .context("previous L2 block is absent from archive")?;
        let mut blocks = Vec::with_capacity(headers.len());
        for header in headers {
            let interop_roots = if self.has_interop_roots {
                db.interop_root_dal()
                    .get_interop_roots(header.number)
                    .await?
            } else {
                vec![]
            };
            blocks.push(L2BlockExecutionData {
                number: header.number,
                timestamp: header.timestamp,
                prev_block_hash: previous_hash,
                virtual_blocks: header.virtual_blocks,
                txs: by_block
                    .remove(&i64::from(header.number.0))
                    .unwrap_or_default(),
                interop_roots,
            });
            previous_hash = header.hash;
        }
        ensure!(
            by_block.is_empty(),
            "transaction has no archive block header"
        );
        Ok(blocks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_defaults_cannot_mask_missing_newer_protocol_inputs() {
        let columns = BTreeSet::new();
        assert_eq!(
            projection(
                &columns,
                "interop_fee",
                "0::numeric",
                ProtocolVersionId::Version25
            )
            .unwrap(),
            "0::numeric AS interop_fee"
        );
        assert!(projection(
            &columns,
            "interop_fee",
            "0::numeric",
            ProtocolVersionId::Version26
        )
        .is_err());
        let columns = BTreeSet::from(["interop_fee".into()]);
        assert_eq!(
            projection(
                &columns,
                "interop_fee",
                "0::numeric",
                ProtocolVersionId::Version26
            )
            .unwrap(),
            "interop_fee"
        );
        let reader = ArchiveReplayReader {
            batch_columns: columns,
            block_columns: BTreeSet::new(),
            has_interop_roots: false,
        };
        assert!(reader
            .check_interop_schema(ProtocolVersionId::Version25)
            .is_ok());
        assert!(reader
            .check_interop_schema(ProtocolVersionId::Version26)
            .is_err());
    }
}
