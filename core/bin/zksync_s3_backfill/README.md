# DeBank execution reconstruction

`debank_execution_replay` executes sealed L1 batches from a restored archive PostgreSQL database. It does not read stored Call blobs, insert traces, upload to S3, send Kafka messages, or persist new chain state. PostgreSQL operations run in a read-only, repeatable-read transaction. Use an isolated restored database.

The offline reader also supports the pre-interop v25 archive schema without running migrations or creating views. It resolves tables through the connection's search path, preserves present fields and interop roots, and supplies only the native migration defaults for absent later fields: nullable rolling transaction hash, DA commitment scheme and pubdata limit, plus L1 settlement, settlement chain ID 1 and zero interop fee. Missing fields or an absent interop table are rejected for other protocols.

The archive must contain original transactions, batch environments, system bytecodes, historical storage values, initial-write batch numbers and enumeration indices. Pruned history is rejected. This command cannot extend an incomplete archive by reading its latest state; restore / synchronize the missing archive history first.

```sh
cargo run --release -p zksync_s3_backfill --bin debank_execution_replay -- \
  --database-url postgres://postgres@localhost:5432/zksync \
  --chain-id 232 --start-batch 2 --end-batch 2 --output-dir /data/lens-replay
```

The range is inclusive. Protocols 25 and later use the fixed EraVM collector; the external-node Writer also uses this VM and enables call tracing when `DEBANK_S3_ENABLED` is set. This change does not add complete event collection to the optional fast VM.

`--chain-id` identifies the batch being executed. A different pre-state chain ID is accepted only when it is nonzero, has a historical enumeration index, and this batch records a write to the requested ID. The VM must reproduce that transition and end with the requested ID; the manifest records the prior ID and whether a transition was required. A missing previous batch root is an immediate input error.

Each locally committed batch directory contains original inputs, transaction execution records, original event emission identities / rollback metadata, batch-tip calls, final storage logs, the historical slots read during execution, per-block BlockFile / Header / full Validation, and a SHA256 manifest. The batch tip is executed with a tracer. Its calls and events are block-scoped (`tx_id` is empty), with identities derived from the canonical block hash; no ordinary fictive transaction is invented.

Unordered used-contract hashes are sorted before serialization. Calls, events and storage logs retain their execution order.

Generation compares transaction coverage/order, status, gas, effective gas price, committed events (including duplicates/order) and net storage changes with the restored PG. Raw event identities and physical rollback are also checked against VM committed logs before export classification. The established protocol still classifies descendants of a failed transaction summary as error records, including committed fee/refund logs; receipt presence does not override that classification. Serialized graphs are checked for unique IDs/paths, parent closure, identity formulas and mixed child positions. These are PG comparisons, not independent chain verification or a recomputed state root. The manifest therefore records `canonical_publish_ready=false`. Validate the frozen source against canonical chain evidence, all actual protocol transitions, genesis, batch 2, batches 2554/2555 and 3590/3591, and real failing executions before publishing. Consumer acceptance of block-scoped system records must also be verified. No StateDiff is exported.

Batch results are staged in a new `.batch-*-prepared-*` directory and committed by directory rename only after checks pass. Failures preserve input and execution records and do not create a committed manifest. A rerun validates inputs and every committed file checksum before reusing a completed batch; it does not skip an uncommitted failure. Completed directories and unrelated files are never overwritten.

## Genesis

Genesis allocations did not execute as transactions. `debank_genesis_rebuild` projects a **complete, independently verified initial allocation** using the existing balance/code/native-token genesis record convention. It includes every code allocation regardless of how many accounts share the same bytecode. It does not derive allocations from incomplete Call records, guess balances, or filter frequent wrapper code.

```sh
cargo run --release -p zksync_s3_backfill --bin debank_genesis_rebuild -- \
  --input /data/lens-genesis.json --output-dir /data/lens-genesis-output
```

Input fields are `chain_id`, `source` (provenance), `block` (BlockMeta), and `alloc` (address-keyed accounts). Every account must explicitly provide `balance`, hex `code`, and `has_storage`; missing information is an input error. The input and generated files are hashed. Balances, code and storage coverage still need to be verified against authoritative genesis state; this command does not compute the genesis state root. The old RPC backfill's system-address list is not a complete allocation source.

## Stored Call compatibility

New traced events retain their emission timestamps and rollback status in `Call.event_metadata`. New post-1.5.0 PG Call blobs start with `CTCALL2\0`; readers support this format, existing DeBank blobs, and official v25 Calls without DeBank extensions. Missing legacy metadata remains explicitly absent and is not a complete reconstruction source.

Old Writer/API readers cannot decode the new versioned blobs. Upgrade all relevant PG readers before enabling writes in a shared database, or keep the reconstruction database isolated. The S3 Validation hash keeps the established Go protocol (complete block / transaction / successful event / normal trace IDs); all six array counts are emitted separately.
