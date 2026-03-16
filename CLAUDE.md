# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ZKsync Era is a Layer 2 ZK rollup for Ethereum. The codebase contains the core node (sequencer), external node, prover, and smart contracts.

## Repository Structure

The repository has three separate Cargo workspaces:
- `/core` - Main node implementation (sequencer, external node, libraries)
- `/prover` - ZK proof generation system
- `/zkstack_cli` - CLI tool for managing ZK Stack ecosystems

Each workspace has its own `Cargo.toml` and must be built separately.

## Build Commands

```bash
# Build core node (from /core directory)
cd core && cargo build

# Build external_node binary
cd core && cargo build --release --bin zksync_external_node

# Build main sequencer binary
cd core && cargo build --release --bin zksync_server

# Build prover (from /prover directory)
cd prover && cargo build

# Build zkstack CLI
cargo install --path zkstack_cli/crates/zkstack --force --locked

# Build with release profile
cargo build --release

# Build with perf profile (for profiling)
cargo build --profile perf
```

## Testing

```bash
# Run all tests in core workspace
cd core && cargo test

# Run a specific test
cd core && cargo test <test_name>

# Run tests for a specific crate
cd core && cargo test -p zksync_state_keeper

# Run prover tests
cd prover && cargo test

# Integration tests via zkstack
zkstack dev test integration
zkstack dev test revert
zkstack dev test recovery
```

## Linting and Formatting

```bash
# Format Rust code
cargo fmt

# Run clippy
cargo clippy

# Format all code via zkstack
zkstack dev fmt

# Lint all code via zkstack
zkstack dev lint
```

## Key Architecture Concepts

### Block Structure
- **L1 Batch**: Unit for generating ZK proofs. Contains multiple L2 blocks. Submitted to L1.
- **L2 Block (Miniblock)**: Created every ~2 seconds. Contains transactions. Used for API compatibility.
- **Transaction**: Individual user operations.

### Core Node Components (`/core/node/`)
- **state_keeper**: Main sequencer logic - forms blocks and L1 batches, executes transactions via VM
- **api_server**: Web3 JSON-RPC server implementation
- **eth_sender**: Submits batches to L1 contract
- **eth_watch**: Monitors L1 contract for deposits/priority operations
- **metadata_calculator**: Maintains Merkle tree
- **vm_runner**: Re-runs sealed batches for various data generation
- **consensus**: P2P networking utilities

### Execution Flow
1. `StateKeeper` (`state_keeper/src/keeper.rs`) orchestrates execution
2. Transactions come from `MempoolIO` (`state_keeper/src/io/mempool.rs`)
3. `BatchExecutor` (`vm_executor/src/batch/executor.rs`) executes transactions in the VM
4. `UpdatesManager` accumulates state changes
5. Sealing criteria determine when to close L2 blocks and L1 batches

### Key Libraries (`/core/lib/`)
- **multivm**: Wrapper over multiple VM versions
- **dal**: Database access layer with migrations in `/dal/migrations/`
- **types**: Core ZKsync types and structures
- **state**: RocksDB state management and caching

### RocksDB Cache Synchronization

The system uses RocksDB as a local cache for faster state access. Two tasks manage synchronization with Postgres:

**`AsyncCatchupTask`** (`lib/state/src/catchup.rs`): One-shot task that catches up RocksDB to match Postgres state at startup. Returns a `RocksdbCell` for accessing the initialized cache.

**`KeepUpdatedTask`** (`lib/state/src/catchup.rs`): Long-running task that continuously syncs RocksDB after initial catch-up. Only needed when state is updated externally (not via local execution).

**Usage by node type:**

| Node | Mode | Catch-up | Continuous Sync | Why |
|------|------|----------|-----------------|-----|
| Main sequencer | - | `AsyncCatchupTask` | No | State keeper updates RocksDB directly during execution |
| External node | Full | `AsyncCatchupTask` | No | Re-executes transactions locally, updates RocksDB directly |
| External node | RPC | `AsyncCatchupTask` | `KeepUpdatedTask` | No local execution; syncs state from Postgres |
| VM runner | - | `AsyncCatchupTask` | `StorageSyncTask` | Catches up to specific batch, then loads batches into memory |

**Key files:**
- `lib/state/src/catchup.rs` - Task definitions
- `node/state_keeper/src/node/state_keeper.rs` - `StateKeeperLayer` (main sequencer, external node full mode)
- `node/state_keeper/src/node/rocksdb_cache.rs` - `StateKeeperRocksdbCacheLayer` (external node RPC mode)
- `node/vm_runner/src/storage.rs` - `StorageSyncTask` (VM runner components)

### External Node Sync Flow

External nodes sync L2 blocks and batches from the main node through a pipeline:

```
Main Node (RPC)
     ↓
Consensus Fetcher ──→ PayloadQueue ──→ IoCursor.advance()
     ↓
ActionQueue (SyncAction: OpenBatch, L2Block, Tx, SealL2Block, SealBatch)
     ↓
ExternalIO (implements StateKeeperIO)
     ↓
StateKeeper (re-executes transactions in VM)
     ↓
OutputHandler (persists to Postgres + RocksDB)
```

**Key components:**

| Component | File | Purpose |
|-----------|------|---------|
| `fetch_blocks()` | `node/consensus/src/en.rs:302` | Fetches blocks from main node via RPC |
| `IoCursor::advance()` | `node/node_sync/src/fetcher.rs:141` | Converts `FetchedBlock` to `SyncAction` sequence |
| `ActionQueue` | `node/node_sync/src/sync_action.rs:86` | Channel between fetcher and ExternalIO (32K capacity) |
| `ExternalIO` | `node/node_sync/src/external_io.rs:38` | Implements `StateKeeperIO`, consumes actions |

**SyncAction types** (`node/node_sync/src/sync_action.rs:160`):
- `OpenBatch` - Start new L1 batch
- `L2Block` - Start new L2 block within batch
- `Tx` - Transaction to execute
- `SealL2Block` - Close current L2 block
- `SealBatch` - Close current L1 batch

**Action processing in StateKeeper** (`node/state_keeper/src/keeper.rs`):

```
StateKeeper::run_inner() loop:
│
├─► process_block()
│   ├─► wait_for_new_batch_params() ←── OpenBatch action
│   │   └─► Inserts L1 batch header to DB
│   │
│   ├─► wait_for_new_l2_block_params() ←── L2Block action (for non-first blocks)
│   │   └─► Sets params in UpdatesManager
│   │
│   └─► process_block_iteration() loop:
│       ├─► should_seal_l1_batch_unconditionally() ←── checks SealBatch
│       ├─► should_seal_l2_block() ←── checks SealL2Block
│       └─► wait_for_next_tx() ←── Tx action
│           └─► process_one_tx()
│               ├─► BatchExecutor.execute_tx() (VM execution)
│               └─► UpdatesManager.extend_from_executed_transaction()
│
├─► seal_last_pending_block_data()
│   └─► OutputHandler.handle_l2_block_data() (write to Postgres)
│
└─► commit_pending_block()
    ├─► BatchExecutor.commit_l2_block() (commit to RocksDB)
    └─► If fictive block: seal_batch()
        └─► OutputHandler.handle_l1_batch()
```

**Key files for sync flow:**
- `node/consensus/src/en.rs` - Block fetching from main node
- `node/node_sync/src/fetcher.rs` - Block to action conversion
- `node/node_sync/src/sync_action.rs` - Action types and queue
- `node/node_sync/src/external_io.rs` - StateKeeperIO implementation
- `node/state_keeper/src/keeper.rs` - Main processing loop

### VM Execution Modes (TxExecutionMode)

The VM supports three execution modes defined in `lib/vm_interface/src/types/inputs/system_env.rs`:

```rust
pub enum TxExecutionMode {
    VerifyExecute,  // Full validation + execution (real transactions)
    EstimateFee,    // Like VerifyExecute but ignores validation errors
    EthCall,        // Simulation via mimicCall (eth_call RPC)
}
```

**Key Differences:**

| Aspect | EthCall | VerifyExecute |
|--------|---------|---------------|
| Validation | Skipped (uses `mimicCall`) | Full account validation |
| Bootloader mode byte | `0x02` | `0x00` |
| System contracts | `playground_*` (better UX) | Standard contracts |
| Storage access limit | Limited (anti-DoS) | Unlimited |
| Use case | `eth_call` RPC | Transaction execution |

**Bootloader Metadata** (`lib/multivm/src/versions/vm_1_4_1/bootloader_state/utils.rs:162`):
```rust
output[0] = match execution_mode {
    TxExecutionMode::VerifyExecute => 0x00,  // validate & execute
    TxExecutionMode::EstimateFee => 0x00,    // validate & execute
    TxExecutionMode::EthCall => 0x02,        // execute WITHOUT validation
};
```

**Execution Flow Comparison:**

EthCall (playground bootloader):
```
Bootloader
├── unsafeOverrideBatch()     ← Creates fake batch context
├── setL2Block()
├── appendTransactionToCurrentL2Block()
├── getCodeHash()
├── incrementTxNumberInBatch()
├── publishEVMBytecode()
└── MsgValueSimulator → target   ← Direct mimicCall execution
```

VerifyExecute (standard bootloader):
```
Bootloader
├── setTxOrigin()             ← Track transaction sender
├── getCodeHash()
├── setL2Block()
├── getBaseFee()
├── getTransactionHashes()    ← Compute EIP-712 hashes
├── setTxHash()
├── appendTransactionToCurrentL2Block()
├── incrementTxNumberInBatch()
├── setBaseFee()
├── getAccountInfo()
├── validateNonceUsage()      ← Nonce check BEFORE
├── Account.validateTransaction()  ← AA validation + ecrecover
├── validateNonceUsage()      ← Nonce check AFTER
├── balanceOf()
├── Account.payForTransaction()    ← Fee payment to operator
├── transferFromTo()          ← Refund excess
├── publishEVMBytecode()
├── getCodeHash()
├── incrementTxNumberInBatch()
├── Account.executeTransaction()   ← Execute via account contract
├── incrementTxNumberInBatch()
└── transferFromTo()          ← Gas refund
```

**Important:** These modes are NOT superset/subset - they use different bootloader paths:
- `unsafeOverrideBatch` (0x29f172ad) exists ONLY in EthCall
- Account Abstraction calls exist ONLY in VerifyExecute

**Key files:**
- `lib/vm_interface/src/types/inputs/system_env.rs` - TxExecutionMode enum
- `lib/vm_executor/src/oneshot/env.rs` - Mode selection (`to_call_env` vs `to_execute_env`)
- `lib/vm_executor/src/oneshot/mod.rs` - Storage limit handling per mode
- `lib/vm_executor/src/oneshot/contracts.rs` - System contracts selection (`playground_*` vs `estimate_gas_*`)
- `lib/multivm/src/versions/vm_1_4_1/bootloader_state/utils.rs` - `assemble_tx_meta()` bootloader flag

## Rust Toolchain

The project uses nightly Rust. The toolchain is specified in `/rust-toolchain`:
```
[toolchain]
channel = "nightly-2025-03-19"
```

Ensure rust-analyzer is installed for this toolchain:
```bash
rustup component add rust-analyzer --toolchain nightly-2025-03-19
```

## zkstack CLI

The primary tool for ecosystem management:

```bash
# Create ecosystem
zkstack ecosystem create

# Initialize ecosystem
zkstack ecosystem init

# Run the server
zkstack server

# Database operations
zkstack dev db setup
zkstack dev db migrate
zkstack dev db reset

# Run external node
zkstack en init
zkstack en run
```

## Documentation

- Core docs: https://matter-labs.github.io/zksync-era/core/latest/
- Prover docs: https://matter-labs.github.io/zksync-era/prover/latest/
- In-repo guides: `/docs/src/guides/`
