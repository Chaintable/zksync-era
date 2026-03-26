//! Dependency injection for RocksDB cache maintenance (catch-up + keep-updated).

use std::path::PathBuf;

use anyhow::Context as _;
use zksync_dal::node::{PoolResource, ReplicaPool};
use zksync_node_framework::{
    service::{ShutdownHook, StopReceiver},
    task::{Task, TaskId, TaskKind},
    wiring_layer::{WiringError, WiringLayer},
    FromContext, IntoContext,
};
use zksync_state::{AsyncCatchupTask, KeepUpdatedTask, RocksdbStorageOptions};
use zksync_storage::RocksDB;

/// Wiring layer that maintains the State Keeper RocksDB cache by syncing it from Postgres.
///
/// This is useful in modes where Postgres is updated externally (i.e. without local re-execution).
#[derive(Debug)]
pub struct StateKeeperRocksdbCacheLayer {
    state_keeper_db_path: PathBuf,
    rocksdb_options: RocksdbStorageOptions,
}

impl StateKeeperRocksdbCacheLayer {
    pub fn new(state_keeper_db_path: PathBuf, rocksdb_options: RocksdbStorageOptions) -> Self {
        Self {
            state_keeper_db_path,
            rocksdb_options,
        }
    }
}

#[derive(Debug, FromContext)]
pub struct Input {
    replica_pool: PoolResource<ReplicaPool>,
}

#[derive(Debug, IntoContext)]
pub struct Output {
    #[context(task)]
    catchup: AsyncCatchupTaskWrapper,
    #[context(task)]
    keep_updated: KeepUpdatedTaskWrapper,
    rocksdb_shutdown_hook: ShutdownHook,
}

#[async_trait::async_trait]
impl WiringLayer for StateKeeperRocksdbCacheLayer {
    type Input = Input;
    type Output = Output;

    fn layer_name(&self) -> &'static str {
        "state_keeper_rocksdb_cache_layer"
    }

    async fn wire(self, input: Self::Input) -> Result<Self::Output, WiringError> {
        let pool = input.replica_pool.get().await?;
        let (catchup_task, rocksdb_cell) =
            AsyncCatchupTask::new(pool.clone(), self.state_keeper_db_path);
        let catchup_task = catchup_task.with_db_options(self.rocksdb_options);
        let keep_updated_task = rocksdb_cell.keep_updated(pool);

        let rocksdb_shutdown_hook = ShutdownHook::new("rocksdb_terminaton", async {
            // Wait for all the instances of RocksDB to be destroyed.
            tokio::task::spawn_blocking(RocksDB::await_rocksdb_termination)
                .await
                .context("failed terminating RocksDB instances")
        });

        Ok(Output {
            catchup: AsyncCatchupTaskWrapper(catchup_task),
            keep_updated: KeepUpdatedTaskWrapper(keep_updated_task),
            rocksdb_shutdown_hook,
        })
    }
}

#[derive(Debug)]
struct AsyncCatchupTaskWrapper(AsyncCatchupTask);

#[async_trait::async_trait]
impl Task for AsyncCatchupTaskWrapper {
    fn kind(&self) -> TaskKind {
        TaskKind::OneshotTask
    }

    fn id(&self) -> TaskId {
        "state_keeper/rocksdb_catchup_task".into()
    }

    async fn run(self: Box<Self>, stop_receiver: StopReceiver) -> anyhow::Result<()> {
        self.0.run(stop_receiver.0).await
    }
}

#[derive(Debug)]
struct KeepUpdatedTaskWrapper(KeepUpdatedTask);

#[async_trait::async_trait]
impl Task for KeepUpdatedTaskWrapper {
    fn kind(&self) -> TaskKind {
        TaskKind::Task
    }

    fn id(&self) -> TaskId {
        "state_keeper/rocksdb_keep_updated_task".into()
    }

    async fn run(self: Box<Self>, stop_receiver: StopReceiver) -> anyhow::Result<()> {
        self.0.run(stop_receiver.0).await
    }
}


