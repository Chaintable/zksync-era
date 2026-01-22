//! Dependency injection for the state keeper.

pub use self::{
    main_batch_executor::MainBatchExecutorLayer,
    mempool_io::MempoolIOLayer,
    output_handler::OutputHandlerLayer,
    rocksdb_cache::StateKeeperRocksdbCacheLayer,
    resources::{BatchExecutorResource, OutputHandlerResource, StateKeeperIOResource},
    state_keeper::StateKeeperLayer,
};

mod main_batch_executor;
mod mempool_io;
mod output_handler;
mod rocksdb_cache;
mod resources;
mod state_keeper;
