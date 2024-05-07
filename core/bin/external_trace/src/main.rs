mod api;
use api::{DebugApiClient, EthApiClient, PreApiClient, PreApiServer, TraceApiServer};

use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use jsonrpsee::{core::RpcResult, server::ServerBuilder};
use tokio::signal;
use zksync_types::transaction_request::CallRequest;
use zksync_types::{
    api::{flat_call, BlockId, OpenEthActionTrace, PreResult, TransactionReceipt},
    H256,
};

/// Constructs a JSON-RPC error, consisting of `code`, `message` and optional `data`.
pub(crate) fn rpc_err(
    code: i32,
    msg: impl Into<String>,
    data: Option<&[u8]>,
) -> jsonrpsee::types::error::ErrorObject<'static> {
    jsonrpsee::types::error::ErrorObject::owned(
        code,
        msg.into(),
        data.map(|data| {
            jsonrpsee::core::to_json_raw_value(&format!("0x{}", 1))
                .expect("serializing String does fail")
        }),
    )
}

pub(crate) fn internal_rpc_err(
    msg: impl Into<String>,
) -> jsonrpsee::types::error::ErrorObject<'static> {
    rpc_err(jsonrpsee::types::error::INTERNAL_ERROR_CODE, msg, None)
}

pub struct TraceApiImpl {
    client: HttpClient,
}

impl TraceApiImpl {
    pub fn new(url: &str) -> Self {
        let client = HttpClientBuilder::default().build(url).unwrap();
        Self { client }
    }
}

#[async_trait::async_trait]
impl TraceApiServer for TraceApiImpl {
    async fn trace_transaction(&self, hash: H256) -> RpcResult<Vec<OpenEthActionTrace>> {
        let res = vec![];
        let tx: Result<zksync_types::api::Transaction, jsonrpsee::core::Error> =
            self.client.get_transaction_by_hash(hash).await;
        if tx.is_err() {
            println!("get_transaction_by_hash Error: {:?}", tx);
            return Ok(res);
        }
        let tx = tx.unwrap();
        let call_traces = self.client.debug_transaction_by_hash(hash).await;
        if call_traces.is_err() {
            println!("debug_transaction_by_hash Error: {:?}", call_traces);
            return Ok(res);
        }
        let call_traces = call_traces.unwrap();
        if call_traces.is_none() {
            println!("call_traces is {:?}", call_traces);
            return Ok(res);
        }
        let call_traces = call_traces.unwrap();
        let res = flat_call(
            call_traces,
            tx.transaction_index.unwrap().as_usize(),
            hash,
            tx.block_number.unwrap().as_u64(),
            tx.block_hash.unwrap(),
            &mut Vec::new(),
        );
        return Ok(res);
    }
}

pub struct PreApiImpl {
    client: HttpClient,
}

impl PreApiImpl {
    pub fn new(url: &str) -> Self {
        let client = HttpClientBuilder::default().build(url).unwrap();
        Self { client }
    }
}

#[async_trait::async_trait]
impl PreApiServer for PreApiImpl {
    async fn pre_trace_transaction(
        &self,
        request: CallRequest,
        block: Option<BlockId>,
    ) -> RpcResult<Vec<OpenEthActionTrace>> {
        let res = vec![];
        let block_num = self.client.get_block_number().await;
        if block_num.is_err() {
            println!("get_block_number Error: {:?}", block_num);
            return Ok(res);
        }
        let call_traces = self.client.trace_call(request.clone(), block, None).await;
        if call_traces.is_err() {
            println!("pre_trace_transaction Error: {:?}", request);
            return Ok(res);
        }
        let call_traces = call_traces.unwrap();
        let res = flat_call(
            call_traces,
            0,
            H256::random(),
            block_num.unwrap().as_u64(),
            H256::random(),
            &mut Vec::new(),
        );
        return Ok(res);
    }

    async fn pre_get_logs(
        &self,
        request: CallRequest,
        block: Option<BlockId>,
    ) -> RpcResult<TransactionReceipt> {
        let res = self
            .client
            .trace_get_log(request, block)
            .await
            .map_err(|e| {
                println!("pre_get_logs Error: {:?}", e);
                internal_rpc_err(e.to_string())
            })?;
        Ok(res)
    }

    async fn pre_trace_many(
        &self,
        requests: Vec<CallRequest>,
        block: Option<BlockId>,
    ) -> RpcResult<serde_json::Value> {
        let res = self
            .client
            .debug_trace_many(requests, block)
            .await
            .map_err(|e| {
                println!("pre_get_logs Error: {:?}", e);
                internal_rpc_err(e.to_string())
            })?;
        Ok(res)
    }
}

/// Runs the future to completion or until:
/// - `ctrl-c` is received.
/// - `SIGTERM` is received.
pub(crate) async fn run_until_ctrl_c<F>(fut: F) -> anyhow::Result<()>
where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    let ctrl_c = signal::ctrl_c();
    let mut stream = signal::unix::signal(signal::unix::SignalKind::terminate())?;
    let sigterm = stream.recv();

    tokio::select! {
        _ = ctrl_c => {
            print!( "Received ctrl-c");
            fut.await?;
        },
        _ = sigterm => {
            print!( "Received SIGTERM");
            fut.await?;
        },
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // get url from cmd args
    use std::env;
    let args: Vec<String> = env::args().collect();
    let url = args.get(1).expect("URL argument missing");
    let local_addr = args.get(2).expect("Local address argument missing");
    println!("URL: {}", url);
    println!("Local address: {}", local_addr);
    let server = ServerBuilder::default().build(local_addr).await?;
    let trace = TraceApiImpl::new(url);
    let pre = PreApiImpl::new(url);
    let mut rpc = trace.into_rpc();
    rpc.merge(pre.into_rpc())?;
    let handle = server.start(rpc)?;
    run_until_ctrl_c(async move {
        println!("stopping leafage server...");
        let _ = handle.stop();
        Ok(())
    })
    .await?;
    Ok(())
}
