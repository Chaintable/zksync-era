use std::time::Duration;

use anyhow::{bail, Result};
use etcd_client::{Client, Compare, CompareOp, Txn, TxnOp};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpStream, sync::watch, time::interval};
use tracing::{debug, error, info};
use zksync_config::configs::EtcdRegisterConfig;
use zksync_node_framework::{
    service::StopReceiver,
    task::{Task, TaskId},
    wiring_layer::{WiringError, WiringLayer},
    IntoContext,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub state_type: u64,
    pub address: String,
    pub port: u64,
    pub node_type: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum NodeType {
    State = 1,
    Archive = 2,
}

#[derive(Debug, Clone, Copy)]
pub enum StateType {
    Delay = 1,
}

pub struct Register {
    etcd_cfg: EtcdRegisterConfig,
    etcd_client: Client,
    key: String,
    value: String,
}

impl Register {
    async fn register(&mut self) -> Result<()> {
        self.etcd_client
            .put(self.key.clone(), self.value.clone(), None)
            .await?;
        info!(target: "register", "register key:{}, success", self.key);
        Ok(())
    }

    async fn unregister(&mut self) -> Result<()> {
        self.etcd_client.delete(self.key.clone(), None).await?;
        info!(target: "register", "unregister key:{} success", self.key);
        Ok(())
    }

    pub async fn new(
        chain_id: u64,
        version: String,
        etcd_cfg: EtcdRegisterConfig,
        is_archive: bool,
    ) -> Result<Self> {
        let etcd_client = Client::connect(&etcd_cfg.endpoints, None).await?;
        let meta = etcd_cfg.meta.clone();
        if meta.is_empty() {
            bail!("meta is empty");
        }
        let ip_host = meta.split(':').collect::<Vec<&str>>();
        if ip_host.len() != 2 {
            bail!("meta format error");
        }
        let ip = ip_host[0];
        let port = ip_host[1].parse::<u64>()?;
        let key = if version.is_empty() {
            format!("{chain_id}/nodes/{ip}_{port}")
        } else {
            format!("{chain_id}/{version}/nodes/{ip}_{port}")
        };
        let value = serde_json::to_string(&NodeInfo {
            state_type: StateType::Delay as u64,
            address: ip.to_string(),
            port,
            node_type: if is_archive {
                NodeType::Archive
            } else {
                NodeType::State
            } as u64,
        })?;

        Ok(Self {
            etcd_cfg,
            etcd_client,
            key,
            value,
        })
    }

    pub async fn start(mut self) -> Result<watch::Sender<()>> {
        let (tx, mut rx) = watch::channel(());
        let keep_alive_interval = Duration::from_millis(self.etcd_cfg.keep_alive_interval_ms);
        let mut interval = interval(keep_alive_interval);
        self.register().await?;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        if let Err(e) = self.unregister().await {
                            error!(target: "register", "unregister error: {e}");
                        }
                        break;
                    }
                    _ = interval.tick() => {
                        let txn = Txn::new()
                            .when(vec![Compare::version(self.key.clone(), CompareOp::Equal, 0)])
                            .and_then(vec![TxnOp::put(self.key.clone(), self.value.clone(), None)])
                            .or_else(vec![]);

                        match self.etcd_client.txn(txn).await {
                            Result::Ok(resp) => {
                                if resp.succeeded() {
                                    info!(target: "register", "register key:{}, success", self.key);
                                } else {
                                    debug!(target: "register", "key:{} already exists, skip registration", self.key);
                                }
                            }
                            Result::Err(e) => {
                                error!(target: "register", "register error: {e}");
                            }
                        }
                    }
                }
            }
        });
        Ok(tx)
    }
}

pub async fn register_build(
    chain_id: u64,
    version: String,
    etcd_cfg: Option<EtcdRegisterConfig>,
    is_archive: bool,
) -> Result<watch::Sender<()>> {
    if let Some(etcd_cfg) = etcd_cfg {
        let register = Register::new(chain_id, version, etcd_cfg, is_archive).await?;
        let register_handle = register.start().await?;
        Ok(register_handle)
    } else {
        Ok(tokio::sync::watch::channel(()).0)
    }
}

pub async fn wait_for_api_ready(meta: &str) -> Result<()> {
    let ip_host = meta.split(':').collect::<Vec<&str>>();
    if ip_host.len() != 2 {
        bail!("meta format error");
    }
    let addr = format!("{}:{}", ip_host[0], ip_host[1]);
    loop {
        match TcpStream::connect(&addr).await {
            Ok(_) => return Ok(()),
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
}

#[derive(Debug)]
pub struct EtcdRegisterLayer {
    pub chain_id: u64,
    pub version: String,
    pub etcd_cfg: Option<EtcdRegisterConfig>,
    pub is_archive: bool,
}

#[derive(Debug, IntoContext)]
pub struct Output {
    #[context(task)]
    pub register_task: Option<EtcdRegisterTask>,
}

#[async_trait::async_trait]
impl WiringLayer for EtcdRegisterLayer {
    type Input = ();
    type Output = Output;

    fn layer_name(&self) -> &'static str {
        "etcd_register_layer"
    }

    async fn wire(self, _input: Self::Input) -> Result<Self::Output, WiringError> {
        let register_task = self
            .etcd_cfg
            .as_ref()
            .filter(|cfg| cfg.enabled)
            .map(|cfg| EtcdRegisterTask {
                chain_id: self.chain_id,
                version: self.version.clone(),
                etcd_cfg: cfg.clone(),
                is_archive: self.is_archive,
            });
        Ok(Output { register_task })
    }
}

#[derive(Debug)]
pub struct EtcdRegisterTask {
    chain_id: u64,
    version: String,
    etcd_cfg: EtcdRegisterConfig,
    is_archive: bool,
}

#[async_trait::async_trait]
impl Task for EtcdRegisterTask {
    fn id(&self) -> TaskId {
        "etcd_register".into()
    }

    async fn run(self: Box<Self>, mut stop_receiver: StopReceiver) -> Result<()> {
        wait_for_api_ready(&self.etcd_cfg.meta).await?;
        let handle = register_build(
            self.chain_id,
            self.version.clone(),
            Some(self.etcd_cfg.clone()),
            self.is_archive,
        )
        .await?;
        stop_receiver.0.changed().await?;
        let _ = handle.send(());
        Ok(())
    }
}
