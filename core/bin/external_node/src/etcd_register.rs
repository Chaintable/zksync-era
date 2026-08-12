use std::time::Duration;

use anyhow::{bail, Result};
use etcd_client::{Client, PutOptions};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpStream,
    sync::watch,
    time::{interval, MissedTickBehavior},
};
use tracing::{error, info};
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
    /// TTL (seconds) of the etcd lease the registration key is attached to.
    /// Derived from `keep_alive_interval_ms` so the lease always outlives a
    /// few missed keep-alive ticks but still expires promptly when the node dies.
    lease_ttl_secs: i64,
}

impl Register {
    /// Grants a fresh lease and writes the registration key attached to it.
    ///
    /// Attaching the key to a lease is what gives the gateway a liveness signal:
    /// if this node stops renewing the lease (crash, network partition, OOM),
    /// etcd expires the lease and removes the key automatically, so the gateway
    /// stops routing to a dead backend. Returns the granted lease id.
    async fn register(&mut self) -> Result<i64> {
        let lease_id = self
            .etcd_client
            .lease_grant(self.lease_ttl_secs, None)
            .await?
            .id();
        self.etcd_client
            .put(
                self.key.clone(),
                self.value.clone(),
                Some(PutOptions::new().with_lease(lease_id)),
            )
            .await?;
        info!(
            target: "register",
            "register key:{} with lease {lease_id} (ttl {}s), success",
            self.key, self.lease_ttl_secs
        );
        Ok(lease_id)
    }

    /// Renews the lease (refreshing its TTL) and re-asserts the key/value.
    ///
    /// The keep-alive renews the TTL so the key survives; the `put` keeps the
    /// advertised `NodeInfo` current and re-creates the key should it have been
    /// removed out-of-band, while re-binding it to the live lease.
    async fn keep_alive(&mut self, lease_id: i64) -> Result<()> {
        let (mut keeper, mut stream) = self.etcd_client.lease_keep_alive(lease_id).await?;
        keeper.keep_alive().await?;
        // Drain the single keep-alive ack; a TTL of 0 means the lease is gone.
        match stream.message().await? {
            Some(resp) if resp.ttl() <= 0 => bail!("lease {lease_id} expired"),
            _ => {}
        }
        self.etcd_client
            .put(
                self.key.clone(),
                self.value.clone(),
                Some(PutOptions::new().with_lease(lease_id)),
            )
            .await?;
        Ok(())
    }

    /// Revokes the lease (which deletes every key attached to it) on graceful
    /// shutdown. Falls back to an explicit delete in case the revoke fails.
    async fn unregister(&mut self, lease_id: i64) {
        if let Err(e) = self.etcd_client.lease_revoke(lease_id).await {
            error!(target: "register", "lease {lease_id} revoke error: {e}");
        }
        if let Err(e) = self.etcd_client.delete(self.key.clone(), None).await {
            error!(target: "register", "unregister delete error: {e}");
        }
        info!(target: "register", "unregister key:{} success", self.key);
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

        // Give the lease enough room to outlive a few missed keep-alive ticks
        // (network blips) while still expiring quickly on real node death.
        let lease_ttl_secs =
            ((etcd_cfg.keep_alive_interval_ms.saturating_mul(3)) / 1000).max(2) as i64;

        Ok(Self {
            etcd_cfg,
            etcd_client,
            key,
            value,
            lease_ttl_secs,
        })
    }

    pub async fn start(mut self) -> Result<watch::Sender<()>> {
        let (tx, mut rx) = watch::channel(());
        let keep_alive_interval = Duration::from_millis(self.etcd_cfg.keep_alive_interval_ms);
        tokio::spawn(async move {
            let mut interval = interval(keep_alive_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            // `None` means "not currently registered"; the first (immediate) tick
            // performs the initial registration. Transient etcd errors are logged
            // and retried on the next tick instead of taking the whole node down.
            let mut lease_id: Option<i64> = None;
            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        if let Some(id) = lease_id {
                            self.unregister(id).await;
                        }
                        break;
                    }
                    _ = interval.tick() => {
                        match lease_id {
                            None => match self.register().await {
                                Ok(id) => lease_id = Some(id),
                                Err(e) => error!(target: "register", "register error: {e}"),
                            },
                            Some(id) => {
                                if let Err(e) = self.keep_alive(id).await {
                                    // Lost the lease/connection: drop it and re-register
                                    // (with a brand-new lease) on the next tick.
                                    error!(target: "register", "keep-alive error: {e}, will re-register");
                                    lease_id = None;
                                }
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
        let register_task =
            self.etcd_cfg
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
