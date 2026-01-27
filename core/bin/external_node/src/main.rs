use std::{collections::HashSet, env, str::FromStr};

use anyhow::Context as _;
use clap::Parser;
use node_builder::ExternalNodeBuilder;
use smart_config::Prefixed;
use zksync_config::{
    cli::ConfigArgs,
    configs::EtcdRegisterConfig,
    sources::{ConfigFilePaths, ConfigSources},
};
use zksync_node_api_server::web3::set_pipeline_node_role;
use zksync_types::L1BatchNumber;

use crate::config::{generate_consensus_secrets, ExternalNodeConfig, LocalConfig};

mod config;
mod etcd_register;
mod metadata;
mod metrics;
mod node_builder;
#[cfg(test)]
mod tests;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Debug, clap::Subcommand)]
enum Command {
    /// Generates consensus secret keys to use in the secrets file.
    /// Prints the keys to the stdout, you need to copy the relevant keys into your secrets file.
    GenerateSecrets,
    /// Configuration-related tools.
    Config(ConfigArgs),
    /// Reverts the node state to the end of the specified L1 batch and then exits.
    Revert {
        /// The last L1 batch to be retained after the revert.
        l1_batch: L1BatchNumber,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum RunMode {
    /// Full external node mode: sync + re-execute blocks and persist state to Postgres.
    Full,
    /// RPC-only mode: connect to an existing Postgres instance and serve RPC without syncing / re-execution.
    Rpc,
}

/// External node for ZKsync Era.
#[derive(Debug, Parser)]
#[command(author = "Matter Labs", version)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Enables consensus-based syncing instead of JSON-RPC based one. This is an experimental and incomplete feature;
    /// do not use unless you know what you're doing.
    #[arg(long)]
    enable_consensus: bool,

    /// Node running mode.
    #[arg(long, value_enum, default_value_t = RunMode::Full, env = "EN_NODE_MODE")]
    mode: RunMode,

    /// Comma-separated list of components to launch.
    #[arg(long, default_value = "all")]
    components: ComponentsToRun,
    /// Path to the yaml config. If set, it will be used instead of env vars.
    #[arg(long)]
    config_path: Option<std::path::PathBuf>,
    /// Path to the yaml with secrets. If set, it will be used instead of env vars.
    #[arg(long, requires = "config_path", requires = "external_node_config_path")]
    secrets_path: Option<std::path::PathBuf>,
    /// Path to the yaml with external node specific configuration. If set, it will be used instead of env vars.
    #[arg(long, requires = "config_path", requires = "secrets_path")]
    external_node_config_path: Option<std::path::PathBuf>,
    /// Path to the yaml with consensus config. If set, it will be used instead of env vars.
    #[arg(
        long,
        requires = "config_path",
        requires = "secrets_path",
        requires = "external_node_config_path",
        requires = "enable_consensus"
    )]
    consensus_path: Option<std::path::PathBuf>,

    // ========== Etcd Register CLI Options ==========
    /// Enable etcd registration for service discovery.
    #[arg(long, env = "EN_ETCD_REGISTER_ENABLED")]
    etcd_register_enabled: Option<bool>,

    /// Etcd endpoints (comma-separated list, e.g., "http://etcd1:2379,http://etcd2:2379").
    #[arg(long, env = "EN_ETCD_REGISTER_ENDPOINTS", value_delimiter = ',')]
    etcd_register_endpoints: Option<Vec<String>>,

    /// Node meta in "ip:port" format (API server address for service discovery).
    #[arg(long, env = "EN_ETCD_REGISTER_META")]
    etcd_register_meta: Option<String>,

    /// Keep-alive interval in milliseconds for etcd lease.
    #[arg(long, env = "EN_ETCD_REGISTER_KEEP_ALIVE_INTERVAL_MS")]
    etcd_register_keep_alive_interval_ms: Option<u64>,

    /// Optional version segment for the etcd key path.
    #[arg(long, env = "EN_ETCD_REGISTER_VERSION")]
    etcd_register_version: Option<String>,
}

impl Cli {
    fn config_sources(&self, env_prefix: Option<&str>) -> anyhow::Result<ConfigSources> {
        let config_file_paths = ConfigFilePaths {
            general: self.config_path.clone(),
            secrets: self.secrets_path.clone(),
            external_node: self.external_node_config_path.clone(),
            consensus: if let Some(path) = self.consensus_path.clone() {
                Some(path)
            } else if let Ok(path) = env::var("EN_CONSENSUS_CONFIG_PATH") {
                Some(path.into())
            } else {
                None
            },
            ..ConfigFilePaths::default()
        };
        let mut config_sources = config_file_paths.into_config_sources(env_prefix)?;
        // Legacy compatibility: read consensus secrets from one more source.
        if let Ok(path) = env::var("EN_CONSENSUS_SECRETS_PATH") {
            let yaml = ConfigFilePaths::read_yaml(path.as_ref())?;
            config_sources.push(Prefixed::new(yaml, "consensus"));
        }
        Ok(config_sources)
    }

    /// Returns true if any etcd CLI arguments were provided.
    fn has_etcd_cli_args(&self) -> bool {
        self.etcd_register_enabled.is_some()
            || self.etcd_register_endpoints.is_some()
            || self.etcd_register_meta.is_some()
            || self.etcd_register_keep_alive_interval_ms.is_some()
            || self.etcd_register_version.is_some()
    }

    /// Applies etcd CLI arguments to the config, overriding values from env/file if provided.
    fn apply_etcd_cli_args(&self, config: &mut ExternalNodeConfig) {
        if !self.has_etcd_cli_args() {
            return;
        }

        // Get or create the etcd config
        let etcd_config = config.local.etcd_register.get_or_insert_with(EtcdRegisterConfig::default);

        // Override with CLI args if provided
        if let Some(enabled) = self.etcd_register_enabled {
            etcd_config.enabled = enabled;
        }
        if let Some(ref endpoints) = self.etcd_register_endpoints {
            etcd_config.endpoints = endpoints.clone();
        }
        if let Some(ref meta) = self.etcd_register_meta {
            etcd_config.meta = meta.clone();
        }
        if let Some(keep_alive_interval_ms) = self.etcd_register_keep_alive_interval_ms {
            etcd_config.keep_alive_interval_ms = keep_alive_interval_ms;
        }
        if let Some(ref version) = self.etcd_register_version {
            etcd_config.version = version.clone();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Hash, Eq)]
pub enum Component {
    HttpApi,
    WsApi,
    Tree,
    TreeApi,
    TreeFetcher,
    Core,
    DataAvailabilityFetcher,
}

impl Component {
    fn components_from_str(s: &str) -> anyhow::Result<&[Component]> {
        match s {
            "api" => Ok(&[Component::HttpApi, Component::WsApi]),
            "http_api" => Ok(&[Component::HttpApi]),
            "ws_api" => Ok(&[Component::WsApi]),
            "tree" => Ok(&[Component::Tree]),
            "tree_api" => Ok(&[Component::TreeApi]),
            "tree_fetcher" => Ok(&[Component::TreeFetcher]),
            "da_fetcher" => Ok(&[Component::DataAvailabilityFetcher]),
            "core" => Ok(&[Component::Core]),
            "all" => Ok(&[
                Component::HttpApi,
                Component::WsApi,
                Component::Tree,
                Component::Core,
            ]),
            other => Err(anyhow::anyhow!("{other} is not a valid component name")),
        }
    }
}

#[derive(Debug, Clone)]
struct ComponentsToRun(HashSet<Component>);

impl FromStr for ComponentsToRun {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let components = s
            .split(',')
            .try_fold(HashSet::new(), |mut acc, component_str| {
                let components = Component::components_from_str(component_str.trim())?;
                acc.extend(components);
                Ok::<_, Self::Err>(acc)
            })?;
        Ok(Self(components))
    }
}

fn tokio_runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

fn main() -> anyhow::Result<()> {
    let runtime = tokio_runtime()?;

    // Initial setup.
    let mut opt = Cli::parse();
    let schema = LocalConfig::schema().context("Internal error: cannot build config schema")?;
    let config_sources = opt.config_sources(Some("EN_"))?;

    let observability = {
        // Observability initialization should be performed within tokio context.
        let _rt_guard = runtime.enter();
        config_sources.observability()?.install()?
    };
    let repo = config_sources.build_repository(&schema);

    let mut revert_to_l1_batch = None;
    if let Some(cmd) = opt.command.take() {
        match cmd {
            Command::GenerateSecrets => {
                generate_consensus_secrets();
                return Ok(());
            }
            Command::Config(config_args) => {
                return config_args.run(repo.into(), "EN_");
            }
            Command::Revert { l1_batch } => {
                // We need to delay revert to after the config is fully read.
                revert_to_l1_batch = Some(l1_batch);
            }
        }
    }

    let mut config = ExternalNodeConfig::new(repo, opt.enable_consensus)?;

    // Apply etcd CLI arguments (overrides env/file config if provided)
    opt.apply_etcd_cli_args(&mut config);

    if let Some(l1_batch) = revert_to_l1_batch {
        anyhow::ensure!(
            opt.mode == RunMode::Full,
            "Revert is only supported in full mode (sync + re-exec)"
        );
        let node = ExternalNodeBuilder::on_runtime(runtime, config).build_for_revert(l1_batch)?;
        node.run(observability)?;
        return Ok(());
    }

    let node = ExternalNodeBuilder::on_runtime(runtime, config)
        .with_mode(opt.mode)
        .build(opt.components.0.into_iter().collect())?;
    let role = if opt.mode == RunMode::Rpc { "replica" } else { "writer" };
    set_pipeline_node_role(role);
    node.run(observability)?;
    Ok(())
}
