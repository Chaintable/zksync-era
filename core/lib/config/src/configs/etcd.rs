use smart_config::{
    de::Delimited, DescribeConfig, DeserializeConfig,
};

#[derive(Debug, Clone, PartialEq, DescribeConfig, DeserializeConfig)]
#[config(derive(Default))]
pub struct EtcdRegisterConfig {
    /// Whether etcd register is enabled.
    #[config(default)]
    pub enabled: bool,
    /// Etcd endpoints (comma-separated list in env vars).
    #[config(default, with = Delimited(","))]
    pub endpoints: Vec<String>,
    /// Keep-alive interval in milliseconds.
    #[config(default_t = 10_000)]
    pub keep_alive_interval_ms: u64,
    /// Node meta in "ip:port" format (API server address).
    #[config(default)]
    pub meta: String,
    /// Optional version segment for the key path.
    #[config(default)]
    pub version: String,
}
