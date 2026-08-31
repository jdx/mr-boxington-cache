use clap::{Parser, ValueEnum};
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum StorageKind {
    Filesystem,
    S3,
}

#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Config {
    #[arg(long, env = "MBX_CACHE_LISTEN", default_value = "0.0.0.0:8080")]
    pub listen: SocketAddr,

    #[arg(
        long,
        env = "MBX_CACHE_STORAGE",
        value_enum,
        default_value = "filesystem"
    )]
    pub storage: StorageKind,

    #[arg(long, env = "MBX_CACHE_DATA_DIR", default_value = "/var/lib/mbx-cache")]
    pub data_dir: PathBuf,

    #[arg(long, env = "MBX_CACHE_DATABASE_URL", default_value = "memory://")]
    pub database_url: String,

    /// Maximum concurrent PostgreSQL connections used for metadata requests.
    #[arg(long, env = "MBX_CACHE_DATABASE_MAX_CONNECTIONS", default_value_t = 32)]
    pub database_max_connections: u32,

    /// Sweep metadata older than this many days, then exit without serving.
    ///
    /// R2 expires the objects themselves through a lifecycle rule, which leaves
    /// their rows behind. Keep this longer than the lifecycle age so storage
    /// deletes first and metadata follows; the reverse drops rows for objects
    /// that still exist and costs needless recompiles.
    #[arg(long, env = "MBX_CACHE_SWEEP_METADATA_OLDER_THAN_DAYS")]
    pub sweep_metadata_older_than_days: Option<u32>,

    #[arg(long, env = "MBX_CACHE_S3_BUCKET")]
    pub s3_bucket: Option<String>,

    #[arg(long, env = "MBX_CACHE_S3_PREFIX", default_value = "v1")]
    pub s3_prefix: String,

    #[arg(long, env = "MBX_CACHE_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    #[arg(long, env = "MBX_CACHE_S3_REGION", default_value = "us-east-1")]
    pub s3_region: String,

    #[arg(long, env = "MBX_CACHE_S3_PATH_STYLE", default_value_t = false)]
    pub s3_path_style: bool,

    /// JSON array of token grants. See README.md for the schema.
    #[arg(long, env = "MBX_CACHE_TOKENS_JSON", hide_env_values = true)]
    pub tokens_json: Option<String>,

    /// JSON array of trusted OIDC providers and authorization rules. See README.md.
    #[arg(long, env = "MBX_CACHE_OIDC_PROVIDERS_JSON", hide_env_values = true)]
    pub oidc_providers_json: Option<String>,

    #[arg(long, env = "MBX_CACHE_ALLOW_ANONYMOUS", default_value_t = false)]
    pub allow_anonymous: bool,

    #[arg(long, env = "MBX_CACHE_MAX_BLOB_BYTES", default_value_t = 5 * 1024 * 1024 * 1024_u64)]
    pub max_blob_bytes: u64,
}
