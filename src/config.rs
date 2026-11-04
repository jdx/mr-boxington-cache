use clap::{Parser, ValueEnum};
use std::net::SocketAddr;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum StorageKind {
    Azure,
    Filesystem,
    Gcs,
    S3,
}

#[derive(Debug, Parser)]
#[command(version, about)]
pub struct Config {
    #[arg(long, env = "MBX_CACHE_LISTEN", default_value = "0.0.0.0:8080")]
    pub listen: SocketAddr,

    /// Path to a PEM certificate chain file. With `--tls-key`, serve HTTPS
    /// instead of HTTP.
    #[arg(long, env = "MBX_CACHE_TLS_CERT", requires = "tls_key")]
    pub tls_cert: Option<PathBuf>,

    /// Path to the PEM private key file for `--tls-cert`.
    #[arg(long, env = "MBX_CACHE_TLS_KEY", requires = "tls_cert")]
    pub tls_key: Option<PathBuf>,

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
    #[arg(
        long,
        env = "MBX_CACHE_DATABASE_MAX_CONNECTIONS",
        default_value_t = 32,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    pub database_max_connections: u32,

    /// Seconds a metadata request waits for a free PostgreSQL connection
    /// before it fails. It also bounds the first connection at startup.
    #[arg(
        long,
        env = "MBX_CACHE_DATABASE_ACQUIRE_TIMEOUT",
        default_value_t = 30,
        value_parser = clap::value_parser!(u64).range(1..)
    )]
    pub database_acquire_timeout: u64,

    /// Sweep metadata older than this many days, then exit without serving.
    ///
    /// Object storage expires blobs through a lifecycle rule, which leaves
    /// their rows behind. Keep this longer than the lifecycle age so storage
    /// deletes first and metadata follows; the reverse drops rows for objects
    /// that still exist and costs needless recompiles.
    #[arg(long, env = "MBX_CACHE_SWEEP_METADATA_OLDER_THAN_DAYS")]
    pub sweep_metadata_older_than_days: Option<u32>,

    #[arg(long, env = "MBX_CACHE_AZURE_ACCOUNT")]
    pub azure_account: Option<String>,

    #[arg(long, env = "MBX_CACHE_AZURE_CONTAINER")]
    pub azure_container: Option<String>,

    #[arg(long, env = "MBX_CACHE_AZURE_PREFIX", default_value = "v1")]
    pub azure_prefix: String,

    /// Azure credential type. `auto` discovers credentials; production VMs use
    /// `managed_identity` so no storage key is present in the service environment.
    #[arg(long, env = "MBX_CACHE_AZURE_CREDENTIAL_TYPE", default_value = "auto")]
    pub azure_credential_type: String,

    /// Override the Azure Blob endpoint, primarily for compatible emulators.
    #[arg(long, env = "MBX_CACHE_AZURE_ENDPOINT")]
    pub azure_endpoint: Option<String>,

    #[arg(long, env = "MBX_CACHE_AZURE_ALLOW_HTTP", default_value_t = false)]
    pub azure_allow_http: bool,

    #[arg(long, env = "MBX_CACHE_GCS_BUCKET")]
    pub gcs_bucket: Option<String>,

    #[arg(long, env = "MBX_CACHE_GCS_PREFIX", default_value = "v1")]
    pub gcs_prefix: String,

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

    /// JSON array of namespace patterns that may be read without authentication.
    #[arg(
        long,
        env = "MBX_CACHE_ANONYMOUS_READ_NAMESPACES_JSON",
        hide_env_values = true
    )]
    pub anonymous_read_namespaces_json: Option<String>,

    #[arg(long, env = "MBX_CACHE_MAX_BLOB_BYTES", default_value_t = 5 * 1024 * 1024 * 1024_u64)]
    pub max_blob_bytes: u64,

    /// Blobs of one uploaded pack stored at the same time. Each store is a
    /// round trip to object storage, so storing them in turn leaves a large
    /// pack waiting on latency rather than bandwidth.
    #[arg(
        long,
        env = "MBX_CACHE_PACK_STORE_CONCURRENCY",
        default_value_t = 8,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    pub pack_store_concurrency: u32,

    /// Bytes all uploads may hold spooled in the temp directory at once; 0
    /// leaves it unbounded. An upload past it gets 503 and Retry-After.
    #[arg(long, env = "MBX_CACHE_MAX_SPOOL_BYTES", default_value_t = 0)]
    pub max_spool_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_an_empty_database_pool() {
        assert!(Config::try_parse_from(["mbx-cache", "--database-max-connections", "0"]).is_err());
    }

    #[test]
    fn rejects_a_zero_database_acquire_timeout() {
        assert!(Config::try_parse_from(["mbx-cache", "--database-acquire-timeout", "0"]).is_err());
    }

    #[test]
    fn rejects_a_tls_cert_without_a_key() {
        assert!(Config::try_parse_from(["mbx-cache", "--tls-cert", "cert.pem"]).is_err());
        assert!(Config::try_parse_from(["mbx-cache", "--tls-key", "key.pem"]).is_err());
    }

    #[test]
    fn rejects_a_zero_pack_store_concurrency() {
        assert!(Config::try_parse_from(["mbx-cache", "--pack-store-concurrency", "0"]).is_err());
    }
}
