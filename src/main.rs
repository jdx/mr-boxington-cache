mod auth;
mod config;
mod metadata;
mod metrics;
mod model;
mod pack;
mod server;
mod storage;

use anyhow::{Context, Result};
use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use config::Config;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::path::Path;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mbx_cache=info,tower_http=info".into()),
        )
        .json()
        .init();

    let metadata = metadata::from_url(
        &config.database_url,
        config.database_max_connections,
        std::time::Duration::from_secs(config.database_acquire_timeout),
    )
    .await?;
    // A sweep is an operator action, not part of serving: do it and exit rather
    // than deleting from under a live server.
    if let Some(days) = config.sweep_metadata_older_than_days {
        let swept = metadata.sweep(days).await?;
        info!(
            older_than_days = days,
            blobs = swept.blobs,
            action_results = swept.action_results,
            manifests = swept.manifests,
            "swept metadata"
        );
        return Ok(());
    }

    anyhow::ensure!(
        config.max_spool_bytes == 0 || config.max_spool_bytes >= config.max_blob_bytes,
        "--max-spool-bytes must be 0 or at least --max-blob-bytes, or the largest uploads can never be spooled"
    );
    let blobs = storage::from_config(&config).await?;
    let state = server::AppState::new(
        blobs,
        metadata,
        auth::Authorizer::new(
            config.tokens_json.as_deref(),
            config.oidc_providers_json.as_deref(),
            config.allow_anonymous,
            config.anonymous_read_namespaces_json.as_deref(),
        )
        .await?,
        config.max_blob_bytes,
        config.pack_store_concurrency as usize,
        config.max_spool_bytes,
    );
    let app = server::router(state);
    if let (Some(cert), Some(key)) = (&config.tls_cert, &config.tls_key) {
        let tls = tls_config(cert, key).with_context(|| {
            format!("load TLS cert {} and key {}", cert.display(), key.display())
        })?;
        let handle = axum_server::Handle::new();
        tokio::spawn({
            let handle = handle.clone();
            async move {
                shutdown_signal().await;
                handle.graceful_shutdown(None);
            }
        });
        info!(address = %config.listen, "mbx-cache listening with TLS");
        axum_server::bind_rustls(config.listen, tls)
            .handle(handle)
            .serve(app.into_make_service())
            .await?;
        return Ok(());
    }
    let listener = TcpListener::bind(config.listen).await?;
    info!(address = %config.listen, "mbx-cache listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn tls_config(cert: &Path, key: &Path) -> Result<RustlsConfig> {
    // rustls has both ring and aws-lc-rs enabled through other crates, so it
    // cannot pick a process default; name the provider for this server only.
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(
            CertificateDer::pem_file_iter(cert)?.collect::<Result<_, _>>()?,
            PrivateKeyDer::from_pem_file(key)?,
        )?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(RustlsConfig::from_config(Arc::new(config)))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}
