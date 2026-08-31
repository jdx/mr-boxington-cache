mod auth;
mod config;
mod metadata;
mod metrics;
mod model;
mod pack;
mod server;
mod storage;

use anyhow::Result;
use clap::Parser;
use config::Config;
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

    let metadata =
        metadata::from_url(&config.database_url, config.database_max_connections).await?;
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

    let blobs = storage::from_config(&config).await?;
    let state = server::AppState::new(
        blobs,
        metadata,
        auth::Authorizer::new(
            config.tokens_json.as_deref(),
            config.oidc_providers_json.as_deref(),
            config.allow_anonymous,
        )
        .await?,
        config.max_blob_bytes,
    );
    let app = server::router(state);
    let listener = TcpListener::bind(config.listen).await?;
    info!(address = %config.listen, "mbx-cache listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
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
