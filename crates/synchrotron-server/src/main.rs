use tracing::info;
use tracing_subscriber::EnvFilter;

mod api;
mod config;

use config::ServerConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = ServerConfig::default();

    // Initialize database (validates schema on startup)
    let _db = synchrotron_core::db::Database::open(&config.db_path)?;
    info!("database initialized");

    let app = api::router();

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    info!("listening on {}", config.listen_addr);
    axum::serve(listener, app).await?;

    Ok(())
}
