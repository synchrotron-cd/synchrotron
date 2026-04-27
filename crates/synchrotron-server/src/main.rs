use std::sync::Arc;

use tracing::info;

use synchrotron_core::metrics::Metrics;
use synchrotron_core::telemetry::{init as telemetry_init, TelemetryConfig};
use synchrotron_server::api;
use synchrotron_server::config::ServerConfig;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry_init(TelemetryConfig::default())?;

    let config = ServerConfig::default();

    // Initialize database (validates schema on startup)
    let _db = synchrotron_core::db::Database::open(&config.db_path)?;
    info!("database initialized");

    let metrics = Arc::new(Metrics::new());
    let app = api::router().merge(api::metrics_router(metrics));

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    info!("listening on {}", config.listen_addr);
    axum::serve(listener, app).await?;

    Ok(())
}
