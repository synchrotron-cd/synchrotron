use std::sync::Arc;

use tracing::{info, warn};

use synchrotron_core::metrics::Metrics;
use synchrotron_core::telemetry::{init as telemetry_init, TelemetryConfig};
use synchrotron_server::api;
use synchrotron_server::config::{reload, Config, ConfigHandle};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry_init(TelemetryConfig::default())?;

    // Resolve the config path from CLI arg, then $SYNCHROTRON_CONFIG,
    // else fall back to a synthesized default. Only file-backed
    // handles get a SIGHUP reloader.
    let path = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("SYNCHROTRON_CONFIG").ok());
    let handle = match path {
        Some(p) => {
            let h = ConfigHandle::load(&p)?;
            #[cfg(unix)]
            reload::spawn_sighup_reloader(h.clone());
            info!(path = %p, "config loaded");
            h
        }
        None => {
            warn!("no config path supplied; using built-in defaults");
            ConfigHandle::from_config(Config::default(), "<default>")
        }
    };

    let cfg = handle.current().await;

    let _db = synchrotron_core::db::Database::open(&cfg.server.db_path)?;
    info!("database initialized");

    let metrics = Arc::new(Metrics::new());
    let readiness = api::ReadinessGate::new();
    let app = api::router()
        .merge(api::metrics_router(metrics))
        .merge(api::probes_router(readiness.clone()));

    let listener = tokio::net::TcpListener::bind(&cfg.server.listen_addr).await?;
    info!("listening on {}", cfg.server.listen_addr);
    // Initialization complete: DB open, config loaded, listener
    // bound. Flip readiness so kubelet routes traffic.
    readiness.mark_ready();
    axum::serve(listener, app).await?;

    Ok(())
}
