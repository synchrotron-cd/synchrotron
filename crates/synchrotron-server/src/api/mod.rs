pub mod health;
pub mod metrics;
pub mod webhooks;

use std::sync::Arc;

use axum::{routing::get, Router};
use synchrotron_core::metrics::Metrics;

pub use webhooks::{WebhookSecrets, WebhookState};

pub fn router() -> Router {
    Router::new().route("/api/v1/health", get(health::health_check))
}

/// Build a `/metrics` router exposing the Prometheus/OpenMetrics endpoint.
pub fn metrics_router(metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/metrics", get(metrics::metrics_handler))
        .with_state(metrics)
}

/// Build the full API router with webhook support wired to the given
/// state. Once other subsystems (reconciler, CLI API) land they plug
/// into this same builder.
pub fn router_with_webhooks(webhook_state: WebhookState) -> Router {
    Router::new()
        .route("/api/v1/health", get(health::health_check))
        .merge(webhooks::router(webhook_state))
}
