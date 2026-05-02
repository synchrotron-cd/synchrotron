pub mod apps;
pub mod clusters;
pub mod errors;
pub mod health;
pub mod metrics;
pub mod openapi;
pub mod repos;
pub mod webhooks;

use std::sync::Arc;

use axum::{routing::get, Router};
use synchrotron_core::metrics::Metrics;
use tower_http::trace::TraceLayer;

pub use apps::AppsState;
pub use clusters::ClustersState;
pub use repos::ReposState;
pub use errors::{ApiError, ApiErrorBody, ApiErrorEnvelope, ErrorCode};
pub use health::{probes_router, ReadinessGate};
pub use openapi::ApiDoc;
pub use webhooks::{WebhookSecrets, WebhookState};

/// Build the public REST API router.
///
/// Layers a [`TraceLayer`] so every request gets a structured span and
/// an access log line via the global tracing subscriber.
pub fn router() -> Router {
    Router::new()
        .route("/api/v1/health", get(health::health_check))
        .merge(openapi::router())
        .layer(TraceLayer::new_for_http())
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

/// Build the API router wired to the apps backend (DB + event bus).
/// Includes the public `/api/v1/health`, `/openapi.json`, and the
/// full apps surface.
pub fn router_with_apps(apps_state: AppsState) -> Router {
    let db = apps_state.db.clone();
    let clusters_state = ClustersState::new(db.clone());
    let repos_state = ReposState::new(db);
    Router::new()
        .route("/api/v1/health", get(health::health_check))
        .merge(openapi::router())
        .merge(apps::router(apps_state))
        .merge(clusters::router(clusters_state))
        .merge(repos::router(repos_state))
        .layer(TraceLayer::new_for_http())
}

/// Single-feature router used by integration tests that want to
/// exercise only the clusters surface.
pub fn router_with_clusters(clusters_state: ClustersState) -> Router {
    Router::new()
        .route("/api/v1/health", get(health::health_check))
        .merge(openapi::router())
        .merge(clusters::router(clusters_state))
        .layer(TraceLayer::new_for_http())
}

/// Single-feature router used by integration tests that want to
/// exercise only the repos surface.
pub fn router_with_repos(repos_state: ReposState) -> Router {
    Router::new()
        .route("/api/v1/health", get(health::health_check))
        .merge(openapi::router())
        .merge(repos::router(repos_state))
        .layer(TraceLayer::new_for_http())
}
