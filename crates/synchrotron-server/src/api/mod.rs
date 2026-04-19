pub mod health;
pub mod webhooks;

use axum::{routing::get, Router};

pub use webhooks::{WebhookSecrets, WebhookState};

pub fn router() -> Router {
    Router::new().route("/api/v1/health", get(health::health_check))
}

/// Build the full API router with webhook support wired to the given
/// state. Once other subsystems (reconciler, CLI API) land they plug
/// into this same builder.
pub fn router_with_webhooks(webhook_state: WebhookState) -> Router {
    Router::new()
        .route("/api/v1/health", get(health::health_check))
        .merge(webhooks::router(webhook_state))
}
