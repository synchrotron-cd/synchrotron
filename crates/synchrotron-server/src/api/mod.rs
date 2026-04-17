pub mod health;

use axum::{routing::get, Router};

pub fn router() -> Router {
    Router::new().route("/api/v1/health", get(health::health_check))
}
