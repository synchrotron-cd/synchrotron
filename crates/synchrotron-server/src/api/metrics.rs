use std::sync::Arc;

use axum::{extract::State, http::header, response::IntoResponse};
use synchrotron_core::metrics::Metrics;

pub async fn metrics_handler(State(metrics): State<Arc<Metrics>>) -> impl IntoResponse {
    let body = metrics.render();
    (
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        body,
    )
}
