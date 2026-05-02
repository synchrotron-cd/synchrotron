//! OpenAPI document and `/openapi.json` route.
//!
//! The spec is generated from code via [`utoipa`]: handler functions
//! carry `#[utoipa::path]` annotations and response/request structs
//! derive [`utoipa::ToSchema`]. Adding a new endpoint is a matter of
//! annotating the handler and listing it in the [`ApiDoc`] derive
//! below — no hand-edited YAML.
//!
//! Two ways to consume the spec:
//!
//! 1. **At runtime:** `GET /openapi.json` returns the live document.
//!    The CLI uses this to bind operations against whichever server
//!    version it's pointed at, so the contract is the binary's, not a
//!    checked-in artifact that can drift.
//! 2. **At build time:** [`ApiDoc::openapi`] gives the same struct,
//!    available to test code (lint, contract diffs) and generators.

use axum::{response::Json, routing::get, Router};
pub use utoipa::OpenApi;

use crate::api::apps::{
    AppView, CreateAppRequest, DiffResponse, HistoryEntry, HistoryResponse, ListAppsResponse,
    RollbackRequest, RollbackResponse, SyncAcceptedResponse, UpdateAppRequest,
};
use crate::api::errors::{ApiErrorBody, ApiErrorEnvelope, ErrorCode};
use crate::api::health::HealthResponse;

/// Code-driven OpenAPI document for the public REST API.
///
/// Schema list and path list are kept in sync by hand: adding a new
/// `#[utoipa::path]` handler means appending it to `paths(...)`, and
/// adding a new schema means appending it to
/// `components(schemas(...))`. utoipa errors at compile time if a
/// path references a schema that isn't registered.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Synchrotron CD API",
        description = "REST API for the Synchrotron-CD GitOps controller.",
        license(name = "Apache-2.0"),
    ),
    paths(
        crate::api::health::health_check,
        crate::api::apps::list_apps,
        crate::api::apps::create_app,
        crate::api::apps::get_app,
        crate::api::apps::update_app,
        crate::api::apps::delete_app,
        crate::api::apps::sync_app,
        crate::api::apps::diff_app,
        crate::api::apps::rollback_app,
        crate::api::apps::history_app,
    ),
    components(schemas(
        HealthResponse,
        ApiErrorEnvelope,
        ApiErrorBody,
        ErrorCode,
        AppView,
        CreateAppRequest,
        UpdateAppRequest,
        ListAppsResponse,
        SyncAcceptedResponse,
        RollbackRequest,
        RollbackResponse,
        DiffResponse,
        HistoryEntry,
        HistoryResponse,
    )),
    tags(
        (name = "system", description = "Server-level health and metadata"),
        (name = "apps", description = "Application CRUD and sync operations"),
    ),
)]
pub struct ApiDoc;

/// Handler for `GET /openapi.json`. Serializes the static
/// [`ApiDoc`] document at request time — utoipa caches internally so
/// this is cheap.
pub async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

pub fn router() -> Router {
    Router::new().route("/openapi.json", get(openapi_json))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn openapi_json_endpoint_returns_spec() {
        let app = router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/openapi.json")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["openapi"].as_str().unwrap_or(""), "3.1.0");
        assert_eq!(json["info"]["title"], "Synchrotron CD API");
        assert!(
            json["paths"]["/api/v1/health"]["get"].is_object(),
            "health endpoint must be in the spec; got: {}",
            json["paths"]
        );
        assert!(
            json["components"]["schemas"]["HealthResponse"].is_object(),
            "HealthResponse schema must be registered"
        );
        assert!(
            json["components"]["schemas"]["ApiErrorEnvelope"].is_object(),
            "ApiErrorEnvelope schema must be registered"
        );
    }

    #[test]
    fn api_doc_has_health_path() {
        let spec = ApiDoc::openapi();
        let paths = spec.paths.paths;
        assert!(
            paths.contains_key("/api/v1/health"),
            "registered paths: {:?}",
            paths.keys().collect::<Vec<_>>()
        );
    }
}
