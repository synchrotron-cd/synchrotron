//! Cluster registration CRUD plus a connectivity check
//! (`POST /api/v1/clusters/{name}/check`).
//!
//! ## Secret handling
//!
//! `bearer_token` is accepted in create/update bodies and persisted in
//! the DB, but is **never** returned in API responses — the
//! [`ClusterView`] DTO simply omits it (rather than masking with
//! `"***"`) to make any leak immediately visible in client code that
//! happens to look it up.
//!
//! ## Connectivity check
//!
//! Builds a [`synchrotron_kube::ClusterConfig`] from the persisted
//! registration and calls [`KubeClient::connect`] +
//! [`apiserver_version`](synchrotron_kube::KubeClient::apiserver_version).
//! That covers kubeconfig parse / context resolution / TLS handshake /
//! one round-trip — enough to catch the common misconfigurations
//! without standing up a probe loop.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use synchrotron_core::db::{ClusterAuthSource, ClusterRegistration, Database};
use synchrotron_kube::{AuthSource, ClusterConfig, ClusterName, KubeClient};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::api::errors::{ApiError, ErrorCode};

#[derive(Clone)]
pub struct ClustersState {
    pub db: Arc<Mutex<Database>>,
}

impl ClustersState {
    pub fn new(db: Arc<Mutex<Database>>) -> Self {
        Self { db }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ClusterView {
    pub id: String,
    pub name: String,
    /// One of `kubeconfig`, `in_cluster`, `default`.
    pub auth_source: String,
    pub kubeconfig_path: Option<String>,
    pub context: Option<String>,
    /// `true` when a bearer token is stored for this cluster. The
    /// token itself is never returned.
    pub has_bearer_token: bool,
    pub labels: serde_json::Value,
    pub created_at: String,
    pub updated_at: String,
}

impl From<ClusterRegistration> for ClusterView {
    fn from(c: ClusterRegistration) -> Self {
        Self {
            id: c.id.to_string(),
            name: c.name,
            auth_source: c.auth_source.as_str().to_string(),
            kubeconfig_path: c.kubeconfig_path,
            context: c.context,
            has_bearer_token: c.bearer_token.is_some(),
            labels: c.labels,
            created_at: c.created_at.to_rfc3339(),
            updated_at: c.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateClusterRequest {
    pub name: String,
    /// One of `kubeconfig`, `in_cluster`, `default`. Defaults to
    /// `kubeconfig` if omitted.
    #[serde(default = "default_auth_source")]
    pub auth_source: String,
    pub kubeconfig_path: Option<String>,
    pub context: Option<String>,
    pub bearer_token: Option<String>,
    #[serde(default)]
    pub labels: Option<serde_json::Value>,
}

fn default_auth_source() -> String {
    "kubeconfig".into()
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateClusterRequest {
    pub auth_source: Option<String>,
    pub kubeconfig_path: Option<String>,
    pub context: Option<String>,
    pub bearer_token: Option<String>,
    pub labels: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListClustersResponse {
    pub clusters: Vec<ClusterView>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ConnectivityCheckResponse {
    pub cluster: String,
    pub ok: bool,
    /// API server version on success; absent on failure.
    pub apiserver_version: Option<String>,
    /// Stage that failed: `"build_config"`, `"connect"`, `"version"`.
    pub failed_stage: Option<String>,
    pub error: Option<String>,
}

// --- Routing ---

pub fn router(state: ClustersState) -> Router {
    Router::new()
        .route("/api/v1/clusters", get(list_clusters).post(create_cluster))
        .route(
            "/api/v1/clusters/{name}",
            get(get_cluster).put(update_cluster).delete(delete_cluster),
        )
        .route("/api/v1/clusters/{name}/check", post(check_cluster))
        .with_state(state)
}

// --- Handlers ---

#[utoipa::path(
    get,
    path = "/api/v1/clusters",
    tag = "clusters",
    responses((status = 200, description = "Cluster registrations", body = ListClustersResponse))
)]
pub async fn list_clusters(
    State(state): State<ClustersState>,
) -> Result<Json<ListClustersResponse>, ApiError> {
    let rows = {
        let db = state.db.lock().unwrap();
        db.list_clusters().map_err(internal_db)?
    };
    Ok(Json(ListClustersResponse {
        clusters: rows.into_iter().map(ClusterView::from).collect(),
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/clusters",
    tag = "clusters",
    request_body = CreateClusterRequest,
    responses(
        (status = 201, description = "Cluster registered", body = ClusterView),
        (status = 400, description = "Validation failed"),
        (status = 409, description = "Cluster with that name already exists"),
    )
)]
pub async fn create_cluster(
    State(state): State<ClustersState>,
    Json(req): Json<CreateClusterRequest>,
) -> Result<(StatusCode, Json<ClusterView>), ApiError> {
    if req.name.trim().is_empty() {
        return Err(ApiError::bad_request("name is required"));
    }
    let auth_source = parse_auth_source(&req.auth_source)?;
    validate_auth_fields(&auth_source, req.kubeconfig_path.as_deref())?;

    let now = Utc::now();
    let reg = ClusterRegistration {
        id: Uuid::new_v4(),
        name: req.name.clone(),
        auth_source,
        kubeconfig_path: req.kubeconfig_path,
        context: req.context,
        bearer_token: req.bearer_token,
        labels: req.labels.unwrap_or_else(|| serde_json::json!({})),
        created_at: now,
        updated_at: now,
    };

    {
        let db = state.db.lock().unwrap();
        if db.get_cluster(&req.name).map_err(internal_db)?.is_some() {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                format!("cluster `{}` already exists", req.name),
            ));
        }
        db.insert_cluster(&reg).map_err(internal_db)?;
    }

    Ok((StatusCode::CREATED, Json(ClusterView::from(reg))))
}

#[utoipa::path(
    get,
    path = "/api/v1/clusters/{name}",
    tag = "clusters",
    params(("name" = String, Path, description = "Cluster name")),
    responses(
        (status = 200, description = "Cluster view", body = ClusterView),
        (status = 404, description = "Not found"),
    )
)]
pub async fn get_cluster(
    State(state): State<ClustersState>,
    Path(name): Path<String>,
) -> Result<Json<ClusterView>, ApiError> {
    let reg = load_cluster(&state, &name)?;
    Ok(Json(ClusterView::from(reg)))
}

#[utoipa::path(
    put,
    path = "/api/v1/clusters/{name}",
    tag = "clusters",
    params(("name" = String, Path, description = "Cluster name")),
    request_body = UpdateClusterRequest,
    responses(
        (status = 200, description = "Updated cluster", body = ClusterView),
        (status = 404, description = "Not found"),
    )
)]
pub async fn update_cluster(
    State(state): State<ClustersState>,
    Path(name): Path<String>,
    Json(req): Json<UpdateClusterRequest>,
) -> Result<Json<ClusterView>, ApiError> {
    let mut reg = load_cluster(&state, &name)?;
    if let Some(s) = req.auth_source {
        reg.auth_source = parse_auth_source(&s)?;
    }
    if let Some(p) = req.kubeconfig_path {
        reg.kubeconfig_path = Some(p);
    }
    if let Some(c) = req.context {
        reg.context = Some(c);
    }
    if let Some(t) = req.bearer_token {
        reg.bearer_token = Some(t);
    }
    if let Some(l) = req.labels {
        reg.labels = l;
    }
    validate_auth_fields(&reg.auth_source, reg.kubeconfig_path.as_deref())?;
    reg.updated_at = Utc::now();

    {
        let db = state.db.lock().unwrap();
        db.delete_cluster(&name).map_err(internal_db)?;
        db.insert_cluster(&reg).map_err(internal_db)?;
    }
    Ok(Json(ClusterView::from(reg)))
}

#[utoipa::path(
    delete,
    path = "/api/v1/clusters/{name}",
    tag = "clusters",
    params(("name" = String, Path, description = "Cluster name")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "Not found"),
    )
)]
pub async fn delete_cluster(
    State(state): State<ClustersState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let removed = {
        let db = state.db.lock().unwrap();
        db.delete_cluster(&name).map_err(internal_db)?
    };
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found(format!("cluster `{name}` not found")))
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/clusters/{name}/check",
    tag = "clusters",
    params(("name" = String, Path, description = "Cluster name")),
    responses(
        (status = 200, description = "Check completed (see body for ok/err)", body = ConnectivityCheckResponse),
        (status = 404, description = "Not found"),
    )
)]
pub async fn check_cluster(
    State(state): State<ClustersState>,
    Path(name): Path<String>,
) -> Result<Json<ConnectivityCheckResponse>, ApiError> {
    let reg = load_cluster(&state, &name)?;
    let cfg = match registration_to_kube_cfg(&reg) {
        Ok(c) => c,
        Err(msg) => {
            return Ok(Json(ConnectivityCheckResponse {
                cluster: name,
                ok: false,
                apiserver_version: None,
                failed_stage: Some("build_config".into()),
                error: Some(msg),
            }));
        }
    };

    let client = match KubeClient::connect(&cfg).await {
        Ok(c) => c,
        Err(e) => {
            return Ok(Json(ConnectivityCheckResponse {
                cluster: name,
                ok: false,
                apiserver_version: None,
                failed_stage: Some("connect".into()),
                error: Some(format!("{e}")),
            }));
        }
    };

    match client.apiserver_version().await {
        Ok(v) => Ok(Json(ConnectivityCheckResponse {
            cluster: name,
            ok: true,
            apiserver_version: Some(v),
            failed_stage: None,
            error: None,
        })),
        Err(e) => Ok(Json(ConnectivityCheckResponse {
            cluster: name,
            ok: false,
            apiserver_version: None,
            failed_stage: Some("version".into()),
            error: Some(format!("{e}")),
        })),
    }
}

// --- Helpers ---

fn load_cluster(state: &ClustersState, name: &str) -> Result<ClusterRegistration, ApiError> {
    let db = state.db.lock().unwrap();
    db.get_cluster(name)
        .map_err(internal_db)?
        .ok_or_else(|| ApiError::not_found(format!("cluster `{name}` not found")))
}

fn parse_auth_source(s: &str) -> Result<ClusterAuthSource, ApiError> {
    match s {
        "kubeconfig" => Ok(ClusterAuthSource::Kubeconfig),
        "in_cluster" => Ok(ClusterAuthSource::InCluster),
        "default" => Ok(ClusterAuthSource::Default),
        other => Err(ApiError::bad_request(format!(
            "unknown auth_source `{other}` (expected kubeconfig, in_cluster, or default)"
        ))),
    }
}

fn validate_auth_fields(
    auth_source: &ClusterAuthSource,
    kubeconfig_path: Option<&str>,
) -> Result<(), ApiError> {
    if matches!(auth_source, ClusterAuthSource::Kubeconfig) && kubeconfig_path.is_none() {
        return Err(ApiError::bad_request(
            "kubeconfig auth_source requires kubeconfig_path",
        ));
    }
    Ok(())
}

fn registration_to_kube_cfg(reg: &ClusterRegistration) -> Result<ClusterConfig, String> {
    let source = match reg.auth_source {
        ClusterAuthSource::Kubeconfig => {
            let path = reg
                .kubeconfig_path
                .as_deref()
                .ok_or("kubeconfig auth_source missing kubeconfig_path")?;
            AuthSource::Kubeconfig {
                path: PathBuf::from(path),
                context: reg.context.clone(),
            }
        }
        ClusterAuthSource::InCluster => AuthSource::InCluster,
        ClusterAuthSource::Default => AuthSource::Default {
            context: reg.context.clone(),
        },
    };
    Ok(ClusterConfig {
        name: ClusterName(reg.name.clone()),
        source,
    })
}

fn internal_db(e: anyhow::Error) -> ApiError {
    ApiError::internal(format!("database error: {e}"))
}
