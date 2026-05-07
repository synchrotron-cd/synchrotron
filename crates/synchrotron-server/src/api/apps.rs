//! Apps CRUD plus operational actions (sync / diff / rollback / history).
//!
//! Endpoints under `/api/v1/apps`:
//!
//! | Method | Path                          | Purpose                                |
//! |--------|-------------------------------|----------------------------------------|
//! | GET    | `/apps`                       | List all apps                          |
//! | POST   | `/apps`                       | Create an app                          |
//! | GET    | `/apps/{name}`                | Fetch one                              |
//! | PUT    | `/apps/{name}`                | Update mutable fields                  |
//! | DELETE | `/apps/{name}`                | Remove                                 |
//! | POST   | `/apps/{name}/sync`           | Request a manual reconcile             |
//! | POST   | `/apps/{name}/diff`           | Compute desired-vs-live diff           |
//! | POST   | `/apps/{name}/rollback`       | Roll back to a prior recorded revision |
//! | GET    | `/apps/{name}/history`        | List recent sync attempts              |
//!
//! ## Threading
//!
//! [`AppsState`] holds the [`Database`] behind a `Mutex` because
//! `rusqlite::Connection` is `Send` but not `Sync`, and axum's
//! state extractor wants `Send + Sync`. DB calls are quick and
//! synchronous — handlers never `.await` while holding the lock.

use std::sync::{Arc, Mutex};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use synchrotron_core::db::Database;
use synchrotron_core::events::{EventBus, SystemEvent};
use synchrotron_types::{
    AppDestination, AppName, AppSource, AppStatus, Application, ClusterName, RepoUrl, SyncPolicy,
};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::api::diff_engine::{DiffEngine, DiffEngineError, DiffEntry};
use crate::api::errors::ApiError;

/// Shared state for the apps API. Cheap to clone — the inner `Arc`s
/// share a single DB handle, event bus, and (optional) diff engine
/// across handlers.
#[derive(Clone)]
pub struct AppsState {
    pub db: Arc<Mutex<Database>>,
    pub bus: EventBus,
    /// Engine that turns an app reference into a structured diff. When
    /// `None` (e.g. server booted without a reconciler), the diff
    /// endpoint returns the historical empty-with-note shape.
    pub diff_engine: Option<Arc<dyn DiffEngine>>,
}

impl AppsState {
    pub fn new(db: Database, bus: EventBus) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
            bus,
            diff_engine: None,
        }
    }

    /// Plug a diff engine into this state. Required for
    /// `POST /apps/{name}/diff` to return real entries.
    pub fn with_diff_engine(mut self, engine: Arc<dyn DiffEngine>) -> Self {
        self.diff_engine = Some(engine);
        self
    }
}

/// API view of an [`Application`]. We project domain fields onto a
/// flat shape so OpenAPI consumers don't have to chase nested
/// `source` / `destination` objects, and so the wire format stays
/// stable when the domain struct grows new fields the API doesn't
/// surface yet.
#[derive(Debug, Serialize, ToSchema)]
pub struct AppView {
    pub id: String,
    pub name: String,
    pub namespace: String,
    pub repo_url: String,
    pub path: String,
    pub target_revision: String,
    pub dest_cluster: String,
    pub dest_namespace: String,
    pub sync_status: String,
    pub health_status: String,
    pub health_message: Option<String>,
    pub last_synced_at: Option<String>,
    pub last_synced_revision: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl From<Application> for AppView {
    fn from(a: Application) -> Self {
        Self {
            id: a.id.to_string(),
            name: a.name.0,
            namespace: a.namespace,
            repo_url: a.source.repo_url.0,
            path: a.source.path,
            target_revision: a.source.target_revision,
            dest_cluster: a.destination.cluster.0,
            dest_namespace: a.destination.namespace,
            sync_status: a.status.sync.as_str().to_string(),
            health_status: a.status.health.as_str().to_string(),
            health_message: a.status.health_message,
            last_synced_at: a.status.last_synced_at.map(|t| t.to_rfc3339()),
            last_synced_revision: a.status.last_synced_revision,
            created_at: a.created_at.to_rfc3339(),
            updated_at: a.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateAppRequest {
    pub name: String,
    pub namespace: String,
    pub repo_url: String,
    pub path: String,
    #[serde(default = "default_revision")]
    pub target_revision: String,
    pub dest_cluster: String,
    pub dest_namespace: String,
}

fn default_revision() -> String {
    "main".to_string()
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateAppRequest {
    pub namespace: Option<String>,
    pub repo_url: Option<String>,
    pub path: Option<String>,
    pub target_revision: Option<String>,
    pub dest_cluster: Option<String>,
    pub dest_namespace: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListAppsResponse {
    pub apps: Vec<AppView>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SyncAcceptedResponse {
    pub app: String,
    pub status: String,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct RollbackRequest {
    pub revision_id: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RollbackResponse {
    pub app: String,
    pub revision_id: i64,
    pub commit_hash: String,
    pub recorded_at: String,
    pub manifest_count: usize,
    pub status: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DiffResponse {
    pub app: String,
    /// Per-resource diff entries. Empty when no engine is configured
    /// (server has no reconciler attached) — `note` then explains.
    pub entries: Vec<DiffEntry>,
    pub note: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HistoryEntry {
    pub id: String,
    pub revision: String,
    pub status: String,
    pub trigger: String,
    pub message: Option<String>,
    pub resources_synced: i32,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HistoryResponse {
    pub app: String,
    pub entries: Vec<HistoryEntry>,
}

// --- Routing ---

pub fn router(state: AppsState) -> Router {
    Router::new()
        .route("/api/v1/apps", get(list_apps).post(create_app))
        .route(
            "/api/v1/apps/{name}",
            get(get_app).put(update_app).delete(delete_app),
        )
        .route("/api/v1/apps/{name}/sync", post(sync_app))
        .route("/api/v1/apps/{name}/diff", post(diff_app))
        .route("/api/v1/apps/{name}/rollback", post(rollback_app))
        .route("/api/v1/apps/{name}/history", get(history_app))
        .with_state(state)
}

// --- Handlers ---

#[utoipa::path(
    get,
    path = "/api/v1/apps",
    tag = "apps",
    responses((status = 200, description = "List of apps", body = ListAppsResponse))
)]
pub async fn list_apps(State(state): State<AppsState>) -> Result<Json<ListAppsResponse>, ApiError> {
    let apps = {
        let db = state.db.lock().unwrap();
        db.list_applications().map_err(internal_db)?
    };
    Ok(Json(ListAppsResponse {
        apps: apps.into_iter().map(AppView::from).collect(),
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/apps",
    tag = "apps",
    request_body = CreateAppRequest,
    responses(
        (status = 201, description = "App created", body = AppView),
        (status = 409, description = "App with that name already exists"),
    )
)]
pub async fn create_app(
    State(state): State<AppsState>,
    Json(req): Json<CreateAppRequest>,
) -> Result<(StatusCode, Json<AppView>), ApiError> {
    if req.name.trim().is_empty() {
        return Err(ApiError::bad_request("name is required"));
    }

    let now = Utc::now();
    let app = Application {
        id: Uuid::new_v4(),
        name: AppName(req.name.clone()),
        namespace: req.namespace,
        source: AppSource {
            repo_url: RepoUrl(req.repo_url),
            path: req.path,
            target_revision: req.target_revision,
            plugin: None,
        },
        destination: AppDestination {
            cluster: ClusterName(req.dest_cluster),
            namespace: req.dest_namespace,
        },
        sync_policy: SyncPolicy::default(),
        status: AppStatus::default(),
        created_at: now,
        updated_at: now,
    };

    {
        let db = state.db.lock().unwrap();
        if db
            .get_application(&req.name)
            .map_err(internal_db)?
            .is_some()
        {
            return Err(ApiError::new(
                crate::api::errors::ErrorCode::Conflict,
                format!("app `{}` already exists", req.name),
            ));
        }
        db.insert_application(&app).map_err(internal_db)?;
    }

    Ok((StatusCode::CREATED, Json(AppView::from(app))))
}

#[utoipa::path(
    get,
    path = "/api/v1/apps/{name}",
    tag = "apps",
    params(("name" = String, Path, description = "App name")),
    responses(
        (status = 200, description = "App view", body = AppView),
        (status = 404, description = "Not found"),
    )
)]
pub async fn get_app(
    State(state): State<AppsState>,
    Path(name): Path<String>,
) -> Result<Json<AppView>, ApiError> {
    let app = load_app(&state, &name)?;
    Ok(Json(AppView::from(app)))
}

#[utoipa::path(
    put,
    path = "/api/v1/apps/{name}",
    tag = "apps",
    params(("name" = String, Path, description = "App name")),
    request_body = UpdateAppRequest,
    responses(
        (status = 200, description = "App after update", body = AppView),
        (status = 404, description = "Not found"),
    )
)]
pub async fn update_app(
    State(state): State<AppsState>,
    Path(name): Path<String>,
    Json(req): Json<UpdateAppRequest>,
) -> Result<Json<AppView>, ApiError> {
    let mut app = load_app(&state, &name)?;
    if let Some(v) = req.namespace {
        app.namespace = v;
    }
    if let Some(v) = req.repo_url {
        app.source.repo_url = RepoUrl(v);
    }
    if let Some(v) = req.path {
        app.source.path = v;
    }
    if let Some(v) = req.target_revision {
        app.source.target_revision = v;
    }
    if let Some(v) = req.dest_cluster {
        app.destination.cluster = ClusterName(v);
    }
    if let Some(v) = req.dest_namespace {
        app.destination.namespace = v;
    }
    app.updated_at = Utc::now();

    // No update-by-name DML in app_repo yet, so do delete+reinsert in
    // a single locked critical section. Insert preserves the original
    // id, so callers' references stay valid.
    {
        let db = state.db.lock().unwrap();
        db.delete_application(&name).map_err(internal_db)?;
        db.insert_application(&app).map_err(internal_db)?;
    }

    Ok(Json(AppView::from(app)))
}

#[utoipa::path(
    delete,
    path = "/api/v1/apps/{name}",
    tag = "apps",
    params(("name" = String, Path, description = "App name")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "Not found"),
    )
)]
pub async fn delete_app(
    State(state): State<AppsState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let removed = {
        let db = state.db.lock().unwrap();
        db.delete_application(&name).map_err(internal_db)?
    };
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found(format!("app `{name}` not found")))
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/apps/{name}/sync",
    tag = "apps",
    params(("name" = String, Path, description = "App name")),
    responses(
        (status = 202, description = "Sync request enqueued", body = SyncAcceptedResponse),
        (status = 404, description = "Not found"),
    )
)]
pub async fn sync_app(
    State(state): State<AppsState>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<SyncAcceptedResponse>), ApiError> {
    let _ = load_app(&state, &name)?;
    state.bus.publish(SystemEvent::ManualSyncRequested {
        app: AppName(name.clone()),
    });
    Ok((
        StatusCode::ACCEPTED,
        Json(SyncAcceptedResponse {
            app: name,
            status: "queued".into(),
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/apps/{name}/diff",
    tag = "apps",
    params(("name" = String, Path, description = "App name")),
    responses(
        (status = 200, description = "Diff entries (may be empty)", body = DiffResponse),
        (status = 404, description = "Not found"),
    )
)]
pub async fn diff_app(
    State(state): State<AppsState>,
    Path(name): Path<String>,
) -> Result<Json<DiffResponse>, ApiError> {
    let app = load_app(&state, &name)?;
    let Some(engine) = state.diff_engine.clone() else {
        return Ok(Json(DiffResponse {
            app: name,
            entries: Vec::new(),
            note: Some(
                "diff engine not configured on this server; \
                 entries will populate once a reconciler is attached."
                    .into(),
            ),
        }));
    };
    let entries = engine
        .compute(&app.name, &app.destination.cluster)
        .map_err(map_engine_err)?;
    Ok(Json(DiffResponse {
        app: name,
        entries,
        note: None,
    }))
}

fn map_engine_err(e: DiffEngineError) -> ApiError {
    use crate::api::errors::ErrorCode;
    match e {
        DiffEngineError::AppNotInCache(_) => ApiError::not_found(e.to_string()),
        DiffEngineError::ClusterNotAvailable(_) => {
            ApiError::new(ErrorCode::Unavailable, e.to_string())
        }
        DiffEngineError::DesiredFetch(_) | DiffEngineError::LiveFetch(_) => {
            ApiError::new(ErrorCode::Unavailable, e.to_string())
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/apps/{name}/rollback",
    tag = "apps",
    params(("name" = String, Path, description = "App name")),
    request_body = RollbackRequest,
    responses(
        (status = 202, description = "Rollback request enqueued", body = RollbackResponse),
        (status = 404, description = "App or revision not found"),
    )
)]
pub async fn rollback_app(
    State(state): State<AppsState>,
    Path(name): Path<String>,
    Json(req): Json<RollbackRequest>,
) -> Result<(StatusCode, Json<RollbackResponse>), ApiError> {
    let app = load_app(&state, &name)?;

    let (rev, manifests) = {
        let db = state.db.lock().unwrap();
        let loaded = db
            .load_sync_revision(req.revision_id)
            .map_err(internal_db)?;
        let Some((rev, manifests)) = loaded else {
            return Err(ApiError::not_found(format!(
                "revision {} not found",
                req.revision_id
            )));
        };
        if rev.app_id != app.id.to_string() {
            return Err(ApiError::not_found(format!(
                "revision {} does not belong to app `{name}`",
                req.revision_id
            )));
        }
        (rev, manifests)
    };

    state.bus.publish(SystemEvent::ManualSyncRequested {
        app: AppName(name.clone()),
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(RollbackResponse {
            app: name,
            revision_id: rev.id,
            commit_hash: rev.commit_hash,
            recorded_at: rev.recorded_at,
            manifest_count: manifests.len(),
            status: "queued".into(),
        }),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/apps/{name}/history",
    tag = "apps",
    params(("name" = String, Path, description = "App name")),
    responses(
        (status = 200, description = "Recent sync attempts", body = HistoryResponse),
        (status = 404, description = "Not found"),
    )
)]
pub async fn history_app(
    State(state): State<AppsState>,
    Path(name): Path<String>,
) -> Result<Json<HistoryResponse>, ApiError> {
    let app = load_app(&state, &name)?;
    let records = {
        let db = state.db.lock().unwrap();
        db.get_sync_history(&app.id, 100).map_err(internal_db)?
    };
    let entries = records
        .into_iter()
        .map(|r| HistoryEntry {
            id: r.id.to_string(),
            revision: r.revision,
            status: r.status.as_str().to_string(),
            trigger: r.trigger.as_str().to_string(),
            message: r.message,
            resources_synced: r.resources_synced,
            started_at: r.started_at.to_rfc3339(),
            finished_at: r.finished_at.map(|t| t.to_rfc3339()),
        })
        .collect();
    Ok(Json(HistoryResponse { app: name, entries }))
}

// --- Helpers ---

fn load_app(state: &AppsState, name: &str) -> Result<Application, ApiError> {
    let db = state.db.lock().unwrap();
    db.get_application(name)
        .map_err(internal_db)?
        .ok_or_else(|| ApiError::not_found(format!("app `{name}` not found")))
}

fn internal_db(e: anyhow::Error) -> ApiError {
    ApiError::internal(format!("database error: {e}"))
}
