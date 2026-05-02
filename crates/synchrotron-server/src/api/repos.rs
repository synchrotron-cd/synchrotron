//! Repo registration CRUD.
//!
//! ## Secret handling
//!
//! `credentials_secret_ref` is a *pointer* into an external secret
//! store — the value behind the pointer is never seen by Synchrotron,
//! so it round-trips through API responses unchanged.
//!
//! `password` is the inline credential fallback for environments that
//! don't run a secret store. It is persisted in the DB but never
//! returned in any GET/LIST/PUT response — clients see only a
//! `has_password` boolean.

use std::sync::{Arc, Mutex};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use synchrotron_core::db::{Database, RepoRegistration};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::api::errors::{ApiError, ErrorCode};

#[derive(Clone)]
pub struct ReposState {
    pub db: Arc<Mutex<Database>>,
}

impl ReposState {
    pub fn new(db: Arc<Mutex<Database>>) -> Self {
        Self { db }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RepoView {
    pub id: String,
    pub name: String,
    pub url: String,
    pub branch: Option<String>,
    /// Pointer into an external secret store (e.g. `vault://...`). The
    /// resolved credential value is never persisted here.
    pub credentials_secret_ref: Option<String>,
    /// `true` when an inline password is stored. The value itself is
    /// never returned.
    pub has_password: bool,
    pub labels: serde_json::Value,
    pub created_at: String,
    pub updated_at: String,
}

impl From<RepoRegistration> for RepoView {
    fn from(r: RepoRegistration) -> Self {
        Self {
            id: r.id.to_string(),
            name: r.name,
            url: r.url,
            branch: r.branch,
            credentials_secret_ref: r.credentials_secret_ref,
            has_password: r.password.is_some(),
            labels: r.labels,
            created_at: r.created_at.to_rfc3339(),
            updated_at: r.updated_at.to_rfc3339(),
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateRepoRequest {
    pub name: String,
    pub url: String,
    pub branch: Option<String>,
    pub credentials_secret_ref: Option<String>,
    pub password: Option<String>,
    #[serde(default)]
    pub labels: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct UpdateRepoRequest {
    pub url: Option<String>,
    pub branch: Option<String>,
    pub credentials_secret_ref: Option<String>,
    pub password: Option<String>,
    pub labels: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListReposResponse {
    pub repos: Vec<RepoView>,
}

// --- Routing ---

pub fn router(state: ReposState) -> Router {
    Router::new()
        .route("/api/v1/repos", get(list_repos).post(create_repo))
        .route(
            "/api/v1/repos/{name}",
            get(get_repo).put(update_repo).delete(delete_repo),
        )
        .with_state(state)
}

// --- Handlers ---

#[utoipa::path(
    get,
    path = "/api/v1/repos",
    tag = "repos",
    responses((status = 200, description = "Repo registrations", body = ListReposResponse))
)]
pub async fn list_repos(
    State(state): State<ReposState>,
) -> Result<Json<ListReposResponse>, ApiError> {
    let rows = {
        let db = state.db.lock().unwrap();
        db.list_repos().map_err(internal_db)?
    };
    Ok(Json(ListReposResponse {
        repos: rows.into_iter().map(RepoView::from).collect(),
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/repos",
    tag = "repos",
    request_body = CreateRepoRequest,
    responses(
        (status = 201, description = "Repo registered", body = RepoView),
        (status = 400, description = "Validation failed"),
        (status = 409, description = "Repo with that name already exists"),
    )
)]
pub async fn create_repo(
    State(state): State<ReposState>,
    Json(req): Json<CreateRepoRequest>,
) -> Result<(StatusCode, Json<RepoView>), ApiError> {
    if req.name.trim().is_empty() {
        return Err(ApiError::bad_request("name is required"));
    }
    validate_url(&req.url)?;
    if let Some(ref s) = req.credentials_secret_ref {
        validate_secret_ref(s)?;
    }

    let now = Utc::now();
    let reg = RepoRegistration {
        id: Uuid::new_v4(),
        name: req.name.clone(),
        url: req.url,
        branch: req.branch,
        credentials_secret_ref: req.credentials_secret_ref,
        password: req.password,
        labels: req.labels.unwrap_or_else(|| serde_json::json!({})),
        created_at: now,
        updated_at: now,
    };

    {
        let db = state.db.lock().unwrap();
        if db.get_repo(&req.name).map_err(internal_db)?.is_some() {
            return Err(ApiError::new(
                ErrorCode::Conflict,
                format!("repo `{}` already exists", req.name),
            ));
        }
        db.insert_repo(&reg).map_err(internal_db)?;
    }

    Ok((StatusCode::CREATED, Json(RepoView::from(reg))))
}

#[utoipa::path(
    get,
    path = "/api/v1/repos/{name}",
    tag = "repos",
    params(("name" = String, Path, description = "Repo name")),
    responses(
        (status = 200, description = "Repo view", body = RepoView),
        (status = 404, description = "Not found"),
    )
)]
pub async fn get_repo(
    State(state): State<ReposState>,
    Path(name): Path<String>,
) -> Result<Json<RepoView>, ApiError> {
    let reg = load_repo(&state, &name)?;
    Ok(Json(RepoView::from(reg)))
}

#[utoipa::path(
    put,
    path = "/api/v1/repos/{name}",
    tag = "repos",
    params(("name" = String, Path, description = "Repo name")),
    request_body = UpdateRepoRequest,
    responses(
        (status = 200, description = "Updated repo", body = RepoView),
        (status = 404, description = "Not found"),
    )
)]
pub async fn update_repo(
    State(state): State<ReposState>,
    Path(name): Path<String>,
    Json(req): Json<UpdateRepoRequest>,
) -> Result<Json<RepoView>, ApiError> {
    let mut reg = load_repo(&state, &name)?;
    if let Some(u) = req.url {
        validate_url(&u)?;
        reg.url = u;
    }
    if let Some(b) = req.branch {
        reg.branch = Some(b);
    }
    if let Some(s) = req.credentials_secret_ref {
        validate_secret_ref(&s)?;
        reg.credentials_secret_ref = Some(s);
    }
    if let Some(p) = req.password {
        reg.password = Some(p);
    }
    if let Some(l) = req.labels {
        reg.labels = l;
    }
    reg.updated_at = Utc::now();

    {
        let db = state.db.lock().unwrap();
        db.delete_repo(&name).map_err(internal_db)?;
        db.insert_repo(&reg).map_err(internal_db)?;
    }
    Ok(Json(RepoView::from(reg)))
}

#[utoipa::path(
    delete,
    path = "/api/v1/repos/{name}",
    tag = "repos",
    params(("name" = String, Path, description = "Repo name")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "Not found"),
    )
)]
pub async fn delete_repo(
    State(state): State<ReposState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let removed = {
        let db = state.db.lock().unwrap();
        db.delete_repo(&name).map_err(internal_db)?
    };
    if removed {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found(format!("repo `{name}` not found")))
    }
}

// --- Helpers ---

fn load_repo(state: &ReposState, name: &str) -> Result<RepoRegistration, ApiError> {
    let db = state.db.lock().unwrap();
    db.get_repo(name)
        .map_err(internal_db)?
        .ok_or_else(|| ApiError::not_found(format!("repo `{name}` not found")))
}

fn validate_url(url: &str) -> Result<(), ApiError> {
    if url.trim().is_empty() {
        return Err(ApiError::bad_request("url is required"));
    }
    // Permissive: SSH (`git@host:org/repo.git`), HTTPS, and `file://` are
    // all valid forms. We reject only obvious garbage rather than try
    // to enumerate every transport.
    if !(url.starts_with("https://")
        || url.starts_with("http://")
        || url.starts_with("git@")
        || url.starts_with("ssh://")
        || url.starts_with("file://"))
    {
        return Err(ApiError::bad_request(format!(
            "url `{url}` does not look like a git URL (expected https://, http://, ssh://, file://, or git@host:path)"
        )));
    }
    Ok(())
}

fn validate_secret_ref(s: &str) -> Result<(), ApiError> {
    if s.trim().is_empty() {
        return Err(ApiError::bad_request(
            "credentials_secret_ref must not be blank when present",
        ));
    }
    // Cheap structural sanity: at least scheme:path form. The
    // resolver layer enforces transport-specific rules.
    if !s.contains(':') {
        return Err(ApiError::bad_request(format!(
            "credentials_secret_ref `{s}` must be of the form `<store>:<path>` (e.g. `vault://secret/git/org`)"
        )));
    }
    Ok(())
}

fn internal_db(e: anyhow::Error) -> ApiError {
    ApiError::internal(format!("database error: {e}"))
}
