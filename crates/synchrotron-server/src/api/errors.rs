//! Common JSON error envelope for the REST API.
//!
//! Every fallible handler returns [`Result<T, ApiError>`]. Axum's
//! [`IntoResponse`] impl serializes the error as
//!
//! ```json
//! { "error": { "code": "not_found", "message": "...", "details": {...} } }
//! ```
//!
//! Codes are stable, machine-readable strings the CLI can switch on;
//! `message` is human-readable and may change; `details` is an
//! optional free-form object for context (validation field paths,
//! upstream identifiers).

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use utoipa::ToSchema;

/// Stable error codes. New variants append; existing ones don't change.
#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    BadRequest,
    NotFound,
    Conflict,
    Unauthorized,
    Forbidden,
    Internal,
    Unavailable,
}

impl ErrorCode {
    fn http_status(self) -> StatusCode {
        match self {
            Self::BadRequest => StatusCode::BAD_REQUEST,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Conflict => StatusCode::CONFLICT,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// JSON body shape. Wrapped in [`ApiErrorEnvelope`] for the outer
/// `{ "error": ... }` key so future top-level fields (request_id,
/// trace_id) can be added without breaking clients that already read
/// `error`.
#[derive(Debug, Serialize, ToSchema)]
pub struct ApiErrorBody {
    pub code: ErrorCode,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ApiErrorEnvelope {
    pub error: ApiErrorBody,
}

#[derive(Debug)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    pub details: Option<serde_json::Value>,
}

impl ApiError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::BadRequest, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.code.http_status();
        let body = Json(ApiErrorEnvelope {
            error: ApiErrorBody {
                code: self.code,
                message: self.message,
                details: self.details,
            },
        });
        (status, body).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn into_response_carries_status_and_body() {
        let err = ApiError::not_found("app `web` does not exist");
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body = to_bytes(resp.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], "not_found");
        assert_eq!(json["error"]["message"], "app `web` does not exist");
        assert!(json["error"]["details"].is_null());
    }

    #[tokio::test]
    async fn details_round_trip() {
        let err = ApiError::bad_request("validation failed")
            .with_details(serde_json::json!({"field": "namespace"}));
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(resp.into_body(), 1024).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["details"]["field"], "namespace");
    }
}
