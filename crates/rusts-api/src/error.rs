//! API error types

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use thiserror::Error;

/// API errors
#[derive(Debug, Error)]
pub enum ApiError {
    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Rate limited")]
    RateLimited,

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Parse error: {0}")]
    Parse(String),
}

/// Result type for API operations
pub type Result<T> = std::result::Result<T, ApiError>;

/// Error response body
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "bad_request", msg.clone()),
            ApiError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, "unauthorized", msg.clone()),
            ApiError::Forbidden(msg) => (StatusCode::FORBIDDEN, "forbidden", msg.clone()),
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, "not_found", msg.clone()),
            ApiError::RateLimited => (StatusCode::TOO_MANY_REQUESTS, "rate_limited", "Rate limit exceeded".to_string()),
            ApiError::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, "internal", msg.clone()),
            ApiError::Storage(msg) => (StatusCode::INTERNAL_SERVER_ERROR, "storage", msg.clone()),
            ApiError::Query(msg) => (StatusCode::BAD_REQUEST, "query", msg.clone()),
            ApiError::Parse(msg) => (StatusCode::BAD_REQUEST, "parse", msg.clone()),
        };

        let body = ErrorResponse {
            error: message,
            code: code.to_string(),
        };

        (status, Json(body)).into_response()
    }
}

impl From<rusts_storage::StorageError> for ApiError {
    fn from(e: rusts_storage::StorageError) -> Self {
        ApiError::Storage(e.to_string())
    }
}

impl From<rusts_query::QueryError> for ApiError {
    fn from(e: rusts_query::QueryError) -> Self {
        ApiError::Query(e.to_string())
    }
}
