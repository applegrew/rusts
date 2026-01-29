//! API router setup

use crate::handlers::{self, AppState};
use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use std::time::Duration;
use tower_http::cors::{Any, CorsLayer};
use axum::http::StatusCode;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

/// Create the API router
pub fn create_router(state: Arc<AppState>, request_timeout: Duration) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        // Health endpoints
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready))
        // Write endpoint
        .route("/write", post(handlers::write))
        // Query endpoints
        .route("/query", post(handlers::query))
        .route("/sql", post(handlers::sql_query))
        // Stats endpoint
        .route("/stats", get(handlers::stats))
        // Add middleware
        .layer(TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, request_timeout))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        // Add state
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusts_index::{SeriesIndex, TagIndex};
    use rusts_storage::{StorageEngine, StorageEngineConfig, WalDurability};
    use tempfile::TempDir;

    fn create_test_state() -> (Arc<AppState>, TempDir) {
        let dir = TempDir::new().unwrap();
        let config = StorageEngineConfig {
            data_dir: dir.path().to_path_buf(),
            wal_durability: WalDurability::None,
            ..Default::default()
        };

        let storage = Arc::new(StorageEngine::new(config).unwrap());
        let series_index = Arc::new(SeriesIndex::new());
        let tag_index = Arc::new(TagIndex::new());

        let state = Arc::new(AppState::new(
            storage,
            series_index,
            tag_index,
            Duration::from_secs(30),
            100,
        ));

        (state, dir)
    }

    #[test]
    fn test_router_creation() {
        let (state, _dir) = create_test_state();
        let _router = create_router(state, Duration::from_secs(30));
    }
}
