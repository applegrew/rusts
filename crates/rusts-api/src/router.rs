//! API router setup

use crate::auth::{auth_middleware, AuthState};
use crate::handlers::{self, AppState};
use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{get, post},
    Extension, Router,
};
use std::sync::Arc;
use std::time::Duration;
use tower_http::cors::{Any, CorsLayer};
use axum::http::StatusCode;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

/// Create the API router
pub fn create_router(
    state: Arc<AppState>,
    request_timeout: Duration,
    max_body_size: usize,
    auth_state: Arc<AuthState>,
) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Protected routes — auth middleware applied
    let protected = Router::new()
        .route("/write", post(handlers::write))
        .route("/query", post(handlers::query))
        .route("/sql", post(handlers::sql_query))
        .route("/stats", get(handlers::stats))
        .layer(middleware::from_fn(auth_middleware))
        .layer(Extension(auth_state));

    // Public routes — no auth required
    let public = Router::new()
        .route("/health", get(handlers::health))
        .route("/ready", get(handlers::ready));

    public
        .merge(protected)
        // Add middleware
        .layer(DefaultBodyLimit::max(max_body_size))
        .layer(TimeoutLayer::with_status_code(StatusCode::REQUEST_TIMEOUT, request_timeout))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        // Add state
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthConfig;
    use crate::handlers::{StartupPhase, StartupState};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use rusts_core::ParallelConfig;
    use rusts_index::{SeriesIndex, TagIndex};
    use rusts_storage::{StorageEngine, StorageEngineConfig, WalDurability};
    use tempfile::TempDir;
    use tower::ServiceExt;

    fn test_auth_state() -> Arc<AuthState> {
        Arc::new(AuthState::new(AuthConfig::default()))
    }

    struct TestState {
        state: Arc<AppState>,
        _dir: TempDir,
    }

    impl TestState {
        fn new() -> Self {
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
                ParallelConfig::default(),
            ));

            Self { state, _dir: dir }
        }
    }

    impl Drop for TestState {
        fn drop(&mut self) {
            if let Some(storage) = self.state.get_storage() {
                let _ = storage.shutdown();
            }
        }
    }

    fn create_initializing_state() -> Arc<AppState> {
        let startup_state = Arc::new(StartupState::new());
        Arc::new(AppState::new_initializing(
            Duration::from_secs(30),
            100,
            startup_state,
            ParallelConfig::default(),
        ))
    }

    #[test]
    fn test_router_creation() {
        let env = TestState::new();
        let _router = create_router(Arc::clone(&env.state), Duration::from_secs(30), 10 * 1024 * 1024, test_auth_state());
    }

    #[tokio::test]
    async fn test_health_endpoint_during_startup() {
        let state = create_initializing_state();
        state.startup_state.set_phase(StartupPhase::WalRecovery);

        let app = create_router(state, Duration::from_secs(30), 10 * 1024 * 1024, test_auth_state());

        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "unhealthy");
        assert_eq!(json["phase"], "wal_recovery");
    }

    #[tokio::test]
    async fn test_health_endpoint_when_ready() {
        let env = TestState::new();
        let app = create_router(Arc::clone(&env.state), Duration::from_secs(30), 10 * 1024 * 1024, test_auth_state());

        let response = app
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["status"], "healthy");
        assert!(json.get("phase").is_none() || json["phase"].is_null());
    }

    #[tokio::test]
    async fn test_ready_endpoint_during_startup() {
        let state = create_initializing_state();
        state.startup_state.set_phase(StartupPhase::IndexRebuilding);

        let app = create_router(state, Duration::from_secs(30), 10 * 1024 * 1024, test_auth_state());

        let response = app
            .oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["ready"], false);
        assert_eq!(json["phase"], "index_rebuilding");
    }

    #[tokio::test]
    async fn test_ready_endpoint_when_ready() {
        let env = TestState::new();
        let app = create_router(Arc::clone(&env.state), Duration::from_secs(30), 10 * 1024 * 1024, test_auth_state());

        let response = app
            .oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["ready"], true);
        assert!(json.get("phase").is_none() || json["phase"].is_null());
    }

    #[tokio::test]
    async fn test_write_endpoint_during_startup() {
        let state = create_initializing_state();
        state.startup_state.set_phase(StartupPhase::WalRecovery);

        let app = create_router(state, Duration::from_secs(30), 10 * 1024 * 1024, test_auth_state());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write")
                    .body(Body::from("cpu,host=server1 value=1.0"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["code"], "service_unavailable");
        assert!(json["error"].as_str().unwrap().contains("starting up"));
    }

    #[tokio::test]
    async fn test_query_endpoint_during_startup() {
        let state = create_initializing_state();
        state.startup_state.set_phase(StartupPhase::IndexRebuilding);

        let app = create_router(state, Duration::from_secs(30), 10 * 1024 * 1024, test_auth_state());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/query")
                    .header("Content-Type", "application/json")
                    .body(Body::from(r#"{"measurement":"cpu"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["code"], "service_unavailable");
        assert!(json["error"].as_str().unwrap().contains("starting up"));
    }

    #[tokio::test]
    async fn test_stats_endpoint_during_startup() {
        let state = create_initializing_state();
        state.startup_state.set_phase(StartupPhase::WalRecovery);

        let app = create_router(state, Duration::from_secs(30), 10 * 1024 * 1024, test_auth_state());

        let response = app
            .oneshot(Request::builder().uri("/stats").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["code"], "service_unavailable");
        assert!(json["error"].as_str().unwrap().contains("starting up"));
    }

    #[tokio::test]
    async fn test_sql_endpoint_during_startup() {
        let state = create_initializing_state();
        state.startup_state.set_phase(StartupPhase::WalRecovery);

        let app = create_router(state, Duration::from_secs(30), 10 * 1024 * 1024, test_auth_state());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/sql")
                    .header("Content-Type", "text/plain")
                    .body(Body::from("SELECT * FROM cpu"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["code"], "service_unavailable");
        assert!(json["error"].as_str().unwrap().contains("starting up"));
    }

    #[tokio::test]
    async fn test_write_endpoint_when_ready() {
        let env = TestState::new();
        let app = create_router(Arc::clone(&env.state), Duration::from_secs(30), 10 * 1024 * 1024, test_auth_state());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/write")
                    .body(Body::from("cpu,host=server1 value=1.0"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["success"], true);
        assert_eq!(json["points_written"], 1);
    }

    #[tokio::test]
    async fn test_startup_phase_progression() {
        let state = create_initializing_state();

        // Simulate the startup progression
        let phases = [
            StartupPhase::Initializing,
            StartupPhase::WalRecovery,
            StartupPhase::IndexRebuilding,
            StartupPhase::Ready,
        ];

        for phase in phases {
            state.startup_state.set_phase(phase.clone());
            let app = create_router(Arc::clone(&state), Duration::from_secs(30), 10 * 1024 * 1024, test_auth_state());

            let response = app
                .oneshot(Request::builder().uri("/ready").body(Body::empty()).unwrap())
                .await
                .unwrap();

            let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

            if phase == StartupPhase::Ready {
                assert_eq!(json["ready"], true);
            } else {
                assert_eq!(json["ready"], false);
                assert_eq!(json["phase"], phase.to_string());
            }
        }
    }
}
