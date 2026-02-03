//! PostgreSQL wire protocol backend implementation

use crate::encoder::{query_result_to_response, tables_to_response};
use crate::error::PgError;
use async_trait::async_trait;
use futures::Sink;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::Response;
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use rusts_api::handlers::AppState;
use rusts_sql::{SqlCommand, SqlParser, SqlTranslator};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// PostgreSQL wire protocol backend for RusTs
pub struct PgWireBackend {
    app_state: Arc<AppState>,
    query_timeout: Duration,
}

impl PgWireBackend {
    /// Create a new PostgreSQL backend
    pub fn new(app_state: Arc<AppState>, query_timeout: Duration) -> Self {
        Self {
            app_state,
            query_timeout,
        }
    }

    /// Execute a SQL command and return the PostgreSQL response
    async fn execute_command(&self, query: &str) -> Result<Vec<Response<'static>>, PgError> {
        // Parse SQL
        let stmt = SqlParser::parse(query)?;

        // Translate to command
        let command = SqlTranslator::translate_command(&stmt)?;

        match command {
            SqlCommand::ShowTables => {
                // Return list of measurements as tables
                let measurements = self.app_state.series_index.measurements();
                let response = tables_to_response(measurements)
                    .map_err(|e| PgError::Internal(e.to_string()))?;
                Ok(vec![response])
            }
            SqlCommand::Explain(_query) => {
                // For now, return a simple text response for EXPLAIN
                // A full implementation would format the query plan
                Err(PgError::SqlParse(rusts_sql::SqlError::UnsupportedFeature(
                    "EXPLAIN not yet supported via PostgreSQL protocol".to_string(),
                )))
            }
            SqlCommand::Query(query) => {
                // Check if storage/executor is ready
                let executor = self.app_state.get_executor().ok_or_else(|| {
                    let phase = self.app_state.startup_state.phase();
                    PgError::ServiceUnavailable(format!("Server is starting up ({})", phase))
                })?;

                // Acquire semaphore permit
                let _permit = self
                    .app_state
                    .query_semaphore
                    .acquire()
                    .await
                    .map_err(|_| PgError::Internal("Query semaphore closed".to_string()))?;

                // Execute with timeout
                let timeout = self.query_timeout;
                let cancel = CancellationToken::new();
                let cancel_clone = cancel.clone();

                let query_result = tokio::time::timeout(timeout, async {
                    tokio::task::spawn_blocking(move || {
                        executor.execute_with_cancellation(query, cancel_clone)
                    })
                    .await
                })
                .await;

                // Handle results
                let result = match query_result {
                    Ok(Ok(Ok(result))) => result,
                    Ok(Ok(Err(query_err))) => {
                        return Err(PgError::Query(query_err));
                    }
                    Ok(Err(join_err)) => {
                        return Err(PgError::Internal(format!("Query task failed: {}", join_err)));
                    }
                    Err(_timeout) => {
                        cancel.cancel();
                        return Err(PgError::Timeout);
                    }
                };

                // Convert to PostgreSQL response
                let response =
                    query_result_to_response(result).map_err(|e| PgError::Internal(e.to_string()))?;
                Ok(vec![response])
            }
        }
    }
}

// Implement NoopStartupHandler for PgWireBackend
impl NoopStartupHandler for PgWireBackend {}

#[async_trait]
impl SimpleQueryHandler for PgWireBackend {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        debug!("Executing simple query: {}", query);

        match self.execute_command(query).await {
            Ok(responses) => {
                // Convert 'static lifetime to 'a (safe because Response is owned)
                Ok(responses)
            }
            Err(e) => {
                warn!("Query error: {}", e);
                Err(e.into())
            }
        }
    }
}

/// Handler factory that creates PgWireBackend instances for each connection
pub struct PgWireHandlerFactory {
    backend: Arc<PgWireBackend>,
}

impl PgWireHandlerFactory {
    /// Create a new handler factory
    pub fn new(app_state: Arc<AppState>, query_timeout: Duration) -> Self {
        Self {
            backend: Arc::new(PgWireBackend::new(app_state, query_timeout)),
        }
    }
}

impl PgWireServerHandlers for PgWireHandlerFactory {
    type StartupHandler = PgWireBackend;
    type SimpleQueryHandler = PgWireBackend;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.backend.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.backend.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}
