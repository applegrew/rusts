//! PostgreSQL wire protocol backend implementation

use crate::encoder::{query_result_to_response, tables_to_response};
use crate::error::PgError;
use async_trait::async_trait;
use futures::Sink;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, NoopErrorHandler, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::extendedquery::Parse;
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
    type ExtendedQueryHandler = UnsupportedExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.backend.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(UnsupportedExtendedQueryHandler)
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

/// Extended query handler that returns a proper error instead of panicking.
/// This allows clients like DBeaver to receive an error message rather than
/// experiencing a connection drop.
#[derive(Debug, Clone)]
pub struct UnsupportedExtendedQueryHandler;

impl UnsupportedExtendedQueryHandler {
    fn not_supported_error() -> PgWireError {
        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_string(),
            "0A000".to_string(), // feature_not_supported
            "Extended query protocol (prepared statements) is not supported. Use simple query mode.".to_string(),
        )))
    }
}

#[async_trait]
impl ExtendedQueryHandler for UnsupportedExtendedQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser)
    }

    /// Override on_parse to return error before any parsing happens
    async fn on_parse<C>(&self, _client: &mut C, _message: Parse) -> PgWireResult<()>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(Self::not_supported_error())
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        _portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Err(Self::not_supported_error())
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Err(Self::not_supported_error())
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Err(Self::not_supported_error())
    }
}
