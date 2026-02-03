//! PostgreSQL wire protocol backend implementation

use crate::encoder::{build_tables_schema, query_result_to_response, tables_to_response};
use crate::error::PgError;
use async_trait::async_trait;
use futures::Sink;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, FieldInfo, Response};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use rusts_api::handlers::AppState;
use rusts_sql::{SqlCommand, SqlParser, SqlTranslator};
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Parsed SQL statement for extended query protocol
#[derive(Debug, Clone)]
pub struct ParsedStatement {
    /// Original SQL text
    pub sql: String,
    /// Parsed command
    pub command: SqlCommand,
}

/// Query parser for RusTs SQL
#[derive(Debug, Clone)]
pub struct RustsQueryParser;

#[async_trait]
impl QueryParser for RustsQueryParser {
    type Statement = ParsedStatement;

    async fn parse_sql(&self, sql: &str, _types: &[Type]) -> PgWireResult<Self::Statement> {
        // Parse SQL
        let stmt = SqlParser::parse(sql).map_err(|e| PgError::SqlParse(e))?;

        // Translate to command
        let command = SqlTranslator::translate_command(&stmt).map_err(|e| PgError::SqlParse(e))?;

        Ok(ParsedStatement {
            sql: sql.to_string(),
            command,
        })
    }
}

/// PostgreSQL wire protocol backend for RusTs
pub struct PgWireBackend {
    app_state: Arc<AppState>,
    query_timeout: Duration,
    query_parser: Arc<RustsQueryParser>,
}

impl PgWireBackend {
    /// Create a new PostgreSQL backend
    pub fn new(app_state: Arc<AppState>, query_timeout: Duration) -> Self {
        Self {
            app_state,
            query_timeout,
            query_parser: Arc::new(RustsQueryParser),
        }
    }

    /// Execute a SQL command and return the PostgreSQL response
    async fn execute_command(&self, command: &SqlCommand) -> Result<Response<'static>, PgError> {
        match command {
            SqlCommand::ShowTables => {
                // Return list of measurements as tables
                let measurements = self.app_state.series_index.measurements();
                let response = tables_to_response(measurements)
                    .map_err(|e| PgError::Internal(e.to_string()))?;
                Ok(response)
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
                let query = query.clone();

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
                Ok(response)
            }
        }
    }

    /// Execute from SQL string (for simple query protocol)
    async fn execute_sql(&self, sql: &str) -> Result<Vec<Response<'static>>, PgError> {
        // Parse SQL
        let stmt = SqlParser::parse(sql)?;

        // Translate to command
        let command = SqlTranslator::translate_command(&stmt)?;

        let response = self.execute_command(&command).await?;
        Ok(vec![response])
    }

    /// Get schema for a command (used for Describe in extended query)
    fn get_command_schema(&self, command: &SqlCommand) -> Vec<FieldInfo> {
        match command {
            SqlCommand::ShowTables => {
                // Single column: name (TEXT)
                (*build_tables_schema()).clone()
            }
            SqlCommand::Explain(_) | SqlCommand::Query(_) => {
                // For queries, we don't know the schema until execution
                // Return empty schema - client will get actual schema on execute
                // This is a limitation but works for most clients
                Vec::new()
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

        match self.execute_sql(query).await {
            Ok(responses) => Ok(responses),
            Err(e) => {
                warn!("Query error: {}", e);
                Err(e.into())
            }
        }
    }
}

#[async_trait]
impl ExtendedQueryHandler for PgWireBackend {
    type Statement = ParsedStatement;
    type QueryParser = RustsQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let statement = &portal.statement;
        debug!("Executing extended query: {}", statement.statement.sql);

        // Check for parameters - we don't support parameterized queries yet
        if !portal.parameters.is_empty() {
            return Err(PgError::SqlParse(rusts_sql::SqlError::UnsupportedFeature(
                "Parameterized queries are not yet supported".to_string(),
            ))
            .into());
        }

        match self.execute_command(&statement.statement.command).await {
            Ok(response) => Ok(response),
            Err(e) => {
                warn!("Query error: {}", e);
                Err(e.into())
            }
        }
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        statement: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let schema = self.get_command_schema(&statement.statement.command);

        // No parameters supported
        Ok(DescribeStatementResponse::new(vec![], schema))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let schema = self.get_command_schema(&portal.statement.statement.command);

        Ok(DescribePortalResponse::new(schema))
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
    type ExtendedQueryHandler = PgWireBackend;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.backend.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        self.backend.clone()
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
