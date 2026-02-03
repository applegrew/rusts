//! PostgreSQL wire protocol backend implementation

use crate::encoder::{build_tables_schema, information_schema_columns_response, information_schema_columns_schema, information_schema_tables_response, information_schema_tables_schema, pg_catalog_response, pg_catalog_schema, query_result_to_response, single_value_response, tables_to_response, MeasurementColumns};
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
use tracing::{info, warn};

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

    /// Build MeasurementColumns for all measurements (shared by pg_catalog and information_schema)
    fn build_measurement_columns(&self) -> Vec<MeasurementColumns> {
        let measurements = self.app_state.series_index.measurements();
        measurements
            .iter()
            .map(|m| MeasurementColumns {
                measurement: m.clone(),
                tag_keys: self.app_state.series_index.get_tag_keys_for_measurement(m),
            })
            .collect()
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
            SqlCommand::SetVariable(_, _) | SqlCommand::Empty => {
                // SET commands and empty commands - return execution response
                Ok(Response::Execution(pgwire::api::results::Tag::new("SET")))
            }
            SqlCommand::SystemQuery { column, value } => {
                // Return a single-value response
                let response = single_value_response(column, value)
                    .map_err(|e| PgError::Internal(e.to_string()))?;
                Ok(response)
            }
            SqlCommand::PgCatalogQuery { table } => {
                // Return pg_catalog response with real data where available
                let measurement_columns = self.build_measurement_columns();
                let response = pg_catalog_response(table, &measurement_columns)
                    .map_err(|e| PgError::Internal(e.to_string()))?;
                Ok(response)
            }
            SqlCommand::InformationSchemaEmpty { .. } | SqlCommand::InformationSchemaViews => {
                // Return empty result for information_schema tables we don't support
                Ok(Response::EmptyQuery)
            }
            SqlCommand::InformationSchemaTables => {
                // Return list of measurements as tables
                let measurements = self.app_state.series_index.measurements();
                let response = information_schema_tables_response(&measurements)
                    .map_err(|e| PgError::Internal(e.to_string()))?;
                Ok(response)
            }
            SqlCommand::InformationSchemaColumns => {
                // Return columns for all measurements with actual tag keys
                let measurement_columns = self.build_measurement_columns();
                let response = information_schema_columns_response(&measurement_columns)
                    .map_err(|e| PgError::Internal(e.to_string()))?;
                Ok(response)
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
            SqlCommand::SystemQuery { column, .. } => {
                // Single column with the given name
                vec![FieldInfo::new(
                    column.clone(),
                    None,
                    None,
                    Type::TEXT,
                    pgwire::api::results::FieldFormat::Text,
                )]
            }
            SqlCommand::SetVariable(_, _) | SqlCommand::Empty => {
                // No result columns
                Vec::new()
            }
            SqlCommand::PgCatalogQuery { table } => {
                // Return proper schema for pg_catalog tables
                pg_catalog_schema(table)
            }
            SqlCommand::InformationSchemaEmpty { .. } | SqlCommand::InformationSchemaViews => {
                // Empty result set - no columns
                Vec::new()
            }
            SqlCommand::InformationSchemaTables => {
                // Schema for information_schema.tables
                information_schema_tables_schema()
            }
            SqlCommand::InformationSchemaColumns => {
                // Schema for information_schema.columns
                information_schema_columns_schema()
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
        info!("Simple query received: {}", query);

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
        info!("Extended query received: {}", statement.statement.sql);

        // For pg_catalog/information_schema queries with parameters, return appropriate results
        // (DBeaver and other clients send these during connection setup)
        if !portal.parameters.is_empty() {
            match &statement.statement.command {
                SqlCommand::PgCatalogQuery { table } => {
                    // Return result for parameterized pg_catalog queries
                    let measurement_columns = self.build_measurement_columns();
                    return pg_catalog_response(table, &measurement_columns)
                        .map_err(|e| PgError::Internal(e.to_string()).into());
                }
                SqlCommand::InformationSchemaEmpty { .. } | SqlCommand::InformationSchemaViews => {
                    // Return empty result
                    return Ok(Response::EmptyQuery);
                }
                SqlCommand::InformationSchemaTables => {
                    // Return tables
                    let measurements = self.app_state.series_index.measurements();
                    return information_schema_tables_response(&measurements)
                        .map_err(|e| PgError::Internal(e.to_string()).into());
                }
                SqlCommand::InformationSchemaColumns => {
                    // Return columns with actual tag keys
                    let measurement_columns = self.build_measurement_columns();
                    return information_schema_columns_response(&measurement_columns)
                        .map_err(|e| PgError::Internal(e.to_string()).into());
                }
                _ => {
                    return Err(PgError::SqlParse(rusts_sql::SqlError::UnsupportedFeature(
                        "Parameterized queries are not yet supported".to_string(),
                    ))
                    .into());
                }
            }
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
