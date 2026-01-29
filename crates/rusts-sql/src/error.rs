//! SQL-specific error types

use thiserror::Error;

/// SQL query errors
#[derive(Debug, Error)]
pub enum SqlError {
    /// SQL parsing error
    #[error("SQL parse error: {0}")]
    Parse(String),

    /// Unsupported SQL feature
    #[error("Unsupported SQL feature: {0}")]
    UnsupportedFeature(String),

    /// Invalid time expression
    #[error("Invalid time expression: {0}")]
    InvalidTimeExpression(String),

    /// Unknown function
    #[error("Unknown function: {0}")]
    UnknownFunction(String),

    /// Invalid aggregation
    #[error("Invalid aggregation: {0}")]
    InvalidAggregation(String),

    /// Translation error
    #[error("Translation error: {0}")]
    Translation(String),

    /// Invalid interval/duration
    #[error("Invalid interval: {0}")]
    InvalidInterval(String),

    /// Missing required clause
    #[error("Missing required clause: {0}")]
    MissingClause(String),

    /// Query error from underlying query engine
    #[error("Query error: {0}")]
    Query(#[from] rusts_query::QueryError),
}

/// Result type for SQL operations
pub type Result<T> = std::result::Result<T, SqlError>;

impl From<sqlparser::parser::ParserError> for SqlError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        SqlError::Parse(e.to_string())
    }
}
