//! PostgreSQL error code mapping for RusTs errors

use pgwire::error::{ErrorInfo, PgWireError};
use rusts_query::QueryError;
use rusts_sql::SqlError;
use thiserror::Error;

/// PostgreSQL SQLSTATE error codes
pub mod sqlstate {
    /// Syntax error (42601)
    pub const SYNTAX_ERROR: &str = "42601";
    /// Undefined table (42P01)
    pub const UNDEFINED_TABLE: &str = "42P01";
    /// Query canceled (57014)
    pub const QUERY_CANCELED: &str = "57014";
    /// Internal error (XX000)
    pub const INTERNAL_ERROR: &str = "XX000";
    /// Feature not supported (0A000)
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
    /// Invalid parameter value (22023)
    pub const INVALID_PARAMETER_VALUE: &str = "22023";
}

/// Error type for PostgreSQL wire protocol operations
#[derive(Debug, Error)]
pub enum PgError {
    #[error("SQL parse error: {0}")]
    SqlParse(#[from] SqlError),

    #[error("Query execution error: {0}")]
    Query(#[from] QueryError),

    #[error("Query timeout")]
    Timeout,

    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl PgError {
    /// Get the PostgreSQL SQLSTATE error code for this error
    pub fn sqlstate(&self) -> &'static str {
        match self {
            PgError::SqlParse(sql_err) => match sql_err {
                SqlError::Parse(_) => sqlstate::SYNTAX_ERROR,
                SqlError::Translation(_) => sqlstate::SYNTAX_ERROR,
                SqlError::InvalidTimeExpression(_) => sqlstate::INVALID_PARAMETER_VALUE,
                SqlError::UnsupportedFeature(_) => sqlstate::FEATURE_NOT_SUPPORTED,
                SqlError::MissingClause(_) => sqlstate::SYNTAX_ERROR,
                SqlError::Query(_) => sqlstate::INTERNAL_ERROR,
                SqlError::UnknownFunction(_) => sqlstate::UNDEFINED_TABLE,
                SqlError::InvalidAggregation(_) => sqlstate::INVALID_PARAMETER_VALUE,
                SqlError::InvalidInterval(_) => sqlstate::INVALID_PARAMETER_VALUE,
            },
            PgError::Query(query_err) => match query_err {
                QueryError::MeasurementNotFound(_) => sqlstate::UNDEFINED_TABLE,
                QueryError::Cancelled => sqlstate::QUERY_CANCELED,
                _ => sqlstate::INTERNAL_ERROR,
            },
            PgError::Timeout => sqlstate::QUERY_CANCELED,
            PgError::ServiceUnavailable(_) => sqlstate::INTERNAL_ERROR,
            PgError::Internal(_) => sqlstate::INTERNAL_ERROR,
        }
    }

    /// Convert to a PgWireError with proper error info
    pub fn to_pgwire_error(&self) -> PgWireError {
        let error_info = ErrorInfo::new(
            "ERROR".to_string(),
            self.sqlstate().to_string(),
            self.to_string(),
        );
        PgWireError::UserError(Box::new(error_info))
    }
}

/// Convert a PgError to PgWireError for returning to clients
impl From<PgError> for PgWireError {
    fn from(err: PgError) -> Self {
        err.to_pgwire_error()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_parse_error_code() {
        let err = PgError::SqlParse(SqlError::Parse("unexpected token".to_string()));
        assert_eq!(err.sqlstate(), sqlstate::SYNTAX_ERROR);
    }

    #[test]
    fn test_measurement_not_found_error_code() {
        let err = PgError::Query(QueryError::MeasurementNotFound("cpu".to_string()));
        assert_eq!(err.sqlstate(), sqlstate::UNDEFINED_TABLE);
    }

    #[test]
    fn test_timeout_error_code() {
        let err = PgError::Timeout;
        assert_eq!(err.sqlstate(), sqlstate::QUERY_CANCELED);
    }

    #[test]
    fn test_cancelled_error_code() {
        let err = PgError::Query(QueryError::Cancelled);
        assert_eq!(err.sqlstate(), sqlstate::QUERY_CANCELED);
    }

    #[test]
    fn test_unsupported_feature_error_code() {
        let err = PgError::SqlParse(SqlError::UnsupportedFeature("UNION not supported".to_string()));
        assert_eq!(err.sqlstate(), sqlstate::FEATURE_NOT_SUPPORTED);
    }
}
