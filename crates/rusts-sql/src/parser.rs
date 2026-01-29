//! SQL parsing wrapper around sqlparser-rs

use crate::error::{Result, SqlError};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// SQL parser for RusTs queries
pub struct SqlParser;

impl SqlParser {
    /// Parse a SQL query string into a statement
    pub fn parse(sql: &str) -> Result<Statement> {
        let dialect = GenericDialect {};
        let statements = Parser::parse_sql(&dialect, sql)?;

        if statements.is_empty() {
            return Err(SqlError::Parse("Empty SQL statement".to_string()));
        }

        if statements.len() > 1 {
            return Err(SqlError::UnsupportedFeature(
                "Multiple statements not supported".to_string(),
            ));
        }

        Ok(statements.into_iter().next().unwrap())
    }

    /// Parse and validate that the statement is a SELECT
    pub fn parse_select(sql: &str) -> Result<Statement> {
        let stmt = Self::parse(sql)?;

        match &stmt {
            Statement::Query(_) => Ok(stmt),
            _ => Err(SqlError::UnsupportedFeature(format!(
                "Only SELECT queries are supported, got: {:?}",
                stmt
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let sql = "SELECT * FROM cpu";
        let stmt = SqlParser::parse(sql).unwrap();
        assert!(matches!(stmt, Statement::Query(_)));
    }

    #[test]
    fn test_parse_select_with_where() {
        let sql = "SELECT usage, temperature FROM cpu WHERE host = 'server01'";
        let stmt = SqlParser::parse(sql).unwrap();
        assert!(matches!(stmt, Statement::Query(_)));
    }

    #[test]
    fn test_parse_select_with_aggregation() {
        let sql = "SELECT COUNT(*), AVG(usage) FROM cpu GROUP BY host";
        let stmt = SqlParser::parse(sql).unwrap();
        assert!(matches!(stmt, Statement::Query(_)));
    }

    #[test]
    fn test_parse_select_with_time_range() {
        let sql = "SELECT * FROM cpu WHERE time >= '2024-01-01' AND time < '2024-01-02'";
        let stmt = SqlParser::parse(sql).unwrap();
        assert!(matches!(stmt, Statement::Query(_)));
    }

    #[test]
    fn test_parse_empty_sql() {
        let sql = "";
        let result = SqlParser::parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_sql() {
        let sql = "SELEKT * FORM cpu";
        let result = SqlParser::parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_multiple_statements_rejected() {
        let sql = "SELECT * FROM cpu; SELECT * FROM memory";
        let result = SqlParser::parse(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_select_validates_query_type() {
        let sql = "SELECT * FROM cpu";
        let stmt = SqlParser::parse_select(sql).unwrap();
        assert!(matches!(stmt, Statement::Query(_)));
    }

    #[test]
    fn test_parse_insert_rejected_by_parse_select() {
        let sql = "INSERT INTO cpu VALUES (1, 2)";
        let result = SqlParser::parse_select(sql);
        assert!(result.is_err());
    }
}
