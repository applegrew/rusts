//! SQL query interface for RusTs time series database
//!
//! This crate provides SQL parsing and translation to the native Query model,
//! using sqlparser-rs for lightweight SQL support.
//!
//! # Features
//!
//! - Basic SELECT queries with field selection
//! - Time range filtering via WHERE clause
//! - Tag filtering (=, !=, IN, regex, IS NOT NULL)
//! - Aggregation functions (COUNT, SUM, AVG, MIN, MAX, etc.)
//! - GROUP BY tags and time bucketing
//! - ORDER BY, LIMIT, OFFSET
//!
//! # Example
//!
//! ```ignore
//! use rusts_sql::{SqlParser, SqlTranslator};
//!
//! let sql = "SELECT mean(usage), max(usage) FROM cpu WHERE host = 'server01' GROUP BY time_bucket('1h', time)";
//! let stmt = SqlParser::parse(sql)?;
//! let query = SqlTranslator::translate(&stmt)?;
//! ```

mod error;
mod functions;
mod parser;
mod translator;

pub use error::{Result, SqlError};
pub use functions::FunctionRegistry;
pub use parser::SqlParser;
pub use translator::SqlTranslator;
