//! RusTs API - REST API for time series database
//!
//! This crate provides the HTTP API:
//! - Line protocol parser (InfluxDB compatible)
//! - Write endpoint with batching
//! - Query endpoint with streaming
//! - Health and ready endpoints
//! - Authentication middleware

pub mod auth;
pub mod error;
pub mod handlers;
pub mod line_protocol;
pub mod memory;
pub mod router;

pub use error::{ApiError, Result};
pub use handlers::{StartupPhase, StartupState};
pub use line_protocol::LineProtocolParser;
pub use router::create_router;
