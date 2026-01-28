//! RusTs Retention - Retention policies for time series database
//!
//! This crate provides:
//! - Retention policy management
//! - Automatic partition dropping
//! - Storage tiering (hot/warm/cold)

pub mod error;
pub mod policy;
pub mod tiering;

pub use error::{Result, RetentionError};
pub use policy::{RetentionPolicy, RetentionPolicyManager};
pub use tiering::{StorageTier, TieringPolicy};
