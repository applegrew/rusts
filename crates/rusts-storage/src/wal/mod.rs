//! Write-Ahead Log implementation
//!
//! The WAL ensures durability by writing data to disk before acknowledging writes.
//! Supports multiple durability modes for different performance/safety tradeoffs.

mod reader;
mod writer;

pub use reader::WalReader;
pub use writer::{WalDurability, WalWriter};
