//! Downsampling engine

use crate::error::{AggregationError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Downsample configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownsampleConfig {
    /// Source measurement
    pub source: String,
    /// Target measurement
    pub target: String,
    /// Downsample interval in nanoseconds
    pub interval: i64,
    /// Field configurations
    pub fields: HashMap<String, DownsampleField>,
}

/// Field downsample configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownsampleField {
    /// Aggregation function
    pub function: DownsampleFunction,
    /// Output field name (defaults to source name)
    pub output_name: Option<String>,
}

/// Downsample function
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownsampleFunction {
    /// Keep first value in interval
    First,
    /// Keep last value in interval
    Last,
    /// Compute mean
    Mean,
    /// Compute min
    Min,
    /// Compute max
    Max,
    /// Compute sum
    Sum,
    /// Count values
    Count,
}

/// Downsample engine for reducing data resolution
pub struct DownsampleEngine {
    /// Registered configurations
    configs: HashMap<String, DownsampleConfig>,
}

impl DownsampleEngine {
    /// Create a new engine
    pub fn new() -> Self {
        Self {
            configs: HashMap::new(),
        }
    }

    /// Register a downsample configuration
    pub fn register(&mut self, config: DownsampleConfig) -> Result<()> {
        if config.interval <= 0 {
            return Err(AggregationError::InvalidDefinition("Invalid interval".to_string()));
        }
        if config.fields.is_empty() {
            return Err(AggregationError::InvalidDefinition("No fields configured".to_string()));
        }

        let name = format!("{}_{}", config.source, config.target);
        self.configs.insert(name, config);
        Ok(())
    }

    /// Get configurations for a source measurement
    pub fn get_for_source(&self, source: &str) -> Vec<&DownsampleConfig> {
        self.configs
            .values()
            .filter(|c| c.source == source)
            .collect()
    }

    /// List all configurations
    pub fn list(&self) -> Vec<&DownsampleConfig> {
        self.configs.values().collect()
    }
}

impl Default for DownsampleEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to create common downsample configurations
pub struct DownsampleBuilder {
    source: String,
    target: String,
    interval: i64,
    fields: HashMap<String, DownsampleField>,
}

impl DownsampleBuilder {
    /// Create a new builder
    pub fn new(source: &str, target: &str, interval: i64) -> Self {
        Self {
            source: source.to_string(),
            target: target.to_string(),
            interval,
            fields: HashMap::new(),
        }
    }

    /// Add a field with mean aggregation
    pub fn mean(mut self, field: &str) -> Self {
        self.fields.insert(
            field.to_string(),
            DownsampleField {
                function: DownsampleFunction::Mean,
                output_name: None,
            },
        );
        self
    }

    /// Add a field with min aggregation
    pub fn min(mut self, field: &str) -> Self {
        self.fields.insert(
            field.to_string(),
            DownsampleField {
                function: DownsampleFunction::Min,
                output_name: Some(format!("{}_min", field)),
            },
        );
        self
    }

    /// Add a field with max aggregation
    pub fn max(mut self, field: &str) -> Self {
        self.fields.insert(
            field.to_string(),
            DownsampleField {
                function: DownsampleFunction::Max,
                output_name: Some(format!("{}_max", field)),
            },
        );
        self
    }

    /// Add a field with custom configuration
    pub fn field(mut self, name: &str, function: DownsampleFunction, output: Option<&str>) -> Self {
        self.fields.insert(
            name.to_string(),
            DownsampleField {
                function,
                output_name: output.map(|s| s.to_string()),
            },
        );
        self
    }

    /// Build the configuration
    pub fn build(self) -> DownsampleConfig {
        DownsampleConfig {
            source: self.source,
            target: self.target,
            interval: self.interval,
            fields: self.fields,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_downsample_builder() {
        let config = DownsampleBuilder::new("cpu", "cpu_1h", 3600_000_000_000)
            .mean("usage")
            .min("usage")
            .max("usage")
            .build();

        assert_eq!(config.source, "cpu");
        assert_eq!(config.target, "cpu_1h");
        assert!(config.fields.contains_key("usage"));
    }

    #[test]
    fn test_downsample_engine() {
        let mut engine = DownsampleEngine::new();

        let config = DownsampleBuilder::new("cpu", "cpu_1h", 3600_000_000_000)
            .mean("usage")
            .build();

        engine.register(config).unwrap();

        let configs = engine.get_for_source("cpu");
        assert_eq!(configs.len(), 1);
    }

    #[test]
    fn test_invalid_config() {
        let mut engine = DownsampleEngine::new();

        // Invalid interval
        let config = DownsampleConfig {
            source: "cpu".to_string(),
            target: "cpu_1h".to_string(),
            interval: 0,
            fields: HashMap::new(),
        };
        assert!(engine.register(config).is_err());

        // No fields
        let config = DownsampleConfig {
            source: "cpu".to_string(),
            target: "cpu_1h".to_string(),
            interval: 3600,
            fields: HashMap::new(),
        };
        assert!(engine.register(config).is_err());
    }
}
