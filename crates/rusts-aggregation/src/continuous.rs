//! Continuous aggregates (materialized views)

use crate::error::{AggregationError, Result};
use parking_lot::RwLock;
use rusts_core::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Aggregate function type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count,
    Sum,
    Mean,
    Min,
    Max,
    First,
    Last,
}

/// Continuous aggregate definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousAggregate {
    /// Unique name
    pub name: String,
    /// Source measurement
    pub source_measurement: String,
    /// Target measurement (for storing results)
    pub target_measurement: String,
    /// Aggregate interval (bucket size in nanos)
    pub interval: i64,
    /// Field aggregations (field_name -> function)
    pub aggregations: HashMap<String, AggregateFunction>,
    /// Group by tags (if any)
    pub group_by: Vec<String>,
    /// Retention duration for aggregated data (optional)
    pub retention: Option<i64>,
    /// Is aggregate enabled
    pub enabled: bool,
    /// Last processed timestamp
    pub last_processed: Timestamp,
}

impl ContinuousAggregate {
    /// Create a new continuous aggregate
    pub fn new(
        name: String,
        source_measurement: String,
        target_measurement: String,
        interval: i64,
    ) -> Self {
        Self {
            name,
            source_measurement,
            target_measurement,
            interval,
            aggregations: HashMap::new(),
            group_by: Vec::new(),
            retention: None,
            enabled: true,
            last_processed: 0,
        }
    }

    /// Add an aggregation
    pub fn add_aggregation(mut self, field: &str, function: AggregateFunction) -> Self {
        self.aggregations.insert(field.to_string(), function);
        self
    }

    /// Set group by tags
    pub fn group_by(mut self, tags: Vec<String>) -> Self {
        self.group_by = tags;
        self
    }

    /// Set retention
    pub fn retention(mut self, duration_nanos: i64) -> Self {
        self.retention = Some(duration_nanos);
        self
    }

    /// Validate the definition
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(AggregationError::InvalidDefinition("Empty name".to_string()));
        }
        if self.source_measurement.is_empty() {
            return Err(AggregationError::InvalidDefinition("Empty source measurement".to_string()));
        }
        if self.interval <= 0 {
            return Err(AggregationError::InvalidDefinition("Invalid interval".to_string()));
        }
        if self.aggregations.is_empty() {
            return Err(AggregationError::InvalidDefinition("No aggregations defined".to_string()));
        }
        Ok(())
    }
}

/// Continuous aggregate engine
pub struct ContinuousAggregateEngine {
    /// Registered aggregates
    aggregates: RwLock<HashMap<String, ContinuousAggregate>>,
}

impl ContinuousAggregateEngine {
    /// Create a new engine
    pub fn new() -> Self {
        Self {
            aggregates: RwLock::new(HashMap::new()),
        }
    }

    /// Register an aggregate
    pub fn register(&self, aggregate: ContinuousAggregate) -> Result<()> {
        aggregate.validate()?;

        let mut aggregates = self.aggregates.write();
        aggregates.insert(aggregate.name.clone(), aggregate);
        Ok(())
    }

    /// Unregister an aggregate
    pub fn unregister(&self, name: &str) -> Option<ContinuousAggregate> {
        let mut aggregates = self.aggregates.write();
        aggregates.remove(name)
    }

    /// Get an aggregate
    pub fn get(&self, name: &str) -> Option<ContinuousAggregate> {
        let aggregates = self.aggregates.read();
        aggregates.get(name).cloned()
    }

    /// List all aggregates
    pub fn list(&self) -> Vec<ContinuousAggregate> {
        let aggregates = self.aggregates.read();
        aggregates.values().cloned().collect()
    }

    /// Enable an aggregate
    pub fn enable(&self, name: &str) -> Result<()> {
        let mut aggregates = self.aggregates.write();
        if let Some(agg) = aggregates.get_mut(name) {
            agg.enabled = true;
            Ok(())
        } else {
            Err(AggregationError::NotFound(name.to_string()))
        }
    }

    /// Disable an aggregate
    pub fn disable(&self, name: &str) -> Result<()> {
        let mut aggregates = self.aggregates.write();
        if let Some(agg) = aggregates.get_mut(name) {
            agg.enabled = false;
            Ok(())
        } else {
            Err(AggregationError::NotFound(name.to_string()))
        }
    }

    /// Get aggregates that need processing for a time range
    pub fn get_pending(&self, current_time: Timestamp) -> Vec<ContinuousAggregate> {
        let aggregates = self.aggregates.read();
        aggregates
            .values()
            .filter(|a| a.enabled && a.last_processed < current_time - a.interval)
            .cloned()
            .collect()
    }

    /// Update last processed time for an aggregate
    pub fn update_processed(&self, name: &str, timestamp: Timestamp) -> Result<()> {
        let mut aggregates = self.aggregates.write();
        if let Some(agg) = aggregates.get_mut(name) {
            agg.last_processed = timestamp;
            Ok(())
        } else {
            Err(AggregationError::NotFound(name.to_string()))
        }
    }
}

impl Default for ContinuousAggregateEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_definition() {
        let agg = ContinuousAggregate::new(
            "cpu_hourly".to_string(),
            "cpu".to_string(),
            "cpu_1h".to_string(),
            3600_000_000_000, // 1 hour
        )
        .add_aggregation("usage", AggregateFunction::Mean)
        .add_aggregation("usage", AggregateFunction::Max)
        .group_by(vec!["host".to_string()]);

        assert!(agg.validate().is_ok());
        assert_eq!(agg.aggregations.len(), 1); // Last one wins for same field
    }

    #[test]
    fn test_aggregate_validation() {
        // Empty name
        let agg = ContinuousAggregate::new(
            "".to_string(),
            "cpu".to_string(),
            "cpu_1h".to_string(),
            3600_000_000_000,
        );
        assert!(agg.validate().is_err());

        // No aggregations
        let agg = ContinuousAggregate::new(
            "test".to_string(),
            "cpu".to_string(),
            "cpu_1h".to_string(),
            3600_000_000_000,
        );
        assert!(agg.validate().is_err());
    }

    #[test]
    fn test_engine_operations() {
        let engine = ContinuousAggregateEngine::new();

        let agg = ContinuousAggregate::new(
            "cpu_hourly".to_string(),
            "cpu".to_string(),
            "cpu_1h".to_string(),
            3600_000_000_000,
        )
        .add_aggregation("usage", AggregateFunction::Mean);

        engine.register(agg.clone()).unwrap();

        assert!(engine.get("cpu_hourly").is_some());
        assert!(engine.get("nonexistent").is_none());

        engine.disable("cpu_hourly").unwrap();
        let retrieved = engine.get("cpu_hourly").unwrap();
        assert!(!retrieved.enabled);

        engine.enable("cpu_hourly").unwrap();
        let retrieved = engine.get("cpu_hourly").unwrap();
        assert!(retrieved.enabled);

        engine.unregister("cpu_hourly");
        assert!(engine.get("cpu_hourly").is_none());
    }
}
