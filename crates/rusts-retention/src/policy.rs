//! Retention policy management

use crate::error::{Result, RetentionError};
use chrono::{DateTime, Duration, Utc};
use parking_lot::RwLock;
use rusts_core::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Retention policy definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Policy name
    pub name: String,
    /// Database or measurement pattern (supports wildcards)
    pub pattern: String,
    /// Retention duration in nanoseconds
    pub duration: i64,
    /// Shard group duration (how data is partitioned)
    pub shard_group_duration: Option<i64>,
    /// Replication factor
    pub replication_factor: Option<u32>,
    /// Is this the default policy
    pub is_default: bool,
}

impl RetentionPolicy {
    /// Create a new retention policy
    pub fn new(name: &str, pattern: &str, duration: i64) -> Self {
        Self {
            name: name.to_string(),
            pattern: pattern.to_string(),
            duration,
            shard_group_duration: None,
            replication_factor: None,
            is_default: false,
        }
    }

    /// Create with duration string (e.g., "30d", "1w", "6h")
    pub fn with_duration_str(name: &str, pattern: &str, duration_str: &str) -> Result<Self> {
        let duration = parse_duration(duration_str)?;
        Ok(Self::new(name, pattern, duration))
    }

    /// Set as default policy
    pub fn default_policy(mut self) -> Self {
        self.is_default = true;
        self
    }

    /// Set shard group duration
    pub fn shard_group_duration(mut self, duration: i64) -> Self {
        self.shard_group_duration = Some(duration);
        self
    }

    /// Set replication factor
    pub fn replication_factor(mut self, factor: u32) -> Self {
        self.replication_factor = Some(factor);
        self
    }

    /// Check if a measurement matches this policy's pattern
    pub fn matches(&self, measurement: &str) -> bool {
        if self.pattern == "*" {
            return true;
        }

        if self.pattern.ends_with('*') {
            let prefix = &self.pattern[..self.pattern.len() - 1];
            return measurement.starts_with(prefix);
        }

        self.pattern == measurement
    }

    /// Calculate expiration timestamp based on current time
    pub fn expiration_time(&self, current_time: Timestamp) -> Timestamp {
        current_time - self.duration
    }

    /// Validate the policy
    pub fn validate(&self) -> Result<()> {
        if self.name.is_empty() {
            return Err(RetentionError::InvalidPolicy("Empty name".to_string()));
        }
        if self.pattern.is_empty() {
            return Err(RetentionError::InvalidPolicy("Empty pattern".to_string()));
        }
        if self.duration <= 0 {
            return Err(RetentionError::InvalidPolicy("Invalid duration".to_string()));
        }
        Ok(())
    }
}

/// Parse duration string to nanoseconds
fn parse_duration(s: &str) -> Result<i64> {
    let s = s.trim();
    if s.is_empty() {
        return Err(RetentionError::InvalidPolicy("Empty duration".to_string()));
    }

    let (num_str, unit) = if s.ends_with("ns") {
        (&s[..s.len() - 2], "ns")
    } else if s.ends_with("us") || s.ends_with("µs") {
        (&s[..s.len() - 2], "us")
    } else if s.ends_with("ms") {
        (&s[..s.len() - 2], "ms")
    } else if s.ends_with('s') && !s.ends_with("ns") && !s.ends_with("us") && !s.ends_with("ms") {
        (&s[..s.len() - 1], "s")
    } else if s.ends_with('m') {
        (&s[..s.len() - 1], "m")
    } else if s.ends_with('h') {
        (&s[..s.len() - 1], "h")
    } else if s.ends_with('d') {
        (&s[..s.len() - 1], "d")
    } else if s.ends_with('w') {
        (&s[..s.len() - 1], "w")
    } else {
        return Err(RetentionError::InvalidPolicy(format!("Invalid duration unit: {}", s)));
    };

    let num: i64 = num_str.parse()
        .map_err(|_| RetentionError::InvalidPolicy(format!("Invalid number: {}", num_str)))?;

    let nanos = match unit {
        "ns" => num,
        "us" => num * 1_000,
        "ms" => num * 1_000_000,
        "s" => num * 1_000_000_000,
        "m" => num * 60 * 1_000_000_000,
        "h" => num * 3600 * 1_000_000_000,
        "d" => num * 86400 * 1_000_000_000,
        "w" => num * 604800 * 1_000_000_000,
        _ => return Err(RetentionError::InvalidPolicy(format!("Unknown unit: {}", unit))),
    };

    Ok(nanos)
}

/// Retention policy manager
pub struct RetentionPolicyManager {
    /// Policies by name
    policies: RwLock<HashMap<String, RetentionPolicy>>,
    /// Default policy name
    default_policy: RwLock<Option<String>>,
}

impl RetentionPolicyManager {
    /// Create a new manager
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
            default_policy: RwLock::new(None),
        }
    }

    /// Add a retention policy
    pub fn add_policy(&self, policy: RetentionPolicy) -> Result<()> {
        policy.validate()?;

        let is_default = policy.is_default;
        let name = policy.name.clone();

        let mut policies = self.policies.write();
        policies.insert(policy.name.clone(), policy);

        if is_default {
            *self.default_policy.write() = Some(name);
        }

        Ok(())
    }

    /// Remove a retention policy
    pub fn remove_policy(&self, name: &str) -> Option<RetentionPolicy> {
        let mut policies = self.policies.write();
        let removed = policies.remove(name);

        if let Some(ref default) = *self.default_policy.read() {
            if default == name {
                *self.default_policy.write() = None;
            }
        }

        removed
    }

    /// Get a policy by name
    pub fn get_policy(&self, name: &str) -> Option<RetentionPolicy> {
        let policies = self.policies.read();
        policies.get(name).cloned()
    }

    /// Get policy for a measurement
    pub fn get_policy_for_measurement(&self, measurement: &str) -> Option<RetentionPolicy> {
        let policies = self.policies.read();

        // Find matching policy (prefer exact match over wildcard)
        let mut best_match: Option<&RetentionPolicy> = None;

        for policy in policies.values() {
            if policy.matches(measurement) {
                match best_match {
                    None => best_match = Some(policy),
                    Some(current) => {
                        // Prefer more specific patterns
                        if policy.pattern.len() > current.pattern.len() {
                            best_match = Some(policy);
                        }
                    }
                }
            }
        }

        best_match.cloned().or_else(|| {
            let default_name = self.default_policy.read().clone();
            default_name.and_then(|name| policies.get(&name).cloned())
        })
    }

    /// List all policies
    pub fn list_policies(&self) -> Vec<RetentionPolicy> {
        let policies = self.policies.read();
        policies.values().cloned().collect()
    }

    /// Get default policy
    pub fn default_policy(&self) -> Option<RetentionPolicy> {
        let default_name = self.default_policy.read().clone()?;
        self.get_policy(&default_name)
    }
}

impl Default for RetentionPolicyManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_creation() {
        let policy = RetentionPolicy::new("30d", "*", 30 * 86400 * 1_000_000_000);
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn test_policy_from_duration_string() {
        let policy = RetentionPolicy::with_duration_str("30d", "*", "30d").unwrap();
        assert_eq!(policy.duration, 30 * 86400 * 1_000_000_000);

        let policy = RetentionPolicy::with_duration_str("1w", "*", "1w").unwrap();
        assert_eq!(policy.duration, 7 * 86400 * 1_000_000_000);

        let policy = RetentionPolicy::with_duration_str("6h", "*", "6h").unwrap();
        assert_eq!(policy.duration, 6 * 3600 * 1_000_000_000);
    }

    #[test]
    fn test_policy_matching() {
        let policy = RetentionPolicy::new("test", "cpu*", 86400 * 1_000_000_000);

        assert!(policy.matches("cpu"));
        assert!(policy.matches("cpu_usage"));
        assert!(policy.matches("cpu.host.server01"));
        assert!(!policy.matches("memory"));
        assert!(!policy.matches("acpu"));

        let wildcard = RetentionPolicy::new("all", "*", 86400 * 1_000_000_000);
        assert!(wildcard.matches("anything"));
    }

    #[test]
    fn test_expiration_time() {
        let policy = RetentionPolicy::new("7d", "*", 7 * 86400 * 1_000_000_000);

        let current = 1000 * 86400 * 1_000_000_000_i64;
        let expiration = policy.expiration_time(current);

        assert_eq!(expiration, 993 * 86400 * 1_000_000_000);
    }

    #[test]
    fn test_policy_manager() {
        let manager = RetentionPolicyManager::new();

        let default_policy = RetentionPolicy::new("default", "*", 30 * 86400 * 1_000_000_000)
            .default_policy();

        let cpu_policy = RetentionPolicy::new("cpu_7d", "cpu*", 7 * 86400 * 1_000_000_000);

        manager.add_policy(default_policy).unwrap();
        manager.add_policy(cpu_policy).unwrap();

        // cpu measurements should use cpu_7d policy
        let policy = manager.get_policy_for_measurement("cpu").unwrap();
        assert_eq!(policy.name, "cpu_7d");

        // memory measurements should use default policy
        let policy = manager.get_policy_for_measurement("memory").unwrap();
        assert_eq!(policy.name, "default");
    }

    #[test]
    fn test_invalid_policy() {
        let policy = RetentionPolicy::new("", "*", 86400 * 1_000_000_000);
        assert!(policy.validate().is_err());

        let policy = RetentionPolicy::new("test", "*", 0);
        assert!(policy.validate().is_err());

        let policy = RetentionPolicy::new("test", "*", -1);
        assert!(policy.validate().is_err());
    }

    #[test]
    fn test_duration_parsing() {
        assert_eq!(parse_duration("1s").unwrap(), 1_000_000_000);
        assert_eq!(parse_duration("5m").unwrap(), 5 * 60 * 1_000_000_000);
        assert_eq!(parse_duration("24h").unwrap(), 24 * 3600 * 1_000_000_000);
        assert_eq!(parse_duration("7d").unwrap(), 7 * 86400 * 1_000_000_000);
        assert_eq!(parse_duration("2w").unwrap(), 14 * 86400 * 1_000_000_000);

        assert!(parse_duration("invalid").is_err());
        assert!(parse_duration("").is_err());
    }
}
