//! Storage tiering (hot/warm/cold)

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Storage tier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageTier {
    /// Hot tier - fast storage (SSD), recent data
    Hot,
    /// Warm tier - medium storage, older data
    Warm,
    /// Cold tier - slow/cheap storage (HDD/S3), archive data
    Cold,
}

impl StorageTier {
    /// Get recommended compression level for this tier
    pub fn recommended_compression(&self) -> rusts_compression::CompressionLevel {
        match self {
            StorageTier::Hot => rusts_compression::CompressionLevel::Fast,
            StorageTier::Warm => rusts_compression::CompressionLevel::Default,
            StorageTier::Cold => rusts_compression::CompressionLevel::Best,
        }
    }
}

/// Tiering policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringPolicy {
    /// Policy name
    pub name: String,
    /// Age threshold for warm tier (nanoseconds)
    pub warm_after: i64,
    /// Age threshold for cold tier (nanoseconds)
    pub cold_after: i64,
    /// Hot tier path
    pub hot_path: PathBuf,
    /// Warm tier path (optional, defaults to hot_path/warm)
    pub warm_path: Option<PathBuf>,
    /// Cold tier path (optional, defaults to hot_path/cold)
    pub cold_path: Option<PathBuf>,
}

impl TieringPolicy {
    /// Create a new tiering policy
    pub fn new(name: &str, hot_path: PathBuf, warm_after: i64, cold_after: i64) -> Self {
        Self {
            name: name.to_string(),
            warm_after,
            cold_after,
            hot_path,
            warm_path: None,
            cold_path: None,
        }
    }

    /// Set warm tier path
    pub fn warm_path(mut self, path: PathBuf) -> Self {
        self.warm_path = Some(path);
        self
    }

    /// Set cold tier path
    pub fn cold_path(mut self, path: PathBuf) -> Self {
        self.cold_path = Some(path);
        self
    }

    /// Determine tier for data of a given age
    pub fn tier_for_age(&self, age_nanos: i64) -> StorageTier {
        if age_nanos >= self.cold_after {
            StorageTier::Cold
        } else if age_nanos >= self.warm_after {
            StorageTier::Warm
        } else {
            StorageTier::Hot
        }
    }

    /// Get path for a tier
    pub fn path_for_tier(&self, tier: StorageTier) -> PathBuf {
        match tier {
            StorageTier::Hot => self.hot_path.clone(),
            StorageTier::Warm => self.warm_path.clone()
                .unwrap_or_else(|| self.hot_path.join("warm")),
            StorageTier::Cold => self.cold_path.clone()
                .unwrap_or_else(|| self.hot_path.join("cold")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier_for_age() {
        let policy = TieringPolicy::new(
            "default",
            PathBuf::from("/data"),
            7 * 86400 * 1_000_000_000,  // warm after 7 days
            30 * 86400 * 1_000_000_000, // cold after 30 days
        );

        // Recent data -> hot
        assert_eq!(policy.tier_for_age(0), StorageTier::Hot);
        assert_eq!(policy.tier_for_age(6 * 86400 * 1_000_000_000), StorageTier::Hot);

        // 7-30 days -> warm
        assert_eq!(policy.tier_for_age(7 * 86400 * 1_000_000_000), StorageTier::Warm);
        assert_eq!(policy.tier_for_age(20 * 86400 * 1_000_000_000), StorageTier::Warm);

        // 30+ days -> cold
        assert_eq!(policy.tier_for_age(30 * 86400 * 1_000_000_000), StorageTier::Cold);
        assert_eq!(policy.tier_for_age(365 * 86400 * 1_000_000_000), StorageTier::Cold);
    }

    #[test]
    fn test_path_for_tier() {
        let policy = TieringPolicy::new(
            "default",
            PathBuf::from("/data/hot"),
            7 * 86400 * 1_000_000_000,
            30 * 86400 * 1_000_000_000,
        )
        .warm_path(PathBuf::from("/data/warm"))
        .cold_path(PathBuf::from("/archive/cold"));

        assert_eq!(policy.path_for_tier(StorageTier::Hot), PathBuf::from("/data/hot"));
        assert_eq!(policy.path_for_tier(StorageTier::Warm), PathBuf::from("/data/warm"));
        assert_eq!(policy.path_for_tier(StorageTier::Cold), PathBuf::from("/archive/cold"));
    }

    #[test]
    fn test_default_paths() {
        let policy = TieringPolicy::new(
            "default",
            PathBuf::from("/data"),
            7 * 86400 * 1_000_000_000,
            30 * 86400 * 1_000_000_000,
        );

        assert_eq!(policy.path_for_tier(StorageTier::Hot), PathBuf::from("/data"));
        assert_eq!(policy.path_for_tier(StorageTier::Warm), PathBuf::from("/data/warm"));
        assert_eq!(policy.path_for_tier(StorageTier::Cold), PathBuf::from("/data/cold"));
    }

    #[test]
    fn test_recommended_compression() {
        assert_eq!(
            StorageTier::Hot.recommended_compression(),
            rusts_compression::CompressionLevel::Fast
        );
        assert_eq!(
            StorageTier::Warm.recommended_compression(),
            rusts_compression::CompressionLevel::Default
        );
        assert_eq!(
            StorageTier::Cold.recommended_compression(),
            rusts_compression::CompressionLevel::Best
        );
    }
}
