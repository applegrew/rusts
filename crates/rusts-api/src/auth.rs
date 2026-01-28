//! Authentication middleware

use crate::error::{ApiError, Result};
use axum::{
    extract::Request,
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::Response,
};
use dashmap::DashMap;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// JWT claims
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    /// Subject (user ID or API key ID)
    pub sub: String,
    /// Expiration time (Unix timestamp)
    pub exp: u64,
    /// Issued at (Unix timestamp)
    pub iat: u64,
    /// Permissions
    pub permissions: Vec<String>,
}

/// API key entry
#[derive(Debug, Clone)]
pub struct ApiKey {
    pub id: String,
    pub key_hash: String,
    pub permissions: Vec<String>,
    pub created_at: u64,
    pub last_used: u64,
}

/// Authentication configuration
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Enable authentication
    pub enabled: bool,
    /// JWT secret key
    pub jwt_secret: String,
    /// Token expiration duration
    pub token_expiration: Duration,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            jwt_secret: "default-secret-change-me".to_string(),
            token_expiration: Duration::from_secs(3600), // 1 hour
        }
    }
}

/// Authentication state
pub struct AuthState {
    config: AuthConfig,
    /// API keys (key hash -> ApiKey)
    api_keys: DashMap<String, ApiKey>,
}

impl AuthState {
    /// Create new auth state
    pub fn new(config: AuthConfig) -> Self {
        Self {
            config,
            api_keys: DashMap::new(),
        }
    }

    /// Check if auth is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Generate a JWT token
    pub fn generate_token(&self, subject: &str, permissions: Vec<String>) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let claims = Claims {
            sub: subject.to_string(),
            exp: now + self.config.token_expiration.as_secs(),
            iat: now,
            permissions,
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.config.jwt_secret.as_bytes()),
        )
        .map_err(|e| ApiError::Internal(format!("Failed to generate token: {}", e)))
    }

    /// Validate a JWT token
    pub fn validate_token(&self, token: &str) -> Result<Claims> {
        decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.config.jwt_secret.as_bytes()),
            &Validation::default(),
        )
        .map(|data| data.claims)
        .map_err(|e| ApiError::Unauthorized(format!("Invalid token: {}", e)))
    }

    /// Register an API key
    pub fn register_api_key(&self, id: String, key_hash: String, permissions: Vec<String>) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.api_keys.insert(
            key_hash.clone(),
            ApiKey {
                id,
                key_hash,
                permissions,
                created_at: now,
                last_used: now,
            },
        );
    }

    /// Validate an API key (by hash)
    pub fn validate_api_key(&self, key_hash: &str) -> Result<ApiKey> {
        self.api_keys
            .get(key_hash)
            .map(|entry| {
                // Update last_used (in a real impl, this would be async)
                entry.clone()
            })
            .ok_or_else(|| ApiError::Unauthorized("Invalid API key".to_string()))
    }

    /// Check if claims have required permission
    pub fn has_permission(claims: &Claims, permission: &str) -> bool {
        claims.permissions.iter().any(|p| p == permission || p == "*")
    }
}

/// Extract authorization from request
pub fn extract_auth(req: &Request) -> Option<String> {
    req.headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| {
            if s.starts_with("Bearer ") {
                s[7..].to_string()
            } else if s.starts_with("Token ") {
                s[6..].to_string()
            } else {
                s.to_string()
            }
        })
}

/// Authentication middleware
pub async fn auth_middleware(
    req: Request,
    next: Next,
) -> std::result::Result<Response, StatusCode> {
    // Get auth state from extensions
    let auth_state = req.extensions().get::<Arc<AuthState>>();

    if let Some(auth_state) = auth_state {
        if auth_state.is_enabled() {
            // Extract token
            let token = extract_auth(&req).ok_or(StatusCode::UNAUTHORIZED)?;

            // Validate token
            auth_state
                .validate_token(&token)
                .map_err(|_| StatusCode::UNAUTHORIZED)?;
        }
    }

    Ok(next.run(req).await)
}

/// Rate limiter
pub struct RateLimiter {
    /// Requests per window
    requests_per_window: u32,
    /// Window duration
    window_duration: Duration,
    /// Client state (client_id -> (count, window_start))
    clients: DashMap<String, (u32, u64)>,
}

impl RateLimiter {
    /// Create new rate limiter
    pub fn new(requests_per_window: u32, window_duration: Duration) -> Self {
        Self {
            requests_per_window,
            window_duration,
            clients: DashMap::new(),
        }
    }

    /// Check if request is allowed
    pub fn check(&self, client_id: &str) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let window_secs = self.window_duration.as_secs();

        let mut entry = self.clients.entry(client_id.to_string()).or_insert((0, now));
        let (count, window_start) = entry.value_mut();

        // Check if window has expired
        if now - *window_start >= window_secs {
            *count = 1;
            *window_start = now;
            return true;
        }

        // Check if under limit
        if *count < self.requests_per_window {
            *count += 1;
            return true;
        }

        false
    }

    /// Get remaining requests in current window
    pub fn remaining(&self, client_id: &str) -> u32 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let window_secs = self.window_duration.as_secs();

        if let Some(entry) = self.clients.get(client_id) {
            let (count, window_start) = *entry;

            if now - window_start >= window_secs {
                return self.requests_per_window;
            }

            return self.requests_per_window.saturating_sub(count);
        }

        self.requests_per_window
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jwt_roundtrip() {
        let config = AuthConfig {
            enabled: true,
            jwt_secret: "test-secret".to_string(),
            token_expiration: Duration::from_secs(3600),
        };

        let auth = AuthState::new(config);

        let permissions = vec!["read".to_string(), "write".to_string()];
        let token = auth.generate_token("user123", permissions.clone()).unwrap();

        let claims = auth.validate_token(&token).unwrap();
        assert_eq!(claims.sub, "user123");
        assert_eq!(claims.permissions, permissions);
    }

    #[test]
    fn test_invalid_token() {
        let config = AuthConfig {
            enabled: true,
            jwt_secret: "test-secret".to_string(),
            ..Default::default()
        };

        let auth = AuthState::new(config);

        assert!(auth.validate_token("invalid-token").is_err());
    }

    #[test]
    fn test_api_key() {
        let auth = AuthState::new(AuthConfig::default());

        auth.register_api_key(
            "key1".to_string(),
            "hash123".to_string(),
            vec!["read".to_string()],
        );

        let key = auth.validate_api_key("hash123").unwrap();
        assert_eq!(key.id, "key1");
        assert_eq!(key.permissions, vec!["read"]);

        assert!(auth.validate_api_key("nonexistent").is_err());
    }

    #[test]
    fn test_has_permission() {
        let claims = Claims {
            sub: "user".to_string(),
            exp: 0,
            iat: 0,
            permissions: vec!["read".to_string(), "write".to_string()],
        };

        assert!(AuthState::has_permission(&claims, "read"));
        assert!(AuthState::has_permission(&claims, "write"));
        assert!(!AuthState::has_permission(&claims, "admin"));

        // Wildcard permission
        let admin_claims = Claims {
            sub: "admin".to_string(),
            exp: 0,
            iat: 0,
            permissions: vec!["*".to_string()],
        };

        assert!(AuthState::has_permission(&admin_claims, "anything"));
    }

    #[test]
    fn test_rate_limiter() {
        let limiter = RateLimiter::new(3, Duration::from_secs(60));

        // First 3 requests should be allowed
        assert!(limiter.check("client1"));
        assert!(limiter.check("client1"));
        assert!(limiter.check("client1"));

        // 4th should be denied
        assert!(!limiter.check("client1"));

        // Different client should be allowed
        assert!(limiter.check("client2"));

        assert_eq!(limiter.remaining("client1"), 0);
        assert_eq!(limiter.remaining("client2"), 2);
    }
}
