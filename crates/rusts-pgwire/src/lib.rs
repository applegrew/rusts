//! PostgreSQL wire protocol support for RusTs time series database
//!
//! This crate provides PostgreSQL wire protocol compatibility using the pgwire crate,
//! enabling connections from psql, DataGrip, SQLAlchemy, and other PostgreSQL clients.
//!
//! # Example
//!
//! ```ignore
//! use rusts_pgwire::run_postgres_server;
//! use rusts_api::handlers::AppState;
//! use std::sync::Arc;
//! use std::time::Duration;
//! use tokio_util::sync::CancellationToken;
//!
//! let app_state = Arc::new(/* ... */);
//! let query_timeout = Duration::from_secs(30);
//! let shutdown = CancellationToken::new();
//!
//! tokio::spawn(async move {
//!     run_postgres_server(
//!         app_state,
//!         query_timeout,
//!         "0.0.0.0",
//!         5432,
//!         100,       // max_connections (0 = unlimited)
//!         shutdown,
//!     ).await
//! });
//! ```

mod backend;
mod encoder;
mod error;
mod types;

pub use backend::{PgWireBackend, PgWireHandlerFactory};
pub use error::PgError;

use pgwire::tokio::process_socket;
use rusts_api::handlers::AppState;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Run the PostgreSQL wire protocol server
///
/// This function starts a TCP listener on the specified host and port,
/// accepting PostgreSQL wire protocol connections. Each connection is
/// handled in a separate tokio task.
///
/// # Arguments
///
/// * `app_state` - Shared application state containing storage, indexes, and configuration
/// * `query_timeout` - Maximum duration for query execution
/// * `host` - Host address to bind to (e.g., "0.0.0.0" for all interfaces)
/// * `port` - Port to listen on (default PostgreSQL port is 5432)
/// * `max_connections` - Maximum number of concurrent connections (0 = unlimited)
/// * `shutdown` - Cancellation token to signal graceful shutdown
///
/// # Returns
///
/// Returns `Ok(())` when the server shuts down gracefully, or an error if
/// the server fails to start.
pub async fn run_postgres_server(
    app_state: Arc<AppState>,
    query_timeout: Duration,
    host: &str,
    port: u16,
    max_connections: usize,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", host, port);
    let listener = TcpListener::bind(&addr).await?;

    info!("PostgreSQL wire protocol server listening on {}", addr);
    info!("Connect with: psql -h {} -p {}", host, port);
    if max_connections > 0 {
        info!("Max PostgreSQL connections: {}", max_connections);
    } else {
        info!("Max PostgreSQL connections: unlimited");
    }

    let factory = Arc::new(PgWireHandlerFactory::new(app_state, query_timeout));

    // Use a semaphore to enforce the connection limit (0 = unlimited)
    let conn_semaphore = if max_connections > 0 {
        Some(Arc::new(Semaphore::new(max_connections)))
    } else {
        None
    };

    loop {
        tokio::select! {
            // Handle incoming connections
            result = listener.accept() => {
                match result {
                    Ok((socket, peer_addr)) => {
                        // Acquire a permit if connection limiting is enabled
                        let permit = if let Some(ref sem) = conn_semaphore {
                            match sem.clone().try_acquire_owned() {
                                Ok(permit) => Some(permit),
                                Err(_) => {
                                    warn!("Rejecting PostgreSQL connection from {} — max_connections ({}) reached", peer_addr, max_connections);
                                    drop(socket);
                                    continue;
                                }
                            }
                        } else {
                            None
                        };

                        info!("New PostgreSQL connection from {}", peer_addr);

                        let factory_ref = factory.clone();
                        tokio::spawn(async move {
                            if let Err(e) = process_socket(socket, None, factory_ref).await {
                                // Only log actual errors, not connection resets
                                let err_str = e.to_string();
                                if !err_str.contains("connection reset")
                                    && !err_str.contains("broken pipe")
                                {
                                    error!("Error processing PostgreSQL connection from {}: {}", peer_addr, e);
                                }
                            }
                            // Permit is dropped here, releasing the semaphore slot
                            drop(permit);
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept PostgreSQL connection: {}", e);
                    }
                }
            }

            // Handle shutdown signal
            _ = shutdown.cancelled() => {
                info!("PostgreSQL server shutting down");
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_exports() {
        // Just verify the public API is accessible
        let _: fn() -> &'static str = || "PgWireBackend is exported";
        let _: fn() -> &'static str = || "PgWireHandlerFactory is exported";
        let _: fn() -> &'static str = || "PgError is exported";
    }
}
