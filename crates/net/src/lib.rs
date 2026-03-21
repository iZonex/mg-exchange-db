pub mod auth;
pub mod auth_routes;
pub mod console;
pub mod http;
pub mod ilp;
pub mod metrics;
pub mod oauth;
pub mod pgwire;
pub mod pool;
pub mod replication_server;
pub mod service_account;
pub mod session;
pub mod tls;
pub mod ws;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use http::handlers::AppState;

use crate::tls::TlsConfig;

/// Global shutdown flag shared across all server components.
///
/// When set to `true`, servers should stop accepting new connections and
/// drain in-flight work before exiting.
static SHUTDOWN_FLAG: AtomicBool = AtomicBool::new(false);

/// Returns `true` if a graceful shutdown has been requested.
pub fn is_shutting_down() -> bool {
    SHUTDOWN_FLAG.load(Ordering::Relaxed)
}

/// Request a graceful shutdown of all servers.
pub fn request_shutdown() {
    SHUTDOWN_FLAG.store(true, Ordering::SeqCst);
}

/// Configuration for the ExchangeDB network server.
pub struct ServerConfig {
    /// Address to bind the HTTP server to.
    pub bind_addr: SocketAddr,
    /// Address to bind the PostgreSQL wire protocol server to.
    pub pg_bind_addr: SocketAddr,
    /// Address to bind the ILP TCP ingestion server to.
    pub ilp_bind_addr: SocketAddr,
    /// Root directory of the database.
    pub db_root: PathBuf,
    /// Optional TLS configuration for the HTTP server.
    pub tls: Option<TlsConfig>,
    /// Whether the HTTP server is enabled.
    pub http_enabled: bool,
    /// Whether the pgwire server is enabled.
    pub pg_enabled: bool,
    /// Whether the ILP TCP server is enabled.
    pub ilp_enabled: bool,
    /// Whether the server is in read-only mode (replica).
    pub read_only: bool,
    /// Optional replication manager for status reporting.
    pub replication_manager: Option<Arc<exchange_core::replication::ReplicationManager>>,
    /// Port for the replication TCP listener (0 = disabled).
    pub replication_port: u16,
}

impl Clone for ServerConfig {
    fn clone(&self) -> Self {
        Self {
            bind_addr: self.bind_addr,
            pg_bind_addr: self.pg_bind_addr,
            ilp_bind_addr: self.ilp_bind_addr,
            db_root: self.db_root.clone(),
            tls: self.tls.clone(),
            http_enabled: self.http_enabled,
            pg_enabled: self.pg_enabled,
            ilp_enabled: self.ilp_enabled,
            read_only: self.read_only,
            replication_manager: self.replication_manager.clone(),
            replication_port: self.replication_port,
        }
    }
}

impl std::fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerConfig")
            .field("bind_addr", &self.bind_addr)
            .field("db_root", &self.db_root)
            .field("read_only", &self.read_only)
            .finish_non_exhaustive()
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 9000)),
            pg_bind_addr: SocketAddr::from(([0, 0, 0, 0], pgwire::DEFAULT_PG_PORT)),
            ilp_bind_addr: SocketAddr::from(([0, 0, 0, 0], ilp::DEFAULT_ILP_PORT)),
            db_root: PathBuf::from("data"),
            tls: None,
            http_enabled: true,
            pg_enabled: true,
            ilp_enabled: true,
            read_only: false,
            replication_manager: None,
            replication_port: 0,
        }
    }
}

/// Start the HTTP server with the given configuration.
///
/// This function runs until the server is shut down. It binds to the
/// configured address and serves the REST API. When TLS is configured
/// and enabled, the server uses HTTPS via rustls; otherwise it serves
/// plain HTTP.
pub async fn start_http_server(config: &ServerConfig) -> std::io::Result<()> {
    let mut app_state = AppState::new(&config.db_root);
    app_state.read_only = config.read_only;
    app_state.replication_manager = config.replication_manager.clone();

    // Initialize plan cache and slow query log.
    app_state.plan_cache = Some(Arc::new(exchange_query::PlanCache::default_config()));
    app_state.slow_query_log = Some(Arc::new(exchange_query::SlowQueryLog::default_config()));

    // Initialize resource manager for query admission control.
    let resource_limits = exchange_core::resource::ResourceLimits::default();
    app_state.resource_mgr = Some(Arc::new(exchange_core::resource::ResourceManager::new(
        resource_limits,
    )));

    // Initialize usage meter for per-tenant metering.
    app_state.usage_meter = Some(Arc::new(exchange_core::metering::UsageMeter::new(
        config.db_root.clone(),
    )));

    // Initialize tenant manager for multi-tenant namespace isolation.
    app_state.tenant_manager = Some(Arc::new(exchange_core::tenant::TenantManager::new(
        config.db_root.clone(),
    )));

    // Initialize hot table registry — keeps all tables open in memory.
    exchange_query::table_registry::init_global(config.db_root.clone());

    // Pre-warm the query engine: parse a dummy query to force-load sqlparser
    // code paths and populate instruction caches. This eliminates the ~1ms
    // cold-start penalty on the first real query.
    {
        let _ = exchange_query::plan_query("SELECT 1");
        // Pre-warm table metadata and column readers for existing tables.
        if let Ok(entries) = std::fs::read_dir(&config.db_root) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    let meta_path = entry.path().join("_meta");
                    if meta_path.exists() {
                        let table_name = entry.file_name().to_string_lossy().to_string();
                        let sql = format!("SELECT * FROM {table_name} LIMIT 1");
                        if let Ok(plan) = exchange_query::plan_query(&sql) {
                            let _ = exchange_query::execute(&config.db_root, &plan);
                            // Cache the plan too.
                            if let Some(ref cache) = app_state.plan_cache {
                                cache.put(&sql, plan);
                            }
                        }
                    }
                }
            }
        }
        tracing::info!("query engine pre-warmed");
    }

    let state = Arc::new(app_state);
    let router = http::router(state);

    // Check if TLS is enabled.
    if let Some(ref tls_config) = config.tls
        && tls_config.enabled
    {
        let rustls_config = tls::load_tls_config(tls_config).await?;
        return tls::serve_tls(config.bind_addr, router, rustls_config).await;
    }

    // Plain HTTP fallback.
    tracing::info!(addr = %config.bind_addr, "starting HTTP server");

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    axum::serve(listener, router).await
}

/// Maximum time (in seconds) to wait for active queries to complete during
/// graceful shutdown before forcibly terminating.
const GRACEFUL_SHUTDOWN_TIMEOUT_SECS: u64 = 30;

/// Tracks the number of active connections across all server protocols.
///
/// Incremented when a connection is accepted, decremented when it closes.
/// Used during graceful shutdown to wait for in-flight work to drain.
static ACTIVE_CONNECTIONS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// Increment the active connection count.
pub fn track_connection_open() {
    ACTIVE_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
}

/// Decrement the active connection count.
pub fn track_connection_close() {
    ACTIVE_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
}

/// Return the current number of active connections.
pub fn active_connection_count() -> u32 {
    ACTIVE_CONNECTIONS.load(Ordering::Relaxed)
}

/// Start all enabled servers (HTTP, pgwire, ILP) concurrently.
///
/// Uses `tokio::select!` to run all servers and exits if any server
/// fails or a shutdown signal (Ctrl+C / SIGTERM) is received.
///
/// On shutdown the function:
/// 1. Sets the global shutdown flag (stops accepting new connections).
/// 2. Waits up to 30 seconds for in-flight tasks to drain.
/// 3. Aborts remaining server tasks.
/// 4. Logs "graceful shutdown complete".
pub async fn start_all_servers(config: ServerConfig) -> std::io::Result<()> {
    let mut handles: Vec<tokio::task::JoinHandle<std::io::Result<()>>> = Vec::new();

    if config.http_enabled {
        let cfg = config.clone();
        handles.push(tokio::spawn(async move { start_http_server(&cfg).await }));
        tracing::info!(addr = %config.bind_addr, "HTTP server enabled");
    } else {
        tracing::info!("HTTP server disabled");
    }

    if config.pg_enabled {
        let pg_addr = config.pg_bind_addr;
        let db_root = config.db_root.clone();
        let replication_manager = config.replication_manager.clone();
        handles.push(tokio::spawn(async move {
            pgwire::start_pg_server(pg_addr, db_root, replication_manager).await
        }));
        tracing::info!(addr = %config.pg_bind_addr, "pgwire server enabled");
    } else {
        tracing::info!("pgwire server disabled");
    }

    if config.ilp_enabled {
        let ilp_addr = config.ilp_bind_addr;
        let db_root = config.db_root.clone();
        handles.push(tokio::spawn(async move {
            ilp::start_ilp_server(ilp_addr, db_root).await
        }));
        tracing::info!(addr = %config.ilp_bind_addr, "ILP TCP server enabled");
    } else {
        tracing::info!("ILP TCP server disabled");
    }

    // Start the replication listener if a non-zero port is configured.
    if config.replication_port > 0 {
        let repl_addr = SocketAddr::from(([0, 0, 0, 0], config.replication_port));
        let db_root = config.db_root.clone();
        handles.push(tokio::spawn(async move {
            replication_server::start_replication_server(repl_addr, db_root).await
        }));
        tracing::info!(addr = %repl_addr, "replication TCP server enabled");
    }

    if handles.is_empty() {
        tracing::warn!("no servers enabled, nothing to do");
        return Ok(());
    }

    // Wait for any server to finish (which means it failed, since they all
    // loop forever) or for a shutdown signal (Ctrl+C or SIGTERM).
    let shutdown_error: Option<std::io::Error> = tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("received SIGINT (Ctrl+C), initiating shutdown");
            None
        }
        _ = sigterm_signal() => {
            tracing::info!("received SIGTERM, initiating shutdown");
            None
        }
        result = async {
            // Poll all handles; return as soon as any completes.
            let (result, _index, _remaining) = futures::future::select_all(
                handles.iter_mut().map(Box::pin)
            ).await;
            result
        } => {
            match result {
                Ok(Ok(())) => {
                    tracing::info!("a server exited cleanly");
                    None
                }
                Ok(Err(e)) => {
                    tracing::error!(error = %e, "a server exited with an error");
                    Some(e)
                }
                Err(e) => {
                    tracing::error!(error = %e, "a server task panicked");
                    Some(std::io::Error::other(e))
                }
            }
        }
    };

    // ── Graceful shutdown sequence ─────────────────────────────────────
    graceful_shutdown(&handles).await;

    match shutdown_error {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Execute the graceful shutdown sequence:
///
/// 1. Set the global shutdown flag so servers stop accepting new connections.
/// 2. Wait for active connections to drain (with 30s timeout).
/// 3. Flush all WAL writers to ensure no data loss.
/// 4. Abort any remaining server tasks that did not exit in time.
/// 5. Log completion.
async fn graceful_shutdown(handles: &[tokio::task::JoinHandle<std::io::Result<()>>]) {
    tracing::info!("initiating graceful shutdown sequence");

    // Step 1: signal all components to stop accepting new connections/queries.
    request_shutdown();

    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs(GRACEFUL_SHUTDOWN_TIMEOUT_SECS);

    // Step 2: wait for active connections to drain.
    let initial_connections = active_connection_count();
    if initial_connections > 0 {
        tracing::info!(
            active_connections = initial_connections,
            "waiting for in-flight connections to drain"
        );
    }

    let drain_result = tokio::time::timeout_at(deadline, async {
        // Wait for active connections to reach zero.
        loop {
            let active = active_connection_count();
            if active == 0 {
                tracing::info!("all connections drained");
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        // Also wait for server task handles to finish.
        for handle in handles {
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while !handle.is_finished() {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            })
            .await;
        }
    })
    .await;

    if drain_result.is_err() {
        let remaining = active_connection_count();
        tracing::warn!(
            timeout_secs = GRACEFUL_SHUTDOWN_TIMEOUT_SECS,
            remaining_connections = remaining,
            "graceful drain timed out, aborting remaining tasks"
        );
    }

    // Step 3: flush all WAL writers to ensure no data loss.
    tracing::info!("flushing WAL writers...");
    match tokio::task::spawn_blocking(move || {
        exchange_core::wal_writer::WalTableWriter::flush_all_global()
    })
    .await
    {
        Ok(Ok(())) => tracing::info!("WAL flush complete"),
        Ok(Err(e)) => tracing::error!(error = %e, "WAL flush failed"),
        Err(e) => tracing::error!(error = %e, "WAL flush task panicked"),
    }

    // Step 4: abort any server tasks that are still running.
    for handle in handles {
        if !handle.is_finished() {
            handle.abort();
        }
    }

    tracing::info!("graceful shutdown complete");
}

/// Wait for a SIGTERM signal (Unix) or return pending forever (non-Unix).
#[cfg(unix)]
async fn sigterm_signal() {
    let mut sig = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("failed to register SIGTERM handler");
    sig.recv().await;
}

#[cfg(not(unix))]
async fn sigterm_signal() {
    // On non-Unix platforms, SIGTERM is not available; wait forever
    // so that Ctrl+C is the only shutdown trigger.
    std::future::pending::<()>().await;
}
