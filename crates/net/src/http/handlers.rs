use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{Path, Query as AxumQuery, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use futures::stream;
use serde::Deserialize;

use exchange_common::error::ExchangeDbError;
use exchange_common::types::{ColumnType, Timestamp};
use exchange_core::encryption::EncryptionConfig;
use exchange_core::engine::Engine;
use exchange_core::health::{HealthChecker, OverallStatus};
use exchange_core::metering::UsageMeter;
use exchange_core::rbac::RbacStore;
use exchange_core::resource::ResourceManager;
use exchange_core::table::{ColumnValue, TableBuilder, WriteMode};
use exchange_core::tenant::TenantManager;
use exchange_core::wal_writer::{WalTableWriter, WalTableWriterConfig};
use exchange_query::context::ExecutionContext;
use exchange_query::cursor_executor::execute_cursor;
use exchange_query::plan::{QueryPlan, QueryResult, Value};
use exchange_query::{execute_with_context, plan_query};

use crate::auth::{AuthConfig, AuthMethod, AuthResult, try_authenticate};
use crate::ilp;
use crate::ilp::parser::IlpValue;
use crate::metrics::Metrics;
use crate::session::SessionManager;
use crate::ws::SubscriptionManager;

use super::response::{
    ColumnInfo, ErrorResponse, HealthCheckInfo, HealthResponse, QueryResponse, TableInfoResponse,
    TablesListResponse, WriteResponse,
};

/// Shared application state available to all handlers.
pub struct AppState {
    pub db_root: std::path::PathBuf,
    /// Thread-safe database engine providing writer pooling and partition-level locking.
    pub engine: Arc<Engine>,
    pub start_time: Instant,
    pub metrics: Arc<Metrics>,
    pub subscriptions: SubscriptionManager,
    pub auth_config: AuthConfig,
    /// Unified authentication method (optional; when set, takes precedence over auth_config).
    pub auth_method: Option<AuthMethod>,
    /// Controls whether writes go through WAL for durability.
    pub write_mode: WriteMode,
    /// RBAC store for looking up user security contexts.
    pub rbac_store: Option<Arc<RbacStore>>,
    /// Resource manager for query admission control and limits.
    pub resource_mgr: Option<Arc<ResourceManager>>,
    /// Whether the server is in read-only mode (replica).
    pub read_only: bool,
    /// Optional replication manager for status reporting.
    pub replication_manager: Option<Arc<exchange_core::replication::ReplicationManager>>,
    /// Query plan cache for reusing optimized plans.
    pub plan_cache: Option<Arc<exchange_query::PlanCache>>,
    /// Slow query logger.
    pub slow_query_log: Option<Arc<exchange_query::SlowQueryLog>>,
    /// When true, SELECT queries are routed through the cursor-based execution engine.
    pub use_cursor_engine: bool,
    /// Session manager for tracking per-connection state and variables.
    pub session_manager: Arc<SessionManager>,
    /// Per-IP rate limiter for HTTP requests.
    pub rate_limiter: Arc<super::rate_limit::RateLimiter>,
    /// Usage meter for per-tenant metering.
    pub usage_meter: Option<Arc<UsageMeter>>,
    /// Tenant manager for multi-tenant namespace isolation.
    pub tenant_manager: Option<Arc<TenantManager>>,
    /// Encryption configuration for encryption at rest.
    pub encryption_config: Option<Arc<EncryptionConfig>>,
    /// Query registry for tracking active queries and supporting cancellation.
    pub query_registry: Arc<exchange_query::QueryRegistry>,
    /// Maximum allowed SQL query size in bytes (0 = unlimited). Default: 1MB.
    pub max_query_size: usize,
    /// Maximum allowed HTTP write body size in bytes (0 = unlimited). Default: 64MB.
    pub max_write_body_size: usize,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("db_root", &self.db_root)
            .field("read_only", &self.read_only)
            .field("use_cursor_engine", &self.use_cursor_engine)
            .finish_non_exhaustive()
    }
}

impl AppState {
    pub fn new(db_root: impl Into<std::path::PathBuf>) -> Self {
        let db_root = db_root.into();
        // Ensure the db_root directory exists on startup.
        if let Err(e) = std::fs::create_dir_all(&db_root) {
            tracing::error!(path = %db_root.display(), error = %e, "failed to create db_root directory");
        }
        let engine = Arc::new(Engine::open(&db_root).unwrap_or_else(|e| {
            tracing::error!(error = %e, "failed to open Engine; creating minimal instance");
            // Fallback: try once more after ensuring the directory exists.
            Engine::open(&db_root).expect("Engine::open failed on second attempt")
        }));
        let session_manager = Arc::new(SessionManager::new(10_000, Duration::from_secs(3600)));
        Self {
            db_root,
            engine,
            start_time: Instant::now(),
            metrics: Arc::new(Metrics::new()),
            subscriptions: SubscriptionManager::new(),
            auth_config: AuthConfig::default(),
            auth_method: None,
            write_mode: WriteMode::default(),
            rbac_store: None,
            resource_mgr: None,
            read_only: false,
            replication_manager: None,
            plan_cache: None,
            slow_query_log: None,
            use_cursor_engine: false,
            session_manager,
            rate_limiter: Arc::new(super::rate_limit::RateLimiter::default_config()),
            usage_meter: None,
            tenant_manager: None,
            encryption_config: None,
            query_registry: Arc::new(exchange_query::QueryRegistry::new()),
            max_query_size: 1_048_576,       // 1 MB
            max_write_body_size: 67_108_864, // 64 MB
        }
    }

    /// Create a new AppState with legacy token authentication configured.
    pub fn with_auth(mut self, auth_config: AuthConfig) -> Self {
        self.auth_config = auth_config;
        self
    }

    /// Create a new AppState with a unified authentication method.
    pub fn with_auth_method(mut self, method: AuthMethod) -> Self {
        self.auth_method = Some(method);
        self
    }

    /// Set the write mode (Direct or Wal).
    pub fn with_write_mode(mut self, mode: WriteMode) -> Self {
        self.write_mode = mode;
        self
    }

    /// Set the RBAC store for permission enforcement.
    pub fn with_rbac_store(mut self, store: Arc<RbacStore>) -> Self {
        self.rbac_store = Some(store);
        self
    }

    /// Set the resource manager for query admission control.
    pub fn with_resource_manager(mut self, mgr: Arc<ResourceManager>) -> Self {
        self.resource_mgr = Some(mgr);
        self
    }

    /// Enable or disable the cursor-based execution engine for SELECT queries.
    pub fn with_cursor_engine(mut self, enabled: bool) -> Self {
        self.use_cursor_engine = enabled;
        self
    }
}

/// Request body for the query endpoint.
#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub query: String,
    /// Optional prepared statement parameters ($1, $2, ...).
    #[serde(default)]
    pub params: Vec<serde_json::Value>,
}

/// Substitute `$1`, `$2`, ... placeholders in a SQL string with their values.
///
/// String parameters are escaped and quoted; numbers are inserted literally.
/// Returns the substituted SQL string.
fn substitute_params(sql: &str, params: &[serde_json::Value]) -> String {
    if params.is_empty() {
        return sql.to_string();
    }

    let mut result = sql.to_string();
    // Replace in reverse order ($10 before $1) to avoid partial matches.
    for i in (0..params.len()).rev() {
        let placeholder = format!("${}", i + 1);
        let replacement = match &params[i] {
            serde_json::Value::Null => "NULL".to_string(),
            serde_json::Value::Bool(b) => {
                if *b {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            }
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::String(s) => {
                // Escape single quotes by doubling them.
                let escaped = s.replace('\'', "''");
                format!("'{escaped}'")
            }
            other => {
                let escaped = other.to_string().replace('\'', "''");
                format!("'{escaped}'")
            }
        };
        result = result.replace(&placeholder, &replacement);
    }
    result
}

/// `GET /api/v1/health`
///
/// Returns service health status, version, and uptime.
/// Uses `HealthChecker` to run actual disk, WAL, memory, and data-dir checks
/// and returns Healthy/Degraded/Unhealthy instead of hardcoded "ok".
pub async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let uptime = state.start_time.elapsed().as_secs_f64();

    let replication = state.replication_manager.as_ref().map(|mgr| mgr.status());

    // Run actual health checks using the HealthChecker.
    let db_root = state.db_root.clone();
    let health_result = tokio::task::spawn_blocking(move || {
        let checker = HealthChecker::new(db_root, "node-0".to_string());
        checker.check()
    })
    .await;

    let (status_str, checks) = match health_result {
        Ok(health_status) => {
            let status_str = match health_status.status {
                OverallStatus::Healthy => "ok",
                OverallStatus::Degraded => "degraded",
                OverallStatus::Unhealthy => "unhealthy",
            };
            let check_infos: Vec<HealthCheckInfo> = health_status
                .checks
                .iter()
                .map(|c| HealthCheckInfo {
                    name: c.name.clone(),
                    status: format!("{:?}", c.status),
                    message: c.message.clone(),
                    duration_ms: c.duration_ms,
                })
                .collect();
            (status_str.to_string(), Some(check_infos))
        }
        Err(_) => ("ok".to_string(), None),
    };

    let status_code = if status_str == "unhealthy" {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };

    (
        status_code,
        Json(HealthResponse {
            status: status_str,
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_secs: uptime,
            replication,
            checks,
        }),
    )
}

/// Map an `ExchangeDbError` to an appropriate HTTP `ErrorResponse`.
fn db_error_to_response(err: ExchangeDbError) -> ErrorResponse {
    match &err {
        ExchangeDbError::TableNotFound(_) => {
            ErrorResponse::new(StatusCode::NOT_FOUND, err.to_string())
        }
        ExchangeDbError::ColumnNotFound(_, _) => {
            ErrorResponse::new(StatusCode::BAD_REQUEST, err.to_string())
        }
        ExchangeDbError::TableAlreadyExists(_) => {
            ErrorResponse::new(StatusCode::CONFLICT, err.to_string())
        }
        ExchangeDbError::Parse(_) | ExchangeDbError::Query(_) => {
            ErrorResponse::new(StatusCode::BAD_REQUEST, err.to_string())
        }
        ExchangeDbError::TypeMismatch { .. } => {
            ErrorResponse::new(StatusCode::BAD_REQUEST, err.to_string())
        }
        ExchangeDbError::PermissionDenied(_) => {
            ErrorResponse::new(StatusCode::FORBIDDEN, err.to_string())
        }
        ExchangeDbError::ResourceExhausted(_) => {
            ErrorResponse::new(StatusCode::TOO_MANY_REQUESTS, err.to_string())
        }
        ExchangeDbError::DiskFull { .. } => {
            // 507 Insufficient Storage — client should retry after space is freed.
            ErrorResponse::new(StatusCode::INSUFFICIENT_STORAGE, err.to_string())
        }
        _ => ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
    }
}

/// Convert a `Value` from the query engine into a `serde_json::Value`.
fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::I64(n) => serde_json::json!(n),
        Value::F64(n) => serde_json::json!(n),
        Value::Str(s) => serde_json::Value::String(s.clone()),
        Value::Timestamp(ns) => serde_json::json!(ns),
    }
}

/// Session header name used by clients to identify their session.
const SESSION_HEADER: &str = "x-exchangedb-session";

/// Resolve or create a session from the request headers.
///
/// If the `X-ExchangeDB-Session` header is present, the existing session is
/// looked up. Otherwise a new session is created and its ID is returned so
/// the caller can include it in the response.
fn resolve_session(
    state: &AppState,
    headers: &axum::http::HeaderMap,
) -> (String, Option<crate::session::Session>) {
    if let Some(hv) = headers.get(SESSION_HEADER)
        && let Ok(id) = hv.to_str()
        && let Some(session) = state.session_manager.get_session(id)
    {
        return (id.to_string(), Some(session));
    }
    // Create a new session.
    match state.session_manager.create_session() {
        Ok(id) => {
            let session = state.session_manager.get_session(&id);
            (id, session)
        }
        Err(_) => {
            // Session limit reached; proceed without a session.
            ("".to_string(), None)
        }
    }
}

/// Build an [`ExecutionContext`] from the current request state.
///
/// If an `AuthMethod` and `RbacStore` are configured, the authenticated
/// user's `SecurityContext` is resolved and attached. If no auth is
/// configured the context is anonymous (all operations allowed).
///
/// Session variables (e.g., `timezone`, `search_path`) are applied from
/// the resolved session, if available.
fn build_execution_context(
    state: &AppState,
    headers: &axum::http::HeaderMap,
    plan: Option<&QueryPlan>,
) -> std::result::Result<ExecutionContext, ErrorResponse> {
    let mut security = None;
    let mut current_user = None;

    // Resolve security context from authenticated identity.
    if let (Some(auth_method), Some(rbac_store)) = (&state.auth_method, &state.rbac_store) {
        match try_authenticate(auth_method, headers) {
            AuthResult::Authenticated { identity, .. } => {
                current_user = Some(identity.clone());
                // Look up the RBAC security context for this identity.
                if let Ok(Some(sec_ctx)) = rbac_store.resolve_security_context(&identity) {
                    security = Some(sec_ctx);
                }
            }
            AuthResult::InvalidCredentials(msg) => {
                return Err(ErrorResponse::new(
                    StatusCode::UNAUTHORIZED,
                    format!("authentication failed: {msg}"),
                ));
            }
            AuthResult::NoCredentials => {
                // No credentials — proceed as anonymous.
            }
        }
    }

    // Determine whether to use cursor engine for this query.
    // Enable for SELECT / Join / SetOperation plans when the server-wide flag is on.
    let use_cursors = state.use_cursor_engine
        && plan.is_some_and(|p| {
            matches!(
                p,
                QueryPlan::Select { .. }
                    | QueryPlan::Join { .. }
                    | QueryPlan::MultiJoin { .. }
                    | QueryPlan::AsofJoin { .. }
            )
        });

    // Resolve session to pick up session variables.
    let (_session_id, session) = resolve_session(state, headers);

    // Apply session variables to the execution context.
    let _ = session.as_ref().and_then(|s| s.settings.get("timezone"));
    // (timezone and search_path will be used when the executor supports them)

    // When replication is enabled, force WAL mode for SQL writes too.
    let effective_use_wal =
        state.write_mode == WriteMode::Wal || state.replication_manager.is_some();

    // Register the query for cancellation support.
    let (query_id, cancel_token) = state.query_registry.register();

    Ok(ExecutionContext {
        db_root: state.engine.db_root().to_path_buf(),
        security,
        resource_mgr: state.resource_mgr.clone(),
        query_id,
        start_time: Instant::now(),
        use_wal: effective_use_wal,
        memory_tracker: None,
        deadline: None,
        use_cursor_engine: use_cursors,
        mvcc: None,
        rls: None,
        current_user,
        sql_text: None,
        audit_log: None,
        replication_manager: state.replication_manager.clone(),
        cancellation_token: Some(cancel_token),
    })
}

/// `POST /api/v1/query`
///
/// Executes a SQL query and returns results as JSON.
/// Body: `{"query": "SELECT ..."}`
pub async fn query(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    Json(req): Json<QueryRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
    // Enforce query size limit.
    if state.max_query_size > 0 && req.query.len() > state.max_query_size {
        return Err(ErrorResponse::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "query too large: {} bytes (max {})",
                req.query.len(),
                state.max_query_size
            ),
        ));
    }

    // Substitute prepared statement parameters if provided.
    // Normalize whitespace so plan cache keys are stable regardless of
    // trailing newlines or extra spaces from the web console textarea.
    let query_sql = if req.params.is_empty() {
        req.query.trim().to_string()
    } else {
        substitute_params(&req.query, &req.params)
            .trim()
            .to_string()
    };

    if query_sql.is_empty() {
        return Err(ErrorResponse::new(
            StatusCode::BAD_REQUEST,
            "query must not be empty",
        ));
    }

    tracing::info!(query = %query_sql, "executing query");

    // Track active queries in metrics for resource monitoring.
    state.metrics.inc_active_queries();

    // Extract tenant ID from request header for multi-tenancy isolation.
    let tenant_id = extract_tenant_id(&headers);

    // Start timing AFTER request parsing/validation, BEFORE planning + execution.
    let start = Instant::now();

    // --- Plan cache: check before planning ---
    let (plan, cache_hit) = if let Some(ref cache) = state.plan_cache {
        if let Some(cached_plan) = cache.get(&query_sql) {
            state.metrics.inc_plan_cache_hits();
            (cached_plan, true)
        } else {
            state.metrics.inc_plan_cache_misses();
            let p = plan_query(&query_sql).map_err(|e| {
                state.metrics.dec_active_queries();
                state.metrics.inc_queries_failed();
                db_error_to_response(e)
            })?;
            // Store in cache for next time.
            if matches!(
                &p,
                QueryPlan::Select { .. }
                    | QueryPlan::Join { .. }
                    | QueryPlan::MultiJoin { .. }
                    | QueryPlan::AsofJoin { .. }
            ) {
                cache.put(&query_sql, p.clone());
            }
            (p, false)
        }
    } else {
        let p = plan_query(&query_sql).map_err(|e| {
            state.metrics.dec_active_queries();
            state.metrics.inc_queries_failed();
            db_error_to_response(e)
        })?;
        (p, false)
    };
    let t_plan = start.elapsed();

    // Handle SET key = value via session manager.
    if let QueryPlan::Set { name, value } = &plan {
        state.metrics.dec_active_queries();
        let (session_id, _) = resolve_session(&state, &headers);
        if !session_id.is_empty() {
            state.session_manager.set_variable(&session_id, name, value);
        }
        let response = QueryResponse {
            columns: vec![ColumnInfo {
                name: "status".to_string(),
                r#type: "Varchar".to_string(),
            }],
            rows: vec![vec![serde_json::json!("OK")]],
            timing_ms: start.elapsed().as_secs_f64() * 1000.0,
        };
        return Ok(Json(response));
    }

    // Build execution context with RBAC, resource limits, and cursor routing.
    let ctx = build_execution_context(&state, &headers, Some(&plan))?;
    let main_query_id = ctx.query_id;
    let t_ctx = start.elapsed();

    // Execute the plan directly on the current thread to avoid spawn_blocking
    // scheduling overhead (~1-2ms). Query execution is CPU-bound and fast enough
    // that the async runtime is not meaningfully blocked.
    let exec_start = Instant::now();
    let result = execute_with_context(&ctx, &plan).map_err(|e| {
        state.query_registry.deregister(main_query_id);
        state.metrics.dec_active_queries();
        state.metrics.inc_queries_failed();
        db_error_to_response(e)
    })?;
    let t_exec = exec_start.elapsed();
    let timing_ms = start.elapsed().as_secs_f64() * 1000.0;
    state.query_registry.deregister(main_query_id);

    // Log per-phase timing for profiling.
    tracing::info!(
        plan_us = t_plan.as_micros(),
        ctx_us = (t_ctx - t_plan).as_micros(),
        exec_us = t_exec.as_micros(),
        total_us = start.elapsed().as_micros(),
        cache_hit = cache_hit,
        "query phases"
    );

    let duration = start.elapsed();
    let duration_secs = duration.as_secs_f64();

    // Update metrics.
    state.metrics.inc_queries();
    state.metrics.observe_query_duration(duration_secs);
    state.metrics.dec_active_queries();

    // Record metering for the tenant.
    if let Some(ref meter) = state.usage_meter {
        let tid = tenant_id.as_deref().unwrap_or("default");
        let rows = match &result {
            QueryResult::Rows { rows, .. } => rows.len() as u64,
            QueryResult::Ok { .. } => 0,
        };
        meter.record_query(tid, rows, 0);
    }

    // Invalidate plan cache after DDL operations.
    if let Some(ref cache) = state.plan_cache {
        let upper = query_sql.trim().to_ascii_uppercase();
        if (upper.starts_with("CREATE")
            || upper.starts_with("DROP")
            || upper.starts_with("ALTER")
            || upper.starts_with("TRUNCATE"))
            && let Ok(ref p) = plan_query(&query_sql)
        {
            match p {
                exchange_query::QueryPlan::CreateTable { name, .. }
                | exchange_query::QueryPlan::DropTable { table: name, .. }
                | exchange_query::QueryPlan::AddColumn { table: name, .. }
                | exchange_query::QueryPlan::DropColumn { table: name, .. }
                | exchange_query::QueryPlan::RenameColumn { table: name, .. }
                | exchange_query::QueryPlan::SetColumnType { table: name, .. }
                | exchange_query::QueryPlan::TruncateTable { table: name, .. } => {
                    cache.invalidate_table(name);
                }
                _ => {}
            }
        }
    }

    // Log slow queries and increment slow metric.
    if let Some(ref slow_log) = state.slow_query_log {
        let row_count = match &result {
            QueryResult::Rows { rows, .. } => rows.len() as u64,
            QueryResult::Ok { affected_rows } => *affected_rows,
        };
        if duration >= slow_log.threshold() {
            state.metrics.inc_slow_queries();
        }
        slow_log.maybe_log(&req.query, duration, row_count);
    }

    format_query_result(result, timing_ms, &state)
}

/// Extract tenant ID from the `X-Tenant-ID` request header.
fn extract_tenant_id(headers: &axum::http::HeaderMap) -> Option<String> {
    headers
        .get("x-tenant-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Format a QueryResult into a JSON response, updating metrics.
fn format_query_result(
    result: QueryResult,
    timing_ms: f64,
    state: &AppState,
) -> Result<Json<QueryResponse>, ErrorResponse> {
    match result {
        QueryResult::Rows { columns, rows } => {
            state.metrics.add_rows_read(rows.len() as u64);

            let col_infos: Vec<ColumnInfo> = columns
                .iter()
                .enumerate()
                .map(|(i, name)| {
                    // Infer column type from the first non-null value in this column.
                    let type_name = rows
                        .iter()
                        .find_map(|row| {
                            row.get(i).and_then(|v| match v {
                                Value::I64(_) => Some("BIGINT"),
                                Value::F64(_) => Some("DOUBLE"),
                                Value::Str(_) => Some("VARCHAR"),
                                Value::Timestamp(_) => Some("TIMESTAMP"),
                                Value::Null => None,
                            })
                        })
                        .unwrap_or("VARCHAR");
                    ColumnInfo {
                        name: name.clone(),
                        r#type: type_name.to_string(),
                    }
                })
                .collect();

            let json_rows: Vec<Vec<serde_json::Value>> = rows
                .iter()
                .map(|row| row.iter().map(value_to_json).collect())
                .collect();

            Ok(Json(QueryResponse {
                columns: col_infos,
                rows: json_rows,
                timing_ms,
            }))
        }
        QueryResult::Ok { affected_rows } => {
            // Track rows written for INSERT/UPDATE operations.
            if affected_rows > 0 {
                state.metrics.add_rows_written(affected_rows);
            }
            let response = QueryResponse {
                columns: vec![ColumnInfo {
                    name: "affected_rows".to_string(),
                    r#type: "BIGINT".to_string(),
                }],
                rows: vec![vec![serde_json::json!(affected_rows)]],
                timing_ms,
            };
            Ok(Json(response))
        }
    }
}

/// `POST /api/v1/write`
///
/// Accepts ILP (InfluxDB Line Protocol) text and ingests the data.
/// Body: plain text, one ILP line per line.
pub async fn write(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: String,
) -> Result<impl IntoResponse, ErrorResponse> {
    // Reject writes on read-only replicas.
    if state.read_only {
        return Err(ErrorResponse::new(
            StatusCode::FORBIDDEN,
            "this is a read-only replica",
        ));
    }

    // Enforce write body size limit.
    if state.max_write_body_size > 0 && body.len() > state.max_write_body_size {
        return Err(ErrorResponse::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "write body too large: {} bytes (max {})",
                body.len(),
                state.max_write_body_size
            ),
        ));
    }

    if body.trim().is_empty() {
        return Err(ErrorResponse::new(
            StatusCode::BAD_REQUEST,
            "request body must not be empty",
        ));
    }

    let lines = ilp::parse_ilp_batch(&body).map_err(|e| {
        ErrorResponse::new(StatusCode::BAD_REQUEST, format!("ILP parse error: {e}"))
    })?;

    if lines.is_empty() {
        return Err(ErrorResponse::new(
            StatusCode::BAD_REQUEST,
            "no valid ILP lines in request body",
        ));
    }

    let count = lines.len();
    let engine = state.engine.clone();
    let db_root = state.db_root.clone();
    let write_mode = state.write_mode;
    let repl_mgr = state.replication_manager.clone();

    // When replication is enabled, force WAL mode so that WAL segments
    // are produced and can be shipped to replicas.
    let effective_write_mode = if repl_mgr.is_some() {
        WriteMode::Wal
    } else {
        write_mode
    };

    tokio::task::spawn_blocking(move || -> Result<(), ErrorResponse> {
        // Group lines by measurement (table name).
        let mut by_table: std::collections::BTreeMap<String, Vec<&ilp::IlpLine>> =
            std::collections::BTreeMap::new();
        for line in &lines {
            by_table
                .entry(line.measurement.clone())
                .or_default()
                .push(line);
        }

        for (table_name, table_lines) in &by_table {
            // Validate measurement name to prevent path traversal.
            exchange_common::validation::validate_measurement_name(table_name)
                .map_err(|e| ErrorResponse::new(StatusCode::BAD_REQUEST, e.to_string()))?;

            let table_dir = db_root.join(table_name);
            let meta_path = table_dir.join("_meta");

            // Auto-create the table if it doesn't exist.
            if !meta_path.exists() {
                let first = table_lines[0];
                auto_create_table(&db_root, table_name, first).map_err(|e| {
                    ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
                })?;
            }

            match effective_write_mode {
                WriteMode::Wal => {
                    write_ilp_lines_wal(&db_root, table_name, table_lines, repl_mgr.clone())?;
                }
                WriteMode::Direct => {
                    // Use per-partition locking for concurrent writes to
                    // different time partitions within the same table.
                    write_ilp_lines_partitioned(&db_root, &engine, table_name, table_lines)?;
                }
            }
        }

        Ok(())
    })
    .await
    .map_err(|e| {
        ErrorResponse::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("task join error: {e}"),
        )
    })??;

    tracing::info!(lines = count, "ingested ILP data");

    // Update metrics.
    state.metrics.add_ilp_lines(count as u64);
    state.metrics.add_rows_written(count as u64);

    // Record metering for the tenant.
    if let Some(ref meter) = state.usage_meter {
        let tid_opt = extract_tenant_id(&headers);
        let tid = tid_opt.as_deref().unwrap_or("default");
        meter.record_write(tid, count as u64);
    }

    // Notify WebSocket subscribers about the written data.
    // Build JSON rows grouped by table for broadcast.
    let lines_for_notify = ilp::parse_ilp_batch(&body).unwrap_or_default();
    let mut by_table_json: std::collections::BTreeMap<String, Vec<serde_json::Value>> =
        std::collections::BTreeMap::new();
    for line in &lines_for_notify {
        let mut obj = serde_json::Map::new();
        for (k, v) in &line.tags {
            obj.insert(k.clone(), serde_json::Value::String(v.clone()));
        }
        for (k, v) in &line.fields {
            let jv = match v {
                IlpValue::Integer(n) => serde_json::json!(n),
                IlpValue::Float(n) => serde_json::json!(n),
                IlpValue::String(s) => serde_json::Value::String(s.clone()),
                IlpValue::Boolean(b) => serde_json::json!(b),
                IlpValue::Timestamp(n) => serde_json::json!(n),
                IlpValue::Symbol(s) => serde_json::Value::String(s.clone()),
                IlpValue::Long256(s) => serde_json::Value::String(format!("0x{s}")),
            };
            obj.insert(k.clone(), jv);
        }
        if let Some(ts) = &line.timestamp {
            obj.insert("timestamp".to_string(), serde_json::json!(ts.as_nanos()));
        }
        by_table_json
            .entry(line.measurement.clone())
            .or_default()
            .push(serde_json::Value::Object(obj));
    }
    for (table, rows) in by_table_json {
        crate::ws::notify_write(&state.subscriptions, &table, rows).await;
    }

    Ok((
        StatusCode::OK,
        Json(WriteResponse {
            status: "ok".to_string(),
            lines_accepted: count,
        }),
    ))
}

/// Auto-create a table from the first ILP line, inferring the schema.
fn auto_create_table(
    db_root: &std::path::Path,
    table_name: &str,
    first_line: &ilp::IlpLine,
) -> std::result::Result<(), ExchangeDbError> {
    let mut builder = TableBuilder::new(table_name);

    // Always add a timestamp column first.
    builder = builder.column("timestamp", ColumnType::Timestamp);

    // Add tag columns as VARCHAR (not Symbol, since Symbol requires an
    // integer symbol-map ID and ILP sends raw strings).
    for tag_name in first_line.tags.keys() {
        builder = builder.column(tag_name, ColumnType::Varchar);
    }

    // Add field columns with types inferred from the first line's values.
    for (field_name, field_value) in &first_line.fields {
        let col_type = match field_value {
            IlpValue::Integer(_) => ColumnType::I64,
            IlpValue::Float(_) => ColumnType::F64,
            IlpValue::String(_) => ColumnType::Varchar,
            IlpValue::Boolean(_) => ColumnType::Boolean,
            IlpValue::Timestamp(_) => ColumnType::Timestamp,
            IlpValue::Symbol(_) => ColumnType::Symbol,
            IlpValue::Long256(_) => ColumnType::Long256,
        };
        builder = builder.column(field_name, col_type);
    }

    builder = builder.timestamp("timestamp");

    builder.build(db_root)?;
    Ok(())
}

/// Convert an ILP field value to a ColumnValue for writing.
fn ilp_value_to_column_value(v: &IlpValue) -> ColumnValue<'_> {
    match v {
        IlpValue::Integer(n) => ColumnValue::I64(*n),
        IlpValue::Float(n) => ColumnValue::F64(*n),
        IlpValue::String(s) => ColumnValue::Str(s.as_str()),
        IlpValue::Boolean(b) => ColumnValue::I64(if *b { 1 } else { 0 }),
        IlpValue::Timestamp(n) => ColumnValue::I64(*n),
        IlpValue::Symbol(s) => ColumnValue::Str(s.as_str()),
        IlpValue::Long256(s) => ColumnValue::Str(s.as_str()),
    }
}

/// Convert an ILP field value to an OwnedColumnValue for WAL writing.
fn ilp_value_to_owned(v: &IlpValue) -> exchange_core::wal::row_codec::OwnedColumnValue {
    use exchange_core::wal::row_codec::OwnedColumnValue as OV;
    match v {
        IlpValue::Integer(n) => OV::I64(*n),
        IlpValue::Float(n) => OV::F64(*n),
        IlpValue::String(s) => OV::Varchar(s.clone()),
        IlpValue::Boolean(b) => OV::I64(if *b { 1 } else { 0 }),
        IlpValue::Timestamp(n) => OV::Timestamp(*n),
        IlpValue::Symbol(s) => OV::Varchar(s.clone()),
        IlpValue::Long256(s) => OV::Varchar(s.clone()),
    }
}

/// Write ILP lines to a table using the WAL writer for durability.
///
/// When `repl_mgr` is `Some`, the writer is configured with the replication
/// manager so that committed WAL segments are automatically shipped to replicas.
fn write_ilp_lines_wal(
    db_root: &std::path::Path,
    table_name: &str,
    table_lines: &[&ilp::IlpLine],
    repl_mgr: Option<Arc<exchange_core::replication::ReplicationManager>>,
) -> Result<(), ErrorResponse> {
    use exchange_core::wal::row_codec::OwnedColumnValue as OV;

    let config = WalTableWriterConfig::default();
    let mut writer = WalTableWriter::open(db_root, table_name, config)
        .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Wire the replication manager into the WAL writer so that on commit,
    // the sealed WAL segment is shipped to all configured replicas.
    if let Some(mgr) = repl_mgr {
        writer.set_replication_manager(mgr);
    }

    let meta = writer.meta().clone();

    for line in table_lines {
        let ts = line.timestamp.unwrap_or_else(Timestamp::now);

        // Build owned column values in schema order (all columns including timestamp).
        let owned_values: Vec<OV> = meta
            .columns
            .iter()
            .enumerate()
            .map(|(i, col_def)| {
                if i == meta.timestamp_column {
                    return OV::Timestamp(ts.as_nanos());
                }
                if let Some(tag_val) = line.tags.get(&col_def.name) {
                    return OV::Varchar(tag_val.clone());
                }
                if let Some(field_val) = line.fields.get(&col_def.name) {
                    return ilp_value_to_owned(field_val);
                }
                OV::Null
            })
            .collect();

        writer
            .write_row(ts, owned_values)
            .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    writer
        .commit()
        .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(())
}

/// Write ILP lines to a table using per-partition locking.
///
/// Groups incoming lines by their target partition, then acquires a partition-level
/// lock for each group. This allows concurrent writers targeting different time
/// partitions to proceed in parallel instead of serializing on a per-table mutex.
fn write_ilp_lines_partitioned(
    db_root: &std::path::Path,
    engine: &Engine,
    table_name: &str,
    table_lines: &[&ilp::IlpLine],
) -> Result<(), ErrorResponse> {
    use exchange_common::types::PartitionBy;
    use exchange_core::partition::partition_dir;
    use std::collections::HashMap;

    let table_dir = db_root.join(table_name);
    let meta = exchange_core::table::TableMeta::load(&table_dir.join("_meta"))
        .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let partition_by: PartitionBy = meta.partition_by.into();

    // Group lines by target partition.
    let mut by_partition: HashMap<String, Vec<&ilp::IlpLine>> = HashMap::new();
    for line in table_lines {
        let ts = line.timestamp.unwrap_or_else(Timestamp::now);
        let part_name = partition_dir(ts, partition_by);
        by_partition.entry(part_name).or_default().push(line);
    }

    // Get or create partition lock manager via Engine.
    let _init = engine.get_reader(table_name).ok(); // ensure init_table was called

    for (part_name, part_lines) in &by_partition {
        // Acquire per-partition lock — other partitions remain unlocked.
        let mut handle = engine
            .get_writer_for_partition(table_name, part_name)
            .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        for line in part_lines {
            let ts = line.timestamp.unwrap_or_else(Timestamp::now);
            let col_values: Vec<ColumnValue<'_>> = meta
                .columns
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != meta.timestamp_column)
                .map(|(_, col_def)| {
                    if let Some(tag_val) = line.tags.get(&col_def.name) {
                        return ColumnValue::Str(tag_val.as_str());
                    }
                    if let Some(field_val) = line.fields.get(&col_def.name) {
                        return ilp_value_to_column_value(field_val);
                    }
                    ColumnValue::Null
                })
                .collect();

            handle.writer().write_row(ts, &col_values).map_err(|e| {
                ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
            })?;
        }

        handle
            .writer()
            .flush()
            .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }

    Ok(())
}

/// `POST /api/v1/query/cancel/{id}`
///
/// Cancel a running query by its unique ID.
///
/// Returns 200 if the query was found and cancellation was requested,
/// or 404 if the query ID was not found (already completed or never existed).
pub async fn cancel_query(
    State(state): State<Arc<AppState>>,
    Path(query_id): Path<u64>,
) -> impl IntoResponse {
    if state.query_registry.cancel(query_id) {
        tracing::info!(query_id = query_id, "query cancellation requested");
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "cancelled",
                "query_id": query_id,
            })),
        )
    } else {
        tracing::debug!(query_id = query_id, "query cancel: ID not found");
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "status": "not_found",
                "query_id": query_id,
                "message": "query not found (already completed or invalid ID)",
            })),
        )
    }
}

/// `GET /api/v1/queries/active`
///
/// List currently active query IDs.
pub async fn active_queries(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let ids = state.query_registry.active_query_ids();
    Json(serde_json::json!({
        "active_queries": ids,
        "count": ids.len(),
    }))
}

/// `GET /api/v1/tables`
///
/// Lists all tables in the database.
pub async fn list_tables(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let engine = state.engine.clone();

    let tables = tokio::task::spawn_blocking(move || -> Vec<String> {
        engine.list_tables().unwrap_or_default()
    })
    .await
    .unwrap_or_default();

    // Update tables count gauge.
    state.metrics.set_tables_count(tables.len() as u64);

    Json(TablesListResponse { tables })
}

/// `GET /api/v1/tables/:name`
///
/// Returns metadata for a specific table.
pub async fn table_info(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let engine = state.engine.clone();
    let table_name = name.clone();

    let info = tokio::task::spawn_blocking(
        move || -> std::result::Result<TableInfoResponse, ExchangeDbError> {
            let meta = engine.get_meta(&table_name)?;
            let table_dir = engine.db_root().join(&table_name);

            let columns: Vec<ColumnInfo> = meta
                .columns
                .iter()
                .map(|col| {
                    let type_name = format!("{:?}", col.col_type);
                    ColumnInfo {
                        name: col.name.clone(),
                        r#type: type_name,
                    }
                })
                .collect();

            let partition_by = format!("{:?}", meta.partition_by);

            // Count rows by scanning partition directories.
            let mut row_count: u64 = 0;
            if let Ok(entries) = std::fs::read_dir(&table_dir) {
                for entry in entries.flatten() {
                    if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                        // Count rows from the timestamp column file size.
                        let ts_col_name = &meta.columns[meta.timestamp_column].name;
                        let ts_file = entry.path().join(format!("{ts_col_name}.d"));
                        if let Ok(file_meta) = std::fs::metadata(&ts_file) {
                            // Timestamp is 8 bytes per row.
                            row_count += file_meta.len() / 8;
                        }
                    }
                }
            }

            Ok(TableInfoResponse {
                name: meta.name,
                columns,
                partition_by,
                row_count,
            })
        },
    )
    .await
    .map_err(|e| {
        ErrorResponse::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("task join error: {e}"),
        )
    })?
    .map_err(db_error_to_response)?;

    Ok(Json(info))
}

/// Query parameters for the streaming query endpoint.
#[derive(Debug, Deserialize)]
pub struct StreamQueryParams {
    pub query: String,
}

/// `GET /api/v1/query/stream?query=SELECT...`
///
/// Streams results as newline-delimited JSON (NDJSON) using cursor-based
/// execution for SELECT queries (backpressure, memory efficiency).
/// Falls back to eager execution for non-SELECT or unsupported plans.
/// - First line: `{"columns": [...]}\n`
/// - Data lines: `{"row": [...]}\n`
/// - Last line:  `{"complete": true, "row_count": N}\n`
pub async fn query_stream(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    AxumQuery(params): AxumQuery<StreamQueryParams>,
) -> Result<impl IntoResponse, ErrorResponse> {
    if params.query.trim().is_empty() {
        return Err(ErrorResponse::new(
            StatusCode::BAD_REQUEST,
            "query must not be empty",
        ));
    }

    tracing::info!(query = %params.query, "executing streaming query");

    let plan = plan_query(&params.query).map_err(db_error_to_response)?;
    let db_root = state.engine.db_root().to_path_buf();

    // For SELECT-family queries, try cursor-based streaming for true
    // backpressure and low memory usage on large result sets.
    let is_select = matches!(
        &plan,
        QueryPlan::Select { .. }
            | QueryPlan::Join { .. }
            | QueryPlan::MultiJoin { .. }
            | QueryPlan::AsofJoin { .. }
    );
    if is_select {
        use futures::StreamExt;
        let plan_c = plan.clone();
        let dbr = db_root.clone();
        let cr = tokio::task::spawn_blocking(move || execute_cursor(&dbr, &plan_c))
            .await
            .map_err(|e| {
                ErrorResponse::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("task join error: {e}"),
                )
            })?;
        if let Ok(cursor) = cr {
            state.metrics.inc_queries();
            let schema: Vec<String> = cursor.schema().iter().map(|(n, _)| n.clone()).collect();
            let mut hl = serde_json::to_string(&serde_json::json!({"columns": schema})).unwrap();
            hl.push('\n');
            let hb: bytes::Bytes = bytes::Bytes::from(hl);
            let bs: usize = 1000;
            let cs = stream::unfold((Some(cursor), 0u64), move |(co, rc)| async move {
                let cu = co?;
                let r = tokio::task::spawn_blocking(move || {
                    let mut c = cu;
                    let b = c.next_batch(bs);
                    (c, b)
                })
                .await;
                match r {
                    Ok((c, Ok(Some(batch)))) => {
                        let nr = batch.row_count() as u64;
                        let mut buf = Vec::new();
                        for ri in 0..batch.row_count() {
                            let jr: Vec<serde_json::Value> = (0..batch.columns.len())
                                .map(|ci| value_to_json(&batch.get_value(ri, ci)))
                                .collect();
                            let mut l =
                                serde_json::to_string(&serde_json::json!({"row": jr})).unwrap();
                            l.push('\n');
                            buf.extend_from_slice(l.as_bytes());
                        }
                        Some((
                            Ok::<bytes::Bytes, std::io::Error>(bytes::Bytes::from(buf)),
                            (Some(c), rc + nr),
                        ))
                    }
                    Ok((_, Ok(None))) => {
                        let mut f = serde_json::to_string(
                            &serde_json::json!({"complete": true, "row_count": rc}),
                        )
                        .unwrap();
                        f.push('\n');
                        Some((Ok(bytes::Bytes::from(f)), (None, rc)))
                    }
                    Ok((_, Err(e))) => Some((
                        Ok(bytes::Bytes::from(format!("{{\"error\":\"{}\"}}\n", e))),
                        (None, rc),
                    )),
                    Err(e) => Some((
                        Ok(bytes::Bytes::from(format!("{{\"error\":\"{}\"}}\n", e))),
                        (None, rc),
                    )),
                }
            });
            let hs = stream::once(async move { Ok::<bytes::Bytes, std::io::Error>(hb) });
            let body = axum::body::Body::from_stream(hs.chain(cs));
            return Ok(axum::response::Response::builder()
                .header("content-type", "application/x-ndjson")
                .body(body)
                .unwrap());
        }
        // If cursor build failed, fall through to eager execution.
    }

    // Fallback: eager execution for non-SELECT plans or when cursor build fails.
    let ctx = build_execution_context(&state, &headers, Some(&plan))?;
    let result = tokio::task::spawn_blocking(move || execute_with_context(&ctx, &plan))
        .await
        .map_err(|e| {
            ErrorResponse::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("task join error: {e}"),
            )
        })?
        .map_err(db_error_to_response)?;

    state.metrics.inc_queries();

    match result {
        QueryResult::Rows { columns, rows } => {
            state.metrics.add_rows_read(rows.len() as u64);

            let row_count = rows.len();

            // Build all NDJSON lines eagerly, then stream them.
            let mut lines: Vec<Result<bytes::Bytes, std::io::Error>> =
                Vec::with_capacity(row_count + 2);

            // First line: column metadata.
            let mut header_line =
                serde_json::to_string(&serde_json::json!({ "columns": columns })).unwrap();
            header_line.push('\n');
            lines.push(Ok(bytes::Bytes::from(header_line)));

            // Data lines.
            for row in &rows {
                let json_row: Vec<serde_json::Value> = row.iter().map(value_to_json).collect();
                let mut line =
                    serde_json::to_string(&serde_json::json!({ "row": json_row })).unwrap();
                line.push('\n');
                lines.push(Ok(bytes::Bytes::from(line)));
            }

            // Final line: completion marker.
            let mut footer_line = serde_json::to_string(
                &serde_json::json!({ "complete": true, "row_count": row_count }),
            )
            .unwrap();
            footer_line.push('\n');
            lines.push(Ok(bytes::Bytes::from(footer_line)));

            let stream = stream::iter(lines);
            let body = axum::body::Body::from_stream(stream);
            Ok(axum::response::Response::builder()
                .header("content-type", "application/x-ndjson")
                .body(body)
                .unwrap())
        }
        QueryResult::Ok { affected_rows } => {
            let mut lines: Vec<Result<bytes::Bytes, std::io::Error>> = Vec::with_capacity(3);

            let mut header_line =
                serde_json::to_string(&serde_json::json!({ "columns": ["affected_rows"] }))
                    .unwrap();
            header_line.push('\n');
            lines.push(Ok(bytes::Bytes::from(header_line)));

            let mut row_line =
                serde_json::to_string(&serde_json::json!({ "row": [affected_rows] })).unwrap();
            row_line.push('\n');
            lines.push(Ok(bytes::Bytes::from(row_line)));

            let mut footer_line =
                serde_json::to_string(&serde_json::json!({ "complete": true, "row_count": 1 }))
                    .unwrap();
            footer_line.push('\n');
            lines.push(Ok(bytes::Bytes::from(footer_line)));

            let stream = stream::iter(lines);
            let body = axum::body::Body::from_stream(stream);
            Ok(axum::response::Response::builder()
                .header("content-type", "application/x-ndjson")
                .body(body)
                .unwrap())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unit test for the NDJSON streaming output logic. We call the
    /// internal `value_to_json` helper and manually verify the format
    /// produced by `build_ndjson_lines`, without needing full HTTP
    /// stack dependencies.
    #[test]
    fn ndjson_format_is_valid() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let rows: Vec<Vec<Value>> = vec![
            vec![Value::I64(1), Value::Str("Alice".into())],
            vec![Value::I64(2), Value::Str("Bob".into())],
        ];

        let row_count = rows.len();
        let mut lines: Vec<String> = Vec::new();

        // Header.
        let header = serde_json::json!({ "columns": columns });
        lines.push(serde_json::to_string(&header).unwrap());

        // Data rows.
        for row in &rows {
            let json_row: Vec<serde_json::Value> = row.iter().map(value_to_json).collect();
            let row_obj = serde_json::json!({ "row": json_row });
            lines.push(serde_json::to_string(&row_obj).unwrap());
        }

        // Footer.
        let footer = serde_json::json!({ "complete": true, "row_count": row_count });
        lines.push(serde_json::to_string(&footer).unwrap());

        // Validate.
        assert_eq!(lines.len(), 4); // header + 2 rows + footer

        let header_parsed: serde_json::Value = serde_json::from_str(&lines[0]).unwrap();
        assert!(header_parsed.get("columns").is_some());
        assert_eq!(header_parsed["columns"][0], "id");
        assert_eq!(header_parsed["columns"][1], "name");

        let row1: serde_json::Value = serde_json::from_str(&lines[1]).unwrap();
        assert!(row1.get("row").is_some());
        assert_eq!(row1["row"][0], 1);
        assert_eq!(row1["row"][1], "Alice");

        let row2: serde_json::Value = serde_json::from_str(&lines[2]).unwrap();
        assert_eq!(row2["row"][0], 2);
        assert_eq!(row2["row"][1], "Bob");

        let footer_parsed: serde_json::Value = serde_json::from_str(&lines[3]).unwrap();
        assert_eq!(footer_parsed["complete"], true);
        assert_eq!(footer_parsed["row_count"], 2);
    }

    // ── Engine wiring tests ────────────────────────────────────────

    #[test]
    fn appstate_creates_engine() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path());
        // Engine is initialized and points to the correct db_root.
        assert_eq!(state.engine.db_root(), dir.path());
        // list_tables should succeed (empty database).
        let tables = state.engine.list_tables().unwrap();
        assert!(tables.is_empty());
    }

    #[test]
    fn appstate_engine_lists_tables() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path());
        // Create a table through the engine.
        use exchange_common::types::ColumnType;
        state
            .engine
            .create_table(
                exchange_core::table::TableBuilder::new("test_tbl")
                    .column("timestamp", ColumnType::Timestamp)
                    .column("value", ColumnType::F64)
                    .timestamp("timestamp"),
            )
            .unwrap();
        let tables = state.engine.list_tables().unwrap();
        assert_eq!(tables, vec!["test_tbl"]);
    }

    // ── Cursor engine flag tests ───────────────────────────────────

    #[test]
    fn cursor_engine_flag_defaults_to_false() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path());
        assert!(!state.use_cursor_engine);
    }

    #[test]
    fn cursor_engine_flag_can_be_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path()).with_cursor_engine(true);
        assert!(state.use_cursor_engine);
    }

    #[test]
    fn build_ctx_enables_cursors_for_select_when_flag_on() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path());
        state.use_cursor_engine = true;

        let headers = axum::http::HeaderMap::new();
        let plan = QueryPlan::Select {
            table: "trades".to_string(),
            columns: vec![],
            filter: None,
            order_by: vec![],
            limit: None,
            offset: None,
            sample_by: None,
            latest_on: None,
            group_by: vec![],
            group_by_mode: exchange_query::plan::GroupByMode::Normal,
            having: None,
            distinct: false,
            distinct_on: vec![],
        };
        let ctx = build_execution_context(&state, &headers, Some(&plan)).unwrap();
        assert!(ctx.use_cursor_engine);
    }

    #[test]
    fn build_ctx_disables_cursors_for_insert() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path());
        state.use_cursor_engine = true;

        let headers = axum::http::HeaderMap::new();
        let plan = QueryPlan::Insert {
            table: "trades".to_string(),
            columns: vec![],
            values: vec![],
            upsert: false,
        };
        let ctx = build_execution_context(&state, &headers, Some(&plan)).unwrap();
        assert!(!ctx.use_cursor_engine);
    }

    #[test]
    fn build_ctx_disables_cursors_when_flag_off() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path());
        // use_cursor_engine defaults to false.

        let headers = axum::http::HeaderMap::new();
        let plan = QueryPlan::Select {
            table: "trades".to_string(),
            columns: vec![],
            filter: None,
            order_by: vec![],
            limit: None,
            offset: None,
            sample_by: None,
            latest_on: None,
            group_by: vec![],
            group_by_mode: exchange_query::plan::GroupByMode::Normal,
            having: None,
            distinct: false,
            distinct_on: vec![],
        };
        let ctx = build_execution_context(&state, &headers, Some(&plan)).unwrap();
        assert!(!ctx.use_cursor_engine);
    }

    // ── Session wiring tests ───────────────────────────────────────

    #[test]
    fn appstate_has_session_manager() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path());
        // Session manager is initialized.
        assert_eq!(state.session_manager.session_count(), 0);
    }

    #[test]
    fn resolve_session_creates_on_first_call() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path());
        let headers = axum::http::HeaderMap::new();

        let (session_id, session) = resolve_session(&state, &headers);
        assert!(!session_id.is_empty());
        assert!(session.is_some());
        assert_eq!(state.session_manager.session_count(), 1);
    }

    #[test]
    fn resolve_session_reuses_existing() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path());

        // Create initial session.
        let id = state.session_manager.create_session().unwrap();

        // Build headers with existing session ID.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(SESSION_HEADER, id.parse().unwrap());

        let (resolved_id, session) = resolve_session(&state, &headers);
        assert_eq!(resolved_id, id);
        assert!(session.is_some());
        // Should not have created a second session.
        assert_eq!(state.session_manager.session_count(), 1);
    }

    // ── Rate limiter wiring tests ──────────────────────────────────

    #[test]
    fn appstate_has_rate_limiter() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path());
        // Default rate limiter allows at least 1 request.
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
        assert!(state.rate_limiter.check(ip));
    }

    // ── Engine db_root used in execution context ───────────────────

    #[test]
    fn execution_context_uses_engine_db_root() {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path());
        let headers = axum::http::HeaderMap::new();
        let ctx = build_execution_context(&state, &headers, None).unwrap();
        assert_eq!(ctx.db_root, state.engine.db_root());
    }

    // ── Feature: Prepared statement parameter substitution ───────

    #[test]
    fn substitute_params_basic() {
        let sql = "SELECT * FROM trades WHERE symbol = $1 AND price > $2";
        let params = vec![serde_json::json!("BTC/USD"), serde_json::json!(50000)];
        let result = substitute_params(sql, &params);
        assert_eq!(
            result,
            "SELECT * FROM trades WHERE symbol = 'BTC/USD' AND price > 50000"
        );
    }

    #[test]
    fn substitute_params_empty() {
        let sql = "SELECT * FROM trades";
        let result = substitute_params(sql, &[]);
        assert_eq!(result, sql);
    }

    #[test]
    fn substitute_params_null_and_bool() {
        let sql = "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $3";
        let params = vec![
            serde_json::json!(null),
            serde_json::json!(true),
            serde_json::json!(false),
        ];
        let result = substitute_params(sql, &params);
        assert_eq!(
            result,
            "SELECT * FROM t WHERE a = NULL AND b = TRUE AND c = FALSE"
        );
    }

    #[test]
    fn substitute_params_escapes_quotes() {
        let sql = "SELECT * FROM t WHERE name = $1";
        let params = vec![serde_json::json!("O'Brien")];
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT * FROM t WHERE name = 'O''Brien'");
    }

    #[test]
    fn substitute_params_multiple_digits() {
        let sql = "SELECT $1, $2, $10";
        let params: Vec<serde_json::Value> = (1..=10).map(|i| serde_json::json!(i)).collect();
        let result = substitute_params(sql, &params);
        // $10 should be replaced with 10, $1 with 1, $2 with 2
        assert_eq!(result, "SELECT 1, 2, 10");
    }

    #[test]
    fn substitute_params_float() {
        let sql = "SELECT * FROM t WHERE price > $1";
        let params = vec![serde_json::json!(99.95)];
        let result = substitute_params(sql, &params);
        assert_eq!(result, "SELECT * FROM t WHERE price > 99.95");
    }
}
