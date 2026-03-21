//! Admin-only REST API endpoints for ExchangeDB.
//!
//! All endpoints require the `Admin` permission and are mounted under `/admin/`.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use super::handlers::AppState;
use super::response::ErrorResponse;

// ---------------------------------------------------------------------------
// Request / response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct ConfigResponse {
    pub data_dir: String,
    pub read_only: bool,
    pub uptime_secs: f64,
}

#[derive(Debug, Deserialize)]
pub struct ConfigUpdateRequest {
    #[serde(default)]
    pub log_level: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct UserSummary {
    pub username: String,
    pub roles: Vec<String>,
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub roles: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct RoleSummary {
    pub name: String,
    pub permissions: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CreateRoleRequest {
    pub name: String,
    #[serde(default)]
    pub permissions: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ClusterStatusResponse {
    pub node_id: String,
    pub role: String,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct ReplicationStatusResponse {
    pub role: String,
    pub lag_bytes: u64,
    pub lag_seconds: u64,
    pub segments_shipped: u64,
    pub segments_applied: u64,
}

#[derive(Debug, Serialize)]
pub struct WalStatusResponse {
    pub wal_enabled: bool,
    pub total_segments: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct PartitionInfo {
    pub name: String,
    pub row_count: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Serialize)]
pub struct PartitionResponse {
    pub table: String,
    pub partitions: Vec<PartitionInfo>,
}

#[derive(Debug, Serialize)]
pub struct VacuumResponse {
    pub table: String,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct CheckpointResponse {
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct SlowQueryEntry {
    pub timestamp: String,
    pub sql: String,
    pub duration_secs: f64,
    pub rows: u64,
}

#[derive(Debug, Serialize)]
pub struct SlowQueriesResponse {
    pub queries: Vec<SlowQueryEntry>,
}

#[derive(Debug, Serialize)]
pub struct JobInfo {
    pub name: String,
    pub status: String,
    pub last_run: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct JobsResponse {
    pub jobs: Vec<JobInfo>,
}

#[derive(Debug, Serialize)]
pub struct AdminStatusResponse {
    pub status: String,
    pub message: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// `GET /admin/config` -- Current server configuration.
pub async fn get_config(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let uptime = state.start_time.elapsed().as_secs_f64();
    Json(ConfigResponse {
        data_dir: state.db_root.display().to_string(),
        read_only: state.read_only,
        uptime_secs: uptime,
    })
}

/// `POST /admin/config` -- Update config (hot reload).
pub async fn update_config(
    State(_state): State<Arc<AppState>>,
    Json(req): Json<ConfigUpdateRequest>,
) -> impl IntoResponse {
    // In a production system, this would update tracing filters, etc.
    let message = if let Some(ref level) = req.log_level {
        format!("log_level update requested: {level}")
    } else {
        "no changes requested".to_string()
    };
    Json(AdminStatusResponse {
        status: "ok".to_string(),
        message,
    })
}

/// `GET /admin/users` -- List users.
pub async fn list_users(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let db_root = state.db_root.clone();
    let users = tokio::task::spawn_blocking(move || {
        let store = exchange_core::rbac::RbacStore::open(&db_root)
            .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let users = store
            .list_users()
            .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        Ok::<_, ErrorResponse>(users)
    })
    .await
    .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;

    let summaries: Vec<UserSummary> = users
        .into_iter()
        .map(|u| UserSummary {
            username: u.username,
            roles: u.roles,
            enabled: u.enabled,
        })
        .collect();

    Ok(Json(summaries))
}

/// `POST /admin/users` -- Create user.
pub async fn create_user(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateUserRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let db_root = state.db_root.clone();
    let username_for_response = req.username.clone();
    tokio::task::spawn_blocking(move || {
        let store = exchange_core::rbac::RbacStore::open(&db_root)
            .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let user = exchange_core::rbac::User {
            username: req.username.clone(),
            password_hash: exchange_core::rbac::hash_password(&req.password),
            roles: req.roles,
            enabled: true,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        };

        store
            .create_user(&user)
            .map_err(|e| ErrorResponse::new(StatusCode::CONFLICT, e.to_string()))?;

        Ok::<_, ErrorResponse>(())
    })
    .await
    .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;

    Ok((
        StatusCode::CREATED,
        Json(AdminStatusResponse {
            status: "ok".to_string(),
            message: format!("user '{}' created", username_for_response),
        }),
    ))
}

/// `DELETE /admin/users/:name` -- Delete user.
pub async fn delete_user(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let db_root = state.db_root.clone();
    let username = name.clone();
    tokio::task::spawn_blocking(move || {
        let store = exchange_core::rbac::RbacStore::open(&db_root)
            .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        store
            .delete_user(&username)
            .map_err(|e| ErrorResponse::new(StatusCode::NOT_FOUND, e.to_string()))?;
        Ok::<_, ErrorResponse>(())
    })
    .await
    .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;

    Ok(Json(AdminStatusResponse {
        status: "ok".to_string(),
        message: format!("user '{name}' deleted"),
    }))
}

/// `GET /admin/roles` -- List roles.
pub async fn list_roles(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let db_root = state.db_root.clone();
    let roles = tokio::task::spawn_blocking(move || {
        let store = exchange_core::rbac::RbacStore::open(&db_root)
            .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        let roles = store
            .list_roles()
            .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        Ok::<_, ErrorResponse>(roles)
    })
    .await
    .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;

    let summaries: Vec<RoleSummary> = roles
        .into_iter()
        .map(|r| RoleSummary {
            name: r.name,
            permissions: r.permissions.iter().map(|p| format!("{p:?}")).collect(),
        })
        .collect();

    Ok(Json(summaries))
}

/// `POST /admin/roles` -- Create role.
pub async fn create_role(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateRoleRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let db_root = state.db_root.clone();
    let role_name_for_response = req.name.clone();
    tokio::task::spawn_blocking(move || {
        let store = exchange_core::rbac::RbacStore::open(&db_root)
            .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        // Parse permission strings into Permission enum values.
        let mut permissions = Vec::new();
        for perm_str in &req.permissions {
            let perm = match perm_str.to_lowercase().as_str() {
                "admin" => exchange_core::rbac::Permission::Admin,
                "ddl" => exchange_core::rbac::Permission::DDL,
                "system" => exchange_core::rbac::Permission::System,
                s if s.starts_with("read:") => exchange_core::rbac::Permission::Read {
                    table: Some(s[5..].to_string()),
                },
                "read" => exchange_core::rbac::Permission::Read { table: None },
                s if s.starts_with("write:") => exchange_core::rbac::Permission::Write {
                    table: Some(s[6..].to_string()),
                },
                "write" => exchange_core::rbac::Permission::Write { table: None },
                _ => {
                    return Err(ErrorResponse::new(
                        StatusCode::BAD_REQUEST,
                        format!("unknown permission: {perm_str}"),
                    ));
                }
            };
            permissions.push(perm);
        }

        let role = exchange_core::rbac::Role {
            name: req.name.clone(),
            permissions,
        };

        store
            .create_role(&role)
            .map_err(|e| ErrorResponse::new(StatusCode::CONFLICT, e.to_string()))?;

        Ok::<_, ErrorResponse>(())
    })
    .await
    .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;

    Ok((
        StatusCode::CREATED,
        Json(AdminStatusResponse {
            status: "ok".to_string(),
            message: format!("role '{}' created", role_name_for_response),
        }),
    ))
}

/// `GET /admin/cluster` -- Cluster status.
pub async fn cluster_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let role = if state.read_only {
        "replica"
    } else if state.replication_manager.is_some() {
        "primary"
    } else {
        "standalone"
    };

    Json(ClusterStatusResponse {
        node_id: "local".to_string(),
        role: role.to_string(),
        status: "healthy".to_string(),
    })
}

/// `GET /admin/replication` -- Replication status.
pub async fn replication_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let role = if state.read_only {
        "replica"
    } else if state.replication_manager.is_some() {
        "primary"
    } else {
        "standalone"
    };

    use std::sync::atomic::Ordering;
    Json(ReplicationStatusResponse {
        role: role.to_string(),
        lag_bytes: state.metrics.replication_lag_bytes.load(Ordering::Relaxed),
        lag_seconds: state
            .metrics
            .replication_lag_seconds
            .load(Ordering::Relaxed),
        segments_shipped: state.metrics.wal_segments_shipped.load(Ordering::Relaxed),
        segments_applied: state.metrics.wal_segments_applied.load(Ordering::Relaxed),
    })
}

/// `GET /admin/wal` -- WAL status.
pub async fn wal_status(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    use std::sync::atomic::Ordering;
    Json(WalStatusResponse {
        wal_enabled: true,
        total_segments: state.metrics.wal_segments_total.load(Ordering::Relaxed),
        total_bytes: state.metrics.wal_bytes_total.load(Ordering::Relaxed),
    })
}

/// `GET /admin/partitions/:table` -- Partition info for table.
pub async fn partitions(
    State(state): State<Arc<AppState>>,
    Path(table): Path<String>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let db_root = state.db_root.clone();
    let table_name = table.clone();

    let parts = tokio::task::spawn_blocking(move || {
        let table_dir = db_root.join(&table_name);
        if !table_dir.exists() {
            return Err(ErrorResponse::new(
                StatusCode::NOT_FOUND,
                format!("table '{table_name}' not found"),
            ));
        }

        let mut partitions = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&table_dir) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with('_') {
                        continue;
                    }
                    // Estimate size by summing file sizes in partition dir.
                    let mut size: u64 = 0;
                    let mut rows: u64 = 0;
                    if let Ok(files) = std::fs::read_dir(entry.path()) {
                        for f in files.flatten() {
                            if let Ok(meta) = f.metadata() {
                                size += meta.len();
                                // Estimate rows from any .d file (assume 8 bytes per row).
                                if f.path().extension().and_then(|e| e.to_str()) == Some("d")
                                    && rows == 0
                                {
                                    rows = meta.len() / 8;
                                }
                            }
                        }
                    }
                    partitions.push(PartitionInfo {
                        name,
                        row_count: rows,
                        size_bytes: size,
                    });
                }
            }
        }
        partitions.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(partitions)
    })
    .await
    .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))??;

    Ok(Json(PartitionResponse {
        table,
        partitions: parts,
    }))
}

/// `POST /admin/vacuum/:table` -- Trigger VACUUM.
pub async fn vacuum(
    State(state): State<Arc<AppState>>,
    Path(table): Path<String>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let table_dir = state.db_root.join(&table);
    if !table_dir.exists() {
        return Err(ErrorResponse::new(
            StatusCode::NOT_FOUND,
            format!("table '{table}' not found"),
        ));
    }

    // VACUUM is a no-op placeholder in this implementation.
    Ok(Json(VacuumResponse {
        table,
        status: "completed".to_string(),
    }))
}

/// `POST /admin/checkpoint` -- Trigger checkpoint.
pub async fn checkpoint(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    // Checkpoint is a no-op placeholder in this implementation.
    Json(CheckpointResponse {
        status: "completed".to_string(),
    })
}

/// `GET /admin/slow-queries` -- Recent slow queries from the in-memory ring buffer.
pub async fn slow_queries(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let queries = match state.slow_query_log {
        Some(ref log) => log
            .recent_queries()
            .into_iter()
            .map(|e| SlowQueryEntry {
                timestamp: e.timestamp,
                sql: e.sql,
                duration_secs: e.duration_secs,
                rows: e.rows,
            })
            .collect(),
        None => Vec::new(),
    };
    Json(SlowQueriesResponse { queries })
}

/// `GET /admin/jobs` -- Background job status.
pub async fn jobs(State(_state): State<Arc<AppState>>) -> impl IntoResponse {
    // Return placeholder job statuses.
    Json(JobsResponse {
        jobs: vec![
            JobInfo {
                name: "retention_check".to_string(),
                status: "idle".to_string(),
                last_run: None,
            },
            JobInfo {
                name: "wal_checkpoint".to_string(),
                status: "idle".to_string(),
                last_run: None,
            },
        ],
    })
}

// ---------------------------------------------------------------------------
// Audit log endpoint
// ---------------------------------------------------------------------------

/// Query parameters for audit log retrieval.
#[derive(Debug, Deserialize)]
pub struct AuditQuery {
    /// Start of time range (Unix seconds). Default: last 24 hours.
    #[serde(default)]
    pub from: Option<i64>,
    /// End of time range (Unix seconds). Default: now.
    #[serde(default)]
    pub to: Option<i64>,
    /// Filter by user.
    #[serde(default)]
    pub user: Option<String>,
    /// Filter by action (e.g. "Query", "Login", "CreateTable").
    #[serde(default)]
    pub action: Option<String>,
    /// Maximum entries to return. Default: 200.
    #[serde(default)]
    pub limit: Option<usize>,
}

/// `GET /admin/audit` — retrieve audit log entries with optional filters.
pub async fn audit_log(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<AuditQuery>,
) -> Result<impl IntoResponse, super::response::ErrorResponse> {
    let db_root = state.db_root.clone();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    let from = params.from.unwrap_or(now - 86400); // default: last 24h
    let to = params.to.unwrap_or(now);
    let max_entries = params.limit.unwrap_or(200).min(1000);
    let user_filter = params.user;
    let action_filter = params.action;

    let entries = tokio::task::spawn_blocking(move || {
        let audit = exchange_core::audit::AuditLog::open(&db_root).map_err(|e| e.to_string())?;
        let all = audit.query_log(from, to).map_err(|e| e.to_string())?;

        // Apply filters
        let filtered: Vec<_> = all
            .into_iter()
            .filter(|e| {
                if let Some(ref u) = user_filter
                    && !e.user.eq_ignore_ascii_case(u)
                {
                    return false;
                }
                if let Some(ref a) = action_filter {
                    let action_str = format!("{:?}", e.action);
                    if !action_str.eq_ignore_ascii_case(a) {
                        return false;
                    }
                }
                true
            })
            .take(max_entries)
            .collect();

        Ok::<_, String>(filtered)
    })
    .await
    .map_err(|e| {
        super::response::ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?
    .map_err(|e| super::response::ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(serde_json::json!({
        "entries": entries,
        "count": entries.len(),
    })))
}

// ---------------------------------------------------------------------------
// Replicas endpoint
// ---------------------------------------------------------------------------

/// `GET /admin/replicas` — list connected replicas with their status.
pub async fn replicas(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let replication = state.replication_manager.as_ref();

    let replicas_info = if let Some(_mgr) = replication {
        // Build replica info from metrics (replicas don't register themselves
        // in the current architecture — we report aggregate stats).
        let lag_bytes = state
            .metrics
            .replication_lag_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        let lag_seconds = state
            .metrics
            .replication_lag_seconds
            .load(std::sync::atomic::Ordering::Relaxed);
        let shipped = state
            .metrics
            .wal_segments_shipped
            .load(std::sync::atomic::Ordering::Relaxed);
        let applied = state
            .metrics
            .wal_segments_applied
            .load(std::sync::atomic::Ordering::Relaxed);

        serde_json::json!({
            "role": if state.read_only { "replica" } else { "primary" },
            "replication_active": true,
            "aggregate_stats": {
                "lag_bytes": lag_bytes,
                "lag_seconds": lag_seconds,
                "segments_shipped": shipped,
                "segments_applied": applied,
                "pending_segments": shipped.saturating_sub(applied),
            }
        })
    } else {
        serde_json::json!({
            "role": "standalone",
            "replication_active": false,
            "aggregate_stats": null,
        })
    };

    Json(replicas_info)
}

// ---------------------------------------------------------------------------
// Connections endpoint
// ---------------------------------------------------------------------------

/// `GET /admin/connections` — active connection summary by protocol.
pub async fn connections(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    use std::sync::atomic::Ordering::Relaxed;

    let m = &state.metrics;
    Json(serde_json::json!({
        "total_accepted": m.connections_total.load(Relaxed),
        "active": {
            "total": m.connections_active.load(Relaxed),
            "http": m.connections_http.load(Relaxed),
            "pgwire": m.connections_pg.load(Relaxed),
            "ilp": m.connections_ilp.load(Relaxed),
        },
        "queries": {
            "active": m.active_queries.load(Relaxed),
            "total": m.queries_total.load(Relaxed),
            "failed": m.queries_failed_total.load(Relaxed),
            "slow": m.slow_queries_total.load(Relaxed),
        },
    }))
}

// ---------------------------------------------------------------------------
// Router builder
// ---------------------------------------------------------------------------

/// Build the admin router with all `/admin/*` routes.
pub fn admin_router() -> axum::Router<Arc<AppState>> {
    use axum::routing::{delete, get, post};

    axum::Router::new()
        .route("/config", get(get_config).post(update_config))
        .route("/users", get(list_users).post(create_user))
        .route("/users/{name}", delete(delete_user))
        .route("/roles", get(list_roles).post(create_role))
        .route("/cluster", get(cluster_status))
        .route("/replication", get(replication_status))
        .route("/replicas", get(replicas))
        .route("/connections", get(connections))
        .route("/audit", get(audit_log))
        .route("/wal", get(wal_status))
        .route("/partitions/{table}", get(partitions))
        .route("/vacuum/{table}", post(vacuum))
        .route("/checkpoint", post(checkpoint))
        .route("/slow-queries", get(slow_queries))
        .route("/jobs", get(jobs))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path());
        // Keep dir alive by leaking -- tests are short-lived.
        let _ = Box::leak(Box::new(dir));
        Arc::new(state)
    }

    fn admin_app(state: Arc<AppState>) -> Router {
        Router::new()
            .nest("/admin", admin_router())
            .with_state(state)
    }

    async fn get_json(app: &Router, path: &str) -> (StatusCode, serde_json::Value) {
        let resp = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        (status, json)
    }

    async fn post_json(
        app: &Router,
        path: &str,
        body: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        (status, json)
    }

    async fn delete_req(app: &Router, path: &str) -> (StatusCode, serde_json::Value) {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(path)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        (status, json)
    }

    #[tokio::test]
    async fn test_get_config() {
        let state = test_state();
        let app = admin_app(state);
        let (status, json) = get_json(&app, "/admin/config").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.get("data_dir").is_some());
        assert!(json.get("read_only").is_some());
        assert!(json.get("uptime_secs").is_some());
    }

    #[tokio::test]
    async fn test_post_config() {
        let state = test_state();
        let app = admin_app(state);
        let (status, json) = post_json(
            &app,
            "/admin/config",
            serde_json::json!({"log_level": "debug"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn test_list_users() {
        let state = test_state();
        let app = admin_app(state);
        let (status, json) = get_json(&app, "/admin/users").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.is_array());
    }

    #[tokio::test]
    async fn test_create_and_delete_user() {
        let state = test_state();
        let app = admin_app(state);

        // Create user.
        let (status, json) = post_json(
            &app,
            "/admin/users",
            serde_json::json!({"username": "alice", "password": "secret123", "roles": ["reader"]}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(json["status"], "ok");

        // List users -- should contain alice.
        let (status, json) = get_json(&app, "/admin/users").await;
        assert_eq!(status, StatusCode::OK);
        let users = json.as_array().unwrap();
        assert!(users.iter().any(|u| u["username"] == "alice"));

        // Delete user.
        let (status, json) = delete_req(&app, "/admin/users/alice").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn test_list_roles() {
        let state = test_state();
        let app = admin_app(state);
        let (status, json) = get_json(&app, "/admin/roles").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.is_array());
    }

    #[tokio::test]
    async fn test_create_role() {
        let state = test_state();
        let app = admin_app(state);

        let (status, json) = post_json(
            &app,
            "/admin/roles",
            serde_json::json!({"name": "reader", "permissions": ["read"]}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn test_cluster_status() {
        let state = test_state();
        let app = admin_app(state);
        let (status, json) = get_json(&app, "/admin/cluster").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.get("role").is_some());
        assert!(json.get("status").is_some());
    }

    #[tokio::test]
    async fn test_replication_status() {
        let state = test_state();
        let app = admin_app(state);
        let (status, json) = get_json(&app, "/admin/replication").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.get("role").is_some());
        assert!(json.get("lag_bytes").is_some());
    }

    #[tokio::test]
    async fn test_wal_status() {
        let state = test_state();
        let app = admin_app(state);
        let (status, json) = get_json(&app, "/admin/wal").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.get("wal_enabled").is_some());
        assert!(json.get("total_segments").is_some());
    }

    #[tokio::test]
    async fn test_partitions_not_found() {
        let state = test_state();
        let app = admin_app(state);
        let (status, _json) = get_json(&app, "/admin/partitions/nonexistent").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_vacuum_not_found() {
        let state = test_state();
        let app = admin_app(state);
        let (_status, _json) =
            post_json(&app, "/admin/vacuum/nonexistent", serde_json::json!({})).await;
        // vacuum sends empty body but we need to match the handler
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/vacuum/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_checkpoint() {
        let state = test_state();
        let app = admin_app(state);
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/checkpoint")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "completed");
    }

    #[tokio::test]
    async fn test_slow_queries() {
        let state = test_state();
        let app = admin_app(state);
        let (status, json) = get_json(&app, "/admin/slow-queries").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.get("queries").is_some());
        assert!(json["queries"].is_array());
    }

    #[tokio::test]
    async fn test_jobs() {
        let state = test_state();
        let app = admin_app(state);
        let (status, json) = get_json(&app, "/admin/jobs").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.get("jobs").is_some());
        let jobs = json["jobs"].as_array().unwrap();
        assert!(!jobs.is_empty());
    }

    #[tokio::test]
    async fn test_all_admin_endpoints_return_valid_json() {
        let state = test_state();
        let app = admin_app(state);

        // GET endpoints that should return 200 with valid JSON.
        let get_endpoints = [
            "/admin/config",
            "/admin/users",
            "/admin/roles",
            "/admin/cluster",
            "/admin/replication",
            "/admin/wal",
            "/admin/slow-queries",
            "/admin/jobs",
        ];

        for endpoint in &get_endpoints {
            let (status, json) = get_json(&app, endpoint).await;
            assert_eq!(status, StatusCode::OK, "GET {endpoint} returned {status}");
            // Verify it's valid JSON (the get_json helper already parses it).
            assert!(
                json.is_object() || json.is_array(),
                "GET {endpoint} did not return JSON object or array"
            );
        }

        // POST config should work.
        let (status, json) = post_json(&app, "/admin/config", serde_json::json!({})).await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.is_object());

        // POST checkpoint should work.
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/checkpoint")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
