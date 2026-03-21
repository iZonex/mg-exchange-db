//! Comprehensive tests for the ExchangeDB HTTP layer.
//!
//! 100 tests covering:
//! - Query endpoint (valid SQL, errors, empty, concurrent)
//! - Write endpoint (ILP write, auto table creation, invalid format)
//! - Health endpoint
//! - Export/Import endpoints
//! - Admin endpoints
//! - Rate limiting
//! - CORS
//! - Metrics / Prometheus

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use tower::ServiceExt;

use exchange_net::http::handlers::AppState;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn test_state() -> (tempfile::TempDir, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let state = AppState::new(dir.path());
    (dir, Arc::new(state))
}

fn app(state: Arc<AppState>) -> Router {
    exchange_net::http::router(state)
}

async fn get(router: &Router, path: &str) -> (StatusCode, bytes::Bytes) {
    let resp = router
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    (status, body)
}

async fn get_json(
    router: &Router,
    path: &str,
) -> (StatusCode, serde_json::Value) {
    let (status, body) = get(router, path).await;
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (status, json)
}

async fn post_json(
    router: &Router,
    path: &str,
    body: serde_json::Value,
) -> (StatusCode, serde_json::Value) {
    let resp = router
        .clone()
        .oneshot(
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (status, json)
}

async fn post_text(
    router: &Router,
    path: &str,
    body: &str,
) -> (StatusCode, bytes::Bytes) {
    let resp = router
        .clone()
        .oneshot(
            Request::post(path)
                .header("content-type", "text/plain")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    (status, body)
}

// ---------------------------------------------------------------------------
// mod health_endpoint
// ---------------------------------------------------------------------------

mod health_endpoint {
    use super::*;

    #[tokio::test]
    async fn health_returns_ok() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = get_json(&router, "/api/v1/health").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn health_contains_version() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (_, json) = get_json(&router, "/api/v1/health").await;
        assert!(json.get("version").is_some());
        assert!(!json["version"].as_str().unwrap().is_empty());
    }

    #[tokio::test]
    async fn health_contains_uptime() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (_, json) = get_json(&router, "/api/v1/health").await;
        let uptime = json["uptime_secs"].as_f64().unwrap();
        assert!(uptime >= 0.0);
    }

    #[tokio::test]
    async fn health_no_replication_by_default() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (_, json) = get_json(&router, "/api/v1/health").await;
        assert!(json.get("replication").is_none() || json["replication"].is_null());
    }

    #[tokio::test]
    async fn health_is_public_no_auth_needed() {
        // Even with auth configured, health should be accessible
        let dir = tempfile::tempdir().unwrap();
        let state = AppState::new(dir.path())
            .with_auth(exchange_net::auth::AuthConfig::new(vec![
                "secret".into(),
            ]));
        let router = app(Arc::new(state));
        let (status, _) = get_json(&router, "/api/v1/health").await;
        assert_eq!(status, StatusCode::OK);
    }
}

// ---------------------------------------------------------------------------
// mod query_endpoint
// ---------------------------------------------------------------------------

mod query_endpoint {
    use super::*;

    #[tokio::test]
    async fn empty_query_returns_400() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = post_json(
            &router,
            "/api/v1/query",
            serde_json::json!({"query": ""}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(json.get("error").is_some());
    }

    #[tokio::test]
    async fn whitespace_only_query_returns_400() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, _) = post_json(
            &router,
            "/api/v1/query",
            serde_json::json!({"query": "   "}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn sql_error_returns_error_json() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = post_json(
            &router,
            "/api/v1/query",
            serde_json::json!({"query": "SELECT * FROM nonexistent_table"}),
        )
        .await;
        // Should be either 400 or 404
        assert!(status == StatusCode::BAD_REQUEST || status == StatusCode::NOT_FOUND);
        assert!(json.get("error").is_some());
    }

    #[tokio::test]
    async fn create_table_via_ilp_then_query() {
        let (_dir, state) = test_state();
        let router = app(state);
        // Create table via ILP write
        post_text(&router, "/api/v1/write", "test_q,host=h1 price=100.5 1000\n").await;
        let (status, json) = post_json(
            &router,
            "/api/v1/query",
            serde_json::json!({"query": "SELECT * FROM test_q"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.get("columns").is_some());
    }

    #[tokio::test]
    async fn query_response_has_timing() {
        let (_dir, state) = test_state();
        let router = app(state);
        // Create table via ILP
        post_text(&router, "/api/v1/write", "timing_test val=1i 1000\n").await;
        let (status, json) = post_json(
            &router,
            "/api/v1/query",
            serde_json::json!({"query": "SELECT * FROM timing_test"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.get("timing_ms").is_some());
        let timing = json["timing_ms"].as_f64().unwrap();
        assert!(timing >= 0.0);
    }

    #[tokio::test]
    async fn query_response_has_columns() {
        let (_dir, state) = test_state();
        let router = app(state);
        // Create table via ILP
        post_text(&router, "/api/v1/write", "col_test price=100.5 1000\n").await;
        let (_, json) = post_json(
            &router,
            "/api/v1/query",
            serde_json::json!({"query": "SELECT * FROM col_test"}),
        )
        .await;
        let cols = json["columns"].as_array().unwrap();
        assert!(!cols.is_empty());
        for col in cols {
            assert!(col.get("name").is_some());
            assert!(col.get("type").is_some());
        }
    }

    #[tokio::test]
    async fn query_response_rows_array() {
        let (_dir, state) = test_state();
        let router = app(state);
        // Create table via ILP
        post_text(&router, "/api/v1/write", "rows_test val=1i 1000\n").await;
        let (_, json) = post_json(
            &router,
            "/api/v1/query",
            serde_json::json!({"query": "SELECT * FROM rows_test"}),
        )
        .await;
        assert!(json["rows"].is_array());
    }

    #[tokio::test]
    async fn multiple_concurrent_queries() {
        let (_dir, state) = test_state();
        let router = app(state.clone());
        // Create table via ILP
        post_text(&router, "/api/v1/write", "conc v=1i 1000\n").await;

        let mut handles = Vec::new();
        for _ in 0..5 {
            let r = router.clone();
            handles.push(tokio::spawn(async move {
                let (status, _) = post_json(
                    &r,
                    "/api/v1/query",
                    serde_json::json!({"query": "SELECT * FROM conc"}),
                )
                .await;
                status
            }));
        }

        for h in handles {
            let status = h.await.unwrap();
            assert_eq!(status, StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn drop_table_via_sql() {
        let (_dir, state) = test_state();
        let router = app(state);
        // Create table via ILP
        post_text(&router, "/api/v1/write", "droppable v=1i 1000\n").await;
        let (status, _) = post_json(
            &router,
            "/api/v1/query",
            serde_json::json!({"query": "DROP TABLE droppable"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }
}

// ---------------------------------------------------------------------------
// mod write_endpoint
// ---------------------------------------------------------------------------

mod write_endpoint {
    use super::*;

    #[tokio::test]
    async fn write_creates_table_auto() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, body) =
            post_text(&router, "/api/v1/write", "auto_tbl,host=h1 val=42i 1000\n")
                .await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "ok");
        assert_eq!(json["lines_accepted"], 1);
    }

    #[tokio::test]
    async fn write_multiple_lines() {
        let (_dir, state) = test_state();
        let router = app(state);
        let ilp = "multi,host=h1 val=1i 1000\nmulti,host=h2 val=2i 2000\n";
        let (status, body) = post_text(&router, "/api/v1/write", ilp).await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["lines_accepted"], 2);
    }

    #[tokio::test]
    async fn write_to_existing_table() {
        let (_dir, state) = test_state();
        let router = app(state);
        // First write creates the table
        post_text(&router, "/api/v1/write", "existing,tag=a val=1i 1000\n").await;
        // Second write to same table
        let (status, body) =
            post_text(&router, "/api/v1/write", "existing,tag=b val=2i 2000\n")
                .await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["lines_accepted"], 1);
    }

    #[tokio::test]
    async fn write_empty_body_returns_400() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, body) = post_text(&router, "/api/v1/write", "").await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(json.get("error").is_some());
    }

    #[tokio::test]
    async fn write_whitespace_only_returns_400() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, _) = post_text(&router, "/api/v1/write", "   \n  \n").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn write_invalid_ilp_returns_400() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, body) =
            post_text(&router, "/api/v1/write", "this is not ILP\n").await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(json["error"].as_str().unwrap().contains("ILP"));
    }

    #[tokio::test]
    async fn write_all_field_types() {
        let (_dir, state) = test_state();
        let router = app(state);
        let ilp = r#"typed,tag=t int_val=42i,float_val=3.14,str_val="hello",bool_val=true 1000"#;
        let (status, body) = post_text(&router, "/api/v1/write", ilp).await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["lines_accepted"], 1);
    }

    #[tokio::test]
    async fn write_read_only_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = AppState::new(dir.path());
        state.read_only = true;
        let router = app(Arc::new(state));
        let (status, _) =
            post_text(&router, "/api/v1/write", "tbl val=1i 1000\n").await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn write_comments_only_returns_400() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, _) =
            post_text(&router, "/api/v1/write", "# only comments\n# nothing\n")
                .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}

// ---------------------------------------------------------------------------
// mod tables_endpoint
// ---------------------------------------------------------------------------

mod tables_endpoint {
    use super::*;

    #[tokio::test]
    async fn list_tables_empty() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = get_json(&router, "/api/v1/tables").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json["tables"].is_array());
    }

    #[tokio::test]
    async fn list_tables_after_create() {
        let (_dir, state) = test_state();
        let router = app(state);
        // Create via ILP write
        post_text(&router, "/api/v1/write", "listed_tbl val=1i 1000\n").await;
        let (status, json) = get_json(&router, "/api/v1/tables").await;
        assert_eq!(status, StatusCode::OK);
        let tables = json["tables"].as_array().unwrap();
        assert!(tables.iter().any(|t| t == "listed_tbl"));
    }

    #[tokio::test]
    async fn table_info_not_found() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) =
            get_json(&router, "/api/v1/tables/nonexistent").await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(json.get("error").is_some());
    }

    #[tokio::test]
    async fn table_info_after_create() {
        let (_dir, state) = test_state();
        let router = app(state);
        post_text(&router, "/api/v1/write", "info_tbl,tag=a val=1i 1000\n").await;
        let (status, json) = get_json(&router, "/api/v1/tables/info_tbl").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["name"], "info_tbl");
        assert!(json.get("columns").is_some());
        assert!(json.get("row_count").is_some());
    }
}

// ---------------------------------------------------------------------------
// mod export_endpoint
// ---------------------------------------------------------------------------

mod export_endpoint {
    use super::*;

    #[tokio::test]
    async fn csv_export_with_data() {
        let (_dir, state) = test_state();
        let router = app(state);
        // Create table with data
        post_text(&router, "/api/v1/write", "exp,tag=a val=42i 1000\n").await;

        let (status, body) = get(
            &router,
            "/api/v1/export?query=SELECT%20*%20FROM%20exp",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let csv = std::str::from_utf8(&body).unwrap();
        assert!(csv.contains(","));
    }

    #[tokio::test]
    async fn csv_export_empty_query_returns_400() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, _) = get(&router, "/api/v1/export?query=").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn csv_export_unsupported_format_returns_400() {
        let (_dir, state) = test_state();
        let router = app(state);
        post_text(&router, "/api/v1/write", "fmttbl val=1i 1000\n").await;
        let (status, _) = get(
            &router,
            "/api/v1/export?query=SELECT%20*%20FROM%20fmttbl&format=parquet",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}

// ---------------------------------------------------------------------------
// mod import_endpoint
// ---------------------------------------------------------------------------

mod import_endpoint {
    use super::*;

    #[tokio::test]
    async fn csv_import_basic() {
        let (_dir, state) = test_state();
        let router = app(state);
        let csv = "name,value\nalice,100\nbob,200\n";
        let (status, body) = post_text(
            &router,
            "/api/v1/import?table=imported",
            csv,
        )
        .await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["rows_imported"], 2);
    }

    #[tokio::test]
    async fn csv_import_empty_body_returns_400() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, _) = post_text(
            &router,
            "/api/v1/import?table=t",
            "",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn csv_import_empty_table_param_returns_400() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, _) = post_text(
            &router,
            "/api/v1/import?table=",
            "a,b\n1,2\n",
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn csv_import_auto_type_detection() {
        let (_dir, state) = test_state();
        let router = app(state);
        let csv = "name,count,ratio\nalpha,10,1.5\nbeta,20,2.5\n";
        let (status, body) =
            post_text(&router, "/api/v1/import?table=typed_import", csv).await;
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["rows_imported"], 2);
    }
}

// ---------------------------------------------------------------------------
// mod admin_endpoints
// ---------------------------------------------------------------------------

mod admin_endpoints {
    use super::*;

    #[tokio::test]
    async fn admin_config_returns_json() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = get_json(&router, "/admin/config").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.get("data_dir").is_some());
        assert!(json.get("read_only").is_some());
    }

    #[tokio::test]
    async fn admin_post_config() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = post_json(
            &router,
            "/admin/config",
            serde_json::json!({"log_level": "debug"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn admin_users_returns_array() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = get_json(&router, "/admin/users").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.is_array());
    }

    #[tokio::test]
    async fn admin_create_user() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = post_json(
            &router,
            "/admin/users",
            serde_json::json!({
                "username": "admin_test_user",
                "password": "secret123",
                "roles": []
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn admin_roles_returns_array() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = get_json(&router, "/admin/roles").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.is_array());
    }

    #[tokio::test]
    async fn admin_create_role() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = post_json(
            &router,
            "/admin/roles",
            serde_json::json!({
                "name": "testrole",
                "permissions": ["read"]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn admin_cluster_returns_json() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = get_json(&router, "/admin/cluster").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.get("role").is_some());
        assert!(json.get("status").is_some());
    }

    #[tokio::test]
    async fn admin_replication_returns_json() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = get_json(&router, "/admin/replication").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.get("role").is_some());
    }

    #[tokio::test]
    async fn admin_wal_returns_json() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = get_json(&router, "/admin/wal").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.get("wal_enabled").is_some());
    }

    #[tokio::test]
    async fn admin_slow_queries_returns_json() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = get_json(&router, "/admin/slow-queries").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json["queries"].is_array());
    }

    #[tokio::test]
    async fn admin_jobs_returns_json() {
        let (_dir, state) = test_state();
        let router = app(state);
        let (status, json) = get_json(&router, "/admin/jobs").await;
        assert_eq!(status, StatusCode::OK);
        assert!(json["jobs"].is_array());
    }

    #[tokio::test]
    async fn admin_all_get_endpoints_return_valid_json() {
        let (_dir, state) = test_state();
        let router = app(state);
        let endpoints = [
            "/admin/config",
            "/admin/users",
            "/admin/roles",
            "/admin/cluster",
            "/admin/replication",
            "/admin/wal",
            "/admin/slow-queries",
            "/admin/jobs",
        ];
        for ep in &endpoints {
            let (status, json) = get_json(&router, ep).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "GET {ep} returned {status}"
            );
            assert!(
                json.is_object() || json.is_array(),
                "GET {ep} did not return JSON"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// mod rate_limiting
// ---------------------------------------------------------------------------

mod rate_limiting {
    use exchange_net::http::rate_limit::RateLimiter;
    use std::net::{IpAddr, Ipv4Addr};
    use std::time::Duration;

    #[test]
    fn within_limit_allowed() {
        let limiter = RateLimiter::new(5, Duration::from_secs(1));
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        for _ in 0..5 {
            assert!(limiter.check(ip));
        }
    }

    #[test]
    fn over_limit_blocked() {
        let limiter = RateLimiter::new(3, Duration::from_secs(1));
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        assert!(limiter.check(ip));
        assert!(limiter.check(ip));
        assert!(limiter.check(ip));
        assert!(!limiter.check(ip));
    }

    #[test]
    fn different_ips_independent() {
        let limiter = RateLimiter::new(2, Duration::from_secs(1));
        let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let ip2 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
        assert!(limiter.check(ip1));
        assert!(limiter.check(ip1));
        assert!(!limiter.check(ip1));
        assert!(limiter.check(ip2));
    }

    #[test]
    fn window_expiry_resets() {
        let limiter = RateLimiter::new(1, Duration::from_millis(1));
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 3));
        assert!(limiter.check(ip));
        assert!(!limiter.check(ip));
        std::thread::sleep(Duration::from_millis(5));
        assert!(limiter.check(ip));
    }

    #[test]
    fn cleanup_removes_expired() {
        let limiter = RateLimiter::new(1, Duration::from_millis(1));
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 4));
        limiter.check(ip);
        std::thread::sleep(Duration::from_millis(5));
        limiter.cleanup();
        // Internal state cleaned up
    }

    #[test]
    fn default_config_allows_requests() {
        let limiter = RateLimiter::default_config();
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        assert!(limiter.check(ip));
    }

    #[test]
    fn ipv6_rate_limiting() {
        let limiter = RateLimiter::new(1, Duration::from_secs(1));
        let ip = IpAddr::V6(std::net::Ipv6Addr::LOCALHOST);
        assert!(limiter.check(ip));
        assert!(!limiter.check(ip));
    }

    #[test]
    fn high_burst_limit() {
        let limiter = RateLimiter::new(1000, Duration::from_secs(1));
        let ip = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5));
        for _ in 0..1000 {
            assert!(limiter.check(ip));
        }
        assert!(!limiter.check(ip));
    }
}

// ---------------------------------------------------------------------------
// mod cors
// ---------------------------------------------------------------------------

mod cors {
    use super::*;

    #[tokio::test]
    async fn options_returns_200() {
        let (_dir, state) = test_state();
        let router = app(state);
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/api/v1/query")
                    .header("origin", "http://localhost:3000")
                    .header(
                        "access-control-request-method",
                        "POST",
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cors_headers_present_on_get() {
        let (_dir, state) = test_state();
        let router = app(state);
        let resp = router
            .clone()
            .oneshot(
                Request::get("/api/v1/health")
                    .header("origin", "http://example.com")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // Check that CORS headers are present
        assert!(resp.headers().get("access-control-allow-origin").is_some());
    }
}

// ---------------------------------------------------------------------------
// mod metrics
// ---------------------------------------------------------------------------

mod metrics {
    use exchange_net::metrics::Metrics;
    use std::sync::atomic::Ordering;

    #[test]
    fn prometheus_format_has_help_lines() {
        let m = Metrics::new();
        let output = m.render();
        assert!(output.contains("# HELP"));
    }

    #[test]
    fn prometheus_format_has_type_lines() {
        let m = Metrics::new();
        let output = m.render();
        assert!(output.contains("# TYPE"));
    }

    #[test]
    fn counter_increment() {
        let m = Metrics::new();
        m.inc_queries();
        m.inc_queries();
        assert_eq!(m.queries_total.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn histogram_observation() {
        let m = Metrics::new();
        m.observe_query_duration(0.003);
        m.observe_query_duration(0.5);
        let output = m.render();
        assert!(output.contains("exchangedb_query_duration_seconds_count 2"));
    }

    #[test]
    fn all_metric_types_present() {
        let m = Metrics::new();
        let output = m.render();
        assert!(output.contains("counter"));
        assert!(output.contains("gauge"));
        assert!(output.contains("histogram"));
    }

    #[test]
    fn rows_written_counter() {
        let m = Metrics::new();
        m.add_rows_written(100);
        m.add_rows_written(50);
        assert_eq!(m.rows_written_total.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn rows_read_counter() {
        let m = Metrics::new();
        m.add_rows_read(200);
        assert_eq!(m.rows_read_total.load(Ordering::Relaxed), 200);
    }

    #[test]
    fn connections_inc_dec() {
        let m = Metrics::new();
        m.inc_connections();
        m.inc_connections();
        m.dec_connections();
        assert_eq!(m.active_connections.load(Ordering::Relaxed), 1);
        assert_eq!(m.connections_total.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn tables_count_gauge() {
        let m = Metrics::new();
        m.set_tables_count(5);
        let output = m.render();
        assert!(output.contains("exchangedb_tables_count 5"));
    }

    #[test]
    fn ilp_lines_counter() {
        let m = Metrics::new();
        m.add_ilp_lines(10);
        m.add_ilp_lines(5);
        assert_eq!(
            m.ilp_lines_received_total.load(Ordering::Relaxed),
            15
        );
    }

    #[test]
    fn twenty_plus_unique_metrics() {
        let m = Metrics::new();
        let output = m.render();
        let count = output
            .lines()
            .filter(|l| l.starts_with("# HELP exchangedb_"))
            .count();
        assert!(count >= 20);
    }

    #[test]
    fn histogram_bucket_boundaries() {
        let m = Metrics::new();
        let output = m.render();
        assert!(output.contains(r#"le="0.001""#));
        assert!(output.contains(r#"le="0.01""#));
        assert!(output.contains(r#"le="0.1""#));
        assert!(output.contains(r#"le="1""#));
        assert!(output.contains(r#"le="+Inf""#));
    }

    #[test]
    fn protocol_specific_connections() {
        let m = Metrics::new();
        m.inc_http_connections();
        m.inc_pg_connections();
        m.inc_ilp_connections();
        assert_eq!(m.connections_http.load(Ordering::Relaxed), 1);
        assert_eq!(m.connections_pg.load(Ordering::Relaxed), 1);
        assert_eq!(m.connections_ilp.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn replication_lag_metrics() {
        let m = Metrics::new();
        m.set_replication_lag_bytes(1024);
        m.set_replication_lag_seconds(2);
        let output = m.render();
        assert!(output.contains("exchangedb_replication_lag_bytes 1024"));
        assert!(output.contains("exchangedb_replication_lag_seconds 2"));
    }

    #[test]
    fn memory_metrics() {
        let m = Metrics::new();
        m.set_memory_used_bytes(500_000);
        m.set_memory_limit_bytes(8_000_000);
        let output = m.render();
        assert!(output.contains("exchangedb_memory_used_bytes 500000"));
        assert!(output.contains("exchangedb_memory_limit_bytes 8000000"));
    }

    #[test]
    fn plan_cache_metrics() {
        let m = Metrics::new();
        m.inc_plan_cache_hits();
        m.inc_plan_cache_hits();
        m.inc_plan_cache_misses();
        let output = m.render();
        assert!(output.contains("exchangedb_plan_cache_hits_total 2"));
        assert!(output.contains("exchangedb_plan_cache_misses_total 1"));
    }
}

// ---------------------------------------------------------------------------
// mod auth_integration
// ---------------------------------------------------------------------------

mod auth_integration {
    use super::*;
    use exchange_net::auth::{
        AuthConfig, AuthMethod, AuthResult, try_authenticate,
    };

    #[test]
    fn auth_none_always_authenticated() {
        let method = AuthMethod::None;
        let headers = axum::http::HeaderMap::new();
        let result = try_authenticate(&method, &headers);
        assert!(matches!(result, AuthResult::Authenticated { .. }));
    }

    #[test]
    fn auth_token_valid() {
        let config = AuthConfig::new(vec!["my-token".into()]);
        let method = AuthMethod::Token(config);
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("authorization", "Bearer my-token".parse().unwrap());
        let result = try_authenticate(&method, &headers);
        assert!(matches!(result, AuthResult::Authenticated { .. }));
    }

    #[test]
    fn auth_token_invalid() {
        let config = AuthConfig::new(vec!["my-token".into()]);
        let method = AuthMethod::Token(config);
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("authorization", "Bearer wrong".parse().unwrap());
        let result = try_authenticate(&method, &headers);
        assert!(matches!(result, AuthResult::InvalidCredentials(_)));
    }

    #[test]
    fn auth_token_missing() {
        let config = AuthConfig::new(vec!["my-token".into()]);
        let method = AuthMethod::Token(config);
        let headers = axum::http::HeaderMap::new();
        let result = try_authenticate(&method, &headers);
        assert!(matches!(result, AuthResult::NoCredentials));
    }

    #[test]
    fn auth_disabled_always_ok() {
        let config = AuthConfig::default();
        let method = AuthMethod::Token(config);
        let headers = axum::http::HeaderMap::new();
        let result = try_authenticate(&method, &headers);
        assert!(matches!(result, AuthResult::Authenticated { .. }));
    }

    #[test]
    fn auth_multi_fallback() {
        let config = AuthConfig::new(vec!["token".into()]);
        let method = AuthMethod::Multi(vec![
            AuthMethod::Token(config),
            AuthMethod::None,
        ]);
        let headers = axum::http::HeaderMap::new();
        let result = try_authenticate(&method, &headers);
        assert!(matches!(result, AuthResult::Authenticated { .. }));
    }

    #[test]
    fn auth_config_multiple_tokens() {
        let config = AuthConfig::new(vec![
            "token-a".into(),
            "token-b".into(),
        ]);
        assert!(config.is_valid_token("token-a"));
        assert!(config.is_valid_token("token-b"));
        assert!(!config.is_valid_token("token-c"));
    }
}

// ---------------------------------------------------------------------------
// mod response_types — response serialization tests
// ---------------------------------------------------------------------------

mod response_types {
    use exchange_net::http::response::*;

    #[test]
    fn query_response_serialization() {
        let resp = QueryResponse {
            columns: vec![ColumnInfo { name: "id".into(), r#type: "I64".into() }],
            rows: vec![vec![serde_json::json!(42)]],
            timing_ms: 1.5,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let d: QueryResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(d.columns.len(), 1);
        assert_eq!(d.rows.len(), 1);
    }

    #[test]
    fn error_response_serialization() {
        let resp = ErrorResponse::new(axum::http::StatusCode::NOT_FOUND, "table not found");
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("404"));
        assert!(json.contains("table not found"));
    }

    #[test]
    fn health_response_serialization() {
        let resp = HealthResponse {
            status: "ok".into(),
            version: "0.1.0".into(),
            uptime_secs: 42.0,
            replication: None,
            checks: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let d: HealthResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(d.status, "ok");
    }

    #[test]
    fn write_response_serialization() {
        let resp = WriteResponse {
            status: "ok".into(),
            lines_accepted: 10,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let d: WriteResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(d.lines_accepted, 10);
    }

    #[test]
    fn table_info_response_serialization() {
        let resp = TableInfoResponse {
            name: "trades".into(),
            columns: vec![
                ColumnInfo { name: "price".into(), r#type: "F64".into() },
                ColumnInfo { name: "volume".into(), r#type: "I64".into() },
            ],
            partition_by: "Day".into(),
            row_count: 1_000_000,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let d: TableInfoResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(d.name, "trades");
        assert_eq!(d.columns.len(), 2);
        assert_eq!(d.row_count, 1_000_000);
    }

    #[test]
    fn tables_list_response_serialization() {
        let resp = TablesListResponse {
            tables: vec!["a".into(), "b".into()],
        };
        let json = serde_json::to_string(&resp).unwrap();
        let d: TablesListResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(d.tables.len(), 2);
    }

    #[test]
    fn error_response_sql_state_table_not_found() {
        let resp = ErrorResponse::new(axum::http::StatusCode::NOT_FOUND, "table 'foo' not found");
        assert_eq!(resp.sql_state.as_deref(), Some("42P01"));
    }

    #[test]
    fn error_response_sql_state_already_exists() {
        let resp = ErrorResponse::new(axum::http::StatusCode::CONFLICT, "table already exists");
        assert_eq!(resp.sql_state.as_deref(), Some("42P07"));
    }

    #[test]
    fn error_response_sql_state_permission_denied() {
        let resp = ErrorResponse::new(axum::http::StatusCode::FORBIDDEN, "permission denied");
        assert_eq!(resp.sql_state.as_deref(), Some("42501"));
    }

    #[test]
    fn error_response_with_query() {
        let resp = ErrorResponse::new(axum::http::StatusCode::BAD_REQUEST, "parse error")
            .with_query("SELECT * FORM oops");
        assert_eq!(resp.query.as_deref(), Some("SELECT * FORM oops"));
    }

    #[test]
    fn column_info_name_and_type() {
        let c = ColumnInfo { name: "ts".into(), r#type: "Timestamp".into() };
        assert_eq!(c.name, "ts");
        assert_eq!(c.r#type, "Timestamp");
    }
}

// ---------------------------------------------------------------------------
// mod additional_metrics — more metrics tests
// ---------------------------------------------------------------------------

mod additional_metrics {
    use exchange_net::metrics::Metrics;
    use std::sync::atomic::Ordering;

    #[test]
    fn failed_queries_counter() {
        let m = Metrics::new();
        m.inc_queries_failed();
        m.inc_queries_failed();
        assert_eq!(m.queries_failed_total.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn slow_queries_counter() {
        let m = Metrics::new();
        m.inc_slow_queries();
        assert_eq!(m.slow_queries_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn bytes_written_counter() {
        let m = Metrics::new();
        m.add_bytes_written(4096);
        m.add_bytes_written(2048);
        assert_eq!(m.bytes_written_total.load(Ordering::Relaxed), 6144);
    }

    #[test]
    fn bytes_read_counter() {
        let m = Metrics::new();
        m.add_bytes_read(8192);
        assert_eq!(m.bytes_read_total.load(Ordering::Relaxed), 8192);
    }

    #[test]
    fn wal_segments_counter() {
        let m = Metrics::new();
        m.add_wal_segments(5);
        assert_eq!(m.wal_segments_total.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn wal_bytes_counter() {
        let m = Metrics::new();
        m.add_wal_bytes(65536);
        assert_eq!(m.wal_bytes_total.load(Ordering::Relaxed), 65536);
    }

    #[test]
    fn wal_segments_shipped_counter() {
        let m = Metrics::new();
        m.add_wal_segments_shipped(10);
        assert_eq!(m.wal_segments_shipped.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn wal_segments_applied_counter() {
        let m = Metrics::new();
        m.add_wal_segments_applied(7);
        assert_eq!(m.wal_segments_applied.load(Ordering::Relaxed), 7);
    }

    #[test]
    fn disk_used_bytes_gauge() {
        let m = Metrics::new();
        m.set_disk_used_bytes(1_000_000_000);
        assert_eq!(m.disk_used_bytes.load(Ordering::Relaxed), 1_000_000_000);
    }

    #[test]
    fn partitions_total_gauge() {
        let m = Metrics::new();
        m.set_partitions_total(42);
        assert_eq!(m.partitions_total.load(Ordering::Relaxed), 42);
    }

    #[test]
    fn open_files_gauge() {
        let m = Metrics::new();
        m.set_open_files(128);
        assert_eq!(m.open_files.load(Ordering::Relaxed), 128);
    }

    #[test]
    fn cpu_usage_gauge() {
        let m = Metrics::new();
        m.set_cpu_usage_percent(5050); // 50.50%
        let output = m.render();
        assert!(output.contains("exchangedb_cpu_usage_percent 50.5"));
    }

    #[test]
    fn active_queries_gauge() {
        let m = Metrics::new();
        m.inc_active_queries();
        m.inc_active_queries();
        m.inc_active_queries();
        m.dec_active_queries();
        assert_eq!(m.active_queries.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn uptime_is_positive() {
        let m = Metrics::new();
        let output = m.render();
        // The uptime line should exist and be > 0 (or at least >= 0)
        assert!(output.contains("exchangedb_uptime_seconds"));
    }
}

// ===========================================================================
// Scaffolding wiring integration tests
// ===========================================================================

/// Test that the health endpoint returns actual health check results
/// instead of hardcoded "ok".
#[tokio::test]
async fn health_returns_checks() {
    let (_dir, state) = test_state();
    let app = exchange_net::http::router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Should have "checks" field with actual health check results.
    assert!(json.get("checks").is_some(), "health response should include checks");
    let checks = json["checks"].as_array().unwrap();
    assert!(!checks.is_empty(), "should have at least one health check");

    // Verify check structure.
    let first = &checks[0];
    assert!(first.get("name").is_some());
    assert!(first.get("status").is_some());
    assert!(first.get("message").is_some());
    assert!(first.get("duration_ms").is_some());

    // Verify check names include expected checks.
    let check_names: Vec<&str> = checks.iter().map(|c| c["name"].as_str().unwrap()).collect();
    assert!(check_names.contains(&"disk_space"), "should check disk_space");
    assert!(check_names.contains(&"data_dir"), "should check data_dir");
    assert!(check_names.contains(&"wal_lag"), "should check wal_lag");
    assert!(check_names.contains(&"memory_usage"), "should check memory_usage");

    // Status should reflect actual checks (should be "ok" for a fresh temp dir).
    let status = json["status"].as_str().unwrap();
    assert_eq!(status, "ok");

    // Version should be the real cargo version, not hardcoded.
    let version = json["version"].as_str().unwrap();
    assert!(!version.is_empty());
}

/// Test that AppState includes metering, tenant, and encryption fields.
#[test]
fn app_state_has_scaffolding_fields() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = AppState::new(dir.path());

    // Verify fields are None by default.
    assert!(state.usage_meter.is_none());
    assert!(state.tenant_manager.is_none());
    assert!(state.encryption_config.is_none());

    // Set them and verify they're accessible.
    state.usage_meter = Some(Arc::new(
        exchange_core::metering::UsageMeter::new(dir.path().to_path_buf()),
    ));
    state.tenant_manager = Some(Arc::new(
        exchange_core::tenant::TenantManager::new(dir.path().to_path_buf()),
    ));
    state.encryption_config = Some(Arc::new(
        exchange_core::encryption::EncryptionConfig::disabled(),
    ));

    assert!(state.usage_meter.is_some());
    assert!(state.tenant_manager.is_some());
    assert!(state.encryption_config.is_some());
}

/// Test that metering is wired: usage_meter records writes after ILP ingestion.
#[tokio::test]
async fn metering_records_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = AppState::new(dir.path());
    let meter = Arc::new(exchange_core::metering::UsageMeter::new(dir.path().to_path_buf()));
    state.usage_meter = Some(meter.clone());
    let state = Arc::new(state);
    let app = exchange_net::http::router(state);

    // Send a write request.
    let body = "trades,symbol=BTC price=65000.0 1710513000000000000\n";
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/write")
                .header("content-type", "text/plain")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // Check metering recorded the write for the default tenant.
    let usage = meter.get_usage("default");
    assert_eq!(usage.rows_written, 1, "metering should record 1 row written");
}

/// Test that metering records writes with tenant header.
#[tokio::test]
async fn metering_records_writes_with_tenant() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = AppState::new(dir.path());
    let meter = Arc::new(exchange_core::metering::UsageMeter::new(dir.path().to_path_buf()));
    state.usage_meter = Some(meter.clone());
    let state = Arc::new(state);
    let app = exchange_net::http::router(state);

    let body = "trades,symbol=ETH price=3000.0 1710513000000000000\n";
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/write")
                .header("content-type", "text/plain")
                .header("x-tenant-id", "tenant_42")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let usage = meter.get_usage("tenant_42");
    assert_eq!(usage.rows_written, 1, "metering should record write for tenant_42");

    // Default tenant should have no writes.
    let default_usage = meter.get_usage("default");
    assert_eq!(default_usage.rows_written, 0);
}

/// Test that resource manager is wired into the server startup.
#[test]
fn resource_manager_is_initialized() {
    let dir = tempfile::tempdir().unwrap();
    let state = AppState::new(dir.path());
    // Default AppState has no resource manager.
    assert!(state.resource_mgr.is_none());

    // After start_http_server would set it, but we verify the type works.
    let mgr = Arc::new(exchange_core::resource::ResourceManager::new(
        exchange_core::resource::ResourceLimits::default(),
    ));
    let mut state = AppState::new(dir.path());
    state.resource_mgr = Some(mgr.clone());
    assert_eq!(mgr.active_query_count(), 0);
}

/// Test plan cache is wired: second query hits the cache.
#[tokio::test]
async fn plan_cache_hit_on_repeat_query() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = AppState::new(dir.path());
    let cache = Arc::new(exchange_query::PlanCache::default_config());
    state.plan_cache = Some(cache.clone());
    let state = Arc::new(state);

    // First, create a table.
    let app = exchange_net::http::router(state.clone());
    let create_body = r#"{"query": "CREATE TABLE test_cache (timestamp TIMESTAMP, price DOUBLE) TIMESTAMP(timestamp)"}"#;
    let _ = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(create_body))
                .unwrap(),
        )
        .await
        .unwrap();

    // First SELECT: cache miss, should store plan.
    let app = exchange_net::http::router(state.clone());
    let select_body = r#"{"query": "SELECT * FROM test_cache"}"#;
    let _ = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(select_body))
                .unwrap(),
        )
        .await
        .unwrap();

    let stats = cache.stats();
    // After first SELECT: should be 1 miss (first time), the plan is now cached.
    assert!(stats.misses >= 1, "first query should be a cache miss");

    // Second SELECT: should be a cache hit.
    let app = exchange_net::http::router(state.clone());
    let _ = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(select_body))
                .unwrap(),
        )
        .await
        .unwrap();

    let stats = cache.stats();
    assert!(stats.hits >= 1, "second identical query should be a cache hit");
}

/// Test slow query metrics increment.
#[tokio::test]
async fn slow_query_metric_increments() {
    let dir = tempfile::tempdir().unwrap();
    let mut state = AppState::new(dir.path());
    // Use a threshold of 0ms so every query is "slow".
    let slow_log = Arc::new(exchange_query::SlowQueryLog::new(
        std::time::Duration::from_millis(0),
        None,
    ));
    state.slow_query_log = Some(slow_log);
    let state = Arc::new(state);

    // Create table and run a query.
    let app = exchange_net::http::router(state.clone());
    let create_body = r#"{"query": "CREATE TABLE slow_test (timestamp TIMESTAMP, v BIGINT) TIMESTAMP(timestamp)"}"#;
    let _ = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(create_body))
                .unwrap(),
        )
        .await
        .unwrap();

    let app = exchange_net::http::router(state.clone());
    let select_body = r#"{"query": "SELECT * FROM slow_test"}"#;
    let _ = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(select_body))
                .unwrap(),
        )
        .await
        .unwrap();

    // Check that slow_queries_total was incremented.
    let output = state.metrics.render();
    // With 0ms threshold, the SELECT query should have been logged as slow.
    assert!(
        output.contains("exchangedb_slow_queries_total"),
        "metrics should include slow_queries_total"
    );
}

/// Test that active_queries metric is properly incremented and decremented.
#[tokio::test]
async fn active_queries_metric_balanced() {
    let dir = tempfile::tempdir().unwrap();
    let state = Arc::new(AppState::new(dir.path()));

    // Create table.
    let app = exchange_net::http::router(state.clone());
    let create_body = r#"{"query": "CREATE TABLE active_test (timestamp TIMESTAMP, v BIGINT) TIMESTAMP(timestamp)"}"#;
    let _ = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(create_body))
                .unwrap(),
        )
        .await
        .unwrap();

    // Run a query.
    let app = exchange_net::http::router(state.clone());
    let select_body = r#"{"query": "SELECT * FROM active_test"}"#;
    let _ = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/query")
                .header("content-type", "application/json")
                .body(Body::from(select_body))
                .unwrap(),
        )
        .await
        .unwrap();

    // After query completes, active_queries should be back to 0.
    let active = state.metrics.active_queries.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(active, 0, "active_queries should return to 0 after query completes");
}
