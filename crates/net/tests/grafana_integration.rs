//! Grafana integration tests.
//!
//! Validates that ExchangeDB is compatible with Grafana via:
//! - Grafana JSON data source API (`/api/v1/grafana/*`)
//! - Prometheus metrics endpoint (`/metrics`)
//! - Health endpoint format
//! - Time-series query patterns used by Grafana dashboards

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

async fn get(router: &Router, path: &str) -> (StatusCode, String) {
    let resp = router
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&body).to_string())
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
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap_or(serde_json::json!(null));
    (status, json)
}

async fn post_text(router: &Router, path: &str, body: &str) -> StatusCode {
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
    resp.status()
}

/// Seed a test table with time-series data via ILP.
async fn seed_trades(router: &Router) {
    let ilp = "\
trades,symbol=BTC price=65000.0,volume=100i 1700000000000000000\n\
trades,symbol=BTC price=65100.0,volume=150i 1700000001000000000\n\
trades,symbol=ETH price=3200.0,volume=500i 1700000002000000000\n\
trades,symbol=ETH price=3210.0,volume=600i 1700000003000000000\n\
trades,symbol=BTC price=65200.0,volume=200i 1700000004000000000\n";
    let status = post_text(router, "/api/v1/write", ilp).await;
    assert_eq!(status, StatusCode::OK, "seeding trades failed");
}

// ===========================================================================
// Grafana JSON Data Source API tests
// ===========================================================================

#[tokio::test]
async fn grafana_connection_test() {
    let (_dir, state) = test_state();
    let router = app(state);
    // Try both with and without trailing slash
    let (status1, _) = get(&router, "/api/v1/grafana").await;
    let (status2, _) = get(&router, "/api/v1/grafana/").await;
    assert!(
        status1 == StatusCode::OK || status2 == StatusCode::OK,
        "connection test should return 200 (got {status1}, {status2})"
    );
}

#[tokio::test]
async fn grafana_search_empty_db() {
    let (_dir, state) = test_state();
    let router = app(state);
    let (status, json) = post_json(
        &router,
        "/api/v1/grafana/search",
        serde_json::json!({"target": ""}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn grafana_search_returns_tables() {
    let (_dir, state) = test_state();
    let router = app(state);
    seed_trades(&router).await;

    let (status, json) = post_json(
        &router,
        "/api/v1/grafana/search",
        serde_json::json!({"target": ""}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let tables = json.as_array().unwrap();
    assert!(tables.contains(&serde_json::json!("trades")));
}

#[tokio::test]
async fn grafana_search_with_filter() {
    let (_dir, state) = test_state();
    let router = app(state);
    seed_trades(&router).await;

    // Filter that matches
    let (_, json) = post_json(
        &router,
        "/api/v1/grafana/search",
        serde_json::json!({"target": "trad"}),
    )
    .await;
    assert_eq!(json.as_array().unwrap().len(), 1);

    // Filter that doesn't match
    let (_, json) = post_json(
        &router,
        "/api/v1/grafana/search",
        serde_json::json!({"target": "nonexistent"}),
    )
    .await;
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn grafana_query_timeseries_raw_sql() {
    let (_dir, state) = test_state();
    let router = app(state);
    seed_trades(&router).await;

    let (status, json) = post_json(
        &router,
        "/api/v1/grafana/query",
        serde_json::json!({
            "targets": [
                {
                    "target": "SELECT timestamp, price FROM trades ORDER BY timestamp",
                    "type": "timeserie"
                }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let results = json.as_array().unwrap();
    assert!(!results.is_empty(), "should return at least one series");

    // Each result has "target" and "datapoints"
    let series = &results[0];
    assert!(series["target"].is_string());
    let datapoints = series["datapoints"].as_array().unwrap();
    assert!(!datapoints.is_empty());

    // Each datapoint is [value, timestamp_ms]
    let dp = &datapoints[0];
    assert!(dp[0].is_number(), "value should be numeric");
    assert!(dp[1].is_number(), "timestamp should be numeric");
}

#[tokio::test]
async fn grafana_query_table_mode() {
    let (_dir, state) = test_state();
    let router = app(state);
    seed_trades(&router).await;

    let (status, json) = post_json(
        &router,
        "/api/v1/grafana/query",
        serde_json::json!({
            "targets": [
                {
                    "target": "SELECT timestamp, symbol, price FROM trades ORDER BY timestamp",
                    "type": "table"
                }
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let results = json.as_array().unwrap();
    assert_eq!(results.len(), 1);

    let table = &results[0];
    assert_eq!(table["type"], "table");

    let columns = table["columns"].as_array().unwrap();
    assert!(columns.len() >= 3);
    // Each column has "text" and "type"
    assert!(columns[0]["text"].is_string());
    assert!(columns[0]["type"].is_string());

    let rows = table["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 5); // 5 seeded rows
}

#[tokio::test]
async fn grafana_query_auto_mode_by_table_name() {
    let (_dir, state) = test_state();
    let router = app(state);
    seed_trades(&router).await;

    let (status, json) = post_json(
        &router,
        "/api/v1/grafana/query",
        serde_json::json!({
            "targets": [{"target": "trades", "type": "timeserie"}],
            "maxDataPoints": 500
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "auto query failed: {json}");
    // Should return time-series data (no time range filter = get all)
    let results = json.as_array().unwrap();
    assert!(!results.is_empty(), "auto query should return data: {json}");
}

#[tokio::test]
async fn grafana_annotations_stub() {
    let (_dir, state) = test_state();
    let router = app(state);
    let (status, json) = post_json(
        &router,
        "/api/v1/grafana/annotations",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(json.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn grafana_tag_keys() {
    let (_dir, state) = test_state();
    let router = app(state);
    seed_trades(&router).await;

    let (status, json) = post_json(
        &router,
        "/api/v1/grafana/tag-keys",
        serde_json::json!({"target": "trades"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let keys = json.as_array().unwrap();
    assert!(!keys.is_empty());
    // Each key has "type" and "text"
    let texts: Vec<&str> = keys
        .iter()
        .filter_map(|k| k["text"].as_str())
        .collect();
    assert!(texts.contains(&"timestamp"));
    assert!(texts.contains(&"price"));
}

#[tokio::test]
async fn grafana_tag_keys_no_target() {
    let (_dir, state) = test_state();
    let router = app(state);
    let (status, json) = post_json(
        &router,
        "/api/v1/grafana/tag-keys",
        serde_json::json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(json.as_array().unwrap().is_empty());
}

// ===========================================================================
// Prometheus /metrics endpoint tests
// ===========================================================================

#[tokio::test]
async fn metrics_returns_prometheus_format() {
    let (_dir, state) = test_state();
    let router = app(state);
    let (status, body) = get(&router, "/metrics").await;
    assert_eq!(status, StatusCode::OK);

    // Must contain key Prometheus metrics
    assert!(body.contains("exchangedb_queries_total"), "missing queries_total");
    assert!(body.contains("exchangedb_rows_written_total"), "missing rows_written");
    assert!(body.contains("exchangedb_uptime_seconds"), "missing uptime");
    assert!(body.contains("exchangedb_connections_active"), "missing connections");
}

#[tokio::test]
async fn metrics_has_valid_prometheus_lines() {
    let (_dir, state) = test_state();
    let router = app(state);
    let (_, body) = get(&router, "/metrics").await;

    // Each non-comment non-empty line must match: metric_name{labels} value
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Basic format: name value or name{labels} value
        let parts: Vec<&str> = line.splitn(2, ' ').collect();
        assert!(
            parts.len() >= 2,
            "invalid prometheus line: {line}"
        );
        // Value should be parseable as a number
        let value_str = parts[1].trim();
        assert!(
            value_str.parse::<f64>().is_ok() || value_str == "+Inf" || value_str == "NaN",
            "invalid metric value in: {line}"
        );
    }
}

#[tokio::test]
async fn metrics_query_histogram_buckets() {
    let (_dir, state) = test_state();
    let router = app(state);
    let (_, body) = get(&router, "/metrics").await;

    // Should have histogram buckets for query duration
    assert!(
        body.contains("exchangedb_query_duration_seconds_bucket"),
        "missing query duration histogram buckets"
    );
    assert!(
        body.contains("exchangedb_query_duration_seconds_sum"),
        "missing query duration sum"
    );
    assert!(
        body.contains("exchangedb_query_duration_seconds_count"),
        "missing query duration count"
    );
}

// ===========================================================================
// Health endpoint tests
// ===========================================================================

#[tokio::test]
async fn health_returns_structured_json() {
    let (_dir, state) = test_state();
    let router = app(state);
    let resp = router
        .clone()
        .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let body = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(status, StatusCode::OK);
    assert!(json["status"].is_string());
    assert!(json["version"].is_string());
    assert!(json["uptime_secs"].is_number());
}

#[tokio::test]
async fn health_has_check_details() {
    let (_dir, state) = test_state();
    let router = app(state);
    let resp = router
        .clone()
        .oneshot(Request::get("/api/v1/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let body = axum::body::to_bytes(resp.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    let checks = json["checks"].as_array().unwrap();
    assert!(!checks.is_empty(), "health should have check details");
    for check in checks {
        assert!(check["name"].is_string());
        assert!(check["status"].is_string());
    }
}

// ===========================================================================
// Time-series query patterns (Grafana-style SQL via HTTP API)
// ===========================================================================

#[tokio::test]
async fn grafana_style_aggregate_query() {
    let (_dir, state) = test_state();
    let router = app(state);
    seed_trades(&router).await;

    // Grafana typically sends GROUP BY time bucket queries
    let (status, json) = post_json(
        &router,
        "/api/v1/query",
        serde_json::json!({
            "query": "SELECT symbol, avg(price) as avg_price, sum(volume) as total_vol FROM trades GROUP BY symbol"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let rows = json["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2); // BTC and ETH
}

#[tokio::test]
async fn grafana_style_time_filter_query() {
    let (_dir, state) = test_state();
    let router = app(state);
    seed_trades(&router).await;

    let (status, json) = post_json(
        &router,
        "/api/v1/query",
        serde_json::json!({
            "query": "SELECT timestamp, price FROM trades WHERE timestamp >= '2023-11-14T22:13:20Z' ORDER BY timestamp LIMIT 100"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(json["rows"].as_array().unwrap().len() > 0);
}

#[tokio::test]
async fn grafana_multiple_targets_in_single_request() {
    let (_dir, state) = test_state();
    let router = app(state);
    seed_trades(&router).await;

    let (status, json) = post_json(
        &router,
        "/api/v1/grafana/query",
        serde_json::json!({
            "targets": [
                {"target": "SELECT timestamp, price FROM trades WHERE symbol = 'BTC' ORDER BY timestamp", "type": "timeserie"},
                {"target": "SELECT timestamp, price FROM trades WHERE symbol = 'ETH' ORDER BY timestamp", "type": "timeserie"}
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let results = json.as_array().unwrap();
    assert_eq!(results.len(), 2, "should return 2 series (BTC + ETH)");
}
