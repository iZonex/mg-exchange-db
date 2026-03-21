//! Grafana JSON data source API.
//!
//! Implements the [Grafana JSON Data Source](https://grafana.github.io/grafana-json-datasource/)
//! protocol so Grafana can query ExchangeDB directly over HTTP without
//! needing the pgwire (PostgreSQL) protocol.
//!
//! Endpoints (nested under `/api/v1/grafana`):
//! - `GET  /` — connection test (returns 200)
//! - `POST /search` — list available tables/metrics
//! - `POST /query` — execute time-series or table queries
//! - `POST /annotations` — return annotations (stub)
//! - `POST /tag-keys` — return tag/column names for a table
//! - `POST /tag-values` — return distinct values for a tag/column

use std::sync::Arc;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use exchange_query::plan::{QueryResult, Value};

use super::handlers::AppState;
use super::response::ErrorResponse;

// ── Connection test ───────────────────────────────────────────────────

/// `GET /api/v1/grafana/` — Grafana uses this to test the data source connection.
pub async fn connection_test() -> impl IntoResponse {
    StatusCode::OK
}

// ── Search ────────────────────────────────────────────────────────────

/// `POST /api/v1/grafana/search` — return available metrics (table names).
#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    #[serde(default)]
    pub target: String,
}

pub async fn search(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SearchRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let db_root = state.db_root.clone();
    let filter = req.target.to_lowercase();

    let tables = tokio::task::spawn_blocking(move || -> Result<Vec<String>, String> {
        let mut result = Vec::new();
        for entry in std::fs::read_dir(&db_root).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();
            if path.is_dir() && path.join("_meta").exists() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with('_') {
                    continue;
                }
                if filter.is_empty() || name.to_lowercase().contains(&filter) {
                    result.push(name);
                }
            }
        }
        result.sort();
        Ok(result)
    })
    .await
    .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(tables))
}

// ── Query ─────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrafanaQueryRequest {
    pub range: Option<GrafanaTimeRange>,
    pub targets: Vec<GrafanaTarget>,
    #[serde(default)]
    pub max_data_points: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct GrafanaTimeRange {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GrafanaTarget {
    pub target: String,
    #[serde(default = "default_target_type")]
    pub r#type: String,
}

fn default_target_type() -> String {
    "timeserie".to_string()
}

#[derive(Debug, Serialize)]
pub struct GrafanaColumn {
    pub text: String,
    #[serde(rename = "type")]
    pub col_type: &'static str,
}

/// `POST /api/v1/grafana/query` — execute queries and return Grafana-formatted results.
///
/// If `target` contains a space, it's treated as raw SQL. Otherwise it's a table name
/// and an auto-generated time-series query is built.
pub async fn query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<GrafanaQueryRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let mut results: Vec<serde_json::Value> = Vec::new();

    for target in &req.targets {
        let sql = if target.target.contains(' ') {
            target.target.clone()
        } else {
            build_auto_query(&target.target, req.range.as_ref())
        };

        let db_root = state.db_root.clone();
        let target_type = target.r#type.clone();
        let target_name = target.target.clone();

        let query_result = tokio::task::spawn_blocking(move || {
            let plan = exchange_query::plan_query(&sql)?;
            exchange_query::execute(&db_root, &plan)
        })
        .await
        .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(|e| ErrorResponse::new(StatusCode::BAD_REQUEST, e.to_string()))?;

        match query_result {
            QueryResult::Rows { columns, rows } => {
                if target_type == "table" {
                    results.push(build_table_response(&columns, &rows));
                } else {
                    results.extend(build_timeseries_response(&target_name, &columns, &rows));
                }
            }
            QueryResult::Ok { .. } => {}
        }
    }

    Ok(Json(results))
}

fn build_auto_query(table_name: &str, range: Option<&GrafanaTimeRange>) -> String {
    // Sanitize: only allow alphanumeric + underscore in table names
    let safe_table: String = table_name
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    let mut sql = format!("SELECT * FROM {safe_table}");

    if let Some(range) = range {
        let from = range.from.replace('\'', "");
        let to = range.to.replace('\'', "");
        sql.push_str(&format!(
            " WHERE timestamp >= '{from}' AND timestamp <= '{to}'"
        ));
    }

    sql.push_str(" ORDER BY timestamp LIMIT 10000");
    sql
}

fn build_table_response(columns: &[String], rows: &[Vec<Value>]) -> serde_json::Value {
    let grafana_columns: Vec<GrafanaColumn> = columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            // Infer type from first non-null value in this column
            let col_type = rows
                .iter()
                .find_map(|row| {
                    row.get(i).and_then(|v| match v {
                        Value::Null => None,
                        Value::Timestamp(_) => Some("time"),
                        Value::I64(_) | Value::F64(_) => Some("number"),
                        Value::Str(_) => Some("string"),
                    })
                })
                .unwrap_or("string");

            GrafanaColumn {
                text: name.clone(),
                col_type,
            }
        })
        .collect();

    let grafana_rows: Vec<Vec<serde_json::Value>> = rows
        .iter()
        .map(|row| row.iter().map(value_to_json).collect())
        .collect();

    serde_json::json!({
        "type": "table",
        "columns": grafana_columns,
        "rows": grafana_rows,
    })
}

fn build_timeseries_response(
    target_name: &str,
    columns: &[String],
    rows: &[Vec<Value>],
) -> Vec<serde_json::Value> {
    if columns.is_empty() || rows.is_empty() {
        return vec![];
    }

    // Find the timestamp column (first Timestamp value, or column named "timestamp")
    let ts_idx = rows
        .first()
        .and_then(|row| row.iter().position(|v| matches!(v, Value::Timestamp(_))))
        .or_else(|| {
            columns
                .iter()
                .position(|c| c.eq_ignore_ascii_case("timestamp"))
        })
        .unwrap_or(0);

    // Find numeric columns
    let numeric_cols: Vec<usize> = (0..columns.len())
        .filter(|&i| {
            i != ts_idx
                && rows
                    .iter()
                    .any(|row| matches!(row.get(i), Some(Value::I64(_)) | Some(Value::F64(_))))
        })
        .collect();

    if numeric_cols.is_empty() {
        return vec![];
    }

    numeric_cols
        .iter()
        .map(|&col_idx| {
            let series_name = if numeric_cols.len() == 1 {
                target_name.to_string()
            } else {
                format!("{}.{}", target_name, columns[col_idx])
            };

            let datapoints: Vec<[serde_json::Value; 2]> = rows
                .iter()
                .filter_map(|row| {
                    let ts_ms = value_to_epoch_ms(row.get(ts_idx)?)?;
                    let val = value_to_json(row.get(col_idx)?);
                    Some([val, serde_json::json!(ts_ms)])
                })
                .collect();

            serde_json::json!({
                "target": series_name,
                "datapoints": datapoints,
            })
        })
        .collect()
}

fn value_to_epoch_ms(v: &Value) -> Option<i64> {
    match v {
        Value::Timestamp(nanos) => Some(nanos / 1_000_000),
        Value::I64(n) => Some(n / 1_000_000),
        Value::F64(f) => Some(*f as i64),
        _ => None,
    }
}

fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::I64(n) => serde_json::json!(n),
        Value::F64(f) => serde_json::json!(f),
        Value::Str(s) => serde_json::json!(s),
        Value::Timestamp(nanos) => serde_json::json!(nanos / 1_000_000),
    }
}

// ── Annotations ───────────────────────────────────────────────────────

pub async fn annotations() -> impl IntoResponse {
    Json(serde_json::json!([]))
}

// ── Tag keys/values ───────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct TagKeysRequest {
    #[serde(default)]
    pub target: Option<String>,
}

pub async fn tag_keys(
    State(state): State<Arc<AppState>>,
    Json(req): Json<TagKeysRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let table_name = match req.target {
        Some(t) if !t.is_empty() => t,
        _ => return Ok(Json(serde_json::json!([]))),
    };

    let db_root = state.db_root.clone();
    let keys = tokio::task::spawn_blocking(move || -> Result<Vec<serde_json::Value>, String> {
        let table_dir = db_root.join(&table_name);
        let meta_path = table_dir.join("_meta");
        if !meta_path.exists() {
            return Ok(vec![]);
        }
        let meta = exchange_core::table::TableMeta::load(&meta_path).map_err(|e| e.to_string())?;
        Ok(meta
            .columns
            .iter()
            .map(|c| serde_json::json!({"type": "string", "text": c.name}))
            .collect())
    })
    .await
    .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e))?;

    Ok(Json(serde_json::json!(keys)))
}

#[derive(Debug, Deserialize)]
pub struct TagValuesRequest {
    pub key: String,
    #[serde(default)]
    pub target: Option<String>,
}

pub async fn tag_values(
    State(state): State<Arc<AppState>>,
    Json(req): Json<TagValuesRequest>,
) -> Result<impl IntoResponse, ErrorResponse> {
    let table_name = match req.target {
        Some(t) if !t.is_empty() => t,
        _ => return Ok(Json(serde_json::json!([]))),
    };

    let safe_table = table_name.replace('\'', "");
    let safe_key = req.key.replace('\'', "");
    let sql = format!("SELECT DISTINCT \"{safe_key}\" FROM \"{safe_table}\" LIMIT 100");

    let db_root = state.db_root.clone();
    let result = tokio::task::spawn_blocking(move || {
        let plan = exchange_query::plan_query(&sql)?;
        exchange_query::execute(&db_root, &plan)
    })
    .await
    .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    .map_err(|e| ErrorResponse::new(StatusCode::BAD_REQUEST, e.to_string()))?;

    let values: Vec<serde_json::Value> = match result {
        QueryResult::Rows { rows, .. } => rows
            .iter()
            .filter_map(|row| {
                row.first()
                    .map(|v| serde_json::json!({"text": value_to_json(v)}))
            })
            .collect(),
        _ => vec![],
    };

    Ok(Json(serde_json::json!(values)))
}

// ── Router ────────────────────────────────────────────────────────────

pub fn grafana_router() -> axum::Router<Arc<AppState>> {
    use axum::routing::{get, post};

    axum::Router::new()
        .route("/", get(connection_test))
        .route("/search", post(search))
        .route("/query", post(query))
        .route("/annotations", post(annotations))
        .route("/tag-keys", post(tag_keys))
        .route("/tag-values", post(tag_values))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_auto_query_no_range() {
        let sql = build_auto_query("trades", None);
        assert!(sql.contains("SELECT * FROM trades"));
        assert!(sql.contains("ORDER BY timestamp"));
        assert!(sql.contains("LIMIT 10000"));
    }

    #[test]
    fn test_build_auto_query_with_range() {
        let range = GrafanaTimeRange {
            from: "2024-01-01T00:00:00Z".to_string(),
            to: "2024-01-02T00:00:00Z".to_string(),
        };
        let sql = build_auto_query("trades", Some(&range));
        assert!(sql.contains("WHERE timestamp >= '2024-01-01T00:00:00Z'"));
        assert!(sql.contains("AND timestamp <= '2024-01-02T00:00:00Z'"));
    }

    #[test]
    fn test_build_auto_query_sanitizes_dangerous_chars() {
        let sql = build_auto_query("trades;DROP TABLE", None);
        assert!(!sql.contains(";"));
        assert!(!sql.contains("DROP TABLE"));
        assert!(sql.contains("tradesDROPTABLE"));
    }

    #[test]
    fn test_value_to_epoch_ms() {
        let v = Value::Timestamp(1_700_000_000_000_000_000);
        assert_eq!(value_to_epoch_ms(&v), Some(1_700_000_000_000));
    }

    #[test]
    fn test_value_to_json_variants() {
        assert_eq!(value_to_json(&Value::Null), serde_json::Value::Null);
        assert_eq!(value_to_json(&Value::I64(42)), serde_json::json!(42));
        assert_eq!(value_to_json(&Value::F64(3.14)), serde_json::json!(3.14));
        assert_eq!(
            value_to_json(&Value::Str("hello".to_string())),
            serde_json::json!("hello")
        );
    }

    #[test]
    fn test_default_target_type() {
        assert_eq!(default_target_type(), "timeserie");
    }

    #[test]
    fn test_build_table_response() {
        let columns = vec!["ts".to_string(), "price".to_string()];
        let rows = vec![
            vec![
                Value::Timestamp(1_700_000_000_000_000_000),
                Value::F64(100.0),
            ],
            vec![
                Value::Timestamp(1_700_000_001_000_000_000),
                Value::F64(101.0),
            ],
        ];
        let resp = build_table_response(&columns, &rows);
        assert_eq!(resp["type"], "table");
        assert_eq!(resp["columns"][0]["text"], "ts");
        assert_eq!(resp["columns"][0]["type"], "time");
        assert_eq!(resp["columns"][1]["text"], "price");
        assert_eq!(resp["columns"][1]["type"], "number");
        assert_eq!(resp["rows"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_build_timeseries_response() {
        let columns = vec!["ts".to_string(), "price".to_string(), "volume".to_string()];
        let rows = vec![
            vec![
                Value::Timestamp(1_700_000_000_000_000_000),
                Value::F64(100.0),
                Value::I64(500),
            ],
            vec![
                Value::Timestamp(1_700_000_001_000_000_000),
                Value::F64(101.0),
                Value::I64(600),
            ],
        ];
        let resp = build_timeseries_response("trades", &columns, &rows);
        assert_eq!(resp.len(), 2); // price + volume series
        assert_eq!(resp[0]["target"], "trades.price");
        assert_eq!(resp[1]["target"], "trades.volume");

        let dp = resp[0]["datapoints"].as_array().unwrap();
        assert_eq!(dp.len(), 2);
        // datapoints format: [value, timestamp_ms]
        assert_eq!(dp[0][0], serde_json::json!(100.0));
        assert_eq!(dp[0][1], serde_json::json!(1_700_000_000_000_i64));
    }

    #[test]
    fn test_build_timeseries_single_numeric_uses_target_name() {
        let columns = vec!["ts".to_string(), "price".to_string()];
        let rows = vec![vec![
            Value::Timestamp(1_700_000_000_000_000_000),
            Value::F64(100.0),
        ]];
        let resp = build_timeseries_response("trades", &columns, &rows);
        assert_eq!(resp.len(), 1);
        assert_eq!(resp[0]["target"], "trades"); // not "trades.price"
    }

    #[test]
    fn test_build_timeseries_empty() {
        let resp = build_timeseries_response("trades", &[], &[]);
        assert!(resp.is_empty());
    }
}
