use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

/// Column metadata returned in query responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub r#type: String,
}

/// Response body for `POST /api/v1/query`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub columns: Vec<ColumnInfo>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub timing_ms: f64,
}

/// Generic error response with optional SQLSTATE code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: u16,
    /// PostgreSQL-compatible SQLSTATE error code.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_state: Option<String>,
    /// The query that caused the error, if available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
}

impl ErrorResponse {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        let msg = message.into();
        let sql_state = Self::infer_sql_state(status, &msg);
        Self {
            error: msg,
            code: status.as_u16(),
            sql_state,
            query: None,
        }
    }

    /// Create an error response with the query that caused it.
    pub fn with_query(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    /// Map HTTP status + error message to a SQLSTATE code.
    fn infer_sql_state(status: StatusCode, message: &str) -> Option<String> {
        let lower = message.to_ascii_lowercase();
        // Check specific error patterns first.
        if lower.contains("table")
            && (lower.contains("not found") || lower.contains("does not exist"))
        {
            return Some("42P01".to_string()); // undefined_table
        }
        if lower.contains("column")
            && (lower.contains("not found") || lower.contains("does not exist"))
        {
            return Some("42703".to_string()); // undefined_column
        }
        if lower.contains("already exists") {
            return Some("42P07".to_string()); // duplicate_table
        }
        if lower.contains("syntax") || lower.contains("parse") {
            return Some("42601".to_string()); // syntax_error
        }
        if lower.contains("type mismatch") || lower.contains("type error") {
            return Some("42804".to_string()); // datatype_mismatch
        }
        if lower.contains("permission") || lower.contains("denied") {
            return Some("42501".to_string()); // insufficient_privilege
        }
        // Fall back to status-based mapping.
        match status {
            StatusCode::BAD_REQUEST => Some("42000".to_string()), // syntax_error_or_access_rule_violation
            StatusCode::NOT_FOUND => Some("42P01".to_string()),   // undefined_table
            StatusCode::CONFLICT => Some("42P07".to_string()),    // duplicate_table
            StatusCode::FORBIDDEN => Some("42501".to_string()),   // insufficient_privilege
            StatusCode::TOO_MANY_REQUESTS => Some("53300".to_string()), // too_many_connections
            StatusCode::INTERNAL_SERVER_ERROR => Some("XX000".to_string()), // internal_error
            _ => None,
        }
    }
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body = serde_json::to_string(&self).unwrap_or_else(|_| {
            r#"{"error":"internal serialization error","code":500}"#.to_string()
        });
        (status, [("content-type", "application/json")], body).into_response()
    }
}

/// Response body for `GET /api/v1/health`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_secs: f64,
    /// Replication status (present when replication is configured).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication: Option<exchange_core::replication::ReplicationStatus>,
    /// Individual health check results (present when HealthChecker is used).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checks: Option<Vec<HealthCheckInfo>>,
}

/// Individual health check result returned in the health endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckInfo {
    pub name: String,
    pub status: String,
    pub message: String,
    pub duration_ms: u64,
}

/// Response body for `GET /api/v1/tables`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TablesListResponse {
    pub tables: Vec<String>,
}

/// Response body for `GET /api/v1/tables/:name`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfoResponse {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub partition_by: String,
    pub row_count: u64,
}

/// Response for ILP write ingestion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResponse {
    pub status: String,
    pub lines_accepted: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_response_serialization() {
        let resp = QueryResponse {
            columns: vec![
                ColumnInfo {
                    name: "time".to_string(),
                    r#type: "Timestamp".to_string(),
                },
                ColumnInfo {
                    name: "value".to_string(),
                    r#type: "F64".to_string(),
                },
            ],
            rows: vec![vec![
                serde_json::Value::String("2024-01-01T00:00:00Z".to_string()),
                serde_json::json!(42.5),
            ]],
            timing_ms: 1.23,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: QueryResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.columns.len(), 2);
        assert_eq!(deserialized.rows.len(), 1);
        assert!((deserialized.timing_ms - 1.23).abs() < f64::EPSILON);
    }

    #[test]
    fn test_error_response_serialization() {
        let resp = ErrorResponse::new(StatusCode::NOT_FOUND, "table not found");
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("table not found"));
        assert!(json.contains("404"));
    }

    #[test]
    fn test_health_response_serialization() {
        let resp = HealthResponse {
            status: "ok".to_string(),
            version: "0.1.0".to_string(),
            uptime_secs: 123.45,
            replication: None,
            checks: None,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: HealthResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.status, "ok");
        assert_eq!(deserialized.version, "0.1.0");
    }

    #[test]
    fn test_write_response_serialization() {
        let resp = WriteResponse {
            status: "ok".to_string(),
            lines_accepted: 5,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: WriteResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.lines_accepted, 5);
    }

    #[test]
    fn test_table_info_serialization() {
        let resp = TableInfoResponse {
            name: "trades".to_string(),
            columns: vec![ColumnInfo {
                name: "price".to_string(),
                r#type: "F64".to_string(),
            }],
            partition_by: "Day".to_string(),
            row_count: 1_000_000,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deserialized: TableInfoResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.name, "trades");
        assert_eq!(deserialized.row_count, 1_000_000);
    }
}
