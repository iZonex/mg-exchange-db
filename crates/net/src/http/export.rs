use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;

use exchange_query::plan::Value;
use exchange_query::{execute, plan_query};

use super::handlers::AppState;
use super::response::ErrorResponse;

#[derive(Debug, Deserialize)]
pub struct ExportParams {
    pub query: String,
    #[serde(default = "default_format")]
    pub format: String,
}

fn default_format() -> String {
    "csv".to_string()
}

/// Escape a CSV field: quote it if it contains commas, quotes, or newlines.
fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        let escaped = value.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        value.to_string()
    }
}

fn value_to_csv_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::I64(n) => n.to_string(),
        Value::F64(n) => n.to_string(),
        Value::Str(s) => csv_escape(s),
        Value::Timestamp(ns) => ns.to_string(),
    }
}

/// `GET /api/v1/export?query=SELECT...&format=csv`
///
/// Executes a SQL query and returns results as CSV.
pub async fn export_csv(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ExportParams>,
) -> Result<impl IntoResponse, ErrorResponse> {
    if params.query.trim().is_empty() {
        return Err(ErrorResponse::new(
            StatusCode::BAD_REQUEST,
            "query parameter must not be empty",
        ));
    }

    if params.format != "csv" {
        return Err(ErrorResponse::new(
            StatusCode::BAD_REQUEST,
            format!(
                "unsupported export format: '{}'. Only 'csv' is supported.",
                params.format
            ),
        ));
    }

    let plan = plan_query(&params.query)
        .map_err(|e| ErrorResponse::new(StatusCode::BAD_REQUEST, e.to_string()))?;

    let db_root = state.db_root.clone();
    let result = tokio::task::spawn_blocking(move || execute(&db_root, &plan))
        .await
        .map_err(|e| {
            ErrorResponse::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("task join error: {e}"),
            )
        })?
        .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    match result {
        exchange_query::QueryResult::Rows { columns, rows } => {
            let mut csv = String::new();

            // Header row
            let header: Vec<String> = columns.iter().map(|c| csv_escape(c)).collect();
            csv.push_str(&header.join(","));
            csv.push('\n');

            // Data rows
            for row in &rows {
                let fields: Vec<String> = row.iter().map(value_to_csv_string).collect();
                csv.push_str(&fields.join(","));
                csv.push('\n');
            }

            Ok((
                StatusCode::OK,
                [
                    ("content-type", "text/csv"),
                    ("content-disposition", "attachment; filename=\"export.csv\""),
                ],
                csv,
            ))
        }
        exchange_query::QueryResult::Ok { affected_rows } => {
            let csv = format!("affected_rows\n{affected_rows}\n");
            Ok((
                StatusCode::OK,
                [
                    ("content-type", "text/csv"),
                    ("content-disposition", "attachment; filename=\"export.csv\""),
                ],
                csv,
            ))
        }
    }
}

/// `POST /api/v1/import?table=trades`
///
/// Accepts a CSV body and imports it into the specified table.
pub async fn import_csv(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ImportParams>,
    body: String,
) -> Result<impl IntoResponse, ErrorResponse> {
    if params.table.trim().is_empty() {
        return Err(ErrorResponse::new(
            StatusCode::BAD_REQUEST,
            "table parameter must not be empty",
        ));
    }

    if body.trim().is_empty() {
        return Err(ErrorResponse::new(
            StatusCode::BAD_REQUEST,
            "CSV body must not be empty",
        ));
    }

    let table_name = params.table.clone();
    let db_root = state.db_root.clone();

    let rows_imported =
        tokio::task::spawn_blocking(move || import_csv_impl(&db_root, &table_name, &body))
            .await
            .map_err(|e| {
                ErrorResponse::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("task join error: {e}"),
                )
            })?
            .map_err(|e| ErrorResponse::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::OK,
        axum::Json(serde_json::json!({
            "rows_imported": rows_imported,
            "table": params.table,
        })),
    ))
}

#[derive(Debug, Deserialize)]
pub struct ImportParams {
    pub table: String,
}

/// Parse a CSV line respecting quoted fields.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    // Escaped quote
                    current.push('"');
                    chars.next();
                } else {
                    // End of quoted field
                    in_quotes = false;
                }
            } else {
                current.push(ch);
            }
        } else if ch == '"' {
            in_quotes = true;
        } else if ch == ',' {
            fields.push(current.trim().to_string());
            current = String::new();
        } else {
            current.push(ch);
        }
    }
    fields.push(current.trim().to_string());
    fields
}

fn import_csv_impl(
    db_root: &std::path::Path,
    table_name: &str,
    csv_body: &str,
) -> std::result::Result<u64, String> {
    use exchange_common::types::{ColumnType, Timestamp};
    use exchange_core::table::{ColumnValue, TableBuilder, TableWriter};

    let mut lines = csv_body.lines();

    // Parse header
    let header_line = lines.next().ok_or("CSV body is empty")?;
    let headers = parse_csv_line(header_line);
    if headers.is_empty() || (headers.len() == 1 && headers[0].is_empty()) {
        return Err("CSV header is empty".to_string());
    }

    // Collect all data rows
    let mut raw_rows: Vec<Vec<String>> = Vec::new();
    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        raw_rows.push(parse_csv_line(line));
    }

    if raw_rows.is_empty() {
        return Err("CSV body contains no data rows".to_string());
    }

    // Auto-detect column types
    let col_types: Vec<ColumnType> = (0..headers.len())
        .map(|col_idx| detect_column_type(&raw_rows, col_idx))
        .collect();

    // Create table if not exists
    let table_dir = db_root.join(table_name);
    if !table_dir.join("_meta").exists() {
        let mut builder = TableBuilder::new(table_name);
        let has_timestamp = col_types.iter().any(|t| matches!(t, ColumnType::Timestamp));

        for (i, header) in headers.iter().enumerate() {
            builder = builder.column(header, col_types[i]);
        }

        if has_timestamp {
            let ts_name = headers
                .iter()
                .zip(col_types.iter())
                .find(|(_, t)| matches!(t, ColumnType::Timestamp))
                .map(|(n, _)| n.as_str())
                .unwrap();
            builder = builder.timestamp(ts_name);
        }

        builder.build(db_root).map_err(|e| e.to_string())?;
    }

    // Write rows
    let mut writer = TableWriter::open(db_root, table_name).map_err(|e| e.to_string())?;
    let meta = writer.meta().clone();
    let ts_col_idx = meta.timestamp_column;

    let mut count = 0u64;
    for raw_row in &raw_rows {
        let ts = if ts_col_idx < raw_row.len() {
            parse_timestamp_value(&raw_row[ts_col_idx])
        } else {
            Timestamp::now()
        };

        let col_values: Vec<ColumnValue<'_>> = raw_row
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != ts_col_idx)
            .map(|(orig_idx, field)| {
                let ct: ColumnType = if orig_idx < meta.columns.len() {
                    meta.columns[orig_idx].col_type.into()
                } else {
                    ColumnType::Varchar
                };
                parse_field_to_column_value(field, ct)
            })
            .collect();

        writer
            .write_row(ts, &col_values)
            .map_err(|e| e.to_string())?;
        count += 1;
    }

    writer.flush().map_err(|e| e.to_string())?;
    Ok(count)
}

/// Detect column type by sampling values.
fn detect_column_type(rows: &[Vec<String>], col_idx: usize) -> exchange_common::types::ColumnType {
    use exchange_common::types::ColumnType;

    let mut all_int = true;
    let mut all_float = true;
    let mut all_timestamp = true;

    for row in rows {
        let field = match row.get(col_idx) {
            Some(f) => f.trim(),
            None => continue,
        };
        if field.is_empty() {
            continue;
        }

        if field.parse::<i64>().is_err() {
            all_int = false;
        }
        if field.parse::<f64>().is_err() {
            all_float = false;
        }
        if !(field.contains('-') && (field.contains('T') || field.contains(':')))
            && !(all_int && field.len() > 15)
        {
            all_timestamp = false;
        }
    }

    if all_timestamp {
        ColumnType::Timestamp
    } else if all_int {
        ColumnType::I64
    } else if all_float {
        ColumnType::F64
    } else {
        ColumnType::Varchar
    }
}

fn parse_timestamp_value(s: &str) -> exchange_common::types::Timestamp {
    use exchange_common::types::Timestamp;
    let s = s.trim();
    if let Ok(ns) = s.parse::<i64>() {
        return Timestamp(ns);
    }
    Timestamp::now()
}

fn parse_field_to_column_value(
    field: &str,
    ct: exchange_common::types::ColumnType,
) -> exchange_core::table::ColumnValue<'_> {
    use exchange_common::types::ColumnType;
    use exchange_core::table::ColumnValue;

    let field = field.trim();
    match ct {
        ColumnType::I64 | ColumnType::I32 | ColumnType::I16 | ColumnType::I8 => {
            if let Ok(v) = field.parse::<i64>() {
                ColumnValue::I64(v)
            } else {
                ColumnValue::I64(0)
            }
        }
        ColumnType::F64 | ColumnType::F32 => {
            if let Ok(v) = field.parse::<f64>() {
                ColumnValue::F64(v)
            } else {
                ColumnValue::F64(0.0)
            }
        }
        ColumnType::Timestamp => {
            let ts = parse_timestamp_value(field);
            ColumnValue::Timestamp(ts)
        }
        ColumnType::Symbol => ColumnValue::I32(0),
        _ => ColumnValue::Str(field),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_escape_plain() {
        assert_eq!(csv_escape("hello"), "hello");
    }

    #[test]
    fn test_csv_escape_with_comma() {
        assert_eq!(csv_escape("hello,world"), "\"hello,world\"");
    }

    #[test]
    fn test_csv_escape_with_quotes() {
        assert_eq!(csv_escape("say \"hi\""), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn test_csv_escape_with_newline() {
        assert_eq!(csv_escape("line1\nline2"), "\"line1\nline2\"");
    }

    #[test]
    fn test_parse_csv_line_simple() {
        let fields = parse_csv_line("a,b,c");
        assert_eq!(fields, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_csv_line_quoted() {
        let fields = parse_csv_line("\"hello,world\",b,c");
        assert_eq!(fields, vec!["hello,world", "b", "c"]);
    }

    #[test]
    fn test_parse_csv_line_escaped_quotes() {
        let fields = parse_csv_line("\"say \"\"hi\"\"\",b");
        assert_eq!(fields, vec!["say \"hi\"", "b"]);
    }

    #[test]
    fn test_value_to_csv_string() {
        assert_eq!(value_to_csv_string(&Value::Null), "");
        assert_eq!(value_to_csv_string(&Value::I64(42)), "42");
        assert_eq!(value_to_csv_string(&Value::F64(3.14)), "3.14");
        assert_eq!(
            value_to_csv_string(&Value::Str("hello".to_string())),
            "hello"
        );
        assert_eq!(
            value_to_csv_string(&Value::Str("a,b".to_string())),
            "\"a,b\""
        );
    }

    #[test]
    fn test_import_csv_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path();

        // Import CSV data
        let csv = "timestamp,price,volume\n1710513000000000000,65000.5,1.5\n1710513001000000000,65001.0,2.0\n";
        let rows = import_csv_impl(db_root, "test_trades", csv).unwrap();
        assert_eq!(rows, 2);

        // Verify table was created
        assert!(db_root.join("test_trades").join("_meta").exists());
    }

    #[test]
    fn test_import_csv_creates_table_with_correct_types() {
        let dir = tempfile::tempdir().unwrap();
        let db_root = dir.path();

        let csv = "name,value,count\nalpha,1.5,10\nbeta,2.5,20\n";
        let rows = import_csv_impl(db_root, "test_types", csv).unwrap();
        assert_eq!(rows, 2);

        use exchange_core::table::TableMeta;
        let meta = TableMeta::load(&db_root.join("test_types").join("_meta")).unwrap();
        assert_eq!(meta.columns.len(), 3);
    }

    #[test]
    fn test_import_csv_empty_body_error() {
        let dir = tempfile::tempdir().unwrap();
        let result = import_csv_impl(dir.path(), "t", "");
        assert!(result.is_err());
    }

    #[test]
    fn test_import_csv_header_only_error() {
        let dir = tempfile::tempdir().unwrap();
        let result = import_csv_impl(dir.path(), "t", "a,b,c\n");
        assert!(result.is_err());
    }

    #[test]
    fn test_detect_column_type_int() {
        let rows = vec![
            vec!["10".to_string()],
            vec!["20".to_string()],
            vec!["30".to_string()],
        ];
        let ct = detect_column_type(&rows, 0);
        assert!(matches!(ct, exchange_common::types::ColumnType::I64));
    }

    #[test]
    fn test_detect_column_type_float() {
        let rows = vec![vec!["1.5".to_string()], vec!["2.5".to_string()]];
        let ct = detect_column_type(&rows, 0);
        assert!(matches!(ct, exchange_common::types::ColumnType::F64));
    }

    #[test]
    fn test_detect_column_type_varchar() {
        let rows = vec![vec!["hello".to_string()], vec!["world".to_string()]];
        let ct = detect_column_type(&rows, 0);
        assert!(matches!(ct, exchange_common::types::ColumnType::Varchar));
    }
}
