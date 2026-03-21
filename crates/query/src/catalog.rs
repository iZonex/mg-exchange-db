//! Virtual system catalog tables for PostgreSQL client compatibility.
//!
//! Many PostgreSQL clients (psql, DBeaver, DataGrip) query `pg_catalog.*` and
//! `information_schema.*` tables on connect. This module provides virtual
//! implementations so those queries succeed.

use std::path::Path;

use crate::plan::{QueryResult, SelectColumn, Value};
use exchange_common::error::{ExchangeDbError, Result};
use exchange_core::table::TableMeta;

// ── Well-known PostgreSQL OIDs ──────────────────────────────────────────

const OID_BOOL: i64 = 16;
const OID_INT8: i64 = 20;
const OID_INT4: i64 = 23;
const OID_TEXT: i64 = 25;
const OID_FLOAT4: i64 = 700;
const OID_FLOAT8: i64 = 701;
const OID_TIMESTAMP: i64 = 1184;
const OID_VARCHAR: i64 = 1043;

const NS_PG_CATALOG: i64 = 11;
const NS_PUBLIC: i64 = 2200;

// ── Public API ──────────────────────────────────────────────────────────

/// Returns `true` if `table` is a recognized system catalog or
/// information_schema table that we handle virtually.
pub fn is_catalog_query(table: &str) -> bool {
    let t = normalize_catalog_name(table);
    matches!(
        t.as_str(),
        "pg_type"
            | "pg_class"
            | "pg_namespace"
            | "pg_attribute"
            | "pg_database"
            | "pg_settings"
            | "pg_roles"
            | "pg_stat_activity"
            | "pg_indexes"
            | "pg_views"
            | "pg_matviews"
            | "pg_tables"
            | "pg_description"
            | "pg_am"
            | "pg_operator"
            | "pg_proc"
            | "pg_constraint"
            | "pg_depend"
            | "pg_enum"
            | "pg_collation"
            | "pg_stat_user_tables"
            | "pg_stat_user_indexes"
            | "pg_statio_user_tables"
            | "pg_locks"
            | "pg_prepared_statements"
            | "pg_cursors"
            | "pg_available_extensions"
            | "pg_timezone_names"
            | "pg_timezone_abbrevs"
            | "pg_file_settings"
            | "pg_hba_file_rules"
            | "pg_replication_slots"
            | "pg_publication"
            | "pg_subscription"
            | "pg_sequences"
            | "pg_largeobject"
            | "pg_shdescription"
            | "pg_seclabel"
            | "pg_event_trigger"
            | "pg_trigger"
            | "pg_cast"
            | "pg_conversion"
            | "pg_opclass"
            | "pg_opfamily"
            | "pg_aggregate"
            | "pg_language"
            | "pg_foreign_server"
            | "pg_foreign_table"
            | "pg_user_mapping"
            | "information_schema.tables"
            | "information_schema.columns"
    )
}

/// Returns `true` if this looks like a query with no FROM clause that uses
/// a system function such as `version()`, `current_database()`, or
/// `current_schema()`.
pub fn is_system_function_query(columns: &[SelectColumn]) -> bool {
    if columns.len() != 1 {
        return false;
    }
    match &columns[0] {
        SelectColumn::ScalarFunction { name, .. } => {
            matches!(
                name.as_str(),
                "version" | "current_database" | "current_schema" | "current_schemas"
            )
        }
        _ => false,
    }
}

/// Execute a system function query (no FROM clause).
pub fn execute_system_function(columns: &[SelectColumn]) -> Result<QueryResult> {
    if columns.len() != 1 {
        return Err(ExchangeDbError::Query(
            "expected exactly one system function".into(),
        ));
    }
    match &columns[0] {
        SelectColumn::ScalarFunction { name, .. } => {
            let val = match name.as_str() {
                "version" => Value::Str("ExchangeDB 0.1.0 (PostgreSQL compatible)".into()),
                "current_database" => Value::Str("exchangedb".into()),
                "current_schema" => Value::Str("public".into()),
                "current_schemas" => Value::Str("{pg_catalog,public}".into()),
                other => {
                    return Err(ExchangeDbError::Query(format!(
                        "unknown system function: {other}"
                    )))
                }
            };
            Ok(QueryResult::Rows {
                columns: vec![name.clone()],
                rows: vec![vec![val]],
            })
        }
        _ => Err(ExchangeDbError::Query(
            "expected a system function call".into(),
        )),
    }
}

/// Execute a catalog query, returning computed results for the given
/// system table.
pub fn execute_catalog_query(
    db_root: &Path,
    table: &str,
    columns: &[SelectColumn],
) -> Result<QueryResult> {
    let t = normalize_catalog_name(table);
    match t.as_str() {
        "pg_type" => catalog_pg_type(columns),
        "pg_class" => catalog_pg_class(db_root, columns),
        "pg_namespace" => catalog_pg_namespace(columns),
        "pg_attribute" => catalog_pg_attribute(db_root, columns),
        "pg_database" => catalog_pg_database(columns),
        "information_schema.tables" => catalog_info_tables(db_root, columns),
        "information_schema.columns" => catalog_info_columns(db_root, columns),
        "pg_settings" => catalog_pg_settings(columns),
        "pg_roles" => catalog_pg_roles(columns),
        "pg_stat_activity" => catalog_pg_stat_activity(columns),
        "pg_indexes" => catalog_pg_indexes(db_root, columns),
        "pg_views" => catalog_pg_views(columns),
        "pg_matviews" => catalog_pg_matviews(db_root, columns),
        "pg_tables" => catalog_pg_tables(db_root, columns),
        "pg_description" => catalog_pg_description(columns),
        "pg_am" => catalog_pg_am(columns),
        "pg_operator" => catalog_pg_operator(columns),
        "pg_proc" => catalog_pg_proc(columns),
        "pg_constraint" => catalog_pg_constraint(columns),
        "pg_depend" => catalog_pg_depend(columns),
        "pg_enum" => catalog_pg_enum(columns),
        "pg_collation" => catalog_pg_collation(columns),
        "pg_stat_user_tables" => catalog_pg_stat_user_tables(db_root, columns),
        "pg_stat_user_indexes" => catalog_pg_stat_user_indexes(db_root, columns),
        "pg_statio_user_tables" => catalog_pg_statio_user_tables(db_root, columns),
        "pg_locks" => catalog_pg_locks(columns),
        "pg_prepared_statements" => catalog_pg_prepared_statements(columns),
        "pg_cursors" => catalog_pg_cursors(columns),
        "pg_available_extensions" => catalog_pg_available_extensions(columns),
        "pg_timezone_names" => catalog_pg_timezone_names(columns),
        "pg_timezone_abbrevs" => catalog_pg_timezone_abbrevs(columns),
        "pg_file_settings" => catalog_pg_file_settings(columns),
        "pg_hba_file_rules" => catalog_pg_hba_file_rules(columns),
        "pg_replication_slots" => catalog_pg_replication_slots(columns),
        "pg_publication" => catalog_pg_publication(columns),
        "pg_subscription" => catalog_pg_subscription(columns),
        "pg_sequences" => catalog_pg_sequences(columns),
        "pg_largeobject" => catalog_pg_largeobject(columns),
        "pg_shdescription" => catalog_pg_shdescription(columns),
        "pg_seclabel" => catalog_pg_seclabel(columns),
        "pg_event_trigger" => catalog_pg_event_trigger(columns),
        "pg_trigger" => catalog_pg_trigger(columns),
        "pg_cast" => catalog_pg_cast(columns),
        "pg_conversion" => catalog_pg_conversion(columns),
        "pg_opclass" => catalog_pg_opclass(columns),
        "pg_opfamily" => catalog_pg_opfamily(columns),
        "pg_aggregate" => catalog_pg_aggregate(columns),
        "pg_language" => catalog_pg_language(columns),
        "pg_foreign_server" => catalog_pg_foreign_server(columns),
        "pg_foreign_table" => catalog_pg_foreign_table(columns),
        "pg_user_mapping" => catalog_pg_user_mapping(columns),
        _ => Err(ExchangeDbError::Query(format!(
            "unknown catalog table: {table}"
        ))),
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

/// Strip `pg_catalog.` prefix and lowercase for matching.
fn normalize_catalog_name(table: &str) -> String {
    let t = table.to_ascii_lowercase();
    if let Some(stripped) = t.strip_prefix("pg_catalog.") {
        stripped.to_string()
    } else if t.starts_with("information_schema.") {
        // Keep the prefix for info_schema tables.
        t
    } else {
        t
    }
}

/// List user table names by scanning `db_root` for directories with a `_meta` file.
fn list_user_tables(db_root: &Path) -> Vec<String> {
    let mut names = Vec::new();
    let entries = match std::fs::read_dir(db_root) {
        Ok(e) => e,
        Err(_) => return names,
    };
    for entry in entries.flatten() {
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            let meta_path = entry.path().join("_meta");
            if meta_path.exists() {
                if let Some(name) = entry.file_name().to_str() {
                    names.push(name.to_string());
                }
            }
        }
    }
    names.sort();
    names
}

/// Load `TableMeta` for a given user table.
fn load_meta(db_root: &Path, table_name: &str) -> Option<TableMeta> {
    let meta_path = db_root.join(table_name).join("_meta");
    TableMeta::load(&meta_path).ok()
}

/// Project full rows down to only the requested columns.
/// `all_col_names` are the names of every column in the full row.
/// Returns (projected_column_names, projected_rows).
fn project(
    all_col_names: &[&str],
    all_rows: &[Vec<Value>],
    select_columns: &[SelectColumn],
) -> (Vec<String>, Vec<Vec<Value>>) {
    // Determine which indices to keep.
    let mut indices: Vec<usize> = Vec::new();
    let mut out_names: Vec<String> = Vec::new();

    for sc in select_columns {
        match sc {
            SelectColumn::Wildcard => {
                for (i, name) in all_col_names.iter().enumerate() {
                    indices.push(i);
                    out_names.push(name.to_string());
                }
            }
            SelectColumn::Name(n) => {
                if let Some(i) = all_col_names
                    .iter()
                    .position(|c| c.eq_ignore_ascii_case(n))
                {
                    indices.push(i);
                    out_names.push(n.clone());
                }
            }
            SelectColumn::ScalarFunction { name, .. } => {
                // System functions without a corresponding column – return
                // the function name as column name with NULL value;
                // actual value is computed elsewhere.
                out_names.push(name.clone());
                // We use usize::MAX as sentinel meaning "not found".
                indices.push(usize::MAX);
            }
            _ => {
                // Aggregate / window columns are not expected in catalog
                // queries; ignore silently.
            }
        }
    }

    let projected_rows: Vec<Vec<Value>> = all_rows
        .iter()
        .map(|row| {
            indices
                .iter()
                .map(|&i| {
                    if i < row.len() {
                        row[i].clone()
                    } else {
                        Value::Null
                    }
                })
                .collect()
        })
        .collect();

    (out_names, projected_rows)
}

// ── Catalog implementations ─────────────────────────────────────────────

fn catalog_pg_type(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["oid", "typname", "typnamespace", "typlen"];

    let all_rows = vec![
        vec![Value::I64(OID_BOOL), Value::Str("bool".into()), Value::I64(NS_PG_CATALOG), Value::I64(1)],
        vec![Value::I64(OID_INT8), Value::Str("int8".into()), Value::I64(NS_PG_CATALOG), Value::I64(8)],
        vec![Value::I64(OID_INT4), Value::Str("int4".into()), Value::I64(NS_PG_CATALOG), Value::I64(4)],
        vec![Value::I64(OID_TEXT), Value::Str("text".into()), Value::I64(NS_PG_CATALOG), Value::I64(-1)],
        vec![Value::I64(OID_FLOAT4), Value::Str("float4".into()), Value::I64(NS_PG_CATALOG), Value::I64(4)],
        vec![Value::I64(OID_FLOAT8), Value::Str("float8".into()), Value::I64(NS_PG_CATALOG), Value::I64(8)],
        vec![Value::I64(OID_TIMESTAMP), Value::Str("timestamptz".into()), Value::I64(NS_PG_CATALOG), Value::I64(8)],
        vec![Value::I64(OID_VARCHAR), Value::Str("varchar".into()), Value::I64(NS_PG_CATALOG), Value::I64(-1)],
    ];

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_class(db_root: &Path, columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["oid", "relname", "relnamespace", "relkind"];

    let tables = list_user_tables(db_root);
    let mut all_rows = Vec::new();
    for (i, tbl) in tables.iter().enumerate() {
        all_rows.push(vec![
            Value::I64(16384 + i as i64), // synthetic OID
            Value::Str(tbl.clone()),
            Value::I64(NS_PUBLIC),
            Value::Str("r".into()), // regular table
        ]);
    }

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_namespace(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["oid", "nspname"];

    let all_rows = vec![
        vec![Value::I64(NS_PG_CATALOG), Value::Str("pg_catalog".into())],
        vec![Value::I64(NS_PUBLIC), Value::Str("public".into())],
    ];

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_attribute(db_root: &Path, columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["attrelid", "attname", "attnum", "atttypid", "attnotnull"];

    let tables = list_user_tables(db_root);
    let mut all_rows = Vec::new();
    for (tbl_idx, tbl) in tables.iter().enumerate() {
        let oid = 16384 + tbl_idx as i64;
        if let Some(meta) = load_meta(db_root, tbl) {
            for (col_idx, col_def) in meta.columns.iter().enumerate() {
                let type_oid = col_type_to_pg_oid(&col_def.col_type);
                all_rows.push(vec![
                    Value::I64(oid),
                    Value::Str(col_def.name.clone()),
                    Value::I64(col_idx as i64 + 1),
                    Value::I64(type_oid),
                    Value::I64(0), // attnotnull = false
                ]);
            }
        }
    }

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_database(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["oid", "datname", "encoding", "datcollate"];

    let all_rows = vec![vec![
        Value::I64(1),
        Value::Str("exchangedb".into()),
        Value::I64(6), // UTF8
        Value::Str("en_US.UTF-8".into()),
    ]];

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_info_tables(db_root: &Path, columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "table_catalog",
        "table_schema",
        "table_name",
        "table_type",
    ];

    let tables = list_user_tables(db_root);
    let mut all_rows = Vec::new();
    for tbl in &tables {
        all_rows.push(vec![
            Value::Str("exchangedb".into()),
            Value::Str("public".into()),
            Value::Str(tbl.clone()),
            Value::Str("BASE TABLE".into()),
        ]);
    }

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_info_columns(db_root: &Path, columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "table_catalog",
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "data_type",
        "is_nullable",
    ];

    let tables = list_user_tables(db_root);
    let mut all_rows = Vec::new();
    for tbl in &tables {
        if let Some(meta) = load_meta(db_root, tbl) {
            for (i, col_def) in meta.columns.iter().enumerate() {
                all_rows.push(vec![
                    Value::Str("exchangedb".into()),
                    Value::Str("public".into()),
                    Value::Str(tbl.clone()),
                    Value::Str(col_def.name.clone()),
                    Value::I64(i as i64 + 1),
                    Value::Str(col_type_to_pg_name(&col_def.col_type)),
                    Value::Str("YES".into()),
                ]);
            }
        }
    }

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

// ── Additional pg_catalog tables ─────────────────────────────────────────

fn catalog_pg_settings(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["name", "setting", "unit", "category", "short_desc"];

    let all_rows = vec![
        vec![
            Value::Str("server_version".into()),
            Value::Str("0.1.0".into()),
            Value::Null,
            Value::Str("Preset Options".into()),
            Value::Str("Shows the server version.".into()),
        ],
        vec![
            Value::Str("server_encoding".into()),
            Value::Str("UTF8".into()),
            Value::Null,
            Value::Str("Client Connection Defaults".into()),
            Value::Str("Shows the server encoding.".into()),
        ],
        vec![
            Value::Str("client_encoding".into()),
            Value::Str("UTF8".into()),
            Value::Null,
            Value::Str("Client Connection Defaults".into()),
            Value::Str("Sets the client encoding.".into()),
        ],
        vec![
            Value::Str("standard_conforming_strings".into()),
            Value::Str("on".into()),
            Value::Null,
            Value::Str("Version and Platform Compatibility".into()),
            Value::Str("Causes ... strings to treat backslashes literally.".into()),
        ],
        vec![
            Value::Str("DateStyle".into()),
            Value::Str("ISO, MDY".into()),
            Value::Null,
            Value::Str("Client Connection Defaults".into()),
            Value::Str("Sets the display format for date and time values.".into()),
        ],
        vec![
            Value::Str("TimeZone".into()),
            Value::Str("UTC".into()),
            Value::Null,
            Value::Str("Client Connection Defaults".into()),
            Value::Str("Sets the time zone for displaying timestamps.".into()),
        ],
        vec![
            Value::Str("integer_datetimes".into()),
            Value::Str("on".into()),
            Value::Null,
            Value::Str("Preset Options".into()),
            Value::Str("Datetimes are integer based.".into()),
        ],
        vec![
            Value::Str("max_connections".into()),
            Value::Str("100".into()),
            Value::Null,
            Value::Str("Connections and Authentication".into()),
            Value::Str("Sets the maximum number of concurrent connections.".into()),
        ],
    ];

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_roles(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "oid", "rolname", "rolsuper", "rolinherit", "rolcreaterole",
        "rolcreatedb", "rolcanlogin", "rolreplication",
    ];

    let all_rows = vec![vec![
        Value::I64(10),
        Value::Str("exchangedb".into()),
        Value::I64(1), // true
        Value::I64(1),
        Value::I64(1),
        Value::I64(1),
        Value::I64(1),
        Value::I64(0),
    ]];

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_stat_activity(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "datid", "datname", "pid", "usename", "application_name",
        "client_addr", "state", "query",
    ];

    // Return one row representing the current connection.
    let all_rows = vec![vec![
        Value::I64(1),
        Value::Str("exchangedb".into()),
        Value::I64(std::process::id() as i64),
        Value::Str("exchangedb".into()),
        Value::Str("ExchangeDB".into()),
        Value::Null,
        Value::Str("active".into()),
        Value::Str("SELECT * FROM pg_stat_activity".into()),
    ]];

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_indexes(db_root: &Path, columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["schemaname", "tablename", "indexname", "indexdef"];

    let tables = list_user_tables(db_root);
    let mut all_rows = Vec::new();
    for tbl in &tables {
        if let Some(meta) = load_meta(db_root, tbl) {
            for col_def in &meta.columns {
                if col_def.indexed {
                    let idx_name = format!("{tbl}_{}_idx", col_def.name);
                    let idx_def = format!(
                        "CREATE INDEX {idx_name} ON {tbl} ({})",
                        col_def.name
                    );
                    all_rows.push(vec![
                        Value::Str("public".into()),
                        Value::Str(tbl.clone()),
                        Value::Str(idx_name),
                        Value::Str(idx_def),
                    ]);
                }
            }
        }
    }

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_views(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["schemaname", "viewname", "viewowner", "definition"];
    // ExchangeDB does not support views yet; return empty.
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_matviews(db_root: &Path, columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["schemaname", "matviewname", "matviewowner", "definition"];

    let mut all_rows = Vec::new();
    // Check for materialized view metadata files.
    let matview_dir = db_root.join("_matviews");
    if matview_dir.exists() {
        if let Ok(entries) = std::fs::read_dir(&matview_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.ends_with(".sql") {
                        let view_name = name.trim_end_matches(".sql");
                        let definition = std::fs::read_to_string(entry.path())
                            .unwrap_or_default();
                        all_rows.push(vec![
                            Value::Str("public".into()),
                            Value::Str(view_name.to_string()),
                            Value::Str("exchangedb".into()),
                            Value::Str(definition),
                        ]);
                    }
                }
            }
        }
    }

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_tables(db_root: &Path, columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["schemaname", "tablename", "tableowner", "hasindexes"];

    let tables = list_user_tables(db_root);
    let mut all_rows = Vec::new();
    for tbl in &tables {
        let has_indexes = if let Some(meta) = load_meta(db_root, &tbl) {
            meta.columns.iter().any(|c| c.indexed)
        } else {
            false
        };
        all_rows.push(vec![
            Value::Str("public".into()),
            Value::Str(tbl.clone()),
            Value::Str("exchangedb".into()),
            Value::I64(if has_indexes { 1 } else { 0 }),
        ]);
    }

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_description(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["objoid", "classoid", "objsubid", "description"];
    // Return empty — no descriptions stored yet.
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

// ── Additional empty pg_catalog tables for client compatibility ──────────

fn catalog_pg_am(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["oid", "amname", "amhandler", "amtype"];
    // Return built-in access methods that clients expect.
    let all_rows = vec![
        vec![
            Value::I64(2),
            Value::Str("heap".into()),
            Value::Str("heap_tableam_handler".into()),
            Value::Str("t".into()), // table AM
        ],
        vec![
            Value::I64(403),
            Value::Str("btree".into()),
            Value::Str("bthandler".into()),
            Value::Str("i".into()), // index AM
        ],
        vec![
            Value::I64(405),
            Value::Str("hash".into()),
            Value::Str("hashhandler".into()),
            Value::Str("i".into()),
        ],
    ];
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_operator(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["oid", "oprname", "oprnamespace", "oprleft", "oprright", "oprresult"];
    // Return empty — ExchangeDB does not expose operator metadata.
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_proc(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["oid", "proname", "pronamespace", "prorettype", "pronargs"];
    // Return empty — no user-defined functions yet.
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_constraint(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["oid", "conname", "connamespace", "contype", "conrelid"];
    // Return empty — ExchangeDB does not support constraints yet.
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_depend(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["classid", "objid", "objsubid", "refclassid", "refobjid", "refobjsubid", "deptype"];
    // Return empty — no dependency tracking.
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_enum(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["oid", "enumtypid", "enumsortorder", "enumlabel"];
    // Return empty — no enum types.
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_collation(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["oid", "collname", "collnamespace", "collencoding"];
    // Return the default collation.
    let all_rows = vec![vec![
        Value::I64(100),
        Value::Str("default".into()),
        Value::I64(NS_PG_CATALOG),
        Value::I64(-1), // all encodings
    ]];
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

// ── Additional pg_catalog tables (batch 2) ──────────────────────────────

fn catalog_pg_stat_user_tables(db_root: &Path, columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "relid", "schemaname", "relname", "seq_scan", "seq_tup_read",
        "idx_scan", "idx_tup_fetch", "n_tup_ins", "n_tup_upd", "n_tup_del",
        "n_live_tup", "n_dead_tup", "last_vacuum", "last_autovacuum",
        "last_analyze", "last_autoanalyze",
    ];

    let tables = list_user_tables(db_root);
    let mut all_rows = Vec::new();
    for (i, tbl) in tables.iter().enumerate() {
        all_rows.push(vec![
            Value::I64(16384 + i as i64),
            Value::Str("public".into()),
            Value::Str(tbl.clone()),
            Value::I64(0), // seq_scan
            Value::I64(0), // seq_tup_read
            Value::I64(0), // idx_scan
            Value::I64(0), // idx_tup_fetch
            Value::I64(0), // n_tup_ins
            Value::I64(0), // n_tup_upd
            Value::I64(0), // n_tup_del
            Value::I64(0), // n_live_tup
            Value::I64(0), // n_dead_tup
            Value::Null,   // last_vacuum
            Value::Null,   // last_autovacuum
            Value::Null,   // last_analyze
            Value::Null,   // last_autoanalyze
        ]);
    }

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_stat_user_indexes(db_root: &Path, columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "relid", "indexrelid", "schemaname", "relname", "indexrelname",
        "idx_scan", "idx_tup_read", "idx_tup_fetch",
    ];

    let tables = list_user_tables(db_root);
    let mut all_rows = Vec::new();
    for (tbl_idx, tbl) in tables.iter().enumerate() {
        if let Some(meta) = load_meta(db_root, tbl) {
            for col_def in &meta.columns {
                if col_def.indexed {
                    let idx_name = format!("{tbl}_{}_idx", col_def.name);
                    all_rows.push(vec![
                        Value::I64(16384 + tbl_idx as i64),
                        Value::I64(32768 + all_rows.len() as i64),
                        Value::Str("public".into()),
                        Value::Str(tbl.clone()),
                        Value::Str(idx_name),
                        Value::I64(0),
                        Value::I64(0),
                        Value::I64(0),
                    ]);
                }
            }
        }
    }

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_statio_user_tables(db_root: &Path, columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "relid", "schemaname", "relname",
        "heap_blks_read", "heap_blks_hit",
        "idx_blks_read", "idx_blks_hit",
        "toast_blks_read", "toast_blks_hit",
    ];

    let tables = list_user_tables(db_root);
    let mut all_rows = Vec::new();
    for (i, tbl) in tables.iter().enumerate() {
        all_rows.push(vec![
            Value::I64(16384 + i as i64),
            Value::Str("public".into()),
            Value::Str(tbl.clone()),
            Value::I64(0),
            Value::I64(0),
            Value::I64(0),
            Value::I64(0),
            Value::I64(0),
            Value::I64(0),
        ]);
    }

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_locks(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "locktype", "database", "relation", "page", "tuple",
        "virtualxid", "transactionid", "classid", "objid", "objsubid",
        "virtualtransaction", "pid", "mode", "granted", "fastpath",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_prepared_statements(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "name", "statement", "prepare_time", "parameter_types", "from_sql",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_cursors(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "name", "statement", "is_holdable", "is_binary", "is_scrollable", "creation_time",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_available_extensions(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "name", "default_version", "installed_version", "comment",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_timezone_names(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["name", "abbrev", "utc_offset", "is_dst"];

    let all_rows = vec![
        vec![
            Value::Str("UTC".into()),
            Value::Str("UTC".into()),
            Value::Str("00:00:00".into()),
            Value::I64(0),
        ],
        vec![
            Value::Str("US/Eastern".into()),
            Value::Str("EST".into()),
            Value::Str("-05:00:00".into()),
            Value::I64(0),
        ],
        vec![
            Value::Str("US/Pacific".into()),
            Value::Str("PST".into()),
            Value::Str("-08:00:00".into()),
            Value::I64(0),
        ],
        vec![
            Value::Str("Europe/London".into()),
            Value::Str("GMT".into()),
            Value::Str("00:00:00".into()),
            Value::I64(0),
        ],
        vec![
            Value::Str("Asia/Tokyo".into()),
            Value::Str("JST".into()),
            Value::Str("09:00:00".into()),
            Value::I64(0),
        ],
    ];

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_timezone_abbrevs(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["abbrev", "utc_offset", "is_dst"];

    let all_rows = vec![
        vec![Value::Str("UTC".into()), Value::Str("00:00:00".into()), Value::I64(0)],
        vec![Value::Str("EST".into()), Value::Str("-05:00:00".into()), Value::I64(0)],
        vec![Value::Str("PST".into()), Value::Str("-08:00:00".into()), Value::I64(0)],
        vec![Value::Str("GMT".into()), Value::Str("00:00:00".into()), Value::I64(0)],
        vec![Value::Str("JST".into()), Value::Str("09:00:00".into()), Value::I64(0)],
        vec![Value::Str("CET".into()), Value::Str("01:00:00".into()), Value::I64(0)],
    ];

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_file_settings(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "sourcefile", "sourceline", "seqno", "name", "setting", "applied", "error",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_hba_file_rules(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "line_number", "type", "database", "user_name", "address", "netmask",
        "auth_method", "options", "error",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_replication_slots(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "slot_name", "plugin", "slot_type", "datoid", "database",
        "temporary", "active", "active_pid", "xmin", "catalog_xmin",
        "restart_lsn", "confirmed_flush_lsn",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_publication(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "oid", "pubname", "pubowner", "puballtables", "pubinsert",
        "pubupdate", "pubdelete", "pubtruncate",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_subscription(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "oid", "subdbid", "subname", "subowner", "subenabled",
        "subconninfo", "subslotname", "subsynccommit", "subpublications",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_sequences(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "schemaname", "sequencename", "sequenceowner", "data_type",
        "start_value", "min_value", "max_value", "increment_by",
        "cycle", "cache_size", "last_value",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_largeobject(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["loid", "pageno", "data"];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_shdescription(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["objoid", "classoid", "description"];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_seclabel(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["objoid", "classoid", "objsubid", "provider", "label"];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_event_trigger(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "oid", "evtname", "evtevent", "evtowner", "evtfoid", "evtenabled", "evttags",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_trigger(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "oid", "tgrelid", "tgname", "tgfoid", "tgtype",
        "tgenabled", "tgisinternal", "tgconstrrelid",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_cast(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["oid", "castsource", "casttarget", "castfunc", "castcontext"];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_conversion(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "oid", "conname", "connamespace", "conowner",
        "conforencoding", "contoencoding", "conproc", "condefault",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_opclass(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "oid", "opcmethod", "opcname", "opcnamespace", "opcowner",
        "opcfamily", "opcintype", "opcdefault", "opckeytype",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_opfamily(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "oid", "opfmethod", "opfname", "opfnamespace", "opfowner",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_aggregate(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "aggfnoid", "aggkind", "aggnumdirectargs", "aggtransfn",
        "aggfinalfn", "aggcombinefn", "aggtranstype", "agginitval",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_language(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "oid", "lanname", "lanowner", "lanispl", "lanpltrusted",
        "lanplcallfoid", "laninline", "lanvalidator",
    ];

    let all_rows = vec![
        vec![
            Value::I64(12),
            Value::Str("internal".into()),
            Value::I64(10),
            Value::I64(0),
            Value::I64(0),
            Value::I64(0),
            Value::I64(0),
            Value::I64(0),
        ],
        vec![
            Value::I64(13),
            Value::Str("c".into()),
            Value::I64(10),
            Value::I64(0),
            Value::I64(0),
            Value::I64(0),
            Value::I64(0),
            Value::I64(0),
        ],
        vec![
            Value::I64(14),
            Value::Str("sql".into()),
            Value::I64(10),
            Value::I64(0),
            Value::I64(1),
            Value::I64(0),
            Value::I64(0),
            Value::I64(0),
        ],
    ];

    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_foreign_server(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &[
        "oid", "srvname", "srvowner", "srvfdw", "srvtype", "srvversion", "srvoptions",
    ];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_foreign_table(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["ftrelid", "ftserver", "ftoptions"];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

fn catalog_pg_user_mapping(columns: &[SelectColumn]) -> Result<QueryResult> {
    let all_cols: &[&str] = &["oid", "umuser", "umserver", "umoptions"];
    let all_rows: Vec<Vec<Value>> = Vec::new();
    let (col_names, rows) = project(all_cols, &all_rows, columns);
    Ok(QueryResult::Rows { columns: col_names, rows })
}

// ── Type mapping helpers ────────────────────────────────────────────────

fn col_type_to_pg_oid(ct: &exchange_core::table::ColumnTypeSerializable) -> i64 {
    use exchange_core::table::ColumnTypeSerializable as CT;
    match ct {
        CT::Boolean => OID_BOOL,
        CT::I8 | CT::I16 | CT::I32 => OID_INT4,
        CT::I64 | CT::Long128 | CT::Long256 => OID_INT8,
        CT::F32 => OID_FLOAT4,
        CT::F64 | CT::Decimal8 | CT::Decimal16 | CT::Decimal32
            | CT::Decimal64 | CT::Decimal128 | CT::Decimal256 => OID_FLOAT8,
        CT::Timestamp | CT::TimestampMicro | CT::TimestampMilli | CT::Date => OID_TIMESTAMP,
        CT::Symbol | CT::Varchar | CT::Char | CT::String | CT::ArrayString => OID_TEXT,
        CT::Binary | CT::Uuid | CT::IPv4 | CT::GeoHash
            | CT::GeoByte | CT::GeoShort | CT::GeoInt => OID_TEXT,
        CT::Interval => OID_TEXT,
        CT::Array | CT::Cursor | CT::Record | CT::RegClass
            | CT::RegProcedure | CT::Null | CT::VarArg | CT::Parameter
            | CT::VarcharSlice | CT::IPv6 => OID_TEXT,
    }
}

fn col_type_to_pg_name(ct: &exchange_core::table::ColumnTypeSerializable) -> String {
    use exchange_core::table::ColumnTypeSerializable as CT;
    match ct {
        CT::Boolean => "boolean".into(),
        CT::I8 => "smallint".into(),
        CT::I16 => "smallint".into(),
        CT::I32 => "integer".into(),
        CT::I64 => "bigint".into(),
        CT::F32 => "real".into(),
        CT::F64 => "double precision".into(),
        CT::Timestamp | CT::TimestampMicro | CT::TimestampMilli => "timestamp with time zone".into(),
        CT::Symbol => "text".into(),
        CT::String | CT::ArrayString => "text".into(),
        CT::Varchar => "character varying".into(),
        CT::Binary => "bytea".into(),
        CT::Uuid => "uuid".into(),
        CT::Date => "date".into(),
        CT::Char => "character".into(),
        CT::IPv4 => "inet".into(),
        CT::Long128 => "numeric".into(),
        CT::Long256 => "numeric".into(),
        CT::GeoHash | CT::GeoByte | CT::GeoShort | CT::GeoInt => "text".into(),
        CT::Interval => "interval".into(),
        CT::Decimal8 | CT::Decimal16 | CT::Decimal32
            | CT::Decimal64 | CT::Decimal128 | CT::Decimal256 => "numeric".into(),
        CT::Array => "anyarray".into(),
        CT::Cursor => "refcursor".into(),
        CT::Record => "record".into(),
        CT::RegClass => "regclass".into(),
        CT::RegProcedure => "regprocedure".into(),
        CT::Null => "void".into(),
        CT::VarArg | CT::Parameter | CT::VarcharSlice => "text".into(),
        CT::IPv6 => "inet".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use exchange_common::types::{ColumnType, PartitionBy};
    use exchange_core::table::TableBuilder;

    fn setup_test_db(db_root: &Path) {
        // Create a "trades" table.
        TableBuilder::new("trades")
            .column("timestamp", ColumnType::Timestamp)
            .column("symbol", ColumnType::Symbol)
            .column("price", ColumnType::F64)
            .column("volume", ColumnType::I64)
            .timestamp("timestamp")
            .partition_by(PartitionBy::Day)
            .build(db_root)
            .unwrap();

        // Create an "orders" table.
        TableBuilder::new("orders")
            .column("timestamp", ColumnType::Timestamp)
            .column("order_id", ColumnType::Varchar)
            .column("quantity", ColumnType::I64)
            .timestamp("timestamp")
            .build(db_root)
            .unwrap();
    }

    #[test]
    fn pg_type_returns_expected_types() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_type(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert_eq!(columns, vec!["oid", "typname", "typnamespace", "typlen"]);
            assert!(rows.len() >= 5);

            // Check that int8 is present with OID 20.
            let int8_row = rows.iter().find(|r| r[1] == Value::Str("int8".into()));
            assert!(int8_row.is_some());
            assert_eq!(int8_row.unwrap()[0], Value::I64(20));

            // Check that float8 is present with OID 701.
            let float8_row = rows.iter().find(|r| r[1] == Value::Str("float8".into()));
            assert!(float8_row.is_some());
            assert_eq!(float8_row.unwrap()[0], Value::I64(701));

            // Check that text is present with OID 25.
            let text_row = rows.iter().find(|r| r[1] == Value::Str("text".into()));
            assert!(text_row.is_some());
            assert_eq!(text_row.unwrap()[0], Value::I64(25));

            // Check that timestamptz is present with OID 1184.
            let ts_row = rows.iter().find(|r| r[1] == Value::Str("timestamptz".into()));
            assert!(ts_row.is_some());
            assert_eq!(ts_row.unwrap()[0], Value::I64(1184));

            // Check bool with OID 16.
            let bool_row = rows.iter().find(|r| r[1] == Value::Str("bool".into()));
            assert!(bool_row.is_some());
            assert_eq!(bool_row.unwrap()[0], Value::I64(16));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn info_schema_tables_lists_user_tables() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        setup_test_db(db_root);

        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_info_tables(db_root, &cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"table_name".to_string()));
            let table_names: Vec<&Value> = rows.iter().map(|r| &r[2]).collect();
            assert!(table_names.contains(&&Value::Str("trades".into())));
            assert!(table_names.contains(&&Value::Str("orders".into())));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn info_schema_columns_returns_correct_info() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        setup_test_db(db_root);

        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_info_columns(db_root, &cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"column_name".to_string()));
            assert!(columns.contains(&"data_type".to_string()));

            // Find trades columns.
            let trades_cols: Vec<&Vec<Value>> = rows
                .iter()
                .filter(|r| r[2] == Value::Str("trades".into()))
                .collect();
            assert_eq!(trades_cols.len(), 4); // timestamp, symbol, price, volume

            let col_names: Vec<&Value> = trades_cols.iter().map(|r| &r[3]).collect();
            assert!(col_names.contains(&&Value::Str("timestamp".into())));
            assert!(col_names.contains(&&Value::Str("price".into())));
            assert!(col_names.contains(&&Value::Str("symbol".into())));
            assert!(col_names.contains(&&Value::Str("volume".into())));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_database_returns_exchangedb() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_database(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"datname".to_string()));
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], Value::Str("exchangedb".into()));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn version_returns_exchangedb_version() {
        let cols = vec![SelectColumn::ScalarFunction {
            name: "version".into(),
            args: vec![],
        }];
        let result = execute_system_function(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert_eq!(columns, vec!["version"]);
            assert_eq!(rows.len(), 1);
            match &rows[0][0] {
                Value::Str(s) => {
                    assert!(s.contains("ExchangeDB"));
                    assert!(s.contains("PostgreSQL compatible"));
                }
                other => panic!("expected Str, got {other:?}"),
            }
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn is_catalog_query_recognizes_qualified_names() {
        assert!(is_catalog_query("pg_catalog.pg_type"));
        assert!(is_catalog_query("pg_catalog.pg_class"));
        assert!(is_catalog_query("pg_catalog.pg_namespace"));
        assert!(is_catalog_query("pg_catalog.pg_attribute"));
        assert!(is_catalog_query("pg_catalog.pg_database"));
        assert!(is_catalog_query("information_schema.tables"));
        assert!(is_catalog_query("information_schema.columns"));
        assert!(is_catalog_query("pg_type")); // unqualified also works
        assert!(!is_catalog_query("trades"));
    }

    #[test]
    fn pg_namespace_returns_schemas() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_namespace(&cols).unwrap();
        if let QueryResult::Rows { rows, .. } = result {
            assert_eq!(rows.len(), 2);
            let names: Vec<&Value> = rows.iter().map(|r| &r[1]).collect();
            assert!(names.contains(&&Value::Str("pg_catalog".into())));
            assert!(names.contains(&&Value::Str("public".into())));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_class_lists_user_tables() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        setup_test_db(db_root);

        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_class(db_root, &cols).unwrap();
        if let QueryResult::Rows { rows, .. } = result {
            let names: Vec<&Value> = rows.iter().map(|r| &r[1]).collect();
            assert!(names.contains(&&Value::Str("trades".into())));
            assert!(names.contains(&&Value::Str("orders".into())));
            // All should be relkind = 'r'
            for row in &rows {
                assert_eq!(row[3], Value::Str("r".into()));
            }
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_attribute_returns_column_info() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        setup_test_db(db_root);

        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_attribute(db_root, &cols).unwrap();
        if let QueryResult::Rows { rows, .. } = result {
            // Should have 4 (trades) + 3 (orders) = 7 columns.
            assert_eq!(rows.len(), 7);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn column_projection_works() {
        let cols = vec![
            SelectColumn::Name("oid".into()),
            SelectColumn::Name("typname".into()),
        ];
        let result = catalog_pg_type(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert_eq!(columns, vec!["oid", "typname"]);
            assert_eq!(rows[0].len(), 2);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_settings_returns_server_version() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_settings(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"name".to_string()));
            assert!(columns.contains(&"setting".to_string()));
            // Find the server_version setting.
            let sv_row = rows.iter().find(|r| r[0] == Value::Str("server_version".into()));
            assert!(sv_row.is_some(), "pg_settings should contain server_version");
            assert_eq!(sv_row.unwrap()[1], Value::Str("0.1.0".into()));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_roles_returns_users() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_roles(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"rolname".to_string()));
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], Value::Str("exchangedb".into()));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_stat_activity_returns_row() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_stat_activity(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"datname".to_string()));
            assert!(columns.contains(&"state".to_string()));
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], Value::Str("exchangedb".into()));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_tables_lists_user_tables() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        setup_test_db(db_root);

        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_tables(db_root, &cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"tablename".to_string()));
            assert!(columns.contains(&"schemaname".to_string()));
            let table_names: Vec<&Value> = rows.iter().map(|r| &r[1]).collect();
            assert!(table_names.contains(&&Value::Str("trades".into())));
            assert!(table_names.contains(&&Value::Str("orders".into())));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_description_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_description(&cols).unwrap();
        if let QueryResult::Rows { rows, .. } = result {
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn is_catalog_query_recognizes_new_tables() {
        assert!(is_catalog_query("pg_catalog.pg_settings"));
        assert!(is_catalog_query("pg_settings"));
        assert!(is_catalog_query("pg_roles"));
        assert!(is_catalog_query("pg_stat_activity"));
        assert!(is_catalog_query("pg_indexes"));
        assert!(is_catalog_query("pg_views"));
        assert!(is_catalog_query("pg_matviews"));
        assert!(is_catalog_query("pg_tables"));
        assert!(is_catalog_query("pg_description"));
    }

    // ── New pg_catalog table tests ──────────────────────────────────────

    #[test]
    fn is_catalog_query_recognizes_m5_tables() {
        assert!(is_catalog_query("pg_am"));
        assert!(is_catalog_query("pg_catalog.pg_am"));
        assert!(is_catalog_query("pg_operator"));
        assert!(is_catalog_query("pg_proc"));
        assert!(is_catalog_query("pg_constraint"));
        assert!(is_catalog_query("pg_depend"));
        assert!(is_catalog_query("pg_enum"));
        assert!(is_catalog_query("pg_collation"));
    }

    #[test]
    fn pg_am_returns_access_methods() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_am(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"amname".to_string()));
            assert!(rows.len() >= 2);
            let names: Vec<&Value> = rows.iter().map(|r| &r[1]).collect();
            assert!(names.contains(&&Value::Str("heap".into())));
            assert!(names.contains(&&Value::Str("btree".into())));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_operator_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_operator(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"oprname".to_string()));
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_proc_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_proc(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"proname".to_string()));
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_constraint_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_constraint(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"conname".to_string()));
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_depend_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_depend(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"deptype".to_string()));
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_enum_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_enum(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"enumlabel".to_string()));
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_collation_returns_default() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_collation(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"collname".to_string()));
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], Value::Str("default".into()));
        } else {
            panic!("expected Rows result");
        }
    }

    // ── Tests for new pg_catalog tables ──────────────────────────────────

    #[test]
    fn is_catalog_query_recognizes_batch2_tables() {
        assert!(is_catalog_query("pg_stat_user_tables"));
        assert!(is_catalog_query("pg_catalog.pg_stat_user_tables"));
        assert!(is_catalog_query("pg_stat_user_indexes"));
        assert!(is_catalog_query("pg_statio_user_tables"));
        assert!(is_catalog_query("pg_locks"));
        assert!(is_catalog_query("pg_prepared_statements"));
        assert!(is_catalog_query("pg_cursors"));
        assert!(is_catalog_query("pg_available_extensions"));
        assert!(is_catalog_query("pg_timezone_names"));
        assert!(is_catalog_query("pg_timezone_abbrevs"));
        assert!(is_catalog_query("pg_file_settings"));
        assert!(is_catalog_query("pg_hba_file_rules"));
        assert!(is_catalog_query("pg_replication_slots"));
        assert!(is_catalog_query("pg_publication"));
        assert!(is_catalog_query("pg_subscription"));
        assert!(is_catalog_query("pg_sequences"));
        assert!(is_catalog_query("pg_largeobject"));
        assert!(is_catalog_query("pg_shdescription"));
        assert!(is_catalog_query("pg_seclabel"));
        assert!(is_catalog_query("pg_event_trigger"));
        assert!(is_catalog_query("pg_trigger"));
        assert!(is_catalog_query("pg_cast"));
        assert!(is_catalog_query("pg_conversion"));
        assert!(is_catalog_query("pg_opclass"));
        assert!(is_catalog_query("pg_opfamily"));
        assert!(is_catalog_query("pg_aggregate"));
        assert!(is_catalog_query("pg_language"));
        assert!(is_catalog_query("pg_foreign_server"));
        assert!(is_catalog_query("pg_foreign_table"));
        assert!(is_catalog_query("pg_user_mapping"));
    }

    #[test]
    fn pg_stat_user_tables_lists_tables() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        setup_test_db(db_root);

        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_stat_user_tables(db_root, &cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"relname".to_string()));
            assert!(columns.contains(&"seq_scan".to_string()));
            assert_eq!(rows.len(), 2);
            let names: Vec<&Value> = rows.iter().map(|r| &r[2]).collect();
            assert!(names.contains(&&Value::Str("trades".into())));
            assert!(names.contains(&&Value::Str("orders".into())));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_stat_user_indexes_returns_indexed_columns() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        setup_test_db(db_root);

        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_stat_user_indexes(db_root, &cols).unwrap();
        if let QueryResult::Rows { columns, .. } = result {
            assert!(columns.contains(&"indexrelname".to_string()));
            assert!(columns.contains(&"idx_scan".to_string()));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_statio_user_tables_lists_tables() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();
        setup_test_db(db_root);

        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_statio_user_tables(db_root, &cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"heap_blks_read".to_string()));
            assert_eq!(rows.len(), 2);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_locks_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_locks(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"locktype".to_string()));
            assert!(columns.contains(&"mode".to_string()));
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_prepared_statements_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_prepared_statements(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"name".to_string()));
            assert!(columns.contains(&"statement".to_string()));
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_cursors_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_cursors(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"name".to_string()));
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_timezone_names_returns_entries() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_timezone_names(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"name".to_string()));
            assert!(columns.contains(&"abbrev".to_string()));
            assert!(rows.len() >= 3);
            let names: Vec<&Value> = rows.iter().map(|r| &r[0]).collect();
            assert!(names.contains(&&Value::Str("UTC".into())));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_timezone_abbrevs_returns_entries() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_timezone_abbrevs(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"abbrev".to_string()));
            assert!(rows.len() >= 3);
            let abbrevs: Vec<&Value> = rows.iter().map(|r| &r[0]).collect();
            assert!(abbrevs.contains(&&Value::Str("UTC".into())));
            assert!(abbrevs.contains(&&Value::Str("EST".into())));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_language_returns_builtins() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_language(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"lanname".to_string()));
            assert_eq!(rows.len(), 3);
            let names: Vec<&Value> = rows.iter().map(|r| &r[1]).collect();
            assert!(names.contains(&&Value::Str("internal".into())));
            assert!(names.contains(&&Value::Str("c".into())));
            assert!(names.contains(&&Value::Str("sql".into())));
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_cast_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_cast(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"castsource".to_string()));
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_trigger_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_trigger(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"tgname".to_string()));
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_foreign_server_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_foreign_server(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"srvname".to_string()));
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_replication_slots_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_replication_slots(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"slot_name".to_string()));
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }

    #[test]
    fn pg_sequences_returns_empty() {
        let cols = vec![SelectColumn::Wildcard];
        let result = catalog_pg_sequences(&cols).unwrap();
        if let QueryResult::Rows { columns, rows } = result {
            assert!(columns.contains(&"sequencename".to_string()));
            assert_eq!(rows.len(), 0);
        } else {
            panic!("expected Rows result");
        }
    }
}
