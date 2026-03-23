//! Query handler implementing pgwire's `SimpleQueryHandler` trait.

use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::Sink;
use futures::stream;
use pgwire::api::{ClientInfo, ClientPortalStore, Type};
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use exchange_common::types::ColumnType;
use exchange_core::replication::ReplicationManager;
use exchange_query::{ExecutionContext, QueryResult, Value};

/// Handler that processes SQL queries against the ExchangeDB engine.
///
/// Implements pgwire's `SimpleQueryHandler` to support the PostgreSQL
/// simple query protocol. Each incoming SQL string is parsed, planned,
/// and executed against the database at `db_root`.
pub struct ExchangeDbHandler {
    db_root: PathBuf,
    replication_manager: Option<Arc<ReplicationManager>>,
}

impl ExchangeDbHandler {
    /// Create a new handler rooted at the given database directory.
    pub fn new(db_root: PathBuf, replication_manager: Option<Arc<ReplicationManager>>) -> Self {
        Self {
            db_root,
            replication_manager,
        }
    }
}

impl ExchangeDbHandler {
    /// Intercept known psql meta-command queries (e.g. \dt, \d table) that use
    /// complex pg_catalog JOINs and return pre-computed results from our catalog.
    fn intercept_psql_meta_query(
        &self,
        query: &str,
    ) -> Option<PgWireResult<Vec<Response>>> {
        let upper = query.to_uppercase();

        // Detect \dt query: SELECT ... FROM pg_catalog.pg_class with RELKIND IN filter
        // (the \dt query has `c.relkind IN ('r','p','')`)
        if upper.contains("PG_CATALOG.PG_CLASS") && upper.contains("RELKIND IN") {
            return Some(self.handle_list_tables());
        }

        // Detect pg_class queries by OID (e.g. WHERE c.oid = '12345') - used by \d table
        if upper.contains("PG_CATALOG.PG_CLASS") {
            if let Some(oid) = extract_oid_from_query(query)
                && let Some(name) = self.resolve_oid_to_table(oid)
            {
                return Some(self.handle_pg_class_by_oid(&name));
            }
            // Try to extract table name from relname pattern
            let table_name = extract_table_name_from_describe(query);
            if let Some(name) = table_name {
                return Some(self.handle_table_oid_lookup(&name));
            }
        }

        // Detect \d <table> attribute query: SELECT ... FROM pg_catalog.pg_attribute
        if upper.contains("PG_CATALOG.PG_ATTRIBUTE") {
            if let Some(oid) = extract_oid_from_query(query)
                && let Some(name) = self.resolve_oid_to_table(oid)
            {
                return Some(self.handle_describe_table(&name));
            }
            let table_name = extract_table_name_from_describe(query);
            if let Some(name) = table_name {
                return Some(self.handle_describe_table(&name));
            }
        }

        // Any other pg_catalog query we can't fully handle: return empty result set
        if upper.contains("FROM PG_CATALOG.") {
            return Some(self.handle_empty_catalog_query(query));
        }

        None
    }

    /// Handle \dt: list all user tables.
    fn handle_list_tables(&self) -> PgWireResult<Vec<Response>> {
        // Read table names from the data directory.
        let mut table_names = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.db_root) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    let meta_path = entry.path().join("_meta");
                    if meta_path.exists() {
                        table_names.push(entry.file_name().to_string_lossy().to_string());
                    }
                }
            }
        }
        table_names.sort();

        let columns = vec![
            "Schema".to_string(),
            "Name".to_string(),
            "Type".to_string(),
            "Owner".to_string(),
        ];
        let rows: Vec<Vec<Value>> = table_names
            .iter()
            .map(|name| {
                vec![
                    Value::Str("public".to_string()),
                    Value::Str(name.clone()),
                    Value::Str("table".to_string()),
                    Value::Str("exchangedb".to_string()),
                ]
            })
            .collect();

        let field_infos = infer_field_infos(&columns, &rows);
        let schema = Arc::new(field_infos);

        let mut data_rows = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut encoder = DataRowEncoder::new(schema.clone());
            for value in row {
                encode_value(&mut encoder, value)?;
            }
            data_rows.push(encoder.take_row());
        }

        let data_row_stream = stream::iter(data_rows.into_iter().map(Ok));
        let response = QueryResponse::new(schema, data_row_stream);
        Ok(vec![Response::Query(response)])
    }

    /// Compute a deterministic fake OID from a table name.
    fn table_name_to_oid(name: &str) -> i64 {
        let mut h: u32 = 5381;
        for b in name.bytes() {
            h = h.wrapping_mul(33).wrapping_add(b as u32);
        }
        (h & 0x7FFFFFFF) as i64
    }

    /// Resolve a fake OID back to a table name by checking all tables.
    fn resolve_oid_to_table(&self, oid: i64) -> Option<String> {
        if let Ok(entries) = std::fs::read_dir(&self.db_root) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    let meta_path = entry.path().join("_meta");
                    if meta_path.exists() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        if Self::table_name_to_oid(&name) == oid {
                            return Some(name);
                        }
                    }
                }
            }
        }
        None
    }

    /// Handle pg_class query by OID: return table metadata.
    /// psql sends: SELECT c.relchecks, c.relkind, ... WHERE c.oid = '<oid>'
    fn handle_pg_class_by_oid(&self, table_name: &str) -> PgWireResult<Vec<Response>> {
        // Return the columns that psql expects for the describe command
        let columns = vec![
            "relchecks".to_string(),
            "relkind".to_string(),
            "relhasindex".to_string(),
            "relhasrules".to_string(),
            "relhastriggers".to_string(),
            "relrowsecurity".to_string(),
            "relforcerowsecurity".to_string(),
            "relhasoids".to_string(),
            "relispartition".to_string(),
            "reltablespace_name".to_string(),
            "reltablespace".to_string(),
            "reloftype".to_string(),
            "relpersistence".to_string(),
        ];
        let _ = table_name;
        let rows = vec![vec![
            Value::I64(0),               // relchecks
            Value::Str("r".to_string()), // relkind = regular table
            Value::I64(0),               // relhasindex (false)
            Value::I64(0),               // relhasrules
            Value::I64(0),               // relhastriggers
            Value::I64(0),               // relrowsecurity (false)
            Value::I64(0),               // relforcerowsecurity (false)
            Value::I64(0),               // relhasoids (false)
            Value::I64(0),               // relispartition (false)
            Value::Str(String::new()),   // reltablespace_name
            Value::I64(0),               // reltablespace
            Value::Str(String::new()),   // reloftype
            Value::Str("p".to_string()), // relpersistence = permanent
        ]];

        let field_infos = infer_field_infos(&columns, &rows);
        let schema = Arc::new(field_infos);

        let mut data_rows = Vec::with_capacity(1);
        for row in &rows {
            let mut encoder = DataRowEncoder::new(schema.clone());
            for value in row {
                encode_value(&mut encoder, value)?;
            }
            data_rows.push(encoder.take_row());
        }

        let data_row_stream = stream::iter(data_rows.into_iter().map(Ok));
        let response = QueryResponse::new(schema, data_row_stream);
        Ok(vec![Response::Query(response)])
    }

    /// Handle unrecognized pg_catalog queries by returning empty result set.
    /// Extracts column names from the SELECT clause to provide correct schema.
    fn handle_empty_catalog_query(&self, _query: &str) -> PgWireResult<Vec<Response>> {
        // Return an empty result with a single generic column
        let field_infos = vec![FieldInfo::new(
            "?column?".to_owned(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        )];
        let schema = Arc::new(field_infos);
        let data_row_stream = stream::iter(Vec::new().into_iter().map(Ok));
        let response = QueryResponse::new(schema, data_row_stream);
        Ok(vec![Response::Query(response)])
    }

    /// Handle \d <table> first query: return fake OID for the table.
    /// psql sends: SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c ...
    fn handle_table_oid_lookup(&self, table_name: &str) -> PgWireResult<Vec<Response>> {
        let table_dir = self.db_root.join(table_name);
        if !table_dir.exists() {
            // Return empty result - psql will say "Did not find any relation"
            let field_infos = vec![
                FieldInfo::new("oid".to_owned(), None, None, Type::INT4, FieldFormat::Text),
                FieldInfo::new(
                    "nspname".to_owned(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Text,
                ),
                FieldInfo::new(
                    "relname".to_owned(),
                    None,
                    None,
                    Type::TEXT,
                    FieldFormat::Text,
                ),
            ];
            let schema = Arc::new(field_infos);
            let data_row_stream = stream::iter(Vec::new().into_iter().map(Ok));
            let response = QueryResponse::new(schema, data_row_stream);
            return Ok(vec![Response::Query(response)]);
        }

        // Use a deterministic fake OID based on the table name hash.
        let fake_oid = Self::table_name_to_oid(table_name);

        let columns = vec![
            "oid".to_string(),
            "nspname".to_string(),
            "relname".to_string(),
        ];
        let rows = vec![vec![
            Value::I64(fake_oid),
            Value::Str("public".to_string()),
            Value::Str(table_name.to_string()),
        ]];

        let field_infos = infer_field_infos(&columns, &rows);
        let schema = Arc::new(field_infos);

        let mut data_rows = Vec::with_capacity(1);
        for row in &rows {
            let mut encoder = DataRowEncoder::new(schema.clone());
            for value in row {
                encode_value(&mut encoder, value)?;
            }
            data_rows.push(encoder.take_row());
        }

        let data_row_stream = stream::iter(data_rows.into_iter().map(Ok));
        let response = QueryResponse::new(schema, data_row_stream);
        Ok(vec![Response::Query(response)])
    }

    /// Handle \d <table>: describe a specific table's columns.
    fn handle_describe_table(&self, table_name: &str) -> PgWireResult<Vec<Response>> {
        let table_dir = self.db_root.join(table_name);
        if !table_dir.exists() {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P01".to_owned(),
                format!("relation \"{table_name}\" does not exist"),
            ))));
        }

        let meta =
            exchange_core::table::TableMeta::load(&table_dir.join("_meta")).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    e.to_string(),
                )))
            })?;

        // Match the column schema that psql expects from pg_attribute:
        // attname, format_type, pg_get_expr, attnotnull, attcollation, attidentity, attgenerated
        let columns = vec![
            "attname".to_string(),
            "format_type".to_string(),
            "pg_get_expr".to_string(),
            "attnotnull".to_string(),
            "attcollation".to_string(),
            "attidentity".to_string(),
            "attgenerated".to_string(),
        ];

        let rows: Vec<Vec<Value>> = meta
            .columns
            .iter()
            .map(|col| {
                let type_name = col_type_to_pg_name(ColumnType::from(col.col_type));
                vec![
                    Value::Str(col.name.clone()),
                    Value::Str(type_name),
                    Value::Null,                 // pg_get_expr (no default)
                    Value::Str("f".to_string()), // attnotnull = false
                    Value::Null,                 // attcollation
                    Value::Str(String::new()),   // attidentity
                    Value::Str(String::new()),   // attgenerated
                ]
            })
            .collect();

        let field_infos = infer_field_infos(&columns, &rows);
        let schema = Arc::new(field_infos);

        let mut data_rows = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut encoder = DataRowEncoder::new(schema.clone());
            for value in row {
                encode_value(&mut encoder, value)?;
            }
            data_rows.push(encoder.take_row());
        }

        let data_row_stream = stream::iter(data_rows.into_iter().map(Ok));
        let response = QueryResponse::new(schema, data_row_stream);
        Ok(vec![Response::Query(response)])
    }
}

/// Extract an OID number from a WHERE clause like `c.oid = '515587784'` or `attrelid = '12345'`.
fn extract_oid_from_query(query: &str) -> Option<i64> {
    // Match patterns like: oid = '12345' or oid = 12345
    for marker in &["oid = '", "attrelid = '"] {
        if let Some(pos) = query.find(marker) {
            let start = pos + marker.len();
            if let Some(end) = query[start..].find('\'')
                && let Ok(oid) = query[start..start + end].parse::<i64>()
            {
                return Some(oid);
            }
        }
    }
    None
}

/// Extract a table name from a psql \d describe query.
/// Looks for patterns like: relname = 'tablename' or relname ~ '^(tablename)$'
fn extract_table_name_from_describe(query: &str) -> Option<String> {
    // Pattern: relname = 'tablename'
    if let Some(pos) = query.find("relname = '") {
        let start = pos + "relname = '".len();
        if let Some(end) = query[start..].find('\'') {
            return Some(query[start..start + end].to_string());
        }
    }
    // Pattern: relname ~ '^(tablename)$' or OPERATOR(pg_catalog.~) '^(tablename)$'
    for marker in &["relname ~", "relname OPERATOR"] {
        if let Some(pos) = query.find(marker) {
            // Find the next single-quoted string
            if let Some(q_start) = query[pos..].find('\'') {
                let abs_start = pos + q_start + 1;
                if let Some(q_end) = query[abs_start..].find('\'') {
                    let pattern = &query[abs_start..abs_start + q_end];
                    // Strip regex anchors: ^(name)$ -> name
                    let name = pattern
                        .trim_start_matches('^')
                        .trim_start_matches('(')
                        .trim_end_matches('$')
                        .trim_end_matches(')');
                    if !name.is_empty() {
                        return Some(name.to_string());
                    }
                }
            }
        }
    }
    None
}

/// Map an ExchangeDB `Value` to a PostgreSQL `Type`.
pub(crate) fn pg_type_for_value(value: &Value) -> Type {
    match value {
        Value::I64(_) => Type::INT8,
        Value::F64(_) => Type::FLOAT8,
        Value::Str(_) => Type::TEXT,
        Value::Timestamp(_) => Type::INT8,
        Value::Null => Type::TEXT,
    }
}

/// Map an ExchangeDB `ColumnType` to a PostgreSQL `Type` OID.
///
/// Provides a comprehensive mapping so that PostgreSQL clients see the
/// correct type for every column, regardless of whether the result set
/// contains data rows.
pub fn pg_type_for_column(col_type: ColumnType) -> Type {
    match col_type {
        ColumnType::Boolean => Type::BOOL,               // OID 16
        ColumnType::I8 => Type::INT2,                    // OID 21
        ColumnType::I16 => Type::INT2,                   // OID 21
        ColumnType::I32 => Type::INT4,                   // OID 23
        ColumnType::I64 => Type::INT8,                   // OID 20
        ColumnType::F32 => Type::FLOAT4,                 // OID 700
        ColumnType::F64 => Type::FLOAT8,                 // OID 701
        ColumnType::Timestamp => Type::TIMESTAMPTZ,      // OID 1184
        ColumnType::Symbol => Type::VARCHAR,             // OID 1043
        ColumnType::Varchar => Type::TEXT,               // OID 25
        ColumnType::Binary => Type::BYTEA,               // OID 17
        ColumnType::Uuid => Type::UUID,                  // OID 2950
        ColumnType::Date => Type::DATE,                  // OID 1082
        ColumnType::IPv4 => Type::INET,                  // OID 869
        ColumnType::GeoHash => Type::INT8,               // OID 20
        ColumnType::Char => Type::CHAR,                  // OID 18
        ColumnType::Long128 => Type::TEXT,               // No direct PG equivalent
        ColumnType::Long256 => Type::TEXT,               // No direct PG equivalent
        ColumnType::String => Type::TEXT,                // OID 25
        ColumnType::TimestampMicro => Type::TIMESTAMPTZ, // OID 1184
        ColumnType::TimestampMilli => Type::TIMESTAMPTZ, // OID 1184
        ColumnType::Interval => Type::INTERVAL,          // OID 1186
        ColumnType::Decimal8 => Type::NUMERIC,           // OID 1700
        ColumnType::Decimal16 => Type::NUMERIC,          // OID 1700
        ColumnType::Decimal32 => Type::NUMERIC,          // OID 1700
        ColumnType::Decimal64 => Type::NUMERIC,          // OID 1700
        ColumnType::Decimal128 => Type::NUMERIC,         // OID 1700
        ColumnType::Decimal256 => Type::NUMERIC,         // OID 1700
        ColumnType::GeoByte => Type::INT2,               // OID 21
        ColumnType::GeoShort => Type::INT2,              // OID 21
        ColumnType::GeoInt => Type::INT4,                // OID 23
        ColumnType::Array => Type::BYTEA,                // OID 17
        ColumnType::Cursor => Type::INT8,                // OID 20
        ColumnType::Record => Type::INT8,                // OID 20
        ColumnType::RegClass => Type::INT4,              // OID 23 (regclass as int4)
        ColumnType::RegProcedure => Type::INT4,          // OID 23 (regprocedure as int4)
        ColumnType::ArrayString => Type::TEXT,           // OID 25 (text[] as text fallback)
        ColumnType::Null => Type::TEXT,                  // fallback
        ColumnType::VarArg => Type::TEXT,                // fallback
        ColumnType::Parameter => Type::TEXT,             // fallback
        ColumnType::VarcharSlice => Type::VARCHAR,       // OID 1043
        ColumnType::IPv6 => Type::INET,                  // OID 869
    }
}

/// Infer PostgreSQL column types from the first row of data.
///
/// If there are no rows, all columns default to `TEXT`.
pub(crate) fn infer_field_infos(columns: &[String], rows: &[Vec<Value>]) -> Vec<FieldInfo> {
    columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let pg_type = rows
                .iter()
                .find_map(|row| {
                    let val = &row[i];
                    if matches!(val, Value::Null) {
                        None
                    } else {
                        Some(pg_type_for_value(val))
                    }
                })
                .unwrap_or(Type::TEXT);

            FieldInfo::new(name.clone(), None, None, pg_type, FieldFormat::Text)
        })
        .collect()
}

/// Encode a single ExchangeDB `Value` into a pgwire `DataRowEncoder`.
pub(crate) fn encode_value(encoder: &mut DataRowEncoder, value: &Value) -> PgWireResult<()> {
    match value {
        Value::I64(v) => encoder.encode_field(v),
        Value::F64(v) => encoder.encode_field(v),
        Value::Str(v) => encoder.encode_field(v),
        Value::Timestamp(v) => encoder.encode_field(v),
        Value::Null => encoder.encode_field(&None::<&str>),
    }
}

#[async_trait]
impl SimpleQueryHandler for ExchangeDbHandler {
    async fn do_query<C>(
        &self,
        _client: &mut C,
        query: &str,
    ) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Intercept psql meta-command queries that use complex pg_catalog JOINs
        // we don't fully support, and return pre-computed results.
        if let Some(response) = self.intercept_psql_meta_query(query) {
            return response;
        }

        // Pre-process PostgreSQL-specific operator syntax that sqlparser may
        // not handle or that our planner doesn't yet support fully.
        let rewritten = rewrite_pg_operators(query);
        let query = rewritten.as_str();

        let plan = exchange_query::plan_query(query).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(), // syntax_error
                e.to_string(),
            )))
        })?;

        let ctx = ExecutionContext {
            db_root: self.db_root.clone(),
            security: None,
            resource_mgr: None,
            query_id: 0,
            start_time: std::time::Instant::now(),
            use_wal: self.replication_manager.is_some(),
            memory_tracker: None,
            deadline: None,
            use_cursor_engine: false,
            mvcc: None,
            rls: None,
            current_user: None,
            sql_text: Some(query.to_string()),
            audit_log: None,
            replication_manager: self.replication_manager.clone(),
            cancellation_token: None,
        };
        let result = exchange_query::execute_with_context(&ctx, &plan).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(), // internal_error
                e.to_string(),
            )))
        })?;

        match result {
            QueryResult::Rows { columns, rows } => {
                let field_infos = infer_field_infos(&columns, &rows);
                let schema = Arc::new(field_infos);

                // Encode all rows into DataRow messages.
                let mut data_rows = Vec::with_capacity(rows.len());
                for row in &rows {
                    let mut encoder = DataRowEncoder::new(schema.clone());
                    for value in row {
                        encode_value(&mut encoder, value)?;
                    }
                    data_rows.push(encoder.take_row());
                }

                let data_row_stream = stream::iter(data_rows.into_iter().map(Ok));
                let response = QueryResponse::new(schema, data_row_stream);

                Ok(vec![Response::Query(response)])
            }
            QueryResult::Ok { affected_rows } => {
                // Determine an appropriate command tag based on the query plan.
                let tag_name = infer_command_tag(query);
                let tag = Tag::new(tag_name).with_rows(affected_rows as usize);
                Ok(vec![Response::Execution(tag)])
            }
        }
    }
}

/// Infer the PostgreSQL command tag from the SQL query text.
///
/// PostgreSQL clients expect specific command tags like "INSERT", "CREATE TABLE",
/// etc. We do a simple prefix match on the trimmed, uppercased query.
pub fn infer_command_tag(query: &str) -> &'static str {
    let trimmed = query.trim();
    let upper: String = trimmed.chars().take(20).collect::<String>().to_uppercase();

    if upper.starts_with("INSERT") {
        "INSERT 0"
    } else if upper.starts_with("CREATE TABLE") {
        "CREATE TABLE"
    } else if upper.starts_with("CREATE") {
        "CREATE"
    } else if upper.starts_with("SELECT") {
        "SELECT"
    } else if upper.starts_with("UPDATE") {
        "UPDATE"
    } else if upper.starts_with("DELETE") {
        "DELETE"
    } else if upper.starts_with("DROP TABLE") {
        "DROP TABLE"
    } else if upper.starts_with("DROP") {
        "DROP"
    } else if upper.starts_with("TRUNCATE") {
        "TRUNCATE"
    } else if upper.starts_with("BEGIN") || upper.starts_with("START") {
        "BEGIN"
    } else if upper.starts_with("COMMIT") {
        "COMMIT"
    } else if upper.starts_with("ROLLBACK") {
        "ROLLBACK"
    } else if upper.starts_with("SET") {
        "SET"
    } else if upper.starts_with("SHOW") {
        "SHOW"
    } else {
        "OK"
    }
}

/// Convert an ExchangeDB ColumnType to a PostgreSQL type name string.
fn col_type_to_pg_name(col_type: ColumnType) -> String {
    match col_type {
        ColumnType::Boolean => "boolean".to_string(),
        ColumnType::I8 | ColumnType::I16 => "smallint".to_string(),
        ColumnType::I32 => "integer".to_string(),
        ColumnType::I64 => "bigint".to_string(),
        ColumnType::F32 => "real".to_string(),
        ColumnType::F64 => "double precision".to_string(),
        ColumnType::Timestamp | ColumnType::TimestampMicro | ColumnType::TimestampMilli => {
            "timestamp with time zone".to_string()
        }
        ColumnType::Symbol => "character varying".to_string(),
        ColumnType::Varchar | ColumnType::String => "text".to_string(),
        ColumnType::Binary => "bytea".to_string(),
        ColumnType::Uuid => "uuid".to_string(),
        ColumnType::Date => "date".to_string(),
        ColumnType::IPv4 | ColumnType::IPv6 => "inet".to_string(),
        ColumnType::Char => "character(1)".to_string(),
        ColumnType::Interval => "interval".to_string(),
        _ => format!("{:?}", col_type).to_lowercase(),
    }
}

/// Rewrite PostgreSQL-specific operator syntax to standard SQL operators
/// that sqlparser and our planner can handle.
///
/// Handles `OPERATOR(pg_catalog.~)`, `OPERATOR(pg_catalog.~~)`, etc.
fn rewrite_pg_operators(sql: &str) -> String {
    let mut result = sql.to_string();
    result = result.replace("OPERATOR(pg_catalog.~~)", "LIKE");
    result = result.replace("OPERATOR(pg_catalog.!~~)", "NOT LIKE");
    result = result.replace("OPERATOR(pg_catalog.~)", "~");
    result = result.replace("OPERATOR(pg_catalog.!~)", "!~");
    result = result.replace("OPERATOR(pg_catalog.~*)", "~*");
    result = result.replace("OPERATOR(pg_catalog.!~*)", "!~*");
    result = result.replace("OPERATOR(pg_catalog.=)", "=");
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pg_type_for_column_boolean() {
        assert_eq!(pg_type_for_column(ColumnType::Boolean), Type::BOOL);
    }

    #[test]
    fn test_pg_type_for_column_integers() {
        assert_eq!(pg_type_for_column(ColumnType::I8), Type::INT2);
        assert_eq!(pg_type_for_column(ColumnType::I16), Type::INT2);
        assert_eq!(pg_type_for_column(ColumnType::I32), Type::INT4);
        assert_eq!(pg_type_for_column(ColumnType::I64), Type::INT8);
    }

    #[test]
    fn test_pg_type_for_column_floats() {
        assert_eq!(pg_type_for_column(ColumnType::F32), Type::FLOAT4);
        assert_eq!(pg_type_for_column(ColumnType::F64), Type::FLOAT8);
    }

    #[test]
    fn test_pg_type_for_column_timestamp() {
        assert_eq!(pg_type_for_column(ColumnType::Timestamp), Type::TIMESTAMPTZ);
    }

    #[test]
    fn test_pg_type_for_column_strings() {
        assert_eq!(pg_type_for_column(ColumnType::Symbol), Type::VARCHAR);
        assert_eq!(pg_type_for_column(ColumnType::Varchar), Type::TEXT);
    }

    #[test]
    fn test_pg_type_for_column_binary() {
        assert_eq!(pg_type_for_column(ColumnType::Binary), Type::BYTEA);
    }

    #[test]
    fn test_pg_type_for_column_uuid() {
        assert_eq!(pg_type_for_column(ColumnType::Uuid), Type::UUID);
    }

    #[test]
    fn test_pg_type_for_column_date() {
        assert_eq!(pg_type_for_column(ColumnType::Date), Type::DATE);
    }

    #[test]
    fn test_pg_type_for_column_ipv4() {
        assert_eq!(pg_type_for_column(ColumnType::IPv4), Type::INET);
    }

    #[test]
    fn test_pg_type_for_column_geohash() {
        assert_eq!(pg_type_for_column(ColumnType::GeoHash), Type::INT8);
    }

    #[test]
    fn test_pg_type_for_column_long_types() {
        assert_eq!(pg_type_for_column(ColumnType::Long128), Type::TEXT);
        assert_eq!(pg_type_for_column(ColumnType::Long256), Type::TEXT);
    }

    #[test]
    fn test_pg_type_for_column_char() {
        assert_eq!(pg_type_for_column(ColumnType::Char), Type::CHAR);
    }

    #[test]
    fn test_infer_command_tag() {
        assert_eq!(infer_command_tag("INSERT INTO t VALUES (1)"), "INSERT 0");
        assert_eq!(infer_command_tag("CREATE TABLE t (id INT)"), "CREATE TABLE");
        assert_eq!(infer_command_tag("SELECT 1"), "SELECT");
        assert_eq!(infer_command_tag("DROP TABLE t"), "DROP TABLE");
        assert_eq!(infer_command_tag("SOMETHING ELSE"), "OK");
    }
}
