//! Extended query protocol handler for pgwire.
//!
//! Implements `ExtendedQueryHandler` so that PostgreSQL clients using prepared
//! statements (e.g., PgBouncer, JDBC, ORMs) can execute queries against
//! ExchangeDB.

use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::Sink;
use futures::stream;
use pgwire::api::portal::Portal;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeStatementResponse, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{QueryParser, StoredStatement};
use pgwire::api::{ClientInfo, ClientPortalStore, Type};
use pgwire::api::store::PortalStore;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use exchange_core::replication::ReplicationManager;
use exchange_query::{ExecutionContext, QueryResult};

use super::handler::{encode_value, infer_command_tag, infer_field_infos};

// ---------------------------------------------------------------------------
// Parsed statement type
// ---------------------------------------------------------------------------

/// A parsed SQL statement stored in the statement cache.
#[derive(Debug, Clone)]
pub struct ParsedStatement {
    /// The original SQL query text.
    pub sql: String,
    /// Parameter type hints provided by the client (may be empty).
    pub parameter_types: Vec<Type>,
}

// ---------------------------------------------------------------------------
// Query parser
// ---------------------------------------------------------------------------

/// Parser that converts SQL text into our `ParsedStatement`.
#[derive(Debug, Clone)]
pub struct ExchangeDbQueryParser;

#[async_trait]
impl QueryParser for ExchangeDbQueryParser {
    type Statement = ParsedStatement;

    async fn parse_sql(&self, sql: &str, types: &[Type]) -> PgWireResult<Self::Statement> {
        // Validate that the SQL can be planned (catches syntax errors early).
        // We accept parameter placeholders ($1, $2, ...) by not rejecting them
        // here — they will be substituted at execution time.
        //
        // For now, we do a lightweight validation: only fully plan the query
        // if it has no parameter placeholders.
        if !sql.contains('$') {
            let _ = exchange_query::plan_query(sql).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42601".to_owned(),
                    e.to_string(),
                )))
            })?;
        }

        Ok(ParsedStatement {
            sql: sql.to_owned(),
            parameter_types: types.to_vec(),
        })
    }
}

// ---------------------------------------------------------------------------
// Extended query handler
// ---------------------------------------------------------------------------

/// Handler implementing the PostgreSQL extended query protocol for ExchangeDB.
pub struct ExchangeDbExtendedHandler {
    db_root: PathBuf,
    replication_manager: Option<Arc<ReplicationManager>>,
    parser: Arc<ExchangeDbQueryParser>,
}

impl ExchangeDbExtendedHandler {
    /// Create a new extended query handler rooted at the given database directory.
    pub fn new(db_root: PathBuf, replication_manager: Option<Arc<ReplicationManager>>) -> Self {
        Self {
            db_root,
            replication_manager,
            parser: Arc::new(ExchangeDbQueryParser),
        }
    }
}

/// Substitute `$1`, `$2`, ... placeholders with their bound parameter values.
///
/// Parameters are transmitted as raw bytes by the client. We decode them as
/// text (UTF-8 strings) and splice them into the SQL text. This is intentionally
/// simple — full binary parameter decoding would require type-aware parsing.
fn substitute_parameters(sql: &str, portal: &Portal<ParsedStatement>) -> String {
    substitute_params_raw(sql, &portal.parameters)
}

fn substitute_params_raw(sql: &str, params: &[Option<bytes::Bytes>]) -> String {
    let mut result = sql.to_owned();

    // Replace in reverse order ($10 before $1) to avoid partial matches.
    for idx in (0..params.len()).rev() {
        let placeholder = format!("${}", idx + 1);
        let replacement = match &params[idx] {
            Some(bytes) => {
                match std::str::from_utf8(bytes) {
                    Ok(s) => {
                        // Only treat as numeric if it strictly parses as a finite number.
                        // This prevents NaN, Infinity, and other non-numeric strings
                        // from being inserted unquoted.
                        if is_safe_numeric_literal(s) {
                            s.to_owned()
                        } else {
                            format!("'{}'", s.replace('\'', "''"))
                        }
                    }
                    Err(_) => "NULL".to_owned(),
                }
            }
            None => "NULL".to_owned(),
        };
        result = result.replace(&placeholder, &replacement);
    }

    result
}

/// Check if a string is a safe numeric literal that can be inserted unquoted.
///
/// Rejects NaN, Infinity, hex literals, and other strings that could be
/// misinterpreted in SQL context.
fn is_safe_numeric_literal(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    // Try integer first (most common case)
    if s.parse::<i64>().is_ok() {
        return true;
    }
    // Try float, but reject non-finite values
    if let Ok(f) = s.parse::<f64>() {
        if f.is_finite() {
            // Also reject hex-like prefixes and other non-decimal representations
            let first = s.as_bytes()[0];
            return first == b'-' || first == b'+' || first.is_ascii_digit() || first == b'.';
        }
    }
    false
}

#[async_trait]
impl ExtendedQueryHandler for ExchangeDbExtendedHandler {
    type Statement = ParsedStatement;
    type QueryParser = ExchangeDbQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Report parameter types (the ones the client told us, or inferred as TEXT).
        let param_types = if stmt.parameter_types.is_empty() {
            // Count $N placeholders in the SQL to determine parameter count.
            let count = count_placeholders(&stmt.statement.sql);
            vec![Type::TEXT; count]
        } else {
            stmt.parameter_types.clone()
        };

        // Try to determine output columns by planning the query.
        // If the query has parameters, we cannot plan it without substitution,
        // so we return empty fields (the portal describe will have them).
        let fields = if !stmt.statement.sql.contains('$') {
            match plan_and_describe(&self.db_root, &stmt.statement.sql) {
                Ok(f) => f,
                Err(_) => vec![],
            }
        } else {
            vec![]
        };

        Ok(DescribeStatementResponse::new(param_types, fields))
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = substitute_parameters(&portal.statement.statement.sql, portal);

        let fields = match plan_and_describe(&self.db_root, &sql) {
            Ok(f) => f,
            Err(_) => vec![],
        };

        Ok(DescribePortalResponse::new(fields))
    }

    async fn do_query<'a, 'b: 'a, C>(
        &'b self,
        _client: &mut C,
        portal: &'a Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response<'a>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = substitute_parameters(&portal.statement.statement.sql, portal);

        let plan = exchange_query::plan_query(&sql).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
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
            sql_text: Some(sql.to_string()),
            audit_log: None,
            replication_manager: self.replication_manager.clone(),
            cancellation_token: None,
        };
        let result = exchange_query::execute_with_context(&ctx, &plan).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                e.to_string(),
            )))
        })?;

        match result {
            QueryResult::Rows { columns, rows } => {
                let field_infos = infer_field_infos(&columns, &rows);
                let schema = Arc::new(field_infos);

                let mut data_rows = Vec::with_capacity(rows.len());
                for row in &rows {
                    let mut encoder = DataRowEncoder::new(schema.clone());
                    for value in row {
                        encode_value(&mut encoder, value)?;
                    }
                    data_rows.push(encoder.finish()?);
                }

                let data_row_stream = stream::iter(data_rows.into_iter().map(Ok));
                let response = QueryResponse::new(schema, data_row_stream);

                Ok(Response::Query(response))
            }
            QueryResult::Ok { affected_rows } => {
                let tag_name = infer_command_tag(&sql);
                let tag = Tag::new(tag_name).with_rows(affected_rows as usize);
                Ok(Response::Execution(tag))
            }
        }
    }
}

/// Count the number of distinct `$N` placeholders in a SQL string.
fn count_placeholders(sql: &str) -> usize {
    let mut max = 0usize;
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '$' {
            let mut num_str = String::new();
            while let Some(&d) = chars.peek() {
                if d.is_ascii_digit() {
                    num_str.push(d);
                    chars.next();
                } else {
                    break;
                }
            }
            if let Ok(n) = num_str.parse::<usize>() {
                if n > max {
                    max = n;
                }
            }
        }
    }
    max
}

/// Plan a SQL query and return the output column field infos.
fn plan_and_describe(db_root: &std::path::Path, sql: &str) -> Result<Vec<FieldInfo>, String> {
    let plan = exchange_query::plan_query(sql).map_err(|e| e.to_string())?;
    let result = exchange_query::execute(db_root, &plan).map_err(|e| e.to_string())?;

    match result {
        QueryResult::Rows { columns, rows } => {
            Ok(infer_field_infos(&columns, &rows))
        }
        QueryResult::Ok { .. } => {
            // DDL/DML — no result columns.
            Ok(vec![])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_placeholders_none() {
        assert_eq!(count_placeholders("SELECT * FROM foo"), 0);
    }

    #[test]
    fn test_count_placeholders_sequential() {
        assert_eq!(count_placeholders("SELECT * FROM foo WHERE a = $1 AND b = $2"), 2);
    }

    #[test]
    fn test_count_placeholders_gaps() {
        assert_eq!(count_placeholders("SELECT * FROM foo WHERE a = $3"), 3);
    }

    #[test]
    fn test_substitute_no_params() {
        let result = substitute_params_raw("SELECT 1", &[]);
        assert_eq!(result, "SELECT 1");
    }

    #[test]
    fn test_substitute_with_params() {
        let params = vec![
            Some(bytes::Bytes::from("42")),
            Some(bytes::Bytes::from("hello")),
        ];
        let result = substitute_params_raw(
            "SELECT * FROM t WHERE a = $1 AND b = $2",
            &params,
        );
        assert_eq!(result, "SELECT * FROM t WHERE a = 42 AND b = 'hello'");
    }

    #[test]
    fn test_substitute_null_param() {
        let params = vec![None];
        let result = substitute_params_raw("SELECT * FROM t WHERE a = $1", &params);
        assert_eq!(result, "SELECT * FROM t WHERE a = NULL");
    }

    #[test]
    fn test_query_parser_simple_sql() {
        // Test that the query parser can parse a simple SQL statement.
        let parser = ExchangeDbQueryParser;
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(parser.parse_sql("SELECT 1", &[]));
        // This may succeed or fail depending on whether exchange_query supports
        // "SELECT 1" — we just verify it does not panic.
        let _ = result;
    }
}
