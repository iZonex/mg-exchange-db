//! Reusable test infrastructure for the ExchangeDB query engine.
//!
//! Provides a `TestDb` helper that creates a temporary on-disk database
//! and exposes convenience methods for executing SQL in tests.

use std::path::{Path, PathBuf};

use tempfile::TempDir;

use exchange_common::error::ExchangeDbError;

use crate::plan::{QueryResult, Value};
use crate::{execute, plan_query};

/// A self-contained test database backed by a temporary directory.
///
/// The underlying directory is automatically cleaned up when this struct
/// is dropped. All SQL execution goes through the full parse -> plan ->
/// execute pipeline against real on-disk data.
pub struct TestDb {
    #[allow(dead_code)]
    dir: TempDir,
    db_root: PathBuf,
}

impl TestDb {
    /// Create an empty test database in a fresh temporary directory.
    pub fn new() -> Self {
        let dir = TempDir::new().expect("failed to create tempdir");
        let db_root = dir.path().to_path_buf();
        Self { dir, db_root }
    }

    /// Execute SQL and return the full `QueryResult`.
    pub fn exec(&self, sql: &str) -> Result<QueryResult, ExchangeDbError> {
        let plan = plan_query(sql)?;
        execute(&self.db_root, &plan)
    }

    /// Execute SQL, expect success, return the row count / affected rows.
    pub fn exec_ok(&self, sql: &str) -> u64 {
        let result = self
            .exec(sql)
            .unwrap_or_else(|e| panic!("exec_ok failed for `{sql}`: {e}"));
        match result {
            QueryResult::Ok { affected_rows } => affected_rows,
            QueryResult::Rows { rows, .. } => rows.len() as u64,
        }
    }

    /// Execute SQL, expect rows, return `(column_names, rows)`.
    pub fn query(&self, sql: &str) -> (Vec<String>, Vec<Vec<Value>>) {
        match self
            .exec(sql)
            .unwrap_or_else(|e| panic!("query failed for `{sql}`: {e}"))
        {
            QueryResult::Rows { columns, rows } => (columns, rows),
            other => panic!("expected Rows result for `{sql}`, got: {other:?}"),
        }
    }

    /// Execute SQL, expect a single scalar value.
    pub fn query_scalar(&self, sql: &str) -> Value {
        let (_, rows) = self.query(sql);
        assert!(
            !rows.is_empty(),
            "query_scalar: expected at least one row for `{sql}`"
        );
        assert!(
            !rows[0].is_empty(),
            "query_scalar: expected at least one column for `{sql}`"
        );
        rows[0][0].clone()
    }

    /// Execute SQL, expect an error, and return it.
    pub fn exec_err(&self, sql: &str) -> ExchangeDbError {
        self.exec(sql)
            .expect_err(&format!("expected error for `{sql}`, but got Ok"))
    }

    /// Create a test database with a standard `trades` table populated with sample data.
    ///
    /// Schema: `timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE, side VARCHAR`
    ///
    /// The table contains `rows` data rows spread across multiple days.
    pub fn with_trades(rows: u64) -> Self {
        let db = Self::new();

        db.exec_ok(
            "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE, side VARCHAR)",
        );

        let base_ts: i64 = 1710460800_000_000_000; // 2024-03-15 00:00:00 UTC in nanos
        let symbols = ["BTC/USD", "ETH/USD", "SOL/USD"];
        let sides = ["buy", "sell"];

        for i in 0..rows {
            let ts_nanos = base_ts + (i as i64) * 600_000_000_000; // every 10 minutes
            let symbol = symbols[(i as usize) % 3];
            let side = sides[(i as usize) % 2];
            let price = match symbol {
                "BTC/USD" => 60000.0 + (i as f64) * 100.0,
                "ETH/USD" => 3000.0 + (i as f64) * 10.0,
                _ => 100.0 + (i as f64),
            };
            let volume = if i % 10 == 0 {
                "NULL".to_string()
            } else {
                format!("{:.1}", 0.5 + (i as f64) * 0.1)
            };

            db.exec_ok(&format!(
                "INSERT INTO trades (timestamp, symbol, price, volume, side) VALUES ({ts_nanos}, '{symbol}', {price:.2}, {volume}, '{side}')"
            ));
        }

        db
    }

    /// Create a test database with `trades` and `quotes` tables for join tests.
    ///
    /// - `trades`: 20 rows with timestamp, symbol, price, volume, side
    /// - `quotes`: 20 rows with timestamp, symbol, bid, ask
    pub fn with_trades_and_quotes() -> Self {
        let db = Self::with_trades(20);

        db.exec_ok(
            "CREATE TABLE quotes (timestamp TIMESTAMP, symbol VARCHAR, bid DOUBLE, ask DOUBLE)",
        );

        let base_ts: i64 = 1710460800_000_000_000;
        let symbols = ["BTC/USD", "ETH/USD", "SOL/USD"];

        for i in 0..20u64 {
            let ts_nanos = base_ts + (i as i64) * 600_000_000_000;
            let symbol = symbols[(i as usize) % 3];
            let mid = match symbol {
                "BTC/USD" => 60000.0 + (i as f64) * 100.0,
                "ETH/USD" => 3000.0 + (i as f64) * 10.0,
                _ => 100.0 + (i as f64),
            };
            let bid = mid - 5.0;
            let ask = mid + 5.0;

            db.exec_ok(&format!(
                "INSERT INTO quotes (timestamp, symbol, bid, ask) VALUES ({ts_nanos}, '{symbol}', {bid:.2}, {ask:.2})"
            ));
        }

        db
    }

    /// Get the database root path.
    pub fn path(&self) -> &Path {
        &self.db_root
    }
}

impl Default for TestDb {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_empty_db() {
        let db = TestDb::new();
        assert!(db.path().exists());
    }

    #[test]
    fn test_exec_create_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
        let (cols, rows) = db.query("SELECT * FROM test");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_exec_insert_and_query() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
        db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 42.5)");
        let (_, rows) = db.query("SELECT value FROM test");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::F64(42.5));
    }

    #[test]
    fn test_query_scalar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE test (timestamp TIMESTAMP, value DOUBLE)");
        db.exec_ok("INSERT INTO test (timestamp, value) VALUES (1000000000000, 99.0)");
        let val = db.query_scalar("SELECT value FROM test");
        assert_eq!(val, Value::F64(99.0));
    }

    #[test]
    fn test_exec_err() {
        let db = TestDb::new();
        let err = db.exec_err("SELECT * FROM nonexistent_table");
        assert!(
            matches!(err, ExchangeDbError::TableNotFound(_)),
            "expected TableNotFound, got: {err:?}"
        );
    }

    #[test]
    fn test_with_trades() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn test_with_trades_and_quotes() {
        let db = TestDb::with_trades_and_quotes();
        let (_, trade_rows) = db.query("SELECT * FROM trades");
        assert_eq!(trade_rows.len(), 20);
        let (_, quote_rows) = db.query("SELECT * FROM quotes");
        assert_eq!(quote_rows.len(), 20);
    }

    #[test]
    fn test_path_returns_valid_dir() {
        let db = TestDb::new();
        assert!(db.path().is_dir());
    }
}
