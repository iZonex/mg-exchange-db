use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExchangeDbError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("table '{0}' not found")]
    TableNotFound(String),

    /// Table not found with additional context (database path).
    #[error("table '{table}' not found in database at '{db_path}'")]
    TableNotFoundAt { table: String, db_path: String },

    #[error("table '{0}' already exists")]
    TableAlreadyExists(String),

    #[error("column '{0}' not found in table '{1}'")]
    ColumnNotFound(String, String),

    /// Column not found with query context.
    #[error("column '{column}' not found in table '{table}'; available columns: {available}")]
    ColumnNotFoundDetailed {
        column: String,
        table: String,
        available: String,
    },

    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    /// Type mismatch with column context.
    #[error(
        "type mismatch for column '{column}' in table '{table}': expected {expected}, got {actual}"
    )]
    TypeMismatchInColumn {
        column: String,
        table: String,
        expected: String,
        actual: String,
    },

    #[error("invalid partition: {0}")]
    InvalidPartition(String),

    /// Invalid partition with table context.
    #[error("invalid partition '{partition}' for table '{table}': {reason}")]
    InvalidPartitionDetailed {
        partition: String,
        table: String,
        reason: String,
    },

    #[error("transaction conflict: {0}")]
    TxnConflict(String),

    #[error("WAL error: {0}")]
    Wal(String),

    /// WAL error with table and segment context.
    #[error("WAL error in table '{table}', segment {segment}: {detail}")]
    WalDetailed {
        table: String,
        segment: u32,
        detail: String,
    },

    #[error("query error: {0}")]
    Query(String),

    /// Query error with SQL context.
    #[error("query error: {detail} [SQL: {sql}]")]
    QueryDetailed { detail: String, sql: String },

    #[error("parse error: {0}")]
    Parse(String),

    /// Parse error with position context.
    #[error("parse error at position {position}: {detail} [SQL: {sql}]")]
    ParseDetailed {
        detail: String,
        sql: String,
        position: usize,
    },

    #[error("corrupted data: {0}")]
    Corruption(String),

    /// Corruption with file context.
    #[error("corrupted data in '{file}': {detail}")]
    CorruptionInFile { file: String, detail: String },

    #[error("column '{0}' already exists in table '{1}'")]
    ColumnAlreadyExists(String, String),

    #[error("cannot drop timestamp column '{0}' in table '{1}'")]
    CannotDropTimestampColumn(String, String),

    #[error("duplicate key found during deduplication")]
    DuplicateKey,

    /// Duplicate key with row context.
    #[error("duplicate key in table '{table}' at timestamp {timestamp}: {detail}")]
    DuplicateKeyDetailed {
        table: String,
        timestamp: i64,
        detail: String,
    },

    #[error("lock timeout: {0}")]
    LockTimeout(String),

    /// Lock timeout with table/partition context.
    #[error(
        "lock timeout on table '{table}', partition '{partition}' after {waited_ms}ms: {detail}"
    )]
    LockTimeoutDetailed {
        table: String,
        partition: String,
        waited_ms: u64,
        detail: String,
    },

    #[error("lock contention: {0}")]
    LockContention(String),

    #[error("permission denied: {0}")]
    PermissionDenied(String),

    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

    /// Snapshot/restore error with context.
    #[error("snapshot error: {detail} [path: {path}]")]
    Snapshot { detail: String, path: String },

    /// Recovery error with context.
    #[error("recovery error in table '{table}': {detail}")]
    Recovery { table: String, detail: String },

    /// Table writer is locked by another operation.
    #[error(
        "could not acquire writer for table '{table}': the table is currently locked by another concurrent write operation. Retry after the other operation completes."
    )]
    WriterLocked { table: String },

    /// Partition is not available for read-only access.
    #[error(
        "could not open partition '{partition}' of table '{table}' for reading: the partition may be detached, being compacted, or in an incomplete state. Check partition status with `SELECT * FROM table_partitions('{table}')`."
    )]
    PartitionReadOnly { table: String, partition: String },

    /// Invalid table name or alias with guidance.
    #[error(
        "invalid table name or alias '{name}': {reason}. Table and alias names must start with a letter or underscore, and contain only letters, digits, or underscores."
    )]
    InvalidTableNameOrAlias { name: String, reason: String },

    /// Disk is full or the filesystem has insufficient space for the requested
    /// operation.
    #[error(
        "disk full: need {needed_bytes} bytes but only {available_bytes} bytes available at '{path}'"
    )]
    DiskFull {
        path: String,
        needed_bytes: u64,
        available_bytes: u64,
    },
}

/// Helper to attach database path context to a TableNotFound error.
impl ExchangeDbError {
    /// Upgrade a plain `TableNotFound` into `TableNotFoundAt` with a database path.
    pub fn with_db_path(self, db_path: &std::path::Path) -> Self {
        match self {
            ExchangeDbError::TableNotFound(table) => ExchangeDbError::TableNotFoundAt {
                table,
                db_path: db_path.display().to_string(),
            },
            other => other,
        }
    }
}

pub type Result<T> = std::result::Result<T, ExchangeDbError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_locked_message() {
        let err = ExchangeDbError::WriterLocked {
            table: "trades".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("trades"), "should mention table name");
        assert!(msg.contains("locked"), "should mention locked");
        assert!(msg.contains("Retry"), "should suggest retry");
    }

    #[test]
    fn partition_read_only_message() {
        let err = ExchangeDbError::PartitionReadOnly {
            table: "trades".into(),
            partition: "2024-01".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("trades"), "should mention table");
        assert!(msg.contains("2024-01"), "should mention partition");
        assert!(msg.contains("detached"), "should mention possible causes");
        assert!(
            msg.contains("table_partitions"),
            "should suggest diagnostic"
        );
    }

    #[test]
    fn invalid_table_name_message() {
        let err = ExchangeDbError::InvalidTableNameOrAlias {
            name: "123bad".into(),
            reason: "starts with a digit".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("123bad"), "should show the invalid name");
        assert!(
            msg.contains("starts with a digit"),
            "should explain what's wrong"
        );
        assert!(
            msg.contains("must start with a letter"),
            "should give valid format"
        );
    }

    #[test]
    fn column_not_found_detailed_message() {
        let err = ExchangeDbError::ColumnNotFoundDetailed {
            column: "prce".into(),
            table: "trades".into(),
            available: "timestamp, price, symbol".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("prce"), "should show the wrong column name");
        assert!(msg.contains("trades"), "should show table");
        assert!(
            msg.contains("price"),
            "should list available columns for user to spot typo"
        );
    }

    #[test]
    fn lock_timeout_detailed_message() {
        let err = ExchangeDbError::LockTimeoutDetailed {
            table: "orders".into(),
            partition: "2024-03".into(),
            waited_ms: 5000,
            detail: "concurrent bulk insert in progress".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("orders"), "should mention table");
        assert!(msg.contains("5000ms"), "should show wait time");
    }
}
