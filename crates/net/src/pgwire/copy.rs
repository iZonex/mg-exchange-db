//! COPY IN protocol handler for pgwire.
//!
//! Supports `COPY table FROM STDIN WITH (FORMAT csv, HEADER true)` for
//! bulk CSV import via the PostgreSQL COPY protocol.

use std::fmt::Debug;
use std::path::PathBuf;

use async_trait::async_trait;
use futures::sink::Sink;
use pgwire::api::ClientInfo;
use pgwire::api::copy::CopyHandler;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::messages::copy::{CopyData, CopyDone, CopyFail};

use exchange_common::types::Timestamp;
use exchange_core::table::{ColumnValue, TableMeta, TableWriter};

/// Parsed COPY IN command options.
#[derive(Debug, Clone)]
pub struct CopyInOptions {
    /// Target table name.
    pub table: String,
    /// Whether the CSV data has a header row.
    pub header: bool,
    /// Column delimiter (default: comma).
    pub delimiter: char,
}

impl CopyInOptions {
    /// Parse a COPY command string.
    ///
    /// Accepted formats:
    /// - `COPY table FROM STDIN`
    /// - `COPY table FROM STDIN WITH (FORMAT csv)`
    /// - `COPY table FROM STDIN WITH (FORMAT csv, HEADER true)`
    /// - `COPY table FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ',')`
    pub fn parse(sql: &str) -> Option<Self> {
        let upper = sql.trim().to_ascii_uppercase();
        if !upper.starts_with("COPY ") || !upper.contains("FROM STDIN") {
            return None;
        }

        let tokens: Vec<&str> = sql.split_whitespace().collect();
        if tokens.len() < 4 {
            return None;
        }

        let table = tokens[1].to_string();
        let mut header = false;
        let mut delimiter = ',';

        // Parse WITH (...) options if present.
        if let Some(with_pos) = upper.find("WITH") {
            let options_part = &sql[with_pos..];
            let upper_options = options_part.to_ascii_uppercase();
            if upper_options.contains("HEADER TRUE") || upper_options.contains("HEADER ON") {
                header = true;
            }
            // Look for DELIMITER
            if let Some(delim_pos) = upper_options.find("DELIMITER") {
                let after = &options_part[delim_pos + 9..];
                let after = after
                    .trim()
                    .trim_start_matches('\'')
                    .trim_start_matches('"');
                if let Some(ch) = after.chars().next() {
                    delimiter = ch;
                }
            }
        }

        Some(CopyInOptions {
            table,
            header,
            delimiter,
        })
    }
}

/// Parse a single CSV line into field strings.
pub fn parse_csv_line(line: &str, delimiter: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '"' {
            if in_quotes {
                // Check for escaped quote (double-quote)
                if chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                in_quotes = true;
            }
        } else if ch == delimiter && !in_quotes {
            fields.push(current.clone());
            current.clear();
        } else {
            current.push(ch);
        }
    }
    fields.push(current);
    fields
}

/// Handler that processes COPY IN data for CSV import.
pub struct ExchangeDbCopyHandler {
    db_root: PathBuf,
    /// Accumulated CSV data from CopyData messages.
    buffer: std::sync::Mutex<Vec<u8>>,
    /// Current COPY options (set when a COPY command is received).
    options: std::sync::Mutex<Option<CopyInOptions>>,
}

impl ExchangeDbCopyHandler {
    pub fn new(db_root: PathBuf) -> Self {
        Self {
            db_root,
            buffer: std::sync::Mutex::new(Vec::new()),
            options: std::sync::Mutex::new(None),
        }
    }

    /// Set the COPY options for the current operation.
    pub fn set_options(&self, opts: CopyInOptions) {
        *self.options.lock().unwrap() = Some(opts);
    }

    /// Process the accumulated CSV buffer and write rows to the table.
    fn flush_buffer(&self) -> PgWireResult<u64> {
        let buffer = {
            let mut buf = self.buffer.lock().unwrap();
            std::mem::take(&mut *buf)
        };
        let opts = {
            let opts = self.options.lock().unwrap();
            opts.clone()
        };

        let opts = opts.ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "COPY IN: no options set".to_owned(),
            )))
        })?;

        let csv_text = String::from_utf8(buffer).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("invalid UTF-8 in COPY data: {e}"),
            )))
        })?;

        let lines: Vec<&str> = csv_text.lines().collect();
        if lines.is_empty() {
            return Ok(0);
        }

        let data_lines = if opts.header && !lines.is_empty() {
            &lines[1..]
        } else {
            &lines[..]
        };

        // Load table metadata to map CSV columns to table columns.
        let meta_path = self.db_root.join(&opts.table).join("_meta");
        let meta = TableMeta::load(&meta_path).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42P01".to_owned(),
                format!("table '{}' not found: {e}", opts.table),
            )))
        })?;

        let mut writer = TableWriter::open(&self.db_root, &opts.table).map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("failed to open table writer: {e}"),
            )))
        })?;

        let mut row_count = 0u64;
        for line in data_lines {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let fields = parse_csv_line(line, opts.delimiter);
            let ts = Timestamp::now();

            // Build column values: skip the timestamp column, map CSV
            // fields positionally to non-timestamp columns.
            let non_ts_cols: Vec<usize> = (0..meta.columns.len())
                .filter(|i| *i != meta.timestamp_column)
                .collect();

            let col_values: Vec<ColumnValue<'_>> = non_ts_cols
                .iter()
                .enumerate()
                .map(|(field_idx, _col_idx)| {
                    if field_idx < fields.len() {
                        let val = fields[field_idx].trim();
                        if val.is_empty() || val.eq_ignore_ascii_case("null") {
                            ColumnValue::Null
                        } else {
                            ColumnValue::Str(val)
                        }
                    } else {
                        ColumnValue::Null
                    }
                })
                .collect();

            writer.write_row(ts, &col_values).map_err(|e| {
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("write row error: {e}"),
                )))
            })?;
            row_count += 1;
        }

        writer.flush().map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("flush error: {e}"),
            )))
        })?;

        Ok(row_count)
    }
}

#[async_trait]
impl CopyHandler for ExchangeDbCopyHandler {
    async fn on_copy_data<C>(&self, _client: &mut C, copy_data: CopyData) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut buffer = self.buffer.lock().unwrap();
        buffer.extend_from_slice(&copy_data.data);
        Ok(())
    }

    async fn on_copy_done<C>(&self, client: &mut C, _done: CopyDone) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let row_count = self.flush_buffer()?;
        tracing::info!(rows = row_count, "COPY IN completed");

        // Send a command complete tag.
        use futures::SinkExt;
        let tag = format!("COPY {row_count}");
        client
            .send(PgWireBackendMessage::CommandComplete(
                pgwire::messages::response::CommandComplete::new(tag),
            ))
            .await?;

        // Clear state for next COPY.
        *self.options.lock().unwrap() = None;

        Ok(())
    }

    async fn on_copy_fail<C>(&self, _client: &mut C, fail: CopyFail) -> PgWireError
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Clear accumulated buffer.
        self.buffer.lock().unwrap().clear();
        *self.options.lock().unwrap() = None;

        PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "57014".to_owned(),
            format!("COPY IN cancelled: {}", fail.message),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_copy_command_basic() {
        let opts = CopyInOptions::parse("COPY trades FROM STDIN").unwrap();
        assert_eq!(opts.table, "trades");
        assert!(!opts.header);
        assert_eq!(opts.delimiter, ',');
    }

    #[test]
    fn test_parse_copy_command_with_options() {
        let opts =
            CopyInOptions::parse("COPY trades FROM STDIN WITH (FORMAT csv, HEADER true)").unwrap();
        assert_eq!(opts.table, "trades");
        assert!(opts.header);
    }

    #[test]
    fn test_parse_copy_command_not_copy() {
        assert!(CopyInOptions::parse("SELECT * FROM trades").is_none());
    }

    #[test]
    fn test_parse_csv_line_simple() {
        let fields = parse_csv_line("hello,world,42", ',');
        assert_eq!(fields, vec!["hello", "world", "42"]);
    }

    #[test]
    fn test_parse_csv_line_quoted() {
        let fields = parse_csv_line(r#""hello, world","test",42"#, ',');
        assert_eq!(fields, vec!["hello, world", "test", "42"]);
    }

    #[test]
    fn test_parse_csv_line_escaped_quotes() {
        let fields = parse_csv_line(r#""say ""hello""",test"#, ',');
        assert_eq!(fields, vec![r#"say "hello""#, "test"]);
    }

    #[test]
    fn test_parse_csv_line_tab_delimiter() {
        let fields = parse_csv_line("a\tb\tc", '\t');
        assert_eq!(fields, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_parse_copy_with_delimiter() {
        let opts = CopyInOptions::parse(
            "COPY trades FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER '|')",
        )
        .unwrap();
        assert_eq!(opts.delimiter, '|');
        assert!(opts.header);
    }
}
