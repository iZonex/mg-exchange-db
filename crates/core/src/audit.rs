//! Audit logging for ExchangeDB.
//!
//! Records security-relevant events as newline-delimited JSON (NDJSON)
//! in daily log files under `<db_root>/_audit/`.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use exchange_common::error::{ExchangeDbError, Result};
use serde::{Deserialize, Serialize};

/// Manages audit log files with daily rotation.
pub struct AuditLog {
    log_dir: PathBuf,
    /// The currently open writer and the date string it belongs to.
    writer: Mutex<Option<(String, BufWriter<File>)>>,
}

/// A single audit log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    /// Unix timestamp in seconds.
    pub timestamp: i64,
    /// Username that initiated the action.
    pub user: String,
    /// The action being performed.
    pub action: AuditAction,
    /// Table affected (if applicable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// The SQL query (if applicable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Outcome of the action.
    pub result: AuditResult,
    /// Client IP address (if known).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<String>,
}

/// Categories of auditable actions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditAction {
    Query,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    AlterTable,
    CreateUser,
    DropUser,
    Grant,
    Revoke,
    Login,
    LoginFailed,
    Snapshot,
    Vacuum,
    Replication,
}

/// The outcome of an audited action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuditResult {
    Success,
    Denied,
    Error(String),
}

impl AuditLog {
    /// Open or create the audit log directory under `<db_root>/_audit/`.
    pub fn open(db_root: &Path) -> Result<Self> {
        let log_dir = db_root.join("_audit");
        fs::create_dir_all(&log_dir)?;
        Ok(Self {
            log_dir,
            writer: Mutex::new(None),
        })
    }

    /// Append an entry to the audit log.
    ///
    /// Automatically rotates to a new file when the date changes.
    pub fn log(&self, entry: AuditEntry) -> Result<()> {
        let date_str = date_from_timestamp(entry.timestamp);
        let line = serde_json::to_string(&entry)
            .map_err(|e| ExchangeDbError::Query(format!("audit serialize error: {e}")))?;

        let mut guard = self
            .writer
            .lock()
            .map_err(|e| ExchangeDbError::Query(format!("audit log lock poisoned: {e}")))?;

        // Rotate if necessary.
        let needs_rotate = match &*guard {
            Some((current_date, _)) => current_date != &date_str,
            None => true,
        };

        if needs_rotate {
            let path = self.log_dir.join(format!("audit-{date_str}.log"));
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            *guard = Some((date_str, BufWriter::new(file)));
        }

        if let Some((_, ref mut writer)) = *guard {
            writeln!(writer, "{line}")?;
            writer.flush()?;
        }

        Ok(())
    }

    /// Query audit entries within a time range `[from, to]` (inclusive, Unix seconds).
    ///
    /// Scans all relevant daily log files and returns matching entries.
    pub fn query_log(&self, from: i64, to: i64) -> Result<Vec<AuditEntry>> {
        let from_date = date_from_timestamp(from);
        let to_date = date_from_timestamp(to);

        let mut entries = Vec::new();

        let dir_entries = fs::read_dir(&self.log_dir)?;
        for entry in dir_entries {
            let entry = entry?;
            let name = entry.file_name();
            let name_str = name.to_string_lossy();

            // Files are named "audit-YYYY-MM-DD.log"
            if !name_str.starts_with("audit-") || !name_str.ends_with(".log") {
                continue;
            }
            let file_date = &name_str[6..name_str.len() - 4];
            if file_date < from_date.as_str() || file_date > to_date.as_str() {
                continue;
            }

            let file = File::open(entry.path())?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let audit_entry: AuditEntry = serde_json::from_str(&line)
                    .map_err(|e| ExchangeDbError::Query(format!("audit parse error: {e}")))?;
                if audit_entry.timestamp >= from && audit_entry.timestamp <= to {
                    entries.push(audit_entry);
                }
            }
        }

        entries.sort_by_key(|e| e.timestamp);
        Ok(entries)
    }
}

/// Convert a Unix timestamp (seconds) to a `YYYY-MM-DD` string.
fn date_from_timestamp(ts: i64) -> String {
    // Manual UTC date calculation to avoid pulling in chrono.
    let days = ts / 86400;
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_conversion() {
        // 2024-01-15 00:00:00 UTC = 1705276800
        assert_eq!(date_from_timestamp(1705276800), "2024-01-15");
        // 2023-11-14 22:13:20 UTC = 1700000000
        assert_eq!(date_from_timestamp(1700000000), "2023-11-14");
    }

    #[test]
    fn write_and_read_audit_log() {
        let tmp = tempfile::tempdir().unwrap();
        let audit = AuditLog::open(tmp.path()).unwrap();

        let entry1 = AuditEntry {
            timestamp: 1700000000,
            user: "alice".to_string(),
            action: AuditAction::Query,
            table: Some("trades".to_string()),
            query: Some("SELECT * FROM trades".to_string()),
            result: AuditResult::Success,
            client_ip: Some("127.0.0.1".to_string()),
        };

        let entry2 = AuditEntry {
            timestamp: 1700000060,
            user: "bob".to_string(),
            action: AuditAction::Insert,
            table: Some("orders".to_string()),
            query: None,
            result: AuditResult::Success,
            client_ip: None,
        };

        let entry3 = AuditEntry {
            timestamp: 1700000120,
            user: "eve".to_string(),
            action: AuditAction::LoginFailed,
            table: None,
            query: None,
            result: AuditResult::Denied,
            client_ip: Some("10.0.0.5".to_string()),
        };

        audit.log(entry1).unwrap();
        audit.log(entry2).unwrap();
        audit.log(entry3).unwrap();

        // Read back all entries.
        let results = audit.query_log(1700000000, 1700000200).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].user, "alice");
        assert_eq!(results[0].action, AuditAction::Query);
        assert_eq!(results[1].user, "bob");
        assert_eq!(results[2].user, "eve");
        assert_eq!(results[2].result, AuditResult::Denied);

        // Range filter: only the first entry.
        let results = audit.query_log(1700000000, 1700000030).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].user, "alice");
    }

    #[test]
    fn audit_log_across_days() {
        let tmp = tempfile::tempdir().unwrap();
        let audit = AuditLog::open(tmp.path()).unwrap();

        // Day 1: 2024-01-15
        let e1 = AuditEntry {
            timestamp: 1705276800,
            user: "alice".to_string(),
            action: AuditAction::Login,
            table: None,
            query: None,
            result: AuditResult::Success,
            client_ip: None,
        };

        // Day 2: 2024-01-16
        let e2 = AuditEntry {
            timestamp: 1705363200,
            user: "bob".to_string(),
            action: AuditAction::CreateTable,
            table: Some("orders".to_string()),
            query: None,
            result: AuditResult::Success,
            client_ip: None,
        };

        audit.log(e1).unwrap();
        audit.log(e2).unwrap();

        // Query spanning both days.
        let results = audit.query_log(1705276800, 1705363200).unwrap();
        assert_eq!(results.len(), 2);

        // Query only day 1.
        let results = audit.query_log(1705276800, 1705276800 + 86399).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].user, "alice");
    }

    #[test]
    fn audit_error_result() {
        let tmp = tempfile::tempdir().unwrap();
        let audit = AuditLog::open(tmp.path()).unwrap();

        let entry = AuditEntry {
            timestamp: 1700000000,
            user: "alice".to_string(),
            action: AuditAction::DropTable,
            table: Some("trades".to_string()),
            query: Some("DROP TABLE trades".to_string()),
            result: AuditResult::Error("table not found".to_string()),
            client_ip: None,
        };

        audit.log(entry).unwrap();

        let results = audit.query_log(1700000000, 1700000000).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].result,
            AuditResult::Error("table not found".to_string())
        );
    }
}
