//! Sequence support (CREATE SEQUENCE, nextval, currval, setval).

use crate::plan::{QueryResult, SequenceOpKind, Value};
use exchange_common::error::{ExchangeDbError, Result};
use std::path::Path;

/// Create a new sequence.
pub fn create_sequence(
    db_root: &Path,
    name: &str,
    start: i64,
    increment: i64,
) -> Result<QueryResult> {
    let seq_dir = db_root.join("_sequences");
    std::fs::create_dir_all(&seq_dir)?;
    let path = seq_dir.join(format!("{name}.json"));
    if path.exists() {
        return Err(ExchangeDbError::Query(format!(
            "sequence '{name}' already exists"
        )));
    }
    // Store start-increment so first nextval returns `start`.
    let initial_current = start - increment;
    let meta =
        format!("{{\"start\":{start},\"increment\":{increment},\"current\":{initial_current}}}");
    std::fs::write(&path, meta.as_bytes())?;
    Ok(QueryResult::Ok { affected_rows: 0 })
}

/// Drop a sequence.
pub fn drop_sequence(db_root: &Path, name: &str) -> Result<QueryResult> {
    let path = db_root.join("_sequences").join(format!("{name}.json"));
    if path.exists() {
        std::fs::remove_file(&path)?;
        Ok(QueryResult::Ok { affected_rows: 0 })
    } else {
        Err(ExchangeDbError::Query(format!(
            "sequence '{name}' not found"
        )))
    }
}

/// Execute a sequence operation (nextval / currval / setval).
pub fn execute_sequence_op(db_root: &Path, op: &SequenceOpKind) -> Result<QueryResult> {
    match op {
        SequenceOpKind::NextVal(name) => {
            let path = db_root.join("_sequences").join(format!("{name}.json"));
            if !path.exists() {
                return Err(ExchangeDbError::Query(format!(
                    "sequence '{name}' not found"
                )));
            }
            let content = std::fs::read_to_string(&path)?;
            let current = extract_json_i64(&content, "current").unwrap_or(0);
            let increment = extract_json_i64(&content, "increment").unwrap_or(1);
            let next = current + increment;
            let new_content = content.replace(
                &format!("\"current\":{current}"),
                &format!("\"current\":{next}"),
            );
            std::fs::write(&path, new_content.as_bytes())?;
            Ok(QueryResult::Rows {
                columns: vec!["nextval".to_string()],
                rows: vec![vec![Value::I64(next)]],
            })
        }
        SequenceOpKind::CurrVal(name) => {
            let path = db_root.join("_sequences").join(format!("{name}.json"));
            if !path.exists() {
                return Err(ExchangeDbError::Query(format!(
                    "sequence '{name}' not found"
                )));
            }
            let content = std::fs::read_to_string(&path)?;
            let current = extract_json_i64(&content, "current").unwrap_or(0);
            Ok(QueryResult::Rows {
                columns: vec!["currval".to_string()],
                rows: vec![vec![Value::I64(current)]],
            })
        }
        SequenceOpKind::SetVal(name, val) => {
            let path = db_root.join("_sequences").join(format!("{name}.json"));
            if !path.exists() {
                return Err(ExchangeDbError::Query(format!(
                    "sequence '{name}' not found"
                )));
            }
            let content = std::fs::read_to_string(&path)?;
            let current = extract_json_i64(&content, "current").unwrap_or(0);
            let new_content = content.replace(
                &format!("\"current\":{current}"),
                &format!("\"current\":{val}"),
            );
            std::fs::write(&path, new_content.as_bytes())?;
            Ok(QueryResult::Rows {
                columns: vec!["setval".to_string()],
                rows: vec![vec![Value::I64(*val)]],
            })
        }
    }
}

fn extract_json_i64(json: &str, key: &str) -> Option<i64> {
    let needle = format!("\"{}\":", key);
    let start = json.find(&needle)? + needle.len();
    let rest = &json[start..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit() && c != '-')
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn sequence_lifecycle() {
        let dir = TempDir::new().unwrap();
        let db = dir.path();

        create_sequence(db, "test_seq", 1, 1).unwrap();

        // First nextval returns start value (1).
        let r = execute_sequence_op(db, &SequenceOpKind::NextVal("test_seq".into())).unwrap();
        match r {
            QueryResult::Rows { rows, .. } => assert_eq!(rows[0][0], Value::I64(1)),
            _ => panic!("expected rows"),
        }

        // Second nextval returns 2.
        let r = execute_sequence_op(db, &SequenceOpKind::NextVal("test_seq".into())).unwrap();
        match r {
            QueryResult::Rows { rows, .. } => assert_eq!(rows[0][0], Value::I64(2)),
            _ => panic!("expected rows"),
        }

        // Currval returns current value without advancing.
        let r = execute_sequence_op(db, &SequenceOpKind::CurrVal("test_seq".into())).unwrap();
        match r {
            QueryResult::Rows { rows, .. } => assert_eq!(rows[0][0], Value::I64(2)),
            _ => panic!("expected rows"),
        }

        // Setval changes the current value.
        execute_sequence_op(db, &SequenceOpKind::SetVal("test_seq".into(), 100)).unwrap();
        let r = execute_sequence_op(db, &SequenceOpKind::NextVal("test_seq".into())).unwrap();
        match r {
            QueryResult::Rows { rows, .. } => assert_eq!(rows[0][0], Value::I64(101)),
            _ => panic!("expected rows"),
        }

        // Drop.
        drop_sequence(db, "test_seq").unwrap();
        assert!(execute_sequence_op(db, &SequenceOpKind::NextVal("test_seq".into())).is_err());
    }

    #[test]
    fn sequence_with_increment() {
        let dir = TempDir::new().unwrap();
        let db = dir.path();

        create_sequence(db, "step_seq", 10, 5).unwrap();

        let r = execute_sequence_op(db, &SequenceOpKind::NextVal("step_seq".into())).unwrap();
        match r {
            QueryResult::Rows { rows, .. } => assert_eq!(rows[0][0], Value::I64(10)),
            _ => panic!("expected rows"),
        }

        let r = execute_sequence_op(db, &SequenceOpKind::NextVal("step_seq".into())).unwrap();
        match r {
            QueryResult::Rows { rows, .. } => assert_eq!(rows[0][0], Value::I64(15)),
            _ => panic!("expected rows"),
        }
    }

    #[test]
    fn duplicate_create_fails() {
        let dir = TempDir::new().unwrap();
        let db = dir.path();

        create_sequence(db, "dup", 1, 1).unwrap();
        assert!(create_sequence(db, "dup", 1, 1).is_err());
    }
}
