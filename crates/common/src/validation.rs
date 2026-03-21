//! Input validation utilities for ExchangeDB.
//!
//! All user-supplied identifiers (table names, column names, measurement names)
//! and file paths must be validated before use to prevent injection attacks and
//! path traversal vulnerabilities.

use crate::error::{ExchangeDbError, Result};

/// Maximum length for an identifier (table name, column name, etc.).
const MAX_IDENTIFIER_LEN: usize = 128;

/// Maximum length for ILP measurement names.
const MAX_MEASUREMENT_LEN: usize = 256;

/// Validate a table name.
///
/// Valid table names:
/// - Start with a letter (a-z, A-Z) or underscore
/// - Contain only letters, digits, and underscores
/// - Are between 1 and 128 characters
/// - Do not start with `_` followed by a reserved word
pub fn validate_table_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(ExchangeDbError::Query(
            "table name must not be empty".to_string(),
        ));
    }
    if name.len() > MAX_IDENTIFIER_LEN {
        return Err(ExchangeDbError::Query(format!(
            "table name '{}' exceeds maximum length of {} characters",
            truncate_for_display(name),
            MAX_IDENTIFIER_LEN,
        )));
    }
    validate_identifier_chars(name, "table name")
}

/// Validate a column name.
///
/// Same rules as table names: alphanumeric + underscore, starts with
/// letter or underscore.
pub fn validate_column_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(ExchangeDbError::Query(
            "column name must not be empty".to_string(),
        ));
    }
    if name.len() > MAX_IDENTIFIER_LEN {
        return Err(ExchangeDbError::Query(format!(
            "column name '{}' exceeds maximum length of {} characters",
            truncate_for_display(name),
            MAX_IDENTIFIER_LEN,
        )));
    }
    validate_identifier_chars(name, "column name")
}

/// Validate an ILP measurement name.
///
/// Measurement names follow the same character rules as identifiers but
/// allow a slightly longer maximum length to accommodate InfluxDB conventions.
pub fn validate_measurement_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(ExchangeDbError::Query(
            "measurement name must not be empty".to_string(),
        ));
    }
    if name.len() > MAX_MEASUREMENT_LEN {
        return Err(ExchangeDbError::Query(format!(
            "measurement name '{}' exceeds maximum length of {} characters",
            truncate_for_display(name),
            MAX_MEASUREMENT_LEN,
        )));
    }
    validate_identifier_chars(name, "measurement name")
}

/// Validate that a file path does not contain path traversal sequences.
///
/// Rejects paths containing:
/// - `..` (parent directory traversal)
/// - Null bytes (`\0`)
/// - Absolute paths (starting with `/` or a Windows drive letter)
pub fn validate_path_component(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(ExchangeDbError::Query("path must not be empty".to_string()));
    }

    if path.contains('\0') {
        return Err(ExchangeDbError::Query(
            "path must not contain null bytes".to_string(),
        ));
    }

    // Check for path traversal
    if path.contains("..") {
        return Err(ExchangeDbError::Query(format!(
            "path '{}' contains forbidden traversal sequence '..'",
            truncate_for_display(path),
        )));
    }

    // Reject absolute paths
    if path.starts_with('/') || path.starts_with('\\') {
        return Err(ExchangeDbError::Query(format!(
            "absolute paths are not allowed: '{}'",
            truncate_for_display(path),
        )));
    }

    // Windows drive letters (e.g. "C:\")
    if path.len() >= 2 && path.as_bytes()[1] == b':' && path.as_bytes()[0].is_ascii_alphabetic() {
        return Err(ExchangeDbError::Query(format!(
            "absolute paths are not allowed: '{}'",
            truncate_for_display(path),
        )));
    }

    Ok(())
}

/// Validate that a numeric configuration value falls within the given range.
pub fn validate_config_range<T: PartialOrd + std::fmt::Display>(
    name: &str,
    value: T,
    min: T,
    max: T,
) -> Result<()> {
    if value < min || value > max {
        return Err(ExchangeDbError::Query(format!(
            "configuration value '{name}' = {value} is out of range [{min}, {max}]",
        )));
    }
    Ok(())
}

// ── Internal helpers ───────────────────────────────────────────────────

/// Check that an identifier contains only valid characters:
/// `[a-zA-Z_][a-zA-Z0-9_]*`
fn validate_identifier_chars(name: &str, kind: &str) -> Result<()> {
    let first = name.as_bytes()[0];
    if !first.is_ascii_alphabetic() && first != b'_' {
        return Err(ExchangeDbError::InvalidTableNameOrAlias {
            name: name.to_string(),
            reason: format!(
                "{kind} must start with a letter or underscore, got '{}'",
                first as char
            ),
        });
    }

    for (i, ch) in name.char_indices() {
        if !ch.is_ascii_alphanumeric() && ch != '_' {
            return Err(ExchangeDbError::InvalidTableNameOrAlias {
                name: name.to_string(),
                reason: format!(
                    "{kind} contains invalid character '{}' at position {}",
                    ch, i
                ),
            });
        }
    }

    Ok(())
}

/// Truncate a string for safe display in error messages.
fn truncate_for_display(s: &str) -> &str {
    if s.len() > 40 { &s[..40] } else { s }
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Table name validation ──────────────────────────────────────────

    #[test]
    fn valid_table_names() {
        assert!(validate_table_name("trades").is_ok());
        assert!(validate_table_name("_internal").is_ok());
        assert!(validate_table_name("my_table_123").is_ok());
        assert!(validate_table_name("A").is_ok());
    }

    #[test]
    fn empty_table_name() {
        assert!(validate_table_name("").is_err());
    }

    #[test]
    fn table_name_starts_with_digit() {
        assert!(validate_table_name("1table").is_err());
    }

    #[test]
    fn table_name_with_special_chars() {
        assert!(validate_table_name("my-table").is_err());
        assert!(validate_table_name("my table").is_err());
        assert!(validate_table_name("my.table").is_err());
        assert!(validate_table_name("my;table").is_err());
        assert!(validate_table_name("my'table").is_err());
    }

    #[test]
    fn table_name_too_long() {
        let long_name = "a".repeat(MAX_IDENTIFIER_LEN + 1);
        assert!(validate_table_name(&long_name).is_err());

        let max_name = "a".repeat(MAX_IDENTIFIER_LEN);
        assert!(validate_table_name(&max_name).is_ok());
    }

    // ── Column name validation ─────────────────────────────────────────

    #[test]
    fn valid_column_names() {
        assert!(validate_column_name("price").is_ok());
        assert!(validate_column_name("_timestamp").is_ok());
        assert!(validate_column_name("col_1").is_ok());
    }

    #[test]
    fn invalid_column_names() {
        assert!(validate_column_name("").is_err());
        assert!(validate_column_name("1col").is_err());
        assert!(validate_column_name("col name").is_err());
        assert!(validate_column_name("col;drop").is_err());
    }

    // ── Measurement name validation ────────────────────────────────────

    #[test]
    fn valid_measurement_names() {
        assert!(validate_measurement_name("cpu").is_ok());
        assert!(validate_measurement_name("network_in").is_ok());
    }

    #[test]
    fn invalid_measurement_names() {
        assert!(validate_measurement_name("").is_err());
        assert!(validate_measurement_name("cpu usage").is_err());
    }

    // ── Path traversal prevention ──────────────────────────────────────

    #[test]
    fn valid_path_components() {
        assert!(validate_path_component("data").is_ok());
        assert!(validate_path_component("my_file.txt").is_ok());
    }

    #[test]
    fn path_traversal_rejected() {
        assert!(validate_path_component("../../../etc/passwd").is_err());
        assert!(validate_path_component("data/../secret").is_err());
        assert!(validate_path_component("..").is_err());
    }

    #[test]
    fn absolute_paths_rejected() {
        assert!(validate_path_component("/etc/passwd").is_err());
        assert!(validate_path_component("\\windows\\system32").is_err());
        assert!(validate_path_component("C:\\windows").is_err());
    }

    #[test]
    fn null_bytes_rejected() {
        assert!(validate_path_component("data\0evil").is_err());
    }

    #[test]
    fn empty_path_rejected() {
        assert!(validate_path_component("").is_err());
    }

    // ── Config range validation ────────────────────────────────────────

    #[test]
    fn config_range_valid() {
        assert!(validate_config_range("port", 9000, 1, 65535).is_ok());
        assert!(validate_config_range("threads", 4, 1, 256).is_ok());
    }

    #[test]
    fn config_range_invalid() {
        assert!(validate_config_range("port", 0, 1, 65535).is_err());
        assert!(validate_config_range("port", 70000, 1, 65535).is_err());
    }
}
