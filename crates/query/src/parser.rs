//! SQL parsing wrapper around `sqlparser`.
//!
//! Supports standard SQL statements (CREATE TABLE, INSERT INTO, SELECT)
//! plus the custom `SAMPLE BY` extension for time bucketing.

use exchange_common::error::{ExchangeDbError, Result};
use sqlparser::ast::Statement;
use sqlparser::dialect::{Dialect, GenericDialect};
use sqlparser::parser::Parser;

/// Custom dialect that extends GenericDialect with additional SQL features
/// needed by ExchangeDB (e.g. FILTER clause on aggregates, WITHIN GROUP).
#[derive(Debug)]
struct ExchangeDbDialect(GenericDialect);

impl Dialect for ExchangeDbDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        self.0.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        self.0.is_identifier_part(ch)
    }

    fn supports_filter_during_aggregation(&self) -> bool {
        true
    }

    fn supports_within_after_array_aggregation(&self) -> bool {
        true
    }

    fn supports_group_by_expr(&self) -> bool {
        true
    }
}

/// Parse a SQL string into a list of `sqlparser` AST statements.
///
/// The `SAMPLE BY <interval>` extension is handled by rewriting the SQL
/// before handing it to sqlparser: we strip the `SAMPLE BY ...` clause
/// and return it out-of-band so the planner can pick it up.
pub fn parse_sql(sql: &str) -> Result<ParsedQuery> {
    macro_rules! empty_pq {
        () => {
            ParsedQuery {
                statements: vec![],
                sample_by_raw: None,
                asof_join: None,
                latest_on: None,
                vacuum_table: None,
                create_matview: None,
                refresh_matview: None,
                drop_matview: None,
                rbac_command: None,
                show_command: None,
                sample_by_fill: None,
                sample_by_align_calendar: false,
                partition_command: None,
                pivot_info: None,
                merge_command: None,
                create_procedure: None,
                drop_procedure: None,
                call_procedure: None,
                create_downsampling: None,
                create_view: None,
                drop_view: None,
                create_trigger: None,
                drop_trigger: None,
                comment_on: None,
                designated_timestamp: None,
                partition_by_clause: None,
            }
        };
    }

    // Check for CREATE PROCEDURE / DROP PROCEDURE / CALL before sqlparser.
    if let Some((name, body)) = extract_create_procedure(sql) {
        let mut pq = empty_pq!();
        pq.create_procedure = Some((name, body));
        return Ok(pq);
    }
    if let Some(name) = extract_drop_procedure(sql) {
        let mut pq = empty_pq!();
        pq.drop_procedure = Some(name);
        return Ok(pq);
    }
    if let Some(name) = extract_call_procedure(sql) {
        let mut pq = empty_pq!();
        pq.call_procedure = Some(name);
        return Ok(pq);
    }

    // Check for CREATE DOWNSAMPLING before sqlparser.
    if let Some(info) = extract_create_downsampling(sql) {
        let mut pq = empty_pq!();
        pq.create_downsampling = Some(info);
        return Ok(pq);
    }

    // Check for CREATE VIEW / DROP VIEW before sqlparser.
    if let Some((name, view_sql)) = extract_create_view(sql) {
        let mut pq = empty_pq!();
        pq.create_view = Some((name, view_sql));
        return Ok(pq);
    }
    if let Some(name) = extract_drop_view(sql) {
        let mut pq = empty_pq!();
        pq.drop_view = Some(name);
        return Ok(pq);
    }

    // Check for CREATE TRIGGER / DROP TRIGGER before sqlparser.
    if let Some((name, table, proc_name)) = extract_create_trigger(sql) {
        let mut pq = empty_pq!();
        pq.create_trigger = Some((name, table, proc_name));
        return Ok(pq);
    }
    if let Some((name, table)) = extract_drop_trigger(sql) {
        let mut pq = empty_pq!();
        pq.drop_trigger = Some((name, table));
        return Ok(pq);
    }

    // Check for COMMENT ON before sqlparser.
    if let Some(info) = extract_comment_on(sql) {
        let mut pq = empty_pq!();
        pq.comment_on = Some(info);
        return Ok(pq);
    }

    // Check for SHOW / DESCRIBE commands before sqlparser.
    if let Some(show) = extract_show(sql) {
        let mut pq = empty_pq!();
        pq.show_command = Some(show);
        return Ok(pq);
    }

    // Check for RBAC commands before sqlparser.
    if let Some(rbac) = extract_rbac_command(sql) {
        let mut pq = empty_pq!();
        pq.rbac_command = Some(rbac);
        return Ok(pq);
    }

    // Check for ALTER TABLE ... DETACH/ATTACH/SQUASH PARTITION(S) before sqlparser.
    if let Some(cmd) = extract_partition_command(sql) {
        let mut pq = empty_pq!();
        pq.partition_command = Some(cmd);
        return Ok(pq);
    }

    // Check for VACUUM before sqlparser (it's not standard SQL).
    let vacuum_table = extract_vacuum(sql);
    if vacuum_table.is_some() {
        let mut pq = empty_pq!();
        pq.vacuum_table = vacuum_table;
        return Ok(pq);
    }

    // Check for CREATE MATERIALIZED VIEW before sqlparser.
    if let Some(cmv) = extract_create_matview(sql) {
        let mut pq = empty_pq!();
        pq.create_matview = Some(cmv);
        return Ok(pq);
    }

    // Check for REFRESH MATERIALIZED VIEW before sqlparser.
    if let Some(name) = extract_refresh_matview(sql) {
        let mut pq = empty_pq!();
        pq.refresh_matview = Some(name);
        return Ok(pq);
    }

    // Check for DROP MATERIALIZED VIEW before sqlparser.
    if let Some(name) = extract_drop_matview(sql) {
        let mut pq = empty_pq!();
        pq.drop_matview = Some(name);
        return Ok(pq);
    }

    // Check for MERGE before sqlparser.
    if let Some(merge) = extract_merge(sql) {
        let mut pq = empty_pq!();
        pq.merge_command = Some(merge);
        return Ok(pq);
    }

    // Pre-process QuestDB-style TIMESTAMP(col) and PARTITION BY <strategy>
    // clauses from CREATE TABLE before passing to sqlparser.
    let (rewritten_ts, designated_timestamp, partition_by_clause) = extract_questdb_create_table_clauses(sql);

    let (rewritten, pivot_info) = extract_pivot(&rewritten_ts);
    let (rewritten, sample_by_raw, sample_by_fill, sample_by_align_calendar) = extract_sample_by(&rewritten);
    let (rewritten, asof_join) = extract_asof_join(&rewritten);
    let (rewritten, latest_on) = extract_latest_on(&rewritten);
    // Pre-process BETWEEN SYMMETRIC into regular BETWEEN.
    let rewritten = rewrite_between_symmetric(&rewritten);
    // Pre-process MySQL-style LIMIT offset, count -> LIMIT count OFFSET offset.
    let rewritten = rewrite_mysql_limit(&rewritten);

    let dialect = ExchangeDbDialect(GenericDialect {});
    let statements = Parser::parse_sql(&dialect, &rewritten)
        .map_err(|e| ExchangeDbError::Parse(e.to_string()))?;

    if statements.is_empty() {
        return Err(ExchangeDbError::Parse("empty SQL statement".into()));
    }

    Ok(ParsedQuery {
        statements,
        sample_by_raw,
        asof_join,
        latest_on,
        vacuum_table: None,
        create_matview: None,
        refresh_matview: None,
        drop_matview: None,
        rbac_command: None,
        show_command: None,
        sample_by_fill,
        sample_by_align_calendar,
        partition_command: None,
        pivot_info,
        merge_command: None,
        create_procedure: None,
        drop_procedure: None,
        call_procedure: None,
        create_downsampling: None,
        create_view: None,
        drop_view: None,
        create_trigger: None,
        drop_trigger: None,
        comment_on: None,
        designated_timestamp,
        partition_by_clause,
    })
}

/// Extract RBAC commands (CREATE USER, DROP USER, CREATE ROLE, DROP ROLE,
/// GRANT, REVOKE) that sqlparser cannot handle natively.
fn extract_rbac_command(sql: &str) -> Option<RbacCommand> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let tokens: Vec<&str> = upper.split_whitespace().collect();
    // Original-case tokens for extracting identifiers/passwords.
    let orig_tokens: Vec<&str> = trimmed.split_whitespace().collect();

    if tokens.len() < 3 {
        return None;
    }

    // CREATE USER <name> WITH PASSWORD '<password>'
    if tokens[0] == "CREATE" && tokens[1] == "USER" && tokens.len() >= 6 {
        let username = orig_tokens[2].to_string();
        if tokens[3] == "WITH" && tokens[4] == "PASSWORD" {
            // Password is everything after "PASSWORD", trimmed of quotes.
            let password_start = upper.find("PASSWORD").unwrap() + "PASSWORD".len();
            let password_raw = trimmed[password_start..].trim();
            let password = password_raw
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
            return Some(RbacCommand::CreateUser { username, password });
        }
    }

    // DROP USER <name>
    if tokens[0] == "DROP" && tokens[1] == "USER" && tokens.len() >= 3 {
        let username = orig_tokens[2].to_string();
        return Some(RbacCommand::DropUser { username });
    }

    // CREATE ROLE <name>
    if tokens[0] == "CREATE" && tokens[1] == "ROLE" && tokens.len() >= 3 {
        let name = orig_tokens[2].to_string();
        return Some(RbacCommand::CreateRole { name });
    }

    // DROP ROLE <name>
    if tokens[0] == "DROP" && tokens[1] == "ROLE" && tokens.len() >= 3 {
        let name = orig_tokens[2].to_string();
        return Some(RbacCommand::DropRole { name });
    }

    // GRANT ... TO <target>
    if tokens[0] == "GRANT" {
        return parse_grant(trimmed, &tokens, &orig_tokens);
    }

    // REVOKE ... FROM <target>
    if tokens[0] == "REVOKE" {
        return parse_revoke(trimmed, &tokens, &orig_tokens);
    }

    None
}

/// Parse a GRANT statement into an RbacCommand.
///
/// Supported forms:
/// - GRANT READ [ON <table>] TO <target>
/// - GRANT WRITE [ON <table>] TO <target>
/// - GRANT DDL TO <target>
/// - GRANT ADMIN TO <target>
/// - GRANT SYSTEM TO <target>
/// - GRANT COLUMN READ (col1, col2) ON <table> TO <target>
/// - GRANT <role_name> TO <user>
fn parse_grant(_sql: &str, tokens: &[&str], orig_tokens: &[&str]) -> Option<RbacCommand> {
    use crate::plan::GrantPermission;

    // Find the TO keyword (last occurrence).
    let to_idx = tokens.iter().rposition(|t| *t == "TO")?;
    if to_idx + 1 >= tokens.len() {
        return None;
    }
    let target = orig_tokens[to_idx + 1].to_string();

    // GRANT COLUMN READ (col1, col2) ON <table> TO <target>
    if tokens.len() > 3 && tokens[1] == "COLUMN" && tokens[2] == "READ" {
        // Find ON keyword.
        let on_idx = tokens.iter().position(|t| *t == "ON")?;
        let table = orig_tokens[on_idx + 1].to_string();
        // Extract columns between parentheses.
        let full = orig_tokens[3..on_idx].join(" ");
        let inner = full.trim_matches(|c| c == '(' || c == ')');
        let columns: Vec<String> = inner.split(',').map(|s| s.trim().to_string()).collect();
        return Some(RbacCommand::Grant {
            permission: GrantPermission::ColumnRead { table, columns },
            target,
        });
    }

    // GRANT READ [ON <table>] TO <target>
    if tokens[1] == "READ" {
        let table = if tokens.len() > 4 && tokens[2] == "ON" {
            Some(orig_tokens[3].to_string())
        } else {
            None
        };
        return Some(RbacCommand::Grant {
            permission: GrantPermission::Read { table },
            target,
        });
    }

    // GRANT WRITE [ON <table>] TO <target>
    if tokens[1] == "WRITE" {
        let table = if tokens.len() > 4 && tokens[2] == "ON" {
            Some(orig_tokens[3].to_string())
        } else {
            None
        };
        return Some(RbacCommand::Grant {
            permission: GrantPermission::Write { table },
            target,
        });
    }

    // GRANT DDL TO <target>
    if tokens[1] == "DDL" {
        return Some(RbacCommand::Grant {
            permission: GrantPermission::DDL,
            target,
        });
    }

    // GRANT ADMIN TO <target>
    if tokens[1] == "ADMIN" {
        return Some(RbacCommand::Grant {
            permission: GrantPermission::Admin,
            target,
        });
    }

    // GRANT SYSTEM TO <target>
    if tokens[1] == "SYSTEM" {
        return Some(RbacCommand::Grant {
            permission: GrantPermission::System,
            target,
        });
    }

    // Standard SQL: GRANT SELECT ON <table> TO <target>
    //               GRANT SELECT, INSERT ON <table> TO <target>
    //               GRANT ALL ON <table> TO <target>
    {
        // Collect privilege keywords between GRANT and ON
        let on_pos = tokens.iter().position(|t| *t == "ON");
        if let Some(on_idx) = on_pos {
            if on_idx > 1 && on_idx + 1 < to_idx {
                let table = orig_tokens[on_idx + 1].to_string();
                // Parse the privilege list between GRANT (idx 0) and ON (on_idx)
                let priv_str = tokens[1..on_idx].join(" ");
                let privs: Vec<&str> = priv_str.split(',').map(|s| s.trim()).collect();
                // If ALL is present, return All
                if privs.iter().any(|p| *p == "ALL" || *p == "ALL PRIVILEGES") {
                    return Some(RbacCommand::Grant {
                        permission: GrantPermission::All { table },
                        target,
                    });
                }
                // Return the first matching privilege (multi-priv is handled
                // by the caller splitting into multiple grants).
                for priv_name in &privs {
                    let perm = match *priv_name {
                        "SELECT" => Some(GrantPermission::Select { table: table.clone() }),
                        "INSERT" => Some(GrantPermission::Insert { table: table.clone() }),
                        "UPDATE" => Some(GrantPermission::Update { table: table.clone() }),
                        "DELETE" => Some(GrantPermission::Delete { table: table.clone() }),
                        _ => None,
                    };
                    if let Some(p) = perm {
                        return Some(RbacCommand::Grant {
                            permission: p,
                            target,
                        });
                    }
                }
            }
        }
    }

    // GRANT <role_name> TO <user> — role assignment
    if tokens.len() >= 4 && tokens[2] == "TO" {
        let role_name = orig_tokens[1].to_string();
        return Some(RbacCommand::Grant {
            permission: GrantPermission::Role { role_name },
            target,
        });
    }

    None
}

/// Parse a REVOKE statement into an RbacCommand.
///
/// Supported forms mirror GRANT but use FROM instead of TO.
fn parse_revoke(_sql: &str, tokens: &[&str], orig_tokens: &[&str]) -> Option<RbacCommand> {
    use crate::plan::GrantPermission;

    // Find the FROM keyword (last occurrence).
    let from_idx = tokens.iter().rposition(|t| *t == "FROM")?;
    if from_idx + 1 >= tokens.len() {
        return None;
    }
    let target = orig_tokens[from_idx + 1].to_string();

    // REVOKE COLUMN READ (col1, col2) ON <table> FROM <target>
    if tokens.len() > 3 && tokens[1] == "COLUMN" && tokens[2] == "READ" {
        let on_idx = tokens.iter().position(|t| *t == "ON")?;
        let table = orig_tokens[on_idx + 1].to_string();
        let full = orig_tokens[3..on_idx].join(" ");
        let inner = full.trim_matches(|c| c == '(' || c == ')');
        let columns: Vec<String> = inner.split(',').map(|s| s.trim().to_string()).collect();
        return Some(RbacCommand::Revoke {
            permission: GrantPermission::ColumnRead { table, columns },
            target,
        });
    }

    // REVOKE READ [ON <table>] FROM <target>
    if tokens[1] == "READ" {
        let table = if tokens.len() > 4 && tokens[2] == "ON" {
            Some(orig_tokens[3].to_string())
        } else {
            None
        };
        return Some(RbacCommand::Revoke {
            permission: GrantPermission::Read { table },
            target,
        });
    }

    // REVOKE WRITE [ON <table>] FROM <target>
    if tokens[1] == "WRITE" {
        let table = if tokens.len() > 4 && tokens[2] == "ON" {
            Some(orig_tokens[3].to_string())
        } else {
            None
        };
        return Some(RbacCommand::Revoke {
            permission: GrantPermission::Write { table },
            target,
        });
    }

    // REVOKE DDL FROM <target>
    if tokens[1] == "DDL" {
        return Some(RbacCommand::Revoke {
            permission: GrantPermission::DDL,
            target,
        });
    }

    // REVOKE ADMIN FROM <target>
    if tokens[1] == "ADMIN" {
        return Some(RbacCommand::Revoke {
            permission: GrantPermission::Admin,
            target,
        });
    }

    // REVOKE SYSTEM FROM <target>
    if tokens[1] == "SYSTEM" {
        return Some(RbacCommand::Revoke {
            permission: GrantPermission::System,
            target,
        });
    }

    // Standard SQL: REVOKE SELECT ON <table> FROM <target>
    //               REVOKE INSERT ON <table> FROM <target>
    //               REVOKE ALL ON <table> FROM <target>
    {
        let on_pos = tokens.iter().position(|t| *t == "ON");
        if let Some(on_idx) = on_pos {
            if on_idx > 1 && on_idx + 1 < from_idx {
                let table = orig_tokens[on_idx + 1].to_string();
                let priv_str = tokens[1..on_idx].join(" ");
                let privs: Vec<&str> = priv_str.split(',').map(|s| s.trim()).collect();
                if privs.iter().any(|p| *p == "ALL" || *p == "ALL PRIVILEGES") {
                    return Some(RbacCommand::Revoke {
                        permission: GrantPermission::All { table },
                        target,
                    });
                }
                for priv_name in &privs {
                    let perm = match *priv_name {
                        "SELECT" => Some(GrantPermission::Select { table: table.clone() }),
                        "INSERT" => Some(GrantPermission::Insert { table: table.clone() }),
                        "UPDATE" => Some(GrantPermission::Update { table: table.clone() }),
                        "DELETE" => Some(GrantPermission::Delete { table: table.clone() }),
                        _ => None,
                    };
                    if let Some(p) = perm {
                        return Some(RbacCommand::Revoke {
                            permission: p,
                            target,
                        });
                    }
                }
            }
        }
    }

    // REVOKE <role_name> FROM <user>
    if tokens.len() >= 4 && tokens[2] == "FROM" {
        let role_name = orig_tokens[1].to_string();
        return Some(RbacCommand::Revoke {
            permission: GrantPermission::Role { role_name },
            target,
        });
    }

    None
}

/// Extract `VACUUM <table_name>;` as a custom statement.
fn extract_vacuum(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if upper.starts_with("VACUUM") {
        let rest = trimmed["VACUUM".len()..].trim();
        if !rest.is_empty() {
            return Some(rest.to_string());
        }
    }
    None
}

/// Metadata extracted from an `ASOF JOIN` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct AsofJoinInfo {
    /// The right table name in the ASOF JOIN.
    pub right_table: String,
    /// Optional alias for the right table.
    pub right_alias: Option<String>,
    /// Optional alias for the left table (extracted from the FROM clause).
    pub left_alias: Option<String>,
    /// Equality columns: pairs of (left_col, right_col) from the ON clause.
    pub on_columns: Vec<(String, String)>,
}

/// Metadata extracted from a `LATEST ON ... PARTITION BY ...` clause.
#[derive(Debug, Clone, PartialEq)]
pub struct LatestOnInfo {
    /// The timestamp column name.
    pub timestamp_col: String,
    /// The partition column name.
    pub partition_col: String,
}

/// The result of parsing: the sqlparser AST plus any custom extensions we
/// extracted before handing the SQL to sqlparser.
#[derive(Debug)]
pub struct ParsedQuery {
    pub statements: Vec<Statement>,
    /// Raw text of the SAMPLE BY interval, e.g. `"1h"`, `"5m"`, `"1d"`.
    pub sample_by_raw: Option<String>,
    /// ASOF JOIN metadata, if present.
    pub asof_join: Option<AsofJoinInfo>,
    /// LATEST ON metadata, if present.
    pub latest_on: Option<LatestOnInfo>,
    /// VACUUM table name, if present.
    pub vacuum_table: Option<String>,
    /// CREATE MATERIALIZED VIEW: (view_name, defining_sql).
    pub create_matview: Option<(String, String)>,
    /// REFRESH MATERIALIZED VIEW: view_name.
    pub refresh_matview: Option<String>,
    /// DROP MATERIALIZED VIEW: view_name.
    pub drop_matview: Option<String>,
    /// RBAC command (pre-processed).
    pub rbac_command: Option<RbacCommand>,
    /// SHOW / DESCRIBE command (pre-processed).
    pub show_command: Option<ShowCommand>,
    /// SAMPLE BY fill mode, e.g. `"NONE"`, `"NULL"`, `"PREV"`, `"0"`, `"LINEAR"`.
    pub sample_by_fill: Option<String>,
    /// SAMPLE BY alignment: true if ALIGN TO CALENDAR was specified.
    pub sample_by_align_calendar: bool,
    /// Partition management command (pre-processed).
    pub partition_command: Option<PartitionCommand>,
    /// PIVOT metadata, if present.
    pub pivot_info: Option<PivotInfo>,
    /// MERGE command, if present.
    pub merge_command: Option<MergeInfo>,
    /// CREATE PROCEDURE command (pre-processed).
    pub create_procedure: Option<(String, String)>,
    /// DROP PROCEDURE command (pre-processed).
    pub drop_procedure: Option<String>,
    /// CALL procedure command (pre-processed).
    pub call_procedure: Option<String>,
    /// CREATE DOWNSAMPLING command (pre-processed).
    pub create_downsampling: Option<DownsamplingInfo>,
    /// CREATE VIEW command: (view_name, defining_sql).
    pub create_view: Option<(String, String)>,
    /// DROP VIEW command: view_name.
    pub drop_view: Option<String>,
    /// CREATE TRIGGER command: (name, table, procedure).
    pub create_trigger: Option<(String, String, String)>,
    /// DROP TRIGGER command: (name, table).
    pub drop_trigger: Option<(String, String)>,
    /// COMMENT ON command: (object_type, object_name, table_name, comment).
    pub comment_on: Option<(String, String, Option<String>, String)>,
    /// Designated timestamp column from QuestDB-style TIMESTAMP(col) clause.
    pub designated_timestamp: Option<String>,
    /// Partition strategy from QuestDB-style PARTITION BY <strategy> clause.
    pub partition_by_clause: Option<String>,
}

/// Metadata extracted from a CREATE DOWNSAMPLING command.
#[derive(Debug, Clone, PartialEq)]
pub struct DownsamplingInfo {
    pub source_table: String,
    pub target_name: String,
    pub interval_secs: u64,
    /// (agg_function, source_column, alias)
    pub columns: Vec<(String, String, String)>,
}

/// Metadata extracted from a PIVOT clause.
#[derive(Debug, Clone, PartialEq)]
pub struct PivotInfo {
    /// The aggregate function name (e.g. "avg").
    pub aggregate: String,
    /// The column being aggregated (e.g. "price").
    pub agg_column: String,
    /// The pivot column (e.g. "symbol").
    pub pivot_col: String,
    /// The pivot values and aliases.
    pub values: Vec<(String, String)>,
}

/// Metadata extracted from a MERGE statement.
#[derive(Debug, Clone, PartialEq)]
pub struct MergeInfo {
    pub target_table: String,
    pub source_table: String,
    /// ON condition: (target_col, source_col).
    pub on_left: String,
    pub on_right: String,
    /// WHEN MATCHED THEN UPDATE SET assignments: (col, expr_str).
    pub matched_update: Option<Vec<(String, String)>>,
    /// WHEN MATCHED THEN DELETE.
    pub matched_delete: bool,
    /// WHEN NOT MATCHED THEN INSERT VALUES (exprs...).
    pub not_matched_values: Option<Vec<String>>,
}

/// Pre-parsed SHOW / DESCRIBE commands.
#[derive(Debug, Clone)]
pub enum ShowCommand {
    ShowTables,
    ShowColumns { table: String },
    ShowCreateTable { table: String },
}

/// Pre-parsed RBAC commands that sqlparser cannot handle.
#[derive(Debug, Clone)]
pub enum RbacCommand {
    CreateUser { username: String, password: String },
    DropUser { username: String },
    CreateRole { name: String },
    DropRole { name: String },
    Grant { permission: crate::plan::GrantPermission, target: String },
    Revoke { permission: crate::plan::GrantPermission, target: String },
}

/// Strip `SAMPLE BY <interval> [FILL(...)] [ALIGN TO CALENDAR]` from the SQL
/// Extract QuestDB-style `TIMESTAMP(col)` and `PARTITION BY <strategy>` clauses
/// from the end of a CREATE TABLE statement.
///
/// These clauses are not standard SQL and confuse sqlparser, so we strip them
/// and return the extracted values as metadata.
///
/// Returns (rewritten_sql, designated_timestamp, partition_by).
fn extract_questdb_create_table_clauses(sql: &str) -> (String, Option<String>, Option<String>) {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();

    // Only applies to CREATE TABLE statements.
    if !upper.starts_with("CREATE TABLE") && !upper.starts_with("CREATE TABLE IF NOT EXISTS") {
        return (sql.to_string(), None, None);
    }

    let mut result = trimmed.to_string();
    let mut designated_ts: Option<String> = None;
    let mut partition_by: Option<String> = None;

    // Extract PARTITION BY <strategy> (must be done first since it comes after TIMESTAMP(col))
    let result_upper = result.to_ascii_uppercase();
    if let Some(pb_pos) = result_upper.rfind("PARTITION BY ") {
        let after = result[pb_pos + 13..].trim();
        // Extract the partition strategy word (DAY, MONTH, YEAR, HOUR, WEEK, NONE)
        let strategy = after.split_whitespace().next().unwrap_or("").to_ascii_uppercase();
        if matches!(strategy.as_str(), "DAY" | "MONTH" | "YEAR" | "HOUR" | "WEEK" | "NONE") {
            partition_by = Some(strategy);
            result = result[..pb_pos].trim().to_string();
        }
    }

    // Extract TIMESTAMP(col) — the trailing clause designating the timestamp column.
    let result_upper = result.to_ascii_uppercase();
    // Look for a standalone TIMESTAMP(<ident>) after the closing paren of columns.
    // We search from the end to avoid matching column definitions.
    if let Some(ts_pos) = result_upper.rfind(") TIMESTAMP(") {
        // The closing paren of the column list is at ts_pos, then " TIMESTAMP(" follows.
        let after_kw = &result[ts_pos + 12..]; // skip ") TIMESTAMP("
        if let Some(close) = after_kw.find(')') {
            let col_name = after_kw[..close].trim().to_string();
            if !col_name.is_empty() {
                designated_ts = Some(col_name);
            }
            // Rebuild: everything up to and including the column-list closing paren.
            result = result[..=ts_pos].to_string();
        }
    }

    // Re-add semicolon if original had one.
    if sql.trim().ends_with(';') {
        result.push(';');
    }

    (result, designated_ts, partition_by)
}

/// so sqlparser can handle the rest.
///
/// Returns (rewritten_sql, optional_sample_by_value, optional_fill_mode, align_calendar).
fn extract_sample_by(sql: &str) -> (String, Option<String>, Option<String>, bool) {
    // Case-insensitive search for "SAMPLE BY"
    let upper = sql.to_ascii_uppercase();
    if let Some(pos) = upper.find("SAMPLE BY") {
        let before = &sql[..pos];
        let after = &sql[pos + "SAMPLE BY".len()..];
        let after_trimmed = after.trim_start();

        // The entire SAMPLE BY clause (interval + FILL + ALIGN) runs until
        // the next SQL keyword or semicolon or end.
        let end = after_trimmed
            .find(|c: char| c == ';' || c == '\n')
            .unwrap_or(after_trimmed.len());

        // Also stop at known keywords that might follow: ORDER, LIMIT, GROUP.
        let end = ["ORDER", "LIMIT", "GROUP"]
            .iter()
            .fold(end, |acc, kw| {
                let upper_after = after_trimmed.to_ascii_uppercase();
                if let Some(kw_pos) = upper_after.find(kw) {
                    if kw_pos < acc {
                        kw_pos
                    } else {
                        acc
                    }
                } else {
                    acc
                }
            });

        let clause = after_trimmed[..end].trim().to_string();
        let remainder = &after_trimmed[end..];
        let rewritten = format!("{before}{remainder}");

        // Parse the clause: "<interval> [FILL(...)] [ALIGN TO CALENDAR]"
        let clause_upper = clause.to_ascii_uppercase();

        // Extract ALIGN TO CALENDAR
        let align_calendar = clause_upper.contains("ALIGN TO CALENDAR");

        // Extract FILL(...)
        let fill_mode = if let Some(fill_pos) = clause_upper.find("FILL(") {
            let fill_start = fill_pos + "FILL(".len();
            if let Some(close) = clause_upper[fill_start..].find(')') {
                let fill_inner = clause[fill_start..fill_start + close].trim().to_string();
                Some(fill_inner)
            } else {
                None
            }
        } else {
            None
        };

        // The interval is the first whitespace-delimited token in the clause.
        let interval = clause.split_whitespace().next().unwrap_or("").to_string();

        (rewritten, Some(interval), fill_mode, align_calendar)
    } else {
        (sql.to_string(), None, None, false)
    }
}

/// Detect `SHOW TABLES`, `SHOW COLUMNS FROM <table>`, `SHOW CREATE TABLE <table>`,
/// `DESCRIBE <table>` and return a `ShowCommand`.
fn extract_show(sql: &str) -> Option<ShowCommand> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let tokens: Vec<&str> = upper.split_whitespace().collect();
    let orig_tokens: Vec<&str> = trimmed.split_whitespace().collect();

    if tokens.is_empty() {
        return None;
    }

    // SHOW TABLES
    if tokens.len() == 2 && tokens[0] == "SHOW" && tokens[1] == "TABLES" {
        return Some(ShowCommand::ShowTables);
    }

    // SHOW COLUMNS FROM <table>
    if tokens.len() == 4 && tokens[0] == "SHOW" && tokens[1] == "COLUMNS" && tokens[2] == "FROM" {
        return Some(ShowCommand::ShowColumns { table: orig_tokens[3].to_string() });
    }

    // SHOW CREATE TABLE <table>
    if tokens.len() == 4 && tokens[0] == "SHOW" && tokens[1] == "CREATE" && tokens[2] == "TABLE" {
        return Some(ShowCommand::ShowCreateTable { table: orig_tokens[3].to_string() });
    }

    // DESCRIBE <table>
    if tokens.len() == 2 && (tokens[0] == "DESCRIBE" || tokens[0] == "DESC") {
        return Some(ShowCommand::ShowColumns { table: orig_tokens[1].to_string() });
    }

    None
}

/// Strip `ASOF JOIN <table> [alias] ON (<left_col> = <right_col>[, ...])` from
/// the SQL and return the metadata out-of-band.
///
/// Rewrites the query so only `FROM <left_table>` remains (the ASOF JOIN
/// clause is removed entirely).
fn extract_asof_join(sql: &str) -> (String, Option<AsofJoinInfo>) {
    let upper = sql.to_ascii_uppercase();
    let Some(pos) = upper.find("ASOF JOIN") else {
        return (sql.to_string(), None);
    };

    let before = &sql[..pos];
    let after = &sql[pos + "ASOF JOIN".len()..];
    let after_trimmed = after.trim_start();

    // Extract left alias from the FROM clause: `FROM <table> <alias> ASOF JOIN ...`
    let left_alias = extract_left_alias(before);

    // Parse: <right_table> [alias] ON (<col> = <col>[, ...])
    let tokens: Vec<&str> = after_trimmed.split_whitespace().collect();
    if tokens.is_empty() {
        return (sql.to_string(), None);
    }

    let right_table = tokens[0].to_string();

    // Find ON keyword
    let on_upper: Vec<String> = tokens.iter().map(|t| t.to_ascii_uppercase()).collect();
    let on_pos = on_upper.iter().position(|t| t == "ON");

    let (right_alias, _on_idx) = match on_pos {
        Some(1) => (None, 1),
        Some(idx) if idx > 1 => (Some(tokens[1].to_string()), idx),
        _ => return (sql.to_string(), None),
    };

    // Find the ON condition in the original string after ASOF JOIN.
    // Look for the parenthesized condition.
    let on_keyword_pos = upper[pos + "ASOF JOIN".len()..]
        .find(" ON")
        .map(|p| p + pos + "ASOF JOIN".len());

    let Some(on_kw_pos) = on_keyword_pos else {
        return (sql.to_string(), None);
    };

    let after_on = &sql[on_kw_pos + 3..]; // skip " ON"
    let after_on_trimmed = after_on.trim_start();

    // Parse the ON condition. Could be parenthesized or not.
    let (on_columns, remainder) = if after_on_trimmed.starts_with('(') {
        let close = after_on_trimmed.find(')');
        let Some(close_pos) = close else {
            return (sql.to_string(), None);
        };
        let inner = &after_on_trimmed[1..close_pos];
        let cols = parse_on_columns(inner);
        let remainder = &after_on_trimmed[close_pos + 1..];
        (cols, remainder)
    } else {
        // No parens: read until a SQL keyword or end.
        let end = find_keyword_boundary(after_on_trimmed);
        let inner = &after_on_trimmed[..end];
        let cols = parse_on_columns(inner);
        let remainder = &after_on_trimmed[end..];
        (cols, remainder)
    };

    // Strip the left alias from the FROM clause in `before` if present.
    let clean_before = if left_alias.is_some() {
        strip_trailing_alias(before)
    } else {
        before.to_string()
    };

    let rewritten = format!("{clean_before}{remainder}");

    (
        rewritten,
        Some(AsofJoinInfo {
            right_table,
            right_alias,
            left_alias,
            on_columns,
        }),
    )
}

/// Extract the alias from the FROM clause, e.g., `FROM trades t` -> Some("t").
fn extract_left_alias(before_asof: &str) -> Option<String> {
    let upper = before_asof.to_ascii_uppercase();
    let from_pos = upper.rfind("FROM")?;
    let after_from = before_asof[from_pos + 4..].trim();
    let tokens: Vec<&str> = after_from.split_whitespace().collect();
    if tokens.len() >= 2 {
        // Second token is the alias (if it's not a keyword).
        let candidate = tokens[1];
        let upper_candidate = candidate.to_ascii_uppercase();
        if !["WHERE", "ORDER", "LIMIT", "GROUP", "HAVING", "ASOF", "LATEST"]
            .contains(&upper_candidate.as_str())
        {
            return Some(candidate.to_string());
        }
    }
    None
}

/// Strip a trailing alias from a string like "... FROM trades t ".
fn strip_trailing_alias(s: &str) -> String {
    let trimmed = s.trim_end();
    if let Some(last_space) = trimmed.rfind(char::is_whitespace) {
        let before_alias = &trimmed[..last_space];
        format!("{before_alias} ")
    } else {
        s.to_string()
    }
}

/// Parse ON column pairs from a string like `t.symbol = q.symbol, t.exchange = q.exchange`.
fn parse_on_columns(s: &str) -> Vec<(String, String)> {
    s.split(',')
        .filter_map(|part| {
            let part = part.trim();
            let parts: Vec<&str> = part.split('=').collect();
            if parts.len() == 2 {
                let left = normalize_col_name(parts[0].trim());
                let right = normalize_col_name(parts[1].trim());
                Some((left, right))
            } else {
                None
            }
        })
        .collect()
}

/// Strip table alias prefix from a column name: `t.symbol` -> `symbol`.
fn normalize_col_name(name: &str) -> String {
    if let Some(dot_pos) = name.find('.') {
        name[dot_pos + 1..].to_string()
    } else {
        name.to_string()
    }
}

/// Find the position of the first SQL keyword boundary in a string.
fn find_keyword_boundary(s: &str) -> usize {
    let upper = s.to_ascii_uppercase();
    let keywords = ["WHERE", "ORDER", "LIMIT", "GROUP", "HAVING"];
    let mut best = s.len();
    for kw in &keywords {
        if let Some(pos) = upper.find(kw) {
            if pos < best {
                best = pos;
            }
        }
    }
    best
}

/// Pre-parsed partition management commands.
#[derive(Debug, Clone)]
pub enum PartitionCommand {
    Detach { table: String, partition: String },
    Attach { table: String, partition: String },
    Squash { table: String, partition1: String, partition2: String },
}

/// Extract `ALTER TABLE <table> DETACH|ATTACH PARTITION '<name>'` or
/// `ALTER TABLE <table> SQUASH PARTITIONS '<p1>', '<p2>'` from the SQL.
fn extract_partition_command(sql: &str) -> Option<PartitionCommand> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let tokens: Vec<&str> = upper.split_whitespace().collect();
    let orig_tokens: Vec<&str> = trimmed.split_whitespace().collect();

    if tokens.len() < 5 {
        return None;
    }

    if tokens[0] != "ALTER" || tokens[1] != "TABLE" {
        return None;
    }

    let table = orig_tokens[2].to_string();

    // ALTER TABLE <table> DETACH PARTITION '<name>'
    if tokens.len() >= 5 && tokens[3] == "DETACH" && tokens[4] == "PARTITION" {
        if tokens.len() >= 6 {
            let partition = strip_quotes(orig_tokens[5]);
            return Some(PartitionCommand::Detach { table, partition });
        }
    }

    // ALTER TABLE <table> ATTACH PARTITION '<name>'
    if tokens.len() >= 5 && tokens[3] == "ATTACH" && tokens[4] == "PARTITION" {
        if tokens.len() >= 6 {
            let partition = strip_quotes(orig_tokens[5]);
            return Some(PartitionCommand::Attach { table, partition });
        }
    }

    // ALTER TABLE <table> SQUASH PARTITIONS '<p1>', '<p2>'
    if tokens.len() >= 5 && tokens[3] == "SQUASH" && tokens[4] == "PARTITIONS" {
        if tokens.len() >= 6 {
            // The rest is "'p1', 'p2'" or "'p1','p2'" — parse from original string.
            let rest_start = trimmed
                .to_ascii_uppercase()
                .find("PARTITIONS")
                .map(|p| p + "PARTITIONS".len())?;
            let rest = trimmed[rest_start..].trim();
            let parts: Vec<&str> = rest.split(',').collect();
            if parts.len() >= 2 {
                let p1 = strip_quotes(parts[0].trim());
                let p2 = strip_quotes(parts[1].trim());
                return Some(PartitionCommand::Squash {
                    table,
                    partition1: p1,
                    partition2: p2,
                });
            }
        }
    }

    None
}

/// Remove surrounding quotes (single or double) from a string.
fn strip_quotes(s: &str) -> String {
    s.trim_matches('\'').trim_matches('"').to_string()
}

/// Strip `LATEST ON <col> PARTITION BY <col>` from the SQL and return metadata.
fn extract_latest_on(sql: &str) -> (String, Option<LatestOnInfo>) {
    let upper = sql.to_ascii_uppercase();
    let Some(pos) = upper.find("LATEST ON") else {
        return (sql.to_string(), None);
    };

    let before = &sql[..pos];
    let after = &sql[pos + "LATEST ON".len()..];
    let after_trimmed = after.trim_start();

    // Parse: <timestamp_col> PARTITION BY <partition_col>
    let upper_after = after_trimmed.to_ascii_uppercase();
    let Some(pb_pos) = upper_after.find("PARTITION BY") else {
        return (sql.to_string(), None);
    };

    let timestamp_col = after_trimmed[..pb_pos].trim().to_string();
    let after_pb = &after_trimmed[pb_pos + "PARTITION BY".len()..];
    let after_pb_trimmed = after_pb.trim_start();

    // The partition column runs until the next SQL keyword or end.
    let end = find_keyword_boundary(after_pb_trimmed);
    let partition_col = after_pb_trimmed[..end].trim().to_string();
    let remainder = &after_pb_trimmed[end..];

    let rewritten = format!("{before}{remainder}");

    (
        rewritten,
        Some(LatestOnInfo {
            timestamp_col,
            partition_col,
        }),
    )
}

/// Parse a human-readable duration string into a `std::time::Duration`.
///
/// Supported suffixes: `s` (seconds), `m` (minutes), `h` (hours), `d` (days).
/// Examples: `"30s"`, `"5m"`, `"1h"`, `"7d"`.
pub fn parse_duration(s: &str) -> Result<std::time::Duration> {
    let s = s.trim().trim_matches('\'').trim_matches('"');
    if s.is_empty() {
        return Err(ExchangeDbError::Parse("empty duration".into()));
    }

    let (num_str, suffix) = if s.ends_with(|c: char| c.is_ascii_alphabetic()) {
        let split = s.len() - s.chars().rev().take_while(|c| c.is_ascii_alphabetic()).count();
        (&s[..split], &s[split..])
    } else {
        return Err(ExchangeDbError::Parse(format!("missing unit suffix in duration: '{s}'")));
    };

    let num: u64 = num_str
        .parse()
        .map_err(|_| ExchangeDbError::Parse(format!("invalid duration number: '{num_str}'")))?;

    let secs = match suffix.to_ascii_lowercase().as_str() {
        "s" => num,
        "m" | "min" => num * 60,
        "h" => num * 3600,
        "d" => num * 86400,
        other => {
            return Err(ExchangeDbError::Parse(format!(
                "unknown duration suffix: '{other}'"
            )))
        }
    };

    Ok(std::time::Duration::from_secs(secs))
}

/// Extract `CREATE MATERIALIZED VIEW <name> AS <query>` from the SQL.
/// Returns `Some((name, defining_sql))` if matched.
fn extract_create_matview(sql: &str) -> Option<(String, String)> {
    let upper = sql.trim().to_ascii_uppercase();
    if !upper.starts_with("CREATE MATERIALIZED VIEW") {
        return None;
    }
    let rest = sql.trim()["CREATE MATERIALIZED VIEW".len()..].trim_start();
    // Find "AS" keyword (case-insensitive).
    let rest_upper = rest.to_ascii_uppercase();
    let as_pos = rest_upper.find(" AS ")?;
    let name = rest[..as_pos].trim().to_string();
    let query = rest[as_pos + 4..].trim().trim_end_matches(';').trim().to_string();
    if name.is_empty() || query.is_empty() {
        return None;
    }
    Some((name, query))
}

/// Extract `REFRESH MATERIALIZED VIEW <name>` from the SQL.
fn extract_refresh_matview(sql: &str) -> Option<String> {
    let upper = sql.trim().to_ascii_uppercase();
    if !upper.starts_with("REFRESH MATERIALIZED VIEW") {
        return None;
    }
    let rest = sql.trim()["REFRESH MATERIALIZED VIEW".len()..].trim();
    let name = rest.trim_end_matches(';').trim().to_string();
    if name.is_empty() {
        return None;
    }
    Some(name)
}

/// Extract `DROP MATERIALIZED VIEW <name>` from the SQL.
fn extract_drop_matview(sql: &str) -> Option<String> {
    let upper = sql.trim().to_ascii_uppercase();
    if !upper.starts_with("DROP MATERIALIZED VIEW") {
        return None;
    }
    let rest = sql.trim()["DROP MATERIALIZED VIEW".len()..].trim();
    let name = rest.trim_end_matches(';').trim().to_string();
    if name.is_empty() {
        return None;
    }
    Some(name)
}

/// Extract PIVOT clause from SQL and return the source query plus metadata.
///
/// Detects patterns like:
/// ```text
/// SELECT * FROM (source_query) PIVOT (agg(col) FOR pivot_col IN ('val1' AS alias1, ...))
/// ```
///
/// Returns (rewritten_sql, optional pivot info). The rewritten SQL is the
/// source query from inside the PIVOT, suitable for sqlparser.
fn extract_pivot(sql: &str) -> (String, Option<PivotInfo>) {
    let upper = sql.to_ascii_uppercase();
    let Some(pos) = upper.find("PIVOT") else {
        return (sql.to_string(), None);
    };

    // Ensure it's not part of another word.
    if pos > 0 {
        let prev = sql.as_bytes()[pos - 1];
        if prev.is_ascii_alphanumeric() || prev == b'_' {
            return (sql.to_string(), None);
        }
    }

    let after_pivot = &sql[pos + 5..].trim_start();
    if !after_pivot.starts_with('(') {
        return (sql.to_string(), None);
    }

    // Find matching close paren for the PIVOT(...) clause.
    let pivot_body = &after_pivot[1..]; // skip '('
    let mut depth = 1;
    let mut end = 0;
    for (i, c) in pivot_body.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    end = i;
                    break;
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return (sql.to_string(), None);
    }

    let pivot_inner = &pivot_body[..end]; // e.g. "avg(price) FOR symbol IN ('BTC/USD' AS btc, 'ETH/USD' AS eth)"
    let pivot_upper = pivot_inner.to_ascii_uppercase();

    // Parse: agg(col) FOR pivot_col IN (values)
    let Some(for_pos) = pivot_upper.find(" FOR ") else {
        return (sql.to_string(), None);
    };
    let Some(in_pos) = pivot_upper.find(" IN ") else {
        return (sql.to_string(), None);
    };

    let agg_part = pivot_inner[..for_pos].trim(); // "avg(price)"
    let pivot_col = pivot_inner[for_pos + 5..in_pos].trim().to_string(); // "symbol"
    let values_part = pivot_inner[in_pos + 4..].trim(); // "('BTC/USD' AS btc, ...)"
    let values_inner = values_part.trim_start_matches('(').trim_end_matches(')');

    // Parse agg(col).
    let agg_upper = agg_part.to_ascii_uppercase();
    let Some(agg_paren) = agg_upper.find('(') else {
        return (sql.to_string(), None);
    };
    let aggregate = agg_part[..agg_paren].trim().to_string();
    let agg_column = agg_part[agg_paren + 1..].trim_end_matches(')').trim().to_string();

    // Parse values: 'val1' AS alias1, 'val2' AS alias2
    let mut values = Vec::new();
    for part in values_inner.split(',') {
        let part = part.trim();
        let part_upper = part.to_ascii_uppercase();
        if let Some(as_pos) = part_upper.find(" AS ") {
            let value = part[..as_pos].trim().trim_matches('\'').to_string();
            let alias = part[as_pos + 4..].trim().to_string();
            values.push((value, alias));
        } else {
            // No alias: use the value itself as alias.
            let value = part.trim_matches('\'').to_string();
            let alias = value.replace('/', "_").replace(' ', "_");
            values.push((value, alias));
        }
    }

    // The source query is everything before PIVOT (the FROM (...) part).
    // We need to find the subquery. Look for FROM (...) before PIVOT.
    let before_pivot = sql[..pos].trim();
    // Remove "SELECT * FROM" prefix and extract the subquery.
    let rewritten = if let Some(from_paren) = before_pivot.to_ascii_uppercase().rfind("FROM") {
        let after_from = before_pivot[from_paren + 4..].trim();
        if after_from.starts_with('(') && after_from.ends_with(')') {
            after_from[1..after_from.len() - 1].to_string()
        } else {
            after_from.to_string()
        }
    } else {
        before_pivot.to_string()
    };

    (
        rewritten,
        Some(PivotInfo {
            aggregate,
            agg_column,
            pivot_col,
            values,
        }),
    )
}

/// Extract MERGE statement from SQL.
///
/// Parses:
/// ```text
/// MERGE INTO target USING source ON target.col = source.col
///   WHEN MATCHED THEN UPDATE SET col = expr, ...
///   WHEN NOT MATCHED THEN INSERT VALUES (expr, ...)
/// ```
fn extract_merge(sql: &str) -> Option<MergeInfo> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("MERGE") {
        return None;
    }

    let tokens_upper: Vec<&str> = upper.split_whitespace().collect();
    let tokens_orig: Vec<&str> = trimmed.split_whitespace().collect();
    if tokens_upper.len() < 8 {
        return None;
    }

    // MERGE INTO target USING source ON condition
    if tokens_upper[1] != "INTO" || tokens_upper[3] != "USING" {
        return None;
    }

    let target_table = tokens_orig[2].to_string();
    let source_table = tokens_orig[4].to_string();

    // Find ON keyword.
    let on_idx = tokens_upper.iter().position(|t| *t == "ON")?;
    if on_idx + 1 >= tokens_upper.len() {
        return None;
    }

    // Parse ON condition: target.col = source.col
    // Collect tokens until WHEN.
    let when_idx = tokens_upper.iter().position(|t| *t == "WHEN")?;
    let on_expr = tokens_orig[on_idx + 1..when_idx].join(" ");
    let parts: Vec<&str> = on_expr.split('=').collect();
    if parts.len() != 2 {
        return None;
    }
    let on_left = normalize_col_name(parts[0].trim());
    let on_right = normalize_col_name(parts[1].trim());

    let mut matched_update = None;
    let mut matched_delete = false;
    let mut not_matched_values = None;

    // Parse WHEN clauses from the remainder.
    let remainder = &trimmed[trimmed.to_ascii_uppercase().find("WHEN")?..];
    let remainder_upper = remainder.to_ascii_uppercase();

    // Find WHEN MATCHED THEN UPDATE SET ...
    if let Some(wm_pos) = remainder_upper.find("WHEN MATCHED THEN UPDATE SET") {
        let after_set = &remainder[wm_pos + "WHEN MATCHED THEN UPDATE SET".len()..];
        // Read until WHEN or end.
        let end = after_set.to_ascii_uppercase().find("WHEN").unwrap_or(after_set.len());
        let set_clause = after_set[..end].trim().trim_end_matches(';');
        let assignments: Vec<(String, String)> = set_clause
            .split(',')
            .filter_map(|part| {
                let parts: Vec<&str> = part.splitn(2, '=').collect();
                if parts.len() == 2 {
                    Some((parts[0].trim().to_string(), parts[1].trim().to_string()))
                } else {
                    None
                }
            })
            .collect();
        if !assignments.is_empty() {
            matched_update = Some(assignments);
        }
    }

    // Find WHEN MATCHED THEN DELETE
    if remainder_upper.contains("WHEN MATCHED THEN DELETE") {
        matched_delete = true;
    }

    // Find WHEN NOT MATCHED THEN INSERT VALUES (...)
    if let Some(wnm_pos) = remainder_upper.find("WHEN NOT MATCHED THEN INSERT VALUES") {
        let after_insert = &remainder[wnm_pos + "WHEN NOT MATCHED THEN INSERT VALUES".len()..];
        let after_insert = after_insert.trim();
        if after_insert.starts_with('(') {
            let close = after_insert.find(')').unwrap_or(after_insert.len());
            let inner = &after_insert[1..close];
            let vals: Vec<String> = inner.split(',').map(|s| s.trim().to_string()).collect();
            not_matched_values = Some(vals);
        }
    }

    Some(MergeInfo {
        target_table,
        source_table,
        on_left,
        on_right,
        matched_update,
        matched_delete,
        not_matched_values,
    })
}

/// Rewrite `BETWEEN SYMMETRIC` to an equivalent OR expression.
///
/// Since sqlparser doesn't understand `BETWEEN SYMMETRIC`, we rewrite:
///   `<col> BETWEEN SYMMETRIC <a> AND <b>`
/// to:
///   `(<col> BETWEEN <a> AND <b> OR <col> BETWEEN <b> AND <a>)`
///
/// This is semantically correct: BETWEEN SYMMETRIC auto-swaps if low > high.
fn rewrite_between_symmetric(sql: &str) -> String {
    let upper = sql.to_ascii_uppercase();
    if !upper.contains("BETWEEN SYMMETRIC") {
        return sql.to_string();
    }

    // Find each occurrence of BETWEEN SYMMETRIC and rewrite it.
    let mut result = sql.to_string();
    loop {
        let up = result.to_ascii_uppercase();
        let Some(bs_pos) = up.find("BETWEEN SYMMETRIC") else {
            break;
        };

        // Find the expression before BETWEEN SYMMETRIC — scan backwards
        // from bs_pos to find the column/expression.
        let before = result[..bs_pos].trim_end();
        // Find the last whitespace before the column name (or WHERE/AND/OR keyword boundary).
        let col_start = before
            .rfind(|c: char| {
                c == ' ' || c == '(' || c == '\t' || c == '\n'
            })
            .map(|p| p + 1)
            .unwrap_or(0);
        let col = before[col_start..].trim().to_string();
        let prefix = &result[..col_start];

        // After BETWEEN SYMMETRIC: <low> AND <high>
        let after_bs = &result[bs_pos + "BETWEEN SYMMETRIC".len()..];
        let after_bs_trimmed = after_bs.trim_start();

        // Find AND (not inside parens).
        let after_upper = after_bs_trimmed.to_ascii_uppercase();
        let and_pos = find_top_level_and(&after_upper);
        if let Some(and_p) = and_pos {
            let low = after_bs_trimmed[..and_p].trim();
            let rest_after_and = &after_bs_trimmed[and_p + 3..]; // skip "AND"
            // High value goes until next keyword or end.
            let high_end = find_keyword_boundary(rest_after_and.trim_start());
            let high = rest_after_and.trim_start()[..high_end].trim();
            let remainder = &rest_after_and.trim_start()[high_end..];

            let rewritten = format!(
                "{prefix}({col} BETWEEN {low} AND {high} OR {col} BETWEEN {high} AND {low}){remainder}"
            );
            result = rewritten;
        } else {
            // Can't parse -- just strip SYMMETRIC.
            result = result.replacen("BETWEEN SYMMETRIC", "BETWEEN", 1);
            let _ = result.replacen("between symmetric", "BETWEEN", 1);
            break;
        }
    }
    result
}

/// Find the position of the first top-level AND keyword (not inside parens).
fn find_top_level_and(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut depth = 0;
    let mut i = 0;
    while i + 2 < bytes.len() {
        match bytes[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            b'A' | b'a' if depth == 0 => {
                if i + 3 <= s.len() && s[i..i+3].eq_ignore_ascii_case("AND") {
                    // Check boundaries.
                    let before_ok = i == 0 || !bytes[i-1].is_ascii_alphanumeric();
                    let after_ok = i + 3 >= s.len() || !bytes[i+3].is_ascii_alphanumeric();
                    if before_ok && after_ok {
                        return Some(i);
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Rewrite MySQL-style `LIMIT offset, count` to `LIMIT count OFFSET offset`.
///
/// MySQL allows `LIMIT 10, 20` meaning skip 10 rows, return 20. Standard SQL
/// uses `LIMIT 20 OFFSET 10`. We rewrite the former into the latter so that
/// sqlparser can handle it.
fn rewrite_mysql_limit(sql: &str) -> String {
    let upper = sql.to_ascii_uppercase();
    // Find LIMIT keyword not inside a string.
    let limit_pos = match find_keyword_outside_strings(&upper, "LIMIT") {
        Some(pos) => pos,
        None => return sql.to_string(),
    };

    // Check if there's already an OFFSET keyword after LIMIT (standard syntax).
    let after_limit = &upper[limit_pos + 5..];
    if find_keyword_outside_strings(after_limit, "OFFSET").is_some() {
        return sql.to_string();
    }

    // Extract the LIMIT clause: everything between LIMIT and the next keyword or end.
    let rest = sql[limit_pos + 5..].trim_start();
    let rest_upper = rest.to_ascii_uppercase();

    // Find the end of the LIMIT clause (next keyword or end of string).
    let end_pos = rest_upper
        .find(|c: char| c == ';')
        .unwrap_or(rest.len());
    let limit_clause = rest[..end_pos].trim();

    // Check if there's a comma (MySQL syntax: offset, count).
    if let Some(comma_pos) = limit_clause.find(',') {
        let offset_str = limit_clause[..comma_pos].trim();
        let count_str = limit_clause[comma_pos + 1..].trim();

        // Verify both parts are valid numbers.
        if offset_str.parse::<u64>().is_ok() && count_str.parse::<u64>().is_ok() {
            let before_limit = &sql[..limit_pos];
            // Calculate the index of remaining SQL after the LIMIT clause.
            let after_idx = limit_pos + 5 + (sql[limit_pos + 5..].len() - rest.len()) + end_pos;
            let after_clause = &sql[after_idx..];
            return format!("{before_limit}LIMIT {count_str} OFFSET {offset_str}{after_clause}");
        }
    }

    sql.to_string()
}

/// Find a keyword in SQL text, skipping occurrences inside string literals.
fn find_keyword_outside_strings(sql: &str, keyword: &str) -> Option<usize> {
    let bytes = sql.as_bytes();
    let kw_bytes = keyword.as_bytes();
    let kw_len = kw_bytes.len();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut i = 0;
    while i + kw_len <= bytes.len() {
        match bytes[i] {
            b'\'' if !in_double_quote => in_single_quote = !in_single_quote,
            b'"' if !in_single_quote => in_double_quote = !in_double_quote,
            _ if !in_single_quote && !in_double_quote => {
                if bytes[i..i + kw_len].eq_ignore_ascii_case(kw_bytes) {
                    // Check word boundaries.
                    let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric();
                    let after_ok = i + kw_len >= bytes.len() || !bytes[i + kw_len].is_ascii_alphanumeric();
                    if before_ok && after_ok {
                        return Some(i);
                    }
                }
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// Extract CREATE PROCEDURE <name>() AS BEGIN <body> END
fn extract_create_procedure(sql: &str) -> Option<(String, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("CREATE PROCEDURE") {
        return None;
    }
    // Extract name: after "CREATE PROCEDURE" and before "(" or "AS"
    let rest = trimmed["CREATE PROCEDURE".len()..].trim();
    let name_end = rest.find(|c: char| c == '(' || c.is_ascii_whitespace())?;
    let name = rest[..name_end].trim().to_string();
    // Find AS BEGIN ... END
    let upper_rest = rest.to_ascii_uppercase();
    let begin_pos = upper_rest.find("BEGIN")?;
    let end_pos = upper_rest.rfind("END")?;
    if end_pos <= begin_pos {
        return None;
    }
    let body = rest[begin_pos + 5..end_pos].trim().to_string();
    Some((name, body))
}

/// Extract DROP PROCEDURE <name>
fn extract_drop_procedure(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("DROP PROCEDURE") {
        return None;
    }
    let rest = trimmed["DROP PROCEDURE".len()..].trim();
    let name = rest.split_whitespace().next()?.to_string();
    Some(name)
}

/// Extract CALL <name>()
fn extract_call_procedure(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("CALL ") {
        return None;
    }
    let rest = trimmed["CALL ".len()..].trim();
    // Remove trailing parentheses if present.
    let name = rest.trim_end_matches("()").trim_end_matches('(').trim_end_matches(')').trim().to_string();
    if name.is_empty() {
        return None;
    }
    Some(name)
}

/// Extract CREATE DOWNSAMPLING ON <table> INTERVAL <interval> AS <name> COLUMNS <col_specs>
fn extract_create_downsampling(sql: &str) -> Option<DownsamplingInfo> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("CREATE DOWNSAMPLING") {
        return None;
    }
    let rest = &trimmed["CREATE DOWNSAMPLING".len()..].trim();
    let upper_rest = rest.to_ascii_uppercase();

    // ON <table>
    let on_pos = upper_rest.find("ON ")?;
    let after_on = rest[on_pos + 3..].trim();
    let table_end = after_on.find(|c: char| c.is_ascii_whitespace())?;
    let source_table = after_on[..table_end].to_string();

    // INTERVAL <interval>
    let upper_after_on = after_on.to_ascii_uppercase();
    let int_pos = upper_after_on.find("INTERVAL ")?;
    let after_int = after_on[int_pos + 9..].trim();
    let int_end = after_int.find(|c: char| c.is_ascii_whitespace())?;
    let interval_str = &after_int[..int_end];
    let interval_secs = parse_duration(interval_str).ok()?.as_secs();

    // AS <name>
    let upper_after_int = after_int.to_ascii_uppercase();
    let as_pos = upper_after_int.find("AS ")?;
    let after_as = after_int[as_pos + 3..].trim();
    let name_end = after_as.find(|c: char| c.is_ascii_whitespace())?;
    let target_name = after_as[..name_end].to_string();

    // COLUMNS <specs>
    let upper_after_as = after_as.to_ascii_uppercase();
    let cols_pos = upper_after_as.find("COLUMNS ")?;
    let cols_str = after_as[cols_pos + 8..].trim();

    // Parse column specs: first(price) as open, max(price) as high, ...
    let mut columns = Vec::new();
    for spec in cols_str.split(',') {
        let spec = spec.trim();
        if spec.is_empty() {
            continue;
        }
        let _spec_upper = spec.to_ascii_uppercase();
        // Parse: func(col) [as alias] or func(col)
        if let Some(paren_pos) = spec.find('(') {
            let func = spec[..paren_pos].trim().to_ascii_lowercase();
            let close_paren = spec.find(')')?;
            let col = spec[paren_pos + 1..close_paren].trim().to_string();
            let rest_of_spec = spec[close_paren + 1..].trim();
            let alias = if let Some(as_pos) = rest_of_spec.to_ascii_uppercase().find("AS ") {
                rest_of_spec[as_pos + 3..].trim().to_string()
            } else if !rest_of_spec.is_empty() {
                rest_of_spec.to_string()
            } else {
                col.clone()
            };
            columns.push((func, col, alias));
        } else {
            // Simple column reference (e.g. "sum(volume)" without parens - skip)
            continue;
        }
    }

    Some(DownsamplingInfo {
        source_table,
        target_name,
        interval_secs,
        columns,
    })
}

/// Extract CREATE VIEW <name> AS <sql> from the input.
fn extract_create_view(sql: &str) -> Option<(String, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("CREATE VIEW ") {
        return None;
    }
    let rest = &trimmed["CREATE VIEW ".len()..].trim();
    let upper_rest = rest.to_ascii_uppercase();
    let as_pos = upper_rest.find(" AS ")?;
    let name = rest[..as_pos].trim().to_string();
    let view_sql = rest[as_pos + 4..].trim().to_string();
    if view_sql.is_empty() {
        return None;
    }
    Some((name, view_sql))
}

/// Extract DROP VIEW <name> from the input.
fn extract_drop_view(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("DROP VIEW ") {
        return None;
    }
    let name = trimmed["DROP VIEW ".len()..].trim().to_string();
    if name.is_empty() {
        return None;
    }
    Some(name)
}

/// Extract CREATE TRIGGER <name> AFTER INSERT ON <table> FOR EACH ROW EXECUTE PROCEDURE <proc>()
fn extract_create_trigger(sql: &str) -> Option<(String, String, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("CREATE TRIGGER ") {
        return None;
    }
    let rest = &trimmed["CREATE TRIGGER ".len()..];
    let tokens: Vec<&str> = rest.split_whitespace().collect();
    if tokens.len() < 10 {
        return None;
    }
    let name = tokens[0].to_string();
    // Expect: AFTER INSERT ON <table> FOR EACH ROW EXECUTE PROCEDURE <proc>()
    let upper_tokens: Vec<String> = tokens.iter().map(|t| t.to_ascii_uppercase()).collect();
    let on_pos = upper_tokens.iter().position(|t| t == "ON")?;
    let table = tokens.get(on_pos + 1)?.to_string();
    // Find EXECUTE PROCEDURE
    let exec_pos = upper_tokens.iter().position(|t| t == "EXECUTE")?;
    if upper_tokens.get(exec_pos + 1).map(|s| s.as_str()) != Some("PROCEDURE") {
        return None;
    }
    let proc_raw = tokens.get(exec_pos + 2)?;
    let proc_name = proc_raw.trim_end_matches("()").trim_end_matches('(').trim_end_matches(')').to_string();
    Some((name, table, proc_name))
}

/// Extract DROP TRIGGER <name> ON <table>
fn extract_drop_trigger(sql: &str) -> Option<(String, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("DROP TRIGGER ") {
        return None;
    }
    let rest = &trimmed["DROP TRIGGER ".len()..];
    let tokens: Vec<&str> = rest.split_whitespace().collect();
    if tokens.len() < 3 {
        return None;
    }
    let name = tokens[0].to_string();
    let upper_tokens: Vec<String> = tokens.iter().map(|t| t.to_ascii_uppercase()).collect();
    if upper_tokens[1] != "ON" {
        return None;
    }
    let table = tokens[2].to_string();
    Some((name, table))
}

/// Extract COMMENT ON TABLE <name> IS '<comment>' or COMMENT ON COLUMN <table.col> IS '<comment>'
fn extract_comment_on(sql: &str) -> Option<(String, String, Option<String>, String)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    if !upper.starts_with("COMMENT ON ") {
        return None;
    }
    let rest = &trimmed["COMMENT ON ".len()..];
    let upper_rest = rest.to_ascii_uppercase();

    // Find IS keyword
    let is_pos = upper_rest.find(" IS ")?;
    let before_is = rest[..is_pos].trim();
    let comment_raw = rest[is_pos + 4..].trim();
    let comment = comment_raw.trim_matches('\'').trim_matches('"').to_string();

    let upper_before = before_is.to_ascii_uppercase();
    if upper_before.starts_with("TABLE ") {
        let obj_name = before_is["TABLE ".len()..].trim().to_string();
        Some(("TABLE".to_string(), obj_name, None, comment))
    } else if upper_before.starts_with("COLUMN ") {
        let qualified = before_is["COLUMN ".len()..].trim();
        if let Some(dot_pos) = qualified.find('.') {
            let table_name = qualified[..dot_pos].to_string();
            let col_name = qualified[dot_pos + 1..].to_string();
            Some(("COLUMN".to_string(), col_name, Some(table_name), comment))
        } else {
            None
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_select() {
        let pq = parse_sql("SELECT * FROM trades WHERE price > 100").unwrap();
        assert_eq!(pq.statements.len(), 1);
        assert!(pq.sample_by_raw.is_none());
    }

    #[test]
    fn parse_select_with_sample_by() {
        let pq =
            parse_sql("SELECT symbol, avg(price) FROM trades SAMPLE BY 1h ORDER BY timestamp")
                .unwrap();
        assert_eq!(pq.sample_by_raw, Some("1h".to_string()));
        assert_eq!(pq.statements.len(), 1);
        // The rewritten SQL should still parse with ORDER BY intact.
        match &pq.statements[0] {
            sqlparser::ast::Statement::Query(_) => {}
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_table() {
        let sql = "CREATE TABLE trades (timestamp TIMESTAMP, symbol VARCHAR, price DOUBLE, volume DOUBLE)";
        let pq = parse_sql(sql).unwrap();
        assert_eq!(pq.statements.len(), 1);
    }

    #[test]
    fn parse_insert() {
        let sql = "INSERT INTO trades VALUES (1000000, 'BTC', 65000.0, 1.5)";
        let pq = parse_sql(sql).unwrap();
        assert_eq!(pq.statements.len(), 1);
    }

    #[test]
    fn parse_duration_values() {
        assert_eq!(parse_duration("30s").unwrap().as_secs(), 30);
        assert_eq!(parse_duration("5m").unwrap().as_secs(), 300);
        assert_eq!(parse_duration("1h").unwrap().as_secs(), 3600);
        assert_eq!(parse_duration("7d").unwrap().as_secs(), 604800);
    }

    #[test]
    fn parse_duration_invalid() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("10x").is_err());
    }

    #[test]
    fn extract_sample_by_works() {
        let (rewritten, interval, fill, align) = extract_sample_by(
            "SELECT avg(price) FROM trades SAMPLE BY 5m ORDER BY ts LIMIT 10",
        );
        assert_eq!(interval, Some("5m".to_string()));
        assert!(fill.is_none());
        assert!(!align);
        // The ORDER BY and LIMIT must remain in the rewritten SQL.
        let upper = rewritten.to_ascii_uppercase();
        assert!(upper.contains("ORDER BY"));
        assert!(upper.contains("LIMIT"));
        assert!(!upper.contains("SAMPLE BY"));
    }

    #[test]
    fn extract_sample_by_with_fill() {
        let (_, interval, fill, _) = extract_sample_by(
            "SELECT avg(price) FROM trades SAMPLE BY 1h FILL(NULL) ORDER BY ts",
        );
        assert_eq!(interval, Some("1h".to_string()));
        assert_eq!(fill, Some("NULL".to_string()));
    }

    #[test]
    fn extract_sample_by_with_fill_prev() {
        let (_, interval, fill, _) = extract_sample_by(
            "SELECT avg(price) FROM trades SAMPLE BY 1h FILL(PREV) ORDER BY ts",
        );
        assert_eq!(interval, Some("1h".to_string()));
        assert_eq!(fill, Some("PREV".to_string()));
    }

    #[test]
    fn extract_sample_by_fill_zero() {
        let (_, interval, fill, _) = extract_sample_by(
            "SELECT avg(price) FROM trades SAMPLE BY 1h FILL(0)",
        );
        assert_eq!(interval, Some("1h".to_string()));
        assert_eq!(fill, Some("0".to_string()));
    }

    #[test]
    fn extract_sample_by_align_calendar() {
        let (_, interval, _, align) = extract_sample_by(
            "SELECT avg(price) FROM trades SAMPLE BY 1h ALIGN TO CALENDAR",
        );
        assert_eq!(interval, Some("1h".to_string()));
        assert!(align);
    }

    #[test]
    fn extract_show_tables() {
        assert!(matches!(extract_show("SHOW TABLES;"), Some(ShowCommand::ShowTables)));
    }

    #[test]
    fn extract_show_columns() {
        match extract_show("SHOW COLUMNS FROM trades") {
            Some(ShowCommand::ShowColumns { table }) => assert_eq!(table, "trades"),
            other => panic!("expected ShowColumns, got {other:?}"),
        }
    }

    #[test]
    fn extract_describe() {
        match extract_show("DESCRIBE trades;") {
            Some(ShowCommand::ShowColumns { table }) => assert_eq!(table, "trades"),
            other => panic!("expected ShowColumns, got {other:?}"),
        }
    }

    #[test]
    fn extract_show_create_table() {
        match extract_show("SHOW CREATE TABLE trades") {
            Some(ShowCommand::ShowCreateTable { table }) => assert_eq!(table, "trades"),
            other => panic!("expected ShowCreateTable, got {other:?}"),
        }
    }

    #[test]
    fn extract_asof_join_basic() {
        let sql = "SELECT t.*, q.bid, q.ask FROM trades t ASOF JOIN quotes q ON (t.symbol = q.symbol)";
        let (rewritten, info) = extract_asof_join(sql);
        let info = info.unwrap();
        assert_eq!(info.right_table, "quotes");
        assert_eq!(info.right_alias, Some("q".to_string()));
        assert_eq!(info.left_alias, Some("t".to_string()));
        assert_eq!(info.on_columns, vec![("symbol".to_string(), "symbol".to_string())]);
        // The rewritten SQL should be a valid SELECT from trades only.
        let upper = rewritten.to_ascii_uppercase();
        assert!(!upper.contains("ASOF"));
        assert!(upper.contains("FROM"));
    }

    #[test]
    fn extract_asof_join_no_alias() {
        let sql = "SELECT * FROM trades ASOF JOIN quotes ON (symbol = symbol)";
        let (_, info) = extract_asof_join(sql);
        let info = info.unwrap();
        assert_eq!(info.right_table, "quotes");
        assert_eq!(info.right_alias, None);
        assert_eq!(info.left_alias, None);
        assert_eq!(info.on_columns, vec![("symbol".to_string(), "symbol".to_string())]);
    }

    #[test]
    fn extract_latest_on_basic() {
        let sql = "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol";
        let (rewritten, info) = extract_latest_on(sql);
        let info = info.unwrap();
        assert_eq!(info.timestamp_col, "timestamp");
        assert_eq!(info.partition_col, "symbol");
        let upper = rewritten.to_ascii_uppercase();
        assert!(!upper.contains("LATEST ON"));
        assert!(!upper.contains("PARTITION BY"));
    }

    #[test]
    fn extract_latest_on_with_where() {
        let sql = "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol WHERE price > 100";
        let (rewritten, info) = extract_latest_on(sql);
        let info = info.unwrap();
        assert_eq!(info.timestamp_col, "timestamp");
        assert_eq!(info.partition_col, "symbol");
        let upper = rewritten.to_ascii_uppercase();
        assert!(upper.contains("WHERE"));
        assert!(!upper.contains("LATEST ON"));
    }

    #[test]
    fn parse_asof_join_full() {
        let sql = "SELECT t.*, q.bid, q.ask FROM trades t ASOF JOIN quotes q ON (t.symbol = q.symbol)";
        let pq = parse_sql(sql).unwrap();
        assert!(pq.asof_join.is_some());
        assert!(pq.latest_on.is_none());
        let aj = pq.asof_join.unwrap();
        assert_eq!(aj.right_table, "quotes");
    }

    #[test]
    fn parse_latest_on_full() {
        let sql = "SELECT * FROM trades LATEST ON timestamp PARTITION BY symbol";
        let pq = parse_sql(sql).unwrap();
        assert!(pq.latest_on.is_some());
        assert!(pq.asof_join.is_none());
        let lo = pq.latest_on.unwrap();
        assert_eq!(lo.timestamp_col, "timestamp");
        assert_eq!(lo.partition_col, "symbol");
    }

    #[test]
    fn parse_alter_table_detach_partition() {
        let pq = parse_sql("ALTER TABLE trades DETACH PARTITION '2024-01-15';").unwrap();
        let cmd = pq.partition_command.unwrap();
        match cmd {
            PartitionCommand::Detach { table, partition } => {
                assert_eq!(table, "trades");
                assert_eq!(partition, "2024-01-15");
            }
            other => panic!("expected Detach, got {other:?}"),
        }
    }

    #[test]
    fn parse_alter_table_attach_partition() {
        let pq = parse_sql("ALTER TABLE trades ATTACH PARTITION '2024-01-15'").unwrap();
        let cmd = pq.partition_command.unwrap();
        match cmd {
            PartitionCommand::Attach { table, partition } => {
                assert_eq!(table, "trades");
                assert_eq!(partition, "2024-01-15");
            }
            other => panic!("expected Attach, got {other:?}"),
        }
    }

    #[test]
    fn parse_alter_table_squash_partitions() {
        let pq = parse_sql("ALTER TABLE trades SQUASH PARTITIONS '2024-01-15', '2024-01-16';").unwrap();
        let cmd = pq.partition_command.unwrap();
        match cmd {
            PartitionCommand::Squash { table, partition1, partition2 } => {
                assert_eq!(table, "trades");
                assert_eq!(partition1, "2024-01-15");
                assert_eq!(partition2, "2024-01-16");
            }
            other => panic!("expected Squash, got {other:?}"),
        }
    }

    #[test]
    fn extract_pivot_basic() {
        let sql = "SELECT * FROM (SELECT symbol, price FROM trades) PIVOT (avg(price) FOR symbol IN ('BTC/USD' AS btc, 'ETH/USD' AS eth))";
        let (rewritten, info) = extract_pivot(sql);
        let info = info.unwrap();
        assert_eq!(info.aggregate.to_ascii_lowercase(), "avg");
        assert_eq!(info.agg_column, "price");
        assert_eq!(info.pivot_col, "symbol");
        assert_eq!(info.values.len(), 2);
        assert_eq!(info.values[0], ("BTC/USD".to_string(), "btc".to_string()));
        assert_eq!(info.values[1], ("ETH/USD".to_string(), "eth".to_string()));
        // The rewritten SQL is the inner subquery.
        assert!(!rewritten.is_empty());
    }

    #[test]
    fn extract_merge_basic() {
        let sql = "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET price = source.price WHEN NOT MATCHED THEN INSERT VALUES (source.id, source.price)";
        let info = extract_merge(sql).unwrap();
        assert_eq!(info.target_table, "target");
        assert_eq!(info.source_table, "source");
        assert_eq!(info.on_left, "id");
        assert_eq!(info.on_right, "id");
        assert!(info.matched_update.is_some());
        let updates = info.matched_update.unwrap();
        assert_eq!(updates[0].0, "price");
        assert!(info.not_matched_values.is_some());
    }

    #[test]
    fn rewrite_between_symmetric_basic() {
        let sql = "SELECT * FROM trades WHERE price BETWEEN SYMMETRIC 200 AND 100";
        let rewritten = rewrite_between_symmetric(sql);
        assert!(!rewritten.contains("SYMMETRIC"));
        assert!(rewritten.contains("BETWEEN"));
        // Should have two BETWEENs ORed together.
        assert!(rewritten.contains("OR"));
    }
}
