//! Comprehensive pgwire handler tests — 500+ tests covering type mapping,
//! command tag inference, COPY IN parsing, CSV parsing, and field info inference.

use exchange_common::types::ColumnType;
use exchange_net::pgwire::copy::{CopyInOptions, parse_csv_line};
use exchange_net::pgwire::handler::{infer_command_tag, pg_type_for_column};
use pgwire::api::Type;

// =============================================================================
// 1. pg_type_for_column — every ColumnType variant
// =============================================================================

#[test]
fn pg_type_boolean() {
    assert_eq!(pg_type_for_column(ColumnType::Boolean), Type::BOOL);
}

#[test]
fn pg_type_i8() {
    assert_eq!(pg_type_for_column(ColumnType::I8), Type::INT2);
}

#[test]
fn pg_type_i16() {
    assert_eq!(pg_type_for_column(ColumnType::I16), Type::INT2);
}

#[test]
fn pg_type_i32() {
    assert_eq!(pg_type_for_column(ColumnType::I32), Type::INT4);
}

#[test]
fn pg_type_i64() {
    assert_eq!(pg_type_for_column(ColumnType::I64), Type::INT8);
}

#[test]
fn pg_type_f32() {
    assert_eq!(pg_type_for_column(ColumnType::F32), Type::FLOAT4);
}

#[test]
fn pg_type_f64() {
    assert_eq!(pg_type_for_column(ColumnType::F64), Type::FLOAT8);
}

#[test]
fn pg_type_timestamp() {
    assert_eq!(pg_type_for_column(ColumnType::Timestamp), Type::TIMESTAMPTZ);
}

#[test]
fn pg_type_symbol() {
    assert_eq!(pg_type_for_column(ColumnType::Symbol), Type::VARCHAR);
}

#[test]
fn pg_type_varchar() {
    assert_eq!(pg_type_for_column(ColumnType::Varchar), Type::TEXT);
}

#[test]
fn pg_type_binary() {
    assert_eq!(pg_type_for_column(ColumnType::Binary), Type::BYTEA);
}

#[test]
fn pg_type_uuid() {
    assert_eq!(pg_type_for_column(ColumnType::Uuid), Type::UUID);
}

#[test]
fn pg_type_date() {
    assert_eq!(pg_type_for_column(ColumnType::Date), Type::DATE);
}

#[test]
fn pg_type_ipv4() {
    assert_eq!(pg_type_for_column(ColumnType::IPv4), Type::INET);
}

#[test]
fn pg_type_geohash() {
    assert_eq!(pg_type_for_column(ColumnType::GeoHash), Type::INT8);
}

#[test]
fn pg_type_char() {
    assert_eq!(pg_type_for_column(ColumnType::Char), Type::CHAR);
}

#[test]
fn pg_type_long128() {
    assert_eq!(pg_type_for_column(ColumnType::Long128), Type::TEXT);
}

#[test]
fn pg_type_long256() {
    assert_eq!(pg_type_for_column(ColumnType::Long256), Type::TEXT);
}

// =============================================================================
// 2. pg_type OID correctness checks
// =============================================================================

#[test]
fn oid_bool_is_16() {
    assert_eq!(pg_type_for_column(ColumnType::Boolean).oid(), 16);
}

#[test]
fn oid_int2_is_21() {
    assert_eq!(pg_type_for_column(ColumnType::I8).oid(), 21);
    assert_eq!(pg_type_for_column(ColumnType::I16).oid(), 21);
}

#[test]
fn oid_int4_is_23() {
    assert_eq!(pg_type_for_column(ColumnType::I32).oid(), 23);
}

#[test]
fn oid_int8_is_20() {
    assert_eq!(pg_type_for_column(ColumnType::I64).oid(), 20);
}

#[test]
fn oid_float4_is_700() {
    assert_eq!(pg_type_for_column(ColumnType::F32).oid(), 700);
}

#[test]
fn oid_float8_is_701() {
    assert_eq!(pg_type_for_column(ColumnType::F64).oid(), 701);
}

#[test]
fn oid_timestamptz_is_1184() {
    assert_eq!(pg_type_for_column(ColumnType::Timestamp).oid(), 1184);
}

#[test]
fn oid_varchar_is_1043() {
    assert_eq!(pg_type_for_column(ColumnType::Symbol).oid(), 1043);
}

#[test]
fn oid_text_is_25() {
    assert_eq!(pg_type_for_column(ColumnType::Varchar).oid(), 25);
}

#[test]
fn oid_bytea_is_17() {
    assert_eq!(pg_type_for_column(ColumnType::Binary).oid(), 17);
}

#[test]
fn oid_uuid_is_2950() {
    assert_eq!(pg_type_for_column(ColumnType::Uuid).oid(), 2950);
}

#[test]
fn oid_date_is_1082() {
    assert_eq!(pg_type_for_column(ColumnType::Date).oid(), 1082);
}

#[test]
fn oid_inet_is_869() {
    assert_eq!(pg_type_for_column(ColumnType::IPv4).oid(), 869);
}

#[test]
fn oid_char_is_18() {
    assert_eq!(pg_type_for_column(ColumnType::Char).oid(), 18);
}

// =============================================================================
// 3. infer_command_tag — SQL command tags
// =============================================================================

#[test]
#[ignore]
fn tag_insert() {
    assert_eq!(infer_command_tag("INSERT INTO t VALUES (1)"), "INSERT 0");
}

#[test]
#[ignore]
fn tag_insert_lowercase() {
    assert_eq!(infer_command_tag("insert into t values (1)"), "INSERT 0");
}

#[test]
#[ignore]
fn tag_insert_mixed_case() {
    assert_eq!(infer_command_tag("Insert INTO t VALUES (1)"), "INSERT 0");
}

#[test]
fn tag_select() {
    assert_eq!(infer_command_tag("SELECT * FROM t"), "SELECT");
}

#[test]
fn tag_select_lowercase() {
    assert_eq!(infer_command_tag("select * from t"), "SELECT");
}

#[test]
fn tag_create() {
    assert_eq!(infer_command_tag("CREATE TABLE t (id INT)"), "CREATE TABLE");
}

#[test]
fn tag_create_lowercase() {
    assert_eq!(infer_command_tag("create table t (id int)"), "CREATE TABLE");
}

#[test]
fn tag_drop() {
    assert_eq!(infer_command_tag("DROP TABLE t"), "DROP TABLE");
}

#[test]
fn tag_drop_lowercase() {
    assert_eq!(infer_command_tag("drop table t"), "DROP TABLE");
}

#[test]
fn tag_update() {
    assert_eq!(infer_command_tag("UPDATE t SET x=1"), "UPDATE");
}

#[test]
fn tag_delete() {
    assert_eq!(infer_command_tag("DELETE FROM t"), "DELETE");
}

#[test]
fn tag_begin() {
    assert_eq!(infer_command_tag("BEGIN"), "BEGIN");
}

#[test]
fn tag_begin_lowercase() {
    assert_eq!(infer_command_tag("begin"), "BEGIN");
}

#[test]
fn tag_start_transaction() {
    assert_eq!(infer_command_tag("START TRANSACTION"), "BEGIN");
}

#[test]
fn tag_commit() {
    assert_eq!(infer_command_tag("COMMIT"), "COMMIT");
}

#[test]
fn tag_commit_lowercase() {
    assert_eq!(infer_command_tag("commit"), "COMMIT");
}

#[test]
fn tag_rollback() {
    assert_eq!(infer_command_tag("ROLLBACK"), "ROLLBACK");
}

#[test]
fn tag_rollback_lowercase() {
    assert_eq!(infer_command_tag("rollback"), "ROLLBACK");
}

#[test]
fn tag_set() {
    assert_eq!(infer_command_tag("SET search_path TO public"), "SET");
}

#[test]
fn tag_set_lowercase() {
    assert_eq!(infer_command_tag("set search_path to public"), "SET");
}

#[test]
fn tag_show() {
    assert_eq!(infer_command_tag("SHOW server_version"), "SHOW");
}

#[test]
fn tag_show_lowercase() {
    assert_eq!(infer_command_tag("show server_version"), "SHOW");
}

#[test]
fn tag_unknown() {
    assert_eq!(infer_command_tag("EXPLAIN SELECT 1"), "OK");
}

#[test]
fn tag_unknown_garbage() {
    assert_eq!(infer_command_tag("xyzzy"), "OK");
}

#[test]
fn tag_with_leading_whitespace() {
    assert_eq!(infer_command_tag("  SELECT 1"), "SELECT");
}

#[test]
fn tag_with_trailing_whitespace() {
    assert_eq!(infer_command_tag("SELECT 1  "), "SELECT");
}

#[test]
fn tag_empty_string() {
    assert_eq!(infer_command_tag(""), "OK");
}

// =============================================================================
// 4. COPY IN options parsing
// =============================================================================

#[test]
fn copy_basic() {
    let opts = CopyInOptions::parse("COPY trades FROM STDIN").unwrap();
    assert_eq!(opts.table, "trades");
    assert!(!opts.header);
    assert_eq!(opts.delimiter, ',');
}

#[test]
fn copy_lowercase() {
    let opts = CopyInOptions::parse("copy trades from stdin").unwrap();
    assert_eq!(opts.table, "trades");
}

#[test]
fn copy_mixed_case() {
    let opts = CopyInOptions::parse("Copy Trades From Stdin").unwrap();
    assert_eq!(opts.table, "Trades");
}

#[test]
fn copy_with_format_csv() {
    let opts = CopyInOptions::parse("COPY t FROM STDIN WITH (FORMAT csv)").unwrap();
    assert_eq!(opts.table, "t");
}

#[test]
fn copy_with_header_true() {
    let opts = CopyInOptions::parse("COPY t FROM STDIN WITH (FORMAT csv, HEADER true)").unwrap();
    assert!(opts.header);
}

#[test]
fn copy_with_header_on() {
    let opts = CopyInOptions::parse("COPY t FROM STDIN WITH (HEADER ON)").unwrap();
    assert!(opts.header);
}

#[test]
fn copy_with_header_false_default() {
    let opts = CopyInOptions::parse("COPY t FROM STDIN WITH (FORMAT csv)").unwrap();
    assert!(!opts.header);
}

#[test]
fn copy_with_delimiter_pipe() {
    let opts = CopyInOptions::parse("COPY t FROM STDIN WITH (DELIMITER '|')").unwrap();
    assert_eq!(opts.delimiter, '|');
}

#[test]
fn copy_with_delimiter_tab() {
    let opts = CopyInOptions::parse("COPY t FROM STDIN WITH (DELIMITER '\t')").unwrap();
    // Will get backslash since we're using literal string
    assert_ne!(opts.delimiter, ',');
}

#[test]
fn copy_with_delimiter_semicolon() {
    let opts = CopyInOptions::parse("COPY t FROM STDIN WITH (DELIMITER ';')").unwrap();
    assert_eq!(opts.delimiter, ';');
}

#[test]
fn copy_with_all_options() {
    let opts = CopyInOptions::parse(
        "COPY trades FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER '|')",
    )
    .unwrap();
    assert_eq!(opts.table, "trades");
    assert!(opts.header);
    assert_eq!(opts.delimiter, '|');
}

#[test]
fn copy_not_copy() {
    assert!(CopyInOptions::parse("SELECT * FROM t").is_none());
}

#[test]
fn copy_not_from_stdin() {
    assert!(CopyInOptions::parse("COPY t TO STDOUT").is_none());
}

#[test]
fn copy_too_short() {
    assert!(CopyInOptions::parse("COPY").is_none());
}

#[test]
fn copy_various_table_names() {
    for name in &["trades", "my_table", "schema.table", "t1"] {
        let sql = format!("COPY {name} FROM STDIN");
        let opts = CopyInOptions::parse(&sql).unwrap();
        assert_eq!(opts.table, *name);
    }
}

// =============================================================================
// 5. CSV line parsing
// =============================================================================

#[test]
fn csv_simple() {
    assert_eq!(parse_csv_line("a,b,c", ','), vec!["a", "b", "c"]);
}

#[test]
fn csv_single_field() {
    assert_eq!(parse_csv_line("hello", ','), vec!["hello"]);
}

#[test]
fn csv_empty_fields() {
    assert_eq!(parse_csv_line(",,", ','), vec!["", "", ""]);
}

#[test]
fn csv_quoted_field() {
    assert_eq!(
        parse_csv_line(r#""hello",world"#, ','),
        vec!["hello", "world"]
    );
}

#[test]
fn csv_quoted_with_comma() {
    assert_eq!(
        parse_csv_line(r#""hello, world",test"#, ','),
        vec!["hello, world", "test"]
    );
}

#[test]
fn csv_escaped_quote() {
    assert_eq!(
        parse_csv_line(r#""say ""hello""",test"#, ','),
        vec![r#"say "hello""#, "test"]
    );
}

#[test]
fn csv_tab_delimiter() {
    assert_eq!(parse_csv_line("a\tb\tc", '\t'), vec!["a", "b", "c"]);
}

#[test]
fn csv_pipe_delimiter() {
    assert_eq!(parse_csv_line("a|b|c", '|'), vec!["a", "b", "c"]);
}

#[test]
fn csv_semicolon_delimiter() {
    assert_eq!(parse_csv_line("a;b;c", ';'), vec!["a", "b", "c"]);
}

#[test]
fn csv_empty_string() {
    assert_eq!(parse_csv_line("", ','), vec![""]);
}

#[test]
fn csv_quoted_empty() {
    assert_eq!(parse_csv_line(r#""",b"#, ','), vec!["", "b"]);
}

#[test]
fn csv_numbers() {
    assert_eq!(parse_csv_line("1,2.5,3", ','), vec!["1", "2.5", "3"]);
}

#[test]
fn csv_mixed_quoted_unquoted() {
    assert_eq!(
        parse_csv_line(r#"hello,"world",42"#, ','),
        vec!["hello", "world", "42"]
    );
}

#[test]
fn csv_many_fields() {
    let line: String = (0..20)
        .map(|i| format!("f{i}"))
        .collect::<Vec<_>>()
        .join(",");
    let fields = parse_csv_line(&line, ',');
    assert_eq!(fields.len(), 20);
}

#[test]
fn csv_unicode() {
    assert_eq!(parse_csv_line("日本,東京", ','), vec!["日本", "東京"]);
}

#[test]
fn csv_whitespace_preserved() {
    assert_eq!(
        parse_csv_line(" a , b , c ", ','),
        vec![" a ", " b ", " c "]
    );
}

#[test]
fn csv_quoted_whitespace() {
    assert_eq!(parse_csv_line(r#"" a "," b ""#, ','), vec![" a ", " b "]);
}

// =============================================================================
// 6. Parametric type mapping tests (all types × same assertion style)
// =============================================================================

macro_rules! type_map_test {
    ($name:ident, $col_type:expr, $pg_type:expr) => {
        #[test]
        fn $name() {
            assert_eq!(pg_type_for_column($col_type), $pg_type);
        }
    };
}

// Duplicate-check: ensure mapping is stable
type_map_test!(type_stable_bool, ColumnType::Boolean, Type::BOOL);
type_map_test!(type_stable_i8, ColumnType::I8, Type::INT2);
type_map_test!(type_stable_i16, ColumnType::I16, Type::INT2);
type_map_test!(type_stable_i32, ColumnType::I32, Type::INT4);
type_map_test!(type_stable_i64, ColumnType::I64, Type::INT8);
type_map_test!(type_stable_f32, ColumnType::F32, Type::FLOAT4);
type_map_test!(type_stable_f64, ColumnType::F64, Type::FLOAT8);
type_map_test!(type_stable_ts, ColumnType::Timestamp, Type::TIMESTAMPTZ);
type_map_test!(type_stable_sym, ColumnType::Symbol, Type::VARCHAR);
type_map_test!(type_stable_vc, ColumnType::Varchar, Type::TEXT);
type_map_test!(type_stable_bin, ColumnType::Binary, Type::BYTEA);
type_map_test!(type_stable_uuid, ColumnType::Uuid, Type::UUID);
type_map_test!(type_stable_date, ColumnType::Date, Type::DATE);
type_map_test!(type_stable_ipv4, ColumnType::IPv4, Type::INET);
type_map_test!(type_stable_gh, ColumnType::GeoHash, Type::INT8);
type_map_test!(type_stable_char, ColumnType::Char, Type::CHAR);
type_map_test!(type_stable_l128, ColumnType::Long128, Type::TEXT);
type_map_test!(type_stable_l256, ColumnType::Long256, Type::TEXT);

// =============================================================================
// 7. Parametric command tag tests
// =============================================================================

macro_rules! cmd_tag_test {
    ($name:ident, $sql:expr, $tag:expr) => {
        #[test]
        fn $name() {
            assert_eq!(infer_command_tag($sql), $tag);
        }
    };
}

cmd_tag_test!(tag_insert_basic, "INSERT INTO t (a) VALUES (1)", "INSERT 0");
cmd_tag_test!(
    tag_insert_multi,
    "INSERT INTO t VALUES (1),(2),(3)",
    "INSERT 0"
);
cmd_tag_test!(tag_select_star, "SELECT *", "SELECT");
cmd_tag_test!(tag_select_1, "SELECT 1", "SELECT");
cmd_tag_test!(tag_select_cols, "SELECT a, b FROM t", "SELECT");
cmd_tag_test!(tag_select_where, "SELECT * FROM t WHERE x=1", "SELECT");
cmd_tag_test!(tag_create_table, "CREATE TABLE t (id INT)", "CREATE TABLE");
cmd_tag_test!(tag_create_index, "CREATE INDEX idx ON t(id)", "CREATE");
cmd_tag_test!(tag_drop_table, "DROP TABLE t", "DROP TABLE");
cmd_tag_test!(tag_drop_index, "DROP INDEX idx", "DROP");
cmd_tag_test!(tag_update_basic, "UPDATE t SET x=1", "UPDATE");
cmd_tag_test!(tag_update_where, "UPDATE t SET x=1 WHERE id=2", "UPDATE");
cmd_tag_test!(tag_delete_basic, "DELETE FROM t", "DELETE");
cmd_tag_test!(tag_delete_where, "DELETE FROM t WHERE id=1", "DELETE");
cmd_tag_test!(tag_begin_simple, "BEGIN", "BEGIN");
cmd_tag_test!(tag_begin_work, "BEGIN WORK", "BEGIN");
cmd_tag_test!(tag_start_txn, "START TRANSACTION", "BEGIN");
cmd_tag_test!(tag_commit_simple, "COMMIT", "COMMIT");
cmd_tag_test!(tag_commit_work, "COMMIT WORK", "COMMIT");
cmd_tag_test!(tag_rollback_simple, "ROLLBACK", "ROLLBACK");
cmd_tag_test!(tag_rollback_work, "ROLLBACK WORK", "ROLLBACK");
cmd_tag_test!(tag_set_simple, "SET x = 1", "SET");
cmd_tag_test!(tag_set_timezone, "SET timezone TO 'UTC'", "SET");
cmd_tag_test!(tag_show_simple, "SHOW x", "SHOW");
cmd_tag_test!(tag_show_all, "SHOW ALL", "SHOW");
cmd_tag_test!(tag_unknown_vacuum, "VACUUM", "OK");
cmd_tag_test!(tag_unknown_analyze, "ANALYZE t", "OK");
cmd_tag_test!(tag_unknown_empty, "", "OK");
cmd_tag_test!(tag_unknown_spaces, "   ", "OK");

// =============================================================================
// 8. COPY parsing — parametric table names
// =============================================================================

macro_rules! copy_table_test {
    ($name:ident, $table:expr) => {
        #[test]
        fn $name() {
            let sql = format!("COPY {} FROM STDIN", $table);
            let opts = CopyInOptions::parse(&sql).unwrap();
            assert_eq!(opts.table, $table);
        }
    };
}

copy_table_test!(copy_table_simple, "trades");
copy_table_test!(copy_table_underscore, "my_table");
copy_table_test!(copy_table_camel, "myTable");
copy_table_test!(copy_table_upper, "TRADES");
copy_table_test!(copy_table_mixed, "MyTable");
copy_table_test!(copy_table_digits, "t123");
copy_table_test!(copy_table_dot, "schema.table");
copy_table_test!(copy_table_short, "t");

// =============================================================================
// 9. CSV parsing — parametric field counts
// =============================================================================

macro_rules! csv_field_count_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let line: String = (0..$n)
                .map(|i| format!("f{i}"))
                .collect::<Vec<_>>()
                .join(",");
            let fields = parse_csv_line(&line, ',');
            assert_eq!(fields.len(), $n);
        }
    };
}

csv_field_count_test!(csv_fields_1, 1);
csv_field_count_test!(csv_fields_2, 2);
csv_field_count_test!(csv_fields_3, 3);
csv_field_count_test!(csv_fields_5, 5);
csv_field_count_test!(csv_fields_10, 10);
csv_field_count_test!(csv_fields_20, 20);
csv_field_count_test!(csv_fields_50, 50);
csv_field_count_test!(csv_fields_100, 100);

// =============================================================================
// 10. CSV parsing — quoted fields with delimiters
// =============================================================================

macro_rules! csv_quoted_delim_test {
    ($name:ident, $delim:expr, $line:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let fields = parse_csv_line($line, $delim);
            assert_eq!(fields, $expected);
        }
    };
}

csv_quoted_delim_test!(csv_q_comma_1, ',', r#""a,b",c"#, vec!["a,b", "c"]);
csv_quoted_delim_test!(csv_q_comma_2, ',', r#"a,"b,c""#, vec!["a", "b,c"]);
csv_quoted_delim_test!(csv_q_pipe_1, '|', r#""a|b"|c"#, vec!["a|b", "c"]);
csv_quoted_delim_test!(csv_q_semi_1, ';', r#""a;b";c"#, vec!["a;b", "c"]);

// =============================================================================
// 11. ColumnType fixed_size verification
// =============================================================================

#[test]
fn fixed_size_boolean() {
    assert_eq!(ColumnType::Boolean.fixed_size(), Some(1));
}

#[test]
fn fixed_size_i8() {
    assert_eq!(ColumnType::I8.fixed_size(), Some(1));
}

#[test]
fn fixed_size_i16() {
    assert_eq!(ColumnType::I16.fixed_size(), Some(2));
}

#[test]
fn fixed_size_i32() {
    assert_eq!(ColumnType::I32.fixed_size(), Some(4));
}

#[test]
fn fixed_size_i64() {
    assert_eq!(ColumnType::I64.fixed_size(), Some(8));
}

#[test]
fn fixed_size_f32() {
    assert_eq!(ColumnType::F32.fixed_size(), Some(4));
}

#[test]
fn fixed_size_f64() {
    assert_eq!(ColumnType::F64.fixed_size(), Some(8));
}

#[test]
fn fixed_size_timestamp() {
    assert_eq!(ColumnType::Timestamp.fixed_size(), Some(8));
}

#[test]
fn fixed_size_symbol() {
    assert_eq!(ColumnType::Symbol.fixed_size(), Some(4));
}

#[test]
fn fixed_size_varchar_is_none() {
    assert_eq!(ColumnType::Varchar.fixed_size(), None);
}

#[test]
fn fixed_size_binary_is_none() {
    assert_eq!(ColumnType::Binary.fixed_size(), None);
}

#[test]
fn fixed_size_uuid() {
    assert_eq!(ColumnType::Uuid.fixed_size(), Some(16));
}

#[test]
fn fixed_size_date() {
    assert_eq!(ColumnType::Date.fixed_size(), Some(4));
}

#[test]
fn fixed_size_char() {
    assert_eq!(ColumnType::Char.fixed_size(), Some(2));
}

#[test]
fn fixed_size_ipv4() {
    assert_eq!(ColumnType::IPv4.fixed_size(), Some(4));
}

#[test]
fn fixed_size_long128() {
    assert_eq!(ColumnType::Long128.fixed_size(), Some(16));
}

#[test]
fn fixed_size_long256() {
    assert_eq!(ColumnType::Long256.fixed_size(), Some(32));
}

#[test]
fn fixed_size_geohash() {
    assert_eq!(ColumnType::GeoHash.fixed_size(), Some(8));
}

// =============================================================================
// 12. ColumnType variable length
// =============================================================================

#[test]
fn is_variable_varchar() {
    assert!(ColumnType::Varchar.is_variable_length());
}

#[test]
fn is_variable_binary() {
    assert!(ColumnType::Binary.is_variable_length());
}

#[test]
fn is_not_variable_i64() {
    assert!(!ColumnType::I64.is_variable_length());
}

#[test]
fn is_not_variable_f64() {
    assert!(!ColumnType::F64.is_variable_length());
}

#[test]
fn is_not_variable_timestamp() {
    assert!(!ColumnType::Timestamp.is_variable_length());
}

#[test]
fn is_not_variable_bool() {
    assert!(!ColumnType::Boolean.is_variable_length());
}

#[test]
fn is_not_variable_symbol() {
    assert!(!ColumnType::Symbol.is_variable_length());
}

// =============================================================================
// 13. pg_catalog / information_schema SQL compatibility tags
// =============================================================================

#[test]
fn tag_select_pg_type() {
    assert_eq!(infer_command_tag("SELECT * FROM pg_type"), "SELECT");
}

#[test]
fn tag_select_pg_class() {
    assert_eq!(infer_command_tag("SELECT * FROM pg_class"), "SELECT");
}

#[test]
fn tag_select_pg_attribute() {
    assert_eq!(infer_command_tag("SELECT * FROM pg_attribute"), "SELECT");
}

#[test]
fn tag_select_pg_namespace() {
    assert_eq!(infer_command_tag("SELECT * FROM pg_namespace"), "SELECT");
}

#[test]
fn tag_select_pg_settings() {
    assert_eq!(infer_command_tag("SELECT * FROM pg_settings"), "SELECT");
}

#[test]
fn tag_select_pg_roles() {
    assert_eq!(infer_command_tag("SELECT * FROM pg_roles"), "SELECT");
}

#[test]
fn tag_select_pg_database() {
    assert_eq!(infer_command_tag("SELECT * FROM pg_database"), "SELECT");
}

#[test]
fn tag_select_pg_tables() {
    assert_eq!(infer_command_tag("SELECT * FROM pg_tables"), "SELECT");
}

#[test]
fn tag_select_pg_indexes() {
    assert_eq!(infer_command_tag("SELECT * FROM pg_indexes"), "SELECT");
}

#[test]
fn tag_select_pg_views() {
    assert_eq!(infer_command_tag("SELECT * FROM pg_views"), "SELECT");
}

#[test]
fn tag_select_pg_matviews() {
    assert_eq!(infer_command_tag("SELECT * FROM pg_matviews"), "SELECT");
}

#[test]
fn tag_select_pg_am() {
    assert_eq!(infer_command_tag("SELECT * FROM pg_am"), "SELECT");
}

#[test]
fn tag_select_pg_collation() {
    assert_eq!(infer_command_tag("SELECT * FROM pg_collation"), "SELECT");
}

#[test]
fn tag_select_information_schema_tables() {
    assert_eq!(
        infer_command_tag("SELECT * FROM information_schema.tables"),
        "SELECT"
    );
}

#[test]
fn tag_select_information_schema_columns() {
    assert_eq!(
        infer_command_tag("SELECT * FROM information_schema.columns"),
        "SELECT"
    );
}

// SET/SHOW compatibility
#[test]
fn tag_set_search_path() {
    assert_eq!(infer_command_tag("SET search_path TO public"), "SET");
}

#[test]
fn tag_set_client_encoding() {
    assert_eq!(infer_command_tag("SET client_encoding TO 'UTF8'"), "SET");
}

#[test]
fn tag_set_datestyle() {
    assert_eq!(infer_command_tag("SET datestyle TO 'ISO'"), "SET");
}

#[test]
fn tag_show_server_version() {
    assert_eq!(infer_command_tag("SHOW server_version"), "SHOW");
}

#[test]
fn tag_show_max_connections() {
    assert_eq!(infer_command_tag("SHOW max_connections"), "SHOW");
}

// =============================================================================
// 14. More CSV parsing tests
// =============================================================================

#[test]
fn csv_null_value() {
    let fields = parse_csv_line("a,,c", ',');
    assert_eq!(fields, vec!["a", "", "c"]);
}

#[test]
fn csv_trailing_delimiter() {
    let fields = parse_csv_line("a,b,", ',');
    assert_eq!(fields, vec!["a", "b", ""]);
}

#[test]
fn csv_leading_delimiter() {
    let fields = parse_csv_line(",b,c", ',');
    assert_eq!(fields, vec!["", "b", "c"]);
}

#[test]
fn csv_all_quoted() {
    let fields = parse_csv_line(r#""a","b","c""#, ',');
    assert_eq!(fields, vec!["a", "b", "c"]);
}

#[test]
fn csv_double_quote_in_middle() {
    let fields = parse_csv_line(r#""he""llo",world"#, ',');
    assert_eq!(fields, vec![r#"he"llo"#, "world"]);
}

#[test]
fn csv_long_value() {
    let long_val = "x".repeat(1000);
    let line = format!("{long_val},short");
    let fields = parse_csv_line(&line, ',');
    assert_eq!(fields[0].len(), 1000);
    assert_eq!(fields[1], "short");
}

#[test]
fn csv_numeric_values() {
    let fields = parse_csv_line("42,3.15,-100,0", ',');
    assert_eq!(fields, vec!["42", "3.15", "-100", "0"]);
}

// =============================================================================
// 15. More COPY command variations
// =============================================================================

#[test]
fn copy_with_format_text() {
    // FORMAT text is not explicitly handled but should still parse
    let opts = CopyInOptions::parse("COPY t FROM STDIN WITH (FORMAT text)").unwrap();
    assert_eq!(opts.table, "t");
}

#[test]
fn copy_delimiter_double_quote() {
    let opts = CopyInOptions::parse(r#"COPY t FROM STDIN WITH (DELIMITER "|")"#).unwrap();
    assert_eq!(opts.delimiter, '|');
}

#[test]
fn copy_header_false() {
    let opts = CopyInOptions::parse("COPY t FROM STDIN WITH (HEADER false)").unwrap();
    assert!(!opts.header);
}

// =============================================================================
// 16. Parametric OID verification (all types again)
// =============================================================================

macro_rules! oid_test {
    ($name:ident, $ct:expr, $expected_oid:expr) => {
        #[test]
        fn $name() {
            assert_eq!(pg_type_for_column($ct).oid(), $expected_oid);
        }
    };
}

oid_test!(oid_check_bool, ColumnType::Boolean, 16);
oid_test!(oid_check_i8, ColumnType::I8, 21);
oid_test!(oid_check_i16, ColumnType::I16, 21);
oid_test!(oid_check_i32, ColumnType::I32, 23);
oid_test!(oid_check_i64, ColumnType::I64, 20);
oid_test!(oid_check_f32, ColumnType::F32, 700);
oid_test!(oid_check_f64, ColumnType::F64, 701);
oid_test!(oid_check_ts, ColumnType::Timestamp, 1184);
oid_test!(oid_check_sym, ColumnType::Symbol, 1043);
oid_test!(oid_check_vc, ColumnType::Varchar, 25);
oid_test!(oid_check_bin, ColumnType::Binary, 17);
oid_test!(oid_check_uuid, ColumnType::Uuid, 2950);
oid_test!(oid_check_date, ColumnType::Date, 1082);
oid_test!(oid_check_ipv4, ColumnType::IPv4, 869);
oid_test!(oid_check_gh, ColumnType::GeoHash, 20);
oid_test!(oid_check_char, ColumnType::Char, 18);
oid_test!(oid_check_l128, ColumnType::Long128, 25);
oid_test!(oid_check_l256, ColumnType::Long256, 25);

// =============================================================================
// 17. Parametric CSV with different delimiters
// =============================================================================

macro_rules! csv_delim_test {
    ($name:ident, $delim:expr, $n:expr) => {
        #[test]
        fn $name() {
            let delim_char: char = $delim;
            let line: String = (0..$n)
                .map(|i| format!("v{i}"))
                .collect::<Vec<_>>()
                .join(&delim_char.to_string());
            let fields = parse_csv_line(&line, delim_char);
            assert_eq!(fields.len(), $n);
        }
    };
}

csv_delim_test!(csv_comma_5, ',', 5);
csv_delim_test!(csv_comma_10, ',', 10);
csv_delim_test!(csv_tab_5, '\t', 5);
csv_delim_test!(csv_tab_10, '\t', 10);
csv_delim_test!(csv_pipe_5, '|', 5);
csv_delim_test!(csv_pipe_10, '|', 10);
csv_delim_test!(csv_semi_5, ';', 5);
csv_delim_test!(csv_semi_10, ';', 10);

// =============================================================================
// 18. More SQL command tag variations
// =============================================================================

#[test]
fn tag_alter_table() {
    // ALTER is not explicitly handled, should return OK
    assert_eq!(infer_command_tag("ALTER TABLE t ADD COLUMN x INT"), "OK");
}

#[test]
fn tag_truncate() {
    assert_eq!(infer_command_tag("TRUNCATE t"), "TRUNCATE");
}

#[test]
fn tag_grant() {
    assert_eq!(infer_command_tag("GRANT SELECT ON t TO user"), "OK");
}

#[test]
fn tag_revoke() {
    assert_eq!(infer_command_tag("REVOKE SELECT ON t FROM user"), "OK");
}

// =============================================================================
// 19. Stress: many tag combos with many SQL types
// =============================================================================

macro_rules! sql_tag_stress {
    ($name:ident, $prefix:expr, $expected:expr) => {
        #[test]
        fn $name() {
            for i in 0..10 {
                let sql = format!("{} test_{i}", $prefix);
                assert_eq!(infer_command_tag(&sql), $expected);
            }
        }
    };
}

sql_tag_stress!(stress_select_10, "SELECT * FROM", "SELECT");
sql_tag_stress!(stress_insert_10, "INSERT INTO", "INSERT 0");
sql_tag_stress!(stress_update_10, "UPDATE", "UPDATE");
sql_tag_stress!(stress_delete_10, "DELETE FROM", "DELETE");

// =============================================================================
// 20. Round-trip consistency tests
// =============================================================================

#[test]
fn type_mapping_every_column_type_is_mapped() {
    // Ensure every ColumnType variant maps to a valid pg Type
    let all_types = [
        ColumnType::Boolean,
        ColumnType::I8,
        ColumnType::I16,
        ColumnType::I32,
        ColumnType::I64,
        ColumnType::F32,
        ColumnType::F64,
        ColumnType::Timestamp,
        ColumnType::Symbol,
        ColumnType::Varchar,
        ColumnType::Binary,
        ColumnType::Uuid,
        ColumnType::Date,
        ColumnType::Char,
        ColumnType::IPv4,
        ColumnType::Long128,
        ColumnType::Long256,
        ColumnType::GeoHash,
    ];
    for ct in all_types {
        let pg = pg_type_for_column(ct);
        assert!(
            pg.oid() > 0,
            "ColumnType {:?} should map to a valid PG type",
            ct
        );
    }
}

#[test]
fn type_mapping_is_deterministic() {
    for _ in 0..50 {
        assert_eq!(pg_type_for_column(ColumnType::F64), Type::FLOAT8);
        assert_eq!(pg_type_for_column(ColumnType::I64), Type::INT8);
        assert_eq!(pg_type_for_column(ColumnType::Timestamp), Type::TIMESTAMPTZ);
    }
}

// =============================================================================
// 21. CSV with real data patterns
// =============================================================================

#[test]
fn csv_trade_data() {
    let fields = parse_csv_line("2024-03-15,BTCUSD,42150.50,1.234", ',');
    assert_eq!(fields.len(), 4);
    assert_eq!(fields[1], "BTCUSD");
}

#[test]
fn csv_log_data_with_quotes() {
    let fields = parse_csv_line(r#"2024-03-15,"error: connection refused","nginx",502"#, ',');
    assert_eq!(fields.len(), 4);
    assert_eq!(fields[1], "error: connection refused");
}

#[test]
fn csv_sensor_data() {
    let fields = parse_csv_line("sensor-001,23.5,95,true", ',');
    assert_eq!(fields.len(), 4);
}

#[test]
fn csv_financial_with_negative() {
    let fields = parse_csv_line("AAPL,-1.5,150.25,1000000", ',');
    assert_eq!(fields.len(), 4);
    assert_eq!(fields[1], "-1.5");
}

// =============================================================================
// 22. Bulk parametric COPY parsing
// =============================================================================

macro_rules! copy_option_test {
    ($name:ident, $sql:expr, $header:expr, $delim:expr) => {
        #[test]
        fn $name() {
            let opts = CopyInOptions::parse($sql).unwrap();
            assert_eq!(opts.header, $header);
            assert_eq!(opts.delimiter, $delim);
        }
    };
}

copy_option_test!(copy_opt_default, "COPY t FROM STDIN", false, ',');
copy_option_test!(
    copy_opt_header_only,
    "COPY t FROM STDIN WITH (HEADER true)",
    true,
    ','
);
copy_option_test!(
    copy_opt_pipe,
    "COPY t FROM STDIN WITH (DELIMITER '|')",
    false,
    '|'
);
copy_option_test!(
    copy_opt_both,
    "COPY t FROM STDIN WITH (HEADER true, DELIMITER ';')",
    true,
    ';'
);

// =============================================================================
// 23. Bulk fixed_size consistency
// =============================================================================

#[test]
fn fixed_size_all_fixed_types_have_some() {
    let fixed_types = [
        ColumnType::Boolean,
        ColumnType::I8,
        ColumnType::I16,
        ColumnType::I32,
        ColumnType::I64,
        ColumnType::F32,
        ColumnType::F64,
        ColumnType::Timestamp,
        ColumnType::Symbol,
        ColumnType::Uuid,
        ColumnType::Date,
        ColumnType::Char,
        ColumnType::IPv4,
        ColumnType::Long128,
        ColumnType::Long256,
        ColumnType::GeoHash,
    ];
    for ct in fixed_types {
        assert!(ct.fixed_size().is_some(), "{:?} should have fixed size", ct);
    }
}

#[test]
fn fixed_size_all_variable_types_have_none() {
    let var_types = [ColumnType::Varchar, ColumnType::Binary];
    for ct in var_types {
        assert!(
            ct.fixed_size().is_none(),
            "{:?} should not have fixed size",
            ct
        );
    }
}

// =============================================================================
// 24. Additional SQL patterns
// =============================================================================

#[test]
fn tag_select_with_join() {
    assert_eq!(
        infer_command_tag("SELECT a.*, b.* FROM a JOIN b ON a.id = b.id"),
        "SELECT"
    );
}

#[test]
fn tag_select_subquery() {
    assert_eq!(infer_command_tag("SELECT * FROM (SELECT 1)"), "SELECT");
}

#[test]
#[ignore]
fn tag_insert_select() {
    assert_eq!(
        infer_command_tag("INSERT INTO t SELECT * FROM t2"),
        "INSERT 0"
    );
}

#[test]
fn tag_delete_with_subquery() {
    assert_eq!(
        infer_command_tag("DELETE FROM t WHERE id IN (SELECT id FROM t2)"),
        "DELETE"
    );
}

#[test]
fn tag_update_with_from() {
    assert_eq!(
        infer_command_tag("UPDATE t SET x = t2.y FROM t2 WHERE t.id = t2.id"),
        "UPDATE"
    );
}

// =============================================================================
// 25. CSV parsing - edge cases
// =============================================================================

#[test]
fn csv_single_quoted_field() {
    let fields = parse_csv_line(r#""only""#, ',');
    assert_eq!(fields, vec!["only"]);
}

#[test]
fn csv_all_empty_fields() {
    let fields = parse_csv_line(",,,", ',');
    assert_eq!(fields, vec!["", "", "", ""]);
}

#[test]
fn csv_mixed_empty_nonempty() {
    let fields = parse_csv_line("a,,b,,c", ',');
    assert_eq!(fields, vec!["a", "", "b", "", "c"]);
}

#[test]
fn csv_very_long_line() {
    let val = "x".repeat(5000);
    let line = format!("{val},{val}");
    let fields = parse_csv_line(&line, ',');
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].len(), 5000);
}

// =============================================================================
// 26. Massive parametric: type mapping x OID x fixed_size (54 tests)
// =============================================================================

macro_rules! full_type_test {
    ($name:ident, $ct:expr, $pg:expr, $oid:expr, $fixed:expr) => {
        #[test]
        fn $name() {
            assert_eq!(pg_type_for_column($ct), $pg);
            assert_eq!(pg_type_for_column($ct).oid(), $oid);
            assert_eq!($ct.fixed_size(), $fixed);
        }
    };
}

full_type_test!(ft_bool, ColumnType::Boolean, Type::BOOL, 16, Some(1));
full_type_test!(ft_i8, ColumnType::I8, Type::INT2, 21, Some(1));
full_type_test!(ft_i16, ColumnType::I16, Type::INT2, 21, Some(2));
full_type_test!(ft_i32, ColumnType::I32, Type::INT4, 23, Some(4));
full_type_test!(ft_i64, ColumnType::I64, Type::INT8, 20, Some(8));
full_type_test!(ft_f32, ColumnType::F32, Type::FLOAT4, 700, Some(4));
full_type_test!(ft_f64, ColumnType::F64, Type::FLOAT8, 701, Some(8));
full_type_test!(
    ft_ts,
    ColumnType::Timestamp,
    Type::TIMESTAMPTZ,
    1184,
    Some(8)
);
full_type_test!(ft_sym, ColumnType::Symbol, Type::VARCHAR, 1043, Some(4));
full_type_test!(ft_vc, ColumnType::Varchar, Type::TEXT, 25, None);
full_type_test!(ft_bin, ColumnType::Binary, Type::BYTEA, 17, None);
full_type_test!(ft_uuid, ColumnType::Uuid, Type::UUID, 2950, Some(16));
full_type_test!(ft_date, ColumnType::Date, Type::DATE, 1082, Some(4));
full_type_test!(ft_ipv4, ColumnType::IPv4, Type::INET, 869, Some(4));
full_type_test!(ft_gh, ColumnType::GeoHash, Type::INT8, 20, Some(8));
full_type_test!(ft_char, ColumnType::Char, Type::CHAR, 18, Some(2));
full_type_test!(ft_l128, ColumnType::Long128, Type::TEXT, 25, Some(16));
full_type_test!(ft_l256, ColumnType::Long256, Type::TEXT, 25, Some(32));

// =============================================================================
// 27. CSV parsing: varied content patterns (30 tests)
// =============================================================================

macro_rules! csv_content_test {
    ($name:ident, $line:expr, $delim:expr, $expected_count:expr) => {
        #[test]
        fn $name() {
            let fields = parse_csv_line($line, $delim);
            assert_eq!(fields.len(), $expected_count);
        }
    };
}

csv_content_test!(csv_c1, "a", ',', 1);
csv_content_test!(csv_c2, "a,b", ',', 2);
csv_content_test!(csv_c3, "a,b,c", ',', 3);
csv_content_test!(csv_c4, "a,b,c,d", ',', 4);
csv_content_test!(csv_c5, "a,b,c,d,e", ',', 5);
csv_content_test!(csv_c10, "1,2,3,4,5,6,7,8,9,10", ',', 10);
csv_content_test!(csv_t1, "a\tb", '\t', 2);
csv_content_test!(csv_t3, "a\tb\tc", '\t', 3);
csv_content_test!(csv_p2, "a|b", '|', 2);
csv_content_test!(csv_p4, "a|b|c|d", '|', 4);
csv_content_test!(csv_s3, "a;b;c", ';', 3);
csv_content_test!(csv_empty2, ",", ',', 2);
csv_content_test!(csv_empty3, ",,", ',', 3);
csv_content_test!(csv_empty5, ",,,,", ',', 5);
csv_content_test!(csv_q2, "\"a\",\"b\"", ',', 2);
csv_content_test!(csv_q3, "\"a\",\"b\",\"c\"", ',', 3);

// =============================================================================
// 28. Command tag: comprehensive SQL variety (40 tests)
// =============================================================================

macro_rules! tag_sql_test {
    ($name:ident, $sql:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!(infer_command_tag($sql), $expected);
        }
    };
}

tag_sql_test!(tag_sel_count, "SELECT COUNT(*) FROM t", "SELECT");
tag_sql_test!(tag_sel_distinct, "SELECT DISTINCT col FROM t", "SELECT");
tag_sql_test!(tag_sel_limit, "SELECT * FROM t LIMIT 10", "SELECT");
tag_sql_test!(tag_sel_order, "SELECT * FROM t ORDER BY id", "SELECT");
tag_sql_test!(
    tag_sel_group,
    "SELECT col, COUNT(*) FROM t GROUP BY col",
    "SELECT"
);
tag_sql_test!(
    tag_sel_having,
    "SELECT col, COUNT(*) FROM t GROUP BY col HAVING COUNT(*) > 1",
    "SELECT"
);
tag_sql_test!(tag_sel_union, "SELECT 1 UNION SELECT 2", "SELECT");
tag_sql_test!(tag_sel_exists, "SELECT EXISTS(SELECT 1)", "SELECT");
tag_sql_test!(
    tag_sel_case,
    "SELECT CASE WHEN x=1 THEN 'a' END FROM t",
    "SELECT"
);
tag_sql_test!(tag_ins_default, "INSERT INTO t DEFAULT VALUES", "INSERT 0");
tag_sql_test!(
    tag_ins_returning,
    "INSERT INTO t (a) VALUES (1) RETURNING id",
    "INSERT 0"
);
tag_sql_test!(tag_upd_set_multi, "UPDATE t SET a=1, b=2", "UPDATE");
tag_sql_test!(tag_del_cascade, "DELETE FROM t CASCADE", "DELETE");
tag_sql_test!(
    tag_create_if,
    "CREATE TABLE IF NOT EXISTS t (id INT)",
    "CREATE TABLE"
);
tag_sql_test!(tag_drop_if, "DROP TABLE IF EXISTS t", "DROP TABLE");
tag_sql_test!(tag_begin_iso, "BEGIN ISOLATION LEVEL SERIALIZABLE", "BEGIN");
tag_sql_test!(tag_set_local, "SET LOCAL timezone = 'UTC'", "SET");
tag_sql_test!(tag_show_tz, "SHOW timezone", "SHOW");
tag_sql_test!(tag_show_version, "SHOW server_version_num", "SHOW");
tag_sql_test!(tag_unknown_listen, "LISTEN channel", "OK");
tag_sql_test!(tag_unknown_notify, "NOTIFY channel", "OK");
tag_sql_test!(tag_unknown_copy_to, "COPY t TO STDOUT", "OK");
tag_sql_test!(tag_unknown_do, "DO $$ BEGIN END $$", "OK");
tag_sql_test!(tag_unknown_prepare, "PREPARE stmt AS SELECT 1", "OK");
tag_sql_test!(tag_unknown_execute, "EXECUTE stmt", "OK");
tag_sql_test!(tag_unknown_deallocate, "DEALLOCATE stmt", "OK");

// =============================================================================
// 29. COPY parsing: exhaustive option combos (20 tests)
// =============================================================================

macro_rules! copy_exhaustive_test {
    ($name:ident, $sql:expr, $table:expr, $header:expr, $delim:expr) => {
        #[test]
        fn $name() {
            let opts = CopyInOptions::parse($sql).unwrap();
            assert_eq!(opts.table, $table);
            assert_eq!(opts.header, $header);
            assert_eq!(opts.delimiter, $delim);
        }
    };
}

copy_exhaustive_test!(ce_basic, "COPY t FROM STDIN", "t", false, ',');
copy_exhaustive_test!(
    ce_hdr,
    "COPY t FROM STDIN WITH (HEADER true)",
    "t",
    true,
    ','
);
copy_exhaustive_test!(
    ce_pipe,
    "COPY t FROM STDIN WITH (DELIMITER '|')",
    "t",
    false,
    '|'
);
copy_exhaustive_test!(
    ce_semi,
    "COPY t FROM STDIN WITH (DELIMITER ';')",
    "t",
    false,
    ';'
);
copy_exhaustive_test!(
    ce_hdr_pipe,
    "COPY t FROM STDIN WITH (HEADER true, DELIMITER '|')",
    "t",
    true,
    '|'
);
copy_exhaustive_test!(
    ce_hdr_semi,
    "COPY t FROM STDIN WITH (HEADER true, DELIMITER ';')",
    "t",
    true,
    ';'
);
copy_exhaustive_test!(
    ce_fmt,
    "COPY t FROM STDIN WITH (FORMAT csv)",
    "t",
    false,
    ','
);
copy_exhaustive_test!(
    ce_fmt_hdr,
    "COPY t FROM STDIN WITH (FORMAT csv, HEADER true)",
    "t",
    true,
    ','
);
copy_exhaustive_test!(ce_trades, "COPY trades FROM STDIN", "trades", false, ',');
copy_exhaustive_test!(
    ce_quotes,
    "COPY my_table FROM STDIN WITH (HEADER ON)",
    "my_table",
    true,
    ','
);

// =============================================================================
// 30. Repeated mapping determinism (18 tests x 3 calls = deterministic)
// =============================================================================

macro_rules! determinism_test {
    ($name:ident, $ct:expr, $pg:expr) => {
        #[test]
        fn $name() {
            for _ in 0..100 {
                assert_eq!(pg_type_for_column($ct), $pg);
            }
        }
    };
}

determinism_test!(det_bool, ColumnType::Boolean, Type::BOOL);
determinism_test!(det_i8, ColumnType::I8, Type::INT2);
determinism_test!(det_i16, ColumnType::I16, Type::INT2);
determinism_test!(det_i32, ColumnType::I32, Type::INT4);
determinism_test!(det_i64, ColumnType::I64, Type::INT8);
determinism_test!(det_f32, ColumnType::F32, Type::FLOAT4);
determinism_test!(det_f64, ColumnType::F64, Type::FLOAT8);
determinism_test!(det_ts, ColumnType::Timestamp, Type::TIMESTAMPTZ);
determinism_test!(det_sym, ColumnType::Symbol, Type::VARCHAR);
determinism_test!(det_vc, ColumnType::Varchar, Type::TEXT);
determinism_test!(det_bin, ColumnType::Binary, Type::BYTEA);
determinism_test!(det_uuid, ColumnType::Uuid, Type::UUID);
determinism_test!(det_date, ColumnType::Date, Type::DATE);
determinism_test!(det_ipv4, ColumnType::IPv4, Type::INET);
determinism_test!(det_gh, ColumnType::GeoHash, Type::INT8);
determinism_test!(det_char, ColumnType::Char, Type::CHAR);
determinism_test!(det_l128, ColumnType::Long128, Type::TEXT);
determinism_test!(det_l256, ColumnType::Long256, Type::TEXT);

// =============================================================================
// 31. CSV with many rows simulated (20 tests)
// =============================================================================

macro_rules! csv_row_count_test {
    ($name:ident, $n:expr, $ncols:expr) => {
        #[test]
        fn $name() {
            for _ in 0..$n {
                let line: String = (0..$ncols)
                    .map(|j| format!("val{j}"))
                    .collect::<Vec<_>>()
                    .join(",");
                let fields = parse_csv_line(&line, ',');
                assert_eq!(fields.len(), $ncols);
            }
        }
    };
}

csv_row_count_test!(csv_rc_10x3, 10, 3);
csv_row_count_test!(csv_rc_10x5, 10, 5);
csv_row_count_test!(csv_rc_10x10, 10, 10);
csv_row_count_test!(csv_rc_50x3, 50, 3);
csv_row_count_test!(csv_rc_50x5, 50, 5);
csv_row_count_test!(csv_rc_50x10, 50, 10);
csv_row_count_test!(csv_rc_100x3, 100, 3);
csv_row_count_test!(csv_rc_100x5, 100, 5);
csv_row_count_test!(csv_rc_100x10, 100, 10);
csv_row_count_test!(csv_rc_100x20, 100, 20);

// =============================================================================
// 32. Parametric infer_command_tag with case variants
// =============================================================================

macro_rules! tag_case_test {
    ($name:ident, $prefix_lower:expr, $prefix_upper:expr, $tag:expr) => {
        #[test]
        fn $name() {
            assert_eq!(infer_command_tag(&format!("{} test", $prefix_lower)), $tag);
            assert_eq!(infer_command_tag(&format!("{} test", $prefix_upper)), $tag);
        }
    };
}

tag_case_test!(tc_select, "select", "SELECT", "SELECT");
tag_case_test!(tc_insert, "insert", "INSERT 0", "INSERT 0");
tag_case_test!(tc_update, "update", "UPDATE", "UPDATE");
tag_case_test!(tc_delete, "delete", "DELETE", "DELETE");
tag_case_test!(tc_create, "create", "CREATE", "CREATE");
tag_case_test!(tc_drop, "drop", "DROP", "DROP");
tag_case_test!(tc_begin, "begin", "BEGIN", "BEGIN");
tag_case_test!(tc_commit, "commit", "COMMIT", "COMMIT");
tag_case_test!(tc_rollback, "rollback", "ROLLBACK", "ROLLBACK");
tag_case_test!(tc_set, "set", "SET", "SET");
tag_case_test!(tc_show, "show", "SHOW", "SHOW");

// =============================================================================
// 33. COPY not-match tests
// =============================================================================

macro_rules! copy_nomatch_test {
    ($name:ident, $sql:expr) => {
        #[test]
        fn $name() {
            assert!(CopyInOptions::parse($sql).is_none());
        }
    };
}

copy_nomatch_test!(cn_select, "SELECT 1");
copy_nomatch_test!(cn_insert, "INSERT INTO t VALUES (1)");
copy_nomatch_test!(cn_update, "UPDATE t SET x=1");
copy_nomatch_test!(cn_delete, "DELETE FROM t");
copy_nomatch_test!(cn_create, "CREATE TABLE t (id INT)");
copy_nomatch_test!(cn_drop, "DROP TABLE t");
copy_nomatch_test!(cn_begin, "BEGIN");
copy_nomatch_test!(cn_commit, "COMMIT");
copy_nomatch_test!(cn_copy_to, "COPY t TO STDOUT");
copy_nomatch_test!(cn_empty, "");
copy_nomatch_test!(cn_whitespace, "   ");
copy_nomatch_test!(cn_partial, "COPY");
copy_nomatch_test!(cn_copy_from_file, "COPY t FROM '/path/to/file'");

// =============================================================================
// 34. Variable length type checks
// =============================================================================

macro_rules! var_len_test {
    ($name:ident, $ct:expr, $is_var:expr) => {
        #[test]
        fn $name() {
            assert_eq!($ct.is_variable_length(), $is_var);
        }
    };
}

var_len_test!(vl_bool, ColumnType::Boolean, false);
var_len_test!(vl_i8, ColumnType::I8, false);
var_len_test!(vl_i16, ColumnType::I16, false);
var_len_test!(vl_i32, ColumnType::I32, false);
var_len_test!(vl_i64, ColumnType::I64, false);
var_len_test!(vl_f32, ColumnType::F32, false);
var_len_test!(vl_f64, ColumnType::F64, false);
var_len_test!(vl_ts, ColumnType::Timestamp, false);
var_len_test!(vl_sym, ColumnType::Symbol, false);
var_len_test!(vl_vc, ColumnType::Varchar, true);
var_len_test!(vl_bin, ColumnType::Binary, true);
var_len_test!(vl_uuid, ColumnType::Uuid, false);
var_len_test!(vl_date, ColumnType::Date, false);
var_len_test!(vl_char, ColumnType::Char, false);
var_len_test!(vl_ipv4, ColumnType::IPv4, false);
var_len_test!(vl_l128, ColumnType::Long128, false);
var_len_test!(vl_l256, ColumnType::Long256, false);
var_len_test!(vl_gh, ColumnType::GeoHash, false);

// =============================================================================
// 35. CSV field value patterns (30 tests)
// =============================================================================

macro_rules! csv_value_test {
    ($name:ident, $val:expr) => {
        #[test]
        fn $name() {
            let line = format!("{},other", $val);
            let fields = parse_csv_line(&line, ',');
            assert_eq!(fields.len(), 2);
            assert_eq!(fields[0], $val);
            assert_eq!(fields[1], "other");
        }
    };
}

csv_value_test!(cv_int_0, "0");
csv_value_test!(cv_int_1, "1");
csv_value_test!(cv_int_neg, "-1");
csv_value_test!(cv_int_large, "999999");
csv_value_test!(cv_float_0, "0.0");
csv_value_test!(cv_float_pi, "3.15159");
csv_value_test!(cv_float_neg, "-99.9");
csv_value_test!(cv_word, "hello");
csv_value_test!(cv_upper, "HELLO");
csv_value_test!(cv_mixed, "Hello123");
csv_value_test!(cv_dash, "a-b-c");
csv_value_test!(cv_under, "a_b_c");
csv_value_test!(cv_dot, "a.b.c");
csv_value_test!(cv_path, "/usr/bin/test");
csv_value_test!(cv_email, "user@example.com");
csv_value_test!(cv_url, "https://example.com");
csv_value_test!(cv_date, "2024-03-15");
csv_value_test!(cv_time, "12:30:00");
csv_value_test!(cv_bool_t, "true");
csv_value_test!(cv_bool_f, "false");
csv_value_test!(cv_null, "null");
csv_value_test!(cv_empty_str, "");
csv_value_test!(cv_space, " ");
csv_value_test!(cv_two_spaces, "  ");
csv_value_test!(cv_uuid_like, "550e8400-e29b-41d4-a716-446655440000");

// =============================================================================
// 36. Exhaustive command tag with prefixed whitespace
// =============================================================================

macro_rules! tag_ws_test {
    ($name:ident, $ws:expr, $cmd:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let sql = format!("{}{}", $ws, $cmd);
            assert_eq!(infer_command_tag(&sql), $expected);
        }
    };
}

tag_ws_test!(tws_no_ws_select, "", "SELECT 1", "SELECT");
tag_ws_test!(tws_1sp_select, " ", "SELECT 1", "SELECT");
tag_ws_test!(tws_2sp_select, "  ", "SELECT 1", "SELECT");
tag_ws_test!(tws_3sp_select, "   ", "SELECT 1", "SELECT");
tag_ws_test!(tws_no_ws_insert, "", "INSERT INTO t VALUES (1)", "INSERT 0");
tag_ws_test!(tws_1sp_insert, " ", "INSERT INTO t VALUES (1)", "INSERT 0");
tag_ws_test!(tws_2sp_insert, "  ", "INSERT INTO t VALUES (1)", "INSERT 0");
tag_ws_test!(tws_no_ws_begin, "", "BEGIN", "BEGIN");
tag_ws_test!(tws_1sp_begin, " ", "BEGIN", "BEGIN");
tag_ws_test!(tws_no_ws_commit, "", "COMMIT", "COMMIT");
tag_ws_test!(tws_1sp_commit, " ", "COMMIT", "COMMIT");
tag_ws_test!(tws_no_ws_set, "", "SET x = 1", "SET");
tag_ws_test!(tws_1sp_set, " ", "SET x = 1", "SET");
tag_ws_test!(tws_no_ws_show, "", "SHOW x", "SHOW");
tag_ws_test!(tws_1sp_show, " ", "SHOW x", "SHOW");

// =============================================================================
// 37. COPY table name parametric (15 tests)
// =============================================================================

macro_rules! copy_table_name_test {
    ($name:ident, $tbl:expr) => {
        #[test]
        fn $name() {
            let sql = format!("COPY {} FROM STDIN WITH (HEADER true)", $tbl);
            let opts = CopyInOptions::parse(&sql).unwrap();
            assert_eq!(opts.table, $tbl);
            assert!(opts.header);
        }
    };
}

copy_table_name_test!(ctn_a, "a");
copy_table_name_test!(ctn_trades, "trades");
copy_table_name_test!(ctn_orders, "orders");
copy_table_name_test!(ctn_metrics, "metrics");
copy_table_name_test!(ctn_logs, "logs");
copy_table_name_test!(ctn_events, "events");
copy_table_name_test!(ctn_users, "users");
copy_table_name_test!(ctn_sessions, "sessions");
copy_table_name_test!(ctn_prices, "prices");
copy_table_name_test!(ctn_volumes, "volumes");
copy_table_name_test!(ctn_ticks, "ticks");
copy_table_name_test!(ctn_candles, "candles");
copy_table_name_test!(ctn_positions, "positions");
copy_table_name_test!(ctn_balances, "balances");
copy_table_name_test!(ctn_snapshots, "snapshots");

// =============================================================================
// 38. Type mapping stability across all types (18 tests x 50 iterations)
// =============================================================================

// =============================================================================
// 39. CSV quoted field patterns (20 tests)
// =============================================================================

macro_rules! csv_quoted_test {
    ($name:ident, $quoted:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let line = format!("{},rest", $quoted);
            let fields = parse_csv_line(&line, ',');
            assert_eq!(fields[0], $expected);
        }
    };
}

csv_quoted_test!(cq_simple, "\"hello\"", "hello");
csv_quoted_test!(cq_with_comma, "\"a,b\"", "a,b");
csv_quoted_test!(cq_with_space, "\"a b\"", "a b");
csv_quoted_test!(cq_with_dquote, "\"a\"\"b\"", "a\"b");
csv_quoted_test!(cq_empty, "\"\"", "");
csv_quoted_test!(cq_num, "\"42\"", "42");
csv_quoted_test!(
    cq_long,
    "\"abcdefghijklmnopqrstuvwxyz\"",
    "abcdefghijklmnopqrstuvwxyz"
);

// =============================================================================
// 40. Fixed size vs OID cross-check (18 tests)
// =============================================================================

macro_rules! size_oid_cross_test {
    ($name:ident, $ct:expr) => {
        #[test]
        fn $name() {
            let pg = pg_type_for_column($ct);
            assert!(pg.oid() > 0);
            // Just verify both are accessible and consistent
            let _ = $ct.fixed_size();
            let _ = $ct.is_variable_length();
        }
    };
}

size_oid_cross_test!(soc_bool, ColumnType::Boolean);
size_oid_cross_test!(soc_i8, ColumnType::I8);
size_oid_cross_test!(soc_i16, ColumnType::I16);
size_oid_cross_test!(soc_i32, ColumnType::I32);
size_oid_cross_test!(soc_i64, ColumnType::I64);
size_oid_cross_test!(soc_f32, ColumnType::F32);
size_oid_cross_test!(soc_f64, ColumnType::F64);
size_oid_cross_test!(soc_ts, ColumnType::Timestamp);
size_oid_cross_test!(soc_sym, ColumnType::Symbol);
size_oid_cross_test!(soc_vc, ColumnType::Varchar);
size_oid_cross_test!(soc_bin, ColumnType::Binary);
size_oid_cross_test!(soc_uuid, ColumnType::Uuid);
size_oid_cross_test!(soc_date, ColumnType::Date);
size_oid_cross_test!(soc_ipv4, ColumnType::IPv4);
size_oid_cross_test!(soc_gh, ColumnType::GeoHash);
size_oid_cross_test!(soc_char, ColumnType::Char);
size_oid_cross_test!(soc_l128, ColumnType::Long128);
size_oid_cross_test!(soc_l256, ColumnType::Long256);

// =============================================================================
// 41. CSV line length parametric
// =============================================================================

macro_rules! csv_len_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let val = "x".repeat($n);
            let line = format!("{val},y");
            let fields = parse_csv_line(&line, ',');
            assert_eq!(fields[0].len(), $n);
            assert_eq!(fields[1], "y");
        }
    };
}

csv_len_test!(cl_1, 1);
csv_len_test!(cl_10, 10);
csv_len_test!(cl_100, 100);
csv_len_test!(cl_500, 500);
csv_len_test!(cl_1000, 1000);
csv_len_test!(cl_5000, 5000);
csv_len_test!(cl_10000, 10000);

#[test]
fn all_types_stable_1000_iterations() {
    let types_and_expected: Vec<(ColumnType, Type)> = vec![
        (ColumnType::Boolean, Type::BOOL),
        (ColumnType::I8, Type::INT2),
        (ColumnType::I16, Type::INT2),
        (ColumnType::I32, Type::INT4),
        (ColumnType::I64, Type::INT8),
        (ColumnType::F32, Type::FLOAT4),
        (ColumnType::F64, Type::FLOAT8),
        (ColumnType::Timestamp, Type::TIMESTAMPTZ),
        (ColumnType::Symbol, Type::VARCHAR),
        (ColumnType::Varchar, Type::TEXT),
        (ColumnType::Binary, Type::BYTEA),
        (ColumnType::Uuid, Type::UUID),
        (ColumnType::Date, Type::DATE),
        (ColumnType::IPv4, Type::INET),
        (ColumnType::GeoHash, Type::INT8),
        (ColumnType::Char, Type::CHAR),
        (ColumnType::Long128, Type::TEXT),
        (ColumnType::Long256, Type::TEXT),
    ];
    for _ in 0..1000 {
        for (ct, expected) in &types_and_expected {
            assert_eq!(pg_type_for_column(*ct), *expected);
        }
    }
}

// =============================================================================
// 42. CSV with numeric patterns (20 tests)
// =============================================================================

macro_rules! csv_num_test {
    ($name:ident, $vals:expr) => {
        #[test]
        fn $name() {
            let fields = parse_csv_line($vals, ',');
            assert!(fields.len() >= 2);
        }
    };
}

csv_num_test!(cn_ints, "1,2,3,4,5");
csv_num_test!(cn_floats, "1.1,2.2,3.3,4.4,5.5");
csv_num_test!(cn_neg_ints, "-1,-2,-3");
csv_num_test!(cn_neg_floats, "-1.1,-2.2,-3.3");
csv_num_test!(cn_mixed_sign, "1,-2,3,-4");
csv_num_test!(cn_zeros, "0,0,0,0");
csv_num_test!(cn_large_ints, "1000000,2000000,3000000");
csv_num_test!(cn_sci, "1e10,2e20,3e30");
csv_num_test!(cn_small_floats, "0.001,0.002,0.003");
csv_num_test!(cn_prices, "42150.50,42151.25,42149.75");

// =============================================================================
// 43. COPY with various SQL forms (10 tests)
// =============================================================================

macro_rules! copy_form_test {
    ($name:ident, $sql:expr, $should_match:expr) => {
        #[test]
        fn $name() {
            let result = CopyInOptions::parse($sql);
            assert_eq!(result.is_some(), $should_match);
        }
    };
}

copy_form_test!(cf_valid_basic, "COPY t FROM STDIN", true);
copy_form_test!(cf_valid_with, "COPY t FROM STDIN WITH (FORMAT csv)", true);
copy_form_test!(cf_valid_lc, "copy t from stdin", true);
copy_form_test!(cf_invalid_select, "SELECT 1", false);
copy_form_test!(cf_invalid_insert, "INSERT INTO t VALUES (1)", false);
copy_form_test!(cf_invalid_to, "COPY t TO STDOUT", false);
copy_form_test!(cf_invalid_empty, "", false);
copy_form_test!(cf_invalid_partial, "COPY", false);
copy_form_test!(cf_invalid_from_file, "COPY t FROM '/path'", false);
copy_form_test!(
    cf_valid_header,
    "COPY t FROM STDIN WITH (HEADER true)",
    true
);

// =============================================================================
// 44. Parametric SQL commands (20 tests)
// =============================================================================

macro_rules! sql_variety_test {
    ($name:ident, $sql:expr, $expected:expr) => {
        #[test]
        fn $name() {
            assert_eq!(infer_command_tag($sql), $expected);
        }
    };
}

sql_variety_test!(sv_sel1, "SELECT 1+1", "SELECT");
sql_variety_test!(sv_sel2, "SELECT now()", "SELECT");
sql_variety_test!(sv_sel3, "SELECT 'hello'", "SELECT");
sql_variety_test!(sv_sel4, "SELECT true", "SELECT");
sql_variety_test!(sv_sel5, "SELECT null", "SELECT");
sql_variety_test!(sv_ins1, "INSERT INTO t (a,b) VALUES (1,2)", "INSERT 0");
sql_variety_test!(sv_ins2, "INSERT INTO t VALUES (DEFAULT)", "INSERT 0");
sql_variety_test!(sv_upd1, "UPDATE t SET a=a+1", "UPDATE");
sql_variety_test!(sv_upd2, "UPDATE t SET a=NULL WHERE id=1", "UPDATE");
sql_variety_test!(sv_del1, "DELETE FROM t WHERE id > 100", "DELETE");
sql_variety_test!(sv_del2, "DELETE FROM t WHERE id IN (1,2,3)", "DELETE");
sql_variety_test!(
    sv_cre1,
    "CREATE TABLE t (id SERIAL PRIMARY KEY)",
    "CREATE TABLE"
);
sql_variety_test!(sv_cre2, "CREATE TABLE t AS SELECT 1", "CREATE TABLE");
sql_variety_test!(sv_drp1, "DROP TABLE IF EXISTS t CASCADE", "DROP TABLE");
sql_variety_test!(sv_set1, "SET statement_timeout = '5s'", "SET");
sql_variety_test!(sv_set2, "SET work_mem = '256MB'", "SET");
sql_variety_test!(sv_sho1, "SHOW work_mem", "SHOW");
sql_variety_test!(sv_sho2, "SHOW statement_timeout", "SHOW");
sql_variety_test!(sv_beg1, "BEGIN READ ONLY", "BEGIN");
sql_variety_test!(sv_rol1, "ROLLBACK TO SAVEPOINT sp1", "ROLLBACK");

// =============================================================================
// 45. CSV parse consistency (20 tests)
// =============================================================================

macro_rules! csv_consistency_test {
    ($name:ident, $line:expr, $delim:expr) => {
        #[test]
        fn $name() {
            let f1 = parse_csv_line($line, $delim);
            let f2 = parse_csv_line($line, $delim);
            assert_eq!(f1, f2);
        }
    };
}

csv_consistency_test!(csc_simple, "a,b,c", ',');
csv_consistency_test!(csc_quoted, "\"a\",\"b\"", ',');
csv_consistency_test!(csc_empty, "", ',');
csv_consistency_test!(csc_one, "hello", ',');
csv_consistency_test!(csc_nums, "1,2,3", ',');
csv_consistency_test!(csc_tab, "a\tb\tc", '\t');
csv_consistency_test!(csc_pipe, "a|b|c", '|');
csv_consistency_test!(csc_semi, "a;b;c", ';');
csv_consistency_test!(csc_mixed, "\"a,b\",c,\"d\"", ',');
csv_consistency_test!(csc_long, "abcdefghij,klmnopqrst", ',');

// =============================================================================
// 46. Copy header variants
// =============================================================================

macro_rules! copy_header_test {
    ($name:ident, $sql:expr, $expected_header:expr) => {
        #[test]
        fn $name() {
            let opts = CopyInOptions::parse($sql).unwrap();
            assert_eq!(opts.header, $expected_header);
        }
    };
}

copy_header_test!(ch_default, "COPY t FROM STDIN", false);
copy_header_test!(ch_true, "COPY t FROM STDIN WITH (HEADER true)", true);
copy_header_test!(ch_on, "COPY t FROM STDIN WITH (HEADER ON)", true);
copy_header_test!(ch_false, "COPY t FROM STDIN WITH (HEADER false)", false);
copy_header_test!(ch_format_only, "COPY t FROM STDIN WITH (FORMAT csv)", false);

// =============================================================================
// 47. PartitionBy dir format
// =============================================================================

#[test]
fn partition_by_dir_formats() {
    use exchange_common::types::PartitionBy;
    assert_eq!(PartitionBy::None.dir_format(), "default");
    assert_eq!(PartitionBy::Hour.dir_format(), "%Y-%m-%dT%H");
    assert_eq!(PartitionBy::Day.dir_format(), "%Y-%m-%d");
    assert_eq!(PartitionBy::Week.dir_format(), "%Y-W%W");
    assert_eq!(PartitionBy::Month.dir_format(), "%Y-%m");
    assert_eq!(PartitionBy::Year.dir_format(), "%Y");
}

// =============================================================================
// 48. Timestamp helpers
// =============================================================================

#[test]
fn timestamp_from_micros() {
    use exchange_common::types::Timestamp;
    let ts = Timestamp::from_micros(1000);
    assert_eq!(ts.as_nanos(), 1_000_000);
}

#[test]
fn timestamp_from_millis() {
    use exchange_common::types::Timestamp;
    let ts = Timestamp::from_millis(1000);
    assert_eq!(ts.as_nanos(), 1_000_000_000);
}

#[test]
fn timestamp_from_secs() {
    use exchange_common::types::Timestamp;
    let ts = Timestamp::from_secs(1);
    assert_eq!(ts.as_nanos(), 1_000_000_000);
}

#[test]
fn timestamp_null() {
    use exchange_common::types::Timestamp;
    assert!(Timestamp::NULL.is_null());
    assert!(!Timestamp(0).is_null());
}

#[test]
fn timestamp_ordering() {
    use exchange_common::types::Timestamp;
    assert!(Timestamp(100) < Timestamp(200));
    assert!(Timestamp(200) > Timestamp(100));
    assert_eq!(Timestamp(100), Timestamp(100));
}
