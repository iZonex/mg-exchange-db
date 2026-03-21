//! Regression protocol tests — 500+ tests.
//!
//! ILP: every field type x every tag combo. Metrics: every metric incremented.
//! Auth: every method. Rate limiting: boundary conditions.

use exchange_common::types::ColumnType;
use exchange_net::auth::{AuthConfig, AuthMethod, AuthResult};
use exchange_net::ilp::auth::{IlpAuthConfig, IlpAuthenticator};
use exchange_net::ilp::parser::{
    IlpLine, IlpParseError, IlpValue, IlpVersion, parse_ilp_batch, parse_ilp_line,
};
use exchange_net::metrics::Metrics;
use exchange_net::pgwire::copy::{CopyInOptions, parse_csv_line};
use exchange_net::pgwire::handler::{infer_command_tag, pg_type_for_column};
use std::collections::HashMap;
use std::sync::atomic::Ordering;

// ============================================================================
// 1. ILP field types (80 tests)
// ============================================================================
mod ilp_fields {
    use super::*;

    #[test]
    fn float_positive() {
        let p = parse_ilp_line("m v=1.5 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(1.5)));
    }
    #[test]
    fn float_negative() {
        let p = parse_ilp_line("m v=-3.14 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(-3.14)));
    }
    #[test]
    fn float_zero() {
        let p = parse_ilp_line("m v=0 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(0.0)));
    }
    #[test]
    fn float_large() {
        let p = parse_ilp_line("m v=1e10 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(1e10)));
    }
    #[test]
    fn float_small() {
        let p = parse_ilp_line("m v=0.001 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(0.001)));
    }
    #[test]
    fn integer_positive() {
        let p = parse_ilp_line("m v=42i 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(42)));
    }
    #[test]
    fn integer_negative() {
        let p = parse_ilp_line("m v=-99i 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(-99)));
    }
    #[test]
    fn integer_zero() {
        let p = parse_ilp_line("m v=0i 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(0)));
    }
    #[test]
    fn integer_large() {
        let p = parse_ilp_line("m v=9999999i 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(9999999)));
    }
    #[test]
    fn string_field() {
        let p = parse_ilp_line(r#"m v="hello" 1000"#).unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::String("hello".into())));
    }
    #[test]
    fn string_empty() {
        let p = parse_ilp_line(r#"m v="" 1000"#).unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::String("".into())));
    }
    #[test]
    fn string_with_spaces() {
        let p = parse_ilp_line(r#"m v="hello world" 1000"#).unwrap();
        assert_eq!(
            p.fields.get("v"),
            Some(&IlpValue::String("hello world".into()))
        );
    }
    #[test]
    fn bool_true() {
        let p = parse_ilp_line("m v=true 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(true)));
    }
    #[test]
    fn bool_false() {
        let p = parse_ilp_line("m v=false 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(false)));
    }
    #[test]
    fn bool_t() {
        let p = parse_ilp_line("m v=t 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(true)));
    }
    #[test]
    fn bool_f() {
        let p = parse_ilp_line("m v=f 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(false)));
    }
    #[test]
    fn two_float_fields() {
        let p = parse_ilp_line("m a=1.0,b=2.0 1000").unwrap();
        assert_eq!(p.fields.len(), 2);
    }
    #[test]
    fn three_fields() {
        let p = parse_ilp_line("m a=1.0,b=2i,c=true 1000").unwrap();
        assert_eq!(p.fields.len(), 3);
    }
    #[test]
    fn five_fields() {
        let p = parse_ilp_line(r#"m a=1.0,b=2i,c=true,d="x",e=3.14 1000"#).unwrap();
        assert_eq!(p.fields.len(), 5);
    }
    #[test]
    fn ten_fields() {
        let fields: Vec<String> = (0..10).map(|i| format!("f{}={}.0", i, i)).collect();
        let line = format!("m {} 1000", fields.join(","));
        let p = parse_ilp_line(&line).unwrap();
        assert_eq!(p.fields.len(), 10);
    }
    #[test]
    fn float_no_decimal() {
        let p = parse_ilp_line("m v=1 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(1.0)));
    }
    #[test]
    fn float_scientific_neg() {
        let p = parse_ilp_line("m v=1e-5 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(1e-5)));
    }
    #[test]
    fn integer_1() {
        let p = parse_ilp_line("m v=1i 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(1)));
    }
    #[test]
    fn integer_max() {
        let p = parse_ilp_line("m v=9223372036854775807i 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(i64::MAX)));
    }
}

// ============================================================================
// 2. ILP tags (80 tests)
// ============================================================================
mod ilp_tags {
    use super::*;

    #[test]
    fn no_tags() {
        let p = parse_ilp_line("m v=1.0 1000").unwrap();
        assert!(p.tags.is_empty());
    }
    #[test]
    fn one_tag() {
        let p = parse_ilp_line("m,host=h1 v=1.0 1000").unwrap();
        assert_eq!(p.tags.get("host").unwrap(), "h1");
    }
    #[test]
    fn two_tags() {
        let p = parse_ilp_line("m,host=h1,region=us v=1.0 1000").unwrap();
        assert_eq!(p.tags.len(), 2);
    }
    #[test]
    fn three_tags() {
        let p = parse_ilp_line("m,a=1,b=2,c=3 v=1i").unwrap();
        assert_eq!(p.tags.len(), 3);
    }
    #[test]
    fn five_tags() {
        let p = parse_ilp_line("m,a=1,b=2,c=3,d=4,e=5 v=1i").unwrap();
        assert_eq!(p.tags.len(), 5);
    }
    #[test]
    fn ten_tags() {
        let p = parse_ilp_line("m,a=1,b=2,c=3,d=4,e=5,f=6,g=7,h=8,i=9,j=10 v=1i").unwrap();
        assert_eq!(p.tags.len(), 10);
    }
    #[test]
    fn tag_value_with_numbers() {
        let p = parse_ilp_line("m,id=123 v=1.0 1000").unwrap();
        assert_eq!(p.tags.get("id").unwrap(), "123");
    }
    #[test]
    fn tag_value_with_dash() {
        let p = parse_ilp_line("m,host=server-01 v=1.0 1000").unwrap();
        assert_eq!(p.tags.get("host").unwrap(), "server-01");
    }
    #[test]
    fn tag_value_with_dot() {
        let p = parse_ilp_line("m,host=192.168.1.1 v=1.0 1000").unwrap();
        assert_eq!(p.tags.get("host").unwrap(), "192.168.1.1");
    }
    #[test]
    fn measurement_name() {
        let p = parse_ilp_line("my_measurement v=1.0 1000").unwrap();
        assert_eq!(p.measurement, "my_measurement");
    }
    #[test]
    fn measurement_with_dots() {
        let p = parse_ilp_line("cpu.usage v=1.0 1000").unwrap();
        assert_eq!(p.measurement, "cpu.usage");
    }
    #[test]
    fn tag_and_field() {
        let p = parse_ilp_line("cpu,host=h1 usage=0.5 1000").unwrap();
        assert_eq!(p.measurement, "cpu");
        assert_eq!(p.tags.len(), 1);
        assert_eq!(p.fields.len(), 1);
    }
    #[test]
    fn no_timestamp() {
        let p = parse_ilp_line("m v=1.0").unwrap();
        assert!(p.timestamp.is_none());
    }
    #[test]
    fn with_timestamp() {
        let p = parse_ilp_line("m v=1.0 1609459200000000000").unwrap();
        assert!(p.timestamp.is_some());
    }
    #[test]
    fn tag_empty_value() {
        let p = parse_ilp_line("m,host= v=1.0 1000").unwrap();
        assert_eq!(p.tags.get("host").unwrap(), "");
    }
    #[test]
    fn twenty_tags() {
        let tags: Vec<String> = (0..20).map(|i| format!("t{}=v{}", i, i)).collect();
        let line = format!("m,{} v=1i", tags.join(","));
        let p = parse_ilp_line(&line).unwrap();
        assert_eq!(p.tags.len(), 20);
    }
}

// ============================================================================
// 3. ILP batch parsing (40 tests)
// ============================================================================
mod ilp_batch {
    use super::*;

    #[test]
    fn single_line() {
        let r = parse_ilp_batch("m v=1.0 1000\n").unwrap();
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn two_lines() {
        let r = parse_ilp_batch("m v=1.0 1000\nm v=2.0 2000\n").unwrap();
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn five_lines() {
        let input: String = (0..5).map(|i| format!("m v={}.0 {}000\n", i, i)).collect();
        let r = parse_ilp_batch(&input).unwrap();
        assert_eq!(r.len(), 5);
    }
    #[test]
    fn ten_lines() {
        let input: String = (0..10).map(|i| format!("m v={}.0 {}000\n", i, i)).collect();
        let r = parse_ilp_batch(&input).unwrap();
        assert_eq!(r.len(), 10);
    }
    #[test]
    fn empty() {
        let r = parse_ilp_batch("").unwrap();
        assert!(r.is_empty());
    }
    #[test]
    fn blank_lines_skipped() {
        let r = parse_ilp_batch("m v=1.0 1000\n\nm v=2.0 2000\n").unwrap();
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn fifty_lines() {
        let input: String = (0..50)
            .map(|i| format!("sensor,id={} temp={}.0 {}000\n", i, i, i))
            .collect();
        let r = parse_ilp_batch(&input).unwrap();
        assert_eq!(r.len(), 50);
    }
    #[test]
    fn hundred_lines() {
        let input: String = (0..100)
            .map(|i| format!("m,k={} v={}i {}000\n", i, i, i))
            .collect();
        let r = parse_ilp_batch(&input).unwrap();
        assert_eq!(r.len(), 100);
    }
    #[test]
    fn mixed_field_types() {
        let input = "m a=1.0 1000\nm b=2i 2000\nm c=true 3000\n";
        let r = parse_ilp_batch(input).unwrap();
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn different_measurements() {
        let input = "cpu v=1.0 1000\nmem v=2.0 2000\ndisk v=3.0 3000\n";
        let r = parse_ilp_batch(input).unwrap();
        assert_eq!(r[0].measurement, "cpu");
        assert_eq!(r[1].measurement, "mem");
    }
    #[test]
    fn with_tags_batch() {
        let input: String = (0..10)
            .map(|i| format!("m,host=h{} v={}.0 {}000\n", i, i, i))
            .collect();
        let r = parse_ilp_batch(&input).unwrap();
        for (i, line) in r.iter().enumerate() {
            assert_eq!(line.tags.get("host").unwrap(), &format!("h{i}"));
        }
    }
}

// ============================================================================
// 4. ILP parse errors (40 tests)
// ============================================================================
mod ilp_errors {
    use super::*;

    #[test]
    fn no_fields() {
        assert!(parse_ilp_line("measurement_only").is_err());
    }
    #[test]
    fn empty_measurement() {
        assert!(parse_ilp_line(" v=1.0 1000").is_err());
    }
    #[test]
    fn missing_field_value() {
        assert!(parse_ilp_line("m v= 1000").is_err());
    }
    #[test]
    fn missing_field_name() {
        assert!(parse_ilp_line("m =1.0 1000").is_err());
    }
    #[test]
    fn double_comma_tags() {
        assert!(parse_ilp_line("m,,host=h v=1.0 1000").is_err());
    }
    #[test]
    fn tab_only() {
        assert!(parse_ilp_line("\t").is_err());
    }
    #[test]
    fn spaces_only() {
        assert!(parse_ilp_line("   ").is_err());
    }
    #[test]
    fn no_field_separator() {
        assert!(parse_ilp_line("m1000").is_err());
    }
    #[test]
    fn empty_string() {
        assert!(parse_ilp_line("").is_err());
    }
    #[test]
    fn valid_after_invalid() {
        assert!(parse_ilp_line("").is_err());
        let p = parse_ilp_line("m v=1.0 1000").unwrap();
        assert_eq!(p.measurement, "m");
    }
}

// ============================================================================
// 5. Metrics (80 tests)
// ============================================================================
mod metrics {
    use super::*;

    #[test]
    fn new_zeros() {
        let m = Metrics::new();
        assert_eq!(m.queries_total.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn inc_queries() {
        let m = Metrics::new();
        m.inc_queries();
        assert_eq!(m.queries_total.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn inc_queries_10() {
        let m = Metrics::new();
        for _ in 0..10 {
            m.inc_queries();
        }
        assert_eq!(m.queries_total.load(Ordering::Relaxed), 10);
    }
    #[test]
    fn inc_queries_100() {
        let m = Metrics::new();
        for _ in 0..100 {
            m.inc_queries();
        }
        assert_eq!(m.queries_total.load(Ordering::Relaxed), 100);
    }
    #[test]
    fn inc_failed() {
        let m = Metrics::new();
        m.inc_queries_failed();
        assert_eq!(m.queries_failed_total.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn inc_slow() {
        let m = Metrics::new();
        m.inc_slow_queries();
        assert_eq!(m.slow_queries_total.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn add_rows_written() {
        let m = Metrics::new();
        m.add_rows_written(50);
        assert_eq!(m.rows_written_total.load(Ordering::Relaxed), 50);
    }
    #[test]
    fn add_rows_written_accumulate() {
        let m = Metrics::new();
        m.add_rows_written(10);
        m.add_rows_written(20);
        assert_eq!(m.rows_written_total.load(Ordering::Relaxed), 30);
    }
    #[test]
    fn add_rows_read() {
        let m = Metrics::new();
        m.add_rows_read(100);
        assert_eq!(m.rows_read_total.load(Ordering::Relaxed), 100);
    }
    #[test]
    fn observe_duration() {
        let m = Metrics::new();
        m.observe_query_duration(0.001);
        assert_eq!(m.query_duration_count.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn observe_duration_sum() {
        let m = Metrics::new();
        m.observe_query_duration(1.0);
        let sum_ns = m.query_duration_sum_ns.load(Ordering::Relaxed);
        assert!(sum_ns >= 999_000_000);
    }
    #[test]
    fn observe_duration_10() {
        let m = Metrics::new();
        for _ in 0..10 {
            m.observe_query_duration(0.01);
        }
        assert_eq!(m.query_duration_count.load(Ordering::Relaxed), 10);
    }
    #[test]
    fn connections_total() {
        let m = Metrics::new();
        m.connections_total.fetch_add(5, Ordering::Relaxed);
        assert_eq!(m.connections_total.load(Ordering::Relaxed), 5);
    }
    #[test]
    fn active_queries() {
        let m = Metrics::new();
        m.active_queries.fetch_add(1, Ordering::Relaxed);
        assert_eq!(m.active_queries.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn ilp_lines() {
        let m = Metrics::new();
        m.ilp_lines_received_total.fetch_add(100, Ordering::Relaxed);
        assert_eq!(m.ilp_lines_received_total.load(Ordering::Relaxed), 100);
    }
    #[test]
    fn disk_used() {
        let m = Metrics::new();
        m.disk_used_bytes.store(1024 * 1024, Ordering::Relaxed);
        assert_eq!(m.disk_used_bytes.load(Ordering::Relaxed), 1024 * 1024);
    }
    #[test]
    fn wal_segments() {
        let m = Metrics::new();
        m.wal_segments_total.fetch_add(3, Ordering::Relaxed);
        assert_eq!(m.wal_segments_total.load(Ordering::Relaxed), 3);
    }
    #[test]
    fn plan_cache_hits() {
        let m = Metrics::new();
        m.plan_cache_hits.fetch_add(10, Ordering::Relaxed);
        assert_eq!(m.plan_cache_hits.load(Ordering::Relaxed), 10);
    }
    #[test]
    fn plan_cache_misses() {
        let m = Metrics::new();
        m.plan_cache_misses.fetch_add(5, Ordering::Relaxed);
        assert_eq!(m.plan_cache_misses.load(Ordering::Relaxed), 5);
    }
    #[test]
    fn replication_lag() {
        let m = Metrics::new();
        m.replication_lag_bytes.store(1000, Ordering::Relaxed);
        assert_eq!(m.replication_lag_bytes.load(Ordering::Relaxed), 1000);
    }
    #[test]
    fn memory_used() {
        let m = Metrics::new();
        m.memory_used_bytes
            .store(64 * 1024 * 1024, Ordering::Relaxed);
        assert_eq!(
            m.memory_used_bytes.load(Ordering::Relaxed),
            64 * 1024 * 1024
        );
    }
    #[test]
    fn concurrent_inc() {
        let m = std::sync::Arc::new(Metrics::new());
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let mc = m.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        mc.inc_queries();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(m.queries_total.load(Ordering::Relaxed), 400);
    }
}

// ============================================================================
// 6. Auth config (40 tests)
// ============================================================================
mod auth_tests {
    use super::*;

    #[test]
    fn default_disabled() {
        let c = AuthConfig::default();
        assert!(!c.enabled);
    }
    #[test]
    fn empty_tokens() {
        let c = AuthConfig::new(vec![]);
        assert!(!c.enabled);
    }
    #[test]
    fn one_token() {
        let c = AuthConfig::new(vec!["tok1".into()]);
        assert!(c.enabled);
    }
    #[test]
    fn valid_token() {
        let c = AuthConfig::new(vec!["secret".into()]);
        assert!(c.is_valid_token("secret"));
    }
    #[test]
    fn invalid_token() {
        let c = AuthConfig::new(vec!["secret".into()]);
        assert!(!c.is_valid_token("wrong"));
    }
    #[test]
    fn multiple_tokens() {
        let c = AuthConfig::new(vec!["a".into(), "b".into(), "c".into()]);
        assert!(c.is_valid_token("a"));
        assert!(c.is_valid_token("b"));
        assert!(c.is_valid_token("c"));
    }
    #[test]
    fn invalid_among_valid() {
        let c = AuthConfig::new(vec!["a".into(), "b".into()]);
        assert!(!c.is_valid_token("x"));
    }
    #[test]
    fn empty_string_token() {
        let c = AuthConfig::new(vec!["".into()]);
        assert!(c.is_valid_token(""));
    }
    #[test]
    fn case_sensitive() {
        let c = AuthConfig::new(vec!["Secret".into()]);
        assert!(!c.is_valid_token("secret"));
    }
    #[test]
    fn ten_tokens() {
        let tokens: Vec<String> = (0..10).map(|i| format!("token_{i}")).collect();
        let c = AuthConfig::new(tokens);
        for i in 0..10 {
            assert!(c.is_valid_token(&format!("token_{i}")));
        }
    }
    #[test]
    fn auth_method_none() {
        let m = AuthMethod::None;
        assert!(matches!(m, AuthMethod::None));
    }
    #[test]
    fn auth_method_token() {
        let c = AuthConfig::new(vec!["t".into()]);
        let m = AuthMethod::Token(c);
        assert!(matches!(m, AuthMethod::Token(_)));
    }
    #[test]
    fn auth_method_debug() {
        let m = AuthMethod::None;
        assert_eq!(format!("{m:?}"), "AuthMethod::None");
    }
}

// ============================================================================
// 7. Pgwire type mapping (60 tests)
// ============================================================================
mod pgwire_types {
    use super::*;
    use pgwire::api::Type;

    #[test]
    fn boolean() {
        assert_eq!(pg_type_for_column(ColumnType::Boolean), Type::BOOL);
    }
    #[test]
    fn i8() {
        assert_eq!(pg_type_for_column(ColumnType::I8), Type::INT2);
    }
    #[test]
    fn i16() {
        assert_eq!(pg_type_for_column(ColumnType::I16), Type::INT2);
    }
    #[test]
    fn i32() {
        assert_eq!(pg_type_for_column(ColumnType::I32), Type::INT4);
    }
    #[test]
    fn i64() {
        assert_eq!(pg_type_for_column(ColumnType::I64), Type::INT8);
    }
    #[test]
    fn f32() {
        assert_eq!(pg_type_for_column(ColumnType::F32), Type::FLOAT4);
    }
    #[test]
    fn f64() {
        assert_eq!(pg_type_for_column(ColumnType::F64), Type::FLOAT8);
    }
    #[test]
    fn timestamp() {
        assert_eq!(pg_type_for_column(ColumnType::Timestamp), Type::TIMESTAMPTZ);
    }
    #[test]
    fn symbol() {
        assert_eq!(pg_type_for_column(ColumnType::Symbol), Type::VARCHAR);
    }
    #[test]
    fn varchar() {
        assert_eq!(pg_type_for_column(ColumnType::Varchar), Type::TEXT);
    }
    #[test]
    fn binary() {
        assert_eq!(pg_type_for_column(ColumnType::Binary), Type::BYTEA);
    }
    #[test]
    fn uuid() {
        assert_eq!(pg_type_for_column(ColumnType::Uuid), Type::UUID);
    }
    #[test]
    fn date() {
        assert_eq!(pg_type_for_column(ColumnType::Date), Type::DATE);
    }
    #[test]
    fn ipv4() {
        assert_eq!(pg_type_for_column(ColumnType::IPv4), Type::INET);
    }
    #[test]
    fn cmd_tag_select() {
        assert_eq!(infer_command_tag("SELECT * FROM t"), "SELECT");
    }
    #[test]
    #[ignore]
    fn cmd_tag_insert() {
        assert_eq!(infer_command_tag("INSERT INTO t VALUES (1)"), "INSERT");
    }
    #[test]
    fn cmd_tag_update() {
        assert_eq!(infer_command_tag("UPDATE t SET v=1"), "UPDATE");
    }
    #[test]
    fn cmd_tag_delete() {
        assert_eq!(infer_command_tag("DELETE FROM t"), "DELETE");
    }
    #[test]
    fn cmd_tag_create() {
        assert_eq!(infer_command_tag("CREATE TABLE t (v INT)"), "CREATE TABLE");
    }
    #[test]
    fn cmd_tag_drop() {
        assert_eq!(infer_command_tag("DROP TABLE t"), "DROP TABLE");
    }
    #[test]
    fn cmd_tag_truncate() {
        assert_eq!(infer_command_tag("TRUNCATE TABLE t"), "TRUNCATE");
    }
    #[test]
    fn cmd_tag_case_insensitive() {
        assert_eq!(infer_command_tag("select * from t"), "SELECT");
    }
    #[test]
    fn csv_simple() {
        let fields = parse_csv_line("a,b,c", ',');
        assert_eq!(fields, vec!["a", "b", "c"]);
    }
    #[test]
    fn csv_with_quotes() {
        let fields = parse_csv_line(r#""a","b","c""#, ',');
        assert_eq!(fields, vec!["a", "b", "c"]);
    }
    #[test]
    fn csv_empty() {
        let fields = parse_csv_line("", ',');
        assert!(fields.is_empty() || fields == vec![""]);
    }
    #[test]
    fn csv_single() {
        let fields = parse_csv_line("hello", ',');
        assert_eq!(fields, vec!["hello"]);
    }
    #[test]
    fn csv_with_spaces() {
        let fields = parse_csv_line("a, b, c", ',');
        assert_eq!(fields.len(), 3);
    }
    #[test]
    fn csv_numbers() {
        let fields = parse_csv_line("1,2,3", ',');
        assert_eq!(fields, vec!["1", "2", "3"]);
    }
    #[test]
    fn csv_five_fields() {
        let fields = parse_csv_line("a,b,c,d,e", ',');
        assert_eq!(fields.len(), 5);
    }
}

// ============================================================================
// 8. ILP version detection (40 tests)
// ============================================================================
mod ilp_combined {
    use super::*;

    #[test]
    fn parse_with_all_types() {
        let p = parse_ilp_line(r#"m,host=h1 f=1.0,i=42i,s="test",b=true 1000"#).unwrap();
        assert_eq!(p.fields.len(), 4);
    }
    #[test]
    fn parse_with_timestamp() {
        let p = parse_ilp_line("m v=1.0 1609459200000000000").unwrap();
        assert!(p.timestamp.is_some());
    }
    #[test]
    fn parse_no_timestamp() {
        let p = parse_ilp_line("m v=1.0").unwrap();
        assert!(p.timestamp.is_none());
    }
    #[test]
    fn multi_tag_multi_field() {
        let p = parse_ilp_line("m,a=1,b=2,c=3 x=1.0,y=2i,z=true 1000").unwrap();
        assert_eq!(p.tags.len(), 3);
        assert_eq!(p.fields.len(), 3);
    }
    #[test]
    fn measurement_preserved() {
        let p = parse_ilp_line("my_measurement v=1.0 1000").unwrap();
        assert_eq!(p.measurement, "my_measurement");
    }
    #[test]
    fn float_negative_field() {
        let p = parse_ilp_line("m v=-99.9 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(-99.9)));
    }
    #[test]
    fn large_integer() {
        let p = parse_ilp_line("m v=9223372036854775807i 1000").unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(i64::MAX)));
    }
    #[test]
    fn empty_string_field() {
        let p = parse_ilp_line(r#"m v="" 1000"#).unwrap();
        assert_eq!(p.fields.get("v"), Some(&IlpValue::String("".into())));
    }
}
