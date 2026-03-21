//! Per-type regression tests for VARCHAR/STRING operators — 500+ tests.
//!
//! Every SQL operator is tested with VARCHAR data: =, !=, LIKE, ILIKE, IN,
//! NOT IN, IS NULL, IS NOT NULL, ORDER BY, GROUP BY, HAVING, LIMIT/OFFSET,
//! DISTINCT, CASE WHEN, string functions, concatenation (||).

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

/// Standard test table with diverse string values.
fn db_str() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
    let vals = ["", "a", "hello", "Hello World", "UPPER", "123"];
    for (i, val) in vals.iter().enumerate() {
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, '{}')",
            ts(i as i64),
            val
        ));
    }
    db
}

/// Nullable VARCHAR table.
fn db_str_nullable() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(0)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(1)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'c')", ts(2)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(3)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'e')", ts(4)));
    db
}

/// Grouped VARCHAR table for GROUP BY tests.
fn db_str_grouped() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
    let data = [
        ("alpha", 10.0),
        ("beta", 20.0),
        ("alpha", 30.0),
        ("beta", 40.0),
        ("alpha", 50.0),
        ("gamma", 60.0),
        ("gamma", 70.0),
        ("beta", 80.0),
    ];
    for (i, (g, v)) in data.iter().enumerate() {
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, '{}', {})",
            ts(i as i64),
            g,
            v
        ));
    }
    db
}

/// LIKE/ILIKE test table with patterns.
fn db_like() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
    let vals = [
        "apple", "application", "banana", "BANANA", "cherry", "apricot",
        "pineapple", "grape", "App Store", "apply",
    ];
    for (i, val) in vals.iter().enumerate() {
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, '{}')",
            ts(i as i64),
            val
        ));
    }
    db
}

// =============================================================================
// Module 1: Equality (=)
// =============================================================================
mod eq {
    use super::*;

    #[test]
    fn eq_basic() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 'hello'");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Str("hello".into()));
    }

    #[test]
    fn eq_single_char() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 'a'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_empty_string() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = ''");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_mixed_case() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 'Hello World'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_no_match() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 'zzz'");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn eq_case_sensitive() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 'HELLO'");
        assert_eq!(rows.len(), 0); // "hello" != "HELLO"
    }

    #[test]
    fn eq_upper() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 'UPPER'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_numeric_string() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v = '123'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn eq_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..4 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'abc')", ts(i)));
        }
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 'abc'");
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn eq_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'only')", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v = 'only'");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 2: Not Equal (!=)
// =============================================================================
mod ne {
    use super::*;

    #[test]
    fn ne_basic() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 'hello'");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_empty() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != ''");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_no_match_returns_all() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 'zzz'");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn ne_all_same_excluded() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'abc')", ts(i)));
        }
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 'abc'");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn ne_upper() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 'UPPER'");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_single_char() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 'a'");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_hello_world() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 'Hello World'");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_numeric_string() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != '123'");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn ne_single_row_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'x')", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 'y'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn ne_single_row_excluded() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'x')", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v != 'x'");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// Module 3: LIKE
// =============================================================================
mod like {
    use super::*;

    #[test]
    fn like_prefix() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE 'app%'");
        // apple, application, apply = 3
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn like_suffix() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE '%apple'");
        // apple, pineapple = 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn like_contains() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE '%an%'");
        // banana, BANANA (no - case sensitive), => banana = 1
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn like_exact() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE 'cherry'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn like_percent_only() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE '%'");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn like_no_match() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE 'zzz%'");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn like_underscore() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE 'app__'");
        // apple, apply = 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn like_grape() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE 'grape'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn like_starts_with_b() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE 'b%'");
        // banana = 1
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn like_ends_with_e() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE '%e'");
        // apple, grape, pineapple, "App Store" ends with 'e' => 4
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn like_banana() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE 'banana'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn like_case_sensitive_capital() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE 'BANANA'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn like_apricot() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE 'apr%'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn like_contains_app() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE '%app%'");
        // apple, application, pineapple, apply = 4
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn like_pine_prefix() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE 'pine%'");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 4: ILIKE (case insensitive)
// =============================================================================
mod ilike {
    use super::*;

    #[test]
    fn ilike_prefix() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v ILIKE 'APP%'");
        // apple, application, apply, App Store = 4
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn ilike_banana() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v ILIKE 'banana'");
        // banana, BANANA = 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn ilike_exact() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v ILIKE 'Cherry'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn ilike_no_match() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v ILIKE 'zzz%'");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn ilike_contains() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v ILIKE '%AN%'");
        // banana, BANANA = 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn ilike_all() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v ILIKE '%'");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn ilike_grape() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v ILIKE 'GRAPE'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn ilike_suffix() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v ILIKE '%APPLE'");
        // apple, pineapple = 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn ilike_mixed_case() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v ILIKE 'app store'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn ilike_starts_b() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v ILIKE 'B%'");
        // banana, BANANA = 2
        assert_eq!(rows.len(), 2);
    }
}

// =============================================================================
// Module 5: IN / NOT IN
// =============================================================================
mod in_op {
    use super::*;

    #[test]
    fn in_single() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN ('hello')");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn in_multiple() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN ('hello', 'a', 'UPPER')");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn in_no_match() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN ('zzz', 'yyy')");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn in_all() {
        let db = db_str();
        let (_, rows) =
            db.query("SELECT v FROM t WHERE v IN ('', 'a', 'hello', 'Hello World', 'UPPER', '123')");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn not_in_single() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v NOT IN ('hello')");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn not_in_multiple() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v NOT IN ('hello', 'a')");
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn not_in_no_match() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v NOT IN ('zzz')");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn in_empty_string() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN ('')");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn not_in_empty_string() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v NOT IN ('')");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn in_case_sensitive() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IN ('HELLO')");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// Module 6: IS NULL / IS NOT NULL
// =============================================================================
mod null_ops {
    use super::*;

    #[test]
    fn is_null() {
        let db = db_str_nullable();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NULL");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn is_not_null() {
        let db = db_str_nullable();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NOT NULL");
        assert!(rows.len() >= 3);
    }

    #[test]
    fn no_nulls() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NULL");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn all_not_null() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NOT NULL");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn null_count() {
        let db = db_str_nullable();
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(5));
    }

    #[test]
    fn all_null() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(i)));
        }
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NULL");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn coalesce_replaces() {
        let db = db_str_nullable();
        let (_, rows) = db.query("SELECT coalesce(v, 'N/A') FROM t");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn coalesce_no_nulls() {
        let db = db_str();
        let (_, rows) = db.query("SELECT coalesce(v, 'default') FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn coalesce_preserves() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'keep')", ts(0)));
        let val = db.query_scalar("SELECT coalesce(v, 'default') FROM t");
        assert_eq!(val, Value::Str("keep".into()));
    }

    #[test]
    fn is_null_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        let (_, rows) = db.query("SELECT v FROM t WHERE v IS NULL");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// Module 7: ORDER BY
// =============================================================================
mod order_by {
    use super::*;

    #[test]
    fn order_asc() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn order_desc() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v DESC");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn order_asc_first() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        // Empty string should be first (lexicographic)
        assert_eq!(rows[0][0], Value::Str("".into()));
    }

    #[test]
    fn order_with_limit() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 3");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn order_desc_limit() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v DESC LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn order_with_offset() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 2 OFFSET 1");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn order_with_where() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != '' ORDER BY v ASC");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn order_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'only')", ts(0)));
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn order_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..3 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'same')", ts(i)));
        }
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn order_empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v ASC");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// Module 8: GROUP BY (string key)
// =============================================================================
mod group_by {
    use super::*;

    #[test]
    fn group_by_count() {
        let db = db_str_grouped();
        let (_, rows) = db.query("SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_sum() {
        let db = db_str_grouped();
        let (_, rows) = db.query("SELECT grp, sum(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_min() {
        let db = db_str_grouped();
        let (_, rows) = db.query("SELECT grp, min(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_max() {
        let db = db_str_grouped();
        let (_, rows) = db.query("SELECT grp, max(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_avg() {
        let db = db_str_grouped();
        let (_, rows) = db.query("SELECT grp, avg(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_first() {
        let db = db_str_grouped();
        let (_, rows) = db.query("SELECT grp, first(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_last() {
        let db = db_str_grouped();
        let (_, rows) = db.query("SELECT grp, last(v) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_single_group() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
        for i in 0..3 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, 'X', {}.0)",
                ts(i),
                i * 10
            ));
        }
        let (_, rows) = db.query("SELECT grp, sum(v) FROM t GROUP BY grp");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn group_by_with_order() {
        let db = db_str_grouped();
        let (_, rows) =
            db.query("SELECT grp, sum(v) AS s FROM t GROUP BY grp ORDER BY grp DESC");
        assert_eq!(rows[0][0], Value::Str("gamma".into()));
    }

    #[test]
    fn group_by_with_limit() {
        let db = db_str_grouped();
        let (_, rows) =
            db.query("SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn group_by_string_values() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'b')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(2)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'b')", ts(3)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'c')", ts(4)));
        let (_, rows) = db.query("SELECT v, count(*) FROM t GROUP BY v ORDER BY v");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_empty_string() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR, n DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, '', 1.0)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a', 2.0)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, '', 3.0)", ts(2)));
        let (_, rows) = db.query("SELECT v, count(*) FROM t GROUP BY v ORDER BY v");
        assert_eq!(rows.len(), 2);
    }
}

// =============================================================================
// Module 9: HAVING
// =============================================================================
mod having {
    use super::*;

    #[test]
    fn having_count() {
        let db = db_str_grouped();
        let (_, rows) = db.query(
            "SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c >= 3",
        );
        assert_eq!(rows.len(), 2); // alpha(3), beta(3)
    }

    #[test]
    fn having_sum() {
        let db = db_str_grouped();
        let (_, rows) = db.query(
            "SELECT grp, sum(v) AS s FROM t GROUP BY grp HAVING s > 100",
        );
        // alpha: 10+30+50=90, beta: 20+40+80=140, gamma: 60+70=130
        assert!(rows.len() >= 2);
    }

    #[test]
    fn having_all() {
        let db = db_str_grouped();
        let (_, rows) = db.query(
            "SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c >= 1",
        );
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn having_none() {
        let db = db_str_grouped();
        let (_, rows) = db.query(
            "SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c > 100",
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn having_with_order() {
        let db = db_str_grouped();
        let (_, rows) = db.query(
            "SELECT grp, sum(v) AS s FROM t GROUP BY grp HAVING s > 100 ORDER BY grp",
        );
        assert!(rows.len() >= 2);
    }
}

// =============================================================================
// Module 10: LIMIT / OFFSET
// =============================================================================
mod limit_offset {
    use super::*;

    #[test]
    fn limit_basic() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 3");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn limit_one() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn limit_exceeds() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 100");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn offset_basic() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v LIMIT 3 OFFSET 2");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn offset_all() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 10 OFFSET 100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn limit_with_where() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != '' LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn limit_zero() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t LIMIT 0");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn offset_one() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t ORDER BY v LIMIT 1 OFFSET 1");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 11: DISTINCT
// =============================================================================
mod distinct {
    use super::*;

    #[test]
    fn distinct_unique() {
        let db = db_str();
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn distinct_duplicates() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'b')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(2)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'b')", ts(3)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'c')", ts(4)));
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn distinct_all_same() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..5 {
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'same')", ts(i)));
        }
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn distinct_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn distinct_single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'only')", ts(0)));
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn distinct_with_order() {
        let db = db_str();
        let (_, rows) = db.query("SELECT DISTINCT v FROM t ORDER BY v ASC");
        assert_eq!(rows[0][0], Value::Str("".into()));
    }

    #[test]
    fn distinct_case_sensitive() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'abc')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'ABC')", ts(1)));
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 2);
    }
}

// =============================================================================
// Module 12: Concatenation (||)
// =============================================================================
mod concat {
    use super::*;

    #[test]
    fn concat_two_columns() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a VARCHAR, b VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello', ' world')", ts(0)));
        let val = db.query_scalar("SELECT a || b FROM t");
        assert_eq!(val, Value::Str("hello world".into()));
    }

    #[test]
    fn concat_with_literal() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello')", ts(0)));
        let val = db.query_scalar("SELECT 'say: ' || v FROM t");
        assert_eq!(val, Value::Str("say: hello".into()));
    }

    #[test]
    fn concat_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'abc')", ts(0)));
        let val = db.query_scalar("SELECT v || '' FROM t");
        assert_eq!(val, Value::Str("abc".into()));
    }

    #[test]
    fn concat_three_parts() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a VARCHAR, b VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a', 'b')", ts(0)));
        let val = db.query_scalar("SELECT a || '-' || b FROM t");
        assert_eq!(val, Value::Str("a-b".into()));
    }

    #[test]
    fn concat_preserves_rows() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v || '!' FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn concat_with_alias() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'x')", ts(0)));
        let (cols, _) = db.query("SELECT v || v AS doubled FROM t");
        assert!(cols.contains(&"doubled".to_string()));
    }

    #[test]
    fn concat_same_column() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'ab')", ts(0)));
        let val = db.query_scalar("SELECT v || v FROM t");
        assert_eq!(val, Value::Str("abab".into()));
    }

    #[test]
    fn concat_with_where() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v || '!' FROM t WHERE v = 'hello'");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Str("hello!".into()));
    }

    #[test]
    fn concat_with_limit() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v || '.' FROM t LIMIT 3");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn concat_with_order() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v || '.' FROM t ORDER BY v ASC LIMIT 1");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 13: String functions in SQL
// =============================================================================
mod string_functions {
    use super::*;

    #[test]
    fn length_basic() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello')", ts(0)));
        let val = db.query_scalar("SELECT length(v) FROM t");
        assert_eq!(val, Value::I64(5));
    }

    #[test]
    fn length_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, '')", ts(0)));
        let val = db.query_scalar("SELECT length(v) FROM t");
        assert_eq!(val, Value::I64(0));
    }

    #[test]
    fn length_rows() {
        let db = db_str();
        let (_, rows) = db.query("SELECT length(v) FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn upper_basic() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello')", ts(0)));
        let val = db.query_scalar("SELECT upper(v) FROM t");
        assert_eq!(val, Value::Str("HELLO".into()));
    }

    #[test]
    fn upper_already_upper() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'ABC')", ts(0)));
        let val = db.query_scalar("SELECT upper(v) FROM t");
        assert_eq!(val, Value::Str("ABC".into()));
    }

    #[test]
    fn upper_mixed() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, 'Hello World')",
            ts(0)
        ));
        let val = db.query_scalar("SELECT upper(v) FROM t");
        assert_eq!(val, Value::Str("HELLO WORLD".into()));
    }

    #[test]
    fn upper_rows() {
        let db = db_str();
        let (_, rows) = db.query("SELECT upper(v) FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn lower_basic() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'HELLO')", ts(0)));
        let val = db.query_scalar("SELECT lower(v) FROM t");
        assert_eq!(val, Value::Str("hello".into()));
    }

    #[test]
    fn lower_mixed() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, 'Hello World')",
            ts(0)
        ));
        let val = db.query_scalar("SELECT lower(v) FROM t");
        assert_eq!(val, Value::Str("hello world".into()));
    }

    #[test]
    fn lower_already() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'abc')", ts(0)));
        let val = db.query_scalar("SELECT lower(v) FROM t");
        assert_eq!(val, Value::Str("abc".into()));
    }

    #[test]
    fn lower_rows() {
        let db = db_str();
        let (_, rows) = db.query("SELECT lower(v) FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn concat_fn_basic() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a VARCHAR, b VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, 'hello', ' world')",
            ts(0)
        ));
        let val = db.query_scalar("SELECT concat(a, b) FROM t");
        assert_eq!(val, Value::Str("hello world".into()));
    }

    #[test]
    fn concat_fn_three() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'mid')", ts(0)));
        let val = db.query_scalar("SELECT concat('pre-', v, '-suf') FROM t");
        assert_eq!(val, Value::Str("pre-mid-suf".into()));
    }

    #[test]
    fn replace_basic() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello world')", ts(0)));
        let val = db.query_scalar("SELECT replace(v, 'world', 'rust') FROM t");
        assert_eq!(val, Value::Str("hello rust".into()));
    }

    #[test]
    fn replace_no_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello')", ts(0)));
        let val = db.query_scalar("SELECT replace(v, 'xyz', 'abc') FROM t");
        assert_eq!(val, Value::Str("hello".into()));
    }

    #[test]
    fn replace_rows() {
        let db = db_str();
        let (_, rows) = db.query("SELECT replace(v, 'l', 'L') FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn multiple_functions() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, 'Hello World')",
            ts(0)
        ));
        let (cols, rows) = db.query("SELECT length(v), upper(v), lower(v) FROM t");
        assert_eq!(cols.len(), 3);
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 14: CASE WHEN
// =============================================================================
mod case_when {
    use super::*;

    #[test]
    fn case_eq_string() {
        let db = db_str();
        let (_, rows) = db.query(
            "SELECT CASE WHEN v = 'hello' THEN 'greeting' ELSE 'other' END FROM t",
        );
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_empty_check() {
        let db = db_str();
        let (_, rows) = db.query(
            "SELECT CASE WHEN v = '' THEN 'empty' ELSE 'nonempty' END FROM t",
        );
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_multi_branch() {
        let db = db_str();
        let (_, rows) = db.query(
            "SELECT CASE WHEN v = 'a' THEN 'letter' WHEN v = '123' THEN 'number' ELSE 'other' END FROM t",
        );
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_with_alias() {
        let db = db_str();
        let (cols, _) = db.query(
            "SELECT CASE WHEN v = 'hello' THEN 'yes' ELSE 'no' END AS flag FROM t",
        );
        assert!(cols.contains(&"flag".to_string()));
    }

    #[test]
    fn case_without_else() {
        let db = db_str();
        let (_, rows) = db.query(
            "SELECT CASE WHEN v = 'hello' THEN 'found' END FROM t",
        );
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_all_else() {
        let db = db_str();
        let (_, rows) = db.query(
            "SELECT CASE WHEN v = 'zzz' THEN 'found' ELSE 'not_found' END FROM t",
        );
        for r in &rows {
            assert_eq!(r[0], Value::Str("not_found".into()));
        }
    }

    #[test]
    fn case_returns_number() {
        let db = db_str();
        let (_, rows) = db.query(
            "SELECT CASE WHEN v = '' THEN 0 ELSE 1 END FROM t",
        );
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn case_upper_check() {
        let db = db_str();
        let (_, rows) = db.query(
            "SELECT CASE WHEN v = 'UPPER' THEN 'is_upper' ELSE 'not_upper' END FROM t",
        );
        assert_eq!(rows.len(), 6);
    }
}

// =============================================================================
// Module 15: Logical (AND, OR)
// =============================================================================
mod logical {
    use super::*;

    #[test]
    fn and_both() {
        let db = db_like();
        let (_, rows) =
            db.query("SELECT v FROM t WHERE v LIKE 'app%' AND v LIKE '%le'");
        // apple = 1
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn or_either() {
        let db = db_like();
        let (_, rows) =
            db.query("SELECT v FROM t WHERE v = 'apple' OR v = 'cherry'");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn and_none() {
        let db = db_like();
        let (_, rows) =
            db.query("SELECT v FROM t WHERE v = 'apple' AND v = 'cherry'");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn or_chain() {
        let db = db_like();
        let (_, rows) =
            db.query("SELECT v FROM t WHERE v = 'apple' OR v = 'banana' OR v = 'cherry'");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn combined() {
        let db = db_like();
        let (_, rows) = db.query(
            "SELECT v FROM t WHERE (v LIKE 'app%' OR v = 'cherry') AND v != 'apply'",
        );
        // app%: apple, application, apply; minus apply = apple, application; plus cherry = 3
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn like_and_ne() {
        let db = db_like();
        let (_, rows) =
            db.query("SELECT v FROM t WHERE v LIKE 'app%' AND v != 'apple'");
        // application, apply = 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn or_both_false() {
        let db = db_like();
        let (_, rows) =
            db.query("SELECT v FROM t WHERE v = 'zzz' OR v = 'yyy'");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// Module 16: Edge cases & multi-column
// =============================================================================
mod edge_cases {
    use super::*;

    #[test]
    fn empty_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        let (_, rows) = db.query("SELECT v FROM t");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn single_row() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'only')", ts(0)));
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(1));
    }

    #[test]
    fn twenty_rows() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..20 {
            db.exec_ok(&format!(
                "INSERT INTO t VALUES ({}, 'row_{}')",
                ts(i),
                i
            ));
        }
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(20));
    }

    #[test]
    fn select_star() {
        let db = db_str();
        let (cols, rows) = db.query("SELECT * FROM t");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn select_with_alias() {
        let db = db_str();
        let (cols, _) = db.query("SELECT v AS val FROM t");
        assert!(cols.contains(&"val".to_string()));
    }

    #[test]
    fn where_order_limit() {
        let db = db_str();
        let (_, rows) = db.query("SELECT v FROM t WHERE v != '' ORDER BY v ASC LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn count_where() {
        let db = db_str();
        let val = db.query_scalar("SELECT count(*) FROM t WHERE v = 'hello'");
        assert_eq!(val, Value::I64(1));
    }

    #[test]
    fn count_like() {
        let db = db_like();
        let val = db.query_scalar("SELECT count(*) FROM t WHERE v LIKE 'app%'");
        assert_eq!(val, Value::I64(3));
    }

    #[test]
    fn count_ilike() {
        let db = db_like();
        let val = db.query_scalar("SELECT count(*) FROM t WHERE v ILIKE 'APP%'");
        assert_eq!(val, Value::I64(4));
    }

    #[test]
    fn combined_in_like() {
        let db = db_like();
        let (_, rows) = db.query(
            "SELECT v FROM t WHERE v IN ('apple', 'banana') AND v LIKE '%a%'",
        );
        // apple and banana both contain 'a' => 2
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn min_string() {
        let db = db_str();
        let val = db.query_scalar("SELECT min(v) FROM t");
        // Empty string is min
        assert_eq!(val, Value::Str("".into()));
    }

    #[test]
    fn max_string() {
        let db = db_str();
        let val = db.query_scalar("SELECT max(v) FROM t");
        // Lexicographic max among "", "a", "hello", "Hello World", "UPPER", "123"
        assert!(matches!(val, Value::Str(_)));
    }

    #[test]
    fn first_string() {
        let db = db_str();
        let val = db.query_scalar("SELECT first(v) FROM t");
        assert_eq!(val, Value::Str("".into()));
    }

    #[test]
    fn last_string() {
        let db = db_str();
        let val = db.query_scalar("SELECT last(v) FROM t");
        assert_eq!(val, Value::Str("123".into()));
    }

    #[test]
    fn count_string() {
        let db = db_str();
        assert_eq!(db.query_scalar("SELECT count(v) FROM t"), Value::I64(6));
    }

    #[test]
    fn distinct_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..10 {
            let v = if i % 3 == 0 { "a" } else if i % 3 == 1 { "b" } else { "c" };
            db.exec_ok(&format!("INSERT INTO t VALUES ({}, '{}')", ts(i), v));
        }
        let (_, rows) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_having_order_limit() {
        let db = db_str_grouped();
        let (_, rows) = db.query(
            "SELECT grp, sum(v) AS s FROM t GROUP BY grp HAVING s > 100 ORDER BY grp LIMIT 1",
        );
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn like_percent_in_middle() {
        let db = db_like();
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE 'a%e'");
        // apple, apricot(no, ends in t) => apple = 1
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn like_single_underscore() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'ab')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'abc')", ts(2)));
        let (_, rows) = db.query("SELECT v FROM t WHERE v LIKE '_'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn not_in_like_combined() {
        let db = db_like();
        let (_, rows) = db.query(
            "SELECT v FROM t WHERE v NOT IN ('apple') AND v LIKE 'app%'",
        );
        // application, apply = 2
        assert_eq!(rows.len(), 2);
    }
}

// =============================================================================
// Module 17: Cast with strings
// =============================================================================
mod cast_ops {
    use super::*;

    #[test]
    fn cast_varchar_identity() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello')", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t");
        assert_eq!(val, Value::Str("hello".into()));
    }

    #[test]
    fn cast_int_to_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 42)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t");
        match val {
            Value::Str(s) => assert_eq!(s, "42"),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn cast_double_to_varchar() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 3.14)", ts(0)));
        let val = db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t");
        match val {
            Value::Str(s) => assert!(s.contains("3.14"), "got: {s}"),
            other => panic!("expected Str, got {other:?}"),
        }
    }

    #[test]
    fn cast_preserves_rows() {
        let db = db_str();
        let (_, rows) = db.query("SELECT CAST(v AS VARCHAR) FROM t");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn cast_with_where() {
        let db = db_str();
        let (_, rows) = db.query("SELECT CAST(v AS VARCHAR) FROM t WHERE v = 'hello'");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Module 18: Multiple expressions
// =============================================================================
mod multi_expr {
    use super::*;

    #[test]
    fn select_column_and_function() {
        let db = db_str();
        let (cols, rows) = db.query("SELECT v, length(v) FROM t");
        assert_eq!(cols.len(), 2);
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn select_column_function_and_concat() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello')", ts(0)));
        let (cols, rows) = db.query("SELECT v, upper(v), v || '!' FROM t");
        assert_eq!(cols.len(), 3);
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn select_all_functions() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'Hello')", ts(0)));
        let (cols, rows) = db.query("SELECT length(v), upper(v), lower(v) FROM t");
        assert_eq!(cols.len(), 3);
        assert_eq!(rows[0][0], Value::I64(5));
        assert_eq!(rows[0][1], Value::Str("HELLO".into()));
        assert_eq!(rows[0][2], Value::Str("hello".into()));
    }

    #[test]
    fn replace_in_concat() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, 'BTC/USD')",
            ts(0)
        ));
        let val = db.query_scalar("SELECT replace(v, '/', '-') FROM t");
        assert_eq!(val, Value::Str("BTC-USD".into()));
    }

    #[test]
    fn upper_with_where() {
        let db = db_str();
        let (_, rows) = db.query("SELECT upper(v) FROM t WHERE v = 'hello'");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Str("HELLO".into()));
    }
}

// =============================================================================
// Module 19: Additional LIKE patterns
// =============================================================================
mod like_extra {
    use super::*;

    #[test] fn like_single_char() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE '_____'"); assert!(r.len() >= 1); }
    #[test] fn like_two_underscores_prefix() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE '__ple'"); assert_eq!(r.len(), 1); }
    #[test] fn like_star_suffix() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE '%ry'"); assert_eq!(r.len(), 1); }
    #[test] fn like_contains_ple() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE '%ple%'"); assert!(r.len() >= 2); }
    #[test] fn like_starts_ch() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE 'ch%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_starts_gr() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE 'gr%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_ends_on() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE '%on'"); assert_eq!(r.len(), 1); }
    #[test] fn like_ends_ot() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE '%ot'"); assert_eq!(r.len(), 1); }
    #[test] fn like_exact_grape() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE 'grape'"); assert_eq!(r.len(), 1); }
    #[test] fn like_exact_apply() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE 'apply'"); assert_eq!(r.len(), 1); }
    #[test] fn like_a_percent() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE 'a%'"); assert!(r.len() >= 3); }
    #[test] fn like_p_percent() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE 'p%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_no_wildcards() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE 'xyz'"); assert_eq!(r.len(), 0); }
    #[test] fn ilike_cherry() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v ILIKE 'CHERRY'"); assert_eq!(r.len(), 1); }
    #[test] fn ilike_pine() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v ILIKE 'pine%'"); assert_eq!(r.len(), 1); }
    #[test] fn ilike_apricot() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v ILIKE 'APRICOT'"); assert_eq!(r.len(), 1); }
    #[test] fn ilike_ends_e() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v ILIKE '%E'"); assert!(r.len() >= 3); }
    #[test] fn ilike_contains_na() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v ILIKE '%NA%'"); assert!(r.len() >= 2); }
}

// =============================================================================
// Module 20: Additional comparison combinations
// =============================================================================
mod comparison_combos {
    use super::*;

    #[test] fn eq_and_ne() { let db = db_str(); let (_, r) = db.query("SELECT v FROM t WHERE v = 'hello' AND v != 'world'"); assert_eq!(r.len(), 1); }
    #[test] fn eq_or_eq() { let db = db_str(); let (_, r) = db.query("SELECT v FROM t WHERE v = 'hello' OR v = 'a'"); assert_eq!(r.len(), 2); }
    #[test] fn ne_and_ne() { let db = db_str(); let (_, r) = db.query("SELECT v FROM t WHERE v != '' AND v != 'a'"); assert_eq!(r.len(), 4); }
    #[test] fn in_and_eq() { let db = db_str(); let (_, r) = db.query("SELECT v FROM t WHERE v IN ('hello', 'a') AND v = 'hello'"); assert_eq!(r.len(), 1); }
    #[test] fn not_in_and_ne() { let db = db_str(); let (_, r) = db.query("SELECT v FROM t WHERE v NOT IN ('hello') AND v != 'a'"); assert_eq!(r.len(), 4); }
    #[test] fn eq_count() { let db = db_str(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v = 'hello'"), Value::I64(1)); }
    #[test] fn ne_count() { let db = db_str(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v != 'hello'"), Value::I64(5)); }
    #[test] fn in_count() { let db = db_str(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v IN ('a', 'hello')"), Value::I64(2)); }
    #[test] fn not_in_count() { let db = db_str(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v NOT IN ('a', 'hello')"), Value::I64(4)); }
    #[test] fn like_count() { let db = db_like(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v LIKE 'app%'"), Value::I64(3)); }
    #[test] fn ilike_count() { let db = db_like(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v ILIKE 'APP%'"), Value::I64(4)); }
    #[test] fn like_and_in() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE '%apple%' AND v IN ('apple', 'pineapple', 'xyz')"); assert_eq!(r.len(), 2); }
    #[test] fn like_or_eq() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE 'ban%' OR v = 'cherry'"); assert_eq!(r.len(), 2); }
    #[test] fn ilike_and_ne() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v ILIKE 'banana' AND v != 'BANANA'"); assert_eq!(r.len(), 1); }
}

// =============================================================================
// Module 21: Additional GROUP BY + HAVING
// =============================================================================
mod group_having_extra {
    use super::*;

    #[test] fn having_avg() { let db = db_str_grouped(); let (_, r) = db.query("SELECT grp, avg(v) AS a FROM t GROUP BY grp HAVING a > 40 ORDER BY grp"); assert!(r.len() >= 1); }
    #[test] fn having_min() { let db = db_str_grouped(); let (_, r) = db.query("SELECT grp, min(v) AS m FROM t GROUP BY grp HAVING m >= 20 ORDER BY grp"); assert!(r.len() >= 1); }
    #[test] fn having_max() { let db = db_str_grouped(); let (_, r) = db.query("SELECT grp, max(v) AS m FROM t GROUP BY grp HAVING m > 60 ORDER BY grp"); assert!(r.len() >= 1); }
    #[test] fn group_order_desc() { let db = db_str_grouped(); let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp ORDER BY c DESC"); assert_eq!(r.len(), 3); }
    #[test] fn group_limit_1() { let db = db_str_grouped(); let (_, r) = db.query("SELECT grp, sum(v) FROM t GROUP BY grp ORDER BY grp LIMIT 1"); assert_eq!(r.len(), 1); }
    #[test] fn having_count_eq() { let db = db_str_grouped(); let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c = 2"); assert_eq!(r.len(), 1); }
    #[test] fn having_count_gt2() { let db = db_str_grouped(); let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c > 2"); assert_eq!(r.len(), 2); }

    #[test]
    fn grouped_count_alpha() {
        let db = db_str_grouped();
        let (_, r) = db.query("SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(r[0][0], Value::Str("alpha".into()));
        assert_eq!(r[0][1], Value::I64(3));
    }

    #[test]
    fn grouped_count_beta() {
        let db = db_str_grouped();
        let (_, r) = db.query("SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(r[1][0], Value::Str("beta".into()));
        assert_eq!(r[1][1], Value::I64(3));
    }

    #[test]
    fn grouped_count_gamma() {
        let db = db_str_grouped();
        let (_, r) = db.query("SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(r[2][0], Value::Str("gamma".into()));
        assert_eq!(r[2][1], Value::I64(2));
    }
}

// =============================================================================
// Module 22: Additional DISTINCT, ORDER combos
// =============================================================================
mod distinct_order_extra {
    use super::*;

    #[test] fn distinct_desc() { let db = db_str(); let (_, r) = db.query("SELECT DISTINCT v FROM t ORDER BY v DESC"); assert_eq!(r.len(), 6); }
    #[test] fn distinct_limit() { let db = db_str(); let (_, r) = db.query("SELECT DISTINCT v FROM t ORDER BY v ASC LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn distinct_where_ne() { let db = db_str(); let (_, r) = db.query("SELECT DISTINCT v FROM t WHERE v != ''"); assert_eq!(r.len(), 5); }
    #[test] fn order_limit_one_asc() { let db = db_str(); let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 1"); assert_eq!(r[0][0], Value::Str("".into())); }
    #[test] fn offset_three() { let db = db_str(); let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 2 OFFSET 3"); assert_eq!(r.len(), 2); }
    #[test] fn distinct_like() { let db = db_like(); let (_, r) = db.query("SELECT DISTINCT v FROM t WHERE v LIKE 'app%'"); assert_eq!(r.len(), 3); }
    #[test] fn order_like() { let db = db_like(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE 'app%' ORDER BY v ASC"); assert_eq!(r.len(), 3); }
    #[test] fn order_in() { let db = db_str(); let (_, r) = db.query("SELECT v FROM t WHERE v IN ('hello', 'a') ORDER BY v ASC"); assert_eq!(r.len(), 2); }
    #[test] fn distinct_in() { let db = db_str(); let (_, r) = db.query("SELECT DISTINCT v FROM t WHERE v IN ('hello', 'a', '123')"); assert_eq!(r.len(), 3); }
}

// =============================================================================
// Module 23: Additional CASE WHEN
// =============================================================================
mod case_extra {
    use super::*;

    #[test] fn case_like() { let db = db_like(); let (_, r) = db.query("SELECT CASE WHEN v LIKE 'app%' THEN 'apple_family' ELSE 'other' END FROM t"); assert_eq!(r.len(), 10); }
    #[test] fn case_in_list() { let db = db_str(); let (_, r) = db.query("SELECT CASE WHEN v IN ('a', 'hello') THEN 'known' ELSE 'unknown' END FROM t"); assert_eq!(r.len(), 6); }
    #[test] fn case_length() { let db = db_str(); let (_, r) = db.query("SELECT CASE WHEN v = '' THEN 'empty' WHEN v = 'a' THEN 'single' ELSE 'multi' END FROM t"); assert_eq!(r.len(), 6); }
    #[test] fn case_returns_count() { let db = db_str(); let (_, r) = db.query("SELECT CASE WHEN v = '' THEN 0 ELSE 1 END FROM t"); assert_eq!(r.len(), 6); }
    #[test] fn case_four_branch() { let db = db_str(); let (_, r) = db.query("SELECT CASE WHEN v = '' THEN 'a' WHEN v = 'a' THEN 'b' WHEN v = 'hello' THEN 'c' ELSE 'd' END FROM t"); assert_eq!(r.len(), 6); }
    #[test] fn case_with_order() { let db = db_str(); let (_, r) = db.query("SELECT CASE WHEN v = 'hello' THEN 1 ELSE 0 END FROM t ORDER BY v"); assert_eq!(r.len(), 6); }
}

// =============================================================================
// Module 24: Many rows with strings
// =============================================================================
mod many_rows_str {
    use super::*;

    #[test]
    fn fifty_rows_count() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..50 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'row_{}')", ts(i), i)); }
        assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(50));
    }

    #[test]
    fn fifty_rows_distinct() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..50 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'grp_{}')", ts(i), i % 10)); }
        let (_, r) = db.query("SELECT DISTINCT v FROM t");
        assert_eq!(r.len(), 10);
    }

    #[test]
    fn fifty_rows_group() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
        let grps = ["A", "B", "C", "D", "E"];
        for i in 0..50 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, '{}', {}.0)", ts(i), grps[i as usize % 5], i)); }
        let (_, r) = db.query("SELECT grp, count(*) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn fifty_rows_like() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..50 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'item_{}')", ts(i), i)); }
        let (_, r) = db.query("SELECT v FROM t WHERE v LIKE 'item_1%'");
        assert!(r.len() >= 1);
    }

    #[test]
    fn fifty_rows_in() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..50 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'v_{}')", ts(i), i)); }
        let (_, r) = db.query("SELECT v FROM t WHERE v IN ('v_0', 'v_10', 'v_20', 'v_30', 'v_40')");
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn fifty_rows_order() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..50 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'z_{:02}')", ts(i), i)); }
        let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 5");
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn fifty_rows_having() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, v DOUBLE)");
        let grps = ["A", "B", "C", "D", "E"];
        for i in 0..50 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, '{}', {}.0)", ts(i), grps[i as usize % 5], i)); }
        let (_, r) = db.query("SELECT grp, count(*) AS c FROM t GROUP BY grp HAVING c = 10");
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn fifty_rows_filter() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        for i in 0..50 { db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'item_{}')", ts(i), i)); }
        let (_, r) = db.query("SELECT v FROM t WHERE v != 'item_0' AND v != 'item_49'");
        assert_eq!(r.len(), 48);
    }
}

// =============================================================================
// Module 25: Additional concat and function combos
// =============================================================================
mod func_combos {
    use super::*;

    #[test] fn concat_upper() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello')", ts(0))); let v = db.query_scalar("SELECT upper(v) FROM t"); assert_eq!(v, Value::Str("HELLO".into())); }
    #[test] fn concat_lower() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'HELLO')", ts(0))); let v = db.query_scalar("SELECT lower(v) FROM t"); assert_eq!(v, Value::Str("hello".into())); }
    #[test] fn length_hello() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello')", ts(0))); assert_eq!(db.query_scalar("SELECT length(v) FROM t"), Value::I64(5)); }
    #[test] fn length_empty_str() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, '')", ts(0))); assert_eq!(db.query_scalar("SELECT length(v) FROM t"), Value::I64(0)); }
    #[test] fn replace_slash() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'a/b/c')", ts(0))); assert_eq!(db.query_scalar("SELECT replace(v, '/', '-') FROM t"), Value::Str("a-b-c".into())); }
    #[test] fn concat_three() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'mid')", ts(0))); assert_eq!(db.query_scalar("SELECT concat('a', v, 'b') FROM t"), Value::Str("amidb".into())); }
    #[test] fn pipe_concat() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'x')", ts(0))); assert_eq!(db.query_scalar("SELECT v || v || v FROM t"), Value::Str("xxx".into())); }
    #[test] fn upper_length() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello')", ts(0))); let (_, r) = db.query("SELECT upper(v), length(v) FROM t"); assert_eq!(r[0][0], Value::Str("HELLO".into())); assert_eq!(r[0][1], Value::I64(5)); }
    #[test] fn replace_empty() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'abc')", ts(0))); assert_eq!(db.query_scalar("SELECT replace(v, 'xyz', 'def') FROM t"), Value::Str("abc".into())); }
    #[test] fn concat_with_order() { let db = db_str(); let (_, r) = db.query("SELECT v || '!' FROM t ORDER BY v ASC LIMIT 3"); assert_eq!(r.len(), 3); }

    #[test]
    fn wide_string_table() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, a VARCHAR, b VARCHAR, c VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'x', 'y', 'z')", ts(0)));
        let (_, r) = db.query("SELECT a, b, c FROM t");
        assert_eq!(r[0][0], Value::Str("x".into()));
        assert_eq!(r[0][1], Value::Str("y".into()));
        assert_eq!(r[0][2], Value::Str("z".into()));
    }

    #[test]
    fn mixed_types_select() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR, d DOUBLE, i BIGINT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello', 3.14, 42)", ts(0)));
        let (_, r) = db.query("SELECT s, d, i FROM t");
        assert_eq!(r[0][0], Value::Str("hello".into()));
        assert_eq!(r[0][1], Value::F64(3.14));
        assert_eq!(r[0][2], Value::I64(42));
    }
}

// =============================================================================
// Module 26: Coalesce and CAST with strings
// =============================================================================
mod coalesce_cast_extra {
    use super::*;

    #[test] fn coalesce_null_default() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL)", ts(0))); let v = db.query_scalar("SELECT coalesce(v, 'default') FROM t"); assert_eq!(v, Value::Str("default".into())); }
    #[test] fn coalesce_nonnull() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'keep')", ts(0))); let v = db.query_scalar("SELECT coalesce(v, 'default') FROM t"); assert_eq!(v, Value::Str("keep".into())); }
    #[test] fn cast_int_to_varchar() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 99)", ts(0))); match db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t") { Value::Str(s) => assert_eq!(s, "99"), other => panic!("got {other:?}") } }
    #[test] fn cast_double_to_varchar() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1.5)", ts(0))); match db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t") { Value::Str(s) => assert!(s.contains("1.5")), other => panic!("got {other:?}") } }
    #[test] fn cast_varchar_identity() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'abc')", ts(0))); assert_eq!(db.query_scalar("SELECT CAST(v AS VARCHAR) FROM t"), Value::Str("abc".into())); }
    #[test] fn cast_with_limit() { let db = db_str(); let (_, r) = db.query("SELECT CAST(v AS VARCHAR) FROM t LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn coalesce_rows() { let db = db_str_nullable(); let (_, r) = db.query("SELECT coalesce(v, 'X') FROM t"); assert_eq!(r.len(), 5); }
}

// =============================================================================
// Module 27: Bulk string operation tests on 10-row table
// =============================================================================
mod bulk_string_ops {
    use super::*;

    fn db10() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v VARCHAR)");
        let vals = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa"];
        for (i, val) in vals.iter().enumerate() { db.exec_ok(&format!("INSERT INTO t VALUES ({}, '{}')", ts(i as i64), val)); }
        db
    }

    #[test] fn eq_alpha() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v = 'alpha'"), Value::I64(1)); }
    #[test] fn eq_beta() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v = 'beta'"), Value::I64(1)); }
    #[test] fn eq_gamma() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v = 'gamma'"), Value::I64(1)); }
    #[test] fn eq_delta() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v = 'delta'"), Value::I64(1)); }
    #[test] fn eq_epsilon() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v = 'epsilon'"), Value::I64(1)); }
    #[test] fn eq_zeta() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v = 'zeta'"), Value::I64(1)); }
    #[test] fn eq_eta() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v = 'eta'"), Value::I64(1)); }
    #[test] fn eq_theta() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v = 'theta'"), Value::I64(1)); }
    #[test] fn eq_iota() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v = 'iota'"), Value::I64(1)); }
    #[test] fn eq_kappa() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v = 'kappa'"), Value::I64(1)); }
    #[test] fn ne_alpha() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v != 'alpha'"), Value::I64(9)); }
    #[test] fn ne_beta() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v != 'beta'"), Value::I64(9)); }
    #[test] fn like_a_pct() { let db = db10(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE 'a%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_b_pct() { let db = db10(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE 'b%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_e_pct() { let db = db10(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE 'e%'"); assert!(r.len() >= 2); }
    #[test] fn like_pct_a() { let db = db10(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE '%a'"); assert!(r.len() >= 3); }
    #[test] fn like_pct_ta() { let db = db10(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE '%ta'"); assert!(r.len() >= 2); }
    #[test] fn like_pct_eta() { let db = db10(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE '%eta'"); assert!(r.len() >= 2); }
    #[test] fn ilike_alpha() { let db = db10(); let (_, r) = db.query("SELECT v FROM t WHERE v ILIKE 'ALPHA'"); assert_eq!(r.len(), 1); }
    #[test] fn ilike_beta() { let db = db10(); let (_, r) = db.query("SELECT v FROM t WHERE v ILIKE 'BETA'"); assert_eq!(r.len(), 1); }
    #[test] fn ilike_pct_a() { let db = db10(); let (_, r) = db.query("SELECT v FROM t WHERE v ILIKE '%A'"); assert!(r.len() >= 3); }
    #[test] fn in_2() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v IN ('alpha', 'beta')"), Value::I64(2)); }
    #[test] fn in_3() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v IN ('alpha', 'beta', 'gamma')"), Value::I64(3)); }
    #[test] fn in_5() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v IN ('alpha', 'beta', 'gamma', 'delta', 'epsilon')"), Value::I64(5)); }
    #[test] fn not_in_1() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v NOT IN ('alpha')"), Value::I64(9)); }
    #[test] fn not_in_3() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v NOT IN ('alpha', 'beta', 'gamma')"), Value::I64(7)); }
    #[test] fn count_all() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(10)); }
    #[test] fn distinct_all() { let db = db10(); let (_, r) = db.query("SELECT DISTINCT v FROM t"); assert_eq!(r.len(), 10); }
    #[test] fn order_asc() { let db = db10(); let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 1"); assert_eq!(r[0][0], Value::Str("alpha".into())); }
    #[test] fn order_desc() { let db = db10(); let (_, r) = db.query("SELECT v FROM t ORDER BY v DESC LIMIT 1"); assert_eq!(r[0][0], Value::Str("zeta".into())); }
    #[test] fn limit5() { let db = db10(); let (_, r) = db.query("SELECT v FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn offset5() { let db = db10(); let (_, r) = db.query("SELECT v FROM t ORDER BY v ASC LIMIT 5 OFFSET 5"); assert_eq!(r.len(), 5); }
    #[test] fn upper_all() { let db = db10(); let (_, r) = db.query("SELECT upper(v) FROM t"); assert_eq!(r.len(), 10); }
    #[test] fn lower_all() { let db = db10(); let (_, r) = db.query("SELECT lower(v) FROM t"); assert_eq!(r.len(), 10); }
    #[test] fn length_all() { let db = db10(); let (_, r) = db.query("SELECT length(v) FROM t"); assert_eq!(r.len(), 10); }
    #[test] fn concat_bang() { let db = db10(); let (_, r) = db.query("SELECT v || '!' FROM t"); assert_eq!(r.len(), 10); }
    #[test] fn replace_a() { let db = db10(); let (_, r) = db.query("SELECT replace(v, 'a', 'A') FROM t"); assert_eq!(r.len(), 10); }
    #[test] fn case_alpha() { let db = db10(); let (_, r) = db.query("SELECT CASE WHEN v = 'alpha' THEN 'first' ELSE 'other' END FROM t"); assert_eq!(r.len(), 10); }
    #[test] fn coalesce_all() { let db = db10(); let (_, r) = db.query("SELECT coalesce(v, 'N/A') FROM t"); assert_eq!(r.len(), 10); }
    #[test] fn min_str() { let db = db10(); let v = db.query_scalar("SELECT min(v) FROM t"); assert_eq!(v, Value::Str("alpha".into())); }
    #[test] fn max_str() { let db = db10(); let v = db.query_scalar("SELECT max(v) FROM t"); assert_eq!(v, Value::Str("zeta".into())); }
    #[test] fn first_str() { let db = db10(); let v = db.query_scalar("SELECT first(v) FROM t"); assert_eq!(v, Value::Str("alpha".into())); }
    #[test] fn last_str() { let db = db10(); let v = db.query_scalar("SELECT last(v) FROM t"); assert_eq!(v, Value::Str("kappa".into())); }
    #[test] fn like_and_ne() { let db = db10(); let (_, r) = db.query("SELECT v FROM t WHERE v LIKE '%eta' AND v != 'zeta'"); assert!(r.len() >= 1); }
    #[test] fn or_eq_eq() { let db = db10(); let (_, r) = db.query("SELECT v FROM t WHERE v = 'alpha' OR v = 'zeta'"); assert_eq!(r.len(), 2); }
    #[test] fn and_in_like() { let db = db10(); let (_, r) = db.query("SELECT v FROM t WHERE v IN ('alpha', 'beta', 'gamma') AND v LIKE '%a%'"); assert!(r.len() >= 2); }
    #[test] fn distinct_like() { let db = db10(); let (_, r) = db.query("SELECT DISTINCT v FROM t WHERE v LIKE '%a%'"); assert!(r.len() >= 3); }
    #[test] fn count_like() { let db = db10(); assert_eq!(db.query_scalar("SELECT count(*) FROM t WHERE v LIKE '%a'"), Value::I64(9)); }
    #[test] fn upper_alpha() { let db = db10(); let v = db.query_scalar("SELECT upper(v) FROM t WHERE v = 'alpha'"); assert_eq!(v, Value::Str("ALPHA".into())); }
    #[test] fn length_alpha() { let db = db10(); let v = db.query_scalar("SELECT length(v) FROM t WHERE v = 'alpha'"); assert_eq!(v, Value::I64(5)); }
    #[test] fn length_epsilon() { let db = db10(); let v = db.query_scalar("SELECT length(v) FROM t WHERE v = 'epsilon'"); assert_eq!(v, Value::I64(7)); }
    #[test] fn concat_two_cols() { let db = db10(); let (_, r) = db.query("SELECT v || '-' || v FROM t LIMIT 1"); assert_eq!(r[0][0], Value::Str("alpha-alpha".into())); }
    #[test] fn cast_varchar() { let db = db10(); let (_, r) = db.query("SELECT CAST(v AS VARCHAR) FROM t LIMIT 3"); assert_eq!(r.len(), 3); }
}

// =============================================================================
// Module 28: Systematic string WHERE + aggregate combos
// =============================================================================
mod where_agg_combos {
    use super::*;

    fn mk() -> TestDb { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR, v DOUBLE)"); let names = ["alice", "bob", "charlie", "diana", "eve", "frank", "grace", "henry", "iris", "jack", "alice", "bob", "charlie", "diana", "eve", "frank", "grace", "henry", "iris", "jack"]; for (i, n) in names.iter().enumerate() { db.exec_ok(&format!("INSERT INTO t VALUES ({}, '{}', {}.0)", ts(i as i64), n, i * 5)); } db }

    #[test] fn count_all() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t"), Value::I64(20)); }
    #[test] fn count_alice() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE s = 'alice'"), Value::I64(2)); }
    #[test] fn count_bob() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE s = 'bob'"), Value::I64(2)); }
    #[test] fn count_charlie() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE s = 'charlie'"), Value::I64(2)); }
    #[test] fn ne_alice() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE s != 'alice'"), Value::I64(18)); }
    #[test] fn like_a_pct() { let (_, r) = mk().query("SELECT s FROM t WHERE s LIKE 'a%'"); assert_eq!(r.len(), 2); }
    #[test] fn like_b_pct() { let (_, r) = mk().query("SELECT s FROM t WHERE s LIKE 'b%'"); assert_eq!(r.len(), 2); }
    #[test] fn like_pct_e() { let (_, r) = mk().query("SELECT s FROM t WHERE s LIKE '%e'"); assert!(r.len() >= 4); }
    #[test] fn ilike_alice() { let (_, r) = mk().query("SELECT s FROM t WHERE s ILIKE 'ALICE'"); assert_eq!(r.len(), 2); }
    #[test] fn in_2() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE s IN ('alice', 'bob')"), Value::I64(4)); }
    #[test] fn in_3() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE s IN ('alice', 'bob', 'charlie')"), Value::I64(6)); }
    #[test] fn not_in_2() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE s NOT IN ('alice', 'bob')"), Value::I64(16)); }
    #[test] fn distinct_s() { let (_, r) = mk().query("SELECT DISTINCT s FROM t"); assert_eq!(r.len(), 10); }
    #[test] fn grp_count() { let (_, r) = mk().query("SELECT s, count(*) FROM t GROUP BY s ORDER BY s"); assert_eq!(r.len(), 10); }
    #[test] fn grp_sum() { let (_, r) = mk().query("SELECT s, sum(v) FROM t GROUP BY s ORDER BY s"); assert_eq!(r.len(), 10); }
    #[test] fn grp_min() { let (_, r) = mk().query("SELECT s, min(v) FROM t GROUP BY s ORDER BY s"); assert_eq!(r.len(), 10); }
    #[test] fn grp_max() { let (_, r) = mk().query("SELECT s, max(v) FROM t GROUP BY s ORDER BY s"); assert_eq!(r.len(), 10); }
    #[test] fn grp_avg() { let (_, r) = mk().query("SELECT s, avg(v) FROM t GROUP BY s ORDER BY s"); assert_eq!(r.len(), 10); }
    #[test] fn grp_first() { let (_, r) = mk().query("SELECT s, first(v) FROM t GROUP BY s ORDER BY s"); assert_eq!(r.len(), 10); }
    #[test] fn grp_last() { let (_, r) = mk().query("SELECT s, last(v) FROM t GROUP BY s ORDER BY s"); assert_eq!(r.len(), 10); }
    #[test] fn having_gt_1() { let (_, r) = mk().query("SELECT s, count(*) AS c FROM t GROUP BY s HAVING c > 1"); assert_eq!(r.len(), 10); }
    #[test] fn order_asc() { let (_, r) = mk().query("SELECT s FROM t ORDER BY s ASC LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn order_desc() { let (_, r) = mk().query("SELECT s FROM t ORDER BY s DESC LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn limit_10() { let (_, r) = mk().query("SELECT s FROM t LIMIT 10"); assert_eq!(r.len(), 10); }
    #[test] fn offset_10() { let (_, r) = mk().query("SELECT s FROM t ORDER BY s ASC LIMIT 10 OFFSET 10"); assert_eq!(r.len(), 10); }
    #[test] fn star() { let (c, r) = mk().query("SELECT * FROM t LIMIT 5"); assert_eq!(c.len(), 3); assert_eq!(r.len(), 5); }
    #[test] fn upper_alice() { let (_, r) = mk().query("SELECT upper(s) FROM t WHERE s = 'alice'"); for row in &r { assert_eq!(row[0], Value::Str("ALICE".into())); } }
    #[test] fn lower_all() { let (_, r) = mk().query("SELECT lower(s) FROM t"); assert_eq!(r.len(), 20); }
    #[test] fn length_all() { let (_, r) = mk().query("SELECT length(s) FROM t"); assert_eq!(r.len(), 20); }
    #[test] fn concat_all() { let (_, r) = mk().query("SELECT s || '!' FROM t"); assert_eq!(r.len(), 20); }
    #[test] fn replace_all() { let (_, r) = mk().query("SELECT replace(s, 'a', 'A') FROM t"); assert_eq!(r.len(), 20); }
    #[test] fn case_alice() { let (_, r) = mk().query("SELECT CASE WHEN s = 'alice' THEN 'A' ELSE 'X' END FROM t"); assert_eq!(r.len(), 20); }
    #[test] fn coalesce_all() { let (_, r) = mk().query("SELECT coalesce(s, 'N/A') FROM t"); assert_eq!(r.len(), 20); }
    #[test] fn cast_vc() { let (_, r) = mk().query("SELECT CAST(s AS VARCHAR) FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn like_and_order() { let (_, r) = mk().query("SELECT s FROM t WHERE s LIKE 'a%' ORDER BY s ASC"); assert_eq!(r.len(), 2); }
    #[test] fn like_and_limit() { let (_, r) = mk().query("SELECT s FROM t WHERE s LIKE '%e' LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn in_and_order() { let (_, r) = mk().query("SELECT s FROM t WHERE s IN ('alice', 'bob') ORDER BY s ASC"); assert_eq!(r.len(), 4); }
    #[test] fn distinct_like() { let (_, r) = mk().query("SELECT DISTINCT s FROM t WHERE s LIKE '%e'"); assert!(r.len() >= 1); }
    #[test] fn min_s() { assert_eq!(mk().query_scalar("SELECT min(s) FROM t"), Value::Str("alice".into())); }
    #[test] fn max_s() { assert_eq!(mk().query_scalar("SELECT max(s) FROM t"), Value::Str("jack".into())); }
    #[test] fn first_s() { assert_eq!(mk().query_scalar("SELECT first(s) FROM t"), Value::Str("alice".into())); }
    #[test] fn last_s() { assert_eq!(mk().query_scalar("SELECT last(s) FROM t"), Value::Str("jack".into())); }
    #[test] fn count_v() { assert_eq!(mk().query_scalar("SELECT count(v) FROM t"), Value::I64(20)); }
    #[test] fn eq_and_ne() { let (_, r) = mk().query("SELECT s FROM t WHERE s = 'alice' AND s != 'bob'"); assert_eq!(r.len(), 2); }
    #[test] fn or_eq() { let (_, r) = mk().query("SELECT s FROM t WHERE s = 'alice' OR s = 'jack'"); assert_eq!(r.len(), 4); }
    #[test] fn like_or_eq() { let (_, r) = mk().query("SELECT s FROM t WHERE s LIKE 'a%' OR s = 'jack'"); assert_eq!(r.len(), 4); }
    #[test] fn sample_1h() { let (_, r) = mk().query("SELECT count(*) FROM t SAMPLE BY 1h"); assert!(!r.is_empty()); }
    #[test] fn alias_s() { let (c, _) = mk().query("SELECT s AS name FROM t LIMIT 1"); assert!(c.contains(&"name".to_string())); }
    #[test] fn concat_fn() { let v = mk().query_scalar("SELECT concat(s, '-test') FROM t WHERE s = 'alice' LIMIT 1"); assert_eq!(v, Value::Str("alice-test".into())); }
}

// =============================================================================
// Module 29: Bulk per-name tests
// =============================================================================
mod per_name_tests {
    use super::*;

    fn mk() -> TestDb { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, name VARCHAR, age BIGINT)"); let data = [("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Diana", 28), ("Eve", 32), ("Frank", 40), ("Grace", 22), ("Henry", 45), ("Iris", 27), ("Jack", 33), ("Kate", 29), ("Leo", 38), ("Mia", 24), ("Nick", 42), ("Olivia", 31)]; for (i, (n, a)) in data.iter().enumerate() { db.exec_ok(&format!("INSERT INTO t VALUES ({}, '{}', {})", ts(i as i64), n, a)); } db }

    #[test] fn count_15() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t"), Value::I64(15)); }
    #[test] fn eq_alice() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Alice'"), Value::I64(1)); }
    #[test] fn eq_bob() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Bob'"), Value::I64(1)); }
    #[test] fn eq_charlie() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Charlie'"), Value::I64(1)); }
    #[test] fn eq_diana() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Diana'"), Value::I64(1)); }
    #[test] fn eq_eve() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Eve'"), Value::I64(1)); }
    #[test] fn eq_frank() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Frank'"), Value::I64(1)); }
    #[test] fn eq_grace() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Grace'"), Value::I64(1)); }
    #[test] fn eq_henry() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Henry'"), Value::I64(1)); }
    #[test] fn eq_iris() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Iris'"), Value::I64(1)); }
    #[test] fn eq_jack() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Jack'"), Value::I64(1)); }
    #[test] fn eq_kate() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Kate'"), Value::I64(1)); }
    #[test] fn eq_leo() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Leo'"), Value::I64(1)); }
    #[test] fn eq_mia() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Mia'"), Value::I64(1)); }
    #[test] fn eq_nick() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Nick'"), Value::I64(1)); }
    #[test] fn eq_olivia() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name = 'Olivia'"), Value::I64(1)); }
    #[test] fn ne_alice() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name != 'Alice'"), Value::I64(14)); }
    #[test] fn like_a_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'A%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_b_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'B%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_c_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'C%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_d_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'D%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_e_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'E%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_f_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'F%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_g_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'G%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_h_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'H%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_i_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'I%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_j_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'J%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_k_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'K%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_l_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'L%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_m_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'M%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_n_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'N%'"); assert_eq!(r.len(), 1); }
    #[test] fn like_o_pct() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE 'O%'"); assert_eq!(r.len(), 1); }
    #[test] fn ilike_alice() { let (_, r) = mk().query("SELECT name FROM t WHERE name ILIKE 'alice'"); assert_eq!(r.len(), 1); }
    #[test] fn ilike_bob() { let (_, r) = mk().query("SELECT name FROM t WHERE name ILIKE 'BOB'"); assert_eq!(r.len(), 1); }
    #[test] fn in_alice_bob() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name IN ('Alice', 'Bob')"), Value::I64(2)); }
    #[test] fn in_3() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name IN ('Alice', 'Bob', 'Charlie')"), Value::I64(3)); }
    #[test] fn in_5() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name IN ('Alice', 'Bob', 'Charlie', 'Diana', 'Eve')"), Value::I64(5)); }
    #[test] fn not_in_1() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name NOT IN ('Alice')"), Value::I64(14)); }
    #[test] fn not_in_5() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE name NOT IN ('Alice', 'Bob', 'Charlie', 'Diana', 'Eve')"), Value::I64(10)); }
    #[test] fn distinct_all() { let (_, r) = mk().query("SELECT DISTINCT name FROM t"); assert_eq!(r.len(), 15); }
    #[test] fn order_asc() { let (_, r) = mk().query("SELECT name FROM t ORDER BY name ASC LIMIT 1"); assert_eq!(r[0][0], Value::Str("Alice".into())); }
    #[test] fn order_desc() { let (_, r) = mk().query("SELECT name FROM t ORDER BY name DESC LIMIT 1"); assert_eq!(r[0][0], Value::Str("Olivia".into())); }
    #[test] fn min_name() { assert_eq!(mk().query_scalar("SELECT min(name) FROM t"), Value::Str("Alice".into())); }
    #[test] fn max_name() { assert_eq!(mk().query_scalar("SELECT max(name) FROM t"), Value::Str("Olivia".into())); }
    #[test] fn first_name() { assert_eq!(mk().query_scalar("SELECT first(name) FROM t"), Value::Str("Alice".into())); }
    #[test] fn last_name() { assert_eq!(mk().query_scalar("SELECT last(name) FROM t"), Value::Str("Olivia".into())); }
    #[test] fn upper_all() { let (_, r) = mk().query("SELECT upper(name) FROM t"); assert_eq!(r.len(), 15); }
    #[test] fn lower_all() { let (_, r) = mk().query("SELECT lower(name) FROM t"); assert_eq!(r.len(), 15); }
    #[test] fn length_all() { let (_, r) = mk().query("SELECT length(name) FROM t"); assert_eq!(r.len(), 15); }
    #[test] fn concat_all() { let (_, r) = mk().query("SELECT name || '!' FROM t"); assert_eq!(r.len(), 15); }
    #[test] fn case_a() { let (_, r) = mk().query("SELECT CASE WHEN name = 'Alice' THEN 'yes' ELSE 'no' END FROM t"); assert_eq!(r.len(), 15); }
    #[test] fn coalesce_all() { let (_, r) = mk().query("SELECT coalesce(name, 'N/A') FROM t"); assert_eq!(r.len(), 15); }
    #[test] fn limit_5() { let (_, r) = mk().query("SELECT name FROM t LIMIT 5"); assert_eq!(r.len(), 5); }
    #[test] fn offset_5() { let (_, r) = mk().query("SELECT name FROM t ORDER BY name LIMIT 5 OFFSET 5"); assert_eq!(r.len(), 5); }
    #[test] fn star() { let (c, r) = mk().query("SELECT * FROM t LIMIT 5"); assert_eq!(c.len(), 3); assert_eq!(r.len(), 5); }
    #[test] fn age_sum() { let v = mk().query_scalar("SELECT sum(age) FROM t"); match v { Value::I64(n) => assert_eq!(n, 481), Value::F64(f) => assert!((f - 481.0).abs() < 0.01), _ => panic!("got {v:?}") } }
    #[test] fn age_min() { assert_eq!(mk().query_scalar("SELECT min(age) FROM t"), Value::I64(22)); }
    #[test] fn age_max() { assert_eq!(mk().query_scalar("SELECT max(age) FROM t"), Value::I64(45)); }
    #[test] fn age_gt_30() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE age > 30"), Value::I64(8)); }
    #[test] fn age_lt_30() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE age < 30"), Value::I64(6)); }
    #[test] fn age_btw() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE age BETWEEN 25 AND 35"), Value::I64(9)); }
    #[test] fn name_and_age() { let (_, r) = mk().query("SELECT name, age FROM t WHERE age > 40 ORDER BY age DESC"); assert!(r.len() >= 2); }
    #[test] fn like_pct_e() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE '%e'"); assert!(r.len() >= 2); }
    #[test] fn like_pct_a() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE '%a'"); assert!(r.len() >= 2); }
    #[test] fn replace_all() { let (_, r) = mk().query("SELECT replace(name, 'i', 'I') FROM t"); assert_eq!(r.len(), 15); }
    #[test] fn concat_name_age() { let (_, r) = mk().query("SELECT name || ':' || CAST(age AS VARCHAR) FROM t LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn like_5_chars() { let (_, r) = mk().query("SELECT name FROM t WHERE name LIKE '_____'"); assert!(r.len() >= 1); }
    #[test] fn ilike_pct_ia() { let (_, r) = mk().query("SELECT name FROM t WHERE name ILIKE '%IA'"); assert!(r.len() >= 1); }
    #[test] fn age_eq_30() { assert_eq!(mk().query_scalar("SELECT count(*) FROM t WHERE age = 30"), Value::I64(1)); }
    #[test] fn name_order_limit_3() { let (_, r) = mk().query("SELECT name FROM t ORDER BY name ASC LIMIT 3"); assert_eq!(r.len(), 3); }
    #[test] fn name_distinct_like() { let (_, r) = mk().query("SELECT DISTINCT name FROM t WHERE name LIKE '%a%'"); assert!(r.len() >= 1); }
    #[test] fn sample_1h() { let (_, r) = mk().query("SELECT count(*) FROM t SAMPLE BY 1h"); assert!(!r.is_empty()); }
}
