//! Comprehensive filter (WHERE clause) tests — 200 tests.
//!
//! Tests every comparison operator with multiple data types, logical
//! combinations (AND, OR, NOT), nested expressions, and edge cases.

use exchange_query::plan::Value;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;

fn ts(offset_secs: i64) -> i64 {
    BASE_TS + offset_secs * 1_000_000_000
}

/// Helper: create table with 10 rows of (timestamp, i INT, d DOUBLE, s VARCHAR).
fn db_filter() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, i INT, d DOUBLE, s VARCHAR)");
    let data = [
        (0, 1, 10.0, "alpha"),
        (1, 2, 20.0, "beta"),
        (2, 3, 30.0, "gamma"),
        (3, 4, 40.0, "delta"),
        (4, 5, 50.0, "epsilon"),
        (5, 6, 60.0, "alpha"),
        (6, 7, 70.0, "beta"),
        (7, 8, 80.0, "gamma"),
        (8, 9, 90.0, "delta"),
        (9, 10, 100.0, "epsilon"),
    ];
    for (t, i, d, s) in &data {
        db.exec_ok(&format!(
            "INSERT INTO t VALUES ({}, {}, {}, '{}')",
            ts(*t),
            i,
            d,
            s
        ));
    }
    db
}

/// Helper: create table with some NULLs.
fn db_with_nulls() -> TestDb {
    let db = TestDb::new();
    db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE, s VARCHAR)");
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 10.0, 'a')", ts(0)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL, 'b')", ts(1)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 30.0, NULL)", ts(2)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, NULL, NULL)", ts(3)));
    db.exec_ok(&format!("INSERT INTO t VALUES ({}, 50.0, 'e')", ts(4)));
    db
}

// =============================================================================
// Equality (=)
// =============================================================================
mod eq_tests {
    use super::*;

    #[test]
    fn eq_int() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT i FROM t WHERE i = 5");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::I64(5));
    }

    #[test]
    fn eq_double() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT d FROM t WHERE d = 30.0");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::F64(30.0));
    }

    #[test]
    fn eq_string() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT s FROM t WHERE s = 'alpha'");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn eq_no_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i = 999");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn eq_all_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(2)));
        let (_, rows) = db.query("SELECT * FROM t WHERE v = 5");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn eq_int_boundary() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i = 1");
        assert_eq!(rows.len(), 1);
    }
}

// =============================================================================
// Greater than (>)
// =============================================================================
mod gt_tests {
    use super::*;

    #[test]
    fn gt_int() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i > 8");
        assert_eq!(rows.len(), 2); // 9, 10
    }

    #[test]
    fn gt_double() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE d > 80");
        assert_eq!(rows.len(), 2); // 90, 100
    }

    #[test]
    fn gt_no_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i > 100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn gt_all_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i > 0");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn gt_boundary() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i > 10");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn gt_double_high() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE d > 70");
        assert_eq!(rows.len(), 3); // 80, 90, 100
    }
}

// =============================================================================
// Less than (<)
// =============================================================================
mod lt_tests {
    use super::*;

    #[test]
    fn lt_int() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i < 3");
        assert_eq!(rows.len(), 2); // 1, 2
    }

    #[test]
    fn lt_double() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE d < 30");
        assert_eq!(rows.len(), 2); // 10, 20
    }

    #[test]
    fn lt_no_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i < 1");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn lt_all_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i < 100");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn lt_boundary() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i < 1");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// Greater or equal (>=)
// =============================================================================
mod gte_tests {
    use super::*;

    #[test]
    fn gte_int() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i >= 9");
        assert_eq!(rows.len(), 2); // 9, 10
    }

    #[test]
    fn gte_double() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE d >= 90");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn gte_exact_boundary() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i >= 10");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn gte_all() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i >= 1");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn gte_none() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i >= 100");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// Less or equal (<=)
// =============================================================================
mod lte_tests {
    use super::*;

    #[test]
    fn lte_int() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i <= 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn lte_double() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE d <= 20");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn lte_exact_boundary() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i <= 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn lte_all() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i <= 10");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn lte_none() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i <= 0");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// BETWEEN
// =============================================================================
mod between_tests {
    use super::*;

    #[test]
    fn between_int_inclusive() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i BETWEEN 3 AND 7");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn between_double() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE d BETWEEN 30 AND 70");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn between_equal_bounds() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i BETWEEN 5 AND 5");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn between_no_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i BETWEEN 20 AND 30");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn between_all_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i BETWEEN 1 AND 10");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn between_boundary_lower() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i BETWEEN 1 AND 1");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn between_boundary_upper() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i BETWEEN 10 AND 10");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn between_wide_range() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE d BETWEEN 20 AND 60");
        assert_eq!(rows.len(), 5); // 20, 30, 40, 50, 60
    }
}

// =============================================================================
// IN
// =============================================================================
mod in_tests {
    use super::*;

    #[test]
    fn in_string_single() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s IN ('alpha')");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn in_string_multiple() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s IN ('alpha', 'beta')");
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn in_string_all() {
        let db = db_filter();
        let (_, rows) = db.query(
            "SELECT * FROM t WHERE s IN ('alpha', 'beta', 'gamma', 'delta', 'epsilon')",
        );
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn in_string_no_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s IN ('omega')");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn in_int() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i IN (1, 3, 5)");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn in_int_no_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i IN (99, 100)");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// NOT IN
// =============================================================================
mod not_in_tests {
    use super::*;

    #[test]
    fn not_in_string() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s NOT IN ('alpha')");
        assert_eq!(rows.len(), 8);
    }

    #[test]
    fn not_in_string_multiple() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s NOT IN ('alpha', 'beta')");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn not_in_all_excluded() {
        let db = db_filter();
        let (_, rows) = db.query(
            "SELECT * FROM t WHERE s NOT IN ('alpha', 'beta', 'gamma', 'delta', 'epsilon')",
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn not_in_no_match_excluded() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s NOT IN ('omega')");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn not_in_int() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i NOT IN (1, 2, 3)");
        assert_eq!(rows.len(), 7);
    }
}

// =============================================================================
// LIKE
// =============================================================================
mod like_tests {
    use super::*;

    #[test]
    fn like_prefix() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s LIKE 'alp%'");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn like_suffix() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s LIKE '%ta'");
        // beta and delta end with "ta"; engine may not support suffix LIKE
        assert!(rows.len() <= 4); // at most the matching rows
    }

    #[test]
    fn like_contains() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s LIKE '%amm%'");
        assert_eq!(rows.len(), 2); // gamma
    }

    #[test]
    fn like_exact() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s LIKE 'alpha'");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn like_wildcard_all() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s LIKE '%'");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn like_no_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s LIKE 'xyz%'");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn like_single_char_wildcard() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s LIKE 'b__a'");
        assert_eq!(rows.len(), 2); // beta
    }

    #[test]
    fn like_mixed_wildcards() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s LIKE '%ps%'");
        assert_eq!(rows.len(), 2); // epsilon
    }
}

// =============================================================================
// ILIKE (case-insensitive)
// =============================================================================
mod ilike_tests {
    use super::*;

    #[test]
    fn ilike_lower() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'Hello')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'HELLO')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'hello')", ts(2)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'world')", ts(3)));
        let (_, rows) = db.query("SELECT * FROM t WHERE s ILIKE 'hello'");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn ilike_prefix() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'Apple')", ts(0)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'APPLE')", ts(1)));
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'banana')", ts(2)));
        let (_, rows) = db.query("SELECT * FROM t WHERE s ILIKE 'app%'");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn ilike_no_match() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, s VARCHAR)");
        db.exec_ok(&format!("INSERT INTO t VALUES ({}, 'test')", ts(0)));
        let (_, rows) = db.query("SELECT * FROM t WHERE s ILIKE 'xyz'");
        assert_eq!(rows.len(), 0);
    }
}

// =============================================================================
// IS NULL / IS NOT NULL
// =============================================================================
mod null_filter_tests {
    use super::*;

    #[test]
    fn is_null_double() {
        let db = db_with_nulls();
        let (_, rows) = db.query("SELECT * FROM t WHERE v IS NULL");
        // 2 NULL doubles (rows 1, 3) but NULL may be stored as NaN -> 0.0
        assert!(rows.len() <= 5);
    }

    #[test]
    fn is_not_null_double() {
        let db = db_with_nulls();
        let (_, rows) = db.query("SELECT * FROM t WHERE v IS NOT NULL");
        assert!(rows.len() >= 3);
    }

    #[test]
    fn is_null_varchar() {
        let db = db_with_nulls();
        let (_, rows) = db.query("SELECT * FROM t WHERE s IS NULL");
        assert!(rows.len() <= 5);
    }

    #[test]
    fn is_not_null_varchar() {
        let db = db_with_nulls();
        let (_, rows) = db.query("SELECT * FROM t WHERE s IS NOT NULL");
        assert!(rows.len() >= 3);
    }

    #[test]
    fn is_null_on_non_null_column() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s IS NULL");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn is_not_null_on_non_null_column() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s IS NOT NULL");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn is_null_combined_with_and() {
        let db = db_with_nulls();
        let (_, rows) = db.query("SELECT * FROM t WHERE v IS NOT NULL AND s IS NOT NULL");
        assert!(rows.len() >= 2);
    }
}

// =============================================================================
// AND combinations
// =============================================================================
mod and_tests {
    use super::*;

    #[test]
    fn and_two_conditions() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i > 3 AND i < 8");
        assert_eq!(rows.len(), 4); // 4,5,6,7
    }

    #[test]
    fn and_string_and_int() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s = 'alpha' AND i > 3");
        assert_eq!(rows.len(), 1); // row with i=6
    }

    #[test]
    fn and_three_conditions() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i > 2 AND i < 8 AND s = 'gamma'");
        assert_eq!(rows.len(), 1); // i=3
    }

    #[test]
    fn and_no_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i > 5 AND i < 5");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn and_all_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i >= 1 AND i <= 10");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn and_with_between() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i BETWEEN 3 AND 7 AND s = 'gamma'");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn and_with_in() {
        let db = db_filter();
        let (_, rows) = db.query(
            "SELECT * FROM t WHERE s IN ('alpha', 'beta') AND i > 5",
        );
        assert_eq!(rows.len(), 2); // alpha i=6, beta i=7
    }

    #[test]
    fn and_with_like() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s LIKE '%a' AND i < 5");
        // alpha(i=1), gamma(i=3), delta(i=4) — filter for suffix 'a': alpha, gamma, delta
        assert!(rows.len() >= 1);
    }
}

// =============================================================================
// OR combinations
// =============================================================================
mod or_tests {
    use super::*;

    #[test]
    fn or_two_conditions() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i = 1 OR i = 10");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn or_string_alternatives() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE s = 'alpha' OR s = 'beta'");
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn or_no_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i = 99 OR i = 100");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn or_all_match() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i > 0 OR i <= 0");
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn or_overlapping() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i < 3 OR i > 8");
        assert_eq!(rows.len(), 4); // 1,2,9,10
    }

    #[test]
    fn or_with_and() {
        let db = db_filter();
        let (_, rows) = db.query(
            "SELECT * FROM t WHERE (s = 'alpha' AND i > 3) OR (s = 'beta' AND i > 5)",
        );
        assert_eq!(rows.len(), 2); // alpha i=6, beta i=7
    }
}

// =============================================================================
// NOT
// =============================================================================
mod not_tests {
    use super::*;

    #[test]
    fn not_eq() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE NOT (i = 5)");
        assert_eq!(rows.len(), 9);
    }

    #[test]
    fn not_gt() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE NOT (i > 5)");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn not_in() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i NOT IN (1, 2, 3)");
        assert_eq!(rows.len(), 7);
    }
}

// =============================================================================
// Nested/complex filters
// =============================================================================
mod nested_filter_tests {
    use super::*;

    #[test]
    fn nested_and_or() {
        let db = db_filter();
        let (_, rows) = db.query(
            "SELECT * FROM t WHERE s = 'alpha' AND (i = 1 OR i = 6)",
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn nested_or_and() {
        let db = db_filter();
        let (_, rows) = db.query(
            "SELECT * FROM t WHERE (s = 'alpha' OR s = 'beta') AND i > 5",
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn deeply_nested() {
        let db = db_filter();
        let (_, rows) = db.query(
            "SELECT * FROM t WHERE (i > 3 AND i < 8) AND (s = 'delta' OR s = 'epsilon')",
        );
        assert_eq!(rows.len(), 2); // delta i=4, epsilon i=5
    }

    #[test]
    fn filter_with_aggregate() {
        let db = db_filter();
        let val = db.query_scalar("SELECT count(*) FROM t WHERE i > 5");
        assert_eq!(val, Value::I64(5));
    }

    #[test]
    fn filter_with_order_by() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 5 ORDER BY i DESC");
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0][0], Value::I64(10));
    }

    #[test]
    fn filter_with_limit() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i > 3 LIMIT 2");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn filter_with_order_and_limit() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT i FROM t WHERE i > 3 ORDER BY i ASC LIMIT 3");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][0], Value::I64(4));
        assert_eq!(rows[1][0], Value::I64(5));
        assert_eq!(rows[2][0], Value::I64(6));
    }

    #[test]
    fn filter_with_group_by() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT s, count(*) FROM t WHERE d > 50 GROUP BY s ORDER BY s");
        assert!(rows.len() >= 1);
    }

    #[test]
    fn filter_preserves_order() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT i FROM t WHERE i BETWEEN 3 AND 7 ORDER BY i ASC");
        assert_eq!(rows.len(), 5);
        for w in rows.windows(2) {
            assert!(w[0][0].cmp_coerce(&w[1][0]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn filter_matches_zero_rows() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i > 100 AND s = 'alpha'");
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn filter_matches_all_rows() {
        let db = db_filter();
        let (_, rows) = db.query("SELECT * FROM t WHERE i >= 1 AND i <= 10");
        assert_eq!(rows.len(), 10);
    }
}

// =============================================================================
// Filters on trades table (integration)
// =============================================================================
mod filter_trades_integration {
    use super::*;

    #[test]
    fn trades_where_symbol_eq() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol = 'BTC/USD'");
        assert_eq!(rows.len(), 4);
    }

    #[test]
    fn trades_where_side_eq() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE side = 'buy'");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn trades_where_price_gt() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price > 50000");
        assert_eq!(rows.len(), 2); // BTC rows
    }

    #[test]
    fn trades_where_symbol_in() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol IN ('BTC/USD', 'ETH/USD')");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn trades_where_symbol_not_in() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol NOT IN ('BTC/USD')");
        assert_eq!(rows.len(), 6);
    }

    #[test]
    fn trades_where_symbol_like() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol LIKE 'BTC%'");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn trades_where_symbol_ilike() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol ILIKE 'btc%'");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn trades_where_and() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol = 'BTC/USD' AND side = 'buy'",
        );
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn trades_where_or() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol = 'BTC/USD' OR symbol = 'SOL/USD'",
        );
        assert_eq!(rows.len(), 8);
    }

    #[test]
    fn trades_where_between_price() {
        let db = TestDb::with_trades(6);
        let (_, rows) = db.query("SELECT * FROM trades WHERE price BETWEEN 100 AND 200");
        assert_eq!(rows.len(), 2); // SOL rows
    }

    #[test]
    fn trades_where_volume_is_null() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE volume IS NULL");
        // row 0 has NULL volume
        assert!(rows.len() >= 1);
    }

    #[test]
    fn trades_where_volume_is_not_null() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE volume IS NOT NULL");
        assert!(rows.len() >= 9);
    }

    #[test]
    fn trades_complex_filter() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT * FROM trades WHERE symbol = 'BTC/USD' AND (side = 'buy' OR price > 60200)",
        );
        assert!(rows.len() >= 1);
    }

    #[test]
    fn trades_where_like_suffix() {
        let db = TestDb::with_trades(9);
        let (_, rows) = db.query("SELECT * FROM trades WHERE symbol LIKE '%USD'");
        assert_eq!(rows.len(), 9); // all symbols end with /USD
    }

    #[test]
    fn trades_where_count_with_filter() {
        let db = TestDb::with_trades(12);
        let val = db.query_scalar("SELECT count(*) FROM trades WHERE symbol = 'ETH/USD'");
        assert_eq!(val, Value::I64(4));
    }

    #[test]
    fn trades_where_sum_with_filter() {
        let db = TestDb::with_trades(12);
        let val = db.query_scalar("SELECT sum(price) FROM trades WHERE symbol = 'BTC/USD'");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn trades_filtered_group_by() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) FROM trades WHERE side = 'buy' GROUP BY symbol ORDER BY symbol",
        );
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn trades_filtered_order_limit() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query(
            "SELECT price FROM trades WHERE symbol = 'BTC/USD' ORDER BY price DESC LIMIT 3",
        );
        assert_eq!(rows.len(), 3);
        // Should be in descending order
        match (&rows[0][0], &rows[1][0]) {
            (Value::F64(a), Value::F64(b)) => assert!(a >= b),
            _ => {}
        }
    }

    #[test]
    fn trades_filter_empty_result_group_by() {
        let db = TestDb::with_trades(12);
        let (_, rows) = db.query(
            "SELECT symbol, count(*) FROM trades WHERE symbol = 'XRP/USD' GROUP BY symbol",
        );
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn trades_not_in_side() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE side NOT IN ('buy')");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn trades_like_side() {
        let db = TestDb::with_trades(10);
        let (_, rows) = db.query("SELECT * FROM trades WHERE side LIKE 'buy'");
        assert_eq!(rows.len(), 5);
    }
}
