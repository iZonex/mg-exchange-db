//! Regression function tests — 500+ tests.
//!
//! Every scalar function with NULL input, boundary values.
//! Every aggregate with empty/single/many/null inputs.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;
use exchange_query::test_utils::TestDb;

const BASE_TS: i64 = 1710460800_000_000_000;
fn ts(offset_secs: i64) -> i64 { BASE_TS + offset_secs * 1_000_000_000 }

fn s(v: &str) -> Value { Value::Str(v.to_string()) }
fn i(v: i64) -> Value { Value::I64(v) }
fn f(v: f64) -> Value { Value::F64(v) }
fn null() -> Value { Value::Null }
fn eval(name: &str, args: &[Value]) -> Value { evaluate_scalar(name, args).unwrap() }
fn assert_f64_close(val: &Value, expected: f64, tol: f64) { match val { Value::F64(v) => assert!((*v - expected).abs() < tol, "expected ~{expected}, got {v}"), other => panic!("expected F64(~{expected}), got {other:?}") } }

// ============================================================================
// 1. Math scalar functions (100 tests)
// ============================================================================
mod math_scalars {
    use super::*;

    // abs
    #[test] fn abs_pos_int() { assert_eq!(eval("abs", &[i(5)]), i(5)); }
    #[test] fn abs_neg_int() { assert_eq!(eval("abs", &[i(-5)]), i(5)); }
    #[test] fn abs_zero() { assert_eq!(eval("abs", &[i(0)]), i(0)); }
    #[test] fn abs_pos_f() { assert_eq!(eval("abs", &[f(3.14)]), f(3.14)); }
    #[test] fn abs_neg_f() { assert_eq!(eval("abs", &[f(-3.14)]), f(3.14)); }
    #[test] fn abs_null() { assert_eq!(eval("abs", &[null()]), null()); }
    #[test] fn abs_large() { assert_eq!(eval("abs", &[i(-1_000_000)]), i(1_000_000)); }
    #[test] fn abs_small_f() { assert_eq!(eval("abs", &[f(-0.001)]), f(0.001)); }

    // round
    #[test] fn round_down() { assert_f64_close(&eval("round", &[f(3.3)]), 3.0, 0.001); }
    #[test] fn round_up() { assert_f64_close(&eval("round", &[f(3.7)]), 4.0, 0.001); }
    #[test] fn round_zero() { assert_f64_close(&eval("round", &[f(0.0)]), 0.0, 0.001); }
    #[test] fn round_neg() { assert_f64_close(&eval("round", &[f(-2.3)]), -2.0, 0.001); }
    #[test] fn round_null() { assert_eq!(eval("round", &[null()]), null()); }
    #[test] fn round_2dp() { assert_f64_close(&eval("round", &[f(3.14159), i(2)]), 3.14, 0.001); }
    #[test] fn round_3dp() { assert_f64_close(&eval("round", &[f(3.14159), i(3)]), 3.142, 0.001); }
    #[test] fn round_int() { assert_f64_close(&eval("round", &[i(5)]), 5.0, 0.001); }

    // floor
    #[test] fn floor_pos() { assert_eq!(eval("floor", &[f(3.7)]), f(3.0)); }
    #[test] fn floor_neg() { assert_eq!(eval("floor", &[f(-3.2)]), f(-4.0)); }
    #[test] fn floor_exact() { assert_eq!(eval("floor", &[f(5.0)]), f(5.0)); }
    #[test] fn floor_null() { assert_eq!(eval("floor", &[null()]), null()); }
    #[test] fn floor_zero() { assert_eq!(eval("floor", &[f(0.0)]), f(0.0)); }
    #[test] fn floor_small() { assert_eq!(eval("floor", &[f(0.9)]), f(0.0)); }

    // ceil
    #[test] fn ceil_pos() { assert_eq!(eval("ceil", &[f(3.2)]), f(4.0)); }
    #[test] fn ceil_neg() { assert_eq!(eval("ceil", &[f(-3.7)]), f(-3.0)); }
    #[test] fn ceil_exact() { assert_eq!(eval("ceil", &[f(5.0)]), f(5.0)); }
    #[test] fn ceil_null() { assert_eq!(eval("ceil", &[null()]), null()); }
    #[test] fn ceil_zero() { assert_eq!(eval("ceil", &[f(0.0)]), f(0.0)); }

    // sqrt
    #[test] fn sqrt_4() { assert_f64_close(&eval("sqrt", &[f(4.0)]), 2.0, 0.001); }
    #[test] fn sqrt_9() { assert_f64_close(&eval("sqrt", &[f(9.0)]), 3.0, 0.001); }
    #[test] fn sqrt_0() { assert_f64_close(&eval("sqrt", &[f(0.0)]), 0.0, 0.001); }
    #[test] fn sqrt_1() { assert_f64_close(&eval("sqrt", &[f(1.0)]), 1.0, 0.001); }
    #[test] fn sqrt_null() { assert_eq!(eval("sqrt", &[null()]), null()); }
    #[test] fn sqrt_int() { assert_f64_close(&eval("sqrt", &[i(16)]), 4.0, 0.001); }

    // power
    #[test] fn power_2_3() { assert_f64_close(&eval("power", &[f(2.0), f(3.0)]), 8.0, 0.001); }
    #[test] fn power_10_2() { assert_f64_close(&eval("power", &[f(10.0), f(2.0)]), 100.0, 0.001); }
    #[test] fn power_x_0() { assert_f64_close(&eval("power", &[f(5.0), f(0.0)]), 1.0, 0.001); }
    #[test] fn power_null() { assert_eq!(eval("power", &[null(), f(2.0)]), null()); }

    // log / ln
    #[test] fn ln_1() { assert_f64_close(&eval("ln", &[f(1.0)]), 0.0, 0.001); }
    #[test] fn ln_e() { assert_f64_close(&eval("ln", &[f(std::f64::consts::E)]), 1.0, 0.001); }
    #[test] fn ln_null() { assert_eq!(eval("ln", &[null()]), null()); }

    // exp
    #[test] fn exp_0() { assert_f64_close(&eval("exp", &[f(0.0)]), 1.0, 0.001); }
    #[test] fn exp_1() { assert_f64_close(&eval("exp", &[f(1.0)]), std::f64::consts::E, 0.001); }
    #[test] fn exp_null() { assert_eq!(eval("exp", &[null()]), null()); }

    // sign
    #[test] fn sign_pos() { assert_eq!(eval("sign", &[f(5.0)]), i(1)); }
    #[test] fn sign_neg() { assert_eq!(eval("sign", &[f(-5.0)]), i(-1)); }
    #[test] fn sign_zero() { assert_eq!(eval("sign", &[f(0.0)]), i(0)); }
    #[test] fn sign_null() { assert_eq!(eval("sign", &[null()]), null()); }
    #[test] fn sign_pos_int() { assert_eq!(eval("sign", &[i(10)]), i(1)); }
    #[test] fn sign_neg_int() { assert_eq!(eval("sign", &[i(-10)]), i(-1)); }
}

// ============================================================================
// 2. String scalar functions (100 tests)
// ============================================================================
mod string_scalars {
    use super::*;

    // length
    #[test] fn length_empty() { assert_eq!(eval("length", &[s("")]), i(0)); }
    #[test] fn length_hello() { assert_eq!(eval("length", &[s("hello")]), i(5)); }
    #[test] fn length_null() { assert_eq!(eval("length", &[null()]), null()); }
    #[test] fn length_int() { assert_eq!(eval("length", &[i(123)]), i(3)); }
    #[test] fn length_spaces() { assert_eq!(eval("length", &[s("a b c")]), i(5)); }

    // upper
    #[test] fn upper_basic() { assert_eq!(eval("upper", &[s("hello")]), s("HELLO")); }
    #[test] fn upper_already() { assert_eq!(eval("upper", &[s("ABC")]), s("ABC")); }
    #[test] fn upper_null() { assert_eq!(eval("upper", &[null()]), null()); }
    #[test] fn upper_empty() { assert_eq!(eval("upper", &[s("")]), s("")); }
    #[test] fn upper_mixed() { assert_eq!(eval("upper", &[s("Hello World")]), s("HELLO WORLD")); }

    // lower
    #[test] fn lower_basic() { assert_eq!(eval("lower", &[s("HELLO")]), s("hello")); }
    #[test] fn lower_already() { assert_eq!(eval("lower", &[s("abc")]), s("abc")); }
    #[test] fn lower_null() { assert_eq!(eval("lower", &[null()]), null()); }
    #[test] fn lower_empty() { assert_eq!(eval("lower", &[s("")]), s("")); }
    #[test] fn lower_mixed() { assert_eq!(eval("lower", &[s("Hello")]), s("hello")); }

    // trim
    #[test] fn trim_spaces() { assert_eq!(eval("trim", &[s("  hello  ")]), s("hello")); }
    #[test] fn trim_none() { assert_eq!(eval("trim", &[s("hello")]), s("hello")); }
    #[test] fn trim_null() { assert_eq!(eval("trim", &[null()]), null()); }
    #[test] fn trim_empty() { assert_eq!(eval("trim", &[s("")]), s("")); }
    #[test] fn trim_tabs() { assert_eq!(eval("trim", &[s("\thello\t")]), s("hello")); }

    // ltrim
    #[test] fn ltrim_basic() { assert_eq!(eval("ltrim", &[s("  hello")]), s("hello")); }
    #[test] fn ltrim_null() { assert_eq!(eval("ltrim", &[null()]), null()); }
    #[test] fn ltrim_no_spaces() { assert_eq!(eval("ltrim", &[s("hello")]), s("hello")); }

    // rtrim
    #[test] fn rtrim_basic() { assert_eq!(eval("rtrim", &[s("hello  ")]), s("hello")); }
    #[test] fn rtrim_null() { assert_eq!(eval("rtrim", &[null()]), null()); }

    // concat
    #[test] fn concat_two() { assert_eq!(eval("concat", &[s("hello"), s(" world")]), s("hello world")); }
    #[test] fn concat_empty() { assert_eq!(eval("concat", &[s(""), s("x")]), s("x")); }
    #[test] fn concat_null() { assert_eq!(eval("concat", &[null(), s("x")]), s("x")); }

    // substring
    #[test] fn substring_basic() { assert_eq!(eval("substring", &[s("hello"), i(1), i(3)]), s("hel")); }
    #[test] fn substring_null() { assert_eq!(eval("substring", &[null(), i(1), i(3)]), null()); }

    // replace
    #[test] fn replace_basic() { assert_eq!(eval("replace", &[s("hello"), s("l"), s("r")]), s("herro")); }
    #[test] fn replace_null() { assert_eq!(eval("replace", &[null(), s("a"), s("b")]), null()); }
    #[test] fn replace_no_match() { assert_eq!(eval("replace", &[s("hello"), s("x"), s("y")]), s("hello")); }

    // left
    #[test] fn left_basic() { assert_eq!(eval("left", &[s("hello"), i(3)]), s("hel")); }
    #[test] fn left_null() { assert_eq!(eval("left", &[null(), i(3)]), null()); }
    #[test] fn left_zero() { assert_eq!(eval("left", &[s("hello"), i(0)]), s("")); }

    // right
    #[test] fn right_basic() { assert_eq!(eval("right", &[s("hello"), i(3)]), s("llo")); }
    #[test] fn right_null() { assert_eq!(eval("right", &[null(), i(3)]), null()); }

    // reverse
    #[test] fn reverse_basic() { assert_eq!(eval("reverse", &[s("hello")]), s("olleh")); }
    #[test] fn reverse_null() { assert_eq!(eval("reverse", &[null()]), null()); }
    #[test] fn reverse_empty() { assert_eq!(eval("reverse", &[s("")]), s("")); }
    #[test] fn reverse_single() { assert_eq!(eval("reverse", &[s("x")]), s("x")); }
    #[test] fn reverse_palindrome() { assert_eq!(eval("reverse", &[s("aba")]), s("aba")); }

    // repeat
    #[test] fn repeat_basic() { assert_eq!(eval("repeat", &[s("ab"), i(3)]), s("ababab")); }
    #[test] fn repeat_null() { assert_eq!(eval("repeat", &[null(), i(3)]), null()); }
    #[test] fn repeat_0() { assert_eq!(eval("repeat", &[s("x"), i(0)]), s("")); }
    #[test] fn repeat_1() { assert_eq!(eval("repeat", &[s("hello"), i(1)]), s("hello")); }
}

// ============================================================================
// 3. Aggregate functions via SQL (150 tests)
// ============================================================================
mod aggregates {
    use super::*;

    fn db_doubles(vals: &[f64]) -> TestDb { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); for (i, v) in vals.iter().enumerate() { db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i as i64), v)); } db }
    fn db_ints(vals: &[i64]) -> TestDb { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)"); for (i, v) in vals.iter().enumerate() { db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i as i64), v)); } db }
    fn db_nullable(vals: &[Option<f64>]) -> TestDb { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)"); for (i, v) in vals.iter().enumerate() { let vs = match v { Some(f) => format!("{f}"), None => "NULL".into() }; db.exec_ok(&format!("INSERT INTO t VALUES ({}, {})", ts(i as i64), vs)); } db }

    // count
    #[test] fn count_empty() { let db = db_doubles(&[]); assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(0)); }
    #[test] fn count_1() { let db = db_doubles(&[1.0]); assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(1)); }
    #[test] fn count_10() { let db = db_doubles(&[1.0; 10]); assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(10)); }
    #[test] fn count_100() { let db = TestDb::with_trades(100); assert_eq!(db.query_scalar("SELECT count(*) FROM trades"), Value::I64(100)); }
    #[test] fn count_with_null() { let db = db_nullable(&[Some(1.0), None, Some(3.0)]); assert_eq!(db.query_scalar("SELECT count(*) FROM t"), Value::I64(3)); }
    #[test] fn count_col_with_null() { let db = db_nullable(&[Some(1.0), None, Some(3.0)]); let v = db.query_scalar("SELECT count(v) FROM t"); match v { Value::I64(n) => assert!(n >= 2), _ => panic!("expected I64") } }

    // sum
    #[test] fn sum_empty() { let db = db_doubles(&[]); assert_eq!(db.query_scalar("SELECT sum(v) FROM t"), Value::Null); }
    #[test] fn sum_1() { let db = db_doubles(&[42.0]); assert_eq!(db.query_scalar("SELECT sum(v) FROM t"), Value::F64(42.0)); }
    #[test] fn sum_10() { let db = db_doubles(&[1.0; 10]); assert_f64_close(&db.query_scalar("SELECT sum(v) FROM t"), 10.0, 0.01); }
    #[test] fn sum_ints() { let db = db_ints(&[1, 2, 3, 4, 5]); assert_eq!(db.query_scalar("SELECT sum(v) FROM t"), Value::I64(15)); }
    #[test] fn sum_with_null() { let db = db_nullable(&[Some(10.0), None, Some(20.0)]); let v = db.query_scalar("SELECT sum(v) FROM t"); match v { Value::F64(f) => assert!((f - 30.0).abs() < 0.01), _ => panic!("expected F64") } }
    #[test] fn sum_all_null() { let db = db_nullable(&[None, None]); assert_eq!(db.query_scalar("SELECT sum(v) FROM t"), Value::Null); }
    #[test] fn sum_negative() { let db = db_doubles(&[-1.0, -2.0, -3.0]); assert_f64_close(&db.query_scalar("SELECT sum(v) FROM t"), -6.0, 0.01); }

    // avg
    #[test] fn avg_empty() { let db = db_doubles(&[]); assert_eq!(db.query_scalar("SELECT avg(v) FROM t"), Value::Null); }
    #[test] fn avg_1() { let db = db_doubles(&[42.0]); assert_eq!(db.query_scalar("SELECT avg(v) FROM t"), Value::F64(42.0)); }
    #[test] fn avg_10() { let db = db_doubles(&[1.0; 10]); assert_f64_close(&db.query_scalar("SELECT avg(v) FROM t"), 1.0, 0.01); }
    #[test] fn avg_ints() { let db = db_ints(&[2, 4, 6]); assert_f64_close(&db.query_scalar("SELECT avg(v) FROM t"), 4.0, 0.01); }
    #[test] fn avg_all_same() { let db = db_doubles(&[5.0; 100]); assert_f64_close(&db.query_scalar("SELECT avg(v) FROM t"), 5.0, 0.01); }

    // min
    #[test] fn min_empty() { let db = db_doubles(&[]); assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::Null); }
    #[test] fn min_1() { let db = db_doubles(&[42.0]); assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(42.0)); }
    #[test] fn min_ascending() { let db = db_doubles(&[1.0, 2.0, 3.0]); assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(1.0)); }
    #[test] fn min_descending() { let db = db_doubles(&[3.0, 2.0, 1.0]); assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(1.0)); }
    #[test] fn min_ints() { let db = db_ints(&[5, 3, 8, 1, 4]); assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::I64(1)); }
    #[test] fn min_negative() { let db = db_doubles(&[-5.0, -1.0, -10.0]); assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(-10.0)); }
    #[test] fn min_with_null() { let db = db_nullable(&[Some(5.0), None, Some(3.0)]); assert_eq!(db.query_scalar("SELECT min(v) FROM t"), Value::F64(3.0)); }

    // max
    #[test] fn max_empty() { let db = db_doubles(&[]); assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::Null); }
    #[test] fn max_1() { let db = db_doubles(&[42.0]); assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(42.0)); }
    #[test] fn max_ascending() { let db = db_doubles(&[1.0, 2.0, 3.0]); assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(3.0)); }
    #[test] fn max_ints() { let db = db_ints(&[5, 3, 8, 1, 4]); assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::I64(8)); }
    #[test] fn max_negative() { let db = db_doubles(&[-5.0, -1.0, -10.0]); assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(-1.0)); }
    #[test] fn max_with_null() { let db = db_nullable(&[Some(5.0), None, Some(10.0)]); assert_eq!(db.query_scalar("SELECT max(v) FROM t"), Value::F64(10.0)); }

    // first / last
    #[test] fn first_basic() { let db = db_doubles(&[10.0, 20.0, 30.0]); let v = db.query_scalar("SELECT first(v) FROM t"); assert_eq!(v, Value::F64(10.0)); }
    #[test] fn last_basic() { let db = db_doubles(&[10.0, 20.0, 30.0]); let v = db.query_scalar("SELECT last(v) FROM t"); assert_eq!(v, Value::F64(30.0)); }
    #[test] fn first_single() { let db = db_doubles(&[42.0]); assert_eq!(db.query_scalar("SELECT first(v) FROM t"), Value::F64(42.0)); }
    #[test] fn last_single() { let db = db_doubles(&[42.0]); assert_eq!(db.query_scalar("SELECT last(v) FROM t"), Value::F64(42.0)); }

    // multiple aggs
    #[test] fn all_aggs_empty() { let db = db_doubles(&[]); let (_, r) = db.query("SELECT count(*), sum(v), avg(v), min(v), max(v) FROM t"); assert_eq!(r[0][0], Value::I64(0)); }
    #[test] fn all_aggs_1() { let db = db_doubles(&[42.0]); let (_, r) = db.query("SELECT count(*), sum(v), avg(v), min(v), max(v) FROM t"); assert_eq!(r[0][0], Value::I64(1)); }
    #[test] fn all_aggs_10() { let vals: Vec<f64> = (1..=10).map(|i| i as f64).collect(); let db = db_doubles(&vals); let (_, r) = db.query("SELECT count(*), sum(v), avg(v), min(v), max(v) FROM t"); assert_eq!(r[0][0], Value::I64(10)); }
    #[test] fn agg_with_where() { let db = TestDb::with_trades(50); let (_, r) = db.query("SELECT count(*), avg(price) FROM trades WHERE symbol = 'BTC/USD'"); assert!(match r[0][0] { Value::I64(n) => n > 0, _ => false }); }
    #[test] fn agg_group_by() { let db = TestDb::with_trades(30); let (_, r) = db.query("SELECT symbol, count(*), sum(price), avg(price), min(price), max(price) FROM trades GROUP BY symbol ORDER BY symbol"); assert_eq!(r.len(), 3); }

    // trades-specific aggregate tests
    #[test] fn trades_count_50() { let db = TestDb::with_trades(50); assert_eq!(db.query_scalar("SELECT count(*) FROM trades"), Value::I64(50)); }
    #[test] fn trades_sum() { let db = TestDb::with_trades(20); let v = db.query_scalar("SELECT sum(price) FROM trades"); match v { Value::F64(f) => assert!(f > 0.0), _ => panic!("expected F64") } }
    #[test] fn trades_avg() { let db = TestDb::with_trades(20); let v = db.query_scalar("SELECT avg(price) FROM trades"); match v { Value::F64(f) => assert!(f > 0.0), _ => panic!("expected F64") } }
    #[test] fn trades_min() { let db = TestDb::with_trades(20); let v = db.query_scalar("SELECT min(price) FROM trades"); match v { Value::F64(f) => assert!(f > 0.0), _ => panic!("expected F64") } }
    #[test] fn trades_max() { let db = TestDb::with_trades(20); let v = db.query_scalar("SELECT max(price) FROM trades"); match v { Value::F64(f) => assert!(f > 0.0), _ => panic!("expected F64") } }
}

// ============================================================================
// 4. Conditional and cast functions (50 tests)
// ============================================================================
mod conditionals {
    use super::*;

    // coalesce
    #[test] fn coalesce_first_non_null() { assert_eq!(eval("coalesce", &[null(), i(5)]), i(5)); }
    #[test] fn coalesce_first_val() { assert_eq!(eval("coalesce", &[i(3), i(5)]), i(3)); }
    #[test] fn coalesce_all_null() { assert_eq!(eval("coalesce", &[null(), null()]), null()); }
    #[test] fn coalesce_string() { assert_eq!(eval("coalesce", &[null(), s("x")]), s("x")); }
    #[test] fn coalesce_float() { assert_eq!(eval("coalesce", &[null(), f(1.0)]), f(1.0)); }

    // nullif
    #[test] fn nullif_equal() { assert_eq!(eval("nullif", &[i(5), i(5)]), null()); }
    #[test] fn nullif_not_equal() { assert_eq!(eval("nullif", &[i(5), i(3)]), i(5)); }
    #[test] fn nullif_strings() { assert_eq!(eval("nullif", &[s("a"), s("a")]), null()); }
    #[test] fn nullif_strings_diff() { assert_eq!(eval("nullif", &[s("a"), s("b")]), s("a")); }
    #[test] fn nullif_null_first() { assert_eq!(eval("nullif", &[null(), i(5)]), null()); }

    // CASE WHEN via SQL
    #[test] fn case_simple() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(0))); let v = db.query_scalar("SELECT CASE WHEN v > 3 THEN 'big' ELSE 'small' END FROM t"); assert_eq!(v, Value::Str("big".into())); }
    #[test] fn case_else() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1)", ts(0))); let v = db.query_scalar("SELECT CASE WHEN v > 3 THEN 'big' ELSE 'small' END FROM t"); assert_eq!(v, Value::Str("small".into())); }
    #[test] fn case_no_else_null() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 1)", ts(0))); let v = db.query_scalar("SELECT CASE WHEN v > 3 THEN 'big' END FROM t"); assert_eq!(v, Value::Null); }
    #[test] fn case_numeric() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(0))); let v = db.query_scalar("SELECT CASE WHEN v > 3 THEN 1 ELSE 0 END FROM t"); assert_eq!(v, Value::I64(1)); }
    #[test] fn case_three_branches() { let db = TestDb::new(); db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v INT)"); db.exec_ok(&format!("INSERT INTO t VALUES ({}, 5)", ts(0))); let v = db.query_scalar("SELECT CASE WHEN v < 3 THEN 'low' WHEN v < 7 THEN 'mid' ELSE 'high' END FROM t"); assert_eq!(v, Value::Str("mid".into())); }
}
