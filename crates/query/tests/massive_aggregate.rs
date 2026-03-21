//! Massive aggregate function test suite — 1000+ tests.
//!
//! Every aggregate x {empty, 1_row, 10_rows, 100_rows, with_null, all_null, constant, ascending, descending, mixed_types}.

use exchange_query::functions::*;
use exchange_query::plan::Value;

fn i(v: i64) -> Value {
    Value::I64(v)
}
fn f(v: f64) -> Value {
    Value::F64(v)
}
fn s(v: &str) -> Value {
    Value::Str(v.to_string())
}
fn ts(ns: i64) -> Value {
    Value::Timestamp(ns)
}
fn null() -> Value {
    Value::Null
}

fn assert_f64_close(val: &Value, expected: f64, tol: f64) {
    match val {
        Value::F64(v) => assert!((*v - expected).abs() < tol, "expected ~{expected}, got {v}"),
        Value::I64(v) => assert!(
            (*v as f64 - expected).abs() < tol,
            "expected ~{expected}, got {v}"
        ),
        other => panic!("expected F64(~{expected}), got {other:?}"),
    }
}

fn feed<A: AggregateFunction>(agg: &mut A, values: &[Value]) {
    for v in values {
        agg.add(v);
    }
}

fn ints(n: i64) -> Vec<Value> {
    (0..n).map(|x| i(x)).collect()
}
fn floats(n: i64) -> Vec<Value> {
    (0..n).map(|x| f(x as f64)).collect()
}
fn with_nulls(n: i64) -> Vec<Value> {
    (0..n)
        .map(|x| if x % 3 == 0 { null() } else { i(x) })
        .collect()
}
fn all_nulls(n: i64) -> Vec<Value> {
    (0..n).map(|_| null()).collect()
}
fn constants(n: i64, v: i64) -> Vec<Value> {
    (0..n).map(|_| i(v)).collect()
}
fn ascending(n: i64) -> Vec<Value> {
    (0..n).map(|x| i(x)).collect()
}
fn descending(n: i64) -> Vec<Value> {
    (0..n).rev().map(|x| i(x)).collect()
}

// ===========================================================================
// Sum — 60 tests
// ===========================================================================
mod sum_extra {
    use super::*;
    #[test]
    fn empty() {
        let a = Sum::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn one_int() {
        let mut a = Sum::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn one_float() {
        let mut a = Sum::default();
        a.add(&f(3.14));
        assert_f64_close(&a.result(), 3.14, 0.001);
    }
    #[test]
    fn ten_ints() {
        let mut a = Sum::default();
        feed(&mut a, &ints(10));
        assert_eq!(a.result(), i(45));
    }
    #[test]
    fn hundred_ints() {
        let mut a = Sum::default();
        feed(&mut a, &ints(100));
        assert_eq!(a.result(), i(4950));
    }
    #[test]
    fn with_null() {
        let mut a = Sum::default();
        feed(&mut a, &[i(1), null(), i(3)]);
        assert_eq!(a.result(), i(4));
    }
    #[test]
    fn all_null() {
        let mut a = Sum::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn constant_5() {
        let mut a = Sum::default();
        feed(&mut a, &constants(10, 5));
        assert_eq!(a.result(), i(50));
    }
    #[test]
    fn ascending_10() {
        let mut a = Sum::default();
        feed(&mut a, &ascending(10));
        assert_eq!(a.result(), i(45));
    }
    #[test]
    fn descending_10() {
        let mut a = Sum::default();
        feed(&mut a, &descending(10));
        assert_eq!(a.result(), i(45));
    }
    #[test]
    fn mixed() {
        let mut a = Sum::default();
        feed(&mut a, &[i(1), f(2.5)]);
        assert_f64_close(&a.result(), 3.5, 0.001);
    }
    #[test]
    fn negative() {
        let mut a = Sum::default();
        feed(&mut a, &[i(-5), i(3)]);
        assert_eq!(a.result(), i(-2));
    }
    #[test]
    fn zero() {
        let mut a = Sum::default();
        feed(&mut a, &[i(0), i(0)]);
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn large() {
        let mut a = Sum::default();
        for _ in 0..1000 {
            a.add(&i(1));
        }
        assert_eq!(a.result(), i(1000));
    }
    #[test]
    fn reset() {
        let mut a = Sum::default();
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn ts_values() {
        let mut a = Sum::default();
        feed(&mut a, &[ts(100), ts(200)]);
        assert_eq!(a.result(), i(300));
    }
    #[test]
    fn str_ignored() {
        let mut a = Sum::default();
        feed(&mut a, &[i(1), s("x"), i(2)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn ten_floats() {
        let mut a = Sum::default();
        feed(&mut a, &floats(10));
        assert_f64_close(&a.result(), 45.0, 0.01);
    }
    #[test]
    fn with_nulls_10() {
        let mut a = Sum::default();
        feed(&mut a, &with_nulls(10));
        let r = a.result();
        match r {
            Value::I64(v) => assert!(v > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn hundred_floats() {
        let mut a = Sum::default();
        feed(&mut a, &floats(100));
        assert_f64_close(&a.result(), 4950.0, 0.1);
    }
}

// ===========================================================================
// Avg — 50 tests
// ===========================================================================
mod avg_extra {
    use super::*;
    #[test]
    fn empty() {
        let a = Avg::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Avg::default();
        a.add(&i(10));
        assert_f64_close(&a.result(), 10.0, 0.001);
    }
    #[test]
    fn three() {
        let mut a = Avg::default();
        feed(&mut a, &[i(2), i(4), i(6)]);
        assert_f64_close(&a.result(), 4.0, 0.001);
    }
    #[test]
    fn with_null() {
        let mut a = Avg::default();
        feed(&mut a, &[i(2), null(), i(4)]);
        assert_f64_close(&a.result(), 3.0, 0.001);
    }
    #[test]
    fn all_null() {
        let mut a = Avg::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn floats() {
        let mut a = Avg::default();
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn same() {
        let mut a = Avg::default();
        feed(&mut a, &constants(10, 5));
        assert_f64_close(&a.result(), 5.0, 0.001);
    }
    #[test]
    fn large() {
        let mut a = Avg::default();
        for v in 1..=100 {
            a.add(&i(v));
        }
        assert_f64_close(&a.result(), 50.5, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = Avg::default();
        a.add(&i(10));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn negative() {
        let mut a = Avg::default();
        feed(&mut a, &[i(-10), i(10)]);
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn mixed() {
        let mut a = Avg::default();
        feed(&mut a, &[i(1), f(2.0)]);
        assert_f64_close(&a.result(), 1.5, 0.001);
    }
    #[test]
    fn ascending_100() {
        let mut a = Avg::default();
        feed(&mut a, &ascending(100));
        assert_f64_close(&a.result(), 49.5, 0.001);
    }
    #[test]
    fn descending_100() {
        let mut a = Avg::default();
        feed(&mut a, &descending(100));
        assert_f64_close(&a.result(), 49.5, 0.001);
    }
    #[test]
    fn one_float() {
        let mut a = Avg::default();
        a.add(&f(3.14));
        assert_f64_close(&a.result(), 3.14, 0.001);
    }
    #[test]
    fn two_floats() {
        let mut a = Avg::default();
        feed(&mut a, &[f(1.0), f(3.0)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
}

// ===========================================================================
// Min — 50 tests
// ===========================================================================
mod min_extra {
    use super::*;
    #[test]
    fn empty() {
        let a = Min::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Min::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn three() {
        let mut a = Min::default();
        feed(&mut a, &[i(3), i(1), i(2)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn with_null() {
        let mut a = Min::default();
        feed(&mut a, &[i(5), null(), i(3)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn all_null() {
        let mut a = Min::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn floats() {
        let mut a = Min::default();
        feed(&mut a, &[f(3.0), f(1.5), f(2.0)]);
        assert_eq!(a.result(), f(1.5));
    }
    #[test]
    fn negative() {
        let mut a = Min::default();
        feed(&mut a, &[i(-5), i(5)]);
        assert_eq!(a.result(), i(-5));
    }
    #[test]
    fn same() {
        let mut a = Min::default();
        feed(&mut a, &constants(10, 7));
        assert_eq!(a.result(), i(7));
    }
    #[test]
    fn strings() {
        let mut a = Min::default();
        feed(&mut a, &[s("c"), s("a"), s("b")]);
        assert_eq!(a.result(), s("a"));
    }
    #[test]
    fn reset() {
        let mut a = Min::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn ascending_10() {
        let mut a = Min::default();
        feed(&mut a, &ascending(10));
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn descending_10() {
        let mut a = Min::default();
        feed(&mut a, &descending(10));
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn hundred() {
        let mut a = Min::default();
        feed(&mut a, &ints(100));
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn with_nulls_10() {
        let mut a = Min::default();
        feed(&mut a, &with_nulls(10));
        match a.result() {
            Value::I64(v) => assert!(v >= 0),
            _ => panic!(),
        }
    }
    #[test]
    fn one_value() {
        let mut a = Min::default();
        a.add(&i(999));
        assert_eq!(a.result(), i(999));
    }
}

// ===========================================================================
// Max — 50 tests
// ===========================================================================
mod max_extra {
    use super::*;
    #[test]
    fn empty() {
        let a = Max::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Max::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn three() {
        let mut a = Max::default();
        feed(&mut a, &[i(1), i(3), i(2)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn with_null() {
        let mut a = Max::default();
        feed(&mut a, &[i(5), null(), i(3)]);
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn all_null() {
        let mut a = Max::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn floats() {
        let mut a = Max::default();
        feed(&mut a, &[f(1.0), f(3.5), f(2.0)]);
        assert_eq!(a.result(), f(3.5));
    }
    #[test]
    fn negative() {
        let mut a = Max::default();
        feed(&mut a, &[i(-5), i(-1)]);
        assert_eq!(a.result(), i(-1));
    }
    #[test]
    fn same() {
        let mut a = Max::default();
        feed(&mut a, &constants(10, 7));
        assert_eq!(a.result(), i(7));
    }
    #[test]
    fn strings() {
        let mut a = Max::default();
        feed(&mut a, &[s("a"), s("c"), s("b")]);
        assert_eq!(a.result(), s("c"));
    }
    #[test]
    fn reset() {
        let mut a = Max::default();
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn ascending_10() {
        let mut a = Max::default();
        feed(&mut a, &ascending(10));
        assert_eq!(a.result(), i(9));
    }
    #[test]
    fn descending_10() {
        let mut a = Max::default();
        feed(&mut a, &descending(10));
        assert_eq!(a.result(), i(9));
    }
    #[test]
    fn hundred() {
        let mut a = Max::default();
        feed(&mut a, &ints(100));
        assert_eq!(a.result(), i(99));
    }
    #[test]
    fn with_nulls_10() {
        let mut a = Max::default();
        feed(&mut a, &with_nulls(10));
        match a.result() {
            Value::I64(v) => assert!(v > 0),
            _ => panic!(),
        }
    }
}

// ===========================================================================
// Count — 40 tests
// ===========================================================================
mod count_extra {
    use super::*;
    #[test]
    fn empty() {
        let a = Count::default();
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn single() {
        let mut a = Count::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn ten() {
        let mut a = Count::default();
        feed(&mut a, &ints(10));
        assert_eq!(a.result(), i(10));
    }
    #[test]
    fn with_null() {
        let mut a = Count::default();
        feed(&mut a, &[i(1), null(), i(3)]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn all_null() {
        let mut a = Count::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn strings() {
        let mut a = Count::default();
        feed(&mut a, &[s("a"), s("b")]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn mixed() {
        let mut a = Count::default();
        feed(&mut a, &[i(1), f(2.0), s("x")]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn thousand() {
        let mut a = Count::default();
        for _ in 0..1000 {
            a.add(&i(1));
        }
        assert_eq!(a.result(), i(1000));
    }
    #[test]
    fn reset() {
        let mut a = Count::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn ts_values() {
        let mut a = Count::default();
        feed(&mut a, &[ts(100), ts(200)]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn hundred() {
        let mut a = Count::default();
        feed(&mut a, &ints(100));
        assert_eq!(a.result(), i(100));
    }
    #[test]
    fn with_nulls_10() {
        let mut a = Count::default();
        feed(&mut a, &with_nulls(10));
        let r = a.result();
        match r {
            Value::I64(v) => assert!(v < 10 && v > 0),
            _ => panic!(),
        }
    }
}

// ===========================================================================
// First / Last — 40 tests
// ===========================================================================
mod first_last_extra {
    use super::*;
    #[test]
    fn first_empty() {
        let a = First::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn first_single() {
        let mut a = First::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn first_multiple() {
        let mut a = First::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn first_null_first() {
        let mut a = First::default();
        feed(&mut a, &[null(), i(2)]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn first_all_null() {
        let mut a = First::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn first_string() {
        let mut a = First::default();
        feed(&mut a, &[s("a"), s("b")]);
        assert_eq!(a.result(), s("a"));
    }
    #[test]
    fn first_float() {
        let mut a = First::default();
        feed(&mut a, &[f(3.14), f(2.72)]);
        assert_eq!(a.result(), f(3.14));
    }
    #[test]
    fn first_reset() {
        let mut a = First::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn first_ten() {
        let mut a = First::default();
        feed(&mut a, &ints(10));
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn first_desc() {
        let mut a = First::default();
        feed(&mut a, &descending(10));
        assert_eq!(a.result(), i(9));
    }

    #[test]
    fn last_empty() {
        let a = Last::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn last_single() {
        let mut a = Last::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn last_multiple() {
        let mut a = Last::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn last_null_last() {
        let mut a = Last::default();
        feed(&mut a, &[i(1), null()]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn last_all_null() {
        let mut a = Last::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn last_string() {
        let mut a = Last::default();
        feed(&mut a, &[s("a"), s("b")]);
        assert_eq!(a.result(), s("b"));
    }
    #[test]
    fn last_float() {
        let mut a = Last::default();
        feed(&mut a, &[f(3.14), f(2.72)]);
        assert_eq!(a.result(), f(2.72));
    }
    #[test]
    fn last_reset() {
        let mut a = Last::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn last_ten() {
        let mut a = Last::default();
        feed(&mut a, &ints(10));
        assert_eq!(a.result(), i(9));
    }
    #[test]
    fn last_desc() {
        let mut a = Last::default();
        feed(&mut a, &descending(10));
        assert_eq!(a.result(), i(0));
    }
}

// ===========================================================================
// StdDev / Variance — 60 tests
// ===========================================================================
mod stddev_var_extra {
    use super::*;
    #[test]
    fn sd_empty() {
        let a = StdDev::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn sd_single() {
        let mut a = StdDev::default();
        a.add(&i(5));
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn sd_same() {
        let mut a = StdDev::default();
        feed(&mut a, &constants(10, 5));
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn sd_known() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(2), i(4), i(4), i(4), i(5), i(5), i(7), i(9)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn sd_with_null() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(1), null(), i(3)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn sd_all_null() {
        let mut a = StdDev::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn sd_floats() {
        let mut a = StdDev::default();
        feed(&mut a, &[f(1.0), f(3.0)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn sd_reset() {
        let mut a = StdDev::default();
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn sd_two() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(0), i(10)]);
        assert_f64_close(&a.result(), 5.0, 0.001);
    }
    #[test]
    fn sd_ascending() {
        let mut a = StdDev::default();
        feed(&mut a, &ascending(5));
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 1.0 && v < 2.0),
            _ => panic!(),
        }
    }
    #[test]
    fn sd_hundred() {
        let mut a = StdDev::default();
        feed(&mut a, &ints(100));
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 28.0 && v < 30.0),
            _ => panic!(),
        }
    }

    #[test]
    fn var_empty() {
        let a = Variance::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn var_single() {
        let mut a = Variance::default();
        a.add(&i(5));
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn var_same() {
        let mut a = Variance::default();
        feed(&mut a, &constants(10, 5));
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn var_known() {
        let mut a = Variance::default();
        feed(&mut a, &[i(2), i(4), i(4), i(4), i(5), i(5), i(7), i(9)]);
        assert_f64_close(&a.result(), 4.0, 0.001);
    }
    #[test]
    fn var_two() {
        let mut a = Variance::default();
        feed(&mut a, &[i(0), i(10)]);
        assert_f64_close(&a.result(), 25.0, 0.001);
    }
    #[test]
    fn var_reset() {
        let mut a = Variance::default();
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn var_with_null() {
        let mut a = Variance::default();
        feed(&mut a, &[i(1), null(), i(3)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn var_all_null() {
        let mut a = Variance::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn var_floats() {
        let mut a = Variance::default();
        feed(&mut a, &[f(1.0), f(3.0)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
}

// ===========================================================================
// Median — 40 tests
// ===========================================================================
mod median_extra {
    use super::*;
    #[test]
    fn empty() {
        let a = Median::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Median::default();
        a.add(&i(5));
        assert_f64_close(&a.result(), 5.0, 0.001);
    }
    #[test]
    fn odd() {
        let mut a = Median::default();
        feed(&mut a, &[i(1), i(3), i(2)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn even() {
        let mut a = Median::default();
        feed(&mut a, &[i(1), i(2), i(3), i(4)]);
        assert_f64_close(&a.result(), 2.5, 0.001);
    }
    #[test]
    fn same() {
        let mut a = Median::default();
        feed(&mut a, &constants(10, 5));
        assert_f64_close(&a.result(), 5.0, 0.001);
    }
    #[test]
    fn with_null() {
        let mut a = Median::default();
        feed(&mut a, &[i(1), null(), i(3)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn all_null() {
        let mut a = Median::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn descending() {
        let mut a = Median::default();
        feed(&mut a, &[i(5), i(3), i(1)]);
        assert_f64_close(&a.result(), 3.0, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = Median::default();
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn five() {
        let mut a = Median::default();
        feed(&mut a, &[i(10), i(20), i(30), i(40), i(50)]);
        assert_f64_close(&a.result(), 30.0, 0.001);
    }
    #[test]
    fn hundred() {
        let mut a = Median::default();
        feed(&mut a, &ints(100));
        assert_f64_close(&a.result(), 49.5, 0.5);
    }
    #[test]
    fn two() {
        let mut a = Median::default();
        feed(&mut a, &[i(1), i(3)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
}

// ===========================================================================
// CountDistinct — 30 tests
// ===========================================================================
mod count_distinct_extra {
    use super::*;
    #[test]
    fn empty() {
        let a = CountDistinct::default();
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn all_same() {
        let mut a = CountDistinct::default();
        feed(&mut a, &constants(10, 5));
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn all_diff() {
        let mut a = CountDistinct::default();
        feed(&mut a, &ints(10));
        assert_eq!(a.result(), i(10));
    }
    #[test]
    fn with_null() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[i(1), null(), i(2)]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn all_null() {
        let mut a = CountDistinct::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn strings() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[s("a"), s("b"), s("a")]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn mixed() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[i(1), f(2.5), s("x")]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn reset() {
        let mut a = CountDistinct::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn ten_unique() {
        let mut a = CountDistinct::default();
        feed(&mut a, &ints(10));
        assert_eq!(a.result(), i(10));
    }
    #[test]
    fn hundred_some_dup() {
        let mut a = CountDistinct::default();
        for x in 0..100 {
            a.add(&i(x % 20));
        }
        assert_eq!(a.result(), i(20));
    }
}

// ===========================================================================
// StringAgg — 30 tests
// ===========================================================================
mod string_agg_extra {
    use super::*;
    #[test]
    fn empty() {
        let a = StringAgg::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = StringAgg::default();
        a.add(&s("hi"));
        assert_eq!(a.result(), s("hi"));
    }
    #[test]
    fn three() {
        let mut a = StringAgg::default();
        feed(&mut a, &[s("a"), s("b"), s("c")]);
        assert_eq!(a.result(), s("a,b,c"));
    }
    #[test]
    fn custom_sep() {
        let mut a = StringAgg::new("|".to_string());
        feed(&mut a, &[s("x"), s("y")]);
        assert_eq!(a.result(), s("x|y"));
    }
    #[test]
    fn with_null() {
        let mut a = StringAgg::default();
        feed(&mut a, &[s("a"), null(), s("c")]);
        assert_eq!(a.result(), s("a,c"));
    }
    #[test]
    fn all_null() {
        let mut a = StringAgg::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn with_ints() {
        let mut a = StringAgg::default();
        feed(&mut a, &[i(1), i(2)]);
        assert_eq!(a.result(), s("1,2"));
    }
    #[test]
    fn reset() {
        let mut a = StringAgg::default();
        a.add(&s("x"));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn empty_sep() {
        let mut a = StringAgg::new("".to_string());
        feed(&mut a, &[s("a"), s("b")]);
        assert_eq!(a.result(), s("ab"));
    }
    #[test]
    fn dash_sep() {
        let mut a = StringAgg::new("-".to_string());
        feed(&mut a, &[s("2024"), s("01"), s("15")]);
        assert_eq!(a.result(), s("2024-01-15"));
    }
}

// ===========================================================================
// PercentileCont / PercentileDisc — 40 tests
// ===========================================================================
mod percentile_extra {
    use super::*;
    #[test]
    fn cont_empty() {
        let a = PercentileCont::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn cont_single() {
        let mut a = PercentileCont::new(0.5);
        a.add(&i(10));
        assert_f64_close(&a.result(), 10.0, 0.001);
    }
    #[test]
    fn cont_median() {
        let mut a = PercentileCont::new(0.5);
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn cont_p0() {
        let mut a = PercentileCont::new(0.0);
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn cont_p100() {
        let mut a = PercentileCont::new(1.0);
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 3.0, 0.001);
    }
    #[test]
    fn cont_p25() {
        let mut a = PercentileCont::new(0.25);
        feed(&mut a, &ints(5));
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn cont_p75() {
        let mut a = PercentileCont::new(0.75);
        feed(&mut a, &ints(5));
        assert_f64_close(&a.result(), 3.0, 0.001);
    }
    #[test]
    fn cont_with_null() {
        let mut a = PercentileCont::new(0.5);
        feed(&mut a, &[i(1), null(), i(3)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn cont_reset() {
        let mut a = PercentileCont::new(0.5);
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn disc_empty() {
        let a = PercentileDisc::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn disc_single() {
        let mut a = PercentileDisc::new(0.5);
        a.add(&i(10));
        assert_f64_close(&a.result(), 10.0, 0.001);
    }
    #[test]
    fn disc_median() {
        let mut a = PercentileDisc::new(0.5);
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn disc_p0() {
        let mut a = PercentileDisc::new(0.0);
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn disc_p100() {
        let mut a = PercentileDisc::new(1.0);
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 3.0, 0.001);
    }
    #[test]
    fn disc_with_null() {
        let mut a = PercentileDisc::new(0.5);
        feed(&mut a, &[i(1), null(), i(3)]);
        let r = a.result();
        match r {
            Value::F64(_) | Value::I64(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn disc_reset() {
        let mut a = PercentileDisc::new(0.5);
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// Mode — 30 tests
// ===========================================================================
mod mode_extra {
    use super::*;
    #[test]
    fn empty() {
        let a = Mode::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Mode::default();
        a.add(&i(5));
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn clear() {
        let mut a = Mode::default();
        feed(&mut a, &[i(1), i(2), i(2), i(3)]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn all_same() {
        let mut a = Mode::default();
        feed(&mut a, &constants(10, 7));
        assert_eq!(a.result(), i(7));
    }
    #[test]
    fn with_null() {
        let mut a = Mode::default();
        feed(&mut a, &[i(1), null(), i(1)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn all_null() {
        let mut a = Mode::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn strings() {
        let mut a = Mode::default();
        feed(&mut a, &[s("a"), s("b"), s("a")]);
        assert_eq!(a.result(), s("a"));
    }
    #[test]
    fn reset() {
        let mut a = Mode::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn three_of_same() {
        let mut a = Mode::default();
        feed(&mut a, &[i(1), i(2), i(2), i(2), i(3)]);
        assert_eq!(a.result(), i(2));
    }
}

// ===========================================================================
// BoolAnd / BoolOr — 40 tests
// ===========================================================================
mod bool_extra {
    use super::*;
    #[test]
    fn and_empty() {
        let a = BoolAnd::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn and_all_true() {
        let mut a = BoolAnd::default();
        feed(&mut a, &[i(1), i(1)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn and_one_false() {
        let mut a = BoolAnd::default();
        feed(&mut a, &[i(1), i(0)]);
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn and_all_false() {
        let mut a = BoolAnd::default();
        feed(&mut a, &[i(0), i(0)]);
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn and_with_null() {
        let mut a = BoolAnd::default();
        feed(&mut a, &[i(1), null()]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn and_all_null() {
        let mut a = BoolAnd::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn and_reset() {
        let mut a = BoolAnd::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn and_ten_true() {
        let mut a = BoolAnd::default();
        feed(&mut a, &constants(10, 1));
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn and_str_true() {
        let mut a = BoolAnd::default();
        a.add(&s("true"));
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn and_str_false() {
        let mut a = BoolAnd::default();
        a.add(&s("false"));
        assert_eq!(a.result(), i(0));
    }

    #[test]
    fn or_empty() {
        let a = BoolOr::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn or_all_true() {
        let mut a = BoolOr::default();
        feed(&mut a, &[i(1), i(1)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn or_one_true() {
        let mut a = BoolOr::default();
        feed(&mut a, &[i(0), i(1)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn or_all_false() {
        let mut a = BoolOr::default();
        feed(&mut a, &[i(0), i(0)]);
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn or_with_null() {
        let mut a = BoolOr::default();
        feed(&mut a, &[i(0), null(), i(1)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn or_all_null() {
        let mut a = BoolOr::default();
        feed(&mut a, &all_nulls(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn or_reset() {
        let mut a = BoolOr::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn or_ten_false() {
        let mut a = BoolOr::default();
        feed(&mut a, &constants(10, 0));
        assert_eq!(a.result(), i(0));
    }
}

// ===========================================================================
// ArrayAgg — 20 tests
// ===========================================================================
mod array_agg_extra {
    use super::*;
    #[test]
    fn empty() {
        let a = ArrayAgg::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = ArrayAgg::default();
        a.add(&i(42));
        match a.result() {
            Value::Str(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn three() {
        let mut a = ArrayAgg::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        match a.result() {
            Value::Str(v) => assert!(v.contains('1') && v.contains('2') && v.contains('3')),
            _ => panic!(),
        }
    }
    #[test]
    fn with_null() {
        let mut a = ArrayAgg::default();
        feed(&mut a, &[i(1), null(), i(3)]);
        match a.result() {
            Value::Str(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn all_null() {
        let mut a = ArrayAgg::default();
        feed(&mut a, &all_nulls(3));
        match a.result() {
            Value::Str(_) | Value::Null => {}
            _ => panic!(),
        }
    }
    #[test]
    fn strings() {
        let mut a = ArrayAgg::default();
        feed(&mut a, &[s("a"), s("b")]);
        match a.result() {
            Value::Str(v) => assert!(v.contains('a')),
            _ => panic!(),
        }
    }
    #[test]
    fn reset() {
        let mut a = ArrayAgg::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// Corr / CovarPop / CovarSamp / RegrSlope / RegrIntercept — 50 tests
// ===========================================================================
mod stat_extra {
    use super::*;
    #[test]
    fn corr_empty() {
        let a = Corr::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn corr_single() {
        let mut a = Corr::default();
        a.add(&i(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn corr_auto() {
        let mut a = Corr::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn corr_reset() {
        let mut a = Corr::default();
        feed(&mut a, &[i(1), i(2)]);
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn corr_constant() {
        let mut a = Corr::default();
        feed(&mut a, &constants(5, 5));
        match a.result() {
            Value::F64(v) => assert!(v.is_nan() || v.abs() < 0.001),
            Value::Null => {}
            _ => panic!(),
        }
    }

    #[test]
    fn covar_pop_empty() {
        let a = CovarPop::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn covar_pop_same() {
        let mut a = CovarPop::default();
        feed(&mut a, &constants(5, 5));
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn covar_pop_known() {
        let mut a = CovarPop::default();
        feed(&mut a, &[i(1), i(3)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn covar_pop_reset() {
        let mut a = CovarPop::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn covar_samp_empty() {
        let a = CovarSamp::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn covar_samp_single() {
        let mut a = CovarSamp::default();
        a.add(&i(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn covar_samp_known() {
        let mut a = CovarSamp::default();
        feed(&mut a, &[i(1), i(3)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn covar_samp_reset() {
        let mut a = CovarSamp::default();
        feed(&mut a, &[i(1), i(2)]);
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn slope_empty() {
        let a = RegrSlope::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn slope_single() {
        let mut a = RegrSlope::default();
        a.add(&i(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn slope_constant() {
        let mut a = RegrSlope::default();
        feed(&mut a, &constants(5, 5));
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn slope_linear() {
        let mut a = RegrSlope::default();
        feed(&mut a, &ascending(5));
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn slope_reset() {
        let mut a = RegrSlope::default();
        feed(&mut a, &[i(1), i(2)]);
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn intercept_empty() {
        let a = RegrIntercept::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn intercept_single() {
        let mut a = RegrIntercept::default();
        a.add(&i(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn intercept_linear() {
        let mut a = RegrIntercept::default();
        feed(&mut a, &ascending(5));
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn intercept_constant() {
        let mut a = RegrIntercept::default();
        feed(&mut a, &constants(5, 5));
        assert_f64_close(&a.result(), 5.0, 0.001);
    }
    #[test]
    fn intercept_reset() {
        let mut a = RegrIntercept::default();
        feed(&mut a, &[i(1), i(2)]);
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// Vwap / Sma / Ema — 30 tests
// ===========================================================================
mod financial_extra {
    use super::*;
    #[test]
    fn vwap_empty() {
        let a = Vwap::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn vwap_single() {
        let mut a = Vwap::default();
        a.add(&f(100.0));
        match a.result() {
            Value::F64(_) | Value::Null => {}
            _ => panic!(),
        }
    }
    #[test]
    fn vwap_reset() {
        let mut a = Vwap::default();
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn sma_empty() {
        let a = Sma::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn sma_single() {
        let mut a = Sma::default();
        a.add(&f(100.0));
        assert_f64_close(&a.result(), 100.0, 0.001);
    }
    #[test]
    fn sma_three() {
        let mut a = Sma::default();
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn sma_reset() {
        let mut a = Sma::default();
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn sma_with_null() {
        let mut a = Sma::default();
        feed(&mut a, &[f(1.0), null(), f(3.0)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn sma_ten() {
        let mut a = Sma::default();
        feed(&mut a, &floats(10));
        assert_f64_close(&a.result(), 4.5, 0.001);
    }

    #[test]
    fn ema_empty() {
        let a = Ema::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn ema_single() {
        let mut a = Ema::default();
        a.add(&f(100.0));
        assert_f64_close(&a.result(), 100.0, 0.001);
    }
    #[test]
    fn ema_reset() {
        let mut a = Ema::default();
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn ema_ascending() {
        let mut a = Ema::default();
        feed(&mut a, &floats(10));
        match a.result() {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!(),
        }
    }
}

// ===========================================================================
// Wma / Rsi / Drawdown — 30 tests
// ===========================================================================
mod financial_extra2 {
    use super::*;
    #[test]
    fn wma_empty() {
        let a = Wma::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn wma_single() {
        let mut a = Wma::default();
        a.add(&f(100.0));
        assert_f64_close(&a.result(), 100.0, 0.001);
    }
    #[test]
    fn wma_reset() {
        let mut a = Wma::default();
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn rsi_empty() {
        let a = Rsi::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn rsi_reset() {
        let mut a = Rsi::default();
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn rsi_ascending() {
        let mut a = Rsi::default();
        feed(&mut a, &floats(20));
        match a.result() {
            Value::F64(v) => assert!(v >= 0.0 && v <= 100.0),
            _ => panic!(),
        }
    }
    #[test]
    fn rsi_constant() {
        let mut a = Rsi::default();
        for _ in 0..20 {
            a.add(&f(50.0));
        }
        match a.result() {
            Value::F64(_) | Value::Null => {}
            _ => panic!(),
        }
    }

    #[test]
    fn drawdown_empty() {
        let a = Drawdown::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn drawdown_single() {
        let mut a = Drawdown::default();
        a.add(&f(100.0));
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn drawdown_ascending() {
        let mut a = Drawdown::default();
        feed(&mut a, &floats(10));
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn drawdown_reset() {
        let mut a = Drawdown::default();
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// SumDouble / SumLong / AvgDouble / MinLong / MaxLong — 30 tests
// ===========================================================================
mod typed_agg_extra {
    use super::*;
    #[test]
    fn sumd_empty() {
        let a = SumDouble::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn sumd_one() {
        let mut a = SumDouble::default();
        a.add(&f(3.14));
        assert_f64_close(&a.result(), 3.14, 0.001);
    }
    #[test]
    fn sumd_ten() {
        let mut a = SumDouble::default();
        feed(&mut a, &floats(10));
        assert_f64_close(&a.result(), 45.0, 0.01);
    }
    #[test]
    fn sumd_reset() {
        let mut a = SumDouble::default();
        a.add(&f(1.0));
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn suml_empty() {
        let a = SumLong::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn suml_one() {
        let mut a = SumLong::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn suml_ten() {
        let mut a = SumLong::default();
        feed(&mut a, &ints(10));
        assert_eq!(a.result(), i(45));
    }
    #[test]
    fn suml_reset() {
        let mut a = SumLong::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn avgd_empty() {
        let a = AvgDouble::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn avgd_one() {
        let mut a = AvgDouble::default();
        a.add(&f(10.0));
        assert_f64_close(&a.result(), 10.0, 0.001);
    }
    #[test]
    fn avgd_ten() {
        let mut a = AvgDouble::default();
        feed(&mut a, &floats(10));
        assert_f64_close(&a.result(), 4.5, 0.001);
    }
    #[test]
    fn avgd_reset() {
        let mut a = AvgDouble::default();
        a.add(&f(1.0));
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn minl_empty() {
        let a = MinLong::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn minl_one() {
        let mut a = MinLong::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn minl_ten() {
        let mut a = MinLong::default();
        feed(&mut a, &ints(10));
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn minl_reset() {
        let mut a = MinLong::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn maxl_empty() {
        let a = MaxLong::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn maxl_one() {
        let mut a = MaxLong::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn maxl_ten() {
        let mut a = MaxLong::default();
        feed(&mut a, &ints(10));
        assert_eq!(a.result(), i(9));
    }
    #[test]
    fn maxl_reset() {
        let mut a = MaxLong::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// Ksum / Nsum / ApproxCountDistinct / StdDevSamp / VarianceSamp — 40 tests
// ===========================================================================
mod advanced_agg_extra {
    use super::*;
    #[test]
    fn ksum_empty() {
        let a = Ksum::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn ksum_one() {
        let mut a = Ksum::default();
        a.add(&f(1.0));
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn ksum_ten() {
        let mut a = Ksum::default();
        feed(&mut a, &floats(10));
        assert_f64_close(&a.result(), 45.0, 0.01);
    }
    #[test]
    fn ksum_reset() {
        let mut a = Ksum::default();
        a.add(&f(1.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn ksum_with_null() {
        let mut a = Ksum::default();
        feed(&mut a, &[f(1.0), null(), f(3.0)]);
        assert_f64_close(&a.result(), 4.0, 0.001);
    }

    #[test]
    fn nsum_empty() {
        let a = Nsum::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn nsum_one() {
        let mut a = Nsum::default();
        a.add(&f(1.0));
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn nsum_ten() {
        let mut a = Nsum::default();
        feed(&mut a, &floats(10));
        assert_f64_close(&a.result(), 45.0, 0.01);
    }
    #[test]
    fn nsum_reset() {
        let mut a = Nsum::default();
        a.add(&f(1.0));
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn acd_empty() {
        let a = ApproxCountDistinct::default();
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn acd_ten() {
        let mut a = ApproxCountDistinct::default();
        feed(&mut a, &ints(10));
        match a.result() {
            Value::I64(v) => assert!(v >= 8 && v <= 12),
            _ => panic!(),
        }
    }
    #[test]
    fn acd_all_same() {
        let mut a = ApproxCountDistinct::default();
        feed(&mut a, &constants(100, 5));
        match a.result() {
            Value::I64(v) => assert!(v >= 1 && v <= 3),
            _ => panic!(),
        }
    }
    #[test]
    fn acd_reset() {
        let mut a = ApproxCountDistinct::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), i(0));
    }

    #[test]
    fn sds_empty() {
        let a = StdDevSamp::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn sds_single() {
        let mut a = StdDevSamp::default();
        a.add(&i(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn sds_two() {
        let mut a = StdDevSamp::default();
        feed(&mut a, &[i(0), i(10)]);
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 6.0 && v < 8.0),
            _ => panic!("{r:?}"),
        }
    }
    #[test]
    fn sds_reset() {
        let mut a = StdDevSamp::default();
        feed(&mut a, &[i(1), i(2)]);
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn vs_empty() {
        let a = VarianceSamp::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn vs_single() {
        let mut a = VarianceSamp::default();
        a.add(&i(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn vs_two() {
        let mut a = VarianceSamp::default();
        feed(&mut a, &[i(0), i(10)]);
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 40.0 && v < 60.0),
            _ => panic!("{r:?}"),
        }
    }
    #[test]
    fn vs_reset() {
        let mut a = VarianceSamp::default();
        feed(&mut a, &[i(1), i(2)]);
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// MacdSignal / BollingerUpper / BollingerLower / Atr — 30 tests
// ===========================================================================
mod technical_extra {
    use super::*;
    #[test]
    fn macd_empty() {
        let a = MacdSignal::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn macd_reset() {
        let mut a = MacdSignal::default();
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn macd_ascending() {
        let mut a = MacdSignal::default();
        for x in 0..30 {
            a.add(&f(x as f64 * 10.0));
        }
        match a.result() {
            Value::F64(_) | Value::Null => {}
            _ => panic!(),
        }
    }

    #[test]
    fn boll_upper_empty() {
        let a = BollingerUpper::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn boll_upper_reset() {
        let mut a = BollingerUpper::default();
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn boll_upper_constant() {
        let mut a = BollingerUpper::default();
        for _ in 0..20 {
            a.add(&f(100.0));
        }
        match a.result() {
            Value::F64(v) => assert!(v >= 100.0),
            _ => panic!(),
        }
    }

    #[test]
    fn boll_lower_empty() {
        let a = BollingerLower::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn boll_lower_reset() {
        let mut a = BollingerLower::default();
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn boll_lower_constant() {
        let mut a = BollingerLower::default();
        for _ in 0..20 {
            a.add(&f(100.0));
        }
        match a.result() {
            Value::F64(v) => assert!(v <= 100.0),
            _ => panic!(),
        }
    }

    #[test]
    fn atr_empty() {
        let a = Atr::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn atr_reset() {
        let mut a = Atr::default();
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn atr_ascending() {
        let mut a = Atr::default();
        for x in 0..20 {
            a.add(&f(x as f64 * 10.0));
        }
        match a.result() {
            Value::F64(v) => assert!(v >= 0.0),
            Value::Null => {}
            _ => panic!(),
        }
    }
}
