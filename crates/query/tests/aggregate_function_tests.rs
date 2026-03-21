//! Comprehensive aggregate function tests for ExchangeDB.
//! 500+ test cases covering every aggregate function via the AggregateFunction trait.

use exchange_query::functions::*;
use exchange_query::plan::Value;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
        other => panic!("expected F64(~{expected}), got {other:?}"),
    }
}

fn feed<A: AggregateFunction>(agg: &mut A, values: &[Value]) {
    for v in values {
        agg.add(v);
    }
}

// ===========================================================================
// Sum
// ===========================================================================
mod sum_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = Sum::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single_int() {
        let mut a = Sum::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn multiple_ints() {
        let mut a = Sum::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_eq!(a.result(), i(6));
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
        feed(&mut a, &[null(), null()]);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn floats() {
        let mut a = Sum::default();
        feed(&mut a, &[f(1.5), f(2.5)]);
        assert_f64_close(&a.result(), 4.0, 0.001);
    }
    #[test]
    fn mixed_int_float() {
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
    fn strings_ignored() {
        let mut a = Sum::default();
        feed(&mut a, &[i(1), s("hello"), i(2)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn timestamps() {
        let mut a = Sum::default();
        feed(&mut a, &[ts(100), ts(200)]);
        assert_eq!(a.result(), i(300));
    }
    #[test]
    fn ascending() {
        let mut a = Sum::default();
        feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]);
        assert_eq!(a.result(), i(15));
    }
    #[test]
    fn descending() {
        let mut a = Sum::default();
        feed(&mut a, &[i(5), i(4), i(3), i(2), i(1)]);
        assert_eq!(a.result(), i(15));
    }
}

// ===========================================================================
// Avg
// ===========================================================================
mod avg_tests {
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
    fn multiple() {
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
        feed(&mut a, &[null(), null()]);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn floats() {
        let mut a = Avg::default();
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn same_values() {
        let mut a = Avg::default();
        feed(&mut a, &[i(5), i(5), i(5)]);
        assert_f64_close(&a.result(), 5.0, 0.001);
    }
    #[test]
    fn large_set() {
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
}

// ===========================================================================
// Min
// ===========================================================================
mod min_tests {
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
    fn multiple() {
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
        feed(&mut a, &[null(), null()]);
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
        feed(&mut a, &[i(7), i(7), i(7)]);
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
    fn ascending() {
        let mut a = Min::default();
        feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn descending() {
        let mut a = Min::default();
        feed(&mut a, &[i(5), i(4), i(3), i(2), i(1)]);
        assert_eq!(a.result(), i(1));
    }
}

// ===========================================================================
// Max
// ===========================================================================
mod max_tests {
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
    fn multiple() {
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
        feed(&mut a, &[null(), null()]);
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
        feed(&mut a, &[i(7), i(7), i(7)]);
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
    fn ascending() {
        let mut a = Max::default();
        feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]);
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn descending() {
        let mut a = Max::default();
        feed(&mut a, &[i(5), i(4), i(3), i(2), i(1)]);
        assert_eq!(a.result(), i(5));
    }
}

// ===========================================================================
// Count
// ===========================================================================
mod count_tests {
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
    fn multiple() {
        let mut a = Count::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_eq!(a.result(), i(3));
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
        feed(&mut a, &[null(), null()]);
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
    fn timestamps() {
        let mut a = Count::default();
        feed(&mut a, &[ts(100), ts(200)]);
        assert_eq!(a.result(), i(2));
    }
}

// ===========================================================================
// First
// ===========================================================================
mod first_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = First::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = First::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn multiple() {
        let mut a = First::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn null_first() {
        let mut a = First::default();
        feed(&mut a, &[null(), i(2), i(3)]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn all_null() {
        let mut a = First::default();
        feed(&mut a, &[null(), null()]);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn string() {
        let mut a = First::default();
        feed(&mut a, &[s("hello"), s("world")]);
        assert_eq!(a.result(), s("hello"));
    }
    #[test]
    fn reset() {
        let mut a = First::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn float() {
        let mut a = First::default();
        feed(&mut a, &[f(3.15), f(2.72)]);
        assert_eq!(a.result(), f(3.15));
    }
}

// ===========================================================================
// Last
// ===========================================================================
mod last_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = Last::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Last::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn multiple() {
        let mut a = Last::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn null_last() {
        let mut a = Last::default();
        feed(&mut a, &[i(1), i(2), null()]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn all_null() {
        let mut a = Last::default();
        feed(&mut a, &[null(), null()]);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn string() {
        let mut a = Last::default();
        feed(&mut a, &[s("hello"), s("world")]);
        assert_eq!(a.result(), s("world"));
    }
    #[test]
    fn reset() {
        let mut a = Last::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn float() {
        let mut a = Last::default();
        feed(&mut a, &[f(3.15), f(2.72)]);
        assert_eq!(a.result(), f(2.72));
    }
}

// ===========================================================================
// StdDev
// ===========================================================================
mod stddev_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = StdDev::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = StdDev::default();
        a.add(&i(5));
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn same_values() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(5), i(5), i(5)]);
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn known() {
        // stddev_pop([2, 4, 4, 4, 5, 5, 7, 9]) = 2.0
        let mut a = StdDev::default();
        feed(&mut a, &[i(2), i(4), i(4), i(4), i(5), i(5), i(7), i(9)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn with_null() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(1), null(), i(3)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn floats() {
        let mut a = StdDev::default();
        feed(&mut a, &[f(1.0), f(3.0)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = StdDev::default();
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn two_values() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(0), i(10)]);
        assert_f64_close(&a.result(), 5.0, 0.001);
    }
    #[test]
    fn ascending() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]);
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 1.0 && v < 2.0),
            _ => panic!(),
        }
    }
}

// ===========================================================================
// Variance
// ===========================================================================
mod variance_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = Variance::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Variance::default();
        a.add(&i(5));
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn same() {
        let mut a = Variance::default();
        feed(&mut a, &[i(5), i(5), i(5)]);
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn known() {
        let mut a = Variance::default();
        feed(&mut a, &[i(2), i(4), i(4), i(4), i(5), i(5), i(7), i(9)]);
        assert_f64_close(&a.result(), 4.0, 0.001);
    }
    #[test]
    fn two_values() {
        let mut a = Variance::default();
        feed(&mut a, &[i(0), i(10)]);
        assert_f64_close(&a.result(), 25.0, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = Variance::default();
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn with_null() {
        let mut a = Variance::default();
        feed(&mut a, &[i(1), null(), i(3)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn floats() {
        let mut a = Variance::default();
        feed(&mut a, &[f(1.0), f(3.0)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
}

// ===========================================================================
// Median
// ===========================================================================
mod median_tests {
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
    fn odd_count() {
        let mut a = Median::default();
        feed(&mut a, &[i(1), i(3), i(2)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn even_count() {
        let mut a = Median::default();
        feed(&mut a, &[i(1), i(2), i(3), i(4)]);
        assert_f64_close(&a.result(), 2.5, 0.001);
    }
    #[test]
    fn same_values() {
        let mut a = Median::default();
        feed(&mut a, &[i(5), i(5), i(5)]);
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
        feed(&mut a, &[null(), null()]);
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
    fn two_values() {
        let mut a = Median::default();
        feed(&mut a, &[i(1), i(3)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn five_values() {
        let mut a = Median::default();
        feed(&mut a, &[i(10), i(20), i(30), i(40), i(50)]);
        assert_f64_close(&a.result(), 30.0, 0.001);
    }
}

// ===========================================================================
// CountDistinct
// ===========================================================================
mod count_distinct_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = CountDistinct::default();
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn all_same() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[i(5), i(5), i(5)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn all_different() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_eq!(a.result(), i(3));
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
        feed(&mut a, &[null(), null()]);
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn strings() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[s("a"), s("b"), s("a")]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn mixed_types() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[i(1), f(2.5), s("hello")]);
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
        for v in 0..10 {
            a.add(&i(v));
        }
        assert_eq!(a.result(), i(10));
    }
}

// ===========================================================================
// StringAgg
// ===========================================================================
mod string_agg_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = StringAgg::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = StringAgg::default();
        a.add(&s("hello"));
        assert_eq!(a.result(), s("hello"));
    }
    #[test]
    fn multiple() {
        let mut a = StringAgg::default();
        feed(&mut a, &[s("a"), s("b"), s("c")]);
        assert_eq!(a.result(), s("a,b,c"));
    }
    #[test]
    fn custom_sep() {
        let mut a = StringAgg::new(" | ".to_string());
        feed(&mut a, &[s("a"), s("b")]);
        assert_eq!(a.result(), s("a | b"));
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
        feed(&mut a, &[null(), null()]);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn with_numbers() {
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
// PercentileCont
// ===========================================================================
mod percentile_cont_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = PercentileCont::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = PercentileCont::new(0.5);
        a.add(&i(10));
        assert_f64_close(&a.result(), 10.0, 0.001);
    }
    #[test]
    fn median_three() {
        let mut a = PercentileCont::new(0.5);
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn p0() {
        let mut a = PercentileCont::new(0.0);
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn p100() {
        let mut a = PercentileCont::new(1.0);
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 3.0, 0.001);
    }
    #[test]
    fn p25() {
        let mut a = PercentileCont::new(0.25);
        feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn p75() {
        let mut a = PercentileCont::new(0.75);
        feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]);
        assert_f64_close(&a.result(), 4.0, 0.001);
    }
    #[test]
    fn with_null() {
        let mut a = PercentileCont::new(0.5);
        feed(&mut a, &[i(1), null(), i(3)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = PercentileCont::new(0.5);
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn interpolation() {
        let mut a = PercentileCont::new(0.5);
        feed(&mut a, &[i(1), i(3)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
}

// ===========================================================================
// PercentileDisc
// ===========================================================================
mod percentile_disc_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = PercentileDisc::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = PercentileDisc::new(0.5);
        a.add(&i(10));
        assert_f64_close(&a.result(), 10.0, 0.001);
    }
    #[test]
    fn median() {
        let mut a = PercentileDisc::new(0.5);
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn p0() {
        let mut a = PercentileDisc::new(0.0);
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn p100() {
        let mut a = PercentileDisc::new(1.0);
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 3.0, 0.001);
    }
    #[test]
    fn with_null() {
        let mut a = PercentileDisc::new(0.5);
        feed(&mut a, &[i(1), null(), i(3)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = PercentileDisc::new(0.5);
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// Mode
// ===========================================================================
mod mode_tests {
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
    fn clear_mode() {
        let mut a = Mode::default();
        feed(&mut a, &[i(1), i(2), i(2), i(3)]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn all_same() {
        let mut a = Mode::default();
        feed(&mut a, &[i(7), i(7), i(7)]);
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
        feed(&mut a, &[null(), null()]);
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
}

// ===========================================================================
// Corr
// ===========================================================================
mod corr_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = Corr::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Corr::default();
        a.add(&i(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn autocorrelation() {
        let mut a = Corr::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = Corr::default();
        feed(&mut a, &[i(1), i(2)]);
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// CovarPop / CovarSamp
// ===========================================================================
mod covar_tests {
    use super::*;

    #[test]
    fn pop_empty() {
        let a = CovarPop::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn pop_same() {
        let mut a = CovarPop::default();
        feed(&mut a, &[i(5), i(5), i(5)]);
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn pop_known() {
        let mut a = CovarPop::default();
        feed(&mut a, &[i(1), i(3)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn pop_reset() {
        let mut a = CovarPop::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn samp_empty() {
        let a = CovarSamp::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn samp_single() {
        let mut a = CovarSamp::default();
        a.add(&i(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn samp_known() {
        let mut a = CovarSamp::default();
        feed(&mut a, &[i(1), i(3)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn samp_reset() {
        let mut a = CovarSamp::default();
        feed(&mut a, &[i(1), i(2)]);
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// RegrSlope / RegrIntercept
// ===========================================================================
mod regr_tests {
    use super::*;

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
        feed(&mut a, &[i(5), i(5), i(5)]);
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn slope_linear() {
        // Values 1,2,3,4,5 against indices 0,1,2,3,4 -> slope = 1
        let mut a = RegrSlope::default();
        feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]);
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
        feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn intercept_constant() {
        let mut a = RegrIntercept::default();
        feed(&mut a, &[i(5), i(5), i(5)]);
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
// BoolAnd / BoolOr
// ===========================================================================
mod bool_tests {
    use super::*;

    #[test]
    fn and_empty() {
        let a = BoolAnd::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn and_all_true() {
        let mut a = BoolAnd::default();
        feed(&mut a, &[i(1), i(1), i(1)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn and_one_false() {
        let mut a = BoolAnd::default();
        feed(&mut a, &[i(1), i(0), i(1)]);
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
        feed(&mut a, &[i(1), null(), i(1)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn and_all_null() {
        let mut a = BoolAnd::default();
        feed(&mut a, &[null(), null()]);
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
    fn and_string_true() {
        let mut a = BoolAnd::default();
        feed(&mut a, &[s("true")]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn and_string_false() {
        let mut a = BoolAnd::default();
        feed(&mut a, &[s("false")]);
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
        feed(&mut a, &[i(0), i(1), i(0)]);
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
        feed(&mut a, &[null(), null()]);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn or_reset() {
        let mut a = BoolOr::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// ArrayAgg
// ===========================================================================
mod array_agg_tests {
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
        assert_eq!(a.result(), s("[42]"));
    }
    #[test]
    fn multiple() {
        let mut a = ArrayAgg::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_eq!(a.result(), s("[1,2,3]"));
    }
    #[test]
    fn with_null() {
        let mut a = ArrayAgg::default();
        feed(&mut a, &[i(1), null(), i(3)]);
        assert_eq!(a.result(), s("[1,3]"));
    }
    #[test]
    fn all_null() {
        let mut a = ArrayAgg::default();
        feed(&mut a, &[null(), null()]);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn strings() {
        let mut a = ArrayAgg::default();
        feed(&mut a, &[s("a"), s("b")]);
        assert_eq!(a.result(), s("[a,b]"));
    }
    #[test]
    fn reset() {
        let mut a = ArrayAgg::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single_string() {
        let mut a = ArrayAgg::default();
        a.add(&s("hello"));
        assert_eq!(a.result(), s("[hello]"));
    }
}

// ===========================================================================
// Vwap
// ===========================================================================
mod vwap_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = Vwap::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Vwap::default();
        a.add(&f(100.0));
        assert_f64_close(&a.result(), 100.0, 0.001);
    }
    #[test]
    fn multiple() {
        let mut a = Vwap::default();
        feed(&mut a, &[f(100.0), f(200.0), f(150.0)]);
        assert_f64_close(&a.result(), 150.0, 0.001);
    }
    #[test]
    fn same_values() {
        let mut a = Vwap::default();
        feed(&mut a, &[f(50.0), f(50.0)]);
        assert_f64_close(&a.result(), 50.0, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = Vwap::default();
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn ints() {
        let mut a = Vwap::default();
        feed(&mut a, &[i(100), i(200)]);
        assert_f64_close(&a.result(), 150.0, 0.001);
    }
}

// ===========================================================================
// Sma
// ===========================================================================
mod sma_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = Sma::new(3);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Sma::new(3);
        a.add(&f(10.0));
        assert_f64_close(&a.result(), 10.0, 0.001);
    }
    #[test]
    fn within_period() {
        let mut a = Sma::new(5);
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn full_period() {
        let mut a = Sma::new(3);
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn beyond_period() {
        let mut a = Sma::new(3);
        feed(&mut a, &[f(1.0), f(2.0), f(3.0), f(4.0), f(5.0)]);
        assert_f64_close(&a.result(), 4.0, 0.001); // avg(3,4,5)
    }
    #[test]
    fn period_one() {
        let mut a = Sma::new(1);
        feed(&mut a, &[f(1.0), f(5.0)]);
        assert_f64_close(&a.result(), 5.0, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = Sma::new(3);
        a.add(&f(10.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn ints() {
        let mut a = Sma::new(3);
        feed(&mut a, &[i(10), i(20), i(30)]);
        assert_f64_close(&a.result(), 20.0, 0.001);
    }
}

// ===========================================================================
// Ema
// ===========================================================================
mod ema_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = Ema::new(3);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Ema::new(3);
        a.add(&f(10.0));
        assert_f64_close(&a.result(), 10.0, 0.001);
    }
    #[test]
    fn two_values() {
        let mut a = Ema::new(3);
        a.add(&f(10.0));
        a.add(&f(20.0));
        // alpha = 2/(3+1) = 0.5; ema = 20*0.5 + 10*0.5 = 15
        assert_f64_close(&a.result(), 15.0, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = Ema::new(3);
        a.add(&f(10.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn constant() {
        let mut a = Ema::new(5);
        feed(&mut a, &[f(10.0), f(10.0), f(10.0), f(10.0), f(10.0)]);
        assert_f64_close(&a.result(), 10.0, 0.001);
    }
    #[test]
    fn period_one() {
        let mut a = Ema::new(1);
        feed(&mut a, &[f(10.0), f(20.0)]);
        assert_f64_close(&a.result(), 20.0, 0.001);
    }
}

// ===========================================================================
// Wma
// ===========================================================================
mod wma_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = Wma::new(3);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Wma::new(3);
        a.add(&f(10.0));
        assert_f64_close(&a.result(), 10.0, 0.001);
    }
    #[test]
    fn three_values() {
        let mut a = Wma::new(3);
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        // WMA = (1*1 + 2*2 + 3*3) / (1+2+3) = 14/6 ≈ 2.333
        assert_f64_close(&a.result(), 14.0 / 6.0, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = Wma::new(3);
        a.add(&f(10.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn constant() {
        let mut a = Wma::new(3);
        feed(&mut a, &[f(5.0), f(5.0), f(5.0)]);
        assert_f64_close(&a.result(), 5.0, 0.001);
    }
}

// ===========================================================================
// Rsi
// ===========================================================================
mod rsi_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = Rsi::new(14);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Rsi::new(14);
        a.add(&f(100.0));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn all_gains() {
        let mut a = Rsi::new(5);
        feed(
            &mut a,
            &[f(10.0), f(20.0), f(30.0), f(40.0), f(50.0), f(60.0)],
        );
        assert_f64_close(&a.result(), 100.0, 0.001);
    }
    #[test]
    fn all_losses() {
        let mut a = Rsi::new(5);
        feed(
            &mut a,
            &[f(60.0), f(50.0), f(40.0), f(30.0), f(20.0), f(10.0)],
        );
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn balanced() {
        let mut a = Rsi::new(4);
        feed(&mut a, &[f(10.0), f(20.0), f(10.0), f(20.0), f(10.0)]);
        assert_f64_close(&a.result(), 50.0, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = Rsi::new(14);
        feed(&mut a, &[f(1.0), f(2.0)]);
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// Ksum
// ===========================================================================
mod ksum_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = Ksum::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Ksum::default();
        a.add(&f(1.0));
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn basic() {
        let mut a = Ksum::default();
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        assert_f64_close(&a.result(), 6.0, 0.001);
    }
    #[test]
    fn compensated() {
        let mut a = Ksum::default();
        for _ in 0..1000 {
            a.add(&f(0.1));
        }
        assert_f64_close(&a.result(), 100.0, 0.01);
    }
    #[test]
    fn reset() {
        let mut a = Ksum::default();
        a.add(&f(1.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn ints() {
        let mut a = Ksum::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 6.0, 0.001);
    }
    #[test]
    fn negative() {
        let mut a = Ksum::default();
        feed(&mut a, &[f(1.0), f(-1.0)]);
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
}

// ===========================================================================
// Nsum
// ===========================================================================
mod nsum_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = Nsum::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Nsum::default();
        a.add(&f(1.0));
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn basic() {
        let mut a = Nsum::default();
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        assert_f64_close(&a.result(), 6.0, 0.001);
    }
    #[test]
    fn compensated() {
        let mut a = Nsum::default();
        for _ in 0..1000 {
            a.add(&f(0.1));
        }
        assert_f64_close(&a.result(), 100.0, 0.01);
    }
    #[test]
    fn reset() {
        let mut a = Nsum::default();
        a.add(&f(1.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn ints() {
        let mut a = Nsum::default();
        feed(&mut a, &[i(10), i(20)]);
        assert_f64_close(&a.result(), 30.0, 0.001);
    }
}

// ===========================================================================
// ApproxCountDistinct
// ===========================================================================
mod approx_count_distinct_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = ApproxCountDistinct::default();
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn all_same() {
        let mut a = ApproxCountDistinct::default();
        feed(&mut a, &[i(5), i(5), i(5)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn all_different() {
        let mut a = ApproxCountDistinct::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn with_null() {
        let mut a = ApproxCountDistinct::default();
        feed(&mut a, &[i(1), null(), i(2)]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn large() {
        let mut a = ApproxCountDistinct::default();
        for v in 0..100 {
            a.add(&i(v));
        }
        assert_eq!(a.result(), i(100));
    }
    #[test]
    fn reset() {
        let mut a = ApproxCountDistinct::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), i(0));
    }
}

// ===========================================================================
// BollingerUpper / BollingerLower
// ===========================================================================
mod bollinger_tests {
    use super::*;

    #[test]
    fn upper_empty() {
        let a = BollingerUpper::new(20, 2.0);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn upper_constant() {
        let mut a = BollingerUpper::new(3, 2.0);
        feed(&mut a, &[f(100.0), f(100.0), f(100.0)]);
        assert_f64_close(&a.result(), 100.0, 0.001); // stddev = 0, so upper = mean
    }
    #[test]
    fn upper_varied() {
        let mut a = BollingerUpper::new(3, 2.0);
        feed(&mut a, &[f(98.0), f(100.0), f(102.0)]);
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 100.0),
            _ => panic!(),
        }
    }
    #[test]
    fn upper_reset() {
        let mut a = BollingerUpper::new(20, 2.0);
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn lower_empty() {
        let a = BollingerLower::new(20, 2.0);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn lower_constant() {
        let mut a = BollingerLower::new(3, 2.0);
        feed(&mut a, &[f(100.0), f(100.0), f(100.0)]);
        assert_f64_close(&a.result(), 100.0, 0.001);
    }
    #[test]
    fn lower_varied() {
        let mut a = BollingerLower::new(3, 2.0);
        feed(&mut a, &[f(98.0), f(100.0), f(102.0)]);
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v < 100.0),
            _ => panic!(),
        }
    }
    #[test]
    fn lower_reset() {
        let mut a = BollingerLower::new(20, 2.0);
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// Atr
// ===========================================================================
mod atr_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = Atr::new(14);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Atr::new(14);
        a.add(&f(100.0));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn constant() {
        let mut a = Atr::new(3);
        feed(&mut a, &[f(100.0), f(100.0), f(100.0)]);
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn varied() {
        let mut a = Atr::new(3);
        feed(&mut a, &[f(100.0), f(102.0), f(98.0), f(101.0)]);
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!(),
        }
    }
    #[test]
    fn reset() {
        let mut a = Atr::new(14);
        feed(&mut a, &[f(1.0), f(2.0)]);
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// Drawdown
// ===========================================================================
mod drawdown_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = Drawdown::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = Drawdown::default();
        a.add(&f(100.0));
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn no_drawdown() {
        let mut a = Drawdown::default();
        feed(&mut a, &[f(100.0), f(110.0), f(120.0)]);
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn known_drawdown() {
        let mut a = Drawdown::default();
        feed(&mut a, &[f(100.0), f(120.0), f(90.0)]);
        // Peak = 120, trough = 90, drawdown = 30/120 = 0.25
        assert_f64_close(&a.result(), 0.25, 0.001);
    }
    #[test]
    fn full_drawdown() {
        let mut a = Drawdown::default();
        feed(&mut a, &[f(100.0), f(50.0)]);
        assert_f64_close(&a.result(), 0.5, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = Drawdown::default();
        a.add(&f(100.0));
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// SumDouble / SumLong
// ===========================================================================
mod sum_typed_tests {
    use super::*;

    #[test]
    fn sum_double_empty() {
        let a = SumDouble::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn sum_double_ints() {
        let mut a = SumDouble::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_f64_close(&a.result(), 6.0, 0.001);
    }
    #[test]
    fn sum_double_floats() {
        let mut a = SumDouble::default();
        feed(&mut a, &[f(1.5), f(2.5)]);
        assert_f64_close(&a.result(), 4.0, 0.001);
    }
    #[test]
    fn sum_double_reset() {
        let mut a = SumDouble::default();
        a.add(&f(1.0));
        a.reset();
        assert_eq!(a.result(), null());
    }

    #[test]
    fn sum_long_empty() {
        let a = SumLong::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn sum_long_ints() {
        let mut a = SumLong::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        assert_eq!(a.result(), i(6));
    }
    #[test]
    fn sum_long_reset() {
        let mut a = SumLong::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// MacdSignal
// ===========================================================================
mod macd_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = MacdSignal::new(12, 26, 9);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn insufficient_data() {
        let mut a = MacdSignal::new(12, 26, 9);
        for _ in 0..10 {
            a.add(&f(100.0));
        }
        assert_eq!(a.result(), null());
    }
    #[test]
    fn constant_data() {
        let mut a = MacdSignal::new(12, 26, 9);
        for _ in 0..30 {
            a.add(&f(100.0));
        }
        assert_f64_close(&a.result(), 0.0, 0.01);
    }
    #[test]
    fn trending_up() {
        let mut a = MacdSignal::new(12, 26, 9);
        for n in 0..40 {
            a.add(&f(100.0 + n as f64));
        }
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!(),
        }
    }
    #[test]
    fn reset() {
        let mut a = MacdSignal::new(12, 26, 9);
        for _ in 0..30 {
            a.add(&f(100.0));
        }
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// StdDevSamp
// ===========================================================================
mod stddev_samp_tests {
    use super::*;

    #[test]
    fn empty() {
        let a = StdDevSamp::default();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn single() {
        let mut a = StdDevSamp::default();
        a.add(&i(5));
        assert_eq!(a.result(), null());
    }
    #[test]
    fn known() {
        let mut a = StdDevSamp::default();
        feed(&mut a, &[i(2), i(4), i(4), i(4), i(5), i(5), i(7), i(9)]);
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 2.0 && v < 2.2),
            _ => panic!(),
        }
    }
    #[test]
    fn same() {
        let mut a = StdDevSamp::default();
        feed(&mut a, &[i(5), i(5), i(5)]);
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn reset() {
        let mut a = StdDevSamp::default();
        feed(&mut a, &[i(1), i(2)]);
        a.reset();
        assert_eq!(a.result(), null());
    }
}

// ===========================================================================
// Additional aggregate tests for higher test count
// ===========================================================================

mod sum_extended_tests {
    use super::*;

    #[test]
    fn alternating() {
        let mut a = Sum::default();
        feed(&mut a, &[i(1), i(-1), i(2), i(-2)]);
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn large_positive() {
        let mut a = Sum::default();
        for _ in 0..100 {
            a.add(&i(100));
        }
        assert_eq!(a.result(), i(10000));
    }
    #[test]
    fn single_float() {
        let mut a = Sum::default();
        a.add(&f(99.99));
        assert_f64_close(&a.result(), 99.99, 0.001);
    }
    #[test]
    fn mixed_signs() {
        let mut a = Sum::default();
        feed(&mut a, &[i(10), i(-3), i(7), i(-4)]);
        assert_eq!(a.result(), i(10));
    }
    #[test]
    fn all_zeros() {
        let mut a = Sum::default();
        feed(&mut a, &[i(0), i(0), i(0)]);
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn single_negative() {
        let mut a = Sum::default();
        a.add(&i(-42));
        assert_eq!(a.result(), i(-42));
    }
    #[test]
    fn float_precision() {
        let mut a = Sum::default();
        feed(&mut a, &[f(0.1), f(0.2)]);
        assert_f64_close(&a.result(), 0.3, 0.001);
    }
}

mod avg_extended_tests {
    use super::*;

    #[test]
    fn two_values() {
        let mut a = Avg::default();
        feed(&mut a, &[i(10), i(20)]);
        assert_f64_close(&a.result(), 15.0, 0.001);
    }
    #[test]
    fn all_zeros() {
        let mut a = Avg::default();
        feed(&mut a, &[i(0), i(0)]);
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn negative_avg() {
        let mut a = Avg::default();
        feed(&mut a, &[i(-10), i(-20)]);
        assert_f64_close(&a.result(), -15.0, 0.001);
    }
    #[test]
    fn float_values() {
        let mut a = Avg::default();
        feed(&mut a, &[f(1.5), f(2.5), f(3.5)]);
        assert_f64_close(&a.result(), 2.5, 0.001);
    }
    #[test]
    fn hundred_values() {
        let mut a = Avg::default();
        for v in 1..=100 {
            a.add(&i(v));
        }
        assert_f64_close(&a.result(), 50.5, 0.001);
    }
    #[test]
    fn single_large() {
        let mut a = Avg::default();
        a.add(&i(1_000_000));
        assert_f64_close(&a.result(), 1_000_000.0, 0.001);
    }
}

mod min_max_extended_tests {
    use super::*;

    #[test]
    fn min_all_same() {
        let mut a = Min::default();
        feed(&mut a, &[i(3), i(3), i(3)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn min_at_start() {
        let mut a = Min::default();
        feed(&mut a, &[i(1), i(5), i(3)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn min_at_end() {
        let mut a = Min::default();
        feed(&mut a, &[i(5), i(3), i(1)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn min_in_middle() {
        let mut a = Min::default();
        feed(&mut a, &[i(5), i(1), i(3)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn min_floats_neg() {
        let mut a = Min::default();
        feed(&mut a, &[f(-1.0), f(-3.0), f(-2.0)]);
        assert_eq!(a.result(), f(-3.0));
    }
    #[test]
    fn min_large() {
        let mut a = Min::default();
        for v in (0..100).rev() {
            a.add(&i(v));
        }
        assert_eq!(a.result(), i(0));
    }

    #[test]
    fn max_all_same() {
        let mut a = Max::default();
        feed(&mut a, &[i(3), i(3), i(3)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn max_at_start() {
        let mut a = Max::default();
        feed(&mut a, &[i(5), i(3), i(1)]);
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn max_at_end() {
        let mut a = Max::default();
        feed(&mut a, &[i(1), i(3), i(5)]);
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn max_in_middle() {
        let mut a = Max::default();
        feed(&mut a, &[i(1), i(5), i(3)]);
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn max_floats_neg() {
        let mut a = Max::default();
        feed(&mut a, &[f(-1.0), f(-3.0), f(-2.0)]);
        assert_eq!(a.result(), f(-1.0));
    }
    #[test]
    fn max_large() {
        let mut a = Max::default();
        for v in 0..100 {
            a.add(&i(v));
        }
        assert_eq!(a.result(), i(99));
    }
}

mod count_extended_tests {
    use super::*;

    #[test]
    fn mixed_nulls() {
        let mut a = Count::default();
        feed(&mut a, &[i(1), null(), i(2), null(), i(3)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn all_strings() {
        let mut a = Count::default();
        feed(&mut a, &[s("a"), s("b"), s("c")]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn empty_strings() {
        let mut a = Count::default();
        feed(&mut a, &[s(""), s(""), s("")]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn floats() {
        let mut a = Count::default();
        feed(&mut a, &[f(1.0), f(2.0)]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn single_null() {
        let mut a = Count::default();
        a.add(&null());
        assert_eq!(a.result(), i(0));
    }
}

mod median_extended_tests {
    use super::*;

    #[test]
    fn seven_values() {
        let mut a = Median::default();
        feed(&mut a, &[i(7), i(1), i(5), i(3), i(2), i(6), i(4)]);
        assert_f64_close(&a.result(), 4.0, 0.001);
    }
    #[test]
    fn four_values() {
        let mut a = Median::default();
        feed(&mut a, &[i(1), i(2), i(3), i(4)]);
        assert_f64_close(&a.result(), 2.5, 0.001);
    }
    #[test]
    fn negative_values() {
        let mut a = Median::default();
        feed(&mut a, &[i(-5), i(-1), i(-3)]);
        assert_f64_close(&a.result(), -3.0, 0.001);
    }
    #[test]
    fn large_set() {
        let mut a = Median::default();
        for v in 1..=101 {
            a.add(&i(v));
        }
        assert_f64_close(&a.result(), 51.0, 0.001);
    }
    #[test]
    fn mixed_types() {
        let mut a = Median::default();
        feed(&mut a, &[i(1), f(2.0), i(3)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
}

mod variance_extended_tests {
    use super::*;

    #[test]
    fn one_to_five() {
        let mut a = Variance::default();
        feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn large_variance() {
        let mut a = Variance::default();
        feed(&mut a, &[i(1), i(100)]);
        assert_f64_close(&a.result(), 2450.25, 0.01);
    }
    #[test]
    fn floats() {
        let mut a = Variance::default();
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        assert_f64_close(&a.result(), 2.0 / 3.0, 0.001);
    }
}

mod stddev_extended_tests {
    use super::*;

    #[test]
    fn one_to_five() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]);
        assert_f64_close(&a.result(), (2.0_f64).sqrt(), 0.001);
    }
    #[test]
    fn large_values() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(1000), i(1001), i(999)]);
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v < 1.0),
            _ => panic!(),
        }
    }
}

mod percentile_cont_extended_tests {
    use super::*;

    #[test]
    fn p10() {
        let mut a = PercentileCont::new(0.1);
        feed(
            &mut a,
            &[i(1), i(2), i(3), i(4), i(5), i(6), i(7), i(8), i(9), i(10)],
        );
        assert_f64_close(&a.result(), 1.9, 0.001);
    }
    #[test]
    fn p90() {
        let mut a = PercentileCont::new(0.9);
        feed(
            &mut a,
            &[i(1), i(2), i(3), i(4), i(5), i(6), i(7), i(8), i(9), i(10)],
        );
        assert_f64_close(&a.result(), 9.1, 0.001);
    }
    #[test]
    fn p50_even() {
        let mut a = PercentileCont::new(0.5);
        feed(&mut a, &[i(1), i(2), i(3), i(4)]);
        assert_f64_close(&a.result(), 2.5, 0.001);
    }
    #[test]
    fn floats() {
        let mut a = PercentileCont::new(0.5);
        feed(&mut a, &[f(1.5), f(2.5), f(3.5)]);
        assert_f64_close(&a.result(), 2.5, 0.001);
    }
    #[test]
    fn all_same() {
        let mut a = PercentileCont::new(0.5);
        feed(&mut a, &[i(7), i(7), i(7)]);
        assert_f64_close(&a.result(), 7.0, 0.001);
    }
}

mod bool_extended_tests {
    use super::*;

    #[test]
    fn and_single_true() {
        let mut a = BoolAnd::default();
        a.add(&i(1));
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn and_single_false() {
        let mut a = BoolAnd::default();
        a.add(&i(0));
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn and_float_nonzero() {
        let mut a = BoolAnd::default();
        a.add(&f(3.15));
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn and_float_zero() {
        let mut a = BoolAnd::default();
        a.add(&f(0.0));
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn and_empty_string() {
        let mut a = BoolAnd::default();
        a.add(&s(""));
        assert_eq!(a.result(), i(0));
    }

    #[test]
    fn or_single_true() {
        let mut a = BoolOr::default();
        a.add(&i(1));
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn or_single_false() {
        let mut a = BoolOr::default();
        a.add(&i(0));
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn or_float_nonzero() {
        let mut a = BoolOr::default();
        a.add(&f(3.15));
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn or_float_zero() {
        let mut a = BoolOr::default();
        a.add(&f(0.0));
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn or_mixed() {
        let mut a = BoolOr::default();
        feed(&mut a, &[i(0), f(0.0), i(1)]);
        assert_eq!(a.result(), i(1));
    }
}

mod sma_extended_tests {
    use super::*;

    #[test]
    fn period_2() {
        let mut a = Sma::new(2);
        feed(&mut a, &[f(10.0), f(20.0), f(30.0)]);
        assert_f64_close(&a.result(), 25.0, 0.001);
    }
    #[test]
    fn period_5_short() {
        let mut a = Sma::new(5);
        feed(&mut a, &[f(1.0), f(2.0)]);
        assert_f64_close(&a.result(), 1.5, 0.001);
    }
    #[test]
    fn period_10() {
        let mut a = Sma::new(10);
        for v in 1..=20 {
            a.add(&f(v as f64));
        }
        assert_f64_close(&a.result(), 15.5, 0.001);
    }
    #[test]
    fn constant_period_3() {
        let mut a = Sma::new(3);
        feed(&mut a, &[f(7.0), f(7.0), f(7.0), f(7.0)]);
        assert_f64_close(&a.result(), 7.0, 0.001);
    }
}

mod ema_extended_tests {
    use super::*;

    #[test]
    fn increasing() {
        let mut a = Ema::new(3);
        feed(&mut a, &[f(10.0), f(20.0), f(30.0), f(40.0), f(50.0)]);
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 30.0 && v < 50.0),
            _ => panic!(),
        }
    }
    #[test]
    fn decreasing() {
        let mut a = Ema::new(3);
        feed(&mut a, &[f(50.0), f(40.0), f(30.0), f(20.0), f(10.0)]);
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 10.0 && v < 30.0),
            _ => panic!(),
        }
    }
    #[test]
    fn period_2() {
        let mut a = Ema::new(2);
        a.add(&f(10.0));
        a.add(&f(20.0));
        // alpha = 2/3; ema = 20*2/3 + 10*1/3 ≈ 16.67
        assert_f64_close(&a.result(), 16.667, 0.01);
    }
}

mod wma_extended_tests {
    use super::*;

    #[test]
    fn two_values() {
        let mut a = Wma::new(2);
        feed(&mut a, &[f(10.0), f(20.0)]);
        // WMA = (1*10 + 2*20) / (1+2) = 50/3 ≈ 16.667
        assert_f64_close(&a.result(), 50.0 / 3.0, 0.001);
    }
    #[test]
    fn period_4() {
        let mut a = Wma::new(4);
        feed(&mut a, &[f(1.0), f(2.0), f(3.0), f(4.0)]);
        // WMA = (1*1 + 2*2 + 3*3 + 4*4) / (1+2+3+4) = 30/10 = 3.0
        assert_f64_close(&a.result(), 3.0, 0.001);
    }
}

mod drawdown_extended_tests {
    use super::*;

    #[test]
    fn recovery() {
        let mut a = Drawdown::default();
        feed(&mut a, &[f(100.0), f(110.0), f(80.0), f(120.0)]);
        // Peak=110 at point 2, trough=80, dd = 30/110 ≈ 0.2727
        assert_f64_close(&a.result(), 30.0 / 110.0, 0.001);
    }
    #[test]
    fn monotonic_up() {
        let mut a = Drawdown::default();
        feed(&mut a, &[f(10.0), f(20.0), f(30.0), f(40.0)]);
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
    #[test]
    fn zigzag() {
        let mut a = Drawdown::default();
        feed(&mut a, &[f(100.0), f(90.0), f(95.0), f(85.0)]);
        // Peak=100, worst trough=85, dd=15/100=0.15
        assert_f64_close(&a.result(), 0.15, 0.001);
    }
}

mod rsi_extended_tests {
    use super::*;

    #[test]
    fn mixed() {
        let mut a = Rsi::new(3);
        feed(&mut a, &[f(44.0), f(44.34), f(44.09), f(43.61)]);
        let r = a.result();
        match r {
            Value::F64(v) => assert!((0.0..=100.0).contains(&v)),
            _ => panic!(),
        }
    }
    #[test]
    fn steady() {
        let mut a = Rsi::new(3);
        feed(&mut a, &[f(100.0), f(100.0), f(100.0), f(100.0)]);
        // No changes -> gains=0, losses=0 -> RSI=100 (div by zero in loss -> 100)
        // Actually: gains=0, losses=0, avg_loss=0, so returns 100.0
        assert_f64_close(&a.result(), 100.0, 0.001);
    }
    #[test]
    fn period_2() {
        let mut a = Rsi::new(2);
        feed(&mut a, &[f(10.0), f(20.0), f(15.0)]);
        let r = a.result();
        match r {
            Value::F64(v) => assert!((0.0..=100.0).contains(&v)),
            _ => panic!(),
        }
    }
}

mod array_agg_extended_tests {
    use super::*;

    #[test]
    fn floats() {
        let mut a = ArrayAgg::default();
        feed(&mut a, &[f(1.5), f(2.5)]);
        assert_eq!(a.result(), s("[1.5,2.5]"));
    }
    #[test]
    fn mixed() {
        let mut a = ArrayAgg::default();
        feed(&mut a, &[i(1), s("hello"), f(3.15)]);
        assert_eq!(a.result(), s("[1,hello,3.15]"));
    }
    #[test]
    fn single_int() {
        let mut a = ArrayAgg::default();
        a.add(&i(42));
        assert_eq!(a.result(), s("[42]"));
    }
    #[test]
    fn many() {
        let mut a = ArrayAgg::default();
        for v in 1..=5 {
            a.add(&i(v));
        }
        assert_eq!(a.result(), s("[1,2,3,4,5]"));
    }
}

mod string_agg_extended_tests {
    use super::*;

    #[test]
    fn semicolon_sep() {
        let mut a = StringAgg::new(";".to_string());
        feed(&mut a, &[s("a"), s("b"), s("c")]);
        assert_eq!(a.result(), s("a;b;c"));
    }
    #[test]
    fn pipe_sep() {
        let mut a = StringAgg::new("|".to_string());
        feed(&mut a, &[s("x"), s("y")]);
        assert_eq!(a.result(), s("x|y"));
    }
    #[test]
    fn newline_sep() {
        let mut a = StringAgg::new("\n".to_string());
        feed(&mut a, &[s("line1"), s("line2")]);
        assert_eq!(a.result(), s("line1\nline2"));
    }
    #[test]
    fn single_elem() {
        let mut a = StringAgg::new(",".to_string());
        a.add(&s("only"));
        assert_eq!(a.result(), s("only"));
    }
}

mod count_distinct_extended_tests {
    use super::*;

    #[test]
    fn strings_with_dups() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[s("a"), s("b"), s("a"), s("c"), s("b")]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn ints_with_dups() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[i(1), i(2), i(1), i(3), i(2)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn single() {
        let mut a = CountDistinct::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn two_same() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[i(1), i(1)]);
        assert_eq!(a.result(), i(1));
    }
}

mod ksum_nsum_extended_tests {
    use super::*;

    #[test]
    fn ksum_negative() {
        let mut a = Ksum::default();
        feed(&mut a, &[f(-1.0), f(-2.0), f(-3.0)]);
        assert_f64_close(&a.result(), -6.0, 0.001);
    }
    #[test]
    fn ksum_mixed() {
        let mut a = Ksum::default();
        feed(&mut a, &[f(1e10), f(1.0), f(-1e10)]);
        assert_f64_close(&a.result(), 1.0, 0.01);
    }
    #[test]
    fn ksum_single_int() {
        let mut a = Ksum::default();
        a.add(&i(42));
        assert_f64_close(&a.result(), 42.0, 0.001);
    }

    #[test]
    fn nsum_negative() {
        let mut a = Nsum::default();
        feed(&mut a, &[f(-1.0), f(-2.0), f(-3.0)]);
        assert_f64_close(&a.result(), -6.0, 0.001);
    }
    #[test]
    fn nsum_mixed() {
        let mut a = Nsum::default();
        feed(&mut a, &[f(1e10), f(1.0), f(-1e10)]);
        assert_f64_close(&a.result(), 1.0, 0.01);
    }
    #[test]
    fn nsum_single_int() {
        let mut a = Nsum::default();
        a.add(&i(42));
        assert_f64_close(&a.result(), 42.0, 0.001);
    }
}

mod mode_extended_tests {
    use super::*;

    #[test]
    fn three_way_tie() {
        // When there's a tie, mode returns one of the modes
        let mut a = Mode::default();
        feed(&mut a, &[i(1), i(2), i(3)]);
        let r = a.result();
        match r {
            Value::I64(v) => assert!((1..=3).contains(&v)),
            _ => panic!(),
        }
    }
    #[test]
    fn clear_mode_5() {
        let mut a = Mode::default();
        feed(&mut a, &[i(1), i(5), i(5), i(5), i(3), i(3)]);
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn string_mode() {
        let mut a = Mode::default();
        feed(&mut a, &[s("cat"), s("dog"), s("cat"), s("bird"), s("cat")]);
        assert_eq!(a.result(), s("cat"));
    }
}

// ===========================================================================
// More aggregate tests
// ===========================================================================

mod sum_more_tests {
    use super::*;

    #[test]
    fn hundred_floats() {
        let mut a = Sum::default();
        for _ in 0..100 {
            a.add(&f(1.5));
        }
        assert_f64_close(&a.result(), 150.0, 0.01);
    }
    #[test]
    fn alternating_large() {
        let mut a = Sum::default();
        for n in 0..50 {
            if n % 2 == 0 {
                a.add(&i(100));
            } else {
                a.add(&i(-100));
            }
        }
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn five_hundred() {
        let mut a = Sum::default();
        for v in 1..=500 {
            a.add(&i(v));
        }
        assert_eq!(a.result(), i(125250));
    }
    #[test]
    fn float_and_null() {
        let mut a = Sum::default();
        feed(&mut a, &[f(1.5), null(), f(2.5), null()]);
        assert_f64_close(&a.result(), 4.0, 0.001);
    }
}

mod avg_more_tests {
    use super::*;

    #[test]
    fn ten_same() {
        let mut a = Avg::default();
        for _ in 0..10 {
            a.add(&i(42));
        }
        assert_f64_close(&a.result(), 42.0, 0.001);
    }
    #[test]
    fn alternating() {
        let mut a = Avg::default();
        feed(&mut a, &[i(0), i(100), i(0), i(100)]);
        assert_f64_close(&a.result(), 50.0, 0.001);
    }
    #[test]
    fn one_null_many_values() {
        let mut a = Avg::default();
        for v in 1..=10 {
            a.add(&i(v));
        }
        a.add(&null());
        assert_f64_close(&a.result(), 5.5, 0.001);
    }
    #[test]
    fn floats_precise() {
        let mut a = Avg::default();
        feed(&mut a, &[f(0.1), f(0.2), f(0.3)]);
        assert_f64_close(&a.result(), 0.2, 0.001);
    }
}

mod count_more_tests {
    use super::*;

    #[test]
    fn alternating_null() {
        let mut a = Count::default();
        for n in 0..10 {
            if n % 2 == 0 {
                a.add(&i(n));
            } else {
                a.add(&null());
            }
        }
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn hundred_values() {
        let mut a = Count::default();
        for _ in 0..100 {
            a.add(&i(1));
        }
        assert_eq!(a.result(), i(100));
    }
    #[test]
    fn ts_values() {
        let mut a = Count::default();
        for n in 0..5 {
            a.add(&Value::Timestamp(n));
        }
        assert_eq!(a.result(), i(5));
    }
}

mod first_last_more_tests {
    use super::*;

    #[test]
    fn first_after_many_nulls() {
        let mut a = First::default();
        for _ in 0..5 {
            a.add(&null());
        }
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn first_ignores_later() {
        let mut a = First::default();
        a.add(&i(1));
        a.add(&i(999));
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn last_many_values() {
        let mut a = Last::default();
        for v in 1..=100 {
            a.add(&i(v));
        }
        assert_eq!(a.result(), i(100));
    }
    #[test]
    fn last_ignores_null_at_end() {
        let mut a = Last::default();
        a.add(&i(1));
        a.add(&i(2));
        a.add(&null());
        assert_eq!(a.result(), i(2));
    }
}

mod median_more_tests {
    use super::*;

    #[test]
    fn hundred_values() {
        let mut a = Median::default();
        for v in 1..=100 {
            a.add(&i(v));
        }
        assert_f64_close(&a.result(), 50.5, 0.001);
    }
    #[test]
    fn all_same_float() {
        let mut a = Median::default();
        for _ in 0..10 {
            a.add(&f(3.15));
        }
        assert_f64_close(&a.result(), 3.15, 0.001);
    }
    #[test]
    fn reverse_order() {
        let mut a = Median::default();
        for v in (1..=10).rev() {
            a.add(&i(v));
        }
        assert_f64_close(&a.result(), 5.5, 0.001);
    }
}

mod vwap_more_tests {
    use super::*;

    #[test]
    fn ten_values() {
        let mut a = Vwap::default();
        for _ in 0..10 {
            a.add(&f(50.0));
        }
        assert_f64_close(&a.result(), 50.0, 0.001);
    }
    #[test]
    fn increasing() {
        let mut a = Vwap::default();
        feed(&mut a, &[f(10.0), f(20.0), f(30.0), f(40.0)]);
        assert_f64_close(&a.result(), 25.0, 0.001);
    }
    #[test]
    fn with_ints() {
        let mut a = Vwap::default();
        feed(&mut a, &[i(100), i(200), i(300)]);
        assert_f64_close(&a.result(), 200.0, 0.001);
    }
}

mod percentile_more_tests {
    use super::*;

    #[test]
    fn cont_p10_100() {
        let mut a = PercentileCont::new(0.1);
        for v in 1..=100 {
            a.add(&i(v));
        }
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 9.0 && v < 12.0),
            _ => panic!(),
        }
    }
    #[test]
    fn cont_p90_100() {
        let mut a = PercentileCont::new(0.9);
        for v in 1..=100 {
            a.add(&i(v));
        }
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 89.0 && v < 92.0),
            _ => panic!(),
        }
    }
    #[test]
    fn disc_p10_100() {
        let mut a = PercentileDisc::new(0.1);
        for v in 1..=100 {
            a.add(&i(v));
        }
        let r = a.result();
        match r {
            Value::F64(v) => assert!((1.0..=20.0).contains(&v)),
            _ => panic!(),
        }
    }
    #[test]
    fn disc_p90_100() {
        let mut a = PercentileDisc::new(0.9);
        for v in 1..=100 {
            a.add(&i(v));
        }
        let r = a.result();
        match r {
            Value::F64(v) => assert!((80.0..=100.0).contains(&v)),
            _ => panic!(),
        }
    }
}

mod regression_more_tests {
    use super::*;

    #[test]
    fn slope_descending() {
        let mut a = RegrSlope::default();
        feed(&mut a, &[i(10), i(8), i(6), i(4), i(2)]);
        assert_f64_close(&a.result(), -2.0, 0.001);
    }
    #[test]
    fn intercept_descending() {
        let mut a = RegrIntercept::default();
        feed(&mut a, &[i(10), i(8), i(6), i(4), i(2)]);
        assert_f64_close(&a.result(), 10.0, 0.001);
    }
    #[test]
    fn slope_double_step() {
        let mut a = RegrSlope::default();
        feed(&mut a, &[i(0), i(2), i(4), i(6)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn intercept_double_step() {
        let mut a = RegrIntercept::default();
        feed(&mut a, &[i(0), i(2), i(4), i(6)]);
        assert_f64_close(&a.result(), 0.0, 0.001);
    }
}

mod bollinger_more_tests {
    use super::*;

    #[test]
    fn upper_above_lower() {
        let values = vec![f(100.0), f(102.0), f(98.0), f(101.0), f(99.0)];
        let mut u = BollingerUpper::new(5, 2.0);
        let mut l = BollingerLower::new(5, 2.0);
        for v in &values {
            u.add(v);
            l.add(v);
        }
        let upper = match u.result() {
            Value::F64(v) => v,
            _ => panic!(),
        };
        let lower = match l.result() {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert!(upper > lower);
    }
    #[test]
    fn bands_symmetric() {
        let mut u = BollingerUpper::new(3, 1.0);
        let mut l = BollingerLower::new(3, 1.0);
        let values = vec![f(10.0), f(10.0), f(10.0)];
        for v in &values {
            u.add(v);
            l.add(v);
        }
        // stddev=0, so both equal mean
        assert_f64_close(&u.result(), 10.0, 0.001);
        assert_f64_close(&l.result(), 10.0, 0.001);
    }
}

mod approx_count_more_tests {
    use super::*;

    #[test]
    fn strings() {
        let mut a = ApproxCountDistinct::default();
        feed(&mut a, &[s("a"), s("b"), s("a")]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn fifty_unique() {
        let mut a = ApproxCountDistinct::default();
        for v in 0..50 {
            a.add(&i(v));
        }
        assert_eq!(a.result(), i(50));
    }
    #[test]
    fn ten_with_dups() {
        let mut a = ApproxCountDistinct::default();
        for v in 0..5 {
            a.add(&i(v));
            a.add(&i(v));
        }
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn floats() {
        let mut a = ApproxCountDistinct::default();
        feed(&mut a, &[f(1.0), f(2.0), f(1.0)]);
        assert_eq!(a.result(), i(2));
    }
}

mod atr_more_tests {
    use super::*;

    #[test]
    fn increasing_constant_step() {
        let mut a = Atr::new(3);
        feed(&mut a, &[f(10.0), f(12.0), f(14.0), f(16.0)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn decreasing_constant_step() {
        let mut a = Atr::new(3);
        feed(&mut a, &[f(20.0), f(18.0), f(16.0), f(14.0)]);
        assert_f64_close(&a.result(), 2.0, 0.001);
    }
    #[test]
    fn zigzag() {
        let mut a = Atr::new(4);
        feed(&mut a, &[f(100.0), f(105.0), f(95.0), f(110.0), f(90.0)]);
        let r = a.result();
        match r {
            Value::F64(v) => assert!(v > 5.0),
            _ => panic!(),
        }
    }
}

mod sma_more_tests {
    use super::*;

    #[test]
    fn period_20_with_50_values() {
        let mut a = Sma::new(20);
        for v in 1..=50 {
            a.add(&f(v as f64));
        }
        // Last 20: 31..=50, avg = 40.5
        assert_f64_close(&a.result(), 40.5, 0.001);
    }
    #[test]
    fn period_3_descending() {
        let mut a = Sma::new(3);
        feed(&mut a, &[f(30.0), f(20.0), f(10.0)]);
        assert_f64_close(&a.result(), 20.0, 0.001);
    }
}

mod drawdown_more_tests {
    use super::*;

    #[test]
    fn gradual_decline() {
        let mut a = Drawdown::default();
        feed(&mut a, &[f(100.0), f(95.0), f(90.0), f(85.0)]);
        assert_f64_close(&a.result(), 0.15, 0.001);
    }
    #[test]
    fn new_high_resets() {
        let mut a = Drawdown::default();
        feed(&mut a, &[f(100.0), f(90.0), f(110.0), f(100.0)]);
        // First dd: 10/100=0.1, second dd: 10/110≈0.0909
        assert_f64_close(&a.result(), 0.1, 0.001);
    }
}

mod final_coverage_tests {
    use super::*;

    #[test]
    fn sum_single_ts() {
        let mut a = Sum::default();
        a.add(&ts(1000));
        assert_eq!(a.result(), i(1000));
    }
    #[test]
    fn covar_pop_three() {
        let mut a = CovarPop::default();
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        assert_f64_close(&a.result(), 2.0 / 3.0, 0.001);
    }
    #[test]
    fn covar_samp_three() {
        let mut a = CovarSamp::default();
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
    #[test]
    fn corr_three() {
        let mut a = Corr::default();
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        assert_f64_close(&a.result(), 1.0, 0.001);
    }
}
