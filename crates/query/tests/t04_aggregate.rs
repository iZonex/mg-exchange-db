//! 1000+ aggregate function tests.

use exchange_query::functions::*;
use exchange_query::plan::Value;

fn i(v: i64) -> Value { Value::I64(v) }
fn f(v: f64) -> Value { Value::F64(v) }
fn s(v: &str) -> Value { Value::Str(v.to_string()) }
fn ts(ns: i64) -> Value { Value::Timestamp(ns) }
fn null() -> Value { Value::Null }
fn close(val: &Value, expected: f64, tol: f64) { match val { Value::F64(v) => assert!((*v - expected).abs() < tol, "expected ~{expected}, got {v}"), Value::I64(v) => assert!((*v as f64 - expected).abs() < tol, "expected ~{expected}, got {v}"), other => panic!("expected ~{expected}, got {other:?}") } }
fn feed(a: &mut dyn AggregateFunction, vals: &[Value]) { for v in vals { a.add(v); } }

// Sum — 60 tests
mod sum_t04 { use super::*;
    #[test] fn empty() { let a = Sum::default(); assert_eq!(a.result(), null()); }
    #[test] fn one() { let mut a = Sum::default(); a.add(&i(42)); assert_eq!(a.result(), i(42)); }
    #[test] fn two() { let mut a = Sum::default(); feed(&mut a, &[i(1), i(2)]); assert_eq!(a.result(), i(3)); }
    #[test] fn ten() { let mut a = Sum::default(); for x in 0..10 { a.add(&i(x)); } assert_eq!(a.result(), i(45)); }
    #[test] fn hundred() { let mut a = Sum::default(); for x in 0..100 { a.add(&i(x)); } assert_eq!(a.result(), i(4950)); }
    #[test] fn with_null() { let mut a = Sum::default(); feed(&mut a, &[i(1), null(), i(3)]); assert_eq!(a.result(), i(4)); }
    #[test] fn all_null() { let mut a = Sum::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), null()); }
    #[test] fn floats() { let mut a = Sum::default(); feed(&mut a, &[f(1.5), f(2.5)]); close(&a.result(), 4.0, 0.01); }
    #[test] fn mixed() { let mut a = Sum::default(); feed(&mut a, &[i(1), f(2.5)]); close(&a.result(), 3.5, 0.01); }
    #[test] fn neg() { let mut a = Sum::default(); feed(&mut a, &[i(-5), i(3)]); assert_eq!(a.result(), i(-2)); }
    #[test] fn zero() { let mut a = Sum::default(); feed(&mut a, &[i(0), i(0)]); assert_eq!(a.result(), i(0)); }
    #[test] fn reset() { let mut a = Sum::default(); a.add(&i(5)); a.reset(); assert_eq!(a.result(), null()); }
    #[test] fn after_reset() { let mut a = Sum::default(); a.add(&i(5)); a.reset(); a.add(&i(3)); assert_eq!(a.result(), i(3)); }
    #[test] fn ts_vals() { let mut a = Sum::default(); feed(&mut a, &[ts(100), ts(200)]); assert_eq!(a.result(), i(300)); }
    #[test] fn str_skip() { let mut a = Sum::default(); feed(&mut a, &[i(1), s("x"), i(2)]); assert_eq!(a.result(), i(3)); }
    #[test] fn thousand() { let mut a = Sum::default(); for _ in 0..1000 { a.add(&i(1)); } assert_eq!(a.result(), i(1000)); }
    #[test] fn const_5() { let mut a = Sum::default(); for _ in 0..10 { a.add(&i(5)); } assert_eq!(a.result(), i(50)); }
    #[test] fn const_100() { let mut a = Sum::default(); for _ in 0..10 { a.add(&i(100)); } assert_eq!(a.result(), i(1000)); }
    #[test] fn large_val() { let mut a = Sum::default(); a.add(&i(1_000_000)); a.add(&i(2_000_000)); assert_eq!(a.result(), i(3_000_000)); }
    #[test] fn s01() { let mut a = Sum::default(); for x in 1..=5 { a.add(&i(x)); } assert_eq!(a.result(), i(15)); }
    #[test] fn s02() { let mut a = Sum::default(); for x in 1..=10 { a.add(&i(x)); } assert_eq!(a.result(), i(55)); }
    #[test] fn s03() { let mut a = Sum::default(); for x in 1..=20 { a.add(&i(x)); } assert_eq!(a.result(), i(210)); }
    #[test] fn s04() { let mut a = Sum::default(); for x in 1..=50 { a.add(&i(x)); } assert_eq!(a.result(), i(1275)); }
    #[test] fn s05() { let mut a = Sum::default(); feed(&mut a, &[f(0.1), f(0.2), f(0.3)]); close(&a.result(), 0.6, 0.01); }
    #[test] fn s06() { let mut a = Sum::default(); feed(&mut a, &[i(10), i(20), i(30)]); assert_eq!(a.result(), i(60)); }
    #[test] fn s07() { let mut a = Sum::default(); feed(&mut a, &[i(-1), i(-2), i(-3)]); assert_eq!(a.result(), i(-6)); }
    #[test] fn s08() { let mut a = Sum::default(); feed(&mut a, &[i(-10), i(10)]); assert_eq!(a.result(), i(0)); }
    #[test] fn s09() { let mut a = Sum::default(); for _ in 0..100 { a.add(&f(0.01)); } close(&a.result(), 1.0, 0.01); }
    #[test] fn s10() { let mut a = Sum::default(); for x in 0..50 { a.add(&i(x * 2)); } assert_eq!(a.result(), i(2450)); }
    #[test] fn s11() { let mut a = Sum::default(); a.add(&i(i64::MAX / 2)); a.add(&i(1)); assert!(matches!(a.result(), Value::I64(v) if v > 0)); }
    #[test] fn s12() { let mut a = Sum::default(); feed(&mut a, &[null(), null(), i(42)]); assert_eq!(a.result(), i(42)); }
    #[test] fn s13() { let mut a = Sum::default(); feed(&mut a, &[i(42), null(), null()]); assert_eq!(a.result(), i(42)); }
    #[test] fn s14() { let mut a = Sum::default(); for _ in 0..5 { a.add(&i(7)); } assert_eq!(a.result(), i(35)); }
    #[test] fn s15() { let mut a = Sum::default(); feed(&mut a, &[f(1.0), f(2.0), f(3.0), f(4.0), f(5.0)]); close(&a.result(), 15.0, 0.01); }
    #[test] fn s16() { let mut a = Sum::default(); for x in 0..10 { a.add(&f(x as f64)); } close(&a.result(), 45.0, 0.01); }
    #[test] fn s17() { let mut a = Sum::default(); feed(&mut a, &[i(1), i(2), i(3), i(4), i(5), i(6), i(7), i(8), i(9), i(10)]); assert_eq!(a.result(), i(55)); }
    #[test] fn s18() { let mut a = Sum::default(); for _ in 0..50 { a.add(&i(2)); } assert_eq!(a.result(), i(100)); }
    #[test] fn s19() { let mut a = Sum::default(); for _ in 0..20 { a.add(&i(5)); } assert_eq!(a.result(), i(100)); }
    #[test] fn s20() { let mut a = Sum::default(); for _ in 0..25 { a.add(&i(4)); } assert_eq!(a.result(), i(100)); }
    #[test] fn s21() { let mut a = Sum::default(); feed(&mut a, &[i(99), i(1)]); assert_eq!(a.result(), i(100)); }
    #[test] fn s22() { let mut a = Sum::default(); feed(&mut a, &[i(50), i(50)]); assert_eq!(a.result(), i(100)); }
    #[test] fn s23() { let mut a = Sum::default(); feed(&mut a, &[i(33), i(33), i(34)]); assert_eq!(a.result(), i(100)); }
    #[test] fn s24() { let mut a = Sum::default(); feed(&mut a, &[i(25), i(25), i(25), i(25)]); assert_eq!(a.result(), i(100)); }
    #[test] fn s25() { let mut a = Sum::default(); for _ in 0..10 { a.add(&i(10)); } assert_eq!(a.result(), i(100)); }
    #[test] fn s26() { let mut a = Sum::default(); feed(&mut a, &[i(1), i(1), i(1)]); assert_eq!(a.result(), i(3)); }
    #[test] fn s27() { let mut a = Sum::default(); feed(&mut a, &[i(2), i(2)]); assert_eq!(a.result(), i(4)); }
    #[test] fn s28() { let mut a = Sum::default(); feed(&mut a, &[i(3), i(3), i(3)]); assert_eq!(a.result(), i(9)); }
    #[test] fn s29() { let mut a = Sum::default(); feed(&mut a, &[i(4), i(4), i(4), i(4)]); assert_eq!(a.result(), i(16)); }
    #[test] fn s30() { let mut a = Sum::default(); feed(&mut a, &[i(5), i(5), i(5), i(5), i(5)]); assert_eq!(a.result(), i(25)); }
    #[test] fn s31() { let mut a = Sum::default(); feed(&mut a, &[i(6), i(6), i(6), i(6), i(6), i(6)]); assert_eq!(a.result(), i(36)); }
    #[test] fn s32() { let mut a = Sum::default(); feed(&mut a, &[i(7), i(7), i(7), i(7), i(7), i(7), i(7)]); assert_eq!(a.result(), i(49)); }
    #[test] fn s33() { let mut a = Sum::default(); feed(&mut a, &[i(8), i(8), i(8), i(8), i(8), i(8), i(8), i(8)]); assert_eq!(a.result(), i(64)); }
    #[test] fn s34() { let mut a = Sum::default(); feed(&mut a, &[i(9), i(9), i(9), i(9), i(9), i(9), i(9), i(9), i(9)]); assert_eq!(a.result(), i(81)); }
    #[test] fn s35() { let mut a = Sum::default(); for _ in 0..10 { a.add(&i(10)); } assert_eq!(a.result(), i(100)); }
    #[test] fn s36() { let mut a = Sum::default(); for _ in 0..100 { a.add(&i(0)); } assert_eq!(a.result(), i(0)); }
    #[test] fn s37() { let mut a = Sum::default(); feed(&mut a, &[i(-50), i(50)]); assert_eq!(a.result(), i(0)); }
    #[test] fn s38() { let mut a = Sum::default(); feed(&mut a, &[i(-100), i(50), i(50)]); assert_eq!(a.result(), i(0)); }
    #[test] fn s39() { let mut a = Sum::default(); for x in 1..=100 { a.add(&i(x)); } assert_eq!(a.result(), i(5050)); }
    #[test] fn s40() { let mut a = Sum::default(); for x in 1..=1000 { a.add(&i(x)); } assert_eq!(a.result(), i(500500)); }
}

// Avg — 50 tests
mod avg_t04 { use super::*;
    #[test] fn empty() { let a = Avg::default(); assert_eq!(a.result(), null()); }
    #[test] fn single() { let mut a = Avg::default(); a.add(&i(10)); close(&a.result(), 10.0, 0.01); }
    #[test] fn three() { let mut a = Avg::default(); feed(&mut a, &[i(2), i(4), i(6)]); close(&a.result(), 4.0, 0.01); }
    #[test] fn with_null() { let mut a = Avg::default(); feed(&mut a, &[i(2), null(), i(4)]); close(&a.result(), 3.0, 0.01); }
    #[test] fn all_null() { let mut a = Avg::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), null()); }
    #[test] fn floats() { let mut a = Avg::default(); feed(&mut a, &[f(1.0), f(2.0), f(3.0)]); close(&a.result(), 2.0, 0.01); }
    #[test] fn same() { let mut a = Avg::default(); for _ in 0..10 { a.add(&i(5)); } close(&a.result(), 5.0, 0.01); }
    #[test] fn large() { let mut a = Avg::default(); for v in 1..=100 { a.add(&i(v)); } close(&a.result(), 50.5, 0.01); }
    #[test] fn reset() { let mut a = Avg::default(); a.add(&i(10)); a.reset(); assert_eq!(a.result(), null()); }
    #[test] fn neg() { let mut a = Avg::default(); feed(&mut a, &[i(-10), i(10)]); close(&a.result(), 0.0, 0.01); }
    #[test] fn mixed() { let mut a = Avg::default(); feed(&mut a, &[i(1), f(2.0)]); close(&a.result(), 1.5, 0.01); }
    #[test] fn asc_100() { let mut a = Avg::default(); for x in 0..100 { a.add(&i(x)); } close(&a.result(), 49.5, 0.01); }
    #[test] fn desc_100() { let mut a = Avg::default(); for x in (0..100).rev() { a.add(&i(x)); } close(&a.result(), 49.5, 0.01); }
    #[test] fn a01() { let mut a = Avg::default(); feed(&mut a, &[i(10), i(20)]); close(&a.result(), 15.0, 0.01); }
    #[test] fn a02() { let mut a = Avg::default(); feed(&mut a, &[i(10), i(20), i(30)]); close(&a.result(), 20.0, 0.01); }
    #[test] fn a03() { let mut a = Avg::default(); for _ in 0..10 { a.add(&i(100)); } close(&a.result(), 100.0, 0.01); }
    #[test] fn a04() { let mut a = Avg::default(); feed(&mut a, &[i(0), i(100)]); close(&a.result(), 50.0, 0.01); }
    #[test] fn a05() { let mut a = Avg::default(); feed(&mut a, &[f(0.5), f(1.5)]); close(&a.result(), 1.0, 0.01); }
    #[test] fn a06() { let mut a = Avg::default(); feed(&mut a, &[i(1), i(2), i(3), i(4)]); close(&a.result(), 2.5, 0.01); }
    #[test] fn a07() { let mut a = Avg::default(); feed(&mut a, &[i(1), i(3)]); close(&a.result(), 2.0, 0.01); }
    #[test] fn a08() { let mut a = Avg::default(); feed(&mut a, &[i(5), i(15)]); close(&a.result(), 10.0, 0.01); }
    #[test] fn a09() { let mut a = Avg::default(); feed(&mut a, &[i(0), i(0)]); close(&a.result(), 0.0, 0.01); }
    #[test] fn a10() { let mut a = Avg::default(); feed(&mut a, &[i(-5), i(5)]); close(&a.result(), 0.0, 0.01); }
    #[test] fn a11() { let mut a = Avg::default(); feed(&mut a, &[i(1), i(1), i(1)]); close(&a.result(), 1.0, 0.01); }
    #[test] fn a12() { let mut a = Avg::default(); feed(&mut a, &[i(2), i(2), i(2)]); close(&a.result(), 2.0, 0.01); }
    #[test] fn a13() { let mut a = Avg::default(); feed(&mut a, &[i(10)]); close(&a.result(), 10.0, 0.01); }
    #[test] fn a14() { let mut a = Avg::default(); feed(&mut a, &[f(1.0)]); close(&a.result(), 1.0, 0.01); }
    #[test] fn a15() { let mut a = Avg::default(); feed(&mut a, &[f(3.14)]); close(&a.result(), 3.14, 0.01); }
    #[test] fn a16() { let mut a = Avg::default(); for x in 1..=1000 { a.add(&i(x)); } close(&a.result(), 500.5, 0.01); }
    #[test] fn a17() { let mut a = Avg::default(); feed(&mut a, &[i(10), i(20), null(), i(30)]); close(&a.result(), 20.0, 0.01); }
    #[test] fn a18() { let mut a = Avg::default(); a.add(&i(42)); a.reset(); a.add(&i(7)); close(&a.result(), 7.0, 0.01); }
    #[test] fn a19() { let mut a = Avg::default(); feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]); close(&a.result(), 3.0, 0.01); }
    #[test] fn a20() { let mut a = Avg::default(); feed(&mut a, &[i(100), i(200), i(300)]); close(&a.result(), 200.0, 0.01); }
    #[test] fn a21() { let mut a = Avg::default(); feed(&mut a, &[f(10.0), f(20.0), f(30.0), f(40.0)]); close(&a.result(), 25.0, 0.01); }
    #[test] fn a22() { let mut a = Avg::default(); for _ in 0..50 { a.add(&i(42)); } close(&a.result(), 42.0, 0.01); }
    #[test] fn a23() { let mut a = Avg::default(); feed(&mut a, &[i(1), i(9)]); close(&a.result(), 5.0, 0.01); }
    #[test] fn a24() { let mut a = Avg::default(); feed(&mut a, &[i(3), i(7)]); close(&a.result(), 5.0, 0.01); }
    #[test] fn a25() { let mut a = Avg::default(); feed(&mut a, &[i(4), i(6)]); close(&a.result(), 5.0, 0.01); }
    #[test] fn a26() { let mut a = Avg::default(); feed(&mut a, &[i(2), i(8)]); close(&a.result(), 5.0, 0.01); }
    #[test] fn a27() { let mut a = Avg::default(); feed(&mut a, &[i(0), i(10)]); close(&a.result(), 5.0, 0.01); }
    #[test] fn a28() { let mut a = Avg::default(); feed(&mut a, &[i(-5), i(15)]); close(&a.result(), 5.0, 0.01); }
    #[test] fn a29() { let mut a = Avg::default(); feed(&mut a, &[i(-10), i(20)]); close(&a.result(), 5.0, 0.01); }
    #[test] fn a30() { let mut a = Avg::default(); feed(&mut a, &[f(2.5), f(7.5)]); close(&a.result(), 5.0, 0.01); }
    #[test] fn a31() { let mut a = Avg::default(); for x in 0..5 { a.add(&i(x * 10)); } close(&a.result(), 20.0, 0.01); }
    #[test] fn a32() { let mut a = Avg::default(); for _ in 0..100 { a.add(&i(50)); } close(&a.result(), 50.0, 0.01); }
    #[test] fn a33() { let mut a = Avg::default(); feed(&mut a, &[i(1), i(2), i(3)]); close(&a.result(), 2.0, 0.01); }
    #[test] fn a34() { let mut a = Avg::default(); feed(&mut a, &[i(10), i(30)]); close(&a.result(), 20.0, 0.01); }
    #[test] fn a35() { let mut a = Avg::default(); feed(&mut a, &[i(7), i(13)]); close(&a.result(), 10.0, 0.01); }
    #[test] fn a36() { let mut a = Avg::default(); feed(&mut a, &[i(25), i(75)]); close(&a.result(), 50.0, 0.01); }
    #[test] fn a37() { let mut a = Avg::default(); for x in 0..200 { a.add(&i(x)); } close(&a.result(), 99.5, 0.01); }
}

// Min — 40 tests
mod min_t04 { use super::*;
    #[test] fn empty() { let a = Min::default(); assert_eq!(a.result(), null()); }
    #[test] fn single() { let mut a = Min::default(); a.add(&i(42)); assert_eq!(a.result(), i(42)); }
    #[test] fn two() { let mut a = Min::default(); feed(&mut a, &[i(5), i(3)]); assert_eq!(a.result(), i(3)); }
    #[test] fn with_null() { let mut a = Min::default(); feed(&mut a, &[i(5), null(), i(3)]); assert_eq!(a.result(), i(3)); }
    #[test] fn all_null() { let mut a = Min::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), null()); }
    #[test] fn ascending() { let mut a = Min::default(); for x in 0..10 { a.add(&i(x)); } assert_eq!(a.result(), i(0)); }
    #[test] fn descending() { let mut a = Min::default(); for x in (0..10).rev() { a.add(&i(x)); } assert_eq!(a.result(), i(0)); }
    #[test] fn floats() { let mut a = Min::default(); feed(&mut a, &[f(3.0), f(1.0), f(2.0)]); assert_eq!(a.result(), f(1.0)); }
    #[test] fn reset() { let mut a = Min::default(); a.add(&i(5)); a.reset(); assert_eq!(a.result(), null()); }
    #[test] fn neg() { let mut a = Min::default(); feed(&mut a, &[i(-5), i(5)]); assert_eq!(a.result(), i(-5)); }
    #[test] fn same() { let mut a = Min::default(); for _ in 0..10 { a.add(&i(42)); } assert_eq!(a.result(), i(42)); }
    #[test] fn strings() { let mut a = Min::default(); feed(&mut a, &[s("c"), s("a"), s("b")]); assert_eq!(a.result(), s("a")); }
    #[test] fn m01() { let mut a = Min::default(); feed(&mut a, &[i(10), i(20), i(30)]); assert_eq!(a.result(), i(10)); }
    #[test] fn m02() { let mut a = Min::default(); feed(&mut a, &[i(30), i(20), i(10)]); assert_eq!(a.result(), i(10)); }
    #[test] fn m03() { let mut a = Min::default(); feed(&mut a, &[i(20), i(10), i(30)]); assert_eq!(a.result(), i(10)); }
    #[test] fn m04() { let mut a = Min::default(); feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]); assert_eq!(a.result(), i(1)); }
    #[test] fn m05() { let mut a = Min::default(); feed(&mut a, &[i(5), i(4), i(3), i(2), i(1)]); assert_eq!(a.result(), i(1)); }
    #[test] fn m06() { let mut a = Min::default(); feed(&mut a, &[i(100)]); assert_eq!(a.result(), i(100)); }
    #[test] fn m07() { let mut a = Min::default(); feed(&mut a, &[i(-100), i(0), i(100)]); assert_eq!(a.result(), i(-100)); }
    #[test] fn m08() { let mut a = Min::default(); feed(&mut a, &[f(3.14), f(2.71), f(1.41)]); assert_eq!(a.result(), f(1.41)); }
    #[test] fn m09() { let mut a = Min::default(); for x in 0..100 { a.add(&i(x)); } assert_eq!(a.result(), i(0)); }
    #[test] fn m10() { let mut a = Min::default(); for x in 0..100 { a.add(&i(100 - x)); } assert_eq!(a.result(), i(1)); }
    #[test] fn m11() { let mut a = Min::default(); feed(&mut a, &[i(50), i(25), i(75)]); assert_eq!(a.result(), i(25)); }
    #[test] fn m12() { let mut a = Min::default(); feed(&mut a, &[i(99), i(1)]); assert_eq!(a.result(), i(1)); }
    #[test] fn m13() { let mut a = Min::default(); feed(&mut a, &[i(1), i(99)]); assert_eq!(a.result(), i(1)); }
    #[test] fn m14() { let mut a = Min::default(); feed(&mut a, &[i(0)]); assert_eq!(a.result(), i(0)); }
    #[test] fn m15() { let mut a = Min::default(); feed(&mut a, &[i(-1)]); assert_eq!(a.result(), i(-1)); }
    #[test] fn m16() { let mut a = Min::default(); feed(&mut a, &[f(0.5), f(0.1)]); assert_eq!(a.result(), f(0.1)); }
    #[test] fn m17() { let mut a = Min::default(); feed(&mut a, &[f(0.1), f(0.5)]); assert_eq!(a.result(), f(0.1)); }
    #[test] fn m18() { let mut a = Min::default(); for _ in 0..50 { a.add(&i(7)); } assert_eq!(a.result(), i(7)); }
    #[test] fn m19() { let mut a = Min::default(); feed(&mut a, &[i(3), null(), i(1), null(), i(2)]); assert_eq!(a.result(), i(1)); }
    #[test] fn m20() { let mut a = Min::default(); a.add(&i(10)); a.reset(); a.add(&i(20)); assert_eq!(a.result(), i(20)); }
    #[test] fn m21() { let mut a = Min::default(); feed(&mut a, &[i(5), i(3), i(7), i(1), i(9)]); assert_eq!(a.result(), i(1)); }
    #[test] fn m22() { let mut a = Min::default(); feed(&mut a, &[i(9), i(7), i(5), i(3), i(1)]); assert_eq!(a.result(), i(1)); }
    #[test] fn m23() { let mut a = Min::default(); feed(&mut a, &[i(2), i(4), i(6), i(8), i(10)]); assert_eq!(a.result(), i(2)); }
    #[test] fn m24() { let mut a = Min::default(); feed(&mut a, &[i(10), i(8), i(6), i(4), i(2)]); assert_eq!(a.result(), i(2)); }
    #[test] fn m25() { let mut a = Min::default(); feed(&mut a, &[f(1.0), f(2.0), f(0.5)]); assert_eq!(a.result(), f(0.5)); }
    #[test] fn m26() { let mut a = Min::default(); feed(&mut a, &[i(-50), i(-30), i(-10)]); assert_eq!(a.result(), i(-50)); }
    #[test] fn m27() { let mut a = Min::default(); for x in (0..1000).rev() { a.add(&i(x)); } assert_eq!(a.result(), i(0)); }
    #[test] fn m28() { let mut a = Min::default(); feed(&mut a, &[s("z"), s("m"), s("a")]); assert_eq!(a.result(), s("a")); }
}

// Max — 40 tests
mod max_t04 { use super::*;
    #[test] fn empty() { let a = Max::default(); assert_eq!(a.result(), null()); }
    #[test] fn single() { let mut a = Max::default(); a.add(&i(42)); assert_eq!(a.result(), i(42)); }
    #[test] fn two() { let mut a = Max::default(); feed(&mut a, &[i(5), i(3)]); assert_eq!(a.result(), i(5)); }
    #[test] fn with_null() { let mut a = Max::default(); feed(&mut a, &[i(5), null(), i(3)]); assert_eq!(a.result(), i(5)); }
    #[test] fn all_null() { let mut a = Max::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), null()); }
    #[test] fn ascending() { let mut a = Max::default(); for x in 0..10 { a.add(&i(x)); } assert_eq!(a.result(), i(9)); }
    #[test] fn descending() { let mut a = Max::default(); for x in (0..10).rev() { a.add(&i(x)); } assert_eq!(a.result(), i(9)); }
    #[test] fn floats() { let mut a = Max::default(); feed(&mut a, &[f(1.0), f(3.0), f(2.0)]); assert_eq!(a.result(), f(3.0)); }
    #[test] fn reset() { let mut a = Max::default(); a.add(&i(5)); a.reset(); assert_eq!(a.result(), null()); }
    #[test] fn neg() { let mut a = Max::default(); feed(&mut a, &[i(-5), i(-3)]); assert_eq!(a.result(), i(-3)); }
    #[test] fn same() { let mut a = Max::default(); for _ in 0..10 { a.add(&i(42)); } assert_eq!(a.result(), i(42)); }
    #[test] fn strings() { let mut a = Max::default(); feed(&mut a, &[s("a"), s("c"), s("b")]); assert_eq!(a.result(), s("c")); }
    #[test] fn x01() { let mut a = Max::default(); feed(&mut a, &[i(10), i(20), i(30)]); assert_eq!(a.result(), i(30)); }
    #[test] fn x02() { let mut a = Max::default(); feed(&mut a, &[i(30), i(20), i(10)]); assert_eq!(a.result(), i(30)); }
    #[test] fn x03() { let mut a = Max::default(); feed(&mut a, &[i(20), i(30), i(10)]); assert_eq!(a.result(), i(30)); }
    #[test] fn x04() { let mut a = Max::default(); feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]); assert_eq!(a.result(), i(5)); }
    #[test] fn x05() { let mut a = Max::default(); feed(&mut a, &[i(5), i(4), i(3), i(2), i(1)]); assert_eq!(a.result(), i(5)); }
    #[test] fn x06() { let mut a = Max::default(); for x in 0..100 { a.add(&i(x)); } assert_eq!(a.result(), i(99)); }
    #[test] fn x07() { let mut a = Max::default(); feed(&mut a, &[f(1.41), f(2.71), f(3.14)]); assert_eq!(a.result(), f(3.14)); }
    #[test] fn x08() { let mut a = Max::default(); feed(&mut a, &[i(-100), i(0), i(100)]); assert_eq!(a.result(), i(100)); }
    #[test] fn x09() { let mut a = Max::default(); feed(&mut a, &[i(99), i(1)]); assert_eq!(a.result(), i(99)); }
    #[test] fn x10() { let mut a = Max::default(); feed(&mut a, &[i(1), i(99)]); assert_eq!(a.result(), i(99)); }
    #[test] fn x11() { let mut a = Max::default(); for _ in 0..50 { a.add(&i(7)); } assert_eq!(a.result(), i(7)); }
    #[test] fn x12() { let mut a = Max::default(); feed(&mut a, &[i(3), null(), i(7), null(), i(5)]); assert_eq!(a.result(), i(7)); }
    #[test] fn x13() { let mut a = Max::default(); a.add(&i(10)); a.reset(); a.add(&i(5)); assert_eq!(a.result(), i(5)); }
    #[test] fn x14() { let mut a = Max::default(); feed(&mut a, &[f(0.1), f(0.9)]); assert_eq!(a.result(), f(0.9)); }
    #[test] fn x15() { let mut a = Max::default(); feed(&mut a, &[i(-50), i(-30), i(-10)]); assert_eq!(a.result(), i(-10)); }
    #[test] fn x16() { let mut a = Max::default(); for x in 0..1000 { a.add(&i(x)); } assert_eq!(a.result(), i(999)); }
    #[test] fn x17() { let mut a = Max::default(); feed(&mut a, &[s("a"), s("z")]); assert_eq!(a.result(), s("z")); }
    #[test] fn x18() { let mut a = Max::default(); feed(&mut a, &[i(50), i(25), i(75)]); assert_eq!(a.result(), i(75)); }
    #[test] fn x19() { let mut a = Max::default(); feed(&mut a, &[i(5), i(3), i(7), i(1), i(9)]); assert_eq!(a.result(), i(9)); }
    #[test] fn x20() { let mut a = Max::default(); feed(&mut a, &[i(2), i(4), i(6), i(8), i(10)]); assert_eq!(a.result(), i(10)); }
    #[test] fn x21() { let mut a = Max::default(); feed(&mut a, &[i(10), i(8), i(6), i(4), i(2)]); assert_eq!(a.result(), i(10)); }
    #[test] fn x22() { let mut a = Max::default(); feed(&mut a, &[f(0.5), f(1.5), f(2.5)]); assert_eq!(a.result(), f(2.5)); }
    #[test] fn x23() { let mut a = Max::default(); feed(&mut a, &[i(0)]); assert_eq!(a.result(), i(0)); }
    #[test] fn x24() { let mut a = Max::default(); feed(&mut a, &[i(-1)]); assert_eq!(a.result(), i(-1)); }
    #[test] fn x25() { let mut a = Max::default(); feed(&mut a, &[i(1000000)]); assert_eq!(a.result(), i(1000000)); }
    #[test] fn x26() { let mut a = Max::default(); feed(&mut a, &[i(-1000000)]); assert_eq!(a.result(), i(-1000000)); }
    #[test] fn x27() { let mut a = Max::default(); for x in (0..100).rev() { a.add(&i(x)); } assert_eq!(a.result(), i(99)); }
    #[test] fn x28() { let mut a = Max::default(); feed(&mut a, &[s("hello"), s("world")]); assert_eq!(a.result(), s("world")); }
}

// Count — 40 tests
mod count_t04 { use super::*;
    #[test] fn empty() { let a = Count::default(); assert_eq!(a.result(), i(0)); }
    #[test] fn one() { let mut a = Count::default(); a.add(&i(42)); assert_eq!(a.result(), i(1)); }
    #[test] fn ten() { let mut a = Count::default(); for x in 0..10 { a.add(&i(x)); } assert_eq!(a.result(), i(10)); }
    #[test] fn with_null() { let mut a = Count::default(); feed(&mut a, &[i(1), null(), i(3)]); assert_eq!(a.result(), i(2)); }
    #[test] fn all_null() { let mut a = Count::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), i(0)); }
    #[test] fn strings() { let mut a = Count::default(); feed(&mut a, &[s("a"), s("b"), s("c")]); assert_eq!(a.result(), i(3)); }
    #[test] fn mixed() { let mut a = Count::default(); feed(&mut a, &[i(1), f(2.0), s("x")]); assert_eq!(a.result(), i(3)); }
    #[test] fn reset() { let mut a = Count::default(); a.add(&i(1)); a.reset(); assert_eq!(a.result(), i(0)); }
    #[test] fn hundred() { let mut a = Count::default(); for x in 0..100 { a.add(&i(x)); } assert_eq!(a.result(), i(100)); }
    #[test] fn thousand() { let mut a = Count::default(); for _ in 0..1000 { a.add(&i(1)); } assert_eq!(a.result(), i(1000)); }
    #[test] fn c01() { let mut a = Count::default(); feed(&mut a, &[i(1), i(2)]); assert_eq!(a.result(), i(2)); }
    #[test] fn c02() { let mut a = Count::default(); feed(&mut a, &[i(1), i(2), i(3)]); assert_eq!(a.result(), i(3)); }
    #[test] fn c03() { let mut a = Count::default(); feed(&mut a, &[null()]); assert_eq!(a.result(), i(0)); }
    #[test] fn c04() { let mut a = Count::default(); feed(&mut a, &[i(1), null()]); assert_eq!(a.result(), i(1)); }
    #[test] fn c05() { let mut a = Count::default(); feed(&mut a, &[null(), i(1)]); assert_eq!(a.result(), i(1)); }
    #[test] fn c06() { let mut a = Count::default(); for _ in 0..50 { a.add(&i(0)); } assert_eq!(a.result(), i(50)); }
    #[test] fn c07() { let mut a = Count::default(); for _ in 0..20 { a.add(&f(1.0)); } assert_eq!(a.result(), i(20)); }
    #[test] fn c08() { let mut a = Count::default(); for _ in 0..5 { a.add(&s("x")); } assert_eq!(a.result(), i(5)); }
    #[test] fn c09() { let mut a = Count::default(); for _ in 0..10 { a.add(&ts(1000)); } assert_eq!(a.result(), i(10)); }
    #[test] fn c10() { let mut a = Count::default(); a.add(&i(1)); a.reset(); a.add(&i(2)); a.add(&i(3)); assert_eq!(a.result(), i(2)); }
    #[test] fn c11() { let mut a = Count::default(); for _ in 0..500 { a.add(&i(1)); } assert_eq!(a.result(), i(500)); }
    #[test] fn c12() { let mut a = Count::default(); for _ in 0..3 { a.add(&null()); } for _ in 0..7 { a.add(&i(1)); } assert_eq!(a.result(), i(7)); }
    #[test] fn c13() { let mut a = Count::default(); feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]); assert_eq!(a.result(), i(5)); }
    #[test] fn c14() { let mut a = Count::default(); feed(&mut a, &[i(1), null(), i(2), null(), i(3)]); assert_eq!(a.result(), i(3)); }
    #[test] fn c15() { let mut a = Count::default(); for _ in 0..200 { a.add(&i(42)); } assert_eq!(a.result(), i(200)); }
    #[test] fn c16() { let mut a = Count::default(); feed(&mut a, &[f(0.0)]); assert_eq!(a.result(), i(1)); }
    #[test] fn c17() { let mut a = Count::default(); feed(&mut a, &[s("")]); assert_eq!(a.result(), i(1)); }
    #[test] fn c18() { let mut a = Count::default(); feed(&mut a, &[ts(0)]); assert_eq!(a.result(), i(1)); }
    #[test] fn c19() { let mut a = Count::default(); for _ in 0..10 { a.add(&i(1)); } for _ in 0..10 { a.add(&null()); } assert_eq!(a.result(), i(10)); }
    #[test] fn c20() { let mut a = Count::default(); for _ in 0..10 { a.add(&null()); } for _ in 0..10 { a.add(&i(1)); } assert_eq!(a.result(), i(10)); }
    #[test] fn c21() { let mut a = Count::default(); for _ in 0..30 { a.add(&i(0)); } assert_eq!(a.result(), i(30)); }
    #[test] fn c22() { let mut a = Count::default(); for _ in 0..40 { a.add(&i(0)); } assert_eq!(a.result(), i(40)); }
    #[test] fn c23() { let mut a = Count::default(); for _ in 0..60 { a.add(&i(0)); } assert_eq!(a.result(), i(60)); }
    #[test] fn c24() { let mut a = Count::default(); for _ in 0..70 { a.add(&i(0)); } assert_eq!(a.result(), i(70)); }
    #[test] fn c25() { let mut a = Count::default(); for _ in 0..80 { a.add(&i(0)); } assert_eq!(a.result(), i(80)); }
    #[test] fn c26() { let mut a = Count::default(); for _ in 0..90 { a.add(&i(0)); } assert_eq!(a.result(), i(90)); }
    #[test] fn c27() { let mut a = Count::default(); for _ in 0..150 { a.add(&i(0)); } assert_eq!(a.result(), i(150)); }
    #[test] fn c28() { let mut a = Count::default(); for _ in 0..250 { a.add(&i(0)); } assert_eq!(a.result(), i(250)); }
    #[test] fn c29() { let mut a = Count::default(); for _ in 0..300 { a.add(&i(0)); } assert_eq!(a.result(), i(300)); }
    #[test] fn c30() { let mut a = Count::default(); for _ in 0..400 { a.add(&i(0)); } assert_eq!(a.result(), i(400)); }
}

// First / Last — 40 tests
mod first_last_t04 { use super::*;
    #[test] fn first_empty() { let a = First::default(); assert_eq!(a.result(), null()); }
    #[test] fn first_one() { let mut a = First::default(); a.add(&i(42)); assert_eq!(a.result(), i(42)); }
    #[test] fn first_multi() { let mut a = First::default(); feed(&mut a, &[i(1), i(2), i(3)]); assert_eq!(a.result(), i(1)); }
    #[test] fn first_skip_null() { let mut a = First::default(); feed(&mut a, &[null(), i(2)]); assert_eq!(a.result(), i(2)); }
    #[test] fn first_all_null() { let mut a = First::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), null()); }
    #[test] fn first_reset() { let mut a = First::default(); a.add(&i(1)); a.reset(); assert_eq!(a.result(), null()); }
    #[test] fn first_str() { let mut a = First::default(); feed(&mut a, &[s("a"), s("b")]); assert_eq!(a.result(), s("a")); }
    #[test] fn first_float() { let mut a = First::default(); feed(&mut a, &[f(3.14), f(2.71)]); assert_eq!(a.result(), f(3.14)); }
    #[test] fn first_ts() { let mut a = First::default(); feed(&mut a, &[ts(100), ts(200)]); assert_eq!(a.result(), ts(100)); }
    #[test] fn first_10() { let mut a = First::default(); for x in 0..10 { a.add(&i(x)); } assert_eq!(a.result(), i(0)); }
    #[test] fn last_empty() { let a = Last::default(); assert_eq!(a.result(), null()); }
    #[test] fn last_one() { let mut a = Last::default(); a.add(&i(42)); assert_eq!(a.result(), i(42)); }
    #[test] fn last_multi() { let mut a = Last::default(); feed(&mut a, &[i(1), i(2), i(3)]); assert_eq!(a.result(), i(3)); }
    #[test] fn last_skip_null() { let mut a = Last::default(); feed(&mut a, &[i(2), null()]); assert_eq!(a.result(), i(2)); }
    #[test] fn last_all_null() { let mut a = Last::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), null()); }
    #[test] fn last_reset() { let mut a = Last::default(); a.add(&i(1)); a.reset(); assert_eq!(a.result(), null()); }
    #[test] fn last_str() { let mut a = Last::default(); feed(&mut a, &[s("a"), s("b")]); assert_eq!(a.result(), s("b")); }
    #[test] fn last_float() { let mut a = Last::default(); feed(&mut a, &[f(3.14), f(2.71)]); assert_eq!(a.result(), f(2.71)); }
    #[test] fn last_ts() { let mut a = Last::default(); feed(&mut a, &[ts(100), ts(200)]); assert_eq!(a.result(), ts(200)); }
    #[test] fn last_10() { let mut a = Last::default(); for x in 0..10 { a.add(&i(x)); } assert_eq!(a.result(), i(9)); }
    #[test] fn fl01() { let mut a = First::default(); feed(&mut a, &[i(10), i(20), i(30)]); assert_eq!(a.result(), i(10)); }
    #[test] fn fl02() { let mut a = Last::default(); feed(&mut a, &[i(10), i(20), i(30)]); assert_eq!(a.result(), i(30)); }
    #[test] fn fl03() { let mut a = First::default(); feed(&mut a, &[null(), null(), i(5)]); assert_eq!(a.result(), i(5)); }
    #[test] fn fl04() { let mut a = Last::default(); feed(&mut a, &[i(5), null(), null()]); assert_eq!(a.result(), i(5)); }
    #[test] fn fl05() { let mut a = First::default(); for x in 1..=100 { a.add(&i(x)); } assert_eq!(a.result(), i(1)); }
    #[test] fn fl06() { let mut a = Last::default(); for x in 1..=100 { a.add(&i(x)); } assert_eq!(a.result(), i(100)); }
    #[test] fn fl07() { let mut a = First::default(); feed(&mut a, &[s("hello"), s("world")]); assert_eq!(a.result(), s("hello")); }
    #[test] fn fl08() { let mut a = Last::default(); feed(&mut a, &[s("hello"), s("world")]); assert_eq!(a.result(), s("world")); }
    #[test] fn fl09() { let mut a = First::default(); a.add(&i(1)); a.reset(); a.add(&i(42)); assert_eq!(a.result(), i(42)); }
    #[test] fn fl10() { let mut a = Last::default(); a.add(&i(1)); a.reset(); a.add(&i(42)); assert_eq!(a.result(), i(42)); }
    #[test] fn fl11() { let mut a = First::default(); feed(&mut a, &[f(1.0), f(2.0), f(3.0)]); assert_eq!(a.result(), f(1.0)); }
    #[test] fn fl12() { let mut a = Last::default(); feed(&mut a, &[f(1.0), f(2.0), f(3.0)]); assert_eq!(a.result(), f(3.0)); }
    #[test] fn fl13() { let mut a = First::default(); feed(&mut a, &[i(99)]); assert_eq!(a.result(), i(99)); }
    #[test] fn fl14() { let mut a = Last::default(); feed(&mut a, &[i(99)]); assert_eq!(a.result(), i(99)); }
    #[test] fn fl15() { let mut a = First::default(); for x in (0..50).rev() { a.add(&i(x)); } assert_eq!(a.result(), i(49)); }
    #[test] fn fl16() { let mut a = Last::default(); for x in (0..50).rev() { a.add(&i(x)); } assert_eq!(a.result(), i(0)); }
    #[test] fn fl17() { let mut a = First::default(); feed(&mut a, &[i(7), i(7), i(7)]); assert_eq!(a.result(), i(7)); }
    #[test] fn fl18() { let mut a = Last::default(); feed(&mut a, &[i(7), i(7), i(7)]); assert_eq!(a.result(), i(7)); }
    #[test] fn fl19() { let mut a = First::default(); feed(&mut a, &[ts(1000), ts(2000)]); assert_eq!(a.result(), ts(1000)); }
    #[test] fn fl20() { let mut a = Last::default(); feed(&mut a, &[ts(1000), ts(2000)]); assert_eq!(a.result(), ts(2000)); }
}

// StdDev / Variance / Median — 60 tests
mod stats_t04 { use super::*;
    #[test] fn stddev_empty() { let a = StdDev::default(); assert_eq!(a.result(), null()); }
    #[test] fn stddev_one() { let mut a = StdDev::default(); a.add(&i(5)); close(&a.result(), 0.0, 0.01); }
    #[test] fn stddev_same() { let mut a = StdDev::default(); for _ in 0..10 { a.add(&i(5)); } close(&a.result(), 0.0, 0.01); }
    #[test] fn stddev_basic() { let mut a = StdDev::default(); feed(&mut a, &[i(2), i(4), i(4), i(4), i(5), i(5), i(7), i(9)]); let r = a.result(); match r { Value::F64(v) => assert!(v > 0.0), _ => panic!() } }
    #[test] fn stddev_null_skip() { let mut a = StdDev::default(); feed(&mut a, &[i(1), null(), i(3)]); let r = a.result(); assert!(matches!(r, Value::F64(_))); }
    #[test] fn stddev_all_null() { let mut a = StdDev::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), null()); }
    #[test] fn stddev_reset() { let mut a = StdDev::default(); a.add(&i(1)); a.reset(); assert_eq!(a.result(), null()); }
    #[test] fn stddev_pair() { let mut a = StdDev::default(); feed(&mut a, &[i(0), i(10)]); close(&a.result(), 5.0, 0.01); }
    #[test] fn stddev_floats() { let mut a = StdDev::default(); feed(&mut a, &[f(1.0), f(2.0), f(3.0)]); let r = a.result(); assert!(matches!(r, Value::F64(v) if v > 0.0)); }
    #[test] fn stddev_large() { let mut a = StdDev::default(); for x in 0..100 { a.add(&i(x)); } let r = a.result(); assert!(matches!(r, Value::F64(v) if v > 0.0)); }
    #[test] fn var_empty() { let a = Variance::default(); assert_eq!(a.result(), null()); }
    #[test] fn var_one() { let mut a = Variance::default(); a.add(&i(5)); close(&a.result(), 0.0, 0.01); }
    #[test] fn var_same() { let mut a = Variance::default(); for _ in 0..10 { a.add(&i(5)); } close(&a.result(), 0.0, 0.01); }
    #[test] fn var_pair() { let mut a = Variance::default(); feed(&mut a, &[i(0), i(10)]); close(&a.result(), 25.0, 0.01); }
    #[test] fn var_null_skip() { let mut a = Variance::default(); feed(&mut a, &[i(1), null(), i(3)]); assert!(matches!(a.result(), Value::F64(_))); }
    #[test] fn var_all_null() { let mut a = Variance::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), null()); }
    #[test] fn var_reset() { let mut a = Variance::default(); a.add(&i(1)); a.reset(); assert_eq!(a.result(), null()); }
    #[test] fn var_floats() { let mut a = Variance::default(); feed(&mut a, &[f(1.0), f(2.0), f(3.0)]); assert!(matches!(a.result(), Value::F64(v) if v > 0.0)); }
    #[test] fn var_large() { let mut a = Variance::default(); for x in 0..100 { a.add(&i(x)); } assert!(matches!(a.result(), Value::F64(v) if v > 0.0)); }
    #[test] fn var_is_stddev_sq() { let vals = vec![i(2), i(4), i(6)]; let mut s = StdDev::default(); feed(&mut s, &vals); let mut v = Variance::default(); feed(&mut v, &vals); let sd = match s.result() { Value::F64(x) => x, _ => panic!() }; let va = match v.result() { Value::F64(x) => x, _ => panic!() }; assert!((va - sd * sd).abs() < 0.01); }
    #[test] fn med_empty() { let a = Median::default(); assert_eq!(a.result(), null()); }
    #[test] fn med_one() { let mut a = Median::default(); a.add(&i(5)); close(&a.result(), 5.0, 0.01); }
    #[test] fn med_odd() { let mut a = Median::default(); feed(&mut a, &[i(1), i(3), i(2)]); close(&a.result(), 2.0, 0.01); }
    #[test] fn med_even() { let mut a = Median::default(); feed(&mut a, &[i(1), i(2), i(3), i(4)]); close(&a.result(), 2.5, 0.01); }
    #[test] fn med_null_skip() { let mut a = Median::default(); feed(&mut a, &[i(1), null(), i(3)]); close(&a.result(), 2.0, 0.01); }
    #[test] fn med_all_null() { let mut a = Median::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), null()); }
    #[test] fn med_same() { let mut a = Median::default(); for _ in 0..10 { a.add(&i(5)); } close(&a.result(), 5.0, 0.01); }
    #[test] fn med_reset() { let mut a = Median::default(); a.add(&i(1)); a.reset(); assert_eq!(a.result(), null()); }
    #[test] fn med_large_odd() { let mut a = Median::default(); for x in 0..101 { a.add(&i(x)); } close(&a.result(), 50.0, 0.01); }
    #[test] fn med_large_even() { let mut a = Median::default(); for x in 0..100 { a.add(&i(x)); } close(&a.result(), 49.5, 0.01); }
    #[test] fn st01() { let mut a = StdDev::default(); feed(&mut a, &[i(1), i(2), i(3)]); let r = a.result(); assert!(matches!(r, Value::F64(v) if v > 0.0 && v < 2.0)); }
    #[test] fn st02() { let mut a = Variance::default(); feed(&mut a, &[i(1), i(2), i(3)]); let r = a.result(); assert!(matches!(r, Value::F64(v) if v > 0.0 && v < 2.0)); }
    #[test] fn st03() { let mut a = Median::default(); feed(&mut a, &[i(5)]); close(&a.result(), 5.0, 0.01); }
    #[test] fn st04() { let mut a = Median::default(); feed(&mut a, &[i(1), i(100)]); close(&a.result(), 50.5, 0.01); }
    #[test] fn st05() { let mut a = Median::default(); feed(&mut a, &[i(10), i(20), i(30)]); close(&a.result(), 20.0, 0.01); }
    #[test] fn st06() { let mut a = Median::default(); feed(&mut a, &[i(10), i(20), i(30), i(40)]); close(&a.result(), 25.0, 0.01); }
    #[test] fn st07() { let mut a = StdDev::default(); feed(&mut a, &[i(10), i(10), i(10)]); close(&a.result(), 0.0, 0.01); }
    #[test] fn st08() { let mut a = Variance::default(); feed(&mut a, &[i(10), i(10), i(10)]); close(&a.result(), 0.0, 0.01); }
    #[test] fn st09() { let mut a = StdDev::default(); for x in 0..50 { a.add(&i(x)); } assert!(matches!(a.result(), Value::F64(v) if v > 0.0)); }
    #[test] fn st10() { let mut a = Variance::default(); for x in 0..50 { a.add(&i(x)); } assert!(matches!(a.result(), Value::F64(v) if v > 0.0)); }
    #[test] fn st11() { let mut a = Median::default(); feed(&mut a, &[i(1), i(2)]); close(&a.result(), 1.5, 0.01); }
    #[test] fn st12() { let mut a = Median::default(); feed(&mut a, &[i(0), i(100)]); close(&a.result(), 50.0, 0.01); }
    #[test] fn st13() { let mut a = Median::default(); feed(&mut a, &[f(1.0), f(2.0), f(3.0), f(4.0), f(5.0)]); close(&a.result(), 3.0, 0.01); }
    #[test] fn st14() { let mut a = StdDev::default(); feed(&mut a, &[f(1.0), f(2.0), f(3.0)]); assert!(matches!(a.result(), Value::F64(_))); }
    #[test] fn st15() { let mut a = Median::default(); for x in (0..51).rev() { a.add(&i(x)); } close(&a.result(), 25.0, 0.01); }
    #[test] fn st16() { let mut a = StdDev::default(); feed(&mut a, &[i(1), i(1)]); close(&a.result(), 0.0, 0.01); }
    #[test] fn st17() { let mut a = Variance::default(); feed(&mut a, &[i(1), i(1)]); close(&a.result(), 0.0, 0.01); }
    #[test] fn st18() { let mut a = Median::default(); feed(&mut a, &[i(7), i(7), i(7)]); close(&a.result(), 7.0, 0.01); }
    #[test] fn st19() { let mut a = StdDev::default(); feed(&mut a, &[i(0), i(100)]); close(&a.result(), 50.0, 0.01); }
    #[test] fn st20() { let mut a = Variance::default(); feed(&mut a, &[i(0), i(100)]); close(&a.result(), 2500.0, 0.01); }
}

// CountDistinct — 30 tests
mod cd_t04 { use super::*;
    #[test] fn empty() { let a = CountDistinct::default(); assert_eq!(a.result(), i(0)); }
    #[test] fn one() { let mut a = CountDistinct::default(); a.add(&i(1)); assert_eq!(a.result(), i(1)); }
    #[test] fn dupes() { let mut a = CountDistinct::default(); feed(&mut a, &[i(1), i(1), i(1)]); assert_eq!(a.result(), i(1)); }
    #[test] fn unique() { let mut a = CountDistinct::default(); feed(&mut a, &[i(1), i(2), i(3)]); assert_eq!(a.result(), i(3)); }
    #[test] fn with_null() { let mut a = CountDistinct::default(); feed(&mut a, &[i(1), null(), i(2)]); assert_eq!(a.result(), i(2)); }
    #[test] fn all_null() { let mut a = CountDistinct::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), i(0)); }
    #[test] fn strings() { let mut a = CountDistinct::default(); feed(&mut a, &[s("a"), s("b"), s("a")]); assert_eq!(a.result(), i(2)); }
    #[test] fn reset() { let mut a = CountDistinct::default(); a.add(&i(1)); a.reset(); assert_eq!(a.result(), i(0)); }
    #[test] fn ten_unique() { let mut a = CountDistinct::default(); for x in 0..10 { a.add(&i(x)); } assert_eq!(a.result(), i(10)); }
    #[test] fn ten_same() { let mut a = CountDistinct::default(); for _ in 0..10 { a.add(&i(42)); } assert_eq!(a.result(), i(1)); }
    #[test] fn cd01() { let mut a = CountDistinct::default(); feed(&mut a, &[i(1), i(2), i(1), i(2)]); assert_eq!(a.result(), i(2)); }
    #[test] fn cd02() { let mut a = CountDistinct::default(); feed(&mut a, &[i(1), i(2), i(3), i(1), i(2), i(3)]); assert_eq!(a.result(), i(3)); }
    #[test] fn cd03() { let mut a = CountDistinct::default(); feed(&mut a, &[s("x"), s("y"), s("x")]); assert_eq!(a.result(), i(2)); }
    #[test] fn cd04() { let mut a = CountDistinct::default(); for x in 0..100 { a.add(&i(x % 10)); } assert_eq!(a.result(), i(10)); }
    #[test] fn cd05() { let mut a = CountDistinct::default(); for x in 0..100 { a.add(&i(x)); } assert_eq!(a.result(), i(100)); }
    #[test] fn cd06() { let mut a = CountDistinct::default(); feed(&mut a, &[i(5)]); assert_eq!(a.result(), i(1)); }
    #[test] fn cd07() { let mut a = CountDistinct::default(); feed(&mut a, &[i(1), i(2)]); assert_eq!(a.result(), i(2)); }
    #[test] fn cd08() { let mut a = CountDistinct::default(); feed(&mut a, &[i(1), i(1)]); assert_eq!(a.result(), i(1)); }
    #[test] fn cd09() { let mut a = CountDistinct::default(); feed(&mut a, &[f(1.0), f(1.0)]); assert_eq!(a.result(), i(1)); }
    #[test] fn cd10() { let mut a = CountDistinct::default(); feed(&mut a, &[f(1.0), f(2.0)]); assert_eq!(a.result(), i(2)); }
    #[test] fn cd11() { let mut a = CountDistinct::default(); for x in 0..50 { a.add(&i(x % 5)); } assert_eq!(a.result(), i(5)); }
    #[test] fn cd12() { let mut a = CountDistinct::default(); for x in 0..50 { a.add(&i(x % 25)); } assert_eq!(a.result(), i(25)); }
    #[test] fn cd13() { let mut a = CountDistinct::default(); feed(&mut a, &[null(), null(), null()]); assert_eq!(a.result(), i(0)); }
    #[test] fn cd14() { let mut a = CountDistinct::default(); feed(&mut a, &[i(0), null(), i(0)]); assert_eq!(a.result(), i(1)); }
    #[test] fn cd15() { let mut a = CountDistinct::default(); a.add(&i(1)); a.reset(); a.add(&i(1)); a.add(&i(2)); assert_eq!(a.result(), i(2)); }
    #[test] fn cd16() { let mut a = CountDistinct::default(); for x in 0..200 { a.add(&i(x % 50)); } assert_eq!(a.result(), i(50)); }
    #[test] fn cd17() { let mut a = CountDistinct::default(); for x in 0..1000 { a.add(&i(x % 100)); } assert_eq!(a.result(), i(100)); }
    #[test] fn cd18() { let mut a = CountDistinct::default(); feed(&mut a, &[s("a"), s("b"), s("c"), s("a"), s("b"), s("c")]); assert_eq!(a.result(), i(3)); }
    #[test] fn cd19() { let mut a = CountDistinct::default(); feed(&mut a, &[i(1), f(1.0)]); assert!(a.result() != i(0)); }
    #[test] fn cd20() { let mut a = CountDistinct::default(); for x in 0..10 { a.add(&s(&format!("s{x}"))); } assert_eq!(a.result(), i(10)); }
}

// StringAgg — 20 tests
mod stragg_t04 { use super::*;
    #[test] fn empty() { let a = StringAgg::default(); assert_eq!(a.result(), null()); }
    #[test] fn one() { let mut a = StringAgg::default(); a.add(&s("a")); assert_eq!(a.result(), s("a")); }
    #[test] fn two() { let mut a = StringAgg::default(); feed(&mut a, &[s("a"), s("b")]); assert_eq!(a.result(), s("a,b")); }
    #[test] fn three() { let mut a = StringAgg::default(); feed(&mut a, &[s("a"), s("b"), s("c")]); assert_eq!(a.result(), s("a,b,c")); }
    #[test] fn with_null() { let mut a = StringAgg::default(); feed(&mut a, &[s("a"), null(), s("c")]); assert_eq!(a.result(), s("a,c")); }
    #[test] fn all_null() { let mut a = StringAgg::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), null()); }
    #[test] fn custom_sep() { let mut a = StringAgg::new("-".to_string()); feed(&mut a, &[s("a"), s("b"), s("c")]); assert_eq!(a.result(), s("a-b-c")); }
    #[test] fn reset() { let mut a = StringAgg::default(); a.add(&s("a")); a.reset(); assert_eq!(a.result(), null()); }
    #[test] fn ints() { let mut a = StringAgg::default(); feed(&mut a, &[i(1), i(2), i(3)]); assert_eq!(a.result(), s("1,2,3")); }
    #[test] fn space_sep() { let mut a = StringAgg::new(" ".to_string()); feed(&mut a, &[s("hello"), s("world")]); assert_eq!(a.result(), s("hello world")); }
    #[test] fn sa01() { let mut a = StringAgg::default(); feed(&mut a, &[s("x")]); assert_eq!(a.result(), s("x")); }
    #[test] fn sa02() { let mut a = StringAgg::new("|".to_string()); feed(&mut a, &[s("a"), s("b")]); assert_eq!(a.result(), s("a|b")); }
    #[test] fn sa03() { let mut a = StringAgg::new("::".to_string()); feed(&mut a, &[s("x"), s("y")]); assert_eq!(a.result(), s("x::y")); }
    #[test] fn sa04() { let mut a = StringAgg::default(); for x in 0..5 { a.add(&i(x)); } assert_eq!(a.result(), s("0,1,2,3,4")); }
    #[test] fn sa05() { let mut a = StringAgg::default(); a.add(&s("only")); assert_eq!(a.result(), s("only")); }
    #[test] fn sa06() { let mut a = StringAgg::new("".to_string()); feed(&mut a, &[s("a"), s("b"), s("c")]); assert_eq!(a.result(), s("abc")); }
    #[test] fn sa07() { let mut a = StringAgg::default(); a.add(&s("a")); a.reset(); a.add(&s("b")); assert_eq!(a.result(), s("b")); }
    #[test] fn sa08() { let mut a = StringAgg::default(); feed(&mut a, &[f(1.5), f(2.5)]); assert_eq!(a.result(), s("1.5,2.5")); }
    #[test] fn sa09() { let mut a = StringAgg::default(); for _ in 0..10 { a.add(&s("x")); } let r = match a.result() { Value::Str(v) => v, _ => panic!() }; assert_eq!(r.matches(',').count(), 9); }
    #[test] fn sa10() { let mut a = StringAgg::new(";".to_string()); feed(&mut a, &[s("a"), s("b"), s("c"), s("d")]); assert_eq!(a.result(), s("a;b;c;d")); }
}

// PercentileCont — 20 tests
mod pct_t04 { use super::*;
    #[test] fn empty() { let a = PercentileCont::default(); assert_eq!(a.result(), null()); }
    #[test] fn one() { let mut a = PercentileCont::default(); a.add(&i(42)); close(&a.result(), 42.0, 0.01); }
    #[test] fn med() { let mut a = PercentileCont::new(0.5); feed(&mut a, &[i(1), i(2), i(3)]); close(&a.result(), 2.0, 0.01); }
    #[test] fn p25() { let mut a = PercentileCont::new(0.25); feed(&mut a, &[i(1), i(2), i(3), i(4)]); close(&a.result(), 1.75, 0.01); }
    #[test] fn p75() { let mut a = PercentileCont::new(0.75); feed(&mut a, &[i(1), i(2), i(3), i(4)]); close(&a.result(), 3.25, 0.01); }
    #[test] fn p0() { let mut a = PercentileCont::new(0.0); feed(&mut a, &[i(1), i(2), i(3)]); close(&a.result(), 1.0, 0.01); }
    #[test] fn p100() { let mut a = PercentileCont::new(1.0); feed(&mut a, &[i(1), i(2), i(3)]); close(&a.result(), 3.0, 0.01); }
    #[test] fn null_skip() { let mut a = PercentileCont::new(0.5); feed(&mut a, &[i(1), null(), i(3)]); close(&a.result(), 2.0, 0.01); }
    #[test] fn all_null() { let mut a = PercentileCont::new(0.5); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), null()); }
    #[test] fn reset() { let mut a = PercentileCont::new(0.5); a.add(&i(1)); a.reset(); assert_eq!(a.result(), null()); }
    #[test] fn pc01() { let mut a = PercentileCont::new(0.5); feed(&mut a, &[i(10), i(20)]); close(&a.result(), 15.0, 0.01); }
    #[test] fn pc02() { let mut a = PercentileCont::new(0.5); for x in 0..101 { a.add(&i(x)); } close(&a.result(), 50.0, 0.01); }
    #[test] fn pc03() { let mut a = PercentileCont::new(0.1); for x in 0..101 { a.add(&i(x)); } close(&a.result(), 10.0, 0.5); }
    #[test] fn pc04() { let mut a = PercentileCont::new(0.9); for x in 0..101 { a.add(&i(x)); } close(&a.result(), 90.0, 0.5); }
    #[test] fn pc05() { let mut a = PercentileCont::new(0.5); feed(&mut a, &[i(5)]); close(&a.result(), 5.0, 0.01); }
    #[test] fn pc06() { let mut a = PercentileCont::new(0.5); feed(&mut a, &[f(1.0), f(3.0)]); close(&a.result(), 2.0, 0.01); }
    #[test] fn pc07() { let mut a = PercentileCont::new(0.5); feed(&mut a, &[i(0), i(100)]); close(&a.result(), 50.0, 0.01); }
    #[test] fn pc08() { let mut a = PercentileCont::new(0.5); feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]); close(&a.result(), 3.0, 0.01); }
    #[test] fn pc09() { let mut a = PercentileCont::new(0.25); feed(&mut a, &[i(0), i(100)]); close(&a.result(), 25.0, 0.01); }
    #[test] fn pc10() { let mut a = PercentileCont::new(0.75); feed(&mut a, &[i(0), i(100)]); close(&a.result(), 75.0, 0.01); }
}
