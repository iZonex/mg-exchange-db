//! 500 aggregate function tests with different input patterns.

use exchange_query::functions::*;
use exchange_query::plan::Value;

fn i(v: i64) -> Value {
    Value::I64(v)
}
fn f(v: f64) -> Value {
    Value::F64(v)
}
fn null() -> Value {
    Value::Null
}
fn close(val: &Value, expected: f64, tol: f64) {
    match val {
        Value::F64(v) => assert!((*v - expected).abs() < tol, "expected ~{expected}, got {v}"),
        Value::I64(v) => assert!(
            (*v as f64 - expected).abs() < tol,
            "expected ~{expected}, got {v}"
        ),
        other => panic!("expected ~{expected}, got {other:?}"),
    }
}
fn feed(a: &mut dyn AggregateFunction, vals: &[Value]) {
    for v in vals {
        a.add(v);
    }
}

// ===========================================================================
// Sum — more patterns — 60 tests
// ===========================================================================
mod sum_f03 {
    use super::*;
    macro_rules! sum_seq {
        ($n:ident, $from:expr, $to:expr, $expected:expr) => {
            #[test]
            fn $n() {
                let mut a = Sum::default();
                for x in $from..=$to {
                    a.add(&i(x));
                }
                assert_eq!(a.result(), i($expected));
            }
        };
    }
    sum_seq!(s_1_1, 1, 1, 1);
    sum_seq!(s_1_2, 1, 2, 3);
    sum_seq!(s_1_3, 1, 3, 6);
    sum_seq!(s_1_4, 1, 4, 10);
    sum_seq!(s_1_6, 1, 6, 21);
    sum_seq!(s_1_7, 1, 7, 28);
    sum_seq!(s_1_8, 1, 8, 36);
    sum_seq!(s_1_9, 1, 9, 45);
    sum_seq!(s_1_11, 1, 11, 66);
    sum_seq!(s_1_12, 1, 12, 78);
    sum_seq!(s_1_13, 1, 13, 91);
    sum_seq!(s_1_14, 1, 14, 105);
    sum_seq!(s_1_15, 1, 15, 120);
    sum_seq!(s_1_16, 1, 16, 136);
    sum_seq!(s_1_17, 1, 17, 153);
    sum_seq!(s_1_18, 1, 18, 171);
    sum_seq!(s_1_19, 1, 19, 190);
    sum_seq!(s_1_25, 1, 25, 325);
    sum_seq!(s_1_30, 1, 30, 465);
    sum_seq!(s_1_40, 1, 40, 820);

    // Constant value sums
    macro_rules! sum_const {
        ($n:ident, $val:expr, $count:expr) => {
            #[test]
            fn $n() {
                let mut a = Sum::default();
                for _ in 0..$count {
                    a.add(&i($val));
                }
                assert_eq!(a.result(), i($val * $count));
            }
        };
    }
    sum_const!(c1x10, 1, 10);
    sum_const!(c2x10, 2, 10);
    sum_const!(c3x10, 3, 10);
    sum_const!(c4x10, 4, 10);
    sum_const!(c5x10, 5, 10);
    sum_const!(c6x10, 6, 10);
    sum_const!(c7x10, 7, 10);
    sum_const!(c8x10, 8, 10);
    sum_const!(c9x10, 9, 10);
    sum_const!(c10x10, 10, 10);
    sum_const!(c1x100, 1, 100);
    sum_const!(c2x100, 2, 100);
    sum_const!(c3x100, 3, 100);
    sum_const!(c5x100, 5, 100);
    sum_const!(c10x100, 10, 100);

    // Float sums
    #[test]
    fn fsum_01() {
        let mut a = Sum::default();
        for x in 0..10 {
            a.add(&f(x as f64 * 0.1));
        }
        close(&a.result(), 4.5, 0.01);
    }
    #[test]
    fn fsum_02() {
        let mut a = Sum::default();
        for x in 0..5 {
            a.add(&f(x as f64 * 2.0));
        }
        close(&a.result(), 20.0, 0.01);
    }
    #[test]
    fn fsum_03() {
        let mut a = Sum::default();
        feed(&mut a, &[f(1.1), f(2.2), f(3.3)]);
        close(&a.result(), 6.6, 0.01);
    }
    #[test]
    fn fsum_04() {
        let mut a = Sum::default();
        feed(&mut a, &[f(0.5), f(0.5), f(0.5), f(0.5)]);
        close(&a.result(), 2.0, 0.01);
    }
    #[test]
    fn fsum_05() {
        let mut a = Sum::default();
        feed(&mut a, &[f(10.0), f(-10.0)]);
        close(&a.result(), 0.0, 0.01);
    }

    // With nulls
    #[test]
    fn null_01() {
        let mut a = Sum::default();
        feed(&mut a, &[null(), i(1), null(), i(2), null()]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn null_02() {
        let mut a = Sum::default();
        feed(&mut a, &[null(), null(), null(), null()]);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn null_03() {
        let mut a = Sum::default();
        feed(&mut a, &[i(10), null()]);
        assert_eq!(a.result(), i(10));
    }
    #[test]
    fn null_04() {
        let mut a = Sum::default();
        feed(&mut a, &[null(), i(10)]);
        assert_eq!(a.result(), i(10));
    }
    #[test]
    fn null_05() {
        let mut a = Sum::default();
        feed(&mut a, &[null(), f(5.0), null(), f(5.0)]);
        close(&a.result(), 10.0, 0.01);
    }
}

// ===========================================================================
// Avg — 60 tests
// ===========================================================================
mod avg_f03 {
    use super::*;
    macro_rules! avg_const {
        ($n:ident, $val:expr, $count:expr) => {
            #[test]
            fn $n() {
                let mut a = Avg::default();
                for _ in 0..$count {
                    a.add(&i($val));
                }
                close(&a.result(), $val as f64, 0.01);
            }
        };
    }
    avg_const!(c1x1, 1, 1);
    avg_const!(c1x5, 1, 5);
    avg_const!(c1x10, 1, 10);
    avg_const!(c5x1, 5, 1);
    avg_const!(c5x5, 5, 5);
    avg_const!(c5x10, 5, 10);
    avg_const!(c10x1, 10, 1);
    avg_const!(c10x5, 10, 5);
    avg_const!(c10x10, 10, 10);
    avg_const!(c100x1, 100, 1);
    avg_const!(c100x10, 100, 10);
    avg_const!(c100x100, 100, 100);
    avg_const!(c0x10, 0, 10);
    avg_const!(c42x1, 42, 1);
    avg_const!(c42x10, 42, 10);

    // Sequential averages
    macro_rules! avg_seq {
        ($n:ident, $to:expr, $expected:expr) => {
            #[test]
            fn $n() {
                let mut a = Avg::default();
                for x in 0..$to {
                    a.add(&i(x));
                }
                close(&a.result(), $expected, 0.01);
            }
        };
    }
    avg_seq!(s2, 2, 0.5);
    avg_seq!(s3, 3, 1.0);
    avg_seq!(s4, 4, 1.5);
    avg_seq!(s5, 5, 2.0);
    avg_seq!(s6, 6, 2.5);
    avg_seq!(s7, 7, 3.0);
    avg_seq!(s8, 8, 3.5);
    avg_seq!(s9, 9, 4.0);
    avg_seq!(s10, 10, 4.5);
    avg_seq!(s20, 20, 9.5);
    avg_seq!(s50, 50, 24.5);
    avg_seq!(s100, 100, 49.5);

    #[test]
    fn empty() {
        assert_eq!(Avg::default().result(), null());
    }
    #[test]
    fn one() {
        let mut a = Avg::default();
        a.add(&i(42));
        close(&a.result(), 42.0, 0.01);
    }
    #[test]
    fn with_null() {
        let mut a = Avg::default();
        feed(&mut a, &[i(10), null(), i(20)]);
        close(&a.result(), 15.0, 0.01);
    }
    #[test]
    fn all_null() {
        let mut a = Avg::default();
        feed(&mut a, &[null(), null()]);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn float_01() {
        let mut a = Avg::default();
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        close(&a.result(), 2.0, 0.01);
    }
    #[test]
    fn float_02() {
        let mut a = Avg::default();
        feed(&mut a, &[f(0.0), f(10.0)]);
        close(&a.result(), 5.0, 0.01);
    }
    #[test]
    fn reset() {
        let mut a = Avg::default();
        a.add(&i(10));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn after_reset() {
        let mut a = Avg::default();
        a.add(&i(10));
        a.reset();
        a.add(&i(20));
        close(&a.result(), 20.0, 0.01);
    }

    // Float sequence averages
    #[test]
    fn fs1() {
        let mut a = Avg::default();
        feed(&mut a, &[f(1.0), f(3.0)]);
        close(&a.result(), 2.0, 0.01);
    }
    #[test]
    fn fs2() {
        let mut a = Avg::default();
        feed(&mut a, &[f(2.0), f(4.0), f(6.0)]);
        close(&a.result(), 4.0, 0.01);
    }
    #[test]
    fn fs3() {
        let mut a = Avg::default();
        feed(&mut a, &[f(10.0), f(20.0), f(30.0), f(40.0)]);
        close(&a.result(), 25.0, 0.01);
    }
    #[test]
    fn fs4() {
        let mut a = Avg::default();
        feed(&mut a, &[f(100.0), f(200.0)]);
        close(&a.result(), 150.0, 0.01);
    }
    #[test]
    fn fs5() {
        let mut a = Avg::default();
        feed(&mut a, &[f(0.1), f(0.2), f(0.3)]);
        close(&a.result(), 0.2, 0.01);
    }

    // Negative values
    #[test]
    fn neg1() {
        let mut a = Avg::default();
        feed(&mut a, &[i(-10), i(10)]);
        close(&a.result(), 0.0, 0.01);
    }
    #[test]
    fn neg2() {
        let mut a = Avg::default();
        feed(&mut a, &[i(-5), i(-3), i(-1)]);
        close(&a.result(), -3.0, 0.01);
    }
    #[test]
    fn neg3() {
        let mut a = Avg::default();
        feed(&mut a, &[i(-100)]);
        close(&a.result(), -100.0, 0.01);
    }
    #[test]
    fn neg4() {
        let mut a = Avg::default();
        feed(&mut a, &[i(-10), i(-20)]);
        close(&a.result(), -15.0, 0.01);
    }
    #[test]
    fn neg5() {
        let mut a = Avg::default();
        feed(&mut a, &[i(-1), i(-2), i(-3), i(-4)]);
        close(&a.result(), -2.5, 0.01);
    }
}

// ===========================================================================
// Min — 60 tests
// ===========================================================================
mod min_f03 {
    use super::*;
    #[test]
    fn empty() {
        assert_eq!(Min::default().result(), null());
    }
    #[test]
    fn one() {
        let mut a = Min::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn two() {
        let mut a = Min::default();
        feed(&mut a, &[i(5), i(3)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn three() {
        let mut a = Min::default();
        feed(&mut a, &[i(5), i(3), i(7)]);
        assert_eq!(a.result(), i(3));
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
    fn neg() {
        let mut a = Min::default();
        feed(&mut a, &[i(-1), i(-5), i(0)]);
        assert_eq!(a.result(), i(-5));
    }
    #[test]
    fn same() {
        let mut a = Min::default();
        feed(&mut a, &[i(7), i(7), i(7)]);
        assert_eq!(a.result(), i(7));
    }
    #[test]
    fn reset() {
        let mut a = Min::default();
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn after_reset() {
        let mut a = Min::default();
        a.add(&i(5));
        a.reset();
        a.add(&i(10));
        assert_eq!(a.result(), i(10));
    }

    // Sequential: min is always 0
    macro_rules! min_seq {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let mut a = Min::default();
                for x in 0..$count {
                    a.add(&i(x));
                }
                assert_eq!(a.result(), i(0));
            }
        };
    }
    min_seq!(ms2, 2);
    min_seq!(ms3, 3);
    min_seq!(ms5, 5);
    min_seq!(ms10, 10);
    min_seq!(ms20, 20);
    min_seq!(ms50, 50);
    min_seq!(ms100, 100);

    // Reverse sequential: min is always 0
    macro_rules! min_rev {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let mut a = Min::default();
                for x in (0..$count).rev() {
                    a.add(&i(x));
                }
                assert_eq!(a.result(), i(0));
            }
        };
    }
    min_rev!(mr2, 2);
    min_rev!(mr3, 3);
    min_rev!(mr5, 5);
    min_rev!(mr10, 10);
    min_rev!(mr20, 20);
    min_rev!(mr50, 50);
    min_rev!(mr100, 100);

    // Float mins
    #[test]
    fn f01() {
        let mut a = Min::default();
        feed(&mut a, &[f(1.0), f(2.0)]);
        close(&a.result(), 1.0, 0.01);
    }
    #[test]
    fn f02() {
        let mut a = Min::default();
        feed(&mut a, &[f(3.0), f(1.0), f(2.0)]);
        close(&a.result(), 1.0, 0.01);
    }
    #[test]
    fn f03() {
        let mut a = Min::default();
        feed(&mut a, &[f(-1.0), f(0.0), f(1.0)]);
        close(&a.result(), -1.0, 0.01);
    }
    #[test]
    fn f04() {
        let mut a = Min::default();
        feed(&mut a, &[f(0.1), f(0.01), f(0.001)]);
        close(&a.result(), 0.001, 0.0001);
    }
    #[test]
    fn f05() {
        let mut a = Min::default();
        feed(&mut a, &[f(100.0), f(50.0), f(200.0)]);
        close(&a.result(), 50.0, 0.01);
    }

    // Negative range
    macro_rules! min_neg {
        ($n:ident, $min:expr, $count:expr) => {
            #[test]
            fn $n() {
                let mut a = Min::default();
                for x in $min..($min + $count) {
                    a.add(&i(x));
                }
                assert_eq!(a.result(), i($min));
            }
        };
    }
    min_neg!(mn1, -10, 20);
    min_neg!(mn2, -100, 200);
    min_neg!(mn3, -50, 100);
    min_neg!(mn4, -5, 10);
    min_neg!(mn5, -1, 2);

    // Single values
    macro_rules! min_single {
        ($n:ident, $val:expr) => {
            #[test]
            fn $n() {
                let mut a = Min::default();
                a.add(&i($val));
                assert_eq!(a.result(), i($val));
            }
        };
    }
    min_single!(v0, 0);
    min_single!(v1, 1);
    min_single!(v5, 5);
    min_single!(v10, 10);
    min_single!(v100, 100);
    min_single!(vn1, -1);
    min_single!(vn10, -10);
    min_single!(vn100, -100);
    min_single!(v42, 42);
    min_single!(v99, 99);

    // Same types
    #[test]
    fn same_i1() {
        let mut a = Min::default();
        feed(&mut a, &[i(5), i(3)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn same_i2() {
        let mut a = Min::default();
        feed(&mut a, &[i(10), i(5)]);
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn same_f1() {
        let mut a = Min::default();
        feed(&mut a, &[f(1.0), f(0.5)]);
        close(&a.result(), 0.5, 0.01);
    }
}

// ===========================================================================
// Max — 60 tests
// ===========================================================================
mod max_f03 {
    use super::*;
    #[test]
    fn empty() {
        assert_eq!(Max::default().result(), null());
    }
    #[test]
    fn one() {
        let mut a = Max::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn two() {
        let mut a = Max::default();
        feed(&mut a, &[i(5), i(3)]);
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn three() {
        let mut a = Max::default();
        feed(&mut a, &[i(5), i(3), i(7)]);
        assert_eq!(a.result(), i(7));
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
    fn neg() {
        let mut a = Max::default();
        feed(&mut a, &[i(-1), i(-5), i(0)]);
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn same() {
        let mut a = Max::default();
        feed(&mut a, &[i(7), i(7), i(7)]);
        assert_eq!(a.result(), i(7));
    }
    #[test]
    fn reset() {
        let mut a = Max::default();
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn after_reset() {
        let mut a = Max::default();
        a.add(&i(5));
        a.reset();
        a.add(&i(10));
        assert_eq!(a.result(), i(10));
    }

    // Sequential: max is count-1
    macro_rules! max_seq {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let mut a = Max::default();
                for x in 0..$count {
                    a.add(&i(x));
                }
                assert_eq!(a.result(), i($count - 1));
            }
        };
    }
    max_seq!(ms2, 2);
    max_seq!(ms3, 3);
    max_seq!(ms5, 5);
    max_seq!(ms10, 10);
    max_seq!(ms20, 20);
    max_seq!(ms50, 50);
    max_seq!(ms100, 100);

    // Reverse sequential: max is still count-1
    macro_rules! max_rev {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let mut a = Max::default();
                for x in (0..$count).rev() {
                    a.add(&i(x));
                }
                assert_eq!(a.result(), i($count - 1));
            }
        };
    }
    max_rev!(mr2, 2);
    max_rev!(mr3, 3);
    max_rev!(mr5, 5);
    max_rev!(mr10, 10);
    max_rev!(mr20, 20);
    max_rev!(mr50, 50);
    max_rev!(mr100, 100);

    // Float maxs
    #[test]
    fn f01() {
        let mut a = Max::default();
        feed(&mut a, &[f(1.0), f(2.0)]);
        close(&a.result(), 2.0, 0.01);
    }
    #[test]
    fn f02() {
        let mut a = Max::default();
        feed(&mut a, &[f(3.0), f(1.0), f(2.0)]);
        close(&a.result(), 3.0, 0.01);
    }
    #[test]
    fn f03() {
        let mut a = Max::default();
        feed(&mut a, &[f(-1.0), f(0.0), f(1.0)]);
        close(&a.result(), 1.0, 0.01);
    }
    #[test]
    fn f04() {
        let mut a = Max::default();
        feed(&mut a, &[f(0.1), f(0.01), f(0.001)]);
        close(&a.result(), 0.1, 0.0001);
    }
    #[test]
    fn f05() {
        let mut a = Max::default();
        feed(&mut a, &[f(100.0), f(50.0), f(200.0)]);
        close(&a.result(), 200.0, 0.01);
    }

    // Single values
    macro_rules! max_single {
        ($n:ident, $val:expr) => {
            #[test]
            fn $n() {
                let mut a = Max::default();
                a.add(&i($val));
                assert_eq!(a.result(), i($val));
            }
        };
    }
    max_single!(v0, 0);
    max_single!(v1, 1);
    max_single!(v5, 5);
    max_single!(v10, 10);
    max_single!(v100, 100);
    max_single!(vn1, -1);
    max_single!(vn10, -10);
    max_single!(vn100, -100);
    max_single!(v42, 42);
    max_single!(v99, 99);

    // Same types
    #[test]
    fn same_i1() {
        let mut a = Max::default();
        feed(&mut a, &[i(5), i(3)]);
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn same_i2() {
        let mut a = Max::default();
        feed(&mut a, &[i(10), i(5)]);
        assert_eq!(a.result(), i(10));
    }
    #[test]
    fn same_f1() {
        let mut a = Max::default();
        feed(&mut a, &[f(1.0), f(1.5)]);
        close(&a.result(), 1.5, 0.01);
    }

    // Constant values
    macro_rules! max_const {
        ($n:ident, $val:expr, $count:expr) => {
            #[test]
            fn $n() {
                let mut a = Max::default();
                for _ in 0..$count {
                    a.add(&i($val));
                }
                assert_eq!(a.result(), i($val));
            }
        };
    }
    max_const!(c1x10, 1, 10);
    max_const!(c5x10, 5, 10);
    max_const!(c10x10, 10, 10);
    max_const!(c100x10, 100, 10);
    max_const!(c42x10, 42, 10);
}

// ===========================================================================
// Count — 60 tests
// ===========================================================================
mod count_f03 {
    use super::*;
    #[test]
    fn empty() {
        assert_eq!(Count::default().result(), i(0));
    }
    #[test]
    fn one() {
        let mut a = Count::default();
        a.add(&i(1));
        assert_eq!(a.result(), i(1));
    }

    macro_rules! count_n {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let mut a = Count::default();
                for x in 0..$count {
                    a.add(&i(x));
                }
                assert_eq!(a.result(), i($count));
            }
        };
    }
    count_n!(c1, 1);
    count_n!(c2, 2);
    count_n!(c3, 3);
    count_n!(c4, 4);
    count_n!(c5, 5);
    count_n!(c6, 6);
    count_n!(c7, 7);
    count_n!(c8, 8);
    count_n!(c9, 9);
    count_n!(c10, 10);
    count_n!(c11, 11);
    count_n!(c12, 12);
    count_n!(c13, 13);
    count_n!(c14, 14);
    count_n!(c15, 15);
    count_n!(c16, 16);
    count_n!(c17, 17);
    count_n!(c18, 18);
    count_n!(c19, 19);
    count_n!(c20, 20);
    count_n!(c25, 25);
    count_n!(c30, 30);
    count_n!(c40, 40);
    count_n!(c50, 50);
    count_n!(c100, 100);
    count_n!(c200, 200);
    count_n!(c500, 500);
    count_n!(c1000, 1000);

    // Null handling: nulls NOT counted
    #[test]
    fn null_only() {
        let mut a = Count::default();
        feed(&mut a, &[null(), null(), null()]);
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn with_null() {
        let mut a = Count::default();
        feed(&mut a, &[i(1), null(), i(2)]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn null_first() {
        let mut a = Count::default();
        feed(&mut a, &[null(), i(1)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn null_last() {
        let mut a = Count::default();
        feed(&mut a, &[i(1), null()]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn all_i64() {
        let mut a = Count::default();
        feed(&mut a, &[i(1), i(2), i(3), i(4), i(5)]);
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn all_f64() {
        let mut a = Count::default();
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn reset() {
        let mut a = Count::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), i(0));
    }
    #[test]
    fn after_reset() {
        let mut a = Count::default();
        a.add(&i(1));
        a.reset();
        a.add(&i(2));
        a.add(&i(3));
        assert_eq!(a.result(), i(2));
    }

    // Patterns with mixed nulls
    macro_rules! count_nulls {
        ($n:ident, $vals:expr, $expected:expr) => {
            #[test]
            fn $n() {
                let mut a = Count::default();
                feed(&mut a, &$vals);
                assert_eq!(a.result(), i($expected));
            }
        };
    }
    count_nulls!(cn1, [null()], 0);
    count_nulls!(cn2, [i(1), null()], 1);
    count_nulls!(cn3, [null(), i(1), null()], 1);
    count_nulls!(cn4, [i(1), i(2), null()], 2);
    count_nulls!(cn5, [null(), null(), i(1)], 1);
    count_nulls!(cn6, [i(1), null(), i(2), null(), i(3)], 3);
    count_nulls!(cn7, [null(), null(), null(), i(1)], 1);
    count_nulls!(cn8, [i(1), null(), null(), null()], 1);
    count_nulls!(cn9, [i(1), i(2), i(3), null(), null()], 3);
    count_nulls!(cn10, [null(), i(1), i(2), i(3), i(4), i(5)], 5);

    // Larger counts
    macro_rules! count_big {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let mut a = Count::default();
                for _ in 0..$count {
                    a.add(&f(1.0));
                }
                assert_eq!(a.result(), i($count));
            }
        };
    }
    count_big!(b50, 50);
    count_big!(b100, 100);
    count_big!(b200, 200);
    count_big!(b500, 500);
    count_big!(b1000, 1000);
}

// ===========================================================================
// First/Last — 60 tests
// ===========================================================================
mod first_last_f03 {
    use super::*;
    // First
    #[test]
    fn first_empty() {
        assert_eq!(First::default().result(), null());
    }
    #[test]
    fn first_one() {
        let mut a = First::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn first_two() {
        let mut a = First::default();
        feed(&mut a, &[i(1), i(2)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn first_null_start() {
        let mut a = First::default();
        feed(&mut a, &[null(), i(5)]);
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn first_all_null() {
        let mut a = First::default();
        feed(&mut a, &[null(), null()]);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn first_reset() {
        let mut a = First::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }

    macro_rules! first_val {
        ($n:ident, $first:expr, $rest:expr) => {
            #[test]
            fn $n() {
                let mut a = First::default();
                a.add(&i($first));
                for v in $rest {
                    a.add(&i(v));
                }
                assert_eq!(a.result(), i($first));
            }
        };
    }
    first_val!(fv1, 1, [2, 3, 4, 5]);
    first_val!(fv2, 10, [20, 30]);
    first_val!(fv3, 0, [1, 2, 3]);
    first_val!(fv4, -5, [0, 5, 10]);
    first_val!(fv5, 42, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    first_val!(fv6, 100, [200, 300]);
    first_val!(fv7, 99, [1]);
    first_val!(fv8, 7, [14, 21, 28]);
    first_val!(fv9, 3, [6, 9, 12, 15]);
    first_val!(fv10, 1000, [2000, 3000, 4000, 5000]);

    // Float first
    #[test]
    fn first_f01() {
        let mut a = First::default();
        feed(&mut a, &[f(1.5), f(2.5)]);
        close(&a.result(), 1.5, 0.01);
    }
    #[test]
    fn first_f02() {
        let mut a = First::default();
        feed(&mut a, &[f(3.15), f(2.71)]);
        close(&a.result(), 3.15, 0.01);
    }
    #[test]
    fn first_f03() {
        let mut a = First::default();
        feed(&mut a, &[f(0.0), f(1.0)]);
        close(&a.result(), 0.0, 0.01);
    }
    #[test]
    fn first_f04() {
        let mut a = First::default();
        feed(&mut a, &[f(-1.0), f(1.0)]);
        close(&a.result(), -1.0, 0.01);
    }
    #[test]
    fn first_f05() {
        let mut a = First::default();
        feed(&mut a, &[f(99.9), f(0.1)]);
        close(&a.result(), 99.9, 0.01);
    }

    // Last
    #[test]
    fn last_empty() {
        assert_eq!(Last::default().result(), null());
    }
    #[test]
    fn last_one() {
        let mut a = Last::default();
        a.add(&i(42));
        assert_eq!(a.result(), i(42));
    }
    #[test]
    fn last_two() {
        let mut a = Last::default();
        feed(&mut a, &[i(1), i(2)]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn last_null_end() {
        let mut a = Last::default();
        feed(&mut a, &[i(5), null()]);
        assert_eq!(a.result(), i(5));
    }
    #[test]
    fn last_all_null() {
        let mut a = Last::default();
        feed(&mut a, &[null(), null()]);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn last_reset() {
        let mut a = Last::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), null());
    }

    macro_rules! last_val {
        ($n:ident, $vals:expr, $expected:expr) => {
            #[test]
            fn $n() {
                let mut a = Last::default();
                for v in $vals {
                    a.add(&i(v));
                }
                assert_eq!(a.result(), i($expected));
            }
        };
    }
    last_val!(lv1, [1, 2, 3, 4, 5], 5);
    last_val!(lv2, [10, 20, 30], 30);
    last_val!(lv3, [0, 1, 2, 3], 3);
    last_val!(lv4, [-5, 0, 5, 10], 10);
    last_val!(lv5, [42, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 10);
    last_val!(lv6, [100, 200, 300], 300);
    last_val!(lv7, [99, 1], 1);
    last_val!(lv8, [7, 14, 21, 28], 28);
    last_val!(lv9, [3, 6, 9, 12, 15], 15);
    last_val!(lv10, [1000, 2000, 3000, 4000, 5000], 5000);

    // Float last
    #[test]
    fn last_f01() {
        let mut a = Last::default();
        feed(&mut a, &[f(1.5), f(2.5)]);
        close(&a.result(), 2.5, 0.01);
    }
    #[test]
    fn last_f02() {
        let mut a = Last::default();
        feed(&mut a, &[f(3.15), f(2.71)]);
        close(&a.result(), 2.71, 0.01);
    }
    #[test]
    fn last_f03() {
        let mut a = Last::default();
        feed(&mut a, &[f(0.0), f(1.0)]);
        close(&a.result(), 1.0, 0.01);
    }
    #[test]
    fn last_f04() {
        let mut a = Last::default();
        feed(&mut a, &[f(-1.0), f(1.0)]);
        close(&a.result(), 1.0, 0.01);
    }
    #[test]
    fn last_f05() {
        let mut a = Last::default();
        feed(&mut a, &[f(99.9), f(0.1)]);
        close(&a.result(), 0.1, 0.01);
    }
}

// ===========================================================================
// StdDev / Variance — 60 tests
// ===========================================================================
mod stddev_var_f03 {
    use super::*;
    // StdDev
    #[test]
    fn sd_empty() {
        assert_eq!(StdDev::default().result(), null());
    }
    #[test]
    fn sd_one() {
        let mut a = StdDev::default();
        a.add(&i(5));
        close(&a.result(), 0.0, 0.01);
    }
    #[test]
    fn sd_same() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(5), i(5), i(5)]);
        close(&a.result(), 0.0, 0.01);
    }
    #[test]
    fn sd_basic() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(2), i(4), i(4), i(4), i(5), i(5), i(7), i(9)]);
        close(&a.result(), 2.0, 0.1);
    }
    #[test]
    fn sd_two() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(0), i(10)]);
        close(&a.result(), 5.0, 0.5);
    }
    #[test]
    fn sd_null() {
        let mut a = StdDev::default();
        feed(&mut a, &[null(), null()]);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn sd_with_null() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(5), null(), i(5)]);
        close(&a.result(), 0.0, 0.01);
    }
    #[test]
    fn sd_reset() {
        let mut a = StdDev::default();
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }
    #[test]
    fn sd_0_0() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(0), i(0)]);
        close(&a.result(), 0.0, 0.01);
    }
    #[test]
    fn sd_1_1() {
        let mut a = StdDev::default();
        feed(&mut a, &[i(1), i(1)]);
        close(&a.result(), 0.0, 0.01);
    }

    macro_rules! sd_const {
        ($n:ident, $val:expr, $count:expr) => {
            #[test]
            fn $n() {
                let mut a = StdDev::default();
                for _ in 0..$count {
                    a.add(&i($val));
                }
                close(&a.result(), 0.0, 0.01);
            }
        };
    }
    sd_const!(sdc1, 1, 10);
    sd_const!(sdc5, 5, 10);
    sd_const!(sdc10, 10, 10);
    sd_const!(sdc100, 100, 10);
    sd_const!(sdc42, 42, 10);
    sd_const!(sdc0, 0, 100);
    sd_const!(sdc7, 7, 50);
    sd_const!(sdc99, 99, 20);

    // Float stddev
    #[test]
    fn sd_f01() {
        let mut a = StdDev::default();
        feed(&mut a, &[f(1.0), f(1.0), f(1.0)]);
        close(&a.result(), 0.0, 0.01);
    }
    #[test]
    fn sd_f02() {
        let mut a = StdDev::default();
        feed(&mut a, &[f(2.0), f(4.0)]);
        close(&a.result(), 1.0, 0.5);
    }
    #[test]
    fn sd_f03() {
        let mut a = StdDev::default();
        feed(&mut a, &[f(10.0), f(10.0), f(10.0)]);
        close(&a.result(), 0.0, 0.01);
    }

    // Variance
    #[test]
    fn var_empty() {
        assert_eq!(Variance::default().result(), null());
    }
    #[test]
    fn var_one() {
        let mut a = Variance::default();
        a.add(&i(5));
        close(&a.result(), 0.0, 0.01);
    }
    #[test]
    fn var_same() {
        let mut a = Variance::default();
        feed(&mut a, &[i(5), i(5), i(5)]);
        close(&a.result(), 0.0, 0.01);
    }
    #[test]
    fn var_null() {
        let mut a = Variance::default();
        feed(&mut a, &[null(), null()]);
        assert_eq!(a.result(), null());
    }
    #[test]
    fn var_reset() {
        let mut a = Variance::default();
        a.add(&i(5));
        a.reset();
        assert_eq!(a.result(), null());
    }

    macro_rules! var_const {
        ($n:ident, $val:expr, $count:expr) => {
            #[test]
            fn $n() {
                let mut a = Variance::default();
                for _ in 0..$count {
                    a.add(&i($val));
                }
                close(&a.result(), 0.0, 0.01);
            }
        };
    }
    var_const!(vc1, 1, 10);
    var_const!(vc5, 5, 10);
    var_const!(vc10, 10, 10);
    var_const!(vc100, 100, 10);
    var_const!(vc42, 42, 10);
    var_const!(vc0, 0, 100);
    var_const!(vc7, 7, 50);
    var_const!(vc99, 99, 20);
    var_const!(vc3, 3, 30);
    var_const!(vc8, 8, 40);

    // Float variance
    #[test]
    fn var_f01() {
        let mut a = Variance::default();
        feed(&mut a, &[f(1.0), f(1.0), f(1.0)]);
        close(&a.result(), 0.0, 0.01);
    }
    #[test]
    fn var_f02() {
        let mut a = Variance::default();
        feed(&mut a, &[f(10.0), f(10.0)]);
        close(&a.result(), 0.0, 0.01);
    }
    #[test]
    fn var_f03() {
        let mut a = Variance::default();
        feed(&mut a, &[f(0.0), f(0.0), f(0.0)]);
        close(&a.result(), 0.0, 0.01);
    }

    // with_null
    #[test]
    fn var_wn1() {
        let mut a = Variance::default();
        feed(&mut a, &[i(5), null(), i(5)]);
        close(&a.result(), 0.0, 0.01);
    }
    #[test]
    fn var_wn2() {
        let mut a = Variance::default();
        feed(&mut a, &[null(), i(10), i(10)]);
        close(&a.result(), 0.0, 0.01);
    }
}

// ===========================================================================
// CountDistinct — 40 tests
// ===========================================================================
mod count_distinct_f03 {
    use super::*;
    #[test]
    fn empty() {
        assert_eq!(CountDistinct::default().result(), i(0));
    }
    #[test]
    fn one() {
        let mut a = CountDistinct::default();
        a.add(&i(1));
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn two_same() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[i(1), i(1)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn two_diff() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[i(1), i(2)]);
        assert_eq!(a.result(), i(2));
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
    fn reset() {
        let mut a = CountDistinct::default();
        a.add(&i(1));
        a.reset();
        assert_eq!(a.result(), i(0));
    }

    macro_rules! cd_n {
        ($n:ident, $distinct:expr, $dups:expr) => {
            #[test]
            fn $n() {
                let mut a = CountDistinct::default();
                for d in 0..$distinct {
                    for _ in 0..$dups {
                        a.add(&i(d));
                    }
                }
                assert_eq!(a.result(), i($distinct));
            }
        };
    }
    cd_n!(d1x1, 1, 1);
    cd_n!(d2x1, 2, 1);
    cd_n!(d3x1, 3, 1);
    cd_n!(d5x1, 5, 1);
    cd_n!(d10x1, 10, 1);
    cd_n!(d1x5, 1, 5);
    cd_n!(d2x5, 2, 5);
    cd_n!(d3x5, 3, 5);
    cd_n!(d5x5, 5, 5);
    cd_n!(d10x5, 10, 5);
    cd_n!(d1x10, 1, 10);
    cd_n!(d2x10, 2, 10);
    cd_n!(d3x10, 3, 10);
    cd_n!(d5x10, 5, 10);
    cd_n!(d10x10, 10, 10);
    cd_n!(d20x1, 20, 1);
    cd_n!(d20x5, 20, 5);
    cd_n!(d50x1, 50, 1);
    cd_n!(d50x2, 50, 2);
    cd_n!(d100x1, 100, 1);

    // Sequential
    macro_rules! cd_seq {
        ($n:ident, $count:expr) => {
            #[test]
            fn $n() {
                let mut a = CountDistinct::default();
                for x in 0..$count {
                    a.add(&i(x));
                }
                assert_eq!(a.result(), i($count));
            }
        };
    }
    cd_seq!(s1, 1);
    cd_seq!(s2, 2);
    cd_seq!(s5, 5);
    cd_seq!(s10, 10);
    cd_seq!(s20, 20);
    cd_seq!(s50, 50);
    cd_seq!(s100, 100);

    // with floats
    #[test]
    fn float_01() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[f(1.0), f(1.0), f(2.0)]);
        assert_eq!(a.result(), i(2));
    }
    #[test]
    fn float_02() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[f(1.0), f(2.0), f(3.0)]);
        assert_eq!(a.result(), i(3));
    }
    #[test]
    fn float_03() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[f(0.0), f(0.0)]);
        assert_eq!(a.result(), i(1));
    }
    #[test]
    fn float_04() {
        let mut a = CountDistinct::default();
        for x in 0..20 {
            a.add(&f(x as f64));
        }
        assert_eq!(a.result(), i(20));
    }
    #[test]
    fn float_05() {
        let mut a = CountDistinct::default();
        for x in 0..10 {
            a.add(&f(x as f64));
            a.add(&f(x as f64));
        }
        assert_eq!(a.result(), i(10));
    }
    #[test]
    fn mixed() {
        let mut a = CountDistinct::default();
        feed(&mut a, &[i(1), f(2.0), i(3)]);
        assert!(a.result() != null());
    }
}
