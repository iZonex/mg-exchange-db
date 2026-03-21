//! 500 aggregate function tests via AggregateFunction trait.

use exchange_query::functions::*;
use exchange_query::plan::Value;

fn i(v: i64) -> Value { Value::I64(v) }
fn f(v: f64) -> Value { Value::F64(v) }
fn null() -> Value { Value::Null }
fn close(val: &Value, expected: f64, tol: f64) {
    match val {
        Value::F64(v) => assert!((*v - expected).abs() < tol, "expected ~{expected}, got {v}"),
        Value::I64(v) => assert!((*v as f64 - expected).abs() < tol, "expected ~{expected}, got {v}"),
        other => panic!("expected ~{expected}, got {other:?}"),
    }
}
fn feed(a: &mut dyn AggregateFunction, vals: &[Value]) { for v in vals { a.add(v); } }

// Sum with ascending sequences 1..=N
mod sum_asc { use super::*;
    macro_rules! sum_n { ($n:ident, $count:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Sum::default(); for x in 1..=$count { a.add(&i(x)); } assert_eq!(a.result(), i($expect)); }
    }; }
    sum_n!(s01, 1, 1); sum_n!(s02, 2, 3); sum_n!(s03, 3, 6); sum_n!(s04, 4, 10); sum_n!(s05, 5, 15);
    sum_n!(s06, 6, 21); sum_n!(s07, 7, 28); sum_n!(s08, 8, 36); sum_n!(s09, 9, 45); sum_n!(s10, 10, 55);
    sum_n!(s11, 11, 66); sum_n!(s12, 12, 78); sum_n!(s13, 13, 91); sum_n!(s14, 14, 105); sum_n!(s15, 15, 120);
    sum_n!(s16, 16, 136); sum_n!(s17, 17, 153); sum_n!(s18, 18, 171); sum_n!(s19, 19, 190); sum_n!(s20, 20, 210);
    #[test] fn empty() { let a = Sum::default(); assert_eq!(a.result(), null()); }
    #[test] fn with_null() { let mut a = Sum::default(); feed(&mut a, &[i(1), null(), i(3)]); assert_eq!(a.result(), i(4)); }
    #[test] fn all_null() { let mut a = Sum::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), null()); }
    #[test] fn reset() { let mut a = Sum::default(); a.add(&i(5)); a.reset(); assert_eq!(a.result(), null()); }
}

// Sum with constant values
mod sum_const { use super::*;
    macro_rules! sc { ($n:ident, $val:expr, $count:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Sum::default(); for _ in 0..$count { a.add(&i($val)); } assert_eq!(a.result(), i($expect)); }
    }; }
    sc!(c1x1, 1, 1, 1); sc!(c1x5, 1, 5, 5); sc!(c1x10, 1, 10, 10); sc!(c1x20, 1, 20, 20);
    sc!(c2x1, 2, 1, 2); sc!(c2x5, 2, 5, 10); sc!(c2x10, 2, 10, 20); sc!(c2x20, 2, 20, 40);
    sc!(c3x1, 3, 1, 3); sc!(c3x5, 3, 5, 15); sc!(c3x10, 3, 10, 30); sc!(c3x20, 3, 20, 60);
    sc!(c5x1, 5, 1, 5); sc!(c5x5, 5, 5, 25); sc!(c5x10, 5, 10, 50); sc!(c5x20, 5, 20, 100);
    sc!(c7x1, 7, 1, 7); sc!(c7x5, 7, 5, 35); sc!(c7x10, 7, 10, 70); sc!(c7x20, 7, 20, 140);
    sc!(c10x1, 10, 1, 10); sc!(c10x5, 10, 5, 50); sc!(c10x10, 10, 10, 100); sc!(c10x20, 10, 20, 200);
}

// Sum with floats
mod sum_float { use super::*;
    macro_rules! sf { ($n:ident, $vals:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Sum::default(); for &v in &$vals { a.add(&f(v)); } close(&a.result(), $expect, 0.01); }
    }; }
    sf!(f01, [1.0, 2.0], 3.0); sf!(f02, [1.5, 2.5], 4.0); sf!(f03, [0.1, 0.2, 0.3], 0.6);
    sf!(f04, [10.0, 20.0, 30.0], 60.0); sf!(f05, [-1.0, 1.0], 0.0);
    sf!(f06, [0.5, 0.5, 0.5, 0.5], 2.0); sf!(f07, [1.1, 2.2, 3.3], 6.6);
    sf!(f08, [100.0, 200.0], 300.0); sf!(f09, [-5.5, 5.5], 0.0);
    sf!(f10, [0.25, 0.25, 0.25, 0.25], 1.0);
}

// Avg with ascending sequences
mod avg_asc { use super::*;
    macro_rules! avg_n { ($n:ident, $count:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Avg::default(); for x in 1..=$count { a.add(&i(x)); } close(&a.result(), $expect, 0.01); }
    }; }
    avg_n!(a01, 1, 1.0); avg_n!(a02, 2, 1.5); avg_n!(a03, 3, 2.0); avg_n!(a04, 4, 2.5);
    avg_n!(a05, 5, 3.0); avg_n!(a06, 6, 3.5); avg_n!(a07, 7, 4.0); avg_n!(a08, 8, 4.5);
    avg_n!(a09, 9, 5.0); avg_n!(a10, 10, 5.5); avg_n!(a11, 11, 6.0); avg_n!(a12, 12, 6.5);
    avg_n!(a13, 13, 7.0); avg_n!(a14, 14, 7.5); avg_n!(a15, 15, 8.0); avg_n!(a16, 16, 8.5);
    avg_n!(a17, 17, 9.0); avg_n!(a18, 18, 9.5); avg_n!(a19, 19, 10.0); avg_n!(a20, 20, 10.5);
    #[test] fn empty() { let a = Avg::default(); assert_eq!(a.result(), null()); }
    #[test] fn with_null() { let mut a = Avg::default(); feed(&mut a, &[i(2), null(), i(4)]); close(&a.result(), 3.0, 0.01); }
    #[test] fn all_null() { let mut a = Avg::default(); feed(&mut a, &[null(), null()]); assert_eq!(a.result(), null()); }
    #[test] fn reset() { let mut a = Avg::default(); a.add(&i(5)); a.reset(); assert_eq!(a.result(), null()); }
}

// Avg with constant
mod avg_const { use super::*;
    macro_rules! ac { ($n:ident, $val:expr, $count:expr) => {
        #[test] fn $n() { let mut a = Avg::default(); for _ in 0..$count { a.add(&i($val)); } close(&a.result(), $val as f64, 0.01); }
    }; }
    ac!(c1x1, 1, 1); ac!(c1x5, 1, 5); ac!(c1x10, 1, 10); ac!(c1x20, 1, 20);
    ac!(c5x1, 5, 1); ac!(c5x5, 5, 5); ac!(c5x10, 5, 10); ac!(c5x20, 5, 20);
    ac!(c10x1, 10, 1); ac!(c10x5, 10, 5); ac!(c10x10, 10, 10); ac!(c10x20, 10, 20);
    ac!(c42x1, 42, 1); ac!(c42x5, 42, 5); ac!(c42x10, 42, 10); ac!(c42x20, 42, 20);
    ac!(c100x1, 100, 1); ac!(c100x5, 100, 5); ac!(c100x10, 100, 10); ac!(c100x20, 100, 20);
}

// Min with ascending sequences
mod min_asc { use super::*;
    macro_rules! min_n { ($n:ident, $count:expr) => {
        #[test] fn $n() { let mut a = Min::default(); for x in 1..=$count { a.add(&i(x)); } assert_eq!(a.result(), i(1)); }
    }; }
    min_n!(m01, 1); min_n!(m02, 2); min_n!(m03, 3); min_n!(m04, 4); min_n!(m05, 5);
    min_n!(m06, 6); min_n!(m07, 7); min_n!(m08, 8); min_n!(m09, 9); min_n!(m10, 10);
    min_n!(m11, 11); min_n!(m12, 12); min_n!(m13, 13); min_n!(m14, 14); min_n!(m15, 15);
    min_n!(m16, 16); min_n!(m17, 17); min_n!(m18, 18); min_n!(m19, 19); min_n!(m20, 20);
    #[test] fn empty() { let a = Min::default(); assert_eq!(a.result(), null()); }
    #[test] fn with_null() { let mut a = Min::default(); feed(&mut a, &[i(5), null(), i(3)]); assert_eq!(a.result(), i(3)); }
    #[test] fn reset() { let mut a = Min::default(); a.add(&i(5)); a.reset(); assert_eq!(a.result(), null()); }
}

// Min with descending sequences (min is last)
mod min_desc { use super::*;
    macro_rules! min_d { ($n:ident, $count:expr) => {
        #[test] fn $n() { let mut a = Min::default(); for x in (1..=$count).rev() { a.add(&i(x)); } assert_eq!(a.result(), i(1)); }
    }; }
    min_d!(m01, 1); min_d!(m02, 2); min_d!(m03, 3); min_d!(m04, 4); min_d!(m05, 5);
    min_d!(m06, 6); min_d!(m07, 7); min_d!(m08, 8); min_d!(m09, 9); min_d!(m10, 10);
    min_d!(m11, 11); min_d!(m12, 12); min_d!(m13, 13); min_d!(m14, 14); min_d!(m15, 15);
    min_d!(m16, 16); min_d!(m17, 17); min_d!(m18, 18); min_d!(m19, 19); min_d!(m20, 20);
}

// Max with ascending sequences
mod max_asc { use super::*;
    macro_rules! max_n { ($n:ident, $count:expr) => {
        #[test] fn $n() { let mut a = Max::default(); for x in 1..=$count { a.add(&i(x)); } assert_eq!(a.result(), i($count)); }
    }; }
    max_n!(m01, 1); max_n!(m02, 2); max_n!(m03, 3); max_n!(m04, 4); max_n!(m05, 5);
    max_n!(m06, 6); max_n!(m07, 7); max_n!(m08, 8); max_n!(m09, 9); max_n!(m10, 10);
    max_n!(m11, 11); max_n!(m12, 12); max_n!(m13, 13); max_n!(m14, 14); max_n!(m15, 15);
    max_n!(m16, 16); max_n!(m17, 17); max_n!(m18, 18); max_n!(m19, 19); max_n!(m20, 20);
    #[test] fn empty() { let a = Max::default(); assert_eq!(a.result(), null()); }
    #[test] fn with_null() { let mut a = Max::default(); feed(&mut a, &[i(5), null(), i(3)]); assert_eq!(a.result(), i(5)); }
    #[test] fn reset() { let mut a = Max::default(); a.add(&i(5)); a.reset(); assert_eq!(a.result(), null()); }
}

// Max with descending sequences (max is first)
mod max_desc { use super::*;
    macro_rules! max_d { ($n:ident, $count:expr) => {
        #[test] fn $n() { let mut a = Max::default(); for x in (1..=$count).rev() { a.add(&i(x)); } assert_eq!(a.result(), i($count)); }
    }; }
    max_d!(m01, 1); max_d!(m02, 2); max_d!(m03, 3); max_d!(m04, 4); max_d!(m05, 5);
    max_d!(m06, 6); max_d!(m07, 7); max_d!(m08, 8); max_d!(m09, 9); max_d!(m10, 10);
    max_d!(m11, 11); max_d!(m12, 12); max_d!(m13, 13); max_d!(m14, 14); max_d!(m15, 15);
    max_d!(m16, 16); max_d!(m17, 17); max_d!(m18, 18); max_d!(m19, 19); max_d!(m20, 20);
}

// Count with ascending sequences
mod count_asc { use super::*;
    macro_rules! cnt_n { ($n:ident, $count:expr) => {
        #[test] fn $n() { let mut a = Count::default(); for x in 1..=$count { a.add(&i(x)); } close(&a.result(), $count as f64, 0.01); }
    }; }
    cnt_n!(c01, 1); cnt_n!(c02, 2); cnt_n!(c03, 3); cnt_n!(c04, 4); cnt_n!(c05, 5);
    cnt_n!(c06, 6); cnt_n!(c07, 7); cnt_n!(c08, 8); cnt_n!(c09, 9); cnt_n!(c10, 10);
    cnt_n!(c11, 11); cnt_n!(c12, 12); cnt_n!(c13, 13); cnt_n!(c14, 14); cnt_n!(c15, 15);
    cnt_n!(c16, 16); cnt_n!(c17, 17); cnt_n!(c18, 18); cnt_n!(c19, 19); cnt_n!(c20, 20);
    #[test] fn empty() { let a = Count::default(); close(&a.result(), 0.0, 0.01); }
    #[test] fn with_null() { let mut a = Count::default(); feed(&mut a, &[i(1), null(), i(3)]); close(&a.result(), 2.0, 0.01); }
    #[test] fn all_null() { let mut a = Count::default(); feed(&mut a, &[null(), null()]); close(&a.result(), 0.0, 0.01); }
    #[test] fn reset() { let mut a = Count::default(); a.add(&i(1)); a.reset(); close(&a.result(), 0.0, 0.01); }
}

// Min/Max with negative values
mod min_neg { use super::*;
    macro_rules! mn { ($n:ident, $vals:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Min::default(); for &v in &$vals { a.add(&i(v)); } assert_eq!(a.result(), i($expect)); }
    }; }
    mn!(n01, [-1, 0, 1], -1); mn!(n02, [-10, -5, 0], -10); mn!(n03, [-100, -50, -1], -100);
    mn!(n04, [5, 3, 1, -1, -3], -3); mn!(n05, [-50, 50], -50);
    mn!(n06, [0, 0, 0], 0); mn!(n07, [-1, -1, -1], -1); mn!(n08, [100, 50, 25, 10, 5, 1], 1);
    mn!(n09, [-99, -98, -97], -99); mn!(n10, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 1);
}

mod max_neg { use super::*;
    macro_rules! mx { ($n:ident, $vals:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Max::default(); for &v in &$vals { a.add(&i(v)); } assert_eq!(a.result(), i($expect)); }
    }; }
    mx!(n01, [-1, 0, 1], 1); mx!(n02, [-10, -5, 0], 0); mx!(n03, [-100, -50, -1], -1);
    mx!(n04, [5, 3, 1, -1, -3], 5); mx!(n05, [-50, 50], 50);
    mx!(n06, [0, 0, 0], 0); mx!(n07, [-1, -1, -1], -1); mx!(n08, [100, 50, 25, 10, 5, 1], 100);
    mx!(n09, [-99, -98, -97], -97); mx!(n10, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 10);
}

// Sum with negative values
mod sum_neg { use super::*;
    macro_rules! sn { ($n:ident, $vals:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Sum::default(); for &v in &$vals { a.add(&i(v)); } assert_eq!(a.result(), i($expect)); }
    }; }
    sn!(n01, [-1, 1], 0); sn!(n02, [-5, 5], 0); sn!(n03, [-10, 10], 0);
    sn!(n04, [-1, -2, -3], -6); sn!(n05, [-10, -20, -30], -60);
    sn!(n06, [1, -1, 2, -2, 3, -3], 0); sn!(n07, [-100, 50, 50], 0);
    sn!(n08, [-1, -1, -1, -1, -1], -5); sn!(n09, [100, -50], 50);
    sn!(n10, [-50, -50], -100);
}

// Avg with floats
mod avg_float { use super::*;
    macro_rules! af { ($n:ident, $vals:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Avg::default(); for &v in &$vals { a.add(&f(v)); } close(&a.result(), $expect, 0.01); }
    }; }
    af!(f01, [1.0, 2.0, 3.0], 2.0); af!(f02, [10.0, 20.0], 15.0);
    af!(f03, [0.0, 100.0], 50.0); af!(f04, [1.5, 2.5, 3.5], 2.5);
    af!(f05, [-1.0, 1.0], 0.0); af!(f06, [0.1, 0.2, 0.3], 0.2);
    af!(f07, [100.0, 200.0, 300.0], 200.0);
    af!(f08, [5.0, 5.0, 5.0, 5.0, 5.0], 5.0);
    af!(f09, [-10.0, 10.0], 0.0); af!(f10, [0.5, 1.5], 1.0);
}

// Min/Max with floats
mod min_float { use super::*;
    macro_rules! mf { ($n:ident, $vals:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Min::default(); for &v in &$vals { a.add(&f(v)); } close(&a.result(), $expect, 0.01); }
    }; }
    mf!(f01, [1.0, 2.0, 3.0], 1.0); mf!(f02, [3.0, 2.0, 1.0], 1.0);
    mf!(f03, [-1.0, 0.0, 1.0], -1.0); mf!(f04, [0.5, 0.1, 0.9], 0.1);
    mf!(f05, [100.0, 50.0, 25.0], 25.0); mf!(f06, [-10.0, -5.0, -1.0], -10.0);
    mf!(f07, [0.0, 0.0, 0.0], 0.0); mf!(f08, [-0.5, 0.5], -0.5);
    mf!(f09, [99.9, 100.0], 99.9); mf!(f10, [1.1, 1.01, 1.001], 1.001);
}

mod max_float { use super::*;
    macro_rules! mf { ($n:ident, $vals:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Max::default(); for &v in &$vals { a.add(&f(v)); } close(&a.result(), $expect, 0.01); }
    }; }
    mf!(f01, [1.0, 2.0, 3.0], 3.0); mf!(f02, [3.0, 2.0, 1.0], 3.0);
    mf!(f03, [-1.0, 0.0, 1.0], 1.0); mf!(f04, [0.5, 0.1, 0.9], 0.9);
    mf!(f05, [100.0, 50.0, 25.0], 100.0); mf!(f06, [-10.0, -5.0, -1.0], -1.0);
    mf!(f07, [0.0, 0.0, 0.0], 0.0); mf!(f08, [-0.5, 0.5], 0.5);
    mf!(f09, [99.9, 100.0], 100.0); mf!(f10, [1.1, 1.01, 1.001], 1.1);
}

// Count with various types
mod count_types { use super::*;
    #[test] fn ints() { let mut a = Count::default(); for x in 0..50 { a.add(&i(x)); } close(&a.result(), 50.0, 0.01); }
    #[test] fn floats() { let mut a = Count::default(); for x in 0..30 { a.add(&f(x as f64)); } close(&a.result(), 30.0, 0.01); }
    #[test] fn mixed() { let mut a = Count::default(); feed(&mut a, &[i(1), f(2.0), i(3)]); close(&a.result(), 3.0, 0.01); }
    #[test] fn hundred() { let mut a = Count::default(); for _ in 0..100 { a.add(&i(1)); } close(&a.result(), 100.0, 0.01); }
    #[test] fn with_5_nulls() { let mut a = Count::default(); for _ in 0..5 { a.add(&null()); } close(&a.result(), 0.0, 0.01); }
    #[test] fn with_10_nulls_10_vals() { let mut a = Count::default(); for _ in 0..10 { a.add(&null()); a.add(&i(1)); } close(&a.result(), 10.0, 0.01); }
}

// Sum with descending sequences
mod sum_desc { use super::*;
    macro_rules! sd { ($n:ident, $count:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Sum::default(); for x in (1..=$count).rev() { a.add(&i(x)); } assert_eq!(a.result(), i($expect)); }
    }; }
    sd!(s01, 1, 1); sd!(s02, 2, 3); sd!(s03, 3, 6); sd!(s04, 4, 10); sd!(s05, 5, 15);
    sd!(s06, 6, 21); sd!(s07, 7, 28); sd!(s08, 8, 36); sd!(s09, 9, 45); sd!(s10, 10, 55);
    sd!(s11, 11, 66); sd!(s12, 12, 78); sd!(s13, 13, 91); sd!(s14, 14, 105); sd!(s15, 15, 120);
    sd!(s16, 16, 136); sd!(s17, 17, 153); sd!(s18, 18, 171); sd!(s19, 19, 190); sd!(s20, 20, 210);
}

// Avg with descending sequences
mod avg_desc { use super::*;
    macro_rules! ad { ($n:ident, $count:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Avg::default(); for x in (1..=$count).rev() { a.add(&i(x)); } close(&a.result(), $expect, 0.01); }
    }; }
    ad!(a01, 1, 1.0); ad!(a02, 2, 1.5); ad!(a03, 3, 2.0); ad!(a04, 4, 2.5);
    ad!(a05, 5, 3.0); ad!(a06, 6, 3.5); ad!(a07, 7, 4.0); ad!(a08, 8, 4.5);
    ad!(a09, 9, 5.0); ad!(a10, 10, 5.5); ad!(a11, 11, 6.0); ad!(a12, 12, 6.5);
    ad!(a13, 13, 7.0); ad!(a14, 14, 7.5); ad!(a15, 15, 8.0); ad!(a16, 16, 8.5);
    ad!(a17, 17, 9.0); ad!(a18, 18, 9.5); ad!(a19, 19, 10.0); ad!(a20, 20, 10.5);
}

// Sum of even numbers
mod sum_even { use super::*;
    macro_rules! se { ($n:ident, $count:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Sum::default(); for x in (0..$count).map(|i| (i + 1) * 2) { a.add(&i(x)); } assert_eq!(a.result(), i($expect)); }
    }; }
    se!(e01, 1, 2); se!(e02, 2, 6); se!(e03, 3, 12); se!(e04, 4, 20); se!(e05, 5, 30);
    se!(e06, 6, 42); se!(e07, 7, 56); se!(e08, 8, 72); se!(e09, 9, 90); se!(e10, 10, 110);
    se!(e11, 11, 132); se!(e12, 12, 156); se!(e13, 13, 182); se!(e14, 14, 210); se!(e15, 15, 240);
    se!(e16, 16, 272); se!(e17, 17, 306); se!(e18, 18, 342); se!(e19, 19, 380); se!(e20, 20, 420);
}

// Sum of odd numbers
mod sum_odd { use super::*;
    macro_rules! so { ($n:ident, $count:expr, $expect:expr) => {
        #[test] fn $n() { let mut a = Sum::default(); for x in (0..$count).map(|i| i * 2 + 1) { a.add(&i(x)); } assert_eq!(a.result(), i($expect)); }
    }; }
    // sum of first n odd = n^2
    so!(o01, 1, 1); so!(o02, 2, 4); so!(o03, 3, 9); so!(o04, 4, 16); so!(o05, 5, 25);
    so!(o06, 6, 36); so!(o07, 7, 49); so!(o08, 8, 64); so!(o09, 9, 81); so!(o10, 10, 100);
    so!(o11, 11, 121); so!(o12, 12, 144); so!(o13, 13, 169); so!(o14, 14, 196); so!(o15, 15, 225);
    so!(o16, 16, 256); so!(o17, 17, 289); so!(o18, 18, 324); so!(o19, 19, 361); so!(o20, 20, 400);
}

// Min/Max with large sequences
mod min_large { use super::*;
    macro_rules! ml { ($n:ident, $count:expr) => {
        #[test] fn $n() { let mut a = Min::default(); for x in 0..$count { a.add(&i(x)); } assert_eq!(a.result(), i(0)); }
    }; }
    ml!(m50, 50); ml!(m100, 100); ml!(m200, 200); ml!(m500, 500); ml!(m1000, 1000);
}

mod max_large { use super::*;
    macro_rules! ml { ($n:ident, $count:expr) => {
        #[test] fn $n() { let mut a = Max::default(); for x in 0..$count { a.add(&i(x)); } assert_eq!(a.result(), i($count - 1)); }
    }; }
    ml!(m50, 50); ml!(m100, 100); ml!(m200, 200); ml!(m500, 500); ml!(m1000, 1000);
}

// Count with large sequences
mod count_large { use super::*;
    macro_rules! cl { ($n:ident, $count:expr) => {
        #[test] fn $n() { let mut a = Count::default(); for x in 0..$count { a.add(&i(x)); } close(&a.result(), $count as f64, 0.01); }
    }; }
    cl!(c50, 50); cl!(c100, 100); cl!(c200, 200); cl!(c500, 500); cl!(c1000, 1000);
}

// Avg with large sequences (avg of 0..n-1 is (n-1)/2)
mod avg_large { use super::*;
    macro_rules! al { ($n:ident, $count:expr) => {
        #[test] fn $n() { let mut a = Avg::default(); for x in 0..$count { a.add(&i(x)); } close(&a.result(), ($count - 1) as f64 / 2.0, 0.01); }
    }; }
    al!(a50, 50); al!(a100, 100); al!(a200, 200); al!(a500, 500); al!(a1000, 1000);
}

// Sum with large sequences (sum of 0..n-1 is n*(n-1)/2)
mod sum_large { use super::*;
    macro_rules! sl { ($n:ident, $count:expr) => {
        #[test] fn $n() { let mut a = Sum::default(); for x in 0..$count as i64 { a.add(&i(x)); } assert_eq!(a.result(), i($count * ($count - 1) / 2)); }
    }; }
    sl!(s25, 25); sl!(s30, 30); sl!(s35, 35); sl!(s40, 40); sl!(s45, 45);
    sl!(s50, 50); sl!(s60, 60); sl!(s70, 70); sl!(s80, 80); sl!(s90, 90);
    sl!(s100, 100); sl!(s150, 150); sl!(s200, 200);
}

// Mixed type aggregates
mod mixed_agg { use super::*;
    #[test] fn sum_i_f() { let mut a = Sum::default(); feed(&mut a, &[i(1), f(2.5), i(3)]); close(&a.result(), 6.5, 0.01); }
    #[test] fn avg_i_f() { let mut a = Avg::default(); feed(&mut a, &[i(1), f(2.0), i(3)]); close(&a.result(), 2.0, 0.01); }
    #[test] fn min_i_f() { let mut a = Min::default(); feed(&mut a, &[i(5), i(3), i(1)]); assert_eq!(a.result(), i(1)); }
    #[test] fn max_i_f() { let mut a = Max::default(); feed(&mut a, &[i(1), f(7.5), i(3)]); close(&a.result(), 7.5, 0.01); }
    #[test] fn count_i_f() { let mut a = Count::default(); feed(&mut a, &[i(1), f(2.0), i(3)]); close(&a.result(), 3.0, 0.01); }
    #[test] fn sum_all_f() { let mut a = Sum::default(); feed(&mut a, &[f(1.1), f(2.2), f(3.3), f(4.4)]); close(&a.result(), 11.0, 0.01); }
    #[test] fn avg_all_f() { let mut a = Avg::default(); feed(&mut a, &[f(1.0), f(2.0), f(3.0), f(4.0)]); close(&a.result(), 2.5, 0.01); }
    #[test] fn min_all_f() { let mut a = Min::default(); feed(&mut a, &[f(3.3), f(1.1), f(2.2)]); close(&a.result(), 1.1, 0.01); }
    #[test] fn max_all_f() { let mut a = Max::default(); feed(&mut a, &[f(3.3), f(1.1), f(2.2)]); close(&a.result(), 3.3, 0.01); }
    #[test] fn sum_neg_f() { let mut a = Sum::default(); feed(&mut a, &[f(-1.5), f(-2.5)]); close(&a.result(), -4.0, 0.01); }
}

// Sum of squares: 1^2 + 2^2 + ... + n^2 = n(n+1)(2n+1)/6
mod sum_squares { use super::*;
    macro_rules! ss { ($n:ident, $count:expr) => {
        #[test] fn $n() { let mut a = Sum::default(); for x in 1..=$count as i64 { a.add(&i(x * x)); } let expect = ($count * ($count + 1) * (2 * $count + 1)) / 6; assert_eq!(a.result(), i(expect)); }
    }; }
    ss!(s01, 1); ss!(s02, 2); ss!(s03, 3); ss!(s04, 4); ss!(s05, 5);
    ss!(s06, 6); ss!(s07, 7); ss!(s08, 8); ss!(s09, 9); ss!(s10, 10);
    ss!(s11, 11); ss!(s12, 12); ss!(s13, 13); ss!(s14, 14); ss!(s15, 15);
    ss!(s16, 16); ss!(s17, 17); ss!(s18, 18); ss!(s19, 19); ss!(s20, 20);
}

// Sum of cubes: 1^3 + 2^3 + ... + n^3 = (n(n+1)/2)^2
mod sum_cubes { use super::*;
    macro_rules! sc { ($n:ident, $count:expr) => {
        #[test] fn $n() { let mut a = Sum::default(); for x in 1..=$count as i64 { a.add(&i(x * x * x)); } let s = ($count * ($count + 1)) / 2; assert_eq!(a.result(), i(s * s)); }
    }; }
    sc!(c01, 1); sc!(c02, 2); sc!(c03, 3); sc!(c04, 4); sc!(c05, 5);
    sc!(c06, 6); sc!(c07, 7); sc!(c08, 8); sc!(c09, 9); sc!(c10, 10);
    sc!(c11, 11); sc!(c12, 12); sc!(c13, 13); sc!(c14, 14); sc!(c15, 15);
    sc!(c16, 16); sc!(c17, 17); sc!(c18, 18); sc!(c19, 19); sc!(c20, 20);
}

// Min of single values
mod min_single { use super::*;
    macro_rules! ms { ($n:ident, $v:expr) => {
        #[test] fn $n() { let mut a = Min::default(); a.add(&i($v)); assert_eq!(a.result(), i($v)); }
    }; }
    ms!(s0, 0); ms!(s1, 1); ms!(s5, 5); ms!(s10, 10); ms!(s42, 42);
    ms!(s100, 100); ms!(s999, 999); ms!(sn1, -1); ms!(sn10, -10); ms!(sn100, -100);
    ms!(sn999, -999); ms!(s_max, i64::MAX); ms!(s_min, i64::MIN);
}

// Max of single values
mod max_single { use super::*;
    macro_rules! ms { ($n:ident, $v:expr) => {
        #[test] fn $n() { let mut a = Max::default(); a.add(&i($v)); assert_eq!(a.result(), i($v)); }
    }; }
    ms!(s0, 0); ms!(s1, 1); ms!(s5, 5); ms!(s10, 10); ms!(s42, 42);
    ms!(s100, 100); ms!(s999, 999); ms!(sn1, -1); ms!(sn10, -10); ms!(sn100, -100);
    ms!(sn999, -999); ms!(s_max, i64::MAX); ms!(s_min, i64::MIN);
}

// Count of single values
mod count_single { use super::*;
    macro_rules! cs { ($n:ident, $v:expr) => {
        #[test] fn $n() { let mut a = Count::default(); a.add(&$v); close(&a.result(), 1.0, 0.01); }
    }; }
    cs!(i0, i(0)); cs!(i1, i(1)); cs!(i5, i(5)); cs!(i10, i(10)); cs!(i42, i(42));
    cs!(i100, i(100)); cs!(f0, f(0.0)); cs!(f1, f(1.0)); cs!(f5, f(5.5));
    cs!(f10, f(10.0)); cs!(f42, f(42.42));
}

// Avg of single values
mod avg_single { use super::*;
    macro_rules! avs { ($n:ident, $v:expr, $e:expr) => {
        #[test] fn $n() { let mut a = Avg::default(); a.add(&i($v)); close(&a.result(), $e, 0.01); }
    }; }
    avs!(a0, 0, 0.0); avs!(a1, 1, 1.0); avs!(a5, 5, 5.0); avs!(a10, 10, 10.0);
    avs!(a42, 42, 42.0); avs!(a100, 100, 100.0); avs!(an1, -1, -1.0);
    avs!(an10, -10, -10.0); avs!(an100, -100, -100.0);
}

// Sum of two values
mod sum_two { use super::*;
    macro_rules! st { ($n:ident, $a:expr, $b:expr, $e:expr) => {
        #[test] fn $n() { let mut agg = Sum::default(); feed(&mut agg, &[i($a), i($b)]); assert_eq!(agg.result(), i($e)); }
    }; }
    st!(s00, 0, 0, 0); st!(s01, 0, 1, 1); st!(s10, 1, 0, 1); st!(s11, 1, 1, 2);
    st!(s23, 2, 3, 5); st!(s55, 5, 5, 10); st!(s_99, 99, 1, 100);
    st!(s_neg, -5, 5, 0); st!(s_neg2, -10, -20, -30); st!(s_big, 1000, 2000, 3000);
    st!(s50_50, 50, 50, 100); st!(s25_75, 25, 75, 100); st!(s33_67, 33, 67, 100);
    st!(s1_99, 1, 99, 100); st!(s0_100, 0, 100, 100);
}

// Count after reset
mod count_reset { use super::*;
    macro_rules! cr { ($n:ident, $count1:expr, $count2:expr) => {
        #[test] fn $n() { let mut a = Count::default(); for _ in 0..$count1 { a.add(&i(1)); } a.reset(); for _ in 0..$count2 { a.add(&i(1)); } close(&a.result(), $count2 as f64, 0.01); }
    }; }
    cr!(r01, 5, 3); cr!(r02, 10, 5); cr!(r03, 20, 10); cr!(r04, 50, 25);
    cr!(r05, 100, 50); cr!(r06, 1, 1); cr!(r07, 10, 1); cr!(r08, 100, 1);
    cr!(r09, 5, 10); cr!(r10, 1, 100);
}

// Sum after reset
mod sum_reset { use super::*;
    macro_rules! sr { ($n:ident, $count1:expr, $count2:expr) => {
        #[test] fn $n() { let mut a = Sum::default(); for x in 0..$count1 { a.add(&i(x)); } a.reset(); for x in 0..$count2 as i64 { a.add(&i(x)); } assert_eq!(a.result(), i($count2 * ($count2 - 1) / 2)); }
    }; }
    sr!(r01, 5, 3); sr!(r02, 10, 5); sr!(r03, 20, 10); sr!(r04, 50, 25);
    sr!(r05, 100, 50); sr!(r06, 1, 2); sr!(r07, 10, 3); sr!(r08, 100, 4);
    sr!(r09, 5, 10); sr!(r10, 1, 20);
}

// Avg of pairs
mod avg_pairs { use super::*;
    macro_rules! ap { ($n:ident, $a:expr, $b:expr, $e:expr) => {
        #[test] fn $n() { let mut agg = Avg::default(); feed(&mut agg, &[i($a), i($b)]); close(&agg.result(), $e, 0.01); }
    }; }
    ap!(a00, 0, 0, 0.0); ap!(a01, 0, 2, 1.0); ap!(a02, 1, 3, 2.0); ap!(a03, 2, 4, 3.0);
    ap!(a04, 5, 15, 10.0); ap!(a05, 10, 20, 15.0); ap!(a06, 0, 100, 50.0);
    ap!(a07, 50, 50, 50.0); ap!(a08, -10, 10, 0.0); ap!(a09, -5, 5, 0.0);
    ap!(a10, 1, 1, 1.0); ap!(a11, 99, 1, 50.0); ap!(a12, 25, 75, 50.0);
    ap!(a13, 33, 67, 50.0); ap!(a14, 40, 60, 50.0); ap!(a15, 45, 55, 50.0);
    ap!(a16, 48, 52, 50.0); ap!(a17, 49, 51, 50.0); ap!(a18, 50, 50, 50.0);
    ap!(a19, 0, 0, 0.0); ap!(a20, 100, 100, 100.0);
}
