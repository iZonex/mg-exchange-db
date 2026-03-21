//! 1000+ math scalar function tests.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

fn i(v: i64) -> Value {
    Value::I64(v)
}
fn f(v: f64) -> Value {
    Value::F64(v)
}
fn s(v: &str) -> Value {
    Value::Str(v.to_string())
}
fn null() -> Value {
    Value::Null
}
fn ev(name: &str, args: &[Value]) -> Value {
    evaluate_scalar(name, args).unwrap()
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

// abs — 40 tests
mod abs_t02 {
    use super::*;
    #[test]
    fn pos() {
        assert_eq!(ev("abs", &[i(5)]), i(5));
    }
    #[test]
    fn neg() {
        assert_eq!(ev("abs", &[i(-5)]), i(5));
    }
    #[test]
    fn zero() {
        assert_eq!(ev("abs", &[i(0)]), i(0));
    }
    #[test]
    fn pos_f() {
        assert_eq!(ev("abs", &[f(3.14)]), f(3.14));
    }
    #[test]
    fn neg_f() {
        assert_eq!(ev("abs", &[f(-3.14)]), f(3.14));
    }
    #[test]
    fn zero_f() {
        assert_eq!(ev("abs", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("abs", &[null()]), null());
    }
    #[test]
    fn large() {
        assert_eq!(ev("abs", &[i(-1_000_000)]), i(1_000_000));
    }
    #[test]
    fn one() {
        assert_eq!(ev("abs", &[i(-1)]), i(1));
    }
    #[test]
    fn tiny() {
        assert_eq!(ev("abs", &[f(-0.001)]), f(0.001));
    }
    #[test]
    fn int_alias() {
        assert_eq!(ev("abs_int", &[i(-5)]), i(5));
    }
    #[test]
    fn long_alias() {
        assert_eq!(ev("abs_long", &[i(-5)]), i(5));
    }
    #[test]
    fn double_alias() {
        assert_eq!(ev("abs_double", &[f(-2.0)]), f(2.0));
    }
    #[test]
    fn float_alias() {
        assert_eq!(ev("abs_float", &[f(-1.5)]), f(1.5));
    }
    #[test]
    fn a01() {
        assert_eq!(ev("abs", &[i(-10)]), i(10));
    }
    #[test]
    fn a02() {
        assert_eq!(ev("abs", &[i(-100)]), i(100));
    }
    #[test]
    fn a03() {
        assert_eq!(ev("abs", &[i(-1000)]), i(1000));
    }
    #[test]
    fn a04() {
        assert_eq!(ev("abs", &[i(42)]), i(42));
    }
    #[test]
    fn a05() {
        assert_eq!(ev("abs", &[i(-42)]), i(42));
    }
    #[test]
    fn a06() {
        assert_eq!(ev("abs", &[f(-0.5)]), f(0.5));
    }
    #[test]
    fn a07() {
        assert_eq!(ev("abs", &[f(-1.0)]), f(1.0));
    }
    #[test]
    fn a08() {
        assert_eq!(ev("abs", &[f(-2.5)]), f(2.5));
    }
    #[test]
    fn a09() {
        assert_eq!(ev("abs", &[f(-100.0)]), f(100.0));
    }
    #[test]
    fn a10() {
        assert_eq!(ev("abs", &[f(0.5)]), f(0.5));
    }
    #[test]
    fn a11() {
        assert_eq!(ev("abs", &[i(-7)]), i(7));
    }
    #[test]
    fn a12() {
        assert_eq!(ev("abs", &[i(-99)]), i(99));
    }
    #[test]
    fn a13() {
        assert_eq!(ev("abs", &[i(-50)]), i(50));
    }
    #[test]
    fn a14() {
        assert_eq!(ev("abs", &[i(-25)]), i(25));
    }
    #[test]
    fn a15() {
        assert_eq!(ev("abs", &[f(-9.99)]), f(9.99));
    }
    #[test]
    fn abs_diff_basic() {
        close(&ev("abs_diff", &[i(10), i(3)]), 7.0, 0.01);
    }
    #[test]
    fn abs_diff_rev() {
        close(&ev("abs_diff", &[i(3), i(10)]), 7.0, 0.01);
    }
    #[test]
    fn abs_diff_same() {
        close(&ev("abs_diff", &[i(5), i(5)]), 0.0, 0.01);
    }
    #[test]
    fn abs_diff_neg() {
        close(&ev("abs_diff", &[i(-3), i(3)]), 6.0, 0.01);
    }
    #[test]
    fn abs_diff_null() {
        assert_eq!(ev("abs_diff", &[null(), i(1)]), null());
    }
    #[test]
    fn a16() {
        assert_eq!(ev("abs", &[i(-13)]), i(13));
    }
    #[test]
    fn a17() {
        assert_eq!(ev("abs", &[i(-77)]), i(77));
    }
    #[test]
    fn a18() {
        assert_eq!(ev("abs", &[i(-33)]), i(33));
    }
    #[test]
    fn a19() {
        assert_eq!(ev("abs", &[f(-7.7)]), f(7.7));
    }
    #[test]
    fn a20() {
        assert_eq!(ev("abs", &[f(-0.1)]), f(0.1));
    }
    #[test]
    fn a21() {
        assert_eq!(ev("abs", &[i(1)]), i(1));
    }
}

// round / floor / ceil — 60 tests
mod round_floor_ceil_t02 {
    use super::*;
    #[test]
    fn round_down() {
        close(&ev("round", &[f(3.3)]), 3.0, 0.01);
    }
    #[test]
    fn round_up() {
        close(&ev("round", &[f(3.7)]), 4.0, 0.01);
    }
    #[test]
    fn round_half() {
        close(&ev("round", &[f(2.5)]), 3.0, 0.01);
    }
    #[test]
    fn round_zero() {
        close(&ev("round", &[f(0.0)]), 0.0, 0.01);
    }
    #[test]
    fn round_neg() {
        close(&ev("round", &[f(-2.3)]), -2.0, 0.01);
    }
    #[test]
    fn round_null() {
        assert_eq!(ev("round", &[null()]), null());
    }
    #[test]
    fn round_dec2() {
        close(&ev("round", &[f(3.14159), i(2)]), 3.14, 0.001);
    }
    #[test]
    fn round_dec3() {
        close(&ev("round", &[f(3.14159), i(3)]), 3.142, 0.0001);
    }
    #[test]
    fn round_int() {
        close(&ev("round", &[i(5)]), 5.0, 0.01);
    }
    #[test]
    fn round_large() {
        close(&ev("round", &[f(123.789), i(1)]), 123.8, 0.01);
    }
    #[test]
    fn floor_pos() {
        assert_eq!(ev("floor", &[f(3.7)]), f(3.0));
    }
    #[test]
    fn floor_neg() {
        assert_eq!(ev("floor", &[f(-3.2)]), f(-4.0));
    }
    #[test]
    fn floor_exact() {
        assert_eq!(ev("floor", &[f(5.0)]), f(5.0));
    }
    #[test]
    fn floor_zero() {
        assert_eq!(ev("floor", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn floor_null() {
        assert_eq!(ev("floor", &[null()]), null());
    }
    #[test]
    fn floor_small() {
        assert_eq!(ev("floor", &[f(0.1)]), f(0.0));
    }
    #[test]
    fn floor_neg_small() {
        assert_eq!(ev("floor", &[f(-0.1)]), f(-1.0));
    }
    #[test]
    fn floor_int() {
        assert_eq!(ev("floor", &[i(5)]), f(5.0));
    }
    #[test]
    fn floor_double_alias() {
        assert_eq!(ev("floor_double", &[f(3.7)]), f(3.0));
    }
    #[test]
    fn round_down_alias() {
        assert_eq!(ev("round_down", &[f(3.7)]), f(3.0));
    }
    #[test]
    fn ceil_pos() {
        assert_eq!(ev("ceil", &[f(3.1)]), f(4.0));
    }
    #[test]
    fn ceil_neg() {
        assert_eq!(ev("ceil", &[f(-3.7)]), f(-3.0));
    }
    #[test]
    fn ceil_exact() {
        assert_eq!(ev("ceil", &[f(5.0)]), f(5.0));
    }
    #[test]
    fn ceil_zero() {
        assert_eq!(ev("ceil", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn ceil_null() {
        assert_eq!(ev("ceil", &[null()]), null());
    }
    #[test]
    fn ceil_small() {
        assert_eq!(ev("ceil", &[f(0.1)]), f(1.0));
    }
    #[test]
    fn ceil_neg_small() {
        assert_eq!(ev("ceil", &[f(-0.1)]), f(0.0));
    }
    #[test]
    fn ceil_int() {
        assert_eq!(ev("ceil", &[i(5)]), f(5.0));
    }
    #[test]
    fn ceiling_alias() {
        assert_eq!(ev("ceiling", &[f(3.1)]), f(4.0));
    }
    #[test]
    fn trunc_pos() {
        close(&ev("trunc", &[f(3.7)]), 3.0, 0.01);
    }
    #[test]
    fn trunc_neg() {
        close(&ev("trunc", &[f(-3.7)]), -3.0, 0.01);
    }
    #[test]
    fn trunc_zero() {
        close(&ev("trunc", &[f(0.0)]), 0.0, 0.01);
    }
    #[test]
    fn trunc_null() {
        assert_eq!(ev("trunc", &[null()]), null());
    }
    #[test]
    fn truncate_alias() {
        close(&ev("truncate", &[f(3.7)]), 3.0, 0.01);
    }
    #[test]
    fn rf01() {
        close(&ev("round", &[f(1.1)]), 1.0, 0.01);
    }
    #[test]
    fn rf02() {
        close(&ev("round", &[f(1.9)]), 2.0, 0.01);
    }
    #[test]
    fn rf03() {
        close(&ev("round", &[f(2.4)]), 2.0, 0.01);
    }
    #[test]
    fn rf04() {
        close(&ev("round", &[f(2.6)]), 3.0, 0.01);
    }
    #[test]
    fn rf05() {
        assert_eq!(ev("floor", &[f(9.9)]), f(9.0));
    }
    #[test]
    fn rf06() {
        assert_eq!(ev("floor", &[f(0.99)]), f(0.0));
    }
    #[test]
    fn rf07() {
        assert_eq!(ev("ceil", &[f(0.01)]), f(1.0));
    }
    #[test]
    fn rf08() {
        assert_eq!(ev("ceil", &[f(9.01)]), f(10.0));
    }
    #[test]
    fn rf09() {
        assert_eq!(ev("floor", &[f(-1.5)]), f(-2.0));
    }
    #[test]
    fn rf10() {
        assert_eq!(ev("ceil", &[f(-1.5)]), f(-1.0));
    }
    #[test]
    fn rf11() {
        close(&ev("round", &[f(-1.5)]), -2.0, 0.01);
    }
    #[test]
    fn rf12() {
        close(&ev("round", &[f(0.5)]), 1.0, 0.01);
    }
    #[test]
    fn rf13() {
        assert_eq!(ev("floor", &[f(100.1)]), f(100.0));
    }
    #[test]
    fn rf14() {
        assert_eq!(ev("ceil", &[f(100.1)]), f(101.0));
    }
    #[test]
    fn rf15() {
        close(&ev("trunc", &[f(9.999)]), 9.0, 0.01);
    }
    #[test]
    fn rf16() {
        close(&ev("trunc", &[f(-9.999)]), -9.0, 0.01);
    }
    #[test]
    fn rf17() {
        assert_eq!(ev("floor", &[f(1.0)]), f(1.0));
    }
    #[test]
    fn rf18() {
        assert_eq!(ev("ceil", &[f(1.0)]), f(1.0));
    }
    #[test]
    fn rf19() {
        close(&ev("round", &[f(1.0)]), 1.0, 0.01);
    }
    #[test]
    fn rf20() {
        close(&ev("trunc", &[f(1.0)]), 1.0, 0.01);
    }
    #[test]
    fn rf21() {
        assert_eq!(ev("floor", &[f(-0.5)]), f(-1.0));
    }
    #[test]
    fn rf22() {
        assert_eq!(ev("ceil", &[f(-0.5)]), f(0.0));
    }
    #[test]
    fn rf23() {
        assert_eq!(ev("floor", &[f(2.5)]), f(2.0));
    }
    #[test]
    fn rf24() {
        assert_eq!(ev("ceil", &[f(2.5)]), f(3.0));
    }
    #[test]
    fn rf25() {
        close(&ev("round", &[f(99.5)]), 100.0, 0.01);
    }
    #[test]
    fn rf26() {
        assert_eq!(ev("floor", &[f(99.5)]), f(99.0));
    }
}

// sqrt / cbrt / pow / square — 50 tests
mod sqrt_pow_t02 {
    use super::*;
    #[test]
    fn sqrt_4() {
        close(&ev("sqrt", &[f(4.0)]), 2.0, 0.001);
    }
    #[test]
    fn sqrt_9() {
        close(&ev("sqrt", &[f(9.0)]), 3.0, 0.001);
    }
    #[test]
    fn sqrt_16() {
        close(&ev("sqrt", &[f(16.0)]), 4.0, 0.001);
    }
    #[test]
    fn sqrt_25() {
        close(&ev("sqrt", &[f(25.0)]), 5.0, 0.001);
    }
    #[test]
    fn sqrt_1() {
        close(&ev("sqrt", &[f(1.0)]), 1.0, 0.001);
    }
    #[test]
    fn sqrt_0() {
        close(&ev("sqrt", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn sqrt_2() {
        close(&ev("sqrt", &[f(2.0)]), 1.414, 0.01);
    }
    #[test]
    fn sqrt_null() {
        assert_eq!(ev("sqrt", &[null()]), null());
    }
    #[test]
    fn sqrt_100() {
        close(&ev("sqrt", &[f(100.0)]), 10.0, 0.001);
    }
    #[test]
    fn sqrt_int() {
        close(&ev("sqrt", &[i(49)]), 7.0, 0.001);
    }
    #[test]
    fn cbrt_8() {
        close(&ev("cbrt", &[f(8.0)]), 2.0, 0.001);
    }
    #[test]
    fn cbrt_27() {
        close(&ev("cbrt", &[f(27.0)]), 3.0, 0.001);
    }
    #[test]
    fn cbrt_64() {
        close(&ev("cbrt", &[f(64.0)]), 4.0, 0.001);
    }
    #[test]
    fn cbrt_1() {
        close(&ev("cbrt", &[f(1.0)]), 1.0, 0.001);
    }
    #[test]
    fn cbrt_0() {
        close(&ev("cbrt", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn cbrt_null() {
        assert_eq!(ev("cbrt", &[null()]), null());
    }
    #[test]
    fn pow_2_3() {
        close(&ev("pow", &[f(2.0), f(3.0)]), 8.0, 0.001);
    }
    #[test]
    fn pow_3_2() {
        close(&ev("pow", &[f(3.0), f(2.0)]), 9.0, 0.001);
    }
    #[test]
    fn pow_2_10() {
        close(&ev("pow", &[f(2.0), f(10.0)]), 1024.0, 0.001);
    }
    #[test]
    fn pow_1_any() {
        close(&ev("pow", &[f(1.0), f(100.0)]), 1.0, 0.001);
    }
    #[test]
    fn pow_any_0() {
        close(&ev("pow", &[f(5.0), f(0.0)]), 1.0, 0.001);
    }
    #[test]
    fn pow_null() {
        assert_eq!(ev("pow", &[null(), f(2.0)]), null());
    }
    #[test]
    fn power_alias() {
        close(&ev("power", &[f(2.0), f(3.0)]), 8.0, 0.001);
    }
    #[test]
    fn square_2() {
        close(&ev("square", &[f(2.0)]), 4.0, 0.001);
    }
    #[test]
    fn square_3() {
        close(&ev("square", &[f(3.0)]), 9.0, 0.001);
    }
    #[test]
    fn square_0() {
        close(&ev("square", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn square_neg() {
        close(&ev("square", &[f(-3.0)]), 9.0, 0.001);
    }
    #[test]
    fn square_null() {
        assert_eq!(ev("square", &[null()]), null());
    }
    #[test]
    fn square_int() {
        close(&ev("square", &[i(5)]), 25.0, 0.001);
    }
    #[test]
    fn sp01() {
        close(&ev("sqrt", &[f(36.0)]), 6.0, 0.001);
    }
    #[test]
    fn sp02() {
        close(&ev("sqrt", &[f(64.0)]), 8.0, 0.001);
    }
    #[test]
    fn sp03() {
        close(&ev("sqrt", &[f(81.0)]), 9.0, 0.001);
    }
    #[test]
    fn sp04() {
        close(&ev("pow", &[f(5.0), f(2.0)]), 25.0, 0.001);
    }
    #[test]
    fn sp05() {
        close(&ev("pow", &[f(10.0), f(3.0)]), 1000.0, 0.001);
    }
    #[test]
    fn sp06() {
        close(&ev("pow", &[f(2.0), f(0.5)]), 1.414, 0.01);
    }
    #[test]
    fn sp07() {
        close(&ev("square", &[f(4.0)]), 16.0, 0.001);
    }
    #[test]
    fn sp08() {
        close(&ev("square", &[f(10.0)]), 100.0, 0.001);
    }
    #[test]
    fn sp09() {
        close(&ev("cbrt", &[f(125.0)]), 5.0, 0.001);
    }
    #[test]
    fn sp10() {
        close(&ev("cbrt", &[f(1000.0)]), 10.0, 0.001);
    }
    #[test]
    fn sp11() {
        close(&ev("square", &[f(7.0)]), 49.0, 0.001);
    }
    #[test]
    fn sp12() {
        close(&ev("pow", &[f(4.0), f(3.0)]), 64.0, 0.001);
    }
    #[test]
    fn sp13() {
        close(&ev("sqrt", &[i(144)]), 12.0, 0.001);
    }
    #[test]
    fn sp14() {
        close(&ev("sqrt", &[i(121)]), 11.0, 0.001);
    }
    #[test]
    fn sp15() {
        close(&ev("square", &[i(6)]), 36.0, 0.001);
    }
    #[test]
    fn sp16() {
        close(&ev("square", &[i(8)]), 64.0, 0.001);
    }
    #[test]
    fn sp17() {
        close(&ev("pow", &[f(2.0), f(8.0)]), 256.0, 0.001);
    }
    #[test]
    fn sp18() {
        close(&ev("pow", &[f(3.0), f(3.0)]), 27.0, 0.001);
    }
    #[test]
    fn sp19() {
        close(&ev("pow", &[f(3.0), f(4.0)]), 81.0, 0.001);
    }
    #[test]
    fn sp20() {
        close(&ev("pow", &[f(2.0), f(16.0)]), 65536.0, 0.1);
    }
}

// sign / negate / reciprocal — 40 tests
mod sign_t02 {
    use super::*;
    #[test]
    fn sign_pos() {
        assert_eq!(ev("sign", &[i(5)]), i(1));
    }
    #[test]
    fn sign_neg() {
        assert_eq!(ev("sign", &[i(-5)]), i(-1));
    }
    #[test]
    fn sign_zero() {
        assert_eq!(ev("sign", &[i(0)]), i(0));
    }
    #[test]
    fn sign_pos_f() {
        assert_eq!(ev("sign", &[f(3.14)]), i(1));
    }
    #[test]
    fn sign_neg_f() {
        assert_eq!(ev("sign", &[f(-3.14)]), i(-1));
    }
    #[test]
    fn sign_zero_f() {
        assert_eq!(ev("sign", &[f(0.0)]), i(0));
    }
    #[test]
    fn sign_null() {
        assert_eq!(ev("sign", &[null()]), null());
    }
    #[test]
    fn signum_alias() {
        assert_eq!(ev("signum", &[i(42)]), i(1));
    }
    #[test]
    fn negate_pos() {
        assert_eq!(ev("negate", &[i(5)]), i(-5));
    }
    #[test]
    fn negate_neg() {
        assert_eq!(ev("negate", &[i(-5)]), i(5));
    }
    #[test]
    fn negate_zero() {
        assert_eq!(ev("negate", &[i(0)]), i(0));
    }
    #[test]
    fn negate_f() {
        close(&ev("negate", &[f(3.14)]), -3.14, 0.001);
    }
    #[test]
    fn negate_null() {
        assert_eq!(ev("negate", &[null()]), null());
    }
    #[test]
    fn reciprocal_2() {
        close(&ev("reciprocal", &[f(2.0)]), 0.5, 0.001);
    }
    #[test]
    fn reciprocal_4() {
        close(&ev("reciprocal", &[f(4.0)]), 0.25, 0.001);
    }
    #[test]
    fn reciprocal_1() {
        close(&ev("reciprocal", &[f(1.0)]), 1.0, 0.001);
    }
    #[test]
    fn reciprocal_null() {
        assert_eq!(ev("reciprocal", &[null()]), null());
    }
    #[test]
    fn s01() {
        assert_eq!(ev("sign", &[i(100)]), i(1));
    }
    #[test]
    fn s02() {
        assert_eq!(ev("sign", &[i(-100)]), i(-1));
    }
    #[test]
    fn s03() {
        assert_eq!(ev("sign", &[i(1)]), i(1));
    }
    #[test]
    fn s04() {
        assert_eq!(ev("sign", &[i(-1)]), i(-1));
    }
    #[test]
    fn n01() {
        assert_eq!(ev("negate", &[i(10)]), i(-10));
    }
    #[test]
    fn n02() {
        assert_eq!(ev("negate", &[i(-10)]), i(10));
    }
    #[test]
    fn n03() {
        assert_eq!(ev("negate", &[i(100)]), i(-100));
    }
    #[test]
    fn n04() {
        assert_eq!(ev("negate", &[i(-100)]), i(100));
    }
    #[test]
    fn n05() {
        assert_eq!(ev("negate", &[i(1)]), i(-1));
    }
    #[test]
    fn n06() {
        assert_eq!(ev("negate", &[i(-1)]), i(1));
    }
    #[test]
    fn r01() {
        close(&ev("reciprocal", &[f(5.0)]), 0.2, 0.001);
    }
    #[test]
    fn r02() {
        close(&ev("reciprocal", &[f(10.0)]), 0.1, 0.001);
    }
    #[test]
    fn r03() {
        close(&ev("reciprocal", &[f(0.5)]), 2.0, 0.001);
    }
    #[test]
    fn r04() {
        close(&ev("reciprocal", &[f(0.25)]), 4.0, 0.001);
    }
    #[test]
    fn r05() {
        close(&ev("reciprocal", &[f(8.0)]), 0.125, 0.001);
    }
    #[test]
    fn s05() {
        assert_eq!(ev("sign", &[f(0.001)]), i(1));
    }
    #[test]
    fn s06() {
        assert_eq!(ev("sign", &[f(-0.001)]), i(-1));
    }
    #[test]
    fn n07() {
        close(&ev("negate", &[f(-1.5)]), 1.5, 0.001);
    }
    #[test]
    fn n08() {
        close(&ev("negate", &[f(2.5)]), -2.5, 0.001);
    }
    #[test]
    fn s07() {
        assert_eq!(ev("sign", &[i(999)]), i(1));
    }
    #[test]
    fn s08() {
        assert_eq!(ev("sign", &[i(-999)]), i(-1));
    }
    #[test]
    fn n09() {
        assert_eq!(ev("negate", &[i(42)]), i(-42));
    }
    #[test]
    fn n10() {
        assert_eq!(ev("negate", &[i(-42)]), i(42));
    }
}

// mod / div / gcd / lcm — 60 tests
mod mod_div_t02 {
    use super::*;
    #[test]
    fn mod_10_3() {
        close(&ev("mod", &[i(10), i(3)]), 1.0, 0.01);
    }
    #[test]
    fn mod_7_2() {
        close(&ev("mod", &[i(7), i(2)]), 1.0, 0.01);
    }
    #[test]
    fn mod_8_4() {
        close(&ev("mod", &[i(8), i(4)]), 0.0, 0.01);
    }
    #[test]
    fn mod_9_3() {
        close(&ev("mod", &[i(9), i(3)]), 0.0, 0.01);
    }
    #[test]
    fn mod_null() {
        assert_eq!(ev("mod", &[null(), i(3)]), null());
    }
    #[test]
    fn remainder_alias() {
        close(&ev("remainder", &[i(10), i(3)]), 1.0, 0.01);
    }
    #[test]
    fn modulo_alias() {
        close(&ev("modulo", &[i(10), i(3)]), 1.0, 0.01);
    }
    #[test]
    fn div_10_3() {
        close(&ev("div", &[i(10), i(3)]), 3.0, 0.01);
    }
    #[test]
    fn div_7_2() {
        close(&ev("div", &[i(7), i(2)]), 3.0, 0.01);
    }
    #[test]
    fn div_8_4() {
        close(&ev("div", &[i(8), i(4)]), 2.0, 0.01);
    }
    #[test]
    fn div_null() {
        assert_eq!(ev("div", &[null(), i(3)]), null());
    }
    #[test]
    fn gcd_12_8() {
        assert_eq!(ev("gcd", &[i(12), i(8)]), i(4));
    }
    #[test]
    fn gcd_15_5() {
        assert_eq!(ev("gcd", &[i(15), i(5)]), i(5));
    }
    #[test]
    fn gcd_7_3() {
        assert_eq!(ev("gcd", &[i(7), i(3)]), i(1));
    }
    #[test]
    fn gcd_0_5() {
        assert_eq!(ev("gcd", &[i(0), i(5)]), i(5));
    }
    #[test]
    fn gcd_null() {
        assert_eq!(ev("gcd", &[null(), i(5)]), null());
    }
    #[test]
    fn lcm_4_6() {
        assert_eq!(ev("lcm", &[i(4), i(6)]), i(12));
    }
    #[test]
    fn lcm_3_5() {
        assert_eq!(ev("lcm", &[i(3), i(5)]), i(15));
    }
    #[test]
    fn lcm_2_3() {
        assert_eq!(ev("lcm", &[i(2), i(3)]), i(6));
    }
    #[test]
    fn lcm_null() {
        assert_eq!(ev("lcm", &[null(), i(5)]), null());
    }
    #[test]
    fn md01() {
        close(&ev("mod", &[i(100), i(7)]), 2.0, 0.01);
    }
    #[test]
    fn md02() {
        close(&ev("mod", &[i(15), i(4)]), 3.0, 0.01);
    }
    #[test]
    fn md03() {
        close(&ev("mod", &[i(20), i(5)]), 0.0, 0.01);
    }
    #[test]
    fn md04() {
        close(&ev("mod", &[i(13), i(5)]), 3.0, 0.01);
    }
    #[test]
    fn md05() {
        close(&ev("div", &[i(100), i(7)]), 14.0, 0.01);
    }
    #[test]
    fn md06() {
        close(&ev("div", &[i(15), i(4)]), 3.0, 0.01);
    }
    #[test]
    fn md07() {
        close(&ev("div", &[i(20), i(5)]), 4.0, 0.01);
    }
    #[test]
    fn gcd_6_9() {
        assert_eq!(ev("gcd", &[i(6), i(9)]), i(3));
    }
    #[test]
    fn gcd_10_15() {
        assert_eq!(ev("gcd", &[i(10), i(15)]), i(5));
    }
    #[test]
    fn gcd_100_75() {
        assert_eq!(ev("gcd", &[i(100), i(75)]), i(25));
    }
    #[test]
    fn gcd_14_21() {
        assert_eq!(ev("gcd", &[i(14), i(21)]), i(7));
    }
    #[test]
    fn gcd_8_12() {
        assert_eq!(ev("gcd", &[i(8), i(12)]), i(4));
    }
    #[test]
    fn lcm_4_5() {
        assert_eq!(ev("lcm", &[i(4), i(5)]), i(20));
    }
    #[test]
    fn lcm_6_8() {
        assert_eq!(ev("lcm", &[i(6), i(8)]), i(24));
    }
    #[test]
    fn lcm_3_7() {
        assert_eq!(ev("lcm", &[i(3), i(7)]), i(21));
    }
    #[test]
    fn lcm_2_4() {
        assert_eq!(ev("lcm", &[i(2), i(4)]), i(4));
    }
    #[test]
    fn lcm_5_10() {
        assert_eq!(ev("lcm", &[i(5), i(10)]), i(10));
    }
    #[test]
    fn gcd_1_1() {
        assert_eq!(ev("gcd", &[i(1), i(1)]), i(1));
    }
    #[test]
    fn gcd_same() {
        assert_eq!(ev("gcd", &[i(7), i(7)]), i(7));
    }
    #[test]
    fn lcm_1_1() {
        assert_eq!(ev("lcm", &[i(1), i(1)]), i(1));
    }
    #[test]
    fn lcm_same() {
        assert_eq!(ev("lcm", &[i(7), i(7)]), i(7));
    }
    #[test]
    fn md08() {
        close(&ev("mod", &[i(17), i(5)]), 2.0, 0.01);
    }
    #[test]
    fn md09() {
        close(&ev("mod", &[i(25), i(6)]), 1.0, 0.01);
    }
    #[test]
    fn md10() {
        close(&ev("mod", &[i(30), i(7)]), 2.0, 0.01);
    }
    #[test]
    fn div_9_3() {
        close(&ev("div", &[i(9), i(3)]), 3.0, 0.01);
    }
    #[test]
    fn div_25_5() {
        close(&ev("div", &[i(25), i(5)]), 5.0, 0.01);
    }
    #[test]
    fn div_17_3() {
        close(&ev("div", &[i(17), i(3)]), 5.0, 0.01);
    }
    #[test]
    fn div_50_7() {
        close(&ev("div", &[i(50), i(7)]), 7.0, 0.01);
    }
    #[test]
    fn gcd_20_30() {
        assert_eq!(ev("gcd", &[i(20), i(30)]), i(10));
    }
    #[test]
    fn lcm_12_18() {
        assert_eq!(ev("lcm", &[i(12), i(18)]), i(36));
    }
    #[test]
    fn gcd_48_36() {
        assert_eq!(ev("gcd", &[i(48), i(36)]), i(12));
    }
    #[test]
    fn lcm_7_11() {
        assert_eq!(ev("lcm", &[i(7), i(11)]), i(77));
    }
    #[test]
    fn gcd_17_13() {
        assert_eq!(ev("gcd", &[i(17), i(13)]), i(1));
    }
    #[test]
    fn lcm_9_12() {
        assert_eq!(ev("lcm", &[i(9), i(12)]), i(36));
    }
    #[test]
    fn md11() {
        close(&ev("mod", &[i(100), i(10)]), 0.0, 0.01);
    }
    #[test]
    fn md12() {
        close(&ev("mod", &[i(101), i(10)]), 1.0, 0.01);
    }
    #[test]
    fn md13() {
        close(&ev("mod", &[i(99), i(10)]), 9.0, 0.01);
    }
    #[test]
    fn md14() {
        close(&ev("div", &[i(100), i(10)]), 10.0, 0.01);
    }
    #[test]
    fn md15() {
        close(&ev("div", &[i(99), i(10)]), 9.0, 0.01);
    }
    #[test]
    fn md16() {
        close(&ev("div", &[i(101), i(10)]), 10.0, 0.01);
    }
}

// exp / log / log2 / log10 / ln — 50 tests
mod exp_log_t02 {
    use super::*;
    #[test]
    fn exp_0() {
        close(&ev("exp", &[f(0.0)]), 1.0, 0.001);
    }
    #[test]
    fn exp_1() {
        close(&ev("exp", &[f(1.0)]), 2.718, 0.01);
    }
    #[test]
    fn exp_2() {
        close(&ev("exp", &[f(2.0)]), 7.389, 0.01);
    }
    #[test]
    fn exp_neg1() {
        close(&ev("exp", &[f(-1.0)]), 0.368, 0.01);
    }
    #[test]
    fn exp_null() {
        assert_eq!(ev("exp", &[null()]), null());
    }
    #[test]
    fn log_1() {
        close(&ev("log", &[f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn log_e() {
        close(&ev("log", &[f(2.718281828)]), 1.0, 0.01);
    }
    #[test]
    fn log_null() {
        assert_eq!(ev("log", &[null()]), null());
    }
    #[test]
    fn ln_alias() {
        close(&ev("ln", &[f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn log2_1() {
        close(&ev("log2", &[f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn log2_2() {
        close(&ev("log2", &[f(2.0)]), 1.0, 0.001);
    }
    #[test]
    fn log2_4() {
        close(&ev("log2", &[f(4.0)]), 2.0, 0.001);
    }
    #[test]
    fn log2_8() {
        close(&ev("log2", &[f(8.0)]), 3.0, 0.001);
    }
    #[test]
    fn log2_16() {
        close(&ev("log2", &[f(16.0)]), 4.0, 0.001);
    }
    #[test]
    fn log2_null() {
        assert_eq!(ev("log2", &[null()]), null());
    }
    #[test]
    fn log10_1() {
        close(&ev("log10", &[f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn log10_10() {
        close(&ev("log10", &[f(10.0)]), 1.0, 0.001);
    }
    #[test]
    fn log10_100() {
        close(&ev("log10", &[f(100.0)]), 2.0, 0.001);
    }
    #[test]
    fn log10_1000() {
        close(&ev("log10", &[f(1000.0)]), 3.0, 0.001);
    }
    #[test]
    fn log10_null() {
        assert_eq!(ev("log10", &[null()]), null());
    }
    #[test]
    fn el01() {
        close(&ev("exp", &[f(3.0)]), 20.0855, 0.01);
    }
    #[test]
    fn el02() {
        close(&ev("log", &[f(10.0)]), 2.302, 0.01);
    }
    #[test]
    fn el03() {
        close(&ev("log", &[f(100.0)]), 4.605, 0.01);
    }
    #[test]
    fn el04() {
        close(&ev("log2", &[f(32.0)]), 5.0, 0.001);
    }
    #[test]
    fn el05() {
        close(&ev("log2", &[f(64.0)]), 6.0, 0.001);
    }
    #[test]
    fn el06() {
        close(&ev("log2", &[f(128.0)]), 7.0, 0.001);
    }
    #[test]
    fn el07() {
        close(&ev("log2", &[f(256.0)]), 8.0, 0.001);
    }
    #[test]
    fn el08() {
        close(&ev("log10", &[f(10000.0)]), 4.0, 0.001);
    }
    #[test]
    fn el09() {
        close(&ev("exp", &[i(0)]), 1.0, 0.001);
    }
    #[test]
    fn el10() {
        close(&ev("exp", &[i(1)]), 2.718, 0.01);
    }
    #[test]
    fn el11() {
        close(&ev("log2", &[f(512.0)]), 9.0, 0.001);
    }
    #[test]
    fn el12() {
        close(&ev("log2", &[f(1024.0)]), 10.0, 0.001);
    }
    #[test]
    fn el13() {
        close(&ev("log10", &[f(100000.0)]), 5.0, 0.001);
    }
    #[test]
    fn el14() {
        close(&ev("log", &[f(50.0)]), 3.912, 0.01);
    }
    #[test]
    fn el15() {
        close(&ev("exp", &[f(0.5)]), 1.6487, 0.01);
    }
    #[test]
    fn el16() {
        close(&ev("exp", &[f(-2.0)]), 0.1353, 0.01);
    }
    #[test]
    fn el17() {
        close(&ev("log", &[f(0.5)]), -0.693, 0.01);
    }
    #[test]
    fn el18() {
        close(&ev("log2", &[f(0.5)]), -1.0, 0.001);
    }
    #[test]
    fn el19() {
        close(&ev("log10", &[f(0.1)]), -1.0, 0.001);
    }
    #[test]
    fn el20() {
        close(&ev("log10", &[f(0.01)]), -2.0, 0.001);
    }
    #[test]
    fn e_const() {
        close(&ev("e", &[]), 2.71828, 0.001);
    }
    #[test]
    fn tau_const() {
        close(&ev("tau", &[]), 6.28318, 0.001);
    }
    #[test]
    fn pi_const() {
        close(&ev("pi", &[]), 3.14159, 0.001);
    }
    #[test]
    fn el21() {
        close(&ev("log2", &[f(2048.0)]), 11.0, 0.001);
    }
    #[test]
    fn el22() {
        close(&ev("log2", &[f(4096.0)]), 12.0, 0.001);
    }
    #[test]
    fn el23() {
        close(&ev("exp", &[f(4.0)]), 54.598, 0.01);
    }
    #[test]
    fn el24() {
        close(&ev("exp", &[f(5.0)]), 148.413, 0.01);
    }
    #[test]
    fn el25() {
        close(&ev("log", &[f(1000.0)]), 6.908, 0.01);
    }
    #[test]
    fn el26() {
        close(&ev("exp", &[f(-0.5)]), 0.6065, 0.01);
    }
    #[test]
    fn el27() {
        close(&ev("log10", &[f(50.0)]), 1.699, 0.01);
    }
}

// sin / cos / tan / asin / acos / atan — 60 tests
mod trig_t02 {
    use super::*;
    #[test]
    fn sin_0() {
        close(&ev("sin", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn sin_pi2() {
        close(&ev("sin", &[f(std::f64::consts::FRAC_PI_2)]), 1.0, 0.001);
    }
    #[test]
    fn sin_pi() {
        close(&ev("sin", &[f(std::f64::consts::PI)]), 0.0, 0.001);
    }
    #[test]
    fn sin_null() {
        assert_eq!(ev("sin", &[null()]), null());
    }
    #[test]
    fn cos_0() {
        close(&ev("cos", &[f(0.0)]), 1.0, 0.001);
    }
    #[test]
    fn cos_pi2() {
        close(&ev("cos", &[f(std::f64::consts::FRAC_PI_2)]), 0.0, 0.001);
    }
    #[test]
    fn cos_pi() {
        close(&ev("cos", &[f(std::f64::consts::PI)]), -1.0, 0.001);
    }
    #[test]
    fn cos_null() {
        assert_eq!(ev("cos", &[null()]), null());
    }
    #[test]
    fn tan_0() {
        close(&ev("tan", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn tan_null() {
        assert_eq!(ev("tan", &[null()]), null());
    }
    #[test]
    fn asin_0() {
        close(&ev("asin", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn asin_1() {
        close(&ev("asin", &[f(1.0)]), std::f64::consts::FRAC_PI_2, 0.001);
    }
    #[test]
    fn asin_null() {
        assert_eq!(ev("asin", &[null()]), null());
    }
    #[test]
    fn acos_1() {
        close(&ev("acos", &[f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn acos_0() {
        close(&ev("acos", &[f(0.0)]), std::f64::consts::FRAC_PI_2, 0.001);
    }
    #[test]
    fn acos_null() {
        assert_eq!(ev("acos", &[null()]), null());
    }
    #[test]
    fn atan_0() {
        close(&ev("atan", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn atan_1() {
        close(&ev("atan", &[f(1.0)]), std::f64::consts::FRAC_PI_4, 0.001);
    }
    #[test]
    fn atan_null() {
        assert_eq!(ev("atan", &[null()]), null());
    }
    #[test]
    fn atan2_1_1() {
        close(
            &ev("atan2", &[f(1.0), f(1.0)]),
            std::f64::consts::FRAC_PI_4,
            0.001,
        );
    }
    #[test]
    fn atan2_null() {
        assert_eq!(ev("atan2", &[null(), f(1.0)]), null());
    }
    #[test]
    fn sinh_0() {
        close(&ev("sinh", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn cosh_0() {
        close(&ev("cosh", &[f(0.0)]), 1.0, 0.001);
    }
    #[test]
    fn tanh_0() {
        close(&ev("tanh", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn sinh_null() {
        assert_eq!(ev("sinh", &[null()]), null());
    }
    #[test]
    fn cosh_null() {
        assert_eq!(ev("cosh", &[null()]), null());
    }
    #[test]
    fn tanh_null() {
        assert_eq!(ev("tanh", &[null()]), null());
    }
    #[test]
    fn degrees_pi() {
        close(&ev("degrees", &[f(std::f64::consts::PI)]), 180.0, 0.001);
    }
    #[test]
    fn degrees_pi2() {
        close(
            &ev("degrees", &[f(std::f64::consts::FRAC_PI_2)]),
            90.0,
            0.001,
        );
    }
    #[test]
    fn degrees_null() {
        assert_eq!(ev("degrees", &[null()]), null());
    }
    #[test]
    fn radians_180() {
        close(&ev("radians", &[f(180.0)]), std::f64::consts::PI, 0.001);
    }
    #[test]
    fn radians_90() {
        close(
            &ev("radians", &[f(90.0)]),
            std::f64::consts::FRAC_PI_2,
            0.001,
        );
    }
    #[test]
    fn radians_null() {
        assert_eq!(ev("radians", &[null()]), null());
    }
    #[test]
    fn degrees_0() {
        close(&ev("degrees", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn radians_0() {
        close(&ev("radians", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn radians_360() {
        close(
            &ev("radians", &[f(360.0)]),
            2.0 * std::f64::consts::PI,
            0.001,
        );
    }
    #[test]
    fn degrees_2pi() {
        close(
            &ev("degrees", &[f(2.0 * std::f64::consts::PI)]),
            360.0,
            0.001,
        );
    }
    #[test]
    fn sin_pi6() {
        close(&ev("sin", &[f(std::f64::consts::FRAC_PI_6)]), 0.5, 0.001);
    }
    #[test]
    fn cos_pi3() {
        close(&ev("cos", &[f(std::f64::consts::FRAC_PI_3)]), 0.5, 0.001);
    }
    #[test]
    fn t01() {
        close(&ev("sin", &[f(1.0)]), 0.8415, 0.01);
    }
    #[test]
    fn t02() {
        close(&ev("cos", &[f(1.0)]), 0.5403, 0.01);
    }
    #[test]
    fn t03() {
        close(&ev("tan", &[f(1.0)]), 1.5574, 0.01);
    }
    #[test]
    fn t04() {
        close(&ev("sinh", &[f(1.0)]), 1.1752, 0.01);
    }
    #[test]
    fn t05() {
        close(&ev("cosh", &[f(1.0)]), 1.5431, 0.01);
    }
    #[test]
    fn t06() {
        close(&ev("tanh", &[f(1.0)]), 0.7616, 0.01);
    }
    #[test]
    fn t07() {
        close(&ev("asin", &[f(0.5)]), 0.5236, 0.01);
    }
    #[test]
    fn t08() {
        close(&ev("acos", &[f(0.5)]), 1.0472, 0.01);
    }
    #[test]
    fn t09() {
        close(&ev("atan", &[f(0.5)]), 0.4636, 0.01);
    }
    #[test]
    fn t10() {
        close(
            &ev("atan2", &[f(1.0), f(0.0)]),
            std::f64::consts::FRAC_PI_2,
            0.001,
        );
    }
    #[test]
    fn t11() {
        close(&ev("degrees", &[f(1.0)]), 57.2958, 0.01);
    }
    #[test]
    fn t12() {
        close(&ev("radians", &[f(45.0)]), 0.7854, 0.01);
    }
    #[test]
    fn hypot_3_4() {
        close(&ev("hypot", &[f(3.0), f(4.0)]), 5.0, 0.001);
    }
    #[test]
    fn hypot_5_12() {
        close(&ev("hypot", &[f(5.0), f(12.0)]), 13.0, 0.001);
    }
    #[test]
    fn hypot_null() {
        assert_eq!(ev("hypot", &[null(), f(1.0)]), null());
    }
    #[test]
    fn hypot_0_0() {
        close(&ev("hypot", &[f(0.0), f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn t13() {
        close(&ev("sin", &[f(2.0)]), 0.9093, 0.01);
    }
    #[test]
    fn t14() {
        close(&ev("cos", &[f(2.0)]), -0.4161, 0.01);
    }
    #[test]
    fn t15() {
        close(&ev("sin", &[f(-1.0)]), -0.8415, 0.01);
    }
    #[test]
    fn t16() {
        close(&ev("cos", &[f(-1.0)]), 0.5403, 0.01);
    }
}

// bit ops — 50 tests
mod bitops_t02 {
    use super::*;
    #[test]
    fn and_ff() {
        assert_eq!(ev("bit_and", &[i(0xFF), i(0x0F)]), i(0x0F));
    }
    #[test]
    fn or_f0() {
        assert_eq!(ev("bit_or", &[i(0xF0), i(0x0F)]), i(0xFF));
    }
    #[test]
    fn xor_ff() {
        assert_eq!(ev("bit_xor", &[i(0xFF), i(0xFF)]), i(0));
    }
    #[test]
    fn not_0() {
        assert_eq!(ev("bit_not", &[i(0)]), i(-1));
    }
    #[test]
    fn and_null() {
        assert_eq!(ev("bit_and", &[null(), i(1)]), null());
    }
    #[test]
    fn or_null() {
        assert_eq!(ev("bit_or", &[null(), i(1)]), null());
    }
    #[test]
    fn xor_null() {
        assert_eq!(ev("bit_xor", &[null(), i(1)]), null());
    }
    #[test]
    fn not_null() {
        assert_eq!(ev("bit_not", &[null()]), null());
    }
    #[test]
    fn shl_1_4() {
        assert_eq!(ev("bit_shift_left", &[i(1), i(4)]), i(16));
    }
    #[test]
    fn shr_16_4() {
        assert_eq!(ev("bit_shift_right", &[i(16), i(4)]), i(1));
    }
    #[test]
    fn and_0() {
        assert_eq!(ev("bit_and", &[i(0), i(0xFF)]), i(0));
    }
    #[test]
    fn or_0() {
        assert_eq!(ev("bit_or", &[i(0), i(0xFF)]), i(0xFF));
    }
    #[test]
    fn xor_0() {
        assert_eq!(ev("bit_xor", &[i(0), i(0xFF)]), i(0xFF));
    }
    #[test]
    fn xor_same() {
        assert_eq!(ev("bit_xor", &[i(42), i(42)]), i(0));
    }
    #[test]
    fn and_same() {
        assert_eq!(ev("bit_and", &[i(42), i(42)]), i(42));
    }
    #[test]
    fn or_same() {
        assert_eq!(ev("bit_or", &[i(42), i(42)]), i(42));
    }
    #[test]
    fn shl_1_0() {
        assert_eq!(ev("bit_shift_left", &[i(1), i(0)]), i(1));
    }
    #[test]
    fn shr_1_0() {
        assert_eq!(ev("bit_shift_right", &[i(1), i(0)]), i(1));
    }
    #[test]
    fn shl_1_1() {
        assert_eq!(ev("bit_shift_left", &[i(1), i(1)]), i(2));
    }
    #[test]
    fn shl_1_2() {
        assert_eq!(ev("bit_shift_left", &[i(1), i(2)]), i(4));
    }
    #[test]
    fn shl_1_3() {
        assert_eq!(ev("bit_shift_left", &[i(1), i(3)]), i(8));
    }
    #[test]
    fn shl_1_8() {
        assert_eq!(ev("bit_shift_left", &[i(1), i(8)]), i(256));
    }
    #[test]
    fn shr_256_8() {
        assert_eq!(ev("bit_shift_right", &[i(256), i(8)]), i(1));
    }
    #[test]
    fn bit_count_0() {
        assert_eq!(ev("bit_count", &[i(0)]), i(0));
    }
    #[test]
    fn bit_count_1() {
        assert_eq!(ev("bit_count", &[i(1)]), i(1));
    }
    #[test]
    fn bit_count_7() {
        assert_eq!(ev("bit_count", &[i(7)]), i(3));
    }
    #[test]
    fn bit_count_255() {
        assert_eq!(ev("bit_count", &[i(255)]), i(8));
    }
    #[test]
    fn bit_count_null() {
        assert_eq!(ev("bit_count", &[null()]), null());
    }
    #[test]
    fn popcount_alias() {
        assert_eq!(ev("popcount", &[i(7)]), i(3));
    }
    #[test]
    fn leading_zeros_1() {
        let r = ev("leading_zeros", &[i(1)]);
        assert!(matches!(r, Value::I64(v) if v > 0));
    }
    #[test]
    fn leading_zeros_null() {
        assert_eq!(ev("leading_zeros", &[null()]), null());
    }
    #[test]
    fn trailing_zeros_2() {
        assert_eq!(ev("trailing_zeros", &[i(2)]), i(1));
    }
    #[test]
    fn trailing_zeros_4() {
        assert_eq!(ev("trailing_zeros", &[i(4)]), i(2));
    }
    #[test]
    fn trailing_zeros_8() {
        assert_eq!(ev("trailing_zeros", &[i(8)]), i(3));
    }
    #[test]
    fn trailing_zeros_null() {
        assert_eq!(ev("trailing_zeros", &[null()]), null());
    }
    #[test]
    fn b01() {
        assert_eq!(ev("bit_and", &[i(15), i(9)]), i(9));
    }
    #[test]
    fn b02() {
        assert_eq!(ev("bit_or", &[i(12), i(10)]), i(14));
    }
    #[test]
    fn b03() {
        assert_eq!(ev("bit_xor", &[i(12), i(10)]), i(6));
    }
    #[test]
    fn b04() {
        assert_eq!(ev("bit_count", &[i(15)]), i(4));
    }
    #[test]
    fn b05() {
        assert_eq!(ev("bit_count", &[i(31)]), i(5));
    }
    #[test]
    fn b06() {
        assert_eq!(ev("bit_count", &[i(63)]), i(6));
    }
    #[test]
    fn b07() {
        assert_eq!(ev("bit_count", &[i(127)]), i(7));
    }
    #[test]
    fn shl_3_2() {
        assert_eq!(ev("bit_shift_left", &[i(3), i(2)]), i(12));
    }
    #[test]
    fn shr_12_2() {
        assert_eq!(ev("bit_shift_right", &[i(12), i(2)]), i(3));
    }
    #[test]
    fn b08() {
        assert_eq!(ev("bit_and", &[i(7), i(5)]), i(5));
    }
    #[test]
    fn b09() {
        assert_eq!(ev("bit_or", &[i(5), i(2)]), i(7));
    }
    #[test]
    fn b10() {
        assert_eq!(ev("bit_xor", &[i(5), i(3)]), i(6));
    }
    #[test]
    fn next_pow2_3() {
        assert_eq!(ev("next_power_of_two", &[i(3)]), i(4));
    }
    #[test]
    fn next_pow2_5() {
        assert_eq!(ev("next_power_of_two", &[i(5)]), i(8));
    }
    #[test]
    fn next_pow2_8() {
        assert_eq!(ev("next_power_of_two", &[i(8)]), i(8));
    }
}

// clamp / lerp / fma / copysign — 40 tests
mod clamp_t02 {
    use super::*;
    #[test]
    fn clamp_mid() {
        close(&ev("clamp", &[f(5.0), f(0.0), f(10.0)]), 5.0, 0.001);
    }
    #[test]
    fn clamp_low() {
        close(&ev("clamp", &[f(-1.0), f(0.0), f(10.0)]), 0.0, 0.001);
    }
    #[test]
    fn clamp_high() {
        close(&ev("clamp", &[f(15.0), f(0.0), f(10.0)]), 10.0, 0.001);
    }
    #[test]
    fn clamp_at_min() {
        close(&ev("clamp", &[f(0.0), f(0.0), f(10.0)]), 0.0, 0.001);
    }
    #[test]
    fn clamp_at_max() {
        close(&ev("clamp", &[f(10.0), f(0.0), f(10.0)]), 10.0, 0.001);
    }
    #[test]
    fn clamp_null() {
        assert_eq!(ev("clamp", &[null(), f(0.0), f(10.0)]), null());
    }
    #[test]
    fn lerp_0() {
        close(&ev("lerp", &[f(0.0), f(10.0), f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn lerp_1() {
        close(&ev("lerp", &[f(0.0), f(10.0), f(1.0)]), 10.0, 0.001);
    }
    #[test]
    fn lerp_half() {
        close(&ev("lerp", &[f(0.0), f(10.0), f(0.5)]), 5.0, 0.001);
    }
    #[test]
    fn lerp_null() {
        assert_eq!(ev("lerp", &[null(), f(10.0), f(0.5)]), null());
    }
    #[test]
    fn fma_basic() {
        close(&ev("fma", &[f(2.0), f(3.0), f(4.0)]), 10.0, 0.001);
    }
    #[test]
    fn fma_zero() {
        close(&ev("fma", &[f(0.0), f(3.0), f(4.0)]), 4.0, 0.001);
    }
    #[test]
    fn fma_null() {
        assert_eq!(ev("fma", &[null(), f(3.0), f(4.0)]), null());
    }
    #[test]
    fn copysign_pos_neg() {
        close(&ev("copysign", &[f(1.0), f(-1.0)]), -1.0, 0.001);
    }
    #[test]
    fn copysign_neg_pos() {
        close(&ev("copysign", &[f(-1.0), f(1.0)]), 1.0, 0.001);
    }
    #[test]
    fn copysign_null() {
        assert_eq!(ev("copysign", &[null(), f(1.0)]), null());
    }
    #[test]
    fn cl01() {
        close(&ev("clamp", &[f(3.0), f(1.0), f(5.0)]), 3.0, 0.001);
    }
    #[test]
    fn cl02() {
        close(&ev("clamp", &[f(0.0), f(1.0), f(5.0)]), 1.0, 0.001);
    }
    #[test]
    fn cl03() {
        close(&ev("clamp", &[f(6.0), f(1.0), f(5.0)]), 5.0, 0.001);
    }
    #[test]
    fn cl04() {
        close(&ev("clamp", &[i(3), i(1), i(5)]), 3.0, 0.001);
    }
    #[test]
    fn cl05() {
        close(&ev("clamp", &[i(0), i(1), i(5)]), 1.0, 0.001);
    }
    #[test]
    fn cl06() {
        close(&ev("clamp", &[i(6), i(1), i(5)]), 5.0, 0.001);
    }
    #[test]
    fn lerp_25() {
        close(&ev("lerp", &[f(0.0), f(100.0), f(0.25)]), 25.0, 0.001);
    }
    #[test]
    fn lerp_75() {
        close(&ev("lerp", &[f(0.0), f(100.0), f(0.75)]), 75.0, 0.001);
    }
    #[test]
    fn fma_01() {
        close(&ev("fma", &[f(5.0), f(5.0), f(5.0)]), 30.0, 0.001);
    }
    #[test]
    fn fma_02() {
        close(&ev("fma", &[f(10.0), f(2.0), f(3.0)]), 23.0, 0.001);
    }
    #[test]
    fn cl07() {
        close(&ev("clamp", &[f(50.0), f(0.0), f(100.0)]), 50.0, 0.001);
    }
    #[test]
    fn cl08() {
        close(&ev("clamp", &[f(-50.0), f(0.0), f(100.0)]), 0.0, 0.001);
    }
    #[test]
    fn cl09() {
        close(&ev("clamp", &[f(150.0), f(0.0), f(100.0)]), 100.0, 0.001);
    }
    #[test]
    fn lerp_01() {
        close(&ev("lerp", &[f(10.0), f(20.0), f(0.5)]), 15.0, 0.001);
    }
    #[test]
    fn lerp_02() {
        close(&ev("lerp", &[f(10.0), f(20.0), f(0.0)]), 10.0, 0.001);
    }
    #[test]
    fn lerp_03() {
        close(&ev("lerp", &[f(10.0), f(20.0), f(1.0)]), 20.0, 0.001);
    }
    #[test]
    fn cp01() {
        close(&ev("copysign", &[f(5.0), f(-1.0)]), -5.0, 0.001);
    }
    #[test]
    fn cp02() {
        close(&ev("copysign", &[f(-5.0), f(1.0)]), 5.0, 0.001);
    }
    #[test]
    fn cp03() {
        close(&ev("copysign", &[f(5.0), f(1.0)]), 5.0, 0.001);
    }
    #[test]
    fn cp04() {
        close(&ev("copysign", &[f(-5.0), f(-1.0)]), -5.0, 0.001);
    }
    #[test]
    fn fma_03() {
        close(&ev("fma", &[f(3.0), f(4.0), f(5.0)]), 17.0, 0.001);
    }
    #[test]
    fn fma_04() {
        close(&ev("fma", &[f(7.0), f(8.0), f(9.0)]), 65.0, 0.001);
    }
    #[test]
    fn cl10() {
        close(&ev("clamp", &[f(2.5), f(1.0), f(3.0)]), 2.5, 0.001);
    }
    #[test]
    fn cl11() {
        close(&ev("clamp", &[f(0.5), f(1.0), f(3.0)]), 1.0, 0.001);
    }
}

// is_finite / is_nan / is_inf / is_positive / is_negative / is_zero / is_even / is_odd / between — 80 tests
mod predicates_t02 {
    use super::*;
    #[test]
    fn is_finite_ok() {
        assert_eq!(ev("is_finite", &[f(1.0)]), i(1));
    }
    #[test]
    fn is_finite_inf() {
        assert_eq!(ev("is_finite", &[f(f64::INFINITY)]), i(0));
    }
    #[test]
    fn is_finite_nan() {
        assert_eq!(ev("is_finite", &[f(f64::NAN)]), i(0));
    }
    #[test]
    fn is_finite_null() {
        assert_eq!(ev("is_finite", &[null()]), null());
    }
    #[test]
    fn is_nan_no() {
        assert_eq!(ev("is_nan", &[f(1.0)]), i(0));
    }
    #[test]
    fn is_nan_yes() {
        assert_eq!(ev("is_nan", &[f(f64::NAN)]), i(1));
    }
    #[test]
    fn is_nan_null() {
        assert_eq!(ev("is_nan", &[null()]), null());
    }
    #[test]
    fn is_inf_no() {
        assert_eq!(ev("is_inf", &[f(1.0)]), i(0));
    }
    #[test]
    fn is_inf_yes() {
        assert_eq!(ev("is_inf", &[f(f64::INFINITY)]), i(1));
    }
    #[test]
    fn is_inf_neg() {
        assert_eq!(ev("is_inf", &[f(f64::NEG_INFINITY)]), i(1));
    }
    #[test]
    fn is_inf_null() {
        assert_eq!(ev("is_inf", &[null()]), null());
    }
    #[test]
    fn is_positive_yes() {
        assert_eq!(ev("is_positive", &[i(5)]), i(1));
    }
    #[test]
    fn is_positive_no() {
        assert_eq!(ev("is_positive", &[i(-5)]), i(0));
    }
    #[test]
    fn is_positive_zero() {
        assert_eq!(ev("is_positive", &[i(0)]), i(0));
    }
    #[test]
    fn is_positive_null() {
        assert_eq!(ev("is_positive", &[null()]), null());
    }
    #[test]
    fn is_negative_yes() {
        assert_eq!(ev("is_negative", &[i(-5)]), i(1));
    }
    #[test]
    fn is_negative_no() {
        assert_eq!(ev("is_negative", &[i(5)]), i(0));
    }
    #[test]
    fn is_negative_zero() {
        assert_eq!(ev("is_negative", &[i(0)]), i(0));
    }
    #[test]
    fn is_negative_null() {
        assert_eq!(ev("is_negative", &[null()]), null());
    }
    #[test]
    fn is_zero_yes() {
        assert_eq!(ev("is_zero", &[i(0)]), i(1));
    }
    #[test]
    fn is_zero_no() {
        assert_eq!(ev("is_zero", &[i(1)]), i(0));
    }
    #[test]
    fn is_zero_null() {
        assert_eq!(ev("is_zero", &[null()]), null());
    }
    #[test]
    fn is_even_yes() {
        assert_eq!(ev("is_even", &[i(4)]), i(1));
    }
    #[test]
    fn is_even_no() {
        assert_eq!(ev("is_even", &[i(3)]), i(0));
    }
    #[test]
    fn is_even_zero() {
        assert_eq!(ev("is_even", &[i(0)]), i(1));
    }
    #[test]
    fn is_even_null() {
        assert_eq!(ev("is_even", &[null()]), null());
    }
    #[test]
    fn is_odd_yes() {
        assert_eq!(ev("is_odd", &[i(3)]), i(1));
    }
    #[test]
    fn is_odd_no() {
        assert_eq!(ev("is_odd", &[i(4)]), i(0));
    }
    #[test]
    fn is_odd_zero() {
        assert_eq!(ev("is_odd", &[i(0)]), i(0));
    }
    #[test]
    fn is_odd_null() {
        assert_eq!(ev("is_odd", &[null()]), null());
    }
    #[test]
    fn between_in() {
        assert_eq!(ev("between", &[i(5), i(1), i(10)]), i(1));
    }
    #[test]
    fn between_out() {
        assert_eq!(ev("between", &[i(15), i(1), i(10)]), i(0));
    }
    #[test]
    fn between_at_lo() {
        assert_eq!(ev("between", &[i(1), i(1), i(10)]), i(1));
    }
    #[test]
    fn between_at_hi() {
        assert_eq!(ev("between", &[i(10), i(1), i(10)]), i(1));
    }
    #[test]
    fn between_null() {
        assert_eq!(ev("between", &[null(), i(1), i(10)]), null());
    }
    #[test]
    fn p01() {
        assert_eq!(ev("is_positive", &[i(1)]), i(1));
    }
    #[test]
    fn p02() {
        assert_eq!(ev("is_positive", &[i(100)]), i(1));
    }
    #[test]
    fn p03() {
        assert_eq!(ev("is_negative", &[i(-1)]), i(1));
    }
    #[test]
    fn p04() {
        assert_eq!(ev("is_negative", &[i(-100)]), i(1));
    }
    #[test]
    fn p05() {
        assert_eq!(ev("is_zero", &[f(0.0)]), i(1));
    }
    #[test]
    fn p06() {
        assert_eq!(ev("is_zero", &[f(1.0)]), i(0));
    }
    #[test]
    fn p07() {
        assert_eq!(ev("is_even", &[i(2)]), i(1));
    }
    #[test]
    fn p08() {
        assert_eq!(ev("is_even", &[i(6)]), i(1));
    }
    #[test]
    fn p09() {
        assert_eq!(ev("is_even", &[i(8)]), i(1));
    }
    #[test]
    fn p10() {
        assert_eq!(ev("is_even", &[i(10)]), i(1));
    }
    #[test]
    fn p11() {
        assert_eq!(ev("is_odd", &[i(1)]), i(1));
    }
    #[test]
    fn p12() {
        assert_eq!(ev("is_odd", &[i(5)]), i(1));
    }
    #[test]
    fn p13() {
        assert_eq!(ev("is_odd", &[i(7)]), i(1));
    }
    #[test]
    fn p14() {
        assert_eq!(ev("is_odd", &[i(9)]), i(1));
    }
    #[test]
    fn p15() {
        assert_eq!(ev("between", &[i(5), i(5), i(5)]), i(1));
    }
    #[test]
    fn p16() {
        assert_eq!(ev("between", &[i(0), i(1), i(10)]), i(0));
    }
    #[test]
    fn p17() {
        assert_eq!(ev("between", &[i(11), i(1), i(10)]), i(0));
    }
    #[test]
    fn p18() {
        assert_eq!(ev("between", &[f(5.5), f(1.0), f(10.0)]), i(1));
    }
    #[test]
    fn p19() {
        assert_eq!(ev("between", &[f(0.5), f(1.0), f(10.0)]), i(0));
    }
    #[test]
    fn p20() {
        assert_eq!(ev("is_finite", &[i(42)]), i(1));
    }
    #[test]
    fn p21() {
        assert_eq!(ev("is_nan", &[i(42)]), i(0));
    }
    #[test]
    fn p22() {
        assert_eq!(ev("is_inf", &[i(42)]), i(0));
    }
    #[test]
    fn p23() {
        assert_eq!(ev("is_positive", &[f(0.1)]), i(1));
    }
    #[test]
    fn p24() {
        assert_eq!(ev("is_negative", &[f(-0.1)]), i(1));
    }
    #[test]
    fn p25() {
        assert_eq!(ev("between", &[i(3), i(1), i(5)]), i(1));
    }
    #[test]
    fn p26() {
        assert_eq!(ev("between", &[i(7), i(1), i(5)]), i(0));
    }
    #[test]
    fn p27() {
        assert_eq!(ev("is_even", &[i(-2)]), i(1));
    }
    #[test]
    fn p28() {
        assert_eq!(ev("is_odd", &[i(-3)]), i(1));
    }
    #[test]
    fn p29() {
        assert_eq!(ev("is_even", &[i(100)]), i(1));
    }
    #[test]
    fn p30() {
        assert_eq!(ev("is_odd", &[i(99)]), i(1));
    }
    #[test]
    fn p31() {
        assert_eq!(ev("between", &[i(50), i(0), i(100)]), i(1));
    }
    #[test]
    fn p32() {
        assert_eq!(ev("between", &[i(-1), i(0), i(100)]), i(0));
    }
    #[test]
    fn p33() {
        assert_eq!(ev("between", &[i(101), i(0), i(100)]), i(0));
    }
    #[test]
    fn p34() {
        assert_eq!(ev("is_positive", &[i(999)]), i(1));
    }
    #[test]
    fn p35() {
        assert_eq!(ev("is_negative", &[i(-999)]), i(1));
    }
    #[test]
    fn p36() {
        assert_eq!(ev("is_zero", &[i(0)]), i(1));
    }
    #[test]
    fn p37() {
        assert_eq!(ev("is_even", &[i(12)]), i(1));
    }
    #[test]
    fn p38() {
        assert_eq!(ev("is_odd", &[i(13)]), i(1));
    }
    #[test]
    fn p39() {
        assert_eq!(ev("between", &[i(2), i(1), i(3)]), i(1));
    }
    #[test]
    fn p40() {
        assert_eq!(ev("between", &[i(4), i(1), i(3)]), i(0));
    }
    #[test]
    fn p41() {
        assert_eq!(ev("is_positive", &[f(100.0)]), i(1));
    }
    #[test]
    fn p42() {
        assert_eq!(ev("is_negative", &[f(-100.0)]), i(1));
    }
    #[test]
    fn p43() {
        assert_eq!(ev("is_zero", &[i(42)]), i(0));
    }
    #[test]
    fn p44() {
        assert_eq!(ev("is_finite", &[f(0.0)]), i(1));
    }
    #[test]
    fn p45() {
        assert_eq!(ev("is_finite", &[f(-100.0)]), i(1));
    }
}

// factorial — 20 tests
mod factorial_t02 {
    use super::*;
    #[test]
    fn f0() {
        assert_eq!(ev("factorial", &[i(0)]), i(1));
    }
    #[test]
    fn f1() {
        assert_eq!(ev("factorial", &[i(1)]), i(1));
    }
    #[test]
    fn f2() {
        assert_eq!(ev("factorial", &[i(2)]), i(2));
    }
    #[test]
    fn f3() {
        assert_eq!(ev("factorial", &[i(3)]), i(6));
    }
    #[test]
    fn f4() {
        assert_eq!(ev("factorial", &[i(4)]), i(24));
    }
    #[test]
    fn f5() {
        assert_eq!(ev("factorial", &[i(5)]), i(120));
    }
    #[test]
    fn f6() {
        assert_eq!(ev("factorial", &[i(6)]), i(720));
    }
    #[test]
    fn f7() {
        assert_eq!(ev("factorial", &[i(7)]), i(5040));
    }
    #[test]
    fn f8() {
        assert_eq!(ev("factorial", &[i(8)]), i(40320));
    }
    #[test]
    fn f9() {
        assert_eq!(ev("factorial", &[i(9)]), i(362880));
    }
    #[test]
    fn f10() {
        assert_eq!(ev("factorial", &[i(10)]), i(3628800));
    }
    #[test]
    fn f11() {
        assert_eq!(ev("factorial", &[i(11)]), i(39916800));
    }
    #[test]
    fn f12() {
        assert_eq!(ev("factorial", &[i(12)]), i(479001600));
    }
    #[test]
    fn f13() {
        assert_eq!(ev("factorial", &[i(13)]), i(6227020800));
    }
    #[test]
    fn f14() {
        assert_eq!(ev("factorial", &[i(14)]), i(87178291200));
    }
    #[test]
    fn f15() {
        assert_eq!(ev("factorial", &[i(15)]), i(1307674368000));
    }
    #[test]
    fn f_null() {
        assert_eq!(ev("factorial", &[null()]), null());
    }
    #[test]
    fn f16() {
        assert_eq!(ev("factorial", &[i(16)]), i(20922789888000));
    }
    #[test]
    fn f17() {
        assert_eq!(ev("factorial", &[i(17)]), i(355687428096000));
    }
    #[test]
    fn f18() {
        assert_eq!(ev("factorial", &[i(18)]), i(6402373705728000));
    }
}

// random / rnd — 20 tests
mod rnd_t02 {
    use super::*;
    #[test]
    fn random_range() {
        let r = ev("random", &[]);
        assert!(matches!(r, Value::F64(v) if (0.0..1.0).contains(&v)));
    }
    #[test]
    fn rand_alias() {
        let r = ev("rand", &[]);
        assert!(matches!(r, Value::F64(_)));
    }
    #[test]
    fn rnd_int_returns_int() {
        let r = ev("rnd_int", &[]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn rnd_long_returns_int() {
        let r = ev("rnd_long", &[]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn rnd_double_returns_f64() {
        let r = ev("rnd_double", &[]);
        assert!(matches!(r, Value::F64(_)));
    }
    #[test]
    fn rnd_float_returns_f64() {
        let r = ev("rnd_float", &[]);
        assert!(matches!(r, Value::F64(_)));
    }
    #[test]
    fn rnd_boolean_returns() {
        let r = ev("rnd_boolean", &[]);
        assert!(matches!(r, Value::I64(0) | Value::I64(1)));
    }
    #[test]
    fn rnd_timestamp_returns_ts() {
        let r = ev("rnd_timestamp", &[]);
        assert!(matches!(r, Value::Timestamp(_)));
    }
    #[test]
    fn rnd_str_returns_str() {
        let r = ev("rnd_str", &[i(5)]);
        assert!(matches!(r, Value::Str(_)));
    }
    #[test]
    fn random_unique() {
        let results: Vec<_> = (0..10)
            .map(|_| format!("{:?}", ev("random", &[])))
            .collect();
        let unique: std::collections::HashSet<_> = results.iter().collect();
        assert!(
            unique.len() > 1,
            "10 calls to random() should produce at least 2 distinct values"
        );
    }
    #[test]
    fn r01() {
        let r = ev("random", &[]);
        assert!(matches!(r, Value::F64(v) if v >= 0.0));
    }
    #[test]
    fn r02() {
        let r = ev("random", &[]);
        assert!(matches!(r, Value::F64(v) if v < 1.0));
    }
    #[test]
    fn r03() {
        let r = ev("rnd_int", &[]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn r04() {
        let r = ev("rnd_double", &[]);
        assert!(matches!(r, Value::F64(_)));
    }
    #[test]
    fn r05() {
        let r = ev("rnd_boolean", &[]);
        assert!(matches!(r, Value::I64(0) | Value::I64(1)));
    }
    #[test]
    fn r06() {
        let r = ev("rnd_str", &[i(10)]);
        match r {
            Value::Str(v) => assert!(v.len() > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn infinity_fn() {
        let r = ev("infinity", &[]);
        assert!(matches!(r, Value::F64(v) if v.is_infinite()));
    }
    #[test]
    fn nan_fn() {
        let r = ev("nan", &[]);
        assert!(matches!(r, Value::F64(v) if v.is_nan()));
    }
    #[test]
    fn r07() {
        for _ in 0..10 {
            let r = ev("random", &[]);
            assert!(matches!(r, Value::F64(_)));
        }
    }
    #[test]
    fn r08() {
        for _ in 0..10 {
            let r = ev("rnd_boolean", &[]);
            assert!(matches!(r, Value::I64(0) | Value::I64(1)));
        }
    }
}

// width_bucket / map_range / log_base — 30 tests
mod misc_math_t02 {
    use super::*;
    #[test]
    fn wb_null() {
        assert_eq!(
            ev("width_bucket", &[null(), f(0.0), f(10.0), i(10)]),
            null()
        );
    }
    #[test]
    fn wb_returns_int() {
        let r = ev("width_bucket", &[f(5.0), f(0.0), f(10.0), i(10)]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn wb_below() {
        let r = ev("width_bucket", &[f(-1.0), f(0.0), f(10.0), i(10)]);
        assert_eq!(r, i(0));
    }
    #[test]
    fn wb_above() {
        let r = ev("width_bucket", &[f(11.0), f(0.0), f(10.0), i(10)]);
        assert_eq!(r, i(11));
    }
    #[test]
    fn wb_in_range() {
        let r = ev("width_bucket", &[f(5.0), f(0.0), f(10.0), i(10)]);
        match r {
            Value::I64(v) => assert!(v >= 1 && v <= 10),
            _ => panic!(),
        }
    }
    #[test]
    fn wb_2_buckets() {
        let r = ev("width_bucket", &[f(3.0), f(0.0), f(10.0), i(2)]);
        match r {
            Value::I64(v) => assert!(v >= 1 && v <= 2),
            _ => panic!(),
        }
    }
    #[test]
    fn log_base_2_8() {
        close(&ev("log_base", &[f(2.0), f(8.0)]), 3.0, 0.01);
    }
    #[test]
    fn log_base_10_100() {
        close(&ev("log_base", &[f(10.0), f(100.0)]), 2.0, 0.01);
    }
    #[test]
    fn log_base_null() {
        assert_eq!(ev("log_base", &[null(), f(8.0)]), null());
    }
    #[test]
    fn hash_int() {
        let r = ev("hash", &[i(42)]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn hash_str() {
        let r = ev("hash", &[s("test")]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn hash_det() {
        assert_eq!(ev("hash", &[i(42)]), ev("hash", &[i(42)]));
    }
    #[test]
    fn hash_diff() {
        assert_ne!(ev("hash", &[i(1)]), ev("hash", &[i(2)]));
    }
    #[test]
    fn murmur3_det() {
        assert_eq!(ev("murmur3", &[s("abc")]), ev("murmur3", &[s("abc")]));
    }
    #[test]
    fn murmur3_diff() {
        assert_ne!(ev("murmur3", &[s("a")]), ev("murmur3", &[s("b")]));
    }
    #[test]
    fn murmur3_null() {
        assert_eq!(ev("murmur3", &[null()]), null());
    }
    #[test]
    fn crc32_det() {
        assert_eq!(ev("crc32", &[s("abc")]), ev("crc32", &[s("abc")]));
    }
    #[test]
    fn crc32_diff() {
        assert_ne!(ev("crc32", &[s("a")]), ev("crc32", &[s("b")]));
    }
    #[test]
    fn crc32_null() {
        assert_eq!(ev("crc32", &[null()]), null());
    }
    #[test]
    fn fnv1a_det() {
        assert_eq!(ev("fnv1a", &[s("abc")]), ev("fnv1a", &[s("abc")]));
    }
    #[test]
    fn fnv1a_diff() {
        assert_ne!(ev("fnv1a", &[s("a")]), ev("fnv1a", &[s("b")]));
    }
    #[test]
    fn fnv1a_null() {
        assert_eq!(ev("fnv1a", &[null()]), null());
    }
    #[test]
    fn hash_combine_det() {
        assert_eq!(
            ev("hash_combine", &[i(1), i(2)]),
            ev("hash_combine", &[i(1), i(2)])
        );
    }
    #[test]
    fn hash_combine_order() {
        assert_ne!(
            ev("hash_combine", &[i(1), i(2)]),
            ev("hash_combine", &[i(2), i(1)])
        );
    }
    #[test]
    fn wb_02() {
        let r = ev("width_bucket", &[f(2.5), f(0.0), f(10.0), i(4)]);
        match r {
            Value::I64(v) => assert!(v >= 1 && v <= 4),
            _ => panic!(),
        }
    }
    #[test]
    fn wb_03() {
        let r = ev("width_bucket", &[f(7.5), f(0.0), f(10.0), i(4)]);
        match r {
            Value::I64(v) => assert!(v >= 1 && v <= 4),
            _ => panic!(),
        }
    }
    #[test]
    fn log_base_3_27() {
        close(&ev("log_base", &[f(3.0), f(27.0)]), 3.0, 0.01);
    }
    #[test]
    fn log_base_2_16() {
        close(&ev("log_base", &[f(2.0), f(16.0)]), 4.0, 0.01);
    }
    #[test]
    fn log_base_2_32() {
        close(&ev("log_base", &[f(2.0), f(32.0)]), 5.0, 0.01);
    }
    #[test]
    fn log_base_5_125() {
        close(&ev("log_base", &[f(5.0), f(125.0)]), 3.0, 0.01);
    }
}

// cast functions — 30 tests
mod cast_t02 {
    use super::*;
    #[test]
    fn cast_int_s() {
        assert_eq!(ev("cast_int", &[s("42")]), i(42));
    }
    #[test]
    fn cast_int_f() {
        assert_eq!(ev("cast_int", &[f(3.7)]), i(3));
    }
    #[test]
    fn cast_int_null() {
        assert_eq!(ev("cast_int", &[null()]), null());
    }
    #[test]
    fn cast_float_s() {
        close(&ev("cast_float", &[s("3.14")]), 3.14, 0.001);
    }
    #[test]
    fn cast_float_i() {
        close(&ev("cast_float", &[i(42)]), 42.0, 0.001);
    }
    #[test]
    fn cast_float_null() {
        assert_eq!(ev("cast_float", &[null()]), null());
    }
    #[test]
    fn cast_str_i() {
        assert_eq!(ev("cast_str", &[i(42)]), s("42"));
    }
    #[test]
    fn cast_str_f() {
        assert_eq!(ev("cast_str", &[f(3.14)]), s("3.14"));
    }
    #[test]
    fn cast_str_null() {
        assert_eq!(ev("cast_str", &[null()]), null());
    }
    #[test]
    fn to_int_alias() {
        assert_eq!(ev("to_int", &[s("99")]), i(99));
    }
    #[test]
    fn to_float_alias() {
        close(&ev("to_float", &[s("2.5")]), 2.5, 0.001);
    }
    #[test]
    fn to_str_alias() {
        assert_eq!(ev("to_str", &[i(7)]), s("7"));
    }
    #[test]
    fn to_long_alias() {
        assert_eq!(ev("to_long", &[s("100")]), i(100));
    }
    #[test]
    fn to_double_alias() {
        close(&ev("to_double", &[s("1.5")]), 1.5, 0.001);
    }
    #[test]
    fn safe_cast_int_ok() {
        assert_eq!(ev("safe_cast_int", &[s("42")]), i(42));
    }
    #[test]
    fn safe_cast_int_bad() {
        assert_eq!(ev("safe_cast_int", &[s("abc")]), null());
    }
    #[test]
    fn safe_cast_float_ok() {
        close(&ev("safe_cast_float", &[s("3.14")]), 3.14, 0.001);
    }
    #[test]
    fn safe_cast_float_bad() {
        assert_eq!(ev("safe_cast_float", &[s("abc")]), null());
    }
    #[test]
    fn try_cast_int_alias() {
        assert_eq!(ev("try_cast_int", &[s("42")]), i(42));
    }
    #[test]
    fn try_cast_float_alias() {
        close(&ev("try_cast_float", &[s("1.5")]), 1.5, 0.001);
    }
    #[test]
    fn c01() {
        assert_eq!(ev("cast_int", &[s("0")]), i(0));
    }
    #[test]
    fn c02() {
        assert_eq!(ev("cast_int", &[s("-1")]), i(-1));
    }
    #[test]
    fn c03() {
        assert_eq!(ev("cast_int", &[s("100")]), i(100));
    }
    #[test]
    fn c04() {
        close(&ev("cast_float", &[s("0.0")]), 0.0, 0.001);
    }
    #[test]
    fn c05() {
        close(&ev("cast_float", &[s("-1.5")]), -1.5, 0.001);
    }
    #[test]
    fn c06() {
        assert_eq!(ev("cast_str", &[i(0)]), s("0"));
    }
    #[test]
    fn c07() {
        assert_eq!(ev("cast_str", &[i(-1)]), s("-1"));
    }
    #[test]
    fn c08() {
        assert_eq!(ev("safe_cast_int", &[null()]), null());
    }
    #[test]
    fn c09() {
        assert_eq!(ev("safe_cast_float", &[null()]), null());
    }
    #[test]
    fn c10() {
        assert_eq!(ev("cast_int", &[i(42)]), i(42));
    }
}
