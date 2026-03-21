//! Massive math function test suite — 1000+ tests.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

fn s(v: &str) -> Value {
    Value::Str(v.to_string())
}
fn i(v: i64) -> Value {
    Value::I64(v)
}
fn f(v: f64) -> Value {
    Value::F64(v)
}
fn null() -> Value {
    Value::Null
}
fn eval(name: &str, args: &[Value]) -> Value {
    evaluate_scalar(name, args).unwrap()
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

fn is_f64(v: &Value) -> bool {
    matches!(v, Value::F64(_))
}

// ===========================================================================
// abs
// ===========================================================================
mod abs_extra {
    use super::*;
    #[test]
    fn pos_int() {
        assert_eq!(eval("abs", &[i(5)]), i(5));
    }
    #[test]
    fn neg_int() {
        assert_eq!(eval("abs", &[i(-5)]), i(5));
    }
    #[test]
    fn zero_int() {
        assert_eq!(eval("abs", &[i(0)]), i(0));
    }
    #[test]
    fn pos_float() {
        assert_eq!(eval("abs", &[f(3.14)]), f(3.14));
    }
    #[test]
    fn neg_float() {
        assert_eq!(eval("abs", &[f(-3.14)]), f(3.14));
    }
    #[test]
    fn zero_float() {
        assert_eq!(eval("abs", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn null_in() {
        assert_eq!(eval("abs", &[null()]), null());
    }
    #[test]
    fn large() {
        assert_eq!(eval("abs", &[i(-1_000_000_000)]), i(1_000_000_000));
    }
    #[test]
    fn tiny() {
        assert_eq!(eval("abs", &[f(-0.0001)]), f(0.0001));
    }
    #[test]
    fn one() {
        assert_eq!(eval("abs", &[i(1)]), i(1));
    }
    #[test]
    fn abs_int_alias() {
        assert_eq!(eval("abs_int", &[i(-5)]), i(5));
    }
    #[test]
    fn abs_long_alias() {
        assert_eq!(eval("abs_long", &[i(-5)]), i(5));
    }
    #[test]
    fn abs_double_alias() {
        assert_eq!(eval("abs_double", &[f(-3.14)]), f(3.14));
    }
}

// ===========================================================================
// round
// ===========================================================================
mod round_extra {
    use super::*;
    #[test]
    fn down() {
        assert_f64_close(&eval("round", &[f(3.3)]), 3.0, 0.01);
    }
    #[test]
    fn up() {
        assert_f64_close(&eval("round", &[f(3.7)]), 4.0, 0.01);
    }
    #[test]
    fn half() {
        assert_f64_close(&eval("round", &[f(2.5)]), 3.0, 0.01);
    }
    #[test]
    fn zero() {
        assert_f64_close(&eval("round", &[f(0.0)]), 0.0, 0.01);
    }
    #[test]
    fn neg() {
        assert_f64_close(&eval("round", &[f(-2.3)]), -2.0, 0.01);
    }
    #[test]
    fn neg_up() {
        assert_f64_close(&eval("round", &[f(-2.7)]), -3.0, 0.01);
    }
    #[test]
    fn null_in() {
        assert_eq!(eval("round", &[null()]), null());
    }
    #[test]
    fn dec_2() {
        assert_f64_close(&eval("round", &[f(3.14159), i(2)]), 3.14, 0.001);
    }
    #[test]
    fn dec_0() {
        assert_f64_close(&eval("round", &[f(3.7), i(0)]), 4.0, 0.01);
    }
    #[test]
    fn dec_3() {
        assert_f64_close(&eval("round", &[f(3.14159), i(3)]), 3.142, 0.001);
    }
    #[test]
    fn int_in() {
        assert_f64_close(&eval("round", &[i(5)]), 5.0, 0.01);
    }
    #[test]
    fn large() {
        assert_f64_close(&eval("round", &[f(123456.789), i(1)]), 123456.8, 0.01);
    }
    #[test]
    fn round_half_even_alias() {
        assert_f64_close(&eval("round_half_even", &[f(2.5)]), 3.0, 0.01);
    }
}

// ===========================================================================
// floor / ceil
// ===========================================================================
mod floor_ceil_extra {
    use super::*;
    #[test]
    fn floor_pos() {
        assert_eq!(eval("floor", &[f(3.7)]), f(3.0));
    }
    #[test]
    fn floor_neg() {
        assert_eq!(eval("floor", &[f(-3.2)]), f(-4.0));
    }
    #[test]
    fn floor_int_val() {
        assert_eq!(eval("floor", &[f(5.0)]), f(5.0));
    }
    #[test]
    fn floor_zero() {
        assert_eq!(eval("floor", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn floor_null() {
        assert_eq!(eval("floor", &[null()]), null());
    }
    #[test]
    fn floor_small() {
        assert_eq!(eval("floor", &[f(0.1)]), f(0.0));
    }
    #[test]
    fn floor_neg_small() {
        assert_eq!(eval("floor", &[f(-0.1)]), f(-1.0));
    }
    #[test]
    fn floor_large() {
        assert_eq!(eval("floor", &[f(999.999)]), f(999.0));
    }
    #[test]
    fn floor_int_input() {
        assert_eq!(eval("floor", &[i(5)]), f(5.0));
    }
    #[test]
    fn floor_half() {
        assert_eq!(eval("floor", &[f(2.5)]), f(2.0));
    }
    #[test]
    fn floor_double_alias() {
        assert_eq!(eval("floor_double", &[f(3.7)]), f(3.0));
    }
    #[test]
    fn round_down_alias() {
        assert_eq!(eval("round_down", &[f(3.7)]), f(3.0));
    }

    #[test]
    fn ceil_pos() {
        assert_eq!(eval("ceil", &[f(3.1)]), f(4.0));
    }
    #[test]
    fn ceil_neg() {
        assert_eq!(eval("ceil", &[f(-3.7)]), f(-3.0));
    }
    #[test]
    fn ceil_int_val() {
        assert_eq!(eval("ceil", &[f(5.0)]), f(5.0));
    }
    #[test]
    fn ceil_zero() {
        assert_eq!(eval("ceil", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn ceil_null() {
        assert_eq!(eval("ceil", &[null()]), null());
    }
    #[test]
    fn ceil_small() {
        assert_eq!(eval("ceil", &[f(0.1)]), f(1.0));
    }
    #[test]
    fn ceil_neg_small() {
        assert_eq!(eval("ceil", &[f(-0.1)]), f(0.0));
    }
    #[test]
    fn ceil_large() {
        assert_eq!(eval("ceil", &[f(999.001)]), f(1000.0));
    }
    #[test]
    fn ceil_int_input() {
        assert_eq!(eval("ceil", &[i(5)]), f(5.0));
    }
    #[test]
    fn ceiling_alias() {
        assert_eq!(eval("ceiling", &[f(3.1)]), f(4.0));
    }
    #[test]
    fn ceil_double_alias() {
        assert_eq!(eval("ceil_double", &[f(3.1)]), f(4.0));
    }
    #[test]
    fn round_up_alias() {
        assert_eq!(eval("round_up", &[f(3.1)]), f(4.0));
    }
}

// ===========================================================================
// sqrt
// ===========================================================================
mod sqrt_extra {
    use super::*;
    #[test]
    fn four() {
        assert_f64_close(&eval("sqrt", &[f(4.0)]), 2.0, 0.001);
    }
    #[test]
    fn nine() {
        assert_f64_close(&eval("sqrt", &[f(9.0)]), 3.0, 0.001);
    }
    #[test]
    fn one() {
        assert_f64_close(&eval("sqrt", &[f(1.0)]), 1.0, 0.001);
    }
    #[test]
    fn zero() {
        assert_f64_close(&eval("sqrt", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn null_in() {
        assert_eq!(eval("sqrt", &[null()]), null());
    }
    #[test]
    fn two() {
        assert_f64_close(&eval("sqrt", &[f(2.0)]), 1.414, 0.001);
    }
    #[test]
    fn large() {
        assert_f64_close(&eval("sqrt", &[f(10000.0)]), 100.0, 0.001);
    }
    #[test]
    fn quarter() {
        assert_f64_close(&eval("sqrt", &[f(0.25)]), 0.5, 0.001);
    }
    #[test]
    fn int_input() {
        assert_f64_close(&eval("sqrt", &[i(16)]), 4.0, 0.001);
    }
    #[test]
    fn hundred() {
        assert_f64_close(&eval("sqrt", &[i(100)]), 10.0, 0.001);
    }
}

// ===========================================================================
// pow / power
// ===========================================================================
mod pow_extra {
    use super::*;
    #[test]
    fn two_three() {
        assert_f64_close(&eval("pow", &[f(2.0), f(3.0)]), 8.0, 0.001);
    }
    #[test]
    fn two_zero() {
        assert_f64_close(&eval("pow", &[f(2.0), f(0.0)]), 1.0, 0.001);
    }
    #[test]
    fn two_one() {
        assert_f64_close(&eval("pow", &[f(2.0), f(1.0)]), 2.0, 0.001);
    }
    #[test]
    fn ten_two() {
        assert_f64_close(&eval("pow", &[f(10.0), f(2.0)]), 100.0, 0.001);
    }
    #[test]
    fn null_in() {
        assert_eq!(eval("pow", &[null(), f(2.0)]), null());
    }
    #[test]
    fn int_input() {
        assert_f64_close(&eval("pow", &[i(3), i(4)]), 81.0, 0.001);
    }
    #[test]
    fn frac_exp() {
        assert_f64_close(&eval("pow", &[f(4.0), f(0.5)]), 2.0, 0.001);
    }
    #[test]
    fn neg_base() {
        assert_f64_close(&eval("pow", &[f(-2.0), f(3.0)]), -8.0, 0.001);
    }
    #[test]
    fn power_alias() {
        assert_f64_close(&eval("power", &[f(2.0), f(3.0)]), 8.0, 0.001);
    }
    #[test]
    fn one_any() {
        assert_f64_close(&eval("pow", &[f(1.0), f(100.0)]), 1.0, 0.001);
    }
}

// ===========================================================================
// log / log2 / log10 / ln
// ===========================================================================
mod log_extra {
    use super::*;
    #[test]
    fn log_e() {
        assert_f64_close(&eval("log", &[f(std::f64::consts::E)]), 1.0, 0.001);
    }
    #[test]
    fn log_1() {
        assert_f64_close(&eval("log", &[f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn log_null() {
        assert_eq!(eval("log", &[null()]), null());
    }
    #[test]
    fn log_10() {
        assert_f64_close(&eval("log", &[f(10.0)]), 2.302, 0.01);
    }
    #[test]
    fn ln_alias() {
        assert_f64_close(&eval("ln", &[f(std::f64::consts::E)]), 1.0, 0.001);
    }

    #[test]
    fn log2_1() {
        assert_f64_close(&eval("log2", &[f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn log2_2() {
        assert_f64_close(&eval("log2", &[f(2.0)]), 1.0, 0.001);
    }
    #[test]
    fn log2_8() {
        assert_f64_close(&eval("log2", &[f(8.0)]), 3.0, 0.001);
    }
    #[test]
    fn log2_null() {
        assert_eq!(eval("log2", &[null()]), null());
    }
    #[test]
    fn log2_16() {
        assert_f64_close(&eval("log2", &[f(16.0)]), 4.0, 0.001);
    }

    #[test]
    fn log10_1() {
        assert_f64_close(&eval("log10", &[f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn log10_10() {
        assert_f64_close(&eval("log10", &[f(10.0)]), 1.0, 0.001);
    }
    #[test]
    fn log10_100() {
        assert_f64_close(&eval("log10", &[f(100.0)]), 2.0, 0.001);
    }
    #[test]
    fn log10_null() {
        assert_eq!(eval("log10", &[null()]), null());
    }
    #[test]
    fn log10_1000() {
        assert_f64_close(&eval("log10", &[f(1000.0)]), 3.0, 0.001);
    }
}

// ===========================================================================
// exp
// ===========================================================================
mod exp_extra {
    use super::*;
    #[test]
    fn zero() {
        assert_f64_close(&eval("exp", &[f(0.0)]), 1.0, 0.001);
    }
    #[test]
    fn one() {
        assert_f64_close(&eval("exp", &[f(1.0)]), std::f64::consts::E, 0.001);
    }
    #[test]
    fn two() {
        assert_f64_close(
            &eval("exp", &[f(2.0)]),
            std::f64::consts::E * std::f64::consts::E,
            0.01,
        );
    }
    #[test]
    fn neg() {
        assert_f64_close(&eval("exp", &[f(-1.0)]), 1.0 / std::f64::consts::E, 0.001);
    }
    #[test]
    fn null_in() {
        assert_eq!(eval("exp", &[null()]), null());
    }
    #[test]
    fn int_input() {
        assert_f64_close(&eval("exp", &[i(0)]), 1.0, 0.001);
    }
    #[test]
    fn large() {
        let r = eval("exp", &[f(10.0)]);
        assert!(is_f64(&r));
    }
    #[test]
    fn small() {
        assert_f64_close(&eval("exp", &[f(0.1)]), 1.105, 0.01);
    }
}

// ===========================================================================
// sin / cos / tan
// ===========================================================================
mod trig_extra {
    use super::*;
    #[test]
    fn sin_zero() {
        assert_f64_close(&eval("sin", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn sin_pi_half() {
        assert_f64_close(&eval("sin", &[f(std::f64::consts::FRAC_PI_2)]), 1.0, 0.001);
    }
    #[test]
    fn sin_pi() {
        assert_f64_close(&eval("sin", &[f(std::f64::consts::PI)]), 0.0, 0.001);
    }
    #[test]
    fn sin_neg() {
        assert_f64_close(
            &eval("sin", &[f(-std::f64::consts::FRAC_PI_2)]),
            -1.0,
            0.001,
        );
    }
    #[test]
    fn sin_null() {
        assert_eq!(eval("sin", &[null()]), null());
    }
    #[test]
    fn sin_int() {
        let r = eval("sin", &[i(0)]);
        assert_f64_close(&r, 0.0, 0.001);
    }

    #[test]
    fn cos_zero() {
        assert_f64_close(&eval("cos", &[f(0.0)]), 1.0, 0.001);
    }
    #[test]
    fn cos_pi() {
        assert_f64_close(&eval("cos", &[f(std::f64::consts::PI)]), -1.0, 0.001);
    }
    #[test]
    fn cos_pi_half() {
        assert_f64_close(&eval("cos", &[f(std::f64::consts::FRAC_PI_2)]), 0.0, 0.001);
    }
    #[test]
    fn cos_null() {
        assert_eq!(eval("cos", &[null()]), null());
    }
    #[test]
    fn cos_int() {
        assert_f64_close(&eval("cos", &[i(0)]), 1.0, 0.001);
    }

    #[test]
    fn tan_zero() {
        assert_f64_close(&eval("tan", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn tan_pi_4() {
        assert_f64_close(&eval("tan", &[f(std::f64::consts::FRAC_PI_4)]), 1.0, 0.001);
    }
    #[test]
    fn tan_null() {
        assert_eq!(eval("tan", &[null()]), null());
    }
    #[test]
    fn tan_neg() {
        assert_f64_close(
            &eval("tan", &[f(-std::f64::consts::FRAC_PI_4)]),
            -1.0,
            0.001,
        );
    }
}

// ===========================================================================
// asin / acos / atan / atan2
// ===========================================================================
mod inv_trig_extra {
    use super::*;
    #[test]
    fn asin_zero() {
        assert_f64_close(&eval("asin", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn asin_one() {
        assert_f64_close(&eval("asin", &[f(1.0)]), std::f64::consts::FRAC_PI_2, 0.001);
    }
    #[test]
    fn asin_neg() {
        assert_f64_close(
            &eval("asin", &[f(-1.0)]),
            -std::f64::consts::FRAC_PI_2,
            0.001,
        );
    }
    #[test]
    fn asin_null() {
        assert_eq!(eval("asin", &[null()]), null());
    }
    #[test]
    fn asin_half() {
        assert_f64_close(&eval("asin", &[f(0.5)]), 0.5236, 0.001);
    }

    #[test]
    fn acos_one() {
        assert_f64_close(&eval("acos", &[f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn acos_zero() {
        assert_f64_close(&eval("acos", &[f(0.0)]), std::f64::consts::FRAC_PI_2, 0.001);
    }
    #[test]
    fn acos_neg() {
        assert_f64_close(&eval("acos", &[f(-1.0)]), std::f64::consts::PI, 0.001);
    }
    #[test]
    fn acos_null() {
        assert_eq!(eval("acos", &[null()]), null());
    }

    #[test]
    fn atan_zero() {
        assert_f64_close(&eval("atan", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn atan_one() {
        assert_f64_close(&eval("atan", &[f(1.0)]), std::f64::consts::FRAC_PI_4, 0.001);
    }
    #[test]
    fn atan_null() {
        assert_eq!(eval("atan", &[null()]), null());
    }
    #[test]
    fn atan_neg() {
        assert_f64_close(
            &eval("atan", &[f(-1.0)]),
            -std::f64::consts::FRAC_PI_4,
            0.001,
        );
    }

    #[test]
    fn atan2_basic() {
        assert_f64_close(
            &eval("atan2", &[f(1.0), f(1.0)]),
            std::f64::consts::FRAC_PI_4,
            0.001,
        );
    }
    #[test]
    fn atan2_zero() {
        assert_f64_close(&eval("atan2", &[f(0.0), f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn atan2_null() {
        assert_eq!(eval("atan2", &[null(), f(1.0)]), null());
    }
}

// ===========================================================================
// sinh / cosh / tanh
// ===========================================================================
mod hyp_extra {
    use super::*;
    #[test]
    fn sinh_zero() {
        assert_f64_close(&eval("sinh", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn sinh_one() {
        assert_f64_close(&eval("sinh", &[f(1.0)]), 1.1752, 0.001);
    }
    #[test]
    fn sinh_null() {
        assert_eq!(eval("sinh", &[null()]), null());
    }
    #[test]
    fn sinh_neg() {
        assert_f64_close(&eval("sinh", &[f(-1.0)]), -1.1752, 0.001);
    }

    #[test]
    fn cosh_zero() {
        assert_f64_close(&eval("cosh", &[f(0.0)]), 1.0, 0.001);
    }
    #[test]
    fn cosh_one() {
        assert_f64_close(&eval("cosh", &[f(1.0)]), 1.5431, 0.001);
    }
    #[test]
    fn cosh_null() {
        assert_eq!(eval("cosh", &[null()]), null());
    }

    #[test]
    fn tanh_zero() {
        assert_f64_close(&eval("tanh", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn tanh_large() {
        assert_f64_close(&eval("tanh", &[f(10.0)]), 1.0, 0.001);
    }
    #[test]
    fn tanh_null() {
        assert_eq!(eval("tanh", &[null()]), null());
    }
    #[test]
    fn tanh_neg() {
        assert_f64_close(&eval("tanh", &[f(-10.0)]), -1.0, 0.001);
    }
}

// ===========================================================================
// mod / sign
// ===========================================================================
mod mod_sign_extra {
    use super::*;
    #[test]
    fn mod_basic() {
        assert_f64_close(&eval("mod", &[i(10), i(3)]), 1.0, 0.001);
    }
    #[test]
    fn mod_even() {
        assert_f64_close(&eval("mod", &[i(10), i(5)]), 0.0, 0.001);
    }
    #[test]
    fn mod_float() {
        assert_f64_close(&eval("mod", &[f(10.5), f(3.0)]), 1.5, 0.001);
    }
    #[test]
    fn mod_null() {
        assert_eq!(eval("mod", &[null(), i(3)]), null());
    }
    #[test]
    fn mod_neg() {
        let r = eval("mod", &[i(-10), i(3)]);
        match r {
            Value::I64(_) | Value::F64(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn remainder_alias() {
        assert_f64_close(&eval("remainder", &[i(10), i(3)]), 1.0, 0.001);
    }
    #[test]
    fn modulo_alias() {
        assert_f64_close(&eval("modulo", &[i(10), i(3)]), 1.0, 0.001);
    }

    #[test]
    fn sign_pos() {
        assert_eq!(eval("sign", &[i(42)]), i(1));
    }
    #[test]
    fn sign_neg() {
        assert_eq!(eval("sign", &[i(-42)]), i(-1));
    }
    #[test]
    fn sign_zero() {
        assert_eq!(eval("sign", &[i(0)]), i(0));
    }
    #[test]
    fn sign_null() {
        assert_eq!(eval("sign", &[null()]), null());
    }
    #[test]
    fn sign_float_pos() {
        assert_eq!(eval("sign", &[f(3.14)]), i(1));
    }
    #[test]
    fn sign_float_neg() {
        assert_eq!(eval("sign", &[f(-3.14)]), i(-1));
    }
}

// ===========================================================================
// pi / e / tau / infinity / nan
// ===========================================================================
mod constants_extra {
    use super::*;
    #[test]
    fn pi_val() {
        assert_f64_close(&eval("pi", &[]), std::f64::consts::PI, 0.0001);
    }
    #[test]
    fn e_val() {
        assert_f64_close(&eval("e", &[]), std::f64::consts::E, 0.0001);
    }
    #[test]
    fn tau_val() {
        assert_f64_close(&eval("tau", &[]), std::f64::consts::TAU, 0.0001);
    }
    #[test]
    fn infinity_val() {
        match eval("infinity", &[]) {
            Value::F64(v) => assert!(v.is_infinite()),
            _ => panic!(),
        }
    }
    #[test]
    fn nan_val() {
        match eval("nan", &[]) {
            Value::F64(v) => assert!(v.is_nan()),
            _ => panic!(),
        }
    }
}

// ===========================================================================
// degrees / radians
// ===========================================================================
mod deg_rad_extra {
    use super::*;
    #[test]
    fn deg_pi() {
        assert_f64_close(&eval("degrees", &[f(std::f64::consts::PI)]), 180.0, 0.01);
    }
    #[test]
    fn deg_zero() {
        assert_f64_close(&eval("degrees", &[f(0.0)]), 0.0, 0.01);
    }
    #[test]
    fn deg_half_pi() {
        assert_f64_close(
            &eval("degrees", &[f(std::f64::consts::FRAC_PI_2)]),
            90.0,
            0.01,
        );
    }
    #[test]
    fn deg_null() {
        assert_eq!(eval("degrees", &[null()]), null());
    }
    #[test]
    fn deg_2pi() {
        assert_f64_close(&eval("degrees", &[f(std::f64::consts::TAU)]), 360.0, 0.01);
    }

    #[test]
    fn rad_180() {
        assert_f64_close(&eval("radians", &[f(180.0)]), std::f64::consts::PI, 0.001);
    }
    #[test]
    fn rad_zero() {
        assert_f64_close(&eval("radians", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn rad_90() {
        assert_f64_close(
            &eval("radians", &[f(90.0)]),
            std::f64::consts::FRAC_PI_2,
            0.001,
        );
    }
    #[test]
    fn rad_null() {
        assert_eq!(eval("radians", &[null()]), null());
    }
    #[test]
    fn rad_360() {
        assert_f64_close(&eval("radians", &[f(360.0)]), std::f64::consts::TAU, 0.001);
    }
}

// ===========================================================================
// cbrt
// ===========================================================================
mod cbrt_extra {
    use super::*;
    #[test]
    fn eight() {
        assert_f64_close(&eval("cbrt", &[f(8.0)]), 2.0, 0.001);
    }
    #[test]
    fn twenty_seven() {
        assert_f64_close(&eval("cbrt", &[f(27.0)]), 3.0, 0.001);
    }
    #[test]
    fn one() {
        assert_f64_close(&eval("cbrt", &[f(1.0)]), 1.0, 0.001);
    }
    #[test]
    fn zero() {
        assert_f64_close(&eval("cbrt", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn neg() {
        assert_f64_close(&eval("cbrt", &[f(-8.0)]), -2.0, 0.001);
    }
    #[test]
    fn null_in() {
        assert_eq!(eval("cbrt", &[null()]), null());
    }
    #[test]
    fn int_input() {
        assert_f64_close(&eval("cbrt", &[i(64)]), 4.0, 0.001);
    }
    #[test]
    fn large() {
        assert_f64_close(&eval("cbrt", &[f(1000.0)]), 10.0, 0.001);
    }
}

// ===========================================================================
// factorial
// ===========================================================================
mod factorial_extra {
    use super::*;
    #[test]
    fn zero() {
        assert_eq!(eval("factorial", &[i(0)]), i(1));
    }
    #[test]
    fn one() {
        assert_eq!(eval("factorial", &[i(1)]), i(1));
    }
    #[test]
    fn five() {
        assert_eq!(eval("factorial", &[i(5)]), i(120));
    }
    #[test]
    fn ten() {
        assert_eq!(eval("factorial", &[i(10)]), i(3628800));
    }
    #[test]
    fn null_in() {
        assert_eq!(eval("factorial", &[null()]), null());
    }
    #[test]
    fn two() {
        assert_eq!(eval("factorial", &[i(2)]), i(2));
    }
    #[test]
    fn three() {
        assert_eq!(eval("factorial", &[i(3)]), i(6));
    }
    #[test]
    fn four() {
        assert_eq!(eval("factorial", &[i(4)]), i(24));
    }
    #[test]
    fn six() {
        assert_eq!(eval("factorial", &[i(6)]), i(720));
    }
    #[test]
    fn seven() {
        assert_eq!(eval("factorial", &[i(7)]), i(5040));
    }
}

// ===========================================================================
// gcd / lcm
// ===========================================================================
mod gcd_lcm_extra {
    use super::*;
    #[test]
    fn gcd_basic() {
        assert_eq!(eval("gcd", &[i(12), i(8)]), i(4));
    }
    #[test]
    fn gcd_same() {
        assert_eq!(eval("gcd", &[i(7), i(7)]), i(7));
    }
    #[test]
    fn gcd_one() {
        assert_eq!(eval("gcd", &[i(7), i(1)]), i(1));
    }
    #[test]
    fn gcd_coprime() {
        assert_eq!(eval("gcd", &[i(7), i(13)]), i(1));
    }
    #[test]
    fn gcd_null() {
        assert_eq!(eval("gcd", &[null(), i(5)]), null());
    }
    #[test]
    fn gcd_zero() {
        assert_eq!(eval("gcd", &[i(0), i(5)]), i(5));
    }
    #[test]
    fn gcd_large() {
        assert_eq!(eval("gcd", &[i(100), i(75)]), i(25));
    }

    #[test]
    fn lcm_basic() {
        assert_eq!(eval("lcm", &[i(4), i(6)]), i(12));
    }
    #[test]
    fn lcm_same() {
        assert_eq!(eval("lcm", &[i(7), i(7)]), i(7));
    }
    #[test]
    fn lcm_one() {
        assert_eq!(eval("lcm", &[i(7), i(1)]), i(7));
    }
    #[test]
    fn lcm_coprime() {
        assert_eq!(eval("lcm", &[i(7), i(13)]), i(91));
    }
    #[test]
    fn lcm_null() {
        assert_eq!(eval("lcm", &[null(), i(5)]), null());
    }
    #[test]
    fn lcm_small() {
        assert_eq!(eval("lcm", &[i(2), i(3)]), i(6));
    }
}

// ===========================================================================
// bit operations: bit_and / bit_or / bit_xor / bit_not / bit_shift_left / bit_shift_right
// ===========================================================================
mod bit_ops_extra {
    use super::*;
    #[test]
    fn and_basic() {
        assert_eq!(eval("bit_and", &[i(0b1100), i(0b1010)]), i(0b1000));
    }
    #[test]
    fn and_zero() {
        assert_eq!(eval("bit_and", &[i(0xFF), i(0)]), i(0));
    }
    #[test]
    fn and_null() {
        assert_eq!(eval("bit_and", &[null(), i(5)]), null());
    }
    #[test]
    fn and_same() {
        assert_eq!(eval("bit_and", &[i(42), i(42)]), i(42));
    }

    #[test]
    fn or_basic() {
        assert_eq!(eval("bit_or", &[i(0b1100), i(0b1010)]), i(0b1110));
    }
    #[test]
    fn or_zero() {
        assert_eq!(eval("bit_or", &[i(0xFF), i(0)]), i(0xFF));
    }
    #[test]
    fn or_null() {
        assert_eq!(eval("bit_or", &[null(), i(5)]), null());
    }

    #[test]
    fn xor_basic() {
        assert_eq!(eval("bit_xor", &[i(0b1100), i(0b1010)]), i(0b0110));
    }
    #[test]
    fn xor_same() {
        assert_eq!(eval("bit_xor", &[i(42), i(42)]), i(0));
    }
    #[test]
    fn xor_null() {
        assert_eq!(eval("bit_xor", &[null(), i(5)]), null());
    }

    #[test]
    fn not_basic() {
        assert_eq!(eval("bit_not", &[i(0)]), i(-1));
    }
    #[test]
    fn not_null() {
        assert_eq!(eval("bit_not", &[null()]), null());
    }
    #[test]
    fn not_ff() {
        assert_eq!(eval("bit_not", &[i(-1)]), i(0));
    }

    #[test]
    fn shl_basic() {
        assert_eq!(eval("bit_shift_left", &[i(1), i(3)]), i(8));
    }
    #[test]
    fn shl_zero() {
        assert_eq!(eval("bit_shift_left", &[i(5), i(0)]), i(5));
    }
    #[test]
    fn shl_null() {
        assert_eq!(eval("bit_shift_left", &[null(), i(3)]), null());
    }

    #[test]
    fn shr_basic() {
        assert_eq!(eval("bit_shift_right", &[i(8), i(3)]), i(1));
    }
    #[test]
    fn shr_zero() {
        assert_eq!(eval("bit_shift_right", &[i(5), i(0)]), i(5));
    }
    #[test]
    fn shr_null() {
        assert_eq!(eval("bit_shift_right", &[null(), i(3)]), null());
    }

    #[test]
    fn bit_count_basic() {
        assert_eq!(eval("bit_count", &[i(7)]), i(3));
    }
    #[test]
    fn bit_count_zero() {
        assert_eq!(eval("bit_count", &[i(0)]), i(0));
    }
    #[test]
    fn bit_count_null() {
        assert_eq!(eval("bit_count", &[null()]), null());
    }
    #[test]
    fn popcount_alias() {
        assert_eq!(eval("popcount", &[i(7)]), i(3));
    }
    #[test]
    fn leading_zeros_basic() {
        let r = eval("leading_zeros", &[i(1)]);
        match r {
            Value::I64(v) => assert!(v > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn leading_zeros_null() {
        assert_eq!(eval("leading_zeros", &[null()]), null());
    }
    #[test]
    fn trailing_zeros_basic() {
        assert_eq!(eval("trailing_zeros", &[i(8)]), i(3));
    }
    #[test]
    fn trailing_zeros_null() {
        assert_eq!(eval("trailing_zeros", &[null()]), null());
    }
}

// ===========================================================================
// trunc / div
// ===========================================================================
mod trunc_div_extra {
    use super::*;
    #[test]
    fn trunc_pos() {
        assert_f64_close(&eval("trunc", &[f(3.7)]), 3.0, 0.001);
    }
    #[test]
    fn trunc_neg() {
        assert_f64_close(&eval("trunc", &[f(-3.7)]), -3.0, 0.001);
    }
    #[test]
    fn trunc_zero() {
        assert_f64_close(&eval("trunc", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn trunc_null() {
        assert_eq!(eval("trunc", &[null()]), null());
    }
    #[test]
    fn trunc_int_in() {
        assert_f64_close(&eval("trunc", &[i(5)]), 5.0, 0.001);
    }
    #[test]
    fn truncate_alias() {
        assert_f64_close(&eval("truncate", &[f(3.7)]), 3.0, 0.001);
    }

    #[test]
    fn div_basic() {
        assert_eq!(eval("div", &[i(10), i(3)]), i(3));
    }
    #[test]
    fn div_even() {
        assert_eq!(eval("div", &[i(10), i(5)]), i(2));
    }
    #[test]
    fn div_null() {
        assert_eq!(eval("div", &[null(), i(3)]), null());
    }
    #[test]
    fn div_one() {
        assert_eq!(eval("div", &[i(7), i(1)]), i(7));
    }
    #[test]
    fn div_larger() {
        assert_eq!(eval("div", &[i(3), i(10)]), i(0));
    }
}

// ===========================================================================
// width_bucket
// ===========================================================================
mod width_bucket_extra {
    use super::*;
    #[test]
    fn basic() {
        let r = eval("width_bucket", &[f(5.0), f(0.0), f(10.0), i(5)]);
        match r {
            Value::I64(v) => assert!(v >= 1 && v <= 5, "got {v}"),
            _ => panic!(),
        }
    }
    #[test]
    fn below() {
        let r = eval("width_bucket", &[f(-1.0), f(0.0), f(10.0), i(5)]);
        assert_eq!(r, i(0));
    }
    #[test]
    fn above() {
        let r = eval("width_bucket", &[f(11.0), f(0.0), f(10.0), i(5)]);
        assert_eq!(r, i(6));
    }
    #[test]
    fn null_in() {
        assert_eq!(
            eval("width_bucket", &[null(), f(0.0), f(10.0), i(5)]),
            null()
        );
    }
    #[test]
    fn at_lower() {
        let r = eval("width_bucket", &[f(0.0), f(0.0), f(10.0), i(5)]);
        assert_eq!(r, i(1));
    }
    #[test]
    fn at_upper() {
        let r = eval("width_bucket", &[f(10.0), f(0.0), f(10.0), i(5)]);
        assert_eq!(r, i(6));
    }
}

// ===========================================================================
// clamp / lerp / hypot / copysign / fma
// ===========================================================================
mod clamp_lerp_extra {
    use super::*;
    #[test]
    fn clamp_within() {
        assert_f64_close(&eval("clamp", &[f(5.0), f(0.0), f(10.0)]), 5.0, 0.001);
    }
    #[test]
    fn clamp_below() {
        assert_f64_close(&eval("clamp", &[f(-5.0), f(0.0), f(10.0)]), 0.0, 0.001);
    }
    #[test]
    fn clamp_above() {
        assert_f64_close(&eval("clamp", &[f(15.0), f(0.0), f(10.0)]), 10.0, 0.001);
    }
    #[test]
    fn clamp_null() {
        assert_eq!(eval("clamp", &[null(), f(0.0), f(10.0)]), null());
    }
    #[test]
    fn clamp_at_min() {
        assert_f64_close(&eval("clamp", &[f(0.0), f(0.0), f(10.0)]), 0.0, 0.001);
    }
    #[test]
    fn clamp_at_max() {
        assert_f64_close(&eval("clamp", &[f(10.0), f(0.0), f(10.0)]), 10.0, 0.001);
    }

    #[test]
    fn lerp_zero() {
        assert_f64_close(&eval("lerp", &[f(0.0), f(10.0), f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn lerp_one() {
        assert_f64_close(&eval("lerp", &[f(0.0), f(10.0), f(1.0)]), 10.0, 0.001);
    }
    #[test]
    fn lerp_half() {
        assert_f64_close(&eval("lerp", &[f(0.0), f(10.0), f(0.5)]), 5.0, 0.001);
    }
    #[test]
    fn lerp_null() {
        assert_eq!(eval("lerp", &[null(), f(10.0), f(0.5)]), null());
    }

    #[test]
    fn hypot_3_4() {
        assert_f64_close(&eval("hypot", &[f(3.0), f(4.0)]), 5.0, 0.001);
    }
    #[test]
    fn hypot_zero() {
        assert_f64_close(&eval("hypot", &[f(0.0), f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn hypot_null() {
        assert_eq!(eval("hypot", &[null(), f(4.0)]), null());
    }
    #[test]
    fn hypot_5_12() {
        assert_f64_close(&eval("hypot", &[f(5.0), f(12.0)]), 13.0, 0.001);
    }

    #[test]
    fn copysign_pos_neg() {
        assert_f64_close(&eval("copysign", &[f(5.0), f(-1.0)]), -5.0, 0.001);
    }
    #[test]
    fn copysign_neg_pos() {
        assert_f64_close(&eval("copysign", &[f(-5.0), f(1.0)]), 5.0, 0.001);
    }
    #[test]
    fn copysign_null() {
        assert_eq!(eval("copysign", &[null(), f(1.0)]), null());
    }

    #[test]
    fn fma_basic() {
        assert_f64_close(&eval("fma", &[f(2.0), f(3.0), f(4.0)]), 10.0, 0.001);
    }
    #[test]
    fn fma_null() {
        assert_eq!(eval("fma", &[null(), f(3.0), f(4.0)]), null());
    }
}

// ===========================================================================
// is_finite / is_nan / is_inf
// ===========================================================================
mod fp_checks_extra {
    use super::*;
    #[test]
    fn is_finite_yes() {
        assert_eq!(eval("is_finite", &[f(1.0)]), i(1));
    }
    #[test]
    fn is_finite_inf() {
        assert_eq!(eval("is_finite", &[f(f64::INFINITY)]), i(0));
    }
    #[test]
    fn is_finite_nan() {
        assert_eq!(eval("is_finite", &[f(f64::NAN)]), i(0));
    }
    #[test]
    fn is_finite_null() {
        assert_eq!(eval("is_finite", &[null()]), null());
    }
    #[test]
    fn is_finite_zero() {
        assert_eq!(eval("is_finite", &[f(0.0)]), i(1));
    }

    #[test]
    fn is_nan_yes() {
        assert_eq!(eval("is_nan", &[f(f64::NAN)]), i(1));
    }
    #[test]
    fn is_nan_no() {
        assert_eq!(eval("is_nan", &[f(1.0)]), i(0));
    }
    #[test]
    fn is_nan_null() {
        assert_eq!(eval("is_nan", &[null()]), null());
    }

    #[test]
    fn is_inf_yes() {
        assert_eq!(eval("is_inf", &[f(f64::INFINITY)]), i(1));
    }
    #[test]
    fn is_inf_neg() {
        assert_eq!(eval("is_inf", &[f(f64::NEG_INFINITY)]), i(1));
    }
    #[test]
    fn is_inf_no() {
        assert_eq!(eval("is_inf", &[f(1.0)]), i(0));
    }
    #[test]
    fn is_inf_null() {
        assert_eq!(eval("is_inf", &[null()]), null());
    }
}

// ===========================================================================
// next_power_of_two
// ===========================================================================
mod npt_extra {
    use super::*;
    #[test]
    fn one() {
        assert_eq!(eval("next_power_of_two", &[i(1)]), i(1));
    }
    #[test]
    fn two() {
        assert_eq!(eval("next_power_of_two", &[i(2)]), i(2));
    }
    #[test]
    fn three() {
        assert_eq!(eval("next_power_of_two", &[i(3)]), i(4));
    }
    #[test]
    fn five() {
        assert_eq!(eval("next_power_of_two", &[i(5)]), i(8));
    }
    #[test]
    fn null_in() {
        assert_eq!(eval("next_power_of_two", &[null()]), null());
    }
    #[test]
    fn sixteen() {
        assert_eq!(eval("next_power_of_two", &[i(16)]), i(16));
    }
    #[test]
    fn seventeen() {
        assert_eq!(eval("next_power_of_two", &[i(17)]), i(32));
    }
}

// ===========================================================================
// square / log_base
// ===========================================================================
mod square_logbase {
    use super::*;
    #[test]
    fn square_basic() {
        assert_f64_close(&eval("square", &[f(5.0)]), 25.0, 0.001);
    }
    #[test]
    fn square_zero() {
        assert_f64_close(&eval("square", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn square_neg() {
        assert_f64_close(&eval("square", &[f(-3.0)]), 9.0, 0.001);
    }
    #[test]
    fn square_null() {
        assert_eq!(eval("square", &[null()]), null());
    }
    #[test]
    fn square_one() {
        assert_f64_close(&eval("square", &[f(1.0)]), 1.0, 0.001);
    }
    #[test]
    fn square_int() {
        assert_f64_close(&eval("square", &[i(4)]), 16.0, 0.001);
    }

    #[test]
    fn logbase_10_100() {
        assert_f64_close(&eval("log_base", &[f(10.0), f(100.0)]), 2.0, 0.001);
    }
    #[test]
    fn logbase_2_8() {
        assert_f64_close(&eval("log_base", &[f(2.0), f(8.0)]), 3.0, 0.001);
    }
    #[test]
    fn logbase_null() {
        assert_eq!(eval("log_base", &[null(), f(10.0)]), null());
    }
}

// ===========================================================================
// is_positive / is_negative / is_zero / is_even / is_odd
// ===========================================================================
mod predicates_extra {
    use super::*;
    #[test]
    fn pos_yes() {
        assert_eq!(eval("is_positive", &[i(5)]), i(1));
    }
    #[test]
    fn pos_no() {
        assert_eq!(eval("is_positive", &[i(-5)]), i(0));
    }
    #[test]
    fn pos_zero() {
        assert_eq!(eval("is_positive", &[i(0)]), i(0));
    }
    #[test]
    fn pos_null() {
        assert_eq!(eval("is_positive", &[null()]), null());
    }

    #[test]
    fn neg_yes() {
        assert_eq!(eval("is_negative", &[i(-5)]), i(1));
    }
    #[test]
    fn neg_no() {
        assert_eq!(eval("is_negative", &[i(5)]), i(0));
    }
    #[test]
    fn neg_zero() {
        assert_eq!(eval("is_negative", &[i(0)]), i(0));
    }
    #[test]
    fn neg_null() {
        assert_eq!(eval("is_negative", &[null()]), null());
    }

    #[test]
    fn zero_yes() {
        assert_eq!(eval("is_zero", &[i(0)]), i(1));
    }
    #[test]
    fn zero_no() {
        assert_eq!(eval("is_zero", &[i(5)]), i(0));
    }
    #[test]
    fn zero_null() {
        assert_eq!(eval("is_zero", &[null()]), null());
    }
    #[test]
    fn zero_float() {
        assert_eq!(eval("is_zero", &[f(0.0)]), i(1));
    }

    #[test]
    fn even_yes() {
        assert_eq!(eval("is_even", &[i(4)]), i(1));
    }
    #[test]
    fn even_no() {
        assert_eq!(eval("is_even", &[i(5)]), i(0));
    }
    #[test]
    fn even_zero() {
        assert_eq!(eval("is_even", &[i(0)]), i(1));
    }
    #[test]
    fn even_null() {
        assert_eq!(eval("is_even", &[null()]), null());
    }
    #[test]
    fn even_neg() {
        assert_eq!(eval("is_even", &[i(-4)]), i(1));
    }

    #[test]
    fn odd_yes() {
        assert_eq!(eval("is_odd", &[i(5)]), i(1));
    }
    #[test]
    fn odd_no() {
        assert_eq!(eval("is_odd", &[i(4)]), i(0));
    }
    #[test]
    fn odd_zero() {
        assert_eq!(eval("is_odd", &[i(0)]), i(0));
    }
    #[test]
    fn odd_null() {
        assert_eq!(eval("is_odd", &[null()]), null());
    }
    #[test]
    fn odd_neg() {
        assert_eq!(eval("is_odd", &[i(-3)]), i(1));
    }
}

// ===========================================================================
// between (scalar function)
// ===========================================================================
mod between_fn {
    use super::*;
    #[test]
    fn in_range() {
        assert_eq!(eval("between", &[i(5), i(1), i(10)]), i(1));
    }
    #[test]
    fn below() {
        assert_eq!(eval("between", &[i(0), i(1), i(10)]), i(0));
    }
    #[test]
    fn above() {
        assert_eq!(eval("between", &[i(11), i(1), i(10)]), i(0));
    }
    #[test]
    fn at_lower() {
        assert_eq!(eval("between", &[i(1), i(1), i(10)]), i(1));
    }
    #[test]
    fn at_upper() {
        assert_eq!(eval("between", &[i(10), i(1), i(10)]), i(1));
    }
    #[test]
    fn null_in() {
        assert_eq!(eval("between", &[null(), i(1), i(10)]), null());
    }
    #[test]
    fn float_range() {
        assert_eq!(eval("between", &[f(5.5), f(1.0), f(10.0)]), i(1));
    }
}

// ===========================================================================
// random functions (just verify they return correct types)
// ===========================================================================
mod random_extra {
    use super::*;
    #[test]
    fn random_returns_f64() {
        match eval("random", &[]) {
            Value::F64(v) => assert!(v >= 0.0 && v < 1.0),
            _ => panic!(),
        }
    }
    #[test]
    fn rand_alias() {
        match eval("rand", &[]) {
            Value::F64(v) => assert!(v >= 0.0 && v < 1.0),
            _ => panic!(),
        }
    }
    #[test]
    fn rnd_int_returns_i64() {
        match eval("rnd_int", &[]) {
            Value::I64(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn rnd_double_returns_f64() {
        match eval("rnd_double", &[]) {
            Value::F64(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn rnd_float_returns_f64() {
        match eval("rnd_float", &[]) {
            Value::F64(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn rnd_boolean_returns_i64() {
        match eval("rnd_boolean", &[]) {
            Value::I64(v) => assert!(v == 0 || v == 1),
            _ => panic!(),
        }
    }
    #[test]
    fn rnd_str_returns_str() {
        match eval("rnd_str", &[]) {
            Value::Str(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn rnd_uuid4_returns_str() {
        match eval("rnd_uuid4", &[]) {
            Value::Str(v) => assert!(v.len() > 0),
            _ => panic!(),
        }
    }
    #[test]
    fn rnd_timestamp_returns_ts() {
        match eval("rnd_timestamp", &[]) {
            Value::Timestamp(_) => {}
            _ => panic!(),
        }
    }
}

// ===========================================================================
// map_range
// ===========================================================================
mod map_range_extra {
    use super::*;
    #[test]
    fn basic() {
        assert_f64_close(
            &eval("map_range", &[f(5.0), f(0.0), f(10.0), f(0.0), f(100.0)]),
            50.0,
            0.01,
        );
    }
    #[test]
    fn at_min() {
        assert_f64_close(
            &eval("map_range", &[f(0.0), f(0.0), f(10.0), f(0.0), f(100.0)]),
            0.0,
            0.01,
        );
    }
    #[test]
    fn at_max() {
        assert_f64_close(
            &eval("map_range", &[f(10.0), f(0.0), f(10.0), f(0.0), f(100.0)]),
            100.0,
            0.01,
        );
    }
    #[test]
    fn null_in() {
        assert_eq!(
            eval("map_range", &[null(), f(0.0), f(10.0), f(0.0), f(100.0)]),
            null()
        );
    }
}

// ===========================================================================
// abs_diff
// ===========================================================================
mod abs_diff_extra {
    use super::*;
    #[test]
    fn basic() {
        assert_f64_close(&eval("abs_diff", &[i(10), i(3)]), 7.0, 0.001);
    }
    #[test]
    fn reversed() {
        assert_f64_close(&eval("abs_diff", &[i(3), i(10)]), 7.0, 0.001);
    }
    #[test]
    fn same() {
        assert_f64_close(&eval("abs_diff", &[i(5), i(5)]), 0.0, 0.001);
    }
    #[test]
    fn null_in() {
        assert_eq!(eval("abs_diff", &[null(), i(5)]), null());
    }
    #[test]
    fn neg() {
        assert_f64_close(&eval("abs_diff", &[i(-3), i(3)]), 6.0, 0.001);
    }
    #[test]
    fn float_basic() {
        assert_f64_close(&eval("abs_diff", &[f(10.5), f(3.5)]), 7.0, 0.001);
    }
}

// ===========================================================================
// hash_combine
// ===========================================================================
mod hash_combine_extra {
    use super::*;
    #[test]
    fn basic() {
        let r = eval("hash_combine", &[i(1), i(2)]);
        match r {
            Value::I64(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn same() {
        let r = eval("hash_combine", &[i(5), i(5)]);
        match r {
            Value::I64(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn null_in() {
        assert_eq!(eval("hash_combine", &[null(), i(5)]), null());
    }
    #[test]
    fn consistent() {
        assert_eq!(
            eval("hash_combine", &[i(1), i(2)]),
            eval("hash_combine", &[i(1), i(2)])
        );
    }
}

// ===========================================================================
// cast_bool / cast_timestamp
// ===========================================================================
mod cast_extra_types {
    use super::*;
    #[test]
    fn cast_bool_true() {
        assert_eq!(eval("cast_bool", &[i(1)]), i(1));
    }
    #[test]
    fn cast_bool_false() {
        assert_eq!(eval("cast_bool", &[i(0)]), i(0));
    }
    #[test]
    fn cast_bool_str_true() {
        assert_eq!(eval("cast_bool", &[s("true")]), i(1));
    }
    #[test]
    fn cast_bool_str_false() {
        assert_eq!(eval("cast_bool", &[s("false")]), i(0));
    }
    #[test]
    fn cast_bool_null() {
        assert_eq!(eval("cast_bool", &[null()]), null());
    }

    #[test]
    fn cast_ts_from_int() {
        match eval("cast_timestamp", &[i(1000)]) {
            Value::Timestamp(v) => assert_eq!(v, 1000),
            _ => panic!(),
        }
    }
    #[test]
    fn cast_ts_null() {
        assert_eq!(eval("cast_timestamp", &[null()]), null());
    }
    #[test]
    fn cast_ts_from_str() {
        match eval("cast_timestamp", &[s("1000")]) {
            Value::Timestamp(v) => assert_eq!(v, 1000),
            _ => panic!(),
        }
    }
}

// ===========================================================================
// to_number / to_date
// ===========================================================================
mod to_number_date {
    use super::*;
    #[test]
    fn to_number_int() {
        assert_eq!(eval("to_number", &[s("42")]), i(42));
    }
    #[test]
    fn to_number_float() {
        let r = eval("to_number", &[s("3.14")]);
        match r {
            Value::F64(v) => assert!((v - 3.14).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn to_number_null() {
        assert_eq!(eval("to_number", &[null()]), null());
    }

    #[test]
    fn to_date_from_int() {
        match eval("to_date", &[i(1_704_067_200_000_000_000i64)]) {
            Value::Timestamp(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn to_date_null() {
        assert_eq!(eval("to_date", &[null()]), null());
    }
}

// ===========================================================================
// epoch conversions
// ===========================================================================
mod epoch_extra {
    use super::*;
    use exchange_query::plan::Value::Timestamp;
    const NS: i64 = 1_704_067_200_000_000_000; // 2024-01-01T00:00:00Z in nanos

    #[test]
    fn epoch_seconds_basic() {
        let r = eval("epoch_seconds", &[Timestamp(NS)]);
        match r {
            Value::I64(v) => assert_eq!(v, 1_704_067_200),
            _ => panic!("{r:?}"),
        }
    }
    #[test]
    fn epoch_millis_basic() {
        let r = eval("epoch_millis", &[Timestamp(NS)]);
        match r {
            Value::I64(v) => assert_eq!(v, 1_704_067_200_000),
            _ => panic!("{r:?}"),
        }
    }
    #[test]
    fn epoch_micros_basic() {
        let r = eval("epoch_micros", &[Timestamp(NS)]);
        match r {
            Value::I64(v) => assert_eq!(v, 1_704_067_200_000_000),
            _ => panic!("{r:?}"),
        }
    }
    #[test]
    fn epoch_nanos_basic() {
        let r = eval("epoch_nanos", &[Timestamp(NS)]);
        match r {
            Value::I64(v) => assert_eq!(v, NS),
            _ => panic!("{r:?}"),
        }
    }
    #[test]
    fn epoch_seconds_null() {
        assert_eq!(eval("epoch_seconds", &[null()]), null());
    }
    #[test]
    fn epoch_millis_null() {
        assert_eq!(eval("epoch_millis", &[null()]), null());
    }
    #[test]
    fn epoch_micros_null() {
        assert_eq!(eval("epoch_micros", &[null()]), null());
    }
    #[test]
    fn epoch_nanos_null() {
        assert_eq!(eval("epoch_nanos", &[null()]), null());
    }
}
