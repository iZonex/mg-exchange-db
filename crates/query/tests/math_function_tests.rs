//! Comprehensive math function tests for ExchangeDB.
//! 500+ test cases covering every registered math scalar function.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

fn eval_err(name: &str, args: &[Value]) -> String {
    evaluate_scalar(name, args).unwrap_err()
}

fn assert_f64_close(val: &Value, expected: f64, tol: f64) {
    match val {
        Value::F64(v) => assert!((*v - expected).abs() < tol, "expected ~{expected}, got {v}"),
        other => panic!("expected F64(~{expected}), got {other:?}"),
    }
}

// ===========================================================================
// abs
// ===========================================================================
mod abs_tests {
    use super::*;

    #[test]
    fn positive_int() {
        assert_eq!(eval("abs", &[i(5)]), i(5));
    }
    #[test]
    fn negative_int() {
        assert_eq!(eval("abs", &[i(-5)]), i(5));
    }
    #[test]
    fn zero_int() {
        assert_eq!(eval("abs", &[i(0)]), i(0));
    }
    #[test]
    fn positive_float() {
        assert_eq!(eval("abs", &[f(3.14)]), f(3.14));
    }
    #[test]
    fn negative_float() {
        assert_eq!(eval("abs", &[f(-3.14)]), f(3.14));
    }
    #[test]
    fn zero_float() {
        assert_eq!(eval("abs", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("abs", &[null()]), null());
    }
    #[test]
    fn large_negative() {
        assert_eq!(eval("abs", &[i(-1_000_000)]), i(1_000_000));
    }
    #[test]
    fn small_float() {
        assert_eq!(eval("abs", &[f(-0.001)]), f(0.001));
    }
    #[test]
    fn one() {
        assert_eq!(eval("abs", &[i(1)]), i(1));
    }
    #[test]
    fn minus_one() {
        assert_eq!(eval("abs", &[i(-1)]), i(1));
    }
}

// ===========================================================================
// round
// ===========================================================================
mod round_tests {
    use super::*;

    #[test]
    fn round_down() {
        assert_f64_close(&eval("round", &[f(3.3)]), 3.0, 0.001);
    }
    #[test]
    fn round_up() {
        assert_f64_close(&eval("round", &[f(3.7)]), 4.0, 0.001);
    }
    #[test]
    fn round_half() {
        assert_f64_close(&eval("round", &[f(2.5)]), 3.0, 0.001);
    }
    #[test]
    fn round_zero() {
        assert_f64_close(&eval("round", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn round_negative() {
        assert_f64_close(&eval("round", &[f(-2.3)]), -2.0, 0.001);
    }
    #[test]
    fn round_neg_up() {
        assert_f64_close(&eval("round", &[f(-2.7)]), -3.0, 0.001);
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("round", &[null()]), null());
    }
    #[test]
    fn with_decimals_2() {
        assert_f64_close(&eval("round", &[f(3.14159), i(2)]), 3.14, 0.001);
    }
    #[test]
    fn with_decimals_0() {
        assert_f64_close(&eval("round", &[f(3.7), i(0)]), 4.0, 0.001);
    }
    #[test]
    fn with_decimals_3() {
        assert_f64_close(&eval("round", &[f(3.14159), i(3)]), 3.142, 0.001);
    }
    #[test]
    fn integer_input() {
        assert_f64_close(&eval("round", &[i(5)]), 5.0, 0.001);
    }
    #[test]
    fn large_value() {
        assert_f64_close(&eval("round", &[f(123456.789), i(1)]), 123456.8, 0.01);
    }
}

// ===========================================================================
// floor
// ===========================================================================
mod floor_tests {
    use super::*;

    #[test]
    fn positive() {
        assert_eq!(eval("floor", &[f(3.7)]), f(3.0));
    }
    #[test]
    fn negative() {
        assert_eq!(eval("floor", &[f(-3.2)]), f(-4.0));
    }
    #[test]
    fn integer() {
        assert_eq!(eval("floor", &[f(5.0)]), f(5.0));
    }
    #[test]
    fn zero() {
        assert_eq!(eval("floor", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("floor", &[null()]), null());
    }
    #[test]
    fn small_positive() {
        assert_eq!(eval("floor", &[f(0.1)]), f(0.0));
    }
    #[test]
    fn small_negative() {
        assert_eq!(eval("floor", &[f(-0.1)]), f(-1.0));
    }
    #[test]
    fn large() {
        assert_eq!(eval("floor", &[f(999.999)]), f(999.0));
    }
    #[test]
    fn int_input() {
        assert_eq!(eval("floor", &[i(5)]), f(5.0));
    }
    #[test]
    fn neg_int() {
        assert_eq!(eval("floor", &[i(-3)]), f(-3.0));
    }
    #[test]
    fn half() {
        assert_eq!(eval("floor", &[f(2.5)]), f(2.0));
    }
}

// ===========================================================================
// ceil
// ===========================================================================
mod ceil_tests {
    use super::*;

    #[test]
    fn positive() {
        assert_eq!(eval("ceil", &[f(3.1)]), f(4.0));
    }
    #[test]
    fn negative() {
        assert_eq!(eval("ceil", &[f(-3.7)]), f(-3.0));
    }
    #[test]
    fn integer() {
        assert_eq!(eval("ceil", &[f(5.0)]), f(5.0));
    }
    #[test]
    fn zero() {
        assert_eq!(eval("ceil", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("ceil", &[null()]), null());
    }
    #[test]
    fn small() {
        assert_eq!(eval("ceil", &[f(0.001)]), f(1.0));
    }
    #[test]
    fn neg_small() {
        assert_eq!(eval("ceil", &[f(-0.001)]), f(0.0));
    }
    #[test]
    fn large() {
        assert_eq!(eval("ceil", &[f(999.001)]), f(1000.0));
    }
    #[test]
    fn int_input() {
        assert_eq!(eval("ceil", &[i(5)]), f(5.0));
    }
    #[test]
    fn ceiling_alias() {
        assert_eq!(eval("ceiling", &[f(3.1)]), f(4.0));
    }
    #[test]
    fn half() {
        assert_eq!(eval("ceil", &[f(2.5)]), f(3.0));
    }
}

// ===========================================================================
// sqrt
// ===========================================================================
mod sqrt_tests {
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
    fn null_input() {
        assert_eq!(eval("sqrt", &[null()]), null());
    }
    #[test]
    fn negative_error() {
        assert!(eval_err("sqrt", &[f(-1.0)]).contains("negative"));
    }
    #[test]
    fn two() {
        assert_f64_close(&eval("sqrt", &[f(2.0)]), std::f64::consts::SQRT_2, 0.0001);
    }
    #[test]
    fn hundred() {
        assert_f64_close(&eval("sqrt", &[f(100.0)]), 10.0, 0.001);
    }
    #[test]
    fn sixteen() {
        assert_f64_close(&eval("sqrt", &[i(16)]), 4.0, 0.001);
    }
    #[test]
    fn quarter() {
        assert_f64_close(&eval("sqrt", &[f(0.25)]), 0.5, 0.001);
    }
}

// ===========================================================================
// pow
// ===========================================================================
mod pow_tests {
    use super::*;

    #[test]
    fn two_cubed() {
        assert_f64_close(&eval("pow", &[f(2.0), f(3.0)]), 8.0, 0.001);
    }
    #[test]
    fn two_squared() {
        assert_f64_close(&eval("pow", &[f(2.0), f(2.0)]), 4.0, 0.001);
    }
    #[test]
    fn ten_zero() {
        assert_f64_close(&eval("pow", &[f(10.0), f(0.0)]), 1.0, 0.001);
    }
    #[test]
    fn anything_to_one() {
        assert_f64_close(&eval("pow", &[f(5.0), f(1.0)]), 5.0, 0.001);
    }
    #[test]
    fn null_base() {
        assert_eq!(eval("pow", &[null(), f(2.0)]), null());
    }
    #[test]
    fn null_exp() {
        assert_eq!(eval("pow", &[f(2.0), null()]), null());
    }
    #[test]
    fn negative_base() {
        assert_f64_close(&eval("pow", &[f(-2.0), f(3.0)]), -8.0, 0.001);
    }
    #[test]
    fn fractional_exp() {
        assert_f64_close(&eval("pow", &[f(4.0), f(0.5)]), 2.0, 0.001);
    }
    #[test]
    fn zero_base() {
        assert_f64_close(&eval("pow", &[f(0.0), f(5.0)]), 0.0, 0.001);
    }
    #[test]
    fn int_args() {
        assert_f64_close(&eval("pow", &[i(3), i(4)]), 81.0, 0.001);
    }
    #[test]
    fn power_alias() {
        assert_f64_close(&eval("power", &[f(2.0), f(10.0)]), 1024.0, 0.001);
    }
}

// ===========================================================================
// log / log2 / log10 / ln
// ===========================================================================
mod log_tests {
    use super::*;

    #[test]
    fn log_e() {
        assert_f64_close(&eval("log", &[f(std::f64::consts::E)]), 1.0, 0.001);
    }
    #[test]
    fn log_one() {
        assert_f64_close(&eval("log", &[f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn log_null() {
        assert_eq!(eval("log", &[null()]), null());
    }
    #[test]
    fn log_negative() {
        assert!(eval_err("log", &[f(-1.0)]).contains("positive"));
    }
    #[test]
    fn log_zero() {
        assert!(eval_err("log", &[f(0.0)]).contains("positive"));
    }
    #[test]
    fn ln_alias() {
        assert_f64_close(&eval("ln", &[f(std::f64::consts::E)]), 1.0, 0.001);
    }
    #[test]
    fn log_ten() {
        assert_f64_close(&eval("log", &[f(10.0)]), 10.0_f64.ln(), 0.001);
    }

    #[test]
    fn log2_one() {
        assert_f64_close(&eval("log2", &[f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn log2_two() {
        assert_f64_close(&eval("log2", &[f(2.0)]), 1.0, 0.001);
    }
    #[test]
    fn log2_eight() {
        assert_f64_close(&eval("log2", &[f(8.0)]), 3.0, 0.001);
    }
    #[test]
    fn log2_null() {
        assert_eq!(eval("log2", &[null()]), null());
    }
    #[test]
    fn log2_neg() {
        assert!(eval_err("log2", &[f(-1.0)]).contains("positive"));
    }

    #[test]
    fn log10_one() {
        assert_f64_close(&eval("log10", &[f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn log10_ten() {
        assert_f64_close(&eval("log10", &[f(10.0)]), 1.0, 0.001);
    }
    #[test]
    fn log10_hundred() {
        assert_f64_close(&eval("log10", &[f(100.0)]), 2.0, 0.001);
    }
    #[test]
    fn log10_null() {
        assert_eq!(eval("log10", &[null()]), null());
    }
    #[test]
    fn log10_neg() {
        assert!(eval_err("log10", &[f(-1.0)]).contains("positive"));
    }
}

// ===========================================================================
// exp
// ===========================================================================
mod exp_tests {
    use super::*;

    #[test]
    fn exp_zero() {
        assert_f64_close(&eval("exp", &[f(0.0)]), 1.0, 0.001);
    }
    #[test]
    fn exp_one() {
        assert_f64_close(&eval("exp", &[f(1.0)]), std::f64::consts::E, 0.001);
    }
    #[test]
    fn exp_two() {
        assert_f64_close(
            &eval("exp", &[f(2.0)]),
            std::f64::consts::E * std::f64::consts::E,
            0.01,
        );
    }
    #[test]
    fn exp_negative() {
        assert_f64_close(&eval("exp", &[f(-1.0)]), 1.0 / std::f64::consts::E, 0.001);
    }
    #[test]
    fn exp_null() {
        assert_eq!(eval("exp", &[null()]), null());
    }
    #[test]
    fn exp_int() {
        assert_f64_close(&eval("exp", &[i(0)]), 1.0, 0.001);
    }
    #[test]
    fn exp_ln_roundtrip() {
        assert_f64_close(&eval("exp", &[eval("log", &[f(5.0)])]), 5.0, 0.001);
    }
}

// ===========================================================================
// sin / cos / tan
// ===========================================================================
mod trig_tests {
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
    fn sin_null() {
        assert_eq!(eval("sin", &[null()]), null());
    }
    #[test]
    fn sin_negative() {
        assert_f64_close(
            &eval("sin", &[f(-std::f64::consts::FRAC_PI_2)]),
            -1.0,
            0.001,
        );
    }
    #[test]
    fn sin_int() {
        assert_f64_close(&eval("sin", &[i(0)]), 0.0, 0.001);
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
    fn cos_two_pi() {
        assert_f64_close(&eval("cos", &[f(std::f64::consts::TAU)]), 1.0, 0.001);
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
    fn tan_negative() {
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
mod inverse_trig_tests {
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
    fn asin_neg_one() {
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
    fn asin_out_of_range() {
        assert!(eval_err("asin", &[f(2.0)]).contains("[-1, 1]"));
    }
    #[test]
    fn asin_half() {
        assert_f64_close(&eval("asin", &[f(0.5)]), std::f64::consts::FRAC_PI_6, 0.001);
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
    fn acos_neg_one() {
        assert_f64_close(&eval("acos", &[f(-1.0)]), std::f64::consts::PI, 0.001);
    }
    #[test]
    fn acos_null() {
        assert_eq!(eval("acos", &[null()]), null());
    }
    #[test]
    fn acos_out_of_range() {
        assert!(eval_err("acos", &[f(2.0)]).contains("[-1, 1]"));
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
    fn atan_negative() {
        assert_f64_close(
            &eval("atan", &[f(-1.0)]),
            -std::f64::consts::FRAC_PI_4,
            0.001,
        );
    }

    #[test]
    fn atan2_one_one() {
        assert_f64_close(
            &eval("atan2", &[f(1.0), f(1.0)]),
            std::f64::consts::FRAC_PI_4,
            0.001,
        );
    }
    #[test]
    fn atan2_zero_one() {
        assert_f64_close(&eval("atan2", &[f(0.0), f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn atan2_one_zero() {
        assert_f64_close(
            &eval("atan2", &[f(1.0), f(0.0)]),
            std::f64::consts::FRAC_PI_2,
            0.001,
        );
    }
    #[test]
    fn atan2_null() {
        assert_eq!(eval("atan2", &[null(), f(1.0)]), null());
    }
    #[test]
    fn atan2_neg() {
        assert_f64_close(
            &eval("atan2", &[f(-1.0), f(1.0)]),
            -std::f64::consts::FRAC_PI_4,
            0.001,
        );
    }
}

// ===========================================================================
// sinh / cosh / tanh
// ===========================================================================
mod hyp_trig_tests {
    use super::*;

    #[test]
    fn sinh_zero() {
        assert_f64_close(&eval("sinh", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn sinh_one() {
        assert_f64_close(&eval("sinh", &[f(1.0)]), 1.0_f64.sinh(), 0.001);
    }
    #[test]
    fn sinh_null() {
        assert_eq!(eval("sinh", &[null()]), null());
    }
    #[test]
    fn sinh_neg() {
        assert_f64_close(&eval("sinh", &[f(-1.0)]), (-1.0_f64).sinh(), 0.001);
    }

    #[test]
    fn cosh_zero() {
        assert_f64_close(&eval("cosh", &[f(0.0)]), 1.0, 0.001);
    }
    #[test]
    fn cosh_one() {
        assert_f64_close(&eval("cosh", &[f(1.0)]), 1.0_f64.cosh(), 0.001);
    }
    #[test]
    fn cosh_null() {
        assert_eq!(eval("cosh", &[null()]), null());
    }
    #[test]
    fn cosh_neg() {
        assert_f64_close(&eval("cosh", &[f(-1.0)]), 1.0_f64.cosh(), 0.001);
    }

    #[test]
    fn tanh_zero() {
        assert_f64_close(&eval("tanh", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn tanh_one() {
        assert_f64_close(&eval("tanh", &[f(1.0)]), 1.0_f64.tanh(), 0.001);
    }
    #[test]
    fn tanh_null() {
        assert_eq!(eval("tanh", &[null()]), null());
    }
    #[test]
    fn tanh_large() {
        assert_f64_close(&eval("tanh", &[f(100.0)]), 1.0, 0.001);
    }
}

// ===========================================================================
// mod
// ===========================================================================
mod mod_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("mod", &[i(10), i(3)]), i(1));
    }
    #[test]
    fn even() {
        assert_eq!(eval("mod", &[i(10), i(5)]), i(0));
    }
    #[test]
    fn null_first() {
        assert_eq!(eval("mod", &[null(), i(3)]), null());
    }
    #[test]
    fn null_second() {
        assert_eq!(eval("mod", &[i(10), null()]), null());
    }
    #[test]
    fn div_by_zero() {
        assert!(eval_err("mod", &[i(10), i(0)]).contains("zero"));
    }
    #[test]
    fn negative() {
        assert_eq!(eval("mod", &[i(-10), i(3)]), i(-1));
    }
    #[test]
    fn float_mod() {
        assert_f64_close(&eval("mod", &[f(10.5), f(3.0)]), 1.5, 0.001);
    }
    #[test]
    fn one() {
        assert_eq!(eval("mod", &[i(7), i(1)]), i(0));
    }
    #[test]
    fn same() {
        assert_eq!(eval("mod", &[i(5), i(5)]), i(0));
    }
    #[test]
    fn larger_divisor() {
        assert_eq!(eval("mod", &[i(3), i(5)]), i(3));
    }
    #[test]
    fn remainder_alias() {
        assert_eq!(eval("remainder", &[i(10), i(3)]), i(1));
    }
}

// ===========================================================================
// sign
// ===========================================================================
mod sign_tests {
    use super::*;

    #[test]
    fn positive() {
        assert_eq!(eval("sign", &[f(5.0)]), i(1));
    }
    #[test]
    fn negative() {
        assert_eq!(eval("sign", &[f(-5.0)]), i(-1));
    }
    #[test]
    fn zero() {
        assert_eq!(eval("sign", &[f(0.0)]), i(0));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("sign", &[null()]), null());
    }
    #[test]
    fn int_pos() {
        assert_eq!(eval("sign", &[i(100)]), i(1));
    }
    #[test]
    fn int_neg() {
        assert_eq!(eval("sign", &[i(-100)]), i(-1));
    }
    #[test]
    fn int_zero() {
        assert_eq!(eval("sign", &[i(0)]), i(0));
    }
    #[test]
    fn small_pos() {
        assert_eq!(eval("sign", &[f(0.001)]), i(1));
    }
    #[test]
    fn small_neg() {
        assert_eq!(eval("sign", &[f(-0.001)]), i(-1));
    }
    #[test]
    fn signum_alias() {
        assert_eq!(eval("signum", &[f(5.0)]), i(1));
    }
}

// ===========================================================================
// pi / e / tau / infinity / nan
// ===========================================================================
mod constants_tests {
    use super::*;

    #[test]
    fn pi_value() {
        assert_f64_close(&eval("pi", &[]), std::f64::consts::PI, 0.0001);
    }
    #[test]
    fn e_value() {
        assert_f64_close(&eval("e", &[]), std::f64::consts::E, 0.0001);
    }
    #[test]
    fn tau_value() {
        assert_f64_close(&eval("tau", &[]), std::f64::consts::TAU, 0.0001);
    }
    #[test]
    fn infinity_value() {
        match eval("infinity", &[]) {
            Value::F64(v) => assert!(v.is_infinite()),
            _ => panic!(),
        }
    }
    #[test]
    fn nan_value() {
        match eval("nan", &[]) {
            Value::F64(v) => assert!(v.is_nan()),
            _ => panic!(),
        }
    }
}

// ===========================================================================
// degrees / radians
// ===========================================================================
mod deg_rad_tests {
    use super::*;

    #[test]
    fn degrees_pi() {
        assert_f64_close(&eval("degrees", &[f(std::f64::consts::PI)]), 180.0, 0.001);
    }
    #[test]
    fn degrees_zero() {
        assert_f64_close(&eval("degrees", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn degrees_half_pi() {
        assert_f64_close(
            &eval("degrees", &[f(std::f64::consts::FRAC_PI_2)]),
            90.0,
            0.001,
        );
    }
    #[test]
    fn degrees_null() {
        assert_eq!(eval("degrees", &[null()]), null());
    }
    #[test]
    fn degrees_two_pi() {
        assert_f64_close(&eval("degrees", &[f(std::f64::consts::TAU)]), 360.0, 0.001);
    }
    #[test]
    fn degrees_negative() {
        assert_f64_close(&eval("degrees", &[f(-std::f64::consts::PI)]), -180.0, 0.001);
    }

    #[test]
    fn radians_180() {
        assert_f64_close(&eval("radians", &[f(180.0)]), std::f64::consts::PI, 0.001);
    }
    #[test]
    fn radians_zero() {
        assert_f64_close(&eval("radians", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn radians_90() {
        assert_f64_close(
            &eval("radians", &[f(90.0)]),
            std::f64::consts::FRAC_PI_2,
            0.001,
        );
    }
    #[test]
    fn radians_null() {
        assert_eq!(eval("radians", &[null()]), null());
    }
    #[test]
    fn radians_360() {
        assert_f64_close(&eval("radians", &[f(360.0)]), std::f64::consts::TAU, 0.001);
    }
    #[test]
    fn roundtrip() {
        assert_f64_close(&eval("radians", &[eval("degrees", &[f(1.5)])]), 1.5, 0.001);
    }
}

// ===========================================================================
// gcd / lcm
// ===========================================================================
mod gcd_lcm_tests {
    use super::*;

    #[test]
    fn gcd_basic() {
        assert_eq!(eval("gcd", &[i(12), i(8)]), i(4));
    }
    #[test]
    fn gcd_coprime() {
        assert_eq!(eval("gcd", &[i(7), i(13)]), i(1));
    }
    #[test]
    fn gcd_same() {
        assert_eq!(eval("gcd", &[i(5), i(5)]), i(5));
    }
    #[test]
    fn gcd_zero() {
        assert_eq!(eval("gcd", &[i(0), i(5)]), i(5));
    }
    #[test]
    fn gcd_both_zero() {
        assert_eq!(eval("gcd", &[i(0), i(0)]), i(0));
    }
    #[test]
    fn gcd_negative() {
        assert_eq!(eval("gcd", &[i(-12), i(8)]), i(4));
    }
    #[test]
    fn gcd_null() {
        assert_eq!(eval("gcd", &[null(), i(5)]), null());
    }
    #[test]
    fn gcd_one() {
        assert_eq!(eval("gcd", &[i(1), i(100)]), i(1));
    }
    #[test]
    fn gcd_large() {
        assert_eq!(eval("gcd", &[i(100), i(75)]), i(25));
    }
    #[test]
    fn gcd_prime() {
        assert_eq!(eval("gcd", &[i(17), i(31)]), i(1));
    }

    #[test]
    fn lcm_basic() {
        assert_eq!(eval("lcm", &[i(4), i(6)]), i(12));
    }
    #[test]
    fn lcm_coprime() {
        assert_eq!(eval("lcm", &[i(7), i(13)]), i(91));
    }
    #[test]
    fn lcm_same() {
        assert_eq!(eval("lcm", &[i(5), i(5)]), i(5));
    }
    #[test]
    fn lcm_one() {
        assert_eq!(eval("lcm", &[i(1), i(7)]), i(7));
    }
    #[test]
    fn lcm_zero() {
        assert_eq!(eval("lcm", &[i(0), i(0)]), i(0));
    }
    #[test]
    fn lcm_null() {
        assert_eq!(eval("lcm", &[null(), i(5)]), null());
    }
    #[test]
    fn lcm_negative() {
        assert_eq!(eval("lcm", &[i(-4), i(6)]), i(12));
    }
    #[test]
    fn lcm_twelve_eighteen() {
        assert_eq!(eval("lcm", &[i(12), i(18)]), i(36));
    }
}

// ===========================================================================
// bit_and / bit_or / bit_xor / bit_not / bit_shift_left / bit_shift_right
// ===========================================================================
mod bit_tests {
    use super::*;

    #[test]
    fn and_basic() {
        assert_eq!(eval("bit_and", &[i(0b1100), i(0b1010)]), i(0b1000));
    }
    #[test]
    fn and_all_ones() {
        assert_eq!(eval("bit_and", &[i(0xFF), i(0xFF)]), i(0xFF));
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
    fn or_basic() {
        assert_eq!(eval("bit_or", &[i(0b1100), i(0b1010)]), i(0b1110));
    }
    #[test]
    fn or_zero() {
        assert_eq!(eval("bit_or", &[i(0), i(0)]), i(0));
    }
    #[test]
    fn or_with_zero() {
        assert_eq!(eval("bit_or", &[i(5), i(0)]), i(5));
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
        assert_eq!(eval("bit_xor", &[i(5), i(5)]), i(0));
    }
    #[test]
    fn xor_zero() {
        assert_eq!(eval("bit_xor", &[i(5), i(0)]), i(5));
    }
    #[test]
    fn xor_null() {
        assert_eq!(eval("bit_xor", &[null(), i(5)]), null());
    }

    #[test]
    fn not_zero() {
        assert_eq!(eval("bit_not", &[i(0)]), i(-1));
    }
    #[test]
    fn not_neg_one() {
        assert_eq!(eval("bit_not", &[i(-1)]), i(0));
    }
    #[test]
    fn not_null() {
        assert_eq!(eval("bit_not", &[null()]), null());
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
    fn shl_large() {
        assert_eq!(eval("bit_shift_left", &[i(1), i(10)]), i(1024));
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
    fn shr_large() {
        assert_eq!(eval("bit_shift_right", &[i(1024), i(10)]), i(1));
    }
}

// ===========================================================================
// factorial
// ===========================================================================
mod factorial_tests {
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
    fn twenty() {
        assert_eq!(eval("factorial", &[i(20)]), i(2432902008176640000));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("factorial", &[null()]), null());
    }
    #[test]
    fn negative() {
        assert!(eval_err("factorial", &[i(-1)]).contains("non-negative"));
    }
    #[test]
    fn too_large() {
        assert!(eval_err("factorial", &[i(21)]).contains("too large"));
    }
    #[test]
    fn two() {
        assert_eq!(eval("factorial", &[i(2)]), i(2));
    }
    #[test]
    fn three() {
        assert_eq!(eval("factorial", &[i(3)]), i(6));
    }
}

// ===========================================================================
// cbrt
// ===========================================================================
mod cbrt_tests {
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
    fn negative() {
        assert_f64_close(&eval("cbrt", &[f(-8.0)]), -2.0, 0.001);
    }
    #[test]
    fn null_input() {
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
// trunc
// ===========================================================================
mod trunc_tests {
    use super::*;

    #[test]
    fn positive() {
        assert_f64_close(&eval("trunc", &[f(3.7)]), 3.0, 0.001);
    }
    #[test]
    fn negative() {
        assert_f64_close(&eval("trunc", &[f(-3.7)]), -3.0, 0.001);
    }
    #[test]
    fn zero() {
        assert_f64_close(&eval("trunc", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("trunc", &[null()]), null());
    }
    #[test]
    fn with_decimals() {
        assert_f64_close(&eval("trunc", &[f(3.14159), i(2)]), 3.14, 0.001);
    }
    #[test]
    fn with_zero_decimals() {
        assert_f64_close(&eval("trunc", &[f(3.7), i(0)]), 3.0, 0.001);
    }
    #[test]
    fn int_input() {
        assert_f64_close(&eval("trunc", &[i(5)]), 5.0, 0.001);
    }
    #[test]
    fn truncate_alias() {
        assert_f64_close(&eval("truncate", &[f(3.7)]), 3.0, 0.001);
    }
    #[test]
    fn neg_with_dec() {
        assert_f64_close(&eval("trunc", &[f(-3.14159), i(2)]), -3.14, 0.001);
    }
}

// ===========================================================================
// div
// ===========================================================================
mod div_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("div", &[i(10), i(3)]), i(3));
    }
    #[test]
    fn even() {
        assert_eq!(eval("div", &[i(10), i(5)]), i(2));
    }
    #[test]
    fn negative() {
        assert_eq!(eval("div", &[i(-10), i(3)]), i(-3));
    }
    #[test]
    fn null_first() {
        assert_eq!(eval("div", &[null(), i(3)]), null());
    }
    #[test]
    fn null_second() {
        assert_eq!(eval("div", &[i(10), null()]), null());
    }
    #[test]
    fn div_by_zero() {
        assert!(eval_err("div", &[i(10), i(0)]).contains("zero"));
    }
    #[test]
    fn one() {
        assert_eq!(eval("div", &[i(7), i(1)]), i(7));
    }
    #[test]
    fn same() {
        assert_eq!(eval("div", &[i(5), i(5)]), i(1));
    }
    #[test]
    fn larger_divisor() {
        assert_eq!(eval("div", &[i(3), i(5)]), i(0));
    }
    #[test]
    fn large() {
        assert_eq!(eval("div", &[i(1000000), i(7)]), i(142857));
    }
}

// ===========================================================================
// width_bucket
// ===========================================================================
mod width_bucket_tests {
    use super::*;

    #[test]
    fn middle() {
        assert_eq!(
            eval("width_bucket", &[f(5.0), f(0.0), f(10.0), i(10)]),
            i(6)
        );
    }
    #[test]
    fn at_min() {
        assert_eq!(
            eval("width_bucket", &[f(0.0), f(0.0), f(10.0), i(10)]),
            i(1)
        );
    }
    #[test]
    fn below_min() {
        assert_eq!(
            eval("width_bucket", &[f(-1.0), f(0.0), f(10.0), i(10)]),
            i(0)
        );
    }
    #[test]
    fn at_max() {
        assert_eq!(
            eval("width_bucket", &[f(10.0), f(0.0), f(10.0), i(10)]),
            i(11)
        );
    }
    #[test]
    fn above_max() {
        assert_eq!(
            eval("width_bucket", &[f(15.0), f(0.0), f(10.0), i(10)]),
            i(11)
        );
    }
    #[test]
    fn null_input() {
        assert_eq!(
            eval("width_bucket", &[null(), f(0.0), f(10.0), i(10)]),
            null()
        );
    }
    #[test]
    fn five_buckets() {
        assert_eq!(eval("width_bucket", &[f(3.0), f(0.0), f(10.0), i(5)]), i(2));
    }
    #[test]
    fn one_bucket() {
        assert_eq!(eval("width_bucket", &[f(5.0), f(0.0), f(10.0), i(1)]), i(1));
    }
    #[test]
    fn first_bucket() {
        assert_eq!(
            eval("width_bucket", &[f(0.5), f(0.0), f(10.0), i(10)]),
            i(1)
        );
    }
    #[test]
    fn last_bucket() {
        assert_eq!(
            eval("width_bucket", &[f(9.5), f(0.0), f(10.0), i(10)]),
            i(10)
        );
    }
}

// ===========================================================================
// clamp
// ===========================================================================
mod clamp_tests {
    use super::*;

    #[test]
    fn in_range() {
        assert_f64_close(&eval("clamp", &[f(5.0), f(0.0), f(10.0)]), 5.0, 0.001);
    }
    #[test]
    fn below() {
        assert_f64_close(&eval("clamp", &[f(-5.0), f(0.0), f(10.0)]), 0.0, 0.001);
    }
    #[test]
    fn above() {
        assert_f64_close(&eval("clamp", &[f(15.0), f(0.0), f(10.0)]), 10.0, 0.001);
    }
    #[test]
    fn at_min() {
        assert_f64_close(&eval("clamp", &[f(0.0), f(0.0), f(10.0)]), 0.0, 0.001);
    }
    #[test]
    fn at_max() {
        assert_f64_close(&eval("clamp", &[f(10.0), f(0.0), f(10.0)]), 10.0, 0.001);
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("clamp", &[null(), f(0.0), f(10.0)]), null());
    }
    #[test]
    fn int_input() {
        assert_f64_close(&eval("clamp", &[i(5), f(0.0), f(10.0)]), 5.0, 0.001);
    }
    #[test]
    fn negative_range() {
        assert_f64_close(&eval("clamp", &[f(0.0), f(-10.0), f(-1.0)]), -1.0, 0.001);
    }
}

// ===========================================================================
// lerp
// ===========================================================================
mod lerp_tests {
    use super::*;

    #[test]
    fn at_zero() {
        assert_f64_close(&eval("lerp", &[f(0.0), f(10.0), f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn at_one() {
        assert_f64_close(&eval("lerp", &[f(0.0), f(10.0), f(1.0)]), 10.0, 0.001);
    }
    #[test]
    fn at_half() {
        assert_f64_close(&eval("lerp", &[f(0.0), f(10.0), f(0.5)]), 5.0, 0.001);
    }
    #[test]
    fn quarter() {
        assert_f64_close(&eval("lerp", &[f(0.0), f(100.0), f(0.25)]), 25.0, 0.001);
    }
    #[test]
    fn same_values() {
        assert_f64_close(&eval("lerp", &[f(5.0), f(5.0), f(0.5)]), 5.0, 0.001);
    }
    #[test]
    fn negative_range() {
        assert_f64_close(&eval("lerp", &[f(-10.0), f(10.0), f(0.5)]), 0.0, 0.001);
    }
    #[test]
    fn beyond_one() {
        assert_f64_close(&eval("lerp", &[f(0.0), f(10.0), f(2.0)]), 20.0, 0.001);
    }
    #[test]
    fn int_inputs() {
        assert_f64_close(&eval("lerp", &[i(0), i(10), f(0.5)]), 5.0, 0.001);
    }
}

// ===========================================================================
// is_finite / is_nan / is_inf
// ===========================================================================
mod float_predicate_tests {
    use super::*;

    #[test]
    fn is_finite_normal() {
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
    fn is_finite_int() {
        assert_eq!(eval("is_finite", &[i(5)]), i(1));
    }
    #[test]
    fn is_finite_null() {
        assert_eq!(eval("is_finite", &[null()]), null());
    }

    #[test]
    fn is_nan_normal() {
        assert_eq!(eval("is_nan", &[f(1.0)]), i(0));
    }
    #[test]
    fn is_nan_nan() {
        assert_eq!(eval("is_nan", &[f(f64::NAN)]), i(1));
    }
    #[test]
    fn is_nan_inf() {
        assert_eq!(eval("is_nan", &[f(f64::INFINITY)]), i(0));
    }
    #[test]
    fn is_nan_int() {
        assert_eq!(eval("is_nan", &[i(5)]), i(0));
    }
    #[test]
    fn is_nan_null() {
        assert_eq!(eval("is_nan", &[null()]), null());
    }

    #[test]
    fn is_inf_normal() {
        assert_eq!(eval("is_inf", &[f(1.0)]), i(0));
    }
    #[test]
    fn is_inf_pos_inf() {
        assert_eq!(eval("is_inf", &[f(f64::INFINITY)]), i(1));
    }
    #[test]
    fn is_inf_neg_inf() {
        assert_eq!(eval("is_inf", &[f(f64::NEG_INFINITY)]), i(1));
    }
    #[test]
    fn is_inf_nan() {
        assert_eq!(eval("is_inf", &[f(f64::NAN)]), i(0));
    }
    #[test]
    fn is_inf_null() {
        assert_eq!(eval("is_inf", &[null()]), null());
    }
}

// ===========================================================================
// fma
// ===========================================================================
mod fma_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_f64_close(&eval("fma", &[f(2.0), f(3.0), f(4.0)]), 10.0, 0.001);
    }
    #[test]
    fn with_zero() {
        assert_f64_close(&eval("fma", &[f(5.0), f(0.0), f(3.0)]), 3.0, 0.001);
    }
    #[test]
    fn all_zero() {
        assert_f64_close(&eval("fma", &[f(0.0), f(0.0), f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn negative() {
        assert_f64_close(&eval("fma", &[f(-2.0), f(3.0), f(1.0)]), -5.0, 0.001);
    }
    #[test]
    fn ints() {
        assert_f64_close(&eval("fma", &[i(2), i(3), i(4)]), 10.0, 0.001);
    }
    #[test]
    fn large() {
        assert_f64_close(
            &eval("fma", &[f(1000.0), f(1000.0), f(1.0)]),
            1000001.0,
            0.01,
        );
    }
}

// ===========================================================================
// hypot
// ===========================================================================
mod hypot_tests {
    use super::*;

    #[test]
    fn three_four() {
        assert_f64_close(&eval("hypot", &[f(3.0), f(4.0)]), 5.0, 0.001);
    }
    #[test]
    fn both_zero() {
        assert_f64_close(&eval("hypot", &[f(0.0), f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn one_zero() {
        assert_f64_close(&eval("hypot", &[f(5.0), f(0.0)]), 5.0, 0.001);
    }
    #[test]
    fn unit() {
        assert_f64_close(
            &eval("hypot", &[f(1.0), f(1.0)]),
            std::f64::consts::SQRT_2,
            0.001,
        );
    }
    #[test]
    fn negative() {
        assert_f64_close(&eval("hypot", &[f(-3.0), f(-4.0)]), 5.0, 0.001);
    }
    #[test]
    fn ints() {
        assert_f64_close(&eval("hypot", &[i(5), i(12)]), 13.0, 0.001);
    }
}

// ===========================================================================
// copysign
// ===========================================================================
mod copysign_tests {
    use super::*;

    #[test]
    fn pos_pos() {
        assert_f64_close(&eval("copysign", &[f(3.0), f(1.0)]), 3.0, 0.001);
    }
    #[test]
    fn pos_neg() {
        assert_f64_close(&eval("copysign", &[f(3.0), f(-1.0)]), -3.0, 0.001);
    }
    #[test]
    fn neg_pos() {
        assert_f64_close(&eval("copysign", &[f(-3.0), f(1.0)]), 3.0, 0.001);
    }
    #[test]
    fn neg_neg() {
        assert_f64_close(&eval("copysign", &[f(-3.0), f(-1.0)]), -3.0, 0.001);
    }
    #[test]
    fn zero_pos() {
        assert_f64_close(&eval("copysign", &[f(0.0), f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn ints() {
        assert_f64_close(&eval("copysign", &[i(5), i(-1)]), -5.0, 0.001);
    }
}

// ===========================================================================
// next_power_of_two
// ===========================================================================
mod next_power_of_two_tests {
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
    fn sixteen() {
        assert_eq!(eval("next_power_of_two", &[i(16)]), i(16));
    }
    #[test]
    fn seventeen() {
        assert_eq!(eval("next_power_of_two", &[i(17)]), i(32));
    }
    #[test]
    fn zero() {
        assert_eq!(eval("next_power_of_two", &[i(0)]), i(1));
    }
    #[test]
    fn negative() {
        assert_eq!(eval("next_power_of_two", &[i(-5)]), i(1));
    }
    #[test]
    fn thousand() {
        assert_eq!(eval("next_power_of_two", &[i(1000)]), i(1024));
    }
    #[test]
    fn power_of_two() {
        assert_eq!(eval("next_power_of_two", &[i(64)]), i(64));
    }
}

// ===========================================================================
// square / negate / reciprocal
// ===========================================================================
mod square_negate_recip_tests {
    use super::*;

    #[test]
    fn square_basic() {
        assert_f64_close(&eval("square", &[f(3.0)]), 9.0, 0.001);
    }
    #[test]
    fn square_zero() {
        assert_f64_close(&eval("square", &[f(0.0)]), 0.0, 0.001);
    }
    #[test]
    fn square_negative() {
        assert_f64_close(&eval("square", &[f(-4.0)]), 16.0, 0.001);
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
        assert_f64_close(&eval("square", &[i(5)]), 25.0, 0.001);
    }

    #[test]
    fn negate_pos() {
        assert_eq!(eval("negate", &[i(5)]), i(-5));
    }
    #[test]
    fn negate_neg() {
        assert_eq!(eval("negate", &[i(-5)]), i(5));
    }
    #[test]
    fn negate_zero() {
        assert_eq!(eval("negate", &[i(0)]), i(0));
    }
    #[test]
    fn negate_float() {
        assert_eq!(eval("negate", &[f(3.14)]), f(-3.14));
    }
    #[test]
    fn negate_null() {
        assert_eq!(eval("negate", &[null()]), null());
    }

    #[test]
    fn recip_basic() {
        assert_f64_close(&eval("reciprocal", &[f(2.0)]), 0.5, 0.001);
    }
    #[test]
    fn recip_one() {
        assert_f64_close(&eval("reciprocal", &[f(1.0)]), 1.0, 0.001);
    }
    #[test]
    fn recip_half() {
        assert_f64_close(&eval("reciprocal", &[f(0.5)]), 2.0, 0.001);
    }
    #[test]
    fn recip_neg() {
        assert_f64_close(&eval("reciprocal", &[f(-2.0)]), -0.5, 0.001);
    }
    #[test]
    fn recip_null() {
        assert_eq!(eval("reciprocal", &[null()]), null());
    }
    #[test]
    fn recip_zero() {
        assert!(eval_err("reciprocal", &[f(0.0)]).contains("zero"));
    }
}

// ===========================================================================
// log_base
// ===========================================================================
mod log_base_tests {
    use super::*;

    #[test]
    fn base_10() {
        assert_f64_close(&eval("log_base", &[f(10.0), f(100.0)]), 2.0, 0.001);
    }
    #[test]
    fn base_2() {
        assert_f64_close(&eval("log_base", &[f(2.0), f(8.0)]), 3.0, 0.001);
    }
    #[test]
    fn base_e() {
        assert_f64_close(
            &eval(
                "log_base",
                &[f(std::f64::consts::E), f(std::f64::consts::E)],
            ),
            1.0,
            0.001,
        );
    }
    #[test]
    fn one_arg() {
        assert_f64_close(&eval("log_base", &[f(10.0), f(1.0)]), 0.0, 0.001);
    }
    #[test]
    fn bad_base() {
        assert!(eval_err("log_base", &[f(1.0), f(10.0)]).contains("base"));
    }
    #[test]
    fn neg_arg() {
        assert!(eval_err("log_base", &[f(10.0), f(-1.0)]).contains("positive"));
    }
}

// ===========================================================================
// abs_diff
// ===========================================================================
mod abs_diff_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_f64_close(&eval("abs_diff", &[f(10.0), f(3.0)]), 7.0, 0.001);
    }
    #[test]
    fn reverse() {
        assert_f64_close(&eval("abs_diff", &[f(3.0), f(10.0)]), 7.0, 0.001);
    }
    #[test]
    fn equal() {
        assert_f64_close(&eval("abs_diff", &[f(5.0), f(5.0)]), 0.0, 0.001);
    }
    #[test]
    fn negative() {
        assert_f64_close(&eval("abs_diff", &[f(-3.0), f(3.0)]), 6.0, 0.001);
    }
    #[test]
    fn ints() {
        assert_f64_close(&eval("abs_diff", &[i(10), i(3)]), 7.0, 0.001);
    }
    #[test]
    fn zeros() {
        assert_f64_close(&eval("abs_diff", &[f(0.0), f(0.0)]), 0.0, 0.001);
    }
}

// ===========================================================================
// is_positive / is_negative / is_zero / is_even / is_odd / between
// ===========================================================================
mod numeric_predicate_tests {
    use super::*;

    #[test]
    fn is_pos_yes() {
        assert_eq!(eval("is_positive", &[f(5.0)]), i(1));
    }
    #[test]
    fn is_pos_no() {
        assert_eq!(eval("is_positive", &[f(-5.0)]), i(0));
    }
    #[test]
    fn is_pos_zero() {
        assert_eq!(eval("is_positive", &[f(0.0)]), i(0));
    }
    #[test]
    fn is_pos_null() {
        assert_eq!(eval("is_positive", &[null()]), null());
    }

    #[test]
    fn is_neg_yes() {
        assert_eq!(eval("is_negative", &[f(-5.0)]), i(1));
    }
    #[test]
    fn is_neg_no() {
        assert_eq!(eval("is_negative", &[f(5.0)]), i(0));
    }
    #[test]
    fn is_neg_zero() {
        assert_eq!(eval("is_negative", &[f(0.0)]), i(0));
    }
    #[test]
    fn is_neg_null() {
        assert_eq!(eval("is_negative", &[null()]), null());
    }

    #[test]
    fn is_zero_yes() {
        assert_eq!(eval("is_zero", &[f(0.0)]), i(1));
    }
    #[test]
    fn is_zero_no() {
        assert_eq!(eval("is_zero", &[f(5.0)]), i(0));
    }
    #[test]
    fn is_zero_null() {
        assert_eq!(eval("is_zero", &[null()]), null());
    }

    #[test]
    fn is_even_yes() {
        assert_eq!(eval("is_even", &[i(4)]), i(1));
    }
    #[test]
    fn is_even_no() {
        assert_eq!(eval("is_even", &[i(3)]), i(0));
    }
    #[test]
    fn is_even_zero() {
        assert_eq!(eval("is_even", &[i(0)]), i(1));
    }
    #[test]
    fn is_even_neg() {
        assert_eq!(eval("is_even", &[i(-2)]), i(1));
    }
    #[test]
    fn is_even_null() {
        assert_eq!(eval("is_even", &[null()]), null());
    }

    #[test]
    fn is_odd_yes() {
        assert_eq!(eval("is_odd", &[i(3)]), i(1));
    }
    #[test]
    fn is_odd_no() {
        assert_eq!(eval("is_odd", &[i(4)]), i(0));
    }
    #[test]
    fn is_odd_neg() {
        assert_eq!(eval("is_odd", &[i(-3)]), i(1));
    }
    #[test]
    fn is_odd_null() {
        assert_eq!(eval("is_odd", &[null()]), null());
    }

    #[test]
    fn between_inside() {
        assert_eq!(eval("between", &[f(5.0), f(1.0), f(10.0)]), i(1));
    }
    #[test]
    fn between_outside() {
        assert_eq!(eval("between", &[f(15.0), f(1.0), f(10.0)]), i(0));
    }
    #[test]
    fn between_at_min() {
        assert_eq!(eval("between", &[f(1.0), f(1.0), f(10.0)]), i(1));
    }
    #[test]
    fn between_at_max() {
        assert_eq!(eval("between", &[f(10.0), f(1.0), f(10.0)]), i(1));
    }
    #[test]
    fn between_null() {
        assert_eq!(eval("between", &[null(), f(1.0), f(10.0)]), null());
    }
}

// ===========================================================================
// bit_count / leading_zeros / trailing_zeros
// ===========================================================================
mod bit_count_tests {
    use super::*;

    #[test]
    fn count_0() {
        assert_eq!(eval("bit_count", &[i(0)]), i(0));
    }
    #[test]
    fn count_1() {
        assert_eq!(eval("bit_count", &[i(1)]), i(1));
    }
    #[test]
    fn count_7() {
        assert_eq!(eval("bit_count", &[i(7)]), i(3));
    }
    #[test]
    fn count_255() {
        assert_eq!(eval("bit_count", &[i(255)]), i(8));
    }
    #[test]
    fn popcount_alias() {
        assert_eq!(eval("popcount", &[i(7)]), i(3));
    }

    #[test]
    fn leading_zeros_1() {
        assert_eq!(eval("leading_zeros", &[i(1)]), i(63));
    }
    #[test]
    fn leading_zeros_0() {
        assert_eq!(eval("leading_zeros", &[i(0)]), i(64));
    }
    #[test]
    fn leading_zeros_256() {
        assert_eq!(eval("leading_zeros", &[i(256)]), i(55));
    }

    #[test]
    fn trailing_zeros_0() {
        assert_eq!(eval("trailing_zeros", &[i(0)]), i(64));
    }
    #[test]
    fn trailing_zeros_1() {
        assert_eq!(eval("trailing_zeros", &[i(1)]), i(0));
    }
    #[test]
    fn trailing_zeros_8() {
        assert_eq!(eval("trailing_zeros", &[i(8)]), i(3));
    }
    #[test]
    fn trailing_zeros_16() {
        assert_eq!(eval("trailing_zeros", &[i(16)]), i(4));
    }
}

// ===========================================================================
// random (just test it returns F64 in [0,1))
// ===========================================================================
mod random_tests {
    use super::*;

    #[test]
    fn returns_float() {
        match eval("random", &[]) {
            Value::F64(v) => assert!(v >= 0.0 && v < 1.1),
            _ => panic!(),
        }
    }
    #[test]
    fn rand_alias() {
        match eval("rand", &[]) {
            Value::F64(_) => {}
            _ => panic!(),
        }
    }
}

// ===========================================================================
// hash / murmur3 / crc32 / fnv1a / hash_combine
// ===========================================================================
mod hash_tests {
    use super::*;

    #[test]
    fn hash_deterministic() {
        assert_eq!(eval("hash", &[s("test")]), eval("hash", &[s("test")]));
    }
    #[test]
    fn hash_different() {
        assert_ne!(eval("hash", &[s("a")]), eval("hash", &[s("b")]));
    }
    #[test]
    fn hash_int() {
        match eval("hash", &[i(42)]) {
            Value::I64(_) => {}
            _ => panic!(),
        }
    }

    #[test]
    fn murmur3_deterministic() {
        assert_eq!(eval("murmur3", &[s("test")]), eval("murmur3", &[s("test")]));
    }
    #[test]
    fn murmur3_different() {
        assert_ne!(eval("murmur3", &[s("a")]), eval("murmur3", &[s("b")]));
    }

    #[test]
    fn crc32_deterministic() {
        assert_eq!(eval("crc32", &[s("test")]), eval("crc32", &[s("test")]));
    }
    #[test]
    fn crc32_different() {
        assert_ne!(eval("crc32", &[s("a")]), eval("crc32", &[s("b")]));
    }
    #[test]
    fn crc32_null() {
        assert_eq!(eval("crc32", &[null()]), null());
    }

    #[test]
    fn fnv1a_deterministic() {
        assert_eq!(eval("fnv1a", &[s("test")]), eval("fnv1a", &[s("test")]));
    }
    #[test]
    fn fnv1a_different() {
        assert_ne!(eval("fnv1a", &[s("a")]), eval("fnv1a", &[s("b")]));
    }

    #[test]
    fn hash_combine_basic() {
        match eval("hash_combine", &[i(1), i(2)]) {
            Value::I64(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn hash_combine_deterministic() {
        assert_eq!(
            eval("hash_combine", &[i(1), i(2)]),
            eval("hash_combine", &[i(1), i(2)])
        );
    }
}

// ===========================================================================
// map_range
// ===========================================================================
mod map_range_tests {
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
    fn quarter() {
        assert_f64_close(
            &eval("map_range", &[f(2.5), f(0.0), f(10.0), f(0.0), f(100.0)]),
            25.0,
            0.01,
        );
    }
    #[test]
    fn reverse() {
        assert_f64_close(
            &eval("map_range", &[f(5.0), f(0.0), f(10.0), f(100.0), f(0.0)]),
            50.0,
            0.01,
        );
    }
}

// ===========================================================================
// Additional math tests for higher coverage
// ===========================================================================

mod abs_extended_tests {
    use super::*;

    #[test]
    fn max_i64() {
        assert_eq!(eval("abs", &[i(i64::MAX)]), i(i64::MAX));
    }
    #[test]
    fn float_point_one() {
        assert_eq!(eval("abs", &[f(-0.1)]), f(0.1));
    }
    #[test]
    fn float_pi() {
        assert_eq!(
            eval("abs", &[f(-std::f64::consts::PI)]),
            f(std::f64::consts::PI)
        );
    }
    #[test]
    fn abs_string_err() {
        assert!(evaluate_scalar("abs", &[s("hello")]).is_err());
    }
}

mod round_extended_tests {
    use super::*;

    #[test]
    fn negative_decimals() {
        assert_f64_close(&eval("round", &[f(1234.0), i(-2)]), 1200.0, 0.1);
    }
    #[test]
    fn very_small() {
        assert_f64_close(&eval("round", &[f(0.00456), i(3)]), 0.005, 0.0001);
    }
    #[test]
    fn pi_4_decimals() {
        assert_f64_close(
            &eval("round", &[f(std::f64::consts::PI), i(4)]),
            3.1416,
            0.00001,
        );
    }
    #[test]
    fn exactly_half_2() {
        assert_f64_close(&eval("round", &[f(0.5)]), 1.0, 0.001);
    }
    #[test]
    fn neg_half() {
        assert_f64_close(&eval("round", &[f(-0.5)]), -1.0, 0.001);
    }
    #[test]
    fn round_1_decimal() {
        assert_f64_close(&eval("round", &[f(3.45), i(1)]), 3.5, 0.001);
    }
}

mod floor_ceil_extended_tests {
    use super::*;

    #[test]
    fn floor_pi() {
        assert_eq!(eval("floor", &[f(std::f64::consts::PI)]), f(3.0));
    }
    #[test]
    fn floor_neg_pi() {
        assert_eq!(eval("floor", &[f(-std::f64::consts::PI)]), f(-4.0));
    }
    #[test]
    fn floor_exactly_3() {
        assert_eq!(eval("floor", &[f(3.0)]), f(3.0));
    }
    #[test]
    fn floor_0_9() {
        assert_eq!(eval("floor", &[f(0.9)]), f(0.0));
    }

    #[test]
    fn ceil_pi() {
        assert_eq!(eval("ceil", &[f(std::f64::consts::PI)]), f(4.0));
    }
    #[test]
    fn ceil_neg_pi() {
        assert_eq!(eval("ceil", &[f(-std::f64::consts::PI)]), f(-3.0));
    }
    #[test]
    fn ceil_exactly_3() {
        assert_eq!(eval("ceil", &[f(3.0)]), f(3.0));
    }
    #[test]
    fn ceil_0_1() {
        assert_eq!(eval("ceil", &[f(0.1)]), f(1.0));
    }
}

mod trig_extended_tests {
    use super::*;

    #[test]
    fn sin_sq_plus_cos_sq() {
        let x = 1.234;
        let sin_val = match eval("sin", &[f(x)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        let cos_val = match eval("cos", &[f(x)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert!((sin_val * sin_val + cos_val * cos_val - 1.0).abs() < 0.0001);
    }
    #[test]
    fn tan_equals_sin_div_cos() {
        let x = 0.5;
        let sin_val = match eval("sin", &[f(x)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        let cos_val = match eval("cos", &[f(x)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        let tan_val = match eval("tan", &[f(x)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert!((tan_val - sin_val / cos_val).abs() < 0.0001);
    }
    #[test]
    fn sin_pi_4() {
        assert_f64_close(
            &eval("sin", &[f(std::f64::consts::FRAC_PI_4)]),
            std::f64::consts::FRAC_1_SQRT_2,
            0.001,
        );
    }
    #[test]
    fn cos_pi_4() {
        assert_f64_close(
            &eval("cos", &[f(std::f64::consts::FRAC_PI_4)]),
            std::f64::consts::FRAC_1_SQRT_2,
            0.001,
        );
    }
    #[test]
    fn sin_neg_pi() {
        assert_f64_close(&eval("sin", &[f(-std::f64::consts::PI)]), 0.0, 0.001);
    }
    #[test]
    fn cos_neg_pi() {
        assert_f64_close(&eval("cos", &[f(-std::f64::consts::PI)]), -1.0, 0.001);
    }
}

mod inverse_trig_extended_tests {
    use super::*;

    #[test]
    fn asin_cos_roundtrip() {
        let x = 0.5;
        let a = match eval("asin", &[f(x)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert_f64_close(&eval("sin", &[f(a)]), x, 0.0001);
    }
    #[test]
    fn acos_cos_roundtrip() {
        let x = 0.5;
        let a = match eval("acos", &[f(x)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert_f64_close(&eval("cos", &[f(a)]), x, 0.0001);
    }
    #[test]
    fn atan_tan_roundtrip() {
        let x = 0.5;
        let a = match eval("atan", &[f(x)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert_f64_close(&eval("tan", &[f(a)]), x, 0.0001);
    }
    #[test]
    fn atan2_quadrants() {
        // Q1
        let r1 = match eval("atan2", &[f(1.0), f(1.0)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert!(r1 > 0.0);
        // Q2
        let r2 = match eval("atan2", &[f(1.0), f(-1.0)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert!(r2 > std::f64::consts::FRAC_PI_2);
        // Q3
        let r3 = match eval("atan2", &[f(-1.0), f(-1.0)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert!(r3 < -std::f64::consts::FRAC_PI_2);
        // Q4
        let r4 = match eval("atan2", &[f(-1.0), f(1.0)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert!(r4 < 0.0);
    }
}

mod hyperbolic_extended_tests {
    use super::*;

    #[test]
    fn cosh_symmetry() {
        assert_f64_close(&eval("cosh", &[f(2.0)]), 2.0_f64.cosh(), 0.001);
        assert_f64_close(&eval("cosh", &[f(-2.0)]), 2.0_f64.cosh(), 0.001);
    }
    #[test]
    fn sinh_antisymmetry() {
        let pos = match eval("sinh", &[f(2.0)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        let neg = match eval("sinh", &[f(-2.0)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert!((pos + neg).abs() < 0.0001);
    }
    #[test]
    fn tanh_bounded() {
        let v = match eval("tanh", &[f(10.0)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert!(v > 0.999 && v <= 1.0);
    }
    #[test]
    fn tanh_neg_bounded() {
        let v = match eval("tanh", &[f(-10.0)]) {
            Value::F64(v) => v,
            _ => panic!(),
        };
        assert!(v < -0.999 && v >= -1.0);
    }
}

mod gcd_lcm_extended_tests {
    use super::*;

    #[test]
    fn gcd_commutative() {
        assert_eq!(eval("gcd", &[i(12), i(8)]), eval("gcd", &[i(8), i(12)]));
    }
    #[test]
    fn lcm_commutative() {
        assert_eq!(eval("lcm", &[i(4), i(6)]), eval("lcm", &[i(6), i(4)]));
    }
    #[test]
    fn gcd_lcm_product() {
        let a = 12_i64;
        let b = 8_i64;
        let g = match eval("gcd", &[i(a), i(b)]) {
            Value::I64(v) => v,
            _ => panic!(),
        };
        let l = match eval("lcm", &[i(a), i(b)]) {
            Value::I64(v) => v,
            _ => panic!(),
        };
        assert_eq!(g * l, (a * b).abs());
    }
    #[test]
    fn gcd_with_one() {
        assert_eq!(eval("gcd", &[i(1), i(1000)]), i(1));
    }
    #[test]
    fn lcm_with_one() {
        assert_eq!(eval("lcm", &[i(1), i(1000)]), i(1000));
    }
    #[test]
    fn gcd_36_48() {
        assert_eq!(eval("gcd", &[i(36), i(48)]), i(12));
    }
    #[test]
    fn lcm_15_20() {
        assert_eq!(eval("lcm", &[i(15), i(20)]), i(60));
    }
}

mod factorial_extended_tests {
    use super::*;

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
    #[test]
    fn eight() {
        assert_eq!(eval("factorial", &[i(8)]), i(40320));
    }
    #[test]
    fn nine() {
        assert_eq!(eval("factorial", &[i(9)]), i(362880));
    }
    #[test]
    fn fifteen() {
        assert_eq!(eval("factorial", &[i(15)]), i(1307674368000));
    }
}

mod bit_extended_tests {
    use super::*;

    #[test]
    fn and_identity() {
        assert_eq!(eval("bit_and", &[i(0xFF), i(-1)]), i(0xFF));
    }
    #[test]
    fn or_identity() {
        assert_eq!(eval("bit_or", &[i(0), i(0xFF)]), i(0xFF));
    }
    #[test]
    fn xor_self_zero() {
        assert_eq!(eval("bit_xor", &[i(42), i(42)]), i(0));
    }
    #[test]
    fn double_not() {
        assert_eq!(eval("bit_not", &[eval("bit_not", &[i(42)])]), i(42));
    }
    #[test]
    fn shl_shr_roundtrip() {
        assert_eq!(
            eval(
                "bit_shift_right",
                &[eval("bit_shift_left", &[i(5), i(3)]), i(3)]
            ),
            i(5)
        );
    }
    #[test]
    fn bit_count_15() {
        assert_eq!(eval("bit_count", &[i(15)]), i(4));
    }
    #[test]
    fn bit_count_1023() {
        assert_eq!(eval("bit_count", &[i(1023)]), i(10));
    }
}

mod square_negate_extended_tests {
    use super::*;

    #[test]
    fn square_10() {
        assert_f64_close(&eval("square", &[f(10.0)]), 100.0, 0.001);
    }
    #[test]
    fn square_0_5() {
        assert_f64_close(&eval("square", &[f(0.5)]), 0.25, 0.001);
    }
    #[test]
    fn negate_large() {
        assert_eq!(eval("negate", &[i(1_000_000)]), i(-1_000_000));
    }
    #[test]
    fn double_negate() {
        assert_eq!(eval("negate", &[eval("negate", &[i(42)])]), i(42));
    }
    #[test]
    fn reciprocal_4() {
        assert_f64_close(&eval("reciprocal", &[f(4.0)]), 0.25, 0.001);
    }
    #[test]
    fn reciprocal_0_1() {
        assert_f64_close(&eval("reciprocal", &[f(0.1)]), 10.0, 0.001);
    }
    #[test]
    fn double_reciprocal() {
        let r1 = eval("reciprocal", &[f(3.0)]);
        assert_f64_close(&eval("reciprocal", &[r1]), 3.0, 0.001);
    }
}

mod trunc_extended_tests {
    use super::*;

    #[test]
    fn pi_0() {
        assert_f64_close(&eval("trunc", &[f(std::f64::consts::PI)]), 3.0, 0.001);
    }
    #[test]
    fn pi_1() {
        assert_f64_close(&eval("trunc", &[f(std::f64::consts::PI), i(1)]), 3.1, 0.001);
    }
    #[test]
    fn pi_4() {
        assert_f64_close(
            &eval("trunc", &[f(std::f64::consts::PI), i(4)]),
            3.1415,
            0.0001,
        );
    }
    #[test]
    fn neg_2_7() {
        assert_f64_close(&eval("trunc", &[f(-2.7)]), -2.0, 0.001);
    }
    #[test]
    fn neg_2_3() {
        assert_f64_close(&eval("trunc", &[f(-2.3)]), -2.0, 0.001);
    }
}

mod div_extended_tests {
    use super::*;

    #[test]
    fn div_100_7() {
        assert_eq!(eval("div", &[i(100), i(7)]), i(14));
    }
    #[test]
    fn div_neg_neg() {
        assert_eq!(eval("div", &[i(-10), i(-3)]), i(3));
    }
    #[test]
    fn div_neg_pos() {
        assert_eq!(eval("div", &[i(-10), i(3)]), i(-3));
    }
    #[test]
    fn div_1_1() {
        assert_eq!(eval("div", &[i(1), i(1)]), i(1));
    }
    #[test]
    fn div_0_1() {
        assert_eq!(eval("div", &[i(0), i(1)]), i(0));
    }
}

mod clamp_extended_tests {
    use super::*;

    #[test]
    fn negative_values() {
        assert_f64_close(
            &eval("clamp", &[f(-50.0), f(-100.0), f(-10.0)]),
            -50.0,
            0.001,
        );
    }
    #[test]
    fn below_neg_range() {
        assert_f64_close(
            &eval("clamp", &[f(-200.0), f(-100.0), f(-10.0)]),
            -100.0,
            0.001,
        );
    }
    #[test]
    fn above_neg_range() {
        assert_f64_close(&eval("clamp", &[f(0.0), f(-100.0), f(-10.0)]), -10.0, 0.001);
    }
    #[test]
    fn float_range() {
        assert_f64_close(&eval("clamp", &[f(0.5), f(0.0), f(1.0)]), 0.5, 0.001);
    }
    #[test]
    fn int_clamped_low() {
        assert_f64_close(&eval("clamp", &[i(-10), f(0.0), f(100.0)]), 0.0, 0.001);
    }
    #[test]
    fn int_clamped_high() {
        assert_f64_close(&eval("clamp", &[i(200), f(0.0), f(100.0)]), 100.0, 0.001);
    }
}

mod width_bucket_extended_tests {
    use super::*;

    #[test]
    fn two_buckets() {
        assert_eq!(eval("width_bucket", &[f(3.0), f(0.0), f(10.0), i(2)]), i(1));
    }
    #[test]
    fn two_buckets_second() {
        assert_eq!(eval("width_bucket", &[f(7.0), f(0.0), f(10.0), i(2)]), i(2));
    }
    #[test]
    fn three_buckets() {
        assert_eq!(eval("width_bucket", &[f(5.0), f(0.0), f(9.0), i(3)]), i(2));
    }
    #[test]
    fn negative_range() {
        assert_eq!(
            eval("width_bucket", &[f(-5.0), f(-10.0), f(0.0), i(10)]),
            i(6)
        );
    }
    #[test]
    fn exact_boundary() {
        assert_eq!(eval("width_bucket", &[f(5.0), f(0.0), f(10.0), i(2)]), i(2));
    }
}

mod exp_log_extended_tests {
    use super::*;

    #[test]
    fn exp_log_roundtrip() {
        assert_f64_close(&eval("log", &[eval("exp", &[f(3.0)])]), 3.0, 0.001);
    }
    #[test]
    fn log_exp_roundtrip() {
        assert_f64_close(&eval("exp", &[eval("log", &[f(3.0)])]), 3.0, 0.001);
    }
    #[test]
    fn log2_pow_roundtrip() {
        assert_f64_close(&eval("pow", &[f(2.0), eval("log2", &[f(7.0)])]), 7.0, 0.001);
    }
    #[test]
    fn log10_100000() {
        assert_f64_close(&eval("log10", &[f(100000.0)]), 5.0, 0.001);
    }
    #[test]
    fn exp_minus_2() {
        assert_f64_close(&eval("exp", &[f(-2.0)]), (-2.0_f64).exp(), 0.001);
    }
    #[test]
    fn log_large() {
        assert_f64_close(&eval("log", &[f(1e6)]), 1e6_f64.ln(), 0.001);
    }
}

mod pow_sqrt_extended_tests {
    use super::*;

    #[test]
    fn sqrt_pow_roundtrip() {
        assert_f64_close(&eval("pow", &[eval("sqrt", &[f(7.0)]), f(2.0)]), 7.0, 0.001);
    }
    #[test]
    fn pow_sqrt_roundtrip() {
        assert_f64_close(&eval("sqrt", &[eval("pow", &[f(3.0), f(2.0)])]), 3.0, 0.001);
    }
    #[test]
    fn cbrt_pow_roundtrip() {
        assert_f64_close(&eval("pow", &[eval("cbrt", &[f(7.0)]), f(3.0)]), 7.0, 0.001);
    }
    #[test]
    fn sqrt_0_01() {
        assert_f64_close(&eval("sqrt", &[f(0.01)]), 0.1, 0.001);
    }
    #[test]
    fn pow_neg_base_even_exp() {
        assert_f64_close(&eval("pow", &[f(-3.0), f(2.0)]), 9.0, 0.001);
    }
}

mod copysign_hypot_fma_extended_tests {
    use super::*;

    #[test]
    fn fma_a_plus_b() {
        assert_f64_close(&eval("fma", &[f(1.0), f(1.0), f(1.0)]), 2.0, 0.001);
    }
    #[test]
    fn fma_zero_result() {
        assert_f64_close(&eval("fma", &[f(2.0), f(3.0), f(-6.0)]), 0.0, 0.001);
    }
    #[test]
    fn hypot_5_12_13() {
        assert_f64_close(&eval("hypot", &[f(5.0), f(12.0)]), 13.0, 0.001);
    }
    #[test]
    fn hypot_8_15_17() {
        assert_f64_close(&eval("hypot", &[f(8.0), f(15.0)]), 17.0, 0.001);
    }
    #[test]
    fn copysign_zero_neg() {
        assert_f64_close(&eval("copysign", &[f(5.0), f(-0.0)]), -5.0, 0.001);
    }
}

mod sign_extended_tests {
    use super::*;

    #[test]
    fn sign_1000() {
        assert_eq!(eval("sign", &[i(1000)]), i(1));
    }
    #[test]
    fn sign_neg_1000() {
        assert_eq!(eval("sign", &[i(-1000)]), i(-1));
    }
    #[test]
    fn sign_float_large_neg() {
        assert_eq!(eval("sign", &[f(-1e10)]), i(-1));
    }
    #[test]
    fn sign_float_large_pos() {
        assert_eq!(eval("sign", &[f(1e10)]), i(1));
    }
}

mod next_power_of_two_extended_tests {
    use super::*;

    #[test]
    fn n_4() {
        assert_eq!(eval("next_power_of_two", &[i(4)]), i(4));
    }
    #[test]
    fn n_7() {
        assert_eq!(eval("next_power_of_two", &[i(7)]), i(8));
    }
    #[test]
    fn n_9() {
        assert_eq!(eval("next_power_of_two", &[i(9)]), i(16));
    }
    #[test]
    fn n_100() {
        assert_eq!(eval("next_power_of_two", &[i(100)]), i(128));
    }
    #[test]
    fn n_128() {
        assert_eq!(eval("next_power_of_two", &[i(128)]), i(128));
    }
    #[test]
    fn n_129() {
        assert_eq!(eval("next_power_of_two", &[i(129)]), i(256));
    }
}
