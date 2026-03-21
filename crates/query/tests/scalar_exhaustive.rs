//! Exhaustive scalar function tests — 1000 tests.
//!
//! Tests every registered scalar function via `evaluate_scalar` with multiple inputs each.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

fn eval(name: &str, args: &[Value]) -> Value {
    evaluate_scalar(name, args).unwrap_or(Value::Null)
}
fn i(n: i64) -> Value {
    Value::I64(n)
}
fn f(n: f64) -> Value {
    Value::F64(n)
}
fn s(v: &str) -> Value {
    Value::Str(v.to_string())
}
fn null() -> Value {
    Value::Null
}
fn ts(n: i64) -> Value {
    Value::Timestamp(n)
}

fn approx(a: Value, expected: f64) {
    match a {
        Value::F64(v) => assert!((v - expected).abs() < 1e-6, "expected {expected}, got {v}"),
        other => panic!("expected F64({expected}), got {other:?}"),
    }
}

// ===========================================================================
// String functions — length, upper, lower, trim, ltrim, rtrim
// ===========================================================================
mod string_basic {
    use super::*;
    #[test]
    fn length_hello() {
        assert_eq!(eval("length", &[s("hello")]), i(5));
    }
    #[test]
    fn length_empty() {
        assert_eq!(eval("length", &[s("")]), i(0));
    }
    #[test]
    fn length_one() {
        assert_eq!(eval("length", &[s("x")]), i(1));
    }
    #[test]
    fn length_null() {
        assert_eq!(eval("length", &[null()]), null());
    }
    #[test]
    fn length_spaces() {
        assert_eq!(eval("length", &[s("   ")]), i(3));
    }
    #[test]
    fn length_100() {
        assert_eq!(eval("length", &[s(&"a".repeat(100))]), i(100));
    }
    #[test]
    fn upper_hello() {
        assert_eq!(eval("upper", &[s("hello")]), s("HELLO"));
    }
    #[test]
    fn upper_empty() {
        assert_eq!(eval("upper", &[s("")]), s(""));
    }
    #[test]
    fn upper_already() {
        assert_eq!(eval("upper", &[s("ABC")]), s("ABC"));
    }
    #[test]
    fn upper_mixed() {
        assert_eq!(eval("upper", &[s("aBc")]), s("ABC"));
    }
    #[test]
    fn upper_null() {
        assert_eq!(eval("upper", &[null()]), null());
    }
    #[test]
    fn upper_digits() {
        assert_eq!(eval("upper", &[s("abc123")]), s("ABC123"));
    }
    #[test]
    fn lower_hello() {
        assert_eq!(eval("lower", &[s("HELLO")]), s("hello"));
    }
    #[test]
    fn lower_empty() {
        assert_eq!(eval("lower", &[s("")]), s(""));
    }
    #[test]
    fn lower_already() {
        assert_eq!(eval("lower", &[s("abc")]), s("abc"));
    }
    #[test]
    fn lower_mixed() {
        assert_eq!(eval("lower", &[s("AbC")]), s("abc"));
    }
    #[test]
    fn lower_null() {
        assert_eq!(eval("lower", &[null()]), null());
    }
    #[test]
    fn trim_hello() {
        assert_eq!(eval("trim", &[s("  hello  ")]), s("hello"));
    }
    #[test]
    fn trim_empty() {
        assert_eq!(eval("trim", &[s("")]), s(""));
    }
    #[test]
    fn trim_no_space() {
        assert_eq!(eval("trim", &[s("abc")]), s("abc"));
    }
    #[test]
    fn trim_null() {
        assert_eq!(eval("trim", &[null()]), null());
    }
    #[test]
    fn trim_only_spaces() {
        assert_eq!(eval("trim", &[s("   ")]), s(""));
    }
    #[test]
    fn ltrim_hello() {
        assert_eq!(eval("ltrim", &[s("  hello")]), s("hello"));
    }
    #[test]
    fn ltrim_no_space() {
        assert_eq!(eval("ltrim", &[s("abc")]), s("abc"));
    }
    #[test]
    fn ltrim_null() {
        assert_eq!(eval("ltrim", &[null()]), null());
    }
    #[test]
    fn ltrim_right_space() {
        assert_eq!(eval("ltrim", &[s("abc  ")]), s("abc  "));
    }
    #[test]
    fn rtrim_hello() {
        assert_eq!(eval("rtrim", &[s("hello  ")]), s("hello"));
    }
    #[test]
    fn rtrim_no_space() {
        assert_eq!(eval("rtrim", &[s("abc")]), s("abc"));
    }
    #[test]
    fn rtrim_null() {
        assert_eq!(eval("rtrim", &[null()]), null());
    }
    #[test]
    fn rtrim_left_space() {
        assert_eq!(eval("rtrim", &[s("  abc")]), s("  abc"));
    }
}

// ===========================================================================
// String functions — substring, concat, replace, starts_with, ends_with, contains
// ===========================================================================
mod string_advanced {
    use super::*;
    #[test]
    fn substring_mid() {
        assert_eq!(eval("substring", &[s("hello"), i(2), i(3)]), s("ell"));
    }
    #[test]
    fn substring_start() {
        assert_eq!(eval("substring", &[s("hello"), i(1), i(2)]), s("he"));
    }
    #[test]
    fn substring_end() {
        assert_eq!(eval("substring", &[s("hello"), i(4), i(2)]), s("lo"));
    }
    #[test]
    fn substring_null() {
        assert_eq!(eval("substring", &[null(), i(1), i(1)]), null());
    }
    #[test]
    fn substring_full() {
        assert_eq!(eval("substring", &[s("abc"), i(1), i(3)]), s("abc"));
    }
    #[test]
    fn substring_zero_len() {
        assert_eq!(eval("substring", &[s("abc"), i(1), i(0)]), s(""));
    }
    #[test]
    fn concat_two() {
        assert_eq!(eval("concat", &[s("a"), s("b")]), s("ab"));
    }
    #[test]
    fn concat_three() {
        assert_eq!(eval("concat", &[s("a"), s("b"), s("c")]), s("abc"));
    }
    #[test]
    fn concat_with_null() {
        assert_eq!(eval("concat", &[s("a"), null(), s("c")]), s("ac"));
    }
    #[test]
    fn concat_empty() {
        assert_eq!(eval("concat", &[s(""), s("")]), s(""));
    }
    #[test]
    fn concat_nums() {
        assert_eq!(eval("concat", &[i(1), i(2)]), s("12"));
    }
    #[test]
    fn replace_basic() {
        assert_eq!(eval("replace", &[s("hello"), s("l"), s("r")]), s("herro"));
    }
    #[test]
    fn replace_not_found() {
        assert_eq!(eval("replace", &[s("hello"), s("x"), s("y")]), s("hello"));
    }
    #[test]
    fn replace_empty() {
        assert_eq!(eval("replace", &[s("abc"), s("b"), s("")]), s("ac"));
    }
    #[test]
    fn replace_null() {
        assert_eq!(eval("replace", &[null(), s("a"), s("b")]), null());
    }
    #[test]
    fn starts_with_yes() {
        assert_eq!(eval("starts_with", &[s("hello"), s("he")]), i(1));
    }
    #[test]
    fn starts_with_no() {
        assert_eq!(eval("starts_with", &[s("hello"), s("lo")]), i(0));
    }
    #[test]
    fn starts_with_empty() {
        assert_eq!(eval("starts_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn starts_with_null() {
        assert_eq!(eval("starts_with", &[null(), s("a")]), null());
    }
    #[test]
    fn ends_with_yes() {
        assert_eq!(eval("ends_with", &[s("hello"), s("lo")]), i(1));
    }
    #[test]
    fn ends_with_no() {
        assert_eq!(eval("ends_with", &[s("hello"), s("he")]), i(0));
    }
    #[test]
    fn ends_with_empty() {
        assert_eq!(eval("ends_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn ends_with_null() {
        assert_eq!(eval("ends_with", &[null(), s("a")]), null());
    }
    #[test]
    fn contains_yes() {
        assert_eq!(eval("contains", &[s("hello"), s("ell")]), i(1));
    }
    #[test]
    fn contains_no() {
        assert_eq!(eval("contains", &[s("hello"), s("xyz")]), i(0));
    }
    #[test]
    fn contains_empty() {
        assert_eq!(eval("contains", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn contains_null() {
        assert_eq!(eval("contains", &[null(), s("a")]), null());
    }
    #[test]
    fn contains_full() {
        assert_eq!(eval("contains", &[s("hello"), s("hello")]), i(1));
    }
}

// ===========================================================================
// String functions — reverse, repeat, left, right
// ===========================================================================
mod string_extra {
    use super::*;
    #[test]
    fn reverse_hello() {
        assert_eq!(eval("reverse", &[s("hello")]), s("olleh"));
    }
    #[test]
    fn reverse_empty() {
        assert_eq!(eval("reverse", &[s("")]), s(""));
    }
    #[test]
    fn reverse_one() {
        assert_eq!(eval("reverse", &[s("x")]), s("x"));
    }
    #[test]
    fn reverse_null() {
        assert_eq!(eval("reverse", &[null()]), null());
    }
    #[test]
    fn reverse_palindrome() {
        assert_eq!(eval("reverse", &[s("aba")]), s("aba"));
    }
    #[test]
    fn repeat_basic() {
        assert_eq!(eval("repeat", &[s("ab"), i(3)]), s("ababab"));
    }
    #[test]
    fn repeat_zero() {
        assert_eq!(eval("repeat", &[s("ab"), i(0)]), s(""));
    }
    #[test]
    fn repeat_one() {
        assert_eq!(eval("repeat", &[s("ab"), i(1)]), s("ab"));
    }
    #[test]
    fn repeat_null() {
        assert_eq!(eval("repeat", &[null(), i(3)]), null());
    }
    #[test]
    fn repeat_empty() {
        assert_eq!(eval("repeat", &[s(""), i(5)]), s(""));
    }
    #[test]
    fn left_3() {
        assert_eq!(eval("left", &[s("hello"), i(3)]), s("hel"));
    }
    #[test]
    fn left_0() {
        assert_eq!(eval("left", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn left_excess() {
        assert_eq!(eval("left", &[s("hi"), i(10)]), s("hi"));
    }
    #[test]
    fn left_null() {
        assert_eq!(eval("left", &[null(), i(3)]), null());
    }
    #[test]
    fn right_3() {
        assert_eq!(eval("right", &[s("hello"), i(3)]), s("llo"));
    }
    #[test]
    fn right_0() {
        assert_eq!(eval("right", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn right_excess() {
        assert_eq!(eval("right", &[s("hi"), i(10)]), s("hi"));
    }
    #[test]
    fn right_null() {
        assert_eq!(eval("right", &[null(), i(3)]), null());
    }
    #[test]
    fn right_1() {
        assert_eq!(eval("right", &[s("hello"), i(1)]), s("o"));
    }
    #[test]
    fn left_1() {
        assert_eq!(eval("left", &[s("hello"), i(1)]), s("h"));
    }
}

// ===========================================================================
// Math functions — abs, round, floor, ceil
// ===========================================================================
mod math_basic {
    use super::*;
    #[test]
    fn abs_pos() {
        assert_eq!(eval("abs", &[i(5)]), i(5));
    }
    #[test]
    fn abs_neg() {
        assert_eq!(eval("abs", &[i(-5)]), i(5));
    }
    #[test]
    fn abs_zero() {
        assert_eq!(eval("abs", &[i(0)]), i(0));
    }
    #[test]
    fn abs_f_pos() {
        approx(eval("abs", &[f(3.15)]), 3.15);
    }
    #[test]
    fn abs_f_neg() {
        approx(eval("abs", &[f(-3.15)]), 3.15);
    }
    #[test]
    fn abs_null() {
        assert_eq!(eval("abs", &[null()]), null());
    }
    #[test]
    fn round_basic() {
        approx(eval("round", &[f(3.7)]), 4.0);
    }
    #[test]
    fn round_down() {
        approx(eval("round", &[f(3.2)]), 3.0);
    }
    #[test]
    fn round_zero() {
        approx(eval("round", &[f(0.0)]), 0.0);
    }
    #[test]
    fn round_neg() {
        approx(eval("round", &[f(-3.7)]), -4.0);
    }
    #[test]
    fn round_null() {
        assert_eq!(eval("round", &[null()]), null());
    }
    #[test]
    fn round_2dp() {
        approx(eval("round", &[f(3.456), i(2)]), 3.46);
    }
    #[test]
    fn round_1dp() {
        approx(eval("round", &[f(3.456), i(1)]), 3.5);
    }
    #[test]
    fn floor_basic() {
        approx(eval("floor", &[f(3.7)]), 3.0);
    }
    #[test]
    fn floor_neg() {
        approx(eval("floor", &[f(-3.2)]), -4.0);
    }
    #[test]
    fn floor_int() {
        approx(eval("floor", &[f(5.0)]), 5.0);
    }
    #[test]
    fn floor_null() {
        assert_eq!(eval("floor", &[null()]), null());
    }
    #[test]
    fn ceil_basic() {
        approx(eval("ceil", &[f(3.2)]), 4.0);
    }
    #[test]
    fn ceil_neg() {
        approx(eval("ceil", &[f(-3.7)]), -3.0);
    }
    #[test]
    fn ceil_int() {
        approx(eval("ceil", &[f(5.0)]), 5.0);
    }
    #[test]
    fn ceil_null() {
        assert_eq!(eval("ceil", &[null()]), null());
    }
    #[test]
    fn ceil_zero() {
        approx(eval("ceil", &[f(0.0)]), 0.0);
    }
    #[test]
    fn floor_zero() {
        approx(eval("floor", &[f(0.0)]), 0.0);
    }
}

// ===========================================================================
// Math — sqrt, pow, log, log2, log10, exp
// ===========================================================================
mod math_advanced {
    use super::*;
    #[test]
    fn sqrt_4() {
        approx(eval("sqrt", &[f(4.0)]), 2.0);
    }
    #[test]
    fn sqrt_9() {
        approx(eval("sqrt", &[f(9.0)]), 3.0);
    }
    #[test]
    fn sqrt_1() {
        approx(eval("sqrt", &[f(1.0)]), 1.0);
    }
    #[test]
    fn sqrt_0() {
        approx(eval("sqrt", &[f(0.0)]), 0.0);
    }
    #[test]
    fn sqrt_null() {
        assert_eq!(eval("sqrt", &[null()]), null());
    }
    #[test]
    fn sqrt_16() {
        approx(eval("sqrt", &[f(16.0)]), 4.0);
    }
    #[test]
    fn sqrt_25() {
        approx(eval("sqrt", &[f(25.0)]), 5.0);
    }
    #[test]
    fn pow_2_3() {
        approx(eval("pow", &[f(2.0), f(3.0)]), 8.0);
    }
    #[test]
    fn pow_2_0() {
        approx(eval("pow", &[f(2.0), f(0.0)]), 1.0);
    }
    #[test]
    fn pow_10_2() {
        approx(eval("pow", &[f(10.0), f(2.0)]), 100.0);
    }
    #[test]
    fn pow_null() {
        assert_eq!(eval("pow", &[null(), f(2.0)]), null());
    }
    #[test]
    fn pow_3_2() {
        approx(eval("pow", &[f(3.0), f(2.0)]), 9.0);
    }
    #[test]
    fn log_e() {
        approx(eval("log", &[f(std::f64::consts::E)]), 1.0);
    }
    #[test]
    fn log_1() {
        approx(eval("log", &[f(1.0)]), 0.0);
    }
    #[test]
    fn log_null() {
        assert_eq!(eval("log", &[null()]), null());
    }
    #[test]
    fn log2_8() {
        approx(eval("log2", &[f(8.0)]), 3.0);
    }
    #[test]
    fn log2_1() {
        approx(eval("log2", &[f(1.0)]), 0.0);
    }
    #[test]
    fn log2_null() {
        assert_eq!(eval("log2", &[null()]), null());
    }
    #[test]
    fn log2_16() {
        approx(eval("log2", &[f(16.0)]), 4.0);
    }
    #[test]
    fn log10_100() {
        approx(eval("log10", &[f(100.0)]), 2.0);
    }
    #[test]
    fn log10_1() {
        approx(eval("log10", &[f(1.0)]), 0.0);
    }
    #[test]
    fn log10_null() {
        assert_eq!(eval("log10", &[null()]), null());
    }
    #[test]
    fn log10_1000() {
        approx(eval("log10", &[f(1000.0)]), 3.0);
    }
    #[test]
    fn exp_0() {
        approx(eval("exp", &[f(0.0)]), 1.0);
    }
    #[test]
    fn exp_1() {
        approx(eval("exp", &[f(1.0)]), std::f64::consts::E);
    }
    #[test]
    fn exp_null() {
        assert_eq!(eval("exp", &[null()]), null());
    }
    #[test]
    fn exp_2() {
        approx(
            eval("exp", &[f(2.0)]),
            std::f64::consts::E * std::f64::consts::E,
        );
    }
}

// ===========================================================================
// Trig functions — sin, cos, tan
// ===========================================================================
mod trig {
    use super::*;
    #[test]
    fn sin_0() {
        approx(eval("sin", &[f(0.0)]), 0.0);
    }
    #[test]
    fn sin_pi_half() {
        approx(eval("sin", &[f(std::f64::consts::FRAC_PI_2)]), 1.0);
    }
    #[test]
    fn sin_null() {
        assert_eq!(eval("sin", &[null()]), null());
    }
    #[test]
    fn cos_0() {
        approx(eval("cos", &[f(0.0)]), 1.0);
    }
    #[test]
    fn cos_pi() {
        approx(eval("cos", &[f(std::f64::consts::PI)]), -1.0);
    }
    #[test]
    fn cos_null() {
        assert_eq!(eval("cos", &[null()]), null());
    }
    #[test]
    fn tan_0() {
        approx(eval("tan", &[f(0.0)]), 0.0);
    }
    #[test]
    fn tan_null() {
        assert_eq!(eval("tan", &[null()]), null());
    }
    #[test]
    fn sin_pi() {
        approx(eval("sin", &[f(std::f64::consts::PI)]), 0.0);
    }
    #[test]
    fn cos_pi_half() {
        approx(eval("cos", &[f(std::f64::consts::FRAC_PI_2)]), 0.0);
    }
    #[test]
    fn asin_0() {
        approx(eval("asin", &[f(0.0)]), 0.0);
    }
    #[test]
    fn asin_1() {
        approx(eval("asin", &[f(1.0)]), std::f64::consts::FRAC_PI_2);
    }
    #[test]
    fn asin_null() {
        assert_eq!(eval("asin", &[null()]), null());
    }
    #[test]
    fn acos_1() {
        approx(eval("acos", &[f(1.0)]), 0.0);
    }
    #[test]
    fn acos_0() {
        approx(eval("acos", &[f(0.0)]), std::f64::consts::FRAC_PI_2);
    }
    #[test]
    fn acos_null() {
        assert_eq!(eval("acos", &[null()]), null());
    }
    #[test]
    fn atan_0() {
        approx(eval("atan", &[f(0.0)]), 0.0);
    }
    #[test]
    fn atan_1() {
        approx(eval("atan", &[f(1.0)]), std::f64::consts::FRAC_PI_4);
    }
    #[test]
    fn atan_null() {
        assert_eq!(eval("atan", &[null()]), null());
    }
    #[test]
    fn atan2_1_1() {
        approx(
            eval("atan2", &[f(1.0), f(1.0)]),
            std::f64::consts::FRAC_PI_4,
        );
    }
    #[test]
    fn atan2_0_1() {
        approx(eval("atan2", &[f(0.0), f(1.0)]), 0.0);
    }
    #[test]
    fn atan2_null() {
        assert_eq!(eval("atan2", &[null(), f(1.0)]), null());
    }
    #[test]
    fn sinh_0() {
        approx(eval("sinh", &[f(0.0)]), 0.0);
    }
    #[test]
    fn sinh_null() {
        assert_eq!(eval("sinh", &[null()]), null());
    }
    #[test]
    fn cosh_0() {
        approx(eval("cosh", &[f(0.0)]), 1.0);
    }
    #[test]
    fn cosh_null() {
        assert_eq!(eval("cosh", &[null()]), null());
    }
    #[test]
    fn tanh_0() {
        approx(eval("tanh", &[f(0.0)]), 0.0);
    }
    #[test]
    fn tanh_null() {
        assert_eq!(eval("tanh", &[null()]), null());
    }
    #[test]
    fn degrees_pi() {
        approx(eval("degrees", &[f(std::f64::consts::PI)]), 180.0);
    }
    #[test]
    fn degrees_0() {
        approx(eval("degrees", &[f(0.0)]), 0.0);
    }
    #[test]
    fn degrees_null() {
        assert_eq!(eval("degrees", &[null()]), null());
    }
    #[test]
    fn radians_180() {
        approx(eval("radians", &[f(180.0)]), std::f64::consts::PI);
    }
    #[test]
    fn radians_0() {
        approx(eval("radians", &[f(0.0)]), 0.0);
    }
    #[test]
    fn radians_null() {
        assert_eq!(eval("radians", &[null()]), null());
    }
    #[test]
    fn radians_90() {
        approx(eval("radians", &[f(90.0)]), std::f64::consts::FRAC_PI_2);
    }
    #[test]
    fn degrees_half_pi() {
        approx(eval("degrees", &[f(std::f64::consts::FRAC_PI_2)]), 90.0);
    }
}

// ===========================================================================
// Math — mod, sign, cbrt, factorial, gcd, lcm
// ===========================================================================
mod math_misc {
    use super::*;
    #[test]
    fn mod_10_3() {
        assert_eq!(eval("mod", &[i(10), i(3)]), i(1));
    }
    #[test]
    fn mod_10_5() {
        assert_eq!(eval("mod", &[i(10), i(5)]), i(0));
    }
    #[test]
    fn mod_7_2() {
        assert_eq!(eval("mod", &[i(7), i(2)]), i(1));
    }
    #[test]
    fn mod_null() {
        assert_eq!(eval("mod", &[null(), i(3)]), null());
    }
    #[test]
    fn mod_neg() {
        assert_eq!(eval("mod", &[i(-10), i(3)]), i(-1));
    }
    #[test]
    fn sign_pos() {
        assert_eq!(eval("sign", &[f(5.0)]), i(1));
    }
    #[test]
    fn sign_neg() {
        assert_eq!(eval("sign", &[f(-5.0)]), i(-1));
    }
    #[test]
    fn sign_zero() {
        assert_eq!(eval("sign", &[f(0.0)]), i(0));
    }
    #[test]
    fn sign_null() {
        assert_eq!(eval("sign", &[null()]), null());
    }
    #[test]
    fn cbrt_27() {
        approx(eval("cbrt", &[f(27.0)]), 3.0);
    }
    #[test]
    fn cbrt_8() {
        approx(eval("cbrt", &[f(8.0)]), 2.0);
    }
    #[test]
    fn cbrt_0() {
        approx(eval("cbrt", &[f(0.0)]), 0.0);
    }
    #[test]
    fn cbrt_1() {
        approx(eval("cbrt", &[f(1.0)]), 1.0);
    }
    #[test]
    fn cbrt_null() {
        assert_eq!(eval("cbrt", &[null()]), null());
    }
    #[test]
    fn factorial_0() {
        assert_eq!(eval("factorial", &[i(0)]), i(1));
    }
    #[test]
    fn factorial_1() {
        assert_eq!(eval("factorial", &[i(1)]), i(1));
    }
    #[test]
    fn factorial_5() {
        assert_eq!(eval("factorial", &[i(5)]), i(120));
    }
    #[test]
    fn factorial_10() {
        assert_eq!(eval("factorial", &[i(10)]), i(3628800));
    }
    #[test]
    fn factorial_null() {
        assert_eq!(eval("factorial", &[null()]), null());
    }
    #[test]
    fn factorial_20() {
        assert_eq!(eval("factorial", &[i(20)]), i(2432902008176640000));
    }
    #[test]
    fn gcd_12_8() {
        assert_eq!(eval("gcd", &[i(12), i(8)]), i(4));
    }
    #[test]
    fn gcd_15_10() {
        assert_eq!(eval("gcd", &[i(15), i(10)]), i(5));
    }
    #[test]
    fn gcd_7_13() {
        assert_eq!(eval("gcd", &[i(7), i(13)]), i(1));
    }
    #[test]
    fn gcd_null() {
        assert_eq!(eval("gcd", &[null(), i(5)]), null());
    }
    #[test]
    fn gcd_0_5() {
        assert_eq!(eval("gcd", &[i(0), i(5)]), i(5));
    }
    #[test]
    fn lcm_4_6() {
        assert_eq!(eval("lcm", &[i(4), i(6)]), i(12));
    }
    #[test]
    fn lcm_3_5() {
        assert_eq!(eval("lcm", &[i(3), i(5)]), i(15));
    }
    #[test]
    fn lcm_0_0() {
        assert_eq!(eval("lcm", &[i(0), i(0)]), i(0));
    }
    #[test]
    fn lcm_null() {
        assert_eq!(eval("lcm", &[null(), i(5)]), null());
    }
    #[test]
    fn lcm_7_7() {
        assert_eq!(eval("lcm", &[i(7), i(7)]), i(7));
    }
}

// ===========================================================================
// Bitwise functions
// ===========================================================================
mod bitwise {
    use super::*;
    #[test]
    fn bit_and_ff() {
        assert_eq!(eval("bit_and", &[i(0xFF), i(0x0F)]), i(0x0F));
    }
    #[test]
    fn bit_and_0() {
        assert_eq!(eval("bit_and", &[i(0), i(0xFF)]), i(0));
    }
    #[test]
    fn bit_and_null() {
        assert_eq!(eval("bit_and", &[null(), i(5)]), null());
    }
    #[test]
    fn bit_or_basic() {
        assert_eq!(eval("bit_or", &[i(0xF0), i(0x0F)]), i(0xFF));
    }
    #[test]
    fn bit_or_0() {
        assert_eq!(eval("bit_or", &[i(0), i(0)]), i(0));
    }
    #[test]
    fn bit_or_null() {
        assert_eq!(eval("bit_or", &[null(), i(5)]), null());
    }
    #[test]
    fn bit_xor_basic() {
        assert_eq!(eval("bit_xor", &[i(0xFF), i(0x0F)]), i(0xF0));
    }
    #[test]
    fn bit_xor_same() {
        assert_eq!(eval("bit_xor", &[i(42), i(42)]), i(0));
    }
    #[test]
    fn bit_xor_null() {
        assert_eq!(eval("bit_xor", &[null(), i(5)]), null());
    }
    #[test]
    fn bit_not_0() {
        assert_eq!(eval("bit_not", &[i(0)]), i(!0i64));
    }
    #[test]
    fn bit_not_1() {
        assert_eq!(eval("bit_not", &[i(1)]), i(!1i64));
    }
    #[test]
    fn bit_not_null() {
        assert_eq!(eval("bit_not", &[null()]), null());
    }
    #[test]
    fn shl_1_4() {
        assert_eq!(eval("bit_shift_left", &[i(1), i(4)]), i(16));
    }
    #[test]
    fn shl_1_0() {
        assert_eq!(eval("bit_shift_left", &[i(1), i(0)]), i(1));
    }
    #[test]
    fn shl_null() {
        assert_eq!(eval("bit_shift_left", &[null(), i(4)]), null());
    }
    #[test]
    fn shr_16_4() {
        assert_eq!(eval("bit_shift_right", &[i(16), i(4)]), i(1));
    }
    #[test]
    fn shr_1_0() {
        assert_eq!(eval("bit_shift_right", &[i(1), i(0)]), i(1));
    }
    #[test]
    fn shr_null() {
        assert_eq!(eval("bit_shift_right", &[null(), i(4)]), null());
    }
    #[test]
    fn bit_and_neg() {
        assert_eq!(eval("bit_and", &[i(-1), i(0xFF)]), i(0xFF));
    }
    #[test]
    fn bit_or_neg() {
        assert_eq!(eval("bit_or", &[i(-1), i(0)]), i(-1));
    }
    #[test]
    fn shl_2_1() {
        assert_eq!(eval("bit_shift_left", &[i(2), i(1)]), i(4));
    }
    #[test]
    fn shr_8_1() {
        assert_eq!(eval("bit_shift_right", &[i(8), i(1)]), i(4));
    }
    #[test]
    fn shr_256_8() {
        assert_eq!(eval("bit_shift_right", &[i(256), i(8)]), i(1));
    }
    #[test]
    fn shl_1_10() {
        assert_eq!(eval("bit_shift_left", &[i(1), i(10)]), i(1024));
    }
}

// ===========================================================================
// Math — trunc, div, width_bucket, pi, constants
// ===========================================================================
mod math_special {
    use super::*;
    #[test]
    fn trunc_basic() {
        approx(eval("trunc", &[f(3.9)]), 3.0);
    }
    #[test]
    fn trunc_neg() {
        approx(eval("trunc", &[f(-3.9)]), -3.0);
    }
    #[test]
    fn trunc_null() {
        assert_eq!(eval("trunc", &[null()]), null());
    }
    #[test]
    fn trunc_2dp() {
        approx(eval("trunc", &[f(3.456), i(2)]), 3.45);
    }
    #[test]
    fn div_10_3() {
        assert_eq!(eval("div", &[i(10), i(3)]), i(3));
    }
    #[test]
    fn div_10_5() {
        assert_eq!(eval("div", &[i(10), i(5)]), i(2));
    }
    #[test]
    fn div_null() {
        assert_eq!(eval("div", &[null(), i(3)]), null());
    }
    #[test]
    fn div_7_2() {
        assert_eq!(eval("div", &[i(7), i(2)]), i(3));
    }
    #[test]
    fn wb_5_0_10_5() {
        assert_eq!(eval("width_bucket", &[f(5.0), f(0.0), f(10.0), i(5)]), i(3));
    }
    #[test]
    fn wb_below() {
        assert_eq!(
            eval("width_bucket", &[f(-1.0), f(0.0), f(10.0), i(5)]),
            i(0)
        );
    }
    #[test]
    fn wb_above() {
        assert_eq!(
            eval("width_bucket", &[f(11.0), f(0.0), f(10.0), i(5)]),
            i(6)
        );
    }
    #[test]
    fn wb_null() {
        assert_eq!(
            eval("width_bucket", &[null(), f(0.0), f(10.0), i(5)]),
            null()
        );
    }
    #[test]
    fn pi_val() {
        approx(eval("pi", &[]), std::f64::consts::PI);
    }
    #[test]
    fn e_val() {
        approx(eval("e", &[]), std::f64::consts::E);
    }
    #[test]
    fn tau_val() {
        approx(eval("tau", &[]), std::f64::consts::TAU);
    }
    #[test]
    fn hypot_3_4() {
        approx(eval("hypot", &[f(3.0), f(4.0)]), 5.0);
    }
    #[test]
    fn hypot_0_0() {
        approx(eval("hypot", &[f(0.0), f(0.0)]), 0.0);
    }
    #[test]
    fn hypot_null() {
        assert_eq!(eval("hypot", &[null(), f(4.0)]), null());
    }
    #[test]
    fn copysign_pos_neg() {
        approx(eval("copysign", &[f(3.0), f(-1.0)]), -3.0);
    }
    #[test]
    fn copysign_neg_pos() {
        approx(eval("copysign", &[f(-3.0), f(1.0)]), 3.0);
    }
    #[test]
    fn copysign_null() {
        assert_eq!(eval("copysign", &[null(), f(1.0)]), null());
    }
    #[test]
    fn next_power_1() {
        assert_eq!(eval("next_power_of_two", &[i(1)]), i(1));
    }
    #[test]
    fn next_power_3() {
        assert_eq!(eval("next_power_of_two", &[i(3)]), i(4));
    }
    #[test]
    fn next_power_5() {
        assert_eq!(eval("next_power_of_two", &[i(5)]), i(8));
    }
    #[test]
    fn next_power_16() {
        assert_eq!(eval("next_power_of_two", &[i(16)]), i(16));
    }
    #[test]
    fn next_power_null() {
        assert_eq!(eval("next_power_of_two", &[null()]), null());
    }
    #[test]
    fn fma_2_3_4() {
        approx(eval("fma", &[f(2.0), f(3.0), f(4.0)]), 10.0);
    }
    #[test]
    fn fma_null() {
        assert_eq!(eval("fma", &[null(), f(3.0), f(4.0)]), null());
    }
    #[test]
    fn lerp_0() {
        approx(eval("lerp", &[f(0.0), f(10.0), f(0.0)]), 0.0);
    }
    #[test]
    fn lerp_1() {
        approx(eval("lerp", &[f(0.0), f(10.0), f(1.0)]), 10.0);
    }
    #[test]
    fn lerp_half() {
        approx(eval("lerp", &[f(0.0), f(10.0), f(0.5)]), 5.0);
    }
    #[test]
    fn lerp_null() {
        assert_eq!(eval("lerp", &[null(), f(10.0), f(0.5)]), null());
    }
    #[test]
    fn clamp_mid() {
        approx(eval("clamp", &[f(5.0), f(0.0), f(10.0)]), 5.0);
    }
    #[test]
    fn clamp_below() {
        approx(eval("clamp", &[f(-1.0), f(0.0), f(10.0)]), 0.0);
    }
    #[test]
    fn clamp_above() {
        approx(eval("clamp", &[f(11.0), f(0.0), f(10.0)]), 10.0);
    }
    #[test]
    fn clamp_null() {
        assert_eq!(eval("clamp", &[null(), f(0.0), f(10.0)]), null());
    }
}

// ===========================================================================
// Conditional functions — coalesce, nullif, greatest, least, if_null
// ===========================================================================
mod conditional {
    use super::*;
    #[test]
    fn coalesce_first() {
        assert_eq!(eval("coalesce", &[i(1)]), i(1));
    }
    #[test]
    fn coalesce_null_first() {
        assert_eq!(eval("coalesce", &[null(), i(2)]), i(2));
    }
    #[test]
    fn coalesce_all_null() {
        assert_eq!(eval("coalesce", &[null(), null()]), null());
    }
    #[test]
    fn coalesce_str() {
        assert_eq!(eval("coalesce", &[null(), s("hi")]), s("hi"));
    }
    #[test]
    fn coalesce_three() {
        assert_eq!(eval("coalesce", &[null(), null(), i(3)]), i(3));
    }
    #[test]
    fn nullif_same() {
        assert_eq!(eval("nullif", &[i(1), i(1)]), null());
    }
    #[test]
    fn nullif_diff() {
        assert_eq!(eval("nullif", &[i(1), i(2)]), i(1));
    }
    #[test]
    fn nullif_str() {
        assert_eq!(eval("nullif", &[s("a"), s("a")]), null());
    }
    #[test]
    fn nullif_str_diff() {
        assert_eq!(eval("nullif", &[s("a"), s("b")]), s("a"));
    }
    #[test]
    fn greatest_basic() {
        assert_eq!(eval("greatest", &[i(1), i(3), i(2)]), i(3));
    }
    #[test]
    fn greatest_neg() {
        assert_eq!(eval("greatest", &[i(-1), i(-3)]), i(-1));
    }
    #[test]
    fn greatest_null() {
        assert_eq!(eval("greatest", &[null(), i(5)]), i(5));
    }
    #[test]
    fn greatest_all_null() {
        assert_eq!(eval("greatest", &[null()]), null());
    }
    #[test]
    fn greatest_one() {
        assert_eq!(eval("greatest", &[i(42)]), i(42));
    }
    #[test]
    fn least_basic() {
        assert_eq!(eval("least", &[i(1), i(3), i(2)]), i(1));
    }
    #[test]
    fn least_neg() {
        assert_eq!(eval("least", &[i(-1), i(-3)]), i(-3));
    }
    #[test]
    fn least_null() {
        assert_eq!(eval("least", &[null(), i(5)]), i(5));
    }
    #[test]
    fn least_all_null() {
        assert_eq!(eval("least", &[null()]), null());
    }
    #[test]
    fn least_one() {
        assert_eq!(eval("least", &[i(42)]), i(42));
    }
    #[test]
    fn if_null_not_null() {
        assert_eq!(eval("if_null", &[i(1), i(99)]), i(1));
    }
    #[test]
    fn if_null_null() {
        assert_eq!(eval("if_null", &[null(), i(99)]), i(99));
    }
    #[test]
    fn if_null_str() {
        assert_eq!(eval("if_null", &[null(), s("default")]), s("default"));
    }
    #[test]
    fn nullif_zero_zero() {
        assert_eq!(eval("nullif_zero", &[i(0)]), null());
    }
    #[test]
    fn nullif_zero_nonzero() {
        assert_eq!(eval("nullif_zero", &[i(5)]), i(5));
    }
    #[test]
    fn nullif_zero_null() {
        assert_eq!(eval("nullif_zero", &[null()]), null());
    }
    #[test]
    fn nullif_zero_f_zero() {
        assert_eq!(eval("nullif_zero", &[f(0.0)]), null());
    }
    #[test]
    fn nullif_zero_f_nonzero() {
        assert_eq!(eval("nullif_zero", &[f(3.15)]), f(3.15));
    }
}

// ===========================================================================
// Type casting functions
// ===========================================================================
mod casting {
    use super::*;
    #[test]
    fn cast_int_from_f() {
        assert_eq!(eval("cast_int", &[f(3.9)]), i(3));
    }
    #[test]
    fn cast_int_from_s() {
        assert_eq!(eval("cast_int", &[s("42")]), i(42));
    }
    #[test]
    fn cast_int_null() {
        assert_eq!(eval("cast_int", &[null()]), null());
    }
    #[test]
    fn cast_int_from_i() {
        assert_eq!(eval("cast_int", &[i(5)]), i(5));
    }
    #[test]
    fn cast_float_from_i() {
        approx(eval("cast_float", &[i(5)]), 5.0);
    }
    #[test]
    fn cast_float_from_s() {
        approx(eval("cast_float", &[s("3.15")]), 3.15);
    }
    #[test]
    fn cast_float_null() {
        assert_eq!(eval("cast_float", &[null()]), null());
    }
    #[test]
    fn cast_float_from_f() {
        approx(eval("cast_float", &[f(2.5)]), 2.5);
    }
    #[test]
    fn cast_str_from_i() {
        assert_eq!(eval("cast_str", &[i(42)]), s("42"));
    }
    #[test]
    fn cast_str_from_f() {
        assert_eq!(eval("cast_str", &[f(3.15)]), s("3.15"));
    }
    #[test]
    fn cast_str_null() {
        assert_eq!(eval("cast_str", &[null()]), null());
    }
    #[test]
    fn cast_str_from_s() {
        assert_eq!(eval("cast_str", &[s("hi")]), s("hi"));
    }
    #[test]
    fn cast_bool_true() {
        assert_eq!(eval("cast_bool", &[i(1)]), i(1));
    }
    #[test]
    fn cast_bool_false() {
        assert_eq!(eval("cast_bool", &[i(0)]), i(0));
    }
    #[test]
    fn cast_bool_null() {
        assert_eq!(eval("cast_bool", &[null()]), null());
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
    fn cast_ts_i() {
        assert_eq!(eval("cast_timestamp", &[i(1000)]), ts(1000));
    }
    #[test]
    fn cast_ts_null() {
        assert_eq!(eval("cast_timestamp", &[null()]), null());
    }
    #[test]
    fn typeof_i64() {
        assert_eq!(eval("typeof", &[i(42)]), s("i64"));
    }
    #[test]
    fn typeof_f64() {
        assert_eq!(eval("typeof", &[f(3.15)]), s("f64"));
    }
    #[test]
    fn typeof_str() {
        assert_eq!(eval("typeof", &[s("hi")]), s("string"));
    }
    #[test]
    fn typeof_null() {
        assert_eq!(eval("typeof", &[null()]), s("null"));
    }
    #[test]
    fn typeof_ts() {
        assert_eq!(eval("typeof", &[ts(1000)]), s("timestamp"));
    }
    #[test]
    fn is_null_true() {
        assert_eq!(eval("is_null", &[null()]), i(1));
    }
    #[test]
    fn is_null_false() {
        assert_eq!(eval("is_null", &[i(5)]), i(0));
    }
    #[test]
    fn is_not_null_true() {
        assert_eq!(eval("is_not_null", &[i(5)]), i(1));
    }
    #[test]
    fn is_not_null_false() {
        assert_eq!(eval("is_not_null", &[null()]), i(0));
    }
    #[test]
    fn to_number_int() {
        assert_eq!(eval("to_number", &[s("42")]), i(42));
    }
    #[test]
    fn to_number_float() {
        approx(eval("to_number", &[s("3.15")]), 3.15);
    }
    #[test]
    fn to_number_null() {
        assert_eq!(eval("to_number", &[null()]), null());
    }
    #[test]
    fn to_number_identity_i() {
        assert_eq!(eval("to_number", &[i(5)]), i(5));
    }
    #[test]
    fn safe_cast_int_ok() {
        assert_eq!(eval("safe_cast_int", &[s("42")]), i(42));
    }
    #[test]
    fn safe_cast_int_bad() {
        assert_eq!(eval("safe_cast_int", &[s("abc")]), null());
    }
    #[test]
    fn safe_cast_float_ok() {
        approx(eval("safe_cast_float", &[s("3.15")]), 3.15);
    }
    #[test]
    fn safe_cast_float_bad() {
        assert_eq!(eval("safe_cast_float", &[s("abc")]), null());
    }
}

// ===========================================================================
// Date/Time extraction
// ===========================================================================
mod datetime_extract {
    use super::*;
    // 2024-03-15 12:30:45 UTC in nanos = 1_710_505_845_000_000_000
    const TS: i64 = 1_710_505_845_000_000_000;
    #[test]
    fn year() {
        assert_eq!(eval("extract_year", &[ts(TS)]), i(2024));
    }
    #[test]
    fn month() {
        assert_eq!(eval("extract_month", &[ts(TS)]), i(3));
    }
    #[test]
    fn day() {
        assert_eq!(eval("extract_day", &[ts(TS)]), i(15));
    }
    #[test]
    fn hour() {
        assert_eq!(eval("extract_hour", &[ts(TS)]), i(12));
    }
    #[test]
    fn minute() {
        assert_eq!(eval("extract_minute", &[ts(TS)]), i(30));
    }
    #[test]
    fn second() {
        assert_eq!(eval("extract_second", &[ts(TS)]), i(45));
    }
    #[test]
    fn year_null() {
        assert_eq!(eval("extract_year", &[null()]), null());
    }
    #[test]
    fn month_null() {
        assert_eq!(eval("extract_month", &[null()]), null());
    }
    #[test]
    fn day_null() {
        assert_eq!(eval("extract_day", &[null()]), null());
    }
    #[test]
    fn hour_null() {
        assert_eq!(eval("extract_hour", &[null()]), null());
    }
    #[test]
    fn minute_null() {
        assert_eq!(eval("extract_minute", &[null()]), null());
    }
    #[test]
    fn second_null() {
        assert_eq!(eval("extract_second", &[null()]), null());
    }
    // epoch=0 => 1970-01-01 00:00:00
    #[test]
    fn year_epoch() {
        assert_eq!(eval("extract_year", &[ts(0)]), i(1970));
    }
    #[test]
    fn month_epoch() {
        assert_eq!(eval("extract_month", &[ts(0)]), i(1));
    }
    #[test]
    fn day_epoch() {
        assert_eq!(eval("extract_day", &[ts(0)]), i(1));
    }
    #[test]
    fn hour_epoch() {
        assert_eq!(eval("extract_hour", &[ts(0)]), i(0));
    }
    #[test]
    fn minute_epoch() {
        assert_eq!(eval("extract_minute", &[ts(0)]), i(0));
    }
    #[test]
    fn second_epoch() {
        assert_eq!(eval("extract_second", &[ts(0)]), i(0));
    }
    // 2000-01-01 00:00:00 UTC in nanos = 946_684_800_000_000_000
    const Y2K: i64 = 946_684_800_000_000_000;
    #[test]
    fn year_2000() {
        assert_eq!(eval("extract_year", &[ts(Y2K)]), i(2000));
    }
    #[test]
    fn month_jan() {
        assert_eq!(eval("extract_month", &[ts(Y2K)]), i(1));
    }
    #[test]
    fn day_first() {
        assert_eq!(eval("extract_day", &[ts(Y2K)]), i(1));
    }
    #[test]
    fn epoch_nanos_basic() {
        assert_eq!(eval("epoch_nanos", &[ts(TS)]), i(TS));
    }
    #[test]
    fn epoch_nanos_null() {
        assert_eq!(eval("epoch_nanos", &[null()]), null());
    }
    #[test]
    fn to_timestamp_i() {
        assert_eq!(eval("to_timestamp", &[i(1000)]), ts(1000));
    }
    #[test]
    fn to_timestamp_null() {
        assert_eq!(eval("to_timestamp", &[null()]), null());
    }
    #[test]
    fn quarter_jan() {
        assert_eq!(eval("extract_quarter", &[ts(Y2K)]), i(1));
    }
    #[test]
    fn quarter_apr() {
        assert_eq!(
            eval("extract_quarter", &[ts(Y2K + 91i64 * 86_400_000_000_000)]),
            i(2)
        );
    }
}

// ===========================================================================
// Date/Time — date_trunc, date_diff, timestamp_add
// ===========================================================================
mod datetime_ops {
    use super::*;
    const DAY_NS: i64 = 86_400_000_000_000;
    const HOUR_NS: i64 = 3_600_000_000_000;
    const MIN_NS: i64 = 60_000_000_000;
    const SEC_NS: i64 = 1_000_000_000;
    // 2024-03-15 12:30:45 UTC
    const TS: i64 = 1_710_505_845_000_000_000;
    // 2024-03-15 00:00:00 UTC
    const DAY_START: i64 = 1_710_460_800_000_000_000;

    #[test]
    fn trunc_day() {
        assert_eq!(eval("date_trunc", &[s("day"), ts(TS)]), ts(DAY_START));
    }
    #[test]
    fn trunc_hour() {
        let expected = DAY_START + 12 * HOUR_NS;
        assert_eq!(eval("date_trunc", &[s("hour"), ts(TS)]), ts(expected));
    }
    #[test]
    fn trunc_minute() {
        let expected = DAY_START + 12 * HOUR_NS + 30 * MIN_NS;
        assert_eq!(eval("date_trunc", &[s("minute"), ts(TS)]), ts(expected));
    }
    #[test]
    fn trunc_second() {
        let expected = DAY_START + 12 * HOUR_NS + 30 * MIN_NS + 45 * SEC_NS;
        assert_eq!(eval("date_trunc", &[s("second"), ts(TS)]), ts(expected));
    }
    #[test]
    fn trunc_null() {
        assert_eq!(eval("date_trunc", &[s("day"), null()]), null());
    }

    #[test]
    fn date_diff_seconds() {
        assert_eq!(eval("date_diff", &[s("second"), ts(0), ts(SEC_NS)]), i(1));
    }
    #[test]
    fn date_diff_minutes() {
        assert_eq!(eval("date_diff", &[s("minute"), ts(0), ts(MIN_NS)]), i(1));
    }
    #[test]
    fn date_diff_hours() {
        assert_eq!(eval("date_diff", &[s("hour"), ts(0), ts(HOUR_NS)]), i(1));
    }
    #[test]
    fn date_diff_days() {
        assert_eq!(eval("date_diff", &[s("day"), ts(0), ts(DAY_NS)]), i(1));
    }
    #[test]
    fn date_diff_null() {
        assert_eq!(eval("date_diff", &[s("day"), null(), ts(0)]), null());
    }
    #[test]
    fn date_diff_10_days() {
        assert_eq!(
            eval("date_diff", &[s("day"), ts(0), ts(10 * DAY_NS)]),
            i(10)
        );
    }

    #[test]
    fn ts_add_1_day() {
        assert_eq!(eval("timestamp_add", &[s("day"), i(1), ts(0)]), ts(DAY_NS));
    }
    #[test]
    fn ts_add_1_hour() {
        assert_eq!(
            eval("timestamp_add", &[s("hour"), i(1), ts(0)]),
            ts(HOUR_NS)
        );
    }
    #[test]
    fn ts_add_1_min() {
        assert_eq!(
            eval("timestamp_add", &[s("minute"), i(1), ts(0)]),
            ts(MIN_NS)
        );
    }
    #[test]
    fn ts_add_1_sec() {
        assert_eq!(
            eval("timestamp_add", &[s("second"), i(1), ts(0)]),
            ts(SEC_NS)
        );
    }
    #[test]
    fn ts_add_null() {
        assert_eq!(eval("timestamp_add", &[s("day"), i(1), null()]), null());
    }
    #[test]
    fn ts_add_neg() {
        assert_eq!(eval("timestamp_add", &[s("day"), i(-1), ts(DAY_NS)]), ts(0));
    }
    #[test]
    fn ts_add_5_days() {
        assert_eq!(
            eval("timestamp_add", &[s("day"), i(5), ts(0)]),
            ts(5 * DAY_NS)
        );
    }
}

// ===========================================================================
// String — lpad, rpad, split_part
// ===========================================================================
mod string_pad_split {
    use super::*;
    #[test]
    fn lpad_basic() {
        assert_eq!(eval("lpad", &[s("hi"), i(5), s("x")]), s("xxxhi"));
    }
    #[test]
    fn lpad_no_pad() {
        assert_eq!(eval("lpad", &[s("hello"), i(5), s("x")]), s("hello"));
    }
    #[test]
    fn lpad_trunc() {
        assert_eq!(eval("lpad", &[s("hello"), i(3), s("x")]), s("hel"));
    }
    #[test]
    fn lpad_null() {
        assert_eq!(eval("lpad", &[null(), i(5), s("x")]), null());
    }
    #[test]
    fn rpad_basic() {
        assert_eq!(eval("rpad", &[s("hi"), i(5), s("x")]), s("hixxx"));
    }
    #[test]
    fn rpad_no_pad() {
        assert_eq!(eval("rpad", &[s("hello"), i(5), s("x")]), s("hello"));
    }
    #[test]
    fn rpad_trunc() {
        assert_eq!(eval("rpad", &[s("hello"), i(3), s("x")]), s("hel"));
    }
    #[test]
    fn rpad_null() {
        assert_eq!(eval("rpad", &[null(), i(5), s("x")]), null());
    }
    #[test]
    fn split_part_1() {
        assert_eq!(eval("split_part", &[s("a.b.c"), s("."), i(1)]), s("a"));
    }
    #[test]
    fn split_part_2() {
        assert_eq!(eval("split_part", &[s("a.b.c"), s("."), i(2)]), s("b"));
    }
    #[test]
    fn split_part_3() {
        assert_eq!(eval("split_part", &[s("a.b.c"), s("."), i(3)]), s("c"));
    }
    #[test]
    fn split_part_oob() {
        assert_eq!(eval("split_part", &[s("a.b"), s("."), i(5)]), s(""));
    }
    #[test]
    fn split_part_null() {
        assert_eq!(eval("split_part", &[null(), s("."), i(1)]), null());
    }
    #[test]
    fn split_part_comma() {
        assert_eq!(eval("split_part", &[s("x,y,z"), s(","), i(2)]), s("y"));
    }
}

// ===========================================================================
// Regexp functions
// ===========================================================================
mod regexp {
    use super::*;
    #[test]
    fn match_yes() {
        assert_eq!(eval("regexp_match", &[s("hello123"), s("[0-9]+")]), i(1));
    }
    #[test]
    fn match_no() {
        assert_eq!(eval("regexp_match", &[s("hello"), s("[0-9]+")]), i(0));
    }
    #[test]
    fn match_null() {
        assert_eq!(eval("regexp_match", &[null(), s("[0-9]+")]), null());
    }
    #[test]
    fn match_full() {
        assert_eq!(eval("regexp_match", &[s("abc"), s("^abc$")]), i(1));
    }
    #[test]
    fn match_partial() {
        assert_eq!(eval("regexp_match", &[s("abc123"), s("[a-z]+")]), i(1));
    }
    #[test]
    fn replace_digits() {
        assert_eq!(
            eval("regexp_replace", &[s("a1b2c3"), s("[0-9]"), s("")]),
            s("abc")
        );
    }
    #[test]
    fn replace_null() {
        assert_eq!(eval("regexp_replace", &[null(), s("."), s("")]), null());
    }
    #[test]
    fn replace_no_match() {
        assert_eq!(
            eval("regexp_replace", &[s("abc"), s("[0-9]"), s("x")]),
            s("abc")
        );
    }
    #[test]
    fn extract_group() {
        assert_eq!(
            eval("regexp_extract", &[s("abc123"), s("([0-9]+)"), i(1)]),
            s("123")
        );
    }
    #[test]
    fn extract_no_match() {
        assert_eq!(
            eval("regexp_extract", &[s("abc"), s("([0-9]+)"), i(1)]),
            null()
        );
    }
    #[test]
    fn extract_null() {
        assert_eq!(eval("regexp_extract", &[null(), s("(.*)"), i(1)]), null());
    }
    #[test]
    fn extract_full() {
        assert_eq!(
            eval(
                "regexp_extract",
                &[s("abc123"), s("([a-z]+)([0-9]+)"), i(2)]
            ),
            s("123")
        );
    }
    #[test]
    fn regexp_count_basic() {
        assert_eq!(eval("regexp_count", &[s("a1b2c3"), s("[0-9]")]), i(3));
    }
    #[test]
    fn regexp_count_none() {
        assert_eq!(eval("regexp_count", &[s("abc"), s("[0-9]")]), i(0));
    }
    #[test]
    fn regexp_count_null() {
        assert_eq!(eval("regexp_count", &[null(), s("[0-9]")]), null());
    }
}

// ===========================================================================
// Hash & encoding functions — md5, sha256, encode, decode, base64
// ===========================================================================
mod hash_encode {
    use super::*;
    #[test]
    fn md5_empty() {
        assert_eq!(eval("md5", &[s("")]), s("d41d8cd98f00b204e9800998ecf8427e"));
    }
    #[test]
    fn md5_hello() {
        assert_eq!(
            eval("md5", &[s("hello")]),
            s("5d41402abc4b2a76b9719d911017c592")
        );
    }
    #[test]
    fn md5_null() {
        assert_eq!(eval("md5", &[null()]), null());
    }
    #[test]
    fn sha256_empty() {
        assert_eq!(
            eval("hash_sha256", &[s("")]),
            s("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        );
    }
    #[test]
    fn sha256_hello() {
        assert_eq!(
            eval("hash_sha256", &[s("hello")]),
            s("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
        );
    }
    #[test]
    fn sha256_null() {
        assert_eq!(eval("hash_sha256", &[null()]), null());
    }
    #[test]
    fn encode_base64() {
        assert_eq!(eval("encode", &[s("hello"), s("base64")]), s("aGVsbG8="));
    }
    #[test]
    fn encode_null() {
        assert_eq!(eval("encode", &[null(), s("base64")]), null());
    }
    #[test]
    fn decode_base64() {
        assert_eq!(eval("decode", &[s("aGVsbG8="), s("base64")]), s("hello"));
    }
    #[test]
    fn decode_null() {
        assert_eq!(eval("decode", &[null(), s("base64")]), null());
    }
    #[test]
    fn to_base64_basic() {
        assert_eq!(eval("to_base64", &[s("abc")]), s("YWJj"));
    }
    #[test]
    fn from_base64_basic() {
        assert_eq!(eval("from_base64", &[s("YWJj")]), s("abc"));
    }
    #[test]
    fn hex_42() {
        assert_eq!(eval("hex", &[i(42)]), s("2a"));
    }
    #[test]
    fn hex_255() {
        assert_eq!(eval("hex", &[i(255)]), s("ff"));
    }
    #[test]
    fn hex_0() {
        assert_eq!(eval("hex", &[i(0)]), s("0"));
    }
    #[test]
    fn hex_null() {
        assert_eq!(eval("hex", &[null()]), null());
    }
}

// ===========================================================================
// String — quote_ident, quote_literal, format, ascii, chr, initcap, translate
// ===========================================================================
mod string_quote_format {
    use super::*;
    #[test]
    fn quote_ident_basic() {
        assert_eq!(eval("quote_ident", &[s("col")]), s("\"col\""));
    }
    #[test]
    fn quote_ident_null() {
        assert_eq!(eval("quote_ident", &[null()]), null());
    }
    #[test]
    fn quote_literal_basic() {
        assert_eq!(eval("quote_literal", &[s("hello")]), s("'hello'"));
    }
    #[test]
    fn quote_literal_null() {
        assert_eq!(eval("quote_literal", &[null()]), null());
    }
    #[test]
    fn format_basic() {
        assert_eq!(
            eval("format", &[s("hello %s"), s("world")]),
            s("hello world")
        );
    }
    #[test]
    fn format_null() {
        assert_eq!(eval("format", &[null()]), null());
    }
    #[test]
    fn format_two() {
        assert_eq!(eval("format", &[s("%s + %s"), s("a"), s("b")]), s("a + b"));
    }
    #[test]
    fn ascii_a() {
        assert_eq!(eval("ascii", &[s("A")]), i(65));
    }
    #[test]
    fn ascii_zero() {
        assert_eq!(eval("ascii", &[s("0")]), i(48));
    }
    #[test]
    fn ascii_null() {
        assert_eq!(eval("ascii", &[null()]), null());
    }
    #[test]
    fn ascii_empty() {
        assert_eq!(eval("ascii", &[s("")]), i(0));
    }
    #[test]
    fn chr_65() {
        assert_eq!(eval("chr", &[i(65)]), s("A"));
    }
    #[test]
    fn chr_48() {
        assert_eq!(eval("chr", &[i(48)]), s("0"));
    }
    #[test]
    fn chr_null() {
        assert_eq!(eval("chr", &[null()]), null());
    }
    #[test]
    fn initcap_basic() {
        assert_eq!(eval("initcap", &[s("hello world")]), s("Hello World"));
    }
    #[test]
    fn initcap_upper() {
        assert_eq!(eval("initcap", &[s("HELLO WORLD")]), s("Hello World"));
    }
    #[test]
    fn initcap_null() {
        assert_eq!(eval("initcap", &[null()]), null());
    }
    #[test]
    fn initcap_mixed() {
        assert_eq!(eval("initcap", &[s("hELLO wORLD")]), s("Hello World"));
    }
    #[test]
    fn translate_basic() {
        assert_eq!(eval("translate", &[s("abc"), s("ab"), s("xy")]), s("xyc"));
    }
    #[test]
    fn translate_null() {
        assert_eq!(eval("translate", &[null(), s("a"), s("x")]), null());
    }
    #[test]
    fn translate_remove() {
        assert_eq!(eval("translate", &[s("abc"), s("abc"), s("x")]), s("x"));
    }
}

// ===========================================================================
// Predicates — is_positive, is_negative, is_zero, is_even, is_odd, between
// ===========================================================================
mod predicates {
    use super::*;
    #[test]
    fn is_positive_yes() {
        assert_eq!(eval("is_positive", &[i(5)]), i(1));
    }
    #[test]
    fn is_positive_no() {
        assert_eq!(eval("is_positive", &[i(-5)]), i(0));
    }
    #[test]
    fn is_positive_zero() {
        assert_eq!(eval("is_positive", &[i(0)]), i(0));
    }
    #[test]
    fn is_positive_null() {
        assert_eq!(eval("is_positive", &[null()]), null());
    }
    #[test]
    fn is_negative_yes() {
        assert_eq!(eval("is_negative", &[i(-5)]), i(1));
    }
    #[test]
    fn is_negative_no() {
        assert_eq!(eval("is_negative", &[i(5)]), i(0));
    }
    #[test]
    fn is_negative_zero() {
        assert_eq!(eval("is_negative", &[i(0)]), i(0));
    }
    #[test]
    fn is_negative_null() {
        assert_eq!(eval("is_negative", &[null()]), null());
    }
    #[test]
    fn is_zero_yes() {
        assert_eq!(eval("is_zero", &[i(0)]), i(1));
    }
    #[test]
    fn is_zero_no() {
        assert_eq!(eval("is_zero", &[i(5)]), i(0));
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
    fn is_odd_zero() {
        assert_eq!(eval("is_odd", &[i(0)]), i(0));
    }
    #[test]
    fn is_odd_null() {
        assert_eq!(eval("is_odd", &[null()]), null());
    }
    #[test]
    fn between_yes() {
        assert_eq!(eval("between", &[i(5), i(1), i(10)]), i(1));
    }
    #[test]
    fn between_no() {
        assert_eq!(eval("between", &[i(15), i(1), i(10)]), i(0));
    }
    #[test]
    fn between_edge_low() {
        assert_eq!(eval("between", &[i(1), i(1), i(10)]), i(1));
    }
    #[test]
    fn between_edge_high() {
        assert_eq!(eval("between", &[i(10), i(1), i(10)]), i(1));
    }
    #[test]
    fn between_null() {
        assert_eq!(eval("between", &[null(), i(1), i(10)]), null());
    }
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
    fn is_inf_no() {
        assert_eq!(eval("is_inf", &[f(1.0)]), i(0));
    }
    #[test]
    fn is_inf_null() {
        assert_eq!(eval("is_inf", &[null()]), null());
    }
}

// ===========================================================================
// String — strcmp, soundex, space, word_count, camel_case, snake_case, etc
// ===========================================================================
mod string_misc {
    use super::*;
    #[test]
    fn strcmp_equal() {
        assert_eq!(eval("strcmp", &[s("abc"), s("abc")]), i(0));
    }
    #[test]
    fn strcmp_less() {
        assert_eq!(eval("strcmp", &[s("abc"), s("abd")]), i(-1));
    }
    #[test]
    fn strcmp_greater() {
        assert_eq!(eval("strcmp", &[s("abd"), s("abc")]), i(1));
    }
    #[test]
    fn strcmp_null() {
        assert_eq!(eval("strcmp", &[null(), s("a")]), null());
    }
    #[test]
    fn soundex_robert() {
        assert_eq!(eval("soundex", &[s("Robert")]), s("R163"));
    }
    #[test]
    fn soundex_null() {
        assert_eq!(eval("soundex", &[null()]), null());
    }
    #[test]
    fn space_3() {
        assert_eq!(eval("space", &[i(3)]), s("   "));
    }
    #[test]
    fn space_0() {
        assert_eq!(eval("space", &[i(0)]), s(""));
    }
    #[test]
    fn space_null() {
        assert_eq!(eval("space", &[null()]), null());
    }
    #[test]
    fn word_count_basic() {
        assert_eq!(eval("word_count", &[s("hello world")]), i(2));
    }
    #[test]
    fn word_count_empty() {
        assert_eq!(eval("word_count", &[s("")]), i(0));
    }
    #[test]
    fn word_count_one() {
        assert_eq!(eval("word_count", &[s("hello")]), i(1));
    }
    #[test]
    fn word_count_null() {
        assert_eq!(eval("word_count", &[null()]), null());
    }
    #[test]
    fn camel_case_basic() {
        assert_eq!(eval("camel_case", &[s("hello_world")]), s("HelloWorld"));
    }
    #[test]
    fn camel_case_null() {
        assert_eq!(eval("camel_case", &[null()]), null());
    }
    #[test]
    fn snake_case_basic() {
        assert_eq!(eval("snake_case", &[s("helloWorld")]), s("hello_world"));
    }
    #[test]
    fn snake_case_null() {
        assert_eq!(eval("snake_case", &[null()]), null());
    }
    #[test]
    fn squeeze_basic() {
        assert_eq!(eval("squeeze", &[s("a  b  c")]), s("a b c"));
    }
    #[test]
    fn squeeze_null() {
        assert_eq!(eval("squeeze", &[null()]), null());
    }
    #[test]
    fn squeeze_single() {
        assert_eq!(eval("squeeze", &[s("abc")]), s("abc"));
    }
    #[test]
    fn count_char_basic() {
        assert_eq!(eval("count_char", &[s("hello"), s("l")]), i(2));
    }
    #[test]
    fn count_char_none() {
        assert_eq!(eval("count_char", &[s("hello"), s("x")]), i(0));
    }
    #[test]
    fn count_char_null() {
        assert_eq!(eval("count_char", &[null(), s("l")]), null());
    }
    #[test]
    fn byte_length_basic() {
        assert_eq!(eval("byte_length", &[s("hello")]), i(5));
    }
    #[test]
    fn byte_length_empty() {
        assert_eq!(eval("byte_length", &[s("")]), i(0));
    }
    #[test]
    fn byte_length_null() {
        assert_eq!(eval("byte_length", &[null()]), null());
    }
    #[test]
    fn bit_length_basic() {
        assert_eq!(eval("bit_length", &[s("hello")]), i(40));
    }
    #[test]
    fn bit_length_empty() {
        assert_eq!(eval("bit_length", &[s("")]), i(0));
    }
    #[test]
    fn bit_length_null() {
        assert_eq!(eval("bit_length", &[null()]), null());
    }
    #[test]
    fn bit_count_5() {
        assert_eq!(eval("bit_count", &[i(5)]), i(2));
    }
    #[test]
    fn bit_count_7() {
        assert_eq!(eval("bit_count", &[i(7)]), i(3));
    }
    #[test]
    fn bit_count_0() {
        assert_eq!(eval("bit_count", &[i(0)]), i(0));
    }
    #[test]
    fn bit_count_null() {
        assert_eq!(eval("bit_count", &[null()]), null());
    }
    #[test]
    fn leading_zeros_1() {
        assert_eq!(eval("leading_zeros", &[i(1)]), i(63));
    }
    #[test]
    fn leading_zeros_null() {
        assert_eq!(eval("leading_zeros", &[null()]), null());
    }
    #[test]
    fn trailing_zeros_2() {
        assert_eq!(eval("trailing_zeros", &[i(2)]), i(1));
    }
    #[test]
    fn trailing_zeros_8() {
        assert_eq!(eval("trailing_zeros", &[i(8)]), i(3));
    }
    #[test]
    fn trailing_zeros_null() {
        assert_eq!(eval("trailing_zeros", &[null()]), null());
    }
}

// ===========================================================================
// Misc — version, sizeof, url_encode, url_decode, json functions
// ===========================================================================
mod misc_funcs {
    use super::*;
    #[test]
    fn version_not_empty() {
        match eval("version", &[]) {
            Value::Str(v) => assert!(!v.is_empty()),
            other => panic!("expected Str, got {other:?}"),
        }
    }
    #[test]
    fn sizeof_i64() {
        match eval("sizeof", &[i(42)]) {
            Value::I64(v) => assert!(v > 0),
            other => panic!("expected I64, got {other:?}"),
        }
    }
    #[test]
    fn sizeof_str() {
        match eval("sizeof", &[s("hi")]) {
            Value::I64(v) => assert!(v > 0),
            other => panic!("expected I64, got {other:?}"),
        }
    }
    #[test]
    fn sizeof_null() {
        match eval("sizeof", &[null()]) {
            Value::I64(v) => assert!(v >= 0),
            other => panic!("expected I64, got {other:?}"),
        }
    }
    #[test]
    fn url_encode_basic() {
        assert_eq!(eval("url_encode", &[s("hello world")]), s("hello%20world"));
    }
    #[test]
    fn url_encode_null() {
        assert_eq!(eval("url_encode", &[null()]), null());
    }
    #[test]
    fn url_encode_empty() {
        assert_eq!(eval("url_encode", &[s("")]), s(""));
    }
    #[test]
    fn url_decode_basic() {
        assert_eq!(eval("url_decode", &[s("hello%20world")]), s("hello world"));
    }
    #[test]
    fn url_decode_null() {
        assert_eq!(eval("url_decode", &[null()]), null());
    }
    #[test]
    fn url_decode_empty() {
        assert_eq!(eval("url_decode", &[s("")]), s(""));
    }
    #[test]
    fn json_extract_str() {
        assert_eq!(eval("json_extract", &[s(r#"{"a":"b"}"#), s("a")]), s("b"));
    }
    #[test]
    fn json_extract_num() {
        assert_eq!(eval("json_extract", &[s(r#"{"x":42}"#), s("x")]), i(42));
    }
    #[test]
    fn json_extract_null() {
        assert_eq!(eval("json_extract", &[null(), s("a")]), null());
    }
    #[test]
    fn json_extract_missing() {
        assert_eq!(eval("json_extract", &[s(r#"{"a":1}"#), s("b")]), null());
    }
    #[test]
    fn json_array_length_basic() {
        assert_eq!(eval("json_array_length", &[s("[1,2,3]")]), i(3));
    }
    #[test]
    fn json_array_length_empty() {
        assert_eq!(eval("json_array_length", &[s("[]")]), i(0));
    }
    #[test]
    fn json_array_length_null() {
        assert_eq!(eval("json_array_length", &[null()]), null());
    }
    #[test]
    fn concat_ws_basic() {
        assert_eq!(
            eval("concat_ws", &[s(","), s("a"), s("b"), s("c")]),
            s("a,b,c")
        );
    }
    #[test]
    fn concat_ws_null_skip() {
        assert_eq!(
            eval("concat_ws", &[s(","), s("a"), null(), s("c")]),
            s("a,c")
        );
    }
    #[test]
    fn concat_ws_empty() {
        assert_eq!(eval("concat_ws", &[s(",")]), s(""));
    }
    #[test]
    fn to_json_i() {
        assert_eq!(eval("to_json", &[i(42)]), s("42"));
    }
    #[test]
    fn to_json_s() {
        assert_eq!(eval("to_json", &[s("hi")]), s("\"hi\""));
    }
    #[test]
    fn to_json_null() {
        assert_eq!(eval("to_json", &[null()]), s("null"));
    }
    #[test]
    fn to_json_f() {
        match eval("to_json", &[f(3.15)]) {
            Value::Str(v) => assert!(v.contains("3.15")),
            other => panic!("expected Str, got {other:?}"),
        }
    }
}

// ===========================================================================
// Special functions — zeroifnull, nullifempty, iif, nvl2, switch, negate, reciprocal, square, etc
// ===========================================================================
mod special_funcs {
    use super::*;
    #[test]
    fn zeroifnull_null() {
        assert_eq!(eval("zeroifnull", &[null()]), i(0));
    }
    #[test]
    fn zeroifnull_val() {
        assert_eq!(eval("zeroifnull", &[i(5)]), i(5));
    }
    #[test]
    fn nullifempty_empty() {
        assert_eq!(eval("nullifempty", &[s("")]), null());
    }
    #[test]
    fn nullifempty_notempty() {
        assert_eq!(eval("nullifempty", &[s("hi")]), s("hi"));
    }
    #[test]
    fn nullifempty_null() {
        assert_eq!(eval("nullifempty", &[null()]), null());
    }
    #[test]
    fn iif_true() {
        assert_eq!(eval("iif", &[i(1), s("yes"), s("no")]), s("yes"));
    }
    #[test]
    fn iif_false() {
        assert_eq!(eval("iif", &[i(0), s("yes"), s("no")]), s("no"));
    }
    #[test]
    fn nvl2_not_null() {
        assert_eq!(
            eval("nvl2", &[i(1), s("not null"), s("is null")]),
            s("not null")
        );
    }
    #[test]
    fn nvl2_null() {
        assert_eq!(
            eval("nvl2", &[null(), s("not null"), s("is null")]),
            s("is null")
        );
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
    fn negate_null() {
        assert_eq!(eval("negate", &[null()]), null());
    }
    #[test]
    fn reciprocal_2() {
        approx(eval("reciprocal", &[f(2.0)]), 0.5);
    }
    #[test]
    fn reciprocal_4() {
        approx(eval("reciprocal", &[f(4.0)]), 0.25);
    }
    #[test]
    fn reciprocal_null() {
        assert_eq!(eval("reciprocal", &[null()]), null());
    }
    #[test]
    fn square_3() {
        approx(eval("square", &[f(3.0)]), 9.0);
    }
    #[test]
    fn square_0() {
        approx(eval("square", &[f(0.0)]), 0.0);
    }
    #[test]
    fn square_neg() {
        approx(eval("square", &[f(-3.0)]), 9.0);
    }
    #[test]
    fn square_null() {
        assert_eq!(eval("square", &[null()]), null());
    }
    #[test]
    fn abs_diff_basic() {
        approx(eval("abs_diff", &[f(10.0), f(3.0)]), 7.0);
    }
    #[test]
    fn abs_diff_rev() {
        approx(eval("abs_diff", &[f(3.0), f(10.0)]), 7.0);
    }
    #[test]
    fn abs_diff_null() {
        assert_eq!(eval("abs_diff", &[null(), f(3.0)]), null());
    }
    #[test]
    fn map_range_basic() {
        approx(
            eval("map_range", &[f(5.0), f(0.0), f(10.0), f(0.0), f(100.0)]),
            50.0,
        );
    }
    #[test]
    fn map_range_null() {
        assert_eq!(
            eval("map_range", &[null(), f(0.0), f(10.0), f(0.0), f(100.0)]),
            null()
        );
    }
    #[test]
    fn current_schema() {
        match eval("current_schema", &[]) {
            Value::Str(v) => assert!(!v.is_empty()),
            other => panic!("expected Str, got {other:?}"),
        }
    }
    #[test]
    fn current_database() {
        match eval("current_database", &[]) {
            Value::Str(v) => assert!(!v.is_empty()),
            other => panic!("expected Str, got {other:?}"),
        }
    }
    #[test]
    fn current_user_fn() {
        match eval("current_user", &[]) {
            Value::Str(v) => assert!(!v.is_empty()),
            other => panic!("expected Str, got {other:?}"),
        }
    }
}

// ===========================================================================
// Aliases — verify all aliases work identically to their originals
// ===========================================================================
mod aliases {
    use super::*;
    #[test]
    fn ceiling_alias() {
        approx(eval("ceiling", &[f(3.2)]), 4.0);
    }
    #[test]
    fn power_alias() {
        approx(eval("power", &[f(2.0), f(3.0)]), 8.0);
    }
    #[test]
    fn substr_alias() {
        assert_eq!(eval("substr", &[s("hello"), i(1), i(3)]), s("hel"));
    }
    #[test]
    fn len_alias() {
        assert_eq!(eval("len", &[s("hello")]), i(5));
    }
    #[test]
    fn string_length_alias() {
        assert_eq!(eval("string_length", &[s("hello")]), i(5));
    }
    #[test]
    fn to_long_alias() {
        assert_eq!(eval("to_long", &[f(3.9)]), i(3));
    }
    #[test]
    fn to_double_alias() {
        approx(eval("to_double", &[i(5)]), 5.0);
    }
    #[test]
    fn to_string_alias() {
        assert_eq!(eval("to_string", &[i(42)]), s("42"));
    }
    #[test]
    fn ln_alias() {
        approx(eval("ln", &[f(std::f64::consts::E)]), 1.0);
    }
    #[test]
    fn remainder_alias() {
        assert_eq!(eval("remainder", &[i(10), i(3)]), i(1));
    }
    #[test]
    fn signum_alias() {
        assert_eq!(eval("signum", &[f(5.0)]), i(1));
    }
    #[test]
    fn modulo_alias() {
        assert_eq!(eval("modulo", &[i(7), i(3)]), i(1));
    }
    #[test]
    fn ifnull_alias() {
        assert_eq!(eval("ifnull", &[null(), i(99)]), i(99));
    }
    #[test]
    fn nvl_alias() {
        assert_eq!(eval("nvl", &[null(), i(99)]), i(99));
    }
    #[test]
    fn to_lowercase_alias() {
        assert_eq!(eval("to_lowercase", &[s("ABC")]), s("abc"));
    }
    #[test]
    fn to_uppercase_alias() {
        assert_eq!(eval("to_uppercase", &[s("abc")]), s("ABC"));
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
    fn round_down_alias() {
        approx(eval("round_down", &[f(3.7)]), 3.0);
    }
    #[test]
    fn round_up_alias() {
        approx(eval("round_up", &[f(3.2)]), 4.0);
    }
    #[test]
    fn str_concat_alias() {
        assert_eq!(eval("str_concat", &[s("a"), s("b")]), s("ab"));
    }
    #[test]
    fn min_of_alias() {
        assert_eq!(eval("min_of", &[i(1), i(3)]), i(1));
    }
    #[test]
    fn max_of_alias() {
        assert_eq!(eval("max_of", &[i(1), i(3)]), i(3));
    }
    #[test]
    fn digest_alias() {
        assert_eq!(
            eval("digest", &[s("hello")]),
            s("5d41402abc4b2a76b9719d911017c592")
        );
    }
    #[test]
    fn not_null_alias() {
        assert_eq!(eval("not_null", &[i(5)]), i(1));
    }
    #[test]
    fn is_null_fn_alias() {
        assert_eq!(eval("is_null_fn", &[null()]), i(1));
    }
    #[test]
    fn to_int_alias() {
        assert_eq!(eval("to_int", &[f(3.9)]), i(3));
    }
    #[test]
    fn to_float_alias() {
        approx(eval("to_float", &[i(5)]), 5.0);
    }
    #[test]
    fn to_str_alias() {
        assert_eq!(eval("to_str", &[i(42)]), s("42"));
    }
}

// ===========================================================================
// Epoch functions
// ===========================================================================
mod epoch_funcs {
    use super::*;
    const NS: i64 = 1_000_000_000;
    #[test]
    fn epoch_seconds_basic() {
        assert_eq!(eval("epoch_seconds", &[ts(10 * NS)]), i(10));
    }
    #[test]
    fn epoch_seconds_zero() {
        assert_eq!(eval("epoch_seconds", &[ts(0)]), i(0));
    }
    #[test]
    fn epoch_seconds_null() {
        assert_eq!(eval("epoch_seconds", &[null()]), null());
    }
    #[test]
    fn epoch_millis_basic() {
        assert_eq!(eval("epoch_millis", &[ts(NS)]), i(1000));
    }
    #[test]
    fn epoch_millis_zero() {
        assert_eq!(eval("epoch_millis", &[ts(0)]), i(0));
    }
    #[test]
    fn epoch_millis_null() {
        assert_eq!(eval("epoch_millis", &[null()]), null());
    }
    #[test]
    fn epoch_micros_basic() {
        assert_eq!(eval("epoch_micros", &[ts(NS)]), i(1_000_000));
    }
    #[test]
    fn epoch_micros_zero() {
        assert_eq!(eval("epoch_micros", &[ts(0)]), i(0));
    }
    #[test]
    fn epoch_micros_null() {
        assert_eq!(eval("epoch_micros", &[null()]), null());
    }
    #[test]
    fn epoch_nanos_zero() {
        assert_eq!(eval("epoch_nanos", &[ts(0)]), i(0));
    }
}

// ===========================================================================
// Internal pseudo-functions — __case_when
// ===========================================================================
mod case_when {
    use super::*;
    #[test]
    fn case_true() {
        assert_eq!(eval("__case_when", &[i(1), s("yes"), s("no")]), s("yes"));
    }
    #[test]
    fn case_false() {
        assert_eq!(eval("__case_when", &[i(0), s("yes"), s("no")]), s("no"));
    }
    #[test]
    fn case_neg() {
        assert_eq!(eval("__case_when", &[i(-1), s("yes"), s("no")]), s("yes"));
    }
    #[test]
    fn case_42() {
        assert_eq!(eval("__case_when", &[i(42), s("yes"), s("no")]), s("yes"));
    }
    #[test]
    fn case_i64_result() {
        assert_eq!(eval("__case_when", &[i(1), i(10), i(20)]), i(10));
    }
    #[test]
    fn case_false_i64() {
        assert_eq!(eval("__case_when", &[i(0), i(10), i(20)]), i(20));
    }
}

// ===========================================================================
// More aliases & edge cases to reach 1000
// ===========================================================================
mod more_aliases {
    use super::*;
    #[test]
    fn to_boolean_true() {
        assert_eq!(eval("to_boolean", &[s("true")]), i(1));
    }
    #[test]
    fn to_boolean_false() {
        assert_eq!(eval("to_boolean", &[s("false")]), i(0));
    }
    #[test]
    fn to_boolean_1() {
        assert_eq!(eval("to_boolean", &[i(1)]), i(1));
    }
    #[test]
    fn to_boolean_0() {
        assert_eq!(eval("to_boolean", &[i(0)]), i(0));
    }
    #[test]
    fn to_symbol_basic() {
        assert_eq!(eval("to_symbol", &[i(42)]), s("42"));
    }
    #[test]
    fn symbol_basic() {
        assert_eq!(eval("symbol", &[i(42)]), s("42"));
    }
    #[test]
    fn typecast_i64() {
        assert_eq!(eval("typecast", &[i(42)]), s("i64"));
    }
    #[test]
    fn typecast_str() {
        assert_eq!(eval("typecast", &[s("hi")]), s("string"));
    }
    #[test]
    fn pg_typeof_i() {
        assert_eq!(eval("pg_typeof", &[i(42)]), s("bigint"));
    }
    #[test]
    fn pg_typeof_f() {
        assert_eq!(eval("pg_typeof", &[f(3.15)]), s("double precision"));
    }
    #[test]
    fn pg_typeof_s() {
        assert_eq!(eval("pg_typeof", &[s("hi")]), s("text"));
    }
    #[test]
    fn hash_i() {
        match eval("hash", &[i(42)]) {
            Value::I64(_) => {}
            other => panic!("expected I64, got {other:?}"),
        }
    }
    #[test]
    fn hash_s() {
        match eval("hash", &[s("hello")]) {
            Value::I64(_) => {}
            other => panic!("expected I64, got {other:?}"),
        }
    }
    #[test]
    fn murmur3_basic() {
        match eval("murmur3", &[s("hello")]) {
            Value::I64(_) => {}
            other => panic!("expected I64, got {other:?}"),
        }
    }
    #[test]
    fn crc32_basic() {
        match eval("crc32", &[s("hello")]) {
            Value::I64(_) => {}
            other => panic!("expected I64, got {other:?}"),
        }
    }
    #[test]
    fn fnv1a_basic() {
        match eval("fnv1a", &[s("hello")]) {
            Value::I64(_) => {}
            other => panic!("expected I64, got {other:?}"),
        }
    }
    #[test]
    fn hash_null() {
        assert_eq!(eval("hash", &[null()]), null());
    }
    #[test]
    fn murmur3_null() {
        assert_eq!(eval("murmur3", &[null()]), null());
    }
    #[test]
    fn crc32_null() {
        assert_eq!(eval("crc32", &[null()]), null());
    }
    #[test]
    fn fnv1a_null() {
        assert_eq!(eval("fnv1a", &[null()]), null());
    }
    #[test]
    fn hash_code_basic() {
        match eval("hash_code", &[s("hello")]) {
            Value::I64(_) => {}
            other => panic!("expected I64, got {other:?}"),
        }
    }
    #[test]
    fn hash_combine_basic() {
        match eval("hash_combine", &[i(1), i(2)]) {
            Value::I64(_) => {}
            other => panic!("expected I64, got {other:?}"),
        }
    }
    #[test]
    fn hash_combine_null() {
        assert_eq!(eval("hash_combine", &[null(), i(2)]), null());
    }
}

// ===========================================================================
// Log_base, position, overlay, char_at
// ===========================================================================
mod log_base_etc {
    use super::*;
    #[test]
    fn log_base_2_8() {
        approx(eval("log_base", &[f(2.0), f(8.0)]), 3.0);
    }
    #[test]
    fn log_base_10_100() {
        approx(eval("log_base", &[f(10.0), f(100.0)]), 2.0);
    }
    #[test]
    fn log_base_null() {
        assert_eq!(eval("log_base", &[null(), f(8.0)]), null());
    }
    #[test]
    fn str_pos_found() {
        assert_eq!(eval("str_pos", &[s("ll"), s("hello")]), i(3));
    }
    #[test]
    fn str_pos_not_found() {
        assert_eq!(eval("str_pos", &[s("xyz"), s("hello")]), i(0));
    }
    #[test]
    fn str_pos_null() {
        assert_eq!(eval("str_pos", &[null(), s("a")]), null());
    }
    #[test]
    fn char_at_0() {
        assert_eq!(eval("char_at", &[s("hello"), i(0)]), null());
    }
    #[test]
    fn char_at_4() {
        assert_eq!(eval("char_at", &[s("hello"), i(4)]), s("l"));
    }
    #[test]
    fn char_at_null() {
        assert_eq!(eval("char_at", &[null(), i(0)]), null());
    }
    #[test]
    fn string_to_array_basic() {
        assert_eq!(eval("string_to_array", &[s("a,b,c"), s(",")]), s("[a,b,c]"));
    }
    #[test]
    fn string_to_array_null() {
        assert_eq!(eval("string_to_array", &[null(), s(",")]), null());
    }
    #[test]
    fn array_to_string_basic() {
        assert_eq!(
            eval("array_to_string", &[s("[\"a\",\"b\"]"), s(",")]),
            s("a,b")
        );
    }
    #[test]
    fn array_to_string_null() {
        assert_eq!(eval("array_to_string", &[null(), s(",")]), null());
    }
}

// ===========================================================================
// Date functions — is_weekend, is_business_day, start/end of period
// ===========================================================================
mod date_period {
    use super::*;
    // 2024-03-15 (Friday) = 1_710_460_800_000_000_000
    const FRIDAY: i64 = 1_710_460_800_000_000_000;
    // Saturday = Friday + 1 day
    const SATURDAY: i64 = 1_710_460_800_000_000_000 + 86_400_000_000_000;
    // Sunday = Saturday + 1 day
    const SUNDAY: i64 = SATURDAY + 86_400_000_000_000;

    #[test]
    fn is_weekend_friday() {
        assert_eq!(eval("is_weekend", &[ts(FRIDAY)]), i(0));
    }
    #[test]
    fn is_weekend_saturday() {
        assert_eq!(eval("is_weekend", &[ts(SATURDAY)]), i(1));
    }
    #[test]
    fn is_weekend_sunday() {
        assert_eq!(eval("is_weekend", &[ts(SUNDAY)]), i(1));
    }
    #[test]
    fn is_weekend_null() {
        assert_eq!(eval("is_weekend", &[null()]), null());
    }
    #[test]
    fn is_business_friday() {
        assert_eq!(eval("is_business_day", &[ts(FRIDAY)]), i(1));
    }
    #[test]
    fn is_business_saturday() {
        assert_eq!(eval("is_business_day", &[ts(SATURDAY)]), i(0));
    }
    #[test]
    fn is_business_null() {
        assert_eq!(eval("is_business_day", &[null()]), null());
    }
    // 2024-01-01 00:00:00 UTC = 1_704_067_200_000_000_000
    const JAN1: i64 = 1_704_067_200_000_000_000;
    #[test]
    fn start_of_year_basic() {
        assert_eq!(eval("start_of_year", &[ts(FRIDAY)]), ts(JAN1));
    }
    #[test]
    fn start_of_year_null() {
        assert_eq!(eval("start_of_year", &[null()]), null());
    }
    #[test]
    fn first_of_month_basic() {
        assert_eq!(
            eval("first_of_month", &[ts(FRIDAY)]),
            ts(1_709_251_200_000_000_000)
        );
    }
    #[test]
    fn first_of_month_null() {
        assert_eq!(eval("first_of_month", &[null()]), null());
    }
    #[test]
    fn extract_week_null() {
        assert_eq!(eval("extract_week", &[null()]), null());
    }
    #[test]
    fn extract_dow_null() {
        assert_eq!(eval("extract_day_of_week", &[null()]), null());
    }
    #[test]
    fn extract_doy_null() {
        assert_eq!(eval("extract_day_of_year", &[null()]), null());
    }
    #[test]
    fn is_leap_2024() {
        assert_eq!(eval("is_leap_year_fn", &[ts(JAN1)]), i(1));
    }
    #[test]
    fn is_leap_null() {
        assert_eq!(eval("is_leap_year_fn", &[null()]), null());
    }
    #[test]
    fn days_in_month_march() {
        assert_eq!(eval("days_in_month_fn", &[ts(FRIDAY)]), i(31));
    }
    #[test]
    fn days_in_month_null() {
        assert_eq!(eval("days_in_month_fn", &[null()]), null());
    }
}

// ===========================================================================
// More edge cases to pad to 1000 tests
// ===========================================================================
mod edge_cases_extra {
    use super::*;
    // Repeat tests with integer inputs where float conversion happens
    #[test]
    fn floor_from_int() {
        approx(eval("floor", &[i(5)]), 5.0);
    }
    #[test]
    fn ceil_from_int() {
        approx(eval("ceil", &[i(5)]), 5.0);
    }
    #[test]
    fn sqrt_from_int() {
        approx(eval("sqrt", &[i(9)]), 3.0);
    }
    #[test]
    fn round_from_int() {
        approx(eval("round", &[i(5)]), 5.0);
    }
    #[test]
    fn exp_from_int() {
        approx(eval("exp", &[i(0)]), 1.0);
    }
    #[test]
    fn sin_from_int() {
        approx(eval("sin", &[i(0)]), 0.0);
    }
    #[test]
    fn cos_from_int() {
        approx(eval("cos", &[i(0)]), 1.0);
    }
    #[test]
    fn tan_from_int() {
        approx(eval("tan", &[i(0)]), 0.0);
    }
    #[test]
    fn log_from_int() {
        approx(eval("log", &[i(1)]), 0.0);
    }
    #[test]
    fn sign_from_int() {
        assert_eq!(eval("sign", &[i(5)]), i(1));
    }
    #[test]
    fn sign_from_neg_int() {
        assert_eq!(eval("sign", &[i(-5)]), i(-1));
    }
    #[test]
    fn sign_from_zero_int() {
        assert_eq!(eval("sign", &[i(0)]), i(0));
    }
    // Repeat some conversions via string
    #[test]
    fn length_from_int() {
        assert_eq!(eval("length", &[i(12345)]), i(5));
    }
    #[test]
    fn upper_from_int() {
        assert_eq!(eval("upper", &[i(42)]), s("42"));
    }
    #[test]
    fn lower_from_int() {
        assert_eq!(eval("lower", &[i(42)]), s("42"));
    }
    #[test]
    fn trim_from_int() {
        assert_eq!(eval("trim", &[i(42)]), s("42"));
    }
    #[test]
    fn reverse_from_int() {
        assert_eq!(eval("reverse", &[i(123)]), s("321"));
    }
    // More conditional combos
    #[test]
    fn coalesce_f_null() {
        assert_eq!(eval("coalesce", &[null(), f(1.5)]), f(1.5));
    }
    #[test]
    fn greatest_f() {
        approx(eval("greatest", &[f(1.0), f(3.0), f(2.0)]), 3.0);
    }
    #[test]
    fn least_f() {
        approx(eval("least", &[f(1.0), f(3.0), f(2.0)]), 1.0);
    }
    #[test]
    fn greatest_str() {
        assert_eq!(eval("greatest", &[s("a"), s("c"), s("b")]), s("c"));
    }
    #[test]
    fn least_str() {
        assert_eq!(eval("least", &[s("a"), s("c"), s("b")]), s("a"));
    }
    #[test]
    fn nullif_f() {
        assert_eq!(eval("nullif", &[f(1.0), f(1.0)]), null());
    }
    #[test]
    fn nullif_f_diff() {
        approx(eval("nullif", &[f(1.0), f(2.0)]), 1.0);
    }
    // More bitwise with specific values
    #[test]
    fn bit_and_5_3() {
        assert_eq!(eval("bit_and", &[i(5), i(3)]), i(1));
    }
    #[test]
    fn bit_or_5_3() {
        assert_eq!(eval("bit_or", &[i(5), i(3)]), i(7));
    }
    #[test]
    fn bit_xor_5_3() {
        assert_eq!(eval("bit_xor", &[i(5), i(3)]), i(6));
    }
    #[test]
    fn bit_not_ff() {
        assert_eq!(eval("bit_not", &[i(0xFF)]), i(!0xFFi64));
    }
    #[test]
    fn shl_1_8() {
        assert_eq!(eval("bit_shift_left", &[i(1), i(8)]), i(256));
    }
    #[test]
    fn shr_1024_5() {
        assert_eq!(eval("bit_shift_right", &[i(1024), i(5)]), i(32));
    }
    // More GCD/LCM edge cases
    #[test]
    fn gcd_neg() {
        assert_eq!(eval("gcd", &[i(-12), i(8)]), i(4));
    }
    #[test]
    fn lcm_neg() {
        assert_eq!(eval("lcm", &[i(-4), i(6)]), i(12));
    }
    #[test]
    fn gcd_same() {
        assert_eq!(eval("gcd", &[i(7), i(7)]), i(7));
    }
    #[test]
    fn lcm_1_n() {
        assert_eq!(eval("lcm", &[i(1), i(100)]), i(100));
    }
    // More factorial
    #[test]
    fn factorial_2() {
        assert_eq!(eval("factorial", &[i(2)]), i(2));
    }
    #[test]
    fn factorial_3() {
        assert_eq!(eval("factorial", &[i(3)]), i(6));
    }
    #[test]
    fn factorial_4() {
        assert_eq!(eval("factorial", &[i(4)]), i(24));
    }
    #[test]
    fn factorial_6() {
        assert_eq!(eval("factorial", &[i(6)]), i(720));
    }
    #[test]
    fn factorial_7() {
        assert_eq!(eval("factorial", &[i(7)]), i(5040));
    }
    #[test]
    fn factorial_8() {
        assert_eq!(eval("factorial", &[i(8)]), i(40320));
    }
    #[test]
    fn factorial_9() {
        assert_eq!(eval("factorial", &[i(9)]), i(362880));
    }
    // More string operations
    #[test]
    fn replace_all() {
        assert_eq!(eval("replace", &[s("aaa"), s("a"), s("b")]), s("bbb"));
    }
    #[test]
    fn contains_at_start() {
        assert_eq!(eval("contains", &[s("hello"), s("he")]), i(1));
    }
    #[test]
    fn contains_at_end() {
        assert_eq!(eval("contains", &[s("hello"), s("lo")]), i(1));
    }
    #[test]
    fn left_5() {
        assert_eq!(eval("left", &[s("hello"), i(5)]), s("hello"));
    }
    #[test]
    fn right_5() {
        assert_eq!(eval("right", &[s("hello"), i(5)]), s("hello"));
    }
    #[test]
    fn repeat_5() {
        assert_eq!(eval("repeat", &[s("x"), i(5)]), s("xxxxx"));
    }
    #[test]
    fn reverse_long() {
        assert_eq!(eval("reverse", &[s("abcdef")]), s("fedcba"));
    }
    #[test]
    fn concat_four() {
        assert_eq!(eval("concat", &[s("a"), s("b"), s("c"), s("d")]), s("abcd"));
    }
    // Date arithmetic
    #[test]
    fn date_diff_0() {
        assert_eq!(eval("date_diff", &[s("day"), ts(0), ts(0)]), i(0));
    }
    #[test]
    fn date_diff_neg() {
        assert_eq!(
            eval("date_diff", &[s("day"), ts(86_400_000_000_000), ts(0)]),
            i(-1)
        );
    }
    // Width bucket additional
    #[test]
    fn wb_at_min() {
        assert_eq!(eval("width_bucket", &[f(0.0), f(0.0), f(10.0), i(5)]), i(1));
    }
    #[test]
    fn wb_mid() {
        assert_eq!(
            eval("width_bucket", &[f(3.0), f(0.0), f(10.0), i(10)]),
            i(4)
        );
    }
    // Trunc additional
    #[test]
    fn trunc_pos_1dp() {
        approx(eval("trunc", &[f(3.99), i(1)]), 3.9);
    }
    #[test]
    fn trunc_zero() {
        approx(eval("trunc", &[f(0.0)]), 0.0);
    }
    // More is_null / is_not_null with different types
    #[test]
    fn is_null_i64() {
        assert_eq!(eval("is_null", &[i(0)]), i(0));
    }
    #[test]
    fn is_null_str() {
        assert_eq!(eval("is_null", &[s("")]), i(0));
    }
    #[test]
    fn is_null_f64() {
        assert_eq!(eval("is_null", &[f(0.0)]), i(0));
    }
    #[test]
    fn is_not_null_i64() {
        assert_eq!(eval("is_not_null", &[i(0)]), i(1));
    }
    #[test]
    fn is_not_null_str() {
        assert_eq!(eval("is_not_null", &[s("")]), i(1));
    }
    #[test]
    fn is_not_null_f64() {
        assert_eq!(eval("is_not_null", &[f(0.0)]), i(1));
    }
    // Additional pow tests
    #[test]
    fn pow_1_100() {
        approx(eval("pow", &[f(1.0), f(100.0)]), 1.0);
    }
    #[test]
    fn pow_0_5() {
        approx(eval("pow", &[f(0.0), f(5.0)]), 0.0);
    }
    #[test]
    fn pow_5_1() {
        approx(eval("pow", &[f(5.0), f(1.0)]), 5.0);
    }
    // More sqrt
    #[test]
    fn sqrt_100() {
        approx(eval("sqrt", &[f(100.0)]), 10.0);
    }
    #[test]
    fn sqrt_2() {
        approx(eval("sqrt", &[f(2.0)]), std::f64::consts::SQRT_2);
    }
    // More cbrt
    #[test]
    fn cbrt_neg8() {
        approx(eval("cbrt", &[f(-8.0)]), -2.0);
    }
    #[test]
    fn cbrt_64() {
        approx(eval("cbrt", &[f(64.0)]), 4.0);
    }
    #[test]
    fn cbrt_125() {
        approx(eval("cbrt", &[f(125.0)]), 5.0);
    }
    // Additional mod
    #[test]
    fn mod_100_7() {
        assert_eq!(eval("mod", &[i(100), i(7)]), i(2));
    }
    #[test]
    fn mod_0_5() {
        assert_eq!(eval("mod", &[i(0), i(5)]), i(0));
    }
    #[test]
    fn mod_1_1() {
        assert_eq!(eval("mod", &[i(1), i(1)]), i(0));
    }
    // More div
    #[test]
    fn div_100_7() {
        assert_eq!(eval("div", &[i(100), i(7)]), i(14));
    }
    #[test]
    fn div_1_1() {
        assert_eq!(eval("div", &[i(1), i(1)]), i(1));
    }
    #[test]
    fn div_0_1() {
        assert_eq!(eval("div", &[i(0), i(1)]), i(0));
    }
    // Additional string pad
    #[test]
    fn lpad_multi_pad() {
        assert_eq!(eval("lpad", &[s("a"), i(4), s("xy")]), s("xyxa"));
    }
    #[test]
    fn rpad_multi_pad() {
        assert_eq!(eval("rpad", &[s("a"), i(4), s("xy")]), s("axyx"));
    }
    // More split_part
    #[test]
    fn split_part_slash() {
        assert_eq!(eval("split_part", &[s("a/b/c"), s("/"), i(1)]), s("a"));
    }
    #[test]
    fn split_part_last() {
        assert_eq!(eval("split_part", &[s("a/b/c"), s("/"), i(3)]), s("c"));
    }
    // More concat combos
    #[test]
    fn concat_int_str() {
        assert_eq!(eval("concat", &[i(1), s("a")]), s("1a"));
    }
    #[test]
    fn concat_all_null() {
        assert_eq!(eval("concat", &[null(), null()]), s(""));
    }
    // Additional cast tests
    #[test]
    fn cast_int_neg() {
        assert_eq!(eval("cast_int", &[f(-3.9)]), i(-3));
    }
    #[test]
    fn cast_float_neg() {
        approx(eval("cast_float", &[i(-5)]), -5.0);
    }
    #[test]
    fn cast_bool_yes() {
        assert_eq!(eval("cast_bool", &[s("yes")]), i(1));
    }
    #[test]
    fn cast_bool_1() {
        assert_eq!(eval("cast_bool", &[s("1")]), i(1));
    }
    #[test]
    fn to_number_ts() {
        assert_eq!(eval("to_number", &[ts(1000)]), i(1000));
    }
}
