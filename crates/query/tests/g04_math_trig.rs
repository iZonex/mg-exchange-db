//! 500 sin/cos/tan/sqrt/pow/exp/log tests.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

fn i(v: i64) -> Value {
    Value::I64(v)
}
fn f(v: f64) -> Value {
    Value::F64(v)
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

use std::f64::consts::{FRAC_PI_2, FRAC_PI_3, FRAC_PI_4, FRAC_PI_6, PI};

// sin for various angles
mod sin_tests {
    use super::*;
    macro_rules! sin_t {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("sin", &[f($v)]), $e, 0.001);
            }
        };
    }
    sin_t!(s000, 0.0, 0.0);
    sin_t!(s_pi6, FRAC_PI_6, 0.5);
    sin_t!(s_pi4, FRAC_PI_4, 0.7071);
    sin_t!(s_pi3, FRAC_PI_3, 0.8660);
    sin_t!(s_pi2, FRAC_PI_2, 1.0);
    sin_t!(s_pi, PI, 0.0);
    sin_t!(s_3pi2, 3.0 * FRAC_PI_2, -1.0);
    sin_t!(s_2pi, 2.0 * PI, 0.0);
    sin_t!(s_neg_pi2, -FRAC_PI_2, -1.0);
    sin_t!(s_neg_pi, -PI, 0.0);
    sin_t!(s01, 0.1, 0.0998);
    sin_t!(s02, 0.2, 0.1987);
    sin_t!(s03, 0.3, 0.2955);
    sin_t!(s04, 0.4, 0.3894);
    sin_t!(s05, 0.5, 0.4794);
    sin_t!(s06, 0.6, 0.5646);
    sin_t!(s07, 0.7, 0.6442);
    sin_t!(s08, 0.8, 0.7174);
    sin_t!(s09, 0.9, 0.7833);
    sin_t!(s10, 1.0, 0.8415);
    sin_t!(s11, 1.1, 0.8912);
    sin_t!(s12, 1.2, 0.9320);
    sin_t!(s13, 1.3, 0.9636);
    sin_t!(s14, 1.4, 0.9854);
    sin_t!(s15, 1.5, 0.9975);
    sin_t!(s16, 2.0, 0.9093);
    sin_t!(s17, 2.5, 0.5985);
    sin_t!(s18, 3.0, 0.1411);
    sin_t!(s19, -0.5, -0.4794);
    sin_t!(s20, -1.0, -0.8415);
    sin_t!(s21, -1.5, -0.9975);
    sin_t!(s22, -2.0, -0.9093);
    sin_t!(s23, -2.5, -0.5985);
    sin_t!(s24, -3.0, -0.1411);
    #[test]
    fn null_in() {
        assert_eq!(ev("sin", &[null()]), null());
    }
    sin_t!(s_int0, 0.0, 0.0);
    sin_t!(s25, 0.15, 0.1494);
    sin_t!(s26, 0.25, 0.2474);
    sin_t!(s27, 0.35, 0.3429);
    sin_t!(s28, 0.45, 0.4350);
    sin_t!(s29, 0.55, 0.5227);
    sin_t!(s30, 0.65, 0.6052);
    sin_t!(s31, 0.75, 0.6816);
    sin_t!(s32, 0.85, 0.7509);
    sin_t!(s33, 0.95, 0.8134);
    sin_t!(s34, 1.05, 0.8674);
    sin_t!(s35, 1.15, 0.9128);
    sin_t!(s36, 1.25, 0.9490);
    sin_t!(s37, 1.35, 0.9757);
    sin_t!(s38, 1.45, 0.9927);
}

// cos for various angles
mod cos_tests {
    use super::*;
    macro_rules! cos_t {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("cos", &[f($v)]), $e, 0.001);
            }
        };
    }
    cos_t!(c000, 0.0, 1.0);
    cos_t!(c_pi6, FRAC_PI_6, 0.8660);
    cos_t!(c_pi4, FRAC_PI_4, 0.7071);
    cos_t!(c_pi3, FRAC_PI_3, 0.5);
    cos_t!(c_pi2, FRAC_PI_2, 0.0);
    cos_t!(c_pi, PI, -1.0);
    cos_t!(c_2pi, 2.0 * PI, 1.0);
    cos_t!(c_neg_pi, -PI, -1.0);
    cos_t!(c01, 0.1, 0.9950);
    cos_t!(c02, 0.2, 0.9801);
    cos_t!(c03, 0.3, 0.9553);
    cos_t!(c04, 0.4, 0.9211);
    cos_t!(c05, 0.5, 0.8776);
    cos_t!(c06, 0.6, 0.8253);
    cos_t!(c07, 0.7, 0.7648);
    cos_t!(c08, 0.8, 0.6967);
    cos_t!(c09, 0.9, 0.6216);
    cos_t!(c10, 1.0, 0.5403);
    cos_t!(c11, 1.1, 0.4536);
    cos_t!(c12, 1.2, 0.3624);
    cos_t!(c13, 1.3, 0.2675);
    cos_t!(c14, 1.4, 0.1700);
    cos_t!(c15, 1.5, 0.0707);
    cos_t!(c16, 2.0, -0.4161);
    cos_t!(c17, 2.5, -0.8011);
    cos_t!(c18, 3.0, -0.9900);
    cos_t!(c19, -0.5, 0.8776);
    cos_t!(c20, -1.0, 0.5403);
    cos_t!(c21, -1.5, 0.0707);
    cos_t!(c22, -2.0, -0.4161);
    cos_t!(c23, -2.5, -0.8011);
    cos_t!(c24, -3.0, -0.9900);
    #[test]
    fn null_in() {
        assert_eq!(ev("cos", &[null()]), null());
    }
    cos_t!(c25, 0.15, 0.9888);
    cos_t!(c26, 0.25, 0.9689);
    cos_t!(c27, 0.35, 0.9394);
    cos_t!(c28, 0.45, 0.9004);
    cos_t!(c29, 0.55, 0.8525);
    cos_t!(c30, 0.65, 0.7961);
    cos_t!(c31, 0.75, 0.7317);
    cos_t!(c32, 0.85, 0.6603);
    cos_t!(c33, 0.95, 0.5817);
    cos_t!(c34, 1.05, 0.4976);
    cos_t!(c35, 1.15, 0.4085);
    cos_t!(c36, 1.25, 0.3153);
    cos_t!(c37, 1.35, 0.2190);
    cos_t!(c38, 1.45, 0.1205);
}

// tan for safe angles
mod tan_tests {
    use super::*;
    macro_rules! tan_t {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("tan", &[f($v)]), $e, 0.01);
            }
        };
    }
    tan_t!(t000, 0.0, 0.0);
    tan_t!(t_pi4, FRAC_PI_4, 1.0);
    tan_t!(t_neg_pi4, -FRAC_PI_4, -1.0);
    tan_t!(t_pi6, FRAC_PI_6, 0.5774);
    tan_t!(t_pi3, FRAC_PI_3, 1.7321);
    tan_t!(t01, 0.1, 0.1003);
    tan_t!(t02, 0.2, 0.2027);
    tan_t!(t03, 0.3, 0.3093);
    tan_t!(t04, 0.4, 0.4228);
    tan_t!(t05, 0.5, 0.5463);
    tan_t!(t06, 0.6, 0.6841);
    tan_t!(t07, 0.7, 0.8423);
    tan_t!(t08, 0.8, 1.0296);
    tan_t!(t09, 0.9, 1.2602);
    tan_t!(t10, 1.0, 1.5574);
    tan_t!(t11, -0.1, -0.1003);
    tan_t!(t12, -0.2, -0.2027);
    tan_t!(t13, -0.3, -0.3093);
    tan_t!(t14, -0.4, -0.4228);
    tan_t!(t15, -0.5, -0.5463);
    tan_t!(t16, -0.6, -0.6841);
    tan_t!(t17, -0.7, -0.8423);
    tan_t!(t18, -0.8, -1.0296);
    tan_t!(t19, -0.9, -1.2602);
    tan_t!(t20, -1.0, -1.5574);
    #[test]
    fn null_in() {
        assert_eq!(ev("tan", &[null()]), null());
    }
    tan_t!(t21, 0.15, 0.1511);
    tan_t!(t22, 0.25, 0.2553);
    tan_t!(t23, 0.35, 0.3654);
    tan_t!(t24, 0.45, 0.4831);
    tan_t!(t25, 0.55, 0.6131);
}

// sqrt for known values
mod sqrt_tests {
    use super::*;
    macro_rules! sq {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("sqrt", &[f($v)]), $e, 0.001);
            }
        };
    }
    sq!(s00, 0.0, 0.0);
    sq!(s01, 1.0, 1.0);
    sq!(s02, 2.0, 1.4142);
    sq!(s03, 3.0, 1.7321);
    sq!(s04, 4.0, 2.0);
    sq!(s05, 5.0, 2.2361);
    sq!(s06, 6.0, 2.4495);
    sq!(s07, 7.0, 2.6458);
    sq!(s08, 8.0, 2.8284);
    sq!(s09, 9.0, 3.0);
    sq!(s10, 10.0, 3.1623);
    sq!(s11, 16.0, 4.0);
    sq!(s12, 25.0, 5.0);
    sq!(s13, 36.0, 6.0);
    sq!(s14, 49.0, 7.0);
    sq!(s15, 64.0, 8.0);
    sq!(s16, 81.0, 9.0);
    sq!(s17, 100.0, 10.0);
    sq!(s18, 121.0, 11.0);
    sq!(s19, 144.0, 12.0);
    sq!(s20, 169.0, 13.0);
    sq!(s21, 196.0, 14.0);
    sq!(s22, 225.0, 15.0);
    sq!(s23, 256.0, 16.0);
    sq!(s24, 289.0, 17.0);
    sq!(s25, 324.0, 18.0);
    sq!(s26, 361.0, 19.0);
    sq!(s27, 400.0, 20.0);
    sq!(s28, 0.25, 0.5);
    sq!(s29, 0.01, 0.1);
    sq!(s30, 0.04, 0.2);
    sq!(s31, 10000.0, 100.0);
    sq!(s32, 1000000.0, 1000.0);
    sq!(s33, 0.5, 0.7071);
    sq!(s34, 1.5, 1.2247);
    sq!(s35, 2.5, 1.5811);
    sq!(s36, 3.5, 1.8708);
    sq!(s37, 4.5, 2.1213);
    sq!(s38, 5.5, 2.3452);
    sq!(s39, 6.5, 2.5495);
    sq!(s40, 7.5, 2.7386);
    #[test]
    fn null_in() {
        assert_eq!(ev("sqrt", &[null()]), null());
    }
    #[test]
    fn int_49() {
        close(&ev("sqrt", &[i(49)]), 7.0, 0.001);
    }
    #[test]
    fn int_100() {
        close(&ev("sqrt", &[i(100)]), 10.0, 0.001);
    }
    #[test]
    fn int_0() {
        close(&ev("sqrt", &[i(0)]), 0.0, 0.001);
    }
    #[test]
    fn int_1() {
        close(&ev("sqrt", &[i(1)]), 1.0, 0.001);
    }
    #[test]
    fn int_4() {
        close(&ev("sqrt", &[i(4)]), 2.0, 0.001);
    }
    #[test]
    fn int_9() {
        close(&ev("sqrt", &[i(9)]), 3.0, 0.001);
    }
    #[test]
    fn int_16() {
        close(&ev("sqrt", &[i(16)]), 4.0, 0.001);
    }
    #[test]
    fn int_25() {
        close(&ev("sqrt", &[i(25)]), 5.0, 0.001);
    }
    #[test]
    fn int_36() {
        close(&ev("sqrt", &[i(36)]), 6.0, 0.001);
    }
    #[test]
    fn int_64() {
        close(&ev("sqrt", &[i(64)]), 8.0, 0.001);
    }
}

// pow for known values
mod pow_tests {
    use super::*;
    macro_rules! pw {
        ($n:ident, $b:expr, $e:expr, $r:expr) => {
            #[test]
            fn $n() {
                close(&ev("pow", &[f($b), f($e)]), $r, 0.01);
            }
        };
    }
    pw!(p01, 2.0, 0.0, 1.0);
    pw!(p02, 2.0, 1.0, 2.0);
    pw!(p03, 2.0, 2.0, 4.0);
    pw!(p04, 2.0, 3.0, 8.0);
    pw!(p05, 2.0, 4.0, 16.0);
    pw!(p06, 2.0, 5.0, 32.0);
    pw!(p07, 2.0, 6.0, 64.0);
    pw!(p08, 2.0, 7.0, 128.0);
    pw!(p09, 2.0, 8.0, 256.0);
    pw!(p10, 2.0, 9.0, 512.0);
    pw!(p11, 2.0, 10.0, 1024.0);
    pw!(p12, 3.0, 0.0, 1.0);
    pw!(p13, 3.0, 1.0, 3.0);
    pw!(p14, 3.0, 2.0, 9.0);
    pw!(p15, 3.0, 3.0, 27.0);
    pw!(p16, 3.0, 4.0, 81.0);
    pw!(p17, 3.0, 5.0, 243.0);
    pw!(p18, 4.0, 0.0, 1.0);
    pw!(p19, 4.0, 1.0, 4.0);
    pw!(p20, 4.0, 2.0, 16.0);
    pw!(p21, 4.0, 3.0, 64.0);
    pw!(p22, 5.0, 0.0, 1.0);
    pw!(p23, 5.0, 1.0, 5.0);
    pw!(p24, 5.0, 2.0, 25.0);
    pw!(p25, 5.0, 3.0, 125.0);
    pw!(p26, 10.0, 0.0, 1.0);
    pw!(p27, 10.0, 1.0, 10.0);
    pw!(p28, 10.0, 2.0, 100.0);
    pw!(p29, 10.0, 3.0, 1000.0);
    pw!(p30, 1.0, 100.0, 1.0);
    pw!(p31, 0.5, 2.0, 0.25);
    pw!(p32, 0.5, 3.0, 0.125);
    pw!(p33, 2.0, 0.5, 1.4142);
    pw!(p34, 4.0, 0.5, 2.0);
    pw!(p35, 9.0, 0.5, 3.0);
    pw!(p36, 2.0, -1.0, 0.5);
    pw!(p37, 2.0, -2.0, 0.25);
    pw!(p38, 10.0, -1.0, 0.1);
    pw!(p39, 10.0, -2.0, 0.01);
    pw!(p40, 6.0, 2.0, 36.0);
    pw!(p41, 7.0, 2.0, 49.0);
    pw!(p42, 8.0, 2.0, 64.0);
    pw!(p43, 9.0, 2.0, 81.0);
    pw!(p44, 11.0, 2.0, 121.0);
    pw!(p45, 12.0, 2.0, 144.0);
    #[test]
    fn null_in() {
        assert_eq!(ev("pow", &[null(), f(2.0)]), null());
    }
    #[test]
    fn power_alias() {
        close(&ev("power", &[f(2.0), f(3.0)]), 8.0, 0.001);
    }
}

// exp for known values
mod exp_tests {
    use super::*;
    macro_rules! ex {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("exp", &[f($v)]), $e, 0.01);
            }
        };
    }
    ex!(e01, 0.0, 1.0);
    ex!(e02, 1.0, 2.7183);
    ex!(e03, 2.0, 7.3891);
    ex!(e04, 3.0, 20.0855);
    ex!(e05, 4.0, 54.598);
    ex!(e06, 5.0, 148.413);
    ex!(e07, -1.0, 0.3679);
    ex!(e08, -2.0, 0.1353);
    ex!(e09, -3.0, 0.0498);
    ex!(e10, -4.0, 0.0183);
    ex!(e11, -5.0, 0.00674);
    ex!(e12, 0.5, 1.6487);
    ex!(e13, 1.5, 4.4817);
    ex!(e14, 2.5, 12.1825);
    ex!(e15, 0.1, 1.1052);
    ex!(e16, 0.2, 1.2214);
    ex!(e17, 0.3, 1.3499);
    ex!(e18, 0.4, 1.4918);
    ex!(e19, 0.6, 1.8221);
    ex!(e20, 0.7, 2.0138);
    ex!(e21, 0.8, 2.2255);
    ex!(e22, 0.9, 2.4596);
    ex!(e23, -0.5, 0.6065);
    ex!(e24, -1.5, 0.2231);
    ex!(e25, -2.5, 0.0821);
    ex!(e26, -0.1, 0.9048);
    ex!(e27, -0.2, 0.8187);
    ex!(e28, -0.3, 0.7408);
    ex!(e29, -0.4, 0.6703);
    ex!(e30, -0.6, 0.5488);
    #[test]
    fn null_in() {
        assert_eq!(ev("exp", &[null()]), null());
    }
    #[test]
    fn int_0() {
        close(&ev("exp", &[i(0)]), 1.0, 0.001);
    }
    #[test]
    fn int_1() {
        close(&ev("exp", &[i(1)]), 2.7183, 0.01);
    }
    #[test]
    fn int_2() {
        close(&ev("exp", &[i(2)]), 7.3891, 0.01);
    }
}

// log (natural) for known values
mod log_tests {
    use super::*;
    macro_rules! lg {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("log", &[f($v)]), $e, 0.01);
            }
        };
    }
    lg!(l01, 1.0, 0.0);
    lg!(l02, 2.718281828, 1.0);
    lg!(l03, 7.389, 2.0);
    lg!(l04, 20.086, 3.0);
    lg!(l05, 2.0, 0.6931);
    lg!(l06, 3.0, 1.0986);
    lg!(l07, 4.0, 1.3863);
    lg!(l08, 5.0, 1.6094);
    lg!(l09, 10.0, 2.3026);
    lg!(l10, 100.0, 4.6052);
    lg!(l11, 1000.0, 6.9078);
    lg!(l12, 0.5, -0.6931);
    lg!(l13, 0.1, -2.3026);
    lg!(l14, 0.01, -4.6052);
    lg!(l15, 50.0, 3.912);
    lg!(l16, 25.0, 3.2189);
    lg!(l17, 75.0, 4.3175);
    lg!(l18, 1.5, 0.4055);
    lg!(l19, 2.5, 0.9163);
    lg!(l20, 3.5, 1.2528);
    lg!(l21, 4.5, 1.5041);
    lg!(l22, 5.5, 1.7047);
    lg!(l23, 6.0, 1.7918);
    lg!(l24, 7.0, 1.9459);
    lg!(l25, 8.0, 2.0794);
    lg!(l26, 9.0, 2.1972);
    lg!(l27, 0.25, -1.3863);
    lg!(l28, 0.75, -0.2877);
    lg!(l29, 1.1, 0.0953);
    lg!(l30, 1.01, 0.00995);
    #[test]
    fn null_in() {
        assert_eq!(ev("log", &[null()]), null());
    }
}

// square / cbrt
mod square_cbrt {
    use super::*;
    macro_rules! sq {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("square", &[f($v)]), $e, 0.01);
            }
        };
    }
    macro_rules! cb {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("cbrt", &[f($v)]), $e, 0.01);
            }
        };
    }
    sq!(sq0, 0.0, 0.0);
    sq!(sq1, 1.0, 1.0);
    sq!(sq2, 2.0, 4.0);
    sq!(sq3, 3.0, 9.0);
    sq!(sq4, 4.0, 16.0);
    sq!(sq5, 5.0, 25.0);
    sq!(sq6, 6.0, 36.0);
    sq!(sq7, 7.0, 49.0);
    sq!(sq8, 8.0, 64.0);
    sq!(sq9, 9.0, 81.0);
    sq!(sq10, 10.0, 100.0);
    sq!(sqn1, -1.0, 1.0);
    sq!(sqn2, -2.0, 4.0);
    sq!(sqn3, -3.0, 9.0);
    sq!(sqn4, -4.0, 16.0);
    sq!(sqn5, -5.0, 25.0);
    sq!(sq_half, 0.5, 0.25);
    sq!(sq_tenth, 0.1, 0.01);
    sq!(sq11, 11.0, 121.0);
    sq!(sq12, 12.0, 144.0);
    sq!(sq13, 13.0, 169.0);
    sq!(sq14, 14.0, 196.0);
    sq!(sq15, 15.0, 225.0);
    sq!(sq20, 20.0, 400.0);
    #[test]
    fn sq_null() {
        assert_eq!(ev("square", &[null()]), null());
    }
    #[test]
    fn sq_int() {
        close(&ev("square", &[i(5)]), 25.0, 0.001);
    }
    cb!(cb0, 0.0, 0.0);
    cb!(cb1, 1.0, 1.0);
    cb!(cb8, 8.0, 2.0);
    cb!(cb27, 27.0, 3.0);
    cb!(cb64, 64.0, 4.0);
    cb!(cb125, 125.0, 5.0);
    cb!(cb216, 216.0, 6.0);
    cb!(cb1000, 1000.0, 10.0);
    #[test]
    fn cb_null() {
        assert_eq!(ev("cbrt", &[null()]), null());
    }
}

// sin for integer inputs
mod sin_int {
    use super::*;
    macro_rules! si {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("sin", &[i($v)]), $e, 0.001);
            }
        };
    }
    si!(i0, 0, 0.0);
    si!(i1, 1, 0.8415);
    si!(i2, 2, 0.9093);
    si!(i3, 3, 0.1411);
    si!(i4, 4, -0.7568);
    si!(i5, 5, -0.9589);
    si!(i6, 6, -0.2794);
    si!(in1, -1, -0.8415);
    si!(in2, -2, -0.9093);
    si!(in3, -3, -0.1411);
}

// cos for integer inputs
mod cos_int {
    use super::*;
    macro_rules! ci {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("cos", &[i($v)]), $e, 0.001);
            }
        };
    }
    ci!(i0, 0, 1.0);
    ci!(i1, 1, 0.5403);
    ci!(i2, 2, -0.4161);
    ci!(i3, 3, -0.9900);
    ci!(i4, 4, -0.6536);
    ci!(i5, 5, 0.2837);
    ci!(i6, 6, 0.9602);
    ci!(in1, -1, 0.5403);
    ci!(in2, -2, -0.4161);
    ci!(in3, -3, -0.9900);
}

// exp for more values
mod exp_extra {
    use super::*;
    macro_rules! ex {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("exp", &[f($v)]), $e, 0.01);
            }
        };
    }
    ex!(e31, 0.05, 1.0513);
    ex!(e32, 0.15, 1.1618);
    ex!(e33, 0.25, 1.2840);
    ex!(e34, 0.35, 1.4191);
    ex!(e35, 0.45, 1.5683);
    ex!(e36, 0.55, 1.7333);
    ex!(e37, 0.65, 1.9155);
    ex!(e38, 0.75, 2.1170);
    ex!(e39, 0.85, 2.3396);
    ex!(e40, 0.95, 2.5857);
}

// log for more values
mod log_extra {
    use super::*;
    macro_rules! lg {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("log", &[f($v)]), $e, 0.01);
            }
        };
    }
    lg!(l31, 11.0, 2.3979);
    lg!(l32, 12.0, 2.4849);
    lg!(l33, 13.0, 2.5649);
    lg!(l34, 14.0, 2.6391);
    lg!(l35, 15.0, 2.7081);
    lg!(l36, 16.0, 2.7726);
    lg!(l37, 17.0, 2.8332);
    lg!(l38, 18.0, 2.8904);
    lg!(l39, 19.0, 2.9444);
    lg!(l40, 20.0, 2.9957);
}

// pow for integer bases
mod pow_int {
    use super::*;
    macro_rules! pw {
        ($n:ident, $b:expr, $e:expr, $r:expr) => {
            #[test]
            fn $n() {
                close(&ev("pow", &[i($b), i($e)]), $r, 0.01);
            }
        };
    }
    pw!(p01, 2, 0, 1.0);
    pw!(p02, 2, 1, 2.0);
    pw!(p03, 2, 2, 4.0);
    pw!(p04, 2, 3, 8.0);
    pw!(p05, 2, 4, 16.0);
    pw!(p06, 2, 5, 32.0);
    pw!(p07, 2, 10, 1024.0);
    pw!(p08, 3, 0, 1.0);
    pw!(p09, 3, 1, 3.0);
    pw!(p10, 3, 2, 9.0);
    pw!(p11, 3, 3, 27.0);
    pw!(p12, 3, 4, 81.0);
    pw!(p13, 5, 2, 25.0);
    pw!(p14, 5, 3, 125.0);
    pw!(p15, 10, 2, 100.0);
    pw!(p16, 10, 3, 1000.0);
    pw!(p17, 10, 4, 10000.0);
    pw!(p18, 7, 2, 49.0);
    pw!(p19, 7, 3, 343.0);
    pw!(p20, 1, 100, 1.0);
}

// Additional sin values at 0.05 increments — use runtime computation for accuracy
mod sin_fine {
    use super::*;
    macro_rules! sf {
        ($n:ident, $v:expr) => {
            #[test]
            fn $n() {
                let expected = ($v as f64).sin();
                close(&ev("sin", &[f($v)]), expected, 0.001);
            }
        };
    }
    sf!(s001, 0.05);
    sf!(s002, 1.55);
    sf!(s003, 1.6);
    sf!(s004, 1.65);
    sf!(s005, 1.7);
    sf!(s006, 1.75);
    sf!(s007, 1.8);
    sf!(s008, 1.85);
    sf!(s009, 1.9);
    sf!(s010, 1.95);
    sf!(s011, 2.05);
    sf!(s012, 2.1);
    sf!(s013, 2.15);
    sf!(s014, 2.2);
    sf!(s015, 2.25);
    sf!(s016, 2.3);
    sf!(s017, 2.35);
    sf!(s018, 2.4);
    sf!(s019, 2.45);
    sf!(s020, 2.55);
    sf!(s021, 2.6);
    sf!(s022, 2.65);
    sf!(s023, 2.7);
    sf!(s024, 2.75);
    sf!(s025, 2.8);
    sf!(s026, 2.85);
    sf!(s027, 2.9);
    sf!(s028, 2.95);
    sf!(s029, 3.05);
    sf!(s030, 3.1);
}

// Additional cos values at 0.05 increments — use runtime computation for accuracy
mod cos_fine {
    use super::*;
    macro_rules! cf {
        ($n:ident, $v:expr) => {
            #[test]
            fn $n() {
                let expected = ($v as f64).cos();
                close(&ev("cos", &[f($v)]), expected, 0.001);
            }
        };
    }
    cf!(c001, 0.05);
    cf!(c002, 1.55);
    cf!(c003, 1.6);
    cf!(c004, 1.65);
    cf!(c005, 1.7);
    cf!(c006, 1.75);
    cf!(c007, 1.8);
    cf!(c008, 1.85);
    cf!(c009, 1.9);
    cf!(c010, 1.95);
    cf!(c011, 2.05);
    cf!(c012, 2.1);
    cf!(c013, 2.15);
    cf!(c014, 2.2);
    cf!(c015, 2.25);
    cf!(c016, 2.3);
    cf!(c017, 2.35);
    cf!(c018, 2.4);
    cf!(c019, 2.45);
    cf!(c020, 2.55);
    cf!(c021, 2.6);
    cf!(c022, 2.65);
    cf!(c023, 2.7);
    cf!(c024, 2.75);
    cf!(c025, 2.8);
    cf!(c026, 2.85);
    cf!(c027, 2.9);
    cf!(c028, 2.95);
    cf!(c029, 3.05);
    cf!(c030, 3.1);
}

// sqrt for perfect squares up to 30^2
mod sqrt_perfect {
    use super::*;
    macro_rules! sqp {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("sqrt", &[f($v)]), $e, 0.001);
            }
        };
    }
    sqp!(s441, 441.0, 21.0);
    sqp!(s484, 484.0, 22.0);
    sqp!(s529, 529.0, 23.0);
    sqp!(s576, 576.0, 24.0);
    sqp!(s625, 625.0, 25.0);
    sqp!(s676, 676.0, 26.0);
    sqp!(s729, 729.0, 27.0);
    sqp!(s784, 784.0, 28.0);
    sqp!(s841, 841.0, 29.0);
    sqp!(s900, 900.0, 30.0);
}

// pow: 2^N for N=11..20
mod pow_2n {
    use super::*;
    macro_rules! p2 {
        ($n:ident, $exp:expr, $r:expr) => {
            #[test]
            fn $n() {
                close(&ev("pow", &[f(2.0), f($exp)]), $r, 0.01);
            }
        };
    }
    p2!(p11, 11.0, 2048.0);
    p2!(p12, 12.0, 4096.0);
    p2!(p13, 13.0, 8192.0);
    p2!(p14, 14.0, 16384.0);
    p2!(p15, 15.0, 32768.0);
    p2!(p16, 16.0, 65536.0);
    p2!(p17, 17.0, 131072.0);
    p2!(p18, 18.0, 262144.0);
    p2!(p19, 19.0, 524288.0);
    p2!(p20, 20.0, 1048576.0);
}

// exp for more fractional values
mod exp_frac {
    use super::*;
    macro_rules! ef {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("exp", &[f($v)]), $e, 0.01);
            }
        };
    }
    ef!(e01, 1.1, 3.0042);
    ef!(e02, 1.2, 3.3201);
    ef!(e03, 1.3, 3.6693);
    ef!(e04, 1.4, 4.0552);
    ef!(e05, 1.6, 4.9530);
    ef!(e06, 1.7, 5.4739);
    ef!(e07, 1.8, 6.0496);
    ef!(e08, 1.9, 6.6859);
    ef!(e09, 2.1, 8.1662);
    ef!(e10, 2.2, 9.0250);
    ef!(e11, 2.3, 9.9749);
    ef!(e12, 2.4, 11.0232);
    ef!(e13, 2.6, 13.4637);
    ef!(e14, 2.7, 14.8797);
    ef!(e15, 2.8, 16.4446);
    ef!(e16, 2.9, 18.1741);
    ef!(e17, 3.1, 22.1980);
    ef!(e18, 3.2, 24.5325);
    ef!(e19, 3.3, 27.1126);
    ef!(e20, 3.4, 29.9641);
    ef!(e21, 3.5, 33.1155);
    ef!(e22, 3.6, 36.5982);
    ef!(e23, 3.7, 40.4473);
    ef!(e24, 3.8, 44.7012);
    ef!(e25, 3.9, 49.4024);
}

// log for more values
mod log_frac {
    use super::*;
    macro_rules! lf {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("log", &[f($v)]), $e, 0.01);
            }
        };
    }
    lf!(l01, 1.2, 0.1823);
    lf!(l02, 1.3, 0.2624);
    lf!(l03, 1.4, 0.3365);
    lf!(l04, 1.6, 0.4700);
    lf!(l05, 1.7, 0.5306);
    lf!(l06, 1.8, 0.5878);
    lf!(l07, 1.9, 0.6419);
    lf!(l08, 2.1, 0.7419);
    lf!(l09, 2.2, 0.7885);
    lf!(l10, 2.3, 0.8329);
    lf!(l11, 2.4, 0.8755);
    lf!(l12, 2.6, 0.9555);
    lf!(l13, 2.7, 0.9933);
    lf!(l14, 2.8, 1.0296);
    lf!(l15, 2.9, 1.0647);
    lf!(l16, 3.1, 1.1314);
    lf!(l17, 3.2, 1.1632);
    lf!(l18, 3.3, 1.1939);
    lf!(l19, 3.4, 1.2238);
    lf!(l20, 3.6, 1.2809);
}

// square for more values
mod square_extra {
    use super::*;
    macro_rules! sq {
        ($n:ident, $v:expr, $e:expr) => {
            #[test]
            fn $n() {
                close(&ev("square", &[f($v)]), $e, 0.01);
            }
        };
    }
    sq!(s16, 16.0, 256.0);
    sq!(s17, 17.0, 289.0);
    sq!(s18, 18.0, 324.0);
    sq!(s19, 19.0, 361.0);
    sq!(s21, 21.0, 441.0);
    sq!(s22, 22.0, 484.0);
    sq!(s23, 23.0, 529.0);
    sq!(s24, 24.0, 576.0);
    sq!(s25, 25.0, 625.0);
    sq!(s30, 30.0, 900.0);
    sq!(s50, 50.0, 2500.0);
    sq!(s100, 100.0, 10000.0);
    sq!(s_half, 0.5, 0.25);
    sq!(s_quarter, 0.25, 0.0625);
    sq!(s_third, 0.333, 0.1109);
    sq!(sn10, -10.0, 100.0);
    sq!(sn20, -20.0, 400.0);
    sq!(sn50, -50.0, 2500.0);
    sq!(s1_5, 1.5, 2.25);
    sq!(s2_5, 2.5, 6.25);
}
