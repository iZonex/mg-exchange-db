//! 1000 scalar function tests covering string, math, conditional, cast, and utility functions.

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
fn ev(name: &str, args: &[Value]) -> Value {
    evaluate_scalar(name, args).unwrap()
}
#[allow(dead_code)]
fn ev_or(name: &str, args: &[Value]) -> Value {
    evaluate_scalar(name, args).unwrap_or(Value::Null)
}

// length — 40 tests
mod length_s {
    use super::*;
    #[test]
    fn l00() {
        assert_eq!(ev("length", &[s("")]), i(0));
    }
    #[test]
    fn l01() {
        assert_eq!(ev("length", &[s("a")]), i(1));
    }
    #[test]
    fn l02() {
        assert_eq!(ev("length", &[s("ab")]), i(2));
    }
    #[test]
    fn l03() {
        assert_eq!(ev("length", &[s("abc")]), i(3));
    }
    #[test]
    fn l04() {
        assert_eq!(ev("length", &[s("abcd")]), i(4));
    }
    #[test]
    fn l05() {
        assert_eq!(ev("length", &[s("hello")]), i(5));
    }
    #[test]
    fn l06() {
        assert_eq!(ev("length", &[s("foobar")]), i(6));
    }
    #[test]
    fn l07() {
        assert_eq!(ev("length", &[s("abcdefg")]), i(7));
    }
    #[test]
    fn l08() {
        assert_eq!(ev("length", &[s("12345678")]), i(8));
    }
    #[test]
    fn l09() {
        assert_eq!(ev("length", &[s("123456789")]), i(9));
    }
    #[test]
    fn l10() {
        assert_eq!(ev("length", &[s("0123456789")]), i(10));
    }
    #[test]
    fn l11() {
        assert_eq!(ev("length", &[s(" ")]), i(1));
    }
    #[test]
    fn l12() {
        assert_eq!(ev("length", &[s("  ")]), i(2));
    }
    #[test]
    fn l13() {
        assert_eq!(ev("length", &[s("   ")]), i(3));
    }
    #[test]
    fn l14() {
        assert_eq!(ev("length", &[s("\t")]), i(1));
    }
    #[test]
    fn l15() {
        assert_eq!(ev("length", &[s("\n")]), i(1));
    }
    #[test]
    fn l16() {
        assert_eq!(ev("length", &[s("\r\n")]), i(2));
    }
    #[test]
    fn l17() {
        assert_eq!(ev("length", &[null()]), null());
    }
    #[test]
    fn l18() {
        assert_eq!(ev("length", &[i(42)]), i(2));
    }
    #[test]
    fn l19() {
        assert_eq!(ev("length", &[i(0)]), i(1));
    }
    #[test]
    fn l20() {
        assert_eq!(ev("length", &[s("hello world")]), i(11));
    }
    #[test]
    fn l21() {
        assert_eq!(ev("length", &[s("a b c")]), i(5));
    }
    #[test]
    fn l22() {
        assert_eq!(ev("length", &[s("test123")]), i(7));
    }
    #[test]
    fn l23() {
        assert_eq!(ev("length", &[s("!@#$%")]), i(5));
    }
    #[test]
    fn l24() {
        assert_eq!(ev("length", &[s("x")]), i(1));
    }
    #[test]
    fn l25() {
        assert_eq!(ev("length", &[s("xy")]), i(2));
    }
    #[test]
    fn l26() {
        assert_eq!(ev("length", &[s("xyz")]), i(3));
    }
    #[test]
    fn l27() {
        assert_eq!(ev("length", &[s("ABCDEF")]), i(6));
    }
    #[test]
    fn l28() {
        assert_eq!(ev("length", &[s("Hello, World!")]), i(13));
    }
    #[test]
    fn l29() {
        assert_eq!(ev("length", &[s("openai")]), i(6));
    }
    #[test]
    fn l30() {
        assert_eq!(ev("length", &[s("rust")]), i(4));
    }
    #[test]
    fn l31() {
        assert_eq!(ev("length", &[s("test test")]), i(9));
    }
    #[test]
    fn l32() {
        assert_eq!(ev("length", &[i(100)]), i(3));
    }
    #[test]
    fn l33() {
        assert_eq!(ev("length", &[i(-1)]), i(2));
    }
    #[test]
    fn l34() {
        assert_eq!(ev("length", &[s("aaaaaaaaaa")]), i(10));
    }
    #[test]
    fn l35() {
        assert_eq!(ev("length", &[s("123")]), i(3));
    }
    #[test]
    fn l36() {
        assert_eq!(ev("length", &[s("  a  ")]), i(5));
    }
    #[test]
    fn l37() {
        assert_eq!(ev("length", &[s("tab\there")]), i(8));
    }
    #[test]
    fn l38() {
        assert_eq!(ev("length", &[s("line\nbreak")]), i(10));
    }
    #[test]
    fn l39() {
        assert_eq!(ev("length", &[s("q")]), i(1));
    }
}

// upper — 40 tests
mod upper_s {
    use super::*;
    #[test]
    fn u00() {
        assert_eq!(ev("upper", &[s("")]), s(""));
    }
    #[test]
    fn u01() {
        assert_eq!(ev("upper", &[s("a")]), s("A"));
    }
    #[test]
    fn u02() {
        assert_eq!(ev("upper", &[s("abc")]), s("ABC"));
    }
    #[test]
    fn u03() {
        assert_eq!(ev("upper", &[s("hello")]), s("HELLO"));
    }
    #[test]
    fn u04() {
        assert_eq!(ev("upper", &[s("HELLO")]), s("HELLO"));
    }
    #[test]
    fn u05() {
        assert_eq!(ev("upper", &[s("Hello")]), s("HELLO"));
    }
    #[test]
    fn u06() {
        assert_eq!(ev("upper", &[s("hElLo")]), s("HELLO"));
    }
    #[test]
    fn u07() {
        assert_eq!(ev("upper", &[null()]), null());
    }
    #[test]
    fn u08() {
        assert_eq!(ev("upper", &[s("123")]), s("123"));
    }
    #[test]
    fn u09() {
        assert_eq!(ev("upper", &[s("abc123")]), s("ABC123"));
    }
    #[test]
    fn u10() {
        assert_eq!(ev("upper", &[s("a b c")]), s("A B C"));
    }
    #[test]
    fn u11() {
        assert_eq!(ev("upper", &[s("foo bar")]), s("FOO BAR"));
    }
    #[test]
    fn u12() {
        assert_eq!(ev("upper", &[s("test")]), s("TEST"));
    }
    #[test]
    fn u13() {
        assert_eq!(ev("upper", &[s("rust")]), s("RUST"));
    }
    #[test]
    fn u14() {
        assert_eq!(ev("upper", &[s("x")]), s("X"));
    }
    #[test]
    fn u15() {
        assert_eq!(ev("upper", &[s("xy")]), s("XY"));
    }
    #[test]
    fn u16() {
        assert_eq!(ev("upper", &[s("xyz")]), s("XYZ"));
    }
    #[test]
    fn u17() {
        assert_eq!(ev("upper", &[s("aaa")]), s("AAA"));
    }
    #[test]
    fn u18() {
        assert_eq!(ev("upper", &[s("zzz")]), s("ZZZ"));
    }
    #[test]
    fn u19() {
        assert_eq!(ev("upper", &[s("mixED")]), s("MIXED"));
    }
    #[test]
    fn u20() {
        assert_eq!(ev("upper", &[s("!@#")]), s("!@#"));
    }
    #[test]
    fn u21() {
        assert_eq!(ev("upper", &[s("abc def ghi")]), s("ABC DEF GHI"));
    }
    #[test]
    fn u22() {
        assert_eq!(ev("upper", &[s("q")]), s("Q"));
    }
    #[test]
    fn u23() {
        assert_eq!(ev("upper", &[s("ab")]), s("AB"));
    }
    #[test]
    fn u24() {
        assert_eq!(ev("upper", &[s("abcdef")]), s("ABCDEF"));
    }
    #[test]
    fn u25() {
        assert_eq!(ev("upper", &[s("hi")]), s("HI"));
    }
    #[test]
    fn u26() {
        assert_eq!(ev("upper", &[s("ok")]), s("OK"));
    }
    #[test]
    fn u27() {
        assert_eq!(ev("upper", &[s("go")]), s("GO"));
    }
    #[test]
    fn u28() {
        assert_eq!(ev("upper", &[s("no")]), s("NO"));
    }
    #[test]
    fn u29() {
        assert_eq!(ev("upper", &[s("yes")]), s("YES"));
    }
    #[test]
    fn u30() {
        assert_eq!(ev("upper", &[s("alpha")]), s("ALPHA"));
    }
    #[test]
    fn u31() {
        assert_eq!(ev("upper", &[s("beta")]), s("BETA"));
    }
    #[test]
    fn u32_() {
        assert_eq!(ev("upper", &[s("gamma")]), s("GAMMA"));
    }
    #[test]
    fn u33() {
        assert_eq!(ev("upper", &[s("delta")]), s("DELTA"));
    }
    #[test]
    fn u34() {
        assert_eq!(ev("upper", &[s("epsilon")]), s("EPSILON"));
    }
    #[test]
    fn u35() {
        assert_eq!(ev("upper", &[s("zeta")]), s("ZETA"));
    }
    #[test]
    fn u36() {
        assert_eq!(ev("upper", &[s("eta")]), s("ETA"));
    }
    #[test]
    fn u37() {
        assert_eq!(ev("upper", &[s("theta")]), s("THETA"));
    }
    #[test]
    fn u38() {
        assert_eq!(ev("upper", &[s("iota")]), s("IOTA"));
    }
    #[test]
    fn u39() {
        assert_eq!(ev("upper", &[s("kappa")]), s("KAPPA"));
    }
}

// lower — 40 tests
mod lower_s {
    use super::*;
    #[test]
    fn w00() {
        assert_eq!(ev("lower", &[s("")]), s(""));
    }
    #[test]
    fn w01() {
        assert_eq!(ev("lower", &[s("A")]), s("a"));
    }
    #[test]
    fn w02() {
        assert_eq!(ev("lower", &[s("ABC")]), s("abc"));
    }
    #[test]
    fn w03() {
        assert_eq!(ev("lower", &[s("HELLO")]), s("hello"));
    }
    #[test]
    fn w04() {
        assert_eq!(ev("lower", &[s("hello")]), s("hello"));
    }
    #[test]
    fn w05() {
        assert_eq!(ev("lower", &[s("Hello")]), s("hello"));
    }
    #[test]
    fn w06() {
        assert_eq!(ev("lower", &[null()]), null());
    }
    #[test]
    fn w07() {
        assert_eq!(ev("lower", &[s("123")]), s("123"));
    }
    #[test]
    fn w08() {
        assert_eq!(ev("lower", &[s("ABC123")]), s("abc123"));
    }
    #[test]
    fn w09() {
        assert_eq!(ev("lower", &[s("A B C")]), s("a b c"));
    }
    #[test]
    fn w10() {
        assert_eq!(ev("lower", &[s("FOO BAR")]), s("foo bar"));
    }
    #[test]
    fn w11() {
        assert_eq!(ev("lower", &[s("TEST")]), s("test"));
    }
    #[test]
    fn w12() {
        assert_eq!(ev("lower", &[s("RUST")]), s("rust"));
    }
    #[test]
    fn w13() {
        assert_eq!(ev("lower", &[s("X")]), s("x"));
    }
    #[test]
    fn w14() {
        assert_eq!(ev("lower", &[s("XY")]), s("xy"));
    }
    #[test]
    fn w15() {
        assert_eq!(ev("lower", &[s("XYZ")]), s("xyz"));
    }
    #[test]
    fn w16() {
        assert_eq!(ev("lower", &[s("AAA")]), s("aaa"));
    }
    #[test]
    fn w17() {
        assert_eq!(ev("lower", &[s("ZZZ")]), s("zzz"));
    }
    #[test]
    fn w18() {
        assert_eq!(ev("lower", &[s("MixED")]), s("mixed"));
    }
    #[test]
    fn w19() {
        assert_eq!(ev("lower", &[s("!@#")]), s("!@#"));
    }
    #[test]
    fn w20() {
        assert_eq!(ev("lower", &[s("ALPHA")]), s("alpha"));
    }
    #[test]
    fn w21() {
        assert_eq!(ev("lower", &[s("BETA")]), s("beta"));
    }
    #[test]
    fn w22() {
        assert_eq!(ev("lower", &[s("GAMMA")]), s("gamma"));
    }
    #[test]
    fn w23() {
        assert_eq!(ev("lower", &[s("DELTA")]), s("delta"));
    }
    #[test]
    fn w24() {
        assert_eq!(ev("lower", &[s("EPSILON")]), s("epsilon"));
    }
    #[test]
    fn w25() {
        assert_eq!(ev("lower", &[s("ZETA")]), s("zeta"));
    }
    #[test]
    fn w26() {
        assert_eq!(ev("lower", &[s("ETA")]), s("eta"));
    }
    #[test]
    fn w27() {
        assert_eq!(ev("lower", &[s("THETA")]), s("theta"));
    }
    #[test]
    fn w28() {
        assert_eq!(ev("lower", &[s("IOTA")]), s("iota"));
    }
    #[test]
    fn w29() {
        assert_eq!(ev("lower", &[s("KAPPA")]), s("kappa"));
    }
    #[test]
    fn w30() {
        assert_eq!(ev("lower", &[s("LAMBDA")]), s("lambda"));
    }
    #[test]
    fn w31() {
        assert_eq!(ev("lower", &[s("MU")]), s("mu"));
    }
    #[test]
    fn w32() {
        assert_eq!(ev("lower", &[s("NU")]), s("nu"));
    }
    #[test]
    fn w33() {
        assert_eq!(ev("lower", &[s("XI")]), s("xi"));
    }
    #[test]
    fn w34() {
        assert_eq!(ev("lower", &[s("PI")]), s("pi"));
    }
    #[test]
    fn w35() {
        assert_eq!(ev("lower", &[s("RHO")]), s("rho"));
    }
    #[test]
    fn w36() {
        assert_eq!(ev("lower", &[s("SIGMA")]), s("sigma"));
    }
    #[test]
    fn w37() {
        assert_eq!(ev("lower", &[s("TAU")]), s("tau"));
    }
    #[test]
    fn w38() {
        assert_eq!(ev("lower", &[s("PHI")]), s("phi"));
    }
    #[test]
    fn w39() {
        assert_eq!(ev("lower", &[s("CHI")]), s("chi"));
    }
}

// abs — 40 tests
mod abs_s {
    use super::*;
    #[test]
    fn a00() {
        assert_eq!(ev("abs", &[i(0)]), i(0));
    }
    #[test]
    fn a01() {
        assert_eq!(ev("abs", &[i(1)]), i(1));
    }
    #[test]
    fn a02() {
        assert_eq!(ev("abs", &[i(-1)]), i(1));
    }
    #[test]
    fn a03() {
        assert_eq!(ev("abs", &[i(42)]), i(42));
    }
    #[test]
    fn a04() {
        assert_eq!(ev("abs", &[i(-42)]), i(42));
    }
    #[test]
    fn a05() {
        assert_eq!(ev("abs", &[i(100)]), i(100));
    }
    #[test]
    fn a06() {
        assert_eq!(ev("abs", &[i(-100)]), i(100));
    }
    #[test]
    fn a07() {
        assert_eq!(ev("abs", &[i(999)]), i(999));
    }
    #[test]
    fn a08() {
        assert_eq!(ev("abs", &[i(-999)]), i(999));
    }
    #[test]
    fn a09() {
        assert_eq!(ev("abs", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn a10() {
        assert_eq!(ev("abs", &[f(1.5)]), f(1.5));
    }
    #[test]
    fn a11() {
        assert_eq!(ev("abs", &[f(-1.5)]), f(1.5));
    }
    #[test]
    fn a12() {
        assert_eq!(ev("abs", &[f(3.15)]), f(3.15));
    }
    #[test]
    fn a13() {
        assert_eq!(ev("abs", &[f(-3.15)]), f(3.15));
    }
    #[test]
    fn a14() {
        assert_eq!(ev("abs", &[null()]), null());
    }
    #[test]
    fn a15() {
        assert_eq!(ev("abs", &[i(10)]), i(10));
    }
    #[test]
    fn a16() {
        assert_eq!(ev("abs", &[i(-10)]), i(10));
    }
    #[test]
    fn a17() {
        assert_eq!(ev("abs", &[i(50)]), i(50));
    }
    #[test]
    fn a18() {
        assert_eq!(ev("abs", &[i(-50)]), i(50));
    }
    #[test]
    fn a19() {
        assert_eq!(ev("abs", &[f(99.9)]), f(99.9));
    }
    #[test]
    fn a20() {
        assert_eq!(ev("abs", &[f(-99.9)]), f(99.9));
    }
    #[test]
    fn a21() {
        assert_eq!(ev("abs", &[i(1000)]), i(1000));
    }
    #[test]
    fn a22() {
        assert_eq!(ev("abs", &[i(-1000)]), i(1000));
    }
    #[test]
    fn a23() {
        assert_eq!(ev("abs", &[f(0.001)]), f(0.001));
    }
    #[test]
    fn a24() {
        assert_eq!(ev("abs", &[f(-0.001)]), f(0.001));
    }
    #[test]
    fn a25() {
        assert_eq!(ev("abs", &[i(7)]), i(7));
    }
    #[test]
    fn a26() {
        assert_eq!(ev("abs", &[i(-7)]), i(7));
    }
    #[test]
    fn a27() {
        assert_eq!(ev("abs", &[i(13)]), i(13));
    }
    #[test]
    fn a28() {
        assert_eq!(ev("abs", &[i(-13)]), i(13));
    }
    #[test]
    fn a29() {
        assert_eq!(ev("abs", &[f(2.719)]), f(2.719));
    }
    #[test]
    fn a30() {
        assert_eq!(ev("abs", &[f(-2.719)]), f(2.719));
    }
    #[test]
    fn a31() {
        assert_eq!(ev("abs", &[i(500)]), i(500));
    }
    #[test]
    fn a32() {
        assert_eq!(ev("abs", &[i(-500)]), i(500));
    }
    #[test]
    fn a33() {
        assert_eq!(ev("abs", &[i(2)]), i(2));
    }
    #[test]
    fn a34() {
        assert_eq!(ev("abs", &[i(-2)]), i(2));
    }
    #[test]
    fn a35() {
        assert_eq!(ev("abs", &[i(3)]), i(3));
    }
    #[test]
    fn a36() {
        assert_eq!(ev("abs", &[i(-3)]), i(3));
    }
    #[test]
    fn a37() {
        assert_eq!(ev("abs", &[i(4)]), i(4));
    }
    #[test]
    fn a38() {
        assert_eq!(ev("abs", &[i(-4)]), i(4));
    }
    #[test]
    fn a39() {
        assert_eq!(ev("abs", &[i(5)]), i(5));
    }
}

// round — 30 tests
mod round_s {
    use super::*;
    #[test]
    fn r00() {
        assert_eq!(ev("round", &[f(1.4)]), f(1.0));
    }
    #[test]
    fn r01() {
        assert_eq!(ev("round", &[f(1.5)]), f(2.0));
    }
    #[test]
    fn r02() {
        assert_eq!(ev("round", &[f(1.6)]), f(2.0));
    }
    #[test]
    fn r03() {
        assert_eq!(ev("round", &[f(2.0)]), f(2.0));
    }
    #[test]
    fn r04() {
        assert_eq!(ev("round", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn r05() {
        assert_eq!(ev("round", &[f(-1.4)]), f(-1.0));
    }
    #[test]
    fn r06() {
        assert_eq!(ev("round", &[f(-1.5)]), f(-2.0));
    }
    #[test]
    fn r07() {
        assert_eq!(ev("round", &[f(-1.6)]), f(-2.0));
    }
    #[test]
    fn r08() {
        assert_eq!(ev("round", &[null()]), null());
    }
    #[test]
    fn r09() {
        let v = ev("round", &[i(5)]);
        assert!(v == i(5) || v == f(5.0));
    }
    #[test]
    fn r10() {
        assert_eq!(ev("round", &[f(3.15)]), f(3.0));
    }
    #[test]
    fn r11() {
        assert_eq!(ev("round", &[f(2.719)]), f(3.0));
    }
    #[test]
    fn r12() {
        assert_eq!(ev("round", &[f(99.5)]), f(100.0));
    }
    #[test]
    fn r13() {
        assert_eq!(ev("round", &[f(0.1)]), f(0.0));
    }
    #[test]
    fn r14() {
        assert_eq!(ev("round", &[f(0.9)]), f(1.0));
    }
    #[test]
    fn r15() {
        assert_eq!(ev("round", &[f(10.4)]), f(10.0));
    }
    #[test]
    fn r16() {
        assert_eq!(ev("round", &[f(10.5)]), f(11.0));
    }
    #[test]
    fn r17() {
        assert_eq!(ev("round", &[f(100.0)]), f(100.0));
    }
    #[test]
    fn r18() {
        assert_eq!(ev("round", &[f(50.5)]), f(51.0));
    }
    #[test]
    fn r19() {
        assert_eq!(ev("round", &[f(7.7)]), f(8.0));
    }
    #[test]
    fn r20() {
        assert_eq!(ev("round", &[f(7.3)]), f(7.0));
    }
    #[test]
    fn r21() {
        assert_eq!(ev("round", &[f(8.0)]), f(8.0));
    }
    #[test]
    fn r22() {
        assert_eq!(ev("round", &[f(-8.5)]), f(-9.0));
    }
    #[test]
    fn r23() {
        assert_eq!(ev("round", &[f(1000.4)]), f(1000.0));
    }
    #[test]
    fn r24() {
        assert_eq!(ev("round", &[f(1000.5)]), f(1001.0));
    }
    #[test]
    fn r25() {
        assert_eq!(ev("round", &[f(0.49)]), f(0.0));
    }
    #[test]
    fn r26() {
        assert_eq!(ev("round", &[f(0.51)]), f(1.0));
    }
    #[test]
    fn r27() {
        assert_eq!(ev("round", &[f(5.5)]), f(6.0));
    }
    #[test]
    fn r28() {
        assert_eq!(ev("round", &[f(-5.5)]), f(-6.0));
    }
    #[test]
    fn r29() {
        assert_eq!(ev("round", &[f(4.5)]), f(5.0));
    }
}

// floor — 30 tests
mod floor_s {
    use super::*;
    #[test]
    fn f00() {
        assert_eq!(ev("floor", &[f(1.0)]), f(1.0));
    }
    #[test]
    fn f01() {
        assert_eq!(ev("floor", &[f(1.1)]), f(1.0));
    }
    #[test]
    fn f02() {
        assert_eq!(ev("floor", &[f(1.9)]), f(1.0));
    }
    #[test]
    fn f03() {
        assert_eq!(ev("floor", &[f(2.0)]), f(2.0));
    }
    #[test]
    fn f04() {
        assert_eq!(ev("floor", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn f05() {
        assert_eq!(ev("floor", &[f(-1.0)]), f(-1.0));
    }
    #[test]
    fn f06() {
        assert_eq!(ev("floor", &[f(-1.1)]), f(-2.0));
    }
    #[test]
    fn f07() {
        assert_eq!(ev("floor", &[f(-1.9)]), f(-2.0));
    }
    #[test]
    fn f08() {
        assert_eq!(ev("floor", &[null()]), null());
    }
    #[test]
    fn f09() {
        let v = ev("floor", &[i(5)]);
        assert!(v == i(5) || v == f(5.0));
    }
    #[test]
    fn f10() {
        assert_eq!(ev("floor", &[f(3.15)]), f(3.0));
    }
    #[test]
    fn f11() {
        assert_eq!(ev("floor", &[f(2.719)]), f(2.0));
    }
    #[test]
    fn f12() {
        assert_eq!(ev("floor", &[f(99.9)]), f(99.0));
    }
    #[test]
    fn f13() {
        assert_eq!(ev("floor", &[f(0.5)]), f(0.0));
    }
    #[test]
    fn f14() {
        assert_eq!(ev("floor", &[f(-0.5)]), f(-1.0));
    }
    #[test]
    fn f15() {
        assert_eq!(ev("floor", &[f(10.0)]), f(10.0));
    }
    #[test]
    fn f16() {
        assert_eq!(ev("floor", &[f(10.9)]), f(10.0));
    }
    #[test]
    fn f17() {
        assert_eq!(ev("floor", &[f(-10.1)]), f(-11.0));
    }
    #[test]
    fn f18() {
        assert_eq!(ev("floor", &[f(100.0)]), f(100.0));
    }
    #[test]
    fn f19() {
        assert_eq!(ev("floor", &[f(100.9)]), f(100.0));
    }
    #[test]
    fn f20() {
        assert_eq!(ev("floor", &[f(7.7)]), f(7.0));
    }
    #[test]
    fn f21() {
        assert_eq!(ev("floor", &[f(-7.7)]), f(-8.0));
    }
    #[test]
    fn f22() {
        assert_eq!(ev("floor", &[f(50.0)]), f(50.0));
    }
    #[test]
    fn f23() {
        assert_eq!(ev("floor", &[f(50.1)]), f(50.0));
    }
    #[test]
    fn f24() {
        assert_eq!(ev("floor", &[f(-50.1)]), f(-51.0));
    }
    #[test]
    fn f25() {
        assert_eq!(ev("floor", &[f(0.001)]), f(0.0));
    }
    #[test]
    fn f26() {
        assert_eq!(ev("floor", &[f(-0.001)]), f(-1.0));
    }
    #[test]
    fn f27() {
        assert_eq!(ev("floor", &[f(999.999)]), f(999.0));
    }
    #[test]
    fn f28() {
        assert_eq!(ev("floor", &[f(1.0001)]), f(1.0));
    }
    #[test]
    fn f29() {
        assert_eq!(ev("floor", &[f(-1.0001)]), f(-2.0));
    }
}

// ceil — 30 tests
mod ceil_s {
    use super::*;
    #[test]
    fn c00() {
        assert_eq!(ev("ceil", &[f(1.0)]), f(1.0));
    }
    #[test]
    fn c01() {
        assert_eq!(ev("ceil", &[f(1.1)]), f(2.0));
    }
    #[test]
    fn c02() {
        assert_eq!(ev("ceil", &[f(1.9)]), f(2.0));
    }
    #[test]
    fn c03() {
        assert_eq!(ev("ceil", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn c04() {
        assert_eq!(ev("ceil", &[f(-1.0)]), f(-1.0));
    }
    #[test]
    fn c05() {
        assert_eq!(ev("ceil", &[f(-1.1)]), f(-1.0));
    }
    #[test]
    fn c06() {
        assert_eq!(ev("ceil", &[f(-1.9)]), f(-1.0));
    }
    #[test]
    fn c07() {
        assert_eq!(ev("ceil", &[null()]), null());
    }
    #[test]
    fn c08() {
        let v = ev("ceil", &[i(5)]);
        assert!(v == i(5) || v == f(5.0));
    }
    #[test]
    fn c09() {
        assert_eq!(ev("ceil", &[f(3.15)]), f(4.0));
    }
    #[test]
    fn c10() {
        assert_eq!(ev("ceil", &[f(2.719)]), f(3.0));
    }
    #[test]
    fn c11() {
        assert_eq!(ev("ceil", &[f(99.1)]), f(100.0));
    }
    #[test]
    fn c12() {
        assert_eq!(ev("ceil", &[f(0.5)]), f(1.0));
    }
    #[test]
    fn c13() {
        assert_eq!(ev("ceil", &[f(10.0)]), f(10.0));
    }
    #[test]
    fn c14() {
        assert_eq!(ev("ceil", &[f(10.1)]), f(11.0));
    }
    #[test]
    fn c15() {
        assert_eq!(ev("ceil", &[f(-10.9)]), f(-10.0));
    }
    #[test]
    fn c16() {
        assert_eq!(ev("ceil", &[f(100.0)]), f(100.0));
    }
    #[test]
    fn c17() {
        assert_eq!(ev("ceil", &[f(100.1)]), f(101.0));
    }
    #[test]
    fn c18() {
        assert_eq!(ev("ceil", &[f(-100.9)]), f(-100.0));
    }
    #[test]
    fn c19() {
        assert_eq!(ev("ceil", &[f(7.0)]), f(7.0));
    }
    #[test]
    fn c20() {
        assert_eq!(ev("ceil", &[f(7.001)]), f(8.0));
    }
    #[test]
    fn c21() {
        assert_eq!(ev("ceil", &[f(-7.001)]), f(-7.0));
    }
    #[test]
    fn c22() {
        assert_eq!(ev("ceil", &[f(50.0)]), f(50.0));
    }
    #[test]
    fn c23() {
        assert_eq!(ev("ceil", &[f(50.5)]), f(51.0));
    }
    #[test]
    fn c24() {
        assert_eq!(ev("ceil", &[f(-50.5)]), f(-50.0));
    }
    #[test]
    fn c25() {
        assert_eq!(ev("ceil", &[f(0.001)]), f(1.0));
    }
    #[test]
    fn c26() {
        assert_eq!(ev("ceil", &[f(999.001)]), f(1000.0));
    }
    #[test]
    fn c27() {
        assert_eq!(ev("ceil", &[f(2.0)]), f(2.0));
    }
    #[test]
    fn c28() {
        assert_eq!(ev("ceil", &[f(3.0)]), f(3.0));
    }
    #[test]
    fn c29() {
        assert_eq!(ev("ceil", &[f(4.0)]), f(4.0));
    }
}

// sqrt — 30 tests
mod sqrt_s {
    use super::*;
    #[test]
    fn s00() {
        assert_eq!(ev("sqrt", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn s01() {
        assert_eq!(ev("sqrt", &[f(1.0)]), f(1.0));
    }
    #[test]
    fn s02() {
        assert_eq!(ev("sqrt", &[f(4.0)]), f(2.0));
    }
    #[test]
    fn s03() {
        assert_eq!(ev("sqrt", &[f(9.0)]), f(3.0));
    }
    #[test]
    fn s04() {
        assert_eq!(ev("sqrt", &[f(16.0)]), f(4.0));
    }
    #[test]
    fn s05() {
        assert_eq!(ev("sqrt", &[f(25.0)]), f(5.0));
    }
    #[test]
    fn s06() {
        assert_eq!(ev("sqrt", &[f(36.0)]), f(6.0));
    }
    #[test]
    fn s07() {
        assert_eq!(ev("sqrt", &[f(49.0)]), f(7.0));
    }
    #[test]
    fn s08() {
        assert_eq!(ev("sqrt", &[f(64.0)]), f(8.0));
    }
    #[test]
    fn s09() {
        assert_eq!(ev("sqrt", &[f(81.0)]), f(9.0));
    }
    #[test]
    fn s10() {
        assert_eq!(ev("sqrt", &[f(100.0)]), f(10.0));
    }
    #[test]
    fn s11() {
        assert_eq!(ev("sqrt", &[null()]), null());
    }
    #[test]
    fn s12() {
        assert_eq!(ev("sqrt", &[i(4)]), f(2.0));
    }
    #[test]
    fn s13() {
        assert_eq!(ev("sqrt", &[i(9)]), f(3.0));
    }
    #[test]
    fn s14() {
        assert_eq!(ev("sqrt", &[i(16)]), f(4.0));
    }
    #[test]
    fn s15() {
        assert_eq!(ev("sqrt", &[i(0)]), f(0.0));
    }
    #[test]
    fn s16() {
        assert_eq!(ev("sqrt", &[i(1)]), f(1.0));
    }
    #[test]
    fn s17() {
        assert_eq!(ev("sqrt", &[f(121.0)]), f(11.0));
    }
    #[test]
    fn s18() {
        assert_eq!(ev("sqrt", &[f(144.0)]), f(12.0));
    }
    #[test]
    fn s19() {
        assert_eq!(ev("sqrt", &[f(169.0)]), f(13.0));
    }
    #[test]
    fn s20() {
        assert_eq!(ev("sqrt", &[f(196.0)]), f(14.0));
    }
    #[test]
    fn s21() {
        assert_eq!(ev("sqrt", &[f(225.0)]), f(15.0));
    }
    #[test]
    fn s22() {
        assert_eq!(ev("sqrt", &[f(256.0)]), f(16.0));
    }
    #[test]
    fn s23() {
        assert_eq!(ev("sqrt", &[f(400.0)]), f(20.0));
    }
    #[test]
    fn s24() {
        assert_eq!(ev("sqrt", &[f(900.0)]), f(30.0));
    }
    #[test]
    fn s25() {
        assert_eq!(ev("sqrt", &[f(10000.0)]), f(100.0));
    }
    #[test]
    fn s26() {
        let v = ev("sqrt", &[f(2.0)]);
        match v {
            Value::F64(v) => assert!((v - std::f64::consts::SQRT_2).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn s27() {
        let v = ev("sqrt", &[f(3.0)]);
        match v {
            Value::F64(v) => assert!((v - 1.7320508).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn s28() {
        assert_eq!(ev("sqrt", &[i(25)]), f(5.0));
    }
    #[test]
    fn s29() {
        assert_eq!(ev("sqrt", &[i(100)]), f(10.0));
    }
}

// sign — 30 tests
mod sign_s {
    use super::*;
    #[test]
    fn g00() {
        assert_eq!(ev("sign", &[i(0)]), i(0));
    }
    #[test]
    fn g01() {
        assert_eq!(ev("sign", &[i(1)]), i(1));
    }
    #[test]
    fn g02() {
        assert_eq!(ev("sign", &[i(-1)]), i(-1));
    }
    #[test]
    fn g03() {
        assert_eq!(ev("sign", &[i(42)]), i(1));
    }
    #[test]
    fn g04() {
        assert_eq!(ev("sign", &[i(-42)]), i(-1));
    }
    #[test]
    fn g05() {
        assert_eq!(ev("sign", &[i(100)]), i(1));
    }
    #[test]
    fn g06() {
        assert_eq!(ev("sign", &[i(-100)]), i(-1));
    }
    #[test]
    fn g07() {
        assert_eq!(ev("sign", &[f(0.0)]), i(0));
    }
    #[test]
    fn g08() {
        assert_eq!(ev("sign", &[f(1.5)]), i(1));
    }
    #[test]
    fn g09() {
        assert_eq!(ev("sign", &[f(-1.5)]), i(-1));
    }
    #[test]
    fn g10() {
        assert_eq!(ev("sign", &[null()]), null());
    }
    #[test]
    fn g11() {
        assert_eq!(ev("sign", &[i(999)]), i(1));
    }
    #[test]
    fn g12() {
        assert_eq!(ev("sign", &[i(-999)]), i(-1));
    }
    #[test]
    fn g13() {
        assert_eq!(ev("sign", &[f(3.15)]), i(1));
    }
    #[test]
    fn g14() {
        assert_eq!(ev("sign", &[f(-3.15)]), i(-1));
    }
    #[test]
    fn g15() {
        assert_eq!(ev("sign", &[i(10)]), i(1));
    }
    #[test]
    fn g16() {
        assert_eq!(ev("sign", &[i(-10)]), i(-1));
    }
    #[test]
    fn g17() {
        assert_eq!(ev("sign", &[i(50)]), i(1));
    }
    #[test]
    fn g18() {
        assert_eq!(ev("sign", &[i(-50)]), i(-1));
    }
    #[test]
    fn g19() {
        assert_eq!(ev("sign", &[f(0.001)]), i(1));
    }
    #[test]
    fn g20() {
        assert_eq!(ev("sign", &[f(-0.001)]), i(-1));
    }
    #[test]
    fn g21() {
        assert_eq!(ev("sign", &[i(7)]), i(1));
    }
    #[test]
    fn g22() {
        assert_eq!(ev("sign", &[i(-7)]), i(-1));
    }
    #[test]
    fn g23() {
        assert_eq!(ev("sign", &[i(2)]), i(1));
    }
    #[test]
    fn g24() {
        assert_eq!(ev("sign", &[i(-2)]), i(-1));
    }
    #[test]
    fn g25() {
        assert_eq!(ev("sign", &[i(3)]), i(1));
    }
    #[test]
    fn g26() {
        assert_eq!(ev("sign", &[i(-3)]), i(-1));
    }
    #[test]
    fn g27() {
        assert_eq!(ev("sign", &[f(100.0)]), i(1));
    }
    #[test]
    fn g28() {
        assert_eq!(ev("sign", &[f(-100.0)]), i(-1));
    }
    #[test]
    fn g29() {
        assert_eq!(ev("sign", &[f(99.9)]), i(1));
    }
}

// trim — 25 tests
mod trim_s {
    use super::*;
    #[test]
    fn t00() {
        assert_eq!(ev("trim", &[s("")]), s(""));
    }
    #[test]
    fn t01() {
        assert_eq!(ev("trim", &[s(" ")]), s(""));
    }
    #[test]
    fn t02() {
        assert_eq!(ev("trim", &[s("  ")]), s(""));
    }
    #[test]
    fn t03() {
        assert_eq!(ev("trim", &[s(" a ")]), s("a"));
    }
    #[test]
    fn t04() {
        assert_eq!(ev("trim", &[s("  ab  ")]), s("ab"));
    }
    #[test]
    fn t05() {
        assert_eq!(ev("trim", &[s("hello")]), s("hello"));
    }
    #[test]
    fn t06() {
        assert_eq!(ev("trim", &[s(" hello ")]), s("hello"));
    }
    #[test]
    fn t07() {
        assert_eq!(ev("trim", &[s("  hello  ")]), s("hello"));
    }
    #[test]
    fn t08() {
        assert_eq!(ev("trim", &[null()]), null());
    }
    #[test]
    fn t09() {
        assert_eq!(ev("trim", &[s("abc")]), s("abc"));
    }
    #[test]
    fn t10() {
        assert_eq!(ev("trim", &[s("  abc  ")]), s("abc"));
    }
    #[test]
    fn t11() {
        assert_eq!(ev("trim", &[s("x ")]), s("x"));
    }
    #[test]
    fn t12() {
        assert_eq!(ev("trim", &[s(" x")]), s("x"));
    }
    #[test]
    fn t13() {
        assert_eq!(ev("trim", &[s("   test   ")]), s("test"));
    }
    #[test]
    fn t14() {
        assert_eq!(ev("trim", &[s("a")]), s("a"));
    }
    #[test]
    fn t15() {
        assert_eq!(ev("trim", &[s(" ab")]), s("ab"));
    }
    #[test]
    fn t16() {
        assert_eq!(ev("trim", &[s("ab ")]), s("ab"));
    }
    #[test]
    fn t17() {
        assert_eq!(ev("trim", &[s("  ab")]), s("ab"));
    }
    #[test]
    fn t18() {
        assert_eq!(ev("trim", &[s("ab  ")]), s("ab"));
    }
    #[test]
    fn t19() {
        assert_eq!(ev("trim", &[s("a b c")]), s("a b c"));
    }
    #[test]
    fn t20() {
        assert_eq!(ev("trim", &[s(" a b c ")]), s("a b c"));
    }
    #[test]
    fn t21() {
        assert_eq!(ev("trim", &[s("   ")]), s(""));
    }
    #[test]
    fn t22() {
        assert_eq!(ev("trim", &[s("    ")]), s(""));
    }
    #[test]
    fn t23() {
        assert_eq!(ev("trim", &[s("ok  ")]), s("ok"));
    }
    #[test]
    fn t24() {
        assert_eq!(ev("trim", &[s("  ok")]), s("ok"));
    }
}

// reverse — 25 tests
mod reverse_s {
    use super::*;
    #[test]
    fn v00() {
        assert_eq!(ev("reverse", &[s("")]), s(""));
    }
    #[test]
    fn v01() {
        assert_eq!(ev("reverse", &[s("a")]), s("a"));
    }
    #[test]
    fn v02() {
        assert_eq!(ev("reverse", &[s("ab")]), s("ba"));
    }
    #[test]
    fn v03() {
        assert_eq!(ev("reverse", &[s("abc")]), s("cba"));
    }
    #[test]
    fn v04() {
        assert_eq!(ev("reverse", &[s("hello")]), s("olleh"));
    }
    #[test]
    fn v05() {
        assert_eq!(ev("reverse", &[s("12345")]), s("54321"));
    }
    #[test]
    fn v06() {
        assert_eq!(ev("reverse", &[null()]), null());
    }
    #[test]
    fn v07() {
        assert_eq!(ev("reverse", &[s("abcdef")]), s("fedcba"));
    }
    #[test]
    fn v08() {
        assert_eq!(ev("reverse", &[s("racecar")]), s("racecar"));
    }
    #[test]
    fn v09() {
        assert_eq!(ev("reverse", &[s("madam")]), s("madam"));
    }
    #[test]
    fn v10() {
        assert_eq!(ev("reverse", &[s("level")]), s("level"));
    }
    #[test]
    fn v11() {
        assert_eq!(ev("reverse", &[s("noon")]), s("noon"));
    }
    #[test]
    fn v12() {
        assert_eq!(ev("reverse", &[s("deed")]), s("deed"));
    }
    #[test]
    fn v13() {
        assert_eq!(ev("reverse", &[s("ab cd")]), s("dc ba"));
    }
    #[test]
    fn v14() {
        assert_eq!(ev("reverse", &[s("xyz")]), s("zyx"));
    }
    #[test]
    fn v15() {
        assert_eq!(ev("reverse", &[s("test")]), s("tset"));
    }
    #[test]
    fn v16() {
        assert_eq!(ev("reverse", &[s("rust")]), s("tsur"));
    }
    #[test]
    fn v17() {
        assert_eq!(ev("reverse", &[s("go")]), s("og"));
    }
    #[test]
    fn v18() {
        assert_eq!(ev("reverse", &[s("hi")]), s("ih"));
    }
    #[test]
    fn v19() {
        assert_eq!(ev("reverse", &[s("ok")]), s("ko"));
    }
    #[test]
    fn v20() {
        assert_eq!(ev("reverse", &[s("no")]), s("on"));
    }
    #[test]
    fn v21() {
        assert_eq!(ev("reverse", &[s("up")]), s("pu"));
    }
    #[test]
    fn v22() {
        assert_eq!(ev("reverse", &[s("it")]), s("ti"));
    }
    #[test]
    fn v23() {
        assert_eq!(ev("reverse", &[s("do")]), s("od"));
    }
    #[test]
    fn v24() {
        assert_eq!(ev("reverse", &[s("to")]), s("ot"));
    }
}

// concat — 25 tests
mod concat_s {
    use super::*;
    #[test]
    fn c00() {
        assert_eq!(ev("concat", &[s("a"), s("b")]), s("ab"));
    }
    #[test]
    fn c01() {
        assert_eq!(ev("concat", &[s(""), s("")]), s(""));
    }
    #[test]
    fn c02() {
        assert_eq!(
            ev("concat", &[s("hello"), s(" "), s("world")]),
            s("hello world")
        );
    }
    #[test]
    fn c03() {
        assert_eq!(ev("concat", &[s("a"), s("")]), s("a"));
    }
    #[test]
    fn c04() {
        assert_eq!(ev("concat", &[s("x"), s("y"), s("z")]), s("xyz"));
    }
    #[test]
    fn c05() {
        assert_eq!(ev("concat", &[s("foo"), s("bar")]), s("foobar"));
    }
    #[test]
    fn c06() {
        assert_eq!(ev("concat", &[s("1"), s("2"), s("3")]), s("123"));
    }
    #[test]
    fn c07() {
        assert_eq!(ev("concat", &[s("a"), s("b"), s("c"), s("d")]), s("abcd"));
    }
    #[test]
    fn c08() {
        assert_eq!(ev("concat", &[s("hello"), s("")]), s("hello"));
    }
    #[test]
    fn c09() {
        assert_eq!(ev("concat", &[s(""), s("world")]), s("world"));
    }
    #[test]
    fn c10() {
        assert_eq!(ev("concat", &[s("AB"), s("CD")]), s("ABCD"));
    }
    #[test]
    fn c11() {
        assert_eq!(ev("concat", &[s("test"), s("123")]), s("test123"));
    }
    #[test]
    fn c12() {
        assert_eq!(ev("concat", &[s("a"), s("b"), s("c")]), s("abc"));
    }
    #[test]
    fn c13() {
        assert_eq!(ev("concat", &[s("hi"), s(" "), s("there")]), s("hi there"));
    }
    #[test]
    fn c14() {
        assert_eq!(ev("concat", &[s("one"), s("two")]), s("onetwo"));
    }
    #[test]
    fn c15() {
        assert_eq!(ev("concat", &[s("rust"), s("lang")]), s("rustlang"));
    }
    #[test]
    fn c16() {
        assert_eq!(ev("concat", &[s("go"), s("od")]), s("good"));
    }
    #[test]
    fn c17() {
        assert_eq!(ev("concat", &[s("m"), s("n")]), s("mn"));
    }
    #[test]
    fn c18() {
        assert_eq!(ev("concat", &[s("p"), s("q"), s("r"), s("s")]), s("pqrs"));
    }
    #[test]
    fn c19() {
        assert_eq!(ev("concat", &[s("data"), s("base")]), s("database"));
    }
    #[test]
    fn c20() {
        assert_eq!(ev("concat", &[s("ex"), s("change")]), s("exchange"));
    }
    #[test]
    fn c21() {
        assert_eq!(ev("concat", &[s("time"), s("series")]), s("timeseries"));
    }
    #[test]
    fn c22() {
        assert_eq!(ev("concat", &[s("a"), s(" "), s("b")]), s("a b"));
    }
    #[test]
    fn c23() {
        assert_eq!(
            ev("concat", &[s("alpha"), s("beta"), s("gamma")]),
            s("alphabetagamma")
        );
    }
    #[test]
    fn c24() {
        assert_eq!(ev("concat", &[s("x"), s("")]), s("x"));
    }
}

// replace — 25 tests
mod replace_s {
    use super::*;
    #[test]
    fn p00() {
        assert_eq!(ev("replace", &[s("hello"), s("l"), s("r")]), s("herro"));
    }
    #[test]
    fn p01() {
        assert_eq!(ev("replace", &[s("hello"), s("ll"), s("LL")]), s("heLLo"));
    }
    #[test]
    fn p02() {
        assert_eq!(ev("replace", &[s("aaa"), s("a"), s("b")]), s("bbb"));
    }
    #[test]
    fn p03() {
        assert_eq!(ev("replace", &[s("abc"), s("b"), s("")]), s("ac"));
    }
    #[test]
    fn p04() {
        assert_eq!(ev("replace", &[s("abc"), s("x"), s("y")]), s("abc"));
    }
    #[test]
    fn p05() {
        assert_eq!(ev("replace", &[s(""), s("a"), s("b")]), s(""));
    }
    #[test]
    fn p06() {
        assert_eq!(ev("replace", &[null(), s("a"), s("b")]), null());
    }
    #[test]
    fn p07() {
        assert_eq!(ev("replace", &[s("foo"), s("o"), s("0")]), s("f00"));
    }
    #[test]
    fn p08() {
        assert_eq!(ev("replace", &[s("bar"), s("a"), s("e")]), s("ber"));
    }
    #[test]
    fn p09() {
        assert_eq!(ev("replace", &[s("test"), s("t"), s("T")]), s("TesT"));
    }
    #[test]
    fn p10() {
        assert_eq!(ev("replace", &[s("abcabc"), s("abc"), s("x")]), s("xx"));
    }
    #[test]
    fn p11() {
        assert_eq!(
            ev("replace", &[s("hello world"), s(" "), s("-")]),
            s("hello-world")
        );
    }
    #[test]
    fn p12() {
        assert_eq!(ev("replace", &[s("aabb"), s("aa"), s("cc")]), s("ccbb"));
    }
    #[test]
    fn p13() {
        assert_eq!(
            ev("replace", &[s("xyzxyz"), s("xyz"), s("abc")]),
            s("abcabc")
        );
    }
    #[test]
    fn p14() {
        assert_eq!(ev("replace", &[s("111"), s("1"), s("2")]), s("222"));
    }
    #[test]
    fn p15() {
        assert_eq!(ev("replace", &[s("cat"), s("c"), s("b")]), s("bat"));
    }
    #[test]
    fn p16() {
        assert_eq!(ev("replace", &[s("dog"), s("d"), s("l")]), s("log"));
    }
    #[test]
    fn p17() {
        assert_eq!(ev("replace", &[s("sun"), s("s"), s("f")]), s("fun"));
    }
    #[test]
    fn p18() {
        assert_eq!(ev("replace", &[s("hat"), s("h"), s("c")]), s("cat"));
    }
    #[test]
    fn p19() {
        assert_eq!(ev("replace", &[s("map"), s("m"), s("c")]), s("cap"));
    }
    #[test]
    fn p20() {
        assert_eq!(ev("replace", &[s("pit"), s("p"), s("b")]), s("bit"));
    }
    #[test]
    fn p21() {
        assert_eq!(ev("replace", &[s("run"), s("r"), s("s")]), s("sun"));
    }
    #[test]
    fn p22() {
        assert_eq!(ev("replace", &[s("hot"), s("h"), s("n")]), s("not"));
    }
    #[test]
    fn p23() {
        assert_eq!(ev("replace", &[s("pan"), s("p"), s("b")]), s("ban"));
    }
    #[test]
    fn p24() {
        assert_eq!(ev("replace", &[s("tin"), s("t"), s("b")]), s("bin"));
    }
}

// starts_with / ends_with / contains — 30 tests
mod string_checks {
    use super::*;
    #[test]
    fn sw00() {
        assert_eq!(ev("starts_with", &[s("hello"), s("he")]), i(1));
    }
    #[test]
    fn sw01() {
        assert_eq!(ev("starts_with", &[s("hello"), s("lo")]), i(0));
    }
    #[test]
    fn sw02() {
        assert_eq!(ev("starts_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn sw03() {
        assert_eq!(ev("starts_with", &[s("hello"), s("hello")]), i(1));
    }
    #[test]
    fn sw04() {
        assert_eq!(ev("starts_with", &[s("hello"), s("helloworld")]), i(0));
    }
    #[test]
    fn sw05() {
        assert_eq!(ev("starts_with", &[s("abc"), s("a")]), i(1));
    }
    #[test]
    fn sw06() {
        assert_eq!(ev("starts_with", &[s("abc"), s("ab")]), i(1));
    }
    #[test]
    fn sw07() {
        assert_eq!(ev("starts_with", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn sw08() {
        assert_eq!(ev("starts_with", &[s("abc"), s("b")]), i(0));
    }
    #[test]
    fn sw09() {
        assert_eq!(ev("starts_with", &[s("abc"), s("c")]), i(0));
    }
    #[test]
    fn ew00() {
        assert_eq!(ev("ends_with", &[s("hello"), s("lo")]), i(1));
    }
    #[test]
    fn ew01() {
        assert_eq!(ev("ends_with", &[s("hello"), s("he")]), i(0));
    }
    #[test]
    fn ew02() {
        assert_eq!(ev("ends_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn ew03() {
        assert_eq!(ev("ends_with", &[s("hello"), s("hello")]), i(1));
    }
    #[test]
    fn ew04() {
        assert_eq!(ev("ends_with", &[s("abc"), s("c")]), i(1));
    }
    #[test]
    fn ew05() {
        assert_eq!(ev("ends_with", &[s("abc"), s("bc")]), i(1));
    }
    #[test]
    fn ew06() {
        assert_eq!(ev("ends_with", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn ew07() {
        assert_eq!(ev("ends_with", &[s("abc"), s("a")]), i(0));
    }
    #[test]
    fn ew08() {
        assert_eq!(ev("ends_with", &[s("abc"), s("ab")]), i(0));
    }
    #[test]
    fn ew09() {
        assert_eq!(ev("ends_with", &[s("test"), s("st")]), i(1));
    }
    #[test]
    fn cn00() {
        assert_eq!(ev("contains", &[s("hello"), s("ell")]), i(1));
    }
    #[test]
    fn cn01() {
        assert_eq!(ev("contains", &[s("hello"), s("xyz")]), i(0));
    }
    #[test]
    fn cn02() {
        assert_eq!(ev("contains", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn cn03() {
        assert_eq!(ev("contains", &[s("hello"), s("hello")]), i(1));
    }
    #[test]
    fn cn04() {
        assert_eq!(ev("contains", &[s("hello"), s("h")]), i(1));
    }
    #[test]
    fn cn05() {
        assert_eq!(ev("contains", &[s("hello"), s("o")]), i(1));
    }
    #[test]
    fn cn06() {
        assert_eq!(ev("contains", &[s("abcdef"), s("cd")]), i(1));
    }
    #[test]
    fn cn07() {
        assert_eq!(ev("contains", &[s("abcdef"), s("ef")]), i(1));
    }
    #[test]
    fn cn08() {
        assert_eq!(ev("contains", &[s("abcdef"), s("gh")]), i(0));
    }
    #[test]
    fn cn09() {
        assert_eq!(ev("contains", &[s(""), s("")]), i(1));
    }
}

// coalesce / nullif / greatest / least — 40 tests
mod conditional_s {
    use super::*;
    #[test]
    fn co00() {
        assert_eq!(ev("coalesce", &[null(), i(1)]), i(1));
    }
    #[test]
    fn co01() {
        assert_eq!(ev("coalesce", &[i(1), i(2)]), i(1));
    }
    #[test]
    fn co02() {
        assert_eq!(ev("coalesce", &[null(), null(), i(3)]), i(3));
    }
    #[test]
    fn co03() {
        assert_eq!(ev("coalesce", &[null(), null(), null()]), null());
    }
    #[test]
    fn co04() {
        assert_eq!(ev("coalesce", &[s("a"), s("b")]), s("a"));
    }
    #[test]
    fn co05() {
        assert_eq!(ev("coalesce", &[null(), s("b")]), s("b"));
    }
    #[test]
    fn co06() {
        assert_eq!(ev("coalesce", &[i(0), i(1)]), i(0));
    }
    #[test]
    fn co07() {
        assert_eq!(ev("coalesce", &[f(1.5), f(2.5)]), f(1.5));
    }
    #[test]
    fn co08() {
        assert_eq!(ev("coalesce", &[null(), f(2.5)]), f(2.5));
    }
    #[test]
    fn co09() {
        assert_eq!(ev("coalesce", &[null(), null(), f(3.0)]), f(3.0));
    }
    #[test]
    fn ni00() {
        assert_eq!(ev("nullif", &[i(1), i(1)]), null());
    }
    #[test]
    fn ni01() {
        assert_eq!(ev("nullif", &[i(1), i(2)]), i(1));
    }
    #[test]
    fn ni02() {
        assert_eq!(ev("nullif", &[s("a"), s("a")]), null());
    }
    #[test]
    fn ni03() {
        assert_eq!(ev("nullif", &[s("a"), s("b")]), s("a"));
    }
    #[test]
    fn ni04() {
        assert_eq!(ev("nullif", &[f(1.0), f(1.0)]), null());
    }
    #[test]
    fn ni05() {
        assert_eq!(ev("nullif", &[f(1.0), f(2.0)]), f(1.0));
    }
    #[test]
    fn ni06() {
        assert_eq!(ev("nullif", &[i(0), i(0)]), null());
    }
    #[test]
    fn ni07() {
        assert_eq!(ev("nullif", &[i(0), i(1)]), i(0));
    }
    #[test]
    fn ni08() {
        assert_eq!(ev("nullif", &[s(""), s("")]), null());
    }
    #[test]
    fn ni09() {
        assert_eq!(ev("nullif", &[s("x"), s("")]), s("x"));
    }
    #[test]
    fn gr00() {
        assert_eq!(ev("greatest", &[i(1), i(2), i(3)]), i(3));
    }
    #[test]
    fn gr01() {
        assert_eq!(ev("greatest", &[i(3), i(2), i(1)]), i(3));
    }
    #[test]
    fn gr02() {
        assert_eq!(ev("greatest", &[i(1), i(1), i(1)]), i(1));
    }
    #[test]
    fn gr03() {
        assert_eq!(ev("greatest", &[f(1.0), f(2.0)]), f(2.0));
    }
    #[test]
    fn gr04() {
        assert_eq!(ev("greatest", &[f(3.15), f(2.71)]), f(3.15));
    }
    #[test]
    fn gr05() {
        assert_eq!(ev("greatest", &[i(10)]), i(10));
    }
    #[test]
    fn gr06() {
        assert_eq!(ev("greatest", &[i(-1), i(-2)]), i(-1));
    }
    #[test]
    fn gr07() {
        assert_eq!(ev("greatest", &[i(0), i(0)]), i(0));
    }
    #[test]
    fn gr08() {
        assert_eq!(ev("greatest", &[i(5), i(10), i(15)]), i(15));
    }
    #[test]
    fn gr09() {
        assert_eq!(ev("greatest", &[i(100), i(50)]), i(100));
    }
    #[test]
    fn le00() {
        assert_eq!(ev("least", &[i(1), i(2), i(3)]), i(1));
    }
    #[test]
    fn le01() {
        assert_eq!(ev("least", &[i(3), i(2), i(1)]), i(1));
    }
    #[test]
    fn le02() {
        assert_eq!(ev("least", &[i(1), i(1), i(1)]), i(1));
    }
    #[test]
    fn le03() {
        assert_eq!(ev("least", &[f(1.0), f(2.0)]), f(1.0));
    }
    #[test]
    fn le04() {
        assert_eq!(ev("least", &[f(3.15), f(2.71)]), f(2.71));
    }
    #[test]
    fn le05() {
        assert_eq!(ev("least", &[i(10)]), i(10));
    }
    #[test]
    fn le06() {
        assert_eq!(ev("least", &[i(-1), i(-2)]), i(-2));
    }
    #[test]
    fn le07() {
        assert_eq!(ev("least", &[i(0), i(0)]), i(0));
    }
    #[test]
    fn le08() {
        assert_eq!(ev("least", &[i(5), i(10), i(15)]), i(5));
    }
    #[test]
    fn le09() {
        assert_eq!(ev("least", &[i(100), i(50)]), i(50));
    }
}

// left / right / repeat — 30 tests
mod leftright_s {
    use super::*;
    #[test]
    fn ll00() {
        assert_eq!(ev("left", &[s("hello"), i(2)]), s("he"));
    }
    #[test]
    fn ll01() {
        assert_eq!(ev("left", &[s("hello"), i(5)]), s("hello"));
    }
    #[test]
    fn ll02() {
        assert_eq!(ev("left", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn ll03() {
        assert_eq!(ev("left", &[s("hello"), i(1)]), s("h"));
    }
    #[test]
    fn ll04() {
        assert_eq!(ev("left", &[s("hello"), i(3)]), s("hel"));
    }
    #[test]
    fn ll05() {
        assert_eq!(ev("left", &[s("abc"), i(1)]), s("a"));
    }
    #[test]
    fn ll06() {
        assert_eq!(ev("left", &[s("abc"), i(2)]), s("ab"));
    }
    #[test]
    fn ll07() {
        assert_eq!(ev("left", &[s("abc"), i(3)]), s("abc"));
    }
    #[test]
    fn ll08() {
        assert_eq!(ev("left", &[s(""), i(5)]), s(""));
    }
    #[test]
    fn ll09() {
        assert_eq!(ev("left", &[null(), i(3)]), null());
    }
    #[test]
    fn rr00() {
        assert_eq!(ev("right", &[s("hello"), i(2)]), s("lo"));
    }
    #[test]
    fn rr01() {
        assert_eq!(ev("right", &[s("hello"), i(5)]), s("hello"));
    }
    #[test]
    fn rr02() {
        assert_eq!(ev("right", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn rr03() {
        assert_eq!(ev("right", &[s("hello"), i(1)]), s("o"));
    }
    #[test]
    fn rr04() {
        assert_eq!(ev("right", &[s("hello"), i(3)]), s("llo"));
    }
    #[test]
    fn rr05() {
        assert_eq!(ev("right", &[s("abc"), i(1)]), s("c"));
    }
    #[test]
    fn rr06() {
        assert_eq!(ev("right", &[s("abc"), i(2)]), s("bc"));
    }
    #[test]
    fn rr07() {
        assert_eq!(ev("right", &[s("abc"), i(3)]), s("abc"));
    }
    #[test]
    fn rr08() {
        assert_eq!(ev("right", &[s(""), i(5)]), s(""));
    }
    #[test]
    fn rr09() {
        assert_eq!(ev("right", &[null(), i(3)]), null());
    }
    #[test]
    fn rp00() {
        assert_eq!(ev("repeat", &[s("a"), i(3)]), s("aaa"));
    }
    #[test]
    fn rp01() {
        assert_eq!(ev("repeat", &[s("ab"), i(2)]), s("abab"));
    }
    #[test]
    fn rp02() {
        assert_eq!(ev("repeat", &[s("x"), i(5)]), s("xxxxx"));
    }
    #[test]
    fn rp03() {
        assert_eq!(ev("repeat", &[s(""), i(10)]), s(""));
    }
    #[test]
    fn rp04() {
        assert_eq!(ev("repeat", &[s("a"), i(0)]), s(""));
    }
    #[test]
    fn rp05() {
        assert_eq!(ev("repeat", &[s("hi"), i(3)]), s("hihihi"));
    }
    #[test]
    fn rp06() {
        assert_eq!(ev("repeat", &[null(), i(3)]), null());
    }
    #[test]
    fn rp07() {
        assert_eq!(ev("repeat", &[s("ab"), i(1)]), s("ab"));
    }
    #[test]
    fn rp08() {
        assert_eq!(ev("repeat", &[s("z"), i(4)]), s("zzzz"));
    }
    #[test]
    fn rp09() {
        assert_eq!(ev("repeat", &[s("q"), i(1)]), s("q"));
    }
}

// pow / log / exp — 30 tests
mod powlogexp_s {
    use super::*;
    #[test]
    fn pw00() {
        assert_eq!(ev("pow", &[f(2.0), f(0.0)]), f(1.0));
    }
    #[test]
    fn pw01() {
        assert_eq!(ev("pow", &[f(2.0), f(1.0)]), f(2.0));
    }
    #[test]
    fn pw02() {
        assert_eq!(ev("pow", &[f(2.0), f(2.0)]), f(4.0));
    }
    #[test]
    fn pw03() {
        assert_eq!(ev("pow", &[f(2.0), f(3.0)]), f(8.0));
    }
    #[test]
    fn pw04() {
        assert_eq!(ev("pow", &[f(2.0), f(10.0)]), f(1024.0));
    }
    #[test]
    fn pw05() {
        assert_eq!(ev("pow", &[f(3.0), f(2.0)]), f(9.0));
    }
    #[test]
    fn pw06() {
        assert_eq!(ev("pow", &[f(3.0), f(3.0)]), f(27.0));
    }
    #[test]
    fn pw07() {
        assert_eq!(ev("pow", &[f(10.0), f(2.0)]), f(100.0));
    }
    #[test]
    fn pw08() {
        assert_eq!(ev("pow", &[f(10.0), f(3.0)]), f(1000.0));
    }
    #[test]
    fn pw09() {
        assert_eq!(ev("pow", &[f(5.0), f(2.0)]), f(25.0));
    }
    #[test]
    fn pw10() {
        assert_eq!(ev("pow", &[null(), f(2.0)]), null());
    }
    #[test]
    fn lg00() {
        let v = ev("log", &[f(1.0)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn lg01() {
        let v = ev("log", &[f(std::f64::consts::E)]);
        match v {
            Value::F64(v) => assert!((v - 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn lg02() {
        assert_eq!(ev("log", &[null()]), null());
    }
    #[test]
    fn lg03() {
        let v = ev("log2", &[f(1.0)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn lg04() {
        let v = ev("log2", &[f(2.0)]);
        match v {
            Value::F64(v) => assert!((v - 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn lg05() {
        let v = ev("log2", &[f(4.0)]);
        match v {
            Value::F64(v) => assert!((v - 2.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn lg06() {
        let v = ev("log2", &[f(8.0)]);
        match v {
            Value::F64(v) => assert!((v - 3.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn lg07() {
        let v = ev("log10", &[f(1.0)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn lg08() {
        let v = ev("log10", &[f(10.0)]);
        match v {
            Value::F64(v) => assert!((v - 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn lg09() {
        let v = ev("log10", &[f(100.0)]);
        match v {
            Value::F64(v) => assert!((v - 2.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn lg10() {
        let v = ev("log10", &[f(1000.0)]);
        match v {
            Value::F64(v) => assert!((v - 3.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn ex00() {
        let v = ev("exp", &[f(0.0)]);
        match v {
            Value::F64(v) => assert!((v - 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn ex01() {
        let v = ev("exp", &[f(1.0)]);
        match v {
            Value::F64(v) => assert!((v - std::f64::consts::E).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn ex02() {
        assert_eq!(ev("exp", &[null()]), null());
    }
    #[test]
    fn ex03() {
        let v = ev("exp", &[i(0)]);
        match v {
            Value::F64(v) => assert!((v - 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn ex04() {
        let v = ev("exp", &[f(2.0)]);
        match v {
            Value::F64(v) => assert!((v - 7.389).abs() < 0.01),
            _ => panic!(),
        }
    }
    #[test]
    fn pw11() {
        assert_eq!(ev("pow", &[i(2), i(3)]), f(8.0));
    }
    #[test]
    fn pw12() {
        assert_eq!(ev("pow", &[i(5), i(0)]), f(1.0));
    }
    #[test]
    fn pw13() {
        assert_eq!(ev("pow", &[i(10), i(2)]), f(100.0));
    }
}

// sin / cos / tan — 30 tests
mod trig_s {
    use super::*;
    use std::f64::consts::PI;
    #[test]
    fn si00() {
        let v = ev("sin", &[f(0.0)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn si01() {
        let v = ev("sin", &[f(PI / 2.0)]);
        match v {
            Value::F64(v) => assert!((v - 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn si02() {
        let v = ev("sin", &[f(PI)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn si03() {
        assert_eq!(ev("sin", &[null()]), null());
    }
    #[test]
    fn si04() {
        let v = ev("sin", &[f(PI / 6.0)]);
        match v {
            Value::F64(v) => assert!((v - 0.5).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn si05() {
        let v = ev("sin", &[i(0)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn si06() {
        let v = ev("sin", &[f(3.0 * PI / 2.0)]);
        match v {
            Value::F64(v) => assert!((v + 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn si07() {
        let v = ev("sin", &[f(2.0 * PI)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn si08() {
        let v = ev("sin", &[f(PI / 4.0)]);
        match v {
            Value::F64(v) => assert!((v - std::f64::consts::FRAC_1_SQRT_2).abs() < 0.01),
            _ => panic!(),
        }
    }
    #[test]
    fn si09() {
        let v = ev("sin", &[f(PI / 3.0)]);
        match v {
            Value::F64(v) => assert!((v - 0.866).abs() < 0.01),
            _ => panic!(),
        }
    }
    #[test]
    fn co00() {
        let v = ev("cos", &[f(0.0)]);
        match v {
            Value::F64(v) => assert!((v - 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn co01() {
        let v = ev("cos", &[f(PI / 2.0)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn co02() {
        let v = ev("cos", &[f(PI)]);
        match v {
            Value::F64(v) => assert!((v + 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn co03() {
        assert_eq!(ev("cos", &[null()]), null());
    }
    #[test]
    fn co04() {
        let v = ev("cos", &[f(PI / 3.0)]);
        match v {
            Value::F64(v) => assert!((v - 0.5).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn co05() {
        let v = ev("cos", &[i(0)]);
        match v {
            Value::F64(v) => assert!((v - 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn co06() {
        let v = ev("cos", &[f(2.0 * PI)]);
        match v {
            Value::F64(v) => assert!((v - 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn co07() {
        let v = ev("cos", &[f(PI / 4.0)]);
        match v {
            Value::F64(v) => assert!((v - std::f64::consts::FRAC_1_SQRT_2).abs() < 0.01),
            _ => panic!(),
        }
    }
    #[test]
    fn co08() {
        let v = ev("cos", &[f(PI / 6.0)]);
        match v {
            Value::F64(v) => assert!((v - 0.866).abs() < 0.01),
            _ => panic!(),
        }
    }
    #[test]
    fn co09() {
        let v = ev("cos", &[f(3.0 * PI / 2.0)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn ta00() {
        let v = ev("tan", &[f(0.0)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn ta01() {
        let v = ev("tan", &[f(PI / 4.0)]);
        match v {
            Value::F64(v) => assert!((v - 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn ta02() {
        let v = ev("tan", &[f(PI)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn ta03() {
        assert_eq!(ev("tan", &[null()]), null());
    }
    #[test]
    fn ta04() {
        let v = ev("tan", &[i(0)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn ta05() {
        let v = ev("tan", &[f(PI / 6.0)]);
        match v {
            Value::F64(v) => assert!((v - 0.5774).abs() < 0.01),
            _ => panic!(),
        }
    }
    #[test]
    fn ta06() {
        let v = ev("tan", &[f(PI / 3.0)]);
        match v {
            Value::F64(v) => assert!((v - 1.7321).abs() < 0.01),
            _ => panic!(),
        }
    }
    #[test]
    fn ta07() {
        let v = ev("tan", &[f(-PI / 4.0)]);
        match v {
            Value::F64(v) => assert!((v + 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn ta08() {
        let v = ev("tan", &[f(2.0 * PI)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn ta09() {
        let v = ev("tan", &[f(-PI)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
}

// substring — 25 tests
mod substring_s {
    use super::*;
    #[test]
    fn ss00() {
        assert_eq!(ev("substring", &[s("hello"), i(1), i(2)]), s("he"));
    }
    #[test]
    fn ss01() {
        assert_eq!(ev("substring", &[s("hello"), i(2), i(3)]), s("ell"));
    }
    #[test]
    fn ss02() {
        assert_eq!(ev("substring", &[s("hello"), i(1), i(5)]), s("hello"));
    }
    #[test]
    fn ss03() {
        assert_eq!(ev("substring", &[s("hello"), i(3), i(2)]), s("ll"));
    }
    #[test]
    fn ss04() {
        assert_eq!(ev("substring", &[s("hello"), i(5), i(1)]), s("o"));
    }
    #[test]
    fn ss05() {
        assert_eq!(ev("substring", &[null(), i(1), i(2)]), null());
    }
    #[test]
    fn ss06() {
        assert_eq!(ev("substring", &[s("abc"), i(1), i(1)]), s("a"));
    }
    #[test]
    fn ss07() {
        assert_eq!(ev("substring", &[s("abc"), i(2), i(1)]), s("b"));
    }
    #[test]
    fn ss08() {
        assert_eq!(ev("substring", &[s("abc"), i(3), i(1)]), s("c"));
    }
    #[test]
    fn ss09() {
        assert_eq!(ev("substring", &[s("abc"), i(1), i(3)]), s("abc"));
    }
    #[test]
    fn ss10() {
        assert_eq!(ev("substring", &[s("abcdef"), i(1), i(3)]), s("abc"));
    }
    #[test]
    fn ss11() {
        assert_eq!(ev("substring", &[s("abcdef"), i(4), i(3)]), s("def"));
    }
    #[test]
    fn ss12() {
        assert_eq!(ev("substring", &[s("abcdef"), i(2), i(4)]), s("bcde"));
    }
    #[test]
    fn ss13() {
        assert_eq!(ev("substring", &[s("test"), i(1), i(4)]), s("test"));
    }
    #[test]
    fn ss14() {
        assert_eq!(ev("substring", &[s("test"), i(2), i(2)]), s("es"));
    }
    #[test]
    fn ss15() {
        assert_eq!(ev("substring", &[s("test"), i(3), i(2)]), s("st"));
    }
    #[test]
    fn ss16() {
        assert_eq!(ev("substring", &[s("test"), i(1), i(1)]), s("t"));
    }
    #[test]
    fn ss17() {
        assert_eq!(ev("substring", &[s("test"), i(4), i(1)]), s("t"));
    }
    #[test]
    fn ss18() {
        assert_eq!(ev("substring", &[s("x"), i(1), i(1)]), s("x"));
    }
    #[test]
    fn ss19() {
        assert_eq!(ev("substring", &[s("xy"), i(1), i(1)]), s("x"));
    }
    #[test]
    fn ss20() {
        assert_eq!(ev("substring", &[s("xy"), i(2), i(1)]), s("y"));
    }
    #[test]
    fn ss21() {
        assert_eq!(ev("substring", &[s("abcdef"), i(1), i(6)]), s("abcdef"));
    }
    #[test]
    fn ss22() {
        assert_eq!(ev("substring", &[s("hello world"), i(1), i(5)]), s("hello"));
    }
    #[test]
    fn ss23() {
        assert_eq!(ev("substring", &[s("hello world"), i(7), i(5)]), s("world"));
    }
    #[test]
    fn ss24() {
        assert_eq!(ev("substring", &[s(""), i(1), i(0)]), s(""));
    }
}

// cast_int / cast_float / cast_str — 30 tests
mod cast_s {
    use super::*;
    #[test]
    fn ci00() {
        assert_eq!(ev("cast_int", &[s("42")]), i(42));
    }
    #[test]
    fn ci01() {
        assert_eq!(ev("cast_int", &[s("0")]), i(0));
    }
    #[test]
    fn ci02() {
        assert_eq!(ev("cast_int", &[s("-1")]), i(-1));
    }
    #[test]
    fn ci03() {
        assert_eq!(ev("cast_int", &[s("100")]), i(100));
    }
    #[test]
    fn ci04() {
        assert_eq!(ev("cast_int", &[s("999")]), i(999));
    }
    #[test]
    fn ci05() {
        assert_eq!(ev("cast_int", &[f(42.0)]), i(42));
    }
    #[test]
    fn ci06() {
        assert_eq!(ev("cast_int", &[f(0.0)]), i(0));
    }
    #[test]
    fn ci07() {
        assert_eq!(ev("cast_int", &[f(-1.0)]), i(-1));
    }
    #[test]
    fn ci08() {
        assert_eq!(ev("cast_int", &[i(5)]), i(5));
    }
    #[test]
    fn ci09() {
        assert_eq!(ev("cast_int", &[null()]), null());
    }
    #[test]
    fn cf00() {
        assert_eq!(ev("cast_float", &[s("1.5")]), f(1.5));
    }
    #[test]
    fn cf01() {
        assert_eq!(ev("cast_float", &[s("0.0")]), f(0.0));
    }
    #[test]
    fn cf02() {
        assert_eq!(ev("cast_float", &[s("-1.5")]), f(-1.5));
    }
    #[test]
    fn cf03() {
        assert_eq!(ev("cast_float", &[s("100.0")]), f(100.0));
    }
    #[test]
    fn cf04() {
        assert_eq!(ev("cast_float", &[i(42)]), f(42.0));
    }
    #[test]
    fn cf05() {
        assert_eq!(ev("cast_float", &[i(0)]), f(0.0));
    }
    #[test]
    fn cf06() {
        assert_eq!(ev("cast_float", &[i(-1)]), f(-1.0));
    }
    #[test]
    fn cf07() {
        assert_eq!(ev("cast_float", &[f(3.15)]), f(3.15));
    }
    #[test]
    fn cf08() {
        assert_eq!(ev("cast_float", &[null()]), null());
    }
    #[test]
    fn cf09() {
        assert_eq!(ev("cast_float", &[s("3.15")]), f(3.15));
    }
    #[test]
    fn cs00() {
        assert_eq!(ev("cast_str", &[i(42)]), s("42"));
    }
    #[test]
    fn cs01() {
        assert_eq!(ev("cast_str", &[i(0)]), s("0"));
    }
    #[test]
    fn cs02() {
        assert_eq!(ev("cast_str", &[i(-1)]), s("-1"));
    }
    #[test]
    fn cs03() {
        assert_eq!(ev("cast_str", &[s("hello")]), s("hello"));
    }
    #[test]
    fn cs04() {
        assert_eq!(ev("cast_str", &[null()]), null());
    }
    #[test]
    fn cs05() {
        assert_eq!(ev("cast_str", &[i(100)]), s("100"));
    }
    #[test]
    fn cs06() {
        assert_eq!(ev("cast_str", &[i(999)]), s("999"));
    }
    #[test]
    fn cs07() {
        assert_eq!(ev("cast_str", &[s("")]), s(""));
    }
    #[test]
    fn cs08() {
        assert_eq!(ev("cast_str", &[i(1)]), s("1"));
    }
    #[test]
    fn cs09() {
        assert_eq!(ev("cast_str", &[i(-100)]), s("-100"));
    }
}

// mod / gcd / lcm / factorial — 30 tests
mod modgcd_s {
    use super::*;
    #[test]
    fn md00() {
        assert_eq!(ev("mod", &[i(10), i(3)]), i(1));
    }
    #[test]
    fn md01() {
        assert_eq!(ev("mod", &[i(10), i(5)]), i(0));
    }
    #[test]
    fn md02() {
        assert_eq!(ev("mod", &[i(7), i(2)]), i(1));
    }
    #[test]
    fn md03() {
        assert_eq!(ev("mod", &[i(100), i(7)]), i(2));
    }
    #[test]
    fn md04() {
        assert_eq!(ev("mod", &[i(0), i(5)]), i(0));
    }
    #[test]
    fn md05() {
        assert_eq!(ev("mod", &[i(15), i(4)]), i(3));
    }
    #[test]
    fn md06() {
        assert_eq!(ev("mod", &[i(20), i(6)]), i(2));
    }
    #[test]
    fn md07() {
        assert_eq!(ev("mod", &[i(9), i(3)]), i(0));
    }
    #[test]
    fn md08() {
        assert_eq!(ev("mod", &[i(17), i(5)]), i(2));
    }
    #[test]
    fn md09() {
        assert_eq!(ev("mod", &[null(), i(3)]), null());
    }
    #[test]
    fn gc00() {
        assert_eq!(ev("gcd", &[i(12), i(8)]), i(4));
    }
    #[test]
    fn gc01() {
        assert_eq!(ev("gcd", &[i(10), i(5)]), i(5));
    }
    #[test]
    fn gc02() {
        assert_eq!(ev("gcd", &[i(7), i(3)]), i(1));
    }
    #[test]
    fn gc03() {
        assert_eq!(ev("gcd", &[i(100), i(75)]), i(25));
    }
    #[test]
    fn gc04() {
        assert_eq!(ev("gcd", &[i(6), i(4)]), i(2));
    }
    #[test]
    fn gc05() {
        assert_eq!(ev("gcd", &[i(15), i(10)]), i(5));
    }
    #[test]
    fn gc06() {
        assert_eq!(ev("gcd", &[i(24), i(36)]), i(12));
    }
    #[test]
    fn gc07() {
        assert_eq!(ev("gcd", &[i(9), i(6)]), i(3));
    }
    #[test]
    fn gc08() {
        assert_eq!(ev("gcd", &[i(8), i(8)]), i(8));
    }
    #[test]
    fn gc09() {
        assert_eq!(ev("gcd", &[null(), i(5)]), null());
    }
    #[test]
    fn lc00() {
        assert_eq!(ev("lcm", &[i(3), i(4)]), i(12));
    }
    #[test]
    fn lc01() {
        assert_eq!(ev("lcm", &[i(5), i(10)]), i(10));
    }
    #[test]
    fn lc02() {
        assert_eq!(ev("lcm", &[i(6), i(8)]), i(24));
    }
    #[test]
    fn lc03() {
        assert_eq!(ev("lcm", &[i(7), i(3)]), i(21));
    }
    #[test]
    fn lc04() {
        assert_eq!(ev("lcm", &[i(4), i(6)]), i(12));
    }
    #[test]
    fn fa00() {
        assert_eq!(ev("factorial", &[i(0)]), i(1));
    }
    #[test]
    fn fa01() {
        assert_eq!(ev("factorial", &[i(1)]), i(1));
    }
    #[test]
    fn fa02() {
        assert_eq!(ev("factorial", &[i(5)]), i(120));
    }
    #[test]
    fn fa03() {
        assert_eq!(ev("factorial", &[i(10)]), i(3628800));
    }
    #[test]
    fn fa04() {
        assert_eq!(ev("factorial", &[null()]), null());
    }
}

// typeof / is_null / is_not_null / nullif_zero — 20 tests
mod typeof_s {
    use super::*;
    #[test]
    fn ty00() {
        let v = ev("typeof", &[i(1)]);
        assert!(matches!(v, Value::Str(_)));
    }
    #[test]
    fn ty01() {
        let v = ev("typeof", &[f(1.0)]);
        assert!(matches!(v, Value::Str(_)));
    }
    #[test]
    fn ty02() {
        let v = ev("typeof", &[s("a")]);
        assert!(matches!(v, Value::Str(_)));
    }
    #[test]
    fn ty03() {
        let v = ev("typeof", &[null()]);
        assert!(matches!(v, Value::Str(_)));
    }
    #[test]
    fn in00() {
        assert_eq!(ev("is_null", &[null()]), i(1));
    }
    #[test]
    fn in01() {
        assert_eq!(ev("is_null", &[i(1)]), i(0));
    }
    #[test]
    fn in02() {
        assert_eq!(ev("is_null", &[f(1.0)]), i(0));
    }
    #[test]
    fn in03() {
        assert_eq!(ev("is_null", &[s("a")]), i(0));
    }
    #[test]
    fn in04() {
        assert_eq!(ev("is_null", &[i(0)]), i(0));
    }
    #[test]
    fn nn00() {
        assert_eq!(ev("is_not_null", &[null()]), i(0));
    }
    #[test]
    fn nn01() {
        assert_eq!(ev("is_not_null", &[i(1)]), i(1));
    }
    #[test]
    fn nn02() {
        assert_eq!(ev("is_not_null", &[f(1.0)]), i(1));
    }
    #[test]
    fn nn03() {
        assert_eq!(ev("is_not_null", &[s("a")]), i(1));
    }
    #[test]
    fn nn04() {
        assert_eq!(ev("is_not_null", &[i(0)]), i(1));
    }
    #[test]
    fn nz00() {
        assert_eq!(ev("nullif_zero", &[i(0)]), null());
    }
    #[test]
    fn nz01() {
        assert_eq!(ev("nullif_zero", &[i(1)]), i(1));
    }
    #[test]
    fn nz02() {
        assert_eq!(ev("nullif_zero", &[i(-1)]), i(-1));
    }
    #[test]
    fn nz03() {
        assert_eq!(ev("nullif_zero", &[f(0.0)]), null());
    }
    #[test]
    fn nz04() {
        assert_eq!(ev("nullif_zero", &[f(1.0)]), f(1.0));
    }
    #[test]
    fn nz05() {
        assert_eq!(ev("nullif_zero", &[null()]), null());
    }
    #[test]
    fn nz06() {
        assert_eq!(ev("nullif_zero", &[i(42)]), i(42));
    }
    #[test]
    fn nz07() {
        assert_eq!(ev("nullif_zero", &[i(100)]), i(100));
    }
    #[test]
    fn nz08() {
        assert_eq!(ev("nullif_zero", &[f(-1.0)]), f(-1.0));
    }
    #[test]
    fn nz09() {
        assert_eq!(ev("nullif_zero", &[f(99.9)]), f(99.9));
    }
}

// ascii / chr — 20 tests
mod asciichr_s {
    use super::*;
    #[test]
    fn ac00() {
        assert_eq!(ev("ascii", &[s("A")]), i(65));
    }
    #[test]
    fn ac01() {
        assert_eq!(ev("ascii", &[s("a")]), i(97));
    }
    #[test]
    fn ac02() {
        assert_eq!(ev("ascii", &[s("0")]), i(48));
    }
    #[test]
    fn ac03() {
        assert_eq!(ev("ascii", &[s(" ")]), i(32));
    }
    #[test]
    fn ac04() {
        assert_eq!(ev("ascii", &[s("Z")]), i(90));
    }
    #[test]
    fn ac05() {
        assert_eq!(ev("ascii", &[s("z")]), i(122));
    }
    #[test]
    fn ac06() {
        assert_eq!(ev("ascii", &[s("1")]), i(49));
    }
    #[test]
    fn ac07() {
        assert_eq!(ev("ascii", &[s("!")]), i(33));
    }
    #[test]
    fn ac08() {
        assert_eq!(ev("ascii", &[null()]), null());
    }
    #[test]
    fn ac09() {
        assert_eq!(ev("ascii", &[s("B")]), i(66));
    }
    #[test]
    fn ch00() {
        assert_eq!(ev("chr", &[i(65)]), s("A"));
    }
    #[test]
    fn ch01() {
        assert_eq!(ev("chr", &[i(97)]), s("a"));
    }
    #[test]
    fn ch02() {
        assert_eq!(ev("chr", &[i(48)]), s("0"));
    }
    #[test]
    fn ch03() {
        assert_eq!(ev("chr", &[i(32)]), s(" "));
    }
    #[test]
    fn ch04() {
        assert_eq!(ev("chr", &[i(90)]), s("Z"));
    }
    #[test]
    fn ch05() {
        assert_eq!(ev("chr", &[i(122)]), s("z"));
    }
    #[test]
    fn ch06() {
        assert_eq!(ev("chr", &[i(49)]), s("1"));
    }
    #[test]
    fn ch07() {
        assert_eq!(ev("chr", &[i(33)]), s("!"));
    }
    #[test]
    fn ch08() {
        assert_eq!(ev("chr", &[null()]), null());
    }
    #[test]
    fn ch09() {
        assert_eq!(ev("chr", &[i(66)]), s("B"));
    }
}

// initcap — 20 tests
mod initcap_s {
    use super::*;
    #[test]
    fn ic00() {
        assert_eq!(ev("initcap", &[s("hello")]), s("Hello"));
    }
    #[test]
    fn ic01() {
        assert_eq!(ev("initcap", &[s("hello world")]), s("Hello World"));
    }
    #[test]
    fn ic02() {
        assert_eq!(ev("initcap", &[s("HELLO")]), s("Hello"));
    }
    #[test]
    fn ic03() {
        assert_eq!(ev("initcap", &[s("HELLO WORLD")]), s("Hello World"));
    }
    #[test]
    fn ic04() {
        assert_eq!(ev("initcap", &[s("")]), s(""));
    }
    #[test]
    fn ic05() {
        assert_eq!(ev("initcap", &[null()]), null());
    }
    #[test]
    fn ic06() {
        assert_eq!(ev("initcap", &[s("a")]), s("A"));
    }
    #[test]
    fn ic07() {
        assert_eq!(ev("initcap", &[s("abc def")]), s("Abc Def"));
    }
    #[test]
    fn ic08() {
        assert_eq!(ev("initcap", &[s("ABC DEF")]), s("Abc Def"));
    }
    #[test]
    fn ic09() {
        assert_eq!(ev("initcap", &[s("aBc dEf")]), s("Abc Def"));
    }
    #[test]
    fn ic10() {
        assert_eq!(ev("initcap", &[s("one two three")]), s("One Two Three"));
    }
    #[test]
    fn ic11() {
        assert_eq!(ev("initcap", &[s("x y z")]), s("X Y Z"));
    }
    #[test]
    fn ic12() {
        assert_eq!(ev("initcap", &[s("foo bar baz")]), s("Foo Bar Baz"));
    }
    #[test]
    fn ic13() {
        assert_eq!(ev("initcap", &[s("test")]), s("Test"));
    }
    #[test]
    fn ic14() {
        assert_eq!(ev("initcap", &[s("rust lang")]), s("Rust Lang"));
    }
    #[test]
    fn ic15() {
        assert_eq!(ev("initcap", &[s("go lang")]), s("Go Lang"));
    }
    #[test]
    fn ic16() {
        assert_eq!(ev("initcap", &[s("hi")]), s("Hi"));
    }
    #[test]
    fn ic17() {
        assert_eq!(ev("initcap", &[s("ok")]), s("Ok"));
    }
    #[test]
    fn ic18() {
        assert_eq!(ev("initcap", &[s("no")]), s("No"));
    }
    #[test]
    fn ic19() {
        assert_eq!(ev("initcap", &[s("yes")]), s("Yes"));
    }
}

// lpad / rpad / split_part — 30 tests
mod pad_split_s {
    use super::*;
    #[test]
    fn lp00() {
        assert_eq!(ev("lpad", &[s("hi"), i(5), s(" ")]), s("   hi"));
    }
    #[test]
    fn lp01() {
        assert_eq!(ev("lpad", &[s("hi"), i(2), s(" ")]), s("hi"));
    }
    #[test]
    fn lp02() {
        assert_eq!(ev("lpad", &[s("hi"), i(5), s("x")]), s("xxxhi"));
    }
    #[test]
    fn lp03() {
        assert_eq!(ev("lpad", &[s("abc"), i(6), s("0")]), s("000abc"));
    }
    #[test]
    fn lp04() {
        assert_eq!(ev("lpad", &[s(""), i(3), s("a")]), s("aaa"));
    }
    #[test]
    fn lp05() {
        assert_eq!(ev("lpad", &[null(), i(5), s(" ")]), null());
    }
    #[test]
    fn lp06() {
        assert_eq!(ev("lpad", &[s("x"), i(1), s(" ")]), s("x"));
    }
    #[test]
    fn lp07() {
        assert_eq!(ev("lpad", &[s("x"), i(4), s("_")]), s("___x"));
    }
    #[test]
    fn lp08() {
        assert_eq!(ev("lpad", &[s("ab"), i(5), s(".")]), s("...ab"));
    }
    #[test]
    fn lp09() {
        assert_eq!(ev("lpad", &[s("test"), i(8), s("-")]), s("----test"));
    }
    #[test]
    fn rp00() {
        assert_eq!(ev("rpad", &[s("hi"), i(5), s(" ")]), s("hi   "));
    }
    #[test]
    fn rp01() {
        assert_eq!(ev("rpad", &[s("hi"), i(2), s(" ")]), s("hi"));
    }
    #[test]
    fn rp02() {
        assert_eq!(ev("rpad", &[s("hi"), i(5), s("x")]), s("hixxx"));
    }
    #[test]
    fn rp03() {
        assert_eq!(ev("rpad", &[s("abc"), i(6), s("0")]), s("abc000"));
    }
    #[test]
    fn rp04() {
        assert_eq!(ev("rpad", &[s(""), i(3), s("a")]), s("aaa"));
    }
    #[test]
    fn rp05() {
        assert_eq!(ev("rpad", &[null(), i(5), s(" ")]), null());
    }
    #[test]
    fn rp06() {
        assert_eq!(ev("rpad", &[s("x"), i(1), s(" ")]), s("x"));
    }
    #[test]
    fn rp07() {
        assert_eq!(ev("rpad", &[s("x"), i(4), s("_")]), s("x___"));
    }
    #[test]
    fn rp08() {
        assert_eq!(ev("rpad", &[s("ab"), i(5), s(".")]), s("ab..."));
    }
    #[test]
    fn rp09() {
        assert_eq!(ev("rpad", &[s("test"), i(8), s("-")]), s("test----"));
    }
    #[test]
    fn sp00() {
        assert_eq!(ev("split_part", &[s("a,b,c"), s(","), i(1)]), s("a"));
    }
    #[test]
    fn sp01() {
        assert_eq!(ev("split_part", &[s("a,b,c"), s(","), i(2)]), s("b"));
    }
    #[test]
    fn sp02() {
        assert_eq!(ev("split_part", &[s("a,b,c"), s(","), i(3)]), s("c"));
    }
    #[test]
    fn sp03() {
        assert_eq!(
            ev("split_part", &[s("hello-world"), s("-"), i(1)]),
            s("hello")
        );
    }
    #[test]
    fn sp04() {
        assert_eq!(
            ev("split_part", &[s("hello-world"), s("-"), i(2)]),
            s("world")
        );
    }
    #[test]
    fn sp05() {
        assert_eq!(ev("split_part", &[s("a.b.c.d"), s("."), i(1)]), s("a"));
    }
    #[test]
    fn sp06() {
        assert_eq!(ev("split_part", &[s("a.b.c.d"), s("."), i(4)]), s("d"));
    }
    #[test]
    fn sp07() {
        assert_eq!(ev("split_part", &[null(), s(","), i(1)]), null());
    }
    #[test]
    fn sp08() {
        assert_eq!(ev("split_part", &[s("x:y"), s(":"), i(1)]), s("x"));
    }
    #[test]
    fn sp09() {
        assert_eq!(ev("split_part", &[s("x:y"), s(":"), i(2)]), s("y"));
    }
}

// bit operations — 30 tests
mod bit_s {
    use super::*;
    #[test]
    fn ba00() {
        assert_eq!(ev("bit_and", &[i(0b1100), i(0b1010)]), i(0b1000));
    }
    #[test]
    fn ba01() {
        assert_eq!(ev("bit_and", &[i(0xFF), i(0x0F)]), i(0x0F));
    }
    #[test]
    fn ba02() {
        assert_eq!(ev("bit_and", &[i(0), i(0xFF)]), i(0));
    }
    #[test]
    fn ba03() {
        assert_eq!(ev("bit_and", &[i(0xFF), i(0xFF)]), i(0xFF));
    }
    #[test]
    fn ba04() {
        assert_eq!(ev("bit_and", &[null(), i(5)]), null());
    }
    #[test]
    fn bo00() {
        assert_eq!(ev("bit_or", &[i(0b1100), i(0b1010)]), i(0b1110));
    }
    #[test]
    fn bo01() {
        assert_eq!(ev("bit_or", &[i(0), i(0xFF)]), i(0xFF));
    }
    #[test]
    fn bo02() {
        assert_eq!(ev("bit_or", &[i(0xFF), i(0)]), i(0xFF));
    }
    #[test]
    fn bo03() {
        assert_eq!(ev("bit_or", &[i(0), i(0)]), i(0));
    }
    #[test]
    fn bo04() {
        assert_eq!(ev("bit_or", &[null(), i(5)]), null());
    }
    #[test]
    fn bx00() {
        assert_eq!(ev("bit_xor", &[i(0b1100), i(0b1010)]), i(0b0110));
    }
    #[test]
    fn bx01() {
        assert_eq!(ev("bit_xor", &[i(0xFF), i(0xFF)]), i(0));
    }
    #[test]
    fn bx02() {
        assert_eq!(ev("bit_xor", &[i(0), i(0xFF)]), i(0xFF));
    }
    #[test]
    fn bx03() {
        assert_eq!(ev("bit_xor", &[i(0xFF), i(0)]), i(0xFF));
    }
    #[test]
    fn bx04() {
        assert_eq!(ev("bit_xor", &[null(), i(5)]), null());
    }
    #[test]
    fn bn00() {
        assert_eq!(ev("bit_not", &[i(0)]), i(!0i64));
    }
    #[test]
    fn bn01() {
        assert_eq!(ev("bit_not", &[null()]), null());
    }
    #[test]
    fn sl00() {
        assert_eq!(ev("bit_shift_left", &[i(1), i(0)]), i(1));
    }
    #[test]
    fn sl01() {
        assert_eq!(ev("bit_shift_left", &[i(1), i(1)]), i(2));
    }
    #[test]
    fn sl02() {
        assert_eq!(ev("bit_shift_left", &[i(1), i(2)]), i(4));
    }
    #[test]
    fn sl03() {
        assert_eq!(ev("bit_shift_left", &[i(1), i(3)]), i(8));
    }
    #[test]
    fn sl04() {
        assert_eq!(ev("bit_shift_left", &[i(1), i(4)]), i(16));
    }
    #[test]
    fn sl05() {
        assert_eq!(ev("bit_shift_left", &[i(1), i(10)]), i(1024));
    }
    #[test]
    fn sr00() {
        assert_eq!(ev("bit_shift_right", &[i(16), i(0)]), i(16));
    }
    #[test]
    fn sr01() {
        assert_eq!(ev("bit_shift_right", &[i(16), i(1)]), i(8));
    }
    #[test]
    fn sr02() {
        assert_eq!(ev("bit_shift_right", &[i(16), i(2)]), i(4));
    }
    #[test]
    fn sr03() {
        assert_eq!(ev("bit_shift_right", &[i(16), i(3)]), i(2));
    }
    #[test]
    fn sr04() {
        assert_eq!(ev("bit_shift_right", &[i(16), i(4)]), i(1));
    }
    #[test]
    fn sr05() {
        assert_eq!(ev("bit_shift_right", &[i(1024), i(10)]), i(1));
    }
    #[test]
    fn sr06() {
        assert_eq!(ev("bit_shift_right", &[null(), i(1)]), null());
    }
}

// position / char_length / ltrim / rtrim — 30 tests
mod pos_charlen_s {
    use super::*;
    #[test]
    fn ps00() {
        assert_eq!(ev("position", &[s("lo"), s("hello")]), i(4));
    }
    #[test]
    fn ps01() {
        assert_eq!(ev("position", &[s("he"), s("hello")]), i(1));
    }
    #[test]
    fn ps02() {
        assert_eq!(ev("position", &[s("xyz"), s("hello")]), i(0));
    }
    #[test]
    fn ps03() {
        assert_eq!(ev("position", &[s(""), s("hello")]), i(1));
    }
    #[test]
    fn ps04() {
        assert_eq!(ev("position", &[s("o"), s("hello")]), i(5));
    }
    #[test]
    fn ps05() {
        assert_eq!(ev("position", &[s("ll"), s("hello")]), i(3));
    }
    #[test]
    fn ps06() {
        assert_eq!(ev("position", &[s("a"), s("abc")]), i(1));
    }
    #[test]
    fn ps07() {
        assert_eq!(ev("position", &[s("b"), s("abc")]), i(2));
    }
    #[test]
    fn ps08() {
        assert_eq!(ev("position", &[s("c"), s("abc")]), i(3));
    }
    #[test]
    fn ps09() {
        assert_eq!(ev("position", &[null(), s("abc")]), null());
    }
    #[test]
    fn cl00() {
        assert_eq!(ev("char_length", &[s("")]), i(0));
    }
    #[test]
    fn cl01() {
        assert_eq!(ev("char_length", &[s("a")]), i(1));
    }
    #[test]
    fn cl02() {
        assert_eq!(ev("char_length", &[s("hello")]), i(5));
    }
    #[test]
    fn cl03() {
        assert_eq!(ev("char_length", &[null()]), null());
    }
    #[test]
    fn cl04() {
        assert_eq!(ev("char_length", &[s("abc")]), i(3));
    }
    #[test]
    fn lt00() {
        assert_eq!(ev("ltrim", &[s("  hello")]), s("hello"));
    }
    #[test]
    fn lt01() {
        assert_eq!(ev("ltrim", &[s("hello  ")]), s("hello  "));
    }
    #[test]
    fn lt02() {
        assert_eq!(ev("ltrim", &[s("  hello  ")]), s("hello  "));
    }
    #[test]
    fn lt03() {
        assert_eq!(ev("ltrim", &[s("hello")]), s("hello"));
    }
    #[test]
    fn lt04() {
        assert_eq!(ev("ltrim", &[s("")]), s(""));
    }
    #[test]
    fn lt05() {
        assert_eq!(ev("ltrim", &[null()]), null());
    }
    #[test]
    fn lt06() {
        assert_eq!(ev("ltrim", &[s("   ")]), s(""));
    }
    #[test]
    fn rt00() {
        assert_eq!(ev("rtrim", &[s("hello  ")]), s("hello"));
    }
    #[test]
    fn rt01() {
        assert_eq!(ev("rtrim", &[s("  hello")]), s("  hello"));
    }
    #[test]
    fn rt02() {
        assert_eq!(ev("rtrim", &[s("  hello  ")]), s("  hello"));
    }
    #[test]
    fn rt03() {
        assert_eq!(ev("rtrim", &[s("hello")]), s("hello"));
    }
    #[test]
    fn rt04() {
        assert_eq!(ev("rtrim", &[s("")]), s(""));
    }
    #[test]
    fn rt05() {
        assert_eq!(ev("rtrim", &[null()]), null());
    }
    #[test]
    fn rt06() {
        assert_eq!(ev("rtrim", &[s("   ")]), s(""));
    }
    #[test]
    fn rt07() {
        assert_eq!(ev("rtrim", &[s("x  ")]), s("x"));
    }
}

// trunc / div — 20 tests
mod trunc_div_s {
    use super::*;
    #[test]
    fn tr00() {
        assert_eq!(ev("trunc", &[f(3.7)]), f(3.0));
    }
    #[test]
    fn tr01() {
        assert_eq!(ev("trunc", &[f(3.2)]), f(3.0));
    }
    #[test]
    fn tr02() {
        assert_eq!(ev("trunc", &[f(-3.7)]), f(-3.0));
    }
    #[test]
    fn tr03() {
        assert_eq!(ev("trunc", &[f(-3.2)]), f(-3.0));
    }
    #[test]
    fn tr04() {
        assert_eq!(ev("trunc", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn tr05() {
        assert_eq!(ev("trunc", &[null()]), null());
    }
    #[test]
    fn tr06() {
        assert_eq!(ev("trunc", &[f(1.9)]), f(1.0));
    }
    #[test]
    fn tr07() {
        assert_eq!(ev("trunc", &[f(-1.9)]), f(-1.0));
    }
    #[test]
    fn tr08() {
        assert_eq!(ev("trunc", &[f(99.99)]), f(99.0));
    }
    #[test]
    fn tr09() {
        assert_eq!(ev("trunc", &[f(100.0)]), f(100.0));
    }
    #[test]
    fn dv00() {
        assert_eq!(ev("div", &[i(10), i(3)]), i(3));
    }
    #[test]
    fn dv01() {
        assert_eq!(ev("div", &[i(10), i(5)]), i(2));
    }
    #[test]
    fn dv02() {
        assert_eq!(ev("div", &[i(7), i(2)]), i(3));
    }
    #[test]
    fn dv03() {
        assert_eq!(ev("div", &[i(100), i(7)]), i(14));
    }
    #[test]
    fn dv04() {
        assert_eq!(ev("div", &[i(0), i(5)]), i(0));
    }
    #[test]
    fn dv05() {
        assert_eq!(ev("div", &[i(15), i(4)]), i(3));
    }
    #[test]
    fn dv06() {
        assert_eq!(ev("div", &[i(20), i(6)]), i(3));
    }
    #[test]
    fn dv07() {
        assert_eq!(ev("div", &[i(9), i(3)]), i(3));
    }
    #[test]
    fn dv08() {
        assert_eq!(ev("div", &[null(), i(3)]), null());
    }
    #[test]
    fn dv09() {
        assert_eq!(ev("div", &[i(100), i(10)]), i(10));
    }
}

// if_null — 15 tests
mod ifnull_s {
    use super::*;
    #[test]
    fn in00() {
        assert_eq!(ev("if_null", &[null(), i(1)]), i(1));
    }
    #[test]
    fn in01() {
        assert_eq!(ev("if_null", &[i(1), i(2)]), i(1));
    }
    #[test]
    fn in02() {
        assert_eq!(ev("if_null", &[null(), s("b")]), s("b"));
    }
    #[test]
    fn in03() {
        assert_eq!(ev("if_null", &[s("a"), s("b")]), s("a"));
    }
    #[test]
    fn in04() {
        assert_eq!(ev("if_null", &[null(), f(1.5)]), f(1.5));
    }
    #[test]
    fn in05() {
        assert_eq!(ev("if_null", &[f(1.5), f(2.5)]), f(1.5));
    }
    #[test]
    fn in06() {
        assert_eq!(ev("if_null", &[null(), null()]), null());
    }
    #[test]
    fn in07() {
        assert_eq!(ev("if_null", &[i(0), i(1)]), i(0));
    }
    #[test]
    fn in08() {
        assert_eq!(ev("if_null", &[null(), i(0)]), i(0));
    }
    #[test]
    fn in09() {
        assert_eq!(ev("if_null", &[i(42), i(99)]), i(42));
    }
    #[test]
    fn in10() {
        assert_eq!(ev("if_null", &[null(), i(100)]), i(100));
    }
    #[test]
    fn in11() {
        assert_eq!(ev("if_null", &[s("x"), s("y")]), s("x"));
    }
    #[test]
    fn in12() {
        assert_eq!(ev("if_null", &[null(), s("y")]), s("y"));
    }
    #[test]
    fn in13() {
        assert_eq!(ev("if_null", &[f(0.0), f(1.0)]), f(0.0));
    }
    #[test]
    fn in14() {
        assert_eq!(ev("if_null", &[null(), f(0.0)]), f(0.0));
    }
}

// cbrt — 10 tests
mod cbrt_s {
    use super::*;
    #[test]
    fn cb00() {
        assert_eq!(ev("cbrt", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn cb01() {
        assert_eq!(ev("cbrt", &[f(1.0)]), f(1.0));
    }
    #[test]
    fn cb02() {
        assert_eq!(ev("cbrt", &[f(8.0)]), f(2.0));
    }
    #[test]
    fn cb03() {
        assert_eq!(ev("cbrt", &[f(27.0)]), f(3.0));
    }
    #[test]
    fn cb04() {
        assert_eq!(ev("cbrt", &[f(64.0)]), f(4.0));
    }
    #[test]
    fn cb05() {
        assert_eq!(ev("cbrt", &[f(125.0)]), f(5.0));
    }
    #[test]
    fn cb06() {
        assert_eq!(ev("cbrt", &[f(-8.0)]), f(-2.0));
    }
    #[test]
    fn cb07() {
        assert_eq!(ev("cbrt", &[f(-27.0)]), f(-3.0));
    }
    #[test]
    fn cb08() {
        assert_eq!(ev("cbrt", &[null()]), null());
    }
    #[test]
    fn cb09() {
        assert_eq!(ev("cbrt", &[f(1000.0)]), f(10.0));
    }
}

// degrees / radians — 20 tests
mod degrad_s {
    use super::*;
    use std::f64::consts::PI;
    #[test]
    fn dg00() {
        let v = ev("degrees", &[f(0.0)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn dg01() {
        let v = ev("degrees", &[f(PI)]);
        match v {
            Value::F64(v) => assert!((v - 180.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn dg02() {
        let v = ev("degrees", &[f(PI / 2.0)]);
        match v {
            Value::F64(v) => assert!((v - 90.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn dg03() {
        let v = ev("degrees", &[f(2.0 * PI)]);
        match v {
            Value::F64(v) => assert!((v - 360.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn dg04() {
        assert_eq!(ev("degrees", &[null()]), null());
    }
    #[test]
    fn dg05() {
        let v = ev("degrees", &[f(PI / 4.0)]);
        match v {
            Value::F64(v) => assert!((v - 45.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn dg06() {
        let v = ev("degrees", &[f(PI / 6.0)]);
        match v {
            Value::F64(v) => assert!((v - 30.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn dg07() {
        let v = ev("degrees", &[f(PI / 3.0)]);
        match v {
            Value::F64(v) => assert!((v - 60.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn dg08() {
        let v = ev("degrees", &[f(3.0 * PI / 2.0)]);
        match v {
            Value::F64(v) => assert!((v - 270.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn dg09() {
        let v = ev("degrees", &[f(PI / 180.0)]);
        match v {
            Value::F64(v) => assert!((v - 1.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn rd00() {
        let v = ev("radians", &[f(0.0)]);
        match v {
            Value::F64(v) => assert!(v.abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn rd01() {
        let v = ev("radians", &[f(180.0)]);
        match v {
            Value::F64(v) => assert!((v - PI).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn rd02() {
        let v = ev("radians", &[f(90.0)]);
        match v {
            Value::F64(v) => assert!((v - PI / 2.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn rd03() {
        let v = ev("radians", &[f(360.0)]);
        match v {
            Value::F64(v) => assert!((v - 2.0 * PI).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn rd04() {
        assert_eq!(ev("radians", &[null()]), null());
    }
    #[test]
    fn rd05() {
        let v = ev("radians", &[f(45.0)]);
        match v {
            Value::F64(v) => assert!((v - PI / 4.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn rd06() {
        let v = ev("radians", &[f(30.0)]);
        match v {
            Value::F64(v) => assert!((v - PI / 6.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn rd07() {
        let v = ev("radians", &[f(60.0)]);
        match v {
            Value::F64(v) => assert!((v - PI / 3.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn rd08() {
        let v = ev("radians", &[f(270.0)]);
        match v {
            Value::F64(v) => assert!((v - 3.0 * PI / 2.0).abs() < 0.001),
            _ => panic!(),
        }
    }
    #[test]
    fn rd09() {
        let v = ev("radians", &[f(1.0)]);
        match v {
            Value::F64(v) => assert!((v - PI / 180.0).abs() < 0.001),
            _ => panic!(),
        }
    }
}
