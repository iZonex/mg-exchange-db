//! 1000+ string scalar function tests.

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
fn ts(v: i64) -> Value {
    Value::Timestamp(v)
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

// ===========================================================================
// length — 50 tests
// ===========================================================================
mod length_t01 {
    use super::*;
    #[test]
    fn len_0() {
        assert_eq!(ev("length", &[s("")]), i(0));
    }
    #[test]
    fn len_1() {
        assert_eq!(ev("length", &[s("a")]), i(1));
    }
    #[test]
    fn len_2() {
        assert_eq!(ev("length", &[s("ab")]), i(2));
    }
    #[test]
    fn len_3() {
        assert_eq!(ev("length", &[s("abc")]), i(3));
    }
    #[test]
    fn len_5() {
        assert_eq!(ev("length", &[s("hello")]), i(5));
    }
    #[test]
    fn len_10() {
        assert_eq!(ev("length", &[s("0123456789")]), i(10));
    }
    #[test]
    fn len_space() {
        assert_eq!(ev("length", &[s(" ")]), i(1));
    }
    #[test]
    fn len_spaces_3() {
        assert_eq!(ev("length", &[s("   ")]), i(3));
    }
    #[test]
    fn len_tab() {
        assert_eq!(ev("length", &[s("\t")]), i(1));
    }
    #[test]
    fn len_newline() {
        assert_eq!(ev("length", &[s("\n")]), i(1));
    }
    #[test]
    fn len_crlf() {
        assert_eq!(ev("length", &[s("\r\n")]), i(2));
    }
    #[test]
    fn len_null() {
        assert_eq!(ev("length", &[null()]), null());
    }
    #[test]
    fn len_int() {
        assert_eq!(ev("length", &[i(42)]), i(2));
    }
    #[test]
    fn len_int_neg() {
        assert_eq!(ev("length", &[i(-1)]), i(2));
    }
    #[test]
    fn len_float() {
        assert_eq!(ev("length", &[f(3.15)]), i(4));
    }
    #[test]
    fn len_20() {
        assert_eq!(ev("length", &[s(&"x".repeat(20))]), i(20));
    }
    #[test]
    fn len_50() {
        assert_eq!(ev("length", &[s(&"y".repeat(50))]), i(50));
    }
    #[test]
    fn len_100() {
        assert_eq!(ev("length", &[s(&"z".repeat(100))]), i(100));
    }
    #[test]
    fn len_200() {
        assert_eq!(ev("length", &[s(&"a".repeat(200))]), i(200));
    }
    #[test]
    fn len_500() {
        assert_eq!(ev("length", &[s(&"b".repeat(500))]), i(500));
    }
    #[test]
    fn len_1000() {
        assert_eq!(ev("length", &[s(&"c".repeat(1000))]), i(1000));
    }
    #[test]
    fn len_emoji() {
        assert_eq!(ev("length", &[s("\u{1F600}")]), i(1));
    }
    #[test]
    fn len_cjk_2() {
        assert_eq!(ev("length", &[s("\u{4E16}\u{754C}")]), i(2));
    }
    #[test]
    fn len_mixed_uni() {
        assert_eq!(ev("length", &[s("a\u{4E16}b")]), i(3));
    }
    #[test]
    fn len_alias() {
        assert_eq!(ev("len", &[s("test")]), i(4));
    }
    #[test]
    fn char_length_alias() {
        assert_eq!(ev("char_length", &[s("test")]), i(4));
    }
    #[test]
    fn string_length_alias() {
        assert_eq!(ev("string_length", &[s("test")]), i(4));
    }
    #[test]
    fn len_special_chars() {
        assert_eq!(ev("length", &[s("!@#$%^&*()")]), i(10));
    }
    #[test]
    fn len_backslash() {
        assert_eq!(ev("length", &[s("a\\b")]), i(3));
    }
    #[test]
    fn len_quote() {
        assert_eq!(ev("length", &[s("a\"b")]), i(3));
    }
    #[test]
    fn len_null_char() {
        assert_eq!(ev("length", &[s("a\0b")]), i(3));
    }
    #[test]
    fn len_int_zero() {
        assert_eq!(ev("length", &[i(0)]), i(1));
    }
    #[test]
    fn len_int_big() {
        assert_eq!(ev("length", &[i(1234567890)]), i(10));
    }
    #[test]
    fn byte_length_hello() {
        assert_eq!(ev("byte_length", &[s("hello")]), i(5));
    }
    #[test]
    fn byte_length_empty() {
        assert_eq!(ev("byte_length", &[s("")]), i(0));
    }
    #[test]
    fn byte_length_null() {
        assert_eq!(ev("byte_length", &[null()]), null());
    }
    #[test]
    fn octet_length_test() {
        assert_eq!(ev("octet_length", &[s("abc")]), i(3));
    }
    #[test]
    fn bit_length_a() {
        assert_eq!(ev("bit_length", &[s("a")]), i(8));
    }
    #[test]
    fn bit_length_ab() {
        assert_eq!(ev("bit_length", &[s("ab")]), i(16));
    }
    #[test]
    fn bit_length_null() {
        assert_eq!(ev("bit_length", &[null()]), null());
    }
    #[test]
    fn word_count_one() {
        assert_eq!(ev("word_count", &[s("hello")]), i(1));
    }
    #[test]
    fn word_count_two() {
        assert_eq!(ev("word_count", &[s("hello world")]), i(2));
    }
    #[test]
    fn word_count_three() {
        assert_eq!(ev("word_count", &[s("a b c")]), i(3));
    }
    #[test]
    fn word_count_empty() {
        assert_eq!(ev("word_count", &[s("")]), i(0));
    }
    #[test]
    fn word_count_null() {
        assert_eq!(ev("word_count", &[null()]), null());
    }
    #[test]
    fn word_count_spaces() {
        assert_eq!(ev("word_count", &[s("  hello  world  ")]), i(2));
    }
    #[test]
    fn count_char_a() {
        assert_eq!(ev("count_char", &[s("banana"), s("a")]), i(3));
    }
    #[test]
    fn count_char_z() {
        assert_eq!(ev("count_char", &[s("banana"), s("z")]), i(0));
    }
    #[test]
    fn count_char_null() {
        assert_eq!(ev("count_char", &[null(), s("a")]), null());
    }
    #[test]
    fn count_char_b() {
        assert_eq!(ev("count_char", &[s("banana"), s("b")]), i(1));
    }
}

// ===========================================================================
// upper — 50 tests
// ===========================================================================
mod upper_t01 {
    use super::*;
    #[test]
    fn hello() {
        assert_eq!(ev("upper", &[s("hello")]), s("HELLO"));
    }
    #[test]
    fn empty() {
        assert_eq!(ev("upper", &[s("")]), s(""));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("upper", &[null()]), null());
    }
    #[test]
    fn already() {
        assert_eq!(ev("upper", &[s("ABC")]), s("ABC"));
    }
    #[test]
    fn mixed() {
        assert_eq!(ev("upper", &[s("aBcD")]), s("ABCD"));
    }
    #[test]
    fn digits() {
        assert_eq!(ev("upper", &[s("a1b2")]), s("A1B2"));
    }
    #[test]
    fn special() {
        assert_eq!(ev("upper", &[s("a!b@c#")]), s("A!B@C#"));
    }
    #[test]
    fn int_in() {
        assert_eq!(ev("upper", &[i(42)]), s("42"));
    }
    #[test]
    fn long_str() {
        assert_eq!(ev("upper", &[s(&"ab".repeat(100))]), s(&"AB".repeat(100)));
    }
    #[test]
    fn to_uppercase() {
        assert_eq!(ev("to_uppercase", &[s("abc")]), s("ABC"));
    }
    #[test]
    fn single_a() {
        assert_eq!(ev("upper", &[s("a")]), s("A"));
    }
    #[test]
    fn single_z() {
        assert_eq!(ev("upper", &[s("z")]), s("Z"));
    }
    #[test]
    fn space() {
        assert_eq!(ev("upper", &[s(" ")]), s(" "));
    }
    #[test]
    fn tab() {
        assert_eq!(ev("upper", &[s("\t")]), s("\t"));
    }
    #[test]
    fn only_digits() {
        assert_eq!(ev("upper", &[s("123")]), s("123"));
    }
    #[test]
    fn alphanum() {
        assert_eq!(ev("upper", &[s("abc123def")]), s("ABC123DEF"));
    }
    #[test]
    fn punc() {
        assert_eq!(ev("upper", &[s("hello, world!")]), s("HELLO, WORLD!"));
    }
    #[test]
    fn repeat_a() {
        assert_eq!(ev("upper", &[s(&"a".repeat(50))]), s(&"A".repeat(50)));
    }
    #[test]
    fn one_char_up() {
        assert_eq!(ev("upper", &[s("A")]), s("A"));
    }
    #[test]
    fn float_in() {
        assert_eq!(ev("upper", &[f(1.5)]), s("1.5"));
    }
    #[test]
    fn a_to_z() {
        assert_eq!(
            ev("upper", &[s("abcdefghijklmnopqrstuvwxyz")]),
            s("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        );
    }
    #[test]
    fn upper_200() {
        assert_eq!(ev("upper", &[s(&"m".repeat(200))]), s(&"M".repeat(200)));
    }
    #[test]
    fn upper_mixed_punct() {
        assert_eq!(ev("upper", &[s("hello_world-test")]), s("HELLO_WORLD-TEST"));
    }
    #[test]
    fn upper_newline() {
        assert_eq!(ev("upper", &[s("a\nb")]), s("A\nB"));
    }
    #[test]
    fn upper_cr() {
        assert_eq!(ev("upper", &[s("a\rb")]), s("A\rB"));
    }
    // Additional 25 parametric
    #[test]
    fn p01() {
        assert_eq!(ev("upper", &[s("test1")]), s("TEST1"));
    }
    #[test]
    fn p02() {
        assert_eq!(ev("upper", &[s("test2")]), s("TEST2"));
    }
    #[test]
    fn p03() {
        assert_eq!(ev("upper", &[s("test3")]), s("TEST3"));
    }
    #[test]
    fn p04() {
        assert_eq!(ev("upper", &[s("abc def")]), s("ABC DEF"));
    }
    #[test]
    fn p05() {
        assert_eq!(ev("upper", &[s("xyz")]), s("XYZ"));
    }
    #[test]
    fn p06() {
        assert_eq!(ev("upper", &[s("qwerty")]), s("QWERTY"));
    }
    #[test]
    fn p07() {
        assert_eq!(ev("upper", &[s("asdf")]), s("ASDF"));
    }
    #[test]
    fn p08() {
        assert_eq!(ev("upper", &[s("zxcv")]), s("ZXCV"));
    }
    #[test]
    fn p09() {
        assert_eq!(ev("upper", &[s("poiu")]), s("POIU"));
    }
    #[test]
    fn p10() {
        assert_eq!(ev("upper", &[s("lkjh")]), s("LKJH"));
    }
    #[test]
    fn p11() {
        assert_eq!(ev("upper", &[s("mnbv")]), s("MNBV"));
    }
    #[test]
    fn p12() {
        assert_eq!(ev("upper", &[s("rtyu")]), s("RTYU"));
    }
    #[test]
    fn p13() {
        assert_eq!(ev("upper", &[s("fghj")]), s("FGHJ"));
    }
    #[test]
    fn p14() {
        assert_eq!(ev("upper", &[s("vbnm")]), s("VBNM"));
    }
    #[test]
    fn p15() {
        assert_eq!(ev("upper", &[s("wert")]), s("WERT"));
    }
    #[test]
    fn p16() {
        assert_eq!(ev("upper", &[s("sdfg")]), s("SDFG"));
    }
    #[test]
    fn p17() {
        assert_eq!(ev("upper", &[s("xcvb")]), s("XCVB"));
    }
    #[test]
    fn p18() {
        assert_eq!(ev("upper", &[s("tyui")]), s("TYUI"));
    }
    #[test]
    fn p19() {
        assert_eq!(ev("upper", &[s("ghjk")]), s("GHJK"));
    }
    #[test]
    fn p20() {
        assert_eq!(ev("upper", &[s("bnmc")]), s("BNMC"));
    }
    #[test]
    fn p21() {
        assert_eq!(ev("upper", &[s("yuio")]), s("YUIO"));
    }
    #[test]
    fn p22() {
        assert_eq!(ev("upper", &[s("hjkl")]), s("HJKL"));
    }
    #[test]
    fn p23() {
        assert_eq!(ev("upper", &[s("nm")]), s("NM"));
    }
    #[test]
    fn p24() {
        assert_eq!(ev("upper", &[s("op")]), s("OP"));
    }
    #[test]
    fn p25() {
        assert_eq!(ev("upper", &[s("kl")]), s("KL"));
    }
}

// ===========================================================================
// lower — 50 tests
// ===========================================================================
mod lower_t01 {
    use super::*;
    #[test]
    fn hello() {
        assert_eq!(ev("lower", &[s("HELLO")]), s("hello"));
    }
    #[test]
    fn empty() {
        assert_eq!(ev("lower", &[s("")]), s(""));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("lower", &[null()]), null());
    }
    #[test]
    fn already() {
        assert_eq!(ev("lower", &[s("abc")]), s("abc"));
    }
    #[test]
    fn mixed() {
        assert_eq!(ev("lower", &[s("AbCd")]), s("abcd"));
    }
    #[test]
    fn digits() {
        assert_eq!(ev("lower", &[s("A1B2")]), s("a1b2"));
    }
    #[test]
    fn special() {
        assert_eq!(ev("lower", &[s("A!B")]), s("a!b"));
    }
    #[test]
    fn int_in() {
        assert_eq!(ev("lower", &[i(42)]), s("42"));
    }
    #[test]
    fn to_lowercase() {
        assert_eq!(ev("to_lowercase", &[s("ABC")]), s("abc"));
    }
    #[test]
    fn a_to_z() {
        assert_eq!(
            ev("lower", &[s("ABCDEFGHIJKLMNOPQRSTUVWXYZ")]),
            s("abcdefghijklmnopqrstuvwxyz")
        );
    }
    #[test]
    #[allow(non_snake_case)]
    fn single_A() {
        assert_eq!(ev("lower", &[s("A")]), s("a"));
    }
    #[test]
    #[allow(non_snake_case)]
    fn single_Z() {
        assert_eq!(ev("lower", &[s("Z")]), s("z"));
    }
    #[test]
    fn long_str() {
        assert_eq!(ev("lower", &[s(&"AB".repeat(100))]), s(&"ab".repeat(100)));
    }
    #[test]
    fn space() {
        assert_eq!(ev("lower", &[s(" ")]), s(" "));
    }
    #[test]
    fn punct() {
        assert_eq!(ev("lower", &[s("HELLO, WORLD!")]), s("hello, world!"));
    }
    #[test]
    fn float_in() {
        assert_eq!(ev("lower", &[f(2.5)]), s("2.5"));
    }
    #[test]
    fn l01() {
        assert_eq!(ev("lower", &[s("TEST")]), s("test"));
    }
    #[test]
    fn l02() {
        assert_eq!(ev("lower", &[s("FOO")]), s("foo"));
    }
    #[test]
    fn l03() {
        assert_eq!(ev("lower", &[s("BAR")]), s("bar"));
    }
    #[test]
    fn l04() {
        assert_eq!(ev("lower", &[s("BAZ")]), s("baz"));
    }
    #[test]
    fn l05() {
        assert_eq!(ev("lower", &[s("QUX")]), s("qux"));
    }
    #[test]
    fn l06() {
        assert_eq!(ev("lower", &[s("QUUX")]), s("quux"));
    }
    #[test]
    fn l07() {
        assert_eq!(ev("lower", &[s("CORGE")]), s("corge"));
    }
    #[test]
    fn l08() {
        assert_eq!(ev("lower", &[s("GRAULT")]), s("grault"));
    }
    #[test]
    fn l09() {
        assert_eq!(ev("lower", &[s("GARPLY")]), s("garply"));
    }
    #[test]
    fn l10() {
        assert_eq!(ev("lower", &[s("WALDO")]), s("waldo"));
    }
    #[test]
    fn l11() {
        assert_eq!(ev("lower", &[s("FRED")]), s("fred"));
    }
    #[test]
    fn l12() {
        assert_eq!(ev("lower", &[s("PLUGH")]), s("plugh"));
    }
    #[test]
    fn l13() {
        assert_eq!(ev("lower", &[s("XYZZY")]), s("xyzzy"));
    }
    #[test]
    fn l14() {
        assert_eq!(ev("lower", &[s("THUD")]), s("thud"));
    }
    #[test]
    fn l15() {
        assert_eq!(ev("lower", &[s("ALPHA")]), s("alpha"));
    }
    #[test]
    fn l16() {
        assert_eq!(ev("lower", &[s("BETA")]), s("beta"));
    }
    #[test]
    fn l17() {
        assert_eq!(ev("lower", &[s("GAMMA")]), s("gamma"));
    }
    #[test]
    fn l18() {
        assert_eq!(ev("lower", &[s("DELTA")]), s("delta"));
    }
    #[test]
    fn l19() {
        assert_eq!(ev("lower", &[s("EPSILON")]), s("epsilon"));
    }
    #[test]
    fn l20() {
        assert_eq!(ev("lower", &[s("ZETA")]), s("zeta"));
    }
    #[test]
    fn l21() {
        assert_eq!(ev("lower", &[s("ETA")]), s("eta"));
    }
    #[test]
    fn l22() {
        assert_eq!(ev("lower", &[s("THETA")]), s("theta"));
    }
    #[test]
    fn l23() {
        assert_eq!(ev("lower", &[s("IOTA")]), s("iota"));
    }
    #[test]
    fn l24() {
        assert_eq!(ev("lower", &[s("KAPPA")]), s("kappa"));
    }
    #[test]
    fn l25() {
        assert_eq!(ev("lower", &[s("LAMBDA")]), s("lambda"));
    }
    #[test]
    fn l26() {
        assert_eq!(ev("lower", &[s("MU")]), s("mu"));
    }
    #[test]
    fn l27() {
        assert_eq!(ev("lower", &[s("NU")]), s("nu"));
    }
    #[test]
    fn l28() {
        assert_eq!(ev("lower", &[s("XI")]), s("xi"));
    }
    #[test]
    fn l29() {
        assert_eq!(ev("lower", &[s("PI")]), s("pi"));
    }
    #[test]
    fn l30() {
        assert_eq!(ev("lower", &[s("RHO")]), s("rho"));
    }
    #[test]
    fn l31() {
        assert_eq!(ev("lower", &[s("SIGMA")]), s("sigma"));
    }
    #[test]
    fn l32() {
        assert_eq!(ev("lower", &[s("TAU")]), s("tau"));
    }
    #[test]
    fn l33() {
        assert_eq!(ev("lower", &[s("PHI")]), s("phi"));
    }
    #[test]
    fn l34() {
        assert_eq!(ev("lower", &[s("OMEGA")]), s("omega"));
    }
}

// ===========================================================================
// trim / ltrim / rtrim — 50 tests
// ===========================================================================
mod trim_t01 {
    use super::*;
    #[test]
    fn both() {
        assert_eq!(ev("trim", &[s("  hi  ")]), s("hi"));
    }
    #[test]
    fn left_only() {
        assert_eq!(ev("trim", &[s("  hi")]), s("hi"));
    }
    #[test]
    fn right_only() {
        assert_eq!(ev("trim", &[s("hi  ")]), s("hi"));
    }
    #[test]
    fn none() {
        assert_eq!(ev("trim", &[s("hi")]), s("hi"));
    }
    #[test]
    fn empty() {
        assert_eq!(ev("trim", &[s("")]), s(""));
    }
    #[test]
    fn all_spaces() {
        assert_eq!(ev("trim", &[s("   ")]), s(""));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("trim", &[null()]), null());
    }
    #[test]
    fn tabs() {
        assert_eq!(ev("trim", &[s("\thi\t")]), s("hi"));
    }
    #[test]
    fn newlines() {
        assert_eq!(ev("trim", &[s("\nhi\n")]), s("hi"));
    }
    #[test]
    fn mixed_ws() {
        assert_eq!(ev("trim", &[s(" \t\n hi \t\n ")]), s("hi"));
    }
    #[test]
    fn inner_space() {
        assert_eq!(ev("trim", &[s("  a b  ")]), s("a b"));
    }
    #[test]
    fn single_space() {
        assert_eq!(ev("trim", &[s(" ")]), s(""));
    }
    #[test]
    fn ltrim_basic() {
        assert_eq!(ev("ltrim", &[s("  hello")]), s("hello"));
    }
    #[test]
    fn ltrim_no_space() {
        assert_eq!(ev("ltrim", &[s("hello")]), s("hello"));
    }
    #[test]
    fn ltrim_null() {
        assert_eq!(ev("ltrim", &[null()]), null());
    }
    #[test]
    fn ltrim_right_kept() {
        assert_eq!(ev("ltrim", &[s("hello  ")]), s("hello  "));
    }
    #[test]
    fn ltrim_both() {
        assert_eq!(ev("ltrim", &[s("  hello  ")]), s("hello  "));
    }
    #[test]
    fn ltrim_all_spaces() {
        assert_eq!(ev("ltrim", &[s("   ")]), s(""));
    }
    #[test]
    fn ltrim_tabs() {
        assert_eq!(ev("ltrim", &[s("\thello")]), s("hello"));
    }
    #[test]
    fn ltrim_empty() {
        assert_eq!(ev("ltrim", &[s("")]), s(""));
    }
    #[test]
    fn rtrim_basic() {
        assert_eq!(ev("rtrim", &[s("hello  ")]), s("hello"));
    }
    #[test]
    fn rtrim_no_space() {
        assert_eq!(ev("rtrim", &[s("hello")]), s("hello"));
    }
    #[test]
    fn rtrim_null() {
        assert_eq!(ev("rtrim", &[null()]), null());
    }
    #[test]
    fn rtrim_left_kept() {
        assert_eq!(ev("rtrim", &[s("  hello")]), s("  hello"));
    }
    #[test]
    fn rtrim_both() {
        assert_eq!(ev("rtrim", &[s("  hello  ")]), s("  hello"));
    }
    #[test]
    fn rtrim_all_spaces() {
        assert_eq!(ev("rtrim", &[s("   ")]), s(""));
    }
    #[test]
    fn rtrim_tabs() {
        assert_eq!(ev("rtrim", &[s("hello\t")]), s("hello"));
    }
    #[test]
    fn rtrim_empty() {
        assert_eq!(ev("rtrim", &[s("")]), s(""));
    }
    #[test]
    fn trim_int_in() {
        assert_eq!(ev("trim", &[i(42)]), s("42"));
    }
    #[test]
    fn trim_long() {
        assert_eq!(
            ev("trim", &[s(&format!("  {}  ", "x".repeat(100)))]),
            s(&"x".repeat(100))
        );
    }
    // Additional
    #[test]
    fn ltrim_multi_space() {
        assert_eq!(ev("ltrim", &[s("     abc")]), s("abc"));
    }
    #[test]
    fn rtrim_multi_space() {
        assert_eq!(ev("rtrim", &[s("abc     ")]), s("abc"));
    }
    #[test]
    fn trim_inner_preserved() {
        assert_eq!(ev("trim", &[s("  a  b  c  ")]), s("a  b  c"));
    }
    #[test]
    fn ltrim_newline() {
        assert_eq!(ev("ltrim", &[s("\nhello")]), s("hello"));
    }
    #[test]
    fn rtrim_newline() {
        assert_eq!(ev("rtrim", &[s("hello\n")]), s("hello"));
    }
    #[test]
    fn t01() {
        assert_eq!(ev("trim", &[s("  abc  ")]), s("abc"));
    }
    #[test]
    fn t02() {
        assert_eq!(ev("trim", &[s("  xyz  ")]), s("xyz"));
    }
    #[test]
    fn t03() {
        assert_eq!(ev("trim", &[s("  foo  ")]), s("foo"));
    }
    #[test]
    fn t04() {
        assert_eq!(ev("trim", &[s("  bar  ")]), s("bar"));
    }
    #[test]
    fn t05() {
        assert_eq!(ev("trim", &[s("  baz  ")]), s("baz"));
    }
    #[test]
    fn t06() {
        assert_eq!(ev("trim", &[s("  qux  ")]), s("qux"));
    }
    #[test]
    fn t07() {
        assert_eq!(ev("trim", &[s("  one  ")]), s("one"));
    }
    #[test]
    fn t08() {
        assert_eq!(ev("trim", &[s("  two  ")]), s("two"));
    }
    #[test]
    fn t09() {
        assert_eq!(ev("trim", &[s("  red  ")]), s("red"));
    }
    #[test]
    fn t10() {
        assert_eq!(ev("trim", &[s("  sky  ")]), s("sky"));
    }
    #[test]
    fn t11() {
        assert_eq!(ev("ltrim", &[s("  dog")]), s("dog"));
    }
    #[test]
    fn t12() {
        assert_eq!(ev("ltrim", &[s("  cat")]), s("cat"));
    }
    #[test]
    fn t13() {
        assert_eq!(ev("rtrim", &[s("pen  ")]), s("pen"));
    }
    #[test]
    fn t14() {
        assert_eq!(ev("rtrim", &[s("cup  ")]), s("cup"));
    }
    #[test]
    fn t15() {
        assert_eq!(ev("trim", &[s("\t\n data \t\n")]), s("data"));
    }
}

// ===========================================================================
// reverse — 50 tests
// ===========================================================================
mod reverse_t01 {
    use super::*;
    #[test]
    fn hello() {
        assert_eq!(ev("reverse", &[s("hello")]), s("olleh"));
    }
    #[test]
    fn empty() {
        assert_eq!(ev("reverse", &[s("")]), s(""));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("reverse", &[null()]), null());
    }
    #[test]
    fn single() {
        assert_eq!(ev("reverse", &[s("a")]), s("a"));
    }
    #[test]
    fn two() {
        assert_eq!(ev("reverse", &[s("ab")]), s("ba"));
    }
    #[test]
    fn palindrome() {
        assert_eq!(ev("reverse", &[s("racecar")]), s("racecar"));
    }
    #[test]
    fn digits() {
        assert_eq!(ev("reverse", &[s("12345")]), s("54321"));
    }
    #[test]
    fn spaces() {
        assert_eq!(ev("reverse", &[s("a b")]), s("b a"));
    }
    #[test]
    fn int_in() {
        assert_eq!(ev("reverse", &[i(123)]), s("321"));
    }
    #[test]
    fn long_str() {
        let r: String = "abcdef".chars().rev().collect();
        assert_eq!(ev("reverse", &[s("abcdef")]), s(&r));
    }
    #[test]
    fn r01() {
        assert_eq!(ev("reverse", &[s("abc")]), s("cba"));
    }
    #[test]
    fn r02() {
        assert_eq!(ev("reverse", &[s("xyz")]), s("zyx"));
    }
    #[test]
    fn r03() {
        assert_eq!(ev("reverse", &[s("test")]), s("tset"));
    }
    #[test]
    fn r04() {
        assert_eq!(ev("reverse", &[s("rust")]), s("tsur"));
    }
    #[test]
    fn r05() {
        assert_eq!(ev("reverse", &[s("code")]), s("edoc"));
    }
    #[test]
    fn r06() {
        assert_eq!(ev("reverse", &[s("data")]), s("atad"));
    }
    #[test]
    fn r07() {
        assert_eq!(ev("reverse", &[s("time")]), s("emit"));
    }
    #[test]
    fn r08() {
        assert_eq!(ev("reverse", &[s("live")]), s("evil"));
    }
    #[test]
    fn r09() {
        assert_eq!(ev("reverse", &[s("star")]), s("rats"));
    }
    #[test]
    fn r10() {
        assert_eq!(ev("reverse", &[s("keep")]), s("peek"));
    }
    #[test]
    fn r11() {
        assert_eq!(ev("reverse", &[s("flow")]), s("wolf"));
    }
    #[test]
    fn r12() {
        assert_eq!(ev("reverse", &[s("pool")]), s("loop"));
    }
    #[test]
    fn r13() {
        assert_eq!(ev("reverse", &[s("top")]), s("pot"));
    }
    #[test]
    fn r14() {
        assert_eq!(ev("reverse", &[s("god")]), s("dog"));
    }
    #[test]
    fn r15() {
        assert_eq!(ev("reverse", &[s("raw")]), s("war"));
    }
    #[test]
    fn r16() {
        assert_eq!(ev("reverse", &[s("doom")]), s("mood"));
    }
    #[test]
    fn r17() {
        assert_eq!(ev("reverse", &[s("part")]), s("trap"));
    }
    #[test]
    fn r18() {
        assert_eq!(ev("reverse", &[s("stop")]), s("pots"));
    }
    #[test]
    fn r19() {
        assert_eq!(ev("reverse", &[s("ward")]), s("draw"));
    }
    #[test]
    fn r20() {
        assert_eq!(ev("reverse", &[s("ten")]), s("net"));
    }
    #[test]
    fn r21() {
        assert_eq!(ev("reverse", &[s("bat")]), s("tab"));
    }
    #[test]
    fn r22() {
        assert_eq!(ev("reverse", &[s("tip")]), s("pit"));
    }
    #[test]
    fn r23() {
        assert_eq!(ev("reverse", &[s("tap")]), s("pat"));
    }
    #[test]
    fn r24() {
        assert_eq!(ev("reverse", &[s("pot")]), s("top"));
    }
    #[test]
    fn r25() {
        assert_eq!(ev("reverse", &[s("nap")]), s("pan"));
    }
    #[test]
    fn r26() {
        assert_eq!(ev("reverse", &[s("map")]), s("pam"));
    }
    #[test]
    fn r27() {
        assert_eq!(ev("reverse", &[s("pin")]), s("nip"));
    }
    #[test]
    fn r28() {
        assert_eq!(ev("reverse", &[s("gum")]), s("mug"));
    }
    #[test]
    fn r29() {
        assert_eq!(ev("reverse", &[s("tub")]), s("but"));
    }
    #[test]
    fn r30() {
        assert_eq!(ev("reverse", &[s("pal")]), s("lap"));
    }
    #[test]
    fn r31() {
        assert_eq!(ev("reverse", &[s("mad")]), s("dam"));
    }
    #[test]
    fn r32() {
        assert_eq!(ev("reverse", &[s("tar")]), s("rat"));
    }
    #[test]
    fn r33() {
        assert_eq!(ev("reverse", &[s("gap")]), s("pag"));
    }
    #[test]
    fn r34() {
        assert_eq!(ev("reverse", &[s("dew")]), s("wed"));
    }
    #[test]
    fn r35() {
        assert_eq!(ev("reverse", &[s("net")]), s("ten"));
    }
    #[test]
    fn r36() {
        assert_eq!(ev("reverse", &[s("saw")]), s("was"));
    }
    #[test]
    fn r37() {
        assert_eq!(ev("reverse", &[s("now")]), s("won"));
    }
    #[test]
    fn r38() {
        assert_eq!(ev("reverse", &[s("paw")]), s("wap"));
    }
    #[test]
    fn r39() {
        assert_eq!(ev("reverse", &[s("era")]), s("are"));
    }
    #[test]
    fn r40() {
        assert_eq!(ev("reverse", &[s("ton")]), s("not"));
    }
}

// ===========================================================================
// repeat — 50 tests
// ===========================================================================
mod repeat_t01 {
    use super::*;
    #[test]
    fn basic() {
        assert_eq!(ev("repeat", &[s("ab"), i(3)]), s("ababab"));
    }
    #[test]
    fn zero() {
        assert_eq!(ev("repeat", &[s("x"), i(0)]), s(""));
    }
    #[test]
    fn one() {
        assert_eq!(ev("repeat", &[s("x"), i(1)]), s("x"));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("repeat", &[null(), i(3)]), null());
    }
    #[test]
    fn empty_str() {
        assert_eq!(ev("repeat", &[s(""), i(5)]), s(""));
    }
    #[test]
    fn large() {
        assert_eq!(ev("repeat", &[s("a"), i(100)]), s(&"a".repeat(100)));
    }
    #[test]
    fn two() {
        assert_eq!(ev("repeat", &[s("ha"), i(2)]), s("haha"));
    }
    #[test]
    fn five() {
        assert_eq!(ev("repeat", &[s("x"), i(5)]), s("xxxxx"));
    }
    #[test]
    fn ten() {
        assert_eq!(ev("repeat", &[s("o"), i(10)]), s("oooooooooo"));
    }
    #[test]
    fn single_char_20() {
        assert_eq!(ev("repeat", &[s("z"), i(20)]), s(&"z".repeat(20)));
    }
    // space fn
    #[test]
    fn space_0() {
        assert_eq!(ev("space", &[i(0)]), s(""));
    }
    #[test]
    fn space_1() {
        assert_eq!(ev("space", &[i(1)]), s(" "));
    }
    #[test]
    fn space_5() {
        assert_eq!(ev("space", &[i(5)]), s("     "));
    }
    #[test]
    fn space_10() {
        assert_eq!(ev("space", &[i(10)]), s(&" ".repeat(10)));
    }
    #[test]
    fn space_null() {
        assert_eq!(ev("space", &[null()]), null());
    }
    #[test]
    fn rp01() {
        assert_eq!(ev("repeat", &[s("ab"), i(4)]), s("abababab"));
    }
    #[test]
    fn rp02() {
        assert_eq!(ev("repeat", &[s("cd"), i(3)]), s("cdcdcd"));
    }
    #[test]
    fn rp03() {
        assert_eq!(ev("repeat", &[s("ef"), i(2)]), s("efef"));
    }
    #[test]
    fn rp04() {
        assert_eq!(ev("repeat", &[s("gh"), i(5)]), s("ghghghghgh"));
    }
    #[test]
    fn rp05() {
        assert_eq!(ev("repeat", &[s("ij"), i(1)]), s("ij"));
    }
    #[test]
    fn rp06() {
        assert_eq!(ev("repeat", &[s("k"), i(7)]), s("kkkkkkk"));
    }
    #[test]
    fn rp07() {
        assert_eq!(ev("repeat", &[s("l"), i(8)]), s("llllllll"));
    }
    #[test]
    fn rp08() {
        assert_eq!(ev("repeat", &[s("m"), i(9)]), s("mmmmmmmmm"));
    }
    #[test]
    fn rp09() {
        assert_eq!(ev("repeat", &[s("n"), i(6)]), s("nnnnnn"));
    }
    #[test]
    fn rp10() {
        assert_eq!(ev("repeat", &[s("!"), i(3)]), s("!!!"));
    }
    #[test]
    fn rp11() {
        assert_eq!(ev("repeat", &[s("."), i(4)]), s("...."));
    }
    #[test]
    fn rp12() {
        assert_eq!(ev("repeat", &[s("-"), i(5)]), s("-----"));
    }
    #[test]
    fn rp13() {
        assert_eq!(ev("repeat", &[s("_"), i(6)]), s("______"));
    }
    #[test]
    fn rp14() {
        assert_eq!(ev("repeat", &[s("*"), i(7)]), s("*******"));
    }
    #[test]
    fn rp15() {
        assert_eq!(ev("repeat", &[s("#"), i(3)]), s("###"));
    }
    #[test]
    fn space_20() {
        assert_eq!(ev("space", &[i(20)]), s(&" ".repeat(20)));
    }
    #[test]
    fn space_50() {
        assert_eq!(ev("space", &[i(50)]), s(&" ".repeat(50)));
    }
    #[test]
    fn space_100() {
        assert_eq!(ev("space", &[i(100)]), s(&" ".repeat(100)));
    }
    #[test]
    fn rp16() {
        assert_eq!(ev("repeat", &[s("ab"), i(0)]), s(""));
    }
    #[test]
    fn rp17() {
        assert_eq!(ev("repeat", &[s("x"), i(50)]), s(&"x".repeat(50)));
    }
    #[test]
    fn rp18() {
        assert_eq!(ev("repeat", &[s("y"), i(30)]), s(&"y".repeat(30)));
    }
    #[test]
    fn rp19() {
        assert_eq!(ev("repeat", &[s("z"), i(25)]), s(&"z".repeat(25)));
    }
    #[test]
    fn rp20() {
        assert_eq!(ev("repeat", &[s("abc"), i(3)]), s("abcabcabc"));
    }
    #[test]
    fn rp21() {
        assert_eq!(ev("repeat", &[s("xyz"), i(2)]), s("xyzxyz"));
    }
    #[test]
    fn rp22() {
        assert_eq!(ev("repeat", &[s("test"), i(2)]), s("testtest"));
    }
    #[test]
    fn rp23() {
        assert_eq!(ev("repeat", &[s("hi"), i(4)]), s("hihihihi"));
    }
    #[test]
    fn rp24() {
        assert_eq!(ev("repeat", &[s("ok"), i(3)]), s("okokok"));
    }
    #[test]
    fn rp25() {
        assert_eq!(ev("repeat", &[s("no"), i(2)]), s("nono"));
    }
    #[test]
    fn rp26() {
        assert_eq!(ev("repeat", &[s("go"), i(5)]), s("gogogogogo"));
    }
    #[test]
    fn rp27() {
        assert_eq!(ev("repeat", &[s("up"), i(3)]), s("upupup"));
    }
}

// ===========================================================================
// left / right — 50 tests
// ===========================================================================
mod left_right_t01 {
    use super::*;
    #[test]
    fn left_3() {
        assert_eq!(ev("left", &[s("hello"), i(3)]), s("hel"));
    }
    #[test]
    fn left_0() {
        assert_eq!(ev("left", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn left_full() {
        assert_eq!(ev("left", &[s("hi"), i(5)]), s("hi"));
    }
    #[test]
    fn left_null() {
        assert_eq!(ev("left", &[null(), i(3)]), null());
    }
    #[test]
    fn left_1() {
        assert_eq!(ev("left", &[s("abc"), i(1)]), s("a"));
    }
    #[test]
    fn left_2() {
        assert_eq!(ev("left", &[s("abc"), i(2)]), s("ab"));
    }
    #[test]
    fn left_empty() {
        assert_eq!(ev("left", &[s(""), i(3)]), s(""));
    }
    #[test]
    fn right_3() {
        assert_eq!(ev("right", &[s("hello"), i(3)]), s("llo"));
    }
    #[test]
    fn right_0() {
        assert_eq!(ev("right", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn right_full() {
        assert_eq!(ev("right", &[s("hi"), i(5)]), s("hi"));
    }
    #[test]
    fn right_null() {
        assert_eq!(ev("right", &[null(), i(3)]), null());
    }
    #[test]
    fn right_1() {
        assert_eq!(ev("right", &[s("abc"), i(1)]), s("c"));
    }
    #[test]
    fn right_2() {
        assert_eq!(ev("right", &[s("abc"), i(2)]), s("bc"));
    }
    #[test]
    fn right_empty() {
        assert_eq!(ev("right", &[s(""), i(3)]), s(""));
    }
    #[test]
    fn lr01() {
        assert_eq!(ev("left", &[s("abcdef"), i(4)]), s("abcd"));
    }
    #[test]
    fn lr02() {
        assert_eq!(ev("right", &[s("abcdef"), i(4)]), s("cdef"));
    }
    #[test]
    fn lr03() {
        assert_eq!(ev("left", &[s("test"), i(2)]), s("te"));
    }
    #[test]
    fn lr04() {
        assert_eq!(ev("right", &[s("test"), i(2)]), s("st"));
    }
    #[test]
    fn lr05() {
        assert_eq!(ev("left", &[s("rust"), i(4)]), s("rust"));
    }
    #[test]
    fn lr06() {
        assert_eq!(ev("right", &[s("rust"), i(4)]), s("rust"));
    }
    #[test]
    fn lr07() {
        assert_eq!(ev("left", &[s("data"), i(1)]), s("d"));
    }
    #[test]
    fn lr08() {
        assert_eq!(ev("right", &[s("data"), i(1)]), s("a"));
    }
    #[test]
    fn lr09() {
        assert_eq!(ev("left", &[s("exchange"), i(4)]), s("exch"));
    }
    #[test]
    fn lr10() {
        assert_eq!(ev("right", &[s("exchange"), i(4)]), s("ange"));
    }
    #[test]
    fn lr11() {
        assert_eq!(ev("left", &[s("database"), i(4)]), s("data"));
    }
    #[test]
    fn lr12() {
        assert_eq!(ev("right", &[s("database"), i(4)]), s("base"));
    }
    #[test]
    fn lr13() {
        assert_eq!(ev("left", &[s("12345"), i(3)]), s("123"));
    }
    #[test]
    fn lr14() {
        assert_eq!(ev("right", &[s("12345"), i(3)]), s("345"));
    }
    #[test]
    fn lr15() {
        assert_eq!(ev("left", &[s("hello world"), i(5)]), s("hello"));
    }
    #[test]
    fn lr16() {
        assert_eq!(ev("right", &[s("hello world"), i(5)]), s("world"));
    }
    #[test]
    fn lr17() {
        assert_eq!(ev("left", &[s("ab"), i(1)]), s("a"));
    }
    #[test]
    fn lr18() {
        assert_eq!(ev("right", &[s("ab"), i(1)]), s("b"));
    }
    #[test]
    fn lr19() {
        assert_eq!(ev("left", &[s("x"), i(1)]), s("x"));
    }
    #[test]
    fn lr20() {
        assert_eq!(ev("right", &[s("x"), i(1)]), s("x"));
    }
    #[test]
    fn lr21() {
        assert_eq!(ev("left", &[s("ab"), i(2)]), s("ab"));
    }
    #[test]
    fn lr22() {
        assert_eq!(ev("right", &[s("ab"), i(2)]), s("ab"));
    }
    #[test]
    fn lr23() {
        assert_eq!(ev("left", &[s("abcde"), i(3)]), s("abc"));
    }
    #[test]
    fn lr24() {
        assert_eq!(ev("right", &[s("abcde"), i(3)]), s("cde"));
    }
    #[test]
    fn lr25() {
        assert_eq!(ev("left", &[s("fghij"), i(2)]), s("fg"));
    }
    #[test]
    fn lr26() {
        assert_eq!(ev("right", &[s("fghij"), i(2)]), s("ij"));
    }
    #[test]
    fn lr27() {
        assert_eq!(ev("left", &[s("klmno"), i(4)]), s("klmn"));
    }
    #[test]
    fn lr28() {
        assert_eq!(ev("right", &[s("klmno"), i(4)]), s("lmno"));
    }
    #[test]
    fn lr29() {
        assert_eq!(ev("left", &[s("pqrst"), i(5)]), s("pqrst"));
    }
    #[test]
    fn lr30() {
        assert_eq!(ev("right", &[s("pqrst"), i(5)]), s("pqrst"));
    }
    #[test]
    fn lr31() {
        assert_eq!(ev("left", &[s("uvwxy"), i(0)]), s(""));
    }
    #[test]
    fn lr32() {
        assert_eq!(ev("right", &[s("uvwxy"), i(0)]), s(""));
    }
    #[test]
    fn lr33() {
        assert_eq!(ev("left", &[s("abcdefghij"), i(7)]), s("abcdefg"));
    }
    #[test]
    fn lr34() {
        assert_eq!(ev("right", &[s("abcdefghij"), i(7)]), s("defghij"));
    }
    #[test]
    fn lr35() {
        assert_eq!(ev("left", &[s("ABCDEFGHIJ"), i(5)]), s("ABCDE"));
    }
    #[test]
    fn lr36() {
        assert_eq!(ev("right", &[s("ABCDEFGHIJ"), i(5)]), s("FGHIJ"));
    }
}

// ===========================================================================
// starts_with / ends_with / contains — 50 tests
// ===========================================================================
mod predicates_t01 {
    use super::*;
    #[test]
    fn sw_yes() {
        assert_eq!(ev("starts_with", &[s("hello"), s("he")]), i(1));
    }
    #[test]
    fn sw_no() {
        assert_eq!(ev("starts_with", &[s("hello"), s("lo")]), i(0));
    }
    #[test]
    fn sw_empty() {
        assert_eq!(ev("starts_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn sw_null() {
        assert_eq!(ev("starts_with", &[null(), s("a")]), null());
    }
    #[test]
    fn sw_full() {
        assert_eq!(ev("starts_with", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn sw_longer() {
        assert_eq!(ev("starts_with", &[s("ab"), s("abc")]), i(0));
    }
    #[test]
    fn ew_yes() {
        assert_eq!(ev("ends_with", &[s("hello"), s("lo")]), i(1));
    }
    #[test]
    fn ew_no() {
        assert_eq!(ev("ends_with", &[s("hello"), s("he")]), i(0));
    }
    #[test]
    fn ew_empty() {
        assert_eq!(ev("ends_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn ew_null() {
        assert_eq!(ev("ends_with", &[null(), s("a")]), null());
    }
    #[test]
    fn ew_full() {
        assert_eq!(ev("ends_with", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn ct_yes() {
        assert_eq!(ev("contains", &[s("hello"), s("ell")]), i(1));
    }
    #[test]
    fn ct_no() {
        assert_eq!(ev("contains", &[s("hello"), s("xyz")]), i(0));
    }
    #[test]
    fn ct_empty() {
        assert_eq!(ev("contains", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn ct_null() {
        assert_eq!(ev("contains", &[null(), s("a")]), null());
    }
    #[test]
    fn ct_full() {
        assert_eq!(ev("contains", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn sw01() {
        assert_eq!(ev("starts_with", &[s("rust"), s("r")]), i(1));
    }
    #[test]
    fn sw02() {
        assert_eq!(ev("starts_with", &[s("rust"), s("ru")]), i(1));
    }
    #[test]
    fn sw03() {
        assert_eq!(ev("starts_with", &[s("rust"), s("rus")]), i(1));
    }
    #[test]
    fn sw04() {
        assert_eq!(ev("starts_with", &[s("rust"), s("rust")]), i(1));
    }
    #[test]
    fn sw05() {
        assert_eq!(ev("starts_with", &[s("rust"), s("ust")]), i(0));
    }
    #[test]
    fn ew01() {
        assert_eq!(ev("ends_with", &[s("rust"), s("t")]), i(1));
    }
    #[test]
    fn ew02() {
        assert_eq!(ev("ends_with", &[s("rust"), s("st")]), i(1));
    }
    #[test]
    fn ew03() {
        assert_eq!(ev("ends_with", &[s("rust"), s("ust")]), i(1));
    }
    #[test]
    fn ew04() {
        assert_eq!(ev("ends_with", &[s("rust"), s("rust")]), i(1));
    }
    #[test]
    fn ew05() {
        assert_eq!(ev("ends_with", &[s("rust"), s("ru")]), i(0));
    }
    #[test]
    fn ct01() {
        assert_eq!(ev("contains", &[s("database"), s("data")]), i(1));
    }
    #[test]
    fn ct02() {
        assert_eq!(ev("contains", &[s("database"), s("base")]), i(1));
    }
    #[test]
    fn ct03() {
        assert_eq!(ev("contains", &[s("database"), s("tab")]), i(1));
    }
    #[test]
    fn ct04() {
        assert_eq!(ev("contains", &[s("database"), s("xyz")]), i(0));
    }
    #[test]
    fn ct05() {
        assert_eq!(ev("contains", &[s("hello world"), s(" ")]), i(1));
    }
    #[test]
    fn ct06() {
        assert_eq!(ev("contains", &[s("hello world"), s("o w")]), i(1));
    }
    #[test]
    fn sw06() {
        assert_eq!(ev("starts_with", &[s("BTC/USD"), s("BTC")]), i(1));
    }
    #[test]
    fn sw07() {
        assert_eq!(ev("starts_with", &[s("ETH/USD"), s("ETH")]), i(1));
    }
    #[test]
    fn ew06() {
        assert_eq!(ev("ends_with", &[s("BTC/USD"), s("USD")]), i(1));
    }
    #[test]
    fn ew07() {
        assert_eq!(ev("ends_with", &[s("ETH/EUR"), s("EUR")]), i(1));
    }
    #[test]
    fn ct07() {
        assert_eq!(ev("contains", &[s("BTC/USD"), s("/")]), i(1));
    }
    #[test]
    fn ct08() {
        assert_eq!(ev("contains", &[s("exchange-db"), s("-")]), i(1));
    }
    #[test]
    fn sw08() {
        assert_eq!(ev("starts_with", &[s(""), s("")]), i(1));
    }
    #[test]
    fn ew08() {
        assert_eq!(ev("ends_with", &[s(""), s("")]), i(1));
    }
    #[test]
    fn ct09() {
        assert_eq!(ev("contains", &[s(""), s("")]), i(1));
    }
    #[test]
    fn sw09() {
        assert_eq!(ev("starts_with", &[s("a"), s("a")]), i(1));
    }
    #[test]
    fn ew09() {
        assert_eq!(ev("ends_with", &[s("a"), s("a")]), i(1));
    }
    #[test]
    fn ct10() {
        assert_eq!(ev("contains", &[s("a"), s("a")]), i(1));
    }
    #[test]
    fn sw10() {
        assert_eq!(ev("starts_with", &[s("abc"), s("x")]), i(0));
    }
    #[test]
    fn ew10() {
        assert_eq!(ev("ends_with", &[s("abc"), s("x")]), i(0));
    }
    #[test]
    fn ct11() {
        assert_eq!(ev("contains", &[s("abc"), s("x")]), i(0));
    }
    #[test]
    fn sw_case() {
        assert_eq!(ev("starts_with", &[s("Hello"), s("hello")]), i(0));
    }
    #[test]
    fn ew_case() {
        assert_eq!(ev("ends_with", &[s("Hello"), s("ELLO")]), i(0));
    }
    #[test]
    fn ct_case() {
        assert_eq!(ev("contains", &[s("Hello"), s("ELLO")]), i(0));
    }
}

// ===========================================================================
// replace — 50 tests
// ===========================================================================
mod replace_t01 {
    use super::*;
    #[test]
    fn basic() {
        assert_eq!(ev("replace", &[s("hello"), s("l"), s("r")]), s("herro"));
    }
    #[test]
    fn not_found() {
        assert_eq!(ev("replace", &[s("hello"), s("x"), s("y")]), s("hello"));
    }
    #[test]
    fn remove() {
        assert_eq!(ev("replace", &[s("abc"), s("b"), s("")]), s("ac"));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("replace", &[null(), s("a"), s("b")]), null());
    }
    #[test]
    fn empty_str() {
        assert_eq!(ev("replace", &[s("abc"), s(""), s("x")]), s("abc"));
    }
    #[test]
    fn full_replace() {
        assert_eq!(ev("replace", &[s("aaa"), s("a"), s("b")]), s("bbb"));
    }
    #[test]
    fn multi_char() {
        assert_eq!(
            ev("replace", &[s("hello world"), s("world"), s("rust")]),
            s("hello rust")
        );
    }
    #[test]
    fn r01() {
        assert_eq!(
            ev("replace", &[s("foo bar"), s("foo"), s("baz")]),
            s("baz bar")
        );
    }
    #[test]
    fn r02() {
        assert_eq!(ev("replace", &[s("aabbcc"), s("bb"), s("xx")]), s("aaxxcc"));
    }
    #[test]
    fn r03() {
        assert_eq!(ev("replace", &[s("test"), s("t"), s("T")]), s("TesT"));
    }
    #[test]
    fn r04() {
        assert_eq!(ev("replace", &[s("hello"), s("hello"), s("bye")]), s("bye"));
    }
    #[test]
    fn r05() {
        assert_eq!(ev("replace", &[s("aaa"), s("aa"), s("b")]), s("ba"));
    }
    #[test]
    fn r06() {
        assert_eq!(ev("replace", &[s("abc abc"), s("abc"), s("x")]), s("x x"));
    }
    #[test]
    fn r07() {
        assert_eq!(ev("replace", &[s("---"), s("-"), s("+")]), s("+++"));
    }
    #[test]
    fn r08() {
        assert_eq!(ev("replace", &[s("a.b.c"), s("."), s("-")]), s("a-b-c"));
    }
    #[test]
    fn r09() {
        assert_eq!(
            ev("replace", &[s("one two three"), s(" "), s("_")]),
            s("one_two_three")
        );
    }
    #[test]
    fn r10() {
        assert_eq!(ev("replace", &[s("BTC/USD"), s("/"), s("-")]), s("BTC-USD"));
    }
    #[test]
    fn r11() {
        assert_eq!(
            ev("replace", &[s("2024-01-15"), s("-"), s("/")]),
            s("2024/01/15")
        );
    }
    #[test]
    fn r12() {
        assert_eq!(ev("replace", &[s("hello"), s("l"), s("ll")]), s("hellllo"));
    }
    #[test]
    fn r13() {
        assert_eq!(
            ev("replace", &[s("abcabc"), s("abc"), s("xyz")]),
            s("xyzxyz")
        );
    }
    #[test]
    fn r14() {
        assert_eq!(ev("replace", &[s("aaa"), s("a"), s("aa")]), s("aaaaaa"));
    }
    #[test]
    fn r15() {
        assert_eq!(
            ev("replace", &[s("test case"), s("case"), s("suite")]),
            s("test suite")
        );
    }
    #[test]
    fn r16() {
        assert_eq!(ev("replace", &[s("xxx"), s("x"), s("")]), s(""));
    }
    #[test]
    fn r17() {
        assert_eq!(ev("replace", &[s("a"), s("a"), s("b")]), s("b"));
    }
    #[test]
    fn r18() {
        assert_eq!(ev("replace", &[s("ab"), s("ab"), s("cd")]), s("cd"));
    }
    #[test]
    fn r19() {
        assert_eq!(ev("replace", &[s("abc"), s("abc"), s("")]), s(""));
    }
    #[test]
    fn r20() {
        assert_eq!(
            ev("replace", &[s("no match here"), s("xyz"), s("abc")]),
            s("no match here")
        );
    }
    #[test]
    fn r21() {
        assert_eq!(ev("replace", &[s("hello"), s("o"), s("0")]), s("hell0"));
    }
    #[test]
    fn r22() {
        assert_eq!(ev("replace", &[s("abba"), s("b"), s("c")]), s("acca"));
    }
    #[test]
    fn r23() {
        assert_eq!(ev("replace", &[s("abcde"), s("c"), s("C")]), s("abCde"));
    }
    #[test]
    fn r24() {
        assert_eq!(ev("replace", &[s("aabba"), s("a"), s("x")]), s("xxbbx"));
    }
    #[test]
    fn r25() {
        assert_eq!(ev("replace", &[s("hello"), s("e"), s("a")]), s("hallo"));
    }
    #[test]
    fn r26() {
        assert_eq!(ev("replace", &[s("foo"), s("oo"), s("ee")]), s("fee"));
    }
    #[test]
    fn r27() {
        assert_eq!(ev("replace", &[s("cat"), s("c"), s("b")]), s("bat"));
    }
    #[test]
    fn r28() {
        assert_eq!(ev("replace", &[s("bat"), s("b"), s("c")]), s("cat"));
    }
    #[test]
    fn r29() {
        assert_eq!(ev("replace", &[s("good"), s("oo"), s("ee")]), s("geed"));
    }
    #[test]
    fn r30() {
        assert_eq!(ev("replace", &[s("look"), s("oo"), s("i")]), s("lik"));
    }
    #[test]
    fn r31() {
        assert_eq!(ev("replace", &[s("moon"), s("oo"), s("a")]), s("man"));
    }
    #[test]
    fn r32() {
        assert_eq!(ev("replace", &[s("feed"), s("ee"), s("ea")]), s("fead"));
    }
    #[test]
    fn r33() {
        assert_eq!(ev("replace", &[s("deed"), s("ee"), s("ea")]), s("dead"));
    }
    #[test]
    fn r34() {
        assert_eq!(ev("replace", &[s("seed"), s("ee"), s("ea")]), s("sead"));
    }
    #[test]
    fn r35() {
        assert_eq!(ev("replace", &[s("meet"), s("ee"), s("ea")]), s("meat"));
    }
    #[test]
    fn r36() {
        assert_eq!(ev("replace", &[s("teem"), s("ee"), s("ea")]), s("team"));
    }
    #[test]
    fn r37() {
        assert_eq!(ev("replace", &[s("deer"), s("ee"), s("ea")]), s("dear"));
    }
    #[test]
    fn r38() {
        assert_eq!(ev("replace", &[s("beer"), s("ee"), s("ea")]), s("bear"));
    }
    #[test]
    fn r39() {
        assert_eq!(ev("replace", &[s("peer"), s("ee"), s("ea")]), s("pear"));
    }
    #[test]
    fn r40() {
        assert_eq!(ev("replace", &[s("eel"), s("ee"), s("ea")]), s("eal"));
    }
    #[test]
    fn r41() {
        assert_eq!(ev("replace", &[s("steel"), s("ee"), s("ea")]), s("steal"));
    }
    #[test]
    fn r42() {
        assert_eq!(ev("replace", &[s("wheel"), s("ee"), s("ea")]), s("wheal"));
    }
    #[test]
    fn r43() {
        assert_eq!(ev("replace", &[s("reel"), s("ee"), s("ea")]), s("real"));
    }
    #[test]
    fn r44() {
        assert_eq!(ev("replace", &[s("feel"), s("ee"), s("ea")]), s("feal"));
    }
}

// ===========================================================================
// concat — 50 tests
// ===========================================================================
mod concat_t01 {
    use super::*;
    #[test]
    fn two() {
        assert_eq!(ev("concat", &[s("a"), s("b")]), s("ab"));
    }
    #[test]
    fn three() {
        assert_eq!(ev("concat", &[s("a"), s("b"), s("c")]), s("abc"));
    }
    #[test]
    fn with_null() {
        assert_eq!(ev("concat", &[s("a"), null(), s("c")]), s("ac"));
    }
    #[test]
    fn empties() {
        assert_eq!(ev("concat", &[s(""), s("")]), s(""));
    }
    #[test]
    fn nums() {
        assert_eq!(ev("concat", &[i(1), i(2)]), s("12"));
    }
    #[test]
    fn single_with_empty() {
        assert_eq!(ev("concat", &[s("only"), s("")]), s("only"));
    }
    #[test]
    fn four() {
        assert_eq!(ev("concat", &[s("a"), s("b"), s("c"), s("d")]), s("abcd"));
    }
    #[test]
    fn five() {
        assert_eq!(
            ev("concat", &[s("a"), s("b"), s("c"), s("d"), s("e")]),
            s("abcde")
        );
    }
    #[test]
    fn mixed_types() {
        assert_eq!(ev("concat", &[s("val:"), i(42)]), s("val:42"));
    }
    #[test]
    fn float_concat() {
        assert_eq!(ev("concat", &[s("pi="), f(3.15)]), s("pi=3.15"));
    }
    #[test]
    fn c01() {
        assert_eq!(
            ev("concat", &[s("hello"), s(" "), s("world")]),
            s("hello world")
        );
    }
    #[test]
    fn c02() {
        assert_eq!(ev("concat", &[s("BTC"), s("/"), s("USD")]), s("BTC/USD"));
    }
    #[test]
    fn c03() {
        assert_eq!(
            ev("concat", &[s("2024"), s("-"), s("01"), s("-"), s("15")]),
            s("2024-01-15")
        );
    }
    #[test]
    fn c04() {
        assert_eq!(ev("concat", &[s("key"), s("="), s("val")]), s("key=val"));
    }
    #[test]
    fn c05() {
        assert_eq!(ev("concat", &[s("("), s("x"), s(")")]), s("(x)"));
    }
    #[test]
    fn c06() {
        assert_eq!(
            ev("concat", &[s("["), i(1), s(","), i(2), s("]")]),
            s("[1,2]")
        );
    }
    #[test]
    fn c07() {
        assert_eq!(ev("concat", &[s("abc"), s("def")]), s("abcdef"));
    }
    #[test]
    fn c08() {
        assert_eq!(
            ev("concat", &[s("xxx"), s("yyy"), s("zzz")]),
            s("xxxyyyzzz")
        );
    }
    #[test]
    fn c09() {
        assert_eq!(ev("concat", &[s(""), s("b")]), s("b"));
    }
    #[test]
    fn c10() {
        assert_eq!(ev("concat", &[s("a"), s("")]), s("a"));
    }
    #[test]
    fn c11() {
        assert_eq!(ev("concat", &[null(), null()]), s(""));
    }
    #[test]
    fn c12() {
        assert_eq!(ev("concat", &[s("x"), null()]), s("x"));
    }
    #[test]
    fn c13() {
        assert_eq!(ev("concat", &[null(), s("y")]), s("y"));
    }
    #[test]
    fn concat_ws_basic() {
        assert_eq!(
            ev("concat_ws", &[s(","), s("a"), s("b"), s("c")]),
            s("a,b,c")
        );
    }
    #[test]
    fn concat_ws_two() {
        assert_eq!(ev("concat_ws", &[s("-"), s("a"), s("b")]), s("a-b"));
    }
    #[test]
    fn concat_ws_null_skip() {
        assert_eq!(ev("concat_ws", &[s(","), s("a"), null(), s("c")]), s("a,c"));
    }
    #[test]
    fn concat_ws_space() {
        assert_eq!(
            ev("concat_ws", &[s(" "), s("hello"), s("world")]),
            s("hello world")
        );
    }
    #[test]
    fn concat_ws_empty_sep() {
        assert_eq!(ev("concat_ws", &[s(""), s("a"), s("b")]), s("ab"));
    }
    #[test]
    fn c14() {
        assert_eq!(
            ev("concat", &[s("foo"), s("bar"), s("baz")]),
            s("foobarbaz")
        );
    }
    #[test]
    fn c15() {
        assert_eq!(
            ev("concat", &[s("one"), s("two"), s("three")]),
            s("onetwothree")
        );
    }
    #[test]
    fn c16() {
        assert_eq!(ev("concat", &[i(100), s("x")]), s("100x"));
    }
    #[test]
    fn c17() {
        assert_eq!(ev("concat", &[s("x"), i(100)]), s("x100"));
    }
    #[test]
    fn c18() {
        assert_eq!(ev("concat", &[i(1), i(2), i(3)]), s("123"));
    }
    #[test]
    fn c19() {
        assert_eq!(
            ev("concat", &[s("a"), s("b"), s("c"), s("d"), s("e"), s("f")]),
            s("abcdef")
        );
    }
    #[test]
    fn c20() {
        assert_eq!(ev("concat", &[s("x"), s("")]), s("x"));
    }
    #[test]
    fn c21() {
        assert_eq!(ev("concat", &[s(""), s(""), s("")]), s(""));
    }
    #[test]
    fn c22() {
        assert_eq!(
            ev("concat", &[s("ab"), s("cd"), s("ef"), s("gh")]),
            s("abcdefgh")
        );
    }
    #[test]
    fn c23() {
        assert_eq!(
            ev("concat", &[s("test"), s("_"), s("case")]),
            s("test_case")
        );
    }
    #[test]
    fn c24() {
        assert_eq!(ev("concat", &[s("db"), s(".")]), s("db."));
    }
    #[test]
    fn c25() {
        assert_eq!(ev("concat", &[s("pre"), s("fix")]), s("prefix"));
    }
    #[test]
    fn c26() {
        assert_eq!(ev("concat", &[s("suf"), s("fix")]), s("suffix"));
    }
    #[test]
    fn c27() {
        assert_eq!(ev("concat", &[s("in"), s("put")]), s("input"));
    }
    #[test]
    fn c28() {
        assert_eq!(ev("concat", &[s("out"), s("put")]), s("output"));
    }
    #[test]
    fn c29() {
        assert_eq!(ev("concat", &[s("up"), s("date")]), s("update"));
    }
    #[test]
    fn c30() {
        assert_eq!(ev("concat", &[s("in"), s("sert")]), s("insert"));
    }
}

// ===========================================================================
// substring / position — 50 tests
// ===========================================================================
mod substring_t01 {
    use super::*;
    #[test]
    fn mid() {
        assert_eq!(ev("substring", &[s("hello"), i(2), i(3)]), s("ell"));
    }
    #[test]
    fn start() {
        assert_eq!(ev("substring", &[s("hello"), i(1), i(2)]), s("he"));
    }
    #[test]
    fn end() {
        assert_eq!(ev("substring", &[s("hello"), i(4), i(2)]), s("lo"));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("substring", &[null(), i(1), i(1)]), null());
    }
    #[test]
    fn full() {
        assert_eq!(ev("substring", &[s("abc"), i(1), i(3)]), s("abc"));
    }
    #[test]
    fn zero_len() {
        assert_eq!(ev("substring", &[s("abc"), i(1), i(0)]), s(""));
    }
    #[test]
    fn beyond() {
        assert_eq!(ev("substring", &[s("abc"), i(1), i(10)]), s("abc"));
    }
    #[test]
    fn one_char() {
        assert_eq!(ev("substring", &[s("abc"), i(2), i(1)]), s("b"));
    }
    #[test]
    fn last() {
        assert_eq!(ev("substring", &[s("abcde"), i(5), i(1)]), s("e"));
    }
    #[test]
    fn substr_alias() {
        assert_eq!(ev("substr", &[s("hello"), i(1), i(3)]), s("hel"));
    }
    // position(needle, haystack)
    #[test]
    fn pos_found() {
        assert_eq!(ev("position", &[s("ell"), s("hello")]), i(2));
    }
    #[test]
    fn pos_start() {
        assert_eq!(ev("position", &[s("h"), s("hello")]), i(1));
    }
    #[test]
    fn pos_end() {
        assert_eq!(ev("position", &[s("o"), s("hello")]), i(5));
    }
    #[test]
    fn pos_not_found() {
        assert_eq!(ev("position", &[s("xyz"), s("hello")]), i(0));
    }
    #[test]
    fn pos_null() {
        assert_eq!(ev("position", &[null(), s("a")]), null());
    }
    #[test]
    fn pos_empty() {
        assert_eq!(ev("position", &[s(""), s("hello")]), i(1));
    }
    #[test]
    fn str_pos_alias() {
        assert_eq!(ev("str_pos", &[s("ell"), s("hello")]), i(2));
    }
    #[test]
    fn s01() {
        assert_eq!(ev("substring", &[s("abcdefgh"), i(3), i(4)]), s("cdef"));
    }
    #[test]
    fn s02() {
        assert_eq!(ev("substring", &[s("abcdefgh"), i(1), i(1)]), s("a"));
    }
    #[test]
    fn s03() {
        assert_eq!(ev("substring", &[s("abcdefgh"), i(8), i(1)]), s("h"));
    }
    #[test]
    fn s04() {
        assert_eq!(ev("substring", &[s("12345"), i(2), i(3)]), s("234"));
    }
    #[test]
    fn s05() {
        assert_eq!(ev("substring", &[s("test"), i(1), i(4)]), s("test"));
    }
    #[test]
    fn s06() {
        assert_eq!(ev("substring", &[s("data"), i(1), i(2)]), s("da"));
    }
    #[test]
    fn s07() {
        assert_eq!(ev("substring", &[s("data"), i(3), i(2)]), s("ta"));
    }
    #[test]
    fn p01() {
        assert_eq!(ev("position", &[s("abc"), s("abcabc")]), i(1));
    }
    #[test]
    fn p02() {
        assert_eq!(ev("position", &[s("bc"), s("abcabc")]), i(2));
    }
    #[test]
    fn p03() {
        assert_eq!(ev("position", &[s("c"), s("abcabc")]), i(3));
    }
    #[test]
    fn p04() {
        assert_eq!(ev("position", &[s("a"), s("abc")]), i(1));
    }
    #[test]
    fn p05() {
        assert_eq!(ev("position", &[s("b"), s("abc")]), i(2));
    }
    #[test]
    fn p06() {
        assert_eq!(ev("position", &[s("c"), s("abc")]), i(3));
    }
    #[test]
    fn p07() {
        assert_eq!(ev("position", &[s("d"), s("abc")]), i(0));
    }
    #[test]
    fn s08() {
        assert_eq!(ev("substring", &[s("hello world"), i(7), i(5)]), s("world"));
    }
    #[test]
    fn s09() {
        assert_eq!(ev("substring", &[s("hello world"), i(1), i(5)]), s("hello"));
    }
    #[test]
    fn s10() {
        assert_eq!(ev("substring", &[s("exchange"), i(3), i(4)]), s("chan"));
    }
    #[test]
    fn p08() {
        assert_eq!(ev("position", &[s("change"), s("exchange")]), i(3));
    }
    #[test]
    fn p09() {
        assert_eq!(ev("position", &[s(" "), s("hello world")]), i(6));
    }
    #[test]
    fn p10() {
        assert_eq!(ev("position", &[s("/"), s("BTC/USD")]), i(4));
    }
    #[test]
    fn s11() {
        assert_eq!(ev("substring", &[s("BTC/USD"), i(1), i(3)]), s("BTC"));
    }
    #[test]
    fn s12() {
        assert_eq!(ev("substring", &[s("BTC/USD"), i(5), i(3)]), s("USD"));
    }
    #[test]
    fn s13() {
        assert_eq!(ev("substring", &[s(""), i(1), i(0)]), s(""));
    }
    #[test]
    fn p11() {
        assert_eq!(ev("position", &[s("a"), s("aaa")]), i(1));
    }
    #[test]
    fn p12() {
        assert_eq!(ev("position", &[s("b"), s("bbb")]), i(1));
    }
    #[test]
    fn s14() {
        assert_eq!(ev("substring", &[s("rust"), i(1), i(1)]), s("r"));
    }
    #[test]
    fn s15() {
        assert_eq!(ev("substring", &[s("rust"), i(2), i(1)]), s("u"));
    }
    #[test]
    fn s16() {
        assert_eq!(ev("substring", &[s("rust"), i(3), i(1)]), s("s"));
    }
    #[test]
    fn s17() {
        assert_eq!(ev("substring", &[s("rust"), i(4), i(1)]), s("t"));
    }
    #[test]
    fn s18() {
        assert_eq!(ev("substring", &[s("rust"), i(1), i(2)]), s("ru"));
    }
    #[test]
    fn s19() {
        assert_eq!(ev("substring", &[s("rust"), i(2), i(2)]), s("us"));
    }
    #[test]
    fn s20() {
        assert_eq!(ev("substring", &[s("rust"), i(3), i(2)]), s("st"));
    }
    #[test]
    fn s21() {
        assert_eq!(ev("substring", &[s("rust"), i(1), i(3)]), s("rus"));
    }
}

// ===========================================================================
// initcap / ascii / chr — 30 tests
// ===========================================================================
mod initcap_t01 {
    use super::*;
    #[test]
    fn basic() {
        assert_eq!(ev("initcap", &[s("hello world")]), s("Hello World"));
    }
    #[test]
    fn all_lower() {
        assert_eq!(ev("initcap", &[s("foo bar baz")]), s("Foo Bar Baz"));
    }
    #[test]
    fn all_upper() {
        assert_eq!(ev("initcap", &[s("FOO BAR")]), s("Foo Bar"));
    }
    #[test]
    fn single() {
        assert_eq!(ev("initcap", &[s("hello")]), s("Hello"));
    }
    #[test]
    fn empty() {
        assert_eq!(ev("initcap", &[s("")]), s(""));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("initcap", &[null()]), null());
    }
    #[test]
    fn mixed() {
        assert_eq!(ev("initcap", &[s("hELLO wORLD")]), s("Hello World"));
    }
    #[test]
    fn one_char() {
        assert_eq!(ev("initcap", &[s("a")]), s("A"));
    }
    #[test]
    fn title_case_alias() {
        assert_eq!(ev("title_case", &[s("hello world")]), s("Hello World"));
    }
    #[test]
    fn i01() {
        assert_eq!(
            ev("initcap", &[s("the quick brown fox")]),
            s("The Quick Brown Fox")
        );
    }
    // ascii
    #[test]
    fn ascii_a() {
        assert_eq!(ev("ascii", &[s("A")]), i(65));
    }
    #[test]
    fn ascii_z() {
        assert_eq!(ev("ascii", &[s("z")]), i(122));
    }
    #[test]
    fn ascii_0() {
        assert_eq!(ev("ascii", &[s("0")]), i(48));
    }
    #[test]
    fn ascii_space() {
        assert_eq!(ev("ascii", &[s(" ")]), i(32));
    }
    #[test]
    fn ascii_null() {
        assert_eq!(ev("ascii", &[null()]), null());
    }
    #[test]
    fn ascii_hello() {
        assert_eq!(ev("ascii", &[s("hello")]), i(104));
    }
    #[test]
    fn ascii_bang() {
        assert_eq!(ev("ascii", &[s("!")]), i(33));
    }
    #[test]
    fn ascii_at() {
        assert_eq!(ev("ascii", &[s("@")]), i(64));
    }
    #[test]
    fn ascii_hash() {
        assert_eq!(ev("ascii", &[s("#")]), i(35));
    }
    #[test]
    fn ascii_newline() {
        assert_eq!(ev("ascii", &[s("\n")]), i(10));
    }
    // chr
    #[test]
    fn chr_65() {
        assert_eq!(ev("chr", &[i(65)]), s("A"));
    }
    #[test]
    fn chr_97() {
        assert_eq!(ev("chr", &[i(97)]), s("a"));
    }
    #[test]
    fn chr_48() {
        assert_eq!(ev("chr", &[i(48)]), s("0"));
    }
    #[test]
    fn chr_32() {
        assert_eq!(ev("chr", &[i(32)]), s(" "));
    }
    #[test]
    fn chr_null() {
        assert_eq!(ev("chr", &[null()]), null());
    }
    #[test]
    fn chr_90() {
        assert_eq!(ev("chr", &[i(90)]), s("Z"));
    }
    #[test]
    fn chr_122() {
        assert_eq!(ev("chr", &[i(122)]), s("z"));
    }
    #[test]
    fn chr_33() {
        assert_eq!(ev("chr", &[i(33)]), s("!"));
    }
    #[test]
    fn chr_64() {
        assert_eq!(ev("chr", &[i(64)]), s("@"));
    }
    #[test]
    fn chr_35() {
        assert_eq!(ev("chr", &[i(35)]), s("#"));
    }
}

// ===========================================================================
// lpad / rpad — 40 tests
// ===========================================================================
mod pad_t01 {
    use super::*;
    #[test]
    fn lpad_basic() {
        assert_eq!(ev("lpad", &[s("hi"), i(5), s(".")]), s("...hi"));
    }
    #[test]
    fn lpad_no_pad() {
        assert_eq!(ev("lpad", &[s("hello"), i(5), s(".")]), s("hello"));
    }
    #[test]
    fn lpad_truncate() {
        assert_eq!(ev("lpad", &[s("hello"), i(3), s(".")]), s("hel"));
    }
    #[test]
    fn lpad_null() {
        assert_eq!(ev("lpad", &[null(), i(5), s(".")]), null());
    }
    #[test]
    fn lpad_zero() {
        assert_eq!(ev("lpad", &[s("hi"), i(0), s(".")]), s(""));
    }
    #[test]
    fn lpad_space() {
        assert_eq!(ev("lpad", &[s("hi"), i(5), s(" ")]), s("   hi"));
    }
    #[test]
    fn rpad_basic() {
        assert_eq!(ev("rpad", &[s("hi"), i(5), s(".")]), s("hi..."));
    }
    #[test]
    fn rpad_no_pad() {
        assert_eq!(ev("rpad", &[s("hello"), i(5), s(".")]), s("hello"));
    }
    #[test]
    fn rpad_truncate() {
        assert_eq!(ev("rpad", &[s("hello"), i(3), s(".")]), s("hel"));
    }
    #[test]
    fn rpad_null() {
        assert_eq!(ev("rpad", &[null(), i(5), s(".")]), null());
    }
    #[test]
    fn rpad_zero() {
        assert_eq!(ev("rpad", &[s("hi"), i(0), s(".")]), s(""));
    }
    #[test]
    fn rpad_space() {
        assert_eq!(ev("rpad", &[s("hi"), i(5), s(" ")]), s("hi   "));
    }
    #[test]
    fn lpad_star() {
        assert_eq!(ev("lpad", &[s("x"), i(5), s("*")]), s("****x"));
    }
    #[test]
    fn rpad_star() {
        assert_eq!(ev("rpad", &[s("x"), i(5), s("*")]), s("x****"));
    }
    #[test]
    fn lpad_digit() {
        assert_eq!(ev("lpad", &[s("7"), i(3), s("0")]), s("007"));
    }
    #[test]
    fn rpad_digit() {
        assert_eq!(ev("rpad", &[s("7"), i(3), s("0")]), s("700"));
    }
    #[test]
    fn lpad_10() {
        assert_eq!(ev("lpad", &[s("a"), i(10), s("-")]), s("---------a"));
    }
    #[test]
    fn rpad_10() {
        assert_eq!(ev("rpad", &[s("a"), i(10), s("-")]), s("a---------"));
    }
    #[test]
    fn lpad_empty() {
        assert_eq!(ev("lpad", &[s(""), i(3), s("x")]), s("xxx"));
    }
    #[test]
    fn rpad_empty() {
        assert_eq!(ev("rpad", &[s(""), i(3), s("x")]), s("xxx"));
    }
    #[test]
    fn lp01() {
        assert_eq!(ev("lpad", &[s("ab"), i(6), s(".")]), s("....ab"));
    }
    #[test]
    fn rp01() {
        assert_eq!(ev("rpad", &[s("ab"), i(6), s(".")]), s("ab...."));
    }
    #[test]
    fn lp02() {
        assert_eq!(ev("lpad", &[s("test"), i(8), s("_")]), s("____test"));
    }
    #[test]
    fn rp02() {
        assert_eq!(ev("rpad", &[s("test"), i(8), s("_")]), s("test____"));
    }
    #[test]
    fn lp03() {
        assert_eq!(ev("lpad", &[s("1"), i(5), s("0")]), s("00001"));
    }
    #[test]
    fn rp03() {
        assert_eq!(ev("rpad", &[s("1"), i(5), s("0")]), s("10000"));
    }
    #[test]
    fn lp04() {
        assert_eq!(ev("lpad", &[s("x"), i(1), s("y")]), s("x"));
    }
    #[test]
    fn rp04() {
        assert_eq!(ev("rpad", &[s("x"), i(1), s("y")]), s("x"));
    }
    #[test]
    fn lp05() {
        assert_eq!(ev("lpad", &[s("abc"), i(6), s("#")]), s("###abc"));
    }
    #[test]
    fn rp05() {
        assert_eq!(ev("rpad", &[s("abc"), i(6), s("#")]), s("abc###"));
    }
    #[test]
    fn lp06() {
        assert_eq!(ev("lpad", &[s("ab"), i(4), s("x")]), s("xxab"));
    }
    #[test]
    fn rp06() {
        assert_eq!(ev("rpad", &[s("ab"), i(4), s("x")]), s("abxx"));
    }
    #[test]
    fn lp07() {
        assert_eq!(ev("lpad", &[s("data"), i(4), s("-")]), s("data"));
    }
    #[test]
    fn rp07() {
        assert_eq!(ev("rpad", &[s("data"), i(4), s("-")]), s("data"));
    }
    #[test]
    fn lp08() {
        assert_eq!(ev("lpad", &[s("data"), i(7), s("+")]), s("+++data"));
    }
    #[test]
    fn rp08() {
        assert_eq!(ev("rpad", &[s("data"), i(7), s("+")]), s("data+++"));
    }
    #[test]
    fn lp09() {
        assert_eq!(ev("lpad", &[s("hi"), i(2), s(".")]), s("hi"));
    }
    #[test]
    fn rp09() {
        assert_eq!(ev("rpad", &[s("hi"), i(2), s(".")]), s("hi"));
    }
    #[test]
    fn lp10() {
        assert_eq!(ev("lpad", &[s("x"), i(3), s("y")]), s("yyx"));
    }
    #[test]
    fn rp10() {
        assert_eq!(ev("rpad", &[s("x"), i(3), s("y")]), s("xyy"));
    }
}

// ===========================================================================
// split_part — 20 tests
// ===========================================================================
mod split_part_t01 {
    use super::*;
    #[test]
    fn basic() {
        assert_eq!(ev("split_part", &[s("a,b,c"), s(","), i(1)]), s("a"));
    }
    #[test]
    fn second() {
        assert_eq!(ev("split_part", &[s("a,b,c"), s(","), i(2)]), s("b"));
    }
    #[test]
    fn third() {
        assert_eq!(ev("split_part", &[s("a,b,c"), s(","), i(3)]), s("c"));
    }
    #[test]
    fn beyond() {
        assert_eq!(ev("split_part", &[s("a,b,c"), s(","), i(4)]), s(""));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("split_part", &[null(), s(","), i(1)]), null());
    }
    #[test]
    fn empty() {
        assert_eq!(ev("split_part", &[s(""), s(","), i(1)]), s(""));
    }
    #[test]
    fn dash_sep() {
        assert_eq!(ev("split_part", &[s("a-b-c"), s("-"), i(2)]), s("b"));
    }
    #[test]
    fn dot_sep() {
        assert_eq!(ev("split_part", &[s("1.2.3"), s("."), i(3)]), s("3"));
    }
    #[test]
    fn space_sep() {
        assert_eq!(
            ev("split_part", &[s("hello world"), s(" "), i(1)]),
            s("hello")
        );
    }
    #[test]
    fn space_sep2() {
        assert_eq!(
            ev("split_part", &[s("hello world"), s(" "), i(2)]),
            s("world")
        );
    }
    #[test]
    fn slash_sep() {
        assert_eq!(ev("split_part", &[s("BTC/USD"), s("/"), i(1)]), s("BTC"));
    }
    #[test]
    fn slash_sep2() {
        assert_eq!(ev("split_part", &[s("BTC/USD"), s("/"), i(2)]), s("USD"));
    }
    #[test]
    fn pipe_sep() {
        assert_eq!(ev("split_part", &[s("a|b|c"), s("|"), i(2)]), s("b"));
    }
    #[test]
    fn no_sep() {
        assert_eq!(ev("split_part", &[s("abc"), s(","), i(1)]), s("abc"));
    }
    #[test]
    fn consecutive() {
        assert_eq!(ev("split_part", &[s("a,,c"), s(","), i(2)]), s(""));
    }
    #[test]
    fn sp01() {
        assert_eq!(ev("split_part", &[s("x:y:z"), s(":"), i(1)]), s("x"));
    }
    #[test]
    fn sp02() {
        assert_eq!(ev("split_part", &[s("x:y:z"), s(":"), i(2)]), s("y"));
    }
    #[test]
    fn sp03() {
        assert_eq!(ev("split_part", &[s("x:y:z"), s(":"), i(3)]), s("z"));
    }
    #[test]
    fn sp04() {
        assert_eq!(ev("split_part", &[s("one-two"), s("-"), i(1)]), s("one"));
    }
    #[test]
    fn sp05() {
        assert_eq!(ev("split_part", &[s("one-two"), s("-"), i(2)]), s("two"));
    }
}

// ===========================================================================
// camel_case / snake_case — 20 tests
// ===========================================================================
mod case_conv_t01 {
    use super::*;
    #[test]
    fn camel_basic() {
        assert_eq!(ev("camel_case", &[s("hello world")]), s("HelloWorld"));
    }
    #[test]
    fn camel_three() {
        assert_eq!(ev("camel_case", &[s("foo bar baz")]), s("FooBarBaz"));
    }
    #[test]
    fn camel_single() {
        assert_eq!(ev("camel_case", &[s("hello")]), s("Hello"));
    }
    #[test]
    fn camel_empty() {
        assert_eq!(ev("camel_case", &[s("")]), s(""));
    }
    #[test]
    fn camel_null() {
        assert_eq!(ev("camel_case", &[null()]), null());
    }
    #[test]
    fn snake_basic() {
        assert_eq!(ev("snake_case", &[s("helloWorld")]), s("hello_world"));
    }
    #[test]
    fn snake_three() {
        assert_eq!(ev("snake_case", &[s("fooBarBaz")]), s("foo_bar_baz"));
    }
    #[test]
    fn snake_single() {
        assert_eq!(ev("snake_case", &[s("hello")]), s("hello"));
    }
    #[test]
    fn snake_empty() {
        assert_eq!(ev("snake_case", &[s("")]), s(""));
    }
    #[test]
    fn snake_null() {
        assert_eq!(ev("snake_case", &[null()]), null());
    }
    #[test]
    fn camel_01() {
        assert_eq!(ev("camel_case", &[s("the quick fox")]), s("TheQuickFox"));
    }
    #[test]
    fn snake_01() {
        assert_eq!(ev("snake_case", &[s("theQuickFox")]), s("the_quick_fox"));
    }
    #[test]
    fn camel_already() {
        assert_eq!(ev("camel_case", &[s("alreadyCamel")]), s("AlreadyCamel"));
    }
    #[test]
    fn snake_already() {
        assert_eq!(ev("snake_case", &[s("already_snake")]), s("already_snake"));
    }
    #[test]
    fn camel_upper() {
        assert_eq!(ev("camel_case", &[s("HELLO WORLD")]), s("HELLOWORLD"));
    }
    #[test]
    fn squeeze_basic() {
        assert_eq!(ev("squeeze", &[s("a  b  c")]), s("a b c"));
    }
    #[test]
    fn squeeze_single() {
        assert_eq!(ev("squeeze", &[s("abc")]), s("abc"));
    }
    #[test]
    fn squeeze_empty() {
        assert_eq!(ev("squeeze", &[s("")]), s(""));
    }
    #[test]
    fn squeeze_null() {
        assert_eq!(ev("squeeze", &[null()]), null());
    }
    #[test]
    fn squeeze_spaces() {
        assert_eq!(ev("squeeze", &[s("  hello   world  ")]), s("hello world"));
    }
}

// ===========================================================================
// strcmp — 20 tests
// ===========================================================================
mod strcmp_t01 {
    use super::*;
    #[test]
    fn equal() {
        assert_eq!(ev("strcmp", &[s("abc"), s("abc")]), i(0));
    }
    #[test]
    fn less() {
        let r = ev("strcmp", &[s("abc"), s("abd")]);
        assert!(matches!(r, Value::I64(v) if v < 0));
    }
    #[test]
    fn greater() {
        let r = ev("strcmp", &[s("abd"), s("abc")]);
        assert!(matches!(r, Value::I64(v) if v > 0));
    }
    #[test]
    fn empty_both() {
        assert_eq!(ev("strcmp", &[s(""), s("")]), i(0));
    }
    #[test]
    fn null_in() {
        assert_eq!(ev("strcmp", &[null(), s("a")]), null());
    }
    #[test]
    fn a_vs_b() {
        let r = ev("strcmp", &[s("a"), s("b")]);
        assert!(matches!(r, Value::I64(v) if v < 0));
    }
    #[test]
    fn b_vs_a() {
        let r = ev("strcmp", &[s("b"), s("a")]);
        assert!(matches!(r, Value::I64(v) if v > 0));
    }
    #[test]
    fn same_single() {
        assert_eq!(ev("strcmp", &[s("x"), s("x")]), i(0));
    }
    #[test]
    fn short_vs_long() {
        let r = ev("strcmp", &[s("ab"), s("abc")]);
        assert!(matches!(r, Value::I64(v) if v < 0));
    }
    #[test]
    fn long_vs_short() {
        let r = ev("strcmp", &[s("abc"), s("ab")]);
        assert!(matches!(r, Value::I64(v) if v > 0));
    }
    #[test]
    fn cmp_01() {
        assert_eq!(ev("strcmp", &[s("hello"), s("hello")]), i(0));
    }
    #[test]
    fn cmp_02() {
        let r = ev("strcmp", &[s("abc"), s("xyz")]);
        assert!(matches!(r, Value::I64(v) if v < 0));
    }
    #[test]
    fn cmp_03() {
        let r = ev("strcmp", &[s("xyz"), s("abc")]);
        assert!(matches!(r, Value::I64(v) if v > 0));
    }
    #[test]
    fn cmp_04() {
        assert_eq!(ev("strcmp", &[s("test"), s("test")]), i(0));
    }
    #[test]
    fn cmp_05() {
        let r = ev("strcmp", &[s("a"), s("z")]);
        assert!(matches!(r, Value::I64(v) if v < 0));
    }
    #[test]
    fn cmp_06() {
        let r = ev("strcmp", &[s("z"), s("a")]);
        assert!(matches!(r, Value::I64(v) if v > 0));
    }
    #[test]
    fn cmp_07() {
        assert_eq!(ev("strcmp", &[s("data"), s("data")]), i(0));
    }
    #[test]
    fn cmp_08() {
        let r = ev("strcmp", &[s("aaa"), s("aab")]);
        assert!(matches!(r, Value::I64(v) if v < 0));
    }
    #[test]
    fn cmp_09() {
        let r = ev("strcmp", &[s("aab"), s("aaa")]);
        assert!(matches!(r, Value::I64(v) if v > 0));
    }
    #[test]
    fn cmp_10() {
        assert_eq!(ev("strcmp", &[s("same"), s("same")]), i(0));
    }
}

// ===========================================================================
// hex / unhex / to_base64 / from_base64 / md5 / sha256 — 50 tests
// ===========================================================================
mod encoding_t01 {
    use super::*;
    #[test]
    fn hex_hello() {
        assert_eq!(ev("hex", &[s("hello")]), s("68656c6c6f"));
    }
    #[test]
    fn hex_empty() {
        assert_eq!(ev("hex", &[s("")]), s(""));
    }
    #[test]
    fn hex_null() {
        assert_eq!(ev("hex", &[null()]), null());
    }
    #[test]
    fn hex_a() {
        assert_eq!(ev("hex", &[s("a")]), s("61"));
    }
    #[test]
    fn hex_ab() {
        assert_eq!(ev("hex", &[s("ab")]), s("6162"));
    }
    #[test]
    fn hex_space() {
        assert_eq!(ev("hex", &[s(" ")]), s("20"));
    }
    #[test]
    fn hex_0() {
        assert_eq!(ev("hex", &[s("0")]), s("30"));
    }
    #[test]
    fn to_hex_alias() {
        assert_eq!(ev("to_hex", &[s("abc")]), s("616263"));
    }
    #[test]
    fn unhex_ff() {
        assert_eq!(ev("unhex", &[s("ff")]), i(255));
    }
    #[test]
    fn unhex_0() {
        assert_eq!(ev("unhex", &[s("0")]), i(0));
    }
    #[test]
    fn unhex_null() {
        assert_eq!(ev("unhex", &[null()]), null());
    }
    #[test]
    fn unhex_a() {
        assert_eq!(ev("unhex", &[s("a")]), i(10));
    }
    #[test]
    fn from_hex_10() {
        assert_eq!(ev("from_hex", &[s("10")]), i(16));
    }
    #[test]
    fn to_base64_hello() {
        assert_eq!(ev("to_base64", &[s("hello")]), s("aGVsbG8="));
    }
    #[test]
    fn to_base64_empty() {
        assert_eq!(ev("to_base64", &[s("")]), s(""));
    }
    #[test]
    fn to_base64_null() {
        assert_eq!(ev("to_base64", &[null()]), null());
    }
    #[test]
    fn from_base64_hello() {
        assert_eq!(ev("from_base64", &[s("aGVsbG8=")]), s("hello"));
    }
    #[test]
    fn from_base64_empty() {
        assert_eq!(ev("from_base64", &[s("")]), s(""));
    }
    #[test]
    fn from_base64_null() {
        assert_eq!(ev("from_base64", &[null()]), null());
    }
    #[test]
    fn roundtrip_b64() {
        assert_eq!(
            ev("from_base64", &[ev("to_base64", &[s("test")])]),
            s("test")
        );
    }
    #[test]
    fn roundtrip_hex_len() {
        let h = ev("hex", &[s("test")]);
        match h {
            Value::Str(v) => assert_eq!(v.len(), 8),
            _ => panic!(),
        }
    }
    #[test]
    fn md5_hello() {
        let r = ev("md5", &[s("hello")]);
        match r {
            Value::Str(v) => assert_eq!(v.len(), 32),
            _ => panic!(),
        }
    }
    #[test]
    fn md5_empty() {
        let r = ev("md5", &[s("")]);
        match r {
            Value::Str(v) => assert_eq!(v.len(), 32),
            _ => panic!(),
        }
    }
    #[test]
    fn md5_null() {
        assert_eq!(ev("md5", &[null()]), null());
    }
    #[test]
    fn md5_deterministic() {
        assert_eq!(ev("md5", &[s("abc")]), ev("md5", &[s("abc")]));
    }
    #[test]
    fn md5_different() {
        assert_ne!(ev("md5", &[s("a")]), ev("md5", &[s("b")]));
    }
    #[test]
    fn sha256_hello() {
        let r = ev("sha256", &[s("hello")]);
        match r {
            Value::Str(v) => assert_eq!(v.len(), 64),
            _ => panic!(),
        }
    }
    #[test]
    fn sha256_empty() {
        let r = ev("sha256", &[s("")]);
        match r {
            Value::Str(v) => assert_eq!(v.len(), 64),
            _ => panic!(),
        }
    }
    #[test]
    fn sha256_null() {
        assert_eq!(ev("sha256", &[null()]), null());
    }
    #[test]
    fn sha256_deterministic() {
        assert_eq!(ev("sha256", &[s("abc")]), ev("sha256", &[s("abc")]));
    }
    #[test]
    fn sha256_different() {
        assert_ne!(ev("sha256", &[s("a")]), ev("sha256", &[s("b")]));
    }
    #[test]
    fn hex_01() {
        assert_eq!(ev("hex", &[s("z")]), s("7a"));
    }
    #[test]
    fn hex_02() {
        assert_eq!(ev("hex", &[s("A")]), s("41"));
    }
    #[test]
    fn hex_03() {
        assert_eq!(ev("hex", &[s("Z")]), s("5a"));
    }
    #[test]
    fn unhex_7a() {
        assert_eq!(ev("unhex", &[s("7a")]), i(122));
    }
    #[test]
    fn unhex_41() {
        assert_eq!(ev("unhex", &[s("41")]), i(65));
    }
    #[test]
    fn b64_01() {
        assert_eq!(ev("to_base64", &[s("a")]), s("YQ=="));
    }
    #[test]
    fn b64_02() {
        assert_eq!(ev("from_base64", &[s("YQ==")]), s("a"));
    }
    #[test]
    fn b64_03() {
        assert_eq!(ev("to_base64", &[s("ab")]), s("YWI="));
    }
    #[test]
    fn b64_04() {
        assert_eq!(ev("from_base64", &[s("YWI=")]), s("ab"));
    }
    #[test]
    fn b64_05() {
        assert_eq!(ev("to_base64", &[s("abc")]), s("YWJj"));
    }
    #[test]
    fn b64_06() {
        assert_eq!(ev("from_base64", &[s("YWJj")]), s("abc"));
    }
    #[test]
    fn b64_roundtrip_long() {
        let v = "x".repeat(100);
        assert_eq!(ev("from_base64", &[ev("to_base64", &[s(&v)])]), s(&v));
    }
    #[test]
    fn hex_long_len() {
        let r = ev("hex", &[s(&"y".repeat(100))]);
        match r {
            Value::Str(v) => assert_eq!(v.len(), 200),
            _ => panic!(),
        }
    }
    #[test]
    fn md5_long() {
        let r = ev("md5", &[s(&"x".repeat(1000))]);
        match r {
            Value::Str(v) => assert_eq!(v.len(), 32),
            _ => panic!(),
        }
    }
    #[test]
    fn sha256_long() {
        let r = ev("sha256", &[s(&"x".repeat(1000))]);
        match r {
            Value::Str(v) => assert_eq!(v.len(), 64),
            _ => panic!(),
        }
    }
    #[test]
    fn hash_hello() {
        let r = ev("hash", &[s("hello")]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn hash_deterministic() {
        assert_eq!(ev("hash", &[s("abc")]), ev("hash", &[s("abc")]));
    }
    #[test]
    fn hash_different() {
        assert_ne!(ev("hash", &[s("a")]), ev("hash", &[s("b")]));
    }
    #[test]
    fn hash_null() {
        assert_eq!(ev("hash", &[null()]), null());
    }
}

// ===========================================================================
// coalesce / nullif / greatest / least / if_null — 50 tests
// ===========================================================================
mod conditional_t01 {
    use super::*;
    #[test]
    fn coalesce_first() {
        assert_eq!(ev("coalesce", &[s("a"), s("b")]), s("a"));
    }
    #[test]
    fn coalesce_skip_null() {
        assert_eq!(ev("coalesce", &[null(), s("b")]), s("b"));
    }
    #[test]
    fn coalesce_all_null() {
        assert_eq!(ev("coalesce", &[null(), null()]), null());
    }
    #[test]
    fn coalesce_three() {
        assert_eq!(ev("coalesce", &[null(), null(), s("c")]), s("c"));
    }
    #[test]
    fn coalesce_int() {
        assert_eq!(ev("coalesce", &[null(), i(42)]), i(42));
    }
    #[test]
    fn nullif_diff() {
        assert_eq!(ev("nullif", &[i(1), i(2)]), i(1));
    }
    #[test]
    fn nullif_same() {
        assert_eq!(ev("nullif", &[i(1), i(1)]), null());
    }
    #[test]
    fn nullif_str_same() {
        assert_eq!(ev("nullif", &[s("a"), s("a")]), null());
    }
    #[test]
    fn nullif_str_diff() {
        assert_eq!(ev("nullif", &[s("a"), s("b")]), s("a"));
    }
    #[test]
    fn greatest_two() {
        assert_eq!(ev("greatest", &[i(1), i(2)]), i(2));
    }
    #[test]
    fn greatest_three() {
        assert_eq!(ev("greatest", &[i(1), i(3), i(2)]), i(3));
    }
    #[test]
    fn greatest_neg() {
        assert_eq!(ev("greatest", &[i(-5), i(-1)]), i(-1));
    }
    #[test]
    fn greatest_float() {
        let r = ev("greatest", &[f(1.5), f(2.5)]);
        assert!(matches!(r, Value::F64(v) if v > 2.0));
    }
    #[test]
    fn least_two() {
        assert_eq!(ev("least", &[i(1), i(2)]), i(1));
    }
    #[test]
    fn least_three() {
        assert_eq!(ev("least", &[i(3), i(1), i(2)]), i(1));
    }
    #[test]
    fn least_neg() {
        assert_eq!(ev("least", &[i(-5), i(-1)]), i(-5));
    }
    #[test]
    fn least_float() {
        let r = ev("least", &[f(1.5), f(2.5)]);
        assert!(matches!(r, Value::F64(v) if v < 2.0));
    }
    #[test]
    fn if_null_not_null() {
        assert_eq!(ev("if_null", &[i(42), i(0)]), i(42));
    }
    #[test]
    fn if_null_null() {
        assert_eq!(ev("if_null", &[null(), i(0)]), i(0));
    }
    #[test]
    fn ifnull_alias() {
        assert_eq!(ev("ifnull", &[null(), i(99)]), i(99));
    }
    #[test]
    fn nvl_alias() {
        assert_eq!(ev("nvl", &[null(), i(99)]), i(99));
    }
    #[test]
    fn is_null_yes() {
        assert_eq!(ev("is_null", &[null()]), i(1));
    }
    #[test]
    fn is_null_no() {
        assert_eq!(ev("is_null", &[i(42)]), i(0));
    }
    #[test]
    fn is_not_null_yes() {
        assert_eq!(ev("is_not_null", &[i(42)]), i(1));
    }
    #[test]
    fn is_not_null_no() {
        assert_eq!(ev("is_not_null", &[null()]), i(0));
    }
    #[test]
    fn nullif_zero_zero() {
        assert_eq!(ev("nullif_zero", &[i(0)]), null());
    }
    #[test]
    fn nullif_zero_nonzero() {
        assert_eq!(ev("nullif_zero", &[i(5)]), i(5));
    }
    #[test]
    fn zeroifnull_null() {
        assert_eq!(ev("zeroifnull", &[null()]), i(0));
    }
    #[test]
    fn zeroifnull_val() {
        assert_eq!(ev("zeroifnull", &[i(5)]), i(5));
    }
    #[test]
    fn nullifempty_empty() {
        assert_eq!(ev("nullifempty", &[s("")]), null());
    }
    #[test]
    fn nullifempty_notempty() {
        assert_eq!(ev("nullifempty", &[s("x")]), s("x"));
    }
    #[test]
    fn max_of_alias() {
        assert_eq!(ev("max_of", &[i(1), i(2)]), i(2));
    }
    #[test]
    fn min_of_alias() {
        assert_eq!(ev("min_of", &[i(1), i(2)]), i(1));
    }
    #[test]
    fn co01() {
        assert_eq!(ev("coalesce", &[i(1)]), i(1));
    }
    #[test]
    fn co02() {
        assert_eq!(ev("coalesce", &[null(), null(), null(), i(99)]), i(99));
    }
    #[test]
    fn co03() {
        assert_eq!(ev("coalesce", &[s("x")]), s("x"));
    }
    #[test]
    fn gr01() {
        assert_eq!(ev("greatest", &[i(10), i(20)]), i(20));
    }
    #[test]
    fn gr02() {
        assert_eq!(ev("greatest", &[i(100), i(50), i(75)]), i(100));
    }
    #[test]
    fn le01() {
        assert_eq!(ev("least", &[i(10), i(20)]), i(10));
    }
    #[test]
    fn le02() {
        assert_eq!(ev("least", &[i(100), i(50), i(75)]), i(50));
    }
    #[test]
    fn typeof_int() {
        assert_eq!(ev("typeof", &[i(42)]), s("i64"));
    }
    #[test]
    fn typeof_float() {
        assert_eq!(ev("typeof", &[f(1.0)]), s("f64"));
    }
    #[test]
    fn typeof_str() {
        assert_eq!(ev("typeof", &[s("x")]), s("string"));
    }
    #[test]
    fn typeof_null() {
        assert_eq!(ev("typeof", &[null()]), s("null"));
    }
    #[test]
    fn typeof_ts() {
        assert_eq!(ev("typeof", &[ts(1000)]), s("timestamp"));
    }
    #[test]
    fn cast_int_str() {
        assert_eq!(ev("cast_int", &[s("42")]), i(42));
    }
    #[test]
    fn cast_float_str() {
        let r = ev("cast_float", &[s("3.15")]);
        assert!(matches!(r, Value::F64(v) if (v - 3.15).abs() < 0.001));
    }
    #[test]
    fn cast_str_int() {
        assert_eq!(ev("cast_str", &[i(42)]), s("42"));
    }
    #[test]
    fn to_int_alias() {
        assert_eq!(ev("to_int", &[s("99")]), i(99));
    }
    #[test]
    fn to_float_alias() {
        let r = ev("to_float", &[s("1.5")]);
        assert!(matches!(r, Value::F64(v) if (v - 1.5).abs() < 0.001));
    }
}

// ===========================================================================
// url_encode / url_decode / json_extract — 30 tests
// ===========================================================================
mod url_json_t01 {
    use super::*;
    #[test]
    fn url_encode_basic() {
        assert_eq!(ev("url_encode", &[s("hello world")]), s("hello%20world"));
    }
    #[test]
    fn url_encode_empty() {
        assert_eq!(ev("url_encode", &[s("")]), s(""));
    }
    #[test]
    fn url_encode_null() {
        assert_eq!(ev("url_encode", &[null()]), null());
    }
    #[test]
    fn url_encode_noop() {
        assert_eq!(ev("url_encode", &[s("hello")]), s("hello"));
    }
    #[test]
    fn url_encode_special() {
        let r = ev("url_encode", &[s("a=b&c=d")]);
        match r {
            Value::Str(v) => assert!(v.contains("%")),
            _ => panic!(),
        }
    }
    #[test]
    fn url_decode_basic() {
        assert_eq!(ev("url_decode", &[s("hello%20world")]), s("hello world"));
    }
    #[test]
    fn url_decode_empty() {
        assert_eq!(ev("url_decode", &[s("")]), s(""));
    }
    #[test]
    fn url_decode_null() {
        assert_eq!(ev("url_decode", &[null()]), null());
    }
    #[test]
    fn url_decode_noop() {
        assert_eq!(ev("url_decode", &[s("hello")]), s("hello"));
    }
    #[test]
    fn url_roundtrip() {
        assert_eq!(
            ev("url_decode", &[ev("url_encode", &[s("hello world")])]),
            s("hello world")
        );
    }
    #[test]
    fn json_extract_str() {
        assert_eq!(ev("json_extract", &[s(r#"{"a":"b"}"#), s("a")]), s("b"));
    }
    #[test]
    fn json_extract_int() {
        assert_eq!(ev("json_extract", &[s(r#"{"x":42}"#), s("x")]), i(42));
    }
    #[test]
    fn json_extract_null() {
        assert_eq!(ev("json_extract", &[null(), s("a")]), null());
    }
    #[test]
    fn json_extract_missing() {
        assert_eq!(ev("json_extract", &[s(r#"{"a":"b"}"#), s("c")]), null());
    }
    #[test]
    fn json_array_length_3() {
        assert_eq!(ev("json_array_length", &[s("[1,2,3]")]), i(3));
    }
    #[test]
    fn json_array_length_0() {
        assert_eq!(ev("json_array_length", &[s("[]")]), i(0));
    }
    #[test]
    fn json_array_length_null() {
        assert_eq!(ev("json_array_length", &[null()]), null());
    }
    #[test]
    fn json_extract_nested() {
        assert_eq!(
            ev("json_extract", &[s(r#"{"a":{"b":"c"}}"#), s("a")]),
            s(r#"{"b":"c"}"#)
        );
    }
    #[test]
    fn json_array_length_5() {
        assert_eq!(ev("json_array_length", &[s("[1,2,3,4,5]")]), i(5));
    }
    #[test]
    fn json_array_length_1() {
        assert_eq!(ev("json_array_length", &[s("[99]")]), i(1));
    }
    #[test]
    fn url_enc01() {
        assert_eq!(ev("url_encode", &[s("test data")]), s("test%20data"));
    }
    #[test]
    fn url_dec01() {
        assert_eq!(ev("url_decode", &[s("test%20data")]), s("test data"));
    }
    #[test]
    fn url_rt01() {
        assert_eq!(
            ev("url_decode", &[ev("url_encode", &[s("a b c")])]),
            s("a b c")
        );
    }
    #[test]
    fn url_rt02() {
        assert_eq!(
            ev("url_decode", &[ev("url_encode", &[s("key=val")])]),
            s("key=val")
        );
    }
    #[test]
    fn to_json_int() {
        assert_eq!(ev("to_json", &[i(42)]), s("42"));
    }
    #[test]
    fn to_json_str() {
        assert_eq!(ev("to_json", &[s("hello")]), s("\"hello\""));
    }
    #[test]
    fn to_json_null() {
        assert_eq!(ev("to_json", &[null()]), s("null"));
    }
    #[test]
    fn to_json_float() {
        let r = ev("to_json", &[f(3.15)]);
        assert!(matches!(r, Value::Str(_)));
    }
    #[test]
    fn json_extract_bool() {
        let r = ev("json_extract", &[s(r#"{"a":true}"#), s("a")]);
        assert_eq!(r, i(1));
    }
    #[test]
    fn json_extract_num() {
        let r = ev("json_extract", &[s(r#"{"x":3.15}"#), s("x")]);
        assert!(matches!(r, Value::F64(v) if (v - 3.15).abs() < 0.001));
    }
}

// ===========================================================================
// misc: version / soundex / quote_ident / quote_literal — 30 tests
// ===========================================================================
mod misc_str_t01 {
    use super::*;
    #[test]
    fn version_returns_str() {
        let r = ev("version", &[]);
        assert!(matches!(r, Value::Str(_)));
    }
    #[test]
    fn version_nonempty() {
        let r = ev("version", &[]);
        match r {
            Value::Str(v) => assert!(!v.is_empty()),
            _ => panic!(),
        }
    }
    #[test]
    fn soundex_robert() {
        assert_eq!(ev("soundex", &[s("Robert")]), s("R163"));
    }
    #[test]
    fn soundex_null() {
        assert_eq!(ev("soundex", &[null()]), null());
    }
    #[test]
    fn soundex_empty() {
        assert_eq!(ev("soundex", &[s("")]), s("0000"));
    }
    #[test]
    fn soundex_same() {
        assert_eq!(ev("soundex", &[s("Smith")]), ev("soundex", &[s("Smyth")]));
    }
    #[test]
    fn quote_ident_basic() {
        let r = ev("quote_ident", &[s("hello")]);
        assert!(matches!(r, Value::Str(_)));
    }
    #[test]
    fn quote_ident_null() {
        assert_eq!(ev("quote_ident", &[null()]), null());
    }
    #[test]
    fn quote_literal_basic() {
        let r = ev("quote_literal", &[s("hello")]);
        assert!(matches!(r, Value::Str(_)));
    }
    #[test]
    fn quote_literal_null() {
        assert_eq!(ev("quote_literal", &[null()]), null());
    }
    #[test]
    fn current_schema() {
        let r = ev("current_schema", &[]);
        assert!(matches!(r, Value::Str(_)));
    }
    #[test]
    fn current_database() {
        let r = ev("current_database", &[]);
        assert!(matches!(r, Value::Str(_)));
    }
    #[test]
    fn current_user_fn() {
        let r = ev("current_user", &[]);
        assert!(matches!(r, Value::Str(_)));
    }
    #[test]
    fn pg_typeof_int() {
        let r = ev("pg_typeof", &[i(42)]);
        assert!(matches!(r, Value::Str(_)));
    }
    #[test]
    fn pg_typeof_str() {
        let r = ev("pg_typeof", &[s("x")]);
        assert!(matches!(r, Value::Str(_)));
    }
    #[test]
    fn pg_typeof_null() {
        let r = ev("pg_typeof", &[null()]);
        assert!(matches!(r, Value::Str(_)));
    }
    #[test]
    fn sizeof_int() {
        let r = ev("sizeof", &[i(42)]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn sizeof_str() {
        let r = ev("sizeof", &[s("hello")]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn sizeof_null() {
        let r = ev("sizeof", &[null()]);
        assert!(matches!(r, Value::I64(_)));
    }
    #[test]
    fn table_name_val() {
        let r = ev("table_name", &[s("trades")]);
        assert_eq!(r, s("trades"));
    }
    #[test]
    fn char_at_1() {
        assert_eq!(ev("char_at", &[s("hello"), i(1)]), s("h"));
    }
    #[test]
    fn char_at_2() {
        assert_eq!(ev("char_at", &[s("hello"), i(2)]), s("e"));
    }
    #[test]
    fn char_at_5() {
        assert_eq!(ev("char_at", &[s("hello"), i(5)]), s("o"));
    }
    #[test]
    fn char_at_null() {
        assert_eq!(ev("char_at", &[null(), i(1)]), null());
    }
    #[test]
    fn rnd_str_returns_str() {
        let r = ev("rnd_str", &[i(5)]);
        assert!(matches!(r, Value::Str(_)));
    }
    #[test]
    fn rnd_uuid4_format() {
        let r = ev("rnd_uuid4", &[]);
        match r {
            Value::Str(v) => assert_eq!(v.len(), 36),
            _ => panic!(),
        }
    }
    #[test]
    fn uuid_alias() {
        let r = ev("uuid", &[]);
        match r {
            Value::Str(v) => assert_eq!(v.len(), 36),
            _ => panic!(),
        }
    }
    #[test]
    fn uuid4_alias() {
        let r = ev("uuid4", &[]);
        match r {
            Value::Str(v) => assert_eq!(v.len(), 36),
            _ => panic!(),
        }
    }
    #[test]
    fn newid_alias() {
        let r = ev("newid", &[]);
        match r {
            Value::Str(v) => assert_eq!(v.len(), 36),
            _ => panic!(),
        }
    }
    #[test]
    fn generate_uid_alias() {
        let r = ev("generate_uid", &[]);
        match r {
            Value::Str(v) => assert_eq!(v.len(), 36),
            _ => panic!(),
        }
    }
}
