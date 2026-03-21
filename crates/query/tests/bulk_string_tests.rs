//! Bulk string function tests -- 1000 tests.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

fn ev(name: &str, args: &[Value]) -> Value {
    evaluate_scalar(name, args).unwrap()
}
fn i(n: i64) -> Value {
    Value::I64(n)
}
fn s(v: &str) -> Value {
    Value::Str(v.into())
}

// ===== length (50) =====
mod length {
    use super::*;
    #[test]
    fn l_empty() {
        assert_eq!(ev("length", &[s("")]), i(0));
    }
    #[test]
    fn l_1() {
        assert_eq!(ev("length", &[s("a")]), i(1));
    }
    #[test]
    fn l_2() {
        assert_eq!(ev("length", &[s("ab")]), i(2));
    }
    #[test]
    fn l_3() {
        assert_eq!(ev("length", &[s("abc")]), i(3));
    }
    #[test]
    fn l_4() {
        assert_eq!(ev("length", &[s("abcd")]), i(4));
    }
    #[test]
    fn l_5() {
        assert_eq!(ev("length", &[s("hello")]), i(5));
    }
    #[test]
    fn l_6() {
        assert_eq!(ev("length", &[s("foobar")]), i(6));
    }
    #[test]
    fn l_7() {
        assert_eq!(ev("length", &[s("abcdefg")]), i(7));
    }
    #[test]
    fn l_10() {
        assert_eq!(ev("length", &[s("0123456789")]), i(10));
    }
    #[test]
    fn l_space() {
        assert_eq!(ev("length", &[s(" ")]), i(1));
    }
    #[test]
    fn l_spaces() {
        assert_eq!(ev("length", &[s("   ")]), i(3));
    }
    #[test]
    fn l_tab() {
        assert_eq!(ev("length", &[s("\t")]), i(1));
    }
    #[test]
    fn l_newline() {
        assert_eq!(ev("length", &[s("\n")]), i(1));
    }
    #[test]
    fn l_special() {
        assert_eq!(ev("length", &[s("!@#")]), i(3));
    }
    #[test]
    fn l_digits() {
        assert_eq!(ev("length", &[s("123")]), i(3));
    }
    #[test]
    fn l_mixed() {
        assert_eq!(ev("length", &[s("a1b2")]), i(4));
    }
    #[test]
    fn l_20() {
        assert_eq!(ev("length", &[s("abcdefghijklmnopqrst")]), i(20));
    }
    #[test]
    fn l_repeat_50() {
        assert_eq!(ev("length", &[s(&"x".repeat(50))]), i(50));
    }
    #[test]
    fn l_repeat_100() {
        assert_eq!(ev("length", &[s(&"y".repeat(100))]), i(100));
    }
    #[test]
    fn l_repeat_200() {
        assert_eq!(ev("length", &[s(&"z".repeat(200))]), i(200));
    }
    #[test]
    fn l_null() {
        assert_eq!(ev("length", &[Value::Null]), Value::Null);
    }
    #[test]
    fn l_int_input() {
        assert_eq!(ev("length", &[i(12345)]), i(5));
    }
    #[test]
    fn l_int_0() {
        assert_eq!(ev("length", &[i(0)]), i(1));
    }
    #[test]
    fn l_int_neg() {
        assert_eq!(ev("length", &[i(-1)]), i(2));
    }
    #[test]
    fn l_punct() {
        assert_eq!(ev("length", &[s(".,;:")]), i(4));
    }
    #[test]
    fn l_len_alias() {
        assert_eq!(ev("len", &[s("abc")]), i(3));
    }
    #[test]
    fn l_char_length_alias() {
        assert_eq!(ev("char_length", &[s("abcd")]), i(4));
    }
    #[test]
    fn l_string_length_alias() {
        assert_eq!(ev("string_length", &[s("ab")]), i(2));
    }
    #[test]
    fn l_len_empty() {
        assert_eq!(ev("len", &[s("")]), i(0));
    }
    #[test]
    fn l_len_1() {
        assert_eq!(ev("len", &[s("x")]), i(1));
    }
    #[test]
    fn l_cl_empty() {
        assert_eq!(ev("char_length", &[s("")]), i(0));
    }
    #[test]
    fn l_sl_5() {
        assert_eq!(ev("string_length", &[s("hello")]), i(5));
    }
    #[test]
    fn l_repeat_500() {
        assert_eq!(ev("length", &[s(&"a".repeat(500))]), i(500));
    }
    #[test]
    fn l_repeat_1000() {
        assert_eq!(ev("length", &[s(&"b".repeat(1000))]), i(1000));
    }
    #[test]
    fn l_two_spaces() {
        assert_eq!(ev("length", &[s("  ")]), i(2));
    }
    #[test]
    fn l_trailing_space() {
        assert_eq!(ev("length", &[s("a ")]), i(2));
    }
    #[test]
    fn l_leading_space() {
        assert_eq!(ev("length", &[s(" a")]), i(2));
    }
    #[test]
    fn l_tab_nl() {
        assert_eq!(ev("length", &[s("\t\n")]), i(2));
    }
    #[test]
    fn l_abc_123() {
        assert_eq!(ev("length", &[s("abc123")]), i(6));
    }
    #[test]
    fn l_upper() {
        assert_eq!(ev("length", &[s("HELLO")]), i(5));
    }
    #[test]
    fn l_single_char_a() {
        assert_eq!(ev("length", &[s("A")]), i(1));
    }
    #[test]
    fn l_single_char_z() {
        assert_eq!(ev("length", &[s("Z")]), i(1));
    }
    #[test]
    fn l_number_str() {
        assert_eq!(ev("length", &[s("42")]), i(2));
    }
    #[test]
    fn l_neg_str() {
        assert_eq!(ev("length", &[s("-1")]), i(2));
    }
    #[test]
    fn l_float_str() {
        assert_eq!(ev("length", &[s("3.15")]), i(4));
    }
    #[test]
    fn l_path() {
        assert_eq!(ev("length", &[s("/a/b/c")]), i(6));
    }
    #[test]
    fn l_url() {
        assert_eq!(ev("length", &[s("http://x")]), i(8));
    }
    #[test]
    fn l_braces() {
        assert_eq!(ev("length", &[s("{}")]), i(2));
    }
    #[test]
    fn l_brackets() {
        assert_eq!(ev("length", &[s("[]")]), i(2));
    }
    #[test]
    fn l_parens() {
        assert_eq!(ev("length", &[s("()")]), i(2));
    }
}

// ===== upper (50) =====
mod upper {
    use super::*;
    #[test]
    fn u_hello() {
        assert_eq!(ev("upper", &[s("hello")]), s("HELLO"));
    }
    #[test]
    fn u_empty() {
        assert_eq!(ev("upper", &[s("")]), s(""));
    }
    #[test]
    fn u_already() {
        assert_eq!(ev("upper", &[s("ABC")]), s("ABC"));
    }
    #[test]
    fn u_mixed() {
        assert_eq!(ev("upper", &[s("aBcD")]), s("ABCD"));
    }
    #[test]
    fn u_digits() {
        assert_eq!(ev("upper", &[s("a1b2")]), s("A1B2"));
    }
    #[test]
    fn u_special() {
        assert_eq!(ev("upper", &[s("a!b")]), s("A!B"));
    }
    #[test]
    fn u_space() {
        assert_eq!(ev("upper", &[s("a b")]), s("A B"));
    }
    #[test]
    fn u_tab() {
        assert_eq!(ev("upper", &[s("a\tb")]), s("A\tB"));
    }
    #[test]
    fn u_single() {
        assert_eq!(ev("upper", &[s("x")]), s("X"));
    }
    #[test]
    fn u_a() {
        assert_eq!(ev("upper", &[s("a")]), s("A"));
    }
    #[test]
    fn u_z() {
        assert_eq!(ev("upper", &[s("z")]), s("Z"));
    }
    #[test]
    fn u_world() {
        assert_eq!(ev("upper", &[s("world")]), s("WORLD"));
    }
    #[test]
    fn u_rust() {
        assert_eq!(ev("upper", &[s("rust")]), s("RUST"));
    }
    #[test]
    fn u_test() {
        assert_eq!(ev("upper", &[s("test")]), s("TEST"));
    }
    #[test]
    fn u_foo() {
        assert_eq!(ev("upper", &[s("foo")]), s("FOO"));
    }
    #[test]
    fn u_bar() {
        assert_eq!(ev("upper", &[s("bar")]), s("BAR"));
    }
    #[test]
    fn u_nums() {
        assert_eq!(ev("upper", &[s("123")]), s("123"));
    }
    #[test]
    fn u_punct() {
        assert_eq!(ev("upper", &[s("!@#")]), s("!@#"));
    }
    #[test]
    fn u_nl() {
        assert_eq!(ev("upper", &[s("a\nb")]), s("A\nB"));
    }
    #[test]
    fn u_long() {
        assert_eq!(ev("upper", &[s("abcdefghijklmnop")]), s("ABCDEFGHIJKLMNOP"));
    }
    #[test]
    fn u_int_in() {
        assert_eq!(ev("upper", &[i(42)]), s("42"));
    }
    #[test]
    fn u_to_uppercase() {
        assert_eq!(ev("to_uppercase", &[s("hello")]), s("HELLO"));
    }
    #[test]
    fn u_to_uppercase_empty() {
        assert_eq!(ev("to_uppercase", &[s("")]), s(""));
    }
    #[test]
    fn u_to_uppercase_mixed() {
        assert_eq!(ev("to_uppercase", &[s("aB")]), s("AB"));
    }
    #[test]
    fn u_abc_up() {
        assert_eq!(ev("upper", &[s("abc")]), s("ABC"));
    }
    #[test]
    fn u_xyz() {
        assert_eq!(ev("upper", &[s("xyz")]), s("XYZ"));
    }
    #[test]
    fn u_qwerty() {
        assert_eq!(ev("upper", &[s("qwerty")]), s("QWERTY"));
    }
    #[test]
    fn u_asdf() {
        assert_eq!(ev("upper", &[s("asdf")]), s("ASDF"));
    }
    #[test]
    fn u_zxcv() {
        assert_eq!(ev("upper", &[s("zxcv")]), s("ZXCV"));
    }
    #[test]
    fn u_mn() {
        assert_eq!(ev("upper", &[s("mn")]), s("MN"));
    }
    #[test]
    fn u_op() {
        assert_eq!(ev("upper", &[s("op")]), s("OP"));
    }
    #[test]
    fn u_gh() {
        assert_eq!(ev("upper", &[s("gh")]), s("GH"));
    }
    #[test]
    fn u_jk() {
        assert_eq!(ev("upper", &[s("jk")]), s("JK"));
    }
    #[test]
    fn u_tu() {
        assert_eq!(ev("upper", &[s("tu")]), s("TU"));
    }
    #[test]
    fn u_vw() {
        assert_eq!(ev("upper", &[s("vw")]), s("VW"));
    }
    #[test]
    fn u_ab() {
        assert_eq!(ev("upper", &[s("ab")]), s("AB"));
    }
    #[test]
    fn u_cd() {
        assert_eq!(ev("upper", &[s("cd")]), s("CD"));
    }
    #[test]
    fn u_ef() {
        assert_eq!(ev("upper", &[s("ef")]), s("EF"));
    }
    #[test]
    fn u_hi() {
        assert_eq!(ev("upper", &[s("hi")]), s("HI"));
    }
    #[test]
    fn u_lm() {
        assert_eq!(ev("upper", &[s("lm")]), s("LM"));
    }
    #[test]
    fn u_no() {
        assert_eq!(ev("upper", &[s("no")]), s("NO"));
    }
    #[test]
    fn u_pq() {
        assert_eq!(ev("upper", &[s("pq")]), s("PQ"));
    }
    #[test]
    fn u_rs() {
        assert_eq!(ev("upper", &[s("rs")]), s("RS"));
    }
    #[test]
    fn u_yz() {
        assert_eq!(ev("upper", &[s("yz")]), s("YZ"));
    }
    #[test]
    fn u_abc_space() {
        assert_eq!(ev("upper", &[s("a b c")]), s("A B C"));
    }
    #[test]
    fn u_hyphen() {
        assert_eq!(ev("upper", &[s("a-b")]), s("A-B"));
    }
    #[test]
    fn u_underscore() {
        assert_eq!(ev("upper", &[s("a_b")]), s("A_B"));
    }
    #[test]
    fn u_dot() {
        assert_eq!(ev("upper", &[s("a.b")]), s("A.B"));
    }
    #[test]
    fn u_comma() {
        assert_eq!(ev("upper", &[s("a,b")]), s("A,B"));
    }
    #[test]
    fn u_colon() {
        assert_eq!(ev("upper", &[s("a:b")]), s("A:B"));
    }
}

// ===== lower (50) =====
mod lower {
    use super::*;
    #[test]
    fn lo_hello() {
        assert_eq!(ev("lower", &[s("HELLO")]), s("hello"));
    }
    #[test]
    fn lo_empty() {
        assert_eq!(ev("lower", &[s("")]), s(""));
    }
    #[test]
    fn lo_already() {
        assert_eq!(ev("lower", &[s("abc")]), s("abc"));
    }
    #[test]
    fn lo_mixed() {
        assert_eq!(ev("lower", &[s("AbCd")]), s("abcd"));
    }
    #[test]
    fn lo_digits() {
        assert_eq!(ev("lower", &[s("A1B2")]), s("a1b2"));
    }
    #[test]
    fn lo_special() {
        assert_eq!(ev("lower", &[s("A!B")]), s("a!b"));
    }
    #[test]
    fn lo_space() {
        assert_eq!(ev("lower", &[s("A B")]), s("a b"));
    }
    #[test]
    fn lo_single() {
        assert_eq!(ev("lower", &[s("X")]), s("x"));
    }
    #[test]
    fn lo_world() {
        assert_eq!(ev("lower", &[s("WORLD")]), s("world"));
    }
    #[test]
    fn lo_rust() {
        assert_eq!(ev("lower", &[s("RUST")]), s("rust"));
    }
    #[test]
    fn lo_test() {
        assert_eq!(ev("lower", &[s("TEST")]), s("test"));
    }
    #[test]
    fn lo_foo() {
        assert_eq!(ev("lower", &[s("FOO")]), s("foo"));
    }
    #[test]
    fn lo_bar() {
        assert_eq!(ev("lower", &[s("BAR")]), s("bar"));
    }
    #[test]
    fn lo_int() {
        assert_eq!(ev("lower", &[i(42)]), s("42"));
    }
    #[test]
    fn lo_to_lowercase() {
        assert_eq!(ev("to_lowercase", &[s("HELLO")]), s("hello"));
    }
    #[test]
    fn lo_to_lowercase_empty() {
        assert_eq!(ev("to_lowercase", &[s("")]), s(""));
    }
    #[test]
    fn lo_xyz() {
        assert_eq!(ev("lower", &[s("XYZ")]), s("xyz"));
    }
    #[test]
    fn lo_abc() {
        assert_eq!(ev("lower", &[s("ABC")]), s("abc"));
    }
    #[test]
    fn lo_qwerty() {
        assert_eq!(ev("lower", &[s("QWERTY")]), s("qwerty"));
    }
    #[test]
    fn lo_asdf() {
        assert_eq!(ev("lower", &[s("ASDF")]), s("asdf"));
    }
    #[test]
    fn lo_zxcv() {
        assert_eq!(ev("lower", &[s("ZXCV")]), s("zxcv"));
    }
    #[test]
    fn lo_mn() {
        assert_eq!(ev("lower", &[s("MN")]), s("mn"));
    }
    #[test]
    fn lo_op() {
        assert_eq!(ev("lower", &[s("OP")]), s("op"));
    }
    #[test]
    fn lo_gh() {
        assert_eq!(ev("lower", &[s("GH")]), s("gh"));
    }
    #[test]
    fn lo_jk() {
        assert_eq!(ev("lower", &[s("JK")]), s("jk"));
    }
    #[test]
    fn lo_tu() {
        assert_eq!(ev("lower", &[s("TU")]), s("tu"));
    }
    #[test]
    fn lo_vw() {
        assert_eq!(ev("lower", &[s("VW")]), s("vw"));
    }
    #[test]
    fn lo_ab() {
        assert_eq!(ev("lower", &[s("AB")]), s("ab"));
    }
    #[test]
    fn lo_cd() {
        assert_eq!(ev("lower", &[s("CD")]), s("cd"));
    }
    #[test]
    fn lo_ef() {
        assert_eq!(ev("lower", &[s("EF")]), s("ef"));
    }
    #[test]
    fn lo_hi() {
        assert_eq!(ev("lower", &[s("HI")]), s("hi"));
    }
    #[test]
    fn lo_lm() {
        assert_eq!(ev("lower", &[s("LM")]), s("lm"));
    }
    #[test]
    fn lo_no() {
        assert_eq!(ev("lower", &[s("NO")]), s("no"));
    }
    #[test]
    fn lo_pq() {
        assert_eq!(ev("lower", &[s("PQ")]), s("pq"));
    }
    #[test]
    fn lo_rs() {
        assert_eq!(ev("lower", &[s("RS")]), s("rs"));
    }
    #[test]
    fn lo_yz() {
        assert_eq!(ev("lower", &[s("YZ")]), s("yz"));
    }
    #[test]
    fn lo_long() {
        assert_eq!(ev("lower", &[s("ABCDEFGHIJKLMNOP")]), s("abcdefghijklmnop"));
    }
    #[test]
    fn lo_nums() {
        assert_eq!(ev("lower", &[s("123")]), s("123"));
    }
    #[test]
    fn lo_punct() {
        assert_eq!(ev("lower", &[s("!@#")]), s("!@#"));
    }
    #[test]
    fn lo_nl() {
        assert_eq!(ev("lower", &[s("A\nB")]), s("a\nb"));
    }
    #[test]
    fn lo_tab() {
        assert_eq!(ev("lower", &[s("A\tB")]), s("a\tb"));
    }
    #[test]
    fn lo_hyphen() {
        assert_eq!(ev("lower", &[s("A-B")]), s("a-b"));
    }
    #[test]
    fn lo_underscore() {
        assert_eq!(ev("lower", &[s("A_B")]), s("a_b"));
    }
    #[test]
    fn lo_dot() {
        assert_eq!(ev("lower", &[s("A.B")]), s("a.b"));
    }
    #[test]
    fn lo_comma() {
        assert_eq!(ev("lower", &[s("A,B")]), s("a,b"));
    }
    #[test]
    fn lo_colon() {
        assert_eq!(ev("lower", &[s("A:B")]), s("a:b"));
    }
    #[test]
    fn lo_a() {
        assert_eq!(ev("lower", &[s("A")]), s("a"));
    }
    #[test]
    fn lo_z() {
        assert_eq!(ev("lower", &[s("Z")]), s("z"));
    }
    #[test]
    fn lo_m() {
        assert_eq!(ev("lower", &[s("M")]), s("m"));
    }
    #[test]
    fn lo_space_mix() {
        assert_eq!(ev("lower", &[s("A B C")]), s("a b c"));
    }
}

// ===== trim (50) =====
mod trim_tests {
    use super::*;
    #[test]
    fn t_both() {
        assert_eq!(ev("trim", &[s("  hi  ")]), s("hi"));
    }
    #[test]
    fn t_empty() {
        assert_eq!(ev("trim", &[s("")]), s(""));
    }
    #[test]
    fn t_no_space() {
        assert_eq!(ev("trim", &[s("hi")]), s("hi"));
    }
    #[test]
    fn t_left_only() {
        assert_eq!(ev("trim", &[s("  hi")]), s("hi"));
    }
    #[test]
    fn t_right_only() {
        assert_eq!(ev("trim", &[s("hi  ")]), s("hi"));
    }
    #[test]
    fn t_single_space() {
        assert_eq!(ev("trim", &[s(" ")]), s(""));
    }
    #[test]
    fn t_all_spaces() {
        assert_eq!(ev("trim", &[s("   ")]), s(""));
    }
    #[test]
    fn t_inner() {
        assert_eq!(ev("trim", &[s("  a b  ")]), s("a b"));
    }
    #[test]
    fn t_tabs() {
        assert_eq!(ev("trim", &[s("\thi\t")]), s("hi"));
    }
    #[test]
    fn t_mixed_ws() {
        assert_eq!(ev("trim", &[s(" \t hi \t ")]), s("hi"));
    }
    #[test]
    fn t_word() {
        assert_eq!(ev("trim", &[s("  hello  ")]), s("hello"));
    }
    #[test]
    fn t_sentence() {
        assert_eq!(ev("trim", &[s("  a b c  ")]), s("a b c"));
    }
    #[test]
    fn t_l_both() {
        assert_eq!(ev("ltrim", &[s("  hi  ")]), s("hi  "));
    }
    #[test]
    fn t_l_empty() {
        assert_eq!(ev("ltrim", &[s("")]), s(""));
    }
    #[test]
    fn t_l_no_space() {
        assert_eq!(ev("ltrim", &[s("hi")]), s("hi"));
    }
    #[test]
    fn t_l_left_only() {
        assert_eq!(ev("ltrim", &[s("  hi")]), s("hi"));
    }
    #[test]
    fn t_l_right_only() {
        assert_eq!(ev("ltrim", &[s("hi  ")]), s("hi  "));
    }
    #[test]
    fn t_l_single() {
        assert_eq!(ev("ltrim", &[s(" ")]), s(""));
    }
    #[test]
    fn t_l_all() {
        assert_eq!(ev("ltrim", &[s("   ")]), s(""));
    }
    #[test]
    fn t_l_inner() {
        assert_eq!(ev("ltrim", &[s("  a b  ")]), s("a b  "));
    }
    #[test]
    fn t_l_word() {
        assert_eq!(ev("ltrim", &[s("  hello  ")]), s("hello  "));
    }
    #[test]
    fn t_l_tabs() {
        assert_eq!(ev("ltrim", &[s("\thi")]), s("hi"));
    }
    #[test]
    fn t_r_both() {
        assert_eq!(ev("rtrim", &[s("  hi  ")]), s("  hi"));
    }
    #[test]
    fn t_r_empty() {
        assert_eq!(ev("rtrim", &[s("")]), s(""));
    }
    #[test]
    fn t_r_no_space() {
        assert_eq!(ev("rtrim", &[s("hi")]), s("hi"));
    }
    #[test]
    fn t_r_left_only() {
        assert_eq!(ev("rtrim", &[s("  hi")]), s("  hi"));
    }
    #[test]
    fn t_r_right_only() {
        assert_eq!(ev("rtrim", &[s("hi  ")]), s("hi"));
    }
    #[test]
    fn t_r_single() {
        assert_eq!(ev("rtrim", &[s(" ")]), s(""));
    }
    #[test]
    fn t_r_all() {
        assert_eq!(ev("rtrim", &[s("   ")]), s(""));
    }
    #[test]
    fn t_r_inner() {
        assert_eq!(ev("rtrim", &[s("  a b  ")]), s("  a b"));
    }
    #[test]
    fn t_r_word() {
        assert_eq!(ev("rtrim", &[s("  hello  ")]), s("  hello"));
    }
    #[test]
    fn t_r_tabs() {
        assert_eq!(ev("rtrim", &[s("hi\t")]), s("hi"));
    }
    #[test]
    fn t_nl() {
        assert_eq!(ev("trim", &[s("\nhi\n")]), s("hi"));
    }
    #[test]
    fn t_r_nl() {
        assert_eq!(ev("rtrim", &[s("hi\n")]), s("hi"));
    }
    #[test]
    fn t_l_nl() {
        assert_eq!(ev("ltrim", &[s("\nhi")]), s("hi"));
    }
    #[test]
    fn t_three_spaces_l() {
        assert_eq!(ev("ltrim", &[s("   x")]), s("x"));
    }
    #[test]
    fn t_three_spaces_r() {
        assert_eq!(ev("rtrim", &[s("x   ")]), s("x"));
    }
    #[test]
    fn t_three_spaces_b() {
        assert_eq!(ev("trim", &[s("   x   ")]), s("x"));
    }
    #[test]
    fn t_multi_word() {
        assert_eq!(ev("trim", &[s("  foo bar  ")]), s("foo bar"));
    }
    #[test]
    fn t_l_multi_word() {
        assert_eq!(ev("ltrim", &[s("  foo bar")]), s("foo bar"));
    }
    #[test]
    fn t_r_multi_word() {
        assert_eq!(ev("rtrim", &[s("foo bar  ")]), s("foo bar"));
    }
    #[test]
    fn t_only_tab() {
        assert_eq!(ev("trim", &[s("\t")]), s(""));
    }
    #[test]
    fn t_l_only_tab() {
        assert_eq!(ev("ltrim", &[s("\t")]), s(""));
    }
    #[test]
    fn t_r_only_tab() {
        assert_eq!(ev("rtrim", &[s("\t")]), s(""));
    }
    #[test]
    fn t_mixed_ws2() {
        assert_eq!(ev("trim", &[s("\n\t x \t\n")]), s("x"));
    }
    #[test]
    fn t_int_in() {
        assert_eq!(ev("trim", &[i(42)]), s("42"));
    }
    #[test]
    fn t_l_int_in() {
        assert_eq!(ev("ltrim", &[i(42)]), s("42"));
    }
    #[test]
    fn t_r_int_in() {
        assert_eq!(ev("rtrim", &[i(42)]), s("42"));
    }
    #[test]
    fn t_null() {
        assert_eq!(ev("trim", &[Value::Null]), Value::Null);
    }
    #[test]
    fn t_null_l() {
        assert_eq!(ev("ltrim", &[Value::Null]), Value::Null);
    }
}

// ===== substring (70) =====
mod substring_tests {
    use super::*;
    #[test]
    fn sub_1_5() {
        assert_eq!(ev("substring", &[s("hello world"), i(1), i(5)]), s("hello"));
    }
    #[test]
    fn sub_7_5() {
        assert_eq!(ev("substring", &[s("hello world"), i(7), i(5)]), s("world"));
    }
    #[test]
    fn sub_1_1() {
        assert_eq!(ev("substring", &[s("abc"), i(1), i(1)]), s("a"));
    }
    #[test]
    fn sub_2_1() {
        assert_eq!(ev("substring", &[s("abc"), i(2), i(1)]), s("b"));
    }
    #[test]
    fn sub_3_1() {
        assert_eq!(ev("substring", &[s("abc"), i(3), i(1)]), s("c"));
    }
    #[test]
    fn sub_1_3() {
        assert_eq!(ev("substring", &[s("abc"), i(1), i(3)]), s("abc"));
    }
    #[test]
    fn sub_1_0() {
        assert_eq!(ev("substring", &[s("abc"), i(1), i(0)]), s(""));
    }
    #[test]
    fn sub_empty() {
        assert_eq!(ev("substring", &[s(""), i(1), i(0)]), s(""));
    }
    #[test]
    fn sub_1_2() {
        assert_eq!(ev("substring", &[s("abcdef"), i(1), i(2)]), s("ab"));
    }
    #[test]
    fn sub_3_2() {
        assert_eq!(ev("substring", &[s("abcdef"), i(3), i(2)]), s("cd"));
    }
    #[test]
    fn sub_5_2() {
        assert_eq!(ev("substring", &[s("abcdef"), i(5), i(2)]), s("ef"));
    }
    #[test]
    fn sub_1_6() {
        assert_eq!(ev("substring", &[s("abcdef"), i(1), i(6)]), s("abcdef"));
    }
    #[test]
    fn sub_2_3() {
        assert_eq!(ev("substring", &[s("abcdef"), i(2), i(3)]), s("bcd"));
    }
    #[test]
    fn sub_4_3() {
        assert_eq!(ev("substring", &[s("abcdef"), i(4), i(3)]), s("def"));
    }
    #[test]
    fn sub_1_10() {
        assert_eq!(ev("substring", &[s("hello"), i(1), i(10)]), s("hello"));
    }
    #[test]
    fn sub_2_10() {
        assert_eq!(ev("substring", &[s("hello"), i(2), i(10)]), s("ello"));
    }
    #[test]
    fn sub_h_1_1() {
        assert_eq!(ev("substring", &[s("hello"), i(1), i(1)]), s("h"));
    }
    #[test]
    fn sub_h_2_1() {
        assert_eq!(ev("substring", &[s("hello"), i(2), i(1)]), s("e"));
    }
    #[test]
    fn sub_h_3_1() {
        assert_eq!(ev("substring", &[s("hello"), i(3), i(1)]), s("l"));
    }
    #[test]
    fn sub_h_4_1() {
        assert_eq!(ev("substring", &[s("hello"), i(4), i(1)]), s("l"));
    }
    #[test]
    fn sub_h_5_1() {
        assert_eq!(ev("substring", &[s("hello"), i(5), i(1)]), s("o"));
    }
    #[test]
    fn sub_h_1_2() {
        assert_eq!(ev("substring", &[s("hello"), i(1), i(2)]), s("he"));
    }
    #[test]
    fn sub_h_1_3() {
        assert_eq!(ev("substring", &[s("hello"), i(1), i(3)]), s("hel"));
    }
    #[test]
    fn sub_h_1_4() {
        assert_eq!(ev("substring", &[s("hello"), i(1), i(4)]), s("hell"));
    }
    #[test]
    fn sub_h_1_5() {
        assert_eq!(ev("substring", &[s("hello"), i(1), i(5)]), s("hello"));
    }
    #[test]
    fn sub_h_2_2() {
        assert_eq!(ev("substring", &[s("hello"), i(2), i(2)]), s("el"));
    }
    #[test]
    fn sub_h_2_3() {
        assert_eq!(ev("substring", &[s("hello"), i(2), i(3)]), s("ell"));
    }
    #[test]
    fn sub_h_3_2() {
        assert_eq!(ev("substring", &[s("hello"), i(3), i(2)]), s("ll"));
    }
    #[test]
    fn sub_h_3_3() {
        assert_eq!(ev("substring", &[s("hello"), i(3), i(3)]), s("llo"));
    }
    #[test]
    fn sub_h_4_2() {
        assert_eq!(ev("substring", &[s("hello"), i(4), i(2)]), s("lo"));
    }
    #[test]
    fn sub_digits_1_3() {
        assert_eq!(ev("substring", &[s("0123456789"), i(1), i(3)]), s("012"));
    }
    #[test]
    fn sub_digits_4_3() {
        assert_eq!(ev("substring", &[s("0123456789"), i(4), i(3)]), s("345"));
    }
    #[test]
    fn sub_digits_8_3() {
        assert_eq!(ev("substring", &[s("0123456789"), i(8), i(3)]), s("789"));
    }
    #[test]
    fn sub_digits_1_10() {
        assert_eq!(
            ev("substring", &[s("0123456789"), i(1), i(10)]),
            s("0123456789")
        );
    }
    #[test]
    fn sub_1char_1_1() {
        assert_eq!(ev("substring", &[s("x"), i(1), i(1)]), s("x"));
    }
    #[test]
    fn sub_space_1_1() {
        assert_eq!(ev("substring", &[s(" "), i(1), i(1)]), s(" "));
    }
    #[test]
    fn sub_ab_1_1() {
        assert_eq!(ev("substring", &[s("ab"), i(1), i(1)]), s("a"));
    }
    #[test]
    fn sub_ab_2_1() {
        assert_eq!(ev("substring", &[s("ab"), i(2), i(1)]), s("b"));
    }
    #[test]
    fn sub_ab_1_2() {
        assert_eq!(ev("substring", &[s("ab"), i(1), i(2)]), s("ab"));
    }
    #[test]
    fn sub_foobar_1_3() {
        assert_eq!(ev("substring", &[s("foobar"), i(1), i(3)]), s("foo"));
    }
    #[test]
    fn sub_foobar_4_3() {
        assert_eq!(ev("substring", &[s("foobar"), i(4), i(3)]), s("bar"));
    }
    #[test]
    fn sub_foobar_2_4() {
        assert_eq!(ev("substring", &[s("foobar"), i(2), i(4)]), s("ooba"));
    }
    #[test]
    fn sub_12345_1_1() {
        assert_eq!(ev("substring", &[s("12345"), i(1), i(1)]), s("1"));
    }
    #[test]
    fn sub_12345_2_1() {
        assert_eq!(ev("substring", &[s("12345"), i(2), i(1)]), s("2"));
    }
    #[test]
    fn sub_12345_5_1() {
        assert_eq!(ev("substring", &[s("12345"), i(5), i(1)]), s("5"));
    }
    #[test]
    fn sub_12345_1_5() {
        assert_eq!(ev("substring", &[s("12345"), i(1), i(5)]), s("12345"));
    }
    #[test]
    fn sub_12345_3_2() {
        assert_eq!(ev("substring", &[s("12345"), i(3), i(2)]), s("34"));
    }
    #[test]
    fn sub_long_1_3() {
        assert_eq!(ev("substring", &[s("abcdefghij"), i(1), i(3)]), s("abc"));
    }
    #[test]
    fn sub_long_5_3() {
        assert_eq!(ev("substring", &[s("abcdefghij"), i(5), i(3)]), s("efg"));
    }
    #[test]
    fn sub_long_8_3() {
        assert_eq!(ev("substring", &[s("abcdefghij"), i(8), i(3)]), s("hij"));
    }
    #[test]
    fn sub_null() {
        assert_eq!(ev("substring", &[Value::Null, i(1), i(1)]), Value::Null);
    }
    #[test]
    fn sub_hw_1_11() {
        assert_eq!(
            ev("substring", &[s("hello world"), i(1), i(11)]),
            s("hello world")
        );
    }
    #[test]
    fn sub_hw_6_1() {
        assert_eq!(ev("substring", &[s("hello world"), i(6), i(1)]), s(" "));
    }
    #[test]
    fn sub_hw_1_0() {
        assert_eq!(ev("substring", &[s("hello world"), i(1), i(0)]), s(""));
    }
    #[test]
    fn sub_xyz_1_1() {
        assert_eq!(ev("substring", &[s("xyz"), i(1), i(1)]), s("x"));
    }
    #[test]
    fn sub_xyz_2_1() {
        assert_eq!(ev("substring", &[s("xyz"), i(2), i(1)]), s("y"));
    }
    #[test]
    fn sub_xyz_3_1() {
        assert_eq!(ev("substring", &[s("xyz"), i(3), i(1)]), s("z"));
    }
    #[test]
    fn sub_xyz_1_2() {
        assert_eq!(ev("substring", &[s("xyz"), i(1), i(2)]), s("xy"));
    }
    #[test]
    fn sub_xyz_2_2() {
        assert_eq!(ev("substring", &[s("xyz"), i(2), i(2)]), s("yz"));
    }
    #[test]
    fn sub_xyz_1_3() {
        assert_eq!(ev("substring", &[s("xyz"), i(1), i(3)]), s("xyz"));
    }
    #[test]
    fn sub_abcde_1_4() {
        assert_eq!(ev("substring", &[s("abcde"), i(1), i(4)]), s("abcd"));
    }
    #[test]
    fn sub_abcde_2_4() {
        assert_eq!(ev("substring", &[s("abcde"), i(2), i(4)]), s("bcde"));
    }
    #[test]
    fn sub_abcde_3_2() {
        assert_eq!(ev("substring", &[s("abcde"), i(3), i(2)]), s("cd"));
    }
    #[test]
    fn sub_abcde_4_2() {
        assert_eq!(ev("substring", &[s("abcde"), i(4), i(2)]), s("de"));
    }
    #[test]
    fn sub_abcde_5_1() {
        assert_eq!(ev("substring", &[s("abcde"), i(5), i(1)]), s("e"));
    }
    #[test]
    fn sub_abcde_1_1() {
        assert_eq!(ev("substring", &[s("abcde"), i(1), i(1)]), s("a"));
    }
    #[test]
    fn sub_abcde_2_1() {
        assert_eq!(ev("substring", &[s("abcde"), i(2), i(1)]), s("b"));
    }
    #[test]
    fn sub_abcde_3_1() {
        assert_eq!(ev("substring", &[s("abcde"), i(3), i(1)]), s("c"));
    }
    #[test]
    fn sub_abcde_4_1() {
        assert_eq!(ev("substring", &[s("abcde"), i(4), i(1)]), s("d"));
    }
    #[test]
    fn sub_abcde_1_5() {
        assert_eq!(ev("substring", &[s("abcde"), i(1), i(5)]), s("abcde"));
    }
    #[test]
    fn sub_abcde_2_3() {
        assert_eq!(ev("substring", &[s("abcde"), i(2), i(3)]), s("bcd"));
    }
}

// ===== concat (50) =====
mod concat_tests {
    use super::*;
    #[test]
    fn c_hw() {
        assert_eq!(ev("concat", &[s("hello"), s(" world")]), s("hello world"));
    }
    #[test]
    fn c_empty_l() {
        assert_eq!(ev("concat", &[s(""), s("hi")]), s("hi"));
    }
    #[test]
    fn c_empty_r() {
        assert_eq!(ev("concat", &[s("hi"), s("")]), s("hi"));
    }
    #[test]
    fn c_both_empty() {
        assert_eq!(ev("concat", &[s(""), s("")]), s(""));
    }
    #[test]
    fn c_ab() {
        assert_eq!(ev("concat", &[s("a"), s("b")]), s("ab"));
    }
    #[test]
    fn c_abc() {
        assert_eq!(ev("concat", &[s("a"), s("b"), s("c")]), s("abc"));
    }
    #[test]
    fn c_four() {
        assert_eq!(ev("concat", &[s("a"), s("b"), s("c"), s("d")]), s("abcd"));
    }
    #[test]
    fn c_spaces() {
        assert_eq!(ev("concat", &[s(" "), s(" ")]), s("  "));
    }
    #[test]
    fn c_nums() {
        assert_eq!(ev("concat", &[s("1"), s("2"), s("3")]), s("123"));
    }
    #[test]
    fn c_int_str() {
        assert_eq!(ev("concat", &[i(1), s("x")]), s("1x"));
    }
    #[test]
    fn c_str_int() {
        assert_eq!(ev("concat", &[s("x"), i(1)]), s("x1"));
    }
    #[test]
    fn c_two_ints() {
        assert_eq!(ev("concat", &[i(1), i(2)]), s("12"));
    }
    #[test]
    fn c_foo_bar() {
        assert_eq!(ev("concat", &[s("foo"), s("bar")]), s("foobar"));
    }
    #[test]
    fn c_hello_bang() {
        assert_eq!(ev("concat", &[s("hello"), s("!")]), s("hello!"));
    }
    #[test]
    fn c_dash() {
        assert_eq!(ev("concat", &[s("a"), s("-"), s("b")]), s("a-b"));
    }
    #[test]
    fn c_underscore() {
        assert_eq!(ev("concat", &[s("a"), s("_"), s("b")]), s("a_b"));
    }
    #[test]
    fn c_dot() {
        assert_eq!(ev("concat", &[s("a"), s("."), s("b")]), s("a.b"));
    }
    #[test]
    fn c_slash() {
        assert_eq!(ev("concat", &[s("a"), s("/"), s("b")]), s("a/b"));
    }
    #[test]
    fn c_colon() {
        assert_eq!(ev("concat", &[s("a"), s(":"), s("b")]), s("a:b"));
    }
    #[test]
    fn c_comma() {
        assert_eq!(ev("concat", &[s("a"), s(","), s("b")]), s("a,b"));
    }
    #[test]
    fn c_five() {
        assert_eq!(
            ev("concat", &[s("a"), s("b"), s("c"), s("d"), s("e")]),
            s("abcde")
        );
    }
    #[test]
    fn c_upper_lower() {
        assert_eq!(ev("concat", &[s("ABC"), s("def")]), s("ABCdef"));
    }
    #[test]
    fn c_single_a() {
        assert_eq!(ev("concat", &[s("a"), s("")]), s("a"));
    }
    #[test]
    fn c_single_empty() {
        assert_eq!(ev("concat", &[s(""), s("")]), s(""));
    }
    #[test]
    fn c_mixed_types() {
        assert_eq!(ev("concat", &[s("val="), i(42)]), s("val=42"));
    }
    #[test]
    fn c_xyz_123() {
        assert_eq!(ev("concat", &[s("xyz"), s("123")]), s("xyz123"));
    }
    #[test]
    fn c_hi_there() {
        assert_eq!(ev("concat", &[s("hi"), s(" "), s("there")]), s("hi there"));
    }
    #[test]
    fn c_three_spaces() {
        assert_eq!(ev("concat", &[s(" "), s(" "), s(" ")]), s("   "));
    }
    #[test]
    fn c_ab_cd() {
        assert_eq!(ev("concat", &[s("ab"), s("cd")]), s("abcd"));
    }
    #[test]
    fn c_ef_gh() {
        assert_eq!(ev("concat", &[s("ef"), s("gh")]), s("efgh"));
    }
    #[test]
    fn c_ij_kl() {
        assert_eq!(ev("concat", &[s("ij"), s("kl")]), s("ijkl"));
    }
    #[test]
    fn c_mn_op() {
        assert_eq!(ev("concat", &[s("mn"), s("op")]), s("mnop"));
    }
    #[test]
    fn c_qr_st() {
        assert_eq!(ev("concat", &[s("qr"), s("st")]), s("qrst"));
    }
    #[test]
    fn c_uv_wx() {
        assert_eq!(ev("concat", &[s("uv"), s("wx")]), s("uvwx"));
    }
    #[test]
    fn c_yz_00() {
        assert_eq!(ev("concat", &[s("yz"), s("00")]), s("yz00"));
    }
    #[test]
    fn c_hello_world_bang() {
        assert_eq!(
            ev("concat", &[s("hello"), s(" "), s("world"), s("!")]),
            s("hello world!")
        );
    }
    #[test]
    fn c_path() {
        assert_eq!(ev("concat", &[s("/"), s("a"), s("/"), s("b")]), s("/a/b"));
    }
    #[test]
    fn c_brackets() {
        assert_eq!(ev("concat", &[s("["), s("x"), s("]")]), s("[x]"));
    }
    #[test]
    fn c_braces() {
        assert_eq!(ev("concat", &[s("{"), s("x"), s("}")]), s("{x}"));
    }
    #[test]
    fn c_parens() {
        assert_eq!(ev("concat", &[s("("), s("x"), s(")")]), s("(x)"));
    }
    #[test]
    fn c_eq() {
        assert_eq!(ev("concat", &[s("a"), s("="), s("b")]), s("a=b"));
    }
    #[test]
    fn c_pipe() {
        assert_eq!(ev("concat", &[s("a"), s("|"), s("b")]), s("a|b"));
    }
    #[test]
    fn c_at() {
        assert_eq!(ev("concat", &[s("a"), s("@"), s("b")]), s("a@b"));
    }
    #[test]
    fn c_hash() {
        assert_eq!(ev("concat", &[s("a"), s("#"), s("b")]), s("a#b"));
    }
    #[test]
    fn c_dollar() {
        assert_eq!(ev("concat", &[s("a"), s("$"), s("b")]), s("a$b"));
    }
    #[test]
    fn c_pct() {
        assert_eq!(ev("concat", &[s("a"), s("%"), s("b")]), s("a%b"));
    }
    #[test]
    fn c_amp() {
        assert_eq!(ev("concat", &[s("a"), s("&"), s("b")]), s("a&b"));
    }
    #[test]
    fn c_star() {
        assert_eq!(ev("concat", &[s("a"), s("*"), s("b")]), s("a*b"));
    }
    #[test]
    fn c_plus() {
        assert_eq!(ev("concat", &[s("a"), s("+"), s("b")]), s("a+b"));
    }
    #[test]
    fn c_tilde() {
        assert_eq!(ev("concat", &[s("a"), s("~"), s("b")]), s("a~b"));
    }
}

// ===== replace (50) =====
mod replace_tests {
    use super::*;
    #[test]
    fn r_basic() {
        assert_eq!(
            ev("replace", &[s("hello world"), s("world"), s("rust")]),
            s("hello rust")
        );
    }
    #[test]
    fn r_no_match() {
        assert_eq!(ev("replace", &[s("hello"), s("xyz"), s("abc")]), s("hello"));
    }
    #[test]
    fn r_empty_from() {
        let r = ev("replace", &[s("hello"), s(""), s("x")]);
        assert!(matches!(r, Value::Str(_)));
    }
    #[test]
    fn r_empty_to() {
        assert_eq!(ev("replace", &[s("hello"), s("l"), s("")]), s("heo"));
    }
    #[test]
    fn r_all() {
        assert_eq!(ev("replace", &[s("aaa"), s("a"), s("b")]), s("bbb"));
    }
    #[test]
    fn r_first_char() {
        assert_eq!(ev("replace", &[s("abc"), s("a"), s("x")]), s("xbc"));
    }
    #[test]
    fn r_last_char() {
        assert_eq!(ev("replace", &[s("abc"), s("c"), s("x")]), s("abx"));
    }
    #[test]
    fn r_middle() {
        assert_eq!(ev("replace", &[s("abc"), s("b"), s("x")]), s("axc"));
    }
    #[test]
    fn r_longer() {
        assert_eq!(ev("replace", &[s("ab"), s("a"), s("xyz")]), s("xyzb"));
    }
    #[test]
    fn r_shorter() {
        assert_eq!(ev("replace", &[s("abc"), s("abc"), s("x")]), s("x"));
    }
    #[test]
    fn r_same() {
        assert_eq!(ev("replace", &[s("abc"), s("b"), s("b")]), s("abc"));
    }
    #[test]
    fn r_multi() {
        assert_eq!(ev("replace", &[s("abab"), s("ab"), s("x")]), s("xx"));
    }
    #[test]
    fn r_space() {
        assert_eq!(ev("replace", &[s("a b c"), s(" "), s("-")]), s("a-b-c"));
    }
    #[test]
    fn r_dot() {
        assert_eq!(ev("replace", &[s("a.b.c"), s("."), s("/")]), s("a/b/c"));
    }
    #[test]
    fn r_empty_str() {
        assert_eq!(ev("replace", &[s(""), s("a"), s("b")]), s(""));
    }
    #[test]
    fn r_full_replace() {
        assert_eq!(ev("replace", &[s("hello"), s("hello"), s("bye")]), s("bye"));
    }
    #[test]
    fn r_double() {
        assert_eq!(ev("replace", &[s("aa"), s("a"), s("bb")]), s("bbbb"));
    }
    #[test]
    fn r_triple() {
        assert_eq!(ev("replace", &[s("aaa"), s("a"), s("bb")]), s("bbbbbb"));
    }
    #[test]
    fn r_case() {
        assert_eq!(ev("replace", &[s("Hello"), s("H"), s("h")]), s("hello"));
    }
    #[test]
    fn r_at() {
        assert_eq!(ev("replace", &[s("a@b"), s("@"), s(" at ")]), s("a at b"));
    }
    #[test]
    fn r_dash() {
        assert_eq!(ev("replace", &[s("a-b-c"), s("-"), s("_")]), s("a_b_c"));
    }
    #[test]
    fn r_comma() {
        assert_eq!(ev("replace", &[s("a,b,c"), s(","), s(";")]), s("a;b;c"));
    }
    #[test]
    fn r_long_from() {
        assert_eq!(
            ev("replace", &[s("fooXYZbar"), s("XYZ"), s("_")]),
            s("foo_bar")
        );
    }
    #[test]
    fn r_begin() {
        assert_eq!(ev("replace", &[s("XXhello"), s("XX"), s("")]), s("hello"));
    }
    #[test]
    fn r_end() {
        assert_eq!(ev("replace", &[s("helloXX"), s("XX"), s("")]), s("hello"));
    }
    #[test]
    fn r_null() {
        assert_eq!(ev("replace", &[Value::Null, s("a"), s("b")]), Value::Null);
    }
    #[test]
    fn r_num_str() {
        assert_eq!(
            ev("replace", &[s("abc123"), s("123"), s("456")]),
            s("abc456")
        );
    }
    #[test]
    fn r_slash() {
        assert_eq!(ev("replace", &[s("a/b/c"), s("/"), s("\\")]), s("a\\b\\c"));
    }
    #[test]
    fn r_pipe() {
        assert_eq!(ev("replace", &[s("a|b"), s("|"), s(",")]), s("a,b"));
    }
    #[test]
    fn r_tab() {
        assert_eq!(ev("replace", &[s("a\tb"), s("\t"), s(" ")]), s("a b"));
    }
    #[test]
    fn r_whole() {
        assert_eq!(ev("replace", &[s("x"), s("x"), s("y")]), s("y"));
    }
    #[test]
    fn r_xy_to_yx() {
        assert_eq!(ev("replace", &[s("xy"), s("xy"), s("yx")]), s("yx"));
    }
    #[test]
    fn r_aa_a() {
        assert_eq!(ev("replace", &[s("aaaa"), s("aa"), s("b")]), s("bb"));
    }
    #[test]
    fn r_single_to_multi() {
        assert_eq!(ev("replace", &[s("a"), s("a"), s("abc")]), s("abc"));
    }
    #[test]
    fn r_multi_to_single() {
        assert_eq!(ev("replace", &[s("abc"), s("abc"), s("a")]), s("a"));
    }
    #[test]
    fn r_no_change() {
        assert_eq!(ev("replace", &[s("hello"), s("x"), s("y")]), s("hello"));
    }
    #[test]
    fn r_int_in() {
        assert_eq!(ev("replace", &[i(123), s("2"), s("X")]), s("1X3"));
    }
    #[test]
    fn r_excl() {
        assert_eq!(ev("replace", &[s("hi!"), s("!"), s(".")]), s("hi."));
    }
    #[test]
    fn r_ques() {
        assert_eq!(ev("replace", &[s("hi?"), s("?"), s("!")]), s("hi!"));
    }
    #[test]
    fn r_hash() {
        assert_eq!(ev("replace", &[s("a#b"), s("#"), s("")]), s("ab"));
    }
    #[test]
    fn r_pct() {
        assert_eq!(
            ev("replace", &[s("50%"), s("%"), s(" percent")]),
            s("50 percent")
        );
    }
    #[test]
    fn r_and() {
        assert_eq!(ev("replace", &[s("a&b"), s("&"), s(" and ")]), s("a and b"));
    }
    #[test]
    fn r_eq() {
        assert_eq!(ev("replace", &[s("a=b"), s("="), s("==")]), s("a==b"));
    }
    #[test]
    fn r_plus() {
        assert_eq!(
            ev("replace", &[s("a+b"), s("+"), s(" plus ")]),
            s("a plus b")
        );
    }
    #[test]
    fn r_nl() {
        assert_eq!(ev("replace", &[s("a\nb"), s("\n"), s(" ")]), s("a b"));
    }
    #[test]
    fn r_cr() {
        assert_eq!(ev("replace", &[s("a\rb"), s("\r"), s("")]), s("ab"));
    }
    #[test]
    fn r_colon() {
        assert_eq!(ev("replace", &[s("a:b"), s(":"), s("=")]), s("a=b"));
    }
    #[test]
    fn r_semi() {
        assert_eq!(ev("replace", &[s("a;b"), s(";"), s(",")]), s("a,b"));
    }
    #[test]
    fn r_tilde() {
        assert_eq!(ev("replace", &[s("a~b"), s("~"), s("-")]), s("a-b"));
    }
    #[test]
    fn r_numbers() {
        assert_eq!(ev("replace", &[s("x1y2z3"), s("1"), s("A")]), s("xAy2z3"));
    }
}

// ===== reverse (50) =====
mod reverse_tests {
    use super::*;
    #[test]
    fn rev_abc() {
        assert_eq!(ev("reverse", &[s("abc")]), s("cba"));
    }
    #[test]
    fn rev_empty() {
        assert_eq!(ev("reverse", &[s("")]), s(""));
    }
    #[test]
    fn rev_a() {
        assert_eq!(ev("reverse", &[s("a")]), s("a"));
    }
    #[test]
    fn rev_ab() {
        assert_eq!(ev("reverse", &[s("ab")]), s("ba"));
    }
    #[test]
    fn rev_hello() {
        assert_eq!(ev("reverse", &[s("hello")]), s("olleh"));
    }
    #[test]
    fn rev_abcde() {
        assert_eq!(ev("reverse", &[s("abcde")]), s("edcba"));
    }
    #[test]
    fn rev_12345() {
        assert_eq!(ev("reverse", &[s("12345")]), s("54321"));
    }
    #[test]
    fn rev_palindrome() {
        assert_eq!(ev("reverse", &[s("madam")]), s("madam"));
    }
    #[test]
    fn rev_racecar() {
        assert_eq!(ev("reverse", &[s("racecar")]), s("racecar"));
    }
    #[test]
    fn rev_spaces() {
        assert_eq!(ev("reverse", &[s("a b")]), s("b a"));
    }
    #[test]
    fn rev_xyz() {
        assert_eq!(ev("reverse", &[s("xyz")]), s("zyx"));
    }
    #[test]
    fn rev_aabb() {
        assert_eq!(ev("reverse", &[s("aabb")]), s("bbaa"));
    }
    #[test]
    fn rev_abba() {
        assert_eq!(ev("reverse", &[s("abba")]), s("abba"));
    }
    #[test]
    fn rev_123() {
        assert_eq!(ev("reverse", &[s("123")]), s("321"));
    }
    #[test]
    fn rev_abcd() {
        assert_eq!(ev("reverse", &[s("abcd")]), s("dcba"));
    }
    #[test]
    fn rev_foobar() {
        assert_eq!(ev("reverse", &[s("foobar")]), s("raboof"));
    }
    #[test]
    fn rev_rust() {
        assert_eq!(ev("reverse", &[s("rust")]), s("tsur"));
    }
    #[test]
    fn rev_test() {
        assert_eq!(ev("reverse", &[s("test")]), s("tset"));
    }
    #[test]
    fn rev_world() {
        assert_eq!(ev("reverse", &[s("world")]), s("dlrow"));
    }
    #[test]
    fn rev_code() {
        assert_eq!(ev("reverse", &[s("code")]), s("edoc"));
    }
    #[test]
    fn rev_null() {
        assert_eq!(ev("reverse", &[Value::Null]), Value::Null);
    }
    #[test]
    fn rev_space() {
        assert_eq!(ev("reverse", &[s(" ")]), s(" "));
    }
    #[test]
    fn rev_tab() {
        assert_eq!(ev("reverse", &[s("\t")]), s("\t"));
    }
    #[test]
    fn rev_special() {
        assert_eq!(ev("reverse", &[s("!@#")]), s("#@!"));
    }
    #[test]
    fn rev_punct() {
        assert_eq!(ev("reverse", &[s(".,;")]), s(";,."));
    }
    #[test]
    fn rev_int() {
        assert_eq!(ev("reverse", &[i(123)]), s("321"));
    }
    #[test]
    fn rev_mn() {
        assert_eq!(ev("reverse", &[s("mn")]), s("nm"));
    }
    #[test]
    fn rev_op() {
        assert_eq!(ev("reverse", &[s("op")]), s("po"));
    }
    #[test]
    fn rev_qr() {
        assert_eq!(ev("reverse", &[s("qr")]), s("rq"));
    }
    #[test]
    fn rev_st() {
        assert_eq!(ev("reverse", &[s("st")]), s("ts"));
    }
    #[test]
    fn rev_uv() {
        assert_eq!(ev("reverse", &[s("uv")]), s("vu"));
    }
    #[test]
    fn rev_wx() {
        assert_eq!(ev("reverse", &[s("wx")]), s("xw"));
    }
    #[test]
    fn rev_yz() {
        assert_eq!(ev("reverse", &[s("yz")]), s("zy"));
    }
    #[test]
    fn rev_ab2() {
        assert_eq!(ev("reverse", &[s("AB")]), s("BA"));
    }
    #[test]
    fn rev_cd2() {
        assert_eq!(ev("reverse", &[s("CD")]), s("DC"));
    }
    #[test]
    fn rev_ef2() {
        assert_eq!(ev("reverse", &[s("EF")]), s("FE"));
    }
    #[test]
    fn rev_gh2() {
        assert_eq!(ev("reverse", &[s("GH")]), s("HG"));
    }
    #[test]
    fn rev_ij() {
        assert_eq!(ev("reverse", &[s("IJ")]), s("JI"));
    }
    #[test]
    fn rev_kl() {
        assert_eq!(ev("reverse", &[s("KL")]), s("LK"));
    }
    #[test]
    fn rev_mn2() {
        assert_eq!(ev("reverse", &[s("MN")]), s("NM"));
    }
    #[test]
    fn rev_op2() {
        assert_eq!(ev("reverse", &[s("OP")]), s("PO"));
    }
    #[test]
    fn rev_qr2() {
        assert_eq!(ev("reverse", &[s("QR")]), s("RQ"));
    }
    #[test]
    fn rev_st2() {
        assert_eq!(ev("reverse", &[s("ST")]), s("TS"));
    }
    #[test]
    fn rev_uv2() {
        assert_eq!(ev("reverse", &[s("UV")]), s("VU"));
    }
    #[test]
    fn rev_wx2() {
        assert_eq!(ev("reverse", &[s("WX")]), s("XW"));
    }
    #[test]
    fn rev_yz2() {
        assert_eq!(ev("reverse", &[s("YZ")]), s("ZY"));
    }
    #[test]
    fn rev_abc2() {
        assert_eq!(ev("reverse", &[s("ABC")]), s("CBA"));
    }
    #[test]
    fn rev_def() {
        assert_eq!(ev("reverse", &[s("DEF")]), s("FED"));
    }
    #[test]
    fn rev_ghi() {
        assert_eq!(ev("reverse", &[s("GHI")]), s("IHG"));
    }
    #[test]
    fn rev_jkl() {
        assert_eq!(ev("reverse", &[s("JKL")]), s("LKJ"));
    }
}

// ===== repeat (50) =====
mod repeat_tests {
    use super::*;
    #[test]
    fn rep_ab_3() {
        assert_eq!(ev("repeat", &[s("ab"), i(3)]), s("ababab"));
    }
    #[test]
    fn rep_a_1() {
        assert_eq!(ev("repeat", &[s("a"), i(1)]), s("a"));
    }
    #[test]
    fn rep_a_2() {
        assert_eq!(ev("repeat", &[s("a"), i(2)]), s("aa"));
    }
    #[test]
    fn rep_a_3() {
        assert_eq!(ev("repeat", &[s("a"), i(3)]), s("aaa"));
    }
    #[test]
    fn rep_a_5() {
        assert_eq!(ev("repeat", &[s("a"), i(5)]), s("aaaaa"));
    }
    #[test]
    fn rep_a_10() {
        assert_eq!(ev("repeat", &[s("a"), i(10)]), s("aaaaaaaaaa"));
    }
    #[test]
    fn rep_x_0() {
        assert_eq!(ev("repeat", &[s("x"), i(0)]), s(""));
    }
    #[test]
    fn rep_xy_2() {
        assert_eq!(ev("repeat", &[s("xy"), i(2)]), s("xyxy"));
    }
    #[test]
    fn rep_abc_2() {
        assert_eq!(ev("repeat", &[s("abc"), i(2)]), s("abcabc"));
    }
    #[test]
    fn rep_hi_4() {
        assert_eq!(ev("repeat", &[s("hi"), i(4)]), s("hihihihi"));
    }
    #[test]
    fn rep_space_3() {
        assert_eq!(ev("repeat", &[s(" "), i(3)]), s("   "));
    }
    #[test]
    fn rep_dot_5() {
        assert_eq!(ev("repeat", &[s("."), i(5)]), s("....."));
    }
    #[test]
    fn rep_dash_4() {
        assert_eq!(ev("repeat", &[s("-"), i(4)]), s("----"));
    }
    #[test]
    fn rep_star_3() {
        assert_eq!(ev("repeat", &[s("*"), i(3)]), s("***"));
    }
    #[test]
    fn rep_hash_2() {
        assert_eq!(ev("repeat", &[s("#"), i(2)]), s("##"));
    }
    #[test]
    fn rep_eq_6() {
        assert_eq!(ev("repeat", &[s("="), i(6)]), s("======"));
    }
    #[test]
    fn rep_empty_5() {
        assert_eq!(ev("repeat", &[s(""), i(5)]), s(""));
    }
    #[test]
    fn rep_b_1() {
        assert_eq!(ev("repeat", &[s("b"), i(1)]), s("b"));
    }
    #[test]
    fn rep_b_4() {
        assert_eq!(ev("repeat", &[s("b"), i(4)]), s("bbbb"));
    }
    #[test]
    fn rep_c_3() {
        assert_eq!(ev("repeat", &[s("c"), i(3)]), s("ccc"));
    }
    #[test]
    fn rep_d_2() {
        assert_eq!(ev("repeat", &[s("d"), i(2)]), s("dd"));
    }
    #[test]
    fn rep_e_5() {
        assert_eq!(ev("repeat", &[s("e"), i(5)]), s("eeeee"));
    }
    #[test]
    fn rep_f_6() {
        assert_eq!(ev("repeat", &[s("f"), i(6)]), s("ffffff"));
    }
    #[test]
    fn rep_g_7() {
        assert_eq!(ev("repeat", &[s("g"), i(7)]), s("ggggggg"));
    }
    #[test]
    fn rep_h_8() {
        assert_eq!(ev("repeat", &[s("h"), i(8)]), s("hhhhhhhh"));
    }
    #[test]
    fn rep_i_9() {
        assert_eq!(ev("repeat", &[s("i"), i(9)]), s("iiiiiiiii"));
    }
    #[test]
    fn rep_j_10() {
        assert_eq!(ev("repeat", &[s("j"), i(10)]), s("jjjjjjjjjj"));
    }
    #[test]
    fn rep_k_1() {
        assert_eq!(ev("repeat", &[s("k"), i(1)]), s("k"));
    }
    #[test]
    fn rep_l_2() {
        assert_eq!(ev("repeat", &[s("l"), i(2)]), s("ll"));
    }
    #[test]
    fn rep_m_3() {
        assert_eq!(ev("repeat", &[s("m"), i(3)]), s("mmm"));
    }
    #[test]
    fn rep_n_4() {
        assert_eq!(ev("repeat", &[s("n"), i(4)]), s("nnnn"));
    }
    #[test]
    fn rep_o_5() {
        assert_eq!(ev("repeat", &[s("o"), i(5)]), s("ooooo"));
    }
    #[test]
    fn rep_p_6() {
        assert_eq!(ev("repeat", &[s("p"), i(6)]), s("pppppp"));
    }
    #[test]
    fn rep_q_7() {
        assert_eq!(ev("repeat", &[s("q"), i(7)]), s("qqqqqqq"));
    }
    #[test]
    fn rep_r_8() {
        assert_eq!(ev("repeat", &[s("r"), i(8)]), s("rrrrrrrr"));
    }
    #[test]
    fn rep_s_9() {
        assert_eq!(ev("repeat", &[s("s"), i(9)]), s("sssssssss"));
    }
    #[test]
    fn rep_t_10() {
        assert_eq!(ev("repeat", &[s("t"), i(10)]), s("tttttttttt"));
    }
    #[test]
    fn rep_null() {
        assert_eq!(ev("repeat", &[Value::Null, i(3)]), Value::Null);
    }
    #[test]
    fn rep_ab_1() {
        assert_eq!(ev("repeat", &[s("ab"), i(1)]), s("ab"));
    }
    #[test]
    fn rep_ab_0() {
        assert_eq!(ev("repeat", &[s("ab"), i(0)]), s(""));
    }
    #[test]
    fn rep_cd_3() {
        assert_eq!(ev("repeat", &[s("cd"), i(3)]), s("cdcdcd"));
    }
    #[test]
    fn rep_ef_2() {
        assert_eq!(ev("repeat", &[s("ef"), i(2)]), s("efef"));
    }
    #[test]
    fn rep_gh_2() {
        assert_eq!(ev("repeat", &[s("gh"), i(2)]), s("ghgh"));
    }
    #[test]
    fn rep_ij_2() {
        assert_eq!(ev("repeat", &[s("ij"), i(2)]), s("ijij"));
    }
    #[test]
    fn rep_kl_2() {
        assert_eq!(ev("repeat", &[s("kl"), i(2)]), s("klkl"));
    }
    #[test]
    fn rep_mn_2() {
        assert_eq!(ev("repeat", &[s("mn"), i(2)]), s("mnmn"));
    }
    #[test]
    fn rep_op_2() {
        assert_eq!(ev("repeat", &[s("op"), i(2)]), s("opop"));
    }
    #[test]
    fn rep_qr_2() {
        assert_eq!(ev("repeat", &[s("qr"), i(2)]), s("qrqr"));
    }
    #[test]
    fn rep_st_2() {
        assert_eq!(ev("repeat", &[s("st"), i(2)]), s("stst"));
    }
    #[test]
    fn rep_uv_2() {
        assert_eq!(ev("repeat", &[s("uv"), i(2)]), s("uvuv"));
    }
}

// ===== left (50) =====
mod left_tests {
    use super::*;
    #[test]
    fn l_3() {
        assert_eq!(ev("left", &[s("hello"), i(3)]), s("hel"));
    }
    #[test]
    fn l_0() {
        assert_eq!(ev("left", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn l_1() {
        assert_eq!(ev("left", &[s("hello"), i(1)]), s("h"));
    }
    #[test]
    fn l_2() {
        assert_eq!(ev("left", &[s("hello"), i(2)]), s("he"));
    }
    #[test]
    fn l_4() {
        assert_eq!(ev("left", &[s("hello"), i(4)]), s("hell"));
    }
    #[test]
    fn l_5() {
        assert_eq!(ev("left", &[s("hello"), i(5)]), s("hello"));
    }
    #[test]
    fn l_10() {
        assert_eq!(ev("left", &[s("hello"), i(10)]), s("hello"));
    }
    #[test]
    fn l_empty() {
        assert_eq!(ev("left", &[s(""), i(5)]), s(""));
    }
    #[test]
    fn l_a_1() {
        assert_eq!(ev("left", &[s("a"), i(1)]), s("a"));
    }
    #[test]
    fn l_ab_1() {
        assert_eq!(ev("left", &[s("ab"), i(1)]), s("a"));
    }
    #[test]
    fn l_ab_2() {
        assert_eq!(ev("left", &[s("ab"), i(2)]), s("ab"));
    }
    #[test]
    fn l_abc_1() {
        assert_eq!(ev("left", &[s("abc"), i(1)]), s("a"));
    }
    #[test]
    fn l_abc_2() {
        assert_eq!(ev("left", &[s("abc"), i(2)]), s("ab"));
    }
    #[test]
    fn l_abc_3() {
        assert_eq!(ev("left", &[s("abc"), i(3)]), s("abc"));
    }
    #[test]
    fn l_foobar_3() {
        assert_eq!(ev("left", &[s("foobar"), i(3)]), s("foo"));
    }
    #[test]
    fn l_foobar_6() {
        assert_eq!(ev("left", &[s("foobar"), i(6)]), s("foobar"));
    }
    #[test]
    fn l_12345_1() {
        assert_eq!(ev("left", &[s("12345"), i(1)]), s("1"));
    }
    #[test]
    fn l_12345_3() {
        assert_eq!(ev("left", &[s("12345"), i(3)]), s("123"));
    }
    #[test]
    fn l_12345_5() {
        assert_eq!(ev("left", &[s("12345"), i(5)]), s("12345"));
    }
    #[test]
    fn l_null() {
        assert_eq!(ev("left", &[Value::Null, i(3)]), Value::Null);
    }
    #[test]
    fn l_world_1() {
        assert_eq!(ev("left", &[s("world"), i(1)]), s("w"));
    }
    #[test]
    fn l_world_2() {
        assert_eq!(ev("left", &[s("world"), i(2)]), s("wo"));
    }
    #[test]
    fn l_world_3() {
        assert_eq!(ev("left", &[s("world"), i(3)]), s("wor"));
    }
    #[test]
    fn l_world_4() {
        assert_eq!(ev("left", &[s("world"), i(4)]), s("worl"));
    }
    #[test]
    fn l_world_5() {
        assert_eq!(ev("left", &[s("world"), i(5)]), s("world"));
    }
    #[test]
    fn l_rust_1() {
        assert_eq!(ev("left", &[s("rust"), i(1)]), s("r"));
    }
    #[test]
    fn l_rust_2() {
        assert_eq!(ev("left", &[s("rust"), i(2)]), s("ru"));
    }
    #[test]
    fn l_rust_3() {
        assert_eq!(ev("left", &[s("rust"), i(3)]), s("rus"));
    }
    #[test]
    fn l_rust_4() {
        assert_eq!(ev("left", &[s("rust"), i(4)]), s("rust"));
    }
    #[test]
    fn l_test_1() {
        assert_eq!(ev("left", &[s("test"), i(1)]), s("t"));
    }
    #[test]
    fn l_test_2() {
        assert_eq!(ev("left", &[s("test"), i(2)]), s("te"));
    }
    #[test]
    fn l_test_3() {
        assert_eq!(ev("left", &[s("test"), i(3)]), s("tes"));
    }
    #[test]
    fn l_test_4() {
        assert_eq!(ev("left", &[s("test"), i(4)]), s("test"));
    }
    #[test]
    fn l_abcdefg_1() {
        assert_eq!(ev("left", &[s("abcdefg"), i(1)]), s("a"));
    }
    #[test]
    fn l_abcdefg_2() {
        assert_eq!(ev("left", &[s("abcdefg"), i(2)]), s("ab"));
    }
    #[test]
    fn l_abcdefg_3() {
        assert_eq!(ev("left", &[s("abcdefg"), i(3)]), s("abc"));
    }
    #[test]
    fn l_abcdefg_4() {
        assert_eq!(ev("left", &[s("abcdefg"), i(4)]), s("abcd"));
    }
    #[test]
    fn l_abcdefg_5() {
        assert_eq!(ev("left", &[s("abcdefg"), i(5)]), s("abcde"));
    }
    #[test]
    fn l_abcdefg_6() {
        assert_eq!(ev("left", &[s("abcdefg"), i(6)]), s("abcdef"));
    }
    #[test]
    fn l_abcdefg_7() {
        assert_eq!(ev("left", &[s("abcdefg"), i(7)]), s("abcdefg"));
    }
    #[test]
    fn l_xyz_1() {
        assert_eq!(ev("left", &[s("xyz"), i(1)]), s("x"));
    }
    #[test]
    fn l_xyz_2() {
        assert_eq!(ev("left", &[s("xyz"), i(2)]), s("xy"));
    }
    #[test]
    fn l_xyz_3() {
        assert_eq!(ev("left", &[s("xyz"), i(3)]), s("xyz"));
    }
    #[test]
    fn l_space_1() {
        assert_eq!(ev("left", &[s(" a"), i(1)]), s(" "));
    }
    #[test]
    fn l_digit_2() {
        assert_eq!(ev("left", &[s("9876"), i(2)]), s("98"));
    }
    #[test]
    fn l_punct_1() {
        assert_eq!(ev("left", &[s("!@#"), i(1)]), s("!"));
    }
    #[test]
    fn l_punct_2() {
        assert_eq!(ev("left", &[s("!@#"), i(2)]), s("!@"));
    }
    #[test]
    fn l_long_3() {
        assert_eq!(ev("left", &[s("abcdefghij"), i(3)]), s("abc"));
    }
    #[test]
    fn l_long_5() {
        assert_eq!(ev("left", &[s("abcdefghij"), i(5)]), s("abcde"));
    }
    #[test]
    fn l_long_10() {
        assert_eq!(ev("left", &[s("abcdefghij"), i(10)]), s("abcdefghij"));
    }
}

// ===== right (50) =====
mod right_tests {
    use super::*;
    #[test]
    fn r_3() {
        assert_eq!(ev("right", &[s("hello"), i(3)]), s("llo"));
    }
    #[test]
    fn r_0() {
        assert_eq!(ev("right", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn r_1() {
        assert_eq!(ev("right", &[s("hello"), i(1)]), s("o"));
    }
    #[test]
    fn r_2() {
        assert_eq!(ev("right", &[s("hello"), i(2)]), s("lo"));
    }
    #[test]
    fn r_4() {
        assert_eq!(ev("right", &[s("hello"), i(4)]), s("ello"));
    }
    #[test]
    fn r_5() {
        assert_eq!(ev("right", &[s("hello"), i(5)]), s("hello"));
    }
    #[test]
    fn r_10() {
        assert_eq!(ev("right", &[s("hello"), i(10)]), s("hello"));
    }
    #[test]
    fn r_empty() {
        assert_eq!(ev("right", &[s(""), i(5)]), s(""));
    }
    #[test]
    fn r_a_1() {
        assert_eq!(ev("right", &[s("a"), i(1)]), s("a"));
    }
    #[test]
    fn r_ab_1() {
        assert_eq!(ev("right", &[s("ab"), i(1)]), s("b"));
    }
    #[test]
    fn r_ab_2() {
        assert_eq!(ev("right", &[s("ab"), i(2)]), s("ab"));
    }
    #[test]
    fn r_abc_1() {
        assert_eq!(ev("right", &[s("abc"), i(1)]), s("c"));
    }
    #[test]
    fn r_abc_2() {
        assert_eq!(ev("right", &[s("abc"), i(2)]), s("bc"));
    }
    #[test]
    fn r_abc_3() {
        assert_eq!(ev("right", &[s("abc"), i(3)]), s("abc"));
    }
    #[test]
    fn r_foobar_3() {
        assert_eq!(ev("right", &[s("foobar"), i(3)]), s("bar"));
    }
    #[test]
    fn r_foobar_6() {
        assert_eq!(ev("right", &[s("foobar"), i(6)]), s("foobar"));
    }
    #[test]
    fn r_12345_1() {
        assert_eq!(ev("right", &[s("12345"), i(1)]), s("5"));
    }
    #[test]
    fn r_12345_3() {
        assert_eq!(ev("right", &[s("12345"), i(3)]), s("345"));
    }
    #[test]
    fn r_12345_5() {
        assert_eq!(ev("right", &[s("12345"), i(5)]), s("12345"));
    }
    #[test]
    fn r_null() {
        assert_eq!(ev("right", &[Value::Null, i(3)]), Value::Null);
    }
    #[test]
    fn r_world_1() {
        assert_eq!(ev("right", &[s("world"), i(1)]), s("d"));
    }
    #[test]
    fn r_world_2() {
        assert_eq!(ev("right", &[s("world"), i(2)]), s("ld"));
    }
    #[test]
    fn r_world_3() {
        assert_eq!(ev("right", &[s("world"), i(3)]), s("rld"));
    }
    #[test]
    fn r_world_4() {
        assert_eq!(ev("right", &[s("world"), i(4)]), s("orld"));
    }
    #[test]
    fn r_world_5() {
        assert_eq!(ev("right", &[s("world"), i(5)]), s("world"));
    }
    #[test]
    fn r_rust_1() {
        assert_eq!(ev("right", &[s("rust"), i(1)]), s("t"));
    }
    #[test]
    fn r_rust_2() {
        assert_eq!(ev("right", &[s("rust"), i(2)]), s("st"));
    }
    #[test]
    fn r_rust_3() {
        assert_eq!(ev("right", &[s("rust"), i(3)]), s("ust"));
    }
    #[test]
    fn r_rust_4() {
        assert_eq!(ev("right", &[s("rust"), i(4)]), s("rust"));
    }
    #[test]
    fn r_test_1() {
        assert_eq!(ev("right", &[s("test"), i(1)]), s("t"));
    }
    #[test]
    fn r_test_2() {
        assert_eq!(ev("right", &[s("test"), i(2)]), s("st"));
    }
    #[test]
    fn r_test_3() {
        assert_eq!(ev("right", &[s("test"), i(3)]), s("est"));
    }
    #[test]
    fn r_test_4() {
        assert_eq!(ev("right", &[s("test"), i(4)]), s("test"));
    }
    #[test]
    fn r_abcdefg_1() {
        assert_eq!(ev("right", &[s("abcdefg"), i(1)]), s("g"));
    }
    #[test]
    fn r_abcdefg_2() {
        assert_eq!(ev("right", &[s("abcdefg"), i(2)]), s("fg"));
    }
    #[test]
    fn r_abcdefg_3() {
        assert_eq!(ev("right", &[s("abcdefg"), i(3)]), s("efg"));
    }
    #[test]
    fn r_abcdefg_4() {
        assert_eq!(ev("right", &[s("abcdefg"), i(4)]), s("defg"));
    }
    #[test]
    fn r_abcdefg_5() {
        assert_eq!(ev("right", &[s("abcdefg"), i(5)]), s("cdefg"));
    }
    #[test]
    fn r_abcdefg_6() {
        assert_eq!(ev("right", &[s("abcdefg"), i(6)]), s("bcdefg"));
    }
    #[test]
    fn r_abcdefg_7() {
        assert_eq!(ev("right", &[s("abcdefg"), i(7)]), s("abcdefg"));
    }
    #[test]
    fn r_xyz_1() {
        assert_eq!(ev("right", &[s("xyz"), i(1)]), s("z"));
    }
    #[test]
    fn r_xyz_2() {
        assert_eq!(ev("right", &[s("xyz"), i(2)]), s("yz"));
    }
    #[test]
    fn r_xyz_3() {
        assert_eq!(ev("right", &[s("xyz"), i(3)]), s("xyz"));
    }
    #[test]
    fn r_space_1() {
        assert_eq!(ev("right", &[s("a "), i(1)]), s(" "));
    }
    #[test]
    fn r_digit_2() {
        assert_eq!(ev("right", &[s("9876"), i(2)]), s("76"));
    }
    #[test]
    fn r_punct_1() {
        assert_eq!(ev("right", &[s("!@#"), i(1)]), s("#"));
    }
    #[test]
    fn r_punct_2() {
        assert_eq!(ev("right", &[s("!@#"), i(2)]), s("@#"));
    }
    #[test]
    fn r_long_3() {
        assert_eq!(ev("right", &[s("abcdefghij"), i(3)]), s("hij"));
    }
    #[test]
    fn r_long_5() {
        assert_eq!(ev("right", &[s("abcdefghij"), i(5)]), s("fghij"));
    }
    #[test]
    fn r_long_10() {
        assert_eq!(ev("right", &[s("abcdefghij"), i(10)]), s("abcdefghij"));
    }
}

// ===== starts_with (50) =====
mod starts_with_tests {
    use super::*;
    #[test]
    fn sw_yes() {
        assert_eq!(ev("starts_with", &[s("hello"), s("hel")]), i(1));
    }
    #[test]
    fn sw_no() {
        assert_eq!(ev("starts_with", &[s("hello"), s("xyz")]), i(0));
    }
    #[test]
    fn sw_empty_prefix() {
        assert_eq!(ev("starts_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn sw_empty_str() {
        assert_eq!(ev("starts_with", &[s(""), s("a")]), i(0));
    }
    #[test]
    fn sw_both_empty() {
        assert_eq!(ev("starts_with", &[s(""), s("")]), i(1));
    }
    #[test]
    fn sw_exact() {
        assert_eq!(ev("starts_with", &[s("hello"), s("hello")]), i(1));
    }
    #[test]
    fn sw_longer() {
        assert_eq!(ev("starts_with", &[s("hi"), s("hello")]), i(0));
    }
    #[test]
    fn sw_h() {
        assert_eq!(ev("starts_with", &[s("hello"), s("h")]), i(1));
    }
    #[test]
    fn sw_he() {
        assert_eq!(ev("starts_with", &[s("hello"), s("he")]), i(1));
    }
    #[test]
    fn sw_hell() {
        assert_eq!(ev("starts_with", &[s("hello"), s("hell")]), i(1));
    }
    #[test]
    fn sw_x() {
        assert_eq!(ev("starts_with", &[s("hello"), s("x")]), i(0));
    }
    #[test]
    fn sw_e() {
        assert_eq!(ev("starts_with", &[s("hello"), s("e")]), i(0));
    }
    #[test]
    fn sw_o() {
        assert_eq!(ev("starts_with", &[s("hello"), s("o")]), i(0));
    }
    #[test]
    fn sw_case() {
        assert_eq!(ev("starts_with", &[s("Hello"), s("h")]), i(0));
    }
    #[test]
    fn sw_case2() {
        assert_eq!(ev("starts_with", &[s("Hello"), s("H")]), i(1));
    }
    #[test]
    fn sw_num() {
        assert_eq!(ev("starts_with", &[s("123"), s("1")]), i(1));
    }
    #[test]
    fn sw_num2() {
        assert_eq!(ev("starts_with", &[s("123"), s("12")]), i(1));
    }
    #[test]
    fn sw_num3() {
        assert_eq!(ev("starts_with", &[s("123"), s("123")]), i(1));
    }
    #[test]
    fn sw_num4() {
        assert_eq!(ev("starts_with", &[s("123"), s("2")]), i(0));
    }
    #[test]
    fn sw_space() {
        assert_eq!(ev("starts_with", &[s(" hi"), s(" ")]), i(1));
    }
    #[test]
    fn sw_abc_a() {
        assert_eq!(ev("starts_with", &[s("abc"), s("a")]), i(1));
    }
    #[test]
    fn sw_abc_ab() {
        assert_eq!(ev("starts_with", &[s("abc"), s("ab")]), i(1));
    }
    #[test]
    fn sw_abc_abc() {
        assert_eq!(ev("starts_with", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn sw_abc_b() {
        assert_eq!(ev("starts_with", &[s("abc"), s("b")]), i(0));
    }
    #[test]
    fn sw_abc_c() {
        assert_eq!(ev("starts_with", &[s("abc"), s("c")]), i(0));
    }
    #[test]
    fn sw_foo_f() {
        assert_eq!(ev("starts_with", &[s("foobar"), s("f")]), i(1));
    }
    #[test]
    fn sw_foo_fo() {
        assert_eq!(ev("starts_with", &[s("foobar"), s("fo")]), i(1));
    }
    #[test]
    fn sw_foo_foo() {
        assert_eq!(ev("starts_with", &[s("foobar"), s("foo")]), i(1));
    }
    #[test]
    fn sw_foo_bar() {
        assert_eq!(ev("starts_with", &[s("foobar"), s("bar")]), i(0));
    }
    #[test]
    fn sw_xyz_x() {
        assert_eq!(ev("starts_with", &[s("xyz"), s("x")]), i(1));
    }
    #[test]
    fn sw_xyz_xy() {
        assert_eq!(ev("starts_with", &[s("xyz"), s("xy")]), i(1));
    }
    #[test]
    fn sw_xyz_xyz() {
        assert_eq!(ev("starts_with", &[s("xyz"), s("xyz")]), i(1));
    }
    #[test]
    fn sw_xyz_y() {
        assert_eq!(ev("starts_with", &[s("xyz"), s("y")]), i(0));
    }
    #[test]
    fn sw_xyz_z() {
        assert_eq!(ev("starts_with", &[s("xyz"), s("z")]), i(0));
    }
    #[test]
    fn sw_null() {
        assert_eq!(ev("starts_with", &[Value::Null, s("a")]), Value::Null);
    }
    #[test]
    fn sw_world_w() {
        assert_eq!(ev("starts_with", &[s("world"), s("w")]), i(1));
    }
    #[test]
    fn sw_world_wo() {
        assert_eq!(ev("starts_with", &[s("world"), s("wo")]), i(1));
    }
    #[test]
    fn sw_world_wor() {
        assert_eq!(ev("starts_with", &[s("world"), s("wor")]), i(1));
    }
    #[test]
    fn sw_world_worl() {
        assert_eq!(ev("starts_with", &[s("world"), s("worl")]), i(1));
    }
    #[test]
    fn sw_world_world() {
        assert_eq!(ev("starts_with", &[s("world"), s("world")]), i(1));
    }
    #[test]
    fn sw_world_d() {
        assert_eq!(ev("starts_with", &[s("world"), s("d")]), i(0));
    }
    #[test]
    fn sw_rust_r() {
        assert_eq!(ev("starts_with", &[s("rust"), s("r")]), i(1));
    }
    #[test]
    fn sw_rust_ru() {
        assert_eq!(ev("starts_with", &[s("rust"), s("ru")]), i(1));
    }
    #[test]
    fn sw_rust_rus() {
        assert_eq!(ev("starts_with", &[s("rust"), s("rus")]), i(1));
    }
    #[test]
    fn sw_rust_rust() {
        assert_eq!(ev("starts_with", &[s("rust"), s("rust")]), i(1));
    }
    #[test]
    fn sw_rust_s() {
        assert_eq!(ev("starts_with", &[s("rust"), s("s")]), i(0));
    }
    #[test]
    fn sw_test_t() {
        assert_eq!(ev("starts_with", &[s("test"), s("t")]), i(1));
    }
    #[test]
    fn sw_test_te() {
        assert_eq!(ev("starts_with", &[s("test"), s("te")]), i(1));
    }
    #[test]
    fn sw_test_tes() {
        assert_eq!(ev("starts_with", &[s("test"), s("tes")]), i(1));
    }
    #[test]
    fn sw_test_test() {
        assert_eq!(ev("starts_with", &[s("test"), s("test")]), i(1));
    }
}

// ===== ends_with (50) =====
mod ends_with_tests {
    use super::*;
    #[test]
    fn ew_yes() {
        assert_eq!(ev("ends_with", &[s("hello"), s("llo")]), i(1));
    }
    #[test]
    fn ew_no() {
        assert_eq!(ev("ends_with", &[s("hello"), s("xyz")]), i(0));
    }
    #[test]
    fn ew_empty() {
        assert_eq!(ev("ends_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn ew_empty_str() {
        assert_eq!(ev("ends_with", &[s(""), s("a")]), i(0));
    }
    #[test]
    fn ew_both_empty() {
        assert_eq!(ev("ends_with", &[s(""), s("")]), i(1));
    }
    #[test]
    fn ew_exact() {
        assert_eq!(ev("ends_with", &[s("hello"), s("hello")]), i(1));
    }
    #[test]
    fn ew_longer() {
        assert_eq!(ev("ends_with", &[s("lo"), s("hello")]), i(0));
    }
    #[test]
    fn ew_o() {
        assert_eq!(ev("ends_with", &[s("hello"), s("o")]), i(1));
    }
    #[test]
    fn ew_lo() {
        assert_eq!(ev("ends_with", &[s("hello"), s("lo")]), i(1));
    }
    #[test]
    fn ew_ello() {
        assert_eq!(ev("ends_with", &[s("hello"), s("ello")]), i(1));
    }
    #[test]
    fn ew_h() {
        assert_eq!(ev("ends_with", &[s("hello"), s("h")]), i(0));
    }
    #[test]
    fn ew_he() {
        assert_eq!(ev("ends_with", &[s("hello"), s("he")]), i(0));
    }
    #[test]
    fn ew_x() {
        assert_eq!(ev("ends_with", &[s("hello"), s("x")]), i(0));
    }
    #[test]
    fn ew_num() {
        assert_eq!(ev("ends_with", &[s("123"), s("3")]), i(1));
    }
    #[test]
    fn ew_num2() {
        assert_eq!(ev("ends_with", &[s("123"), s("23")]), i(1));
    }
    #[test]
    fn ew_num3() {
        assert_eq!(ev("ends_with", &[s("123"), s("123")]), i(1));
    }
    #[test]
    fn ew_num4() {
        assert_eq!(ev("ends_with", &[s("123"), s("1")]), i(0));
    }
    #[test]
    fn ew_space() {
        assert_eq!(ev("ends_with", &[s("hi "), s(" ")]), i(1));
    }
    #[test]
    fn ew_abc_c() {
        assert_eq!(ev("ends_with", &[s("abc"), s("c")]), i(1));
    }
    #[test]
    fn ew_abc_bc() {
        assert_eq!(ev("ends_with", &[s("abc"), s("bc")]), i(1));
    }
    #[test]
    fn ew_abc_abc() {
        assert_eq!(ev("ends_with", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn ew_abc_a() {
        assert_eq!(ev("ends_with", &[s("abc"), s("a")]), i(0));
    }
    #[test]
    fn ew_abc_b() {
        assert_eq!(ev("ends_with", &[s("abc"), s("b")]), i(0));
    }
    #[test]
    fn ew_foobar_r() {
        assert_eq!(ev("ends_with", &[s("foobar"), s("r")]), i(1));
    }
    #[test]
    fn ew_foobar_ar() {
        assert_eq!(ev("ends_with", &[s("foobar"), s("ar")]), i(1));
    }
    #[test]
    fn ew_foobar_bar() {
        assert_eq!(ev("ends_with", &[s("foobar"), s("bar")]), i(1));
    }
    #[test]
    fn ew_foobar_foo() {
        assert_eq!(ev("ends_with", &[s("foobar"), s("foo")]), i(0));
    }
    #[test]
    fn ew_xyz_z() {
        assert_eq!(ev("ends_with", &[s("xyz"), s("z")]), i(1));
    }
    #[test]
    fn ew_xyz_yz() {
        assert_eq!(ev("ends_with", &[s("xyz"), s("yz")]), i(1));
    }
    #[test]
    fn ew_xyz_xyz() {
        assert_eq!(ev("ends_with", &[s("xyz"), s("xyz")]), i(1));
    }
    #[test]
    fn ew_xyz_x() {
        assert_eq!(ev("ends_with", &[s("xyz"), s("x")]), i(0));
    }
    #[test]
    fn ew_null() {
        assert_eq!(ev("ends_with", &[Value::Null, s("a")]), Value::Null);
    }
    #[test]
    fn ew_world_d() {
        assert_eq!(ev("ends_with", &[s("world"), s("d")]), i(1));
    }
    #[test]
    fn ew_world_ld() {
        assert_eq!(ev("ends_with", &[s("world"), s("ld")]), i(1));
    }
    #[test]
    fn ew_world_rld() {
        assert_eq!(ev("ends_with", &[s("world"), s("rld")]), i(1));
    }
    #[test]
    fn ew_world_orld() {
        assert_eq!(ev("ends_with", &[s("world"), s("orld")]), i(1));
    }
    #[test]
    fn ew_world_world() {
        assert_eq!(ev("ends_with", &[s("world"), s("world")]), i(1));
    }
    #[test]
    fn ew_world_w() {
        assert_eq!(ev("ends_with", &[s("world"), s("w")]), i(0));
    }
    #[test]
    fn ew_rust_t() {
        assert_eq!(ev("ends_with", &[s("rust"), s("t")]), i(1));
    }
    #[test]
    fn ew_rust_st() {
        assert_eq!(ev("ends_with", &[s("rust"), s("st")]), i(1));
    }
    #[test]
    fn ew_rust_ust() {
        assert_eq!(ev("ends_with", &[s("rust"), s("ust")]), i(1));
    }
    #[test]
    fn ew_rust_rust() {
        assert_eq!(ev("ends_with", &[s("rust"), s("rust")]), i(1));
    }
    #[test]
    fn ew_rust_r() {
        assert_eq!(ev("ends_with", &[s("rust"), s("r")]), i(0));
    }
    #[test]
    fn ew_test_t() {
        assert_eq!(ev("ends_with", &[s("test"), s("t")]), i(1));
    }
    #[test]
    fn ew_test_st() {
        assert_eq!(ev("ends_with", &[s("test"), s("st")]), i(1));
    }
    #[test]
    fn ew_test_est() {
        assert_eq!(ev("ends_with", &[s("test"), s("est")]), i(1));
    }
    #[test]
    fn ew_test_test() {
        assert_eq!(ev("ends_with", &[s("test"), s("test")]), i(1));
    }
    #[test]
    fn ew_test_e() {
        assert_eq!(ev("ends_with", &[s("test"), s("e")]), i(0));
    }
    #[test]
    fn ew_case() {
        assert_eq!(ev("ends_with", &[s("Hello"), s("O")]), i(0));
    }
    #[test]
    fn ew_case2() {
        assert_eq!(ev("ends_with", &[s("Hello"), s("o")]), i(1));
    }
}

// ===== contains (50) =====
mod contains_tests {
    use super::*;
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
    fn ct_empty_str() {
        assert_eq!(ev("contains", &[s(""), s("a")]), i(0));
    }
    #[test]
    fn ct_both_empty() {
        assert_eq!(ev("contains", &[s(""), s("")]), i(1));
    }
    #[test]
    fn ct_exact() {
        assert_eq!(ev("contains", &[s("hello"), s("hello")]), i(1));
    }
    #[test]
    fn ct_start() {
        assert_eq!(ev("contains", &[s("hello"), s("hel")]), i(1));
    }
    #[test]
    fn ct_end() {
        assert_eq!(ev("contains", &[s("hello"), s("llo")]), i(1));
    }
    #[test]
    fn ct_mid() {
        assert_eq!(ev("contains", &[s("hello"), s("ll")]), i(1));
    }
    #[test]
    fn ct_h() {
        assert_eq!(ev("contains", &[s("hello"), s("h")]), i(1));
    }
    #[test]
    fn ct_e() {
        assert_eq!(ev("contains", &[s("hello"), s("e")]), i(1));
    }
    #[test]
    fn ct_l() {
        assert_eq!(ev("contains", &[s("hello"), s("l")]), i(1));
    }
    #[test]
    fn ct_o() {
        assert_eq!(ev("contains", &[s("hello"), s("o")]), i(1));
    }
    #[test]
    fn ct_x() {
        assert_eq!(ev("contains", &[s("hello"), s("x")]), i(0));
    }
    #[test]
    fn ct_case() {
        assert_eq!(ev("contains", &[s("Hello"), s("h")]), i(0));
    }
    #[test]
    fn ct_case2() {
        assert_eq!(ev("contains", &[s("Hello"), s("H")]), i(1));
    }
    #[test]
    fn ct_num() {
        assert_eq!(ev("contains", &[s("abc123"), s("123")]), i(1));
    }
    #[test]
    fn ct_num2() {
        assert_eq!(ev("contains", &[s("abc123"), s("abc")]), i(1));
    }
    #[test]
    fn ct_num3() {
        assert_eq!(ev("contains", &[s("abc123"), s("c1")]), i(1));
    }
    #[test]
    fn ct_num4() {
        assert_eq!(ev("contains", &[s("abc123"), s("xyz")]), i(0));
    }
    #[test]
    fn ct_space() {
        assert_eq!(ev("contains", &[s("a b c"), s(" ")]), i(1));
    }
    #[test]
    fn ct_space2() {
        assert_eq!(ev("contains", &[s("a b c"), s("b c")]), i(1));
    }
    #[test]
    fn ct_null() {
        assert_eq!(ev("contains", &[Value::Null, s("a")]), Value::Null);
    }
    #[test]
    fn ct_longer() {
        assert_eq!(ev("contains", &[s("hi"), s("hello")]), i(0));
    }
    #[test]
    fn ct_abc_a() {
        assert_eq!(ev("contains", &[s("abc"), s("a")]), i(1));
    }
    #[test]
    fn ct_abc_b() {
        assert_eq!(ev("contains", &[s("abc"), s("b")]), i(1));
    }
    #[test]
    fn ct_abc_c() {
        assert_eq!(ev("contains", &[s("abc"), s("c")]), i(1));
    }
    #[test]
    fn ct_abc_ab() {
        assert_eq!(ev("contains", &[s("abc"), s("ab")]), i(1));
    }
    #[test]
    fn ct_abc_bc() {
        assert_eq!(ev("contains", &[s("abc"), s("bc")]), i(1));
    }
    #[test]
    fn ct_abc_abc() {
        assert_eq!(ev("contains", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn ct_abc_d() {
        assert_eq!(ev("contains", &[s("abc"), s("d")]), i(0));
    }
    #[test]
    fn ct_foobar_oo() {
        assert_eq!(ev("contains", &[s("foobar"), s("oo")]), i(1));
    }
    #[test]
    fn ct_foobar_ob() {
        assert_eq!(ev("contains", &[s("foobar"), s("ob")]), i(1));
    }
    #[test]
    fn ct_foobar_ba() {
        assert_eq!(ev("contains", &[s("foobar"), s("ba")]), i(1));
    }
    #[test]
    fn ct_foobar_oob() {
        assert_eq!(ev("contains", &[s("foobar"), s("oob")]), i(1));
    }
    #[test]
    fn ct_foobar_oba() {
        assert_eq!(ev("contains", &[s("foobar"), s("oba")]), i(1));
    }
    #[test]
    fn ct_foobar_bar() {
        assert_eq!(ev("contains", &[s("foobar"), s("bar")]), i(1));
    }
    #[test]
    fn ct_foobar_foo() {
        assert_eq!(ev("contains", &[s("foobar"), s("foo")]), i(1));
    }
    #[test]
    fn ct_foobar_xyz() {
        assert_eq!(ev("contains", &[s("foobar"), s("xyz")]), i(0));
    }
    #[test]
    fn ct_world_orl() {
        assert_eq!(ev("contains", &[s("world"), s("orl")]), i(1));
    }
    #[test]
    fn ct_world_wor() {
        assert_eq!(ev("contains", &[s("world"), s("wor")]), i(1));
    }
    #[test]
    fn ct_world_rld() {
        assert_eq!(ev("contains", &[s("world"), s("rld")]), i(1));
    }
    #[test]
    fn ct_world_or() {
        assert_eq!(ev("contains", &[s("world"), s("or")]), i(1));
    }
    #[test]
    fn ct_world_rl() {
        assert_eq!(ev("contains", &[s("world"), s("rl")]), i(1));
    }
    #[test]
    fn ct_world_w() {
        assert_eq!(ev("contains", &[s("world"), s("w")]), i(1));
    }
    #[test]
    fn ct_world_d() {
        assert_eq!(ev("contains", &[s("world"), s("d")]), i(1));
    }
    #[test]
    fn ct_world_x() {
        assert_eq!(ev("contains", &[s("world"), s("x")]), i(0));
    }
    #[test]
    fn ct_dot() {
        assert_eq!(ev("contains", &[s("a.b"), s(".")]), i(1));
    }
    #[test]
    fn ct_at() {
        assert_eq!(ev("contains", &[s("a@b"), s("@")]), i(1));
    }
    #[test]
    fn ct_dash() {
        assert_eq!(ev("contains", &[s("a-b"), s("-")]), i(1));
    }
}

// ===== initcap (40) =====
mod initcap_tests {
    use super::*;
    #[test]
    fn ic_hw() {
        assert_eq!(ev("initcap", &[s("hello world")]), s("Hello World"));
    }
    #[test]
    fn ic_hw_foo() {
        assert_eq!(ev("initcap", &[s("hello world foo")]), s("Hello World Foo"));
    }
    #[test]
    fn ic_empty() {
        assert_eq!(ev("initcap", &[s("")]), s(""));
    }
    #[test]
    fn ic_single() {
        assert_eq!(ev("initcap", &[s("a")]), s("A"));
    }
    #[test]
    fn ic_already() {
        assert_eq!(ev("initcap", &[s("Hello")]), s("Hello"));
    }
    #[test]
    fn ic_all_upper() {
        assert_eq!(ev("initcap", &[s("HELLO WORLD")]), s("Hello World"));
    }
    #[test]
    fn ic_all_lower() {
        assert_eq!(ev("initcap", &[s("hello world")]), s("Hello World"));
    }
    #[test]
    fn ic_three() {
        assert_eq!(ev("initcap", &[s("one two three")]), s("One Two Three"));
    }
    #[test]
    fn ic_four() {
        assert_eq!(ev("initcap", &[s("a b c d")]), s("A B C D"));
    }
    #[test]
    fn ic_abc() {
        assert_eq!(ev("initcap", &[s("abc")]), s("Abc"));
    }
    #[test]
    fn ic_xyz() {
        assert_eq!(ev("initcap", &[s("xyz")]), s("Xyz"));
    }
    #[test]
    fn ic_rust() {
        assert_eq!(ev("initcap", &[s("rust lang")]), s("Rust Lang"));
    }
    #[test]
    fn ic_test() {
        assert_eq!(ev("initcap", &[s("test case")]), s("Test Case"));
    }
    #[test]
    fn ic_foo() {
        assert_eq!(ev("initcap", &[s("foo bar baz")]), s("Foo Bar Baz"));
    }
    #[test]
    fn ic_nums() {
        assert_eq!(ev("initcap", &[s("hello 123")]), s("Hello 123"));
    }
    #[test]
    fn ic_single_word() {
        assert_eq!(ev("initcap", &[s("word")]), s("Word"));
    }
    #[test]
    fn ic_two_words() {
        assert_eq!(ev("initcap", &[s("two words")]), s("Two Words"));
    }
    #[test]
    fn ic_upper_first() {
        assert_eq!(ev("initcap", &[s("Hello world")]), s("Hello World"));
    }
    #[test]
    fn ic_upper_all() {
        assert_eq!(ev("initcap", &[s("RUST")]), s("Rust"));
    }
    #[test]
    fn ic_mixed() {
        assert_eq!(ev("initcap", &[s("hELLO wORLD")]), s("Hello World"));
    }
    #[test]
    fn ic_one() {
        assert_eq!(ev("initcap", &[s("one")]), s("One"));
    }
    #[test]
    fn ic_two() {
        assert_eq!(ev("initcap", &[s("two")]), s("Two"));
    }
    #[test]
    fn ic_hi() {
        assert_eq!(ev("initcap", &[s("hi there")]), s("Hi There"));
    }
    #[test]
    fn ic_good() {
        assert_eq!(ev("initcap", &[s("good morning")]), s("Good Morning"));
    }
    #[test]
    fn ic_the() {
        assert_eq!(
            ev("initcap", &[s("the quick brown fox")]),
            s("The Quick Brown Fox")
        );
    }
    #[test]
    fn ic_ab() {
        assert_eq!(ev("initcap", &[s("ab cd ef")]), s("Ab Cd Ef"));
    }
    #[test]
    fn ic_mn() {
        assert_eq!(ev("initcap", &[s("mn op")]), s("Mn Op"));
    }
    #[test]
    fn ic_qr() {
        assert_eq!(ev("initcap", &[s("qr st uv")]), s("Qr St Uv"));
    }
    #[test]
    fn ic_w() {
        assert_eq!(ev("initcap", &[s("w")]), s("W"));
    }
    #[test]
    fn ic_x() {
        assert_eq!(ev("initcap", &[s("x")]), s("X"));
    }
    #[test]
    fn ic_y() {
        assert_eq!(ev("initcap", &[s("y")]), s("Y"));
    }
    #[test]
    fn ic_z() {
        assert_eq!(ev("initcap", &[s("z")]), s("Z"));
    }
    #[test]
    fn ic_null() {
        assert_eq!(ev("initcap", &[Value::Null]), Value::Null);
    }
    #[test]
    fn ic_int() {
        assert_eq!(ev("initcap", &[i(42)]), s("42"));
    }
    #[test]
    fn ic_five_words() {
        assert_eq!(ev("initcap", &[s("a b c d e")]), s("A B C D E"));
    }
    #[test]
    fn ic_long() {
        assert_eq!(
            ev("initcap", &[s("this is a longer sentence")]),
            s("This Is A Longer Sentence")
        );
    }
    #[test]
    fn ic_trail_space() {
        assert_eq!(ev("initcap", &[s("hello ")]), s("Hello "));
    }
    #[test]
    fn ic_lead_space() {
        assert_eq!(ev("initcap", &[s(" hello")]), s(" Hello"));
    }
    #[test]
    fn ic_multi_space() {
        assert_eq!(ev("initcap", &[s("hello  world")]), s("Hello  World"));
    }
    #[test]
    fn ic_digit_start() {
        assert_eq!(ev("initcap", &[s("3abc")]), s("3abc"));
    }
}

// ===== lpad / rpad (50) =====
mod pad_tests {
    use super::*;
    #[test]
    fn lp_basic() {
        assert_eq!(ev("lpad", &[s("hi"), i(5), s("xy")]), s("xyxhi"));
    }
    #[test]
    fn rp_basic() {
        assert_eq!(ev("rpad", &[s("hi"), i(5), s("xy")]), s("hixyx"));
    }
    #[test]
    fn lp_trunc() {
        assert_eq!(ev("lpad", &[s("hello"), i(3), s("x")]), s("hel"));
    }
    #[test]
    fn rp_trunc() {
        assert_eq!(ev("rpad", &[s("hello"), i(3), s("x")]), s("hel"));
    }
    #[test]
    fn lp_exact() {
        assert_eq!(ev("lpad", &[s("abc"), i(3), s("x")]), s("abc"));
    }
    #[test]
    fn rp_exact() {
        assert_eq!(ev("rpad", &[s("abc"), i(3), s("x")]), s("abc"));
    }
    #[test]
    fn lp_spaces() {
        assert_eq!(ev("lpad", &[s("hi"), i(5), s(" ")]), s("   hi"));
    }
    #[test]
    fn rp_spaces() {
        assert_eq!(ev("rpad", &[s("hi"), i(5), s(" ")]), s("hi   "));
    }
    #[test]
    fn lp_zeros() {
        assert_eq!(ev("lpad", &[s("42"), i(5), s("0")]), s("00042"));
    }
    #[test]
    fn rp_zeros() {
        assert_eq!(ev("rpad", &[s("42"), i(5), s("0")]), s("42000"));
    }
    #[test]
    fn lp_single() {
        assert_eq!(ev("lpad", &[s("a"), i(5), s("*")]), s("****a"));
    }
    #[test]
    fn rp_single() {
        assert_eq!(ev("rpad", &[s("a"), i(5), s("*")]), s("a****"));
    }
    #[test]
    fn lp_empty() {
        assert_eq!(ev("lpad", &[s(""), i(3), s("x")]), s("xxx"));
    }
    #[test]
    fn rp_empty() {
        assert_eq!(ev("rpad", &[s(""), i(3), s("x")]), s("xxx"));
    }
    #[test]
    fn lp_1() {
        assert_eq!(ev("lpad", &[s(""), i(1), s("a")]), s("a"));
    }
    #[test]
    fn rp_1() {
        assert_eq!(ev("rpad", &[s(""), i(1), s("a")]), s("a"));
    }
    #[test]
    fn lp_dash() {
        assert_eq!(ev("lpad", &[s("x"), i(4), s("-")]), s("---x"));
    }
    #[test]
    fn rp_dash() {
        assert_eq!(ev("rpad", &[s("x"), i(4), s("-")]), s("x---"));
    }
    #[test]
    fn lp_dot() {
        assert_eq!(ev("lpad", &[s("x"), i(4), s(".")]), s("...x"));
    }
    #[test]
    fn rp_dot() {
        assert_eq!(ev("rpad", &[s("x"), i(4), s(".")]), s("x..."));
    }
    #[test]
    fn lp_hash() {
        assert_eq!(ev("lpad", &[s("x"), i(3), s("#")]), s("##x"));
    }
    #[test]
    fn rp_hash() {
        assert_eq!(ev("rpad", &[s("x"), i(3), s("#")]), s("x##"));
    }
    #[test]
    fn lp_ab_8() {
        assert_eq!(ev("lpad", &[s("hi"), i(8), s("ab")]), s("abababhi"));
    }
    #[test]
    fn rp_ab_8() {
        assert_eq!(ev("rpad", &[s("hi"), i(8), s("ab")]), s("hiababab"));
    }
    #[test]
    fn lp_null() {
        assert_eq!(ev("lpad", &[Value::Null, i(5), s("x")]), Value::Null);
    }
    #[test]
    fn rp_null() {
        assert_eq!(ev("rpad", &[Value::Null, i(5), s("x")]), Value::Null);
    }
    #[test]
    fn lp_len0() {
        assert_eq!(ev("lpad", &[s("abc"), i(0), s("x")]), s(""));
    }
    #[test]
    fn rp_len0() {
        assert_eq!(ev("rpad", &[s("abc"), i(0), s("x")]), s(""));
    }
    #[test]
    fn lp_len1() {
        assert_eq!(ev("lpad", &[s("abc"), i(1), s("x")]), s("a"));
    }
    #[test]
    fn rp_len1() {
        assert_eq!(ev("rpad", &[s("abc"), i(1), s("x")]), s("a"));
    }
    #[test]
    fn lp_len2() {
        assert_eq!(ev("lpad", &[s("abc"), i(2), s("x")]), s("ab"));
    }
    #[test]
    fn rp_len2() {
        assert_eq!(ev("rpad", &[s("abc"), i(2), s("x")]), s("ab"));
    }
    #[test]
    fn lp_10() {
        assert_eq!(ev("lpad", &[s("x"), i(10), s("0")]), s("000000000x"));
    }
    #[test]
    fn rp_10() {
        assert_eq!(ev("rpad", &[s("x"), i(10), s("0")]), s("x000000000"));
    }
    #[test]
    fn lp_eq_pad() {
        assert_eq!(ev("lpad", &[s("ab"), i(6), s("=")]), s("====ab"));
    }
    #[test]
    fn rp_eq_pad() {
        assert_eq!(ev("rpad", &[s("ab"), i(6), s("=")]), s("ab===="));
    }
    #[test]
    fn lp_cd_4() {
        assert_eq!(ev("lpad", &[s("x"), i(4), s("cd")]), s("cdcx"));
    }
    #[test]
    fn rp_cd_4() {
        assert_eq!(ev("rpad", &[s("x"), i(4), s("cd")]), s("xcdc"));
    }
    #[test]
    fn lp_long_pad() {
        assert_eq!(ev("lpad", &[s("a"), i(6), s("xyz")]), s("xyzxya"));
    }
    #[test]
    fn rp_long_pad() {
        assert_eq!(ev("rpad", &[s("a"), i(6), s("xyz")]), s("axyzxy"));
    }
    #[test]
    fn lp_int() {
        assert_eq!(ev("lpad", &[i(42), i(5), s("0")]), s("00042"));
    }
    #[test]
    fn rp_int() {
        assert_eq!(ev("rpad", &[i(42), i(5), s("0")]), s("42000"));
    }
    #[test]
    fn lp_star_6() {
        assert_eq!(ev("lpad", &[s("ab"), i(6), s("*")]), s("****ab"));
    }
    #[test]
    fn rp_star_6() {
        assert_eq!(ev("rpad", &[s("ab"), i(6), s("*")]), s("ab****"));
    }
    #[test]
    fn lp_at_5() {
        assert_eq!(ev("lpad", &[s("ab"), i(5), s("@")]), s("@@@ab"));
    }
    #[test]
    fn rp_at_5() {
        assert_eq!(ev("rpad", &[s("ab"), i(5), s("@")]), s("ab@@@"));
    }
    #[test]
    fn lp_pct_5() {
        assert_eq!(ev("lpad", &[s("ab"), i(5), s("%")]), s("%%%ab"));
    }
    #[test]
    fn rp_pct_5() {
        assert_eq!(ev("rpad", &[s("ab"), i(5), s("%")]), s("ab%%%"));
    }
    #[test]
    fn lp_under_5() {
        assert_eq!(ev("lpad", &[s("ab"), i(5), s("_")]), s("___ab"));
    }
    #[test]
    fn rp_under_5() {
        assert_eq!(ev("rpad", &[s("ab"), i(5), s("_")]), s("ab___"));
    }
}

// ===== split_part (40) =====
mod split_part_tests {
    use super::*;
    #[test]
    fn sp_dot_1() {
        assert_eq!(ev("split_part", &[s("a.b.c"), s("."), i(1)]), s("a"));
    }
    #[test]
    fn sp_dot_2() {
        assert_eq!(ev("split_part", &[s("a.b.c"), s("."), i(2)]), s("b"));
    }
    #[test]
    fn sp_dot_3() {
        assert_eq!(ev("split_part", &[s("a.b.c"), s("."), i(3)]), s("c"));
    }
    #[test]
    fn sp_dot_4() {
        assert_eq!(ev("split_part", &[s("a.b.c"), s("."), i(4)]), s(""));
    }
    #[test]
    fn sp_comma_1() {
        assert_eq!(ev("split_part", &[s("x,y,z"), s(","), i(1)]), s("x"));
    }
    #[test]
    fn sp_comma_2() {
        assert_eq!(ev("split_part", &[s("x,y,z"), s(","), i(2)]), s("y"));
    }
    #[test]
    fn sp_comma_3() {
        assert_eq!(ev("split_part", &[s("x,y,z"), s(","), i(3)]), s("z"));
    }
    #[test]
    fn sp_slash_1() {
        assert_eq!(ev("split_part", &[s("a/b/c"), s("/"), i(1)]), s("a"));
    }
    #[test]
    fn sp_slash_2() {
        assert_eq!(ev("split_part", &[s("a/b/c"), s("/"), i(2)]), s("b"));
    }
    #[test]
    fn sp_slash_3() {
        assert_eq!(ev("split_part", &[s("a/b/c"), s("/"), i(3)]), s("c"));
    }
    #[test]
    fn sp_dash_1() {
        assert_eq!(ev("split_part", &[s("a-b-c"), s("-"), i(1)]), s("a"));
    }
    #[test]
    fn sp_dash_2() {
        assert_eq!(ev("split_part", &[s("a-b-c"), s("-"), i(2)]), s("b"));
    }
    #[test]
    fn sp_dash_3() {
        assert_eq!(ev("split_part", &[s("a-b-c"), s("-"), i(3)]), s("c"));
    }
    #[test]
    fn sp_space_1() {
        assert_eq!(ev("split_part", &[s("a b c"), s(" "), i(1)]), s("a"));
    }
    #[test]
    fn sp_space_2() {
        assert_eq!(ev("split_part", &[s("a b c"), s(" "), i(2)]), s("b"));
    }
    #[test]
    fn sp_space_3() {
        assert_eq!(ev("split_part", &[s("a b c"), s(" "), i(3)]), s("c"));
    }
    #[test]
    fn sp_no_delim() {
        assert_eq!(ev("split_part", &[s("abc"), s("."), i(1)]), s("abc"));
    }
    #[test]
    fn sp_no_delim_2() {
        assert_eq!(ev("split_part", &[s("abc"), s("."), i(2)]), s(""));
    }
    #[test]
    fn sp_single() {
        assert_eq!(ev("split_part", &[s("a"), s("."), i(1)]), s("a"));
    }
    #[test]
    fn sp_empty() {
        assert_eq!(ev("split_part", &[s(""), s("."), i(1)]), s(""));
    }
    #[test]
    fn sp_null() {
        assert_eq!(ev("split_part", &[Value::Null, s("."), i(1)]), Value::Null);
    }
    #[test]
    fn sp_colon_1() {
        assert_eq!(ev("split_part", &[s("a:b:c"), s(":"), i(1)]), s("a"));
    }
    #[test]
    fn sp_colon_2() {
        assert_eq!(ev("split_part", &[s("a:b:c"), s(":"), i(2)]), s("b"));
    }
    #[test]
    fn sp_colon_3() {
        assert_eq!(ev("split_part", &[s("a:b:c"), s(":"), i(3)]), s("c"));
    }
    #[test]
    fn sp_pipe_1() {
        assert_eq!(ev("split_part", &[s("a|b|c"), s("|"), i(1)]), s("a"));
    }
    #[test]
    fn sp_pipe_2() {
        assert_eq!(ev("split_part", &[s("a|b|c"), s("|"), i(2)]), s("b"));
    }
    #[test]
    fn sp_pipe_3() {
        assert_eq!(ev("split_part", &[s("a|b|c"), s("|"), i(3)]), s("c"));
    }
    #[test]
    fn sp_multi_delim_1() {
        assert_eq!(ev("split_part", &[s("a::b::c"), s("::"), i(1)]), s("a"));
    }
    #[test]
    fn sp_multi_delim_2() {
        assert_eq!(ev("split_part", &[s("a::b::c"), s("::"), i(2)]), s("b"));
    }
    #[test]
    fn sp_multi_delim_3() {
        assert_eq!(ev("split_part", &[s("a::b::c"), s("::"), i(3)]), s("c"));
    }
    #[test]
    fn sp_four_1() {
        assert_eq!(ev("split_part", &[s("a.b.c.d"), s("."), i(1)]), s("a"));
    }
    #[test]
    fn sp_four_2() {
        assert_eq!(ev("split_part", &[s("a.b.c.d"), s("."), i(2)]), s("b"));
    }
    #[test]
    fn sp_four_3() {
        assert_eq!(ev("split_part", &[s("a.b.c.d"), s("."), i(3)]), s("c"));
    }
    #[test]
    fn sp_four_4() {
        assert_eq!(ev("split_part", &[s("a.b.c.d"), s("."), i(4)]), s("d"));
    }
    #[test]
    fn sp_four_5() {
        assert_eq!(ev("split_part", &[s("a.b.c.d"), s("."), i(5)]), s(""));
    }
    #[test]
    fn sp_semi_1() {
        assert_eq!(ev("split_part", &[s("a;b;c"), s(";"), i(1)]), s("a"));
    }
    #[test]
    fn sp_semi_2() {
        assert_eq!(ev("split_part", &[s("a;b;c"), s(";"), i(2)]), s("b"));
    }
    #[test]
    fn sp_semi_3() {
        assert_eq!(ev("split_part", &[s("a;b;c"), s(";"), i(3)]), s("c"));
    }
    #[test]
    fn sp_tab_1() {
        assert_eq!(ev("split_part", &[s("a\tb\tc"), s("\t"), i(1)]), s("a"));
    }
    #[test]
    fn sp_tab_2() {
        assert_eq!(ev("split_part", &[s("a\tb\tc"), s("\t"), i(2)]), s("b"));
    }
}

// ===== ascii / chr (50) =====
mod ascii_chr_tests {
    use super::*;
    #[test]
    fn ascii_a() {
        assert_eq!(ev("ascii", &[s("A")]), i(65));
    }
    #[test]
    fn ascii_b() {
        assert_eq!(ev("ascii", &[s("B")]), i(66));
    }
    #[test]
    fn ascii_c() {
        assert_eq!(ev("ascii", &[s("C")]), i(67));
    }
    #[test]
    fn ascii_d() {
        assert_eq!(ev("ascii", &[s("D")]), i(68));
    }
    #[test]
    fn ascii_e() {
        assert_eq!(ev("ascii", &[s("E")]), i(69));
    }
    #[test]
    fn ascii_z() {
        assert_eq!(ev("ascii", &[s("Z")]), i(90));
    }
    #[test]
    fn ascii_la() {
        assert_eq!(ev("ascii", &[s("a")]), i(97));
    }
    #[test]
    fn ascii_lb() {
        assert_eq!(ev("ascii", &[s("b")]), i(98));
    }
    #[test]
    fn ascii_lz() {
        assert_eq!(ev("ascii", &[s("z")]), i(122));
    }
    #[test]
    fn ascii_0() {
        assert_eq!(ev("ascii", &[s("0")]), i(48));
    }
    #[test]
    fn ascii_1() {
        assert_eq!(ev("ascii", &[s("1")]), i(49));
    }
    #[test]
    fn ascii_9() {
        assert_eq!(ev("ascii", &[s("9")]), i(57));
    }
    #[test]
    fn ascii_space() {
        assert_eq!(ev("ascii", &[s(" ")]), i(32));
    }
    #[test]
    fn ascii_excl() {
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
    fn ascii_dollar() {
        assert_eq!(ev("ascii", &[s("$")]), i(36));
    }
    #[test]
    fn ascii_pct() {
        assert_eq!(ev("ascii", &[s("%")]), i(37));
    }
    #[test]
    fn ascii_multi() {
        assert_eq!(ev("ascii", &[s("ABC")]), i(65));
    }
    #[test]
    fn ascii_null() {
        assert_eq!(ev("ascii", &[Value::Null]), Value::Null);
    }
    #[test]
    fn chr_65() {
        assert_eq!(ev("chr", &[i(65)]), s("A"));
    }
    #[test]
    fn chr_66() {
        assert_eq!(ev("chr", &[i(66)]), s("B"));
    }
    #[test]
    fn chr_67() {
        assert_eq!(ev("chr", &[i(67)]), s("C"));
    }
    #[test]
    fn chr_90() {
        assert_eq!(ev("chr", &[i(90)]), s("Z"));
    }
    #[test]
    fn chr_97() {
        assert_eq!(ev("chr", &[i(97)]), s("a"));
    }
    #[test]
    fn chr_98() {
        assert_eq!(ev("chr", &[i(98)]), s("b"));
    }
    #[test]
    fn chr_122() {
        assert_eq!(ev("chr", &[i(122)]), s("z"));
    }
    #[test]
    fn chr_48() {
        assert_eq!(ev("chr", &[i(48)]), s("0"));
    }
    #[test]
    fn chr_57() {
        assert_eq!(ev("chr", &[i(57)]), s("9"));
    }
    #[test]
    fn chr_32() {
        assert_eq!(ev("chr", &[i(32)]), s(" "));
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
    #[test]
    fn chr_36() {
        assert_eq!(ev("chr", &[i(36)]), s("$"));
    }
    #[test]
    fn chr_37() {
        assert_eq!(ev("chr", &[i(37)]), s("%"));
    }
    #[test]
    fn chr_42() {
        assert_eq!(ev("chr", &[i(42)]), s("*"));
    }
    #[test]
    fn chr_43() {
        assert_eq!(ev("chr", &[i(43)]), s("+"));
    }
    #[test]
    fn chr_44() {
        assert_eq!(ev("chr", &[i(44)]), s(","));
    }
    #[test]
    fn chr_45() {
        assert_eq!(ev("chr", &[i(45)]), s("-"));
    }
    #[test]
    fn chr_46() {
        assert_eq!(ev("chr", &[i(46)]), s("."));
    }
    #[test]
    fn chr_47() {
        assert_eq!(ev("chr", &[i(47)]), s("/"));
    }
    #[test]
    fn chr_58() {
        assert_eq!(ev("chr", &[i(58)]), s(":"));
    }
    #[test]
    fn chr_59() {
        assert_eq!(ev("chr", &[i(59)]), s(";"));
    }
    #[test]
    fn chr_61() {
        assert_eq!(ev("chr", &[i(61)]), s("="));
    }
    #[test]
    fn chr_63() {
        assert_eq!(ev("chr", &[i(63)]), s("?"));
    }
    #[test]
    fn chr_91() {
        assert_eq!(ev("chr", &[i(91)]), s("["));
    }
    #[test]
    fn chr_93() {
        assert_eq!(ev("chr", &[i(93)]), s("]"));
    }
    #[test]
    fn chr_95() {
        assert_eq!(ev("chr", &[i(95)]), s("_"));
    }
    #[test]
    fn chr_126() {
        assert_eq!(ev("chr", &[i(126)]), s("~"));
    }
    #[test]
    fn chr_null() {
        assert_eq!(ev("chr", &[Value::Null]), Value::Null);
    }
}

// ===== md5 / sha256 (50) =====
mod hash_tests {
    use super::*;
    #[test]
    fn md5_empty() {
        assert_eq!(ev("md5", &[s("")]), s("d41d8cd98f00b204e9800998ecf8427e"));
    }
    #[test]
    fn md5_hello() {
        assert_eq!(
            ev("md5", &[s("hello")]),
            s("5d41402abc4b2a76b9719d911017c592")
        );
    }
    #[test]
    fn md5_world() {
        assert_eq!(
            ev("md5", &[s("world")]),
            s("7d793037a0760186574b0282f2f435e7")
        );
    }
    #[test]
    fn md5_a() {
        assert_eq!(ev("md5", &[s("a")]), s("0cc175b9c0f1b6a831c399e269772661"));
    }
    #[test]
    fn md5_b() {
        assert_eq!(ev("md5", &[s("b")]), s("92eb5ffee6ae2fec3ad71c777531578f"));
    }
    #[test]
    fn md5_abc() {
        assert_eq!(
            ev("md5", &[s("abc")]),
            s("900150983cd24fb0d6963f7d28e17f72")
        );
    }
    #[test]
    fn md5_123() {
        assert_eq!(
            ev("md5", &[s("123")]),
            s("202cb962ac59075b964b07152d234b70")
        );
    }
    #[test]
    fn md5_test() {
        assert_eq!(
            ev("md5", &[s("test")]),
            s("098f6bcd4621d373cade4e832627b4f6")
        );
    }
    #[test]
    fn md5_foo() {
        assert_eq!(
            ev("md5", &[s("foo")]),
            s("acbd18db4cc2f85cedef654fccc4a4d8")
        );
    }
    #[test]
    fn md5_bar() {
        assert_eq!(
            ev("md5", &[s("bar")]),
            s("37b51d194a7513e45b56f6524f2d51f2")
        );
    }
    #[test]
    fn md5_null() {
        assert_eq!(ev("md5", &[Value::Null]), Value::Null);
    }
    #[test]
    fn md5_space() {
        assert_eq!(ev("md5", &[s(" ")]), s("7215ee9c7d9dc229d2921a40e899ec5f"));
    }
    #[test]
    fn md5_len32() {
        let v = ev("md5", &[s("x")]);
        if let Value::Str(h) = v {
            assert_eq!(h.len(), 32);
        }
    }
    #[test]
    fn md5_x() {
        assert_eq!(ev("md5", &[s("x")]), s("9dd4e461268c8034f5c8564e155c67a6"));
    }
    #[test]
    fn md5_y() {
        assert_eq!(ev("md5", &[s("y")]), s("415290769594460e2e485922904f345d"));
    }
    #[test]
    fn md5_z() {
        assert_eq!(ev("md5", &[s("z")]), s("fbade9e36a3f36d3d676c1b808451dd7"));
    }
    #[test]
    fn md5_int() {
        assert_eq!(ev("md5", &[i(42)]), s("a1d0c6e83f027327d8461063f4ac58a6"));
    }
    #[test]
    fn md5_rust() {
        assert_eq!(
            ev("md5", &[s("rust")]),
            s("72812e30873455dcee2ce2d1ee26e4ab")
        );
    }
    #[test]
    fn md5_ab() {
        assert_eq!(ev("md5", &[s("ab")]), s("187ef4436122d1cc2f40dc2b92f0eba0"));
    }
    #[test]
    fn md5_cd() {
        assert_eq!(ev("md5", &[s("cd")]), s("6865aeb3a9ed28f9a79ec454b259e5d0"));
    }
    #[test]
    fn sha_empty() {
        assert_eq!(
            ev("sha256", &[s("")]),
            s("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        );
    }
    #[test]
    fn sha_hello() {
        assert_eq!(
            ev("sha256", &[s("hello")]),
            s("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
        );
    }
    #[test]
    fn sha_world() {
        assert_eq!(
            ev("sha256", &[s("world")]),
            s("486ea46224d1bb4fb680f34f7c9ad96a8f24ec88be73ea8e5a6c65260e9cb8a7")
        );
    }
    #[test]
    fn sha_a() {
        assert_eq!(
            ev("sha256", &[s("a")]),
            s("ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb")
        );
    }
    #[test]
    fn sha_abc() {
        assert_eq!(
            ev("sha256", &[s("abc")]),
            s("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")
        );
    }
    #[test]
    fn sha_test() {
        assert_eq!(
            ev("sha256", &[s("test")]),
            s("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08")
        );
    }
    #[test]
    fn sha_null() {
        assert_eq!(ev("sha256", &[Value::Null]), Value::Null);
    }
    #[test]
    fn sha_len64() {
        let v = ev("sha256", &[s("x")]);
        if let Value::Str(h) = v {
            assert_eq!(h.len(), 64);
        }
    }
    #[test]
    fn sha_123() {
        assert_eq!(
            ev("sha256", &[s("123")]),
            s("a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3")
        );
    }
    #[test]
    fn sha_foo() {
        assert_eq!(
            ev("sha256", &[s("foo")]),
            s("2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae")
        );
    }
    #[test]
    fn sha_bar() {
        assert_eq!(
            ev("sha256", &[s("bar")]),
            s("fcde2b2edba56bf408601fb721fe9b5c338d10ee429ea04fae5511b68fbf8fb9")
        );
    }
    #[test]
    fn sha_x() {
        assert_eq!(
            ev("sha256", &[s("x")]),
            s("2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881")
        );
    }
    #[test]
    fn sha_int() {
        assert_eq!(
            ev("sha256", &[i(42)]),
            s("73475cb40a568e8da8a045ced110137e159f890ac4da883b6b17dc651b3a8049")
        );
    }
    #[test]
    fn md5_hw() {
        assert_eq!(
            ev("md5", &[s("hello world")]),
            s("5eb63bbbe01eeed093cb22bb8f5acdc3")
        );
    }
    #[test]
    fn md5_0() {
        assert_eq!(ev("md5", &[s("0")]), s("cfcd208495d565ef66e7dff9f98764da"));
    }
    #[test]
    fn md5_1() {
        assert_eq!(ev("md5", &[s("1")]), s("c4ca4238a0b923820dcc509a6f75849b"));
    }
    #[test]
    fn md5_2() {
        assert_eq!(ev("md5", &[s("2")]), s("c81e728d9d4c2f636f067f89cc14862c"));
    }
    #[test]
    fn md5_3() {
        assert_eq!(ev("md5", &[s("3")]), s("eccbc87e4b5ce2fe28308fd9f2a7baf3"));
    }
    #[test]
    fn md5_4() {
        assert_eq!(ev("md5", &[s("4")]), s("a87ff679a2f3e71d9181a67b7542122c"));
    }
    #[test]
    fn md5_5() {
        assert_eq!(ev("md5", &[s("5")]), s("e4da3b7fbbce2345d7772b0674a318d5"));
    }
    #[test]
    fn md5_6() {
        assert_eq!(ev("md5", &[s("6")]), s("1679091c5a880faf6fb5e6087eb1b2dc"));
    }
    #[test]
    fn md5_7() {
        assert_eq!(ev("md5", &[s("7")]), s("8f14e45fceea167a5a36dedd4bea2543"));
    }
    #[test]
    fn md5_8() {
        assert_eq!(ev("md5", &[s("8")]), s("c9f0f895fb98ab9159f51fd0297e236d"));
    }
    #[test]
    fn md5_9() {
        assert_eq!(ev("md5", &[s("9")]), s("45c48cce2e2d7fbdea1afc51c7c6ad26"));
    }
    #[test]
    fn sha_b() {
        assert_eq!(
            ev("sha256", &[s("b")]),
            s("3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d")
        );
    }
    #[test]
    fn sha_c() {
        assert_eq!(
            ev("sha256", &[s("c")]),
            s("2e7d2c03a9507ae265ecf5b5356885a53393a2029d241394997265a1a25aefc6")
        );
    }
    #[test]
    fn sha_d() {
        assert_eq!(
            ev("sha256", &[s("d")]),
            s("18ac3e7343f016890c510e93f935261169d9e3f565436429830faf0934f4f8e4")
        );
    }
    #[test]
    fn sha_e() {
        assert_eq!(
            ev("sha256", &[s("e")]),
            s("3f79bb7b435b05321651daefd374cdc681dc06faa65e374e38337b88ca046dea")
        );
    }
    #[test]
    fn sha_space() {
        assert_eq!(
            ev("sha256", &[s(" ")]),
            s("36a9e7f1c95b82ffb99743e0c5c4ce95d83c9a430aac59f84ef3cbfab6145068")
        );
    }
    #[test]
    fn sha_ab() {
        assert_eq!(
            ev("sha256", &[s("ab")]),
            s("fb8e20fc2e4c3f248c60c39bd652f3c1347298bb977b8b4d5903b85055620603")
        );
    }
}

// ===== hex / space / char_at (50) =====
mod misc_str_tests {
    use super::*;
    #[test]
    fn hex_0() {
        assert_eq!(ev("hex", &[i(0)]), s("0"));
    }
    #[test]
    fn hex_1() {
        assert_eq!(ev("hex", &[i(1)]), s("1"));
    }
    #[test]
    fn hex_10() {
        assert_eq!(ev("hex", &[i(10)]), s("a"));
    }
    #[test]
    fn hex_15() {
        assert_eq!(ev("hex", &[i(15)]), s("f"));
    }
    #[test]
    fn hex_16() {
        assert_eq!(ev("hex", &[i(16)]), s("10"));
    }
    #[test]
    fn hex_255() {
        assert_eq!(ev("hex", &[i(255)]), s("ff"));
    }
    #[test]
    fn hex_256() {
        assert_eq!(ev("hex", &[i(256)]), s("100"));
    }
    #[test]
    fn hex_100() {
        assert_eq!(ev("hex", &[i(100)]), s("64"));
    }
    #[test]
    fn hex_1000() {
        assert_eq!(ev("hex", &[i(1000)]), s("3e8"));
    }
    #[test]
    fn hex_null() {
        assert_eq!(ev("hex", &[Value::Null]), Value::Null);
    }
    #[test]
    fn space_0() {
        assert_eq!(ev("space", &[i(0)]), s(""));
    }
    #[test]
    fn space_1() {
        assert_eq!(ev("space", &[i(1)]), s(" "));
    }
    #[test]
    fn space_2() {
        assert_eq!(ev("space", &[i(2)]), s("  "));
    }
    #[test]
    fn space_3() {
        assert_eq!(ev("space", &[i(3)]), s("   "));
    }
    #[test]
    fn space_5() {
        assert_eq!(ev("space", &[i(5)]), s("     "));
    }
    #[test]
    fn space_10() {
        assert_eq!(ev("space", &[i(10)]), s("          "));
    }
    #[test]
    fn space_null() {
        assert_eq!(ev("space", &[Value::Null]), Value::Null);
    }
    #[test]
    fn ca_h() {
        assert_eq!(ev("char_at", &[s("hello"), i(1)]), s("h"));
    }
    #[test]
    fn ca_e() {
        assert_eq!(ev("char_at", &[s("hello"), i(2)]), s("e"));
    }
    #[test]
    fn ca_l1() {
        assert_eq!(ev("char_at", &[s("hello"), i(3)]), s("l"));
    }
    #[test]
    fn ca_l2() {
        assert_eq!(ev("char_at", &[s("hello"), i(4)]), s("l"));
    }
    #[test]
    fn ca_o() {
        assert_eq!(ev("char_at", &[s("hello"), i(5)]), s("o"));
    }
    #[test]
    fn ca_oob() {
        assert_eq!(ev("char_at", &[s("hello"), i(10)]), Value::Null);
    }
    #[test]
    fn ca_a() {
        assert_eq!(ev("char_at", &[s("abc"), i(1)]), s("a"));
    }
    #[test]
    fn ca_b() {
        assert_eq!(ev("char_at", &[s("abc"), i(2)]), s("b"));
    }
    #[test]
    fn ca_c() {
        assert_eq!(ev("char_at", &[s("abc"), i(3)]), s("c"));
    }
    #[test]
    fn ca_abc_4() {
        assert_eq!(ev("char_at", &[s("abc"), i(4)]), Value::Null);
    }
    #[test]
    fn ca_null() {
        assert_eq!(ev("char_at", &[Value::Null, i(1)]), Value::Null);
    }
    #[test]
    fn ca_x() {
        assert_eq!(ev("char_at", &[s("xyz"), i(1)]), s("x"));
    }
    #[test]
    fn ca_y() {
        assert_eq!(ev("char_at", &[s("xyz"), i(2)]), s("y"));
    }
    #[test]
    fn ca_z() {
        assert_eq!(ev("char_at", &[s("xyz"), i(3)]), s("z"));
    }
    #[test]
    fn ca_1() {
        assert_eq!(ev("char_at", &[s("12345"), i(1)]), s("1"));
    }
    #[test]
    fn ca_5() {
        assert_eq!(ev("char_at", &[s("12345"), i(5)]), s("5"));
    }
    #[test]
    fn hex_2() {
        assert_eq!(ev("hex", &[i(2)]), s("2"));
    }
    #[test]
    fn hex_3() {
        assert_eq!(ev("hex", &[i(3)]), s("3"));
    }
    #[test]
    fn hex_4() {
        assert_eq!(ev("hex", &[i(4)]), s("4"));
    }
    #[test]
    fn hex_5() {
        assert_eq!(ev("hex", &[i(5)]), s("5"));
    }
    #[test]
    fn hex_6() {
        assert_eq!(ev("hex", &[i(6)]), s("6"));
    }
    #[test]
    fn hex_7() {
        assert_eq!(ev("hex", &[i(7)]), s("7"));
    }
    #[test]
    fn hex_8() {
        assert_eq!(ev("hex", &[i(8)]), s("8"));
    }
    #[test]
    fn hex_9() {
        assert_eq!(ev("hex", &[i(9)]), s("9"));
    }
    #[test]
    fn hex_11() {
        assert_eq!(ev("hex", &[i(11)]), s("b"));
    }
    #[test]
    fn hex_12() {
        assert_eq!(ev("hex", &[i(12)]), s("c"));
    }
    #[test]
    fn hex_13() {
        assert_eq!(ev("hex", &[i(13)]), s("d"));
    }
    #[test]
    fn hex_14() {
        assert_eq!(ev("hex", &[i(14)]), s("e"));
    }
    #[test]
    fn space_4() {
        assert_eq!(ev("space", &[i(4)]), s("    "));
    }
    #[test]
    fn space_6() {
        assert_eq!(ev("space", &[i(6)]), s("      "));
    }
    #[test]
    fn space_7() {
        assert_eq!(ev("space", &[i(7)]), s("       "));
    }
    #[test]
    fn space_8() {
        assert_eq!(ev("space", &[i(8)]), s("        "));
    }
    #[test]
    fn space_9() {
        assert_eq!(ev("space", &[i(9)]), s("         "));
    }
}
