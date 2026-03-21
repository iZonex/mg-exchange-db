//! Comprehensive string function tests for ExchangeDB.
//! 500+ test cases covering every registered string scalar function.

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

fn ts(ns: i64) -> Value {
    Value::Timestamp(ns)
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

// ===========================================================================
// length
// ===========================================================================
mod length_tests {
    use super::*;

    #[test]
    fn empty_string() {
        assert_eq!(eval("length", &[s("")]), i(0));
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("length", &[s("a")]), i(1));
    }
    #[test]
    fn hello() {
        assert_eq!(eval("length", &[s("hello")]), i(5));
    }
    #[test]
    fn with_spaces() {
        assert_eq!(eval("length", &[s("a b c")]), i(5));
    }
    #[test]
    fn with_newline() {
        assert_eq!(eval("length", &[s("a\nb")]), i(3));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("length", &[null()]), null());
    }
    #[test]
    fn number_input() {
        assert_eq!(eval("length", &[i(12345)]), i(5));
    }
    #[test]
    fn negative_number() {
        assert_eq!(eval("length", &[i(-42)]), i(3));
    }
    #[test]
    fn float_input() {
        assert_eq!(eval("length", &[f(3.14)]), i(4));
    }
    #[test]
    fn long_string() {
        let long = "x".repeat(1000);
        assert_eq!(eval("length", &[s(&long)]), i(1000));
    }
    #[test]
    fn special_chars() {
        assert_eq!(eval("length", &[s("!@#$%")]), i(5));
    }
    #[test]
    fn tab_char() {
        assert_eq!(eval("length", &[s("\t")]), i(1));
    }
}

// ===========================================================================
// upper
// ===========================================================================
mod upper_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("upper", &[s("hello")]), s("HELLO"));
    }
    #[test]
    fn mixed_case() {
        assert_eq!(eval("upper", &[s("Hello World")]), s("HELLO WORLD"));
    }
    #[test]
    fn already_upper() {
        assert_eq!(eval("upper", &[s("ABC")]), s("ABC"));
    }
    #[test]
    fn empty() {
        assert_eq!(eval("upper", &[s("")]), s(""));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("upper", &[null()]), null());
    }
    #[test]
    fn digits() {
        assert_eq!(eval("upper", &[s("abc123")]), s("ABC123"));
    }
    #[test]
    fn special_chars() {
        assert_eq!(eval("upper", &[s("a!b@c")]), s("A!B@C"));
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("upper", &[s("z")]), s("Z"));
    }
    #[test]
    fn with_spaces() {
        assert_eq!(eval("upper", &[s("a b c")]), s("A B C"));
    }
    #[test]
    fn with_newlines() {
        assert_eq!(eval("upper", &[s("a\nb")]), s("A\nB"));
    }
    #[test]
    fn integer_input() {
        assert_eq!(eval("upper", &[i(42)]), s("42"));
    }
}

// ===========================================================================
// lower
// ===========================================================================
mod lower_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("lower", &[s("HELLO")]), s("hello"));
    }
    #[test]
    fn mixed_case() {
        assert_eq!(eval("lower", &[s("Hello World")]), s("hello world"));
    }
    #[test]
    fn already_lower() {
        assert_eq!(eval("lower", &[s("abc")]), s("abc"));
    }
    #[test]
    fn empty() {
        assert_eq!(eval("lower", &[s("")]), s(""));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("lower", &[null()]), null());
    }
    #[test]
    fn digits_mixed() {
        assert_eq!(eval("lower", &[s("ABC123")]), s("abc123"));
    }
    #[test]
    fn special_chars() {
        assert_eq!(eval("lower", &[s("A!B@C")]), s("a!b@c"));
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("lower", &[s("Z")]), s("z"));
    }
    #[test]
    fn spaces() {
        assert_eq!(eval("lower", &[s("A B C")]), s("a b c"));
    }
    #[test]
    fn number_input() {
        assert_eq!(eval("lower", &[i(42)]), s("42"));
    }
}

// ===========================================================================
// trim, ltrim, rtrim
// ===========================================================================
mod trim_tests {
    use super::*;

    #[test]
    fn trim_spaces() {
        assert_eq!(eval("trim", &[s("  hello  ")]), s("hello"));
    }
    #[test]
    fn trim_tabs() {
        assert_eq!(eval("trim", &[s("\thello\t")]), s("hello"));
    }
    #[test]
    fn trim_none() {
        assert_eq!(eval("trim", &[s("hello")]), s("hello"));
    }
    #[test]
    fn trim_null() {
        assert_eq!(eval("trim", &[null()]), null());
    }
    #[test]
    fn trim_only_ws() {
        assert_eq!(eval("trim", &[s("   ")]), s(""));
    }
    #[test]
    fn trim_empty() {
        assert_eq!(eval("trim", &[s("")]), s(""));
    }
    #[test]
    fn trim_mixed_ws() {
        assert_eq!(eval("trim", &[s(" \t\n hello \t\n ")]), s("hello"));
    }
    #[test]
    fn trim_inner_spaces() {
        assert_eq!(eval("trim", &[s("  a b  ")]), s("a b"));
    }
    #[test]
    fn trim_single_space() {
        assert_eq!(eval("trim", &[s(" ")]), s(""));
    }
    #[test]
    fn trim_newlines() {
        assert_eq!(eval("trim", &[s("\nhello\n")]), s("hello"));
    }

    #[test]
    fn ltrim_basic() {
        assert_eq!(eval("ltrim", &[s("  hello  ")]), s("hello  "));
    }
    #[test]
    fn ltrim_null() {
        assert_eq!(eval("ltrim", &[null()]), null());
    }
    #[test]
    fn ltrim_no_leading() {
        assert_eq!(eval("ltrim", &[s("hello")]), s("hello"));
    }
    #[test]
    fn ltrim_all_spaces() {
        assert_eq!(eval("ltrim", &[s("   ")]), s(""));
    }
    #[test]
    fn ltrim_empty() {
        assert_eq!(eval("ltrim", &[s("")]), s(""));
    }
    #[test]
    fn ltrim_tab() {
        assert_eq!(eval("ltrim", &[s("\thello")]), s("hello"));
    }

    #[test]
    fn rtrim_basic() {
        assert_eq!(eval("rtrim", &[s("  hello  ")]), s("  hello"));
    }
    #[test]
    fn rtrim_null() {
        assert_eq!(eval("rtrim", &[null()]), null());
    }
    #[test]
    fn rtrim_no_trailing() {
        assert_eq!(eval("rtrim", &[s("hello")]), s("hello"));
    }
    #[test]
    fn rtrim_all_spaces() {
        assert_eq!(eval("rtrim", &[s("   ")]), s(""));
    }
    #[test]
    fn rtrim_empty() {
        assert_eq!(eval("rtrim", &[s("")]), s(""));
    }
    #[test]
    fn rtrim_tab() {
        assert_eq!(eval("rtrim", &[s("hello\t")]), s("hello"));
    }
}

// ===========================================================================
// substring
// ===========================================================================
mod substring_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("substring", &[s("hello"), i(2), i(3)]), s("ell"));
    }
    #[test]
    fn from_start() {
        assert_eq!(eval("substring", &[s("hello"), i(1), i(5)]), s("hello"));
    }
    #[test]
    fn past_end() {
        assert_eq!(eval("substring", &[s("hi"), i(1), i(10)]), s("hi"));
    }
    #[test]
    fn zero_len() {
        assert_eq!(eval("substring", &[s("hello"), i(1), i(0)]), s(""));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("substring", &[null(), i(1), i(3)]), null());
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("substring", &[s("abc"), i(2), i(1)]), s("b"));
    }
    #[test]
    fn full_string() {
        assert_eq!(eval("substring", &[s("world"), i(1), i(5)]), s("world"));
    }
    #[test]
    fn last_char() {
        assert_eq!(eval("substring", &[s("hello"), i(5), i(1)]), s("o"));
    }
    #[test]
    fn empty_input() {
        assert_eq!(eval("substring", &[s(""), i(1), i(5)]), s(""));
    }
    #[test]
    fn large_start() {
        assert_eq!(eval("substring", &[s("hi"), i(10), i(5)]), s(""));
    }
    #[test]
    fn middle() {
        assert_eq!(eval("substring", &[s("abcdef"), i(3), i(2)]), s("cd"));
    }
}

// ===========================================================================
// concat
// ===========================================================================
mod concat_tests {
    use super::*;

    #[test]
    fn two_strings() {
        assert_eq!(eval("concat", &[s("hello"), s(" world")]), s("hello world"));
    }
    #[test]
    fn three_strings() {
        assert_eq!(eval("concat", &[s("a"), s("b"), s("c")]), s("abc"));
    }
    #[test]
    fn with_null() {
        assert_eq!(eval("concat", &[null(), s("world")]), s("world"));
    }
    #[test]
    fn both_null() {
        assert_eq!(eval("concat", &[null(), null()]), s(""));
    }
    #[test]
    fn empty_strings() {
        assert_eq!(eval("concat", &[s(""), s("")]), s(""));
    }
    #[test]
    fn with_number() {
        assert_eq!(eval("concat", &[s("val="), i(42)]), s("val=42"));
    }
    #[test]
    fn with_float() {
        assert_eq!(eval("concat", &[s("pi="), f(3.14)]), s("pi=3.14"));
    }
    #[test]
    fn four_args() {
        assert_eq!(eval("concat", &[s("a"), s("b"), s("c"), s("d")]), s("abcd"));
    }
    #[test]
    fn null_in_middle() {
        assert_eq!(eval("concat", &[s("a"), null(), s("c")]), s("ac"));
    }
    #[test]
    fn single_char_each() {
        assert_eq!(eval("concat", &[s("x"), s("y")]), s("xy"));
    }
    #[test]
    fn long_concat() {
        let a = "a".repeat(500);
        let b = "b".repeat(500);
        let expected = format!("{}{}", a, b);
        assert_eq!(eval("concat", &[s(&a), s(&b)]), s(&expected));
    }
}

// ===========================================================================
// replace
// ===========================================================================
mod replace_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(
            eval("replace", &[s("hello world"), s("world"), s("rust")]),
            s("hello rust")
        );
    }
    #[test]
    fn no_match() {
        assert_eq!(
            eval("replace", &[s("hello"), s("xyz"), s("abc")]),
            s("hello")
        );
    }
    #[test]
    fn multiple_occurrences() {
        assert_eq!(eval("replace", &[s("aaa"), s("a"), s("b")]), s("bbb"));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("replace", &[null(), s("a"), s("b")]), null());
    }
    #[test]
    fn remove_chars() {
        assert_eq!(eval("replace", &[s("hello"), s("l"), s("")]), s("heo"));
    }
    #[test]
    fn replace_with_longer() {
        assert_eq!(eval("replace", &[s("ab"), s("a"), s("xyz")]), s("xyzb"));
    }
    #[test]
    fn case_sensitive() {
        assert_eq!(
            eval("replace", &[s("Hello"), s("hello"), s("x")]),
            s("Hello")
        );
    }
    #[test]
    fn empty_source() {
        assert_eq!(eval("replace", &[s(""), s("a"), s("b")]), s(""));
    }
    #[test]
    fn replace_spaces() {
        assert_eq!(eval("replace", &[s("a b c"), s(" "), s("-")]), s("a-b-c"));
    }
    #[test]
    fn replace_entire() {
        assert_eq!(eval("replace", &[s("abc"), s("abc"), s("xyz")]), s("xyz"));
    }
}

// ===========================================================================
// starts_with
// ===========================================================================
mod starts_with_tests {
    use super::*;

    #[test]
    fn true_case() {
        assert_eq!(eval("starts_with", &[s("hello"), s("hel")]), i(1));
    }
    #[test]
    fn false_case() {
        assert_eq!(eval("starts_with", &[s("hello"), s("xyz")]), i(0));
    }
    #[test]
    fn empty_prefix() {
        assert_eq!(eval("starts_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("starts_with", &[null(), s("a")]), null());
    }
    #[test]
    fn exact_match() {
        assert_eq!(eval("starts_with", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn longer_prefix() {
        assert_eq!(eval("starts_with", &[s("ab"), s("abc")]), i(0));
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("starts_with", &[s("hello"), s("h")]), i(1));
    }
    #[test]
    fn case_sensitive() {
        assert_eq!(eval("starts_with", &[s("Hello"), s("hello")]), i(0));
    }
    #[test]
    fn empty_source() {
        assert_eq!(eval("starts_with", &[s(""), s("a")]), i(0));
    }
    #[test]
    fn both_empty() {
        assert_eq!(eval("starts_with", &[s(""), s("")]), i(1));
    }
}

// ===========================================================================
// ends_with
// ===========================================================================
mod ends_with_tests {
    use super::*;

    #[test]
    fn true_case() {
        assert_eq!(eval("ends_with", &[s("hello"), s("llo")]), i(1));
    }
    #[test]
    fn false_case() {
        assert_eq!(eval("ends_with", &[s("hello"), s("xyz")]), i(0));
    }
    #[test]
    fn empty_suffix() {
        assert_eq!(eval("ends_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("ends_with", &[null(), s("a")]), null());
    }
    #[test]
    fn exact_match() {
        assert_eq!(eval("ends_with", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn longer_suffix() {
        assert_eq!(eval("ends_with", &[s("ab"), s("abc")]), i(0));
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("ends_with", &[s("hello"), s("o")]), i(1));
    }
    #[test]
    fn case_sensitive() {
        assert_eq!(eval("ends_with", &[s("Hello"), s("HELLO")]), i(0));
    }
    #[test]
    fn empty_source() {
        assert_eq!(eval("ends_with", &[s(""), s("a")]), i(0));
    }
    #[test]
    fn both_empty() {
        assert_eq!(eval("ends_with", &[s(""), s("")]), i(1));
    }
}

// ===========================================================================
// contains
// ===========================================================================
mod contains_tests {
    use super::*;

    #[test]
    fn true_case() {
        assert_eq!(eval("contains", &[s("hello world"), s("lo wo")]), i(1));
    }
    #[test]
    fn false_case() {
        assert_eq!(eval("contains", &[s("hello"), s("xyz")]), i(0));
    }
    #[test]
    fn at_start() {
        assert_eq!(eval("contains", &[s("hello"), s("hel")]), i(1));
    }
    #[test]
    fn at_end() {
        assert_eq!(eval("contains", &[s("hello"), s("llo")]), i(1));
    }
    #[test]
    fn empty_needle() {
        assert_eq!(eval("contains", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("contains", &[null(), s("a")]), null());
    }
    #[test]
    fn exact_match() {
        assert_eq!(eval("contains", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn case_sensitive() {
        assert_eq!(eval("contains", &[s("Hello"), s("hello")]), i(0));
    }
    #[test]
    fn empty_haystack() {
        assert_eq!(eval("contains", &[s(""), s("a")]), i(0));
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("contains", &[s("abc"), s("b")]), i(1));
    }
}

// ===========================================================================
// reverse
// ===========================================================================
mod reverse_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("reverse", &[s("hello")]), s("olleh"));
    }
    #[test]
    fn empty() {
        assert_eq!(eval("reverse", &[s("")]), s(""));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("reverse", &[null()]), null());
    }
    #[test]
    fn palindrome() {
        assert_eq!(eval("reverse", &[s("racecar")]), s("racecar"));
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("reverse", &[s("a")]), s("a"));
    }
    #[test]
    fn digits() {
        assert_eq!(eval("reverse", &[s("12345")]), s("54321"));
    }
    #[test]
    fn with_spaces() {
        assert_eq!(eval("reverse", &[s("a b")]), s("b a"));
    }
    #[test]
    fn special_chars() {
        assert_eq!(eval("reverse", &[s("!@#")]), s("#@!"));
    }
    #[test]
    fn two_chars() {
        assert_eq!(eval("reverse", &[s("ab")]), s("ba"));
    }
    #[test]
    fn number_input() {
        assert_eq!(eval("reverse", &[i(123)]), s("321"));
    }
}

// ===========================================================================
// repeat
// ===========================================================================
mod repeat_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("repeat", &[s("ab"), i(3)]), s("ababab"));
    }
    #[test]
    fn zero_times() {
        assert_eq!(eval("repeat", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn once() {
        assert_eq!(eval("repeat", &[s("hello"), i(1)]), s("hello"));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("repeat", &[null(), i(3)]), null());
    }
    #[test]
    fn negative() {
        assert_eq!(eval("repeat", &[s("a"), i(-1)]), s(""));
    }
    #[test]
    fn empty_string() {
        assert_eq!(eval("repeat", &[s(""), i(5)]), s(""));
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("repeat", &[s("x"), i(5)]), s("xxxxx"));
    }
    #[test]
    fn space() {
        assert_eq!(eval("repeat", &[s(" "), i(3)]), s("   "));
    }
    #[test]
    fn two_times() {
        assert_eq!(eval("repeat", &[s("ha"), i(2)]), s("haha"));
    }
    #[test]
    fn large() {
        assert_eq!(eval("repeat", &[s("a"), i(100)]), s(&"a".repeat(100)));
    }
}

// ===========================================================================
// left
// ===========================================================================
mod left_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("left", &[s("hello"), i(3)]), s("hel"));
    }
    #[test]
    fn full() {
        assert_eq!(eval("left", &[s("hello"), i(10)]), s("hello"));
    }
    #[test]
    fn zero() {
        assert_eq!(eval("left", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("left", &[null(), i(3)]), null());
    }
    #[test]
    fn negative() {
        assert_eq!(eval("left", &[s("hello"), i(-1)]), s(""));
    }
    #[test]
    fn empty() {
        assert_eq!(eval("left", &[s(""), i(3)]), s(""));
    }
    #[test]
    fn one() {
        assert_eq!(eval("left", &[s("hello"), i(1)]), s("h"));
    }
    #[test]
    fn exact_len() {
        assert_eq!(eval("left", &[s("abc"), i(3)]), s("abc"));
    }
    #[test]
    fn single_char_input() {
        assert_eq!(eval("left", &[s("a"), i(5)]), s("a"));
    }
    #[test]
    fn two() {
        assert_eq!(eval("left", &[s("abcde"), i(2)]), s("ab"));
    }
}

// ===========================================================================
// right
// ===========================================================================
mod right_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("right", &[s("hello"), i(3)]), s("llo"));
    }
    #[test]
    fn full() {
        assert_eq!(eval("right", &[s("hello"), i(10)]), s("hello"));
    }
    #[test]
    fn zero() {
        assert_eq!(eval("right", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("right", &[null(), i(3)]), null());
    }
    #[test]
    fn negative() {
        assert_eq!(eval("right", &[s("hello"), i(-1)]), s(""));
    }
    #[test]
    fn empty() {
        assert_eq!(eval("right", &[s(""), i(3)]), s(""));
    }
    #[test]
    fn one() {
        assert_eq!(eval("right", &[s("hello"), i(1)]), s("o"));
    }
    #[test]
    fn exact_len() {
        assert_eq!(eval("right", &[s("abc"), i(3)]), s("abc"));
    }
    #[test]
    fn single_char_input() {
        assert_eq!(eval("right", &[s("z"), i(5)]), s("z"));
    }
    #[test]
    fn two() {
        assert_eq!(eval("right", &[s("abcde"), i(2)]), s("de"));
    }
}

// ===========================================================================
// position
// ===========================================================================
mod position_tests {
    use super::*;

    #[test]
    fn found() {
        assert_eq!(eval("position", &[s("lo"), s("hello")]), i(4));
    }
    #[test]
    fn not_found() {
        assert_eq!(eval("position", &[s("xyz"), s("hello")]), i(0));
    }
    #[test]
    fn at_start() {
        assert_eq!(eval("position", &[s("hel"), s("hello")]), i(1));
    }
    #[test]
    fn at_end() {
        assert_eq!(eval("position", &[s("llo"), s("hello")]), i(3));
    }
    #[test]
    fn empty_substr() {
        assert_eq!(eval("position", &[s(""), s("hello")]), i(1));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("position", &[null(), s("hello")]), null());
    }
    #[test]
    fn null_haystack() {
        assert_eq!(eval("position", &[s("a"), null()]), null());
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("position", &[s("e"), s("hello")]), i(2));
    }
    #[test]
    fn case_sensitive() {
        assert_eq!(eval("position", &[s("H"), s("hello")]), i(0));
    }
    #[test]
    fn same_string() {
        assert_eq!(eval("position", &[s("abc"), s("abc")]), i(1));
    }
}

// ===========================================================================
// lpad
// ===========================================================================
mod lpad_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("lpad", &[s("hi"), i(5), s(" ")]), s("   hi"));
    }
    #[test]
    fn no_padding() {
        assert_eq!(eval("lpad", &[s("hello"), i(5), s(" ")]), s("hello"));
    }
    #[test]
    fn truncate() {
        assert_eq!(eval("lpad", &[s("hello"), i(3), s(" ")]), s("hel"));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("lpad", &[null(), i(5), s(" ")]), null());
    }
    #[test]
    fn multi_char_pad() {
        assert_eq!(eval("lpad", &[s("x"), i(5), s("ab")]), s("ababx"));
    }
    #[test]
    fn zero_len() {
        assert_eq!(eval("lpad", &[s("hello"), i(0), s(" ")]), s(""));
    }
    #[test]
    fn pad_with_zero() {
        assert_eq!(eval("lpad", &[s("42"), i(5), s("0")]), s("00042"));
    }
    #[test]
    fn empty_input() {
        assert_eq!(eval("lpad", &[s(""), i(3), s("x")]), s("xxx"));
    }
    #[test]
    fn one_pad() {
        assert_eq!(eval("lpad", &[s("ab"), i(3), s("x")]), s("xab"));
    }
    #[test]
    fn exact_fit() {
        assert_eq!(eval("lpad", &[s("abc"), i(3), s("x")]), s("abc"));
    }
}

// ===========================================================================
// rpad
// ===========================================================================
mod rpad_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("rpad", &[s("hi"), i(5), s(" ")]), s("hi   "));
    }
    #[test]
    fn no_padding() {
        assert_eq!(eval("rpad", &[s("hello"), i(5), s(" ")]), s("hello"));
    }
    #[test]
    fn truncate() {
        assert_eq!(eval("rpad", &[s("hello"), i(3), s(" ")]), s("hel"));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("rpad", &[null(), i(5), s(" ")]), null());
    }
    #[test]
    fn multi_char_pad() {
        assert_eq!(eval("rpad", &[s("x"), i(5), s("ab")]), s("xabab"));
    }
    #[test]
    fn zero_len() {
        assert_eq!(eval("rpad", &[s("hello"), i(0), s(" ")]), s(""));
    }
    #[test]
    fn pad_with_zero() {
        assert_eq!(eval("rpad", &[s("42"), i(5), s("0")]), s("42000"));
    }
    #[test]
    fn empty_input() {
        assert_eq!(eval("rpad", &[s(""), i(3), s("x")]), s("xxx"));
    }
    #[test]
    fn one_pad() {
        assert_eq!(eval("rpad", &[s("ab"), i(3), s("x")]), s("abx"));
    }
    #[test]
    fn exact_fit() {
        assert_eq!(eval("rpad", &[s("abc"), i(3), s("x")]), s("abc"));
    }
}

// ===========================================================================
// split_part
// ===========================================================================
mod split_part_tests {
    use super::*;

    #[test]
    fn first_part() {
        assert_eq!(eval("split_part", &[s("a,b,c"), s(","), i(1)]), s("a"));
    }
    #[test]
    fn second_part() {
        assert_eq!(eval("split_part", &[s("a,b,c"), s(","), i(2)]), s("b"));
    }
    #[test]
    fn third_part() {
        assert_eq!(eval("split_part", &[s("a,b,c"), s(","), i(3)]), s("c"));
    }
    #[test]
    fn out_of_range() {
        assert_eq!(eval("split_part", &[s("a,b"), s(","), i(5)]), s(""));
    }
    #[test]
    fn zero_index() {
        assert_eq!(eval("split_part", &[s("a,b"), s(","), i(0)]), s(""));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("split_part", &[null(), s(","), i(1)]), null());
    }
    #[test]
    fn no_delim() {
        assert_eq!(eval("split_part", &[s("hello"), s(","), i(1)]), s("hello"));
    }
    #[test]
    fn multi_char_delim() {
        assert_eq!(eval("split_part", &[s("a::b::c"), s("::"), i(2)]), s("b"));
    }
    #[test]
    fn empty_parts() {
        assert_eq!(eval("split_part", &[s(",a,"), s(","), i(1)]), s(""));
    }
    #[test]
    fn space_delim() {
        assert_eq!(
            eval("split_part", &[s("hello world"), s(" "), i(2)]),
            s("world")
        );
    }
}

// ===========================================================================
// initcap
// ===========================================================================
mod initcap_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("initcap", &[s("hello world")]), s("Hello World"));
    }
    #[test]
    fn all_lower() {
        assert_eq!(eval("initcap", &[s("abc def")]), s("Abc Def"));
    }
    #[test]
    fn all_upper() {
        assert_eq!(eval("initcap", &[s("ABC DEF")]), s("Abc Def"));
    }
    #[test]
    fn single_word() {
        assert_eq!(eval("initcap", &[s("hello")]), s("Hello"));
    }
    #[test]
    fn empty() {
        assert_eq!(eval("initcap", &[s("")]), s(""));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("initcap", &[null()]), null());
    }
    #[test]
    fn hyphenated() {
        assert_eq!(eval("initcap", &[s("jean-paul")]), s("Jean-Paul"));
    }
    #[test]
    fn multiple_spaces() {
        assert_eq!(eval("initcap", &[s("a  b")]), s("A  B"));
    }
    #[test]
    fn with_numbers() {
        assert_eq!(eval("initcap", &[s("hello 2world")]), s("Hello 2world"));
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("initcap", &[s("a")]), s("A"));
    }
}

// ===========================================================================
// char_length
// ===========================================================================
mod char_length_tests {
    use super::*;

    #[test]
    fn ascii() {
        assert_eq!(eval("char_length", &[s("hello")]), i(5));
    }
    #[test]
    fn empty() {
        assert_eq!(eval("char_length", &[s("")]), i(0));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("char_length", &[null()]), null());
    }
    #[test]
    fn single() {
        assert_eq!(eval("char_length", &[s("a")]), i(1));
    }
    #[test]
    fn with_spaces() {
        assert_eq!(eval("char_length", &[s("a b c")]), i(5));
    }
    #[test]
    fn digits() {
        assert_eq!(eval("char_length", &[s("12345")]), i(5));
    }
    #[test]
    fn special() {
        assert_eq!(eval("char_length", &[s("!@#")]), i(3));
    }
    #[test]
    fn newlines() {
        assert_eq!(eval("char_length", &[s("a\nb")]), i(3));
    }
    #[test]
    fn tabs() {
        assert_eq!(eval("char_length", &[s("a\tb")]), i(3));
    }
    #[test]
    fn number_input() {
        assert_eq!(eval("char_length", &[i(999)]), i(3));
    }
}

// ===========================================================================
// ascii
// ===========================================================================
mod ascii_tests {
    use super::*;

    #[test]
    fn letter_a() {
        assert_eq!(eval("ascii", &[s("A")]), i(65));
    }
    #[test]
    fn letter_z() {
        assert_eq!(eval("ascii", &[s("z")]), i(122));
    }
    #[test]
    fn digit_0() {
        assert_eq!(eval("ascii", &[s("0")]), i(48));
    }
    #[test]
    fn space() {
        assert_eq!(eval("ascii", &[s(" ")]), i(32));
    }
    #[test]
    fn exclaim() {
        assert_eq!(eval("ascii", &[s("!")]), i(33));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("ascii", &[null()]), null());
    }
    #[test]
    fn empty_string() {
        assert_eq!(eval("ascii", &[s("")]), i(0));
    }
    #[test]
    fn multi_char() {
        assert_eq!(eval("ascii", &[s("Hello")]), i(72));
    }
    #[test]
    fn newline() {
        assert_eq!(eval("ascii", &[s("\n")]), i(10));
    }
    #[test]
    fn tab() {
        assert_eq!(eval("ascii", &[s("\t")]), i(9));
    }
}

// ===========================================================================
// chr
// ===========================================================================
mod chr_tests {
    use super::*;

    #[test]
    fn letter_a() {
        assert_eq!(eval("chr", &[i(65)]), s("A"));
    }
    #[test]
    fn letter_z() {
        assert_eq!(eval("chr", &[i(122)]), s("z"));
    }
    #[test]
    fn digit_0() {
        assert_eq!(eval("chr", &[i(48)]), s("0"));
    }
    #[test]
    fn space() {
        assert_eq!(eval("chr", &[i(32)]), s(" "));
    }
    #[test]
    fn exclaim() {
        assert_eq!(eval("chr", &[i(33)]), s("!"));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("chr", &[null()]), null());
    }
    #[test]
    fn newline() {
        assert_eq!(eval("chr", &[i(10)]), s("\n"));
    }
    #[test]
    fn tab() {
        assert_eq!(eval("chr", &[i(9)]), s("\t"));
    }
    #[test]
    fn at_sign() {
        assert_eq!(eval("chr", &[i(64)]), s("@"));
    }
    #[test]
    fn tilde() {
        assert_eq!(eval("chr", &[i(126)]), s("~"));
    }
}

// ===========================================================================
// char_at
// ===========================================================================
mod char_at_tests {
    use super::*;

    #[test]
    fn first() {
        assert_eq!(eval("char_at", &[s("hello"), i(1)]), s("h"));
    }
    #[test]
    fn last() {
        assert_eq!(eval("char_at", &[s("hello"), i(5)]), s("o"));
    }
    #[test]
    fn middle() {
        assert_eq!(eval("char_at", &[s("hello"), i(3)]), s("l"));
    }
    #[test]
    fn out_of_range() {
        assert_eq!(eval("char_at", &[s("hi"), i(10)]), null());
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("char_at", &[null(), i(1)]), null());
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("char_at", &[s("a"), i(1)]), s("a"));
    }
    #[test]
    fn second() {
        assert_eq!(eval("char_at", &[s("abc"), i(2)]), s("b"));
    }
    #[test]
    fn digit_char() {
        assert_eq!(eval("char_at", &[s("123"), i(2)]), s("2"));
    }
    #[test]
    fn space_char() {
        assert_eq!(eval("char_at", &[s("a b"), i(2)]), s(" "));
    }
    #[test]
    fn empty_string() {
        assert_eq!(eval("char_at", &[s(""), i(1)]), null());
    }
}

// ===========================================================================
// hex
// ===========================================================================
mod hex_tests {
    use super::*;

    #[test]
    fn integer() {
        assert_eq!(eval("hex", &[i(255)]), s("ff"));
    }
    #[test]
    fn zero() {
        assert_eq!(eval("hex", &[i(0)]), s("0"));
    }
    #[test]
    fn sixteen() {
        assert_eq!(eval("hex", &[i(16)]), s("10"));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("hex", &[null()]), null());
    }
    #[test]
    fn string_input() {
        assert_eq!(eval("hex", &[s("AB")]), s("4142"));
    }
    #[test]
    fn one() {
        assert_eq!(eval("hex", &[i(1)]), s("1"));
    }
    #[test]
    fn large() {
        assert_eq!(eval("hex", &[i(4096)]), s("1000"));
    }
    #[test]
    fn string_hello() {
        assert_eq!(eval("hex", &[s("hello")]), s("68656c6c6f"));
    }
    #[test]
    fn ten() {
        assert_eq!(eval("hex", &[i(10)]), s("a"));
    }
    #[test]
    fn two_fifty_six() {
        assert_eq!(eval("hex", &[i(256)]), s("100"));
    }
}

// ===========================================================================
// unhex
// ===========================================================================
mod unhex_tests {
    use super::*;

    #[test]
    fn ff() {
        assert_eq!(eval("unhex", &[s("ff")]), i(255));
    }
    #[test]
    fn zero() {
        assert_eq!(eval("unhex", &[s("0")]), i(0));
    }
    #[test]
    fn ten_hex() {
        assert_eq!(eval("unhex", &[s("10")]), i(16));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("unhex", &[null()]), null());
    }
    #[test]
    fn one() {
        assert_eq!(eval("unhex", &[s("1")]), i(1));
    }
    #[test]
    fn a() {
        assert_eq!(eval("unhex", &[s("a")]), i(10));
    }
    #[test]
    fn uppercase() {
        assert_eq!(eval("unhex", &[s("FF")]), i(255));
    }
    #[test]
    fn hundred_hex() {
        assert_eq!(eval("unhex", &[s("100")]), i(256));
    }
    #[test]
    fn thousand_hex() {
        assert_eq!(eval("unhex", &[s("1000")]), i(4096));
    }
    #[test]
    fn deadbeef() {
        assert_eq!(eval("unhex", &[s("deadbeef")]), i(0xdeadbeef));
    }
}

// ===========================================================================
// regexp_match
// ===========================================================================
mod regexp_match_tests {
    use super::*;

    #[test]
    fn matches() {
        assert_eq!(eval("regexp_match", &[s("hello123"), s(r"\d+")]), i(1));
    }
    #[test]
    fn no_match() {
        assert_eq!(eval("regexp_match", &[s("hello"), s(r"\d+")]), i(0));
    }
    #[test]
    fn full_match() {
        assert_eq!(eval("regexp_match", &[s("abc"), s("^abc$")]), i(1));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("regexp_match", &[null(), s("a")]), null());
    }
    #[test]
    fn dot_star() {
        assert_eq!(eval("regexp_match", &[s("anything"), s(".*")]), i(1));
    }
    #[test]
    fn email_like() {
        assert_eq!(eval("regexp_match", &[s("a@b.com"), s(r"@")]), i(1));
    }
    #[test]
    fn start_anchor() {
        assert_eq!(eval("regexp_match", &[s("hello"), s("^hel")]), i(1));
    }
    #[test]
    fn end_anchor() {
        assert_eq!(eval("regexp_match", &[s("hello"), s("llo$")]), i(1));
    }
    #[test]
    fn word_boundary() {
        assert_eq!(
            eval("regexp_match", &[s("hello world"), s(r"\bworld\b")]),
            i(1)
        );
    }
    #[test]
    fn char_class() {
        assert_eq!(eval("regexp_match", &[s("abc"), s("[a-z]+")]), i(1));
    }
}

// ===========================================================================
// regexp_replace
// ===========================================================================
mod regexp_replace_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(
            eval(
                "regexp_replace",
                &[s("hello 123 world"), s(r"\d+"), s("NUM")]
            ),
            s("hello NUM world")
        );
    }
    #[test]
    fn no_match() {
        assert_eq!(
            eval("regexp_replace", &[s("abc"), s(r"\d+"), s("X")]),
            s("abc")
        );
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("regexp_replace", &[null(), s("a"), s("b")]), null());
    }
    #[test]
    fn remove() {
        assert_eq!(
            eval("regexp_replace", &[s("a1b2c3"), s(r"\d"), s("")]),
            s("abc")
        );
    }
    #[test]
    fn replace_all() {
        assert_eq!(
            eval("regexp_replace", &[s("aaa"), s("a"), s("b")]),
            s("bbb")
        );
    }
    #[test]
    fn spaces() {
        assert_eq!(
            eval("regexp_replace", &[s("a  b  c"), s(r"\s+"), s(" ")]),
            s("a b c")
        );
    }
    #[test]
    fn start_anchor() {
        assert_eq!(
            eval("regexp_replace", &[s("abc"), s("^a"), s("X")]),
            s("Xbc")
        );
    }
    #[test]
    fn end_anchor() {
        assert_eq!(
            eval("regexp_replace", &[s("abc"), s("c$"), s("X")]),
            s("abX")
        );
    }
    #[test]
    fn empty_source() {
        assert_eq!(eval("regexp_replace", &[s(""), s("a"), s("b")]), s(""));
    }
    #[test]
    fn dot() {
        assert_eq!(
            eval("regexp_replace", &[s("abc"), s("."), s("X")]),
            s("XXX")
        );
    }
}

// ===========================================================================
// regexp_extract
// ===========================================================================
mod regexp_extract_tests {
    use super::*;

    #[test]
    fn group_0() {
        assert_eq!(
            eval("regexp_extract", &[s("hello 123"), s(r"(\d+)"), i(0)]),
            s("123")
        );
    }
    #[test]
    fn group_1() {
        assert_eq!(
            eval("regexp_extract", &[s("hello 123"), s(r"(\d+)"), i(1)]),
            s("123")
        );
    }
    #[test]
    fn no_match() {
        assert_eq!(
            eval("regexp_extract", &[s("hello"), s(r"(\d+)"), i(1)]),
            null()
        );
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("regexp_extract", &[null(), s("a"), i(0)]), null());
    }
    #[test]
    fn multi_group() {
        assert_eq!(
            eval(
                "regexp_extract",
                &[s("2024-01-15"), s(r"(\d{4})-(\d{2})-(\d{2})"), i(1)]
            ),
            s("2024")
        );
    }
    #[test]
    fn multi_group_2() {
        assert_eq!(
            eval(
                "regexp_extract",
                &[s("2024-01-15"), s(r"(\d{4})-(\d{2})-(\d{2})"), i(2)]
            ),
            s("01")
        );
    }
    #[test]
    fn multi_group_3() {
        assert_eq!(
            eval(
                "regexp_extract",
                &[s("2024-01-15"), s(r"(\d{4})-(\d{2})-(\d{2})"), i(3)]
            ),
            s("15")
        );
    }
    #[test]
    fn bad_group() {
        assert_eq!(
            eval("regexp_extract", &[s("hello"), s(r"(hel)"), i(5)]),
            null()
        );
    }
    #[test]
    fn word_group() {
        assert_eq!(
            eval("regexp_extract", &[s("foo bar"), s(r"(\w+)\s(\w+)"), i(2)]),
            s("bar")
        );
    }
    #[test]
    fn empty_match() {
        assert_eq!(eval("regexp_extract", &[s("abc"), s(r"(x?)"), i(1)]), s(""));
    }
}

// ===========================================================================
// md5
// ===========================================================================
mod md5_tests {
    use super::*;

    #[test]
    fn empty_string() {
        assert_eq!(eval("md5", &[s("")]), s("d41d8cd98f00b204e9800998ecf8427e"));
    }
    #[test]
    fn hello() {
        let result = eval("md5", &[s("hello")]);
        match result {
            Value::Str(h) => assert_eq!(h.len(), 32),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("md5", &[null()]), null());
    }
    #[test]
    fn abc() {
        let result = eval("md5", &[s("abc")]);
        match result {
            Value::Str(h) => assert_eq!(h.len(), 32),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn deterministic() {
        assert_eq!(eval("md5", &[s("test")]), eval("md5", &[s("test")]));
    }
    #[test]
    fn different_inputs() {
        assert_ne!(eval("md5", &[s("a")]), eval("md5", &[s("b")]));
    }
    #[test]
    fn number_input() {
        let r = eval("md5", &[i(42)]);
        match r {
            Value::Str(h) => assert_eq!(h.len(), 32),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn space() {
        let r = eval("md5", &[s(" ")]);
        match r {
            Value::Str(h) => assert_eq!(h.len(), 32),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn long_input() {
        let r = eval("md5", &[s(&"x".repeat(1000))]);
        match r {
            Value::Str(h) => assert_eq!(h.len(), 32),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn case_sensitive() {
        assert_ne!(eval("md5", &[s("A")]), eval("md5", &[s("a")]));
    }
}

// ===========================================================================
// sha256
// ===========================================================================
mod sha256_tests {
    use super::*;

    #[test]
    fn empty_string() {
        let r = eval("sha256", &[s("")]);
        match r {
            Value::Str(h) => assert_eq!(h.len(), 64),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn hello() {
        let r = eval("sha256", &[s("hello")]);
        match r {
            Value::Str(h) => assert_eq!(h.len(), 64),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("sha256", &[null()]), null());
    }
    #[test]
    fn deterministic() {
        assert_eq!(eval("sha256", &[s("test")]), eval("sha256", &[s("test")]));
    }
    #[test]
    fn different_inputs() {
        assert_ne!(eval("sha256", &[s("a")]), eval("sha256", &[s("b")]));
    }
    #[test]
    fn long_input() {
        let r = eval("sha256", &[s(&"y".repeat(1000))]);
        match r {
            Value::Str(h) => assert_eq!(h.len(), 64),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn number_input() {
        let r = eval("sha256", &[i(42)]);
        match r {
            Value::Str(h) => assert_eq!(h.len(), 64),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn case_sensitive() {
        assert_ne!(eval("sha256", &[s("A")]), eval("sha256", &[s("a")]));
    }
}

// ===========================================================================
// to_base64 / from_base64
// ===========================================================================
mod base64_tests {
    use super::*;

    #[test]
    fn encode_hello() {
        assert_eq!(eval("to_base64", &[s("hello")]), s("aGVsbG8="));
    }
    #[test]
    fn encode_empty() {
        assert_eq!(eval("to_base64", &[s("")]), s(""));
    }
    #[test]
    fn encode_null() {
        assert_eq!(eval("to_base64", &[null()]), null());
    }
    #[test]
    fn encode_a() {
        assert_eq!(eval("to_base64", &[s("a")]), s("YQ=="));
    }
    #[test]
    fn encode_ab() {
        assert_eq!(eval("to_base64", &[s("ab")]), s("YWI="));
    }
    #[test]
    fn encode_abc() {
        assert_eq!(eval("to_base64", &[s("abc")]), s("YWJj"));
    }
    #[test]
    fn decode_hello() {
        assert_eq!(eval("from_base64", &[s("aGVsbG8=")]), s("hello"));
    }
    #[test]
    fn decode_null() {
        assert_eq!(eval("from_base64", &[null()]), null());
    }
    #[test]
    fn decode_a() {
        assert_eq!(eval("from_base64", &[s("YQ==")]), s("a"));
    }
    #[test]
    fn roundtrip() {
        let encoded = eval("to_base64", &[s("test data")]);
        assert_eq!(eval("from_base64", &[encoded]), s("test data"));
    }
}

// ===========================================================================
// encode / decode (base64 format)
// ===========================================================================
mod encode_decode_tests {
    use super::*;

    #[test]
    fn encode_base64() {
        assert_eq!(eval("encode", &[s("hello"), s("base64")]), s("aGVsbG8="));
    }
    #[test]
    fn decode_base64() {
        assert_eq!(eval("decode", &[s("aGVsbG8="), s("base64")]), s("hello"));
    }
    #[test]
    fn encode_null() {
        assert_eq!(eval("encode", &[null(), s("base64")]), null());
    }
    #[test]
    fn decode_null() {
        assert_eq!(eval("decode", &[null(), s("base64")]), null());
    }
    #[test]
    fn roundtrip() {
        let encoded = eval("encode", &[s("test 123"), s("base64")]);
        assert_eq!(eval("decode", &[encoded, s("base64")]), s("test 123"));
    }
    #[test]
    fn encode_empty() {
        assert_eq!(eval("encode", &[s(""), s("base64")]), s(""));
    }
    #[test]
    fn bad_format() {
        assert!(eval_err("encode", &[s("x"), s("unknown")]).contains("unsupported"));
    }
}

// ===========================================================================
// quote_ident / quote_literal
// ===========================================================================
mod quote_tests {
    use super::*;

    #[test]
    fn ident_basic() {
        assert_eq!(eval("quote_ident", &[s("table")]), s("\"table\""));
    }
    #[test]
    fn ident_null() {
        assert_eq!(eval("quote_ident", &[null()]), null());
    }
    #[test]
    fn ident_with_quote() {
        assert_eq!(eval("quote_ident", &[s("ta\"ble")]), s("\"ta\"\"ble\""));
    }
    #[test]
    fn ident_empty() {
        assert_eq!(eval("quote_ident", &[s("")]), s("\"\""));
    }
    #[test]
    fn ident_spaces() {
        assert_eq!(eval("quote_ident", &[s("my table")]), s("\"my table\""));
    }

    #[test]
    fn literal_basic() {
        assert_eq!(eval("quote_literal", &[s("hello")]), s("'hello'"));
    }
    #[test]
    fn literal_null() {
        assert_eq!(eval("quote_literal", &[null()]), null());
    }
    #[test]
    fn literal_with_quote() {
        assert_eq!(eval("quote_literal", &[s("it's")]), s("'it''s'"));
    }
    #[test]
    fn literal_empty() {
        assert_eq!(eval("quote_literal", &[s("")]), s("''"));
    }
    #[test]
    fn literal_number() {
        assert_eq!(eval("quote_literal", &[i(42)]), s("'42'"));
    }
}

// ===========================================================================
// url_encode / url_decode
// ===========================================================================
mod url_tests {
    use super::*;

    #[test]
    fn encode_basic() {
        assert_eq!(eval("url_encode", &[s("hello world")]), s("hello%20world"));
    }
    #[test]
    fn encode_special() {
        assert_eq!(eval("url_encode", &[s("a&b=c")]), s("a%26b%3Dc"));
    }
    #[test]
    fn encode_null() {
        assert_eq!(eval("url_encode", &[null()]), null());
    }
    #[test]
    fn encode_empty() {
        assert_eq!(eval("url_encode", &[s("")]), s(""));
    }
    #[test]
    fn encode_plain() {
        assert_eq!(eval("url_encode", &[s("abc")]), s("abc"));
    }
    #[test]
    fn decode_basic() {
        assert_eq!(eval("url_decode", &[s("hello%20world")]), s("hello world"));
    }
    #[test]
    fn decode_plus() {
        assert_eq!(eval("url_decode", &[s("hello+world")]), s("hello world"));
    }
    #[test]
    fn decode_null() {
        assert_eq!(eval("url_decode", &[null()]), null());
    }
    #[test]
    fn decode_empty() {
        assert_eq!(eval("url_decode", &[s("")]), s(""));
    }
    #[test]
    fn roundtrip() {
        let encoded = eval("url_encode", &[s("a b&c=d")]);
        assert_eq!(eval("url_decode", &[encoded]), s("a b&c=d"));
    }
}

// ===========================================================================
// soundex
// ===========================================================================
mod soundex_tests {
    use super::*;

    #[test]
    fn robert() {
        assert_eq!(eval("soundex", &[s("Robert")]), s("R163"));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("soundex", &[null()]), null());
    }
    #[test]
    fn empty() {
        assert_eq!(eval("soundex", &[s("")]), s("0000"));
    }
    #[test]
    fn a() {
        assert_eq!(eval("soundex", &[s("A")]), s("A000"));
    }
    #[test]
    fn smith() {
        assert_eq!(eval("soundex", &[s("Smith")]), s("S530"));
    }
    #[test]
    fn smythe() {
        assert_eq!(eval("soundex", &[s("Smythe")]), s("S530"));
    }
    #[test]
    fn ashcraft() {
        let r = eval("soundex", &[s("Ashcraft")]);
        match r {
            Value::Str(h) => assert_eq!(h.len(), 4),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("soundex", &[s("Z")]), s("Z000"));
    }
}

// ===========================================================================
// word_count
// ===========================================================================
mod word_count_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("word_count", &[s("hello world")]), i(2));
    }
    #[test]
    fn single() {
        assert_eq!(eval("word_count", &[s("hello")]), i(1));
    }
    #[test]
    fn empty() {
        assert_eq!(eval("word_count", &[s("")]), i(0));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("word_count", &[null()]), null());
    }
    #[test]
    fn multiple_spaces() {
        assert_eq!(eval("word_count", &[s("a  b  c")]), i(3));
    }
    #[test]
    fn tabs() {
        assert_eq!(eval("word_count", &[s("a\tb\tc")]), i(3));
    }
    #[test]
    fn newlines() {
        assert_eq!(eval("word_count", &[s("a\nb\nc")]), i(3));
    }
    #[test]
    fn only_spaces() {
        assert_eq!(eval("word_count", &[s("   ")]), i(0));
    }
    #[test]
    fn three_words() {
        assert_eq!(eval("word_count", &[s("one two three")]), i(3));
    }
    #[test]
    fn leading_trailing() {
        assert_eq!(eval("word_count", &[s("  hello  ")]), i(1));
    }
}

// ===========================================================================
// camel_case
// ===========================================================================
mod camel_case_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("camel_case", &[s("hello_world")]), s("HelloWorld"));
    }
    #[test]
    fn spaces() {
        assert_eq!(eval("camel_case", &[s("hello world")]), s("HelloWorld"));
    }
    #[test]
    fn dashes() {
        assert_eq!(eval("camel_case", &[s("hello-world")]), s("HelloWorld"));
    }
    #[test]
    fn single() {
        assert_eq!(eval("camel_case", &[s("hello")]), s("Hello"));
    }
    #[test]
    fn empty() {
        assert_eq!(eval("camel_case", &[s("")]), s(""));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("camel_case", &[null()]), null());
    }
    #[test]
    fn already_camel() {
        assert_eq!(eval("camel_case", &[s("HelloWorld")]), s("HelloWorld"));
    }
    #[test]
    fn upper() {
        assert_eq!(eval("camel_case", &[s("HELLO_WORLD")]), s("HELLOWORLD"));
    }
    #[test]
    fn mixed_sep() {
        assert_eq!(eval("camel_case", &[s("a_b-c d")]), s("ABCD"));
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("camel_case", &[s("a")]), s("A"));
    }
}

// ===========================================================================
// snake_case
// ===========================================================================
mod snake_case_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("snake_case", &[s("HelloWorld")]), s("hello_world"));
    }
    #[test]
    fn already_snake() {
        assert_eq!(eval("snake_case", &[s("hello_world")]), s("hello_world"));
    }
    #[test]
    fn all_lower() {
        assert_eq!(eval("snake_case", &[s("hello")]), s("hello"));
    }
    #[test]
    fn empty() {
        assert_eq!(eval("snake_case", &[s("")]), s(""));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("snake_case", &[null()]), null());
    }
    #[test]
    fn single_upper() {
        assert_eq!(eval("snake_case", &[s("A")]), s("a"));
    }
    #[test]
    fn two_words() {
        assert_eq!(eval("snake_case", &[s("MyVar")]), s("my_var"));
    }
    #[test]
    fn three_words() {
        assert_eq!(eval("snake_case", &[s("MyVarName")]), s("my_var_name"));
    }
    #[test]
    fn all_upper() {
        assert_eq!(eval("snake_case", &[s("ABC")]), s("a_b_c"));
    }
    #[test]
    fn with_numbers() {
        assert_eq!(eval("snake_case", &[s("Test123")]), s("test123"));
    }
}

// ===========================================================================
// squeeze
// ===========================================================================
mod squeeze_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("squeeze", &[s("hello   world")]), s("hello world"));
    }
    #[test]
    fn no_change() {
        assert_eq!(eval("squeeze", &[s("hello world")]), s("hello world"));
    }
    #[test]
    fn empty() {
        assert_eq!(eval("squeeze", &[s("")]), s(""));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("squeeze", &[null()]), null());
    }
    #[test]
    fn only_spaces() {
        assert_eq!(eval("squeeze", &[s("     ")]), s(""));
    }
    #[test]
    fn tabs_and_spaces() {
        assert_eq!(eval("squeeze", &[s("a \t b")]), s("a b"));
    }
    #[test]
    fn leading_trailing() {
        assert_eq!(eval("squeeze", &[s("  hello  ")]), s("hello"));
    }
    #[test]
    fn single_word() {
        assert_eq!(eval("squeeze", &[s("hello")]), s("hello"));
    }
    #[test]
    fn multiple_gaps() {
        assert_eq!(eval("squeeze", &[s("a  b  c  d")]), s("a b c d"));
    }
    #[test]
    fn newlines() {
        assert_eq!(eval("squeeze", &[s("a\n\nb")]), s("a b"));
    }
}

// ===========================================================================
// regexp_count
// ===========================================================================
mod regexp_count_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("regexp_count", &[s("aaa"), s("a")]), i(3));
    }
    #[test]
    fn no_match() {
        assert_eq!(eval("regexp_count", &[s("hello"), s(r"\d")]), i(0));
    }
    #[test]
    fn digits() {
        assert_eq!(eval("regexp_count", &[s("a1b2c3"), s(r"\d")]), i(3));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("regexp_count", &[null(), s("a")]), null());
    }
    #[test]
    fn overlapping() {
        assert_eq!(eval("regexp_count", &[s("aaa"), s("aa")]), i(1));
    }
    #[test]
    fn words() {
        assert_eq!(eval("regexp_count", &[s("one two three"), s(r"\w+")]), i(3));
    }
    #[test]
    fn empty_pattern() {
        assert!(eval("regexp_count", &[s("abc"), s("")]) != null());
    }
    #[test]
    fn spaces() {
        assert_eq!(eval("regexp_count", &[s("a b c"), s(r"\s")]), i(2));
    }
    #[test]
    fn empty_input() {
        assert_eq!(eval("regexp_count", &[s(""), s("a")]), i(0));
    }
    #[test]
    fn dot() {
        assert_eq!(eval("regexp_count", &[s("abc"), s(".")]), i(3));
    }
}

// ===========================================================================
// count_char
// ===========================================================================
mod count_char_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("count_char", &[s("hello"), s("l")]), i(2));
    }
    #[test]
    fn no_match() {
        assert_eq!(eval("count_char", &[s("hello"), s("x")]), i(0));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("count_char", &[null(), s("a")]), null());
    }
    #[test]
    fn empty() {
        assert_eq!(eval("count_char", &[s(""), s("a")]), i(0));
    }
    #[test]
    fn all_same() {
        assert_eq!(eval("count_char", &[s("aaaa"), s("a")]), i(4));
    }
    #[test]
    fn space() {
        assert_eq!(eval("count_char", &[s("a b c"), s(" ")]), i(2));
    }
    #[test]
    fn single_char() {
        assert_eq!(eval("count_char", &[s("x"), s("x")]), i(1));
    }
    #[test]
    fn digit() {
        assert_eq!(eval("count_char", &[s("a1b1c1"), s("1")]), i(3));
    }
}

// ===========================================================================
// byte_length / bit_length
// ===========================================================================
mod byte_bit_length_tests {
    use super::*;

    #[test]
    fn byte_len_ascii() {
        assert_eq!(eval("byte_length", &[s("hello")]), i(5));
    }
    #[test]
    fn byte_len_empty() {
        assert_eq!(eval("byte_length", &[s("")]), i(0));
    }
    #[test]
    fn byte_len_null() {
        assert_eq!(eval("byte_length", &[null()]), null());
    }
    #[test]
    fn byte_len_space() {
        assert_eq!(eval("byte_length", &[s(" ")]), i(1));
    }
    #[test]
    fn byte_len_number() {
        assert_eq!(eval("byte_length", &[i(42)]), i(2));
    }

    #[test]
    fn bit_len_ascii() {
        assert_eq!(eval("bit_length", &[s("hello")]), i(40));
    }
    #[test]
    fn bit_len_empty() {
        assert_eq!(eval("bit_length", &[s("")]), i(0));
    }
    #[test]
    fn bit_len_null() {
        assert_eq!(eval("bit_length", &[null()]), null());
    }
    #[test]
    fn bit_len_one() {
        assert_eq!(eval("bit_length", &[s("a")]), i(8));
    }
    #[test]
    fn bit_len_number() {
        assert_eq!(eval("bit_length", &[i(42)]), i(16));
    }
}

// ===========================================================================
// strcmp
// ===========================================================================
mod strcmp_tests {
    use super::*;

    #[test]
    fn equal() {
        assert_eq!(eval("strcmp", &[s("abc"), s("abc")]), i(0));
    }
    #[test]
    fn less() {
        assert_eq!(eval("strcmp", &[s("abc"), s("def")]), i(-1));
    }
    #[test]
    fn greater() {
        assert_eq!(eval("strcmp", &[s("def"), s("abc")]), i(1));
    }
    #[test]
    fn empty_vs_a() {
        assert_eq!(eval("strcmp", &[s(""), s("a")]), i(-1));
    }
    #[test]
    fn a_vs_empty() {
        assert_eq!(eval("strcmp", &[s("a"), s("")]), i(1));
    }
    #[test]
    fn both_empty() {
        assert_eq!(eval("strcmp", &[s(""), s("")]), i(0));
    }
    #[test]
    fn case_diff() {
        assert_eq!(eval("strcmp", &[s("A"), s("a")]), i(-1));
    }
    #[test]
    fn prefix() {
        assert_eq!(eval("strcmp", &[s("ab"), s("abc")]), i(-1));
    }
}

// ===========================================================================
// concat_ws
// ===========================================================================
mod concat_ws_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(
            eval("concat_ws", &[s(","), s("a"), s("b"), s("c")]),
            s("a,b,c")
        );
    }
    #[test]
    fn space_sep() {
        assert_eq!(
            eval("concat_ws", &[s(" "), s("hello"), s("world")]),
            s("hello world")
        );
    }
    #[test]
    fn with_null() {
        assert_eq!(
            eval("concat_ws", &[s(","), s("a"), null(), s("c")]),
            s("a,c")
        );
    }
    #[test]
    fn empty_sep() {
        assert_eq!(eval("concat_ws", &[s(""), s("a"), s("b")]), s("ab"));
    }
    #[test]
    fn no_values() {
        assert_eq!(eval("concat_ws", &[s(",")]), s(""));
    }
    #[test]
    fn single_value() {
        assert_eq!(eval("concat_ws", &[s(","), s("a")]), s("a"));
    }
    #[test]
    fn all_null() {
        assert_eq!(eval("concat_ws", &[s(","), null(), null()]), s(""));
    }
    #[test]
    fn dash_sep() {
        assert_eq!(
            eval("concat_ws", &[s("-"), s("2024"), s("01"), s("15")]),
            s("2024-01-15")
        );
    }
}

// ===========================================================================
// overlay
// ===========================================================================
mod overlay_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(
            eval("overlay", &[s("hello world"), s("XX"), i(6), i(5)]),
            s("helloXXd")
        );
    }
    #[test]
    fn at_start() {
        assert_eq!(
            eval("overlay", &[s("hello"), s("X"), i(1), i(1)]),
            s("Xello")
        );
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("overlay", &[null(), s("X"), i(1), i(1)]), null());
    }
    #[test]
    fn replace_all() {
        assert_eq!(eval("overlay", &[s("abc"), s("XYZ"), i(1), i(3)]), s("XYZ"));
    }
    #[test]
    fn insert() {
        assert_eq!(eval("overlay", &[s("ac"), s("b"), i(2), i(0)]), s("abc"));
    }
    #[test]
    fn empty_replacement() {
        assert_eq!(eval("overlay", &[s("hello"), s(""), i(2), i(3)]), s("ho"));
    }
}

// ===========================================================================
// translate
// ===========================================================================
mod translate_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(
            eval("translate", &[s("hello"), s("helo"), s("HELO")]),
            s("HELLO")
        );
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("translate", &[null(), s("a"), s("b")]), null());
    }
    #[test]
    fn remove_chars() {
        assert_eq!(eval("translate", &[s("hello"), s("l"), s("")]), s("heo"));
    }
    #[test]
    fn no_match() {
        assert_eq!(
            eval("translate", &[s("hello"), s("xyz"), s("XYZ")]),
            s("hello")
        );
    }
    #[test]
    fn digits() {
        assert_eq!(eval("translate", &[s("a1b2"), s("12"), s("xy")]), s("axby"));
    }
    #[test]
    fn empty_input() {
        assert_eq!(eval("translate", &[s(""), s("a"), s("b")]), s(""));
    }
}

// ===========================================================================
// space
// ===========================================================================
mod space_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("space", &[i(3)]), s("   "));
    }
    #[test]
    fn zero() {
        assert_eq!(eval("space", &[i(0)]), s(""));
    }
    #[test]
    fn negative() {
        assert_eq!(eval("space", &[i(-1)]), s(""));
    }
    #[test]
    fn one() {
        assert_eq!(eval("space", &[i(1)]), s(" "));
    }
    #[test]
    fn ten() {
        assert_eq!(eval("space", &[i(10)]), s("          "));
    }
}

// ===========================================================================
// format
// ===========================================================================
mod format_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(
            eval("format", &[s("Hello %s"), s("World")]),
            s("Hello World")
        );
    }
    #[test]
    fn two_args() {
        assert_eq!(eval("format", &[s("%s + %s"), s("a"), s("b")]), s("a + b"));
    }
    #[test]
    fn no_placeholder() {
        assert_eq!(eval("format", &[s("hello")]), s("hello"));
    }
    #[test]
    fn number_arg() {
        assert_eq!(eval("format", &[s("val=%s"), i(42)]), s("val=42"));
    }
    #[test]
    fn empty_template() {
        assert_eq!(eval("format", &[s("")]), s(""));
    }
}

// ===========================================================================
// Additional string function coverage
// ===========================================================================
mod string_to_array_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("string_to_array", &[s("a,b,c"), s(",")]), s("[a,b,c]"));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("string_to_array", &[null(), s(",")]), null());
    }
    #[test]
    fn space_sep() {
        assert_eq!(eval("string_to_array", &[s("a b c"), s(" ")]), s("[a,b,c]"));
    }
    #[test]
    fn single() {
        assert_eq!(eval("string_to_array", &[s("abc"), s(",")]), s("[abc]"));
    }
    #[test]
    fn empty() {
        assert_eq!(eval("string_to_array", &[s(""), s(",")]), s("[]"));
    }
}

mod array_to_string_tests {
    use super::*;

    #[test]
    fn basic() {
        assert_eq!(eval("array_to_string", &[s("[a,b,c]"), s(",")]), s("a,b,c"));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("array_to_string", &[null(), s(",")]), null());
    }
    #[test]
    fn space_sep() {
        assert_eq!(eval("array_to_string", &[s("[a,b,c]"), s(" ")]), s("a b c"));
    }
}

// ===========================================================================
// json_extract / json_array_length
// ===========================================================================
mod json_tests {
    use super::*;

    #[test]
    fn extract_string() {
        assert_eq!(
            eval("json_extract", &[s(r#"{"name":"Alice"}"#), s("name")]),
            s("Alice")
        );
    }
    #[test]
    fn extract_number() {
        assert_eq!(eval("json_extract", &[s(r#"{"age":30}"#), s("age")]), i(30));
    }
    #[test]
    fn extract_null_input() {
        assert_eq!(eval("json_extract", &[null(), s("key")]), null());
    }
    #[test]
    fn extract_missing() {
        assert_eq!(eval("json_extract", &[s(r#"{"a":1}"#), s("b")]), null());
    }
    #[test]
    fn extract_nested() {
        assert_eq!(
            eval("json_extract", &[s(r#"{"a":{"b":42}}"#), s("a.b")]),
            i(42)
        );
    }
    #[test]
    fn extract_bool() {
        assert_eq!(
            eval("json_extract", &[s(r#"{"flag":true}"#), s("flag")]),
            i(1)
        );
    }
    #[test]
    fn extract_float() {
        let r = eval("json_extract", &[s(r#"{"val":3.14}"#), s("val")]);
        match r {
            Value::F64(v) => assert!((v - 3.14).abs() < 0.001),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn array_len_basic() {
        assert_eq!(eval("json_array_length", &[s("[1,2,3]")]), i(3));
    }
    #[test]
    fn array_len_empty() {
        assert_eq!(eval("json_array_length", &[s("[]")]), i(0));
    }
    #[test]
    fn array_len_null() {
        assert_eq!(eval("json_array_length", &[null()]), null());
    }
    #[test]
    fn array_len_one() {
        assert_eq!(eval("json_array_length", &[s("[42]")]), i(1));
    }
}

// ===========================================================================
// to_char / to_str_timestamp
// ===========================================================================
mod to_char_tests {
    use super::*;

    // 2024-01-15T12:30:45Z in nanos: use make_timestamp to get the right value
    fn make_ts() -> Value {
        evaluate_scalar(
            "make_timestamp",
            &[i(2024), i(1), i(15), i(12), i(30), i(45)],
        )
        .unwrap()
    }

    #[test]
    fn year_month_day() {
        assert_eq!(
            eval("to_char", &[make_ts(), s("YYYY-MM-DD")]),
            s("2024-01-15")
        );
    }
    #[test]
    fn full_datetime() {
        assert_eq!(
            eval("to_char", &[make_ts(), s("YYYY-MM-DD HH24:MI:SS")]),
            s("2024-01-15 12:30:45")
        );
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("to_char", &[null(), s("YYYY")]), null());
    }
    #[test]
    fn year_only() {
        assert_eq!(eval("to_char", &[make_ts(), s("YYYY")]), s("2024"));
    }
}

mod to_str_timestamp_tests {
    use super::*;

    fn make_ts() -> Value {
        evaluate_scalar(
            "make_timestamp",
            &[i(2024), i(1), i(15), i(12), i(30), i(45)],
        )
        .unwrap()
    }

    #[test]
    fn basic() {
        assert_eq!(
            eval("to_str_timestamp", &[make_ts(), s("YYYY-MM-DD")]),
            s("2024-01-15")
        );
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("to_str_timestamp", &[null(), s("YYYY")]), null());
    }
    #[test]
    fn with_time() {
        assert_eq!(
            eval("to_str_timestamp", &[make_ts(), s("HH24:MI:SS")]),
            s("12:30:45")
        );
    }
}

// ===========================================================================
// Aliases and misc coverage
// ===========================================================================
mod alias_tests {
    use super::*;

    // len is alias for length
    #[test]
    fn len_basic() {
        assert_eq!(eval("len", &[s("hello")]), i(5));
    }
    #[test]
    fn len_empty() {
        assert_eq!(eval("len", &[s("")]), i(0));
    }

    // to_lowercase/to_uppercase aliases
    #[test]
    fn to_lowercase() {
        assert_eq!(eval("to_lowercase", &[s("ABC")]), s("abc"));
    }
    #[test]
    fn to_uppercase() {
        assert_eq!(eval("to_uppercase", &[s("abc")]), s("ABC"));
    }

    // substr alias
    #[test]
    fn substr_basic() {
        assert_eq!(eval("substr", &[s("hello"), i(1), i(3)]), s("hel"));
    }

    // str_pos alias
    #[test]
    fn str_pos_basic() {
        assert_eq!(eval("str_pos", &[s("lo"), s("hello")]), i(4));
    }

    // string_length alias
    #[test]
    fn string_length() {
        assert_eq!(eval("string_length", &[s("test")]), i(4));
    }

    // title_case is alias for initcap
    #[test]
    fn title_case() {
        assert_eq!(eval("title_case", &[s("hello world")]), s("Hello World"));
    }
}

// ===========================================================================
// nullifempty / to_json
// ===========================================================================
mod nullifempty_tests {
    use super::*;

    #[test]
    fn empty_gives_null() {
        assert_eq!(eval("nullifempty", &[s("")]), null());
    }
    #[test]
    fn non_empty_passes() {
        assert_eq!(eval("nullifempty", &[s("hello")]), s("hello"));
    }
    #[test]
    fn null_stays_null() {
        assert_eq!(eval("nullifempty", &[null()]), null());
    }
    #[test]
    fn number_passes() {
        assert_eq!(eval("nullifempty", &[i(42)]), i(42));
    }
}

mod to_json_tests {
    use super::*;

    #[test]
    fn string() {
        assert_eq!(eval("to_json", &[s("hello")]), s("\"hello\""));
    }
    #[test]
    fn number() {
        assert_eq!(eval("to_json", &[i(42)]), s("42"));
    }
    #[test]
    fn float() {
        assert_eq!(eval("to_json", &[f(3.14)]), s("3.14"));
    }
    #[test]
    fn null_input() {
        assert_eq!(eval("to_json", &[null()]), s("null"));
    }
    #[test]
    fn array() {
        let r = eval("to_json", &[i(1), i(2), i(3)]);
        assert_eq!(r, s("[1,2,3]"));
    }
}

// ===========================================================================
// iif / nvl2 / switch / decode_fn (conditional scalars)
// ===========================================================================
mod conditional_scalar_tests {
    use super::*;

    #[test]
    fn iif_true() {
        assert_eq!(eval("iif", &[i(1), s("yes"), s("no")]), s("yes"));
    }
    #[test]
    fn iif_false() {
        assert_eq!(eval("iif", &[i(0), s("yes"), s("no")]), s("no"));
    }
    #[test]
    fn iif_null_cond() {
        assert_eq!(eval("iif", &[null(), s("yes"), s("no")]), s("no"));
    }
    #[test]
    fn iif_string_cond() {
        assert_eq!(eval("iif", &[s("true"), s("y"), s("n")]), s("y"));
    }
    #[test]
    fn iif_false_string() {
        assert_eq!(eval("iif", &[s("false"), s("y"), s("n")]), s("n"));
    }

    #[test]
    fn nvl2_not_null() {
        assert_eq!(eval("nvl2", &[i(1), s("has"), s("none")]), s("has"));
    }
    #[test]
    fn nvl2_null() {
        assert_eq!(eval("nvl2", &[null(), s("has"), s("none")]), s("none"));
    }

    #[test]
    fn switch_match() {
        assert_eq!(
            eval("switch", &[i(1), i(1), s("one"), i(2), s("two")]),
            s("one")
        );
    }
    #[test]
    fn switch_no_match() {
        assert_eq!(
            eval(
                "switch",
                &[i(3), i(1), s("one"), i(2), s("two"), s("other")]
            ),
            s("other")
        );
    }

    #[test]
    fn decode_match() {
        assert_eq!(
            eval("decode_fn", &[s("a"), s("a"), i(1), s("b"), i(2)]),
            i(1)
        );
    }
    #[test]
    fn decode_default() {
        assert_eq!(
            eval("decode_fn", &[s("c"), s("a"), i(1), s("b"), i(2), i(0)]),
            i(0)
        );
    }
}

// ===========================================================================
// coalesce / nullif / greatest / least / if_null
// ===========================================================================
mod conditional_tests {
    use super::*;

    #[test]
    fn coalesce_first_non_null() {
        assert_eq!(eval("coalesce", &[null(), null(), i(3)]), i(3));
    }
    #[test]
    fn coalesce_first() {
        assert_eq!(eval("coalesce", &[i(1), i(2)]), i(1));
    }
    #[test]
    fn coalesce_all_null() {
        assert_eq!(eval("coalesce", &[null(), null()]), null());
    }
    #[test]
    fn coalesce_string() {
        assert_eq!(eval("coalesce", &[null(), s("x")]), s("x"));
    }

    #[test]
    fn nullif_equal() {
        assert_eq!(eval("nullif", &[i(1), i(1)]), null());
    }
    #[test]
    fn nullif_different() {
        assert_eq!(eval("nullif", &[i(1), i(2)]), i(1));
    }
    #[test]
    fn nullif_strings() {
        assert_eq!(eval("nullif", &[s("a"), s("a")]), null());
    }

    #[test]
    fn greatest_ints() {
        assert_eq!(eval("greatest", &[i(1), i(5), i(3)]), i(5));
    }
    #[test]
    fn greatest_with_null() {
        assert_eq!(eval("greatest", &[null(), i(3), i(1)]), i(3));
    }
    #[test]
    fn greatest_single() {
        assert_eq!(eval("greatest", &[i(42)]), i(42));
    }
    #[test]
    fn greatest_strings() {
        assert_eq!(eval("greatest", &[s("a"), s("c"), s("b")]), s("c"));
    }

    #[test]
    fn least_ints() {
        assert_eq!(eval("least", &[i(5), i(1), i(3)]), i(1));
    }
    #[test]
    fn least_with_null() {
        assert_eq!(eval("least", &[null(), i(3), i(1)]), i(1));
    }
    #[test]
    fn least_single() {
        assert_eq!(eval("least", &[i(42)]), i(42));
    }

    #[test]
    fn ifnull_null() {
        assert_eq!(eval("if_null", &[null(), i(42)]), i(42));
    }
    #[test]
    fn ifnull_not_null() {
        assert_eq!(eval("if_null", &[i(1), i(42)]), i(1));
    }
}

// ===========================================================================
// typeof / is_null / is_not_null / nullif_zero / zeroifnull
// ===========================================================================
mod type_tests {
    use super::*;

    #[test]
    fn typeof_null() {
        assert_eq!(eval("typeof", &[null()]), s("null"));
    }
    #[test]
    fn typeof_int() {
        assert_eq!(eval("typeof", &[i(42)]), s("i64"));
    }
    #[test]
    fn typeof_float() {
        assert_eq!(eval("typeof", &[f(3.14)]), s("f64"));
    }
    #[test]
    fn typeof_string() {
        assert_eq!(eval("typeof", &[s("hi")]), s("string"));
    }
    #[test]
    fn typeof_timestamp() {
        assert_eq!(eval("typeof", &[ts(0)]), s("timestamp"));
    }

    #[test]
    fn is_null_true() {
        assert_eq!(eval("is_null", &[null()]), i(1));
    }
    #[test]
    fn is_null_false() {
        assert_eq!(eval("is_null", &[i(0)]), i(0));
    }
    #[test]
    fn is_not_null_true() {
        assert_eq!(eval("is_not_null", &[i(1)]), i(1));
    }
    #[test]
    fn is_not_null_false() {
        assert_eq!(eval("is_not_null", &[null()]), i(0));
    }

    #[test]
    fn nullif_zero_int() {
        assert_eq!(eval("nullif_zero", &[i(0)]), null());
    }
    #[test]
    fn nullif_zero_non_zero() {
        assert_eq!(eval("nullif_zero", &[i(5)]), i(5));
    }
    #[test]
    fn nullif_zero_float() {
        assert_eq!(eval("nullif_zero", &[f(0.0)]), null());
    }
    #[test]
    fn nullif_zero_null() {
        assert_eq!(eval("nullif_zero", &[null()]), null());
    }

    #[test]
    fn zeroifnull_null() {
        assert_eq!(eval("zeroifnull", &[null()]), i(0));
    }
    #[test]
    fn zeroifnull_value() {
        assert_eq!(eval("zeroifnull", &[i(5)]), i(5));
    }
}

// ===========================================================================
// cast functions
// ===========================================================================
mod cast_tests {
    use super::*;

    #[test]
    fn cast_int_from_str() {
        assert_eq!(eval("cast_int", &[s("42")]), i(42));
    }
    #[test]
    fn cast_int_from_float() {
        assert_eq!(eval("cast_int", &[f(3.9)]), i(3));
    }
    #[test]
    fn cast_int_null() {
        assert_eq!(eval("cast_int", &[null()]), null());
    }
    #[test]
    fn cast_float_from_str() {
        assert_eq!(eval("cast_float", &[s("3.14")]), f(3.14));
    }
    #[test]
    fn cast_float_from_int() {
        assert_eq!(eval("cast_float", &[i(42)]), f(42.0));
    }
    #[test]
    fn cast_float_null() {
        assert_eq!(eval("cast_float", &[null()]), null());
    }
    #[test]
    fn cast_str_from_int() {
        assert_eq!(eval("cast_str", &[i(42)]), s("42"));
    }
    #[test]
    fn cast_str_from_float() {
        assert_eq!(eval("cast_str", &[f(3.14)]), s("3.14"));
    }
    #[test]
    fn cast_str_null() {
        assert_eq!(eval("cast_str", &[null()]), null());
    }
    #[test]
    fn cast_bool_true() {
        assert_eq!(eval("cast_bool", &[s("true")]), i(1));
    }
    #[test]
    fn cast_bool_false() {
        assert_eq!(eval("cast_bool", &[s("false")]), i(0));
    }
    #[test]
    fn cast_bool_one() {
        assert_eq!(eval("cast_bool", &[i(1)]), i(1));
    }
    #[test]
    fn cast_bool_zero() {
        assert_eq!(eval("cast_bool", &[i(0)]), i(0));
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
        assert_eq!(eval("safe_cast_float", &[s("3.14")]), f(3.14));
    }
    #[test]
    fn safe_cast_float_bad() {
        assert_eq!(eval("safe_cast_float", &[s("abc")]), null());
    }
}

// ===========================================================================
// utility functions
// ===========================================================================
mod utility_tests {
    use super::*;

    #[test]
    fn version() {
        let r = eval("version", &[]);
        match r {
            Value::Str(v) => assert!(v.contains("ExchangeDB")),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn sizeof_int() {
        assert_eq!(eval("sizeof", &[i(42)]), i(8));
    }
    #[test]
    fn sizeof_string() {
        assert_eq!(eval("sizeof", &[s("hello")]), i(5));
    }
    #[test]
    fn sizeof_null() {
        assert_eq!(eval("sizeof", &[null()]), i(0));
    }
    #[test]
    fn pg_typeof_int() {
        assert_eq!(eval("pg_typeof", &[i(42)]), s("bigint"));
    }
    #[test]
    fn pg_typeof_str() {
        assert_eq!(eval("pg_typeof", &[s("hi")]), s("text"));
    }
    #[test]
    fn current_schema() {
        assert_eq!(eval("current_schema", &[]), s("public"));
    }
    #[test]
    fn current_database() {
        assert_eq!(eval("current_database", &[]), s("exchangedb"));
    }
    #[test]
    fn current_user() {
        assert_eq!(eval("current_user", &[]), s("admin"));
    }
    #[test]
    fn table_name() {
        assert_eq!(eval("table_name", &[]), s("unknown"));
    }
}

// ===========================================================================
// Additional string tests for higher coverage
// ===========================================================================

mod length_extended_tests {
    use super::*;

    #[test]
    fn length_100() {
        assert_eq!(eval("length", &[s(&"a".repeat(100))]), i(100));
    }
    #[test]
    fn length_whitespace() {
        assert_eq!(eval("length", &[s(" \t\n ")]), i(4));
    }
    #[test]
    fn length_mixed_ws() {
        assert_eq!(eval("length", &[s("a b\tc\nd")]), i(7));
    }
    #[test]
    fn char_length_100() {
        assert_eq!(eval("char_length", &[s(&"x".repeat(100))]), i(100));
    }
    #[test]
    fn byte_length_100() {
        assert_eq!(eval("byte_length", &[s(&"z".repeat(100))]), i(100));
    }
}

mod upper_lower_extended_tests {
    use super::*;

    #[test]
    fn upper_long() {
        assert_eq!(eval("upper", &[s(&"abc".repeat(50))]), s(&"ABC".repeat(50)));
    }
    #[test]
    fn lower_long() {
        assert_eq!(eval("lower", &[s(&"XYZ".repeat(50))]), s(&"xyz".repeat(50)));
    }
    #[test]
    fn upper_numbers_only() {
        assert_eq!(eval("upper", &[s("123")]), s("123"));
    }
    #[test]
    fn lower_numbers_only() {
        assert_eq!(eval("lower", &[s("123")]), s("123"));
    }
    #[test]
    fn upper_special_only() {
        assert_eq!(eval("upper", &[s("!@#$")]), s("!@#$"));
    }
    #[test]
    fn lower_special_only() {
        assert_eq!(eval("lower", &[s("!@#$")]), s("!@#$"));
    }
}

mod substring_extended_tests {
    use super::*;

    #[test]
    fn mid_of_long() {
        assert_eq!(eval("substring", &[s("abcdefghij"), i(4), i(3)]), s("def"));
    }
    #[test]
    fn from_end() {
        assert_eq!(eval("substring", &[s("hello"), i(4), i(10)]), s("lo"));
    }
    #[test]
    fn full_extract() {
        assert_eq!(eval("substring", &[s("test"), i(1), i(4)]), s("test"));
    }
    #[test]
    fn single_from_middle() {
        assert_eq!(eval("substring", &[s("hello"), i(3), i(1)]), s("l"));
    }
}

mod concat_extended_tests {
    use super::*;

    #[test]
    fn five_args() {
        assert_eq!(
            eval("concat", &[s("a"), s("b"), s("c"), s("d"), s("e")]),
            s("abcde")
        );
    }
    #[test]
    fn all_numbers() {
        assert_eq!(eval("concat", &[i(1), i(2), i(3)]), s("123"));
    }
    #[test]
    fn mixed_types() {
        assert_eq!(eval("concat", &[s("val="), f(3.14)]), s("val=3.14"));
    }
    #[test]
    fn null_at_end() {
        assert_eq!(eval("concat", &[s("hello"), null()]), s("hello"));
    }
    #[test]
    fn null_at_start() {
        assert_eq!(eval("concat", &[null(), s("world")]), s("world"));
    }
}

mod replace_extended_tests {
    use super::*;

    #[test]
    fn replace_with_longer_multiple() {
        assert_eq!(eval("replace", &[s("aaa"), s("a"), s("bb")]), s("bbbbbb"));
    }
    #[test]
    fn replace_with_same() {
        assert_eq!(eval("replace", &[s("hello"), s("l"), s("l")]), s("hello"));
    }
    #[test]
    fn replace_first_char() {
        assert_eq!(eval("replace", &[s("hello"), s("h"), s("j")]), s("jello"));
    }
    #[test]
    fn replace_last_char() {
        assert_eq!(eval("replace", &[s("hello"), s("o"), s("a")]), s("hella"));
    }
}

mod starts_ends_contains_extended_tests {
    use super::*;

    #[test]
    fn starts_with_full() {
        assert_eq!(eval("starts_with", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn starts_with_longer_than_src() {
        assert_eq!(eval("starts_with", &[s("ab"), s("abc")]), i(0));
    }
    #[test]
    fn ends_with_full() {
        assert_eq!(eval("ends_with", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn ends_with_longer_than_src() {
        assert_eq!(eval("ends_with", &[s("ab"), s("abc")]), i(0));
    }
    #[test]
    fn contains_full() {
        assert_eq!(eval("contains", &[s("abc"), s("abc")]), i(1));
    }
    #[test]
    fn contains_longer() {
        assert_eq!(eval("contains", &[s("ab"), s("abc")]), i(0));
    }
    #[test]
    fn contains_single_char() {
        assert_eq!(eval("contains", &[s("a"), s("a")]), i(1));
    }
}

mod reverse_repeat_extended_tests {
    use super::*;

    #[test]
    fn reverse_long() {
        assert_eq!(eval("reverse", &[s("abcdef")]), s("fedcba"));
    }
    #[test]
    fn reverse_spaces() {
        assert_eq!(eval("reverse", &[s("a b c")]), s("c b a"));
    }
    #[test]
    fn repeat_three_chars() {
        assert_eq!(eval("repeat", &[s("abc"), i(3)]), s("abcabcabc"));
    }
    #[test]
    fn repeat_four_times() {
        assert_eq!(eval("repeat", &[s("xy"), i(4)]), s("xyxyxyxy"));
    }
}

mod left_right_extended_tests {
    use super::*;

    #[test]
    fn left_three() {
        assert_eq!(eval("left", &[s("abcdef"), i(3)]), s("abc"));
    }
    #[test]
    fn left_all() {
        assert_eq!(eval("left", &[s("ab"), i(5)]), s("ab"));
    }
    #[test]
    fn right_three() {
        assert_eq!(eval("right", &[s("abcdef"), i(3)]), s("def"));
    }
    #[test]
    fn right_all() {
        assert_eq!(eval("right", &[s("ab"), i(5)]), s("ab"));
    }
    #[test]
    fn left_four() {
        assert_eq!(eval("left", &[s("hello world"), i(4)]), s("hell"));
    }
    #[test]
    fn right_four() {
        assert_eq!(eval("right", &[s("hello world"), i(4)]), s("orld"));
    }
}

mod position_extended_tests {
    use super::*;

    #[test]
    fn position_first_char() {
        assert_eq!(eval("position", &[s("h"), s("hello")]), i(1));
    }
    #[test]
    fn position_last_char() {
        assert_eq!(eval("position", &[s("o"), s("hello")]), i(5));
    }
    #[test]
    fn position_word() {
        assert_eq!(eval("position", &[s("world"), s("hello world")]), i(7));
    }
    #[test]
    fn position_repeated() {
        assert_eq!(eval("position", &[s("l"), s("hello")]), i(3));
    } // first occurrence
}

mod lpad_rpad_extended_tests {
    use super::*;

    #[test]
    fn lpad_zeros() {
        assert_eq!(eval("lpad", &[s("5"), i(3), s("0")]), s("005"));
    }
    #[test]
    fn rpad_dots() {
        assert_eq!(eval("rpad", &[s("hi"), i(5), s(".")]), s("hi..."));
    }
    #[test]
    fn lpad_stars() {
        assert_eq!(eval("lpad", &[s("x"), i(4), s("*")]), s("***x"));
    }
    #[test]
    fn rpad_stars() {
        assert_eq!(eval("rpad", &[s("x"), i(4), s("*")]), s("x***"));
    }
}

mod split_part_extended_tests {
    use super::*;

    #[test]
    fn comma_first() {
        assert_eq!(
            eval("split_part", &[s("one,two,three"), s(","), i(1)]),
            s("one")
        );
    }
    #[test]
    fn comma_last() {
        assert_eq!(
            eval("split_part", &[s("one,two,three"), s(","), i(3)]),
            s("three")
        );
    }
    #[test]
    fn pipe_delim() {
        assert_eq!(eval("split_part", &[s("a|b|c"), s("|"), i(2)]), s("b"));
    }
    #[test]
    fn dot_delim() {
        assert_eq!(
            eval("split_part", &[s("www.example.com"), s("."), i(2)]),
            s("example")
        );
    }
}

mod initcap_extended_tests {
    use super::*;

    #[test]
    fn all_spaces() {
        assert_eq!(eval("initcap", &[s("   ")]), s("   "));
    }
    #[test]
    fn underscored() {
        assert_eq!(eval("initcap", &[s("hello_world")]), s("Hello_World"));
    }
    #[test]
    fn three_words() {
        assert_eq!(eval("initcap", &[s("one two three")]), s("One Two Three"));
    }
    #[test]
    fn mixed_separators() {
        assert_eq!(eval("initcap", &[s("a-b c_d")]), s("A-B C_D"));
    }
}

mod char_at_extended_tests {
    use super::*;

    #[test]
    fn fourth_char() {
        assert_eq!(eval("char_at", &[s("abcde"), i(4)]), s("d"));
    }
    #[test]
    fn fifth_char() {
        assert_eq!(eval("char_at", &[s("abcde"), i(5)]), s("e"));
    }
    #[test]
    fn out_of_bounds_large() {
        assert_eq!(eval("char_at", &[s("ab"), i(100)]), null());
    }
}

mod hex_unhex_extended_tests {
    use super::*;

    #[test]
    fn hex_128() {
        assert_eq!(eval("hex", &[i(128)]), s("80"));
    }
    #[test]
    fn hex_65535() {
        assert_eq!(eval("hex", &[i(65535)]), s("ffff"));
    }
    #[test]
    fn unhex_80() {
        assert_eq!(eval("unhex", &[s("80")]), i(128));
    }
    #[test]
    fn unhex_ffff() {
        assert_eq!(eval("unhex", &[s("ffff")]), i(65535));
    }
    #[test]
    fn hex_unhex_roundtrip() {
        let hex_val = eval("hex", &[i(42)]);
        assert_eq!(eval("unhex", &[hex_val]), i(42));
    }
}

mod url_extended_tests {
    use super::*;

    #[test]
    fn encode_slash() {
        assert_eq!(
            eval("url_encode", &[s("/path/to/file")]),
            s("%2Fpath%2Fto%2Ffile")
        );
    }
    #[test]
    fn encode_question() {
        assert_eq!(eval("url_encode", &[s("key?val")]), s("key%3Fval"));
    }
    #[test]
    fn decode_encoded_slash() {
        assert_eq!(eval("url_decode", &[s("%2Fpath")]), s("/path"));
    }
    #[test]
    fn encode_hash() {
        assert_eq!(eval("url_encode", &[s("#section")]), s("%23section"));
    }
}

mod soundex_extended_tests {
    use super::*;

    #[test]
    fn johnson() {
        let r = eval("soundex", &[s("Johnson")]);
        match r {
            Value::Str(h) => {
                assert_eq!(h.len(), 4);
                assert!(h.starts_with('J'));
            }
            _ => panic!(),
        }
    }
    #[test]
    fn williams() {
        let r = eval("soundex", &[s("Williams")]);
        match r {
            Value::Str(h) => {
                assert_eq!(h.len(), 4);
                assert!(h.starts_with('W'));
            }
            _ => panic!(),
        }
    }
    #[test]
    fn deterministic() {
        assert_eq!(eval("soundex", &[s("Test")]), eval("soundex", &[s("Test")]));
    }
}

mod word_count_extended_tests {
    use super::*;

    #[test]
    fn four_words() {
        assert_eq!(eval("word_count", &[s("one two three four")]), i(4));
    }
    #[test]
    fn five_words() {
        assert_eq!(eval("word_count", &[s("a b c d e")]), i(5));
    }
    #[test]
    fn single_char_words() {
        assert_eq!(eval("word_count", &[s("a b c")]), i(3));
    }
    #[test]
    fn mixed_whitespace() {
        assert_eq!(eval("word_count", &[s("a\tb\nc")]), i(3));
    }
}

mod camel_snake_extended_tests {
    use super::*;

    #[test]
    fn camel_three_words() {
        assert_eq!(eval("camel_case", &[s("one_two_three")]), s("OneTwoThree"));
    }
    #[test]
    fn camel_dashes() {
        assert_eq!(eval("camel_case", &[s("a-b-c")]), s("ABC"));
    }
    #[test]
    fn snake_three_words() {
        assert_eq!(eval("snake_case", &[s("OneTwoThree")]), s("one_two_three"));
    }
    #[test]
    fn snake_single_word() {
        assert_eq!(eval("snake_case", &[s("hello")]), s("hello"));
    }
}

mod squeeze_extended_tests {
    use super::*;

    #[test]
    fn triple_spaces() {
        assert_eq!(eval("squeeze", &[s("a   b   c")]), s("a b c"));
    }
    #[test]
    fn tabs_only() {
        assert_eq!(eval("squeeze", &[s("\t\t\t")]), s(""));
    }
    #[test]
    fn mixed_ws() {
        assert_eq!(eval("squeeze", &[s("a \t\n b")]), s("a b"));
    }
    #[test]
    fn no_squeeze_needed() {
        assert_eq!(eval("squeeze", &[s("hello world")]), s("hello world"));
    }
}

mod base64_extended_tests {
    use super::*;

    #[test]
    fn encode_abcd() {
        assert_eq!(eval("to_base64", &[s("abcd")]), s("YWJjZA=="));
    }
    #[test]
    fn roundtrip_long() {
        let input = "The quick brown fox jumps over the lazy dog";
        let enc = eval("to_base64", &[s(input)]);
        assert_eq!(eval("from_base64", &[enc]), s(input));
    }
    #[test]
    fn encode_numbers() {
        assert_eq!(eval("to_base64", &[s("123")]), s("MTIz"));
    }
    #[test]
    fn decode_MTIz() {
        assert_eq!(eval("from_base64", &[s("MTIz")]), s("123"));
    }
}

mod crc32_extended_tests {
    use super::*;

    #[test]
    fn crc32_hello() {
        match eval("crc32", &[s("hello")]) {
            Value::I64(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn crc32_empty() {
        match eval("crc32", &[s("")]) {
            Value::I64(_) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn crc32_deterministic() {
        assert_eq!(eval("crc32", &[s("abc")]), eval("crc32", &[s("abc")]));
    }
    #[test]
    fn crc32_different() {
        assert_ne!(eval("crc32", &[s("a")]), eval("crc32", &[s("b")]));
    }
}

mod regexp_extended_tests {
    use super::*;

    #[test]
    fn match_digits_only() {
        assert_eq!(eval("regexp_match", &[s("12345"), s(r"^\d+$")]), i(1));
    }
    #[test]
    fn match_alpha_only() {
        assert_eq!(eval("regexp_match", &[s("hello"), s(r"^[a-z]+$")]), i(1));
    }
    #[test]
    fn match_email() {
        assert_eq!(
            eval(
                "regexp_match",
                &[s("user@example.com"), s(r"^[\w.]+@[\w.]+$")]
            ),
            i(1)
        );
    }
    #[test]
    fn replace_all_digits() {
        assert_eq!(
            eval("regexp_replace", &[s("a1b2c3"), s(r"\d"), s("X")]),
            s("aXbXcX")
        );
    }
    #[test]
    fn extract_year_from_date() {
        assert_eq!(
            eval("regexp_extract", &[s("2024-01-15"), s(r"(\d{4})"), i(1)]),
            s("2024")
        );
    }
    #[test]
    fn count_vowels() {
        assert_eq!(
            eval("regexp_count", &[s("hello world"), s("[aeiou]")]),
            i(3)
        );
    }
}

mod count_char_extended_tests {
    use super::*;

    #[test]
    fn count_a_in_banana() {
        assert_eq!(eval("count_char", &[s("banana"), s("a")]), i(3));
    }
    #[test]
    fn count_n_in_banana() {
        assert_eq!(eval("count_char", &[s("banana"), s("n")]), i(2));
    }
    #[test]
    fn count_z_in_banana() {
        assert_eq!(eval("count_char", &[s("banana"), s("z")]), i(0));
    }
}
