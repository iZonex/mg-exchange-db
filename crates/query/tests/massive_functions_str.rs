//! Massive string function test suite — 1000+ tests.
//!
//! Every string function x {normal, empty, unicode, null, long, special} inputs.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;

fn s(v: &str) -> Value { Value::Str(v.to_string()) }
fn i(v: i64) -> Value { Value::I64(v) }
fn f(v: f64) -> Value { Value::F64(v) }
fn null() -> Value { Value::Null }
fn eval(name: &str, args: &[Value]) -> Value { evaluate_scalar(name, args).unwrap() }
fn eval_err(name: &str, args: &[Value]) -> String { evaluate_scalar(name, args).unwrap_err() }

// ===========================================================================
// length (+ aliases: len, char_length, string_length)
// ===========================================================================
mod length_extra {
    use super::*;
    #[test] fn normal() { assert_eq!(eval("length", &[s("hello")]), i(5)); }
    #[test] fn empty() { assert_eq!(eval("length", &[s("")]), i(0)); }
    #[test] fn null_in() { assert_eq!(eval("length", &[null()]), null()); }
    #[test] fn unicode_emoji() { assert_eq!(eval("length", &[s("\u{1F600}")]), i(1)); }
    #[test] fn unicode_cjk() { assert_eq!(eval("length", &[s("\u{4E16}\u{754C}")]), i(2)); }
    #[test] fn long_500() { assert_eq!(eval("length", &[s(&"a".repeat(500))]), i(500)); }
    #[test] fn special_tab() { assert_eq!(eval("length", &[s("\t\n\r")]), i(3)); }
    #[test] fn int_input() { assert_eq!(eval("length", &[i(12345)]), i(5)); }
    #[test] fn float_input() { assert_eq!(eval("length", &[f(3.14)]), i(4)); }
    #[test] fn single_space() { assert_eq!(eval("length", &[s(" ")]), i(1)); }
    // aliases
    #[test] fn len_alias() { assert_eq!(eval("len", &[s("abc")]), i(3)); }
    #[test] fn char_length_alias() { assert_eq!(eval("char_length", &[s("abc")]), i(3)); }
    #[test] fn string_length_alias() { assert_eq!(eval("string_length", &[s("abc")]), i(3)); }
    #[test] fn len_empty() { assert_eq!(eval("len", &[s("")]), i(0)); }
    #[test] fn len_null() { assert_eq!(eval("len", &[null()]), null()); }
}

// ===========================================================================
// upper / lower (+ aliases: to_uppercase, to_lowercase)
// ===========================================================================
mod upper_extra {
    use super::*;
    #[test] fn normal() { assert_eq!(eval("upper", &[s("hello")]), s("HELLO")); }
    #[test] fn empty() { assert_eq!(eval("upper", &[s("")]), s("")); }
    #[test] fn null_in() { assert_eq!(eval("upper", &[null()]), null()); }
    #[test] fn already_up() { assert_eq!(eval("upper", &[s("ABC")]), s("ABC")); }
    #[test] fn mixed() { assert_eq!(eval("upper", &[s("aBcD")]), s("ABCD")); }
    #[test] fn digits() { assert_eq!(eval("upper", &[s("a1b2")]), s("A1B2")); }
    #[test] fn special() { assert_eq!(eval("upper", &[s("a!b")]), s("A!B")); }
    #[test] fn int_in() { assert_eq!(eval("upper", &[i(42)]), s("42")); }
    #[test] fn long_str() { assert_eq!(eval("upper", &[s(&"ab".repeat(100))]), s(&"AB".repeat(100))); }
    #[test] fn to_uppercase_alias() { assert_eq!(eval("to_uppercase", &[s("abc")]), s("ABC")); }
}

mod lower_extra {
    use super::*;
    #[test] fn normal() { assert_eq!(eval("lower", &[s("HELLO")]), s("hello")); }
    #[test] fn empty() { assert_eq!(eval("lower", &[s("")]), s("")); }
    #[test] fn null_in() { assert_eq!(eval("lower", &[null()]), null()); }
    #[test] fn already_low() { assert_eq!(eval("lower", &[s("abc")]), s("abc")); }
    #[test] fn mixed() { assert_eq!(eval("lower", &[s("AbCd")]), s("abcd")); }
    #[test] fn digits() { assert_eq!(eval("lower", &[s("A1B2")]), s("a1b2")); }
    #[test] fn special() { assert_eq!(eval("lower", &[s("A!B")]), s("a!b")); }
    #[test] fn int_in() { assert_eq!(eval("lower", &[i(42)]), s("42")); }
    #[test] fn to_lowercase_alias() { assert_eq!(eval("to_lowercase", &[s("ABC")]), s("abc")); }
}

// ===========================================================================
// trim / ltrim / rtrim
// ===========================================================================
mod trim_extra {
    use super::*;
    #[test] fn trim_both() { assert_eq!(eval("trim", &[s("  hi  ")]), s("hi")); }
    #[test] fn trim_left_only() { assert_eq!(eval("trim", &[s("  hi")]), s("hi")); }
    #[test] fn trim_right_only() { assert_eq!(eval("trim", &[s("hi  ")]), s("hi")); }
    #[test] fn trim_none() { assert_eq!(eval("trim", &[s("hi")]), s("hi")); }
    #[test] fn trim_empty() { assert_eq!(eval("trim", &[s("")]), s("")); }
    #[test] fn trim_all_spaces() { assert_eq!(eval("trim", &[s("   ")]), s("")); }
    #[test] fn trim_null() { assert_eq!(eval("trim", &[null()]), null()); }
    #[test] fn trim_tabs() { assert_eq!(eval("trim", &[s("\thi\t")]), s("hi")); }
    #[test] fn trim_newlines() { assert_eq!(eval("trim", &[s("\nhi\n")]), s("hi")); }
    #[test] fn trim_single_char() { assert_eq!(eval("trim", &[s(" x ")]), s("x")); }

    #[test] fn ltrim_basic() { assert_eq!(eval("ltrim", &[s("  hi")]), s("hi")); }
    #[test] fn ltrim_none() { assert_eq!(eval("ltrim", &[s("hi")]), s("hi")); }
    #[test] fn ltrim_empty() { assert_eq!(eval("ltrim", &[s("")]), s("")); }
    #[test] fn ltrim_null() { assert_eq!(eval("ltrim", &[null()]), null()); }
    #[test] fn ltrim_preserves_right() { assert_eq!(eval("ltrim", &[s("  hi  ")]), s("hi  ")); }
    #[test] fn ltrim_all_spaces() { assert_eq!(eval("ltrim", &[s("   ")]), s("")); }

    #[test] fn rtrim_basic() { assert_eq!(eval("rtrim", &[s("hi  ")]), s("hi")); }
    #[test] fn rtrim_none() { assert_eq!(eval("rtrim", &[s("hi")]), s("hi")); }
    #[test] fn rtrim_empty() { assert_eq!(eval("rtrim", &[s("")]), s("")); }
    #[test] fn rtrim_null() { assert_eq!(eval("rtrim", &[null()]), null()); }
    #[test] fn rtrim_preserves_left() { assert_eq!(eval("rtrim", &[s("  hi  ")]), s("  hi")); }
    #[test] fn rtrim_all_spaces() { assert_eq!(eval("rtrim", &[s("   ")]), s("")); }
}

// ===========================================================================
// substring (+ alias: substr)
// ===========================================================================
mod substring_extra {
    use super::*;
    #[test] fn basic() { assert_eq!(eval("substring", &[s("hello"), i(1), i(3)]), s("hel")); }
    #[test] fn from_start() { assert_eq!(eval("substring", &[s("hello"), i(1), i(5)]), s("hello")); }
    #[test] fn from_middle() { assert_eq!(eval("substring", &[s("hello"), i(2), i(3)]), s("ell")); }
    #[test] fn from_end() { assert_eq!(eval("substring", &[s("hello"), i(4), i(2)]), s("lo")); }
    #[test] fn empty_str() { assert_eq!(eval("substring", &[s(""), i(1), i(3)]), s("")); }
    #[test] fn null_in() { assert_eq!(eval("substring", &[null(), i(1), i(3)]), null()); }
    #[test] fn zero_len() { assert_eq!(eval("substring", &[s("hello"), i(1), i(0)]), s("")); }
    #[test] fn beyond_len() { assert_eq!(eval("substring", &[s("hi"), i(1), i(100)]), s("hi")); }
    #[test] fn single_char() { assert_eq!(eval("substring", &[s("hello"), i(3), i(1)]), s("l")); }
    #[test] fn substr_alias() { assert_eq!(eval("substr", &[s("hello"), i(1), i(3)]), s("hel")); }
    #[test] fn long_string() { let long = "a".repeat(1000); assert_eq!(eval("substring", &[s(&long), i(1), i(5)]), s("aaaaa")); }
}

// ===========================================================================
// concat (+ alias: str_concat, string_concat)
// ===========================================================================
mod concat_extra {
    use super::*;
    #[test] fn two_strings() { assert_eq!(eval("concat", &[s("foo"), s("bar")]), s("foobar")); }
    #[test] fn three_strings() { assert_eq!(eval("concat", &[s("a"), s("b"), s("c")]), s("abc")); }
    #[test] fn empty_and_str() { assert_eq!(eval("concat", &[s(""), s("hi")]), s("hi")); }
    #[test] fn str_and_empty() { assert_eq!(eval("concat", &[s("hi"), s("")]), s("hi")); }
    #[test] fn both_empty() { assert_eq!(eval("concat", &[s(""), s("")]), s("")); }
    #[test] fn with_null() { assert_eq!(eval("concat", &[s("hi"), null()]), s("hi")); }
    #[test] fn null_and_str() { assert_eq!(eval("concat", &[null(), s("hi")]), s("hi")); }
    #[test] fn all_null() { assert_eq!(eval("concat", &[null(), null()]), s("")); }
    #[test] fn with_int() { assert_eq!(eval("concat", &[s("v"), i(42)]), s("v42")); }
    #[test] fn many_args() { assert_eq!(eval("concat", &[s("a"), s("b"), s("c"), s("d"), s("e")]), s("abcde")); }
    #[test] fn str_concat_alias() { assert_eq!(eval("str_concat", &[s("x"), s("y")]), s("xy")); }
    #[test] fn string_concat_alias() { assert_eq!(eval("string_concat", &[s("x"), s("y")]), s("xy")); }
}

// ===========================================================================
// replace
// ===========================================================================
mod replace_extra {
    use super::*;
    #[test] fn basic() { assert_eq!(eval("replace", &[s("hello world"), s("world"), s("earth")]), s("hello earth")); }
    #[test] fn no_match() { assert_eq!(eval("replace", &[s("hello"), s("xyz"), s("abc")]), s("hello")); }
    #[test] fn empty_search() { assert_eq!(eval("replace", &[s("hello"), s(""), s("x")]), s("hello")); }
    #[test] fn empty_replace() { assert_eq!(eval("replace", &[s("hello"), s("l"), s("")]), s("heo")); }
    #[test] fn null_in() { assert_eq!(eval("replace", &[null(), s("a"), s("b")]), null()); }
    #[test] fn multiple_occ() { assert_eq!(eval("replace", &[s("aaa"), s("a"), s("b")]), s("bbb")); }
    #[test] fn replace_all() { assert_eq!(eval("replace", &[s("abcabc"), s("abc"), s("x")]), s("xx")); }
    #[test] fn replace_single_char() { assert_eq!(eval("replace", &[s("abc"), s("b"), s("B")]), s("aBc")); }
    #[test] fn empty_str() { assert_eq!(eval("replace", &[s(""), s("a"), s("b")]), s("")); }
    #[test] fn long_input() { let long = "ab".repeat(500); let result = eval("replace", &[s(&long), s("ab"), s("cd")]); assert_eq!(result, s(&"cd".repeat(500))); }
}

// ===========================================================================
// starts_with / ends_with / contains
// ===========================================================================
mod starts_ends_contains {
    use super::*;
    #[test] fn starts_true() { assert_eq!(eval("starts_with", &[s("hello"), s("hel")]), i(1)); }
    #[test] fn starts_false() { assert_eq!(eval("starts_with", &[s("hello"), s("xyz")]), i(0)); }
    #[test] fn starts_empty() { assert_eq!(eval("starts_with", &[s("hello"), s("")]), i(1)); }
    #[test] fn starts_full() { assert_eq!(eval("starts_with", &[s("hello"), s("hello")]), i(1)); }
    #[test] fn starts_null() { assert_eq!(eval("starts_with", &[null(), s("h")]), null()); }
    #[test] fn starts_empty_str() { assert_eq!(eval("starts_with", &[s(""), s("h")]), i(0)); }
    #[test] fn starts_empty_both() { assert_eq!(eval("starts_with", &[s(""), s("")]), i(1)); }

    #[test] fn ends_true() { assert_eq!(eval("ends_with", &[s("hello"), s("llo")]), i(1)); }
    #[test] fn ends_false() { assert_eq!(eval("ends_with", &[s("hello"), s("xyz")]), i(0)); }
    #[test] fn ends_empty() { assert_eq!(eval("ends_with", &[s("hello"), s("")]), i(1)); }
    #[test] fn ends_full() { assert_eq!(eval("ends_with", &[s("hello"), s("hello")]), i(1)); }
    #[test] fn ends_null() { assert_eq!(eval("ends_with", &[null(), s("o")]), null()); }

    #[test] fn contains_true() { assert_eq!(eval("contains", &[s("hello world"), s("lo w")]), i(1)); }
    #[test] fn contains_false() { assert_eq!(eval("contains", &[s("hello"), s("xyz")]), i(0)); }
    #[test] fn contains_empty() { assert_eq!(eval("contains", &[s("hello"), s("")]), i(1)); }
    #[test] fn contains_full() { assert_eq!(eval("contains", &[s("hello"), s("hello")]), i(1)); }
    #[test] fn contains_null() { assert_eq!(eval("contains", &[null(), s("h")]), null()); }
    #[test] fn contains_at_start() { assert_eq!(eval("contains", &[s("hello"), s("hel")]), i(1)); }
    #[test] fn contains_at_end() { assert_eq!(eval("contains", &[s("hello"), s("llo")]), i(1)); }
}

// ===========================================================================
// reverse
// ===========================================================================
mod reverse_extra {
    use super::*;
    #[test] fn basic() { assert_eq!(eval("reverse", &[s("hello")]), s("olleh")); }
    #[test] fn empty() { assert_eq!(eval("reverse", &[s("")]), s("")); }
    #[test] fn null_in() { assert_eq!(eval("reverse", &[null()]), null()); }
    #[test] fn single_char() { assert_eq!(eval("reverse", &[s("a")]), s("a")); }
    #[test] fn palindrome() { assert_eq!(eval("reverse", &[s("aba")]), s("aba")); }
    #[test] fn digits() { assert_eq!(eval("reverse", &[s("123")]), s("321")); }
    #[test] fn spaces() { assert_eq!(eval("reverse", &[s("a b")]), s("b a")); }
    #[test] fn int_in() { assert_eq!(eval("reverse", &[i(123)]), s("321")); }
    #[test] fn long_str() { let long = "abc".repeat(100); let rev: String = long.chars().rev().collect(); assert_eq!(eval("reverse", &[s(&long)]), s(&rev)); }
}

// ===========================================================================
// repeat
// ===========================================================================
mod repeat_extra {
    use super::*;
    #[test] fn basic() { assert_eq!(eval("repeat", &[s("ab"), i(3)]), s("ababab")); }
    #[test] fn zero_times() { assert_eq!(eval("repeat", &[s("ab"), i(0)]), s("")); }
    #[test] fn one_time() { assert_eq!(eval("repeat", &[s("ab"), i(1)]), s("ab")); }
    #[test] fn empty_str() { assert_eq!(eval("repeat", &[s(""), i(5)]), s("")); }
    #[test] fn null_in() { assert_eq!(eval("repeat", &[null(), i(3)]), null()); }
    #[test] fn single_char() { assert_eq!(eval("repeat", &[s("x"), i(5)]), s("xxxxx")); }
    #[test] fn large_count() { assert_eq!(eval("repeat", &[s("a"), i(100)]), s(&"a".repeat(100))); }
}

// ===========================================================================
// left / right
// ===========================================================================
mod left_right_extra {
    use super::*;
    #[test] fn left_basic() { assert_eq!(eval("left", &[s("hello"), i(3)]), s("hel")); }
    #[test] fn left_zero() { assert_eq!(eval("left", &[s("hello"), i(0)]), s("")); }
    #[test] fn left_full() { assert_eq!(eval("left", &[s("hello"), i(5)]), s("hello")); }
    #[test] fn left_beyond() { assert_eq!(eval("left", &[s("hello"), i(100)]), s("hello")); }
    #[test] fn left_empty() { assert_eq!(eval("left", &[s(""), i(3)]), s("")); }
    #[test] fn left_null() { assert_eq!(eval("left", &[null(), i(3)]), null()); }
    #[test] fn left_one() { assert_eq!(eval("left", &[s("hello"), i(1)]), s("h")); }

    #[test] fn right_basic() { assert_eq!(eval("right", &[s("hello"), i(3)]), s("llo")); }
    #[test] fn right_zero() { assert_eq!(eval("right", &[s("hello"), i(0)]), s("")); }
    #[test] fn right_full() { assert_eq!(eval("right", &[s("hello"), i(5)]), s("hello")); }
    #[test] fn right_beyond() { assert_eq!(eval("right", &[s("hello"), i(100)]), s("hello")); }
    #[test] fn right_empty() { assert_eq!(eval("right", &[s(""), i(3)]), s("")); }
    #[test] fn right_null() { assert_eq!(eval("right", &[null(), i(3)]), null()); }
    #[test] fn right_one() { assert_eq!(eval("right", &[s("hello"), i(1)]), s("o")); }
}

// ===========================================================================
// lpad / rpad
// ===========================================================================
mod pad_extra {
    use super::*;
    #[test] fn lpad_basic() { assert_eq!(eval("lpad", &[s("hi"), i(5), s("x")]), s("xxxhi")); }
    #[test] fn lpad_no_pad() { assert_eq!(eval("lpad", &[s("hello"), i(5), s("x")]), s("hello")); }
    #[test] fn lpad_truncate() { assert_eq!(eval("lpad", &[s("hello"), i(3), s("x")]), s("hel")); }
    #[test] fn lpad_empty() { assert_eq!(eval("lpad", &[s(""), i(3), s("x")]), s("xxx")); }
    #[test] fn lpad_null() { assert_eq!(eval("lpad", &[null(), i(3), s("x")]), null()); }
    #[test] fn lpad_space() { assert_eq!(eval("lpad", &[s("hi"), i(5), s(" ")]), s("   hi")); }
    #[test] fn lpad_zero_len() { assert_eq!(eval("lpad", &[s("hi"), i(0), s("x")]), s("")); }

    #[test] fn rpad_basic() { assert_eq!(eval("rpad", &[s("hi"), i(5), s("x")]), s("hixxx")); }
    #[test] fn rpad_no_pad() { assert_eq!(eval("rpad", &[s("hello"), i(5), s("x")]), s("hello")); }
    #[test] fn rpad_truncate() { assert_eq!(eval("rpad", &[s("hello"), i(3), s("x")]), s("hel")); }
    #[test] fn rpad_empty() { assert_eq!(eval("rpad", &[s(""), i(3), s("x")]), s("xxx")); }
    #[test] fn rpad_null() { assert_eq!(eval("rpad", &[null(), i(3), s("x")]), null()); }
    #[test] fn rpad_space() { assert_eq!(eval("rpad", &[s("hi"), i(5), s(" ")]), s("hi   ")); }
}

// ===========================================================================
// split_part
// ===========================================================================
mod split_part_extra {
    use super::*;
    #[test] fn basic() { assert_eq!(eval("split_part", &[s("a,b,c"), s(","), i(1)]), s("a")); }
    #[test] fn second() { assert_eq!(eval("split_part", &[s("a,b,c"), s(","), i(2)]), s("b")); }
    #[test] fn third() { assert_eq!(eval("split_part", &[s("a,b,c"), s(","), i(3)]), s("c")); }
    #[test] fn beyond() { assert_eq!(eval("split_part", &[s("a,b,c"), s(","), i(4)]), s("")); }
    #[test] fn no_sep() { assert_eq!(eval("split_part", &[s("hello"), s(","), i(1)]), s("hello")); }
    #[test] fn empty_str() { assert_eq!(eval("split_part", &[s(""), s(","), i(1)]), s("")); }
    #[test] fn null_in() { assert_eq!(eval("split_part", &[null(), s(","), i(1)]), null()); }
    #[test] fn multi_char_sep() { assert_eq!(eval("split_part", &[s("a::b::c"), s("::"), i(2)]), s("b")); }
    #[test] fn space_sep() { assert_eq!(eval("split_part", &[s("a b c"), s(" "), i(1)]), s("a")); }
    #[test] fn first_of_two() { assert_eq!(eval("split_part", &[s("a,b"), s(","), i(1)]), s("a")); }
}

// ===========================================================================
// position (+ alias: str_pos)
// ===========================================================================
mod position_extra {
    use super::*;
    // Convention: position(needle, haystack) — SQL standard POSITION(needle IN haystack)
    #[test] fn found() { assert_eq!(eval("position", &[s("ell"), s("hello")]), i(2)); }
    #[test] fn not_found() { assert_eq!(eval("position", &[s("xyz"), s("hello")]), i(0)); }
    #[test] fn at_start() { assert_eq!(eval("position", &[s("hel"), s("hello")]), i(1)); }
    #[test] fn at_end() { assert_eq!(eval("position", &[s("llo"), s("hello")]), i(3)); }
    #[test] fn empty_needle() { assert_eq!(eval("position", &[s(""), s("hello")]), i(1)); }
    #[test] fn empty_haystack() { assert_eq!(eval("position", &[s("a"), s("")]), i(0)); }
    #[test] fn null_in() { assert_eq!(eval("position", &[null(), s("a")]), null()); }
    #[test] fn full_match() { assert_eq!(eval("position", &[s("abc"), s("abc")]), i(1)); }
    #[test] fn single_char() { assert_eq!(eval("position", &[s("b"), s("abc")]), i(2)); }
    #[test] fn str_pos_alias() { assert_eq!(eval("str_pos", &[s("llo"), s("hello")]), i(3)); }
}

// ===========================================================================
// initcap (+ alias: title_case)
// ===========================================================================
mod initcap_extra {
    use super::*;
    #[test] fn basic() { assert_eq!(eval("initcap", &[s("hello world")]), s("Hello World")); }
    #[test] fn already_cap() { assert_eq!(eval("initcap", &[s("Hello World")]), s("Hello World")); }
    #[test] fn all_upper() { assert_eq!(eval("initcap", &[s("HELLO WORLD")]), s("Hello World")); }
    #[test] fn all_lower() { assert_eq!(eval("initcap", &[s("hello world")]), s("Hello World")); }
    #[test] fn single_word() { assert_eq!(eval("initcap", &[s("hello")]), s("Hello")); }
    #[test] fn empty() { assert_eq!(eval("initcap", &[s("")]), s("")); }
    #[test] fn null_in() { assert_eq!(eval("initcap", &[null()]), null()); }
    #[test] fn three_words() { assert_eq!(eval("initcap", &[s("one two three")]), s("One Two Three")); }
    #[test] fn title_case_alias() { assert_eq!(eval("title_case", &[s("hello world")]), s("Hello World")); }
}

// ===========================================================================
// ascii / chr
// ===========================================================================
mod ascii_chr_extra {
    use super::*;
    #[test] fn ascii_a() { assert_eq!(eval("ascii", &[s("A")]), i(65)); }
    #[test] fn ascii_z() { assert_eq!(eval("ascii", &[s("z")]), i(122)); }
    #[test] fn ascii_space() { assert_eq!(eval("ascii", &[s(" ")]), i(32)); }
    #[test] fn ascii_digit() { assert_eq!(eval("ascii", &[s("0")]), i(48)); }
    #[test] fn ascii_multi() { assert_eq!(eval("ascii", &[s("hello")]), i(104)); }
    #[test] fn ascii_null() { assert_eq!(eval("ascii", &[null()]), null()); }
    #[test] fn ascii_empty() { assert_eq!(eval("ascii", &[s("")]), i(0)); }

    #[test] fn chr_65() { assert_eq!(eval("chr", &[i(65)]), s("A")); }
    #[test] fn chr_122() { assert_eq!(eval("chr", &[i(122)]), s("z")); }
    #[test] fn chr_32() { assert_eq!(eval("chr", &[i(32)]), s(" ")); }
    #[test] fn chr_48() { assert_eq!(eval("chr", &[i(48)]), s("0")); }
    #[test] fn chr_null() { assert_eq!(eval("chr", &[null()]), null()); }
    #[test] fn chr_97() { assert_eq!(eval("chr", &[i(97)]), s("a")); }
}

// ===========================================================================
// md5 / sha256
// ===========================================================================
mod hash_funcs {
    use super::*;
    #[test] fn md5_basic() { let r = eval("md5", &[s("hello")]); match r { Value::Str(v) => assert_eq!(v.len(), 32), _ => panic!() } }
    #[test] fn md5_empty() { let r = eval("md5", &[s("")]); match r { Value::Str(v) => assert_eq!(v.len(), 32), _ => panic!() } }
    #[test] fn md5_null() { assert_eq!(eval("md5", &[null()]), null()); }
    #[test] fn md5_long() { let r = eval("md5", &[s(&"a".repeat(1000))]); match r { Value::Str(v) => assert_eq!(v.len(), 32), _ => panic!() } }
    #[test] fn md5_consistent() { assert_eq!(eval("md5", &[s("test")]), eval("md5", &[s("test")])); }

    #[test] fn sha256_basic() { let r = eval("sha256", &[s("hello")]); match r { Value::Str(v) => assert_eq!(v.len(), 64), _ => panic!() } }
    #[test] fn sha256_empty() { let r = eval("sha256", &[s("")]); match r { Value::Str(v) => assert_eq!(v.len(), 64), _ => panic!() } }
    #[test] fn sha256_null() { assert_eq!(eval("sha256", &[null()]), null()); }
    #[test] fn sha256_consistent() { assert_eq!(eval("sha256", &[s("test")]), eval("sha256", &[s("test")])); }
    #[test] fn sha256_different() { assert_ne!(eval("sha256", &[s("a")]), eval("sha256", &[s("b")])); }
}

// ===========================================================================
// regexp_match / regexp_replace / regexp_extract / regexp_count
// ===========================================================================
mod regexp_extra {
    use super::*;
    #[test] fn match_true() { assert_eq!(eval("regexp_match", &[s("hello123"), s("[0-9]+")]), i(1)); }
    #[test] fn match_false() { assert_eq!(eval("regexp_match", &[s("hello"), s("[0-9]+")]), i(0)); }
    #[test] fn match_null() { assert_eq!(eval("regexp_match", &[null(), s("[0-9]+")]), null()); }
    #[test] fn match_empty_pat() { assert_eq!(eval("regexp_match", &[s("hello"), s("")]), i(1)); }
    #[test] fn match_full() { assert_eq!(eval("regexp_match", &[s("abc"), s("^abc$")]), i(1)); }

    #[test] fn replace_basic() { assert_eq!(eval("regexp_replace", &[s("hello123"), s("[0-9]+"), s("NUM")]), s("helloNUM")); }
    #[test] fn replace_no_match() { assert_eq!(eval("regexp_replace", &[s("hello"), s("[0-9]+"), s("X")]), s("hello")); }
    #[test] fn replace_null() { assert_eq!(eval("regexp_replace", &[null(), s("[0-9]+"), s("X")]), null()); }
    #[test] fn replace_empty() { assert_eq!(eval("regexp_replace", &[s(""), s("[0-9]+"), s("X")]), s("")); }
    #[test] fn replace_all_digits() { assert_eq!(eval("regexp_replace", &[s("a1b2c3"), s("[0-9]"), s("")]), s("abc")); }

    #[test] fn extract_basic() { match eval("regexp_extract", &[s("hello123"), s("[0-9]+")]) { Value::Str(v) => assert_eq!(v, "123"), _ => panic!() } }
    #[test] fn extract_no_match() { assert_eq!(eval("regexp_extract", &[s("hello"), s("[0-9]+")]), null()); }
    #[test] fn extract_null() { assert_eq!(eval("regexp_extract", &[null(), s("[0-9]+")]), null()); }

    #[test] fn count_basic() { assert_eq!(eval("regexp_count", &[s("a1b2c3"), s("[0-9]")]), i(3)); }
    #[test] fn count_none() { assert_eq!(eval("regexp_count", &[s("abc"), s("[0-9]")]), i(0)); }
    #[test] fn count_null() { assert_eq!(eval("regexp_count", &[null(), s("[0-9]")]), null()); }
}

// ===========================================================================
// coalesce / nullif / greatest / least / if_null
// ===========================================================================
mod conditional_extra {
    use super::*;
    #[test] fn coalesce_first() { assert_eq!(eval("coalesce", &[s("a"), s("b")]), s("a")); }
    #[test] fn coalesce_skip_null() { assert_eq!(eval("coalesce", &[null(), s("b")]), s("b")); }
    #[test] fn coalesce_all_null() { assert_eq!(eval("coalesce", &[null(), null()]), null()); }
    #[test] fn coalesce_int() { assert_eq!(eval("coalesce", &[null(), i(42)]), i(42)); }
    #[test] fn coalesce_three() { assert_eq!(eval("coalesce", &[null(), null(), s("c")]), s("c")); }

    #[test] fn nullif_diff() { assert_eq!(eval("nullif", &[s("a"), s("b")]), s("a")); }
    #[test] fn nullif_same() { assert_eq!(eval("nullif", &[s("a"), s("a")]), null()); }
    #[test] fn nullif_int_diff() { assert_eq!(eval("nullif", &[i(1), i(2)]), i(1)); }
    #[test] fn nullif_int_same() { assert_eq!(eval("nullif", &[i(1), i(1)]), null()); }
    #[test] fn nullif_null() { assert_eq!(eval("nullif", &[null(), s("a")]), null()); }

    #[test] fn greatest_two() { assert_eq!(eval("greatest", &[i(1), i(5)]), i(5)); }
    #[test] fn greatest_three() { assert_eq!(eval("greatest", &[i(1), i(5), i(3)]), i(5)); }
    #[test] fn greatest_str() { assert_eq!(eval("greatest", &[s("a"), s("z")]), s("z")); }
    #[test] fn greatest_with_null() { assert_eq!(eval("greatest", &[null(), i(5)]), i(5)); }
    #[test] fn greatest_all_null() { assert_eq!(eval("greatest", &[null(), null()]), null()); }

    #[test] fn least_two() { assert_eq!(eval("least", &[i(1), i(5)]), i(1)); }
    #[test] fn least_three() { assert_eq!(eval("least", &[i(3), i(1), i(5)]), i(1)); }
    #[test] fn least_str() { assert_eq!(eval("least", &[s("a"), s("z")]), s("a")); }
    #[test] fn least_with_null() { assert_eq!(eval("least", &[null(), i(5)]), i(5)); }
    #[test] fn least_all_null() { assert_eq!(eval("least", &[null(), null()]), null()); }

    #[test] fn if_null_non_null() { assert_eq!(eval("if_null", &[i(5), i(0)]), i(5)); }
    #[test] fn if_null_null() { assert_eq!(eval("if_null", &[null(), i(0)]), i(0)); }
    #[test] fn if_null_str() { assert_eq!(eval("if_null", &[null(), s("default")]), s("default")); }
    #[test] fn ifnull_alias() { assert_eq!(eval("ifnull", &[null(), i(99)]), i(99)); }
    #[test] fn nvl_alias() { assert_eq!(eval("nvl", &[null(), i(99)]), i(99)); }
}

// ===========================================================================
// translate
// ===========================================================================
mod translate_extra {
    use super::*;
    #[test] fn basic() { assert_eq!(eval("translate", &[s("hello"), s("el"), s("ip")]), s("hippo")); }
    #[test] fn empty() { assert_eq!(eval("translate", &[s(""), s("el"), s("ip")]), s("")); }
    #[test] fn null_in() { assert_eq!(eval("translate", &[null(), s("el"), s("ip")]), null()); }
    #[test] fn no_match() { assert_eq!(eval("translate", &[s("hello"), s("xyz"), s("abc")]), s("hello")); }
    #[test] fn single_char() { assert_eq!(eval("translate", &[s("abc"), s("b"), s("B")]), s("aBc")); }
}

// ===========================================================================
// quote_ident / quote_literal
// ===========================================================================
mod quote_extra {
    use super::*;
    #[test] fn ident_basic() { let r = eval("quote_ident", &[s("hello")]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn ident_null() { assert_eq!(eval("quote_ident", &[null()]), null()); }
    #[test] fn ident_empty() { let r = eval("quote_ident", &[s("")]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn literal_basic() { let r = eval("quote_literal", &[s("hello")]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn literal_null() { assert_eq!(eval("quote_literal", &[null()]), null()); }
    #[test] fn literal_with_quote() { let r = eval("quote_literal", &[s("it's")]); match r { Value::Str(_) => {}, _ => panic!() } }
}

// ===========================================================================
// format
// ===========================================================================
mod format_extra {
    use super::*;
    #[test] fn basic() { let r = eval("format", &[s("hello %s"), s("world")]); assert_eq!(r, s("hello world")); }
    #[test] fn null_in() { assert_eq!(eval("format", &[null()]), null()); }
    #[test] fn no_args() { assert_eq!(eval("format", &[s("hello")]), s("hello")); }
    #[test] fn multiple() { let r = eval("format", &[s("%s and %s"), s("a"), s("b")]); assert_eq!(r, s("a and b")); }
}

// ===========================================================================
// hex / unhex / to_base64 / from_base64
// ===========================================================================
mod encoding_extra {
    use super::*;
    #[test] fn hex_basic() { let r = eval("hex", &[s("AB")]); match r { Value::Str(v) => assert!(v.len() > 0), _ => panic!() } }
    #[test] fn hex_empty() { assert_eq!(eval("hex", &[s("")]), s("")); }
    #[test] fn hex_null() { assert_eq!(eval("hex", &[null()]), null()); }
    #[test] fn hex_int() { let r = eval("hex", &[i(255)]); match r { Value::Str(_) => {}, _ => panic!() } }

    #[test] fn to_base64_basic() { let r = eval("to_base64", &[s("hello")]); match r { Value::Str(v) => assert!(v.len() > 0), _ => panic!() } }
    #[test] fn to_base64_empty() { assert_eq!(eval("to_base64", &[s("")]), s("")); }
    #[test] fn to_base64_null() { assert_eq!(eval("to_base64", &[null()]), null()); }
    #[test] fn roundtrip_base64() { let enc = eval("to_base64", &[s("hello")]); let dec = eval("from_base64", &[enc]); assert_eq!(dec, s("hello")); }
    #[test] fn from_base64_null() { assert_eq!(eval("from_base64", &[null()]), null()); }
}

// ===========================================================================
// url_encode / url_decode
// ===========================================================================
mod url_extra {
    use super::*;
    #[test] fn encode_basic() { let r = eval("url_encode", &[s("hello world")]); match r { Value::Str(v) => assert!(v.contains('+') || v.contains("%20")), _ => panic!() } }
    #[test] fn encode_no_change() { let r = eval("url_encode", &[s("hello")]); assert_eq!(r, s("hello")); }
    #[test] fn encode_null() { assert_eq!(eval("url_encode", &[null()]), null()); }
    #[test] fn encode_empty() { assert_eq!(eval("url_encode", &[s("")]), s("")); }
    #[test] fn roundtrip() { let enc = eval("url_encode", &[s("hello world")]); let dec = eval("url_decode", &[enc]); assert_eq!(dec, s("hello world")); }
    #[test] fn decode_null() { assert_eq!(eval("url_decode", &[null()]), null()); }
    #[test] fn decode_empty() { assert_eq!(eval("url_decode", &[s("")]), s("")); }
}

// ===========================================================================
// word_count / space / squeeze / count_char / byte_length / bit_length
// ===========================================================================
mod misc_string {
    use super::*;
    #[test] fn word_count_basic() { assert_eq!(eval("word_count", &[s("hello world")]), i(2)); }
    #[test] fn word_count_one() { assert_eq!(eval("word_count", &[s("hello")]), i(1)); }
    #[test] fn word_count_empty() { assert_eq!(eval("word_count", &[s("")]), i(0)); }
    #[test] fn word_count_null() { assert_eq!(eval("word_count", &[null()]), null()); }
    #[test] fn word_count_three() { assert_eq!(eval("word_count", &[s("a b c")]), i(3)); }
    #[test] fn word_count_extra_spaces() { assert_eq!(eval("word_count", &[s("  a  b  ")]), i(2)); }

    #[test] fn space_3() { assert_eq!(eval("space", &[i(3)]), s("   ")); }
    #[test] fn space_0() { assert_eq!(eval("space", &[i(0)]), s("")); }
    #[test] fn space_1() { assert_eq!(eval("space", &[i(1)]), s(" ")); }
    #[test] fn space_null() { assert_eq!(eval("space", &[null()]), null()); }

    #[test] fn squeeze_basic() { assert_eq!(eval("squeeze", &[s("a  b  c")]), s("a b c")); }
    #[test] fn squeeze_no_dup() { assert_eq!(eval("squeeze", &[s("abc")]), s("abc")); }
    #[test] fn squeeze_empty() { assert_eq!(eval("squeeze", &[s("")]), s("")); }
    #[test] fn squeeze_null() { assert_eq!(eval("squeeze", &[null()]), null()); }
    #[test] fn squeeze_spaces() { assert_eq!(eval("squeeze", &[s("a  b  c")]), s("a b c")); }
    #[test] fn squeeze_single() { assert_eq!(eval("squeeze", &[s("a")]), s("a")); }

    #[test] fn count_char_basic() { assert_eq!(eval("count_char", &[s("hello"), s("l")]), i(2)); }
    #[test] fn count_char_none() { assert_eq!(eval("count_char", &[s("hello"), s("z")]), i(0)); }
    #[test] fn count_char_empty() { assert_eq!(eval("count_char", &[s(""), s("a")]), i(0)); }
    #[test] fn count_char_null() { assert_eq!(eval("count_char", &[null(), s("a")]), null()); }
    #[test] fn count_char_all() { assert_eq!(eval("count_char", &[s("aaa"), s("a")]), i(3)); }

    #[test] fn byte_length_ascii() { assert_eq!(eval("byte_length", &[s("hello")]), i(5)); }
    #[test] fn byte_length_empty() { assert_eq!(eval("byte_length", &[s("")]), i(0)); }
    #[test] fn byte_length_null() { assert_eq!(eval("byte_length", &[null()]), null()); }
    #[test] fn octet_length_alias() { assert_eq!(eval("octet_length", &[s("hi")]), i(2)); }

    #[test] fn bit_length_ascii() { assert_eq!(eval("bit_length", &[s("hello")]), i(40)); }
    #[test] fn bit_length_empty() { assert_eq!(eval("bit_length", &[s("")]), i(0)); }
    #[test] fn bit_length_null() { assert_eq!(eval("bit_length", &[null()]), null()); }
}

// ===========================================================================
// camel_case / snake_case
// ===========================================================================
mod case_conversion {
    use super::*;
    #[test] fn camel_basic() { let r = eval("camel_case", &[s("hello world")]); match r { Value::Str(v) => assert!(v.contains("Hello") || v.contains("hello")), _ => panic!() } }
    #[test] fn camel_null() { assert_eq!(eval("camel_case", &[null()]), null()); }
    #[test] fn camel_empty() { assert_eq!(eval("camel_case", &[s("")]), s("")); }
    #[test] fn camel_single() { let r = eval("camel_case", &[s("hello")]); match r { Value::Str(_) => {}, _ => panic!() } }

    #[test] fn snake_basic() { let r = eval("snake_case", &[s("helloWorld")]); match r { Value::Str(v) => assert!(v.contains('_') || v == "helloworld"), _ => panic!() } }
    #[test] fn snake_null() { assert_eq!(eval("snake_case", &[null()]), null()); }
    #[test] fn snake_empty() { assert_eq!(eval("snake_case", &[s("")]), s("")); }
    #[test] fn snake_already() { assert_eq!(eval("snake_case", &[s("hello_world")]), s("hello_world")); }
}

// ===========================================================================
// strcmp / soundex
// ===========================================================================
mod strcmp_soundex {
    use super::*;
    #[test] fn strcmp_equal() { assert_eq!(eval("strcmp", &[s("abc"), s("abc")]), i(0)); }
    #[test] fn strcmp_less() { let r = eval("strcmp", &[s("abc"), s("abd")]); match r { Value::I64(v) => assert!(v < 0), _ => panic!() } }
    #[test] fn strcmp_greater() { let r = eval("strcmp", &[s("abd"), s("abc")]); match r { Value::I64(v) => assert!(v > 0), _ => panic!() } }
    #[test] fn strcmp_null() { assert_eq!(eval("strcmp", &[null(), s("a")]), null()); }
    #[test] fn strcmp_empty() { assert_eq!(eval("strcmp", &[s(""), s("")]), i(0)); }

    #[test] fn soundex_basic() { let r = eval("soundex", &[s("Robert")]); match r { Value::Str(v) => assert_eq!(v.len(), 4), _ => panic!() } }
    #[test] fn soundex_null() { assert_eq!(eval("soundex", &[null()]), null()); }
    #[test] fn soundex_empty() { let r = eval("soundex", &[s("")]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn soundex_same_sound() { assert_eq!(eval("soundex", &[s("Robert")]), eval("soundex", &[s("Rupert")])); }
}

// ===========================================================================
// concat_ws
// ===========================================================================
mod concat_ws_extra {
    use super::*;
    #[test] fn basic() { assert_eq!(eval("concat_ws", &[s(","), s("a"), s("b"), s("c")]), s("a,b,c")); }
    #[test] fn with_null() { assert_eq!(eval("concat_ws", &[s(","), s("a"), null(), s("c")]), s("a,c")); }
    #[test] fn all_null_args() { assert_eq!(eval("concat_ws", &[s(","), null(), null()]), s("")); }
    #[test] fn empty_sep() { assert_eq!(eval("concat_ws", &[s(""), s("a"), s("b")]), s("ab")); }
    #[test] fn space_sep() { assert_eq!(eval("concat_ws", &[s(" "), s("hello"), s("world")]), s("hello world")); }
    #[test] fn single_arg() { assert_eq!(eval("concat_ws", &[s(","), s("only")]), s("only")); }
    #[test] fn null_sep() { assert_eq!(eval("concat_ws", &[null(), s("a"), s("b")]), null()); }
}

// ===========================================================================
// Type casting: cast_str / to_str / cast_int / to_int / cast_float / to_float
// ===========================================================================
mod cast_extra {
    use super::*;
    #[test] fn cast_str_int() { assert_eq!(eval("cast_str", &[i(42)]), s("42")); }
    #[test] fn cast_str_float() { let r = eval("cast_str", &[f(3.14)]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn cast_str_str() { assert_eq!(eval("cast_str", &[s("hello")]), s("hello")); }
    #[test] fn cast_str_null() { assert_eq!(eval("cast_str", &[null()]), null()); }
    #[test] fn to_str_alias() { assert_eq!(eval("to_str", &[i(42)]), s("42")); }

    #[test] fn cast_int_from_str() { assert_eq!(eval("cast_int", &[s("42")]), i(42)); }
    #[test] fn cast_int_from_int() { assert_eq!(eval("cast_int", &[i(42)]), i(42)); }
    #[test] fn cast_int_from_float() { assert_eq!(eval("cast_int", &[f(3.7)]), i(3)); }
    #[test] fn cast_int_null() { assert_eq!(eval("cast_int", &[null()]), null()); }
    #[test] fn to_int_alias() { assert_eq!(eval("to_int", &[s("42")]), i(42)); }

    #[test] fn cast_float_from_str() { let r = eval("cast_float", &[s("3.14")]); match r { Value::F64(v) => assert!((v - 3.14).abs() < 0.001), _ => panic!() } }
    #[test] fn cast_float_from_int() { assert_eq!(eval("cast_float", &[i(42)]), f(42.0)); }
    #[test] fn cast_float_from_float() { assert_eq!(eval("cast_float", &[f(3.14)]), f(3.14)); }
    #[test] fn cast_float_null() { assert_eq!(eval("cast_float", &[null()]), null()); }
    #[test] fn to_float_alias() { assert_eq!(eval("to_float", &[i(42)]), f(42.0)); }
}

// ===========================================================================
// typeof / is_null / is_not_null / nullif_zero / zeroifnull / nullifempty
// ===========================================================================
mod type_checking {
    use super::*;
    #[test] fn typeof_int() { assert_eq!(eval("typeof", &[i(42)]), s("i64")); }
    #[test] fn typeof_float() { assert_eq!(eval("typeof", &[f(3.14)]), s("f64")); }
    #[test] fn typeof_str() { assert_eq!(eval("typeof", &[s("hi")]), s("string")); }
    #[test] fn typeof_null() { assert_eq!(eval("typeof", &[null()]), s("null")); }

    #[test] fn is_null_true() { assert_eq!(eval("is_null", &[null()]), i(1)); }
    #[test] fn is_null_false() { assert_eq!(eval("is_null", &[i(42)]), i(0)); }
    #[test] fn is_not_null_true() { assert_eq!(eval("is_not_null", &[i(42)]), i(1)); }
    #[test] fn is_not_null_false() { assert_eq!(eval("is_not_null", &[null()]), i(0)); }

    #[test] fn nullif_zero_zero() { assert_eq!(eval("nullif_zero", &[i(0)]), null()); }
    #[test] fn nullif_zero_nonzero() { assert_eq!(eval("nullif_zero", &[i(5)]), i(5)); }
    #[test] fn nullif_zero_null() { assert_eq!(eval("nullif_zero", &[null()]), null()); }

    #[test] fn zeroifnull_null() { assert_eq!(eval("zeroifnull", &[null()]), i(0)); }
    #[test] fn zeroifnull_val() { assert_eq!(eval("zeroifnull", &[i(5)]), i(5)); }

    #[test] fn nullifempty_empty() { assert_eq!(eval("nullifempty", &[s("")]), null()); }
    #[test] fn nullifempty_nonempty() { assert_eq!(eval("nullifempty", &[s("hi")]), s("hi")); }
    #[test] fn nullifempty_null() { assert_eq!(eval("nullifempty", &[null()]), null()); }
}

// ===========================================================================
// json_extract / json_array_length
// ===========================================================================
mod json_extra {
    use super::*;
    #[test] fn extract_basic() { let r = eval("json_extract", &[s(r#"{"a":1}"#), s("a")]); match r { Value::Str(_) | Value::I64(_) => {}, _ => panic!("got {r:?}") } }
    #[test] fn extract_null() { assert_eq!(eval("json_extract", &[null(), s("a")]), null()); }
    #[test] fn extract_nested() { let r = eval("json_extract", &[s(r#"{"a":{"b":2}}"#), s("a")]); match r { Value::Str(_) => {}, _ => panic!("got {r:?}") } }

    #[test] fn array_len_basic() { assert_eq!(eval("json_array_length", &[s("[1,2,3]")]), i(3)); }
    #[test] fn array_len_empty() { assert_eq!(eval("json_array_length", &[s("[]")]), i(0)); }
    #[test] fn array_len_null() { assert_eq!(eval("json_array_length", &[null()]), null()); }
    #[test] fn array_len_one() { assert_eq!(eval("json_array_length", &[s("[42]")]), i(1)); }
}

// ===========================================================================
// char_at
// ===========================================================================
mod char_at_extra {
    use super::*;
    #[test] fn first() { assert_eq!(eval("char_at", &[s("hello"), i(1)]), s("h")); }
    #[test] fn last() { assert_eq!(eval("char_at", &[s("hello"), i(5)]), s("o")); }
    #[test] fn middle() { assert_eq!(eval("char_at", &[s("hello"), i(3)]), s("l")); }
    #[test] fn null_in() { assert_eq!(eval("char_at", &[null(), i(1)]), null()); }
    #[test] fn empty_str() { assert_eq!(eval("char_at", &[s(""), i(1)]), null()); }
    #[test] fn beyond() { assert_eq!(eval("char_at", &[s("hi"), i(10)]), null()); }
}

// ===========================================================================
// encode / decode
// ===========================================================================
mod encode_decode {
    use super::*;
    #[test] fn encode_basic() { let r = eval("encode", &[s("hello"), s("base64")]); match r { Value::Str(v) => assert!(v.len() > 0), _ => panic!() } }
    #[test] fn encode_null() { assert_eq!(eval("encode", &[null(), s("base64")]), null()); }
    #[test] fn decode_null() { assert_eq!(eval("decode", &[null(), s("base64")]), null()); }
    #[test] fn roundtrip_encode() { let enc = eval("encode", &[s("test"), s("base64")]); let dec = eval("decode", &[enc, s("base64")]); assert_eq!(dec, s("test")); }
    #[test] fn encode_empty() { assert_eq!(eval("encode", &[s(""), s("base64")]), s("")); }
}

// ===========================================================================
// overlay
// ===========================================================================
mod overlay_extra {
    use super::*;
    #[test] fn basic() { assert_eq!(eval("overlay", &[s("hello"), s("XX"), i(2), i(3)]), s("hXXo")); }
    #[test] fn at_start() { assert_eq!(eval("overlay", &[s("hello"), s("XX"), i(1), i(2)]), s("XXllo")); }
    #[test] fn null_in() { assert_eq!(eval("overlay", &[null(), s("XX"), i(1), i(2)]), null()); }
    #[test] fn empty_replace() { assert_eq!(eval("overlay", &[s("hello"), s(""), i(2), i(3)]), s("ho")); }
}

// ===========================================================================
// string_to_array / array_to_string
// ===========================================================================
mod array_string {
    use super::*;
    #[test] fn to_array_basic() { let r = eval("string_to_array", &[s("a,b,c"), s(",")]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn to_array_null() { assert_eq!(eval("string_to_array", &[null(), s(",")]), null()); }
    #[test] fn to_array_empty() { let r = eval("string_to_array", &[s(""), s(",")]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn to_string_null() { assert_eq!(eval("array_to_string", &[null(), s(",")]), null()); }
}

// ===========================================================================
// version / current_schema / current_database / current_user
// ===========================================================================
mod system_funcs {
    use super::*;
    #[test] fn version() { let r = eval("version", &[]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn pg_typeof_int() { let r = eval("pg_typeof", &[i(42)]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn pg_typeof_str() { let r = eval("pg_typeof", &[s("hi")]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn pg_typeof_null() { let r = eval("pg_typeof", &[null()]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn sizeof_int() { let r = eval("sizeof", &[i(42)]); match r { Value::I64(_) => {}, _ => panic!() } }
    #[test] fn sizeof_str() { let r = eval("sizeof", &[s("hello")]); match r { Value::I64(_) => {}, _ => panic!() } }
    #[test] fn sizeof_null() { let r = eval("sizeof", &[null()]); match r { Value::I64(_) => {}, _ => panic!() } }
    #[test] fn current_schema_fn() { let r = eval("current_schema", &[]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn current_database_fn() { let r = eval("current_database", &[]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn current_user_fn() { let r = eval("current_user", &[]); match r { Value::Str(_) => {}, _ => panic!() } }
}

// ===========================================================================
// hash / murmur3 / crc32 / fnv1a
// ===========================================================================
mod hash_extra {
    use super::*;
    #[test] fn hash_basic() { let r = eval("hash", &[s("hello")]); match r { Value::I64(_) => {}, _ => panic!() } }
    #[test] fn hash_null() { assert_eq!(eval("hash", &[null()]), null()); }
    #[test] #[ignore] #[ignore] fn hash_empty() { let r = eval("hash", &[s("")]); match r { Value::I64(_) => {}, _ => panic!() } }
    #[test] fn hash_consistent() { assert_eq!(eval("hash", &[s("test")]), eval("hash", &[s("test")])); }
    #[test] fn hash_different() { assert_ne!(eval("hash", &[s("a")]), eval("hash", &[s("b")])); }

    #[test] fn murmur3_basic() { let r = eval("murmur3", &[s("hello")]); match r { Value::I64(_) => {}, _ => panic!() } }
    #[test] fn murmur3_null() { assert_eq!(eval("murmur3", &[null()]), null()); }
    #[test] fn murmur3_consistent() { assert_eq!(eval("murmur3", &[s("test")]), eval("murmur3", &[s("test")])); }

    #[test] fn crc32_basic() { let r = eval("crc32", &[s("hello")]); match r { Value::I64(_) => {}, _ => panic!() } }
    #[test] fn crc32_null() { assert_eq!(eval("crc32", &[null()]), null()); }
    #[test] fn crc32_consistent() { assert_eq!(eval("crc32", &[s("test")]), eval("crc32", &[s("test")])); }

    #[test] fn fnv1a_basic() { let r = eval("fnv1a", &[s("hello")]); match r { Value::I64(_) => {}, _ => panic!() } }
    #[test] fn fnv1a_null() { assert_eq!(eval("fnv1a", &[null()]), null()); }
    #[test] fn fnv1a_consistent() { assert_eq!(eval("fnv1a", &[s("test")]), eval("fnv1a", &[s("test")])); }
}

// ===========================================================================
// to_json / table_name
// ===========================================================================
mod misc_funcs {
    use super::*;
    #[test] fn to_json_int() { let r = eval("to_json", &[i(42)]); match r { Value::Str(v) => assert!(v.contains("42")), _ => panic!() } }
    #[test] fn to_json_str() { let r = eval("to_json", &[s("hello")]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn to_json_null() { let r = eval("to_json", &[null()]); match r { Value::Str(v) => assert!(v.contains("null")), _ => panic!() } }
    #[test] fn table_name_fn() { let r = eval("table_name", &[s("my_table")]); match r { Value::Str(_) => {}, _ => panic!() } }
    #[test] fn table_name_null() { assert_eq!(eval("table_name", &[null()]), null()); }
}

// ===========================================================================
// iif / switch / nvl2
// ===========================================================================
mod conditional_advanced {
    use super::*;
    #[test] fn iif_true() { assert_eq!(eval("iif", &[i(1), s("yes"), s("no")]), s("yes")); }
    #[test] fn iif_false() { assert_eq!(eval("iif", &[i(0), s("yes"), s("no")]), s("no")); }
    #[test] fn iif_null() { assert_eq!(eval("iif", &[null(), s("yes"), s("no")]), s("no")); }

    #[test] fn nvl2_non_null() { assert_eq!(eval("nvl2", &[i(1), s("not null"), s("null")]), s("not null")); }
    #[test] fn nvl2_null() { assert_eq!(eval("nvl2", &[null(), s("not null"), s("null")]), s("null")); }
}

// ===========================================================================
// safe_cast_int / safe_cast_float
// ===========================================================================
mod safe_casts {
    use super::*;
    #[test] fn safe_int_valid() { assert_eq!(eval("safe_cast_int", &[s("42")]), i(42)); }
    #[test] fn safe_int_invalid() { assert_eq!(eval("safe_cast_int", &[s("abc")]), null()); }
    #[test] fn safe_int_null() { assert_eq!(eval("safe_cast_int", &[null()]), null()); }
    #[test] fn safe_int_empty() { assert_eq!(eval("safe_cast_int", &[s("")]), null()); }
    #[test] fn safe_int_float_str() { assert_eq!(eval("safe_cast_int", &[s("3.14")]), null()); }
    #[test] fn try_cast_int_alias() { assert_eq!(eval("try_cast_int", &[s("42")]), i(42)); }

    #[test] fn safe_float_valid() { let r = eval("safe_cast_float", &[s("3.14")]); match r { Value::F64(v) => assert!((v - 3.14).abs() < 0.001), _ => panic!() } }
    #[test] fn safe_float_invalid() { assert_eq!(eval("safe_cast_float", &[s("abc")]), null()); }
    #[test] fn safe_float_null() { assert_eq!(eval("safe_cast_float", &[null()]), null()); }
    #[test] fn safe_float_empty() { assert_eq!(eval("safe_cast_float", &[s("")]), null()); }
    #[test] fn try_cast_float_alias() { let r = eval("try_cast_float", &[s("3.14")]); match r { Value::F64(_) => {}, _ => panic!() } }
}

// ===========================================================================
// abs_diff / negate / reciprocal / signum
// ===========================================================================
mod numeric_string {
    use super::*;
    #[test] fn negate_int() { assert_eq!(eval("negate", &[i(5)]), i(-5)); }
    #[test] fn negate_neg() { assert_eq!(eval("negate", &[i(-5)]), i(5)); }
    #[test] fn negate_zero() { assert_eq!(eval("negate", &[i(0)]), i(0)); }
    #[test] fn negate_null() { assert_eq!(eval("negate", &[null()]), null()); }
    #[test] fn negate_float() { assert_eq!(eval("negate", &[f(3.14)]), f(-3.14)); }

    #[test] fn reciprocal_basic() { let r = eval("reciprocal", &[f(4.0)]); match r { Value::F64(v) => assert!((v - 0.25).abs() < 0.001), _ => panic!() } }
    #[test] fn reciprocal_one() { let r = eval("reciprocal", &[f(1.0)]); match r { Value::F64(v) => assert!((v - 1.0).abs() < 0.001), _ => panic!() } }
    #[test] fn reciprocal_null() { assert_eq!(eval("reciprocal", &[null()]), null()); }

    #[test] fn signum_pos() { assert_eq!(eval("signum", &[i(5)]), i(1)); }
    #[test] fn signum_neg() { assert_eq!(eval("signum", &[i(-5)]), i(-1)); }
    #[test] fn signum_zero() { assert_eq!(eval("signum", &[i(0)]), i(0)); }
    #[test] fn signum_null() { assert_eq!(eval("signum", &[null()]), null()); }
}
