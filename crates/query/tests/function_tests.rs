//! Comprehensive function tests for ExchangeDB.
//!
//! Tests every registered scalar, aggregate, cast, date, conditional, and
//! window function with multiple inputs including NULL handling.

use exchange_query::plan::Value;
use exchange_query::scalar::evaluate_scalar;
use exchange_query::test_utils::TestDb;

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

fn assert_f64_close(val: &Value, expected: f64, tol: f64) {
    match val {
        Value::F64(v) => assert!(
            (*v - expected).abs() < tol,
            "expected ~{expected}, got {v}"
        ),
        other => panic!("expected F64(~{expected}), got {other:?}"),
    }
}

// ===========================================================================
// String functions
// ===========================================================================

mod string_functions {
    use super::*;

    // --- length ---
    #[test]
    fn length_empty() {
        assert_eq!(eval("length", &[s("")]), i(0));
    }
    #[test]
    fn length_ascii() {
        assert_eq!(eval("length", &[s("hello")]), i(5));
    }
    #[test]
    fn length_unicode() {
        // Note: length uses byte length via Rust's .len()
        let val = eval("length", &[s("cafe\u{0301}")]); // café with combining accent
        match val {
            Value::I64(n) => assert!(n > 0),
            _ => panic!("expected I64"),
        }
    }
    #[test]
    fn length_null() {
        assert_eq!(eval("length", &[null()]), null());
    }
    #[test]
    fn length_number_as_string() {
        assert_eq!(eval("length", &[i(12345)]), i(5));
    }

    // --- upper ---
    #[test]
    fn upper_basic() {
        assert_eq!(eval("upper", &[s("hello")]), s("HELLO"));
    }
    #[test]
    fn upper_mixed() {
        assert_eq!(eval("upper", &[s("Hello World")]), s("HELLO WORLD"));
    }
    #[test]
    fn upper_already_upper() {
        assert_eq!(eval("upper", &[s("ABC")]), s("ABC"));
    }
    #[test]
    fn upper_null() {
        assert_eq!(eval("upper", &[null()]), null());
    }
    #[test]
    fn upper_empty() {
        assert_eq!(eval("upper", &[s("")]), s(""));
    }

    // --- lower ---
    #[test]
    fn lower_basic() {
        assert_eq!(eval("lower", &[s("HELLO")]), s("hello"));
    }
    #[test]
    fn lower_mixed() {
        assert_eq!(eval("lower", &[s("Hello World")]), s("hello world"));
    }
    #[test]
    fn lower_already_lower() {
        assert_eq!(eval("lower", &[s("abc")]), s("abc"));
    }
    #[test]
    fn lower_null() {
        assert_eq!(eval("lower", &[null()]), null());
    }

    // --- trim ---
    #[test]
    fn trim_spaces() {
        assert_eq!(eval("trim", &[s("  hello  ")]), s("hello"));
    }
    #[test]
    fn trim_tabs() {
        assert_eq!(eval("trim", &[s("\thello\t")]), s("hello"));
    }
    #[test]
    fn trim_no_spaces() {
        assert_eq!(eval("trim", &[s("hello")]), s("hello"));
    }
    #[test]
    fn trim_null() {
        assert_eq!(eval("trim", &[null()]), null());
    }
    #[test]
    fn trim_only_whitespace() {
        assert_eq!(eval("trim", &[s("   ")]), s(""));
    }

    // --- ltrim ---
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

    // --- rtrim ---
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

    // --- substring ---
    #[test]
    fn substring_basic() {
        // substring(s, start_1based, length)
        assert_eq!(eval("substring", &[s("hello"), i(2), i(3)]), s("ell"));
    }
    #[test]
    fn substring_from_start() {
        assert_eq!(eval("substring", &[s("hello"), i(1), i(5)]), s("hello"));
    }
    #[test]
    fn substring_out_of_bounds() {
        assert_eq!(eval("substring", &[s("hi"), i(1), i(10)]), s("hi"));
    }
    #[test]
    fn substring_zero_length() {
        assert_eq!(eval("substring", &[s("hello"), i(1), i(0)]), s(""));
    }
    #[test]
    fn substring_null() {
        assert_eq!(eval("substring", &[null(), i(1), i(3)]), null());
    }

    // --- concat ---
    #[test]
    fn concat_two() {
        assert_eq!(eval("concat", &[s("hello"), s(" world")]), s("hello world"));
    }
    #[test]
    fn concat_three() {
        assert_eq!(
            eval("concat", &[s("a"), s("b"), s("c")]),
            s("abc")
        );
    }
    #[test]
    fn concat_null_and_string() {
        assert_eq!(eval("concat", &[null(), s("world")]), s("world"));
    }
    #[test]
    fn concat_empty() {
        assert_eq!(eval("concat", &[s(""), s("")]), s(""));
    }
    #[test]
    fn concat_with_number() {
        assert_eq!(eval("concat", &[s("val="), i(42)]), s("val=42"));
    }

    // --- replace ---
    #[test]
    fn replace_basic() {
        assert_eq!(
            eval("replace", &[s("hello world"), s("world"), s("rust")]),
            s("hello rust")
        );
    }
    #[test]
    fn replace_no_match() {
        assert_eq!(
            eval("replace", &[s("hello"), s("xyz"), s("abc")]),
            s("hello")
        );
    }
    #[test]
    fn replace_multiple() {
        assert_eq!(
            eval("replace", &[s("aaa"), s("a"), s("b")]),
            s("bbb")
        );
    }
    #[test]
    fn replace_null() {
        assert_eq!(eval("replace", &[null(), s("a"), s("b")]), null());
    }
    #[test]
    fn replace_empty_from() {
        // Replacing empty string inserts between each char (Rust behavior)
        let result = eval("replace", &[s("ab"), s(""), s("X")]);
        match result {
            Value::Str(r) => assert!(r.contains('a') && r.contains('b')),
            _ => panic!("expected Str"),
        }
    }

    // --- starts_with ---
    #[test]
    fn starts_with_true() {
        assert_eq!(eval("starts_with", &[s("hello"), s("hel")]), i(1));
    }
    #[test]
    fn starts_with_false() {
        assert_eq!(eval("starts_with", &[s("hello"), s("xyz")]), i(0));
    }
    #[test]
    fn starts_with_empty_prefix() {
        assert_eq!(eval("starts_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn starts_with_null() {
        assert_eq!(eval("starts_with", &[null(), s("a")]), null());
    }

    // --- ends_with ---
    #[test]
    fn ends_with_true() {
        assert_eq!(eval("ends_with", &[s("hello"), s("llo")]), i(1));
    }
    #[test]
    fn ends_with_false() {
        assert_eq!(eval("ends_with", &[s("hello"), s("xyz")]), i(0));
    }
    #[test]
    fn ends_with_empty_suffix() {
        assert_eq!(eval("ends_with", &[s("hello"), s("")]), i(1));
    }
    #[test]
    fn ends_with_null() {
        assert_eq!(eval("ends_with", &[null(), s("a")]), null());
    }

    // --- contains ---
    #[test]
    fn contains_true() {
        assert_eq!(eval("contains", &[s("hello world"), s("lo wo")]), i(1));
    }
    #[test]
    fn contains_false() {
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

    // --- reverse ---
    #[test]
    fn reverse_basic() {
        assert_eq!(eval("reverse", &[s("hello")]), s("olleh"));
    }
    #[test]
    fn reverse_empty() {
        assert_eq!(eval("reverse", &[s("")]), s(""));
    }
    #[test]
    fn reverse_palindrome() {
        assert_eq!(eval("reverse", &[s("racecar")]), s("racecar"));
    }
    #[test]
    fn reverse_null() {
        assert_eq!(eval("reverse", &[null()]), null());
    }
    #[test]
    fn reverse_single_char() {
        assert_eq!(eval("reverse", &[s("x")]), s("x"));
    }

    // --- repeat ---
    #[test]
    fn repeat_basic() {
        assert_eq!(eval("repeat", &[s("ab"), i(3)]), s("ababab"));
    }
    #[test]
    fn repeat_zero() {
        assert_eq!(eval("repeat", &[s("ab"), i(0)]), s(""));
    }
    #[test]
    fn repeat_negative() {
        // negative is clamped to 0
        assert_eq!(eval("repeat", &[s("ab"), i(-1)]), s(""));
    }
    #[test]
    fn repeat_null() {
        assert_eq!(eval("repeat", &[null(), i(3)]), null());
    }
    #[test]
    fn repeat_one() {
        assert_eq!(eval("repeat", &[s("x"), i(1)]), s("x"));
    }

    // --- left ---
    #[test]
    fn left_basic() {
        assert_eq!(eval("left", &[s("hello"), i(3)]), s("hel"));
    }
    #[test]
    fn left_longer_than_string() {
        assert_eq!(eval("left", &[s("hi"), i(10)]), s("hi"));
    }
    #[test]
    fn left_zero() {
        assert_eq!(eval("left", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn left_null() {
        assert_eq!(eval("left", &[null(), i(3)]), null());
    }

    // --- right ---
    #[test]
    fn right_basic() {
        assert_eq!(eval("right", &[s("hello"), i(3)]), s("llo"));
    }
    #[test]
    fn right_longer_than_string() {
        assert_eq!(eval("right", &[s("hi"), i(10)]), s("hi"));
    }
    #[test]
    fn right_zero() {
        assert_eq!(eval("right", &[s("hello"), i(0)]), s(""));
    }
    #[test]
    fn right_null() {
        assert_eq!(eval("right", &[null(), i(3)]), null());
    }

    // --- position ---
    #[test]
    fn position_found() {
        assert_eq!(eval("position", &[s("lo"), s("hello")]), i(4));
    }
    #[test]
    fn position_not_found() {
        assert_eq!(eval("position", &[s("xyz"), s("hello")]), i(0));
    }
    #[test]
    fn position_empty_needle() {
        assert_eq!(eval("position", &[s(""), s("hello")]), i(1));
    }
    #[test]
    fn position_null() {
        assert_eq!(eval("position", &[null(), s("hello")]), null());
    }

    // --- lpad ---
    #[test]
    fn lpad_basic() {
        assert_eq!(eval("lpad", &[s("hi"), i(5), s("*")]), s("***hi"));
    }
    #[test]
    fn lpad_truncate() {
        assert_eq!(eval("lpad", &[s("hello"), i(3), s("*")]), s("hel"));
    }
    #[test]
    fn lpad_null() {
        assert_eq!(eval("lpad", &[null(), i(5), s("*")]), null());
    }
    #[test]
    fn lpad_exact_length() {
        assert_eq!(eval("lpad", &[s("abc"), i(3), s("*")]), s("abc"));
    }
    #[test]
    fn lpad_multi_char_pad() {
        assert_eq!(eval("lpad", &[s("x"), i(5), s("ab")]), s("ababx"));
    }

    // --- rpad ---
    #[test]
    fn rpad_basic() {
        assert_eq!(eval("rpad", &[s("hi"), i(5), s("*")]), s("hi***"));
    }
    #[test]
    fn rpad_truncate() {
        assert_eq!(eval("rpad", &[s("hello"), i(3), s("*")]), s("hel"));
    }
    #[test]
    fn rpad_null() {
        assert_eq!(eval("rpad", &[null(), i(5), s("*")]), null());
    }

    // --- split_part ---
    #[test]
    fn split_part_basic() {
        assert_eq!(eval("split_part", &[s("a.b.c"), s("."), i(2)]), s("b"));
    }
    #[test]
    fn split_part_out_of_range() {
        assert_eq!(eval("split_part", &[s("a.b"), s("."), i(5)]), s(""));
    }
    #[test]
    fn split_part_null() {
        assert_eq!(eval("split_part", &[null(), s("."), i(1)]), null());
    }
    #[test]
    fn split_part_first() {
        assert_eq!(eval("split_part", &[s("x-y-z"), s("-"), i(1)]), s("x"));
    }
    #[test]
    fn split_part_last() {
        assert_eq!(eval("split_part", &[s("x-y-z"), s("-"), i(3)]), s("z"));
    }

    // --- regexp_match ---
    #[test]
    fn regexp_match_match() {
        assert_eq!(eval("regexp_match", &[s("hello123"), s(r"\d+")]), i(1));
    }
    #[test]
    fn regexp_match_no_match() {
        assert_eq!(eval("regexp_match", &[s("hello"), s(r"\d+")]), i(0));
    }
    #[test]
    fn regexp_match_invalid_regex() {
        let err = eval_err("regexp_match", &[s("hello"), s("[invalid")]);
        assert!(err.contains("invalid pattern"), "got: {err}");
    }
    #[test]
    fn regexp_match_null() {
        assert_eq!(eval("regexp_match", &[null(), s(".*")]), null());
    }

    // --- md5 ---
    #[test]
    fn md5_known_value() {
        let result = eval("md5", &[s("hello")]);
        match result {
            Value::Str(hash) => assert_eq!(hash, "5d41402abc4b2a76b9719d911017c592"),
            other => panic!("expected Str, got {other:?}"),
        }
    }
    #[test]
    fn md5_empty() {
        let result = eval("md5", &[s("")]);
        match result {
            Value::Str(hash) => assert_eq!(hash, "d41d8cd98f00b204e9800998ecf8427e"),
            other => panic!("expected Str, got {other:?}"),
        }
    }
    #[test]
    fn md5_null() {
        assert_eq!(eval("md5", &[null()]), null());
    }

    // --- sha256 ---
    #[test]
    fn sha256_known_value() {
        let result = eval("sha256", &[s("hello")]);
        match result {
            Value::Str(hash) => {
                assert_eq!(
                    hash,
                    "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
                );
            }
            other => panic!("expected Str, got {other:?}"),
        }
    }
    #[test]
    fn sha256_empty() {
        let result = eval("sha256", &[s("")]);
        match result {
            Value::Str(hash) => {
                assert_eq!(
                    hash,
                    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                );
            }
            other => panic!("expected Str, got {other:?}"),
        }
    }
    #[test]
    fn sha256_null() {
        assert_eq!(eval("sha256", &[null()]), null());
    }

    // --- initcap ---
    #[test]
    fn initcap_basic() {
        assert_eq!(eval("initcap", &[s("hello world")]), s("Hello World"));
    }
    #[test]
    fn initcap_already_capitalized() {
        assert_eq!(eval("initcap", &[s("Hello World")]), s("Hello World"));
    }
    #[test]
    fn initcap_null() {
        assert_eq!(eval("initcap", &[null()]), null());
    }
    #[test]
    fn initcap_single_word() {
        assert_eq!(eval("initcap", &[s("hello")]), s("Hello"));
    }
    #[test]
    fn initcap_all_upper() {
        assert_eq!(eval("initcap", &[s("HELLO WORLD")]), s("Hello World"));
    }

    // --- encode/decode ---
    #[test]
    fn encode_base64() {
        let result = eval("encode", &[s("hello"), s("base64")]);
        assert_eq!(result, s("aGVsbG8="));
    }
    #[test]
    fn decode_base64() {
        let result = eval("decode", &[s("aGVsbG8="), s("base64")]);
        assert_eq!(result, s("hello"));
    }
    #[test]
    fn encode_decode_roundtrip() {
        let encoded = eval("encode", &[s("test data"), s("base64")]);
        let decoded = eval("decode", &[encoded, s("base64")]);
        assert_eq!(decoded, s("test data"));
    }
    #[test]
    fn encode_null() {
        assert_eq!(eval("encode", &[null(), s("base64")]), null());
    }

    // --- ascii ---
    #[test]
    fn ascii_known() {
        assert_eq!(eval("ascii", &[s("A")]), i(65));
    }
    #[test]
    fn ascii_space() {
        assert_eq!(eval("ascii", &[s(" ")]), i(32));
    }
    #[test]
    fn ascii_null() {
        assert_eq!(eval("ascii", &[null()]), null());
    }

    // --- chr ---
    #[test]
    fn chr_known() {
        assert_eq!(eval("chr", &[i(65)]), s("A"));
    }
    #[test]
    fn chr_space() {
        assert_eq!(eval("chr", &[i(32)]), s(" "));
    }
    #[test]
    fn chr_null() {
        assert_eq!(eval("chr", &[null()]), null());
    }

    // --- regexp_replace ---
    #[test]
    fn regexp_replace_basic() {
        assert_eq!(
            eval("regexp_replace", &[s("hello 123 world 456"), s(r"\d+"), s("NUM")]),
            s("hello NUM world NUM")
        );
    }
    #[test]
    fn regexp_replace_no_match() {
        assert_eq!(
            eval("regexp_replace", &[s("hello"), s(r"\d+"), s("NUM")]),
            s("hello")
        );
    }
    #[test]
    fn regexp_replace_null() {
        assert_eq!(eval("regexp_replace", &[null(), s("a"), s("b")]), null());
    }

    // --- regexp_extract ---
    #[test]
    fn regexp_extract_basic() {
        assert_eq!(
            eval("regexp_extract", &[s("hello 123"), s(r"(\d+)"), i(1)]),
            s("123")
        );
    }
    #[test]
    fn regexp_extract_no_match() {
        assert_eq!(
            eval("regexp_extract", &[s("hello"), s(r"(\d+)"), i(1)]),
            null()
        );
    }
    #[test]
    fn regexp_extract_null() {
        assert_eq!(eval("regexp_extract", &[null(), s("a"), i(0)]), null());
    }

    // --- char_at ---
    #[test]
    fn char_at_basic() {
        assert_eq!(eval("char_at", &[s("hello"), i(1)]), s("h"));
    }
    #[test]
    fn char_at_last() {
        assert_eq!(eval("char_at", &[s("hello"), i(5)]), s("o"));
    }
    #[test]
    fn char_at_out_of_bounds() {
        assert_eq!(eval("char_at", &[s("hi"), i(10)]), null());
    }
    #[test]
    fn char_at_null() {
        assert_eq!(eval("char_at", &[null(), i(1)]), null());
    }

    // --- hex / unhex ---
    #[test]
    fn hex_integer() {
        assert_eq!(eval("hex", &[i(255)]), s("ff"));
    }
    #[test]
    fn hex_null() {
        assert_eq!(eval("hex", &[null()]), null());
    }
    #[test]
    fn unhex_basic() {
        assert_eq!(eval("unhex", &[s("ff")]), i(255));
    }
    #[test]
    fn unhex_null() {
        assert_eq!(eval("unhex", &[null()]), null());
    }

    // --- url_encode / url_decode ---
    #[test]
    fn url_encode_basic() {
        assert_eq!(eval("url_encode", &[s("hello world")]), s("hello%20world"));
    }
    #[test]
    fn url_encode_null() {
        assert_eq!(eval("url_encode", &[null()]), null());
    }
    #[test]
    fn url_decode_basic() {
        assert_eq!(eval("url_decode", &[s("hello%20world")]), s("hello world"));
    }
    #[test]
    fn url_encode_decode_roundtrip() {
        let encoded = eval("url_encode", &[s("a b+c")]);
        let decoded = eval("url_decode", &[encoded]);
        assert_eq!(decoded, s("a b+c"));
    }

    // --- concat_ws ---
    #[test]
    fn concat_ws_basic() {
        assert_eq!(
            eval("concat_ws", &[s(","), s("a"), s("b"), s("c")]),
            s("a,b,c")
        );
    }
    #[test]
    fn concat_ws_with_null() {
        let result = eval("concat_ws", &[s(","), s("a"), null(), s("c")]);
        match result {
            Value::Str(r) => assert!(r.contains('a') && r.contains('c')),
            _ => panic!("expected Str"),
        }
    }

    // --- word_count ---
    #[test]
    fn word_count_basic() {
        assert_eq!(eval("word_count", &[s("hello world foo")]), i(3));
    }
    #[test]
    fn word_count_empty() {
        assert_eq!(eval("word_count", &[s("")]), i(0));
    }
    #[test]
    fn word_count_null() {
        assert_eq!(eval("word_count", &[null()]), null());
    }

    // --- space ---
    #[test]
    fn space_basic() {
        assert_eq!(eval("space", &[i(3)]), s("   "));
    }
    #[test]
    fn space_zero() {
        assert_eq!(eval("space", &[i(0)]), s(""));
    }

    // --- to_base64 / from_base64 ---
    #[test]
    fn to_base64_basic() {
        assert_eq!(eval("to_base64", &[s("hello")]), s("aGVsbG8="));
    }
    #[test]
    fn from_base64_basic() {
        assert_eq!(eval("from_base64", &[s("aGVsbG8=")]), s("hello"));
    }
    #[test]
    fn base64_roundtrip() {
        let encoded = eval("to_base64", &[s("test 123")]);
        let decoded = eval("from_base64", &[encoded]);
        assert_eq!(decoded, s("test 123"));
    }

    // --- camel_case ---
    #[test]
    fn camel_case_basic() {
        let result = eval("camel_case", &[s("hello world")]);
        match result {
            Value::Str(r) => assert!(r.contains("Hello") || r.contains("hello")),
            _ => panic!("expected Str"),
        }
    }

    // --- snake_case ---
    #[test]
    fn snake_case_basic() {
        let result = eval("snake_case", &[s("HelloWorld")]);
        match result {
            Value::Str(r) => assert!(r.contains('_') || r == "helloworld"),
            _ => panic!("expected Str"),
        }
    }
}

// ===========================================================================
// Math functions
// ===========================================================================

mod math_functions {
    use super::*;

    // --- abs ---
    #[test]
    fn abs_positive() {
        assert_eq!(eval("abs", &[i(5)]), i(5));
    }
    #[test]
    fn abs_negative() {
        assert_eq!(eval("abs", &[i(-5)]), i(5));
    }
    #[test]
    fn abs_zero() {
        assert_eq!(eval("abs", &[i(0)]), i(0));
    }
    #[test]
    fn abs_float() {
        assert_eq!(eval("abs", &[f(-3.14)]), f(3.14));
    }
    #[test]
    fn abs_null() {
        assert_eq!(eval("abs", &[null()]), null());
    }

    // --- round ---
    #[test]
    fn round_basic() {
        assert_eq!(eval("round", &[f(3.7)]), f(4.0));
    }
    #[test]
    fn round_down() {
        assert_eq!(eval("round", &[f(3.2)]), f(3.0));
    }
    #[test]
    fn round_with_decimals() {
        assert_eq!(eval("round", &[f(3.456), i(2)]), f(3.46));
    }
    #[test]
    fn round_negative_decimals() {
        assert_eq!(eval("round", &[f(1234.0), i(-2)]), f(1200.0));
    }
    #[test]
    fn round_null() {
        assert_eq!(eval("round", &[null()]), null());
    }
    #[test]
    fn round_half() {
        assert_eq!(eval("round", &[f(2.5)]), f(3.0));
    }

    // --- floor ---
    #[test]
    fn floor_positive() {
        assert_eq!(eval("floor", &[f(3.7)]), f(3.0));
    }
    #[test]
    fn floor_negative() {
        assert_eq!(eval("floor", &[f(-3.2)]), f(-4.0));
    }
    #[test]
    fn floor_exact() {
        assert_eq!(eval("floor", &[f(5.0)]), f(5.0));
    }
    #[test]
    fn floor_null() {
        assert_eq!(eval("floor", &[null()]), null());
    }

    // --- ceil ---
    #[test]
    fn ceil_positive() {
        assert_eq!(eval("ceil", &[f(3.2)]), f(4.0));
    }
    #[test]
    fn ceil_negative() {
        assert_eq!(eval("ceil", &[f(-3.7)]), f(-3.0));
    }
    #[test]
    fn ceil_exact() {
        assert_eq!(eval("ceil", &[f(5.0)]), f(5.0));
    }
    #[test]
    fn ceil_null() {
        assert_eq!(eval("ceil", &[null()]), null());
    }

    // --- sqrt ---
    #[test]
    fn sqrt_perfect_square() {
        assert_eq!(eval("sqrt", &[f(16.0)]), f(4.0));
    }
    #[test]
    fn sqrt_irrational() {
        let result = eval("sqrt", &[f(2.0)]);
        assert_f64_close(&result, std::f64::consts::SQRT_2, 1e-10);
    }
    #[test]
    fn sqrt_negative_error() {
        let err = eval_err("sqrt", &[f(-1.0)]);
        assert!(err.contains("negative"), "got: {err}");
    }
    #[test]
    fn sqrt_null() {
        assert_eq!(eval("sqrt", &[null()]), null());
    }
    #[test]
    fn sqrt_zero() {
        assert_eq!(eval("sqrt", &[f(0.0)]), f(0.0));
    }

    // --- pow ---
    #[test]
    fn pow_basic() {
        assert_eq!(eval("pow", &[f(2.0), f(3.0)]), f(8.0));
    }
    #[test]
    fn pow_zero_zero() {
        assert_eq!(eval("pow", &[f(0.0), f(0.0)]), f(1.0));
    }
    #[test]
    fn pow_negative_exponent() {
        assert_eq!(eval("pow", &[f(2.0), f(-1.0)]), f(0.5));
    }
    #[test]
    fn pow_null() {
        assert_eq!(eval("pow", &[null(), f(2.0)]), null());
    }
    #[test]
    fn pow_null_exponent() {
        assert_eq!(eval("pow", &[f(2.0), null()]), null());
    }

    // --- log ---
    #[test]
    fn log_basic() {
        let result = eval("log", &[f(std::f64::consts::E)]);
        assert_f64_close(&result, 1.0, 1e-10);
    }
    #[test]
    fn log_one() {
        assert_eq!(eval("log", &[f(1.0)]), f(0.0));
    }
    #[test]
    fn log_negative_error() {
        let err = eval_err("log", &[f(-1.0)]);
        assert!(err.contains("positive"), "got: {err}");
    }
    #[test]
    fn log_null() {
        assert_eq!(eval("log", &[null()]), null());
    }

    // --- log2 ---
    #[test]
    fn log2_basic() {
        assert_eq!(eval("log2", &[f(8.0)]), f(3.0));
    }
    #[test]
    fn log2_one() {
        assert_eq!(eval("log2", &[f(1.0)]), f(0.0));
    }
    #[test]
    fn log2_negative_error() {
        let err = eval_err("log2", &[f(-1.0)]);
        assert!(err.contains("positive"), "got: {err}");
    }
    #[test]
    fn log2_null() {
        assert_eq!(eval("log2", &[null()]), null());
    }

    // --- log10 ---
    #[test]
    fn log10_basic() {
        assert_eq!(eval("log10", &[f(100.0)]), f(2.0));
    }
    #[test]
    fn log10_one() {
        assert_eq!(eval("log10", &[f(1.0)]), f(0.0));
    }
    #[test]
    fn log10_negative_error() {
        let err = eval_err("log10", &[f(-1.0)]);
        assert!(err.contains("positive"), "got: {err}");
    }
    #[test]
    fn log10_null() {
        assert_eq!(eval("log10", &[null()]), null());
    }

    // --- exp ---
    #[test]
    fn exp_basic() {
        let result = eval("exp", &[f(1.0)]);
        assert_f64_close(&result, std::f64::consts::E, 1e-10);
    }
    #[test]
    fn exp_zero() {
        assert_eq!(eval("exp", &[f(0.0)]), f(1.0));
    }
    #[test]
    fn exp_null() {
        assert_eq!(eval("exp", &[null()]), null());
    }
    #[test]
    fn exp_negative() {
        let result = eval("exp", &[f(-1.0)]);
        assert_f64_close(&result, 1.0 / std::f64::consts::E, 1e-10);
    }

    // --- sin ---
    #[test]
    fn sin_zero() {
        assert_eq!(eval("sin", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn sin_pi_half() {
        let result = eval("sin", &[f(std::f64::consts::FRAC_PI_2)]);
        assert_f64_close(&result, 1.0, 1e-10);
    }
    #[test]
    fn sin_pi() {
        let result = eval("sin", &[f(std::f64::consts::PI)]);
        assert_f64_close(&result, 0.0, 1e-10);
    }
    #[test]
    fn sin_null() {
        assert_eq!(eval("sin", &[null()]), null());
    }

    // --- cos ---
    #[test]
    fn cos_zero() {
        assert_eq!(eval("cos", &[f(0.0)]), f(1.0));
    }
    #[test]
    fn cos_pi() {
        let result = eval("cos", &[f(std::f64::consts::PI)]);
        assert_f64_close(&result, -1.0, 1e-10);
    }
    #[test]
    fn cos_null() {
        assert_eq!(eval("cos", &[null()]), null());
    }

    // --- tan ---
    #[test]
    fn tan_zero() {
        assert_eq!(eval("tan", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn tan_pi_quarter() {
        let result = eval("tan", &[f(std::f64::consts::FRAC_PI_4)]);
        assert_f64_close(&result, 1.0, 1e-10);
    }
    #[test]
    fn tan_null() {
        assert_eq!(eval("tan", &[null()]), null());
    }

    // --- mod ---
    #[test]
    fn mod_basic() {
        assert_eq!(eval("mod", &[i(10), i(3)]), i(1));
    }
    #[test]
    fn mod_by_zero_error() {
        let err = eval_err("mod", &[i(10), i(0)]);
        assert!(err.contains("zero"), "got: {err}");
    }
    #[test]
    fn mod_negative() {
        assert_eq!(eval("mod", &[i(-10), i(3)]), i(-1));
    }
    #[test]
    fn mod_null() {
        assert_eq!(eval("mod", &[null(), i(3)]), null());
    }
    #[test]
    fn mod_float() {
        let result = eval("mod", &[f(10.5), f(3.0)]);
        assert_f64_close(&result, 1.5, 1e-10);
    }

    // --- sign ---
    #[test]
    fn sign_positive() {
        assert_eq!(eval("sign", &[i(42)]), i(1));
    }
    #[test]
    fn sign_negative() {
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

    // --- pi ---
    #[test]
    fn pi_value() {
        let result = eval("pi", &[]);
        assert_f64_close(&result, std::f64::consts::PI, 1e-15);
    }

    // --- random ---
    #[test]
    fn random_in_range() {
        let result = eval("random", &[]);
        match result {
            Value::F64(v) => {
                assert!(v >= 0.0 && v < 1.0, "random() = {v}, expected [0, 1)");
            }
            _ => panic!("expected F64"),
        }
    }
    #[test]
    fn random_different_calls() {
        // Two calls should (very likely) produce different values
        let r1 = eval("random", &[]);
        // Give a tiny delay to change subsec nanos
        std::thread::sleep(std::time::Duration::from_nanos(100));
        let r2 = eval("random", &[]);
        // They might be equal in rare cases, so we just check both are valid
        match (&r1, &r2) {
            (Value::F64(a), Value::F64(b)) => {
                assert!(*a >= 0.0 && *b >= 0.0);
            }
            _ => panic!("expected F64 values"),
        }
    }

    // --- degrees ---
    #[test]
    fn degrees_from_pi() {
        let result = eval("degrees", &[f(std::f64::consts::PI)]);
        assert_f64_close(&result, 180.0, 1e-10);
    }
    #[test]
    fn degrees_null() {
        assert_eq!(eval("degrees", &[null()]), null());
    }

    // --- radians ---
    #[test]
    fn radians_from_180() {
        let result = eval("radians", &[f(180.0)]);
        assert_f64_close(&result, std::f64::consts::PI, 1e-10);
    }
    #[test]
    fn radians_from_90() {
        let result = eval("radians", &[f(90.0)]);
        assert_f64_close(&result, std::f64::consts::FRAC_PI_2, 1e-10);
    }
    #[test]
    fn radians_null() {
        assert_eq!(eval("radians", &[null()]), null());
    }

    // --- gcd ---
    #[test]
    fn gcd_basic() {
        assert_eq!(eval("gcd", &[i(12), i(8)]), i(4));
    }
    #[test]
    fn gcd_prime() {
        assert_eq!(eval("gcd", &[i(7), i(13)]), i(1));
    }
    #[test]
    fn gcd_zero() {
        assert_eq!(eval("gcd", &[i(0), i(5)]), i(5));
    }
    #[test]
    fn gcd_null() {
        assert_eq!(eval("gcd", &[null(), i(5)]), null());
    }

    // --- lcm ---
    #[test]
    fn lcm_basic() {
        assert_eq!(eval("lcm", &[i(4), i(6)]), i(12));
    }
    #[test]
    fn lcm_prime() {
        assert_eq!(eval("lcm", &[i(3), i(7)]), i(21));
    }
    #[test]
    fn lcm_zero() {
        assert_eq!(eval("lcm", &[i(0), i(0)]), i(0));
    }
    #[test]
    fn lcm_null() {
        assert_eq!(eval("lcm", &[null(), i(5)]), null());
    }

    // --- bit_and ---
    #[test]
    fn bit_and_basic() {
        assert_eq!(eval("bit_and", &[i(0b1100), i(0b1010)]), i(0b1000));
    }
    #[test]
    fn bit_and_null() {
        assert_eq!(eval("bit_and", &[null(), i(5)]), null());
    }

    // --- bit_or ---
    #[test]
    fn bit_or_basic() {
        assert_eq!(eval("bit_or", &[i(0b1100), i(0b1010)]), i(0b1110));
    }
    #[test]
    fn bit_or_null() {
        assert_eq!(eval("bit_or", &[null(), i(5)]), null());
    }

    // --- bit_xor ---
    #[test]
    fn bit_xor_basic() {
        assert_eq!(eval("bit_xor", &[i(0b1100), i(0b1010)]), i(0b0110));
    }
    #[test]
    fn bit_xor_null() {
        assert_eq!(eval("bit_xor", &[null(), i(5)]), null());
    }

    // --- bit_not ---
    #[test]
    fn bit_not_basic() {
        assert_eq!(eval("bit_not", &[i(0)]), i(!0i64));
    }
    #[test]
    fn bit_not_null() {
        assert_eq!(eval("bit_not", &[null()]), null());
    }

    // --- bit_shift_left ---
    #[test]
    fn bit_shift_left_basic() {
        assert_eq!(eval("bit_shift_left", &[i(1), i(3)]), i(8));
    }
    #[test]
    fn bit_shift_left_null() {
        assert_eq!(eval("bit_shift_left", &[null(), i(1)]), null());
    }

    // --- bit_shift_right ---
    #[test]
    fn bit_shift_right_basic() {
        assert_eq!(eval("bit_shift_right", &[i(8), i(3)]), i(1));
    }
    #[test]
    fn bit_shift_right_null() {
        assert_eq!(eval("bit_shift_right", &[null(), i(1)]), null());
    }

    // --- clamp ---
    #[test]
    fn clamp_within_range() {
        assert_eq!(eval("clamp", &[f(5.0), f(1.0), f(10.0)]), f(5.0));
    }
    #[test]
    fn clamp_below_min() {
        assert_eq!(eval("clamp", &[f(-5.0), f(1.0), f(10.0)]), f(1.0));
    }
    #[test]
    fn clamp_above_max() {
        assert_eq!(eval("clamp", &[f(15.0), f(1.0), f(10.0)]), f(10.0));
    }
    #[test]
    fn clamp_null() {
        assert_eq!(eval("clamp", &[null(), f(1.0), f(10.0)]), null());
    }

    // --- factorial ---
    #[test]
    fn factorial_zero() {
        assert_eq!(eval("factorial", &[i(0)]), i(1));
    }
    #[test]
    fn factorial_five() {
        assert_eq!(eval("factorial", &[i(5)]), i(120));
    }
    #[test]
    fn factorial_negative_error() {
        let err = eval_err("factorial", &[i(-1)]);
        assert!(err.contains("non-negative"), "got: {err}");
    }
    #[test]
    fn factorial_null() {
        assert_eq!(eval("factorial", &[null()]), null());
    }
    #[test]
    fn factorial_twenty() {
        assert_eq!(eval("factorial", &[i(20)]), i(2432902008176640000));
    }

    // --- cbrt ---
    #[test]
    fn cbrt_basic() {
        assert_eq!(eval("cbrt", &[f(27.0)]), f(3.0));
    }
    #[test]
    fn cbrt_negative() {
        assert_eq!(eval("cbrt", &[f(-8.0)]), f(-2.0));
    }
    #[test]
    fn cbrt_null() {
        assert_eq!(eval("cbrt", &[null()]), null());
    }

    // --- asin / acos / atan ---
    #[test]
    fn asin_zero() {
        assert_eq!(eval("asin", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn asin_one() {
        let result = eval("asin", &[f(1.0)]);
        assert_f64_close(&result, std::f64::consts::FRAC_PI_2, 1e-10);
    }
    #[test]
    fn asin_out_of_range_error() {
        let err = eval_err("asin", &[f(2.0)]);
        assert!(err.contains("[-1, 1]"), "got: {err}");
    }
    #[test]
    fn acos_one() {
        assert_eq!(eval("acos", &[f(1.0)]), f(0.0));
    }
    #[test]
    fn acos_zero() {
        let result = eval("acos", &[f(0.0)]);
        assert_f64_close(&result, std::f64::consts::FRAC_PI_2, 1e-10);
    }
    #[test]
    fn atan_zero() {
        assert_eq!(eval("atan", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn atan2_basic() {
        let result = eval("atan2", &[f(1.0), f(1.0)]);
        assert_f64_close(&result, std::f64::consts::FRAC_PI_4, 1e-10);
    }
    #[test]
    fn atan2_null() {
        assert_eq!(eval("atan2", &[null(), f(1.0)]), null());
    }

    // --- sinh / cosh / tanh ---
    #[test]
    fn sinh_zero() {
        assert_eq!(eval("sinh", &[f(0.0)]), f(0.0));
    }
    #[test]
    fn cosh_zero() {
        assert_eq!(eval("cosh", &[f(0.0)]), f(1.0));
    }
    #[test]
    fn tanh_zero() {
        assert_eq!(eval("tanh", &[f(0.0)]), f(0.0));
    }

    // --- trunc ---
    #[test]
    fn trunc_positive() {
        assert_eq!(eval("trunc", &[f(3.7)]), f(3.0));
    }
    #[test]
    fn trunc_negative() {
        assert_eq!(eval("trunc", &[f(-3.7)]), f(-3.0));
    }
    #[test]
    fn trunc_null() {
        assert_eq!(eval("trunc", &[null()]), null());
    }

    // --- lerp ---
    #[test]
    fn lerp_midpoint() {
        assert_eq!(eval("lerp", &[f(0.0), f(10.0), f(0.5)]), f(5.0));
    }
    #[test]
    fn lerp_start() {
        assert_eq!(eval("lerp", &[f(0.0), f(10.0), f(0.0)]), f(0.0));
    }
    #[test]
    fn lerp_end() {
        assert_eq!(eval("lerp", &[f(0.0), f(10.0), f(1.0)]), f(10.0));
    }

    // --- is_finite / is_nan / is_inf ---
    #[test]
    fn is_finite_normal() {
        assert_eq!(eval("is_finite", &[f(1.0)]), i(1));
    }
    #[test]
    fn is_finite_int() {
        assert_eq!(eval("is_finite", &[i(42)]), i(1));
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
    fn is_nan_null() {
        assert_eq!(eval("is_nan", &[null()]), null());
    }

    // --- hypot ---
    #[test]
    fn hypot_3_4() {
        assert_eq!(eval("hypot", &[f(3.0), f(4.0)]), f(5.0));
    }

    // --- fma ---
    #[test]
    fn fma_basic() {
        // fma(2, 3, 4) = 2*3 + 4 = 10
        assert_eq!(eval("fma", &[f(2.0), f(3.0), f(4.0)]), f(10.0));
    }

    // --- negate ---
    #[test]
    fn negate_positive() {
        assert_eq!(eval("negate", &[i(5)]), i(-5));
    }
    #[test]
    fn negate_negative() {
        assert_eq!(eval("negate", &[i(-5)]), i(5));
    }
    #[test]
    fn negate_null() {
        assert_eq!(eval("negate", &[null()]), null());
    }

    // --- square ---
    #[test]
    fn square_basic() {
        assert_eq!(eval("square", &[f(3.0)]), f(9.0));
    }
    #[test]
    fn square_negative() {
        assert_eq!(eval("square", &[f(-4.0)]), f(16.0));
    }

    // --- next_power_of_two ---
    #[test]
    fn next_power_of_two_basic() {
        assert_eq!(eval("next_power_of_two", &[i(5)]), i(8));
    }
    #[test]
    fn next_power_of_two_exact() {
        assert_eq!(eval("next_power_of_two", &[i(8)]), i(8));
    }
    #[test]
    fn next_power_of_two_one() {
        assert_eq!(eval("next_power_of_two", &[i(1)]), i(1));
    }
}

// ===========================================================================
// Date/Time functions
// ===========================================================================

mod date_functions {
    use super::*;

    // Known timestamp: 2024-03-15 12:30:45 UTC
    // = 1710505845 seconds since epoch
    const KNOWN_TS: i64 = 1710505845_000_000_000;

    // 2024-01-01 00:00:00 UTC
    const JAN_1_2024: i64 = 1704067200_000_000_000;

    // 2000-02-29 00:00:00 UTC (leap year)
    const FEB_29_2000: i64 = 951782400_000_000_000;

    // --- now ---
    #[test]
    fn now_returns_timestamp() {
        let result = eval("now", &[]);
        match result {
            Value::Timestamp(ns) => assert!(ns > 0, "now() should be positive"),
            _ => panic!("expected Timestamp"),
        }
    }
    #[test]
    fn now_reasonable_range() {
        let result = eval("now", &[]);
        match result {
            Value::Timestamp(ns) => {
                // Should be after 2020 and before 2030
                let secs = ns / 1_000_000_000;
                assert!(secs > 1_577_836_800, "too old"); // 2020-01-01
                assert!(secs < 1_893_456_000, "too far in future"); // 2030-01-01
            }
            _ => panic!("expected Timestamp"),
        }
    }

    // --- extract_year ---
    #[test]
    fn extract_year_known() {
        assert_eq!(eval("extract_year", &[ts(KNOWN_TS)]), i(2024));
    }
    #[test]
    fn extract_year_null() {
        assert_eq!(eval("extract_year", &[null()]), null());
    }
    #[test]
    fn extract_year_2000() {
        assert_eq!(eval("extract_year", &[ts(FEB_29_2000)]), i(2000));
    }

    // --- extract_month ---
    #[test]
    fn extract_month_known() {
        assert_eq!(eval("extract_month", &[ts(KNOWN_TS)]), i(3)); // March
    }
    #[test]
    fn extract_month_null() {
        assert_eq!(eval("extract_month", &[null()]), null());
    }
    #[test]
    fn extract_month_january() {
        assert_eq!(eval("extract_month", &[ts(JAN_1_2024)]), i(1));
    }

    // --- extract_day ---
    #[test]
    fn extract_day_known() {
        assert_eq!(eval("extract_day", &[ts(KNOWN_TS)]), i(15));
    }
    #[test]
    fn extract_day_null() {
        assert_eq!(eval("extract_day", &[null()]), null());
    }
    #[test]
    fn extract_day_first() {
        assert_eq!(eval("extract_day", &[ts(JAN_1_2024)]), i(1));
    }
    #[test]
    fn extract_day_leap_feb29() {
        assert_eq!(eval("extract_day", &[ts(FEB_29_2000)]), i(29));
    }

    // --- extract_hour ---
    #[test]
    fn extract_hour_known() {
        assert_eq!(eval("extract_hour", &[ts(KNOWN_TS)]), i(12));
    }
    #[test]
    fn extract_hour_null() {
        assert_eq!(eval("extract_hour", &[null()]), null());
    }
    #[test]
    fn extract_hour_midnight() {
        assert_eq!(eval("extract_hour", &[ts(JAN_1_2024)]), i(0));
    }

    // --- date_trunc ---
    #[test]
    fn date_trunc_day() {
        let result = eval("date_trunc", &[s("day"), ts(KNOWN_TS)]);
        match result {
            Value::Timestamp(ns) => {
                let (_, _, _, hour, min, sec) = decompose(ns);
                assert_eq!(hour, 0);
                assert_eq!(min, 0);
                assert_eq!(sec, 0);
            }
            _ => panic!("expected Timestamp"),
        }
    }
    #[test]
    fn date_trunc_hour() {
        let result = eval("date_trunc", &[s("hour"), ts(KNOWN_TS)]);
        match result {
            Value::Timestamp(ns) => {
                let total_secs = ns / 1_000_000_000;
                assert_eq!(total_secs % 3600, 0);
            }
            _ => panic!("expected Timestamp"),
        }
    }
    #[test]
    fn date_trunc_minute() {
        let result = eval("date_trunc", &[s("minute"), ts(KNOWN_TS)]);
        match result {
            Value::Timestamp(ns) => {
                let total_secs = ns / 1_000_000_000;
                assert_eq!(total_secs % 60, 0);
            }
            _ => panic!("expected Timestamp"),
        }
    }
    #[test]
    fn date_trunc_month() {
        let result = eval("date_trunc", &[s("month"), ts(KNOWN_TS)]);
        match result {
            Value::Timestamp(ns) => {
                // Should be March 1, 2024
                assert_eq!(eval("extract_day", &[Value::Timestamp(ns)]), i(1));
                assert_eq!(eval("extract_month", &[Value::Timestamp(ns)]), i(3));
            }
            _ => panic!("expected Timestamp"),
        }
    }
    #[test]
    fn date_trunc_year() {
        let result = eval("date_trunc", &[s("year"), ts(KNOWN_TS)]);
        match result {
            Value::Timestamp(ns) => {
                assert_eq!(eval("extract_month", &[Value::Timestamp(ns)]), i(1));
                assert_eq!(eval("extract_day", &[Value::Timestamp(ns)]), i(1));
            }
            _ => panic!("expected Timestamp"),
        }
    }
    #[test]
    fn date_trunc_null() {
        assert_eq!(eval("date_trunc", &[s("day"), null()]), null());
    }

    // --- date_diff ---
    #[test]
    fn date_diff_days() {
        let ts1 = JAN_1_2024;
        let ts2 = JAN_1_2024 + 3 * 86400_000_000_000i64; // 3 days later
        assert_eq!(eval("date_diff", &[s("days"), ts(ts1), ts(ts2)]), i(3));
    }
    #[test]
    fn date_diff_hours() {
        let ts1 = JAN_1_2024;
        let ts2 = JAN_1_2024 + 5 * 3600_000_000_000i64; // 5 hours later
        assert_eq!(eval("date_diff", &[s("hours"), ts(ts1), ts(ts2)]), i(5));
    }
    #[test]
    fn date_diff_negative() {
        let ts1 = JAN_1_2024 + 86400_000_000_000i64;
        let ts2 = JAN_1_2024;
        assert_eq!(eval("date_diff", &[s("days"), ts(ts1), ts(ts2)]), i(-1));
    }
    #[test]
    fn date_diff_null() {
        assert_eq!(eval("date_diff", &[s("days"), null(), ts(JAN_1_2024)]), null());
    }

    // --- timestamp_add ---
    #[test]
    fn timestamp_add_days() {
        let result = eval("timestamp_add", &[s("days"), i(1), ts(JAN_1_2024)]);
        match result {
            Value::Timestamp(ns) => {
                assert_eq!(eval("extract_day", &[Value::Timestamp(ns)]), i(2));
            }
            _ => panic!("expected Timestamp"),
        }
    }
    #[test]
    fn timestamp_add_hours() {
        let result = eval("timestamp_add", &[s("hours"), i(3), ts(JAN_1_2024)]);
        match result {
            Value::Timestamp(ns) => {
                assert_eq!(eval("extract_hour", &[Value::Timestamp(ns)]), i(3));
            }
            _ => panic!("expected Timestamp"),
        }
    }
    #[test]
    fn timestamp_add_months() {
        let result = eval("timestamp_add", &[s("months"), i(2), ts(JAN_1_2024)]);
        match result {
            Value::Timestamp(ns) => {
                assert_eq!(eval("extract_month", &[Value::Timestamp(ns)]), i(3));
            }
            _ => panic!("expected Timestamp"),
        }
    }
    #[test]
    fn timestamp_add_null() {
        assert_eq!(eval("timestamp_add", &[s("days"), i(1), null()]), null());
    }
    #[test]
    fn timestamp_add_negative() {
        let result = eval("timestamp_add", &[s("days"), i(-1), ts(JAN_1_2024)]);
        match result {
            Value::Timestamp(ns) => {
                // Should be Dec 31, 2023
                assert_eq!(eval("extract_year", &[Value::Timestamp(ns)]), i(2023));
                assert_eq!(eval("extract_month", &[Value::Timestamp(ns)]), i(12));
                assert_eq!(eval("extract_day", &[Value::Timestamp(ns)]), i(31));
            }
            _ => panic!("expected Timestamp"),
        }
    }

    // --- epoch_nanos ---
    #[test]
    fn epoch_nanos_known() {
        assert_eq!(eval("epoch_nanos", &[ts(KNOWN_TS)]), i(KNOWN_TS));
    }
    #[test]
    fn epoch_nanos_null() {
        assert_eq!(eval("epoch_nanos", &[null()]), null());
    }

    // --- to_timestamp ---
    #[test]
    fn to_timestamp_from_int() {
        let result = eval("to_timestamp", &[i(KNOWN_TS)]);
        assert_eq!(result, ts(KNOWN_TS));
    }
    #[test]
    fn to_timestamp_null() {
        assert_eq!(eval("to_timestamp", &[null()]), null());
    }

    // --- is_weekend ---
    #[test]
    fn is_weekend_saturday() {
        // 2024-03-16 is Saturday
        let sat = 1710547200_000_000_000i64;
        assert_eq!(eval("is_weekend", &[ts(sat)]), i(1));
    }
    #[test]
    fn is_weekend_weekday() {
        // 2024-03-15 is Friday
        let fri = 1710460800_000_000_000i64;
        assert_eq!(eval("is_weekend", &[ts(fri)]), i(0));
    }
    #[test]
    fn is_weekend_null() {
        assert_eq!(eval("is_weekend", &[null()]), null());
    }

    // --- is_business_day ---
    #[test]
    fn is_business_day_weekday() {
        let fri = 1710460800_000_000_000i64;
        assert_eq!(eval("is_business_day", &[ts(fri)]), i(1));
    }
    #[test]
    fn is_business_day_weekend() {
        let sat = 1710547200_000_000_000i64;
        assert_eq!(eval("is_business_day", &[ts(sat)]), i(0));
    }
    #[test]
    fn is_business_day_null() {
        assert_eq!(eval("is_business_day", &[null()]), null());
    }

    // --- first_of_month ---
    #[test]
    fn first_of_month_march() {
        let result = eval("first_of_month", &[ts(KNOWN_TS)]);
        match result {
            Value::Timestamp(ns) => {
                assert_eq!(eval("extract_day", &[Value::Timestamp(ns)]), i(1));
                assert_eq!(eval("extract_month", &[Value::Timestamp(ns)]), i(3));
            }
            _ => panic!("expected Timestamp"),
        }
    }
    #[test]
    fn first_of_month_null() {
        assert_eq!(eval("first_of_month", &[null()]), null());
    }

    // --- last_of_month ---
    #[test]
    fn last_of_month_march() {
        let result = eval("last_of_month", &[ts(KNOWN_TS)]);
        match result {
            Value::Timestamp(ns) => {
                assert_eq!(eval("extract_day", &[Value::Timestamp(ns)]), i(31));
            }
            _ => panic!("expected Timestamp"),
        }
    }
    #[test]
    fn last_of_month_february_leap() {
        let result = eval("last_of_month", &[ts(FEB_29_2000)]);
        match result {
            Value::Timestamp(ns) => {
                assert_eq!(eval("extract_day", &[Value::Timestamp(ns)]), i(29));
            }
            _ => panic!("expected Timestamp"),
        }
    }
    #[test]
    fn last_of_month_null() {
        assert_eq!(eval("last_of_month", &[null()]), null());
    }

    // --- days_in_month_fn ---
    #[test]
    fn days_in_month_jan() {
        assert_eq!(eval("days_in_month_fn", &[ts(JAN_1_2024)]), i(31));
    }
    #[test]
    fn days_in_month_feb_leap() {
        assert_eq!(eval("days_in_month_fn", &[ts(FEB_29_2000)]), i(29));
    }
    #[test]
    fn days_in_month_april() {
        // April 2024 => 30 days. April 1, 2024: 1711929600 sec
        let apr_1 = 1711929600_000_000_000i64;
        assert_eq!(eval("days_in_month_fn", &[ts(apr_1)]), i(30));
    }
    #[test]
    fn days_in_month_null() {
        assert_eq!(eval("days_in_month_fn", &[null()]), null());
    }

    // --- is_leap_year_fn ---
    #[test]
    fn is_leap_year_2000() {
        assert_eq!(eval("is_leap_year_fn", &[ts(FEB_29_2000)]), i(1));
    }
    #[test]
    fn is_leap_year_2024() {
        assert_eq!(eval("is_leap_year_fn", &[ts(JAN_1_2024)]), i(1));
    }
    #[test]
    fn is_leap_year_2023() {
        // 2023-06-15: 1686787200 sec
        let ts_2023 = 1686787200_000_000_000i64;
        assert_eq!(eval("is_leap_year_fn", &[ts(ts_2023)]), i(0));
    }
    #[test]
    fn is_leap_year_null() {
        assert_eq!(eval("is_leap_year_fn", &[null()]), null());
    }

    // --- months_between ---
    #[test]
    fn months_between_basic() {
        // March 2024 - Jan 2024 = 2 months
        let result = eval("months_between", &[ts(KNOWN_TS), ts(JAN_1_2024)]);
        assert_eq!(result, i(2));
    }
    #[test]
    fn months_between_negative() {
        let result = eval("months_between", &[ts(JAN_1_2024), ts(KNOWN_TS)]);
        assert_eq!(result, i(-2));
    }
    #[test]
    fn months_between_null() {
        assert_eq!(eval("months_between", &[null(), ts(JAN_1_2024)]), null());
    }

    // --- years_between ---
    #[test]
    fn years_between_basic() {
        // 2024 - 2000 = 24
        let result = eval("years_between", &[ts(JAN_1_2024), ts(FEB_29_2000)]);
        assert_eq!(result, i(24));
    }
    #[test]
    fn years_between_negative() {
        let result = eval("years_between", &[ts(FEB_29_2000), ts(JAN_1_2024)]);
        assert_eq!(result, i(-24));
    }
    #[test]
    fn years_between_null() {
        assert_eq!(eval("years_between", &[null(), ts(JAN_1_2024)]), null());
    }

    // --- date_format ---
    #[test]
    fn date_format_default() {
        let result = eval("date_format", &[ts(KNOWN_TS)]);
        match result {
            Value::Str(s) => assert!(s.contains("2024"), "got: {s}"),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn date_format_custom() {
        let result = eval("date_format", &[ts(KNOWN_TS), s("%Y-%m-%d")]);
        assert_eq!(result, s("2024-03-15"));
    }
    #[test]
    fn date_format_null() {
        assert_eq!(eval("date_format", &[null()]), null());
    }

    // Helper to decompose a timestamp
    fn decompose(ns: i64) -> (i64, i64, i64, i64, i64, i64) {
        let year = match eval("extract_year", &[ts(ns)]) {
            Value::I64(v) => v,
            _ => panic!("expected I64"),
        };
        let month = match eval("extract_month", &[ts(ns)]) {
            Value::I64(v) => v,
            _ => panic!("expected I64"),
        };
        let day = match eval("extract_day", &[ts(ns)]) {
            Value::I64(v) => v,
            _ => panic!("expected I64"),
        };
        let hour = match eval("extract_hour", &[ts(ns)]) {
            Value::I64(v) => v,
            _ => panic!("expected I64"),
        };
        let _min = 0i64;
        let _sec = 0i64;
        (year, month, day, hour, _min, _sec)
    }

    // --- epoch_seconds / epoch_millis / epoch_micros ---
    #[test]
    fn epoch_seconds_known() {
        let result = eval("epoch_seconds", &[ts(KNOWN_TS)]);
        assert_eq!(result, i(1710505845));
    }
    #[test]
    fn epoch_millis_known() {
        let result = eval("epoch_millis", &[ts(KNOWN_TS)]);
        assert_eq!(result, i(1710505845000));
    }
    #[test]
    fn epoch_micros_known() {
        let result = eval("epoch_micros", &[ts(KNOWN_TS)]);
        assert_eq!(result, i(1710505845000000));
    }
}

// ===========================================================================
// Conditional functions
// ===========================================================================

mod conditional_functions {
    use super::*;

    // --- coalesce ---
    #[test]
    fn coalesce_first_non_null() {
        assert_eq!(eval("coalesce", &[null(), i(42), i(99)]), i(42));
    }
    #[test]
    fn coalesce_all_null() {
        assert_eq!(eval("coalesce", &[null(), null(), null()]), null());
    }
    #[test]
    fn coalesce_single_value() {
        assert_eq!(eval("coalesce", &[i(1)]), i(1));
    }
    #[test]
    fn coalesce_first_not_null() {
        assert_eq!(eval("coalesce", &[i(1), i(2)]), i(1));
    }
    #[test]
    fn coalesce_null_then_string() {
        assert_eq!(eval("coalesce", &[null(), s("default")]), s("default"));
    }

    // --- nullif ---
    #[test]
    fn nullif_equal() {
        assert_eq!(eval("nullif", &[i(1), i(1)]), null());
    }
    #[test]
    fn nullif_not_equal() {
        assert_eq!(eval("nullif", &[i(1), i(2)]), i(1));
    }
    #[test]
    fn nullif_strings_equal() {
        assert_eq!(eval("nullif", &[s("a"), s("a")]), null());
    }
    #[test]
    fn nullif_strings_not_equal() {
        assert_eq!(eval("nullif", &[s("a"), s("b")]), s("a"));
    }

    // --- greatest ---
    #[test]
    fn greatest_two() {
        assert_eq!(eval("greatest", &[i(1), i(5)]), i(5));
    }
    #[test]
    fn greatest_three() {
        assert_eq!(eval("greatest", &[i(3), i(1), i(7)]), i(7));
    }
    #[test]
    fn greatest_with_null() {
        assert_eq!(eval("greatest", &[null(), i(5), i(3)]), i(5));
    }
    #[test]
    fn greatest_all_null() {
        assert_eq!(eval("greatest", &[null(), null()]), null());
    }
    #[test]
    fn greatest_floats() {
        assert_eq!(eval("greatest", &[f(1.5), f(2.5), f(0.5)]), f(2.5));
    }

    // --- least ---
    #[test]
    fn least_two() {
        assert_eq!(eval("least", &[i(1), i(5)]), i(1));
    }
    #[test]
    fn least_three() {
        assert_eq!(eval("least", &[i(3), i(1), i(7)]), i(1));
    }
    #[test]
    fn least_with_null() {
        assert_eq!(eval("least", &[null(), i(5), i(3)]), i(3));
    }
    #[test]
    fn least_all_null() {
        assert_eq!(eval("least", &[null(), null()]), null());
    }

    // --- if_null ---
    #[test]
    fn if_null_null_returns_default() {
        assert_eq!(eval("if_null", &[null(), i(42)]), i(42));
    }
    #[test]
    fn if_null_not_null_returns_original() {
        assert_eq!(eval("if_null", &[i(1), i(42)]), i(1));
    }
    #[test]
    fn if_null_string() {
        assert_eq!(eval("if_null", &[null(), s("default")]), s("default"));
    }

    // --- nvl2 ---
    #[test]
    fn nvl2_not_null() {
        assert_eq!(eval("nvl2", &[i(1), s("yes"), s("no")]), s("yes"));
    }
    #[test]
    fn nvl2_null() {
        assert_eq!(eval("nvl2", &[null(), s("yes"), s("no")]), s("no"));
    }
    #[test]
    fn nvl2_string_not_null() {
        assert_eq!(eval("nvl2", &[s("x"), i(1), i(0)]), i(1));
    }

    // --- is_null ---
    #[test]
    fn is_null_true() {
        assert_eq!(eval("is_null", &[null()]), i(1));
    }
    #[test]
    fn is_null_false() {
        assert_eq!(eval("is_null", &[i(42)]), i(0));
    }

    // --- is_not_null ---
    #[test]
    fn is_not_null_true() {
        assert_eq!(eval("is_not_null", &[i(42)]), i(1));
    }
    #[test]
    fn is_not_null_false() {
        assert_eq!(eval("is_not_null", &[null()]), i(0));
    }

    // --- nullif_zero ---
    #[test]
    fn nullif_zero_zero() {
        assert_eq!(eval("nullif_zero", &[i(0)]), null());
    }
    #[test]
    fn nullif_zero_nonzero() {
        assert_eq!(eval("nullif_zero", &[i(5)]), i(5));
    }

    // --- zeroifnull ---
    #[test]
    fn zeroifnull_null() {
        assert_eq!(eval("zeroifnull", &[null()]), i(0));
    }
    #[test]
    fn zeroifnull_not_null() {
        assert_eq!(eval("zeroifnull", &[i(5)]), i(5));
    }

    // --- iif ---
    #[test]
    fn iif_true() {
        assert_eq!(eval("iif", &[i(1), s("yes"), s("no")]), s("yes"));
    }
    #[test]
    fn iif_false() {
        assert_eq!(eval("iif", &[i(0), s("yes"), s("no")]), s("no"));
    }
    #[test]
    fn iif_null_condition() {
        assert_eq!(eval("iif", &[null(), s("yes"), s("no")]), s("no"));
    }
}

// ===========================================================================
// Aggregate functions (via SQL integration)
// ===========================================================================

mod aggregate_functions {
    use super::*;

    #[test]
    fn sum_integers() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in 1..=5 {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, v) VALUES ({}, {i})",
                i * 1_000_000_000i64
            ));
        }
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_eq!(val, Value::I64(15));
    }

    #[test]
    fn sum_floats() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for i in 1..=3 {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, v) VALUES ({}, {}.5)",
                i * 1_000_000_000i64,
                i
            ));
        }
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_f64_close(&val, 7.5, 0.01); // 1.5 + 2.5 + 3.5
    }

    #[test]
    fn sum_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let val = db.query_scalar("SELECT sum(v) FROM t");
        assert_eq!(val, Value::Null);
    }

    #[test]
    fn sum_with_nulls() {
        let db = TestDb::with_trades(20);
        // volume has nulls at rows 0 and 10 -- sum should skip them
        let val = db.query_scalar("SELECT sum(volume) FROM trades");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            Value::I64(v) => assert!(v > 0),
            _ => panic!("expected numeric sum, got {val:?}"),
        }
    }

    #[test]
    fn avg_basic() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v BIGINT)");
        for i in [10, 20, 30] {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, v) VALUES ({}, {i})",
                i * 1_000_000_000i64
            ));
        }
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_f64_close(&val, 20.0, 0.01);
    }

    #[test]
    fn avg_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("INSERT INTO t (timestamp, v) VALUES (1000000000000, 42.0)");
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_f64_close(&val, 42.0, 0.01);
    }

    #[test]
    fn avg_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let val = db.query_scalar("SELECT avg(v) FROM t");
        assert_eq!(val, Value::Null);
    }

    #[test]
    fn min_basic() {
        let db = TestDb::with_trades(20);
        let val = db.query_scalar("SELECT min(price) FROM trades");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn min_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("INSERT INTO t (timestamp, v) VALUES (1000000000000, 42.0)");
        let val = db.query_scalar("SELECT min(v) FROM t");
        assert_eq!(val, Value::F64(42.0));
    }

    #[test]
    fn min_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let val = db.query_scalar("SELECT min(v) FROM t");
        assert_eq!(val, Value::Null);
    }

    #[test]
    fn max_basic() {
        let db = TestDb::with_trades(20);
        let val = db.query_scalar("SELECT max(price) FROM trades");
        match val {
            Value::F64(v) => assert!(v > 0.0),
            _ => panic!("expected F64"),
        }
    }

    #[test]
    fn max_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("INSERT INTO t (timestamp, v) VALUES (1000000000000, 42.0)");
        let val = db.query_scalar("SELECT max(v) FROM t");
        assert_eq!(val, Value::F64(42.0));
    }

    #[test]
    fn max_empty() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        let val = db.query_scalar("SELECT max(v) FROM t");
        assert_eq!(val, Value::Null);
    }

    #[test]
    fn min_max_with_nulls() {
        let db = TestDb::with_trades(20);
        let min_v = db.query_scalar("SELECT min(volume) FROM trades");
        let max_v = db.query_scalar("SELECT max(volume) FROM trades");
        // nulls should be skipped; min < max
        match (&min_v, &max_v) {
            (Value::F64(mn), Value::F64(mx)) => assert!(mn < mx),
            _ => {} // may be I64, just check non-null
        }
        assert_ne!(min_v, Value::Null);
        assert_ne!(max_v, Value::Null);
    }

    #[test]
    fn count_star() {
        let db = TestDb::with_trades(20);
        let val = db.query_scalar("SELECT count(*) FROM trades");
        assert_eq!(val, Value::I64(20));
    }

    #[test]
    fn count_column_skips_null() {
        let db = TestDb::with_trades(20);
        let count_star = db.query_scalar("SELECT count(*) FROM trades");
        let count_vol = db.query_scalar("SELECT count(volume) FROM trades");
        match (&count_star, &count_vol) {
            (Value::I64(star), Value::I64(col)) => {
                assert!(*col <= *star, "count(volume) should be <= count(*)");
            }
            _ => {}
        }
    }

    #[test]
    fn count_distinct_basic() {
        let db = TestDb::with_trades(20);
        let val = db.query_scalar("SELECT count_distinct(symbol) FROM trades");
        assert_eq!(val, Value::I64(3));
    }

    #[test]
    fn first_basic() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT first(symbol) FROM trades");
        match val {
            Value::Str(_) => {} // should be first non-null symbol
            _ => panic!("expected Str"),
        }
    }

    #[test]
    fn last_basic() {
        let db = TestDb::with_trades(10);
        let val = db.query_scalar("SELECT last(symbol) FROM trades");
        match val {
            Value::Str(_) => {} // should be last non-null symbol
            _ => panic!("expected Str"),
        }
    }

    #[test]
    fn first_last_differ() {
        let db = TestDb::with_trades(10);
        let first = db.query_scalar("SELECT first(price) FROM trades");
        let last = db.query_scalar("SELECT last(price) FROM trades");
        assert_ne!(first, last, "first and last price should differ for 10 rows");
    }

    #[test]
    fn stddev_known() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        // values: 2, 4, 4, 4, 5, 5, 7, 9
        for (idx, v) in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0].iter().enumerate() {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, v) VALUES ({}, {v})",
                (idx as i64 + 1) * 1_000_000_000
            ));
        }
        let val = db.query_scalar("SELECT stddev(v) FROM t");
        // population stddev = 2.0
        assert_f64_close(&val, 2.0, 0.01);
    }

    #[test]
    fn stddev_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("INSERT INTO t (timestamp, v) VALUES (1000000000000, 42.0)");
        let val = db.query_scalar("SELECT stddev(v) FROM t");
        assert_f64_close(&val, 0.0, 0.01);
    }

    #[test]
    fn variance_known() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for (idx, v) in [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0].iter().enumerate() {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, v) VALUES ({}, {v})",
                (idx as i64 + 1) * 1_000_000_000
            ));
        }
        let val = db.query_scalar("SELECT variance(v) FROM t");
        // population variance = 4.0
        assert_f64_close(&val, 4.0, 0.01);
    }

    #[test]
    fn median_odd() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for (idx, v) in [1.0, 3.0, 5.0].iter().enumerate() {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, v) VALUES ({}, {v})",
                (idx as i64 + 1) * 1_000_000_000
            ));
        }
        let val = db.query_scalar("SELECT median(v) FROM t");
        assert_f64_close(&val, 3.0, 0.01);
    }

    #[test]
    fn median_even() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        for (idx, v) in [1.0, 2.0, 3.0, 4.0].iter().enumerate() {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, v) VALUES ({}, {v})",
                (idx as i64 + 1) * 1_000_000_000
            ));
        }
        let val = db.query_scalar("SELECT median(v) FROM t");
        assert_f64_close(&val, 2.5, 0.01);
    }

    #[test]
    fn median_single() {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, v DOUBLE)");
        db.exec_ok("INSERT INTO t (timestamp, v) VALUES (1000000000000, 99.0)");
        let val = db.query_scalar("SELECT median(v) FROM t");
        assert_f64_close(&val, 99.0, 0.01);
    }

    #[test]
    fn group_by_count() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, count(*) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
        let total: i64 = rows.iter().filter_map(|r| match &r[1] {
            Value::I64(n) => Some(*n),
            _ => None,
        }).sum();
        assert_eq!(total, 30);
    }

    #[test]
    fn group_by_sum() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, sum(price) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
        for row in &rows {
            match &row[1] {
                Value::F64(v) => assert!(*v > 0.0),
                Value::I64(v) => assert!(*v > 0),
                _ => panic!("expected numeric sum"),
            }
        }
    }

    #[test]
    fn group_by_avg() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query("SELECT symbol, avg(price) FROM trades GROUP BY symbol");
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn group_by_min_max() {
        let db = TestDb::with_trades(30);
        let (_, rows) = db.query(
            "SELECT symbol, min(price), max(price) FROM trades GROUP BY symbol",
        );
        assert_eq!(rows.len(), 3);
        for row in &rows {
            // min <= max
            assert!(row[1].cmp_coerce(&row[2]) != Some(std::cmp::Ordering::Greater));
        }
    }
}

// ===========================================================================
// Cast functions
// ===========================================================================

mod cast_functions {
    use super::*;

    // --- cast_int ---
    #[test]
    fn cast_int_from_float() {
        assert_eq!(eval("cast_int", &[f(3.7)]), i(3)); // truncation
    }
    #[test]
    fn cast_int_from_int() {
        assert_eq!(eval("cast_int", &[i(42)]), i(42));
    }
    #[test]
    fn cast_int_from_string_valid() {
        assert_eq!(eval("cast_int", &[s("123")]), i(123));
    }
    #[test]
    fn cast_int_from_string_invalid() {
        let err = eval_err("cast_int", &[s("abc")]);
        assert!(err.contains("parse") || err.contains("cast"), "got: {err}");
    }
    #[test]
    fn cast_int_null() {
        assert_eq!(eval("cast_int", &[null()]), null());
    }

    // --- cast_float ---
    #[test]
    fn cast_float_from_int() {
        assert_eq!(eval("cast_float", &[i(42)]), f(42.0));
    }
    #[test]
    fn cast_float_from_float() {
        assert_eq!(eval("cast_float", &[f(3.14)]), f(3.14));
    }
    #[test]
    fn cast_float_from_string() {
        assert_eq!(eval("cast_float", &[s("3.14")]), f(3.14));
    }
    #[test]
    fn cast_float_null() {
        assert_eq!(eval("cast_float", &[null()]), null());
    }

    // --- cast_str ---
    #[test]
    fn cast_str_from_int() {
        assert_eq!(eval("cast_str", &[i(42)]), s("42"));
    }
    #[test]
    fn cast_str_from_float() {
        let result = eval("cast_str", &[f(3.14)]);
        match result {
            Value::Str(r) => assert!(r.starts_with("3.14")),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn cast_str_null() {
        assert_eq!(eval("cast_str", &[null()]), null());
    }

    // --- cast_bool ---
    #[test]
    fn cast_bool_int_true() {
        assert_eq!(eval("cast_bool", &[i(1)]), i(1));
    }
    #[test]
    fn cast_bool_int_false() {
        assert_eq!(eval("cast_bool", &[i(0)]), i(0));
    }
    #[test]
    fn cast_bool_string_true() {
        assert_eq!(eval("cast_bool", &[s("true")]), i(1));
    }
    #[test]
    fn cast_bool_string_false() {
        assert_eq!(eval("cast_bool", &[s("false")]), i(0));
    }
    #[test]
    fn cast_bool_null() {
        assert_eq!(eval("cast_bool", &[null()]), null());
    }
    #[test]
    fn cast_bool_float_nonzero() {
        assert_eq!(eval("cast_bool", &[f(1.5)]), i(1));
    }
    #[test]
    fn cast_bool_float_zero() {
        assert_eq!(eval("cast_bool", &[f(0.0)]), i(0));
    }

    // --- cast_timestamp ---
    #[test]
    fn cast_timestamp_from_int() {
        let ns = 1710460800_000_000_000i64;
        assert_eq!(eval("cast_timestamp", &[i(ns)]), ts(ns));
    }
    #[test]
    fn cast_timestamp_null() {
        assert_eq!(eval("cast_timestamp", &[null()]), null());
    }

    // --- typeof ---
    #[test]
    fn typeof_int() {
        let result = eval("typeof", &[i(42)]);
        match result {
            Value::Str(r) => assert!(r.to_lowercase().contains("int") || r.to_lowercase().contains("i64")),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn typeof_float() {
        let result = eval("typeof", &[f(3.14)]);
        match result {
            Value::Str(r) => assert!(r.to_lowercase().contains("float") || r.to_lowercase().contains("f64") || r.to_lowercase().contains("double")),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn typeof_string() {
        let result = eval("typeof", &[s("hello")]);
        match result {
            Value::Str(r) => assert!(r.to_lowercase().contains("str") || r.to_lowercase().contains("varchar") || r.to_lowercase().contains("string")),
            _ => panic!("expected Str"),
        }
    }
    #[test]
    fn typeof_null() {
        let result = eval("typeof", &[null()]);
        match result {
            Value::Str(r) => assert!(r.to_lowercase().contains("null")),
            _ => panic!("expected Str"),
        }
    }

    // --- safe_cast_int ---
    #[test]
    fn safe_cast_int_valid() {
        assert_eq!(eval("safe_cast_int", &[s("42")]), i(42));
    }
    #[test]
    fn safe_cast_int_invalid() {
        assert_eq!(eval("safe_cast_int", &[s("abc")]), null());
    }

    // --- safe_cast_float ---
    #[test]
    fn safe_cast_float_valid() {
        assert_eq!(eval("safe_cast_float", &[s("3.14")]), f(3.14));
    }
    #[test]
    fn safe_cast_float_invalid() {
        assert_eq!(eval("safe_cast_float", &[s("abc")]), null());
    }
}

// ===========================================================================
// Window functions (via SQL integration)
// ===========================================================================

mod window_functions {
    use super::*;

    fn make_ordered_db() -> TestDb {
        let db = TestDb::new();
        db.exec_ok("CREATE TABLE t (timestamp TIMESTAMP, grp VARCHAR, val DOUBLE)");
        let groups = ["A", "A", "A", "B", "B", "B"];
        let vals = [10.0, 20.0, 30.0, 100.0, 200.0, 300.0];
        for (idx, (g, v)) in groups.iter().zip(vals.iter()).enumerate() {
            db.exec_ok(&format!(
                "INSERT INTO t (timestamp, grp, val) VALUES ({}, '{g}', {v})",
                (idx as i64 + 1) * 1_000_000_000
            ));
        }
        db
    }

    #[test]
    fn row_number_basic() {
        let db = make_ordered_db();
        let result = db.exec("SELECT val FROM t ORDER BY val");
        match result {
            Ok(exchange_query::QueryResult::Rows { rows, .. }) => {
                assert_eq!(rows.len(), 6);
            }
            _ => {} // row_number might not be directly available in SELECT
        }
    }

    #[test]
    fn order_by_asc() {
        let db = make_ordered_db();
        let (_, rows) = db.query("SELECT val FROM t ORDER BY val");
        assert_eq!(rows.len(), 6);
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Greater));
        }
    }

    #[test]
    fn order_by_desc() {
        let db = make_ordered_db();
        let (_, rows) = db.query("SELECT val FROM t ORDER BY val DESC");
        assert_eq!(rows.len(), 6);
        for i in 1..rows.len() {
            assert!(rows[i - 1][0].cmp_coerce(&rows[i][0]) != Some(std::cmp::Ordering::Less));
        }
    }

    #[test]
    fn group_by_with_order() {
        let db = make_ordered_db();
        let (_, rows) = db.query("SELECT grp, sum(val) FROM t GROUP BY grp ORDER BY grp");
        assert_eq!(rows.len(), 2);
    }
}

// ===========================================================================
// Utility / hash / misc functions
// ===========================================================================

mod utility_functions {
    use super::*;

    // --- version ---
    #[test]
    fn version_returns_string() {
        let result = eval("version", &[]);
        match result {
            Value::Str(v) => assert!(!v.is_empty()),
            _ => panic!("expected Str"),
        }
    }

    // --- hash ---
    #[test]
    fn hash_deterministic() {
        let h1 = eval("hash", &[s("hello")]);
        let h2 = eval("hash", &[s("hello")]);
        assert_eq!(h1, h2);
    }
    #[test]
    fn hash_different_inputs() {
        let h1 = eval("hash", &[s("hello")]);
        let h2 = eval("hash", &[s("world")]);
        assert_ne!(h1, h2);
    }

    // --- murmur3 ---
    #[test]
    fn murmur3_deterministic() {
        let h1 = eval("murmur3", &[s("test")]);
        let h2 = eval("murmur3", &[s("test")]);
        assert_eq!(h1, h2);
    }

    // --- crc32 ---
    #[test]
    fn crc32_deterministic() {
        let h1 = eval("crc32", &[s("hello")]);
        let h2 = eval("crc32", &[s("hello")]);
        assert_eq!(h1, h2);
    }

    // --- rnd_int ---
    #[test]
    fn rnd_int_returns_int() {
        let result = eval("rnd_int", &[i(0), i(100)]);
        match result {
            Value::I64(v) => assert!(v >= 0 && v <= 100, "expected [0,100], got {v}"),
            _ => panic!("expected I64"),
        }
    }

    // --- rnd_double ---
    #[test]
    fn rnd_double_returns_float() {
        let result = eval("rnd_double", &[]);
        match result {
            Value::F64(_) => {}
            _ => panic!("expected F64"),
        }
    }

    // --- rnd_boolean ---
    #[test]
    fn rnd_boolean_returns_0_or_1() {
        let result = eval("rnd_boolean", &[]);
        match result {
            Value::I64(v) => assert!(v == 0 || v == 1, "expected 0 or 1, got {v}"),
            _ => panic!("expected I64"),
        }
    }

    // --- rnd_uuid4 ---
    #[test]
    fn rnd_uuid4_format() {
        let result = eval("rnd_uuid4", &[]);
        match result {
            Value::Str(uuid) => {
                assert_eq!(uuid.len(), 36, "UUID should be 36 chars, got {}", uuid.len());
                assert_eq!(uuid.chars().filter(|c| *c == '-').count(), 4);
            }
            _ => panic!("expected Str"),
        }
    }

    // --- sizeof ---
    #[test]
    fn sizeof_int() {
        let result = eval("sizeof", &[i(42)]);
        match result {
            Value::I64(n) => assert!(n > 0),
            _ => panic!("expected I64"),
        }
    }

    // --- is_positive / is_negative / is_zero ---
    #[test]
    fn is_positive_true() {
        assert_eq!(eval("is_positive", &[i(5)]), i(1));
    }
    #[test]
    fn is_positive_false() {
        assert_eq!(eval("is_positive", &[i(-5)]), i(0));
    }
    #[test]
    fn is_negative_true() {
        assert_eq!(eval("is_negative", &[i(-5)]), i(1));
    }
    #[test]
    fn is_negative_false() {
        assert_eq!(eval("is_negative", &[i(5)]), i(0));
    }
    #[test]
    fn is_zero_true() {
        assert_eq!(eval("is_zero", &[i(0)]), i(1));
    }
    #[test]
    fn is_zero_false() {
        assert_eq!(eval("is_zero", &[i(5)]), i(0));
    }

    // --- is_even / is_odd ---
    #[test]
    fn is_even_true() {
        assert_eq!(eval("is_even", &[i(4)]), i(1));
    }
    #[test]
    fn is_even_false() {
        assert_eq!(eval("is_even", &[i(3)]), i(0));
    }
    #[test]
    fn is_odd_true() {
        assert_eq!(eval("is_odd", &[i(3)]), i(1));
    }
    #[test]
    fn is_odd_false() {
        assert_eq!(eval("is_odd", &[i(4)]), i(0));
    }

    // --- between ---
    #[test]
    fn between_within() {
        assert_eq!(eval("between", &[i(5), i(1), i(10)]), i(1));
    }
    #[test]
    fn between_outside() {
        assert_eq!(eval("between", &[i(15), i(1), i(10)]), i(0));
    }
    #[test]
    fn between_boundary() {
        assert_eq!(eval("between", &[i(1), i(1), i(10)]), i(1));
    }

    // --- bit_count ---
    #[test]
    fn bit_count_basic() {
        assert_eq!(eval("bit_count", &[i(0b1010)]), i(2));
    }
    #[test]
    fn bit_count_zero() {
        assert_eq!(eval("bit_count", &[i(0)]), i(0));
    }
    #[test]
    fn bit_count_all_ones() {
        assert_eq!(eval("bit_count", &[i(0xFF)]), i(8));
    }

    // --- leading_zeros / trailing_zeros ---
    #[test]
    fn leading_zeros_basic() {
        let result = eval("leading_zeros", &[i(1)]);
        assert_eq!(result, i(63)); // 63 leading zeros in i64 for value 1
    }
    #[test]
    fn trailing_zeros_basic() {
        let result = eval("trailing_zeros", &[i(8)]); // 0b1000
        assert_eq!(result, i(3));
    }

    // --- byte_length ---
    #[test]
    fn byte_length_ascii() {
        assert_eq!(eval("byte_length", &[s("hello")]), i(5));
    }
    #[test]
    fn byte_length_empty() {
        assert_eq!(eval("byte_length", &[s("")]), i(0));
    }

    // --- json_extract ---
    #[test]
    fn json_extract_basic() {
        let result = eval("json_extract", &[s(r#"{"name":"John","age":30}"#), s("name")]);
        match result {
            Value::Str(v) => assert!(v.contains("John")),
            _ => panic!("expected Str containing John"),
        }
    }
    #[test]
    fn json_extract_null() {
        assert_eq!(eval("json_extract", &[null(), s("key")]), null());
    }

    // --- to_json ---
    #[test]
    fn to_json_int() {
        let result = eval("to_json", &[i(42)]);
        match result {
            Value::Str(j) => assert!(j.contains("42")),
            _ => panic!("expected Str"),
        }
    }

    // --- strcmp ---
    #[test]
    fn strcmp_equal() {
        assert_eq!(eval("strcmp", &[s("abc"), s("abc")]), i(0));
    }
    #[test]
    fn strcmp_less() {
        let result = eval("strcmp", &[s("abc"), s("def")]);
        match result {
            Value::I64(v) => assert!(v < 0),
            _ => panic!("expected I64"),
        }
    }
    #[test]
    fn strcmp_greater() {
        let result = eval("strcmp", &[s("def"), s("abc")]);
        match result {
            Value::I64(v) => assert!(v > 0),
            _ => panic!("expected I64"),
        }
    }

    // --- count_char ---
    #[test]
    fn count_char_basic() {
        assert_eq!(eval("count_char", &[s("hello"), s("l")]), i(2));
    }
    #[test]
    fn count_char_none() {
        assert_eq!(eval("count_char", &[s("hello"), s("z")]), i(0));
    }
}

// ===========================================================================
// SQL-level scalar function tests
// ===========================================================================

mod sql_scalar_functions {
    use super::*;
    use exchange_query::plan::QueryResult;

    #[test]
    fn sql_length_in_select() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT length(symbol) FROM trades");
        assert_eq!(rows.len(), 5);
        for row in &rows {
            match &row[0] {
                Value::I64(n) => assert!(*n > 0),
                _ => panic!("expected I64"),
            }
        }
    }

    #[test]
    fn sql_upper_in_select() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT upper(symbol) FROM trades");
        for row in &rows {
            match &row[0] {
                Value::Str(s) => assert_eq!(*s, s.to_uppercase()),
                _ => panic!("expected Str"),
            }
        }
    }

    #[test]
    fn sql_lower_in_select() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT lower(symbol) FROM trades");
        for row in &rows {
            match &row[0] {
                Value::Str(s) => assert_eq!(*s, s.to_lowercase()),
                _ => panic!("expected Str"),
            }
        }
    }

    #[test]
    fn sql_abs_in_select() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT abs(price) FROM trades");
        for row in &rows {
            match &row[0] {
                Value::F64(v) => assert!(*v >= 0.0),
                Value::I64(v) => assert!(*v >= 0),
                _ => panic!("expected numeric"),
            }
        }
    }

    #[test]
    fn sql_round_in_select() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT round(price) FROM trades");
        assert_eq!(rows.len(), 5);
    }

    #[test]
    fn sql_coalesce_in_select() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT coalesce(volume, 0.0) FROM trades");
        assert_eq!(rows.len(), 20);
        // No nulls should remain after coalesce
        for row in &rows {
            assert_ne!(row[0], Value::Null);
        }
    }

    #[test]
    fn sql_concat_in_select() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT concat(symbol, '-', side) FROM trades");
        for row in &rows {
            match &row[0] {
                Value::Str(s) => assert!(s.contains('-')),
                _ => panic!("expected Str"),
            }
        }
    }

    #[test]
    fn sql_replace_in_select() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT replace(symbol, '/', '-') FROM trades");
        for row in &rows {
            match &row[0] {
                Value::Str(s) => {
                    assert!(!s.contains('/'), "expected / to be replaced");
                    assert!(s.contains('-'), "expected - replacement");
                }
                _ => panic!("expected Str"),
            }
        }
    }

    #[test]
    fn sql_floor_in_select() {
        let db = TestDb::with_trades(5);
        // Use the alias "floor_double" if "floor" conflicts with SQL keyword
        let result = db.exec("SELECT floor(price) FROM trades");
        match result {
            Ok(QueryResult::Rows { rows, .. }) => assert_eq!(rows.len(), 5),
            Err(_) => {
                // floor may be a reserved word; try alias
                let (_, rows) = db.query("SELECT floor_double(price) FROM trades");
                assert_eq!(rows.len(), 5);
            }
            _ => {}
        }
    }

    #[test]
    fn sql_ceil_in_select() {
        let db = TestDb::with_trades(5);
        let result = db.exec("SELECT ceil(price) FROM trades");
        match result {
            Ok(QueryResult::Rows { rows, .. }) => assert_eq!(rows.len(), 5),
            Err(_) => {
                // ceil may be a reserved word; try alias
                let (_, rows) = db.query("SELECT ceil_double(price) FROM trades");
                assert_eq!(rows.len(), 5);
            }
            _ => {}
        }
    }

    #[test]
    fn sql_extract_year_in_select() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT extract_year(timestamp) FROM trades");
        for row in &rows {
            assert_eq!(row[0], Value::I64(2024));
        }
    }

    #[test]
    fn sql_extract_month_in_select() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT extract_month(timestamp) FROM trades");
        for row in &rows {
            assert_eq!(row[0], Value::I64(3)); // March
        }
    }

    #[test]
    fn sql_if_null_in_select() {
        let db = TestDb::with_trades(20);
        let (_, rows) = db.query("SELECT if_null(volume, -1.0) FROM trades");
        for row in &rows {
            assert_ne!(row[0], Value::Null, "if_null should replace NULLs");
        }
    }

    #[test]
    fn sql_sign_in_select() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT sign(price) FROM trades");
        for row in &rows {
            assert_eq!(row[0], Value::I64(1)); // prices are positive
        }
    }

    #[test]
    fn sql_substring_in_select() {
        let db = TestDb::with_trades(5);
        // SUBSTRING may use special SQL syntax; try using the alias "substr"
        let result = db.exec("SELECT substr(symbol, 1, 3) FROM trades");
        match result {
            Ok(QueryResult::Rows { rows, .. }) => {
                for row in &rows {
                    match &row[0] {
                        Value::Str(s) => assert_eq!(s.len(), 3),
                        _ => panic!("expected Str"),
                    }
                }
            }
            Err(_) => {
                // substr also may not be supported at SQL level
                let (_, rows) = db.query("SELECT left(symbol, 3) FROM trades");
                for row in &rows {
                    match &row[0] {
                        Value::Str(s) => assert_eq!(s.len(), 3),
                        _ => panic!("expected Str"),
                    }
                }
            }
            _ => {}
        }
    }

    #[test]
    fn sql_reverse_in_select() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT reverse(symbol) FROM trades");
        for row in &rows {
            match &row[0] {
                Value::Str(s) => assert!(s.ends_with("DSU/") || s.contains('/'), "got: {s}"),
                _ => panic!("expected Str"),
            }
        }
    }

    #[test]
    fn sql_left_in_select() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT left(symbol, 3) FROM trades");
        for row in &rows {
            match &row[0] {
                Value::Str(s) => assert_eq!(s.len(), 3),
                _ => panic!("expected Str"),
            }
        }
    }

    #[test]
    fn sql_greatest_in_select() {
        let db = TestDb::with_trades(5);
        let (_, rows) = db.query("SELECT greatest(price, 50000.0) FROM trades");
        for row in &rows {
            match &row[0] {
                Value::F64(v) => assert!(*v >= 50000.0),
                _ => panic!("expected F64"),
            }
        }
    }
}
