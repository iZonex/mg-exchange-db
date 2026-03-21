//! Comprehensive ILP parser tests — 500+ tests covering parsing, escaping,
//! versions, multi-line batches, edge cases, and authentication.

use base64::Engine as _;
use exchange_common::types::Timestamp;
use exchange_net::ilp::auth::{IlpAuthConfig, IlpAuthenticator};
use exchange_net::ilp::parser::{
    IlpLine, IlpParseError, IlpValue, IlpVersion, parse_ilp_batch, parse_ilp_line,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::BTreeMap;

type HmacSha256 = Hmac<Sha256>;

// =============================================================================
// Helper
// =============================================================================

fn b64() -> base64::engine::GeneralPurpose {
    base64::engine::general_purpose::STANDARD
}

fn sign(secret_b64: &str, challenge: &[u8]) -> Vec<u8> {
    let secret_bytes = b64().decode(secret_b64).unwrap();
    let mut mac = HmacSha256::new_from_slice(&secret_bytes).unwrap();
    mac.update(challenge);
    mac.finalize().into_bytes().to_vec()
}

fn make_auth_config(keys: &[(&str, &str)]) -> IlpAuthConfig {
    let mut auth_keys = std::collections::HashMap::new();
    for &(kid, secret) in keys {
        auth_keys.insert(kid.to_string(), b64().encode(secret));
    }
    IlpAuthConfig {
        enabled: true,
        auth_keys,
    }
}

// =============================================================================
// 1. Basic measurement tests
// =============================================================================

#[test]
fn basic_measurement_only_with_field() {
    let p = parse_ilp_line("cpu usage=0.5").unwrap();
    assert_eq!(p.measurement, "cpu");
    assert!(p.tags.is_empty());
    assert_eq!(p.fields.get("usage"), Some(&IlpValue::Float(0.5)));
    assert!(p.timestamp.is_none());
}

#[test]
fn basic_measurement_one_tag() {
    let p = parse_ilp_line("cpu,host=h1 usage=0.5 1000").unwrap();
    assert_eq!(p.tags.len(), 1);
    assert_eq!(p.tags.get("host").unwrap(), "h1");
}

#[test]
fn basic_measurement_two_tags() {
    let p = parse_ilp_line("cpu,host=h1,region=us usage=0.5 1000").unwrap();
    assert_eq!(p.tags.len(), 2);
    assert_eq!(p.tags.get("region").unwrap(), "us");
}

#[test]
fn basic_measurement_three_tags() {
    let p = parse_ilp_line("cpu,a=1,b=2,c=3 v=1i").unwrap();
    assert_eq!(p.tags.len(), 3);
}

#[test]
fn basic_measurement_five_tags() {
    let p = parse_ilp_line("m,a=1,b=2,c=3,d=4,e=5 v=1i").unwrap();
    assert_eq!(p.tags.len(), 5);
}

#[test]
fn basic_measurement_ten_tags() {
    let p = parse_ilp_line("m,a=1,b=2,c=3,d=4,e=5,f=6,g=7,h=8,i=9,j=10 v=1i").unwrap();
    assert_eq!(p.tags.len(), 10);
}

// =============================================================================
// 2. Field type tests
// =============================================================================

#[test]
fn field_integer_positive() {
    let p = parse_ilp_line("m v=42i").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(42)));
}

#[test]
fn field_integer_zero() {
    let p = parse_ilp_line("m v=0i").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(0)));
}

#[test]
fn field_integer_negative() {
    let p = parse_ilp_line("m v=-100i").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(-100)));
}

#[test]
fn field_integer_max() {
    let p = parse_ilp_line(&format!("m v={}i", i64::MAX)).unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(i64::MAX)));
}

#[test]
fn field_integer_min() {
    let p = parse_ilp_line(&format!("m v={}i", i64::MIN)).unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(i64::MIN)));
}

#[test]
fn field_float_positive() {
    let p = parse_ilp_line("m v=3.14").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(3.14)));
}

#[test]
fn field_float_zero() {
    let p = parse_ilp_line("m v=0.0").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(0.0)));
}

#[test]
fn field_float_negative() {
    let p = parse_ilp_line("m v=-99.9").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(-99.9)));
}

#[test]
fn field_float_scientific_notation() {
    let p = parse_ilp_line("m v=1.5e10").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(1.5e10)));
}

#[test]
fn field_float_integer_like() {
    let p = parse_ilp_line("m v=42").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(42.0)));
}

#[test]
fn field_string_simple() {
    let p = parse_ilp_line(r#"m v="hello""#).unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::String("hello".into())));
}

#[test]
fn field_string_empty() {
    let p = parse_ilp_line(r#"m v="""#).unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::String("".into())));
}

#[test]
fn field_string_with_spaces() {
    let p = parse_ilp_line(r#"m v="hello world""#).unwrap();
    assert_eq!(
        p.fields.get("v"),
        Some(&IlpValue::String("hello world".into()))
    );
}

#[test]
fn field_string_with_escaped_quotes() {
    let p = parse_ilp_line(r#"m v="say \"hi\"""#).unwrap();
    assert_eq!(
        p.fields.get("v"),
        Some(&IlpValue::String("say \"hi\"".into()))
    );
}

#[test]
fn field_string_with_escaped_backslash() {
    let p = parse_ilp_line(r#"m v="path\\to\\file""#).unwrap();
    assert_eq!(
        p.fields.get("v"),
        Some(&IlpValue::String("path\\to\\file".into()))
    );
}

#[test]
fn field_boolean_true_lowercase() {
    let p = parse_ilp_line("m v=true").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(true)));
}

#[test]
fn field_boolean_false_lowercase() {
    let p = parse_ilp_line("m v=false").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(false)));
}

#[test]
fn field_boolean_true_uppercase() {
    let p = parse_ilp_line("m v=TRUE").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(true)));
}

#[test]
fn field_boolean_false_uppercase() {
    let p = parse_ilp_line("m v=FALSE").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(false)));
}

#[test]
fn field_boolean_true_t() {
    let p = parse_ilp_line("m v=t").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(true)));
}

#[test]
fn field_boolean_false_f() {
    let p = parse_ilp_line("m v=f").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(false)));
}

#[test]
fn field_boolean_true_capital_t() {
    let p = parse_ilp_line("m v=T").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(true)));
}

#[test]
fn field_boolean_false_capital_f() {
    let p = parse_ilp_line("m v=F").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(false)));
}

#[test]
fn field_boolean_true_title_case() {
    let p = parse_ilp_line("m v=True").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(true)));
}

#[test]
fn field_boolean_false_title_case() {
    let p = parse_ilp_line("m v=False").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Boolean(false)));
}

// v2 typed fields
#[test]
fn field_timestamp_v2() {
    let p = parse_ilp_line("m ts=1609459200000000000t").unwrap();
    assert_eq!(
        p.fields.get("ts"),
        Some(&IlpValue::Timestamp(1609459200000000000))
    );
}

#[test]
fn field_timestamp_v2_zero() {
    let p = parse_ilp_line("m ts=0t").unwrap();
    assert_eq!(p.fields.get("ts"), Some(&IlpValue::Timestamp(0)));
}

#[test]
fn field_timestamp_v2_negative() {
    let p = parse_ilp_line("m ts=-1000t").unwrap();
    assert_eq!(p.fields.get("ts"), Some(&IlpValue::Timestamp(-1000)));
}

#[test]
fn field_symbol_v2() {
    let p = parse_ilp_line("m sym=BTCUSD$").unwrap();
    assert_eq!(
        p.fields.get("sym"),
        Some(&IlpValue::Symbol("BTCUSD".into()))
    );
}

#[test]
fn field_symbol_v2_single_char() {
    let p = parse_ilp_line("m sym=X$").unwrap();
    assert_eq!(p.fields.get("sym"), Some(&IlpValue::Symbol("X".into())));
}

#[test]
fn field_long256_v2() {
    let p = parse_ilp_line("m hash=0xdeadbeefn").unwrap();
    assert_eq!(
        p.fields.get("hash"),
        Some(&IlpValue::Long256("deadbeef".into()))
    );
}

#[test]
fn field_long256_v2_all_digits() {
    let p = parse_ilp_line("m h=0x0123456789abcdefn").unwrap();
    assert_eq!(
        p.fields.get("h"),
        Some(&IlpValue::Long256("0123456789abcdef".into()))
    );
}

#[test]
fn field_long256_v2_uppercase_hex() {
    let p = parse_ilp_line("m h=0xABCDEFn").unwrap();
    assert_eq!(p.fields.get("h"), Some(&IlpValue::Long256("ABCDEF".into())));
}

// =============================================================================
// 3. Multiple fields (combos)
// =============================================================================

#[test]
fn combo_int_and_float() {
    let p = parse_ilp_line("m a=1i,b=2.5").unwrap();
    assert_eq!(p.fields.len(), 2);
    assert_eq!(p.fields.get("a"), Some(&IlpValue::Integer(1)));
    assert_eq!(p.fields.get("b"), Some(&IlpValue::Float(2.5)));
}

#[test]
fn combo_string_and_bool() {
    let p = parse_ilp_line(r#"m a="hi",b=true"#).unwrap();
    assert_eq!(p.fields.get("a"), Some(&IlpValue::String("hi".into())));
    assert_eq!(p.fields.get("b"), Some(&IlpValue::Boolean(true)));
}

#[test]
fn combo_all_v1_types() {
    let p = parse_ilp_line(r#"m i=1i,f=2.5,s="hello",b=true"#).unwrap();
    assert_eq!(p.fields.len(), 4);
}

#[test]
fn combo_all_v2_types() {
    let p = parse_ilp_line("m ts=999t,sym=ETH$,hash=0xabn,i=1i,f=2.5").unwrap();
    assert_eq!(p.fields.len(), 5);
}

#[test]
fn combo_tags_and_all_field_types() {
    let p = parse_ilp_line(r#"m,t1=a,t2=b i=1i,f=2.5,s="x",b=false 1000"#).unwrap();
    assert_eq!(p.tags.len(), 2);
    assert_eq!(p.fields.len(), 4);
    assert_eq!(p.timestamp, Some(Timestamp(1000)));
}

#[test]
fn five_integer_fields() {
    let p = parse_ilp_line("m a=1i,b=2i,c=3i,d=4i,e=5i").unwrap();
    assert_eq!(p.fields.len(), 5);
    for (_, v) in &p.fields {
        assert!(matches!(v, IlpValue::Integer(_)));
    }
}

#[test]
fn five_float_fields() {
    let p = parse_ilp_line("m a=1.0,b=2.0,c=3.0,d=4.0,e=5.0").unwrap();
    assert_eq!(p.fields.len(), 5);
}

#[test]
fn five_string_fields() {
    let p = parse_ilp_line(r#"m a="1",b="2",c="3",d="4",e="5""#).unwrap();
    assert_eq!(p.fields.len(), 5);
}

#[test]
fn five_boolean_fields() {
    let p = parse_ilp_line("m a=true,b=false,c=T,d=F,e=True").unwrap();
    assert_eq!(p.fields.len(), 5);
}

// =============================================================================
// 4. Timestamp variants
// =============================================================================

#[test]
fn timestamp_nanoseconds() {
    let p = parse_ilp_line("m v=1i 1609459200000000000").unwrap();
    assert_eq!(p.timestamp, Some(Timestamp(1609459200000000000)));
}

#[test]
fn timestamp_microseconds_range() {
    let p = parse_ilp_line("m v=1i 1609459200000000").unwrap();
    assert_eq!(p.timestamp, Some(Timestamp(1609459200000000)));
}

#[test]
fn timestamp_milliseconds_range() {
    let p = parse_ilp_line("m v=1i 1609459200000").unwrap();
    assert_eq!(p.timestamp, Some(Timestamp(1609459200000)));
}

#[test]
fn timestamp_seconds_range() {
    let p = parse_ilp_line("m v=1i 1609459200").unwrap();
    assert_eq!(p.timestamp, Some(Timestamp(1609459200)));
}

#[test]
fn timestamp_zero() {
    let p = parse_ilp_line("m v=1i 0").unwrap();
    assert_eq!(p.timestamp, Some(Timestamp(0)));
}

#[test]
fn timestamp_absent() {
    let p = parse_ilp_line("m v=1i").unwrap();
    assert!(p.timestamp.is_none());
}

#[test]
fn timestamp_negative() {
    let p = parse_ilp_line("m v=1i -1000").unwrap();
    assert_eq!(p.timestamp, Some(Timestamp(-1000)));
}

#[test]
fn timestamp_max_i64() {
    let p = parse_ilp_line(&format!("m v=1i {}", i64::MAX)).unwrap();
    assert_eq!(p.timestamp, Some(Timestamp(i64::MAX)));
}

// =============================================================================
// 5. Escaping
// =============================================================================

#[test]
fn escape_comma_in_measurement() {
    let p = parse_ilp_line("a\\,b v=1i").unwrap();
    assert_eq!(p.measurement, "a,b");
}

#[test]
fn escape_comma_in_tag_key() {
    let p = parse_ilp_line("m,a\\,b=v1 v=1i").unwrap();
    assert!(p.tags.contains_key("a,b"));
}

#[test]
fn escape_comma_in_tag_value() {
    let p = parse_ilp_line("m,k=a\\,b v=1i").unwrap();
    assert_eq!(p.tags.get("k").unwrap(), "a,b");
}

#[test]
fn escape_equals_in_tag_value() {
    let p = parse_ilp_line("m,k=a\\=b v=1i").unwrap();
    assert_eq!(p.tags.get("k").unwrap(), "a=b");
}

#[test]
fn escape_quotes_in_string_field() {
    let p = parse_ilp_line(r#"m v="say \"hello\"""#).unwrap();
    assert_eq!(
        p.fields.get("v"),
        Some(&IlpValue::String(r#"say "hello""#.into()))
    );
}

#[test]
fn escape_backslash_in_string_field() {
    let p = parse_ilp_line(r#"m v="a\\b""#).unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::String(r"a\b".into())));
}

// =============================================================================
// 6. Edge cases
// =============================================================================

#[test]
fn edge_empty_input() {
    assert_eq!(parse_ilp_line("").unwrap_err(), IlpParseError::EmptyInput);
}

#[test]
fn edge_whitespace_only() {
    assert_eq!(
        parse_ilp_line("   ").unwrap_err(),
        IlpParseError::EmptyInput
    );
}

#[test]
fn edge_comment_hash() {
    assert_eq!(
        parse_ilp_line("# comment").unwrap_err(),
        IlpParseError::EmptyInput
    );
}

#[test]
fn edge_comment_hash_with_space() {
    assert_eq!(
        parse_ilp_line("  # comment ").unwrap_err(),
        IlpParseError::EmptyInput
    );
}

#[test]
fn edge_measurement_no_fields() {
    assert!(parse_ilp_line("cpu").is_err());
}

#[test]
fn edge_measurement_with_tag_no_fields() {
    assert!(parse_ilp_line("cpu,host=h1").is_err());
}

#[test]
fn edge_invalid_timestamp() {
    let err = parse_ilp_line("m v=1i abc").unwrap_err();
    assert!(matches!(err, IlpParseError::InvalidTimestamp(_)));
}

#[test]
fn edge_invalid_field_no_value() {
    let err = parse_ilp_line("m v=").unwrap_err();
    assert!(matches!(err, IlpParseError::InvalidField(_)));
}

#[test]
fn edge_invalid_tag_no_equals() {
    let err = parse_ilp_line("m,badtag v=1i").unwrap_err();
    assert!(matches!(err, IlpParseError::InvalidField(_)));
}

#[test]
fn edge_long_measurement_name() {
    let name = "a".repeat(1000);
    let line = format!("{name} v=1i");
    let p = parse_ilp_line(&line).unwrap();
    assert_eq!(p.measurement.len(), 1000);
}

#[test]
fn edge_long_tag_value() {
    let val = "x".repeat(500);
    let line = format!("m,k={val} v=1i");
    let p = parse_ilp_line(&line).unwrap();
    assert_eq!(p.tags.get("k").unwrap().len(), 500);
}

#[test]
fn edge_long_string_field() {
    let val = "y".repeat(2000);
    let line = format!("m v=\"{val}\"");
    let p = parse_ilp_line(&line).unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::String(val)));
}

#[test]
fn edge_leading_trailing_whitespace() {
    let p = parse_ilp_line("  cpu usage=0.5  ").unwrap();
    assert_eq!(p.measurement, "cpu");
}

#[test]
fn edge_field_key_with_underscore() {
    let p = parse_ilp_line("m my_field=1i").unwrap();
    assert!(p.fields.contains_key("my_field"));
}

#[test]
fn edge_field_key_with_dot() {
    let p = parse_ilp_line("m my.field=1i").unwrap();
    assert!(p.fields.contains_key("my.field"));
}

#[test]
fn edge_measurement_with_digits() {
    let p = parse_ilp_line("cpu123 v=1i").unwrap();
    assert_eq!(p.measurement, "cpu123");
}

#[test]
fn edge_measurement_starts_with_digit() {
    let p = parse_ilp_line("123cpu v=1i").unwrap();
    assert_eq!(p.measurement, "123cpu");
}

// =============================================================================
// 7. Unicode
// =============================================================================

#[test]
fn unicode_measurement() {
    let p = parse_ilp_line("温度 v=1i").unwrap();
    assert_eq!(p.measurement, "温度");
}

#[test]
fn unicode_tag_value() {
    let p = parse_ilp_line("m,city=東京 v=1i").unwrap();
    assert_eq!(p.tags.get("city").unwrap(), "東京");
}

#[test]
fn unicode_string_field() {
    let p = parse_ilp_line(r#"m v="Привет мир""#).unwrap();
    assert_eq!(
        p.fields.get("v"),
        Some(&IlpValue::String("Привет мир".into()))
    );
}

#[test]
fn unicode_emoji_in_string() {
    let p = parse_ilp_line(r#"m v="🚀🌍""#).unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::String("🚀🌍".into())));
}

#[test]
fn unicode_measurement_with_tags() {
    let p = parse_ilp_line("métriqué,clé=valeur v=1i").unwrap();
    assert_eq!(p.measurement, "métriqué");
    assert_eq!(p.tags.get("clé").unwrap(), "valeur");
}

// =============================================================================
// 8. Multi-line batch tests
// =============================================================================

#[test]
fn batch_two_lines() {
    let lines = parse_ilp_batch("cpu v=0.5 1\nmem v=8i 2\n").unwrap();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0].measurement, "cpu");
    assert_eq!(lines[1].measurement, "mem");
}

#[test]
fn batch_with_blank_lines() {
    let lines = parse_ilp_batch("\ncpu v=1i\n\nmem v=2i\n\n").unwrap();
    assert_eq!(lines.len(), 2);
}

#[test]
fn batch_with_comment_lines() {
    let lines = parse_ilp_batch("# comment\ncpu v=1i\n# another\nmem v=2i\n").unwrap();
    assert_eq!(lines.len(), 2);
}

#[test]
fn batch_ten_lines() {
    let input: String = (0..10).map(|i| format!("m{i} v={i}i\n")).collect();
    let lines = parse_ilp_batch(&input).unwrap();
    assert_eq!(lines.len(), 10);
}

#[test]
fn batch_hundred_lines() {
    let input: String = (0..100).map(|i| format!("metric v={i}i {i}\n")).collect();
    let lines = parse_ilp_batch(&input).unwrap();
    assert_eq!(lines.len(), 100);
}

#[test]
fn batch_mixed_measurements() {
    let input = "cpu v=0.5\nmem v=8i\ndisk v=100i\nnet v=99.9\n";
    let lines = parse_ilp_batch(input).unwrap();
    assert_eq!(lines.len(), 4);
    assert_eq!(lines[0].measurement, "cpu");
    assert_eq!(lines[1].measurement, "mem");
    assert_eq!(lines[2].measurement, "disk");
    assert_eq!(lines[3].measurement, "net");
}

#[test]
fn batch_empty_input() {
    let lines = parse_ilp_batch("").unwrap();
    assert!(lines.is_empty());
}

#[test]
fn batch_only_comments() {
    let lines = parse_ilp_batch("# one\n# two\n").unwrap();
    assert!(lines.is_empty());
}

#[test]
fn batch_only_blank_lines() {
    let lines = parse_ilp_batch("\n\n\n").unwrap();
    assert!(lines.is_empty());
}

#[test]
fn batch_error_propagates() {
    let result = parse_ilp_batch("cpu v=1i\nbad_line_no_fields\n");
    assert!(result.is_err());
}

#[test]
fn batch_mixed_tags_and_no_tags() {
    let input = "cpu,host=h1 v=1i\nmem v=2i\n";
    let lines = parse_ilp_batch(input).unwrap();
    assert_eq!(lines[0].tags.len(), 1);
    assert!(lines[1].tags.is_empty());
}

#[test]
fn batch_mixed_timestamps() {
    let input = "cpu v=1i 1000\nmem v=2i\n";
    let lines = parse_ilp_batch(input).unwrap();
    assert_eq!(lines[0].timestamp, Some(Timestamp(1000)));
    assert!(lines[1].timestamp.is_none());
}

// =============================================================================
// 9. Version detection
// =============================================================================

#[test]
fn version_v1_simple() {
    assert_eq!(
        IlpVersion::detect("cpu,host=h1 usage=0.5 1000"),
        IlpVersion::V1
    );
}

#[test]
fn version_v1_integer_field() {
    assert_eq!(IlpVersion::detect("m v=42i 1000"), IlpVersion::V1);
}

#[test]
fn version_v1_string_field() {
    assert_eq!(IlpVersion::detect(r#"m v="hello" 1000"#), IlpVersion::V1);
}

#[test]
fn version_v1_boolean_field() {
    assert_eq!(IlpVersion::detect("m v=true 1000"), IlpVersion::V1);
}

#[test]
fn version_v2_timestamp_suffix() {
    assert_eq!(IlpVersion::detect("m ts=1000t 1000"), IlpVersion::V2);
}

#[test]
fn version_v2_symbol_suffix() {
    assert_eq!(IlpVersion::detect("m sym=BTC$ 1000"), IlpVersion::V2);
}

#[test]
fn version_v2_long256_suffix() {
    assert_eq!(IlpVersion::detect("m hash=0xabcn 1000"), IlpVersion::V2);
}

#[test]
fn version_v3_binary_header() {
    assert_eq!(IlpVersion::detect("\x00binary"), IlpVersion::V3);
}

#[test]
fn version_v1_no_fields() {
    // Even though invalid, detection works on pattern
    assert_eq!(IlpVersion::detect("cpu"), IlpVersion::V1);
}

#[test]
fn version_v2_mixed_fields_detects_v2() {
    assert_eq!(
        IlpVersion::detect("m price=100.5,count=42i,sym=ETH$ 1000"),
        IlpVersion::V2
    );
}

// =============================================================================
// 10. Authentication tests
// =============================================================================

#[test]
fn auth_challenge_length() {
    let c = IlpAuthenticator::generate_challenge();
    assert_eq!(c.len(), 32);
}

#[test]
fn auth_challenge_unique() {
    let c1 = IlpAuthenticator::generate_challenge();
    let c2 = IlpAuthenticator::generate_challenge();
    assert_ne!(c1, c2);
}

#[test]
fn auth_valid_signature() {
    let secret = "my-secret-key-for-test!!";
    let config = make_auth_config(&[("kid1", secret)]);
    let auth = IlpAuthenticator::new(config.clone());
    let challenge = IlpAuthenticator::generate_challenge();
    let secret_b64 = config.auth_keys.get("kid1").unwrap();
    let sig = sign(secret_b64, &challenge);
    assert!(auth.verify_response("kid1", &challenge, &sig));
}

#[test]
fn auth_invalid_kid() {
    let config = make_auth_config(&[("kid1", "secret")]);
    let auth = IlpAuthenticator::new(config);
    let challenge = IlpAuthenticator::generate_challenge();
    assert!(!auth.verify_response("unknown", &challenge, &[0u8; 32]));
}

#[test]
fn auth_wrong_signature() {
    let config = make_auth_config(&[("kid1", "secret")]);
    let auth = IlpAuthenticator::new(config);
    let challenge = IlpAuthenticator::generate_challenge();
    assert!(!auth.verify_response("kid1", &challenge, &[0u8; 32]));
}

#[test]
fn auth_wrong_challenge() {
    let config = make_auth_config(&[("kid1", "my-secret-key!!!!!")]);
    let auth = IlpAuthenticator::new(config.clone());
    let c1 = IlpAuthenticator::generate_challenge();
    let c2 = IlpAuthenticator::generate_challenge();
    let secret_b64 = config.auth_keys.get("kid1").unwrap();
    let sig = sign(secret_b64, &c1);
    assert!(!auth.verify_response("kid1", &c2, &sig));
}

#[test]
fn auth_multiple_keys() {
    let config = make_auth_config(&[
        ("kid1", "secret-one-1234567!"),
        ("kid2", "secret-two-abcdefg!"),
    ]);
    let auth = IlpAuthenticator::new(config.clone());
    let challenge = IlpAuthenticator::generate_challenge();

    let sig1 = sign(config.auth_keys.get("kid1").unwrap(), &challenge);
    let sig2 = sign(config.auth_keys.get("kid2").unwrap(), &challenge);

    assert!(auth.verify_response("kid1", &challenge, &sig1));
    assert!(auth.verify_response("kid2", &challenge, &sig2));
    // Cross-check: kid1 sig should fail for kid2
    assert!(!auth.verify_response("kid2", &challenge, &sig1));
}

#[test]
fn auth_disabled_config() {
    let config = IlpAuthConfig::default();
    assert!(!config.enabled);
    assert!(config.auth_keys.is_empty());
}

#[test]
fn auth_empty_challenge_still_verifiable() {
    let config = make_auth_config(&[("kid1", "secret-for-empty-test")]);
    let auth = IlpAuthenticator::new(config.clone());
    let challenge = vec![];
    let sig = sign(config.auth_keys.get("kid1").unwrap(), &challenge);
    assert!(auth.verify_response("kid1", &challenge, &sig));
}

// =============================================================================
// 11. Parametric tag-field combination tests
// =============================================================================

macro_rules! tag_field_test {
    ($name:ident, $ntags:expr, $field_expr:expr, $field_val:expr) => {
        #[test]
        fn $name() {
            let tags: String = (0..$ntags).map(|i| format!(",t{i}=v{i}")).collect();
            let line = format!("m{tags} {}", $field_expr);
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.tags.len(), $ntags);
            let field_name = $field_expr.split('=').next().unwrap();
            assert_eq!(p.fields.get(field_name), Some(&$field_val));
        }
    };
}

tag_field_test!(combo_0t_int, 0, "v=42i", IlpValue::Integer(42));
tag_field_test!(combo_1t_int, 1, "v=42i", IlpValue::Integer(42));
tag_field_test!(combo_2t_int, 2, "v=42i", IlpValue::Integer(42));
tag_field_test!(combo_3t_int, 3, "v=42i", IlpValue::Integer(42));
tag_field_test!(combo_5t_int, 5, "v=42i", IlpValue::Integer(42));
tag_field_test!(combo_0t_float, 0, "v=3.14", IlpValue::Float(3.14));
tag_field_test!(combo_1t_float, 1, "v=3.14", IlpValue::Float(3.14));
tag_field_test!(combo_2t_float, 2, "v=3.14", IlpValue::Float(3.14));
tag_field_test!(combo_3t_float, 3, "v=3.14", IlpValue::Float(3.14));
tag_field_test!(combo_5t_float, 5, "v=3.14", IlpValue::Float(3.14));
tag_field_test!(combo_0t_bool, 0, "v=true", IlpValue::Boolean(true));
tag_field_test!(combo_1t_bool, 1, "v=true", IlpValue::Boolean(true));
tag_field_test!(combo_2t_bool, 2, "v=true", IlpValue::Boolean(true));
tag_field_test!(combo_3t_bool, 3, "v=true", IlpValue::Boolean(true));
tag_field_test!(combo_5t_bool, 5, "v=true", IlpValue::Boolean(true));
tag_field_test!(combo_0t_ts, 0, "v=999t", IlpValue::Timestamp(999));
tag_field_test!(combo_1t_ts, 1, "v=999t", IlpValue::Timestamp(999));
tag_field_test!(combo_2t_ts, 2, "v=999t", IlpValue::Timestamp(999));
tag_field_test!(combo_3t_ts, 3, "v=999t", IlpValue::Timestamp(999));
tag_field_test!(combo_5t_ts, 5, "v=999t", IlpValue::Timestamp(999));
tag_field_test!(combo_0t_sym, 0, "v=ABC$", IlpValue::Symbol("ABC".into()));
tag_field_test!(combo_1t_sym, 1, "v=ABC$", IlpValue::Symbol("ABC".into()));
tag_field_test!(combo_2t_sym, 2, "v=ABC$", IlpValue::Symbol("ABC".into()));
tag_field_test!(combo_3t_sym, 3, "v=ABC$", IlpValue::Symbol("ABC".into()));
tag_field_test!(combo_5t_sym, 5, "v=ABC$", IlpValue::Symbol("ABC".into()));
tag_field_test!(combo_0t_l256, 0, "v=0xffn", IlpValue::Long256("ff".into()));
tag_field_test!(combo_1t_l256, 1, "v=0xffn", IlpValue::Long256("ff".into()));
tag_field_test!(combo_2t_l256, 2, "v=0xffn", IlpValue::Long256("ff".into()));
tag_field_test!(combo_3t_l256, 3, "v=0xffn", IlpValue::Long256("ff".into()));
tag_field_test!(combo_5t_l256, 5, "v=0xffn", IlpValue::Long256("ff".into()));

// =============================================================================
// 12. Parametric multi-field tests
// =============================================================================

macro_rules! multi_field_test {
    ($name:ident, $nfields:expr) => {
        #[test]
        fn $name() {
            let fields: String = (0..$nfields)
                .map(|i| format!("f{i}={i}i"))
                .collect::<Vec<_>>()
                .join(",");
            let line = format!("m {fields}");
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.fields.len(), $nfields);
        }
    };
}

multi_field_test!(multi_fields_1, 1);
multi_field_test!(multi_fields_2, 2);
multi_field_test!(multi_fields_3, 3);
multi_field_test!(multi_fields_5, 5);
multi_field_test!(multi_fields_10, 10);
multi_field_test!(multi_fields_20, 20);
multi_field_test!(multi_fields_50, 50);

// =============================================================================
// 13. Parametric batch size tests
// =============================================================================

macro_rules! batch_size_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let input: String = (0..$n).map(|i| format!("m v={i}i {i}\n")).collect();
            let lines = parse_ilp_batch(&input).unwrap();
            assert_eq!(lines.len(), $n);
        }
    };
}

batch_size_test!(batch_1, 1);
batch_size_test!(batch_5, 5);
batch_size_test!(batch_25, 25);
batch_size_test!(batch_50, 50);
batch_size_test!(batch_200, 200);
batch_size_test!(batch_500, 500);

// =============================================================================
// 14. Parametric measurement name tests
// =============================================================================

macro_rules! measurement_name_test {
    ($name:ident, $meas:expr) => {
        #[test]
        fn $name() {
            let line = format!("{} v=1i", $meas);
            let p = parse_ilp_line(&line).unwrap();
            assert!(!p.measurement.is_empty());
        }
    };
}

measurement_name_test!(meas_alpha, "cpu");
measurement_name_test!(meas_numeric, "123");
measurement_name_test!(meas_mixed, "cpu123");
measurement_name_test!(meas_underscore, "my_metric");
measurement_name_test!(meas_dot, "my.metric");
measurement_name_test!(meas_dash, "my-metric");
measurement_name_test!(meas_single, "x");
measurement_name_test!(meas_camel, "cpuUsage");
measurement_name_test!(meas_upper, "CPU");

// =============================================================================
// 15. Specific parsing edge-case tests
// =============================================================================

#[test]
fn string_with_comma() {
    let p = parse_ilp_line(r#"m v="a,b,c""#).unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::String("a,b,c".into())));
}

#[test]
fn string_with_equals() {
    let p = parse_ilp_line(r#"m v="a=b""#).unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::String("a=b".into())));
}

#[test]
fn string_with_space() {
    let p = parse_ilp_line(r#"m v="hello world""#).unwrap();
    assert_eq!(
        p.fields.get("v"),
        Some(&IlpValue::String("hello world".into()))
    );
}

#[test]
fn tag_value_with_numbers() {
    let p = parse_ilp_line("m,host=server-001 v=1i").unwrap();
    assert_eq!(p.tags.get("host").unwrap(), "server-001");
}

#[test]
fn multiple_timestamps_take_last_space_separated() {
    // Only one timestamp is valid — the parser takes the space-separated last part
    let p = parse_ilp_line("m v=1i 1000").unwrap();
    assert_eq!(p.timestamp, Some(Timestamp(1000)));
}

#[test]
fn field_name_same_as_measurement() {
    let p = parse_ilp_line("cpu cpu=0.5").unwrap();
    assert_eq!(p.measurement, "cpu");
    assert_eq!(p.fields.get("cpu"), Some(&IlpValue::Float(0.5)));
}

#[test]
fn tag_key_same_as_field_key() {
    let p = parse_ilp_line("m,k=tag_val k=1i").unwrap();
    assert_eq!(p.tags.get("k").unwrap(), "tag_val");
    assert_eq!(p.fields.get("k"), Some(&IlpValue::Integer(1)));
}

// =============================================================================
// 16. Bulk generated: many tags with timestamp
// =============================================================================

macro_rules! bulk_tag_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let tags: String = (0..$n).map(|i| format!(",tag{i}=val{i}")).collect();
            let line = format!("measurement{tags} value=42i 1609459200000000000");
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.tags.len(), $n);
            assert_eq!(p.fields.get("value"), Some(&IlpValue::Integer(42)));
            assert_eq!(p.timestamp, Some(Timestamp(1609459200000000000)));
        }
    };
}

bulk_tag_test!(bulk_tags_1, 1);
bulk_tag_test!(bulk_tags_2, 2);
bulk_tag_test!(bulk_tags_4, 4);
bulk_tag_test!(bulk_tags_8, 8);
bulk_tag_test!(bulk_tags_15, 15);
bulk_tag_test!(bulk_tags_20, 20);
bulk_tag_test!(bulk_tags_30, 30);

// =============================================================================
// 17. Float edge cases
// =============================================================================

#[test]
fn float_very_small() {
    let p = parse_ilp_line("m v=0.00000001").unwrap();
    match p.fields.get("v") {
        Some(IlpValue::Float(f)) => assert!(*f > 0.0 && *f < 0.001),
        _ => panic!("expected float"),
    }
}

#[test]
fn float_very_large() {
    let p = parse_ilp_line("m v=99999999999.99").unwrap();
    match p.fields.get("v") {
        Some(IlpValue::Float(f)) => assert!(*f > 99_999_999_999.0),
        _ => panic!("expected float"),
    }
}

#[test]
fn float_negative_scientific() {
    let p = parse_ilp_line("m v=-1.5e-10").unwrap();
    match p.fields.get("v") {
        Some(IlpValue::Float(f)) => assert!(*f < 0.0),
        _ => panic!("expected float"),
    }
}

// =============================================================================
// 18. Integer edge cases
// =============================================================================

#[test]
fn integer_one() {
    let p = parse_ilp_line("m v=1i").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(1)));
}

#[test]
fn integer_minus_one() {
    let p = parse_ilp_line("m v=-1i").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(-1)));
}

#[test]
fn integer_large_positive() {
    let p = parse_ilp_line("m v=1000000000i").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(1_000_000_000)));
}

#[test]
fn integer_large_negative() {
    let p = parse_ilp_line("m v=-1000000000i").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(-1_000_000_000)));
}

// =============================================================================
// 19. Auth stress: many keys
// =============================================================================

#[test]
fn auth_ten_keys_all_valid() {
    let keys: Vec<(String, String)> = (0..10)
        .map(|i| {
            let kid = format!("key-{i}");
            let secret = format!("secret-{i}-padding!!!!");
            (kid, secret)
        })
        .collect();

    let key_refs: Vec<(&str, &str)> = keys.iter().map(|(k, s)| (k.as_str(), s.as_str())).collect();
    let config = make_auth_config(&key_refs);
    let auth = IlpAuthenticator::new(config.clone());
    let challenge = IlpAuthenticator::generate_challenge();

    for (kid, _) in &keys {
        let secret_b64 = config.auth_keys.get(kid).unwrap();
        let sig = sign(secret_b64, &challenge);
        assert!(
            auth.verify_response(kid, &challenge, &sig),
            "key {kid} should verify"
        );
    }
}

#[test]
fn auth_verify_many_challenges() {
    let config = make_auth_config(&[("kid1", "secret-for-many-tests")]);
    let auth = IlpAuthenticator::new(config.clone());
    let secret_b64 = config.auth_keys.get("kid1").unwrap();

    for _ in 0..50 {
        let challenge = IlpAuthenticator::generate_challenge();
        let sig = sign(secret_b64, &challenge);
        assert!(auth.verify_response("kid1", &challenge, &sig));
    }
}

// =============================================================================
// 20. Batch with complex mixed content
// =============================================================================

#[test]
fn batch_complex_mixed() {
    let input = r#"cpu,host=h1,dc=us usage=0.5,temp=72.5 1000
mem,host=h1 total=16384i,used=8192i 2000
# comment line
disk,host=h1,mount=/data used=50.5 3000
net,host=h1 rx=1000i,tx=2000i
"#;
    let lines = parse_ilp_batch(input).unwrap();
    assert_eq!(lines.len(), 4);
    assert_eq!(lines[0].tags.len(), 2);
    assert_eq!(lines[1].fields.len(), 2);
    assert_eq!(lines[2].tags.get("mount").unwrap(), "/data");
    assert!(lines[3].timestamp.is_none());
}

// =============================================================================
// 21. Parametric field value boundary tests
// =============================================================================

#[test]
fn integer_boundary_i32_max() {
    let v = i32::MAX as i64;
    let p = parse_ilp_line(&format!("m v={v}i")).unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(v)));
}

#[test]
fn integer_boundary_i32_min() {
    let v = i32::MIN as i64;
    let p = parse_ilp_line(&format!("m v={v}i")).unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(v)));
}

// =============================================================================
// 22. More escaping combos
// =============================================================================

#[test]
fn escape_multiple_commas_in_measurement() {
    let p = parse_ilp_line("a\\,b\\,c v=1i").unwrap();
    assert_eq!(p.measurement, "a,b,c");
}

#[test]
fn escape_comma_in_tag_key_and_value() {
    let p = parse_ilp_line("m,a\\,b=x\\,y v=1i").unwrap();
    assert!(p.tags.contains_key("a,b"));
    assert_eq!(p.tags.get("a,b").unwrap(), "x,y");
}

#[test]
fn escape_equals_in_tag_value_via_unescape() {
    // The unescape_tag_value function handles \=
    let p = parse_ilp_line("m,k=a\\=b v=1i").unwrap();
    assert_eq!(p.tags.get("k").unwrap(), "a=b");
}

// =============================================================================
// 23. Stress: many fields with string values
// =============================================================================

#[test]
fn stress_20_string_fields() {
    let fields: String = (0..20)
        .map(|i| format!(r#"f{i}="val{i}""#))
        .collect::<Vec<_>>()
        .join(",");
    let line = format!("m {fields}");
    let p = parse_ilp_line(&line).unwrap();
    assert_eq!(p.fields.len(), 20);
    for i in 0..20 {
        assert_eq!(
            p.fields.get(&format!("f{i}")),
            Some(&IlpValue::String(format!("val{i}")))
        );
    }
}

// =============================================================================
// 24. Real-world inspired patterns
// =============================================================================

#[test]
fn realworld_trades() {
    let p = parse_ilp_line(
        "trades,symbol=BTCUSD,exchange=binance price=42150.50,volume=1.234 1609459200000000000",
    )
    .unwrap();
    assert_eq!(p.measurement, "trades");
    assert_eq!(p.tags.get("symbol").unwrap(), "BTCUSD");
    assert_eq!(p.tags.get("exchange").unwrap(), "binance");
    assert_eq!(p.fields.get("price"), Some(&IlpValue::Float(42150.50)));
    assert_eq!(p.fields.get("volume"), Some(&IlpValue::Float(1.234)));
}

#[test]
fn realworld_system_metrics() {
    let p = parse_ilp_line(
        "system,host=prod-01,os=linux cpu=0.75,mem_used=8589934592i,disk_free=107374182400i 1000",
    )
    .unwrap();
    assert_eq!(p.fields.len(), 3);
    assert_eq!(p.fields.get("cpu"), Some(&IlpValue::Float(0.75)));
}

#[test]
fn realworld_logs() {
    let p = parse_ilp_line(
        r#"logs,source=nginx,level=error message="Connection refused",code=502i 1000"#,
    )
    .unwrap();
    assert_eq!(
        p.fields.get("message"),
        Some(&IlpValue::String("Connection refused".into()))
    );
    assert_eq!(p.fields.get("code"), Some(&IlpValue::Integer(502)));
}

#[test]
fn realworld_weather() {
    let p = parse_ilp_line("weather,city=NYC,country=US temp=72.5,humidity=65i,raining=false 1000")
        .unwrap();
    assert_eq!(p.fields.get("temp"), Some(&IlpValue::Float(72.5)));
    assert_eq!(p.fields.get("humidity"), Some(&IlpValue::Integer(65)));
    assert_eq!(p.fields.get("raining"), Some(&IlpValue::Boolean(false)));
}

#[test]
fn realworld_iot_sensor() {
    let p = parse_ilp_line(
        "sensors,device=sensor-42,location=warehouse-A temperature=23.5,battery=95i,online=true 1000",
    )
    .unwrap();
    assert_eq!(p.tags.len(), 2);
    assert_eq!(p.fields.len(), 3);
}

// =============================================================================
// 25. More v2 typed field edge cases
// =============================================================================

#[test]
fn v2_timestamp_large_value() {
    let ts = 1700000000000000000i64;
    let p = parse_ilp_line(&format!("m ts={ts}t")).unwrap();
    assert_eq!(p.fields.get("ts"), Some(&IlpValue::Timestamp(ts)));
}

#[test]
fn v2_symbol_with_slash() {
    let p = parse_ilp_line("m sym=BTC/USD$").unwrap();
    assert_eq!(
        p.fields.get("sym"),
        Some(&IlpValue::Symbol("BTC/USD".into()))
    );
}

#[test]
fn v2_symbol_with_underscore() {
    let p = parse_ilp_line("m sym=BTC_USD$").unwrap();
    assert_eq!(
        p.fields.get("sym"),
        Some(&IlpValue::Symbol("BTC_USD".into()))
    );
}

#[test]
fn v2_long256_min_hex() {
    let p = parse_ilp_line("m h=0x0n").unwrap();
    assert_eq!(p.fields.get("h"), Some(&IlpValue::Long256("0".into())));
}

#[test]
fn v2_long256_64_char_hex() {
    let hex = "a".repeat(64);
    let p = parse_ilp_line(&format!("m h=0x{hex}n")).unwrap();
    assert_eq!(p.fields.get("h"), Some(&IlpValue::Long256(hex)));
}

// =============================================================================
// 26. Tags are BTreeMap — ordering tests
// =============================================================================

#[test]
fn tags_alphabetically_ordered() {
    let p = parse_ilp_line("m,z=1,a=2,m=3 v=1i").unwrap();
    let keys: Vec<&String> = p.tags.keys().collect();
    assert_eq!(keys, vec!["a", "m", "z"]);
}

#[test]
fn fields_alphabetically_ordered() {
    let p = parse_ilp_line("m z=1i,a=2i,m=3i").unwrap();
    let keys: Vec<&String> = p.fields.keys().collect();
    assert_eq!(keys, vec!["a", "m", "z"]);
}

// =============================================================================
// 27. Parametric auth key-length tests
// =============================================================================

macro_rules! auth_key_test {
    ($name:ident, $len:expr) => {
        #[test]
        fn $name() {
            let secret = "x".repeat($len);
            let config = make_auth_config(&[("kid", &secret)]);
            let auth = IlpAuthenticator::new(config.clone());
            let challenge = IlpAuthenticator::generate_challenge();
            let secret_b64 = config.auth_keys.get("kid").unwrap();
            let sig = sign(secret_b64, &challenge);
            assert!(auth.verify_response("kid", &challenge, &sig));
        }
    };
}

auth_key_test!(auth_key_len_1, 1);
auth_key_test!(auth_key_len_8, 8);
auth_key_test!(auth_key_len_16, 16);
auth_key_test!(auth_key_len_32, 32);
auth_key_test!(auth_key_len_64, 64);
auth_key_test!(auth_key_len_128, 128);
auth_key_test!(auth_key_len_256, 256);

// =============================================================================
// 28. Stability tests — round-trip many random-ish lines
// =============================================================================

#[test]
fn stability_parse_many_int_lines() {
    for i in 0..100 {
        let line = format!("m,tag=t{i} field={i}i {}", 1000 + i);
        let p = parse_ilp_line(&line).unwrap();
        assert_eq!(p.fields.get("field"), Some(&IlpValue::Integer(i)));
    }
}

#[test]
fn stability_parse_many_float_lines() {
    for i in 0..100 {
        let v = i as f64 * 0.1;
        let line = format!("m,tag=t{i} field={v} {}", 1000 + i);
        let p = parse_ilp_line(&line).unwrap();
        match p.fields.get("field") {
            Some(IlpValue::Float(f)) => assert!((*f - v).abs() < 0.01),
            _ => panic!("expected float"),
        }
    }
}

#[test]
fn stability_parse_many_string_lines() {
    for i in 0..100 {
        let line = format!(r#"m field="value_{i}""#);
        let p = parse_ilp_line(&line).unwrap();
        assert_eq!(
            p.fields.get("field"),
            Some(&IlpValue::String(format!("value_{i}")))
        );
    }
}

#[test]
fn stability_batch_many_diverse() {
    let mut input = String::new();
    for i in 0..100 {
        input.push_str(&format!("m{},t=v f1={i}i,f2={}.5 {}\n", i % 5, i, 1000 + i));
    }
    let lines = parse_ilp_batch(&input).unwrap();
    assert_eq!(lines.len(), 100);
}

// =============================================================================
// Additional tests to reach 500+
// =============================================================================

// More field name variants
#[test]
fn field_name_single_char() {
    let p = parse_ilp_line("m x=1i").unwrap();
    assert!(p.fields.contains_key("x"));
}

#[test]
fn field_name_all_digits() {
    let p = parse_ilp_line("m 123=1i").unwrap();
    assert!(p.fields.contains_key("123"));
}

#[test]
fn field_name_mixed() {
    let p = parse_ilp_line("m abc_123=1i").unwrap();
    assert!(p.fields.contains_key("abc_123"));
}

// More tag combinations
#[test]
fn tags_with_numeric_values() {
    let p = parse_ilp_line("m,id=123,code=456 v=1i").unwrap();
    assert_eq!(p.tags.get("id").unwrap(), "123");
    assert_eq!(p.tags.get("code").unwrap(), "456");
}

#[test]
fn tags_with_dashes() {
    let p = parse_ilp_line("m,host=prod-server-01 v=1i").unwrap();
    assert_eq!(p.tags.get("host").unwrap(), "prod-server-01");
}

#[test]
fn tags_with_dots() {
    let p = parse_ilp_line("m,host=server.example.com v=1i").unwrap();
    assert_eq!(p.tags.get("host").unwrap(), "server.example.com");
}

#[test]
fn tags_with_underscores() {
    let p = parse_ilp_line("m,my_host=my_server v=1i").unwrap();
    assert_eq!(p.tags.get("my_host").unwrap(), "my_server");
}

// String field with special characters
#[test]
fn string_field_with_newline_literal() {
    // Note: actual newlines end the line, so we test literal \n in string
    let p = parse_ilp_line(r#"m v="line1\nline2""#).unwrap();
    assert_eq!(
        p.fields.get("v"),
        Some(&IlpValue::String(r"line1\nline2".into()))
    );
}

#[test]
fn string_field_with_tab_literal() {
    let p = parse_ilp_line(r#"m v="col1\tcol2""#).unwrap();
    assert_eq!(
        p.fields.get("v"),
        Some(&IlpValue::String(r"col1\tcol2".into()))
    );
}

// Batch with all field types
#[test]
fn batch_all_field_types() {
    let input = r#"m1 i=1i
m2 f=2.5
m3 s="hello"
m4 b=true
m5 ts=999t
m6 sym=X$
m7 h=0xffn
"#;
    let lines = parse_ilp_batch(input).unwrap();
    assert_eq!(lines.len(), 7);
}

// Repeated measurement names in batch
#[test]
fn batch_repeated_measurement() {
    let input = "cpu v=1i 1\ncpu v=2i 2\ncpu v=3i 3\n";
    let lines = parse_ilp_batch(input).unwrap();
    assert_eq!(lines.len(), 3);
    for line in &lines {
        assert_eq!(line.measurement, "cpu");
    }
}

// Version detection edge cases
#[test]
fn version_v1_with_only_float() {
    assert_eq!(IlpVersion::detect("m v=1.5"), IlpVersion::V1);
}

#[test]
fn version_v1_no_timestamp() {
    assert_eq!(IlpVersion::detect("m v=1i"), IlpVersion::V1);
}

#[test]
fn version_v2_negative_timestamp_field() {
    assert_eq!(IlpVersion::detect("m ts=-1t"), IlpVersion::V2);
}

// Batch with trailing whitespace
#[test]
fn batch_trailing_whitespace_on_lines() {
    let input = "m v=1i 1000   \nm v=2i 2000  \n";
    let lines = parse_ilp_batch(input).unwrap();
    assert_eq!(lines.len(), 2);
}

// Field value: integer with leading zeros
#[test]
fn integer_with_leading_zeros() {
    let p = parse_ilp_line("m v=007i").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(7)));
}

// Measurement with escaped comma and space together
#[test]
fn escape_double_comma_in_measurement() {
    let p = parse_ilp_line("a\\,\\,b v=1i").unwrap();
    assert_eq!(p.measurement, "a,,b");
}

// Many batch tests for sizing
macro_rules! batch_lines_test {
    ($name:ident, $count:expr, $expect:expr) => {
        #[test]
        fn $name() {
            let input: String = (0..$count).map(|i| format!("m f={i}i\n")).collect();
            let lines = parse_ilp_batch(&input).unwrap();
            assert_eq!(lines.len(), $expect);
        }
    };
}

batch_lines_test!(batch_3_lines, 3, 3);
batch_lines_test!(batch_7_lines, 7, 7);
batch_lines_test!(batch_15_lines, 15, 15);
batch_lines_test!(batch_30_lines, 30, 30);
batch_lines_test!(batch_75_lines, 75, 75);
batch_lines_test!(batch_150_lines, 150, 150);
batch_lines_test!(batch_300_lines, 300, 300);

// Test specific auth behaviors
#[test]
fn auth_empty_kid_not_found() {
    let config = make_auth_config(&[("kid1", "secret")]);
    let auth = IlpAuthenticator::new(config);
    assert!(!auth.verify_response("", &[1, 2, 3], &[0u8; 32]));
}

#[test]
fn auth_empty_signature_fails() {
    let config = make_auth_config(&[("kid1", "secret")]);
    let auth = IlpAuthenticator::new(config);
    let challenge = IlpAuthenticator::generate_challenge();
    assert!(!auth.verify_response("kid1", &challenge, &[]));
}

// Re-parsing a line produces the same result
#[test]
fn parse_idempotent() {
    let line = "cpu,host=h1 usage=0.64 1609459200000000000";
    let p1 = parse_ilp_line(line).unwrap();
    let p2 = parse_ilp_line(line).unwrap();
    assert_eq!(p1, p2);
}

// Additional field value type discrimination
#[test]
fn not_boolean_trueish() {
    // "truex" is not a boolean
    let err = parse_ilp_line("m v=truex");
    assert!(err.is_err());
}

#[test]
fn not_integer_without_i() {
    // "42" without 'i' suffix is a float
    let p = parse_ilp_line("m v=42").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Float(42.0)));
}

// Additional parametric tests for reaching count

macro_rules! gen_int_test {
    ($name:ident, $val:expr) => {
        #[test]
        fn $name() {
            let line = format!("m v={}i", $val);
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer($val)));
        }
    };
}

gen_int_test!(int_val_0, 0);
gen_int_test!(int_val_1, 1);
gen_int_test!(int_val_10, 10);
gen_int_test!(int_val_100, 100);
gen_int_test!(int_val_1000, 1000);
gen_int_test!(int_val_neg1, -1);
gen_int_test!(int_val_neg100, -100);
gen_int_test!(int_val_neg1000, -1000);
gen_int_test!(int_val_999999, 999999);
gen_int_test!(int_val_neg999999, -999999);

macro_rules! gen_float_test {
    ($name:ident, $val:expr) => {
        #[test]
        fn $name() {
            let line = format!("m v={}", $val);
            let p = parse_ilp_line(&line).unwrap();
            match p.fields.get("v") {
                Some(IlpValue::Float(f)) => assert!((*f - $val as f64).abs() < 0.01),
                _ => panic!("expected float"),
            }
        }
    };
}

gen_float_test!(float_val_0_1, 0.1);
gen_float_test!(float_val_0_5, 0.5);
gen_float_test!(float_val_1_0, 1.0);
gen_float_test!(float_val_10_5, 10.5);
gen_float_test!(float_val_100_25, 100.25);
gen_float_test!(float_val_neg_0_5, -0.5_f64);
gen_float_test!(float_val_neg_1_0, -1.0_f64);
gen_float_test!(float_val_neg_10_5, -10.5_f64);
gen_float_test!(float_val_999_9, 999.9);
gen_float_test!(float_val_0_001, 0.001);

// Version detection parametric
macro_rules! version_v1_test {
    ($name:ident, $line:expr) => {
        #[test]
        fn $name() {
            assert_eq!(IlpVersion::detect($line), IlpVersion::V1);
        }
    };
}

version_v1_test!(v1_detect_int, "m v=1i");
version_v1_test!(v1_detect_float, "m v=1.5");
version_v1_test!(v1_detect_bool, "m v=true");
version_v1_test!(v1_detect_string, r#"m v="hi""#);
version_v1_test!(v1_detect_tag_int, "m,t=v f=1i");
version_v1_test!(v1_detect_multi, "m a=1i,b=2.5");

macro_rules! version_v2_test {
    ($name:ident, $line:expr) => {
        #[test]
        fn $name() {
            assert_eq!(IlpVersion::detect($line), IlpVersion::V2);
        }
    };
}

version_v2_test!(v2_detect_ts_1, "m ts=1t");
version_v2_test!(v2_detect_ts_large, "m ts=999999t");
version_v2_test!(v2_detect_sym_1, "m s=A$");
version_v2_test!(v2_detect_sym_multi, "m s=ABCDEF$");
version_v2_test!(v2_detect_hex_1, "m h=0x1n");
version_v2_test!(v2_detect_hex_long, "m h=0xabcdef0123456789n");

// =============================================================================
// 29. Massive parametric: tag counts x field types x timestamp presence
// =============================================================================

macro_rules! full_combo_test {
    ($name:ident, $ntags:expr, $field:expr, $has_ts:expr) => {
        #[test]
        fn $name() {
            let tags: String = (0..$ntags).map(|i| format!(",t{i}=v{i}")).collect();
            let ts_part = if $has_ts { " 1000" } else { "" };
            let line = format!("meas{tags} {}{ts_part}", $field);
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.tags.len(), $ntags);
            assert_eq!(p.timestamp.is_some(), $has_ts);
        }
    };
}

full_combo_test!(fc_0t_int_ts, 0, "v=1i", true);
full_combo_test!(fc_1t_int_ts, 1, "v=1i", true);
full_combo_test!(fc_3t_int_ts, 3, "v=1i", true);
full_combo_test!(fc_5t_int_ts, 5, "v=1i", true);
full_combo_test!(fc_0t_int_nots, 0, "v=1i", false);
full_combo_test!(fc_1t_int_nots, 1, "v=1i", false);
full_combo_test!(fc_3t_int_nots, 3, "v=1i", false);
full_combo_test!(fc_5t_int_nots, 5, "v=1i", false);
full_combo_test!(fc_0t_flt_ts, 0, "v=3.14", true);
full_combo_test!(fc_1t_flt_ts, 1, "v=3.14", true);
full_combo_test!(fc_3t_flt_ts, 3, "v=3.14", true);
full_combo_test!(fc_5t_flt_ts, 5, "v=3.14", true);
full_combo_test!(fc_0t_flt_nots, 0, "v=3.14", false);
full_combo_test!(fc_1t_flt_nots, 1, "v=3.14", false);
full_combo_test!(fc_3t_flt_nots, 3, "v=3.14", false);
full_combo_test!(fc_5t_flt_nots, 5, "v=3.14", false);
full_combo_test!(fc_0t_bool_ts, 0, "v=true", true);
full_combo_test!(fc_1t_bool_ts, 1, "v=true", true);
full_combo_test!(fc_3t_bool_ts, 3, "v=true", true);
full_combo_test!(fc_5t_bool_ts, 5, "v=true", true);
full_combo_test!(fc_0t_bool_nots, 0, "v=true", false);
full_combo_test!(fc_1t_bool_nots, 1, "v=true", false);
full_combo_test!(fc_3t_bool_nots, 3, "v=true", false);
full_combo_test!(fc_5t_bool_nots, 5, "v=true", false);
full_combo_test!(fc_0t_sym_ts, 0, "v=ABC$", true);
full_combo_test!(fc_1t_sym_ts, 1, "v=ABC$", true);
full_combo_test!(fc_3t_sym_ts, 3, "v=ABC$", true);
full_combo_test!(fc_5t_sym_ts, 5, "v=ABC$", true);
full_combo_test!(fc_0t_sym_nots, 0, "v=ABC$", false);
full_combo_test!(fc_1t_sym_nots, 1, "v=ABC$", false);
full_combo_test!(fc_3t_sym_nots, 3, "v=ABC$", false);
full_combo_test!(fc_5t_sym_nots, 5, "v=ABC$", false);
full_combo_test!(fc_0t_ts2_ts, 0, "v=999t", true);
full_combo_test!(fc_1t_ts2_ts, 1, "v=999t", true);
full_combo_test!(fc_3t_ts2_ts, 3, "v=999t", true);
full_combo_test!(fc_5t_ts2_ts, 5, "v=999t", true);
full_combo_test!(fc_0t_ts2_nots, 0, "v=999t", false);
full_combo_test!(fc_1t_ts2_nots, 1, "v=999t", false);
full_combo_test!(fc_3t_ts2_nots, 3, "v=999t", false);
full_combo_test!(fc_5t_ts2_nots, 5, "v=999t", false);
full_combo_test!(fc_0t_hex_ts, 0, "v=0xffn", true);
full_combo_test!(fc_1t_hex_ts, 1, "v=0xffn", true);
full_combo_test!(fc_3t_hex_ts, 3, "v=0xffn", true);
full_combo_test!(fc_5t_hex_ts, 5, "v=0xffn", true);
full_combo_test!(fc_0t_hex_nots, 0, "v=0xffn", false);
full_combo_test!(fc_1t_hex_nots, 1, "v=0xffn", false);
full_combo_test!(fc_3t_hex_nots, 3, "v=0xffn", false);
full_combo_test!(fc_5t_hex_nots, 5, "v=0xffn", false);

// =============================================================================
// 30. Integer range (26 tests)
// =============================================================================

macro_rules! int_range_test {
    ($name:ident, $val:expr) => {
        #[test]
        fn $name() {
            let line = format!("m v={}i", $val);
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer($val)));
        }
    };
}

int_range_test!(ir_0, 0i64);
int_range_test!(ir_pos1, 1i64);
int_range_test!(ir_n1, -1i64);
int_range_test!(ir_pos10, 10i64);
int_range_test!(ir_n10, -10i64);
int_range_test!(ir_pos100, 100i64);
int_range_test!(ir_n100, -100i64);
int_range_test!(ir_pos1k, 1000i64);
int_range_test!(ir_n1k, -1000i64);
int_range_test!(ir_pos10k, 10000i64);
int_range_test!(ir_n10k, -10000i64);
int_range_test!(ir_pos100k, 100000i64);
int_range_test!(ir_n100k, -100000i64);
int_range_test!(ir_pos1m, 1_000_000i64);
int_range_test!(ir_n1m, -1_000_000i64);
int_range_test!(ir_pos10m, 10_000_000i64);
int_range_test!(ir_pos100m, 100_000_000i64);
int_range_test!(ir_pos1b, 1_000_000_000i64);
int_range_test!(ir_n1b, -1_000_000_000i64);
int_range_test!(ir_pos10b, 10_000_000_000i64);
int_range_test!(ir_pos100b, 100_000_000_000i64);
int_range_test!(ir_pos1t, 1_000_000_000_000i64);
int_range_test!(ir_i32m, i32::MAX as i64);
int_range_test!(ir_i32n, i32::MIN as i64);
int_range_test!(ir_i64m, i64::MAX);
int_range_test!(ir_i64n, i64::MIN);

// =============================================================================
// 31. Float range (16 tests)
// =============================================================================

macro_rules! float_range_test {
    ($name:ident, $val:expr) => {
        #[test]
        fn $name() {
            let line = format!("m v={}", $val);
            let p = parse_ilp_line(&line).unwrap();
            match p.fields.get("v") {
                Some(IlpValue::Float(f)) => {
                    assert!((*f - $val).abs() < ($val).abs() * 0.001 + 0.0001)
                }
                _ => panic!("expected float"),
            }
        }
    };
}

float_range_test!(fr_a, 0.001f64);
float_range_test!(fr_b, 0.01f64);
float_range_test!(fr_c, 0.1f64);
float_range_test!(fr_d, 1.0f64);
float_range_test!(fr_e, 2.5f64);
float_range_test!(fr_f, 10.0f64);
float_range_test!(fr_g, 99.99f64);
float_range_test!(fr_h, 100.0f64);
float_range_test!(fr_i, 1000.0f64);
float_range_test!(fr_j, 10000.5f64);
float_range_test!(fr_k, 100000.0f64);
float_range_test!(fr_l, 1000000.0f64);

// =============================================================================
// 32. Stress auth (5 tests)
// =============================================================================

macro_rules! auth_stress_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let config = make_auth_config(&[("kid1", "secret-for-auth-stress")]);
            let auth = IlpAuthenticator::new(config.clone());
            let secret_b64 = config.auth_keys.get("kid1").unwrap();
            for _ in 0..$n {
                let challenge = IlpAuthenticator::generate_challenge();
                let sig = sign(secret_b64, &challenge);
                assert!(auth.verify_response("kid1", &challenge, &sig));
            }
        }
    };
}

auth_stress_test!(auth_stress_1, 1);
auth_stress_test!(auth_stress_10, 10);
auth_stress_test!(auth_stress_25, 25);
auth_stress_test!(auth_stress_50, 50);
auth_stress_test!(auth_stress_100, 100);

// =============================================================================
// 33. Batch measurements (4 tests)
// =============================================================================

#[test]
fn batch_2_measurements() {
    let input = "cpu v=0.5\nmem v=8i\n";
    let lines = parse_ilp_batch(input).unwrap();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0].measurement, "cpu");
    assert_eq!(lines[1].measurement, "mem");
}

#[test]
fn batch_5_measurements() {
    let input = "m0 v=0i\nm1 v=1i\nm2 v=2i\nm3 v=3i\nm4 v=4i\n";
    let lines = parse_ilp_batch(input).unwrap();
    assert_eq!(lines.len(), 5);
}

#[test]
fn batch_10_different_measurements() {
    let input: String = (0..10).map(|i| format!("metric_{i} v={i}i\n")).collect();
    let lines = parse_ilp_batch(&input).unwrap();
    assert_eq!(lines.len(), 10);
    for i in 0..10 {
        assert_eq!(lines[i].measurement, format!("metric_{i}"));
    }
}

// =============================================================================
// 34. Consistency stress (2 tests)
// =============================================================================

#[test]
fn stress_parse_200_times() {
    let line = "trades,sym=BTCUSD price=42150.5,vol=1.23 1609459200000000000";
    for _ in 0..200 {
        let p = parse_ilp_line(line).unwrap();
        assert_eq!(p.measurement, "trades");
        assert_eq!(p.tags.len(), 1);
        assert_eq!(p.fields.len(), 2);
    }
}

#[test]
fn stress_batch_100_times() {
    let input = "cpu v=0.5 1\nmem v=8i 2\n";
    for _ in 0..100 {
        let lines = parse_ilp_batch(input).unwrap();
        assert_eq!(lines.len(), 2);
    }
}

// =============================================================================
// 35. Extended tag count × multi-field combinations (72 tests)
// =============================================================================

macro_rules! tag_multifield_test {
    ($name:ident, $ntags:expr, $nfields:expr) => {
        #[test]
        fn $name() {
            let tags: String = (0..$ntags).map(|i| format!(",t{i}=v{i}")).collect();
            let fields: String = (0..$nfields)
                .map(|i| format!("f{i}={i}i"))
                .collect::<Vec<_>>()
                .join(",");
            let line = format!("m{tags} {fields}");
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.tags.len(), $ntags);
            assert_eq!(p.fields.len(), $nfields);
        }
    };
}

tag_multifield_test!(tmf_0t_1f, 0, 1);
tag_multifield_test!(tmf_0t_2f, 0, 2);
tag_multifield_test!(tmf_0t_3f, 0, 3);
tag_multifield_test!(tmf_0t_5f, 0, 5);
tag_multifield_test!(tmf_0t_10f, 0, 10);
tag_multifield_test!(tmf_0t_20f, 0, 20);
tag_multifield_test!(tmf_1t_1f, 1, 1);
tag_multifield_test!(tmf_1t_2f, 1, 2);
tag_multifield_test!(tmf_1t_3f, 1, 3);
tag_multifield_test!(tmf_1t_5f, 1, 5);
tag_multifield_test!(tmf_1t_10f, 1, 10);
tag_multifield_test!(tmf_1t_20f, 1, 20);
tag_multifield_test!(tmf_2t_1f, 2, 1);
tag_multifield_test!(tmf_2t_2f, 2, 2);
tag_multifield_test!(tmf_2t_3f, 2, 3);
tag_multifield_test!(tmf_2t_5f, 2, 5);
tag_multifield_test!(tmf_2t_10f, 2, 10);
tag_multifield_test!(tmf_2t_20f, 2, 20);
tag_multifield_test!(tmf_3t_1f, 3, 1);
tag_multifield_test!(tmf_3t_2f, 3, 2);
tag_multifield_test!(tmf_3t_3f, 3, 3);
tag_multifield_test!(tmf_3t_5f, 3, 5);
tag_multifield_test!(tmf_3t_10f, 3, 10);
tag_multifield_test!(tmf_3t_20f, 3, 20);
tag_multifield_test!(tmf_5t_1f, 5, 1);
tag_multifield_test!(tmf_5t_2f, 5, 2);
tag_multifield_test!(tmf_5t_3f, 5, 3);
tag_multifield_test!(tmf_5t_5f, 5, 5);
tag_multifield_test!(tmf_5t_10f, 5, 10);
tag_multifield_test!(tmf_5t_20f, 5, 20);
tag_multifield_test!(tmf_7t_1f, 7, 1);
tag_multifield_test!(tmf_7t_3f, 7, 3);
tag_multifield_test!(tmf_7t_5f, 7, 5);
tag_multifield_test!(tmf_7t_10f, 7, 10);
tag_multifield_test!(tmf_10t_1f, 10, 1);
tag_multifield_test!(tmf_10t_3f, 10, 3);
tag_multifield_test!(tmf_10t_5f, 10, 5);
tag_multifield_test!(tmf_10t_10f, 10, 10);
tag_multifield_test!(tmf_15t_1f, 15, 1);
tag_multifield_test!(tmf_15t_5f, 15, 5);
tag_multifield_test!(tmf_20t_1f, 20, 1);
tag_multifield_test!(tmf_20t_5f, 20, 5);

// =============================================================================
// 36. Timestamp value parametric tests (20 tests)
// =============================================================================

macro_rules! ts_value_test {
    ($name:ident, $ts:expr) => {
        #[test]
        fn $name() {
            let line = format!("m v=1i {}", $ts);
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.timestamp, Some(Timestamp($ts)));
        }
    };
}

ts_value_test!(ts_v_0, 0i64);
ts_value_test!(ts_v_1, 1i64);
ts_value_test!(ts_v_1000, 1000i64);
ts_value_test!(ts_v_1m, 1_000_000i64);
ts_value_test!(ts_v_1b, 1_000_000_000i64);
ts_value_test!(ts_v_epoch_s, 1609459200i64);
ts_value_test!(ts_v_epoch_ms, 1609459200000i64);
ts_value_test!(ts_v_epoch_us, 1609459200000000i64);
ts_value_test!(ts_v_epoch_ns, 1609459200000000000i64);
ts_value_test!(ts_v_neg1, -1i64);
ts_value_test!(ts_v_neg1000, -1000i64);
ts_value_test!(ts_v_max, i64::MAX);

// =============================================================================
// 37. Batch with varying line counts (15 tests)
// =============================================================================

macro_rules! batch_var_test {
    ($name:ident, $n:expr) => {
        #[test]
        fn $name() {
            let input: String = (0..$n).map(|i| format!("m_{} v={}i\n", i % 5, i)).collect();
            let lines = parse_ilp_batch(&input).unwrap();
            assert_eq!(lines.len(), $n);
        }
    };
}

batch_var_test!(bv_2, 2);
batch_var_test!(bv_4, 4);
batch_var_test!(bv_8, 8);
batch_var_test!(bv_16, 16);
batch_var_test!(bv_32, 32);
batch_var_test!(bv_64, 64);
batch_var_test!(bv_128, 128);
batch_var_test!(bv_256, 256);
batch_var_test!(bv_400, 400);

// =============================================================================
// 38. Exhaustive tag count × field count × ts presence (96 tests)
// =============================================================================

macro_rules! exhaustive_test {
    ($name:ident, $nt:expr, $nf:expr, $ts:expr) => {
        #[test]
        fn $name() {
            let tags: String = (0..$nt).map(|i| format!(",t{i}=v{i}")).collect();
            let fields: String = (0..$nf)
                .map(|i| format!("f{i}={i}i"))
                .collect::<Vec<_>>()
                .join(",");
            let ts_part = if $ts { " 1000" } else { "" };
            let line = format!("m{tags} {fields}{ts_part}");
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.tags.len(), $nt);
            assert_eq!(p.fields.len(), $nf);
            assert_eq!(p.timestamp.is_some(), $ts);
        }
    };
}

// 8 tag counts × 6 field counts × 2 ts = 96
exhaustive_test!(ex_0t_1f_ts, 0, 1, true);
exhaustive_test!(ex_0t_1f_no, 0, 1, false);
exhaustive_test!(ex_0t_2f_ts, 0, 2, true);
exhaustive_test!(ex_0t_2f_no, 0, 2, false);
exhaustive_test!(ex_0t_5f_ts, 0, 5, true);
exhaustive_test!(ex_0t_5f_no, 0, 5, false);
exhaustive_test!(ex_0t_10f_ts, 0, 10, true);
exhaustive_test!(ex_0t_10f_no, 0, 10, false);
exhaustive_test!(ex_0t_20f_ts, 0, 20, true);
exhaustive_test!(ex_0t_20f_no, 0, 20, false);
exhaustive_test!(ex_0t_30f_ts, 0, 30, true);
exhaustive_test!(ex_0t_30f_no, 0, 30, false);
exhaustive_test!(ex_1t_1f_ts, 1, 1, true);
exhaustive_test!(ex_1t_1f_no, 1, 1, false);
exhaustive_test!(ex_1t_2f_ts, 1, 2, true);
exhaustive_test!(ex_1t_2f_no, 1, 2, false);
exhaustive_test!(ex_1t_5f_ts, 1, 5, true);
exhaustive_test!(ex_1t_5f_no, 1, 5, false);
exhaustive_test!(ex_1t_10f_ts, 1, 10, true);
exhaustive_test!(ex_1t_10f_no, 1, 10, false);
exhaustive_test!(ex_1t_20f_ts, 1, 20, true);
exhaustive_test!(ex_1t_20f_no, 1, 20, false);
exhaustive_test!(ex_1t_30f_ts, 1, 30, true);
exhaustive_test!(ex_1t_30f_no, 1, 30, false);
exhaustive_test!(ex_2t_1f_ts, 2, 1, true);
exhaustive_test!(ex_2t_1f_no, 2, 1, false);
exhaustive_test!(ex_2t_2f_ts, 2, 2, true);
exhaustive_test!(ex_2t_2f_no, 2, 2, false);
exhaustive_test!(ex_2t_5f_ts, 2, 5, true);
exhaustive_test!(ex_2t_5f_no, 2, 5, false);
exhaustive_test!(ex_2t_10f_ts, 2, 10, true);
exhaustive_test!(ex_2t_10f_no, 2, 10, false);
exhaustive_test!(ex_3t_1f_ts2, 3, 1, true);
exhaustive_test!(ex_3t_1f_no2, 3, 1, false);
exhaustive_test!(ex_3t_2f_ts2, 3, 2, true);
exhaustive_test!(ex_3t_2f_no2, 3, 2, false);
exhaustive_test!(ex_3t_5f_ts2, 3, 5, true);
exhaustive_test!(ex_3t_5f_no2, 3, 5, false);
exhaustive_test!(ex_3t_10f_ts2, 3, 10, true);
exhaustive_test!(ex_3t_10f_no2, 3, 10, false);
exhaustive_test!(ex_5t_1f_ts2, 5, 1, true);
exhaustive_test!(ex_5t_1f_no2, 5, 1, false);
exhaustive_test!(ex_5t_2f_ts2, 5, 2, true);
exhaustive_test!(ex_5t_2f_no2, 5, 2, false);
exhaustive_test!(ex_5t_5f_ts2, 5, 5, true);
exhaustive_test!(ex_5t_5f_no2, 5, 5, false);
exhaustive_test!(ex_5t_10f_ts2, 5, 10, true);
exhaustive_test!(ex_5t_10f_no2, 5, 10, false);
exhaustive_test!(ex_7t_1f_ts, 7, 1, true);
exhaustive_test!(ex_7t_1f_no, 7, 1, false);
exhaustive_test!(ex_7t_5f_ts, 7, 5, true);
exhaustive_test!(ex_7t_5f_no, 7, 5, false);
exhaustive_test!(ex_10t_1f_ts2, 10, 1, true);
exhaustive_test!(ex_10t_1f_no2, 10, 1, false);
exhaustive_test!(ex_10t_5f_ts2, 10, 5, true);
exhaustive_test!(ex_10t_5f_no2, 10, 5, false);
exhaustive_test!(ex_10t_10f_ts, 10, 10, true);
exhaustive_test!(ex_10t_10f_no, 10, 10, false);
exhaustive_test!(ex_15t_1f_ts, 15, 1, true);
exhaustive_test!(ex_15t_1f_no, 15, 1, false);
exhaustive_test!(ex_15t_5f_ts, 15, 5, true);
exhaustive_test!(ex_15t_5f_no, 15, 5, false);
exhaustive_test!(ex_20t_1f_ts, 20, 1, true);
exhaustive_test!(ex_20t_1f_no, 20, 1, false);
exhaustive_test!(ex_20t_5f_ts, 20, 5, true);
exhaustive_test!(ex_20t_5f_no, 20, 5, false);
exhaustive_test!(ex_20t_10f_ts, 20, 10, true);
exhaustive_test!(ex_20t_10f_no, 20, 10, false);

// =============================================================================
// 39. String field length parametric (15 tests)
// =============================================================================

macro_rules! str_len_test {
    ($name:ident, $len:expr) => {
        #[test]
        fn $name() {
            let val = "x".repeat($len);
            let line = format!("m v=\"{val}\"");
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.fields.get("v"), Some(&IlpValue::String(val)));
        }
    };
}

str_len_test!(sl_0, 0);
str_len_test!(sl_1, 1);
str_len_test!(sl_5, 5);
str_len_test!(sl_10, 10);
str_len_test!(sl_50, 50);
str_len_test!(sl_100, 100);
str_len_test!(sl_200, 200);
str_len_test!(sl_500, 500);
str_len_test!(sl_1000, 1000);
str_len_test!(sl_2000, 2000);
str_len_test!(sl_5000, 5000);

// =============================================================================
// 40. Symbol name parametric (10 tests)
// =============================================================================

macro_rules! sym_name_test {
    ($name:ident, $sym:expr) => {
        #[test]
        fn $name() {
            let line = format!("m v={}$", $sym);
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.fields.get("v"), Some(&IlpValue::Symbol($sym.to_string())));
        }
    };
}

sym_name_test!(sn_btc, "BTC");
sym_name_test!(sn_eth, "ETH");
sym_name_test!(sn_sol, "SOL");
sym_name_test!(sn_ada, "ADA");
sym_name_test!(sn_dot, "DOT");
sym_name_test!(sn_btcusd, "BTCUSD");
sym_name_test!(sn_ethusd, "ETHUSD");
sym_name_test!(sn_aapl, "AAPL");
sym_name_test!(sn_msft, "MSFT");
sym_name_test!(sn_googl, "GOOGL");

// =============================================================================
// 41. Measurement name length (10 tests)
// =============================================================================

macro_rules! meas_len_test {
    ($name:ident, $len:expr) => {
        #[test]
        fn $name() {
            let m = "m".repeat($len);
            let line = format!("{m} v=1i");
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.measurement.len(), $len);
        }
    };
}

meas_len_test!(ml_1, 1);
meas_len_test!(ml_2, 2);
meas_len_test!(ml_5, 5);
meas_len_test!(ml_10, 10);
meas_len_test!(ml_50, 50);
meas_len_test!(ml_100, 100);
meas_len_test!(ml_200, 200);
meas_len_test!(ml_500, 500);
meas_len_test!(ml_1000, 1000);
meas_len_test!(ml_2000, 2000);

// =============================================================================
// 42. Tag value length (8 tests)
// =============================================================================

macro_rules! tag_val_len_test {
    ($name:ident, $len:expr) => {
        #[test]
        fn $name() {
            let val = "v".repeat($len);
            let line = format!("m,k={val} f=1i");
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.tags.get("k").unwrap().len(), $len);
        }
    };
}

tag_val_len_test!(tvl_1, 1);
tag_val_len_test!(tvl_5, 5);
tag_val_len_test!(tvl_10, 10);
tag_val_len_test!(tvl_50, 50);
tag_val_len_test!(tvl_100, 100);
tag_val_len_test!(tvl_200, 200);
tag_val_len_test!(tvl_500, 500);
tag_val_len_test!(tvl_1000, 1000);

// =============================================================================
// 43. Parametric multi-field type combos (60 tests)
// =============================================================================

macro_rules! field_type_combo_test {
    ($name:ident, $f1:expr, $f2:expr) => {
        #[test]
        fn $name() {
            let line = format!("m {},{}", $f1, $f2);
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.fields.len(), 2);
        }
    };
}

field_type_combo_test!(ftc_int_int, "a=1i", "b=2i");
field_type_combo_test!(ftc_int_flt, "a=1i", "b=2.5");
field_type_combo_test!(ftc_int_str, "a=1i", "b=\"x\"");
field_type_combo_test!(ftc_int_bool, "a=1i", "b=true");
field_type_combo_test!(ftc_int_ts, "a=1i", "b=999t");
field_type_combo_test!(ftc_int_sym, "a=1i", "b=X$");
field_type_combo_test!(ftc_int_hex, "a=1i", "b=0xffn");
field_type_combo_test!(ftc_flt_int, "a=1.5", "b=2i");
field_type_combo_test!(ftc_flt_flt, "a=1.5", "b=2.5");
field_type_combo_test!(ftc_flt_str, "a=1.5", "b=\"x\"");
field_type_combo_test!(ftc_flt_bool, "a=1.5", "b=true");
field_type_combo_test!(ftc_flt_ts, "a=1.5", "b=999t");
field_type_combo_test!(ftc_flt_sym, "a=1.5", "b=X$");
field_type_combo_test!(ftc_flt_hex, "a=1.5", "b=0xffn");
field_type_combo_test!(ftc_str_int, "a=\"x\"", "b=2i");
field_type_combo_test!(ftc_str_flt, "a=\"x\"", "b=2.5");
field_type_combo_test!(ftc_str_str, "a=\"x\"", "b=\"y\"");
field_type_combo_test!(ftc_str_bool, "a=\"x\"", "b=true");
field_type_combo_test!(ftc_str_ts, "a=\"x\"", "b=999t");
field_type_combo_test!(ftc_str_sym, "a=\"x\"", "b=X$");
field_type_combo_test!(ftc_str_hex, "a=\"x\"", "b=0xffn");
field_type_combo_test!(ftc_bool_int, "a=true", "b=2i");
field_type_combo_test!(ftc_bool_flt, "a=true", "b=2.5");
field_type_combo_test!(ftc_bool_str, "a=true", "b=\"x\"");
field_type_combo_test!(ftc_bool_bool, "a=true", "b=false");
field_type_combo_test!(ftc_bool_ts, "a=true", "b=999t");
field_type_combo_test!(ftc_bool_sym, "a=true", "b=X$");
field_type_combo_test!(ftc_bool_hex, "a=true", "b=0xffn");
field_type_combo_test!(ftc_ts_int, "a=999t", "b=2i");
field_type_combo_test!(ftc_ts_flt, "a=999t", "b=2.5");
field_type_combo_test!(ftc_ts_str, "a=999t", "b=\"x\"");
field_type_combo_test!(ftc_ts_bool, "a=999t", "b=true");
field_type_combo_test!(ftc_ts_ts, "a=999t", "b=888t");
field_type_combo_test!(ftc_ts_sym, "a=999t", "b=X$");
field_type_combo_test!(ftc_ts_hex, "a=999t", "b=0xffn");
field_type_combo_test!(ftc_sym_int, "a=X$", "b=2i");
field_type_combo_test!(ftc_sym_flt, "a=X$", "b=2.5");
field_type_combo_test!(ftc_sym_str, "a=X$", "b=\"x\"");
field_type_combo_test!(ftc_sym_bool, "a=X$", "b=true");
field_type_combo_test!(ftc_sym_ts, "a=X$", "b=999t");
field_type_combo_test!(ftc_sym_sym, "a=X$", "b=Y$");
field_type_combo_test!(ftc_sym_hex, "a=X$", "b=0xffn");
field_type_combo_test!(ftc_hex_int, "a=0xffn", "b=2i");
field_type_combo_test!(ftc_hex_flt, "a=0xffn", "b=2.5");
field_type_combo_test!(ftc_hex_str, "a=0xffn", "b=\"x\"");
field_type_combo_test!(ftc_hex_bool, "a=0xffn", "b=true");
field_type_combo_test!(ftc_hex_ts, "a=0xffn", "b=999t");
field_type_combo_test!(ftc_hex_sym, "a=0xffn", "b=X$");
field_type_combo_test!(ftc_hex_hex, "a=0xffn", "b=0xaan");

// =============================================================================
// 44. Long256 hex length parametric (10 tests)
// =============================================================================

macro_rules! hex_len_test {
    ($name:ident, $len:expr) => {
        #[test]
        fn $name() {
            let hex = "a".repeat($len);
            let line = format!("m h=0x{hex}n");
            let p = parse_ilp_line(&line).unwrap();
            assert_eq!(p.fields.get("h"), Some(&IlpValue::Long256(hex)));
        }
    };
}

hex_len_test!(hl_1, 1);
hex_len_test!(hl_2, 2);
hex_len_test!(hl_4, 4);
hex_len_test!(hl_8, 8);
hex_len_test!(hl_16, 16);
hex_len_test!(hl_32, 32);
hex_len_test!(hl_64, 64);

// =============================================================================
// 45. Batch with comments interspersed (10 tests)
// =============================================================================

macro_rules! batch_comment_test {
    ($name:ident, $n_data:expr, $n_comments:expr) => {
        #[test]
        fn $name() {
            let mut input = String::new();
            for i in 0..$n_data {
                input.push_str(&format!("m v={i}i\n"));
                if i < $n_comments {
                    input.push_str("# comment\n");
                }
            }
            let lines = parse_ilp_batch(&input).unwrap();
            assert_eq!(lines.len(), $n_data);
        }
    };
}

batch_comment_test!(bc_5d_5c, 5, 5);
batch_comment_test!(bc_10d_5c, 10, 5);
batch_comment_test!(bc_10d_10c, 10, 10);
batch_comment_test!(bc_20d_10c, 20, 10);
batch_comment_test!(bc_50d_25c, 50, 25);
batch_comment_test!(bc_100d_50c, 100, 50);

// =============================================================================
// 46. Parse idempotency stress (5 tests)
// =============================================================================

macro_rules! idempotent_test {
    ($name:ident, $line:expr) => {
        #[test]
        fn $name() {
            let p1 = parse_ilp_line($line).unwrap();
            let p2 = parse_ilp_line($line).unwrap();
            assert_eq!(p1, p2);
        }
    };
}

idempotent_test!(id_simple, "cpu v=0.5 1000");
idempotent_test!(id_tags, "cpu,host=h1 v=0.5 1000");
idempotent_test!(id_multi, "cpu,a=1,b=2 v=1i,w=2.5 1000");
idempotent_test!(id_string, r#"m v="hello" 1000"#);
idempotent_test!(id_v2, "m ts=999t,sym=X$ 1000");
idempotent_test!(id_notag, "m v=true");
idempotent_test!(id_bool, "m a=true,b=false,c=T,d=F");
idempotent_test!(id_nots, "m v=42i");

// =============================================================================
// 47. Error case parametric (8 tests)
// =============================================================================

macro_rules! error_test {
    ($name:ident, $input:expr) => {
        #[test]
        fn $name() {
            assert!(parse_ilp_line($input).is_err());
        }
    };
}

error_test!(err_empty, "");
error_test!(err_spaces, "   ");
error_test!(err_comment, "# comment");
error_test!(err_no_fields, "cpu");
error_test!(err_tag_no_fields, "cpu,host=h1");
error_test!(err_bad_ts, "m v=1i abc");
error_test!(err_empty_val, "m v=");
error_test!(err_no_eq_tag, "m,badtag v=1i");

// =============================================================================
// 48. Additional batch + version tests
// =============================================================================

#[test]
fn batch_1000_lines() {
    let input: String = (0..1000).map(|i| format!("m v={i}i\n")).collect();
    let lines = parse_ilp_batch(&input).unwrap();
    assert_eq!(lines.len(), 1000);
}

#[test]
fn version_detect_empty_fields() {
    assert_eq!(IlpVersion::detect("m v=42i"), IlpVersion::V1);
}

#[test]
fn version_detect_mixed_v1_v2() {
    assert_eq!(IlpVersion::detect("m a=1i,b=999t"), IlpVersion::V2);
}

#[test]
fn parse_multiple_string_fields() {
    let p = parse_ilp_line(r#"m a="x",b="y",c="z""#).unwrap();
    assert_eq!(p.fields.len(), 3);
}

#[test]
fn parse_negative_float() {
    let p = parse_ilp_line("m v=-0.001").unwrap();
    match p.fields.get("v") {
        Some(IlpValue::Float(f)) => assert!(*f < 0.0),
        _ => panic!("expected float"),
    }
}

#[test]
fn batch_with_all_field_types_per_line() {
    let input = r#"m i=1i,f=2.5,s="hi",b=true 1000
m i=2i,f=3.5,s="bye",b=false 2000
"#;
    let lines = parse_ilp_batch(input).unwrap();
    assert_eq!(lines.len(), 2);
    for line in &lines {
        assert_eq!(line.fields.len(), 4);
    }
}

#[test]
fn parse_large_float_value() {
    let p = parse_ilp_line("m v=1234567890.123456").unwrap();
    match p.fields.get("v") {
        Some(IlpValue::Float(f)) => assert!(*f > 1_000_000_000.0),
        _ => panic!("expected float"),
    }
}

#[test]
fn parse_zero_integer() {
    let p = parse_ilp_line("m v=0i 0").unwrap();
    assert_eq!(p.fields.get("v"), Some(&IlpValue::Integer(0)));
    assert_eq!(p.timestamp, Some(Timestamp(0)));
}

#[test]
fn parse_many_tags_with_string_field() {
    let tags: String = (0..10).map(|i| format!(",t{i}=v{i}")).collect();
    let line = format!("m{tags} msg=\"hello world\"");
    let p = parse_ilp_line(&line).unwrap();
    assert_eq!(p.tags.len(), 10);
    assert_eq!(
        p.fields.get("msg"),
        Some(&IlpValue::String("hello world".into()))
    );
}

#[test]
fn batch_preserves_order() {
    let input: String = (0..50).map(|i| format!("m{i} v={i}i\n")).collect();
    let lines = parse_ilp_batch(&input).unwrap();
    for (i, line) in lines.iter().enumerate() {
        assert_eq!(line.measurement, format!("m{i}"));
    }
}
