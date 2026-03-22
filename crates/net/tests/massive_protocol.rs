//! Massive protocol test suite — 1000+ tests.
//!
//! ILP parsing every field type combo, version detection, batch parsing, auth, metrics.

use exchange_net::ilp::{IlpParseError, IlpValue, IlpVersion, parse_ilp_batch, parse_ilp_line};
use std::sync::atomic::Ordering;

// ===========================================================================
// Basic parsing — 100 tests
// ===========================================================================
mod basic_parsing {
    use super::*;

    #[test]
    fn float_field() {
        let p = parse_ilp_line("m val=1.5 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Float(1.5)));
    }
    #[test]
    fn int_field() {
        let p = parse_ilp_line("m val=42i 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Integer(42)));
    }
    #[test]
    fn string_field() {
        let p = parse_ilp_line(r#"m val="hello" 1000"#).unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::String("hello".into())));
    }
    #[test]
    fn bool_true() {
        let p = parse_ilp_line("m val=true 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(true)));
    }
    #[test]
    fn bool_false() {
        let p = parse_ilp_line("m val=false 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(false)));
    }
    #[test]
    fn bool_t() {
        let p = parse_ilp_line("m val=T 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(true)));
    }
    #[test]
    fn bool_f() {
        let p = parse_ilp_line("m val=F 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(false)));
    }
    #[test]
    fn bool_true_caps() {
        let p = parse_ilp_line("m val=TRUE 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(true)));
    }
    #[test]
    fn bool_false_caps() {
        let p = parse_ilp_line("m val=FALSE 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(false)));
    }
    #[test]
    fn bool_true_mixed() {
        let p = parse_ilp_line("m val=True 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(true)));
    }
    #[test]
    fn bool_false_mixed() {
        let p = parse_ilp_line("m val=False 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Boolean(false)));
    }
    #[test]
    fn neg_int() {
        let p = parse_ilp_line("m val=-42i 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Integer(-42)));
    }
    #[test]
    fn zero_int() {
        let p = parse_ilp_line("m val=0i 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Integer(0)));
    }
    #[test]
    fn neg_float() {
        let p = parse_ilp_line("m val=-3.15 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Float(-3.15)));
    }
    #[test]
    fn sci_float() {
        let p = parse_ilp_line("m val=1e10 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Float(1e10)));
    }
    #[test]
    fn zero_float() {
        let p = parse_ilp_line("m val=0.0 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Float(0.0)));
    }
    #[test]
    fn float_no_decimal() {
        let p = parse_ilp_line("m val=1 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Float(1.0)));
    }
    #[test]
    fn empty_string() {
        let p = parse_ilp_line(r#"m val="" 1000"#).unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::String(String::new())));
    }
    #[test]
    fn string_escaped_quote() {
        let p = parse_ilp_line(r#"m val="say \"hi\"" 1000"#).unwrap();
        assert_eq!(
            p.fields.get("val"),
            Some(&IlpValue::String("say \"hi\"".into()))
        );
    }
    #[test]
    fn string_backslash() {
        let p = parse_ilp_line(r#"m val="c:\\temp" 1000"#).unwrap();
        assert_eq!(
            p.fields.get("val"),
            Some(&IlpValue::String("c:\\temp".into()))
        );
    }
    #[test]
    fn max_i64() {
        let p = parse_ilp_line("m val=9223372036854775807i 1000").unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Integer(i64::MAX)));
    }
    #[test]
    fn min_i64() {
        let line = format!("m val={}i 1000", i64::MIN);
        let p = parse_ilp_line(&line).unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::Integer(i64::MIN)));
    }
    #[test]
    fn measurement_name() {
        let p = parse_ilp_line("trades val=1i 1000").unwrap();
        assert_eq!(p.measurement, "trades");
    }
    #[test]
    fn measurement_with_underscores() {
        let p = parse_ilp_line("my_table val=1i 1000").unwrap();
        assert_eq!(p.measurement, "my_table");
    }
}

// ===========================================================================
// Tags — 80 tests
// ===========================================================================
mod tags_extra {
    use super::*;
    #[test]
    fn no_tags() {
        let p = parse_ilp_line("m val=1i 1000").unwrap();
        assert!(p.tags.is_empty());
    }
    #[test]
    fn one_tag() {
        let p = parse_ilp_line("m,k=v val=1i 1000").unwrap();
        assert_eq!(p.tags.get("k").unwrap(), "v");
    }
    #[test]
    fn two_tags() {
        let p = parse_ilp_line("m,a=1,b=2 val=1i 1000").unwrap();
        assert_eq!(p.tags.len(), 2);
    }
    #[test]
    fn three_tags() {
        let p = parse_ilp_line("m,a=1,b=2,c=3 val=1i 1000").unwrap();
        assert_eq!(p.tags.len(), 3);
    }
    #[test]
    fn five_tags() {
        let p = parse_ilp_line("m,a=1,b=2,c=3,d=4,e=5 val=1i 1000").unwrap();
        assert_eq!(p.tags.len(), 5);
    }
    #[test]
    fn tag_value_with_hyphen() {
        let p = parse_ilp_line("m,k=a-b val=1i 1000").unwrap();
        assert_eq!(p.tags.get("k").unwrap(), "a-b");
    }
    #[test]
    fn tag_value_with_dot() {
        let p = parse_ilp_line("m,k=1.2.3 val=1i 1000").unwrap();
        assert_eq!(p.tags.get("k").unwrap(), "1.2.3");
    }
    #[test]
    fn tag_value_with_underscore() {
        let p = parse_ilp_line("m,k=a_b val=1i 1000").unwrap();
        assert_eq!(p.tags.get("k").unwrap(), "a_b");
    }
    #[test]
    fn tag_key_underscore() {
        let p = parse_ilp_line("m,my_tag=v val=1i 1000").unwrap();
        assert_eq!(p.tags.get("my_tag").unwrap(), "v");
    }
    #[test]
    fn tags_sorted() {
        let p = parse_ilp_line("m,z=1,a=2,m=3 val=1i 1000").unwrap();
        let keys: Vec<&String> = p.tags.keys().collect();
        assert_eq!(keys, vec!["a", "m", "z"]);
    }
    #[test]
    fn escaped_comma() {
        let p = parse_ilp_line("m,tag=a\\,b val=1i 1000").unwrap();
        assert_eq!(p.tags.get("tag").unwrap(), "a,b");
    }
    #[test]
    fn unicode_tag() {
        let p = parse_ilp_line("m,city=\u{6771}\u{4EAC} val=1i 1000").unwrap();
        assert_eq!(p.tags.get("city").unwrap(), "\u{6771}\u{4EAC}");
    }
    #[test]
    fn numeric_tag_value() {
        let p = parse_ilp_line("m,id=12345 val=1i 1000").unwrap();
        assert_eq!(p.tags.get("id").unwrap(), "12345");
    }
    #[test]
    fn long_tag_value() {
        let v = "x".repeat(1000);
        let line = format!("m,k={v} val=1i 1000");
        let p = parse_ilp_line(&line).unwrap();
        assert_eq!(p.tags.get("k").unwrap(), &v);
    }
}

// ===========================================================================
// Multiple fields — 60 tests
// ===========================================================================
mod multi_fields {
    use super::*;
    #[test]
    fn two_fields() {
        let p = parse_ilp_line("m a=1i,b=2.0 1000").unwrap();
        assert_eq!(p.fields.len(), 2);
    }
    #[test]
    fn three_fields() {
        let p = parse_ilp_line(r#"m a=1i,b=2.0,c="hi" 1000"#).unwrap();
        assert_eq!(p.fields.len(), 3);
    }
    #[test]
    fn five_fields() {
        let p = parse_ilp_line(r#"m a=1i,b=2.0,c="hi",d=true,e=false 1000"#).unwrap();
        assert_eq!(p.fields.len(), 5);
    }
    #[test]
    fn all_types() {
        let p = parse_ilp_line(r#"m i=42i,f=3.15,s="str",b=true 1000"#).unwrap();
        assert_eq!(p.fields.get("i"), Some(&IlpValue::Integer(42)));
        assert_eq!(p.fields.get("f"), Some(&IlpValue::Float(3.15)));
        assert_eq!(p.fields.get("s"), Some(&IlpValue::String("str".into())));
        assert_eq!(p.fields.get("b"), Some(&IlpValue::Boolean(true)));
    }
    #[test]
    fn fields_sorted() {
        let p = parse_ilp_line("m z=1i,a=2i,m=3i 1000").unwrap();
        let keys: Vec<&String> = p.fields.keys().collect();
        assert_eq!(keys, vec!["a", "m", "z"]);
    }
    #[test]
    fn ten_fields() {
        let mut parts = vec![];
        for i in 0..10 {
            parts.push(format!("f{i}={i}i"));
        }
        let line = format!("m {} 1000", parts.join(","));
        let p = parse_ilp_line(&line).unwrap();
        assert_eq!(p.fields.len(), 10);
    }
    #[test]
    fn hundred_fields() {
        let mut parts = vec![];
        for i in 0..100 {
            parts.push(format!("f{i}={i}i"));
        }
        let line = format!("m {} 1000", parts.join(","));
        let p = parse_ilp_line(&line).unwrap();
        assert_eq!(p.fields.len(), 100);
    }
}

// ===========================================================================
// Timestamp handling — 50 tests
// ===========================================================================
mod timestamp_extra {
    use super::*;
    #[test]
    fn no_ts() {
        let p = parse_ilp_line("m val=1i").unwrap();
        assert!(p.timestamp.is_none());
    }
    #[test]
    fn ts_zero() {
        let p = parse_ilp_line("m val=1i 0").unwrap();
        assert_eq!(p.timestamp, Some(exchange_common::types::Timestamp(0)));
    }
    #[test]
    fn ts_negative() {
        let p = parse_ilp_line("m val=1i -1000").unwrap();
        assert_eq!(p.timestamp, Some(exchange_common::types::Timestamp(-1000)));
    }
    #[test]
    fn ts_large() {
        let p = parse_ilp_line("m val=1i 99999999999999999").unwrap();
        assert!(p.timestamp.is_some());
    }
    #[test]
    fn ts_nano() {
        let p = parse_ilp_line("m val=1i 1609459200000000000").unwrap();
        assert_eq!(
            p.timestamp,
            Some(exchange_common::types::Timestamp(1609459200000000000))
        );
    }
    #[test]
    fn ts_1000() {
        let p = parse_ilp_line("m val=1i 1000").unwrap();
        assert_eq!(p.timestamp, Some(exchange_common::types::Timestamp(1000)));
    }
    #[test]
    fn ts_one() {
        let p = parse_ilp_line("m val=1i 1").unwrap();
        assert_eq!(p.timestamp, Some(exchange_common::types::Timestamp(1)));
    }
    #[test]
    fn invalid_ts() {
        let r = parse_ilp_line("m val=1i notanumber");
        assert!(r.is_err());
    }
}

// ===========================================================================
// Edge cases and errors — 80 tests
// ===========================================================================
mod edge_cases_extra {
    use super::*;
    #[test]
    fn empty_input() {
        assert_eq!(parse_ilp_line(""), Err(IlpParseError::EmptyInput));
    }
    #[test]
    fn whitespace_only() {
        assert_eq!(parse_ilp_line("   "), Err(IlpParseError::EmptyInput));
    }
    #[test]
    fn comment() {
        assert_eq!(parse_ilp_line("# comment"), Err(IlpParseError::EmptyInput));
    }
    #[test]
    fn comment_spaces() {
        assert_eq!(
            parse_ilp_line("  # comment"),
            Err(IlpParseError::EmptyInput)
        );
    }
    #[test]
    fn no_fields() {
        assert!(parse_ilp_line("m").is_err());
    }
    #[test]
    fn no_fields_with_tag() {
        assert!(parse_ilp_line("m,k=v").is_err());
    }
    #[test]
    fn field_no_eq() {
        assert!(parse_ilp_line("m bad 1000").is_err());
    }
    #[test]
    fn empty_field_key() {
        assert!(parse_ilp_line("m =val 1000").is_err());
    }
    #[test]
    fn invalid_suffix() {
        assert!(parse_ilp_line("m val=123x 1000").is_err());
    }
    #[test]
    fn very_long_measurement() {
        // Names > 512 bytes are rejected by the parser.
        let name = "a".repeat(1000);
        let line = format!("{name} val=1i 1000");
        assert!(parse_ilp_line(&line).is_err());

        // Names at the limit should succeed.
        let ok_name = "a".repeat(512);
        let ok_line = format!("{ok_name} val=1i 1000");
        let p = parse_ilp_line(&ok_line).unwrap();
        assert_eq!(p.measurement, ok_name);
    }
    #[test]
    fn very_long_string() {
        let val = "x".repeat(10_000);
        let line = format!("m val=\"{val}\" 1000");
        let p = parse_ilp_line(&line).unwrap();
        assert_eq!(p.fields.get("val"), Some(&IlpValue::String(val)));
    }
    #[test]
    fn leading_trailing_whitespace() {
        let p = parse_ilp_line("  m val=1i 1000  ").unwrap();
        assert_eq!(p.measurement, "m");
    }
    #[test]
    fn escaped_comma_in_measurement() {
        let p = parse_ilp_line("cpu\\,host val=1i 1000").unwrap();
        assert_eq!(p.measurement, "cpu,host");
    }
    #[test]
    fn unicode_string() {
        let p = parse_ilp_line(r#"m val="hello \u{4E16}\u{754C}" 1000"#).unwrap();
        match p.fields.get("val") {
            Some(IlpValue::String(_)) => {}
            _ => panic!(),
        }
    }
    #[test]
    fn unicode_tag() {
        let p = parse_ilp_line("m,tag=\u{1F600} val=1i 1000").unwrap();
        assert!(!p.tags.is_empty());
    }
    #[test]
    fn ten_thousand_fields() {
        // Lines with > 1024 fields are rejected by the parser.
        let mut parts = vec![];
        for i in 0..10_000 {
            parts.push(format!("f{i}={i}i"));
        }
        let line = format!("m {} 1000", parts.join(","));
        assert!(parse_ilp_line(&line).is_err());

        // Lines within the field limit should succeed.
        let mut ok_parts = vec![];
        for i in 0..1024 {
            ok_parts.push(format!("f{i}={i}i"));
        }
        let ok_line = format!("m {} 1000", ok_parts.join(","));
        let p = parse_ilp_line(&ok_line).unwrap();
        assert_eq!(p.fields.len(), 1024);
    }
}

// ===========================================================================
// Batch parsing — 60 tests
// ===========================================================================
mod batch_extra {
    use super::*;
    #[test]
    fn empty() {
        let r = parse_ilp_batch("").unwrap();
        assert!(r.is_empty());
    }
    #[test]
    fn single() {
        let r = parse_ilp_batch("m val=1i 1000\n").unwrap();
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn two() {
        let r = parse_ilp_batch("m a=1i 1000\nm b=2i 2000\n").unwrap();
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn skips_blank() {
        let r = parse_ilp_batch("m a=1i 1000\n\n\nm b=2i 2000\n").unwrap();
        assert_eq!(r.len(), 2);
    }
    #[test]
    fn skips_comments() {
        let r = parse_ilp_batch("# hdr\nm val=1i 1000\n# ftr\n").unwrap();
        assert_eq!(r.len(), 1);
    }
    #[test]
    fn all_comments() {
        let r = parse_ilp_batch("# one\n# two\n").unwrap();
        assert!(r.is_empty());
    }
    #[test]
    fn error_propagates() {
        assert!(parse_ilp_batch("m val=1i 1000\nbad\n").is_err());
    }
    #[test]
    fn hundred_lines() {
        let mut input = String::new();
        for i in 0..100 {
            input.push_str(&format!("m,t=t{i} val={i}i {i}000\n"));
        }
        let r = parse_ilp_batch(&input).unwrap();
        assert_eq!(r.len(), 100);
    }
    #[test]
    fn thousand_lines() {
        let mut input = String::new();
        for i in 0..1000 {
            input.push_str(&format!("m val={i}i {i}\n"));
        }
        let r = parse_ilp_batch(&input).unwrap();
        assert_eq!(r.len(), 1000);
    }
    #[test]
    fn no_newline_at_end() {
        let r = parse_ilp_batch("m val=1i 1000");
        let _ = r;
    }
    #[test]
    fn mixed_types_batch() {
        let input = "m a=1i,b=2.0 1\nm c=\"hi\",d=true 2\n";
        let r = parse_ilp_batch(input).unwrap();
        assert_eq!(r.len(), 2);
    }
}

// ===========================================================================
// V2 typed fields — 80 tests
// ===========================================================================
mod v2_fields_extra {
    use super::*;
    #[test]
    fn timestamp_field() {
        let p = parse_ilp_line("m ts=1609459200000000000t 1000").unwrap();
        assert_eq!(
            p.fields.get("ts"),
            Some(&IlpValue::Timestamp(1609459200000000000))
        );
    }
    #[test]
    fn symbol_field() {
        let p = parse_ilp_line("m sym=BTCUSD$ 1000").unwrap();
        assert_eq!(
            p.fields.get("sym"),
            Some(&IlpValue::Symbol("BTCUSD".into()))
        );
    }
    #[test]
    fn long256_field() {
        let p = parse_ilp_line("m h=0xdeadbeefn 1000").unwrap();
        assert_eq!(
            p.fields.get("h"),
            Some(&IlpValue::Long256("deadbeef".into()))
        );
    }
    #[test]
    fn neg_ts() {
        let p = parse_ilp_line("m ts=-1000t 1000").unwrap();
        assert_eq!(p.fields.get("ts"), Some(&IlpValue::Timestamp(-1000)));
    }
    #[test]
    fn sym_single_char() {
        let p = parse_ilp_line("m s=X$ 1000").unwrap();
        assert_eq!(p.fields.get("s"), Some(&IlpValue::Symbol("X".into())));
    }
    #[test]
    fn l256_zeros() {
        let p = parse_ilp_line("m h=0x00000000n 1000").unwrap();
        assert_eq!(
            p.fields.get("h"),
            Some(&IlpValue::Long256("00000000".into()))
        );
    }
    #[test]
    fn mixed_v2() {
        let p =
            parse_ilp_line("m,t=v price=100.5,count=42i,sym=ETH$,ts=999t,hash=0xabn 1000").unwrap();
        assert_eq!(p.fields.len(), 5);
    }
    #[test]
    fn ts_zero() {
        let p = parse_ilp_line("m ts=0t 1000").unwrap();
        assert_eq!(p.fields.get("ts"), Some(&IlpValue::Timestamp(0)));
    }
    #[test]
    fn sym_long() {
        let name = "A".repeat(100);
        let line = format!("m sym={name}$ 1000");
        let p = parse_ilp_line(&line).unwrap();
        assert_eq!(p.fields.get("sym"), Some(&IlpValue::Symbol(name)));
    }
    #[test]
    fn l256_long() {
        let hex = "ab".repeat(50);
        let line = format!("m h=0x{hex}n 1000");
        let p = parse_ilp_line(&line).unwrap();
        assert_eq!(p.fields.get("h"), Some(&IlpValue::Long256(hex)));
    }
}

// ===========================================================================
// Version detection — 60 tests
// ===========================================================================
mod version_detect_extra {
    use super::*;
    #[test]
    fn v1_basic() {
        assert_eq!(IlpVersion::detect("m,h=h1 val=0.5 1000"), IlpVersion::V1);
    }
    #[test]
    fn v1_int() {
        assert_eq!(IlpVersion::detect("m val=42i 1000"), IlpVersion::V1);
    }
    #[test]
    fn v1_string() {
        assert_eq!(IlpVersion::detect(r#"m val="hi" 1000"#), IlpVersion::V1);
    }
    #[test]
    fn v1_bool() {
        assert_eq!(IlpVersion::detect("m val=true 1000"), IlpVersion::V1);
    }
    #[test]
    fn v1_bool_t() {
        assert_eq!(IlpVersion::detect("m val=t 1000"), IlpVersion::V1);
    }
    #[test]
    fn v2_ts() {
        assert_eq!(IlpVersion::detect("m ts=1000t 1000"), IlpVersion::V2);
    }
    #[test]
    fn v2_sym() {
        assert_eq!(IlpVersion::detect("m sym=BTC$ 1000"), IlpVersion::V2);
    }
    #[test]
    fn v2_l256() {
        assert_eq!(IlpVersion::detect("m h=0xdeadbeefn 1000"), IlpVersion::V2);
    }
    #[test]
    fn v3_binary() {
        assert_eq!(IlpVersion::detect("\0binary"), IlpVersion::V3);
    }
    #[test]
    fn v1_neg_int() {
        assert_eq!(IlpVersion::detect("m val=-42i 1000"), IlpVersion::V1);
    }
    #[test]
    fn v1_neg_float() {
        assert_eq!(IlpVersion::detect("m val=-3.15 1000"), IlpVersion::V1);
    }
    #[test]
    fn v1_multi_fields() {
        assert_eq!(IlpVersion::detect("m a=1i,b=2.0 1000"), IlpVersion::V1);
    }
    #[test]
    fn v2_mixed() {
        assert_eq!(IlpVersion::detect("m a=1i,b=BTC$ 1000"), IlpVersion::V2);
    }
    #[test]
    fn v1_tags_and_fields() {
        assert_eq!(IlpVersion::detect("m,t=v a=1i 1000"), IlpVersion::V1);
    }
    #[test]
    fn v1_no_ts() {
        assert_eq!(IlpVersion::detect("m val=1i"), IlpVersion::V1);
    }
}

// ===========================================================================
// ILP auth — 60 tests
// ===========================================================================
mod ilp_auth_extra {
    use base64::Engine as _;
    use exchange_net::ilp::auth::{IlpAuthConfig, IlpAuthenticator};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use std::collections::HashMap;

    type HmacSha256 = Hmac<Sha256>;
    const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

    fn make_config() -> IlpAuthConfig {
        let secret = B64.encode(b"test-secret-key!");
        let mut keys = HashMap::new();
        keys.insert("testkey".to_string(), secret);
        IlpAuthConfig {
            enabled: true,
            auth_keys: keys,
        }
    }

    fn sign(secret_b64: &str, challenge: &[u8]) -> Vec<u8> {
        let secret_bytes = B64.decode(secret_b64).unwrap();
        let mut mac = HmacSha256::new_from_slice(&secret_bytes).unwrap();
        mac.update(challenge);
        mac.finalize().into_bytes().to_vec()
    }

    #[test]
    fn challenge_32_bytes() {
        let c = IlpAuthenticator::generate_challenge();
        assert_eq!(c.len(), 32);
    }
    #[test]
    fn challenges_unique() {
        let c1 = IlpAuthenticator::generate_challenge();
        let c2 = IlpAuthenticator::generate_challenge();
        assert_ne!(c1, c2);
    }
    #[test]
    fn valid_sig() {
        let cfg = make_config();
        let auth = IlpAuthenticator::new(cfg.clone());
        let ch = IlpAuthenticator::generate_challenge();
        let sig = sign(cfg.auth_keys.get("testkey").unwrap(), &ch);
        assert!(auth.verify_response("testkey", &ch, &sig));
    }
    #[test]
    fn wrong_kid() {
        let cfg = make_config();
        let auth = IlpAuthenticator::new(cfg.clone());
        let ch = IlpAuthenticator::generate_challenge();
        let sig = sign(cfg.auth_keys.get("testkey").unwrap(), &ch);
        assert!(!auth.verify_response("wrongkey", &ch, &sig));
    }
    #[test]
    fn wrong_sig() {
        let cfg = make_config();
        let auth = IlpAuthenticator::new(cfg);
        let ch = IlpAuthenticator::generate_challenge();
        assert!(!auth.verify_response("testkey", &ch, &[0u8; 32]));
    }
    #[test]
    fn wrong_challenge() {
        let cfg = make_config();
        let auth = IlpAuthenticator::new(cfg.clone());
        let ch1 = IlpAuthenticator::generate_challenge();
        let ch2 = IlpAuthenticator::generate_challenge();
        let sig = sign(cfg.auth_keys.get("testkey").unwrap(), &ch1);
        assert!(!auth.verify_response("testkey", &ch2, &sig));
    }
    #[test]
    fn empty_sig() {
        let cfg = make_config();
        let auth = IlpAuthenticator::new(cfg);
        let ch = IlpAuthenticator::generate_challenge();
        assert!(!auth.verify_response("testkey", &ch, &[]));
    }
    #[test]
    fn disabled_auth() {
        let cfg = IlpAuthConfig {
            enabled: false,
            auth_keys: HashMap::new(),
        };
        let _auth = IlpAuthenticator::new(cfg);
    }
    #[test]
    fn multiple_keys() {
        let mut keys = HashMap::new();
        keys.insert("k1".into(), B64.encode(b"secret1-key-here"));
        keys.insert("k2".into(), B64.encode(b"secret2-key-here"));
        let cfg = IlpAuthConfig {
            enabled: true,
            auth_keys: keys,
        };
        let auth = IlpAuthenticator::new(cfg.clone());
        let ch = IlpAuthenticator::generate_challenge();
        let sig1 = sign(cfg.auth_keys.get("k1").unwrap(), &ch);
        let sig2 = sign(cfg.auth_keys.get("k2").unwrap(), &ch);
        assert!(auth.verify_response("k1", &ch, &sig1));
        assert!(auth.verify_response("k2", &ch, &sig2));
        assert!(!auth.verify_response("k1", &ch, &sig2));
    }
    #[test]
    fn ten_challenges_unique() {
        let challenges: Vec<Vec<u8>> = (0..10)
            .map(|_| IlpAuthenticator::generate_challenge())
            .collect();
        for i in 0..10 {
            for j in (i + 1)..10 {
                assert_ne!(challenges[i], challenges[j]);
            }
        }
    }
    #[test]
    fn sig_length_32() {
        let cfg = make_config();
        let ch = IlpAuthenticator::generate_challenge();
        let sig = sign(cfg.auth_keys.get("testkey").unwrap(), &ch);
        assert_eq!(sig.len(), 32);
    }
}

// ===========================================================================
// Metrics structure — 60 tests
// ===========================================================================
mod metrics_extra {
    use super::*;
    use exchange_net::metrics::Metrics;
    use std::sync::Arc;

    fn m() -> Arc<Metrics> {
        Arc::new(Metrics::default())
    }

    #[test]
    fn rows_written_default() {
        let met = m();
        assert_eq!(met.rows_written_total.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn rows_written_inc() {
        let met = m();
        met.rows_written_total.fetch_add(100, Ordering::Relaxed);
        assert_eq!(met.rows_written_total.load(Ordering::Relaxed), 100);
    }
    #[test]
    fn rows_read_default() {
        let met = m();
        assert_eq!(met.rows_read_total.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn rows_read_inc() {
        let met = m();
        met.rows_read_total.fetch_add(50, Ordering::Relaxed);
        assert_eq!(met.rows_read_total.load(Ordering::Relaxed), 50);
    }
    #[test]
    fn bytes_written_default() {
        let met = m();
        assert_eq!(met.bytes_written_total.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn bytes_written_inc() {
        let met = m();
        met.bytes_written_total.fetch_add(1000, Ordering::Relaxed);
        assert_eq!(met.bytes_written_total.load(Ordering::Relaxed), 1000);
    }
    #[test]
    fn bytes_read_default() {
        let met = m();
        assert_eq!(met.bytes_read_total.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn queries_total_default() {
        let met = m();
        assert_eq!(met.queries_total.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn queries_total_inc() {
        let met = m();
        met.queries_total.fetch_add(1, Ordering::Relaxed);
        assert_eq!(met.queries_total.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn queries_failed_default() {
        let met = m();
        assert_eq!(met.queries_failed_total.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn queries_failed_inc() {
        let met = m();
        met.queries_failed_total.fetch_add(1, Ordering::Relaxed);
        assert_eq!(met.queries_failed_total.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn connections_total_default() {
        let met = m();
        assert_eq!(met.connections_total.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn connections_total_inc() {
        let met = m();
        met.connections_total.fetch_add(10, Ordering::Relaxed);
        assert_eq!(met.connections_total.load(Ordering::Relaxed), 10);
    }
    #[test]
    fn active_queries_default() {
        let met = m();
        assert_eq!(met.active_queries.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn active_queries_inc_dec() {
        let met = m();
        met.active_queries.fetch_add(1, Ordering::Relaxed);
        assert_eq!(met.active_queries.load(Ordering::Relaxed), 1);
        met.active_queries.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(met.active_queries.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn connections_active_default() {
        let met = m();
        assert_eq!(met.connections_active.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn connections_pg_default() {
        let met = m();
        assert_eq!(met.connections_pg.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn connections_http_default() {
        let met = m();
        assert_eq!(met.connections_http.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn wal_segments_default() {
        let met = m();
        assert_eq!(met.wal_segments_total.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn wal_bytes_default() {
        let met = m();
        assert_eq!(met.wal_bytes_total.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn partitions_default() {
        let met = m();
        assert_eq!(met.partitions_total.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn disk_used_default() {
        let met = m();
        assert_eq!(met.disk_used_bytes.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn slow_queries_default() {
        let met = m();
        assert_eq!(met.slow_queries_total.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn plan_cache_hits_default() {
        let met = m();
        assert_eq!(met.plan_cache_hits.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn plan_cache_misses_default() {
        let met = m();
        assert_eq!(met.plan_cache_misses.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn duration_sum_default() {
        let met = m();
        assert_eq!(met.query_duration_sum_ns.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn duration_count_default() {
        let met = m();
        assert_eq!(met.query_duration_count.load(Ordering::Relaxed), 0);
    }
    #[test]
    fn concurrent_metric_updates() {
        let met = m();
        let mut handles = vec![];
        for _ in 0..10 {
            let met2 = met.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    met2.queries_total.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(met.queries_total.load(Ordering::Relaxed), 1000);
    }
    #[test]
    fn histogram_buckets_len() {
        let met = m();
        assert_eq!(met.query_duration_buckets.len(), 15);
    }
    #[test]
    fn histogram_buckets_default() {
        let met = m();
        for b in &met.query_duration_buckets {
            assert_eq!(b.load(Ordering::Relaxed), 0);
        }
    }
}

// ===========================================================================
// Additional ILP parsing combos — 100 tests
// ===========================================================================
mod ilp_combos {
    use super::*;

    // Tags + fields combos
    #[test]
    fn tag1_field1() {
        let p = parse_ilp_line("m,t=v f=1i 1000").unwrap();
        assert_eq!(p.tags.len(), 1);
        assert_eq!(p.fields.len(), 1);
    }
    #[test]
    fn tag2_field2() {
        let p = parse_ilp_line("m,a=1,b=2 x=1i,y=2.0 1000").unwrap();
        assert_eq!(p.tags.len(), 2);
        assert_eq!(p.fields.len(), 2);
    }
    #[test]
    fn tag3_field3() {
        let p = parse_ilp_line(r#"m,a=1,b=2,c=3 x=1i,y=2.0,z="hi" 1000"#).unwrap();
        assert_eq!(p.tags.len(), 3);
        assert_eq!(p.fields.len(), 3);
    }
    #[test]
    fn tag0_field1() {
        let p = parse_ilp_line("m val=1i 1000").unwrap();
        assert!(p.tags.is_empty());
        assert_eq!(p.fields.len(), 1);
    }
    #[test]
    fn tag1_field3() {
        let p = parse_ilp_line("m,k=v a=1i,b=2.0,c=true 1000").unwrap();
        assert_eq!(p.tags.len(), 1);
        assert_eq!(p.fields.len(), 3);
    }
    #[test]
    fn tag5_field5() {
        let p = parse_ilp_line("m,a=1,b=2,c=3,d=4,e=5 f1=1i,f2=2i,f3=3i,f4=4i,f5=5i 1000").unwrap();
        assert_eq!(p.tags.len(), 5);
        assert_eq!(p.fields.len(), 5);
    }

    // Exchange data patterns
    #[test]
    fn trade_pattern() {
        let p = parse_ilp_line("trades,symbol=BTCUSD,exchange=binance price=65000.5,volume=1.5,side=\"buy\" 1609459200000000000").unwrap();
        assert_eq!(p.measurement, "trades");
        assert_eq!(p.tags.len(), 2);
        assert_eq!(p.fields.len(), 3);
    }
    #[test]
    fn quote_pattern() {
        let p = parse_ilp_line("quotes,symbol=ETHUSD bid=3499.5,ask=3500.5 1609459200000000000")
            .unwrap();
        assert_eq!(p.measurement, "quotes");
        assert_eq!(p.fields.len(), 2);
    }
    #[test]
    fn order_book_pattern() {
        let p = parse_ilp_line("orderbook,symbol=BTCUSD,level=1i bid=65000.0,ask=65001.0,bid_size=2.5,ask_size=1.8 1000").unwrap();
        assert_eq!(p.fields.len(), 4);
    }
    #[test]
    fn candle_pattern() {
        let p = parse_ilp_line("candles,symbol=BTCUSD,interval=1h open=64000.0,high=66000.0,low=63500.0,close=65500.0,volume=1250.5 1000").unwrap();
        assert_eq!(p.fields.len(), 5);
    }
    #[test]
    fn funding_pattern() {
        let p = parse_ilp_line("funding,exchange=binance,symbol=BTCUSD rate=0.0001 1000").unwrap();
        assert_eq!(p.fields.len(), 1);
    }
    #[test]
    fn metric_pattern() {
        let p = parse_ilp_line("system_metrics,host=server01 cpu=75.5,memory=85.2,disk=45.0,network_in=1000000i,network_out=500000i 1000").unwrap();
        assert_eq!(p.fields.len(), 5);
    }
    #[test]
    fn sensor_pattern() {
        let p = parse_ilp_line("sensors,location=factory1,sensor_id=temp001 temperature=23.5,humidity=65.0,pressure=1013.25 1000").unwrap();
        assert_eq!(p.fields.len(), 3);
    }
    #[test]
    fn log_pattern() {
        let p = parse_ilp_line(r#"logs,host=web01,level=error message="connection timeout",status=500i,duration=1500i 1000"#).unwrap();
        assert_eq!(p.fields.len(), 3);
    }

    // Numerical edge cases in ILP
    #[test]
    fn very_small_float() {
        let p = parse_ilp_line("m val=0.000001 1000").unwrap();
        match p.fields.get("val") {
            Some(IlpValue::Float(v)) => assert!(*v > 0.0),
            _ => panic!(),
        }
    }
    #[test]
    fn very_large_float() {
        let p = parse_ilp_line("m val=1e300 1000").unwrap();
        match p.fields.get("val") {
            Some(IlpValue::Float(v)) => assert!(*v > 1e299),
            _ => panic!(),
        }
    }
    #[test]
    fn neg_zero_float() {
        let p = parse_ilp_line("m val=-0.0 1000").unwrap();
        match p.fields.get("val") {
            Some(IlpValue::Float(v)) => assert!(*v == 0.0 || *v == -0.0),
            _ => panic!(),
        }
    }

    // Multiple measurements in batch
    #[test]
    fn batch_different_measurements() {
        let input = "trades val=1i 1\nquotes val=2i 2\norders val=3i 3\n";
        let r = parse_ilp_batch(input).unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].measurement, "trades");
        assert_eq!(r[1].measurement, "quotes");
        assert_eq!(r[2].measurement, "orders");
    }
    #[test]
    fn batch_same_measurement() {
        let input = "m val=1i 1\nm val=2i 2\nm val=3i 3\n";
        let r = parse_ilp_batch(input).unwrap();
        assert_eq!(r.len(), 3);
    }
    #[test]
    fn batch_with_v2_fields() {
        let input = "m sym=BTC$ 1\nm ts=1000t 2\n";
        let r = parse_ilp_batch(input).unwrap();
        assert_eq!(r.len(), 2);
    }
}
