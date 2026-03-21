use std::collections::BTreeMap;

use exchange_common::types::Timestamp;

/// ILP protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IlpVersion {
    /// v1: basic line protocol (original InfluxDB format).
    V1,
    /// v2: adds typed tags, array fields, timestamp field type.
    V2,
    /// v3: adds binary format support.
    V3,
}

impl IlpVersion {
    /// Detect protocol version from a line.
    ///
    /// - v3: line starts with `\x00` (binary header byte).
    /// - v2: line contains typed suffixes like `=123t` (timestamp),
    ///   `=SYM$` (symbol), or `=0x...n` (long256).
    /// - v1: everything else (default).
    pub fn detect(line: &str) -> Self {
        let bytes = line.as_bytes();
        if !bytes.is_empty() && bytes[0] == 0x00 {
            return IlpVersion::V3;
        }
        // Look for v2 typed field suffixes after the fields section.
        if let Some((_meas_tags, rest)) = split_first_unescaped_space(line) {
            let fields_str = match split_first_unescaped_space(rest) {
                Some((f, _)) => f,
                None => rest,
            };
            for part in split_fields_on_comma(fields_str) {
                if let Some(eq_pos) = part.find('=') {
                    let val = &part[eq_pos + 1..];
                    // Timestamp suffix 't'
                    if val.ends_with('t') && val[..val.len() - 1].parse::<i64>().is_ok() {
                        return IlpVersion::V2;
                    }
                    // Long256 hex prefix
                    if val.starts_with("0x") && val.ends_with('n') {
                        return IlpVersion::V2;
                    }
                    // Symbol type: value ends with '$'
                    if val.ends_with('$') && val.len() > 1 && !val.starts_with('"') {
                        return IlpVersion::V2;
                    }
                }
            }
        }
        IlpVersion::V1
    }
}

/// A parsed value from an ILP field.
#[derive(Debug, Clone, PartialEq)]
pub enum IlpValue {
    Integer(i64),
    Float(f64),
    String(String),
    Boolean(bool),
    /// Explicit timestamp field (nanoseconds), v2+ suffix `t`.
    Timestamp(i64),
    /// Symbol type, v2+ suffix `$`.
    Symbol(String),
    /// Long256 hex string, v2+ prefix `0x` suffix `n`.
    Long256(String),
}

/// A single parsed ILP (InfluxDB Line Protocol) line.
#[derive(Debug, Clone, PartialEq)]
pub struct IlpLine {
    pub measurement: String,
    pub tags: BTreeMap<String, String>,
    pub fields: BTreeMap<String, IlpValue>,
    pub timestamp: Option<Timestamp>,
}

/// Maximum allowed length of a single ILP line in bytes.
pub const MAX_ILP_LINE_LENGTH: usize = 1_048_576; // 1 MB

/// Maximum number of tags per ILP line.
pub const MAX_ILP_TAGS: usize = 256;

/// Maximum number of fields per ILP line.
pub const MAX_ILP_FIELDS: usize = 1024;

/// Maximum length of a measurement name in bytes.
pub const MAX_MEASUREMENT_LENGTH: usize = 512;

/// Errors that can occur during ILP parsing.
#[derive(Debug, Clone, PartialEq)]
pub enum IlpParseError {
    EmptyInput,
    MissingMeasurement,
    MissingFields,
    InvalidField(String),
    InvalidTimestamp(String),
    InvalidEscape(String),
    LineTooLong(usize),
    TooManyTags(usize),
    TooManyFields(usize),
    MeasurementTooLong(usize),
}

impl std::fmt::Display for IlpParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyInput => write!(f, "empty input"),
            Self::MissingMeasurement => write!(f, "missing measurement name"),
            Self::MissingFields => write!(f, "missing fields"),
            Self::InvalidField(s) => write!(f, "invalid field: {s}"),
            Self::InvalidTimestamp(s) => write!(f, "invalid timestamp: {s}"),
            Self::InvalidEscape(s) => write!(f, "invalid escape sequence: {s}"),
            Self::LineTooLong(n) => {
                write!(f, "line too long: {n} bytes (max {MAX_ILP_LINE_LENGTH})")
            }
            Self::TooManyTags(n) => write!(f, "too many tags: {n} (max {MAX_ILP_TAGS})"),
            Self::TooManyFields(n) => write!(f, "too many fields: {n} (max {MAX_ILP_FIELDS})"),
            Self::MeasurementTooLong(n) => write!(
                f,
                "measurement name too long: {n} bytes (max {MAX_MEASUREMENT_LENGTH})"
            ),
        }
    }
}

impl std::error::Error for IlpParseError {}

/// Parse a single ILP line.
///
/// Format: `measurement,tag1=val1,tag2=val2 field1=val1,field2=val2 timestamp_ns`
///
/// Field value types:
/// - Integer: `42i`
/// - Float: `3.14` (no suffix)
/// - String: `"hello"`
/// - Boolean: `true`, `false`, `t`, `f`, `TRUE`, `FALSE`, `T`, `F`
pub fn parse_ilp_line(line: &str) -> Result<IlpLine, IlpParseError> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Err(IlpParseError::EmptyInput);
    }

    // Enforce line length limit to prevent memory exhaustion.
    if line.len() > MAX_ILP_LINE_LENGTH {
        return Err(IlpParseError::LineTooLong(line.len()));
    }

    // Split into: measurement_and_tags, fields, optional_timestamp
    // The first space separates measurement+tags from fields.
    // The second space (if present) separates fields from timestamp.
    let (measurement_tags, rest) =
        split_first_unescaped_space(line).ok_or(IlpParseError::MissingFields)?;

    let (fields_str, timestamp_str) = match split_first_unescaped_space(rest) {
        Some((f, t)) => (f, Some(t)),
        None => (rest, None),
    };

    // Parse measurement and tags
    let (measurement, tags) = parse_measurement_tags(measurement_tags)?;

    // Parse fields
    let fields = parse_fields(fields_str)?;
    if fields.is_empty() {
        return Err(IlpParseError::MissingFields);
    }

    // Parse timestamp
    let timestamp = match timestamp_str {
        Some(ts) => {
            let ts = ts.trim();
            if ts.is_empty() {
                None
            } else {
                let nanos: i64 = ts
                    .parse()
                    .map_err(|_| IlpParseError::InvalidTimestamp(ts.to_string()))?;
                Some(Timestamp(nanos))
            }
        }
        None => None,
    };

    Ok(IlpLine {
        measurement,
        tags,
        fields,
        timestamp,
    })
}

/// Parse multiple ILP lines (one per line, skip blanks and comments).
pub fn parse_ilp_batch(input: &str) -> Result<Vec<IlpLine>, IlpParseError> {
    let mut lines = Vec::new();
    for line in input.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        lines.push(parse_ilp_line(trimmed)?);
    }
    Ok(lines)
}

fn split_first_unescaped_space(s: &str) -> Option<(&str, &str)> {
    let mut in_quotes = false;
    let mut prev_backslash = false;
    for (i, ch) in s.char_indices() {
        if ch == '"' && !prev_backslash {
            in_quotes = !in_quotes;
        }
        if ch == ' ' && !in_quotes {
            return Some((&s[..i], &s[i + 1..]));
        }
        prev_backslash = ch == '\\';
    }
    None
}

fn parse_measurement_tags(s: &str) -> Result<(String, BTreeMap<String, String>), IlpParseError> {
    // Split on unescaped commas
    let parts = split_on_unescaped_comma(s);

    let measurement = parts
        .first()
        .filter(|m| !m.is_empty())
        .ok_or(IlpParseError::MissingMeasurement)?;
    let measurement = unescape_measurement(measurement);

    if measurement.len() > MAX_MEASUREMENT_LENGTH {
        return Err(IlpParseError::MeasurementTooLong(measurement.len()));
    }

    let tag_count = parts.len() - 1;
    if tag_count > MAX_ILP_TAGS {
        return Err(IlpParseError::TooManyTags(tag_count));
    }

    let mut tags = BTreeMap::new();
    for part in &parts[1..] {
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| IlpParseError::InvalidField(part.to_string()))?;
        tags.insert(unescape_tag_key(key), unescape_tag_value(value));
    }

    Ok((measurement, tags))
}

fn split_on_unescaped_comma(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let bytes = s.as_bytes();
    for i in 0..bytes.len() {
        if bytes[i] == b',' && (i == 0 || bytes[i - 1] != b'\\') {
            parts.push(&s[start..i]);
            start = i + 1;
        }
    }
    parts.push(&s[start..]);
    parts
}

fn parse_fields(s: &str) -> Result<BTreeMap<String, IlpValue>, IlpParseError> {
    let mut fields = BTreeMap::new();

    // Split fields on commas, respecting quoted strings
    let parts = split_fields_on_comma(s);

    if parts.len() > MAX_ILP_FIELDS {
        return Err(IlpParseError::TooManyFields(parts.len()));
    }

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let eq_pos = part
            .find('=')
            .ok_or_else(|| IlpParseError::InvalidField(part.to_string()))?;
        let key = &part[..eq_pos];
        let value_str = &part[eq_pos + 1..];

        if key.is_empty() {
            return Err(IlpParseError::InvalidField(part.to_string()));
        }

        let value = parse_field_value(value_str)
            .ok_or_else(|| IlpParseError::InvalidField(format!("{key}={value_str}")))?;

        fields.insert(unescape_tag_key(key), value);
    }

    Ok(fields)
}

fn split_fields_on_comma(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    let mut prev_backslash = false;
    for (i, ch) in s.char_indices() {
        if ch == '"' && !prev_backslash {
            in_quotes = !in_quotes;
        }
        if ch == ',' && !in_quotes {
            parts.push(&s[start..i]);
            start = i + 1;
        }
        prev_backslash = ch == '\\';
    }
    parts.push(&s[start..]);
    parts
}

fn parse_field_value(s: &str) -> Option<IlpValue> {
    if s.is_empty() {
        return None;
    }

    // String value: "..."
    if s.starts_with('"') && s.ends_with('"') && s.len() >= 2 {
        let inner = &s[1..s.len() - 1];
        return Some(IlpValue::String(unescape_string(inner)));
    }

    // Boolean
    match s {
        "true" | "TRUE" | "True" | "t" | "T" => return Some(IlpValue::Boolean(true)),
        "false" | "FALSE" | "False" | "f" | "F" => return Some(IlpValue::Boolean(false)),
        _ => {}
    }

    // v2: Long256 hex: 0x...n
    if s.starts_with("0x") && s.ends_with('n') && s.len() > 3 {
        let hex = &s[2..s.len() - 1];
        // Validate hex characters
        if hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Some(IlpValue::Long256(hex.to_string()));
        }
    }

    // v2: Timestamp field: ends with 't' (but not "true"/"t" which are handled above)
    if s.ends_with('t') && s.len() > 1 {
        let num_str = &s[..s.len() - 1];
        if let Ok(v) = num_str.parse::<i64>() {
            return Some(IlpValue::Timestamp(v));
        }
    }

    // v2: Symbol type: ends with '$'
    if s.ends_with('$') && s.len() > 1 {
        let sym = &s[..s.len() - 1];
        return Some(IlpValue::Symbol(sym.to_string()));
    }

    // Integer: ends with 'i'
    if let Some(num_str) = s.strip_suffix('i') {
        if let Ok(v) = num_str.parse::<i64>() {
            return Some(IlpValue::Integer(v));
        }
        // Also handle float-with-i-suffix (e.g. "1.5i") by truncating to integer.
        if let Ok(v) = num_str.parse::<f64>() {
            return Some(IlpValue::Integer(v as i64));
        }
    }

    // Float
    if let Ok(v) = s.parse::<f64>() {
        return Some(IlpValue::Float(v));
    }

    None
}

fn unescape_measurement(s: &str) -> String {
    s.replace("\\,", ",").replace("\\ ", " ")
}

fn unescape_tag_key(s: &str) -> String {
    s.replace("\\,", ",")
        .replace("\\=", "=")
        .replace("\\ ", " ")
}

fn unescape_tag_value(s: &str) -> String {
    s.replace("\\,", ",")
        .replace("\\=", "=")
        .replace("\\ ", " ")
}

fn unescape_string(s: &str) -> String {
    s.replace("\\\"", "\"").replace("\\\\", "\\")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_line() {
        let line = "cpu,host=server01 usage=0.64 1609459200000000000";
        let parsed = parse_ilp_line(line).unwrap();

        assert_eq!(parsed.measurement, "cpu");
        assert_eq!(parsed.tags.get("host").unwrap(), "server01");
        assert_eq!(parsed.fields.get("usage").unwrap(), &IlpValue::Float(0.64));
        assert_eq!(parsed.timestamp, Some(Timestamp(1609459200000000000)));
    }

    #[test]
    fn test_integer_field() {
        let line = "mem,host=h1 total=16384i 1000000000";
        let parsed = parse_ilp_line(line).unwrap();

        assert_eq!(
            parsed.fields.get("total").unwrap(),
            &IlpValue::Integer(16384)
        );
    }

    #[test]
    fn test_string_field() {
        let line = r#"logs,source=app message="hello world" 1000000000"#;
        let parsed = parse_ilp_line(line).unwrap();

        assert_eq!(
            parsed.fields.get("message").unwrap(),
            &IlpValue::String("hello world".to_string())
        );
    }

    #[test]
    fn test_boolean_field() {
        let line = "status,host=h1 alive=true,dead=false 1000";
        let parsed = parse_ilp_line(line).unwrap();

        assert_eq!(
            parsed.fields.get("alive").unwrap(),
            &IlpValue::Boolean(true)
        );
        assert_eq!(
            parsed.fields.get("dead").unwrap(),
            &IlpValue::Boolean(false)
        );
    }

    #[test]
    fn test_no_tags() {
        let line = "cpu usage=0.5 1000";
        let parsed = parse_ilp_line(line).unwrap();

        assert_eq!(parsed.measurement, "cpu");
        assert!(parsed.tags.is_empty());
        assert_eq!(parsed.fields.get("usage").unwrap(), &IlpValue::Float(0.5));
    }

    #[test]
    fn test_no_timestamp() {
        let line = "cpu,host=h1 usage=0.5";
        let parsed = parse_ilp_line(line).unwrap();

        assert_eq!(parsed.measurement, "cpu");
        assert!(parsed.timestamp.is_none());
    }

    #[test]
    fn test_multiple_tags_and_fields() {
        let line = "weather,city=nyc,state=ny temp=72.5,humidity=65i 999";
        let parsed = parse_ilp_line(line).unwrap();

        assert_eq!(parsed.tags.len(), 2);
        assert_eq!(parsed.tags.get("city").unwrap(), "nyc");
        assert_eq!(parsed.tags.get("state").unwrap(), "ny");
        assert_eq!(parsed.fields.len(), 2);
        assert_eq!(parsed.fields.get("temp").unwrap(), &IlpValue::Float(72.5));
        assert_eq!(
            parsed.fields.get("humidity").unwrap(),
            &IlpValue::Integer(65)
        );
    }

    #[test]
    fn test_empty_input() {
        assert_eq!(parse_ilp_line("").unwrap_err(), IlpParseError::EmptyInput);
    }

    #[test]
    fn test_comment_line() {
        assert_eq!(
            parse_ilp_line("# this is a comment").unwrap_err(),
            IlpParseError::EmptyInput
        );
    }

    #[test]
    fn test_missing_fields() {
        assert!(parse_ilp_line("cpu").is_err());
    }

    #[test]
    fn test_batch_parse() {
        let input = "cpu,host=a usage=0.5 1000\nmem,host=b total=8i 2000\n\n# comment\n";
        let lines = parse_ilp_batch(input).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].measurement, "cpu");
        assert_eq!(lines[1].measurement, "mem");
    }

    #[test]
    fn test_negative_integer() {
        let line = "sensor,id=1 temp=-10i 1000";
        let parsed = parse_ilp_line(line).unwrap();
        assert_eq!(parsed.fields.get("temp").unwrap(), &IlpValue::Integer(-10));
    }

    #[test]
    fn test_escaped_string_field() {
        let line = r#"logs message="say \"hello\"" 1000"#;
        let parsed = parse_ilp_line(line).unwrap();
        assert_eq!(
            parsed.fields.get("message").unwrap(),
            &IlpValue::String("say \"hello\"".to_string())
        );
    }

    #[test]
    fn test_boolean_variants() {
        let line = "check ok=T,fail=F 1000";
        let parsed = parse_ilp_line(line).unwrap();
        assert_eq!(parsed.fields.get("ok").unwrap(), &IlpValue::Boolean(true));
        assert_eq!(
            parsed.fields.get("fail").unwrap(),
            &IlpValue::Boolean(false)
        );
    }

    // ── ILP v2 typed field tests ────────────────────────────────────────

    #[test]
    fn test_v2_timestamp_field() {
        let line = "trades,sym=AAPL ts=1609459200000000000t 1000";
        let parsed = parse_ilp_line(line).unwrap();
        assert_eq!(
            parsed.fields.get("ts").unwrap(),
            &IlpValue::Timestamp(1609459200000000000)
        );
    }

    #[test]
    fn test_v2_symbol_field() {
        let line = "trades instrument=BTCUSD$ 1000";
        let parsed = parse_ilp_line(line).unwrap();
        assert_eq!(
            parsed.fields.get("instrument").unwrap(),
            &IlpValue::Symbol("BTCUSD".to_string())
        );
    }

    #[test]
    fn test_v2_long256_field() {
        let line = "tx hash=0xdeadbeef01234567n 1000";
        let parsed = parse_ilp_line(line).unwrap();
        assert_eq!(
            parsed.fields.get("hash").unwrap(),
            &IlpValue::Long256("deadbeef01234567".to_string())
        );
    }

    #[test]
    fn test_v2_mixed_typed_fields() {
        let line = "data,tag=v2 price=100.5,count=42i,sym=ETH$,ts=999t,hash=0xabn 1000";
        let parsed = parse_ilp_line(line).unwrap();
        assert_eq!(parsed.fields.get("price").unwrap(), &IlpValue::Float(100.5));
        assert_eq!(parsed.fields.get("count").unwrap(), &IlpValue::Integer(42));
        assert_eq!(
            parsed.fields.get("sym").unwrap(),
            &IlpValue::Symbol("ETH".to_string())
        );
        assert_eq!(parsed.fields.get("ts").unwrap(), &IlpValue::Timestamp(999));
        assert_eq!(
            parsed.fields.get("hash").unwrap(),
            &IlpValue::Long256("ab".to_string())
        );
    }

    // ── Protocol version detection tests ────────────────────────────────

    #[test]
    fn test_version_detect_v1() {
        let line = "cpu,host=h1 usage=0.5 1000";
        assert_eq!(IlpVersion::detect(line), IlpVersion::V1);
    }

    #[test]
    fn test_version_detect_v2_timestamp() {
        let line = "trades,sym=AAPL ts=1609459200000000000t 1000";
        assert_eq!(IlpVersion::detect(line), IlpVersion::V2);
    }

    #[test]
    fn test_version_detect_v2_symbol() {
        let line = "trades instrument=BTCUSD$ 1000";
        assert_eq!(IlpVersion::detect(line), IlpVersion::V2);
    }

    #[test]
    fn test_version_detect_v2_long256() {
        let line = "tx hash=0xdeadbeefn 1000";
        assert_eq!(IlpVersion::detect(line), IlpVersion::V2);
    }
}
