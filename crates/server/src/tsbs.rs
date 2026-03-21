//! TSBS (Time Series Benchmark Suite) compatibility layer.
//!
//! Parses TSBS-formatted bulk insert lines (InfluxDB line protocol format)
//! into structured rows for ingestion into ExchangeDB.

/// A single parsed row from a TSBS bulk insert line.
#[derive(Debug, Clone, PartialEq)]
pub struct TsbsRow {
    /// Measurement name (e.g., "cpu", "mem", "disk").
    pub measurement: String,
    /// Tag key-value pairs.
    pub tags: Vec<(String, String)>,
    /// Field key-value pairs (numeric fields).
    pub fields: Vec<(String, f64)>,
    /// Timestamp in nanoseconds since Unix epoch.
    pub timestamp: i64,
}

/// Parse a TSBS-formatted line (InfluxDB line protocol) into a [`TsbsRow`].
///
/// The format is:
/// ```text
/// measurement,tag1=val1,tag2=val2 field1=1.0,field2=2.0 1609459200000000000
/// ```
///
/// Tags are separated from the measurement by a comma (no space). Fields
/// follow after a space. The timestamp is an optional trailing integer
/// separated by a space.
pub fn parse_tsbs_line(line: &str) -> Result<TsbsRow, String> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return Err("empty or comment line".into());
    }

    // Split into: measurement+tags, fields, timestamp
    // The tricky part is that the first space separates measurement+tags from
    // fields, and the last space before the end separates fields from timestamp.

    // Find the first unescaped space to split measurement+tags from the rest.
    let first_space = find_unescaped_space(line)
        .ok_or_else(|| "invalid line: no space found between tags and fields".to_string())?;

    let measurement_tags = &line[..first_space];
    let rest = &line[first_space + 1..];

    // Parse measurement and tags from "measurement,tag1=val1,tag2=val2".
    let (measurement, tags) = parse_measurement_tags(measurement_tags)?;

    // Split rest into fields and optional timestamp.
    let (fields_str, timestamp) = if let Some(last_space) = rest.rfind(' ') {
        let ts_part = &rest[last_space + 1..];
        if let Ok(ts) = ts_part.parse::<i64>() {
            (&rest[..last_space], ts)
        } else {
            // No valid timestamp; treat entire rest as fields.
            (rest, 0)
        }
    } else {
        (rest, 0)
    };

    let fields = parse_fields(fields_str)?;

    Ok(TsbsRow {
        measurement,
        tags,
        fields,
        timestamp,
    })
}

/// Find the first unescaped space in a line.
fn find_unescaped_space(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            i += 2; // skip escaped char
            continue;
        }
        if bytes[i] == b' ' {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Parse "measurement,tag1=val1,tag2=val2" into measurement name and tags.
fn parse_measurement_tags(s: &str) -> Result<(String, Vec<(String, String)>), String> {
    let parts: Vec<&str> = s.splitn(2, ',').collect();
    let measurement = parts[0].to_string();

    if measurement.is_empty() {
        return Err("empty measurement name".into());
    }

    let mut tags = Vec::new();
    if parts.len() > 1 && !parts[1].is_empty() {
        for tag_pair in parts[1].split(',') {
            let kv: Vec<&str> = tag_pair.splitn(2, '=').collect();
            if kv.len() != 2 {
                return Err(format!("invalid tag: {tag_pair}"));
            }
            tags.push((kv[0].to_string(), kv[1].to_string()));
        }
    }

    Ok((measurement, tags))
}

/// Parse "field1=1.0,field2=2.0" into field name-value pairs.
fn parse_fields(s: &str) -> Result<Vec<(String, f64)>, String> {
    let mut fields = Vec::new();

    for field_pair in s.split(',') {
        let kv: Vec<&str> = field_pair.splitn(2, '=').collect();
        if kv.len() != 2 {
            return Err(format!("invalid field: {field_pair}"));
        }

        let key = kv[0].to_string();
        // Strip trailing 'i' for integer fields (InfluxDB line protocol).
        let val_str = kv[1].trim_end_matches('i');
        let value: f64 = val_str
            .parse()
            .map_err(|e| format!("invalid field value for '{key}': {e}"))?;
        fields.push((key, value));
    }

    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_line() {
        let line = "cpu,host=server01,region=us-east usage_idle=98.5,usage_user=1.2 1609459200000000000";
        let row = parse_tsbs_line(line).unwrap();

        assert_eq!(row.measurement, "cpu");
        assert_eq!(
            row.tags,
            vec![
                ("host".into(), "server01".into()),
                ("region".into(), "us-east".into()),
            ]
        );
        assert_eq!(
            row.fields,
            vec![
                ("usage_idle".into(), 98.5),
                ("usage_user".into(), 1.2),
            ]
        );
        assert_eq!(row.timestamp, 1609459200000000000);
    }

    #[test]
    fn parse_no_tags() {
        let line = "mem free=1024.0 1609459200000000000";
        let row = parse_tsbs_line(line).unwrap();

        assert_eq!(row.measurement, "mem");
        assert!(row.tags.is_empty());
        assert_eq!(row.fields, vec![("free".into(), 1024.0)]);
        assert_eq!(row.timestamp, 1609459200000000000);
    }

    #[test]
    fn parse_integer_fields() {
        let line = "disk,host=server01 reads=100i,writes=50i 1609459200000000000";
        let row = parse_tsbs_line(line).unwrap();

        assert_eq!(row.fields, vec![("reads".into(), 100.0), ("writes".into(), 50.0)]);
    }

    #[test]
    fn parse_no_timestamp() {
        let line = "cpu,host=a usage=50.0";
        let row = parse_tsbs_line(line).unwrap();

        assert_eq!(row.measurement, "cpu");
        assert_eq!(row.tags, vec![("host".into(), "a".into())]);
        assert_eq!(row.fields, vec![("usage".into(), 50.0)]);
        assert_eq!(row.timestamp, 0);
    }

    #[test]
    fn parse_empty_line() {
        assert!(parse_tsbs_line("").is_err());
        assert!(parse_tsbs_line("# comment").is_err());
    }

    #[test]
    fn parse_multiple_fields() {
        let line = "cpu,host=h1 a=1.0,b=2.0,c=3.0 100";
        let row = parse_tsbs_line(line).unwrap();

        assert_eq!(row.fields.len(), 3);
        assert_eq!(row.fields[0], ("a".into(), 1.0));
        assert_eq!(row.fields[1], ("b".into(), 2.0));
        assert_eq!(row.fields[2], ("c".into(), 3.0));
        assert_eq!(row.timestamp, 100);
    }
}
