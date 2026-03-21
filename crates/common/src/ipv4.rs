//! IPv4 address utilities: parsing, formatting, and subnet checking.

use crate::error::{ExchangeDbError, Result};

/// Parse a dotted-decimal IPv4 string (e.g. "192.168.1.1") into a `u32`.
///
/// The most-significant byte holds the first octet, matching network byte
/// order (big-endian).
pub fn parse_ipv4(s: &str) -> Result<u32> {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 4 {
        return Err(ExchangeDbError::Parse(format!(
            "invalid IPv4 address: '{s}' (expected 4 octets)"
        )));
    }

    let mut ip: u32 = 0;
    for (i, part) in parts.iter().enumerate() {
        let octet: u8 = part.parse::<u8>().map_err(|_| {
            ExchangeDbError::Parse(format!("invalid IPv4 octet: '{part}' in address '{s}'"))
        })?;
        ip |= (octet as u32) << (24 - i * 8);
    }

    Ok(ip)
}

/// Format a `u32` as a dotted-decimal IPv4 string.
pub fn format_ipv4(ip: u32) -> String {
    format!(
        "{}.{}.{}.{}",
        (ip >> 24) & 0xFF,
        (ip >> 16) & 0xFF,
        (ip >> 8) & 0xFF,
        ip & 0xFF,
    )
}

/// Check whether `ip` belongs to the subnet defined by `subnet` and `mask`.
///
/// Returns `true` when `(ip & mask) == (subnet & mask)`.
pub fn ipv4_in_subnet(ip: u32, subnet: u32, mask: u32) -> bool {
    (ip & mask) == (subnet & mask)
}

/// Parse an IPv6 address string (e.g. "::1", "2001:db8::1") into a 16-byte array.
///
/// Supports full form ("2001:0db8:0000:0000:0000:0000:0000:0001") and
/// abbreviated form with `::` for consecutive zero groups.
pub fn parse_ipv6(s: &str) -> Result<[u8; 16]> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ExchangeDbError::Parse("empty IPv6 address".to_string()));
    }

    // Handle :: expansion
    let (left, right) = if let Some(pos) = s.find("::") {
        let left_str = &s[..pos];
        let right_str = &s[pos + 2..];
        let left_groups: Vec<&str> = if left_str.is_empty() {
            Vec::new()
        } else {
            left_str.split(':').collect()
        };
        let right_groups: Vec<&str> = if right_str.is_empty() {
            Vec::new()
        } else {
            right_str.split(':').collect()
        };
        let total = left_groups.len() + right_groups.len();
        if total > 8 {
            return Err(ExchangeDbError::Parse(format!(
                "invalid IPv6 address: '{s}' (too many groups)"
            )));
        }
        let zeros_needed = 8 - total;
        let mut all_groups = left_groups;
        all_groups.extend(std::iter::repeat_n("0", zeros_needed));
        all_groups.extend(right_groups);
        (all_groups, None)
    } else {
        let groups: Vec<&str> = s.split(':').collect();
        if groups.len() != 8 {
            return Err(ExchangeDbError::Parse(format!(
                "invalid IPv6 address: '{s}' (expected 8 groups, got {})",
                groups.len()
            )));
        }
        (groups, None)
    };

    let groups = if let Some(r) = right { r } else { left };

    let mut result = [0u8; 16];
    for (i, group) in groups.iter().enumerate() {
        if i >= 8 {
            return Err(ExchangeDbError::Parse(format!(
                "invalid IPv6 address: '{s}' (too many groups)"
            )));
        }
        let val = u16::from_str_radix(group, 16).map_err(|_| {
            ExchangeDbError::Parse(format!("invalid IPv6 group: '{group}' in address '{s}'"))
        })?;
        result[i * 2] = (val >> 8) as u8;
        result[i * 2 + 1] = (val & 0xFF) as u8;
    }

    Ok(result)
}

/// Format a 16-byte IPv6 address as a string.
///
/// Uses the longest run of consecutive zero groups to produce `::` notation
/// per RFC 5952.
pub fn format_ipv6(ip: &[u8; 16]) -> String {
    let groups: Vec<u16> = (0..8)
        .map(|i| u16::from_be_bytes([ip[i * 2], ip[i * 2 + 1]]))
        .collect();

    // Find the longest run of consecutive zero groups.
    let mut best_start = None;
    let mut best_len = 0usize;
    let mut cur_start = None;
    let mut cur_len = 0usize;

    for (i, &g) in groups.iter().enumerate() {
        if g == 0 {
            if cur_start.is_none() {
                cur_start = Some(i);
                cur_len = 1;
            } else {
                cur_len += 1;
            }
        } else {
            if cur_len > best_len && cur_len >= 2 {
                best_start = cur_start;
                best_len = cur_len;
            }
            cur_start = None;
            cur_len = 0;
        }
    }
    if cur_len > best_len && cur_len >= 2 {
        best_start = cur_start;
        best_len = cur_len;
    }

    if let Some(start) = best_start {
        let end = start + best_len;
        let left: Vec<String> = groups[..start].iter().map(|g| format!("{g:x}")).collect();
        let right: Vec<String> = groups[end..].iter().map(|g| format!("{g:x}")).collect();
        if left.is_empty() && right.is_empty() {
            "::".to_string()
        } else if left.is_empty() {
            format!("::{}", right.join(":"))
        } else if right.is_empty() {
            format!("{}::", left.join(":"))
        } else {
            format!("{}::{}", left.join(":"), right.join(":"))
        }
    } else {
        groups
            .iter()
            .map(|g| format!("{g:x}"))
            .collect::<Vec<_>>()
            .join(":")
    }
}

/// Check whether `ip` belongs to the IPv6 subnet defined by `subnet` and `prefix_len`.
pub fn ipv6_in_subnet(ip: &[u8; 16], subnet: &[u8; 16], prefix_len: u8) -> bool {
    let full_bytes = (prefix_len / 8) as usize;
    let remaining_bits = prefix_len % 8;

    for i in 0..full_bytes.min(16) {
        if ip[i] != subnet[i] {
            return false;
        }
    }

    if remaining_bits > 0 && full_bytes < 16 {
        let mask = 0xFF << (8 - remaining_bits);
        if (ip[full_bytes] & mask) != (subnet[full_bytes] & mask) {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_format_roundtrip() {
        let addrs = [
            "0.0.0.0",
            "127.0.0.1",
            "192.168.1.1",
            "255.255.255.255",
            "10.0.0.1",
        ];
        for addr in &addrs {
            let ip = parse_ipv4(addr).unwrap();
            assert_eq!(format_ipv4(ip), *addr);
        }
    }

    #[test]
    fn parse_specific_values() {
        assert_eq!(parse_ipv4("0.0.0.0").unwrap(), 0);
        assert_eq!(parse_ipv4("0.0.0.1").unwrap(), 1);
        assert_eq!(parse_ipv4("255.255.255.255").unwrap(), u32::MAX);
        assert_eq!(parse_ipv4("192.168.1.1").unwrap(), 0xC0A80101);
    }

    #[test]
    fn parse_invalid() {
        assert!(parse_ipv4("").is_err());
        assert!(parse_ipv4("1.2.3").is_err());
        assert!(parse_ipv4("1.2.3.4.5").is_err());
        assert!(parse_ipv4("256.0.0.0").is_err());
        assert!(parse_ipv4("abc.0.0.0").is_err());
    }

    #[test]
    fn subnet_check() {
        let ip = parse_ipv4("192.168.1.100").unwrap();
        let subnet = parse_ipv4("192.168.1.0").unwrap();
        let mask = parse_ipv4("255.255.255.0").unwrap();
        assert!(ipv4_in_subnet(ip, subnet, mask));

        let outside = parse_ipv4("192.168.2.100").unwrap();
        assert!(!ipv4_in_subnet(outside, subnet, mask));
    }

    #[test]
    fn subnet_wider_mask() {
        let ip = parse_ipv4("10.1.2.3").unwrap();
        let subnet = parse_ipv4("10.0.0.0").unwrap();
        let mask = parse_ipv4("255.0.0.0").unwrap();
        assert!(ipv4_in_subnet(ip, subnet, mask));

        let outside = parse_ipv4("11.1.2.3").unwrap();
        assert!(!ipv4_in_subnet(outside, subnet, mask));
    }

    // ── IPv6 tests ──────────────────────────────────────────────

    #[test]
    fn ipv6_parse_loopback() {
        let ip = parse_ipv6("::1").unwrap();
        let mut expected = [0u8; 16];
        expected[15] = 1;
        assert_eq!(ip, expected);
    }

    #[test]
    fn ipv6_parse_full() {
        let ip = parse_ipv6("2001:0db8:0000:0000:0000:0000:0000:0001").unwrap();
        assert_eq!(ip[0], 0x20);
        assert_eq!(ip[1], 0x01);
        assert_eq!(ip[2], 0x0d);
        assert_eq!(ip[3], 0xb8);
        assert_eq!(ip[15], 0x01);
    }

    #[test]
    fn ipv6_parse_abbreviated() {
        let ip = parse_ipv6("2001:db8::1").unwrap();
        assert_eq!(ip[0], 0x20);
        assert_eq!(ip[1], 0x01);
        assert_eq!(ip[15], 0x01);
        // Middle should be all zeros
        for i in 4..15 {
            assert_eq!(ip[i], 0, "byte {i} should be 0");
        }
    }

    #[test]
    fn ipv6_format_loopback() {
        let mut ip = [0u8; 16];
        ip[15] = 1;
        assert_eq!(format_ipv6(&ip), "::1");
    }

    #[test]
    fn ipv6_format_all_zeros() {
        let ip = [0u8; 16];
        assert_eq!(format_ipv6(&ip), "::");
    }

    #[test]
    fn ipv6_roundtrip() {
        let addrs = ["::1", "2001:db8::1", "fe80::1"];
        for addr in &addrs {
            let ip = parse_ipv6(addr).unwrap();
            let formatted = format_ipv6(&ip);
            let ip2 = parse_ipv6(&formatted).unwrap();
            assert_eq!(ip, ip2, "roundtrip failed for {addr}");
        }
    }

    #[test]
    fn ipv6_parse_invalid() {
        assert!(parse_ipv6("").is_err());
        assert!(parse_ipv6("1:2:3").is_err()); // too few groups, no ::
        assert!(parse_ipv6("gggg::1").is_err()); // invalid hex
    }

    #[test]
    fn ipv6_subnet_check() {
        let ip = parse_ipv6("2001:db8::1").unwrap();
        let subnet = parse_ipv6("2001:db8::").unwrap();
        assert!(ipv6_in_subnet(&ip, &subnet, 32));

        let outside = parse_ipv6("2001:db9::1").unwrap();
        assert!(!ipv6_in_subnet(&outside, &subnet, 32));
    }
}
