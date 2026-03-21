//! Compact value representation with Small String Optimization (SSO).
//!
//! The standard `Value::Str(String)` variant causes a heap allocation for
//! every string value in every row. For a 1M row scan with 1 string column,
//! that is 1M allocations. Most exchange symbols (BTC/USD, ETH/USD, etc.)
//! are well under 22 bytes.
//!
//! `CompactValue` stores strings up to 22 bytes inline, eliminating heap
//! allocations for the vast majority of string values in financial data.

use crate::plan::Value;

/// Maximum inline string length (bytes). Chosen so that `CompactValue`
/// fits in 32 bytes total (same as `Value` with a `String`).
const INLINE_MAX: usize = 22;

/// Compact value representation that avoids heap allocation for small strings.
///
/// Strings up to 22 bytes are stored inline (SSO - Small String Optimization).
/// This eliminates nearly all string allocations for typical exchange data
/// where ticker symbols like "BTC/USD" or "ETHUSD" are well under 22 bytes.
#[derive(Debug, Clone)]
pub enum CompactValue {
    Null,
    I64(i64),
    F64(f64),
    Timestamp(i64),
    /// Inline string (up to 22 bytes, no heap allocation).
    InlineStr([u8; INLINE_MAX], u8),
    /// Heap string (for strings > 22 bytes).
    HeapStr(String),
}

impl CompactValue {
    /// Create a `CompactValue` from a string slice. Strings up to 22 bytes
    /// are stored inline; longer strings are heap-allocated.
    #[inline]
    pub fn from_str(s: &str) -> Self {
        if s.len() <= INLINE_MAX {
            let mut buf = [0u8; INLINE_MAX];
            buf[..s.len()].copy_from_slice(s.as_bytes());
            Self::InlineStr(buf, s.len() as u8)
        } else {
            Self::HeapStr(s.to_string())
        }
    }

    /// Get the string value if this is a string variant.
    #[inline]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::InlineStr(buf, len) => {
                // SAFETY: The bytes were copied from a valid UTF-8 `&str` in
                // `from_str`, so they are guaranteed to be valid UTF-8.
                Some(unsafe { std::str::from_utf8_unchecked(&buf[..*len as usize]) })
            }
            Self::HeapStr(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Convert to the standard `Value` type.
    #[inline]
    pub fn to_value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::I64(v) => Value::I64(*v),
            Self::F64(v) => Value::F64(*v),
            Self::Timestamp(v) => Value::Timestamp(*v),
            Self::InlineStr(buf, len) => {
                let s = unsafe { std::str::from_utf8_unchecked(&buf[..*len as usize]) };
                Value::Str(s.to_string())
            }
            Self::HeapStr(s) => Value::Str(s.clone()),
        }
    }

    /// Create a `CompactValue` from a standard `Value`.
    #[inline]
    pub fn from_value(v: &Value) -> Self {
        match v {
            Value::Null => Self::Null,
            Value::I64(n) => Self::I64(*n),
            Value::F64(n) => Self::F64(*n),
            Value::Timestamp(n) => Self::Timestamp(*n),
            Value::Str(s) => Self::from_str(s),
        }
    }

    /// Compare two compact values with numeric type coercion.
    #[inline]
    pub fn cmp_coerce(&self, other: &CompactValue) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (CompactValue::I64(a), CompactValue::F64(b)) => (*a as f64).partial_cmp(b),
            (CompactValue::F64(a), CompactValue::I64(b)) => a.partial_cmp(&(*b as f64)),
            (CompactValue::I64(a), CompactValue::I64(b)) => a.partial_cmp(b),
            (CompactValue::F64(a), CompactValue::F64(b)) => a.partial_cmp(b),
            (CompactValue::Timestamp(a), CompactValue::Timestamp(b)) => a.partial_cmp(b),
            (CompactValue::Timestamp(a), CompactValue::I64(b)) => a.partial_cmp(b),
            (CompactValue::I64(a), CompactValue::Timestamp(b)) => a.partial_cmp(b),
            _ => None,
        }
    }

    /// Equality with numeric type coercion.
    #[inline]
    pub fn eq_coerce(&self, other: &CompactValue) -> bool {
        match (self, other) {
            (CompactValue::I64(a), CompactValue::F64(b)) => (*a as f64) == *b,
            (CompactValue::F64(a), CompactValue::I64(b)) => *a == (*b as f64),
            (CompactValue::I64(a), CompactValue::I64(b)) => a == b,
            (CompactValue::F64(a), CompactValue::F64(b)) => a == b,
            (CompactValue::Timestamp(a), CompactValue::Timestamp(b)) => a == b,
            (CompactValue::Null, CompactValue::Null) => true,
            _ => {
                // String comparison
                match (self.as_str(), other.as_str()) {
                    (Some(a), Some(b)) => a == b,
                    _ => false,
                }
            }
        }
    }

    /// Returns true if this value is Null.
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Extract f64 value (with coercion from i64).
    #[inline]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::F64(v) => Some(*v),
            Self::I64(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Extract i64 value.
    #[inline]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::I64(v) => Some(*v),
            Self::Timestamp(v) => Some(*v),
            _ => None,
        }
    }
}

impl PartialEq for CompactValue {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.eq_coerce(other)
    }
}

impl Eq for CompactValue {}

impl std::hash::Hash for CompactValue {
    #[inline]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Null => {}
            Self::I64(v) => v.hash(state),
            Self::F64(v) => v.to_bits().hash(state),
            Self::Timestamp(v) => v.hash(state),
            Self::InlineStr(_, len) => {
                let s = unsafe { std::str::from_utf8_unchecked(&self.as_str().unwrap().as_bytes()) };
                s.hash(state);
                len.hash(state);
            }
            Self::HeapStr(s) => s.hash(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sso_short_string() {
        let cv = CompactValue::from_str("BTC/USD");
        assert!(matches!(cv, CompactValue::InlineStr(_, 7)));
        assert_eq!(cv.as_str(), Some("BTC/USD"));
    }

    #[test]
    fn sso_exact_max() {
        let s = "a".repeat(INLINE_MAX);
        let cv = CompactValue::from_str(&s);
        assert!(matches!(cv, CompactValue::InlineStr(_, _)));
        assert_eq!(cv.as_str(), Some(s.as_str()));
    }

    #[test]
    fn sso_overflow_to_heap() {
        let s = "a".repeat(INLINE_MAX + 1);
        let cv = CompactValue::from_str(&s);
        assert!(matches!(cv, CompactValue::HeapStr(_)));
        assert_eq!(cv.as_str(), Some(s.as_str()));
    }

    #[test]
    fn roundtrip_value() {
        let original = Value::Str("ETH/USDT".to_string());
        let compact = CompactValue::from_value(&original);
        let back = compact.to_value();
        assert_eq!(original, back);
    }

    #[test]
    fn numeric_coercion() {
        let a = CompactValue::I64(42);
        let b = CompactValue::F64(42.0);
        assert!(a.eq_coerce(&b));
    }
}
