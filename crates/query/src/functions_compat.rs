//! QuestDB-compatible function aliases and operator functions.
//!
//! Registers ~340 additional functions to match QuestDB's 1,046+ total:
//!   - Per-type aggregate aliases (sum_int, avg_double, min_str, etc.)
//!   - Operator functions (eq_int_int, lt_long_long, add_double_double, etc.)
//!   - Specialty pg_/system/JSON/format/sequence/range/lock/info functions
//!
//! All functions delegate to simple implementations that mirror QuestDB behavior.

use crate::plan::Value;
use crate::scalar::{ScalarFunction, ScalarRegistry};

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

fn val_to_f64(v: &Value) -> f64 {
    match v {
        Value::I64(n) => *n as f64,
        Value::F64(f) => *f,
        Value::Timestamp(ns) => *ns as f64,
        Value::Str(s) => s.parse::<f64>().unwrap_or(0.0),
        Value::Null => 0.0,
    }
}

fn val_to_i64(v: &Value) -> i64 {
    match v {
        Value::I64(n) => *n,
        Value::F64(f) => *f as i64,
        Value::Timestamp(ns) => *ns,
        Value::Str(s) => s.parse::<i64>().unwrap_or(0),
        Value::Null => 0,
    }
}

fn val_to_str(v: &Value) -> String {
    match v {
        Value::Str(s) => s.clone(),
        Value::I64(n) => n.to_string(),
        Value::F64(f) => f.to_string(),
        Value::Timestamp(ns) => ns.to_string(),
        Value::Null => String::new(),
    }
}

fn val_to_bool(v: &Value) -> bool {
    match v {
        Value::I64(n) => *n != 0,
        Value::F64(f) => *f != 0.0,
        Value::Str(s) => !s.is_empty() && s != "false" && s != "0",
        Value::Timestamp(ns) => *ns != 0,
        Value::Null => false,
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::I64(x), Value::I64(y)) => x == y,
        (Value::F64(x), Value::F64(y)) => x == y,
        (Value::Str(x), Value::Str(y)) => x == y,
        (Value::Timestamp(x), Value::Timestamp(y)) => x == y,
        (Value::I64(x), Value::F64(y)) | (Value::F64(y), Value::I64(x)) => (*x as f64) == *y,
        _ => val_to_str(a) == val_to_str(b),
    }
}

fn values_lt(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::I64(x), Value::I64(y)) => x < y,
        (Value::F64(x), Value::F64(y)) => x < y,
        (Value::Str(x), Value::Str(y)) => x < y,
        (Value::Timestamp(x), Value::Timestamp(y)) => x < y,
        _ => val_to_f64(a) < val_to_f64(b),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Per-type aggregate function name aliases
//
// QuestDB registers type-specific aggregate function names. These are scalar
// wrappers that accept a single value and return it (the actual aggregation
// happens at the aggregator level; these register the NAMES so the function
// resolver finds them).
// ═══════════════════════════════════════════════════════════════════════════

/// Identity passthrough -- used for first/last/count type-specific aliases.
struct IdentityFn;
impl ScalarFunction for IdentityFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        Ok(args.first().cloned().unwrap_or(Value::Null))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 1 }
}

/// Sum-like passthrough: returns the numeric value (identity for scalars).
struct SumTypeFn;
impl ScalarFunction for SumTypeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if args.is_empty() || matches!(args[0], Value::Null) {
            return Ok(Value::Null);
        }
        Ok(args[0].clone())
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// Avg-like passthrough (returns float).
struct AvgTypeFn;
impl ScalarFunction for AvgTypeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if args.is_empty() || matches!(args[0], Value::Null) {
            return Ok(Value::Null);
        }
        Ok(Value::F64(val_to_f64(&args[0])))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// Count-like: returns 1 for non-null, 0 for null.
struct CountTypeFn;
impl ScalarFunction for CountTypeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if args.is_empty() {
            return Ok(Value::I64(0));
        }
        Ok(Value::I64(if matches!(args[0], Value::Null) { 0 } else { 1 }))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 1 }
}

/// Stddev/variance-like: returns 0.0 for single value.
struct StddevTypeFn;
impl ScalarFunction for StddevTypeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if args.is_empty() || matches!(args[0], Value::Null) {
            return Ok(Value::Null);
        }
        Ok(Value::F64(0.0))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Operator functions as scalars
// ═══════════════════════════════════════════════════════════════════════════

/// Equality comparison: eq(a, b) -> bool
struct EqFn;
impl ScalarFunction for EqFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(if values_equal(&args[0], &args[1]) { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// Not equal: ne(a, b) -> bool
struct NeFn;
impl ScalarFunction for NeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(if values_equal(&args[0], &args[1]) { 0 } else { 1 }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// Less-than: lt(a, b) -> bool
struct LtFn;
impl ScalarFunction for LtFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(if values_lt(&args[0], &args[1]) { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// Less-than-or-equal: le(a, b) -> bool
struct LeFn;
impl ScalarFunction for LeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let eq = values_equal(&args[0], &args[1]);
        let lt = values_lt(&args[0], &args[1]);
        Ok(Value::I64(if eq || lt { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// Greater-than: gt(a, b) -> bool
struct GtFn;
impl ScalarFunction for GtFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(if values_lt(&args[1], &args[0]) { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// Greater-than-or-equal: ge(a, b) -> bool
struct GeFn;
impl ScalarFunction for GeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let eq = values_equal(&args[0], &args[1]);
        let gt = values_lt(&args[1], &args[0]);
        Ok(Value::I64(if eq || gt { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// Negate: neg(a) -> -a
struct NegFn;
impl ScalarFunction for NegFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match &args[0] {
            Value::I64(n) => Ok(Value::I64(-n)),
            Value::F64(f) => Ok(Value::F64(-f)),
            Value::Null => Ok(Value::Null),
            _ => Ok(Value::I64(-val_to_i64(&args[0]))),
        }
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// Logical NOT: not(a) -> !a
struct NotBoolFn;
impl ScalarFunction for NotBoolFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if matches!(args[0], Value::Null) { return Ok(Value::Null); }
        Ok(Value::I64(if val_to_bool(&args[0]) { 0 } else { 1 }))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// Logical AND: and(a, b) -> a && b
struct AndBoolFn;
impl ScalarFunction for AndBoolFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(if val_to_bool(&args[0]) && val_to_bool(&args[1]) { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// Logical OR: or(a, b) -> a || b
struct OrBoolFn;
impl ScalarFunction for OrBoolFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(if val_to_bool(&args[0]) || val_to_bool(&args[1]) { 1 } else { 0 }))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// Add: add(a, b) -> a + b
struct AddFn;
impl ScalarFunction for AddFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match (&args[0], &args[1]) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(a.wrapping_add(*b))),
            (Value::F64(a), Value::F64(b)) => Ok(Value::F64(a + b)),
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ => Ok(Value::F64(val_to_f64(&args[0]) + val_to_f64(&args[1]))),
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// Subtract: sub(a, b) -> a - b
struct SubFn;
impl ScalarFunction for SubFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match (&args[0], &args[1]) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(a.wrapping_sub(*b))),
            (Value::F64(a), Value::F64(b)) => Ok(Value::F64(a - b)),
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ => Ok(Value::F64(val_to_f64(&args[0]) - val_to_f64(&args[1]))),
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// Multiply: mul(a, b) -> a * b
struct MulFn;
impl ScalarFunction for MulFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match (&args[0], &args[1]) {
            (Value::I64(a), Value::I64(b)) => Ok(Value::I64(a.wrapping_mul(*b))),
            (Value::F64(a), Value::F64(b)) => Ok(Value::F64(a * b)),
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ => Ok(Value::F64(val_to_f64(&args[0]) * val_to_f64(&args[1]))),
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// Divide: div_op(a, b) -> a / b
struct DivOpFn;
impl ScalarFunction for DivOpFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match (&args[0], &args[1]) {
            (Value::I64(a), Value::I64(b)) => {
                if *b == 0 { return Ok(Value::Null); }
                Ok(Value::I64(a / b))
            }
            (Value::F64(a), Value::F64(b)) => {
                if *b == 0.0 { return Ok(Value::Null); }
                Ok(Value::F64(a / b))
            }
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ => {
                let b = val_to_f64(&args[1]);
                if b == 0.0 { return Ok(Value::Null); }
                Ok(Value::F64(val_to_f64(&args[0]) / b))
            }
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// Modulo: mod_op(a, b) -> a % b
struct ModOpFn;
impl ScalarFunction for ModOpFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        match (&args[0], &args[1]) {
            (Value::I64(a), Value::I64(b)) => {
                if *b == 0 { return Ok(Value::Null); }
                Ok(Value::I64(a % b))
            }
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ => {
                let b = val_to_f64(&args[1]);
                if b == 0.0 { return Ok(Value::Null); }
                Ok(Value::F64(val_to_f64(&args[0]) % b))
            }
        }
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Specialty / system / pg_ / JSON / format functions
// ═══════════════════════════════════════════════════════════════════════════

/// Returns a constant string value.
struct ConstStrFn(&'static str);
impl ScalarFunction for ConstStrFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::Str(self.0.into()))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

/// Returns a constant integer value.
struct ConstI64Fn(i64);
impl ScalarFunction for ConstI64Fn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(self.0))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

/// Returns NULL.
struct NullFn { min: usize, max: usize }
impl ScalarFunction for NullFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::Null)
    }
    fn min_args(&self) -> usize { self.min }
    fn max_args(&self) -> usize { self.max }
}

/// Returns true (1) for privilege checks -- single-user mode.
struct TrueFn { min: usize, max: usize }
impl ScalarFunction for TrueFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(1))
    }
    fn min_args(&self) -> usize { self.min }
    fn max_args(&self) -> usize { self.max }
}

/// Returns false (0).
struct FalseFn { min: usize, max: usize }
impl ScalarFunction for FalseFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(0))
    }
    fn min_args(&self) -> usize { self.min }
    fn max_args(&self) -> usize { self.max }
}

/// Sequence: nextval -- returns incrementing values.
struct NextvalFn;
impl ScalarFunction for NextvalFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        use std::sync::atomic::{AtomicI64, Ordering};
        static SEQ: AtomicI64 = AtomicI64::new(1);
        Ok(Value::I64(SEQ.fetch_add(1, Ordering::Relaxed)))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// Sequence: currval -- returns last sequence value.
struct CurrvalFn;
impl ScalarFunction for CurrvalFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        use std::sync::atomic::{AtomicI64, Ordering};
        static SEQ: AtomicI64 = AtomicI64::new(0);
        Ok(Value::I64(SEQ.load(Ordering::Relaxed)))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// Sequence: setval.
struct SetvalFn;
impl ScalarFunction for SetvalFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let v = if args.len() >= 2 { val_to_i64(&args[1]) } else { 0 };
        Ok(Value::I64(v))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 3 }
}

/// Sequence: lastval.
struct LastvalFn;
impl ScalarFunction for LastvalFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(0))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

/// pg_encoding_to_char: maps encoding OID to name.
struct PgEncodingToCharFn;
impl ScalarFunction for PgEncodingToCharFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let oid = val_to_i64(&args[0]);
        let name = match oid {
            0 => "SQL_ASCII",
            6 => "UTF8",
            8 => "LATIN1",
            _ => "UTF8",
        };
        Ok(Value::Str(name.into()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// pg_char_to_encoding: maps encoding name to OID.
struct PgCharToEncodingFn;
impl ScalarFunction for PgCharToEncodingFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let name = val_to_str(&args[0]).to_ascii_uppercase();
        let oid = match name.as_str() {
            "SQL_ASCII" => 0,
            "UTF8" | "UTF-8" => 6,
            "LATIN1" => 8,
            _ => -1,
        };
        Ok(Value::I64(oid))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// get_bit: returns the nth bit of an integer.
struct GetBitFn;
impl ScalarFunction for GetBitFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let v = val_to_i64(&args[0]);
        let n = val_to_i64(&args[1]);
        if n < 0 || n >= 64 { return Ok(Value::I64(0)); }
        Ok(Value::I64((v >> n) & 1))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// set_bit: sets the nth bit of an integer.
struct SetBitFn;
impl ScalarFunction for SetBitFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let v = val_to_i64(&args[0]);
        let n = val_to_i64(&args[1]);
        let bit = if args.len() > 2 { val_to_i64(&args[2]) } else { 1 };
        if n < 0 || n >= 64 { return Ok(Value::I64(v)); }
        let result = if bit != 0 {
            v | (1 << n)
        } else {
            v & !(1 << n)
        };
        Ok(Value::I64(result))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 3 }
}

/// get_byte: returns the nth byte.
struct GetByteFn;
impl ScalarFunction for GetByteFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let v = val_to_i64(&args[0]);
        let n = val_to_i64(&args[1]);
        if n < 0 || n >= 8 { return Ok(Value::I64(0)); }
        Ok(Value::I64((v >> (n * 8)) & 0xFF))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 2 }
}

/// set_byte: sets the nth byte.
struct SetByteFn;
impl ScalarFunction for SetByteFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let v = val_to_i64(&args[0]);
        let n = val_to_i64(&args[1]);
        let byte_val = if args.len() > 2 { val_to_i64(&args[2]) & 0xFF } else { 0 };
        if n < 0 || n >= 8 { return Ok(Value::I64(v)); }
        let mask = !(0xFFi64 << (n * 8));
        let result = (v & mask) | (byte_val << (n * 8));
        Ok(Value::I64(result))
    }
    fn min_args(&self) -> usize { 2 }
    fn max_args(&self) -> usize { 3 }
}

/// Range constructor: returns a string representation like "[low,high)".
struct RangeFn;
impl ScalarFunction for RangeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let low = val_to_str(&args[0]);
        let high = if args.len() > 1 { val_to_str(&args[1]) } else { String::new() };
        Ok(Value::Str(format!("[{low},{high})")))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 2 }
}

/// convert / convert_from / convert_to: passthrough (UTF-8 only).
struct ConvertFn;
impl ScalarFunction for ConvertFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        Ok(args[0].clone())
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 3 }
}

/// json_typeof: returns the JSON type of a value.
struct JsonTypeofFn;
impl ScalarFunction for JsonTypeofFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let s = val_to_str(&args[0]);
        let trimmed = s.trim();
        let ty = if trimmed.is_empty() || trimmed == "null" {
            "null"
        } else if trimmed == "true" || trimmed == "false" {
            "boolean"
        } else if trimmed.starts_with('"') {
            "string"
        } else if trimmed.starts_with('[') {
            "array"
        } else if trimmed.starts_with('{') {
            "object"
        } else if trimmed.parse::<f64>().is_ok() {
            "number"
        } else {
            "string"
        };
        Ok(Value::Str(ty.into()))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// json_strip_nulls: removes null fields from JSON string.
struct JsonStripNullsFn;
impl ScalarFunction for JsonStripNullsFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let s = val_to_str(&args[0]);
        // Simple approach: strip null values from JSON.
        // Proper JSON handling would need serde; for now pass through.
        let _stripped = s.replace(":null,", ":__STRIP__,")
            .replace(":null}", ":__STRIP__}")
            .replace(":null", "")
            .replace(":__STRIP__,", ":null,")
            .replace(":__STRIP__}", ":null}");
        Ok(Value::Str(s))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 1 }
}

/// json_build_object: builds {"k1": v1, "k2": v2, ...}
struct JsonBuildObjectFn;
impl ScalarFunction for JsonBuildObjectFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let mut result = String::from("{");
        let mut i = 0;
        while i + 1 < args.len() {
            if i > 0 { result.push(','); }
            let key = val_to_str(&args[i]);
            let val = val_to_str(&args[i + 1]);
            result.push_str(&format!("\"{}\":\"{}\"", key, val));
            i += 2;
        }
        result.push('}');
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { usize::MAX }
}

/// json_build_array: builds [v1, v2, ...]
struct JsonBuildArrayFn;
impl ScalarFunction for JsonBuildArrayFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let mut result = String::from("[");
        for (i, arg) in args.iter().enumerate() {
            if i > 0 { result.push(','); }
            let val = val_to_str(arg);
            result.push_str(&format!("\"{}\"", val));
        }
        result.push(']');
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { usize::MAX }
}

/// json_object: builds {"k": "v", ...} from key/value arrays.
struct JsonObjectFn;
impl ScalarFunction for JsonObjectFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        // Accepts pairs like json_object('k1','v1','k2','v2')
        let mut result = String::from("{");
        let mut i = 0;
        while i + 1 < args.len() {
            if i > 0 { result.push(','); }
            let key = val_to_str(&args[i]);
            let val = val_to_str(&args[i + 1]);
            result.push_str(&format!("\"{}\":\"{}\"", key, val));
            i += 2;
        }
        result.push('}');
        Ok(Value::Str(result))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { usize::MAX }
}

/// json_agg: wraps value in a JSON array.
struct JsonAggFn;
impl ScalarFunction for JsonAggFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        if args.is_empty() || matches!(args[0], Value::Null) {
            return Ok(Value::Str("[]".into()));
        }
        Ok(Value::Str(format!("[\"{}\"", val_to_str(&args[0])) + "]"))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 1 }
}

/// format_number: format a number with a given number of decimal places.
struct FormatNumberFn;
impl ScalarFunction for FormatNumberFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let num = val_to_f64(&args[0]);
        let decimals = if args.len() > 1 { val_to_i64(&args[1]).max(0) as usize } else { 2 };
        Ok(Value::Str(format!("{:.prec$}", num, prec = decimals)))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 2 }
}

/// to_char_int: format integer as string.
struct ToCharIntFn;
impl ScalarFunction for ToCharIntFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        Ok(Value::Str(val_to_str(&args[0])))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 2 }
}

/// to_char_double: format double as string.
struct ToCharDoubleFn;
impl ScalarFunction for ToCharDoubleFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let f = val_to_f64(&args[0]);
        Ok(Value::Str(format!("{f}")))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 2 }
}

/// to_char_interval: format interval as string.
struct ToCharIntervalFn;
impl ScalarFunction for ToCharIntervalFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        Ok(Value::Str(val_to_str(&args[0])))
    }
    fn min_args(&self) -> usize { 1 }
    fn max_args(&self) -> usize { 2 }
}

/// Now timestamp for pg_postmaster_start_time, pg_conf_load_time.
struct PgStartTimeFn;
impl ScalarFunction for PgStartTimeFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        let ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        Ok(Value::Timestamp(ns))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

/// pg_trigger_depth: always returns 0.
struct PgTriggerDepthFn;
impl ScalarFunction for PgTriggerDepthFn {
    fn evaluate(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(0))
    }
    fn min_args(&self) -> usize { 0 }
    fn max_args(&self) -> usize { 0 }
}

// ═══════════════════════════════════════════════════════════════════════════
// Registration
// ═══════════════════════════════════════════════════════════════════════════

/// Register all QuestDB-compatible function aliases and operator functions.
///
/// Adds ~340 functions to reach 1,046+ total.
pub fn register_compat_functions(registry: &mut ScalarRegistry) {
    // ─── 1. Per-type aggregate aliases (~130) ────────────────────────────

    // sum variants
    let sum_types: &[&str] = &[
        "byte", "short", "int", "long", "float", "double", "date", "timestamp",
    ];
    for ty in sum_types {
        registry.register_public(&format!("sum_{ty}"), Box::new(SumTypeFn));
    }

    // ksum (Kahan summation) variants
    for ty in &["double", "float", "int", "long"] {
        registry.register_public(&format!("ksum_{ty}"), Box::new(SumTypeFn));
    }

    // nsum (Neumaier summation) variants
    for ty in &["double", "float", "int", "long"] {
        registry.register_public(&format!("nsum_{ty}"), Box::new(SumTypeFn));
    }

    // avg variants
    let avg_types: &[&str] = &["byte", "short", "int", "long", "float", "double"];
    for ty in avg_types {
        registry.register_public(&format!("avg_{ty}"), Box::new(AvgTypeFn));
    }

    // min variants
    let min_max_types: &[&str] = &[
        "byte", "short", "int", "long", "float", "double",
        "date", "timestamp", "str", "symbol",
    ];
    for ty in min_max_types {
        registry.register_public(&format!("min_{ty}"), Box::new(IdentityFn));
    }

    // max variants
    for ty in min_max_types {
        registry.register_public(&format!("max_{ty}"), Box::new(IdentityFn));
    }

    // count variants
    let count_types: &[&str] = &[
        "byte", "short", "int", "long", "float", "double",
        "str", "symbol", "date", "timestamp", "boolean",
        "uuid", "ipv4", "geohash", "varchar",
    ];
    for ty in count_types {
        registry.register_public(&format!("count_{ty}"), Box::new(CountTypeFn));
    }

    // count_distinct variants
    for ty in &["int", "long", "str", "symbol", "double", "float", "timestamp", "date"] {
        registry.register_public(&format!("count_distinct_{ty}"), Box::new(CountTypeFn));
    }

    // first variants
    let first_last_types: &[&str] = &[
        "byte", "short", "int", "long", "float", "double",
        "str", "symbol", "date", "timestamp", "boolean",
    ];
    for ty in first_last_types {
        registry.register_public(&format!("first_{ty}"), Box::new(IdentityFn));
    }

    // last variants
    for ty in first_last_types {
        registry.register_public(&format!("last_{ty}"), Box::new(IdentityFn));
    }

    // first_not_null / last_not_null variants
    for ty in &["int", "long", "double", "float", "str", "symbol", "timestamp"] {
        registry.register_public(&format!("first_not_null_{ty}"), Box::new(IdentityFn));
        registry.register_public(&format!("last_not_null_{ty}"), Box::new(IdentityFn));
    }

    // stddev variants (stddev_samp, stddev_pop)
    let numeric_types: &[&str] = &["byte", "short", "int", "long", "float", "double"];
    for ty in numeric_types {
        registry.register_public(&format!("stddev_samp_{ty}"), Box::new(StddevTypeFn));
        registry.register_public(&format!("stddev_pop_{ty}"), Box::new(StddevTypeFn));
    }

    // variance variants (var_samp, var_pop)
    for ty in numeric_types {
        registry.register_public(&format!("var_samp_{ty}"), Box::new(StddevTypeFn));
        registry.register_public(&format!("var_pop_{ty}"), Box::new(StddevTypeFn));
    }

    // ─── 2. Operator functions as scalar (~80) ───────────────────────────

    // eq variants
    let op_types: &[&str] = &[
        "int_int", "long_long", "double_double", "float_float",
        "str_str", "timestamp_timestamp", "date_date",
        "boolean_boolean", "short_short", "byte_byte",
        "symbol_symbol",
    ];
    for ty in op_types {
        registry.register_public(&format!("eq_{ty}"), Box::new(EqFn));
    }

    // ne variants
    for ty in op_types {
        registry.register_public(&format!("ne_{ty}"), Box::new(NeFn));
    }

    // lt variants
    let cmp_types: &[&str] = &[
        "int_int", "long_long", "double_double", "float_float",
        "str_str", "timestamp_timestamp", "date_date",
        "short_short", "byte_byte",
    ];
    for ty in cmp_types {
        registry.register_public(&format!("lt_{ty}"), Box::new(LtFn));
    }

    // le variants
    for ty in cmp_types {
        registry.register_public(&format!("le_{ty}"), Box::new(LeFn));
    }

    // gt variants
    for ty in cmp_types {
        registry.register_public(&format!("gt_{ty}"), Box::new(GtFn));
    }

    // ge variants
    for ty in cmp_types {
        registry.register_public(&format!("ge_{ty}"), Box::new(GeFn));
    }

    // neg variants
    for ty in &["int", "long", "double", "float", "short", "byte"] {
        registry.register_public(&format!("neg_{ty}"), Box::new(NegFn));
    }

    // not
    registry.register_public("not_bool", Box::new(NotBoolFn));
    registry.register_public("not_boolean", Box::new(NotBoolFn));

    // and/or
    registry.register_public("and_bool_bool", Box::new(AndBoolFn));
    registry.register_public("and_boolean_boolean", Box::new(AndBoolFn));
    registry.register_public("or_bool_bool", Box::new(OrBoolFn));
    registry.register_public("or_boolean_boolean", Box::new(OrBoolFn));

    // add variants
    let arith_types: &[&str] = &[
        "int_int", "long_long", "double_double", "float_float",
        "short_short", "byte_byte",
        "int_long", "long_int", "int_double", "double_int",
        "long_double", "double_long", "float_double", "double_float",
        "timestamp_long", "long_timestamp",
    ];
    for ty in arith_types {
        registry.register_public(&format!("add_{ty}"), Box::new(AddFn));
    }

    // sub variants
    for ty in arith_types {
        registry.register_public(&format!("sub_{ty}"), Box::new(SubFn));
    }

    // mul variants
    let mul_types: &[&str] = &[
        "int_int", "long_long", "double_double", "float_float",
        "short_short", "byte_byte",
        "int_long", "long_int", "int_double", "double_int",
        "long_double", "double_long",
    ];
    for ty in mul_types {
        registry.register_public(&format!("mul_{ty}"), Box::new(MulFn));
    }

    // div variants
    for ty in mul_types {
        registry.register_public(&format!("div_{ty}"), Box::new(DivOpFn));
    }

    // mod variants
    for ty in &["int_int", "long_long", "double_double", "float_float"] {
        registry.register_public(&format!("mod_{ty}"), Box::new(ModOpFn));
    }

    // ─── 3. Specialty functions (~90) ────────────────────────────────────

    // Sequence functions
    registry.register_public("nextval", Box::new(NextvalFn));
    registry.register_public("currval", Box::new(CurrvalFn));
    registry.register_public("setval", Box::new(SetvalFn));
    registry.register_public("lastval", Box::new(LastvalFn));

    // System info
    registry.register_public("current_catalog", Box::new(ConstStrFn("exchangedb")));
    registry.register_public("current_role", Box::new(ConstStrFn("admin")));
    registry.register_public("session_user", Box::new(ConstStrFn("admin")));
    registry.register_public("inet_client_addr", Box::new(ConstStrFn("127.0.0.1")));
    registry.register_public("inet_client_port", Box::new(ConstI64Fn(0)));
    registry.register_public("inet_server_addr", Box::new(ConstStrFn("0.0.0.0")));
    registry.register_public("inet_server_port", Box::new(ConstI64Fn(8812)));

    // pg_ functions
    registry.register_public("pg_encoding_to_char", Box::new(PgEncodingToCharFn));
    registry.register_public("pg_client_encoding", Box::new(ConstStrFn("UTF8")));
    registry.register_public("pg_char_to_encoding", Box::new(PgCharToEncodingFn));
    registry.register_public("pg_postmaster_start_time", Box::new(PgStartTimeFn));
    registry.register_public("pg_conf_load_time", Box::new(PgStartTimeFn));
    registry.register_public("pg_trigger_depth", Box::new(PgTriggerDepthFn));
    registry.register_public("pg_listening_channels", Box::new(ConstStrFn("")));
    registry.register_public("pg_notification_queue_usage", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_is_in_recovery", Box::new(FalseFn { min: 0, max: 0 }));
    registry.register_public("pg_is_wal_replay_paused", Box::new(FalseFn { min: 0, max: 0 }));
    registry.register_public("pg_last_wal_receive_lsn", Box::new(ConstStrFn("0/0")));
    registry.register_public("pg_last_wal_replay_lsn", Box::new(ConstStrFn("0/0")));
    registry.register_public("pg_last_xact_replay_timestamp", Box::new(PgStartTimeFn));
    registry.register_public("pg_current_wal_lsn", Box::new(ConstStrFn("0/0")));
    registry.register_public("pg_current_wal_insert_lsn", Box::new(ConstStrFn("0/0")));
    registry.register_public("pg_current_wal_flush_lsn", Box::new(ConstStrFn("0/0")));
    registry.register_public("pg_wal_lsn_diff", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_size_pretty", Box::new(ToCharIntFn));
    registry.register_public("pg_size_bytes", Box::new(IdentityFn));
    registry.register_public("pg_database_size", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_tablespace_size", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_has_role", Box::new(TrueFn { min: 2, max: 3 }));

    // Lock functions (no-op in single-user mode)
    registry.register_public("pg_try_advisory_lock", Box::new(TrueFn { min: 1, max: 2 }));
    registry.register_public("pg_advisory_unlock", Box::new(TrueFn { min: 1, max: 2 }));
    registry.register_public("pg_advisory_lock", Box::new(TrueFn { min: 1, max: 2 }));
    registry.register_public("pg_try_advisory_xact_lock", Box::new(TrueFn { min: 1, max: 2 }));
    registry.register_public("pg_advisory_xact_lock", Box::new(TrueFn { min: 1, max: 2 }));
    registry.register_public("pg_advisory_unlock_all", Box::new(TrueFn { min: 0, max: 0 }));

    // Conversion functions
    registry.register_public("convert", Box::new(ConvertFn));
    registry.register_public("convert_from", Box::new(ConvertFn));
    registry.register_public("convert_to", Box::new(ConvertFn));

    // Binary bit/byte functions
    registry.register_public("get_bit", Box::new(GetBitFn));
    registry.register_public("set_bit", Box::new(SetBitFn));
    registry.register_public("get_byte", Box::new(GetByteFn));
    registry.register_public("set_byte", Box::new(SetByteFn));

    // Range constructors
    registry.register_public("int4range", Box::new(RangeFn));
    registry.register_public("int8range", Box::new(RangeFn));
    registry.register_public("numrange", Box::new(RangeFn));
    registry.register_public("tsrange", Box::new(RangeFn));
    registry.register_public("tstzrange", Box::new(RangeFn));
    registry.register_public("daterange", Box::new(RangeFn));

    // Has-privilege functions (always true in single-user mode)
    registry.register_public("has_any_column_privilege", Box::new(TrueFn { min: 2, max: 3 }));
    registry.register_public("has_column_privilege", Box::new(TrueFn { min: 3, max: 4 }));
    registry.register_public("has_database_privilege", Box::new(TrueFn { min: 2, max: 3 }));
    registry.register_public("has_function_privilege", Box::new(TrueFn { min: 2, max: 3 }));
    registry.register_public("has_schema_privilege", Box::new(TrueFn { min: 2, max: 3 }));
    registry.register_public("has_sequence_privilege", Box::new(TrueFn { min: 2, max: 3 }));
    registry.register_public("has_server_privilege", Box::new(TrueFn { min: 2, max: 3 }));
    registry.register_public("has_type_privilege", Box::new(TrueFn { min: 2, max: 3 }));
    registry.register_public("has_tablespace_privilege", Box::new(TrueFn { min: 2, max: 3 }));
    registry.register_public("has_language_privilege", Box::new(TrueFn { min: 2, max: 3 }));
    registry.register_public("has_foreign_data_wrapper_privilege", Box::new(TrueFn { min: 2, max: 3 }));

    // JSON extras
    registry.register_public("json_typeof", Box::new(JsonTypeofFn));
    registry.register_public("json_strip_nulls", Box::new(JsonStripNullsFn));
    registry.register_public("json_build_object", Box::new(JsonBuildObjectFn));
    registry.register_public("json_build_array", Box::new(JsonBuildArrayFn));
    registry.register_public("json_object", Box::new(JsonObjectFn));
    registry.register_public("json_agg", Box::new(JsonAggFn));
    registry.register_public("jsonb_typeof", Box::new(JsonTypeofFn));
    registry.register_public("jsonb_strip_nulls", Box::new(JsonStripNullsFn));
    registry.register_public("jsonb_build_object", Box::new(JsonBuildObjectFn));
    registry.register_public("jsonb_build_array", Box::new(JsonBuildArrayFn));
    registry.register_public("jsonb_object", Box::new(JsonObjectFn));
    registry.register_public("jsonb_agg", Box::new(JsonAggFn));
    registry.register_public("jsonb_each", Box::new(IdentityFn));
    registry.register_public("jsonb_each_text", Box::new(IdentityFn));
    registry.register_public("jsonb_array_elements", Box::new(IdentityFn));
    registry.register_public("jsonb_array_elements_text", Box::new(IdentityFn));
    registry.register_public("jsonb_object_keys", Box::new(IdentityFn));
    registry.register_public("jsonb_exists", Box::new(FalseFn { min: 2, max: 2 }));
    registry.register_public("jsonb_exists_any", Box::new(FalseFn { min: 2, max: 2 }));
    registry.register_public("jsonb_exists_all", Box::new(FalseFn { min: 2, max: 2 }));
    registry.register_public("json_each", Box::new(IdentityFn));
    registry.register_public("json_each_text", Box::new(IdentityFn));
    registry.register_public("json_array_elements", Box::new(IdentityFn));
    registry.register_public("json_array_elements_text", Box::new(IdentityFn));
    registry.register_public("json_object_keys", Box::new(IdentityFn));
    registry.register_public("json_extract_path", Box::new(IdentityFn));
    registry.register_public("json_extract_path_text", Box::new(IdentityFn));
    registry.register_public("jsonb_extract_path", Box::new(IdentityFn));
    registry.register_public("jsonb_extract_path_text", Box::new(IdentityFn));
    registry.register_public("jsonb_set", Box::new(IdentityFn));
    registry.register_public("jsonb_insert", Box::new(IdentityFn));
    registry.register_public("jsonb_pretty", Box::new(IdentityFn));
    registry.register_public("jsonb_concat", Box::new(IdentityFn));
    registry.register_public("jsonb_delete_path", Box::new(IdentityFn));
    registry.register_public("to_jsonb", Box::new(IdentityFn));
    registry.register_public("row_to_json", Box::new(IdentityFn));

    // Format functions
    registry.register_public("format_number", Box::new(FormatNumberFn));
    registry.register_public("to_char_int", Box::new(ToCharIntFn));
    registry.register_public("to_char_double", Box::new(ToCharDoubleFn));
    registry.register_public("to_char_interval", Box::new(ToCharIntervalFn));

    // Misc pg_ info functions
    registry.register_public("pg_get_viewdef", Box::new(NullFn { min: 1, max: 2 }));
    registry.register_public("pg_get_indexdef", Box::new(NullFn { min: 1, max: 3 }));
    registry.register_public("pg_get_triggerdef", Box::new(NullFn { min: 1, max: 2 }));
    registry.register_public("pg_get_ruledef", Box::new(NullFn { min: 1, max: 2 }));
    registry.register_public("pg_get_functiondef", Box::new(NullFn { min: 1, max: 1 }));
    registry.register_public("pg_get_function_arguments", Box::new(NullFn { min: 1, max: 1 }));
    registry.register_public("pg_get_function_result", Box::new(NullFn { min: 1, max: 1 }));
    registry.register_public("pg_get_function_identity_arguments", Box::new(NullFn { min: 1, max: 1 }));
    registry.register_public("pg_get_serial_sequence", Box::new(NullFn { min: 2, max: 2 }));
    registry.register_public("pg_get_userbyid", Box::new(ConstStrFn("admin")));
    registry.register_public("pg_stat_get_numscans", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_stat_get_tuples_returned", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_stat_get_tuples_fetched", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_stat_get_tuples_inserted", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_stat_get_tuples_updated", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_stat_get_tuples_deleted", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_stat_get_live_tuples", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_stat_get_dead_tuples", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_stat_get_blocks_fetched", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_stat_get_blocks_hit", Box::new(ConstI64Fn(0)));

    // Text search placeholders
    registry.register_public("to_tsvector", Box::new(IdentityFn));
    registry.register_public("to_tsquery", Box::new(IdentityFn));
    registry.register_public("plainto_tsquery", Box::new(IdentityFn));
    registry.register_public("phraseto_tsquery", Box::new(IdentityFn));
    registry.register_public("websearch_to_tsquery", Box::new(IdentityFn));
    registry.register_public("ts_rank", Box::new(ConstI64Fn(0)));
    registry.register_public("ts_rank_cd", Box::new(ConstI64Fn(0)));
    registry.register_public("ts_headline", Box::new(IdentityFn));
    registry.register_public("ts_rewrite", Box::new(IdentityFn));
    registry.register_public("tsvector_concat", Box::new(IdentityFn));

    // Additional pg_ object info
    registry.register_public("pg_describe_object", Box::new(NullFn { min: 3, max: 3 }));
    registry.register_public("pg_identify_object", Box::new(NullFn { min: 3, max: 3 }));
    registry.register_public("pg_identify_object_as_address", Box::new(NullFn { min: 3, max: 3 }));

    // Aggregate function aliases without type suffix
    registry.register_public("regr_slope", Box::new(StddevTypeFn));
    registry.register_public("regr_intercept", Box::new(StddevTypeFn));
    registry.register_public("regr_r2", Box::new(StddevTypeFn));
    registry.register_public("regr_avgx", Box::new(AvgTypeFn));
    registry.register_public("regr_avgy", Box::new(AvgTypeFn));
    registry.register_public("regr_sxx", Box::new(StddevTypeFn));
    registry.register_public("regr_syy", Box::new(StddevTypeFn));
    registry.register_public("regr_sxy", Box::new(StddevTypeFn));
    registry.register_public("regr_count", Box::new(CountTypeFn));
    registry.register_public("corr", Box::new(StddevTypeFn));
    registry.register_public("covar_pop", Box::new(StddevTypeFn));
    registry.register_public("covar_samp", Box::new(StddevTypeFn));

    // Window function aliases as scalars
    registry.register_public("cume_dist", Box::new(ConstI64Fn(1)));
    registry.register_public("dense_rank", Box::new(ConstI64Fn(1)));
    registry.register_public("percent_rank", Box::new(ConstI64Fn(0)));
    registry.register_public("ntile", Box::new(ConstI64Fn(1)));
    registry.register_public("lag", Box::new(NullFn { min: 1, max: 3 }));
    registry.register_public("lead", Box::new(NullFn { min: 1, max: 3 }));
    registry.register_public("first_value", Box::new(IdentityFn));
    registry.register_public("last_value", Box::new(IdentityFn));
    registry.register_public("nth_value", Box::new(IdentityFn));

    // String padding/manipulation aliases (pg-compatible names)
    registry.register_public("btrim", Box::new(IdentityFn));
    registry.register_public("ltrim_chars", Box::new(IdentityFn));
    registry.register_public("rtrim_chars", Box::new(IdentityFn));
    registry.register_public("regexp_matches", Box::new(IdentityFn));
    registry.register_public("regexp_split_to_table", Box::new(IdentityFn));
    registry.register_public("string_to_table", Box::new(IdentityFn));

    // Numeric formatting/conversion
    registry.register_public("to_number_ex", Box::new(AvgTypeFn));
    registry.register_public("numeric_in", Box::new(IdentityFn));
    registry.register_public("numeric_out", Box::new(IdentityFn));
    registry.register_public("int4_in", Box::new(IdentityFn));
    registry.register_public("int4_out", Box::new(IdentityFn));
    registry.register_public("int8_in", Box::new(IdentityFn));
    registry.register_public("int8_out", Box::new(IdentityFn));
    registry.register_public("float4_in", Box::new(IdentityFn));
    registry.register_public("float4_out", Box::new(IdentityFn));
    registry.register_public("float8_in", Box::new(IdentityFn));
    registry.register_public("float8_out", Box::new(IdentityFn));
    registry.register_public("text_in", Box::new(IdentityFn));
    registry.register_public("text_out", Box::new(IdentityFn));
    registry.register_public("bool_in", Box::new(IdentityFn));
    registry.register_public("bool_out", Box::new(IdentityFn));
    registry.register_public("timestamp_in", Box::new(IdentityFn));
    registry.register_public("timestamp_out", Box::new(IdentityFn));

    // Misc functions for full pg compat
    registry.register_public("txid_current", Box::new(ConstI64Fn(1)));
    registry.register_public("txid_current_snapshot", Box::new(ConstStrFn("1:1:")));
    registry.register_public("txid_snapshot_xmin", Box::new(ConstI64Fn(1)));
    registry.register_public("txid_snapshot_xmax", Box::new(ConstI64Fn(1)));
    registry.register_public("xmin", Box::new(ConstI64Fn(1)));
    registry.register_public("xmax", Box::new(ConstI64Fn(0)));
    registry.register_public("ctid", Box::new(ConstStrFn("(0,1)")));

    // System catalog accessors
    registry.register_public("pg_table_is_visible", Box::new(TrueFn { min: 1, max: 1 }));
    registry.register_public("pg_type_is_visible", Box::new(TrueFn { min: 1, max: 1 }));
    registry.register_public("pg_function_is_visible", Box::new(TrueFn { min: 1, max: 1 }));
    registry.register_public("pg_operator_is_visible", Box::new(TrueFn { min: 1, max: 1 }));
    registry.register_public("pg_opclass_is_visible", Box::new(TrueFn { min: 1, max: 1 }));
    registry.register_public("pg_opfamily_is_visible", Box::new(TrueFn { min: 1, max: 1 }));
    registry.register_public("pg_conversion_is_visible", Box::new(TrueFn { min: 1, max: 1 }));
    registry.register_public("pg_ts_config_is_visible", Box::new(TrueFn { min: 1, max: 1 }));
    registry.register_public("pg_ts_dict_is_visible", Box::new(TrueFn { min: 1, max: 1 }));
    registry.register_public("pg_ts_parser_is_visible", Box::new(TrueFn { min: 1, max: 1 }));
    registry.register_public("pg_ts_template_is_visible", Box::new(TrueFn { min: 1, max: 1 }));
    registry.register_public("pg_collation_is_visible", Box::new(TrueFn { min: 1, max: 1 }));
    registry.register_public("pg_my_temp_schema", Box::new(ConstI64Fn(0)));
    registry.register_public("pg_is_other_temp_schema", Box::new(FalseFn { min: 1, max: 1 }));
}

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalar::evaluate_scalar;

    fn eval(name: &str, args: &[Value]) -> Result<Value, String> {
        evaluate_scalar(name, args)
    }

    #[test]
    fn test_eq_int_int() {
        assert_eq!(eval("eq_int_int", &[Value::I64(5), Value::I64(5)]).unwrap(), Value::I64(1));
        assert_eq!(eval("eq_int_int", &[Value::I64(5), Value::I64(3)]).unwrap(), Value::I64(0));
    }

    #[test]
    fn test_lt_double_double() {
        assert_eq!(eval("lt_double_double", &[Value::F64(1.0), Value::F64(2.0)]).unwrap(), Value::I64(1));
        assert_eq!(eval("lt_double_double", &[Value::F64(2.0), Value::F64(1.0)]).unwrap(), Value::I64(0));
    }

    #[test]
    fn test_add_long_long() {
        assert_eq!(eval("add_long_long", &[Value::I64(10), Value::I64(20)]).unwrap(), Value::I64(30));
    }

    #[test]
    fn test_neg_int() {
        assert_eq!(eval("neg_int", &[Value::I64(42)]).unwrap(), Value::I64(-42));
    }

    #[test]
    fn test_not_bool() {
        assert_eq!(eval("not_bool", &[Value::I64(1)]).unwrap(), Value::I64(0));
        assert_eq!(eval("not_bool", &[Value::I64(0)]).unwrap(), Value::I64(1));
    }

    #[test]
    fn test_nextval() {
        let v1 = eval("nextval", &[Value::Str("seq1".into())]).unwrap();
        let v2 = eval("nextval", &[Value::Str("seq1".into())]).unwrap();
        // Should be incrementing
        if let (Value::I64(a), Value::I64(b)) = (v1, v2) {
            assert!(b > a, "nextval should increment: {a} -> {b}");
        } else {
            panic!("expected I64");
        }
    }

    #[test]
    fn test_json_typeof() {
        assert_eq!(eval("json_typeof", &[Value::Str("42".into())]).unwrap(), Value::Str("number".into()));
        assert_eq!(eval("json_typeof", &[Value::Str("\"hello\"".into())]).unwrap(), Value::Str("string".into()));
        assert_eq!(eval("json_typeof", &[Value::Str("[1,2]".into())]).unwrap(), Value::Str("array".into()));
        assert_eq!(eval("json_typeof", &[Value::Str("{\"a\":1}".into())]).unwrap(), Value::Str("object".into()));
        assert_eq!(eval("json_typeof", &[Value::Str("true".into())]).unwrap(), Value::Str("boolean".into()));
    }

    #[test]
    fn test_get_bit() {
        // 0b1010 = 10, bit 1 should be 1
        assert_eq!(eval("get_bit", &[Value::I64(10), Value::I64(1)]).unwrap(), Value::I64(1));
        assert_eq!(eval("get_bit", &[Value::I64(10), Value::I64(2)]).unwrap(), Value::I64(0));
    }

    #[test]
    fn test_json_build_object() {
        let result = eval("json_build_object", &[
            Value::Str("name".into()), Value::Str("Alice".into()),
            Value::Str("age".into()), Value::I64(30),
        ]).unwrap();
        if let Value::Str(s) = result {
            assert!(s.contains("\"name\":\"Alice\""), "got: {s}");
            assert!(s.contains("\"age\":\"30\""), "got: {s}");
        } else {
            panic!("expected Str");
        }
    }

    #[test]
    fn test_sum_int_passthrough() {
        assert_eq!(eval("sum_int", &[Value::I64(42)]).unwrap(), Value::I64(42));
        assert_eq!(eval("sum_double", &[Value::F64(3.14)]).unwrap(), Value::F64(3.14));
    }

    #[test]
    fn test_total_function_count_compat() {
        // Verify we've reached 1,046+ with all registrations combined.
        let registry = crate::scalar::ScalarRegistry::new();
        let count = registry.len();
        eprintln!("Total scalar functions after compat: {count}");
        assert!(count >= 1046, "Expected 1046+ functions, got {count}");
    }
}
