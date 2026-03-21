//! Fixed-point 128-bit decimal with configurable scale.

use crate::error::{ExchangeDbError, Result};
use std::fmt;
use std::ops::{Add, Div, Mul, Sub};
use std::str::FromStr;

/// A 128-bit fixed-point decimal number.
///
/// The value represented is `mantissa * 10^(-scale)`.
/// For example, `Decimal128 { mantissa: 123456, scale: 3 }` represents `123.456`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Decimal128 {
    pub mantissa: i128,
    pub scale: u8,
}

impl Decimal128 {
    /// Create a new `Decimal128` from mantissa and scale.
    pub fn new(mantissa: i128, scale: u8) -> Self {
        Self { mantissa, scale }
    }

    /// Parse a decimal string like "123.456" or "-0.001".
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.is_empty() {
            return Err(ExchangeDbError::Parse(
                "empty string cannot be parsed as Decimal128".to_string(),
            ));
        }

        let (integer_part, frac_part) = if let Some(dot_pos) = s.find('.') {
            (&s[..dot_pos], &s[dot_pos + 1..])
        } else {
            (s, "")
        };

        let scale = frac_part.len() as u8;

        // Build the full numeric string without the dot
        let combined = if frac_part.is_empty() {
            integer_part.to_string()
        } else {
            format!("{integer_part}{frac_part}")
        };

        let mantissa: i128 = combined
            .parse::<i128>()
            .map_err(|e| ExchangeDbError::Parse(format!("invalid Decimal128 string '{s}': {e}")))?;

        Ok(Self { mantissa, scale })
    }

    /// Convert to `f64`. May lose precision for very large mantissas.
    pub fn to_f64(self) -> f64 {
        self.mantissa as f64 / 10_f64.powi(self.scale as i32)
    }

    /// Round the decimal to the given number of decimal places.
    ///
    /// If `places >= self.scale`, returns self unchanged.
    pub fn round(self, places: u8) -> Self {
        if places >= self.scale {
            return self;
        }
        let diff = (self.scale - places) as u32;
        let divisor = 10_i128.pow(diff);
        let half = divisor / 2;
        let rounded = if self.mantissa >= 0 {
            (self.mantissa + half) / divisor
        } else {
            (self.mantissa - half) / divisor
        };
        Self {
            mantissa: rounded,
            scale: places,
        }
    }

    /// Rescale self to the target scale (adding or removing trailing zeros
    /// from the mantissa).
    fn rescale(self, target_scale: u8) -> Self {
        if self.scale == target_scale {
            return self;
        }
        if target_scale > self.scale {
            let diff = (target_scale - self.scale) as u32;
            Self {
                mantissa: self.mantissa * 10_i128.pow(diff),
                scale: target_scale,
            }
        } else {
            // Reducing scale — this truncates
            let diff = (self.scale - target_scale) as u32;
            Self {
                mantissa: self.mantissa / 10_i128.pow(diff),
                scale: target_scale,
            }
        }
    }
}

impl fmt::Display for Decimal128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scale == 0 {
            return write!(f, "{}", self.mantissa);
        }

        let is_negative = self.mantissa < 0;
        let abs_mantissa = self.mantissa.unsigned_abs();
        let divisor = 10_u128.pow(self.scale as u32);
        let integer = abs_mantissa / divisor;
        let frac = abs_mantissa % divisor;

        if is_negative {
            write!(f, "-{integer}.{frac:0>width$}", width = self.scale as usize)
        } else {
            write!(f, "{integer}.{frac:0>width$}", width = self.scale as usize)
        }
    }
}

impl Add for Decimal128 {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        let target_scale = self.scale.max(rhs.scale);
        let a = self.rescale(target_scale);
        let b = rhs.rescale(target_scale);
        Self {
            mantissa: a.mantissa + b.mantissa,
            scale: target_scale,
        }
    }
}

impl Sub for Decimal128 {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        let target_scale = self.scale.max(rhs.scale);
        let a = self.rescale(target_scale);
        let b = rhs.rescale(target_scale);
        Self {
            mantissa: a.mantissa - b.mantissa,
            scale: target_scale,
        }
    }
}

impl Mul for Decimal128 {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        Self {
            mantissa: self.mantissa * rhs.mantissa,
            scale: self.scale + rhs.scale,
        }
    }
}

impl Div for Decimal128 {
    type Output = Self;

    /// Division uses a fixed extra precision of 10 decimal places beyond the
    /// dividend's scale to reduce rounding error.
    fn div(self, rhs: Self) -> Self {
        assert!(rhs.mantissa != 0, "division by zero");
        // Add extra precision to the numerator before dividing
        let extra = 10u8;
        let result_scale = self.scale + extra;
        let scaled_mantissa = self.mantissa * 10_i128.pow((rhs.scale + extra) as u32);
        Self {
            mantissa: scaled_mantissa / rhs.mantissa,
            scale: result_scale,
        }
    }
}

impl FromStr for Decimal128 {
    type Err = ExchangeDbError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_display() {
        let d = Decimal128::parse("123.456").unwrap();
        assert_eq!(d.mantissa, 123456);
        assert_eq!(d.scale, 3);
        assert_eq!(d.to_string(), "123.456");
    }

    #[test]
    fn parse_integer() {
        let d = Decimal128::parse("42").unwrap();
        assert_eq!(d.mantissa, 42);
        assert_eq!(d.scale, 0);
        assert_eq!(d.to_string(), "42");
    }

    #[test]
    fn parse_negative() {
        let d = Decimal128::parse("-0.001").unwrap();
        assert_eq!(d.mantissa, -1);
        assert_eq!(d.scale, 3);
        assert_eq!(d.to_string(), "-0.001");
    }

    #[test]
    fn parse_invalid() {
        assert!(Decimal128::parse("").is_err());
        assert!(Decimal128::parse("abc").is_err());
        assert!(Decimal128::parse("12.34.56").is_err());
    }

    #[test]
    fn to_f64_conversion() {
        let d = Decimal128::parse("123.456").unwrap();
        assert!((d.to_f64() - 123.456).abs() < 1e-10);

        let d2 = Decimal128::parse("-99.99").unwrap();
        assert!((d2.to_f64() - (-99.99)).abs() < 1e-10);
    }

    #[test]
    fn addition() {
        let a = Decimal128::parse("1.5").unwrap();
        let b = Decimal128::parse("2.25").unwrap();
        let c = a + b;
        assert_eq!(c.to_string(), "3.75");
    }

    #[test]
    fn subtraction() {
        let a = Decimal128::parse("10.00").unwrap();
        let b = Decimal128::parse("3.50").unwrap();
        let c = a - b;
        assert_eq!(c.to_string(), "6.50");
    }

    #[test]
    fn multiplication() {
        let a = Decimal128::parse("2.5").unwrap();
        let b = Decimal128::parse("4.0").unwrap();
        let c = a * b;
        assert!((c.to_f64() - 10.0).abs() < 1e-10);
    }

    #[test]
    fn division() {
        let a = Decimal128::parse("10.0").unwrap();
        let b = Decimal128::parse("3.0").unwrap();
        let c = a / b;
        // Should be approximately 3.333...
        assert!((c.to_f64() - 3.333333333).abs() < 1e-6);
    }

    #[test]
    fn rounding() {
        let d = Decimal128::parse("3.15159").unwrap();
        let r = d.round(2);
        assert_eq!(r.to_string(), "3.15");

        let d2 = Decimal128::parse("2.555").unwrap();
        let r2 = d2.round(2);
        assert_eq!(r2.to_string(), "2.56");
    }

    #[test]
    fn round_no_change_if_fewer_places() {
        let d = Decimal128::parse("3.15").unwrap();
        let r = d.round(5);
        assert_eq!(r, d);
    }

    #[test]
    fn add_different_scales() {
        let a = Decimal128::parse("1.1").unwrap();
        let b = Decimal128::parse("2.222").unwrap();
        let c = a + b;
        assert_eq!(c.to_string(), "3.322");
    }

    #[test]
    fn display_leading_zeros_in_fraction() {
        let d = Decimal128::new(1001, 3);
        assert_eq!(d.to_string(), "1.001");
    }
}
