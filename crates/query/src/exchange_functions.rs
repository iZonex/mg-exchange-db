//! Exchange-domain scalar functions: OHLCV helpers, orderbook analytics,
//! and tick delta encoding/decoding.
//!
//! These make the `exchange-exchange` crate's functionality accessible from
//! SQL via `SELECT vwap(price, volume), mid_price(bid, ask), ...`.

use crate::plan::Value;
use crate::scalar::{ScalarFunction, ScalarRegistry};

// ---------------------------------------------------------------------------
// OHLCV helpers
// ---------------------------------------------------------------------------

/// `ohlcv_vwap(sum_price_volume, sum_volume)` -> VWAP as f64.
///
/// Computes Volume-Weighted Average Price: `sum(price * volume) / sum(volume)`.
/// Typically used with aggregate expressions:
///   SELECT ohlcv_vwap(sum(price * volume), sum(volume)) FROM trades SAMPLE BY 1m;
struct VwapFn;

impl ScalarFunction for VwapFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let sum_pv = match &args[0] {
            Value::F64(v) => *v,
            Value::I64(v) => *v as f64,
            Value::Null => return Ok(Value::Null),
            _ => return Err("vwap: first argument must be numeric".into()),
        };
        let sum_vol = match &args[1] {
            Value::F64(v) => *v,
            Value::I64(v) => *v as f64,
            Value::Null => return Ok(Value::Null),
            _ => return Err("vwap: second argument must be numeric".into()),
        };
        if sum_vol == 0.0 {
            return Ok(Value::Null);
        }
        Ok(Value::F64(sum_pv / sum_vol))
    }

    fn min_args(&self) -> usize {
        2
    }
    fn max_args(&self) -> usize {
        2
    }
}

/// `ohlcv_bar_align(timestamp_nanos, interval_str)` -> aligned timestamp.
///
/// Aligns a nanosecond timestamp to the start of the given OHLCV bar interval.
/// `interval_str`: "1s", "1m", "5m", "15m", "1h", "4h", "1d", "1w".
struct BarAlignFn;

impl ScalarFunction for BarAlignFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let ts = match &args[0] {
            Value::I64(v) => *v,
            Value::Null => return Ok(Value::Null),
            _ => {
                return Err(
                    "ohlcv_bar_align: first argument must be integer timestamp".into(),
                )
            }
        };
        let interval = match &args[1] {
            Value::Str(s) => s.as_str(),
            _ => {
                return Err(
                    "ohlcv_bar_align: second argument must be interval string".into(),
                )
            }
        };
        let tf = parse_timeframe(interval)
            .ok_or_else(|| format!("ohlcv_bar_align: unknown interval '{interval}'"))?;
        Ok(Value::I64(tf.truncate(ts)))
    }

    fn min_args(&self) -> usize {
        2
    }
    fn max_args(&self) -> usize {
        2
    }
}

/// `ohlcv_interval_nanos(interval_str)` -> nanoseconds as i64.
struct IntervalNanosFn;

impl ScalarFunction for IntervalNanosFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let interval = match &args[0] {
            Value::Str(s) => s.as_str(),
            _ => {
                return Err(
                    "ohlcv_interval_nanos: argument must be interval string".into(),
                )
            }
        };
        let tf = parse_timeframe(interval)
            .ok_or_else(|| format!("ohlcv_interval_nanos: unknown interval '{interval}'"))?;
        Ok(Value::I64(tf.as_nanos()))
    }

    fn min_args(&self) -> usize {
        1
    }
    fn max_args(&self) -> usize {
        1
    }
}

fn parse_timeframe(s: &str) -> Option<exchange_exchange::TimeFrame> {
    match s.to_lowercase().as_str() {
        "1s" | "s1" => Some(exchange_exchange::TimeFrame::S1),
        "1m" | "m1" => Some(exchange_exchange::TimeFrame::M1),
        "5m" | "m5" => Some(exchange_exchange::TimeFrame::M5),
        "15m" | "m15" => Some(exchange_exchange::TimeFrame::M15),
        "1h" | "h1" => Some(exchange_exchange::TimeFrame::H1),
        "4h" | "h4" => Some(exchange_exchange::TimeFrame::H4),
        "1d" | "d1" => Some(exchange_exchange::TimeFrame::D1),
        "1w" | "w1" => Some(exchange_exchange::TimeFrame::W1),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Orderbook helpers
// ---------------------------------------------------------------------------

/// `best_bid(bids_json)` -> f64 price of the best (highest) bid.
///
/// Expects a JSON array of `[price, quantity, ...]` arrays, sorted
/// highest-first (standard L2 format).
struct BestBidFn;

impl ScalarFunction for BestBidFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let json = match &args[0] {
            Value::Str(s) => s.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err("best_bid: argument must be JSON string".into()),
        };
        let levels: Vec<Vec<f64>> =
            serde_json::from_str(json).map_err(|e| format!("best_bid: invalid JSON: {e}"))?;
        match levels.first().and_then(|l| l.first()) {
            Some(price) => Ok(Value::F64(*price)),
            None => Ok(Value::Null),
        }
    }

    fn min_args(&self) -> usize {
        1
    }
    fn max_args(&self) -> usize {
        1
    }
}

/// `best_ask(asks_json)` -> f64 price of the best (lowest) ask.
struct BestAskFn;

impl ScalarFunction for BestAskFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let json = match &args[0] {
            Value::Str(s) => s.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err("best_ask: argument must be JSON string".into()),
        };
        let levels: Vec<Vec<f64>> =
            serde_json::from_str(json).map_err(|e| format!("best_ask: invalid JSON: {e}"))?;
        match levels.first().and_then(|l| l.first()) {
            Some(price) => Ok(Value::F64(*price)),
            None => Ok(Value::Null),
        }
    }

    fn min_args(&self) -> usize {
        1
    }
    fn max_args(&self) -> usize {
        1
    }
}

/// `spread(bids_json, asks_json)` -> f64 absolute spread.
struct SpreadFn;

impl ScalarFunction for SpreadFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let bid_json = match &args[0] {
            Value::Str(s) => s.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err("spread: first argument must be JSON string".into()),
        };
        let ask_json = match &args[1] {
            Value::Str(s) => s.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err("spread: second argument must be JSON string".into()),
        };
        let bids: Vec<Vec<f64>> = serde_json::from_str(bid_json)
            .map_err(|e| format!("spread: invalid bids JSON: {e}"))?;
        let asks: Vec<Vec<f64>> = serde_json::from_str(ask_json)
            .map_err(|e| format!("spread: invalid asks JSON: {e}"))?;
        let best_bid = bids.first().and_then(|l| l.first());
        let best_ask = asks.first().and_then(|l| l.first());
        match (best_bid, best_ask) {
            (Some(b), Some(a)) => Ok(Value::F64(a - b)),
            _ => Ok(Value::Null),
        }
    }

    fn min_args(&self) -> usize {
        2
    }
    fn max_args(&self) -> usize {
        2
    }
}

/// `mid_price(bids_json, asks_json)` -> f64 mid price.
struct MidPriceFn;

impl ScalarFunction for MidPriceFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let bid_json = match &args[0] {
            Value::Str(s) => s.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err("mid_price: first argument must be JSON string".into()),
        };
        let ask_json = match &args[1] {
            Value::Str(s) => s.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err("mid_price: second argument must be JSON string".into()),
        };
        let bids: Vec<Vec<f64>> = serde_json::from_str(bid_json)
            .map_err(|e| format!("mid_price: invalid bids JSON: {e}"))?;
        let asks: Vec<Vec<f64>> = serde_json::from_str(ask_json)
            .map_err(|e| format!("mid_price: invalid asks JSON: {e}"))?;
        let best_bid = bids.first().and_then(|l| l.first());
        let best_ask = asks.first().and_then(|l| l.first());
        match (best_bid, best_ask) {
            (Some(b), Some(a)) => Ok(Value::F64((b + a) / 2.0)),
            _ => Ok(Value::Null),
        }
    }

    fn min_args(&self) -> usize {
        2
    }
    fn max_args(&self) -> usize {
        2
    }
}

// ---------------------------------------------------------------------------
// Tick delta encoding/decoding
// ---------------------------------------------------------------------------

/// `tick_delta_encode(prices_json, decimal_digits)` -> JSON string of (base, deltas).
struct TickDeltaEncodeFn;

impl ScalarFunction for TickDeltaEncodeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let prices_json = match &args[0] {
            Value::Str(s) => s.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => {
                return Err(
                    "tick_delta_encode: first argument must be JSON array string".into(),
                )
            }
        };
        let decimals = match &args[1] {
            Value::I64(v) => *v as u32,
            _ => return Err("tick_delta_encode: second argument must be integer".into()),
        };
        let prices: Vec<f64> = serde_json::from_str(prices_json)
            .map_err(|e| format!("tick_delta_encode: invalid JSON: {e}"))?;
        let (base, deltas) = exchange_exchange::tick::delta_encode_prices(&prices, decimals);
        let result = serde_json::json!({ "base": base, "deltas": deltas });
        Ok(Value::Str(result.to_string()))
    }

    fn min_args(&self) -> usize {
        2
    }
    fn max_args(&self) -> usize {
        2
    }
}

/// `tick_delta_decode(encoded_json, decimal_digits)` -> JSON array of f64 prices.
struct TickDeltaDecodeFn;

impl ScalarFunction for TickDeltaDecodeFn {
    fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
        let encoded_json = match &args[0] {
            Value::Str(s) => s.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => {
                return Err(
                    "tick_delta_decode: first argument must be JSON string".into(),
                )
            }
        };
        let decimals = match &args[1] {
            Value::I64(v) => *v as u32,
            _ => return Err("tick_delta_decode: second argument must be integer".into()),
        };
        let parsed: serde_json::Value = serde_json::from_str(encoded_json)
            .map_err(|e| format!("tick_delta_decode: invalid JSON: {e}"))?;
        let base = parsed["base"]
            .as_i64()
            .ok_or("tick_delta_decode: missing 'base' field")?;
        let deltas: Vec<i64> = parsed["deltas"]
            .as_array()
            .ok_or("tick_delta_decode: missing 'deltas' field")?
            .iter()
            .map(|v| v.as_i64().unwrap_or(0))
            .collect();
        let prices = exchange_exchange::tick::delta_decode_prices(base, &deltas, decimals);
        let result = serde_json::to_string(&prices)
            .map_err(|e| format!("tick_delta_decode: serialization error: {e}"))?;
        Ok(Value::Str(result))
    }

    fn min_args(&self) -> usize {
        2
    }
    fn max_args(&self) -> usize {
        2
    }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

/// Register all exchange-domain scalar functions into the given registry.
pub fn register_exchange_functions(registry: &mut ScalarRegistry) {
    // OHLCV helpers
    registry.register_public("vwap", Box::new(VwapFn));
    registry.register_public("ohlcv_vwap", Box::new(VwapFn));
    registry.register_public("ohlcv_bar_align", Box::new(BarAlignFn));
    registry.register_public("bar_align", Box::new(BarAlignFn));
    registry.register_public("ohlcv_interval_nanos", Box::new(IntervalNanosFn));

    // Orderbook analytics
    registry.register_public("best_bid", Box::new(BestBidFn));
    registry.register_public("best_ask", Box::new(BestAskFn));
    registry.register_public("spread", Box::new(SpreadFn));
    registry.register_public("mid_price", Box::new(MidPriceFn));

    // Tick delta encoding/decoding
    registry.register_public("tick_delta_encode", Box::new(TickDeltaEncodeFn));
    registry.register_public("tick_delta_decode", Box::new(TickDeltaDecodeFn));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalar::evaluate_scalar;

    #[test]
    fn test_vwap() {
        let result =
            evaluate_scalar("vwap", &[Value::F64(1050.0), Value::F64(10.0)]).unwrap();
        assert_eq!(result, Value::F64(105.0));
    }

    #[test]
    fn test_vwap_zero_volume() {
        let result =
            evaluate_scalar("vwap", &[Value::F64(100.0), Value::F64(0.0)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_vwap_null() {
        let result = evaluate_scalar("vwap", &[Value::Null, Value::F64(10.0)]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_vwap_integer_args() {
        let result =
            evaluate_scalar("vwap", &[Value::I64(1000), Value::I64(10)]).unwrap();
        assert_eq!(result, Value::F64(100.0));
    }

    #[test]
    fn test_bar_align_1m() {
        let ts = 90_500_000_000i64;
        let result = evaluate_scalar(
            "ohlcv_bar_align",
            &[Value::I64(ts), Value::Str("1m".into())],
        )
        .unwrap();
        assert_eq!(result, Value::I64(60_000_000_000));
    }

    #[test]
    fn test_bar_align_1s() {
        let ts = 1_500_000_000i64;
        let result = evaluate_scalar(
            "bar_align",
            &[Value::I64(ts), Value::Str("1s".into())],
        )
        .unwrap();
        assert_eq!(result, Value::I64(1_000_000_000));
    }

    #[test]
    fn test_bar_align_5m() {
        let ts = 4 * 60_000_000_000i64;
        let result = evaluate_scalar(
            "ohlcv_bar_align",
            &[Value::I64(ts), Value::Str("5m".into())],
        )
        .unwrap();
        assert_eq!(result, Value::I64(0));
    }

    #[test]
    fn test_interval_nanos() {
        let result =
            evaluate_scalar("ohlcv_interval_nanos", &[Value::Str("1m".into())]).unwrap();
        assert_eq!(result, Value::I64(60_000_000_000));
    }

    #[test]
    fn test_interval_nanos_1h() {
        let result =
            evaluate_scalar("ohlcv_interval_nanos", &[Value::Str("1h".into())]).unwrap();
        assert_eq!(result, Value::I64(3_600_000_000_000));
    }

    #[test]
    fn test_best_bid() {
        let bids = "[[100.5, 10.0], [99.0, 20.0]]";
        let result = evaluate_scalar("best_bid", &[Value::Str(bids.into())]).unwrap();
        assert_eq!(result, Value::F64(100.5));
    }

    #[test]
    fn test_best_bid_empty() {
        let result = evaluate_scalar("best_bid", &[Value::Str("[]".into())]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_best_ask() {
        let asks = "[[101.0, 5.0], [102.0, 15.0]]";
        let result = evaluate_scalar("best_ask", &[Value::Str(asks.into())]).unwrap();
        assert_eq!(result, Value::F64(101.0));
    }

    #[test]
    fn test_best_ask_empty() {
        let result = evaluate_scalar("best_ask", &[Value::Str("[]".into())]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_spread() {
        let bids = "[[100.0, 10.0]]";
        let asks = "[[101.0, 5.0]]";
        let result = evaluate_scalar(
            "spread",
            &[Value::Str(bids.into()), Value::Str(asks.into())],
        )
        .unwrap();
        assert_eq!(result, Value::F64(1.0));
    }

    #[test]
    fn test_mid_price() {
        let bids = "[[100.0, 10.0]]";
        let asks = "[[102.0, 5.0]]";
        let result = evaluate_scalar(
            "mid_price",
            &[Value::Str(bids.into()), Value::Str(asks.into())],
        )
        .unwrap();
        assert_eq!(result, Value::F64(101.0));
    }

    #[test]
    fn test_spread_empty_book() {
        let bids = "[]";
        let asks = "[[101.0, 5.0]]";
        let result = evaluate_scalar(
            "spread",
            &[Value::Str(bids.into()), Value::Str(asks.into())],
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_mid_price_empty_book() {
        let bids = "[[100.0, 10.0]]";
        let asks = "[]";
        let result = evaluate_scalar(
            "mid_price",
            &[Value::Str(bids.into()), Value::Str(asks.into())],
        )
        .unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_tick_delta_encode_decode_roundtrip() {
        let prices = "[100.25, 100.30, 100.28, 100.35, 100.50]";
        let encoded = evaluate_scalar(
            "tick_delta_encode",
            &[Value::Str(prices.into()), Value::I64(2)],
        )
        .unwrap();
        let encoded_str = match &encoded {
            Value::Str(s) => s.clone(),
            _ => panic!("expected string"),
        };

        let decoded = evaluate_scalar(
            "tick_delta_decode",
            &[Value::Str(encoded_str), Value::I64(2)],
        )
        .unwrap();
        let decoded_str = match decoded {
            Value::Str(s) => s,
            _ => panic!("expected string"),
        };
        let decoded_prices: Vec<f64> = serde_json::from_str(&decoded_str).unwrap();
        let original: Vec<f64> = serde_json::from_str(prices).unwrap();
        assert_eq!(decoded_prices.len(), original.len());
        for (a, b) in original.iter().zip(decoded_prices.iter()) {
            assert!((a - b).abs() < 1e-9, "{a} != {b}");
        }
    }

    #[test]
    fn test_tick_delta_encode_empty() {
        let result = evaluate_scalar(
            "tick_delta_encode",
            &[Value::Str("[]".into()), Value::I64(2)],
        )
        .unwrap();
        match result {
            Value::Str(s) => {
                let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
                assert_eq!(parsed["base"], 0);
                assert!(parsed["deltas"].as_array().unwrap().is_empty());
            }
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn test_tick_delta_encode_single_value() {
        let result = evaluate_scalar(
            "tick_delta_encode",
            &[Value::Str("[42.50]".into()), Value::I64(2)],
        )
        .unwrap();
        match result {
            Value::Str(s) => {
                let parsed: serde_json::Value = serde_json::from_str(&s).unwrap();
                assert_eq!(parsed["base"], 4250);
                assert!(parsed["deltas"].as_array().unwrap().is_empty());
            }
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn test_null_handling() {
        assert_eq!(
            evaluate_scalar("best_bid", &[Value::Null]).unwrap(),
            Value::Null
        );
        assert_eq!(
            evaluate_scalar("best_ask", &[Value::Null]).unwrap(),
            Value::Null
        );
        assert_eq!(
            evaluate_scalar("spread", &[Value::Null, Value::Str("[]".into())]).unwrap(),
            Value::Null
        );
        assert_eq!(
            evaluate_scalar("mid_price", &[Value::Str("[]".into()), Value::Null]).unwrap(),
            Value::Null
        );
        assert_eq!(
            evaluate_scalar("tick_delta_encode", &[Value::Null, Value::I64(2)]).unwrap(),
            Value::Null
        );
        assert_eq!(
            evaluate_scalar("tick_delta_decode", &[Value::Null, Value::I64(2)]).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_ohlcv_vwap_alias() {
        let result =
            evaluate_scalar("ohlcv_vwap", &[Value::F64(500.0), Value::F64(5.0)]).unwrap();
        assert_eq!(result, Value::F64(100.0));
    }
}
