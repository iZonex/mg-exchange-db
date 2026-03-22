//! Exchange-domain balance, margin, position, order, and trade-volume
//! scalar functions.
//!
//! All functions registered here are accessible from SQL via
//! `SELECT balance_available(total, locked), ...`.

use crate::plan::Value;
use crate::scalar::{ScalarFunction, ScalarRegistry};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract an f64 from a Value, coercing I64 -> f64.
fn as_f64(v: &Value, fn_name: &str, arg_name: &str) -> Result<f64, String> {
    match v {
        Value::F64(f) => Ok(*f),
        Value::I64(i) => Ok(*i as f64),
        Value::Null => Err("__null__".into()), // sentinel, callers return Null
        _ => Err(format!("{fn_name}: {arg_name} must be numeric")),
    }
}

/// Helper: try to extract f64, returning Ok(None) for Null.
fn try_f64(v: &Value, fn_name: &str, arg_name: &str) -> Result<Option<f64>, String> {
    match as_f64(v, fn_name, arg_name) {
        Ok(f) => Ok(Some(f)),
        Err(e) if e == "__null__" => Ok(None),
        Err(e) => Err(e),
    }
}

/// Macro to reduce boilerplate for simple N-arg scalar functions.
macro_rules! simple_fn {
    ($name:ident, $min:expr, $max:expr) => {
        impl ScalarFunction for $name {
            fn evaluate(&self, args: &[Value]) -> Result<Value, String> {
                self.eval(args)
            }
            fn min_args(&self) -> usize {
                $min
            }
            fn max_args(&self) -> usize {
                $max
            }
        }
    };
}

// ===========================================================================
// 1. Balance Engine Functions
// ===========================================================================

/// `balance_available(total, locked)` -> total - locked
struct BalanceAvailableFn;
simple_fn!(BalanceAvailableFn, 2, 2);
impl BalanceAvailableFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let total = match try_f64(&args[0], "balance_available", "total")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let locked = match try_f64(&args[1], "balance_available", "locked")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        Ok(Value::F64(total - locked))
    }
}

/// `margin_ratio(position_value, margin)` -> position_value / margin
struct MarginRatioFn;
simple_fn!(MarginRatioFn, 2, 2);
impl MarginRatioFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let pv = match try_f64(&args[0], "margin_ratio", "position_value")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let margin = match try_f64(&args[1], "margin_ratio", "margin")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        if margin == 0.0 {
            return Ok(Value::Null);
        }
        Ok(Value::F64(pv / margin))
    }
}

/// `liquidation_price(entry_price, leverage, side)` -> liquidation price.
///
/// LONG:  entry_price * (1 - 1/leverage)
/// SHORT: entry_price * (1 + 1/leverage)
struct LiquidationPriceFn;
simple_fn!(LiquidationPriceFn, 3, 3);
impl LiquidationPriceFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let entry = match try_f64(&args[0], "liquidation_price", "entry_price")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let leverage = match try_f64(&args[1], "liquidation_price", "leverage")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        if leverage == 0.0 {
            return Err("liquidation_price: leverage must not be zero".into());
        }
        let side = match &args[2] {
            Value::Str(s) => s.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err("liquidation_price: side must be a string".into()),
        };
        let factor = 1.0 / leverage;
        match side.to_lowercase().as_str() {
            "long" | "buy" => Ok(Value::F64(entry * (1.0 - factor))),
            "short" | "sell" => Ok(Value::F64(entry * (1.0 + factor))),
            _ => Err(format!(
                "liquidation_price: side must be 'long'/'buy' or 'short'/'sell', got '{side}'"
            )),
        }
    }
}

/// `unrealized_pnl(entry_price, current_price, size, side)` -> PnL.
///
/// LONG:  (current_price - entry_price) * size
/// SHORT: (entry_price - current_price) * size
struct UnrealizedPnlFn;
simple_fn!(UnrealizedPnlFn, 4, 4);
impl UnrealizedPnlFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let entry = match try_f64(&args[0], "unrealized_pnl", "entry_price")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let current = match try_f64(&args[1], "unrealized_pnl", "current_price")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let size = match try_f64(&args[2], "unrealized_pnl", "size")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let side = match &args[3] {
            Value::Str(s) => s.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err("unrealized_pnl: side must be a string".into()),
        };
        match side.to_lowercase().as_str() {
            "long" | "buy" => Ok(Value::F64((current - entry) * size)),
            "short" | "sell" => Ok(Value::F64((entry - current) * size)),
            _ => Err(format!(
                "unrealized_pnl: side must be 'long'/'buy' or 'short'/'sell', got '{side}'"
            )),
        }
    }
}

/// `position_value(price, size, leverage)` -> price * size  (notional value)
struct PositionValueFn;
simple_fn!(PositionValueFn, 2, 3);
impl PositionValueFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let price = match try_f64(&args[0], "position_value", "price")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let size = match try_f64(&args[1], "position_value", "size")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        // leverage arg is accepted but not used in notional calculation
        Ok(Value::F64(price * size))
    }
}

/// `margin_required(price, size, leverage)` -> price * size / leverage
struct MarginRequiredFn;
simple_fn!(MarginRequiredFn, 3, 3);
impl MarginRequiredFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let price = match try_f64(&args[0], "margin_required", "price")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let size = match try_f64(&args[1], "margin_required", "size")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let leverage = match try_f64(&args[2], "margin_required", "leverage")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        if leverage == 0.0 {
            return Err("margin_required: leverage must not be zero".into());
        }
        Ok(Value::F64(price * size / leverage))
    }
}

/// `funding_payment(position_size, funding_rate, mark_price)` -> position_size * funding_rate * mark_price
struct FundingPaymentFn;
simple_fn!(FundingPaymentFn, 3, 3);
impl FundingPaymentFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let pos = match try_f64(&args[0], "funding_payment", "position_size")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let rate = match try_f64(&args[1], "funding_payment", "funding_rate")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let mark = match try_f64(&args[2], "funding_payment", "mark_price")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        Ok(Value::F64(pos * rate * mark))
    }
}

/// `fee_amount(price, qty, fee_rate)` -> price * qty * fee_rate
struct FeeAmountFn;
simple_fn!(FeeAmountFn, 3, 3);
impl FeeAmountFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let price = match try_f64(&args[0], "fee_amount", "price")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let qty = match try_f64(&args[1], "fee_amount", "qty")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let rate = match try_f64(&args[2], "fee_amount", "fee_rate")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        Ok(Value::F64(price * qty * rate))
    }
}

// ===========================================================================
// 2. Extended OHLCV / Trade Volume helpers (scalar, composable with SUM)
// ===========================================================================

/// `taker_buy_volume(qty, side)` -> qty if side='buy', else 0.
/// Use with SUM: `SUM(taker_buy_volume(qty, side))`
struct TakerBuyVolumeFn;
simple_fn!(TakerBuyVolumeFn, 2, 2);
impl TakerBuyVolumeFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let qty = match try_f64(&args[0], "taker_buy_volume", "qty")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let side = match &args[1] {
            Value::Str(s) => s.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err("taker_buy_volume: side must be a string".into()),
        };
        if side.eq_ignore_ascii_case("buy") {
            Ok(Value::F64(qty))
        } else {
            Ok(Value::F64(0.0))
        }
    }
}

/// `taker_sell_volume(qty, side)` -> qty if side='sell', else 0.
struct TakerSellVolumeFn;
simple_fn!(TakerSellVolumeFn, 2, 2);
impl TakerSellVolumeFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let qty = match try_f64(&args[0], "taker_sell_volume", "qty")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let side = match &args[1] {
            Value::Str(s) => s.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err("taker_sell_volume: side must be a string".into()),
        };
        if side.eq_ignore_ascii_case("sell") {
            Ok(Value::F64(qty))
        } else {
            Ok(Value::F64(0.0))
        }
    }
}

/// `quote_volume(price, qty)` -> price * qty.
/// Use with SUM: `SUM(quote_volume(price, qty))`
struct QuoteVolumeFn;
simple_fn!(QuoteVolumeFn, 2, 2);
impl QuoteVolumeFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let price = match try_f64(&args[0], "quote_volume", "price")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let qty = match try_f64(&args[1], "quote_volume", "qty")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        Ok(Value::F64(price * qty))
    }
}

/// `trade_count()` -> 1.  Use with SUM: `SUM(trade_count())`
struct TradeCountFn;
simple_fn!(TradeCountFn, 0, 0);
impl TradeCountFn {
    fn eval(&self, _args: &[Value]) -> Result<Value, String> {
        Ok(Value::I64(1))
    }
}

/// `delta(qty, side)` -> +qty for buy, -qty for sell.
/// Use with SUM: `SUM(delta(qty, side))` = taker_buy_volume - taker_sell_volume.
struct DeltaFn;
simple_fn!(DeltaFn, 2, 2);
impl DeltaFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let qty = match try_f64(&args[0], "delta", "qty")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let side = match &args[1] {
            Value::Str(s) => s.as_str(),
            Value::Null => return Ok(Value::Null),
            _ => return Err("delta: side must be a string".into()),
        };
        if side.eq_ignore_ascii_case("buy") {
            Ok(Value::F64(qty))
        } else if side.eq_ignore_ascii_case("sell") {
            Ok(Value::F64(-qty))
        } else {
            Err(format!("delta: side must be 'buy' or 'sell', got '{side}'"))
        }
    }
}

/// `delta_pct(buy_volume, sell_volume)` -> (buy - sell) / (buy + sell) * 100.
///
/// Note: In per-row mode this is used as a post-aggregate function:
///   `delta_pct(SUM(taker_buy_volume(qty, side)), SUM(taker_sell_volume(qty, side)))`
struct DeltaPctFn;
simple_fn!(DeltaPctFn, 2, 2);
impl DeltaPctFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let buy_vol = match try_f64(&args[0], "delta_pct", "buy_volume")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let sell_vol = match try_f64(&args[1], "delta_pct", "sell_volume")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let total = buy_vol + sell_vol;
        if total == 0.0 {
            return Ok(Value::Null);
        }
        Ok(Value::F64((buy_vol - sell_vol) / total * 100.0))
    }
}

// ===========================================================================
// 3. Order State Functions
// ===========================================================================

/// `order_fill_pct(filled_qty, total_qty)` -> filled_qty / total_qty * 100
struct OrderFillPctFn;
simple_fn!(OrderFillPctFn, 2, 2);
impl OrderFillPctFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let filled = match try_f64(&args[0], "order_fill_pct", "filled_qty")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let total = match try_f64(&args[1], "order_fill_pct", "total_qty")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        if total == 0.0 {
            return Ok(Value::Null);
        }
        Ok(Value::F64(filled / total * 100.0))
    }
}

/// `is_fully_filled(filled_qty, total_qty)` -> 1 if filled >= total, else 0
struct IsFullyFilledFn;
simple_fn!(IsFullyFilledFn, 2, 2);
impl IsFullyFilledFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let filled = match try_f64(&args[0], "is_fully_filled", "filled_qty")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let total = match try_f64(&args[1], "is_fully_filled", "total_qty")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        Ok(Value::I64(if filled >= total { 1 } else { 0 }))
    }
}

/// `remaining_qty(total_qty, filled_qty)` -> total_qty - filled_qty
struct RemainingQtyFn;
simple_fn!(RemainingQtyFn, 2, 2);
impl RemainingQtyFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let total = match try_f64(&args[0], "remaining_qty", "total_qty")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let filled = match try_f64(&args[1], "remaining_qty", "filled_qty")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        Ok(Value::F64(total - filled))
    }
}

/// `effective_price(total_cost, filled_qty)` -> total_cost / filled_qty
struct EffectivePriceFn;
simple_fn!(EffectivePriceFn, 2, 2);
impl EffectivePriceFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let cost = match try_f64(&args[0], "effective_price", "total_cost")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let filled = match try_f64(&args[1], "effective_price", "filled_qty")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        if filled == 0.0 {
            return Ok(Value::Null);
        }
        Ok(Value::F64(cost / filled))
    }
}

/// `slippage(expected_price, actual_price)` -> (actual - expected) / expected * 100
struct SlippageFn;
simple_fn!(SlippageFn, 2, 2);
impl SlippageFn {
    fn eval(&self, args: &[Value]) -> Result<Value, String> {
        let expected = match try_f64(&args[0], "slippage", "expected_price")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        let actual = match try_f64(&args[1], "slippage", "actual_price")? {
            Some(v) => v,
            None => return Ok(Value::Null),
        };
        if expected == 0.0 {
            return Ok(Value::Null);
        }
        Ok(Value::F64((actual - expected) / expected * 100.0))
    }
}

// ===========================================================================
// Registration
// ===========================================================================

/// Register all balance, margin, position, order, and trade-volume scalar
/// functions into the given registry.
pub fn register_balance_functions(registry: &mut ScalarRegistry) {
    // Balance engine
    registry.register_public("balance_available", Box::new(BalanceAvailableFn));
    registry.register_public("margin_ratio", Box::new(MarginRatioFn));
    registry.register_public("liquidation_price", Box::new(LiquidationPriceFn));
    registry.register_public("unrealized_pnl", Box::new(UnrealizedPnlFn));
    registry.register_public("position_value", Box::new(PositionValueFn));
    registry.register_public("margin_required", Box::new(MarginRequiredFn));
    registry.register_public("funding_payment", Box::new(FundingPaymentFn));
    registry.register_public("fee_amount", Box::new(FeeAmountFn));

    // Extended OHLCV / trade-volume helpers
    registry.register_public("taker_buy_volume", Box::new(TakerBuyVolumeFn));
    registry.register_public("taker_sell_volume", Box::new(TakerSellVolumeFn));
    registry.register_public("quote_volume", Box::new(QuoteVolumeFn));
    registry.register_public("trade_count", Box::new(TradeCountFn));
    registry.register_public("delta", Box::new(DeltaFn));
    registry.register_public("delta_pct", Box::new(DeltaPctFn));

    // Order state
    registry.register_public("order_fill_pct", Box::new(OrderFillPctFn));
    registry.register_public("is_fully_filled", Box::new(IsFullyFilledFn));
    registry.register_public("remaining_qty", Box::new(RemainingQtyFn));
    registry.register_public("effective_price", Box::new(EffectivePriceFn));
    registry.register_public("slippage", Box::new(SlippageFn));
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalar::evaluate_scalar;

    // -- Balance engine --

    #[test]
    fn test_balance_available() {
        let r = evaluate_scalar(
            "balance_available",
            &[Value::F64(1000.0), Value::F64(250.0)],
        )
        .unwrap();
        assert_eq!(r, Value::F64(750.0));
    }

    #[test]
    fn test_balance_available_null() {
        let r = evaluate_scalar("balance_available", &[Value::Null, Value::F64(250.0)]).unwrap();
        assert_eq!(r, Value::Null);
    }

    #[test]
    fn test_margin_ratio() {
        let r =
            evaluate_scalar("margin_ratio", &[Value::F64(10000.0), Value::F64(1000.0)]).unwrap();
        assert_eq!(r, Value::F64(10.0));
    }

    #[test]
    fn test_margin_ratio_zero() {
        let r = evaluate_scalar("margin_ratio", &[Value::F64(10000.0), Value::F64(0.0)]).unwrap();
        assert_eq!(r, Value::Null);
    }

    #[test]
    fn test_liquidation_price_long() {
        // entry=100, leverage=10 -> 100*(1-0.1) = 90
        let r = evaluate_scalar(
            "liquidation_price",
            &[
                Value::F64(100.0),
                Value::F64(10.0),
                Value::Str("long".into()),
            ],
        )
        .unwrap();
        assert_eq!(r, Value::F64(90.0));
    }

    #[test]
    fn test_liquidation_price_short() {
        // entry=100, leverage=10 -> 100*(1+0.1) = 110
        let r = evaluate_scalar(
            "liquidation_price",
            &[
                Value::F64(100.0),
                Value::F64(10.0),
                Value::Str("short".into()),
            ],
        )
        .unwrap();
        match r {
            Value::F64(v) => assert!((v - 110.0).abs() < 1e-9, "expected ~110.0, got {v}"),
            other => panic!("expected F64, got {other:?}"),
        }
    }

    #[test]
    fn test_liquidation_price_buy_alias() {
        let r = evaluate_scalar(
            "liquidation_price",
            &[Value::F64(100.0), Value::F64(5.0), Value::Str("buy".into())],
        )
        .unwrap();
        assert_eq!(r, Value::F64(80.0));
    }

    #[test]
    fn test_unrealized_pnl_long() {
        // (110-100)*2 = 20
        let r = evaluate_scalar(
            "unrealized_pnl",
            &[
                Value::F64(100.0),
                Value::F64(110.0),
                Value::F64(2.0),
                Value::Str("long".into()),
            ],
        )
        .unwrap();
        assert_eq!(r, Value::F64(20.0));
    }

    #[test]
    fn test_unrealized_pnl_short() {
        // (100-90)*2 = 20
        let r = evaluate_scalar(
            "unrealized_pnl",
            &[
                Value::F64(100.0),
                Value::F64(90.0),
                Value::F64(2.0),
                Value::Str("short".into()),
            ],
        )
        .unwrap();
        assert_eq!(r, Value::F64(20.0));
    }

    #[test]
    fn test_position_value() {
        let r = evaluate_scalar("position_value", &[Value::F64(50000.0), Value::F64(0.5)]).unwrap();
        assert_eq!(r, Value::F64(25000.0));
    }

    #[test]
    fn test_position_value_with_leverage() {
        // leverage arg is accepted but not used for notional
        let r = evaluate_scalar(
            "position_value",
            &[Value::F64(50000.0), Value::F64(0.5), Value::F64(10.0)],
        )
        .unwrap();
        assert_eq!(r, Value::F64(25000.0));
    }

    #[test]
    fn test_margin_required() {
        // 50000 * 0.5 / 10 = 2500
        let r = evaluate_scalar(
            "margin_required",
            &[Value::F64(50000.0), Value::F64(0.5), Value::F64(10.0)],
        )
        .unwrap();
        assert_eq!(r, Value::F64(2500.0));
    }

    #[test]
    fn test_funding_payment() {
        // 1.0 * 0.0001 * 50000 = 5.0
        let r = evaluate_scalar(
            "funding_payment",
            &[Value::F64(1.0), Value::F64(0.0001), Value::F64(50000.0)],
        )
        .unwrap();
        assert_eq!(r, Value::F64(5.0));
    }

    #[test]
    fn test_fee_amount() {
        // 100 * 0.5 * 0.001 = 0.05
        let r = evaluate_scalar(
            "fee_amount",
            &[Value::F64(100.0), Value::F64(0.5), Value::F64(0.001)],
        )
        .unwrap();
        assert_eq!(r, Value::F64(0.05));
    }

    // -- Extended OHLCV --

    #[test]
    fn test_taker_buy_volume_buy() {
        let r = evaluate_scalar(
            "taker_buy_volume",
            &[Value::F64(5.0), Value::Str("buy".into())],
        )
        .unwrap();
        assert_eq!(r, Value::F64(5.0));
    }

    #[test]
    fn test_taker_buy_volume_sell() {
        let r = evaluate_scalar(
            "taker_buy_volume",
            &[Value::F64(5.0), Value::Str("sell".into())],
        )
        .unwrap();
        assert_eq!(r, Value::F64(0.0));
    }

    #[test]
    fn test_taker_sell_volume() {
        let r = evaluate_scalar(
            "taker_sell_volume",
            &[Value::F64(3.0), Value::Str("sell".into())],
        )
        .unwrap();
        assert_eq!(r, Value::F64(3.0));

        let r2 = evaluate_scalar(
            "taker_sell_volume",
            &[Value::F64(3.0), Value::Str("buy".into())],
        )
        .unwrap();
        assert_eq!(r2, Value::F64(0.0));
    }

    #[test]
    fn test_quote_volume() {
        let r = evaluate_scalar("quote_volume", &[Value::F64(100.0), Value::F64(2.5)]).unwrap();
        assert_eq!(r, Value::F64(250.0));
    }

    #[test]
    fn test_trade_count() {
        let r = evaluate_scalar("trade_count", &[]).unwrap();
        assert_eq!(r, Value::I64(1));
    }

    #[test]
    fn test_delta_buy() {
        let r = evaluate_scalar("delta", &[Value::F64(5.0), Value::Str("buy".into())]).unwrap();
        assert_eq!(r, Value::F64(5.0));
    }

    #[test]
    fn test_delta_sell() {
        let r = evaluate_scalar("delta", &[Value::F64(5.0), Value::Str("sell".into())]).unwrap();
        assert_eq!(r, Value::F64(-5.0));
    }

    #[test]
    fn test_delta_pct() {
        // buy_vol=70, sell_vol=30 -> (70-30)/100*100 = 40%
        let r = evaluate_scalar("delta_pct", &[Value::F64(70.0), Value::F64(30.0)]).unwrap();
        assert_eq!(r, Value::F64(40.0));
    }

    #[test]
    fn test_delta_pct_zero_total() {
        let r = evaluate_scalar("delta_pct", &[Value::F64(0.0), Value::F64(0.0)]).unwrap();
        assert_eq!(r, Value::Null);
    }

    // -- Order state --

    #[test]
    fn test_order_fill_pct() {
        let r = evaluate_scalar("order_fill_pct", &[Value::F64(7.5), Value::F64(10.0)]).unwrap();
        assert_eq!(r, Value::F64(75.0));
    }

    #[test]
    fn test_is_fully_filled_yes() {
        let r = evaluate_scalar("is_fully_filled", &[Value::F64(10.0), Value::F64(10.0)]).unwrap();
        assert_eq!(r, Value::I64(1));
    }

    #[test]
    fn test_is_fully_filled_no() {
        let r = evaluate_scalar("is_fully_filled", &[Value::F64(5.0), Value::F64(10.0)]).unwrap();
        assert_eq!(r, Value::I64(0));
    }

    #[test]
    fn test_remaining_qty() {
        let r = evaluate_scalar("remaining_qty", &[Value::F64(10.0), Value::F64(3.0)]).unwrap();
        assert_eq!(r, Value::F64(7.0));
    }

    #[test]
    fn test_effective_price() {
        // total_cost=5000, filled_qty=50 -> 100
        let r =
            evaluate_scalar("effective_price", &[Value::F64(5000.0), Value::F64(50.0)]).unwrap();
        assert_eq!(r, Value::F64(100.0));
    }

    #[test]
    fn test_effective_price_zero_filled() {
        let r = evaluate_scalar("effective_price", &[Value::F64(5000.0), Value::F64(0.0)]).unwrap();
        assert_eq!(r, Value::Null);
    }

    #[test]
    fn test_slippage() {
        // expected=100, actual=101 -> (101-100)/100*100 = 1%
        let r = evaluate_scalar("slippage", &[Value::F64(100.0), Value::F64(101.0)]).unwrap();
        assert_eq!(r, Value::F64(1.0));
    }

    #[test]
    fn test_slippage_negative() {
        // expected=100, actual=99 -> -1%
        let r = evaluate_scalar("slippage", &[Value::F64(100.0), Value::F64(99.0)]).unwrap();
        assert_eq!(r, Value::F64(-1.0));
    }

    #[test]
    fn test_slippage_zero_expected() {
        let r = evaluate_scalar("slippage", &[Value::F64(0.0), Value::F64(1.0)]).unwrap();
        assert_eq!(r, Value::Null);
    }

    // -- Integer args coercion --

    #[test]
    fn test_integer_coercion() {
        let r = evaluate_scalar("balance_available", &[Value::I64(1000), Value::I64(250)]).unwrap();
        assert_eq!(r, Value::F64(750.0));
    }

    // -- Null propagation --

    #[test]
    fn test_null_propagation() {
        assert_eq!(
            evaluate_scalar(
                "margin_required",
                &[Value::Null, Value::F64(1.0), Value::F64(10.0)]
            )
            .unwrap(),
            Value::Null
        );
        assert_eq!(
            evaluate_scalar(
                "fee_amount",
                &[Value::F64(1.0), Value::Null, Value::F64(0.01)]
            )
            .unwrap(),
            Value::Null
        );
        assert_eq!(
            evaluate_scalar("delta", &[Value::F64(5.0), Value::Null]).unwrap(),
            Value::Null
        );
        assert_eq!(
            evaluate_scalar("order_fill_pct", &[Value::Null, Value::F64(10.0)]).unwrap(),
            Value::Null
        );
        assert_eq!(
            evaluate_scalar(
                "unrealized_pnl",
                &[
                    Value::F64(100.0),
                    Value::F64(110.0),
                    Value::F64(1.0),
                    Value::Null
                ]
            )
            .unwrap(),
            Value::Null
        );
    }
}
