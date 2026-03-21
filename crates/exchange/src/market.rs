use serde::{Deserialize, Serialize};

use crate::orderbook::OrderBookDelta;
use crate::tick::Side;

/// Trading pair symbol (e.g., BTC/USD).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol {
    /// Base asset (e.g., "BTC").
    pub base: String,
    /// Quote asset (e.g., "USD").
    pub quote: String,
}

impl Symbol {
    pub fn new(base: impl Into<String>, quote: impl Into<String>) -> Self {
        Self {
            base: base.into(),
            quote: quote.into(),
        }
    }

    /// Canonical representation: "BASE/QUOTE".
    pub fn as_pair(&self) -> String {
        format!("{}/{}", self.base, self.quote)
    }
}

impl std::fmt::Display for Symbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.base, self.quote)
    }
}

/// Supported exchanges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Exchange {
    Binance,
    Coinbase,
    Kraken,
    Bitfinex,
    Bybit,
    OKX,
    Deribit,
    Bitstamp,
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::Binance => "Binance",
            Self::Coinbase => "Coinbase",
            Self::Kraken => "Kraken",
            Self::Bitfinex => "Bitfinex",
            Self::Bybit => "Bybit",
            Self::OKX => "OKX",
            Self::Deribit => "Deribit",
            Self::Bitstamp => "Bitstamp",
        };
        write!(f, "{}", name)
    }
}

/// A trade event from an exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub symbol: Symbol,
    pub exchange: Exchange,
    pub timestamp: i64,
    pub price: f64,
    pub volume: f64,
    pub side: Side,
    pub trade_id: u64,
}

/// A top-of-book quote (best bid/ask).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub symbol: Symbol,
    pub exchange: Exchange,
    pub timestamp: i64,
    pub bid_price: f64,
    pub bid_size: f64,
    pub ask_price: f64,
    pub ask_size: f64,
}

/// An order book update event carrying deltas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookUpdate {
    pub symbol: Symbol,
    pub exchange: Exchange,
    pub timestamp: i64,
    pub deltas: Vec<OrderBookDelta>,
    pub sequence: u64,
}

/// Unified market data event enum.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MarketEvent {
    Trade(Trade),
    Quote(Quote),
    OrderBookUpdate(OrderBookUpdate),
}

impl MarketEvent {
    /// Returns the timestamp common to all event variants.
    pub fn timestamp(&self) -> i64 {
        match self {
            Self::Trade(t) => t.timestamp,
            Self::Quote(q) => q.timestamp,
            Self::OrderBookUpdate(u) => u.timestamp,
        }
    }

    /// Returns the symbol common to all event variants.
    pub fn symbol(&self) -> &Symbol {
        match self {
            Self::Trade(t) => &t.symbol,
            Self::Quote(q) => &q.symbol,
            Self::OrderBookUpdate(u) => &u.symbol,
        }
    }

    /// Returns the exchange common to all event variants.
    pub fn exchange(&self) -> Exchange {
        match self {
            Self::Trade(t) => t.exchange,
            Self::Quote(q) => q.exchange,
            Self::OrderBookUpdate(u) => u.exchange,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_display() {
        let s = Symbol::new("BTC", "USD");
        assert_eq!(s.as_pair(), "BTC/USD");
        assert_eq!(format!("{}", s), "BTC/USD");
    }

    #[test]
    fn market_event_accessors() {
        let trade = MarketEvent::Trade(Trade {
            symbol: Symbol::new("ETH", "USD"),
            exchange: Exchange::Coinbase,
            timestamp: 1000,
            price: 3000.0,
            volume: 1.5,
            side: Side::Buy,
            trade_id: 42,
        });
        assert_eq!(trade.timestamp(), 1000);
        assert_eq!(trade.symbol().base, "ETH");
        assert_eq!(trade.exchange(), Exchange::Coinbase);
    }
}
