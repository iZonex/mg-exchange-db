//! Exchange-specific features for ExchangeDB.
//!
//! Provides OHLCV aggregation, order book storage, tick data types, and
//! unified market event types tailored for cryptocurrency and financial
//! exchange data.

pub mod market;
pub mod ohlcv;
pub mod orderbook;
pub mod tick;

// Re-export primary types at crate root for convenience.
pub use market::{Exchange, MarketEvent, OrderBookUpdate, Quote, Symbol, Trade};
pub use ohlcv::{OhlcvAggregator, OhlcvBar, TimeFrame};
pub use orderbook::{
    BookSide, DeltaAction, OrderBookDelta, OrderBookLevel, OrderBookSnapshot, OrderBookStore,
};
pub use tick::{Side, Tick, TickBuffer};
