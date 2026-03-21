use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A single price level in the order book.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OrderBookLevel {
    pub price: f64,
    pub quantity: f64,
    pub order_count: u32,
}

/// A full order book snapshot at a point in time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub symbol: String,
    pub timestamp: i64,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub sequence: u64,
}

/// The kind of incremental update applied to a single price level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeltaAction {
    Add,
    Modify,
    Delete,
}

/// Side of the book a delta applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookSide {
    Bid,
    Ask,
}

/// An incremental update to one price level.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OrderBookDelta {
    pub action: DeltaAction,
    pub side: BookSide,
    pub price: f64,
    pub quantity: f64,
    pub order_count: u32,
}

// ---------------------------------------------------------------------------
// Helpers: we key the internal BTreeMap on ordered-bits of f64 so we get
// deterministic price-level ordering without floating-point issues.
// ---------------------------------------------------------------------------

/// Convert f64 to a u64 key that preserves total ordering for non-NaN values.
fn price_key(price: f64) -> u64 {
    let bits = price.to_bits();
    // Flip all bits for negatives, flip sign bit for positives.
    if bits & (1u64 << 63) != 0 {
        !bits
    } else {
        bits ^ (1u64 << 63)
    }
}

/// Internal level stored in the BTreeMap.
#[derive(Debug, Clone, Copy)]
struct InternalLevel {
    price: f64,
    quantity: f64,
    order_count: u32,
}

impl InternalLevel {
    fn as_level(&self) -> OrderBookLevel {
        OrderBookLevel {
            price: self.price,
            quantity: self.quantity,
            order_count: self.order_count,
        }
    }
}

/// Persistent order book store.
///
/// Maintains the live book state and stores timestamped snapshots so that the
/// book can be reconstructed at any historical point.
pub struct OrderBookStore {
    symbol: String,
    /// Bids keyed by price_key — iterated high-to-low via `rev()`.
    bids: BTreeMap<u64, InternalLevel>,
    /// Asks keyed by price_key — iterated low-to-high.
    asks: BTreeMap<u64, InternalLevel>,
    /// Last sequence number.
    sequence: u64,
    /// Stored snapshots (timestamp -> snapshot). Kept sorted by BTreeMap key.
    snapshots: BTreeMap<i64, OrderBookSnapshot>,
}

impl OrderBookStore {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            sequence: 0,
            snapshots: BTreeMap::new(),
        }
    }

    /// Load a full snapshot into the live book, replacing current state.
    pub fn load_snapshot(&mut self, snapshot: &OrderBookSnapshot) {
        self.bids.clear();
        self.asks.clear();
        self.sequence = snapshot.sequence;

        for lvl in &snapshot.bids {
            self.bids.insert(
                price_key(lvl.price),
                InternalLevel {
                    price: lvl.price,
                    quantity: lvl.quantity,
                    order_count: lvl.order_count,
                },
            );
        }
        for lvl in &snapshot.asks {
            self.asks.insert(
                price_key(lvl.price),
                InternalLevel {
                    price: lvl.price,
                    quantity: lvl.quantity,
                    order_count: lvl.order_count,
                },
            );
        }
    }

    /// Apply a single incremental delta to the live book.
    pub fn apply_delta(&mut self, delta: &OrderBookDelta) {
        let map = match delta.side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        };
        let key = price_key(delta.price);

        match delta.action {
            DeltaAction::Add | DeltaAction::Modify => {
                map.insert(
                    key,
                    InternalLevel {
                        price: delta.price,
                        quantity: delta.quantity,
                        order_count: delta.order_count,
                    },
                );
            }
            DeltaAction::Delete => {
                map.remove(&key);
            }
        }
    }

    /// Apply a batch of deltas and bump the sequence number.
    pub fn apply_deltas(&mut self, deltas: &[OrderBookDelta], sequence: u64) {
        for d in deltas {
            self.apply_delta(d);
        }
        self.sequence = sequence;
    }

    /// Take a snapshot of the current live book and store it for later retrieval.
    pub fn save_snapshot(&mut self, timestamp: i64) -> OrderBookSnapshot {
        let snap = self.current_snapshot(timestamp);
        self.snapshots.insert(timestamp, snap.clone());
        snap
    }

    /// Build a snapshot of the current live book state (without storing it).
    pub fn current_snapshot(&self, timestamp: i64) -> OrderBookSnapshot {
        // Bids: highest price first.
        let bids: Vec<OrderBookLevel> = self
            .bids
            .values()
            .rev()
            .map(InternalLevel::as_level)
            .collect();

        // Asks: lowest price first.
        let asks: Vec<OrderBookLevel> = self
            .asks
            .values()
            .map(InternalLevel::as_level)
            .collect();

        OrderBookSnapshot {
            symbol: self.symbol.clone(),
            timestamp,
            bids,
            asks,
            sequence: self.sequence,
        }
    }

    /// Retrieve the stored snapshot at exactly the given timestamp, if any.
    pub fn get_snapshot(&self, timestamp: i64) -> Option<&OrderBookSnapshot> {
        self.snapshots.get(&timestamp)
    }

    /// Retrieve the most recent stored snapshot at or before `timestamp`.
    pub fn get_snapshot_at_or_before(&self, timestamp: i64) -> Option<&OrderBookSnapshot> {
        self.snapshots
            .range(..=timestamp)
            .next_back()
            .map(|(_, s)| s)
    }

    /// Best bid (highest price level), if any.
    pub fn best_bid(&self) -> Option<OrderBookLevel> {
        self.bids.values().next_back().map(InternalLevel::as_level)
    }

    /// Best ask (lowest price level), if any.
    pub fn best_ask(&self) -> Option<OrderBookLevel> {
        self.asks.values().next().map(InternalLevel::as_level)
    }

    /// Spread in absolute price terms, if both sides are present.
    pub fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some(ask.price - bid.price),
            _ => None,
        }
    }

    /// Mid price, if both sides are present.
    pub fn mid_price(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(bid), Some(ask)) => Some((bid.price + ask.price) / 2.0),
            _ => None,
        }
    }

    /// Number of bid levels.
    pub fn bid_depth(&self) -> usize {
        self.bids.len()
    }

    /// Number of ask levels.
    pub fn ask_depth(&self) -> usize {
        self.asks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_delta(
        action: DeltaAction,
        side: BookSide,
        price: f64,
        quantity: f64,
        order_count: u32,
    ) -> OrderBookDelta {
        OrderBookDelta {
            action,
            side,
            price,
            quantity,
            order_count,
        }
    }

    #[test]
    fn build_book_from_deltas_and_check_best() {
        let mut store = OrderBookStore::new("BTC/USD");

        store.apply_delta(&make_delta(DeltaAction::Add, BookSide::Bid, 100.0, 5.0, 3));
        store.apply_delta(&make_delta(DeltaAction::Add, BookSide::Bid, 99.0, 10.0, 5));
        store.apply_delta(&make_delta(DeltaAction::Add, BookSide::Ask, 101.0, 2.0, 1));
        store.apply_delta(&make_delta(DeltaAction::Add, BookSide::Ask, 102.0, 8.0, 4));

        let best_bid = store.best_bid().unwrap();
        assert_eq!(best_bid.price, 100.0);
        assert_eq!(best_bid.quantity, 5.0);

        let best_ask = store.best_ask().unwrap();
        assert_eq!(best_ask.price, 101.0);

        assert!((store.spread().unwrap() - 1.0).abs() < 1e-9);
        assert!((store.mid_price().unwrap() - 100.5).abs() < 1e-9);
    }

    #[test]
    fn modify_and_delete_levels() {
        let mut store = OrderBookStore::new("ETH/USD");

        store.apply_delta(&make_delta(DeltaAction::Add, BookSide::Bid, 50.0, 10.0, 2));
        store.apply_delta(&make_delta(DeltaAction::Add, BookSide::Bid, 49.0, 20.0, 5));

        // Modify the 50.0 level.
        store.apply_delta(&make_delta(
            DeltaAction::Modify,
            BookSide::Bid,
            50.0,
            15.0,
            3,
        ));
        let best = store.best_bid().unwrap();
        assert_eq!(best.quantity, 15.0);
        assert_eq!(best.order_count, 3);

        // Delete the 50.0 level — best bid should become 49.0.
        store.apply_delta(&make_delta(DeltaAction::Delete, BookSide::Bid, 50.0, 0.0, 0));
        let best = store.best_bid().unwrap();
        assert_eq!(best.price, 49.0);
        assert_eq!(store.bid_depth(), 1);
    }

    #[test]
    fn snapshot_round_trip() {
        let mut store = OrderBookStore::new("BTC/USD");

        store.apply_delta(&make_delta(DeltaAction::Add, BookSide::Bid, 100.0, 5.0, 3));
        store.apply_delta(&make_delta(DeltaAction::Add, BookSide::Ask, 101.0, 2.0, 1));

        let snap = store.save_snapshot(1000);
        assert_eq!(snap.bids.len(), 1);
        assert_eq!(snap.asks.len(), 1);
        assert_eq!(snap.bids[0].price, 100.0);
        assert_eq!(snap.asks[0].price, 101.0);

        // Create a new store and load the snapshot.
        let mut store2 = OrderBookStore::new("BTC/USD");
        store2.load_snapshot(&snap);
        assert_eq!(store2.best_bid().unwrap().price, 100.0);
        assert_eq!(store2.best_ask().unwrap().price, 101.0);
    }

    #[test]
    fn reconstruct_book_at_timestamp() {
        let mut store = OrderBookStore::new("BTC/USD");

        // State at T=100.
        store.apply_deltas(
            &[
                make_delta(DeltaAction::Add, BookSide::Bid, 100.0, 5.0, 1),
                make_delta(DeltaAction::Add, BookSide::Ask, 101.0, 3.0, 1),
            ],
            1,
        );
        store.save_snapshot(100);

        // State at T=200 — price moves up.
        store.apply_deltas(
            &[
                make_delta(DeltaAction::Delete, BookSide::Bid, 100.0, 0.0, 0),
                make_delta(DeltaAction::Add, BookSide::Bid, 101.0, 4.0, 2),
                make_delta(DeltaAction::Delete, BookSide::Ask, 101.0, 0.0, 0),
                make_delta(DeltaAction::Add, BookSide::Ask, 102.0, 6.0, 3),
            ],
            2,
        );
        store.save_snapshot(200);

        // Query the historical snapshot at T=100.
        let snap100 = store.get_snapshot(100).unwrap();
        assert_eq!(snap100.bids[0].price, 100.0);
        assert_eq!(snap100.asks[0].price, 101.0);

        // Query the snapshot at T=200.
        let snap200 = store.get_snapshot(200).unwrap();
        assert_eq!(snap200.bids[0].price, 101.0);
        assert_eq!(snap200.asks[0].price, 102.0);

        // at_or_before T=150 should give us the T=100 snapshot.
        let snap_before = store.get_snapshot_at_or_before(150).unwrap();
        assert_eq!(snap_before.timestamp, 100);
    }

    #[test]
    fn load_full_snapshot() {
        let snap = OrderBookSnapshot {
            symbol: "SOL/USD".into(),
            timestamp: 500,
            bids: vec![
                OrderBookLevel {
                    price: 25.0,
                    quantity: 100.0,
                    order_count: 10,
                },
                OrderBookLevel {
                    price: 24.5,
                    quantity: 200.0,
                    order_count: 15,
                },
            ],
            asks: vec![
                OrderBookLevel {
                    price: 25.5,
                    quantity: 50.0,
                    order_count: 5,
                },
                OrderBookLevel {
                    price: 26.0,
                    quantity: 150.0,
                    order_count: 12,
                },
            ],
            sequence: 42,
        };

        let mut store = OrderBookStore::new("SOL/USD");
        store.load_snapshot(&snap);

        assert_eq!(store.bid_depth(), 2);
        assert_eq!(store.ask_depth(), 2);
        assert_eq!(store.best_bid().unwrap().price, 25.0);
        assert_eq!(store.best_ask().unwrap().price, 25.5);
        assert!((store.spread().unwrap() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn empty_book() {
        let store = OrderBookStore::new("DOGE/USD");
        assert!(store.best_bid().is_none());
        assert!(store.best_ask().is_none());
        assert!(store.spread().is_none());
        assert!(store.mid_price().is_none());
        assert_eq!(store.bid_depth(), 0);
        assert_eq!(store.ask_depth(), 0);
    }
}
