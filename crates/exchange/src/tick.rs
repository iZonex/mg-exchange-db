use serde::{Deserialize, Serialize};

/// Trade side (aggressor direction).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

/// A single trade tick.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Tick {
    /// Nanosecond-precision timestamp.
    pub timestamp: i64,
    /// Trade price.
    pub price: f64,
    /// Trade volume (quantity).
    pub volume: f64,
    /// Aggressor side.
    pub side: Side,
    /// Exchange-assigned trade identifier.
    pub trade_id: u64,
}

/// A batch buffer that accumulates incoming ticks and sorts them by timestamp
/// before flushing. Useful for handling out-of-order data from exchange feeds.
#[derive(Debug)]
pub struct TickBuffer {
    ticks: Vec<Tick>,
    capacity: usize,
}

impl TickBuffer {
    /// Create a new buffer that will auto-sort on flush.
    pub fn new(capacity: usize) -> Self {
        Self {
            ticks: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// Push a tick into the buffer.
    pub fn push(&mut self, tick: Tick) {
        self.ticks.push(tick);
    }

    /// Returns true when the buffer has reached its configured capacity.
    pub fn is_full(&self) -> bool {
        self.ticks.len() >= self.capacity
    }

    /// Number of ticks currently in the buffer.
    pub fn len(&self) -> usize {
        self.ticks.len()
    }

    /// True if the buffer contains no ticks.
    pub fn is_empty(&self) -> bool {
        self.ticks.is_empty()
    }

    /// Sort ticks by timestamp and drain the buffer, returning the sorted batch.
    pub fn flush(&mut self) -> Vec<Tick> {
        self.ticks.sort_by_key(|t| t.timestamp);
        std::mem::replace(&mut self.ticks, Vec::with_capacity(self.capacity))
    }
}

// ---------------------------------------------------------------------------
// Delta encoding helpers for price compression
// ---------------------------------------------------------------------------

/// Encode a sequence of f64 prices as integer deltas (in a fixed number of
/// decimal digits). Returns the base price (first value) and the delta sequence.
///
/// `decimal_digits` controls precision: 2 means prices are in cents, 8 for
/// satoshis, etc.
pub fn delta_encode_prices(prices: &[f64], decimal_digits: u32) -> (i64, Vec<i64>) {
    if prices.is_empty() {
        return (0, Vec::new());
    }

    let scale = 10_f64.powi(decimal_digits as i32);

    let mut ints: Vec<i64> = prices.iter().map(|p| (p * scale).round() as i64).collect();
    let base = ints[0];

    // Replace values with deltas (in-place, backwards to avoid overwrite issues).
    for i in (1..ints.len()).rev() {
        ints[i] -= ints[i - 1];
    }
    // The first element stays as the absolute base; remove it from the delta vec.
    let deltas = ints[1..].to_vec();

    (base, deltas)
}

/// Reconstruct prices from a base value and delta sequence.
pub fn delta_decode_prices(base: i64, deltas: &[i64], decimal_digits: u32) -> Vec<f64> {
    if base == 0 && deltas.is_empty() {
        return Vec::new();
    }
    let scale = 10_f64.powi(decimal_digits as i32);
    let mut result = Vec::with_capacity(deltas.len() + 1);
    result.push(base as f64 / scale);

    let mut current = base;
    for &d in deltas {
        current += d;
        result.push(current as f64 / scale);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_buffer_sorts_by_timestamp() {
        let mut buf = TickBuffer::new(8);
        buf.push(Tick {
            timestamp: 300,
            price: 100.0,
            volume: 1.0,
            side: Side::Buy,
            trade_id: 1,
        });
        buf.push(Tick {
            timestamp: 100,
            price: 99.0,
            volume: 2.0,
            side: Side::Sell,
            trade_id: 2,
        });
        buf.push(Tick {
            timestamp: 200,
            price: 101.0,
            volume: 0.5,
            side: Side::Buy,
            trade_id: 3,
        });

        let sorted = buf.flush();
        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0].timestamp, 100);
        assert_eq!(sorted[1].timestamp, 200);
        assert_eq!(sorted[2].timestamp, 300);
        assert!(buf.is_empty());
    }

    #[test]
    fn tick_buffer_is_full() {
        let mut buf = TickBuffer::new(2);
        assert!(!buf.is_full());
        buf.push(Tick {
            timestamp: 1,
            price: 1.0,
            volume: 1.0,
            side: Side::Buy,
            trade_id: 1,
        });
        assert!(!buf.is_full());
        buf.push(Tick {
            timestamp: 2,
            price: 2.0,
            volume: 1.0,
            side: Side::Buy,
            trade_id: 2,
        });
        assert!(buf.is_full());
    }

    #[test]
    fn delta_encode_decode_round_trip() {
        let prices = vec![100.25, 100.30, 100.28, 100.35, 100.50];
        let (base, deltas) = delta_encode_prices(&prices, 2);

        assert_eq!(base, 10025); // 100.25 * 100
        assert_eq!(deltas, vec![5, -2, 7, 15]); // diffs in cents

        let reconstructed = delta_decode_prices(base, &deltas, 2);
        assert_eq!(reconstructed.len(), prices.len());
        for (orig, rec) in prices.iter().zip(reconstructed.iter()) {
            assert!((orig - rec).abs() < 1e-9, "{} != {}", orig, rec);
        }
    }

    #[test]
    fn delta_encode_empty() {
        let (base, deltas) = delta_encode_prices(&[], 2);
        assert_eq!(base, 0);
        assert!(deltas.is_empty());

        let reconstructed = delta_decode_prices(base, &deltas, 2);
        assert!(reconstructed.is_empty());
    }

    #[test]
    fn delta_encode_single_value() {
        let prices = vec![42.50];
        let (base, deltas) = delta_encode_prices(&prices, 2);
        assert_eq!(base, 4250);
        assert!(deltas.is_empty());

        let reconstructed = delta_decode_prices(base, &deltas, 2);
        assert_eq!(reconstructed.len(), 1);
        assert!((reconstructed[0] - 42.50).abs() < 1e-9);
    }

    #[test]
    fn delta_encode_high_precision() {
        let prices = vec![0.00012345, 0.00012350, 0.00012340];
        let (base, deltas) = delta_encode_prices(&prices, 8);
        assert_eq!(base, 12345); // 0.00012345 * 10^8
        assert_eq!(deltas, vec![5, -10]);

        let reconstructed = delta_decode_prices(base, &deltas, 8);
        for (orig, rec) in prices.iter().zip(reconstructed.iter()) {
            assert!((orig - rec).abs() < 1e-12, "{} != {}", orig, rec);
        }
    }
}
