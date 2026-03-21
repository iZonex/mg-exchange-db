use serde::{Deserialize, Serialize};

/// Supported bar time frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TimeFrame {
    /// 1 second
    S1,
    /// 1 minute
    M1,
    /// 5 minutes
    M5,
    /// 15 minutes
    M15,
    /// 1 hour
    H1,
    /// 4 hours
    H4,
    /// 1 day
    D1,
    /// 1 week
    W1,
}

impl TimeFrame {
    /// Duration of this time frame in nanoseconds.
    pub fn as_nanos(&self) -> i64 {
        match self {
            Self::S1 => 1_000_000_000,
            Self::M1 => 60 * 1_000_000_000,
            Self::M5 => 5 * 60 * 1_000_000_000,
            Self::M15 => 15 * 60 * 1_000_000_000,
            Self::H1 => 3_600 * 1_000_000_000,
            Self::H4 => 4 * 3_600 * 1_000_000_000,
            Self::D1 => 86_400 * 1_000_000_000,
            Self::W1 => 7 * 86_400 * 1_000_000_000,
        }
    }

    /// Align a nanosecond timestamp down to the start of its interval.
    pub fn truncate(&self, timestamp_nanos: i64) -> i64 {
        let interval = self.as_nanos();
        // Integer floor-division that works for negative timestamps too.
        let div = timestamp_nanos.div_euclid(interval);
        div * interval
    }
}

/// A single OHLCV candlestick bar.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct OhlcvBar {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    /// Bar open timestamp (nanoseconds since epoch), aligned to the time frame.
    pub timestamp: i64,
    /// Number of trades that contributed to this bar.
    pub trade_count: u64,
}

/// Streaming OHLCV aggregator.
///
/// Feed it ticks (price, volume, timestamp) and it accumulates bars. When a
/// tick falls into a new interval the previous bar is emitted.
pub struct OhlcvAggregator {
    time_frame: TimeFrame,
    current_bar: Option<OhlcvBar>,
}

impl OhlcvAggregator {
    pub fn new(time_frame: TimeFrame) -> Self {
        Self {
            time_frame,
            current_bar: None,
        }
    }

    /// Returns the time frame this aggregator uses.
    pub fn time_frame(&self) -> TimeFrame {
        self.time_frame
    }

    /// Process a single tick.
    ///
    /// Returns `Some(bar)` when the tick closes a previous bar (i.e., falls
    /// into a new interval), otherwise returns `None`.
    pub fn update(&mut self, price: f64, volume: f64, timestamp: i64) -> Option<OhlcvBar> {
        let bar_ts = self.time_frame.truncate(timestamp);

        match &mut self.current_bar {
            Some(bar) if bar.timestamp == bar_ts => {
                // Same bar — update in place.
                if price > bar.high {
                    bar.high = price;
                }
                if price < bar.low {
                    bar.low = price;
                }
                bar.close = price;
                bar.volume += volume;
                bar.trade_count += 1;
                None
            }
            Some(_) => {
                // New interval — emit the completed bar and start fresh.
                let completed = self.current_bar.take().unwrap();
                self.current_bar = Some(OhlcvBar {
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume,
                    timestamp: bar_ts,
                    trade_count: 1,
                });
                Some(completed)
            }
            None => {
                // First tick ever.
                self.current_bar = Some(OhlcvBar {
                    open: price,
                    high: price,
                    low: price,
                    close: price,
                    volume,
                    timestamp: bar_ts,
                    trade_count: 1,
                });
                None
            }
        }
    }

    /// Flush the currently accumulating bar (e.g., at end of data stream).
    /// Returns `None` if no ticks have been processed.
    pub fn flush(&mut self) -> Option<OhlcvBar> {
        self.current_bar.take()
    }

    /// Peek at the bar currently being built without consuming it.
    pub fn current_bar(&self) -> Option<&OhlcvBar> {
        self.current_bar.as_ref()
    }

    /// Feed a batch of ticks and collect all completed bars plus the final
    /// (potentially incomplete) bar.
    pub fn aggregate_all(
        &mut self,
        ticks: &[(f64, f64, i64)], // (price, volume, timestamp)
    ) -> Vec<OhlcvBar> {
        let mut bars = Vec::new();
        for &(price, volume, ts) in ticks {
            if let Some(bar) = self.update(price, volume, ts) {
                bars.push(bar);
            }
        }
        if let Some(bar) = self.flush() {
            bars.push(bar);
        }
        bars
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: i64 = 1_000_000_000;
    const MINUTE: i64 = 60 * SECOND;

    #[test]
    fn time_frame_truncate() {
        let tf = TimeFrame::M1;
        // 90.5 seconds -> truncated to 60s (minute boundary)
        let ts = 90 * SECOND + 500_000_000;
        assert_eq!(tf.truncate(ts), MINUTE);
    }

    #[test]
    fn time_frame_truncate_on_boundary() {
        let tf = TimeFrame::S1;
        let ts = 5 * SECOND;
        assert_eq!(tf.truncate(ts), 5 * SECOND);
    }

    #[test]
    fn single_bar_aggregation() {
        let mut agg = OhlcvAggregator::new(TimeFrame::S1);
        let base = 10 * SECOND;

        // Three ticks in the same 1-second window.
        assert!(agg.update(100.0, 1.0, base).is_none());
        assert!(agg.update(102.0, 2.0, base + 100).is_none());
        assert!(agg.update(99.0, 0.5, base + 500).is_none());

        let bar = agg.flush().unwrap();
        assert_eq!(bar.timestamp, base);
        assert_eq!(bar.open, 100.0);
        assert_eq!(bar.high, 102.0);
        assert_eq!(bar.low, 99.0);
        assert_eq!(bar.close, 99.0);
        assert!((bar.volume - 3.5).abs() < 1e-9);
        assert_eq!(bar.trade_count, 3);
    }

    #[test]
    fn multiple_bars() {
        let mut agg = OhlcvAggregator::new(TimeFrame::S1);

        // Bar 0 (0s–1s)
        assert!(agg.update(100.0, 1.0, 0).is_none());
        assert!(agg.update(101.0, 1.0, 500_000_000).is_none());

        // This tick is in the next second, so bar 0 is emitted.
        let bar0 = agg.update(102.0, 2.0, SECOND + 100).unwrap();
        assert_eq!(bar0.timestamp, 0);
        assert_eq!(bar0.open, 100.0);
        assert_eq!(bar0.high, 101.0);
        assert_eq!(bar0.close, 101.0);
        assert_eq!(bar0.trade_count, 2);

        // Another tick in second 1.
        assert!(agg.update(103.0, 1.0, SECOND + 500_000_000).is_none());

        // Tick in second 2 -> emits bar for second 1.
        let bar1 = agg.update(99.0, 0.5, 2 * SECOND).unwrap();
        assert_eq!(bar1.timestamp, SECOND);
        assert_eq!(bar1.open, 102.0);
        assert_eq!(bar1.high, 103.0);
        assert_eq!(bar1.low, 102.0);
        assert_eq!(bar1.close, 103.0);
        assert_eq!(bar1.trade_count, 2);
    }

    #[test]
    fn aggregate_all_produces_complete_bars() {
        let mut agg = OhlcvAggregator::new(TimeFrame::M1);

        let ticks: Vec<(f64, f64, i64)> = vec![
            (100.0, 1.0, 0),
            (101.0, 1.0, 30 * SECOND),
            (102.0, 2.0, MINUTE + 10 * SECOND),
            (99.0, 0.5, MINUTE + 50 * SECOND),
            (105.0, 3.0, 2 * MINUTE),
        ];

        let bars = agg.aggregate_all(&ticks);
        // Bar for minute 0, minute 1, and incomplete minute 2.
        assert_eq!(bars.len(), 3);

        assert_eq!(bars[0].timestamp, 0);
        assert_eq!(bars[0].trade_count, 2);
        assert_eq!(bars[0].open, 100.0);
        assert_eq!(bars[0].high, 101.0);

        assert_eq!(bars[1].timestamp, MINUTE);
        assert_eq!(bars[1].trade_count, 2);
        assert_eq!(bars[1].low, 99.0);

        assert_eq!(bars[2].timestamp, 2 * MINUTE);
        assert_eq!(bars[2].trade_count, 1);
        assert_eq!(bars[2].open, 105.0);
    }

    #[test]
    fn five_minute_bars() {
        let mut agg = OhlcvAggregator::new(TimeFrame::M5);

        // Tick at 3 minutes (within first 5m bar).
        assert!(agg.update(50.0, 1.0, 3 * MINUTE).is_none());
        // Tick at 4m59s (still within first 5m bar).
        assert!(agg.update(55.0, 1.0, 4 * MINUTE + 59 * SECOND).is_none());
        // Tick at 5m0s (new bar).
        let bar = agg.update(60.0, 1.0, 5 * MINUTE).unwrap();
        assert_eq!(bar.timestamp, 0);
        assert_eq!(bar.open, 50.0);
        assert_eq!(bar.high, 55.0);
        assert_eq!(bar.close, 55.0);
        assert_eq!(bar.trade_count, 2);
    }

    #[test]
    fn no_ticks_flush_returns_none() {
        let mut agg = OhlcvAggregator::new(TimeFrame::H1);
        assert!(agg.flush().is_none());
    }
}
