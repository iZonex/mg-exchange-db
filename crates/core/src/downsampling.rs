//! Automatic downsampling for ExchangeDB.
//!
//! Provides materialized aggregation views at configurable time intervals
//! (e.g., 1-minute, 1-hour OHLCV bars from raw trade data).

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use exchange_common::error::{ExchangeDbError, Result};

/// A single aggregate row: (bucket_timestamp, partition_key, finalized_values).
type AggregateRow = (i64, Vec<String>, Vec<f64>);
use serde::{Deserialize, Serialize};

/// Configuration for downsampling a single source table.
#[derive(Debug, Clone)]
pub struct DownsamplingConfig {
    /// Name of the source table to downsample.
    pub source_table: String,
    /// List of downsampling intervals to maintain.
    pub intervals: Vec<DownsampleInterval>,
    /// Whether to auto-refresh on a schedule.
    pub auto_refresh: bool,
    /// How far behind real-time the refresh operates (e.g., 1 minute lag).
    pub refresh_lag: Duration,
}

/// A single downsampling interval definition.
#[derive(Debug, Clone)]
pub struct DownsampleInterval {
    /// Name for the downsampled output (e.g., "trades_1m").
    pub name: String,
    /// Aggregation interval duration (e.g., 1 minute, 1 hour).
    pub interval: Duration,
    /// Columns to compute in each interval bucket.
    pub columns: Vec<DownsampleColumn>,
    /// Columns to partition (group) by (e.g., ["symbol"]).
    pub partition_by: Vec<String>,
}

/// Defines how a single output column is computed from source data.
#[derive(Debug, Clone)]
pub enum DownsampleColumn {
    /// First value in the interval.
    First { source: String, alias: String },
    /// Last value in the interval.
    Last { source: String, alias: String },
    /// Minimum value in the interval.
    Min { source: String, alias: String },
    /// Maximum value in the interval.
    Max { source: String, alias: String },
    /// Sum of values in the interval.
    Sum { source: String, alias: String },
    /// Average of values in the interval.
    Avg { source: String, alias: String },
    /// Count of rows in the interval.
    Count { alias: String },
}

impl DownsampleColumn {
    /// Get the output alias name.
    pub fn alias(&self) -> &str {
        match self {
            Self::First { alias, .. }
            | Self::Last { alias, .. }
            | Self::Min { alias, .. }
            | Self::Max { alias, .. }
            | Self::Sum { alias, .. }
            | Self::Avg { alias, .. }
            | Self::Count { alias } => alias,
        }
    }

    /// Get the source column name (None for Count).
    pub fn source(&self) -> Option<&str> {
        match self {
            Self::First { source, .. }
            | Self::Last { source, .. }
            | Self::Min { source, .. }
            | Self::Max { source, .. }
            | Self::Sum { source, .. }
            | Self::Avg { source, .. } => Some(source),
            Self::Count { .. } => None,
        }
    }
}

/// Statistics from a downsampling refresh operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownsampleStats {
    /// Number of source rows processed.
    pub rows_processed: u64,
    /// Number of aggregated rows written.
    pub rows_written: u64,
    /// Number of intervals that were refreshed.
    pub intervals_refreshed: u32,
}

/// Status of a single downsampled interval.
#[derive(Debug, Clone)]
pub struct DownsampleStatus {
    /// Name of the downsampled view.
    pub name: String,
    /// Timestamp of the last refresh (nanoseconds), if any.
    pub last_refresh: Option<i64>,
    /// Estimated number of source rows not yet processed.
    pub rows_behind: u64,
    /// Current refresh status.
    pub status: RefreshStatus,
}

/// Whether a downsampled view is up-to-date.
#[derive(Debug, Clone)]
pub enum RefreshStatus {
    /// The view is current.
    UpToDate,
    /// The view has unprocessed source data.
    Stale,
    /// An error occurred during the last refresh.
    Error(String),
}

/// Persistent state for a downsampled interval.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct IntervalState {
    /// Name of the interval.
    name: String,
    /// Timestamp (nanos) of the last refresh.
    last_refresh: Option<i64>,
    /// The watermark: source rows up to this timestamp (nanos) have been processed.
    watermark: i64,
}

/// An in-memory representation of a single aggregation bucket.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct AggBucket {
    /// The interval-aligned timestamp for this bucket.
    bucket_ts: i64,
    /// Per-column aggregation state.
    columns: Vec<AggState>,
}

/// Aggregation state for a single column within a bucket.
#[derive(Debug, Clone)]
enum AggState {
    First { value: Option<f64> },
    Last { value: Option<f64> },
    Min { value: Option<f64> },
    Max { value: Option<f64> },
    Sum { value: f64 },
    Avg { sum: f64, count: u64 },
    Count { count: u64 },
}

impl AggState {
    fn new(col: &DownsampleColumn) -> Self {
        match col {
            DownsampleColumn::First { .. } => AggState::First { value: None },
            DownsampleColumn::Last { .. } => AggState::Last { value: None },
            DownsampleColumn::Min { .. } => AggState::Min { value: None },
            DownsampleColumn::Max { .. } => AggState::Max { value: None },
            DownsampleColumn::Sum { .. } => AggState::Sum { value: 0.0 },
            DownsampleColumn::Avg { .. } => AggState::Avg { sum: 0.0, count: 0 },
            DownsampleColumn::Count { .. } => AggState::Count { count: 0 },
        }
    }

    fn update(&mut self, value: f64) {
        match self {
            AggState::First { value: v } => {
                if v.is_none() {
                    *v = Some(value);
                }
            }
            AggState::Last { value: v } => {
                *v = Some(value);
            }
            AggState::Min { value: v } => {
                *v = Some(v.map_or(value, |cur| cur.min(value)));
            }
            AggState::Max { value: v } => {
                *v = Some(v.map_or(value, |cur| cur.max(value)));
            }
            AggState::Sum { value: v } => {
                *v += value;
            }
            AggState::Avg { sum, count } => {
                *sum += value;
                *count += 1;
            }
            AggState::Count { count } => {
                *count += 1;
            }
        }
    }

    fn finalize(&self) -> f64 {
        match self {
            AggState::First { value } => value.unwrap_or(0.0),
            AggState::Last { value } => value.unwrap_or(0.0),
            AggState::Min { value } => value.unwrap_or(0.0),
            AggState::Max { value } => value.unwrap_or(0.0),
            AggState::Sum { value } => *value,
            AggState::Avg { sum, count } => {
                if *count > 0 {
                    *sum / *count as f64
                } else {
                    0.0
                }
            }
            AggState::Count { count } => *count as f64,
        }
    }
}

/// A single source row for downsampling.
#[derive(Debug, Clone)]
pub struct SourceRow {
    /// Nanosecond timestamp of this row.
    pub timestamp: i64,
    /// Named column values.
    pub columns: HashMap<String, f64>,
}

/// Manages downsampling configurations and refresh operations.
#[allow(dead_code)]
pub struct DownsamplingManager {
    db_root: PathBuf,
    configs: Vec<DownsamplingConfig>,
    /// Per-interval persistent state (name -> state).
    states: HashMap<String, IntervalState>,
}

impl DownsamplingManager {
    /// Create a new downsampling manager.
    pub fn new(db_root: PathBuf) -> Self {
        Self {
            db_root,
            configs: Vec::new(),
            states: HashMap::new(),
        }
    }

    /// Register a downsampling configuration.
    pub fn register(&mut self, config: DownsamplingConfig) -> Result<()> {
        // Initialize state for each interval.
        for interval in &config.intervals {
            let state = IntervalState {
                name: interval.name.clone(),
                last_refresh: None,
                watermark: 0,
            };
            self.states.insert(interval.name.clone(), state);
        }
        self.configs.push(config);
        Ok(())
    }

    /// Refresh all downsampled views using the provided source data.
    ///
    /// In a real system this would read from the column store; here we accept
    /// pre-loaded rows for testability.
    pub fn refresh_all_with_data(&mut self, source_rows: &[SourceRow]) -> Result<DownsampleStats> {
        let mut total = DownsampleStats {
            rows_processed: 0,
            rows_written: 0,
            intervals_refreshed: 0,
        };

        // Process each config.
        for config in &self.configs {
            for interval in &config.intervals {
                let stats = Self::refresh_interval(interval, source_rows, &mut self.states)?;
                total.rows_processed += stats.rows_processed;
                total.rows_written += stats.rows_written;
                total.intervals_refreshed += stats.intervals_refreshed;
            }
        }

        Ok(total)
    }

    /// Refresh a specific interval by name.
    pub fn refresh_with_data(
        &mut self,
        interval_name: &str,
        source_rows: &[SourceRow],
    ) -> Result<DownsampleStats> {
        for config in &self.configs {
            for interval in &config.intervals {
                if interval.name == interval_name {
                    return Self::refresh_interval(interval, source_rows, &mut self.states);
                }
            }
        }

        Err(ExchangeDbError::TableNotFound(format!(
            "downsampling interval '{}' not found",
            interval_name
        )))
    }

    /// Refresh a single interval from source data.
    fn refresh_interval(
        interval: &DownsampleInterval,
        source_rows: &[SourceRow],
        states: &mut HashMap<String, IntervalState>,
    ) -> Result<DownsampleStats> {
        let interval_nanos = interval.interval.as_nanos() as i64;
        if interval_nanos == 0 {
            return Err(ExchangeDbError::Query(
                "downsampling interval must be > 0".into(),
            ));
        }

        let state = states.get(&interval.name);
        let watermark = state.map_or(0, |s| s.watermark);

        // Filter rows newer than the watermark.
        let rows_to_process: Vec<&SourceRow> = source_rows
            .iter()
            .filter(|r| r.timestamp > watermark)
            .collect();

        let rows_processed = rows_to_process.len() as u64;

        // Group by partition key + time bucket.
        // Key: (partition_key_values, bucket_timestamp)
        let mut buckets: HashMap<(Vec<String>, i64), Vec<AggState>> = HashMap::new();

        for row in &rows_to_process {
            let bucket_ts = (row.timestamp / interval_nanos) * interval_nanos;

            // Compute partition key.
            let partition_key: Vec<String> = interval
                .partition_by
                .iter()
                .map(|col| {
                    row.columns
                        .get(col)
                        .map(|v| format!("{}", v))
                        .unwrap_or_default()
                })
                .collect();

            let key = (partition_key, bucket_ts);
            let agg_states = buckets
                .entry(key)
                .or_insert_with(|| interval.columns.iter().map(AggState::new).collect());

            // Update each aggregation.
            for (i, col_def) in interval.columns.iter().enumerate() {
                let value = match col_def {
                    DownsampleColumn::Count { .. } => 1.0, // just needs update call
                    other => {
                        if let Some(src) = other.source() {
                            row.columns.get(src).copied().unwrap_or(0.0)
                        } else {
                            0.0
                        }
                    }
                };
                agg_states[i].update(value);
            }
        }

        let rows_written = buckets.len() as u64;

        // Update watermark.
        if let Some(max_ts) = rows_to_process.iter().map(|r| r.timestamp).max()
            && let Some(state) = states.get_mut(&interval.name)
        {
            state.watermark = max_ts;
            state.last_refresh = Some(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as i64,
            );
        }

        Ok(DownsampleStats {
            rows_processed,
            rows_written,
            intervals_refreshed: if rows_processed > 0 { 1 } else { 0 },
        })
    }

    /// Get the aggregate results for a specific interval from source data.
    /// Returns a list of (bucket_timestamp, partition_key, finalized_values).
    pub fn compute_aggregates(
        &self,
        interval_name: &str,
        source_rows: &[SourceRow],
    ) -> Result<Vec<AggregateRow>> {
        let interval = self
            .configs
            .iter()
            .flat_map(|c| &c.intervals)
            .find(|i| i.name == interval_name)
            .ok_or_else(|| {
                ExchangeDbError::TableNotFound(format!(
                    "downsampling interval '{}' not found",
                    interval_name
                ))
            })?;

        let interval_nanos = interval.interval.as_nanos() as i64;
        if interval_nanos == 0 {
            return Err(ExchangeDbError::Query(
                "downsampling interval must be > 0".into(),
            ));
        }

        let mut buckets: HashMap<(Vec<String>, i64), Vec<AggState>> = HashMap::new();

        for row in source_rows {
            let bucket_ts = (row.timestamp / interval_nanos) * interval_nanos;

            let partition_key: Vec<String> = interval
                .partition_by
                .iter()
                .map(|col| {
                    row.columns
                        .get(col)
                        .map(|v| format!("{}", v))
                        .unwrap_or_default()
                })
                .collect();

            let key = (partition_key, bucket_ts);
            let agg_states = buckets
                .entry(key)
                .or_insert_with(|| interval.columns.iter().map(AggState::new).collect());

            for (i, col_def) in interval.columns.iter().enumerate() {
                let value = match col_def {
                    DownsampleColumn::Count { .. } => 1.0,
                    other => {
                        if let Some(src) = other.source() {
                            row.columns.get(src).copied().unwrap_or(0.0)
                        } else {
                            0.0
                        }
                    }
                };
                agg_states[i].update(value);
            }
        }

        let mut results: Vec<AggregateRow> = buckets
            .into_iter()
            .map(|((partition_key, bucket_ts), states)| {
                let values: Vec<f64> = states.iter().map(|s| s.finalize()).collect();
                (bucket_ts, partition_key, values)
            })
            .collect();

        results.sort_by_key(|(ts, pk, _)| (*ts, pk.clone()));
        Ok(results)
    }

    /// Get refresh status for all intervals.
    pub fn status(&self) -> Vec<DownsampleStatus> {
        let mut statuses = Vec::new();
        for config in &self.configs {
            for interval in &config.intervals {
                let state = self.states.get(&interval.name);
                statuses.push(DownsampleStatus {
                    name: interval.name.clone(),
                    last_refresh: state.and_then(|s| s.last_refresh),
                    rows_behind: 0, // Would need source row count to compute.
                    status: if state.and_then(|s| s.last_refresh).is_some() {
                        RefreshStatus::UpToDate
                    } else {
                        RefreshStatus::Stale
                    },
                });
            }
        }
        statuses
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_trades() -> Vec<SourceRow> {
        let minute = 60_000_000_000i64; // 1 minute in nanos
        let base = 1_710_513_000_000_000_000i64;

        vec![
            // Minute 0: 2 trades
            SourceRow {
                timestamp: base,
                columns: HashMap::from([("price".into(), 100.0), ("volume".into(), 10.0)]),
            },
            SourceRow {
                timestamp: base + 30_000_000_000, // +30s
                columns: HashMap::from([("price".into(), 105.0), ("volume".into(), 5.0)]),
            },
            // Minute 1: 2 trades
            SourceRow {
                timestamp: base + minute + 10_000_000_000, // +1m10s
                columns: HashMap::from([("price".into(), 102.0), ("volume".into(), 8.0)]),
            },
            SourceRow {
                timestamp: base + minute + 50_000_000_000, // +1m50s
                columns: HashMap::from([("price".into(), 98.0), ("volume".into(), 12.0)]),
            },
            // Minute 2: 1 trade
            SourceRow {
                timestamp: base + 2 * minute,
                columns: HashMap::from([("price".into(), 110.0), ("volume".into(), 3.0)]),
            },
        ]
    }

    fn make_ohlcv_interval() -> DownsampleInterval {
        DownsampleInterval {
            name: "trades_1m".to_string(),
            interval: Duration::from_secs(60),
            columns: vec![
                DownsampleColumn::First {
                    source: "price".into(),
                    alias: "open".into(),
                },
                DownsampleColumn::Max {
                    source: "price".into(),
                    alias: "high".into(),
                },
                DownsampleColumn::Min {
                    source: "price".into(),
                    alias: "low".into(),
                },
                DownsampleColumn::Last {
                    source: "price".into(),
                    alias: "close".into(),
                },
                DownsampleColumn::Sum {
                    source: "volume".into(),
                    alias: "volume".into(),
                },
                DownsampleColumn::Count {
                    alias: "trade_count".into(),
                },
            ],
            partition_by: vec![],
        }
    }

    #[test]
    fn test_register_and_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = DownsamplingManager::new(dir.path().to_path_buf());

        let config = DownsamplingConfig {
            source_table: "trades".into(),
            intervals: vec![make_ohlcv_interval()],
            auto_refresh: true,
            refresh_lag: Duration::from_secs(60),
        };
        mgr.register(config).unwrap();

        let trades = make_trades();
        let stats = mgr.refresh_all_with_data(&trades).unwrap();

        assert_eq!(stats.rows_processed, 5);
        assert_eq!(stats.rows_written, 3); // 3 one-minute buckets
        assert_eq!(stats.intervals_refreshed, 1);
    }

    #[test]
    fn test_compute_aggregates_ohlcv() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = DownsamplingManager::new(dir.path().to_path_buf());

        let config = DownsamplingConfig {
            source_table: "trades".into(),
            intervals: vec![make_ohlcv_interval()],
            auto_refresh: true,
            refresh_lag: Duration::from_secs(60),
        };
        mgr.register(config).unwrap();

        let trades = make_trades();
        let results = mgr.compute_aggregates("trades_1m", &trades).unwrap();

        // 3 minute buckets.
        assert_eq!(results.len(), 3);

        // Minute 0: open=100, high=105, low=100, close=105, volume=15, count=2
        let (_, _, vals0) = &results[0];
        assert_eq!(vals0[0], 100.0); // open (first)
        assert_eq!(vals0[1], 105.0); // high (max)
        assert_eq!(vals0[2], 100.0); // low (min)
        assert_eq!(vals0[3], 105.0); // close (last)
        assert!((vals0[4] - 15.0).abs() < 1e-9); // volume (sum)
        assert_eq!(vals0[5], 2.0); // count

        // Minute 1: open=102, high=102, low=98, close=98, volume=20, count=2
        let (_, _, vals1) = &results[1];
        assert_eq!(vals1[0], 102.0); // open
        assert_eq!(vals1[1], 102.0); // high
        assert_eq!(vals1[2], 98.0); // low
        assert_eq!(vals1[3], 98.0); // close
        assert!((vals1[4] - 20.0).abs() < 1e-9); // volume
        assert_eq!(vals1[5], 2.0); // count

        // Minute 2: open=110, high=110, low=110, close=110, volume=3, count=1
        let (_, _, vals2) = &results[2];
        assert_eq!(vals2[0], 110.0);
        assert_eq!(vals2[1], 110.0);
        assert_eq!(vals2[2], 110.0);
        assert_eq!(vals2[3], 110.0);
        assert!((vals2[4] - 3.0).abs() < 1e-9);
        assert_eq!(vals2[5], 1.0);
    }

    #[test]
    fn test_incremental_refresh() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = DownsamplingManager::new(dir.path().to_path_buf());

        let config = DownsamplingConfig {
            source_table: "trades".into(),
            intervals: vec![make_ohlcv_interval()],
            auto_refresh: true,
            refresh_lag: Duration::from_secs(60),
        };
        mgr.register(config).unwrap();

        let trades = make_trades();

        // First refresh processes all 5 rows.
        let stats1 = mgr.refresh_all_with_data(&trades).unwrap();
        assert_eq!(stats1.rows_processed, 5);

        // Second refresh with same data: no new rows (all <= watermark).
        let stats2 = mgr.refresh_all_with_data(&trades).unwrap();
        assert_eq!(stats2.rows_processed, 0);
        assert_eq!(stats2.rows_written, 0);
    }

    #[test]
    fn test_status() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = DownsamplingManager::new(dir.path().to_path_buf());

        let config = DownsamplingConfig {
            source_table: "trades".into(),
            intervals: vec![make_ohlcv_interval()],
            auto_refresh: true,
            refresh_lag: Duration::from_secs(60),
        };
        mgr.register(config).unwrap();

        // Before any refresh, status should be Stale.
        let statuses = mgr.status();
        assert_eq!(statuses.len(), 1);
        assert_eq!(statuses[0].name, "trades_1m");
        assert!(matches!(statuses[0].status, RefreshStatus::Stale));

        // After refresh, status should be UpToDate.
        let trades = make_trades();
        mgr.refresh_all_with_data(&trades).unwrap();

        let statuses = mgr.status();
        assert!(matches!(statuses[0].status, RefreshStatus::UpToDate));
        assert!(statuses[0].last_refresh.is_some());
    }

    #[test]
    fn test_partitioned_downsampling() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = DownsamplingManager::new(dir.path().to_path_buf());

        let interval = DownsampleInterval {
            name: "trades_by_sym_1m".to_string(),
            interval: Duration::from_secs(60),
            columns: vec![
                DownsampleColumn::Sum {
                    source: "volume".into(),
                    alias: "total_volume".into(),
                },
                DownsampleColumn::Count {
                    alias: "trade_count".into(),
                },
            ],
            partition_by: vec!["symbol".into()],
        };

        let config = DownsamplingConfig {
            source_table: "trades".into(),
            intervals: vec![interval],
            auto_refresh: true,
            refresh_lag: Duration::from_secs(0),
        };
        mgr.register(config).unwrap();

        let base = 1_710_513_000_000_000_000i64;
        let rows = vec![
            SourceRow {
                timestamp: base,
                columns: HashMap::from([
                    ("symbol".into(), 1.0), // BTC
                    ("volume".into(), 10.0),
                ]),
            },
            SourceRow {
                timestamp: base + 10_000_000_000,
                columns: HashMap::from([
                    ("symbol".into(), 2.0), // ETH
                    ("volume".into(), 20.0),
                ]),
            },
            SourceRow {
                timestamp: base + 20_000_000_000,
                columns: HashMap::from([
                    ("symbol".into(), 1.0), // BTC
                    ("volume".into(), 5.0),
                ]),
            },
        ];

        let results = mgr.compute_aggregates("trades_by_sym_1m", &rows).unwrap();

        // Same minute, but 2 symbols -> 2 buckets.
        assert_eq!(results.len(), 2);

        // Find BTC (symbol=1) and ETH (symbol=2).
        let btc = results.iter().find(|(_, pk, _)| pk[0] == "1").unwrap();
        let eth = results.iter().find(|(_, pk, _)| pk[0] == "2").unwrap();

        assert!((btc.2[0] - 15.0).abs() < 1e-9); // BTC total volume = 10 + 5
        assert_eq!(btc.2[1], 2.0); // BTC trade count = 2

        assert!((eth.2[0] - 20.0).abs() < 1e-9); // ETH total volume = 20
        assert_eq!(eth.2[1], 1.0); // ETH trade count = 1
    }

    #[test]
    fn test_refresh_specific_interval() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = DownsamplingManager::new(dir.path().to_path_buf());

        let config = DownsamplingConfig {
            source_table: "trades".into(),
            intervals: vec![make_ohlcv_interval()],
            auto_refresh: true,
            refresh_lag: Duration::from_secs(60),
        };
        mgr.register(config).unwrap();

        let trades = make_trades();
        let stats = mgr.refresh_with_data("trades_1m", &trades).unwrap();
        assert_eq!(stats.rows_processed, 5);
        assert_eq!(stats.rows_written, 3);
    }

    #[test]
    fn test_refresh_nonexistent_interval_errors() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = DownsamplingManager::new(dir.path().to_path_buf());

        let trades = make_trades();
        let result = mgr.refresh_with_data("nonexistent", &trades);
        assert!(result.is_err());
    }

    #[test]
    fn test_avg_column() {
        let dir = tempfile::tempdir().unwrap();
        let mut mgr = DownsamplingManager::new(dir.path().to_path_buf());

        let interval = DownsampleInterval {
            name: "trades_avg_1m".to_string(),
            interval: Duration::from_secs(60),
            columns: vec![DownsampleColumn::Avg {
                source: "price".into(),
                alias: "avg_price".into(),
            }],
            partition_by: vec![],
        };

        let config = DownsamplingConfig {
            source_table: "trades".into(),
            intervals: vec![interval],
            auto_refresh: true,
            refresh_lag: Duration::from_secs(0),
        };
        mgr.register(config).unwrap();

        let base = 1_710_513_000_000_000_000i64;
        let rows = vec![
            SourceRow {
                timestamp: base,
                columns: HashMap::from([("price".into(), 100.0)]),
            },
            SourceRow {
                timestamp: base + 10_000_000_000,
                columns: HashMap::from([("price".into(), 200.0)]),
            },
        ];

        let results = mgr.compute_aggregates("trades_avg_1m", &rows).unwrap();
        assert_eq!(results.len(), 1);
        assert!((results[0].2[0] - 150.0).abs() < 1e-9); // avg(100, 200) = 150
    }
}
