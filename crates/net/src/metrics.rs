use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::extract::State;
use axum::response::IntoResponse;

/// Lock-free Prometheus-style metrics for ExchangeDB.
///
/// All counters use `AtomicU64` for thread-safe, lock-free updates.
/// Histogram buckets for query duration are pre-defined.
pub struct Metrics {
    // --- Storage metrics ---
    /// Total rows written.
    pub rows_written_total: AtomicU64,
    /// Total rows read during queries.
    pub rows_read_total: AtomicU64,
    /// Total bytes written to storage.
    pub bytes_written_total: AtomicU64,
    /// Total bytes read from storage.
    pub bytes_read_total: AtomicU64,
    /// Total WAL segments created.
    pub wal_segments_total: AtomicU64,
    /// Total bytes written to WAL.
    pub wal_bytes_total: AtomicU64,
    /// Number of partitions across all tables.
    pub partitions_total: AtomicU64,
    /// Disk space used in bytes.
    pub disk_used_bytes: AtomicU64,

    // --- Query metrics ---
    /// Total number of queries executed.
    pub queries_total: AtomicU64,
    /// Total number of failed queries.
    pub queries_failed_total: AtomicU64,
    /// Histogram bucket counts for query duration (seconds).
    /// Buckets: 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, +Inf
    pub query_duration_buckets: [AtomicU64; 15],
    /// Sum of all query durations in nanoseconds (stored as u64, converted on render).
    pub query_duration_sum_ns: AtomicU64,
    /// Total count of observations in the histogram.
    pub query_duration_count: AtomicU64,
    /// Total number of slow queries.
    pub slow_queries_total: AtomicU64,
    /// Plan cache hits.
    pub plan_cache_hits: AtomicU64,
    /// Plan cache misses.
    pub plan_cache_misses: AtomicU64,
    /// Number of currently active queries.
    pub active_queries: AtomicU32,

    // --- Connection metrics ---
    /// Total connections accepted over server lifetime.
    pub connections_total: AtomicU64,
    /// Number of currently active connections (all protocols).
    pub connections_active: AtomicU32,
    /// Number of active PostgreSQL wire connections.
    pub connections_pg: AtomicU32,
    /// Number of active HTTP connections.
    pub connections_http: AtomicU32,
    /// Number of active ILP connections.
    pub connections_ilp: AtomicU32,

    // --- Replication metrics ---
    /// Replication lag in bytes (replica only).
    pub replication_lag_bytes: AtomicU64,
    /// Replication lag in seconds (replica only).
    pub replication_lag_seconds: AtomicU64,
    /// Total WAL segments shipped to replicas (primary only).
    pub wal_segments_shipped: AtomicU64,
    /// Total WAL segments applied from primary (replica only).
    pub wal_segments_applied: AtomicU64,

    // --- Resource metrics ---
    /// Current memory usage in bytes.
    pub memory_used_bytes: AtomicU64,
    /// Configured memory limit in bytes.
    pub memory_limit_bytes: AtomicU64,
    /// Number of open file descriptors.
    pub open_files: AtomicU32,

    // --- System info (gauges) ---
    /// Uptime in seconds (computed from start_time).
    pub uptime_seconds: AtomicU64,
    /// CPU usage as a percentage (0-10000 = 0.00%-100.00%).
    pub cpu_usage_percent: AtomicU64,

    // --- Legacy / ILP ---
    /// Total ILP lines received.
    pub ilp_lines_received_total: AtomicU64,
    /// Number of tables (set by periodic scan or on write).
    pub tables_count: AtomicU64,
    /// Number of currently active HTTP connections (legacy alias).
    pub active_connections: AtomicU64,

    /// Server start time, used to compute uptime.
    start_time: Instant,
}

/// Pre-defined histogram bucket boundaries in seconds (15 buckets: 14 finite + Inf).
const BUCKET_BOUNDS: [f64; 14] = [
    0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

impl Metrics {
    /// Create a new `Metrics` instance with all counters at zero.
    pub fn new() -> Self {
        Self {
            // Storage
            rows_written_total: AtomicU64::new(0),
            rows_read_total: AtomicU64::new(0),
            bytes_written_total: AtomicU64::new(0),
            bytes_read_total: AtomicU64::new(0),
            wal_segments_total: AtomicU64::new(0),
            wal_bytes_total: AtomicU64::new(0),
            partitions_total: AtomicU64::new(0),
            disk_used_bytes: AtomicU64::new(0),

            // Query
            queries_total: AtomicU64::new(0),
            queries_failed_total: AtomicU64::new(0),
            query_duration_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            query_duration_sum_ns: AtomicU64::new(0),
            query_duration_count: AtomicU64::new(0),
            slow_queries_total: AtomicU64::new(0),
            plan_cache_hits: AtomicU64::new(0),
            plan_cache_misses: AtomicU64::new(0),
            active_queries: AtomicU32::new(0),

            // Connection
            connections_total: AtomicU64::new(0),
            connections_active: AtomicU32::new(0),
            connections_pg: AtomicU32::new(0),
            connections_http: AtomicU32::new(0),
            connections_ilp: AtomicU32::new(0),

            // Replication
            replication_lag_bytes: AtomicU64::new(0),
            replication_lag_seconds: AtomicU64::new(0),
            wal_segments_shipped: AtomicU64::new(0),
            wal_segments_applied: AtomicU64::new(0),

            // Resource
            memory_used_bytes: AtomicU64::new(0),
            memory_limit_bytes: AtomicU64::new(0),
            open_files: AtomicU32::new(0),

            // System
            uptime_seconds: AtomicU64::new(0),
            cpu_usage_percent: AtomicU64::new(0),

            // Legacy / ILP
            ilp_lines_received_total: AtomicU64::new(0),
            tables_count: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),

            start_time: Instant::now(),
        }
    }

    /// Increment the query counter by one.
    pub fn inc_queries(&self) {
        self.queries_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the failed query counter by one.
    pub fn inc_queries_failed(&self) {
        self.queries_failed_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment slow query counter.
    pub fn inc_slow_queries(&self) {
        self.slow_queries_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a query duration observation into the histogram.
    pub fn observe_query_duration(&self, duration_secs: f64) {
        // Increment bucket counters for all buckets where the value fits.
        for (i, bound) in BUCKET_BOUNDS.iter().enumerate() {
            if duration_secs <= *bound {
                self.query_duration_buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        // +Inf bucket always incremented.
        self.query_duration_buckets[14].fetch_add(1, Ordering::Relaxed);

        // Add to sum (store as nanoseconds for precision).
        let nanos = (duration_secs * 1_000_000_000.0) as u64;
        self.query_duration_sum_ns.fetch_add(nanos, Ordering::Relaxed);
        self.query_duration_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Add to the rows written counter.
    pub fn add_rows_written(&self, count: u64) {
        self.rows_written_total.fetch_add(count, Ordering::Relaxed);
    }

    /// Add to the rows read counter.
    pub fn add_rows_read(&self, count: u64) {
        self.rows_read_total.fetch_add(count, Ordering::Relaxed);
    }

    /// Add to bytes written counter.
    pub fn add_bytes_written(&self, count: u64) {
        self.bytes_written_total.fetch_add(count, Ordering::Relaxed);
    }

    /// Add to bytes read counter.
    pub fn add_bytes_read(&self, count: u64) {
        self.bytes_read_total.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment active connections.
    pub fn inc_connections(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.connections_active.fetch_add(1, Ordering::Relaxed);
        self.connections_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active connections.
    pub fn dec_connections(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Increment active HTTP connections.
    pub fn inc_http_connections(&self) {
        self.connections_http.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active HTTP connections.
    pub fn dec_http_connections(&self) {
        self.connections_http.fetch_sub(1, Ordering::Relaxed);
    }

    /// Increment active PG connections.
    pub fn inc_pg_connections(&self) {
        self.connections_pg.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active PG connections.
    pub fn dec_pg_connections(&self) {
        self.connections_pg.fetch_sub(1, Ordering::Relaxed);
    }

    /// Increment active ILP connections.
    pub fn inc_ilp_connections(&self) {
        self.connections_ilp.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active ILP connections.
    pub fn dec_ilp_connections(&self) {
        self.connections_ilp.fetch_sub(1, Ordering::Relaxed);
    }

    /// Increment active queries.
    pub fn inc_active_queries(&self) {
        self.active_queries.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active queries.
    pub fn dec_active_queries(&self) {
        self.active_queries.fetch_sub(1, Ordering::Relaxed);
    }

    /// Add to ILP lines received counter.
    pub fn add_ilp_lines(&self, count: u64) {
        self.ilp_lines_received_total
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Increment the plan cache hit counter.
    pub fn inc_plan_cache_hits(&self) {
        self.plan_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the plan cache miss counter.
    pub fn inc_plan_cache_misses(&self) {
        self.plan_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Set the tables count gauge.
    pub fn set_tables_count(&self, count: u64) {
        self.tables_count.store(count, Ordering::Relaxed);
    }

    /// Set the disk used bytes gauge.
    pub fn set_disk_used_bytes(&self, bytes: u64) {
        self.disk_used_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Set the partitions count gauge.
    pub fn set_partitions_total(&self, count: u64) {
        self.partitions_total.store(count, Ordering::Relaxed);
    }

    /// Set memory used bytes gauge.
    pub fn set_memory_used_bytes(&self, bytes: u64) {
        self.memory_used_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Set memory limit bytes gauge.
    pub fn set_memory_limit_bytes(&self, bytes: u64) {
        self.memory_limit_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Set open files gauge.
    pub fn set_open_files(&self, count: u32) {
        self.open_files.store(count, Ordering::Relaxed);
    }

    /// Set replication lag bytes.
    pub fn set_replication_lag_bytes(&self, bytes: u64) {
        self.replication_lag_bytes.store(bytes, Ordering::Relaxed);
    }

    /// Set replication lag seconds.
    pub fn set_replication_lag_seconds(&self, secs: u64) {
        self.replication_lag_seconds.store(secs, Ordering::Relaxed);
    }

    /// Set CPU usage (stored as percent * 100 for precision).
    pub fn set_cpu_usage_percent(&self, hundredths: u64) {
        self.cpu_usage_percent.store(hundredths, Ordering::Relaxed);
    }

    /// Add WAL segments total.
    pub fn add_wal_segments(&self, count: u64) {
        self.wal_segments_total.fetch_add(count, Ordering::Relaxed);
    }

    /// Add WAL bytes total.
    pub fn add_wal_bytes(&self, bytes: u64) {
        self.wal_bytes_total.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Add WAL segments shipped.
    pub fn add_wal_segments_shipped(&self, count: u64) {
        self.wal_segments_shipped.fetch_add(count, Ordering::Relaxed);
    }

    /// Add WAL segments applied.
    pub fn add_wal_segments_applied(&self, count: u64) {
        self.wal_segments_applied.fetch_add(count, Ordering::Relaxed);
    }

    /// Render all metrics in Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(8192);

        let uptime = self.start_time.elapsed().as_secs_f64();

        // =====================================================================
        // Storage metrics
        // =====================================================================

        out.push_str("# HELP exchangedb_rows_written_total Total rows written.\n");
        out.push_str("# TYPE exchangedb_rows_written_total counter\n");
        push_metric(&mut out, "exchangedb_rows_written_total",
            self.rows_written_total.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_rows_read_total Total rows read during queries.\n");
        out.push_str("# TYPE exchangedb_rows_read_total counter\n");
        push_metric(&mut out, "exchangedb_rows_read_total",
            self.rows_read_total.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_bytes_written_total Total bytes written to storage.\n");
        out.push_str("# TYPE exchangedb_bytes_written_total counter\n");
        push_metric(&mut out, "exchangedb_bytes_written_total",
            self.bytes_written_total.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_bytes_read_total Total bytes read from storage.\n");
        out.push_str("# TYPE exchangedb_bytes_read_total counter\n");
        push_metric(&mut out, "exchangedb_bytes_read_total",
            self.bytes_read_total.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_wal_segments_total Total WAL segments created.\n");
        out.push_str("# TYPE exchangedb_wal_segments_total counter\n");
        push_metric(&mut out, "exchangedb_wal_segments_total",
            self.wal_segments_total.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_wal_bytes_total Total bytes written to WAL.\n");
        out.push_str("# TYPE exchangedb_wal_bytes_total counter\n");
        push_metric(&mut out, "exchangedb_wal_bytes_total",
            self.wal_bytes_total.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_partitions_total Total number of partitions.\n");
        out.push_str("# TYPE exchangedb_partitions_total gauge\n");
        push_metric(&mut out, "exchangedb_partitions_total",
            self.partitions_total.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_disk_used_bytes Disk space used in bytes.\n");
        out.push_str("# TYPE exchangedb_disk_used_bytes gauge\n");
        push_metric(&mut out, "exchangedb_disk_used_bytes",
            self.disk_used_bytes.load(Ordering::Relaxed) as f64);

        // =====================================================================
        // Query metrics
        // =====================================================================

        out.push_str("# HELP exchangedb_queries_total Total number of queries executed.\n");
        out.push_str("# TYPE exchangedb_queries_total counter\n");
        push_metric(&mut out, "exchangedb_queries_total",
            self.queries_total.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_queries_failed_total Total number of failed queries.\n");
        out.push_str("# TYPE exchangedb_queries_failed_total counter\n");
        push_metric(&mut out, "exchangedb_queries_failed_total",
            self.queries_failed_total.load(Ordering::Relaxed) as f64);

        // --- Histogram: query_duration_seconds ---
        out.push_str(
            "# HELP exchangedb_query_duration_seconds Query execution time in seconds.\n",
        );
        out.push_str("# TYPE exchangedb_query_duration_seconds histogram\n");

        let mut cumulative: u64 = 0;
        for (i, bound) in BUCKET_BOUNDS.iter().enumerate() {
            cumulative += self.query_duration_buckets[i].load(Ordering::Relaxed);
            out.push_str(&format!(
                "exchangedb_query_duration_seconds_bucket{{le=\"{bound}\"}} {cumulative}\n"
            ));
        }
        let total_count = self.query_duration_count.load(Ordering::Relaxed);
        out.push_str(&format!(
            "exchangedb_query_duration_seconds_bucket{{le=\"+Inf\"}} {total_count}\n"
        ));

        let sum_ns = self.query_duration_sum_ns.load(Ordering::Relaxed);
        let sum_secs = sum_ns as f64 / 1_000_000_000.0;
        out.push_str(&format!(
            "exchangedb_query_duration_seconds_sum {sum_secs}\n"
        ));
        out.push_str(&format!(
            "exchangedb_query_duration_seconds_count {total_count}\n"
        ));

        out.push_str("# HELP exchangedb_slow_queries_total Total slow queries logged.\n");
        out.push_str("# TYPE exchangedb_slow_queries_total counter\n");
        push_metric(&mut out, "exchangedb_slow_queries_total",
            self.slow_queries_total.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_plan_cache_hits_total Plan cache hit count.\n");
        out.push_str("# TYPE exchangedb_plan_cache_hits_total counter\n");
        push_metric(&mut out, "exchangedb_plan_cache_hits_total",
            self.plan_cache_hits.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_plan_cache_misses_total Plan cache miss count.\n");
        out.push_str("# TYPE exchangedb_plan_cache_misses_total counter\n");
        push_metric(&mut out, "exchangedb_plan_cache_misses_total",
            self.plan_cache_misses.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_active_queries Number of currently active queries.\n");
        out.push_str("# TYPE exchangedb_active_queries gauge\n");
        push_metric(&mut out, "exchangedb_active_queries",
            self.active_queries.load(Ordering::Relaxed) as f64);

        // =====================================================================
        // Connection metrics
        // =====================================================================

        out.push_str("# HELP exchangedb_connections_total Total connections accepted.\n");
        out.push_str("# TYPE exchangedb_connections_total counter\n");
        push_metric(&mut out, "exchangedb_connections_total",
            self.connections_total.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_connections_active Currently active connections.\n");
        out.push_str("# TYPE exchangedb_connections_active gauge\n");
        push_metric(&mut out, "exchangedb_connections_active",
            self.connections_active.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_connections_pg Active PostgreSQL wire connections.\n");
        out.push_str("# TYPE exchangedb_connections_pg gauge\n");
        push_metric(&mut out, "exchangedb_connections_pg",
            self.connections_pg.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_connections_http Active HTTP connections.\n");
        out.push_str("# TYPE exchangedb_connections_http gauge\n");
        push_metric(&mut out, "exchangedb_connections_http",
            self.connections_http.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_connections_ilp Active ILP connections.\n");
        out.push_str("# TYPE exchangedb_connections_ilp gauge\n");
        push_metric(&mut out, "exchangedb_connections_ilp",
            self.connections_ilp.load(Ordering::Relaxed) as f64);

        // =====================================================================
        // Replication metrics
        // =====================================================================

        out.push_str("# HELP exchangedb_replication_lag_bytes Replication lag in bytes.\n");
        out.push_str("# TYPE exchangedb_replication_lag_bytes gauge\n");
        push_metric(&mut out, "exchangedb_replication_lag_bytes",
            self.replication_lag_bytes.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_replication_lag_seconds Replication lag in seconds.\n");
        out.push_str("# TYPE exchangedb_replication_lag_seconds gauge\n");
        push_metric(&mut out, "exchangedb_replication_lag_seconds",
            self.replication_lag_seconds.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_wal_segments_shipped_total WAL segments shipped to replicas.\n");
        out.push_str("# TYPE exchangedb_wal_segments_shipped_total counter\n");
        push_metric(&mut out, "exchangedb_wal_segments_shipped_total",
            self.wal_segments_shipped.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_wal_segments_applied_total WAL segments applied from primary.\n");
        out.push_str("# TYPE exchangedb_wal_segments_applied_total counter\n");
        push_metric(&mut out, "exchangedb_wal_segments_applied_total",
            self.wal_segments_applied.load(Ordering::Relaxed) as f64);

        // =====================================================================
        // Resource metrics
        // =====================================================================

        out.push_str("# HELP exchangedb_memory_used_bytes Current memory usage in bytes.\n");
        out.push_str("# TYPE exchangedb_memory_used_bytes gauge\n");
        push_metric(&mut out, "exchangedb_memory_used_bytes",
            self.memory_used_bytes.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_memory_limit_bytes Configured memory limit in bytes.\n");
        out.push_str("# TYPE exchangedb_memory_limit_bytes gauge\n");
        push_metric(&mut out, "exchangedb_memory_limit_bytes",
            self.memory_limit_bytes.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_open_files Number of open file descriptors.\n");
        out.push_str("# TYPE exchangedb_open_files gauge\n");
        push_metric(&mut out, "exchangedb_open_files",
            self.open_files.load(Ordering::Relaxed) as f64);

        // =====================================================================
        // System info gauges
        // =====================================================================

        out.push_str("# HELP exchangedb_uptime_seconds Server uptime in seconds.\n");
        out.push_str("# TYPE exchangedb_uptime_seconds gauge\n");
        push_metric(&mut out, "exchangedb_uptime_seconds", uptime);

        out.push_str("# HELP exchangedb_cpu_usage_percent CPU usage percentage (0-100).\n");
        out.push_str("# TYPE exchangedb_cpu_usage_percent gauge\n");
        let cpu_hundredths = self.cpu_usage_percent.load(Ordering::Relaxed);
        push_metric(&mut out, "exchangedb_cpu_usage_percent", cpu_hundredths as f64 / 100.0);

        // =====================================================================
        // Legacy metrics (kept for backward compatibility)
        // =====================================================================

        out.push_str("# HELP exchangedb_tables_count Number of tables.\n");
        out.push_str("# TYPE exchangedb_tables_count gauge\n");
        push_metric(&mut out, "exchangedb_tables_count",
            self.tables_count.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_active_connections Active HTTP connections (legacy).\n");
        out.push_str("# TYPE exchangedb_active_connections gauge\n");
        push_metric(&mut out, "exchangedb_active_connections",
            self.active_connections.load(Ordering::Relaxed) as f64);

        out.push_str("# HELP exchangedb_ilp_lines_received_total Total ILP lines ingested.\n");
        out.push_str("# TYPE exchangedb_ilp_lines_received_total counter\n");
        push_metric(&mut out, "exchangedb_ilp_lines_received_total",
            self.ilp_lines_received_total.load(Ordering::Relaxed) as f64);

        out
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field("queries_total", &self.queries_total.load(Ordering::Relaxed))
            .field(
                "rows_written_total",
                &self.rows_written_total.load(Ordering::Relaxed),
            )
            .finish_non_exhaustive()
    }
}

/// Push a single metric line `name value\n` to the output buffer.
fn push_metric(out: &mut String, name: &str, value: f64) {
    // Format integers without decimal point for cleaner output.
    if value.fract() == 0.0 && value.is_finite() && value.abs() < (u64::MAX as f64) {
        out.push_str(&format!("{name} {}\n", value as u64));
    } else {
        out.push_str(&format!("{name} {value}\n"));
    }
}

/// `GET /metrics`
///
/// Returns Prometheus text exposition format metrics.
pub async fn metrics_handler(
    State(state): State<Arc<crate::http::handlers::AppState>>,
) -> impl IntoResponse {
    let body = state.metrics.render();
    (
        axum::http::StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_render_contains_all_metric_names() {
        let m = Metrics::new();
        let output = m.render();

        // Storage metrics
        assert!(output.contains("exchangedb_rows_written_total"));
        assert!(output.contains("exchangedb_rows_read_total"));
        assert!(output.contains("exchangedb_bytes_written_total"));
        assert!(output.contains("exchangedb_bytes_read_total"));
        assert!(output.contains("exchangedb_wal_segments_total"));
        assert!(output.contains("exchangedb_wal_bytes_total"));
        assert!(output.contains("exchangedb_partitions_total"));
        assert!(output.contains("exchangedb_disk_used_bytes"));

        // Query metrics
        assert!(output.contains("exchangedb_queries_total"));
        assert!(output.contains("exchangedb_queries_failed_total"));
        assert!(output.contains("exchangedb_query_duration_seconds"));
        assert!(output.contains("exchangedb_slow_queries_total"));
        assert!(output.contains("exchangedb_plan_cache_hits_total"));
        assert!(output.contains("exchangedb_plan_cache_misses_total"));
        assert!(output.contains("exchangedb_active_queries"));

        // Connection metrics
        assert!(output.contains("exchangedb_connections_total"));
        assert!(output.contains("exchangedb_connections_active"));
        assert!(output.contains("exchangedb_connections_pg"));
        assert!(output.contains("exchangedb_connections_http"));
        assert!(output.contains("exchangedb_connections_ilp"));

        // Replication metrics
        assert!(output.contains("exchangedb_replication_lag_bytes"));
        assert!(output.contains("exchangedb_replication_lag_seconds"));
        assert!(output.contains("exchangedb_wal_segments_shipped_total"));
        assert!(output.contains("exchangedb_wal_segments_applied_total"));

        // Resource metrics
        assert!(output.contains("exchangedb_memory_used_bytes"));
        assert!(output.contains("exchangedb_memory_limit_bytes"));
        assert!(output.contains("exchangedb_open_files"));

        // System info
        assert!(output.contains("exchangedb_uptime_seconds"));
        assert!(output.contains("exchangedb_cpu_usage_percent"));

        // Legacy
        assert!(output.contains("exchangedb_tables_count"));
        assert!(output.contains("exchangedb_active_connections"));
        assert!(output.contains("exchangedb_ilp_lines_received_total"));
    }

    #[test]
    fn test_metrics_counters() {
        let m = Metrics::new();

        m.inc_queries();
        m.inc_queries();
        m.add_rows_written(100);
        m.add_rows_read(50);
        m.add_ilp_lines(10);
        m.set_tables_count(3);
        m.inc_connections();
        m.add_bytes_written(4096);
        m.add_bytes_read(2048);
        m.inc_queries_failed();
        m.inc_slow_queries();
        m.set_disk_used_bytes(1_000_000);
        m.set_memory_used_bytes(500_000);
        m.set_memory_limit_bytes(8_000_000);
        m.set_open_files(42);
        m.set_partitions_total(12);
        m.add_wal_segments(3);
        m.add_wal_bytes(8192);
        m.set_replication_lag_bytes(1024);
        m.set_replication_lag_seconds(2);
        m.add_wal_segments_shipped(5);
        m.add_wal_segments_applied(4);
        m.set_cpu_usage_percent(5050);

        let output = m.render();

        assert!(output.contains("exchangedb_queries_total 2\n"));
        assert!(output.contains("exchangedb_rows_written_total 100\n"));
        assert!(output.contains("exchangedb_rows_read_total 50\n"));
        assert!(output.contains("exchangedb_ilp_lines_received_total 10\n"));
        assert!(output.contains("exchangedb_tables_count 3\n"));
        assert!(output.contains("exchangedb_active_connections 1\n"));
        assert!(output.contains("exchangedb_bytes_written_total 4096\n"));
        assert!(output.contains("exchangedb_bytes_read_total 2048\n"));
        assert!(output.contains("exchangedb_queries_failed_total 1\n"));
        assert!(output.contains("exchangedb_slow_queries_total 1\n"));
        assert!(output.contains("exchangedb_disk_used_bytes 1000000\n"));
        assert!(output.contains("exchangedb_memory_used_bytes 500000\n"));
        assert!(output.contains("exchangedb_memory_limit_bytes 8000000\n"));
        assert!(output.contains("exchangedb_open_files 42\n"));
        assert!(output.contains("exchangedb_partitions_total 12\n"));
        assert!(output.contains("exchangedb_wal_segments_total 3\n"));
        assert!(output.contains("exchangedb_wal_bytes_total 8192\n"));
        assert!(output.contains("exchangedb_replication_lag_bytes 1024\n"));
        assert!(output.contains("exchangedb_replication_lag_seconds 2\n"));
        assert!(output.contains("exchangedb_wal_segments_shipped_total 5\n"));
        assert!(output.contains("exchangedb_wal_segments_applied_total 4\n"));
        assert!(output.contains("exchangedb_cpu_usage_percent 50.5\n"));
    }

    #[test]
    fn test_histogram_observation() {
        let m = Metrics::new();

        // Observe a 0.003s query (should land in 0.005 bucket and above).
        m.observe_query_duration(0.003);
        // Observe a 0.5s query.
        m.observe_query_duration(0.5);

        let output = m.render();

        assert!(output.contains("exchangedb_query_duration_seconds_count 2\n"));
        assert!(output.contains("exchangedb_query_duration_seconds_sum"));
        // The +Inf bucket must equal count.
        assert!(output.contains(
            "exchangedb_query_duration_seconds_bucket{le=\"+Inf\"} 2\n"
        ));
        // The 0.0005 bucket should have 0 (0.003 > 0.0005).
        assert!(output.contains(
            "exchangedb_query_duration_seconds_bucket{le=\"0.0005\"} 0\n"
        ));
        // The 0.005 bucket should have 1 (0.003 <= 0.005).
        assert!(output.contains(
            "exchangedb_query_duration_seconds_bucket{le=\"0.005\"} 1\n"
        ));
    }

    #[test]
    fn test_type_annotations() {
        let m = Metrics::new();
        let output = m.render();

        assert!(output.contains("# TYPE exchangedb_queries_total counter\n"));
        assert!(output.contains("# TYPE exchangedb_query_duration_seconds histogram\n"));
        assert!(output.contains("# TYPE exchangedb_tables_count gauge\n"));
        assert!(output.contains("# TYPE exchangedb_active_connections gauge\n"));
        assert!(output.contains("# TYPE exchangedb_uptime_seconds gauge\n"));
        assert!(output.contains("# TYPE exchangedb_connections_pg gauge\n"));
        assert!(output.contains("# TYPE exchangedb_connections_http gauge\n"));
        assert!(output.contains("# TYPE exchangedb_connections_ilp gauge\n"));
        assert!(output.contains("# TYPE exchangedb_replication_lag_bytes gauge\n"));
        assert!(output.contains("# TYPE exchangedb_memory_used_bytes gauge\n"));
        assert!(output.contains("# TYPE exchangedb_open_files gauge\n"));
    }

    #[test]
    fn test_connections_inc_dec() {
        let m = Metrics::new();
        m.inc_connections();
        m.inc_connections();
        m.dec_connections();

        assert_eq!(m.active_connections.load(Ordering::Relaxed), 1);
        assert_eq!(m.connections_active.load(Ordering::Relaxed), 1);
        assert_eq!(m.connections_total.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_active_queries_inc_dec() {
        let m = Metrics::new();
        m.inc_active_queries();
        m.inc_active_queries();
        m.dec_active_queries();

        assert_eq!(m.active_queries.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_protocol_specific_connections() {
        let m = Metrics::new();
        m.inc_http_connections();
        m.inc_pg_connections();
        m.inc_pg_connections();
        m.inc_ilp_connections();
        m.dec_pg_connections();

        assert_eq!(m.connections_http.load(Ordering::Relaxed), 1);
        assert_eq!(m.connections_pg.load(Ordering::Relaxed), 1);
        assert_eq!(m.connections_ilp.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_metric_count_exceeds_20() {
        // Verify we have 20+ unique metric names in the render output.
        let m = Metrics::new();
        let output = m.render();
        let metric_count = output
            .lines()
            .filter(|line| line.starts_with("# HELP exchangedb_"))
            .count();
        assert!(
            metric_count >= 20,
            "expected >= 20 metrics, got {metric_count}"
        );
    }
}
