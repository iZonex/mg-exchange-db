//! Usage metering for ExchangeDB multi-tenant deployments.
//!
//! Tracks per-tenant query counts, rows read/written, and bytes
//! scanned/stored. Counters can be persisted to disk and loaded
//! on restart.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use exchange_common::error::{ExchangeDbError, Result};

/// Atomic usage counters for a single tenant.
#[derive(Debug)]
pub struct UsageCounters {
    pub queries: AtomicU64,
    pub rows_read: AtomicU64,
    pub rows_written: AtomicU64,
    pub bytes_scanned: AtomicU64,
    pub bytes_stored: AtomicU64,
}

impl UsageCounters {
    /// Create zeroed counters.
    pub fn new() -> Self {
        Self {
            queries: AtomicU64::new(0),
            rows_read: AtomicU64::new(0),
            rows_written: AtomicU64::new(0),
            bytes_scanned: AtomicU64::new(0),
            bytes_stored: AtomicU64::new(0),
        }
    }

    /// Create counters from explicit values (used when loading from disk).
    fn from_snapshot(snap: &CounterSnapshot) -> Self {
        Self {
            queries: AtomicU64::new(snap.queries),
            rows_read: AtomicU64::new(snap.rows_read),
            rows_written: AtomicU64::new(snap.rows_written),
            bytes_scanned: AtomicU64::new(snap.bytes_scanned),
            bytes_stored: AtomicU64::new(snap.bytes_stored),
        }
    }

    /// Take a snapshot of the current counter values.
    pub fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            queries: self.queries.load(Ordering::Relaxed),
            rows_read: self.rows_read.load(Ordering::Relaxed),
            rows_written: self.rows_written.load(Ordering::Relaxed),
            bytes_scanned: self.bytes_scanned.load(Ordering::Relaxed),
            bytes_stored: self.bytes_stored.load(Ordering::Relaxed),
        }
    }
}

impl Default for UsageCounters {
    fn default() -> Self {
        Self::new()
    }
}

/// Non-atomic snapshot of usage counters, suitable for serialization.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CounterSnapshot {
    pub queries: u64,
    pub rows_read: u64,
    pub rows_written: u64,
    pub bytes_scanned: u64,
    pub bytes_stored: u64,
}

/// Tracks per-tenant usage metrics with lock-free atomic counters.
pub struct UsageMeter {
    db_root: PathBuf,
    counters: DashMap<String, UsageCounters>,
}

impl UsageMeter {
    /// Create a new usage meter that persists data under `db_root/_metering/`.
    pub fn new(db_root: PathBuf) -> Self {
        Self {
            db_root,
            counters: DashMap::new(),
        }
    }

    /// Record a query execution for a tenant.
    pub fn record_query(&self, tenant: &str, rows_read: u64, bytes_scanned: u64) {
        let entry = self
            .counters
            .entry(tenant.to_string())
            .or_insert_with(UsageCounters::new);
        entry.queries.fetch_add(1, Ordering::Relaxed);
        entry.rows_read.fetch_add(rows_read, Ordering::Relaxed);
        entry
            .bytes_scanned
            .fetch_add(bytes_scanned, Ordering::Relaxed);
    }

    /// Record a write operation for a tenant.
    pub fn record_write(&self, tenant: &str, rows: u64) {
        let entry = self
            .counters
            .entry(tenant.to_string())
            .or_insert_with(UsageCounters::new);
        entry.rows_written.fetch_add(rows, Ordering::Relaxed);
    }

    /// Get a snapshot of usage for a specific tenant.
    pub fn get_usage(&self, tenant: &str) -> CounterSnapshot {
        match self.counters.get(tenant) {
            Some(c) => c.snapshot(),
            None => CounterSnapshot {
                queries: 0,
                rows_read: 0,
                rows_written: 0,
                bytes_scanned: 0,
                bytes_stored: 0,
            },
        }
    }

    /// Get snapshots of all tenant usage.
    pub fn get_all_usage(&self) -> HashMap<String, CounterSnapshot> {
        self.counters
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().snapshot()))
            .collect()
    }

    /// Persist all usage counters to disk as JSON.
    pub fn persist(&self) -> Result<()> {
        let metering_dir = self.db_root.join("_metering");
        fs::create_dir_all(&metering_dir)?;

        let all = self.get_all_usage();
        let json = serde_json::to_string_pretty(&all)
            .map_err(|e| ExchangeDbError::Corruption(format!("serialize metering: {e}")))?;
        fs::write(metering_dir.join("usage.json"), json)?;
        Ok(())
    }

    /// Load usage counters from disk.
    pub fn load(&mut self) -> Result<()> {
        let path = self.db_root.join("_metering").join("usage.json");
        if !path.exists() {
            return Ok(());
        }

        let data = fs::read_to_string(&path)?;
        let snapshots: HashMap<String, CounterSnapshot> = serde_json::from_str(&data)
            .map_err(|e| ExchangeDbError::Corruption(format!("deserialize metering: {e}")))?;

        self.counters.clear();
        for (tenant, snap) in snapshots {
            self.counters
                .insert(tenant, UsageCounters::from_snapshot(&snap));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn record_query_increments_counters() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());

        meter.record_query("t1", 100, 4096);
        meter.record_query("t1", 50, 2048);

        let usage = meter.get_usage("t1");
        assert_eq!(usage.queries, 2);
        assert_eq!(usage.rows_read, 150);
        assert_eq!(usage.bytes_scanned, 6144);
    }

    #[test]
    fn record_write_increments_counters() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());

        meter.record_write("t1", 500);
        meter.record_write("t1", 300);

        let usage = meter.get_usage("t1");
        assert_eq!(usage.rows_written, 800);
    }

    #[test]
    fn get_usage_unknown_tenant_returns_zeros() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());

        let usage = meter.get_usage("nonexistent");
        assert_eq!(usage.queries, 0);
        assert_eq!(usage.rows_read, 0);
    }

    #[test]
    fn get_all_usage() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());

        meter.record_query("t1", 10, 100);
        meter.record_query("t2", 20, 200);

        let all = meter.get_all_usage();
        assert_eq!(all.len(), 2);
        assert_eq!(all["t1"].queries, 1);
        assert_eq!(all["t2"].queries, 1);
    }

    #[test]
    fn persist_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();

        // Record some usage and persist.
        {
            let meter = UsageMeter::new(dir.path().to_path_buf());
            meter.record_query("t1", 100, 4096);
            meter.record_write("t1", 50);
            meter.record_query("t2", 200, 8192);
            meter.persist().unwrap();
        }

        // Load into a fresh meter.
        {
            let mut meter = UsageMeter::new(dir.path().to_path_buf());
            meter.load().unwrap();

            let t1 = meter.get_usage("t1");
            assert_eq!(t1.queries, 1);
            assert_eq!(t1.rows_read, 100);
            assert_eq!(t1.bytes_scanned, 4096);
            assert_eq!(t1.rows_written, 50);

            let t2 = meter.get_usage("t2");
            assert_eq!(t2.queries, 1);
            assert_eq!(t2.rows_read, 200);
        }
    }

    #[test]
    fn load_nonexistent_is_ok() {
        let dir = TempDir::new().unwrap();
        let mut meter = UsageMeter::new(dir.path().to_path_buf());
        meter.load().unwrap(); // Should not error.
    }

    #[test]
    fn multiple_tenants_isolated() {
        let dir = TempDir::new().unwrap();
        let meter = UsageMeter::new(dir.path().to_path_buf());

        meter.record_query("t1", 100, 1000);
        meter.record_query("t2", 200, 2000);

        let t1 = meter.get_usage("t1");
        let t2 = meter.get_usage("t2");

        assert_eq!(t1.rows_read, 100);
        assert_eq!(t2.rows_read, 200);
    }
}
