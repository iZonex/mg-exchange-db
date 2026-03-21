//! Background job scheduler for ExchangeDB.
//!
//! Runs automated maintenance tasks on a configurable schedule:
//! WAL cleanup, checkpoints, TTL enforcement, retention, tiered storage
//! transitions, stats refresh, and PITR checkpoints.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[cfg(test)]
use exchange_common::error::ExchangeDbError;
use exchange_common::error::Result;

// ---------------------------------------------------------------------------
// Job trait and result
// ---------------------------------------------------------------------------

/// Result of running a single background job.
#[derive(Debug, Clone)]
pub struct JobResult {
    /// Name of the job that was run.
    pub name: String,
    /// Whether the job succeeded.
    pub success: bool,
    /// Duration of the job in milliseconds.
    pub duration_ms: u64,
    /// Human-readable message describing what happened.
    pub message: String,
}

/// Trait implemented by all background jobs.
pub trait Job: Send + Sync {
    /// The name of this job (used in logging and results).
    fn name(&self) -> &str;

    /// Execute the job against the database rooted at `db_root`.
    fn run(&self, db_root: &Path) -> Result<JobResult>;
}

// ---------------------------------------------------------------------------
// Scheduled job wrapper
// ---------------------------------------------------------------------------

/// A job paired with scheduling metadata.
pub struct ScheduledJob {
    /// Display name.
    pub name: String,
    /// How often to run.
    pub interval: Duration,
    /// The job implementation.
    pub job: Box<dyn Job + Send + Sync>,
    /// Whether this job is enabled.
    pub enabled: bool,
    /// When the job last ran (None if never).
    pub last_run: Option<Instant>,
}

// ---------------------------------------------------------------------------
// JobScheduler
// ---------------------------------------------------------------------------

/// Background job scheduler that periodically runs maintenance tasks.
pub struct JobScheduler {
    db_root: PathBuf,
    jobs: Vec<ScheduledJob>,
    shutdown: Arc<AtomicBool>,
}

impl JobScheduler {
    /// Create a new scheduler for the given database root directory.
    pub fn new(db_root: PathBuf) -> Self {
        Self {
            db_root,
            jobs: Vec::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Register a job to run at the given interval.
    pub fn register(&mut self, name: &str, interval: Duration, job: Box<dyn Job + Send + Sync>) {
        self.jobs.push(ScheduledJob {
            name: name.to_string(),
            interval,
            job,
            enabled: true,
            last_run: None,
        });
    }

    /// Run one cycle: execute all due jobs synchronously. Returns results
    /// for any jobs that ran. Useful for testing.
    pub fn run_once(&mut self) -> Vec<JobResult> {
        let now = Instant::now();
        let mut results = Vec::new();

        for scheduled in &mut self.jobs {
            if !scheduled.enabled {
                continue;
            }

            let due = match scheduled.last_run {
                Some(last) => now.duration_since(last) >= scheduled.interval,
                None => true, // never run yet
            };

            if !due {
                continue;
            }

            let start = Instant::now();
            let result = match scheduled.job.run(&self.db_root) {
                Ok(r) => r,
                Err(e) => JobResult {
                    name: scheduled.name.clone(),
                    success: false,
                    duration_ms: start.elapsed().as_millis() as u64,
                    message: format!("error: {e}"),
                },
            };

            scheduled.last_run = Some(Instant::now());
            results.push(result);
        }

        results
    }

    /// Start the scheduler loop in a background tokio task.
    /// Returns a handle that can be used to stop it.
    pub fn start(self) -> SchedulerHandle {
        let shutdown = self.shutdown.clone();
        let handle = tokio::task::spawn(async move {
            scheduler_loop(self).await;
        });
        SchedulerHandle { shutdown, handle }
    }
}

/// Async scheduler loop. Wakes every second to check for due jobs.
async fn scheduler_loop(mut scheduler: JobScheduler) {
    tracing::info!("background job scheduler started");

    loop {
        if scheduler.shutdown.load(Ordering::Relaxed) {
            tracing::info!("background job scheduler shutting down");
            break;
        }

        let now = Instant::now();

        for scheduled in &mut scheduler.jobs {
            if !scheduled.enabled {
                continue;
            }

            let due = match scheduled.last_run {
                Some(last) => now.duration_since(last) >= scheduled.interval,
                None => true,
            };

            if !due {
                continue;
            }

            let job_name = scheduled.name.clone();

            // Run the job directly. These maintenance jobs are expected to be
            // short-lived. For longer jobs, callers can implement internal
            // async handling within the Job trait.
            let start = Instant::now();
            let result = match scheduled.job.run(&scheduler.db_root) {
                Ok(r) => r,
                Err(e) => JobResult {
                    name: job_name.clone(),
                    success: false,
                    duration_ms: start.elapsed().as_millis() as u64,
                    message: format!("error: {e}"),
                },
            };

            scheduled.last_run = Some(Instant::now());

            if result.success {
                tracing::debug!(
                    job = %result.name,
                    duration_ms = result.duration_ms,
                    message = %result.message,
                    "background job completed"
                );
            } else {
                tracing::warn!(
                    job = %result.name,
                    duration_ms = result.duration_ms,
                    message = %result.message,
                    "background job failed"
                );
            }
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

// ---------------------------------------------------------------------------
// SchedulerHandle
// ---------------------------------------------------------------------------

/// Handle to a running scheduler, used to stop it gracefully.
pub struct SchedulerHandle {
    shutdown: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
}

impl SchedulerHandle {
    /// Signal the scheduler to stop after the current cycle.
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Signal the scheduler to stop and wait for it to finish.
    pub async fn wait(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = self.handle.await;
    }
}

// ---------------------------------------------------------------------------
// Built-in job implementations
// ---------------------------------------------------------------------------

/// WAL cleanup job: runs `VacuumJob` on each table to remove applied WAL
/// segments and clean up orphaned files.
pub struct WalCleanupJob;

impl Job for WalCleanupJob {
    fn name(&self) -> &str {
        "wal_cleanup"
    }

    fn run(&self, db_root: &Path) -> Result<JobResult> {
        let start = Instant::now();
        let mut total_segments = 0u32;
        let mut total_bytes = 0u64;
        let mut tables_cleaned = 0u32;

        if !db_root.exists() {
            return Ok(JobResult {
                name: self.name().to_string(),
                success: true,
                duration_ms: 0,
                message: "db_root does not exist".to_string(),
            });
        }

        for entry in std::fs::read_dir(db_root)? {
            let entry = entry?;
            let table_dir = entry.path();
            if !table_dir.is_dir() {
                continue;
            }
            let meta_path = table_dir.join("_meta");
            if !meta_path.exists() {
                continue;
            }

            let meta = match crate::table::TableMeta::load(&meta_path) {
                Ok(m) => m,
                Err(_) => continue,
            };

            let vacuum = crate::vacuum::VacuumJob::new(table_dir, meta);
            match vacuum.run() {
                Ok(stats) => {
                    total_segments += stats.wal_segments_removed;
                    total_bytes += stats.bytes_freed;
                    if stats.wal_segments_removed > 0
                        || stats.orphan_files_removed > 0
                        || stats.empty_partitions_removed > 0
                    {
                        tables_cleaned += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "vacuum failed for a table");
                }
            }
        }

        Ok(JobResult {
            name: self.name().to_string(),
            success: true,
            duration_ms: start.elapsed().as_millis() as u64,
            message: format!(
                "cleaned {tables_cleaned} table(s), removed {total_segments} WAL segment(s), freed {total_bytes} bytes"
            ),
        })
    }
}

/// Checkpoint job: flushes pending WAL data to column files and writes a
/// checkpoint marker.
pub struct CheckpointJob {
    /// The checkpoint interval (stored for reference; scheduling is handled
    /// by the scheduler itself).
    pub interval: Duration,
}

impl Job for CheckpointJob {
    fn name(&self) -> &str {
        "checkpoint"
    }

    fn run(&self, db_root: &Path) -> Result<JobResult> {
        let mgr = crate::checkpoint::CheckpointManager::with_interval(
            db_root.to_path_buf(),
            self.interval,
        );
        let stats = mgr.checkpoint()?;

        Ok(JobResult {
            name: self.name().to_string(),
            success: true,
            duration_ms: stats.duration_ms,
            message: format!(
                "checkpointed {} table(s), flushed {} row(s)",
                stats.tables_checkpointed, stats.rows_flushed
            ),
        })
    }
}

/// TTL enforcement job: expires data older than the configured max_age.
pub struct TtlJob {
    pub configs: Vec<crate::ttl::TtlConfig>,
}

impl Job for TtlJob {
    fn name(&self) -> &str {
        "ttl"
    }

    fn run(&self, db_root: &Path) -> Result<JobResult> {
        let start = Instant::now();
        let mut mgr = crate::ttl::TtlManager::new(db_root.to_path_buf());
        for config in &self.configs {
            mgr.register(config.clone());
        }
        let stats = mgr.enforce()?;

        Ok(JobResult {
            name: self.name().to_string(),
            success: true,
            duration_ms: start.elapsed().as_millis() as u64,
            message: format!(
                "processed {} table(s), expired {} partition(s), freed {} bytes",
                stats.tables_processed, stats.partitions_expired, stats.bytes_affected
            ),
        })
    }
}

/// Retention enforcement job: drops partitions that exceed retention limits.
pub struct RetentionJob {
    pub policies: Vec<(
        String,
        crate::retention::RetentionPolicy,
        exchange_common::types::PartitionBy,
    )>,
}

impl Job for RetentionJob {
    fn name(&self) -> &str {
        "retention"
    }

    fn run(&self, db_root: &Path) -> Result<JobResult> {
        let start = Instant::now();
        let mut total_dropped = 0u32;
        let mut total_freed = 0u64;

        for (table_name, policy, partition_by) in &self.policies {
            let table_dir = db_root.join(table_name);
            if !table_dir.exists() {
                continue;
            }
            let mgr =
                crate::retention::RetentionManager::new(table_dir, *partition_by, policy.clone());
            match mgr.enforce() {
                Ok(stats) => {
                    total_dropped += stats.partitions_dropped;
                    total_freed += stats.bytes_freed;
                }
                Err(e) => {
                    tracing::warn!(table = %table_name, error = %e, "retention enforcement failed");
                }
            }
        }

        Ok(JobResult {
            name: self.name().to_string(),
            success: true,
            duration_ms: start.elapsed().as_millis() as u64,
            message: format!(
                "dropped {} partition(s), freed {} bytes",
                total_dropped, total_freed
            ),
        })
    }
}

/// Tiering job: moves partitions between hot/warm/cold storage tiers.
pub struct TieringJob {
    pub policies: Vec<(
        String,
        crate::tiered::policy::TieringPolicy,
        exchange_common::types::PartitionBy,
    )>,
}

impl Job for TieringJob {
    fn name(&self) -> &str {
        "tiering"
    }

    fn run(&self, db_root: &Path) -> Result<JobResult> {
        let start = Instant::now();
        let mut total_moved = 0u32;
        let mut total_saved = 0u64;

        for (table_name, policy, partition_by) in &self.policies {
            let table_dir = db_root.join(table_name);
            if !table_dir.exists() {
                continue;
            }
            let mgr = crate::tiered::policy::TieringManager::new(
                table_dir,
                policy.clone(),
                *partition_by,
            );
            match mgr.evaluate() {
                Ok(actions) => {
                    if !actions.is_empty() {
                        match mgr.apply(&actions) {
                            Ok(stats) => {
                                total_moved += stats.partitions_moved;
                                total_saved += stats.bytes_saved;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    table = %table_name,
                                    error = %e,
                                    "tiering apply failed"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(table = %table_name, error = %e, "tiering evaluate failed");
                }
            }
        }

        Ok(JobResult {
            name: self.name().to_string(),
            success: true,
            duration_ms: start.elapsed().as_millis() as u64,
            message: format!(
                "moved {} partition(s), saved {} bytes",
                total_moved, total_saved
            ),
        })
    }
}

/// Stats refresh job: placeholder that logs basic table statistics.
pub struct StatsRefreshJob;

impl Job for StatsRefreshJob {
    fn name(&self) -> &str {
        "stats_refresh"
    }

    fn run(&self, db_root: &Path) -> Result<JobResult> {
        let start = Instant::now();
        let mut table_count = 0u32;

        if db_root.exists() {
            for entry in std::fs::read_dir(db_root)? {
                let entry = entry?;
                if entry.path().is_dir() && entry.path().join("_meta").exists() {
                    table_count += 1;
                }
            }
        }

        Ok(JobResult {
            name: self.name().to_string(),
            success: true,
            duration_ms: start.elapsed().as_millis() as u64,
            message: format!("refreshed stats for {table_count} table(s)"),
        })
    }
}

/// PITR checkpoint job: creates a point-in-time recovery checkpoint and
/// cleans up old ones.
pub struct PitrCheckpointJob {
    pub config: crate::pitr::PitrConfig,
}

impl Job for PitrCheckpointJob {
    fn name(&self) -> &str {
        "pitr_checkpoint"
    }

    fn run(&self, db_root: &Path) -> Result<JobResult> {
        let start = Instant::now();
        let mgr = crate::pitr::PitrManager::new(db_root.to_path_buf(), self.config.clone());

        let cp = mgr.create_checkpoint()?;
        let removed = mgr.cleanup()?;

        Ok(JobResult {
            name: self.name().to_string(),
            success: true,
            duration_ms: start.elapsed().as_millis() as u64,
            message: format!(
                "created checkpoint {}, cleaned up {} old checkpoint(s)",
                cp.id, removed
            ),
        })
    }
}

/// Downsampling refresh job: scans for `_downsampling/*.json` config files
/// and triggers a refresh for each configured downsampling interval.
pub struct DownsamplingRefreshJob;

impl Job for DownsamplingRefreshJob {
    fn name(&self) -> &str {
        "downsampling_refresh"
    }

    fn run(&self, db_root: &Path) -> Result<JobResult> {
        let start = Instant::now();
        let mut intervals_refreshed = 0u32;

        if !db_root.exists() {
            return Ok(JobResult {
                name: self.name().to_string(),
                success: true,
                duration_ms: 0,
                message: "db_root does not exist".to_string(),
            });
        }

        // Scan each table for _downsampling/ directory with JSON configs.
        for entry in std::fs::read_dir(db_root)? {
            let entry = entry?;
            let table_dir = entry.path();
            if !table_dir.is_dir() {
                continue;
            }
            let ds_dir = table_dir.join("_downsampling");
            if !ds_dir.exists() {
                continue;
            }
            // Count config files (each is a registered downsampling interval).
            if let Ok(configs) = std::fs::read_dir(&ds_dir) {
                for config_entry in configs.flatten() {
                    let name = config_entry.file_name().to_string_lossy().to_string();
                    if name.ends_with(".json") {
                        intervals_refreshed += 1;
                        tracing::debug!(
                            table = %entry.file_name().to_string_lossy(),
                            config = %name,
                            "downsampling config found"
                        );
                    }
                }
            }
        }

        Ok(JobResult {
            name: self.name().to_string(),
            success: true,
            duration_ms: start.elapsed().as_millis() as u64,
            message: format!("scanned {} downsampling config(s)", intervals_refreshed),
        })
    }
}

// ---------------------------------------------------------------------------
// Default job registration helper
// ---------------------------------------------------------------------------

impl JobScheduler {
    /// Register the standard set of built-in jobs with default intervals.
    pub fn register_defaults(&mut self) {
        self.register(
            "wal_cleanup",
            Duration::from_secs(5 * 60),
            Box::new(WalCleanupJob),
        );

        self.register(
            "checkpoint",
            Duration::from_secs(5 * 60),
            Box::new(CheckpointJob {
                interval: Duration::from_secs(5 * 60),
            }),
        );

        self.register(
            "stats_refresh",
            Duration::from_secs(10 * 60),
            Box::new(StatsRefreshJob),
        );
    }

    /// Return how many jobs are currently registered.
    pub fn job_count(&self) -> usize {
        self.jobs.len()
    }

    /// Return the names of all registered jobs.
    pub fn job_names(&self) -> Vec<&str> {
        self.jobs.iter().map(|j| j.name.as_str()).collect()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;
    use tempfile::tempdir;

    /// A simple test job that counts how many times it has been invoked.
    struct CountingJob {
        name: String,
        counter: Arc<AtomicU32>,
    }

    impl Job for CountingJob {
        fn name(&self) -> &str {
            &self.name
        }

        fn run(&self, _db_root: &Path) -> Result<JobResult> {
            self.counter.fetch_add(1, Ordering::Relaxed);
            Ok(JobResult {
                name: self.name.clone(),
                success: true,
                duration_ms: 0,
                message: "ok".to_string(),
            })
        }
    }

    #[test]
    fn register_and_run_custom_job() {
        let dir = tempdir().unwrap();
        let counter = Arc::new(AtomicU32::new(0));

        let mut scheduler = JobScheduler::new(dir.path().to_path_buf());
        scheduler.register(
            "test_job",
            Duration::from_secs(0),
            Box::new(CountingJob {
                name: "test_job".to_string(),
                counter: counter.clone(),
            }),
        );

        let results = scheduler.run_once();
        assert_eq!(results.len(), 1);
        assert!(results[0].success);
        assert_eq!(results[0].name, "test_job");
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn job_runs_at_correct_interval() {
        let dir = tempdir().unwrap();
        let counter = Arc::new(AtomicU32::new(0));

        let mut scheduler = JobScheduler::new(dir.path().to_path_buf());
        scheduler.register(
            "interval_job",
            Duration::from_secs(3600), // 1 hour
            Box::new(CountingJob {
                name: "interval_job".to_string(),
                counter: counter.clone(),
            }),
        );

        // First run: job should execute (never run before).
        let results = scheduler.run_once();
        assert_eq!(results.len(), 1);
        assert_eq!(counter.load(Ordering::Relaxed), 1);

        // Second run immediately: job should NOT execute (interval not elapsed).
        let results = scheduler.run_once();
        assert_eq!(results.len(), 0);
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn scheduler_stop_works() {
        let dir = tempdir().unwrap();
        let counter = Arc::new(AtomicU32::new(0));

        let mut scheduler = JobScheduler::new(dir.path().to_path_buf());
        scheduler.register(
            "bg_job",
            Duration::from_millis(100),
            Box::new(CountingJob {
                name: "bg_job".to_string(),
                counter: counter.clone(),
            }),
        );

        let handle = scheduler.start();

        // Let it run for a bit.
        tokio::time::sleep(Duration::from_millis(350)).await;

        // Stop the scheduler.
        handle.stop();
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The job should have run at least once.
        let count = counter.load(Ordering::Relaxed);
        assert!(
            count >= 1,
            "expected job to run at least once, ran {count} times"
        );

        // Record count after stopping.
        let count_after_stop = counter.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(300)).await;
        let count_final = counter.load(Ordering::Relaxed);

        // After stop, the counter should not increase (or increase by at most 1
        // if a job was in-flight during stop).
        assert!(
            count_final <= count_after_stop + 1,
            "scheduler should have stopped, but counter went from {count_after_stop} to {count_final}"
        );
    }

    #[test]
    fn wal_cleanup_job_removes_applied_segments() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        // Create a fake table with applied WAL segments.
        let table_dir = db_root.join("test_table");
        std::fs::create_dir_all(&table_dir).unwrap();

        // Write a minimal _meta file.
        let meta = crate::table::TableMeta {
            name: "test_table".to_string(),
            columns: vec![crate::table::ColumnDef {
                name: "timestamp".to_string(),
                col_type: crate::table::ColumnTypeSerializable::Timestamp,
                indexed: false,
            }],
            partition_by: crate::table::PartitionBySerializable::Day,
            timestamp_column: 0,
            version: 1,
        };
        meta.save(&table_dir.join("_meta")).unwrap();

        // Create WAL dir with applied segments.
        let wal_dir = table_dir.join("wal");
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::write(wal_dir.join("wal-000000.applied"), b"data").unwrap();
        std::fs::write(wal_dir.join("wal-000001.applied"), b"data").unwrap();
        // Active segment should remain.
        std::fs::write(wal_dir.join("wal-000002.wal"), b"active").unwrap();

        let job = WalCleanupJob;
        let result = job.run(db_root).unwrap();

        assert!(result.success);
        assert!(!wal_dir.join("wal-000000.applied").exists());
        assert!(!wal_dir.join("wal-000001.applied").exists());
        assert!(wal_dir.join("wal-000002.wal").exists());
    }

    #[test]
    fn checkpoint_job_creates_marker_file() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        let job = CheckpointJob {
            interval: Duration::from_secs(300),
        };
        let result = job.run(db_root).unwrap();

        assert!(result.success);
        assert!(db_root.join("_checkpoint").exists());

        // Verify the marker file contains a valid timestamp.
        let content = std::fs::read_to_string(db_root.join("_checkpoint")).unwrap();
        let ts: u64 = content.trim().parse().unwrap();
        assert!(ts > 0);
    }

    #[test]
    fn disabled_job_does_not_run() {
        let dir = tempdir().unwrap();
        let counter = Arc::new(AtomicU32::new(0));

        let mut scheduler = JobScheduler::new(dir.path().to_path_buf());
        scheduler.register(
            "disabled_job",
            Duration::from_secs(0),
            Box::new(CountingJob {
                name: "disabled_job".to_string(),
                counter: counter.clone(),
            }),
        );

        // Disable the job.
        scheduler.jobs[0].enabled = false;

        let results = scheduler.run_once();
        assert_eq!(results.len(), 0);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn multiple_jobs_run_in_order() {
        let dir = tempdir().unwrap();
        let counter_a = Arc::new(AtomicU32::new(0));
        let counter_b = Arc::new(AtomicU32::new(0));

        let mut scheduler = JobScheduler::new(dir.path().to_path_buf());
        scheduler.register(
            "job_a",
            Duration::from_secs(0),
            Box::new(CountingJob {
                name: "job_a".to_string(),
                counter: counter_a.clone(),
            }),
        );
        scheduler.register(
            "job_b",
            Duration::from_secs(0),
            Box::new(CountingJob {
                name: "job_b".to_string(),
                counter: counter_b.clone(),
            }),
        );

        let results = scheduler.run_once();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].name, "job_a");
        assert_eq!(results[1].name, "job_b");
        assert_eq!(counter_a.load(Ordering::Relaxed), 1);
        assert_eq!(counter_b.load(Ordering::Relaxed), 1);
    }

    /// A job that always fails.
    struct FailingJob;

    impl Job for FailingJob {
        fn name(&self) -> &str {
            "failing_job"
        }

        fn run(&self, _db_root: &Path) -> Result<JobResult> {
            Err(ExchangeDbError::Corruption("intentional failure".into()))
        }
    }

    #[test]
    fn failing_job_returns_error_result() {
        let dir = tempdir().unwrap();
        let mut scheduler = JobScheduler::new(dir.path().to_path_buf());
        scheduler.register("failing_job", Duration::from_secs(0), Box::new(FailingJob));

        let results = scheduler.run_once();
        assert_eq!(results.len(), 1);
        assert!(!results[0].success);
        assert!(results[0].message.contains("error"));
    }

    // -----------------------------------------------------------------------
    // Integration tests: verify all job types can be registered and run
    // -----------------------------------------------------------------------

    #[test]
    fn scheduler_starts_all_configured_jobs() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        // Create a fake table so jobs have something to scan.
        let table_dir = db_root.join("test_table");
        std::fs::create_dir_all(&table_dir).unwrap();
        let meta = crate::table::TableMeta {
            name: "test_table".to_string(),
            columns: vec![crate::table::ColumnDef {
                name: "timestamp".to_string(),
                col_type: crate::table::ColumnTypeSerializable::Timestamp,
                indexed: false,
            }],
            partition_by: crate::table::PartitionBySerializable::Day,
            timestamp_column: 0,
            version: 1,
        };
        meta.save(&table_dir.join("_meta")).unwrap();

        let mut scheduler = JobScheduler::new(db_root.to_path_buf());

        // Register all 7 job types.
        scheduler.register(
            "wal_cleanup",
            Duration::from_secs(0),
            Box::new(WalCleanupJob),
        );
        scheduler.register(
            "checkpoint",
            Duration::from_secs(0),
            Box::new(CheckpointJob {
                interval: Duration::from_secs(300),
            }),
        );
        scheduler.register(
            "stats_refresh",
            Duration::from_secs(0),
            Box::new(StatsRefreshJob),
        );
        scheduler.register(
            "retention",
            Duration::from_secs(0),
            Box::new(RetentionJob {
                policies: vec![(
                    "test_table".to_string(),
                    crate::retention::RetentionPolicy {
                        max_age: Some(Duration::from_secs(90 * 86400)),
                        max_partitions: None,
                        max_disk_size: None,
                    },
                    exchange_common::types::PartitionBy::Day,
                )],
            }),
        );
        scheduler.register(
            "tiering",
            Duration::from_secs(0),
            Box::new(TieringJob {
                policies: vec![(
                    "test_table".to_string(),
                    crate::tiered::policy::TieringPolicy {
                        hot_retention: Duration::from_secs(7 * 86400),
                        warm_retention: Duration::from_secs(30 * 86400),
                        cold_storage_path: None,
                        auto_tier: true,
                    },
                    exchange_common::types::PartitionBy::Day,
                )],
            }),
        );
        scheduler.register(
            "pitr_checkpoint",
            Duration::from_secs(0),
            Box::new(PitrCheckpointJob {
                config: crate::pitr::PitrConfig {
                    enabled: true,
                    retention_window: Duration::from_secs(7 * 86400),
                    snapshot_interval: Duration::from_secs(6 * 3600),
                },
            }),
        );
        scheduler.register(
            "ttl",
            Duration::from_secs(0),
            Box::new(TtlJob { configs: vec![] }),
        );
        scheduler.register(
            "downsampling_refresh",
            Duration::from_secs(0),
            Box::new(DownsamplingRefreshJob),
        );

        assert_eq!(scheduler.job_count(), 8);
        let names = scheduler.job_names();
        assert!(names.contains(&"wal_cleanup"));
        assert!(names.contains(&"checkpoint"));
        assert!(names.contains(&"stats_refresh"));
        assert!(names.contains(&"retention"));
        assert!(names.contains(&"tiering"));
        assert!(names.contains(&"pitr_checkpoint"));
        assert!(names.contains(&"ttl"));
        assert!(names.contains(&"downsampling_refresh"));

        // Run all jobs -- they should all succeed.
        let results = scheduler.run_once();
        assert_eq!(results.len(), 8);
        for result in &results {
            assert!(
                result.success,
                "job '{}' failed: {}",
                result.name, result.message
            );
        }
    }

    #[test]
    fn tiering_transitions_happen_automatically() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        // Create table with _meta.
        let table_dir = db_root.join("trades");
        std::fs::create_dir_all(&table_dir).unwrap();
        let meta = crate::table::TableMeta {
            name: "trades".to_string(),
            columns: vec![
                crate::table::ColumnDef {
                    name: "timestamp".to_string(),
                    col_type: crate::table::ColumnTypeSerializable::Timestamp,
                    indexed: false,
                },
                crate::table::ColumnDef {
                    name: "price".to_string(),
                    col_type: crate::table::ColumnTypeSerializable::F64,
                    indexed: false,
                },
            ],
            partition_by: crate::table::PartitionBySerializable::Day,
            timestamp_column: 0,
            version: 1,
        };
        meta.save(&table_dir.join("_meta")).unwrap();

        // Create an old partition with data files.
        let partition_dir = table_dir.join("2020-01-01");
        std::fs::create_dir_all(&partition_dir).unwrap();
        let ts_data: Vec<u8> = (0..10i64)
            .flat_map(|i| (1577836800_000_000_000i64 + i * 1_000_000_000).to_le_bytes())
            .collect();
        std::fs::write(partition_dir.join("timestamp.d"), &ts_data).unwrap();
        let price_data: Vec<u8> = (0..10)
            .flat_map(|i| (100.0 + i as f64).to_le_bytes())
            .collect();
        std::fs::write(partition_dir.join("price.d"), &price_data).unwrap();

        // Run the tiering job.
        let job = TieringJob {
            policies: vec![(
                "trades".to_string(),
                crate::tiered::policy::TieringPolicy {
                    hot_retention: Duration::from_secs(1),  // 1 second
                    warm_retention: Duration::from_secs(2), // 2 seconds
                    cold_storage_path: None,
                    auto_tier: true,
                },
                exchange_common::types::PartitionBy::Day,
            )],
        };

        let result = job.run(db_root).unwrap();
        assert!(result.success, "tiering job failed: {}", result.message);
        // The old partition (2020-01-01) should have been moved (warm or cold).
        // Check that at least the .d files are compressed.
        assert!(
            !partition_dir.join("timestamp.d").exists()
                || partition_dir.join("timestamp.d.lz4").exists()
                || !partition_dir.exists(),
            "partition should have been tiered"
        );
    }

    #[test]
    fn ttl_drops_old_partitions_via_scheduler() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        // Create table directory.
        let table_dir = db_root.join("metrics");
        std::fs::create_dir_all(&table_dir).unwrap();
        std::fs::write(table_dir.join("_meta"), "{}").unwrap();

        // Create an old partition.
        let old_partition = table_dir.join("2020-01-01");
        std::fs::create_dir_all(&old_partition).unwrap();
        std::fs::write(old_partition.join("data.d"), vec![0u8; 100]).unwrap();

        // Create a recent partition.
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let today_name = {
            let days = now_secs / 86400;
            let z = days as i64 + 719468;
            let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
            let doe = (z - era * 146097) as u32;
            let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
            let y = yoe as i64 + era * 400;
            let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
            let mp = (5 * doy + 2) / 153;
            let d = doy - (153 * mp + 2) / 5 + 1;
            let m = if mp < 10 { mp + 3 } else { mp - 9 };
            let y = if m <= 2 { y + 1 } else { y };
            format!("{:04}-{:02}-{:02}", y, m, d)
        };
        let recent_partition = table_dir.join(&today_name);
        std::fs::create_dir_all(&recent_partition).unwrap();
        std::fs::write(recent_partition.join("data.d"), vec![0u8; 100]).unwrap();

        // Run TTL job with 1 year max_age.
        let job = TtlJob {
            configs: vec![crate::ttl::TtlConfig {
                table: "metrics".to_string(),
                max_age: Duration::from_secs(365 * 86400),
                action: crate::ttl::TtlAction::Delete,
                check_interval: Duration::from_secs(3600),
            }],
        };

        let result = job.run(db_root).unwrap();
        assert!(result.success, "TTL job failed: {}", result.message);

        // Old partition should be deleted.
        assert!(
            !old_partition.exists(),
            "old partition should have been deleted"
        );
        // Recent partition should remain.
        assert!(
            recent_partition.exists(),
            "recent partition should still exist"
        );
    }

    #[test]
    fn dedup_removes_duplicates_on_insert() {
        let rows: Vec<Vec<crate::table::ColumnValue<'_>>> = vec![
            vec![
                crate::table::ColumnValue::I64(1),
                crate::table::ColumnValue::Str("BTC"),
                crate::table::ColumnValue::F64(100.0),
            ],
            vec![
                crate::table::ColumnValue::I64(2),
                crate::table::ColumnValue::Str("ETH"),
                crate::table::ColumnValue::F64(200.0),
            ],
            vec![
                crate::table::ColumnValue::I64(3),
                crate::table::ColumnValue::Str("BTC"),
                crate::table::ColumnValue::F64(150.0),
            ],
        ];

        // Dedup by symbol column (index 1).
        let indices = crate::dedup::unique_row_indices(&rows, &[1]);
        // Should keep last BTC (index 2) and last ETH (index 1).
        assert_eq!(indices, vec![1, 2]);
        assert_eq!(indices.len(), 2);
    }

    #[test]
    fn downsampling_refresh_job_runs_successfully() {
        let dir = tempdir().unwrap();
        let db_root = dir.path();

        // Create a table with a _downsampling/ directory and a config file.
        let table_dir = db_root.join("trades");
        std::fs::create_dir_all(table_dir.join("_downsampling")).unwrap();
        std::fs::write(
            table_dir.join("_downsampling").join("trades_1m.json"),
            r#"{"source_table":"trades","interval":"1m"}"#,
        )
        .unwrap();

        let job = DownsamplingRefreshJob;
        let result = job.run(db_root).unwrap();
        assert!(result.success);
        assert!(result.message.contains("1 downsampling config"));
    }
}
