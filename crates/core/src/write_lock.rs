//! File-based lock for exclusive table write access.
//!
//! Uses a lock file (`_write.lock`) in the table directory and the `fs2` crate
//! for cross-process advisory locking (`flock(2)` on Unix).

use exchange_common::error::{ExchangeDbError, Result};
use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// File-based lock for exclusive table write access.
/// Uses a lock file (`_write.lock`) in the table directory.
#[derive(Debug)]
pub struct TableWriteLock {
    lock_file: File,
    #[allow(dead_code)]
    lock_path: PathBuf,
}

impl TableWriteLock {
    /// Try to acquire the write lock. Returns error if already locked.
    pub fn try_acquire(table_dir: &Path) -> Result<Self> {
        let lock_path = table_dir.join("_write.lock");

        let lock_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;

        lock_file.try_lock_exclusive().map_err(|_| {
            ExchangeDbError::LockContention(format!(
                "table write lock already held: {}",
                table_dir.display()
            ))
        })?;

        Ok(Self {
            lock_file,
            lock_path,
        })
    }

    /// Acquire with timeout (spin with sleep).
    pub fn acquire_timeout(table_dir: &Path, timeout: Duration) -> Result<Self> {
        let start = Instant::now();
        let sleep_interval = Duration::from_millis(1);

        loop {
            match Self::try_acquire(table_dir) {
                Ok(lock) => return Ok(lock),
                Err(ExchangeDbError::LockContention(_)) => {
                    if start.elapsed() >= timeout {
                        return Err(ExchangeDbError::LockTimeout(format!(
                            "timed out acquiring write lock for table: {}",
                            table_dir.display()
                        )));
                    }
                    std::thread::sleep(sleep_interval);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Check if a table is currently locked (non-blocking probe).
    pub fn is_locked(table_dir: &Path) -> bool {
        let lock_path = table_dir.join("_write.lock");

        let file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&lock_path)
        {
            Ok(f) => f,
            Err(_) => return false, // lock file doesn't exist => not locked
        };

        // Try to acquire; if we can, it's not locked. Release immediately.
        match file.try_lock_exclusive() {
            Ok(()) => {
                let _ = file.unlock();
                false
            }
            Err(_) => true,
        }
    }
}

impl Drop for TableWriteLock {
    fn drop(&mut self) {
        let _ = self.lock_file.unlock();
        // We intentionally do NOT delete the lock file to avoid races
        // where another process is about to open it.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn acquire_and_release_on_drop() {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("test_table");
        std::fs::create_dir_all(&table_dir).unwrap();

        {
            let _lock = TableWriteLock::try_acquire(&table_dir).unwrap();
            // Lock is held
            assert!(TableWriteLock::is_locked(&table_dir));
        }
        // Lock released on drop
        assert!(!TableWriteLock::is_locked(&table_dir));
    }

    #[test]
    fn double_acquire_fails() {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("test_table");
        std::fs::create_dir_all(&table_dir).unwrap();

        let _lock = TableWriteLock::try_acquire(&table_dir).unwrap();
        let result = TableWriteLock::try_acquire(&table_dir);
        assert!(result.is_err());
    }

    #[test]
    fn acquire_timeout_succeeds_after_release() {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("test_table");
        std::fs::create_dir_all(&table_dir).unwrap();

        let table_dir_clone = table_dir.clone();

        // Spawn a thread that holds the lock briefly
        let handle = std::thread::spawn(move || {
            let _lock = TableWriteLock::try_acquire(&table_dir_clone).unwrap();
            std::thread::sleep(Duration::from_millis(50));
            // lock released on drop
        });

        // Give the thread a moment to acquire
        std::thread::sleep(Duration::from_millis(10));

        // This should succeed within the timeout
        let lock = TableWriteLock::acquire_timeout(&table_dir, Duration::from_secs(2)).unwrap();
        drop(lock);
        handle.join().unwrap();
    }

    #[test]
    fn acquire_timeout_times_out() {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("test_table");
        std::fs::create_dir_all(&table_dir).unwrap();

        let _lock = TableWriteLock::try_acquire(&table_dir).unwrap();

        let result =
            TableWriteLock::acquire_timeout(&table_dir, Duration::from_millis(50));
        assert!(result.is_err());
        match result.unwrap_err() {
            ExchangeDbError::LockTimeout(_) => {}
            e => panic!("expected LockTimeout, got: {e}"),
        }
    }

    #[test]
    fn is_locked_no_file() {
        let dir = tempdir().unwrap();
        let table_dir = dir.path().join("nonexistent_table");
        assert!(!TableWriteLock::is_locked(&table_dir));
    }
}
