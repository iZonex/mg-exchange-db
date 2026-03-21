//! Simple log rotation helper for ExchangeDB.
//!
//! `RotatingLog` writes to a file and automatically rotates when the file
//! exceeds `max_size` bytes. Rotated files are renamed with a numeric suffix
//! (`.1`, `.2`, ...) up to `max_files`. The oldest file is deleted when the
//! limit is reached.


use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use anyhow::{Context, Result};

/// A rotating log file writer.
///
/// Thread-safe via an internal `Mutex` around the writer.
pub struct RotatingLog {
    dir: PathBuf,
    prefix: String,
    max_size: u64,
    max_files: usize,
    current: Mutex<BufWriter<File>>,
}

impl RotatingLog {
    /// Create a new rotating log.
    ///
    /// - `dir` -- directory where log files are stored (created if missing).
    /// - `prefix` -- base name of the log file (e.g. `"exchangedb"`).
    /// - `max_size` -- maximum size in bytes before rotation.
    /// - `max_files` -- maximum number of rotated files to keep.
    pub fn new(dir: &Path, prefix: &str, max_size: u64, max_files: usize) -> Result<Self> {
        fs::create_dir_all(dir)
            .with_context(|| format!("failed to create log directory: {}", dir.display()))?;

        let current_path = dir.join(format!("{prefix}.log"));
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current_path)
            .with_context(|| format!("failed to open log file: {}", current_path.display()))?;

        Ok(Self {
            dir: dir.to_path_buf(),
            prefix: prefix.to_string(),
            max_size,
            max_files,
            current: Mutex::new(BufWriter::new(file)),
        })
    }

    /// Write a message to the log, rotating if needed.
    pub fn write(&self, msg: &str) -> Result<()> {
        let mut writer = self.current.lock().map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;

        writer
            .write_all(msg.as_bytes())
            .context("failed to write to log")?;
        writer.flush().context("failed to flush log")?;

        // Check if rotation is needed.
        let current_path = self.current_path();
        let size = fs::metadata(&current_path)
            .map(|m| m.len())
            .unwrap_or(0);

        if size >= self.max_size {
            // Drop the old writer before renaming files.
            drop(writer);
            self.rotate()?;
        }

        Ok(())
    }

    /// Perform rotation: rename current -> .1, .1 -> .2, etc.
    fn rotate(&self) -> Result<()> {
        // Remove the oldest file if it exceeds max_files.
        let oldest = self.rotated_path(self.max_files);
        if oldest.exists() {
            fs::remove_file(&oldest)
                .with_context(|| format!("failed to remove oldest log: {}", oldest.display()))?;
        }

        // Shift existing rotated files up by one.
        for i in (1..self.max_files).rev() {
            let from = self.rotated_path(i);
            let to = self.rotated_path(i + 1);
            if from.exists() {
                fs::rename(&from, &to).with_context(|| {
                    format!(
                        "failed to rename {} -> {}",
                        from.display(),
                        to.display()
                    )
                })?;
            }
        }

        // Rename current -> .1
        let current = self.current_path();
        let first_rotated = self.rotated_path(1);
        if current.exists() {
            fs::rename(&current, &first_rotated).with_context(|| {
                format!(
                    "failed to rename {} -> {}",
                    current.display(),
                    first_rotated.display()
                )
            })?;
        }

        // Open a fresh file.
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&current)
            .with_context(|| format!("failed to open new log file: {}", current.display()))?;

        let mut writer = self
            .current
            .lock()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
        *writer = BufWriter::new(file);

        Ok(())
    }

    fn current_path(&self) -> PathBuf {
        self.dir.join(format!("{}.log", self.prefix))
    }

    fn rotated_path(&self, n: usize) -> PathBuf {
        self.dir.join(format!("{}.log.{}", self.prefix, n))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn rotating_log_rotates_at_max_size() {
        let tmp = TempDir::new().unwrap();
        let log = RotatingLog::new(tmp.path(), "test", 50, 3).unwrap();

        // Write enough data to trigger rotation.
        // Each write is ~26 bytes ("AAAAAAAAAAAAAAAAAAAAAAAAA\n").
        let msg = "AAAAAAAAAAAAAAAAAAAAAAAAA\n"; // 25 + 1 = 26 bytes
        log.write(msg).unwrap(); // 26 bytes total
        log.write(msg).unwrap(); // 52 bytes total -> triggers rotation

        // After rotation, current file should exist and be small/empty.
        let current = tmp.path().join("test.log");
        assert!(current.exists());

        // Rotated file .1 should exist with the old content.
        let rotated_1 = tmp.path().join("test.log.1");
        assert!(rotated_1.exists());
        let content = fs::read_to_string(&rotated_1).unwrap();
        assert!(content.len() >= 50); // Should have the old data
    }

    #[test]
    fn rotating_log_keeps_max_files() {
        let tmp = TempDir::new().unwrap();
        let max_files = 2;
        let log = RotatingLog::new(tmp.path(), "app", 20, max_files).unwrap();

        let msg = "12345678901234567890\n"; // 21 bytes -- triggers rotation each time

        // Write enough to trigger 3 rotations, but only 2 rotated files should exist.
        log.write(msg).unwrap(); // 21 bytes -> rotate
        log.write(msg).unwrap(); // rotate again
        log.write(msg).unwrap(); // rotate again

        let current = tmp.path().join("app.log");
        let rot1 = tmp.path().join("app.log.1");
        let rot2 = tmp.path().join("app.log.2");
        let rot3 = tmp.path().join("app.log.3");

        assert!(current.exists(), "current log must exist");
        assert!(rot1.exists(), "rotated .1 must exist");
        assert!(rot2.exists(), "rotated .2 must exist");
        assert!(!rot3.exists(), "rotated .3 must NOT exist (max_files=2)");
    }

    #[test]
    fn rotating_log_creates_directory() {
        let tmp = TempDir::new().unwrap();
        let nested = tmp.path().join("logs").join("nested");
        let log = RotatingLog::new(&nested, "db", 1000, 5).unwrap();
        log.write("hello\n").unwrap();

        assert!(nested.join("db.log").exists());
    }
}
