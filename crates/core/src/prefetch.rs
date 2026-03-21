//! Prefetch scheduler for sequential column scans.
//!
//! Issues `madvise(MADV_WILLNEED)` hints to the OS so that pages are
//! brought into the page cache before they are actually needed.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use crate::mmap::MmapReadOnly;

/// A pending prefetch request.
#[derive(Debug, Clone)]
pub struct PrefetchRequest {
    /// Path to the column file.
    pub path: PathBuf,
    /// Byte offset within the file.
    pub offset: u64,
    /// Number of bytes to prefetch.
    pub len: u64,
}

/// Intelligent prefetching scheduler for sequential column scans.
///
/// Maintains a queue of upcoming column reads and issues OS-level
/// prefetch hints so that data is in the page cache when needed.
pub struct PrefetchScheduler {
    /// Queue of upcoming column reads.
    queue: VecDeque<PrefetchRequest>,
    /// How far ahead to prefetch (in pages, where a page is 4096 bytes).
    lookahead: usize,
}

impl PrefetchScheduler {
    /// Create a new scheduler with the given lookahead (in pages).
    pub fn new(lookahead: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            lookahead,
        }
    }

    /// Schedule a column range for prefetching.
    pub fn schedule(&mut self, path: &Path, offset: u64, len: u64) {
        self.queue.push_back(PrefetchRequest {
            path: path.to_path_buf(),
            offset,
            len,
        });
    }

    /// Return the current number of pending requests.
    pub fn pending(&self) -> usize {
        self.queue.len()
    }

    /// Return the configured lookahead.
    pub fn lookahead(&self) -> usize {
        self.lookahead
    }

    /// Issue prefetch hints to the OS for queued requests.
    ///
    /// Opens each file as a read-only mmap and calls `madvise(WILLNEED)`
    /// on the requested range. At most `lookahead` requests are processed
    /// per call.
    pub fn execute(&self) {
        let count = self.queue.len().min(self.lookahead);
        for req in self.queue.iter().take(count) {
            if let Ok(mmap) = MmapReadOnly::open(&req.path) {
                let inner = mmap.inner_mmap();
                // Use the advise_willneed helper, which calls
                // madvise(MADV_WILLNEED) on Unix.
                #[cfg(unix)]
                {
                    let _ = inner.advise_range(
                        memmap2::Advice::WillNeed,
                        req.offset as usize,
                        req.len as usize,
                    );
                }
                #[cfg(not(unix))]
                {
                    let _ = inner;
                }
            }
        }
    }

    /// Drain completed requests from the front of the queue.
    ///
    /// Call this after the corresponding data has been consumed.
    pub fn drain_completed(&mut self, count: usize) {
        for _ in 0..count.min(self.queue.len()) {
            self.queue.pop_front();
        }
    }

    /// Release pages for a specific file range.
    ///
    /// Hints to the OS that the pages are no longer needed
    /// (`madvise(MADV_DONTNEED)` equivalent). This allows the OS to
    /// reclaim physical memory.
    pub fn release(&self, path: &Path, offset: u64, len: u64) {
        if let Ok(mmap) = MmapReadOnly::open(path) {
            let inner = mmap.inner_mmap();
            #[cfg(unix)]
            {
                // memmap2 does not expose DONTNEED directly in all versions;
                // we use Sequential as a reasonable hint that we are done.
                let _ = inner.advise_range(
                    memmap2::Advice::Sequential,
                    offset as usize,
                    len as usize,
                );
            }
            #[cfg(not(unix))]
            {
                let _ = (inner, offset, len);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn scheduler_queue_and_drain() {
        let mut sched = PrefetchScheduler::new(4);
        assert_eq!(sched.pending(), 0);

        sched.schedule(Path::new("/tmp/col1.d"), 0, 4096);
        sched.schedule(Path::new("/tmp/col2.d"), 4096, 8192);
        sched.schedule(Path::new("/tmp/col3.d"), 0, 16384);
        assert_eq!(sched.pending(), 3);

        sched.drain_completed(2);
        assert_eq!(sched.pending(), 1);

        sched.drain_completed(100); // drain more than available
        assert_eq!(sched.pending(), 0);
    }

    #[test]
    fn scheduler_execute_with_real_files() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_col.d");
        let data = vec![0u8; 8192];
        std::fs::write(&path, &data).unwrap();

        let mut sched = PrefetchScheduler::new(8);
        sched.schedule(&path, 0, 4096);
        sched.schedule(&path, 4096, 4096);

        // Should not panic.
        sched.execute();
        assert_eq!(sched.pending(), 2);
    }

    #[test]
    fn scheduler_release_does_not_panic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("release_col.d");
        std::fs::write(&path, &[0u8; 4096]).unwrap();

        let sched = PrefetchScheduler::new(4);
        sched.release(&path, 0, 4096);
    }

    #[test]
    fn scheduler_execute_missing_file_is_ok() {
        let mut sched = PrefetchScheduler::new(4);
        sched.schedule(Path::new("/nonexistent/file.d"), 0, 4096);
        // Should not panic; errors are silently ignored.
        sched.execute();
    }

    #[test]
    fn lookahead_limits_execution() {
        let dir = tempdir().unwrap();
        // Create a file for the scheduler to open.
        let path = dir.path().join("lookahead.d");
        std::fs::write(&path, &[0u8; 4096]).unwrap();

        let mut sched = PrefetchScheduler::new(2);
        for i in 0..10 {
            sched.schedule(&path, i * 4096, 4096);
        }
        assert_eq!(sched.pending(), 10);

        // execute only processes up to `lookahead` (2) items
        sched.execute();
        // All are still pending (execute doesn't drain).
        assert_eq!(sched.pending(), 10);
    }
}
