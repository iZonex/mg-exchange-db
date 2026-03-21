//! Fsync strategies for controlling durability vs. performance trade-offs.

use std::time::Duration;

/// Controls when data is fsynced to disk.
#[derive(Debug, Clone, PartialEq)]
pub enum SyncMode {
    /// fsync after every commit (safest, slowest).
    Full,
    /// fsync periodically (default: every 1 second).
    Periodic { interval: Duration },
    /// Let the OS handle flushing (fastest, risk of data loss on crash).
    None,
}

impl Default for SyncMode {
    fn default() -> Self {
        SyncMode::Periodic {
            interval: Duration::from_secs(1),
        }
    }
}

impl SyncMode {
    /// Create a periodic sync mode with the given interval.
    pub fn periodic(interval: Duration) -> Self {
        SyncMode::Periodic { interval }
    }

    /// Returns true if this mode requires an immediate fsync after every write.
    pub fn is_full(&self) -> bool {
        matches!(self, SyncMode::Full)
    }

    /// Returns true if fsync is entirely delegated to the OS.
    pub fn is_none(&self) -> bool {
        matches!(self, SyncMode::None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_periodic() {
        let mode = SyncMode::default();
        match mode {
            SyncMode::Periodic { interval } => {
                assert_eq!(interval, Duration::from_secs(1));
            }
            _ => panic!("expected Periodic"),
        }
    }

    #[test]
    fn sync_mode_full() {
        let mode = SyncMode::Full;
        assert!(mode.is_full());
        assert!(!mode.is_none());
    }

    #[test]
    fn sync_mode_none() {
        let mode = SyncMode::None;
        assert!(mode.is_none());
        assert!(!mode.is_full());
    }

    #[test]
    fn sync_mode_periodic_custom() {
        let mode = SyncMode::periodic(Duration::from_millis(500));
        assert!(!mode.is_full());
        assert!(!mode.is_none());
        match mode {
            SyncMode::Periodic { interval } => {
                assert_eq!(interval, Duration::from_millis(500));
            }
            _ => panic!("expected Periodic"),
        }
    }
}
