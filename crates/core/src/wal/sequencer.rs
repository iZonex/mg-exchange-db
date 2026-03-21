use std::sync::atomic::{AtomicU64, Ordering};

/// Assigns monotonically increasing transaction IDs.
///
/// The sequencer is thread-safe and lock-free. It uses an atomic counter
/// to hand out unique, strictly increasing txn IDs.
pub struct Sequencer {
    next_txn_id: AtomicU64,
}

impl Sequencer {
    /// Create a new sequencer starting from txn_id 1.
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
        }
    }

    /// Create a sequencer that resumes from a given ID.
    /// The next issued ID will be `start + 1`.
    pub fn resume_from(last_txn_id: u64) -> Self {
        Self {
            next_txn_id: AtomicU64::new(last_txn_id + 1),
        }
    }

    /// Allocate the next transaction ID.
    pub fn next_txn_id(&self) -> u64 {
        self.next_txn_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Peek at the next ID that would be issued without consuming it.
    pub fn peek_next(&self) -> u64 {
        self.next_txn_id.load(Ordering::SeqCst)
    }

    /// Return the last issued transaction ID, or 0 if none have been issued.
    pub fn last_txn_id(&self) -> u64 {
        let next = self.next_txn_id.load(Ordering::SeqCst);
        if next == 0 { 0 } else { next - 1 }
    }
}

impl Default for Sequencer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_ids() {
        let seq = Sequencer::new();
        assert_eq!(seq.next_txn_id(), 1);
        assert_eq!(seq.next_txn_id(), 2);
        assert_eq!(seq.next_txn_id(), 3);
    }

    #[test]
    fn resume_from() {
        let seq = Sequencer::resume_from(100);
        assert_eq!(seq.next_txn_id(), 101);
        assert_eq!(seq.next_txn_id(), 102);
    }

    #[test]
    fn peek_does_not_consume() {
        let seq = Sequencer::new();
        assert_eq!(seq.peek_next(), 1);
        assert_eq!(seq.peek_next(), 1);
        assert_eq!(seq.next_txn_id(), 1);
        assert_eq!(seq.peek_next(), 2);
    }

    #[test]
    fn last_txn_id() {
        let seq = Sequencer::new();
        assert_eq!(seq.last_txn_id(), 0);
        seq.next_txn_id();
        assert_eq!(seq.last_txn_id(), 1);
        seq.next_txn_id();
        assert_eq!(seq.last_txn_id(), 2);
    }

    #[test]
    fn concurrent_ids_are_unique() {
        use std::sync::Arc;
        use std::thread;

        let seq = Arc::new(Sequencer::new());
        let n_threads = 8;
        let ids_per_thread = 1000;

        let handles: Vec<_> = (0..n_threads)
            .map(|_| {
                let seq = Arc::clone(&seq);
                thread::spawn(move || {
                    let mut ids = Vec::with_capacity(ids_per_thread);
                    for _ in 0..ids_per_thread {
                        ids.push(seq.next_txn_id());
                    }
                    ids
                })
            })
            .collect();

        let mut all_ids: Vec<u64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        all_ids.sort();
        all_ids.dedup();
        assert_eq!(all_ids.len(), n_threads * ids_per_thread);
    }
}
