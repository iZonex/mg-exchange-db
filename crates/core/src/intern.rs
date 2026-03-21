//! String interning for deduplicating repeated strings.
//!
//! Common strings like symbol names ("BTCUSD", "ETHUSD") appear millions of
//! times in a time-series database. `StringInterner` deduplicates them so
//! that each unique string is stored exactly once, and all references share
//! the same `Arc<str>`.

use dashmap::DashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Thread-safe string interner backed by `DashMap`.
///
/// Stores each unique string once and returns `Arc<str>` handles for
/// zero-copy sharing across threads.
pub struct StringInterner {
    /// Maps hash -> interned string.
    /// We also store the string to handle hash collisions via linear probing
    /// within the DashMap bucket (DashMap handles this internally).
    map: DashMap<u64, Arc<str>>,
}

impl StringInterner {
    /// Create a new, empty interner.
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
        }
    }

    /// Intern a string.
    ///
    /// If the string has been seen before, returns the existing `Arc<str>`.
    /// Otherwise, allocates a new `Arc<str>` and stores it.
    pub fn intern(&self, s: &str) -> Arc<str> {
        let hash = Self::hash_str(s);

        // Fast path: check if already interned.
        if let Some(existing) = self.map.get(&hash)
            && &**existing == s
        {
            return Arc::clone(&existing);
        }

        // Slow path: insert.
        // Handle hash collisions by using a secondary hash offset.
        let mut key = hash;
        loop {
            let entry = self.map.entry(key).or_insert_with(|| Arc::from(s));
            if &**entry == s {
                return Arc::clone(&entry);
            }
            // Collision with a different string: linear probe.
            key = key.wrapping_add(1);
        }
    }

    /// Get an already-interned string, or `None` if not found.
    pub fn get(&self, s: &str) -> Option<Arc<str>> {
        let hash = Self::hash_str(s);
        let mut key = hash;
        loop {
            match self.map.get(&key) {
                Some(existing) if &**existing == s => return Some(Arc::clone(&existing)),
                Some(_) => {
                    // Collision: linear probe.
                    key = key.wrapping_add(1);
                }
                None => return None,
            }
        }
    }

    /// Number of unique strings stored.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Whether the interner is empty.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Approximate total memory used by the interned strings (bytes).
    ///
    /// This counts the string data only, not the `DashMap` overhead.
    pub fn memory_used(&self) -> usize {
        let mut total = 0;
        for entry in self.map.iter() {
            // Arc<str> stores the string data inline after a header.
            // We approximate as: len of the string + size of Arc header.
            total += entry.value().len() + std::mem::size_of::<usize>() * 2;
        }
        total
    }

    fn hash_str(s: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_deduplicates() {
        let interner = StringInterner::new();

        let a1 = interner.intern("BTCUSD");
        let a2 = interner.intern("BTCUSD");
        let b1 = interner.intern("ETHUSD");

        // Same string should return the same Arc.
        assert!(Arc::ptr_eq(&a1, &a2));
        // Different strings should not share Arc.
        assert!(!Arc::ptr_eq(&a1, &b1));

        assert_eq!(interner.len(), 2);
    }

    #[test]
    fn intern_get() {
        let interner = StringInterner::new();
        assert!(interner.get("BTCUSD").is_none());

        interner.intern("BTCUSD");
        let got = interner.get("BTCUSD");
        assert!(got.is_some());
        assert_eq!(&*got.unwrap(), "BTCUSD");

        assert!(interner.get("SOLUSD").is_none());
    }

    #[test]
    fn intern_many_strings() {
        let interner = StringInterner::new();
        let mut arcs = Vec::new();

        for i in 0..1000 {
            let s = format!("SYM{i:04}");
            arcs.push(interner.intern(&s));
        }
        assert_eq!(interner.len(), 1000);

        // Re-intern the same strings.
        for (i, original) in arcs.iter().enumerate() {
            let s = format!("SYM{i:04}");
            let arc = interner.intern(&s);
            assert!(Arc::ptr_eq(&arc, original));
        }
        // No new entries should have been added.
        assert_eq!(interner.len(), 1000);
    }

    #[test]
    fn memory_used_grows() {
        let interner = StringInterner::new();
        let before = interner.memory_used();
        assert_eq!(before, 0);

        interner.intern("hello");
        interner.intern("world");
        let after = interner.memory_used();
        assert!(after > 0);
    }

    #[test]
    fn is_empty() {
        let interner = StringInterner::new();
        assert!(interner.is_empty());
        interner.intern("x");
        assert!(!interner.is_empty());
    }

    #[test]
    fn thread_safety() {
        use std::sync::Arc as StdArc;
        let interner = StdArc::new(StringInterner::new());
        let mut handles = Vec::new();

        for t in 0..4 {
            let interner = StdArc::clone(&interner);
            handles.push(std::thread::spawn(move || {
                for i in 0..250 {
                    let s = format!("SYM{}", i + t * 250);
                    interner.intern(&s);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(interner.len(), 1000);
    }
}
