use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

/// Single-Producer Single-Consumer lock-free ring buffer.
/// Optimized for low-latency message passing between threads.
pub struct SpscRingBuffer<T> {
    buffer: Box<[UnsafeCell<Option<T>>]>,
    capacity: usize,
    mask: usize,
    head: CachePadded<AtomicU64>,
    tail: CachePadded<AtomicU64>,
}

/// Cache-line padded wrapper to prevent false sharing.
#[repr(align(128))]
struct CachePadded<T>(T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.0
    }
}

// SAFETY: The SPSC design ensures only one thread writes (producer) and
// one thread reads (consumer), so Send+Sync is safe when T: Send.
unsafe impl<T: Send> Send for SpscRingBuffer<T> {}
unsafe impl<T: Send> Sync for SpscRingBuffer<T> {}

impl<T> SpscRingBuffer<T> {
    /// Create a new ring buffer. Capacity is rounded up to next power of 2.
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.next_power_of_two();
        let buffer: Vec<UnsafeCell<Option<T>>> =
            (0..capacity).map(|_| UnsafeCell::new(None)).collect();

        Self {
            buffer: buffer.into_boxed_slice(),
            capacity,
            mask: capacity - 1,
            head: CachePadded(AtomicU64::new(0)),
            tail: CachePadded(AtomicU64::new(0)),
        }
    }

    /// Try to push a value. Returns Err(value) if full.
    pub fn try_push(&self, value: T) -> Result<(), T> {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        if tail - head >= self.capacity as u64 {
            return Err(value);
        }

        let slot = (tail as usize) & self.mask;
        // SAFETY: Only the producer thread calls try_push, and we've verified
        // the slot is available (not yet consumed).
        unsafe {
            *self.buffer[slot].get() = Some(value);
        }
        self.tail.store(tail + 1, Ordering::Release);
        Ok(())
    }

    /// Try to pop a value. Returns None if empty.
    pub fn try_pop(&self) -> Option<T> {
        let head = self.head.load(Ordering::Relaxed);
        let tail = self.tail.load(Ordering::Acquire);

        if head >= tail {
            return None;
        }

        let slot = (head as usize) & self.mask;
        // SAFETY: Only the consumer thread calls try_pop, and we've verified
        // the slot contains data.
        let value = unsafe { (*self.buffer[slot].get()).take() };
        self.head.store(head + 1, Ordering::Release);
        value
    }

    /// Number of elements currently in the buffer.
    pub fn len(&self) -> usize {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);
        (tail - head) as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_pop_basic() {
        let rb = SpscRingBuffer::new(4);
        assert!(rb.is_empty());

        rb.try_push(1).unwrap();
        rb.try_push(2).unwrap();
        assert_eq!(rb.len(), 2);

        assert_eq!(rb.try_pop(), Some(1));
        assert_eq!(rb.try_pop(), Some(2));
        assert_eq!(rb.try_pop(), None);
    }

    #[test]
    fn full_buffer() {
        let rb = SpscRingBuffer::new(2);
        rb.try_push(1).unwrap();
        rb.try_push(2).unwrap();
        assert_eq!(rb.try_push(3), Err(3));
    }

    #[test]
    fn wrap_around() {
        let rb = SpscRingBuffer::new(2);
        for i in 0..10 {
            rb.try_push(i).unwrap();
            assert_eq!(rb.try_pop(), Some(i));
        }
    }
}
