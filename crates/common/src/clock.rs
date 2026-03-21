use crate::types::Timestamp;

/// High-resolution clock abstraction (mockable for tests).
pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
}

/// System clock using std::time.
#[derive(Debug, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    #[inline]
    fn now(&self) -> Timestamp {
        Timestamp::now()
    }
}
