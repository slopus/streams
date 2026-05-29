//! Time abstraction. All recency/TTL/priority logic in the engine and
//! scheduler MUST read time through a [`Clock`] so phase-3 tests can inject a
//! [`TestClock`] and never depend on wall-clock sleeps.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// A source of the current time in integer milliseconds since the Unix epoch.
pub trait Clock: Send + Sync + 'static {
    /// Milliseconds since the Unix epoch.
    fn now_ms(&self) -> i64;
}

/// Real wall-clock time via [`SystemTime`].
#[derive(Debug, Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

/// A manually-advanceable clock for tests. Cheap to clone (shares the atomic).
#[derive(Debug, Clone)]
pub struct TestClock {
    now: Arc<AtomicI64>,
}

impl TestClock {
    /// Create a test clock starting at the given epoch-ms.
    pub fn new(start_ms: i64) -> Self {
        TestClock {
            now: Arc::new(AtomicI64::new(start_ms)),
        }
    }

    /// Advance the clock by `delta_ms` milliseconds.
    pub fn advance(&self, delta_ms: i64) {
        self.now.fetch_add(delta_ms, Ordering::SeqCst);
    }

    /// Set the clock to an absolute epoch-ms value.
    pub fn set(&self, ms: i64) {
        self.now.store(ms, Ordering::SeqCst);
    }
}

impl Default for TestClock {
    fn default() -> Self {
        TestClock::new(0)
    }
}

impl Clock for TestClock {
    fn now_ms(&self) -> i64 {
        self.now.load(Ordering::SeqCst)
    }
}

/// A type-erased, shareable clock handle.
pub type SharedClock = Arc<dyn Clock>;
