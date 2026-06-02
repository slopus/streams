//! Simplified priority scheduler (DESIGN §3, ARCHITECTURE §7).
//!
//! Phase 2 carries the *shape* of the scheduler — effective-priority
//! computation (manual clamp or recency-based auto) and a banded ready-set —
//! but with nothing to fsync and no real CPU throttling. The effective-priority
//! math is exercised directly by `GET /v0/topics/:topic`'s `effective_priority`.

use crate::clock::SharedClock;
use crate::config::{
    AGE_CAP_MS, AGE_RATE_PER_MS, AUTO_FLOOR_MS, AUTO_MAX, HALF_LIFE_MS, PRIORITY_MAX, PRIORITY_MIN,
};
use parking_lot::Mutex;
use std::collections::VecDeque;

/// DWRR bands keyed by effective priority (ARCHITECTURE §7.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Band {
    /// `>= 750`, weight 8.
    B4,
    /// `500..=749`, weight 4.
    B3,
    /// `250..=499`, weight 2.
    B2,
    /// `0..=249`, weight 1.
    B1,
    /// `< 0`, weight 1 (explicitly deprioritized).
    B0,
}

impl Band {
    /// Classify an effective priority into a band.
    pub fn of(eff_priority: i64) -> Band {
        if eff_priority >= 750 {
            Band::B4
        } else if eff_priority >= 500 {
            Band::B3
        } else if eff_priority >= 250 {
            Band::B2
        } else if eff_priority >= 0 {
            Band::B1
        } else {
            Band::B0
        }
    }

    /// DWRR weight for this band.
    pub fn weight(self) -> u32 {
        match self {
            Band::B4 => 8,
            Band::B3 => 4,
            Band::B2 => 2,
            Band::B1 | Band::B0 => 1,
        }
    }

    /// The ready-set queue for this band.
    fn queue_mut(self, ready: &mut ReadySet) -> &mut VecDeque<String> {
        match self {
            Band::B4 => &mut ready.b4,
            Band::B3 => &mut ready.b3,
            Band::B2 => &mut ready.b2,
            Band::B1 => &mut ready.b1,
            Band::B0 => &mut ready.b0,
        }
    }
}

/// The simplified scheduler. Holds the banded ready-set of dirty topics; phase 2
/// drains synchronously, so this is mostly priority bookkeeping.
pub struct Scheduler {
    clock: SharedClock,
    /// Banded ready-set of dirty topic names (B4..B0). FIFO within a band.
    ready: Mutex<ReadySet>,
}

#[derive(Default)]
struct ReadySet {
    b4: VecDeque<String>,
    b3: VecDeque<String>,
    b2: VecDeque<String>,
    b1: VecDeque<String>,
    b0: VecDeque<String>,
}

impl Scheduler {
    pub fn new(clock: SharedClock) -> Self {
        Scheduler {
            clock,
            ready: Mutex::new(ReadySet::default()),
        }
    }

    /// Compute the effective priority for a topic (DESIGN §3.1). `manual` is the
    /// configured `priority` (`None` ⇒ auto-only); `last_consumed_ms`/
    /// `enqueued_ms` drive the recency and aging terms.
    pub fn effective_priority(
        &self,
        manual: Option<i32>,
        auto_priority: bool,
        last_consumed_ms: Option<i64>,
        enqueued_ms: Option<i64>,
    ) -> i64 {
        let now = self.clock.now_ms();
        effective_priority_at(now, manual, auto_priority, last_consumed_ms, enqueued_ms)
    }

    /// Mark a topic dirty at the given effective priority (insert into its band
    /// if not already present). Phase 2 drains inline so this is advisory.
    pub fn mark_dirty(&self, topic_name: &str, eff_priority: i64) {
        let mut ready = self.ready.lock();
        let band = Band::of(eff_priority);
        let q = band.queue_mut(&mut ready);
        if !q.iter().any(|b| b == topic_name) {
            q.push_back(topic_name.to_string());
        }
    }

    /// Mark a topic dirty on the WRITE HOT PATH without taking the global ready-set
    /// mutex once the topic is already dirty (codex P1). `already_dirty` is the topic's
    /// `sched_dirty` atomic: a `compare_exchange` flips it `false → true` lock-free,
    /// and only the FIRST transition takes the mutex to enqueue the name. Since the
    /// ready-set is drained only by the (not-yet-wired) phase-4 governor — which
    /// clears the flag via [`Scheduler::drain_order_clearing`] — a hot topic stays
    /// dirty and every subsequent append on it is a single relaxed atomic load +
    /// failed CAS, removing the per-write global lock that capped WAL-shard scaling.
    pub fn mark_dirty_fast(
        &self,
        topic_name: &str,
        eff_priority: i64,
        already_dirty: &std::sync::atomic::AtomicBool,
    ) {
        use std::sync::atomic::Ordering;
        // Lock-free fast path: already dirty ⇒ nothing to do (and no lock).
        if already_dirty.load(Ordering::Relaxed) {
            return;
        }
        // Claim the first transition; a lost race means another writer enqueued it.
        if already_dirty
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        self.mark_dirty(topic_name, eff_priority);
    }

    /// As [`Scheduler::drain_order`], but also clears each drained topic's
    /// `sched_dirty` flag (looked up via `clear`) so a future write re-enqueues it.
    /// The phase-4 governor will use this; provided now so the `sched_dirty`
    /// fast-path stays correct whenever a drainer is wired in.
    pub fn drain_order_clearing<F>(&self, clear: F) -> Vec<String>
    where
        F: Fn(&str),
    {
        let out = self.drain_order();
        for name in &out {
            clear(name);
        }
        out
    }

    /// Bump a topic's recency clock to `now`, so its auto-priority term resets to
    /// `AUTO_MAX` (a "consume" event: GET state / diff / SSE attach or delivery;
    /// DESIGN §3.1). Centralizes the recency write behind the scheduler so the
    /// phase-4 governor can hook it.
    pub fn touch(&self, last_consumed_ms: &std::sync::atomic::AtomicI64) {
        last_consumed_ms.store(self.clock.now_ms(), std::sync::atomic::Ordering::Relaxed);
    }

    /// Drain the ready-set in strict priority-band order (B4→B0), FIFO within a
    /// band. Phase 2 drains inline (delivery is synchronous-on-append), so this
    /// is the clean ordering abstraction the phase-4 DWRR scheduler replaces.
    /// Returns the dirty topic names in the order they would be serviced.
    pub fn drain_order(&self) -> Vec<String> {
        let mut ready = self.ready.lock();
        let mut out = Vec::new();
        for band in [Band::B4, Band::B3, Band::B2, Band::B1, Band::B0] {
            let q = band.queue_mut(&mut ready);
            while let Some(name) = q.pop_front() {
                out.push(name);
            }
        }
        out
    }
}

/// Pure effective-priority computation at an explicit `now` (testable).
pub fn effective_priority_at(
    now_ms: i64,
    manual: Option<i32>,
    auto_priority: bool,
    last_consumed_ms: Option<i64>,
    enqueued_ms: Option<i64>,
) -> i64 {
    // W_manual = W_auto = W_age = 1.0.
    let manual_term = manual.unwrap_or(0).clamp(PRIORITY_MIN, PRIORITY_MAX) as f64;

    let auto_term = if manual.is_none() && auto_priority {
        match last_consumed_ms {
            Some(last) => {
                let idle = (now_ms - last).max(0) as u64;
                if idle >= AUTO_FLOOR_MS {
                    0.0
                } else {
                    AUTO_MAX * 2f64.powf(-(idle as f64) / HALF_LIFE_MS)
                }
            }
            None => 0.0,
        }
    } else {
        0.0
    };

    let age_term = match enqueued_ms {
        Some(enq) => {
            let waited = (now_ms - enq).max(0).min(AGE_CAP_MS as i64) as f64;
            AGE_RATE_PER_MS * waited
        }
        None => 0.0,
    };

    (manual_term + auto_term + age_term).round() as i64
}

// ---------------------------------------------------------------------------
// Unit tests (TestClock; no sleeps).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::{SharedClock, TestClock};
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Arc;

    fn sched_with_clock(now: i64) -> (Scheduler, TestClock) {
        let clock = TestClock::new(now);
        let shared: SharedClock = Arc::new(clock.clone());
        (Scheduler::new(shared), clock)
    }

    #[test]
    fn manual_priority_overrides_auto() {
        // A topic with a manual priority ignores the auto-recency term entirely,
        // even if just consumed (DESIGN §3.1: config priority ⇒ auto-only off).
        let now = 1_000_000;
        // Manual +750 with a fresh consume: result is exactly the clamped manual
        // value, no +500 auto bonus stacked on top.
        let p = effective_priority_at(now, Some(750), true, Some(now), None);
        assert_eq!(p, 750);

        // Manual clamps to the documented [-1000, 1000] range.
        assert_eq!(
            effective_priority_at(now, Some(5000), true, Some(now), None),
            1000
        );
        assert_eq!(
            effective_priority_at(now, Some(-5000), true, Some(now), None),
            -1000
        );

        // Auto-only (manual=None): a fresh consume yields AUTO_MAX (500).
        assert_eq!(effective_priority_at(now, None, true, Some(now), None), 500);
    }

    #[test]
    fn auto_recency_decays_with_time() {
        // AUTO_MAX=500 halves every HALF_LIFE_MS=30s; the DESIGN §3.1 table:
        // 0s→500, 30s→250, 60s→125, >=300s→0.
        let last = 1_000_000;
        let at = |dt: i64| effective_priority_at(last + dt, None, true, Some(last), None);
        assert_eq!(at(0), 500);
        assert_eq!(at(30_000), 250);
        assert_eq!(at(60_000), 125);
        // After AUTO_FLOOR_MS (5 min) untouched the auto term is forced to 0.
        assert_eq!(at(300_000), 0);
        assert_eq!(at(600_000), 0);
    }

    #[test]
    fn touch_resets_recency_via_clock() {
        let (sched, clock) = sched_with_clock(1_000_000);
        let last = AtomicI64::new(crate::engine::topic_state::TS_NEVER);
        // Before any touch, an untouched topic has no auto bonus.
        let lc = match last.load(Ordering::Relaxed) {
            crate::engine::topic_state::TS_NEVER => None,
            v => Some(v),
        };
        assert_eq!(sched.effective_priority(None, true, lc, None), 0);

        // Touch bumps the recency clock to now → fresh AUTO_MAX.
        sched.touch(&last);
        let lc = Some(last.load(Ordering::Relaxed));
        assert_eq!(sched.effective_priority(None, true, lc, None), 500);

        // Advance 30s; the same topic decays to 250.
        clock.advance(30_000);
        assert_eq!(sched.effective_priority(None, true, lc, None), 250);
    }

    #[test]
    fn banding_and_drain_order_is_priority_then_fifo() {
        assert_eq!(Band::of(800), Band::B4);
        assert_eq!(Band::of(600), Band::B3);
        assert_eq!(Band::of(300), Band::B2);
        assert_eq!(Band::of(100), Band::B1);
        assert_eq!(Band::of(-5), Band::B0);

        let (sched, _clock) = sched_with_clock(0);
        // Insert across bands out of order; drain must come back B4..B0,
        // FIFO within a band.
        sched.mark_dirty("low", 10); // B1
        sched.mark_dirty("hi", 800); // B4
        sched.mark_dirty("mid", 300); // B2
        sched.mark_dirty("hi2", 900); // B4
        sched.mark_dirty("neg", -50); // B0
                                      // Dedupe: re-marking an already-dirty topic is a no-op.
        sched.mark_dirty("hi", 800);

        let order = sched.drain_order();
        assert_eq!(order, vec!["hi", "hi2", "mid", "low", "neg"]);
        // Drained empties the set.
        assert!(sched.drain_order().is_empty());
    }

    /// The lock-free `mark_dirty_fast` enqueues a topic exactly once (the first
    /// transition of its `sched_dirty` flag), and `drain_order_clearing` clears the
    /// flag so a later write re-enqueues it (codex P1 hot-path lock removal).
    #[test]
    fn mark_dirty_fast_enqueues_once_then_reenqueues_after_clear() {
        use std::sync::atomic::AtomicBool;
        let (sched, _clock) = sched_with_clock(0);
        let dirty = AtomicBool::new(false);

        // First mark enqueues; repeated marks while still dirty are no-ops (and take
        // no lock).
        sched.mark_dirty_fast("hot", 800, &dirty);
        sched.mark_dirty_fast("hot", 800, &dirty);
        sched.mark_dirty_fast("hot", 800, &dirty);
        assert!(dirty.load(Ordering::Relaxed), "flag set after first mark");

        // Drain (clearing the flag via the supplied closure) returns it exactly once.
        let order = sched.drain_order_clearing(|name| {
            assert_eq!(name, "hot");
            dirty.store(false, Ordering::Relaxed);
        });
        assert_eq!(order, vec!["hot"], "enqueued exactly once despite 3 marks");
        assert!(!dirty.load(Ordering::Relaxed), "flag cleared on drain");

        // After the clear a fresh write re-enqueues it.
        sched.mark_dirty_fast("hot", 800, &dirty);
        assert_eq!(sched.drain_order(), vec!["hot"], "re-enqueued after clear");
    }
}
