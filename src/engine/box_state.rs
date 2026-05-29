//! Per-box in-memory state: the base+offset [`BoxIndex`], the watermark
//! atomics, retained payload bytes, the tag-delete filter set, recency clocks,
//! and the `Notify` used to wake SSE/diff long-pollers (ARCHITECTURE §1).
//!
//! Phase 2 stores payload bytes directly on the heap (`StoredRecord`); phase 4
//! re-points `RecordLoc` at mmap'd segments. The serving/indexing logic here is
//! written once and reused.

use crate::engine::eviction::Floors;
use crate::engine::filters::FilterSet;
use crate::types::BoxConfig;
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use tokio::sync::Notify;

/// A remembered idempotent write: the seqs it assigned and when it landed, so a
/// retry within `idempotency_window_ms` returns the original seqs (API §0.8).
#[derive(Debug, Clone)]
pub struct DedupeEntry {
    pub seqs: Vec<u64>,
    pub head_seq: u64,
    pub created_ms: i64,
}

/// One stored record. In phase 2 the payload lives inline on the heap; the
/// `ts`/`tag`/`node` are kept for the read pipeline and TTL math.
#[derive(Debug, Clone)]
pub struct StoredRecord {
    pub ts: i64,
    pub node: Option<String>,
    pub tag: Option<String>,
    pub data: Value,
    pub meta: Option<Value>,
    /// Accounted payload size (`data` + `meta` + framing estimate).
    pub bytes: u64,
}

/// The seq→record index: a contiguous deque offset by `base_seq`
/// (ARCHITECTURE §1.1). `index i` corresponds to `seq = base_seq + i`.
#[derive(Debug, Default)]
pub struct BoxIndex {
    /// Seq of `records[0]`; the earliest physically present seq.
    pub base_seq: u64,
    pub records: VecDeque<StoredRecord>,
}

impl BoxIndex {
    pub fn new(base_seq: u64) -> Self {
        BoxIndex {
            base_seq,
            records: VecDeque::new(),
        }
    }

    /// Lookup a record by seq, if physically present.
    pub fn get(&self, seq: u64) -> Option<&StoredRecord> {
        if seq < self.base_seq {
            return None;
        }
        self.records.get((seq - self.base_seq) as usize)
    }

    /// Drop the oldest `n` records, advancing `base_seq`.
    pub fn drain_front(&mut self, n: usize) {
        let n = n.min(self.records.len());
        self.records.drain(..n);
        self.base_seq += n as u64;
    }
}

/// The full in-memory state of one box.
pub struct BoxState {
    /// The box name (also the identity).
    pub name: String,
    /// Live config (read-mostly; mutated under `index` write lock on `PUT`).
    pub config: RwLock<BoxConfig>,
    /// The seq→record index.
    pub index: RwLock<BoxIndex>,
    /// Tag-delete rule set (DESIGN §7).
    pub filters: RwLock<FilterSet>,
    /// Eviction/expiry floors driving `earliest_seq`.
    pub floors: RwLock<Floors>,
    /// `(idempotency_key → assigned seqs)` dedupe state (API §0.8). Entries are
    /// reclaimed lazily once older than the box's `idempotency_window_ms`.
    pub dedupe: RwLock<HashMap<String, DedupeEntry>>,

    /// Highest assigned seq (`0` for a fresh empty box).
    pub head_seq: AtomicU64,
    /// First seq this box instance will ever assign (`seq_base`, default 1).
    pub seq_base: u64,
    /// Bumped on create; detects delete+recreate (DESIGN §5.5).
    pub epoch: AtomicU64,
    /// Retained payload bytes (approximate under lazy eviction).
    pub bytes_retained: AtomicU64,

    /// Recency clocks (ms; `0`/`MIN` sentinel for never).
    pub last_write_ms: AtomicI64,
    pub last_read_ms: AtomicI64,
    /// `last_consumed_at` for auto-priority (DESIGN §3).
    pub last_consumed_ms: AtomicI64,

    /// Wakes SSE/diff long-pollers on append (ARCHITECTURE §1.2).
    pub notify: Notify,
}

/// Sentinel for a recency clock that has never fired.
pub const TS_NEVER: i64 = i64::MIN;

impl BoxState {
    /// Create a fresh box with the given config and epoch.
    pub fn new(name: String, config: BoxConfig, seq_base: u64, epoch: u64) -> Self {
        BoxState {
            name,
            config: RwLock::new(config),
            index: RwLock::new(BoxIndex::new(seq_base)),
            filters: RwLock::new(FilterSet::new()),
            floors: RwLock::new(Floors::default()),
            dedupe: RwLock::new(HashMap::new()),
            head_seq: AtomicU64::new(seq_base.saturating_sub(1)),
            seq_base,
            epoch: AtomicU64::new(epoch),
            bytes_retained: AtomicU64::new(0),
            last_write_ms: AtomicI64::new(TS_NEVER),
            last_read_ms: AtomicI64::new(TS_NEVER),
            last_consumed_ms: AtomicI64::new(TS_NEVER),
            notify: Notify::new(),
        }
    }

    pub fn head_seq(&self) -> u64 {
        self.head_seq.load(Ordering::Acquire)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    /// Next seq an append will receive (`head_seq + 1`).
    pub fn next_seq(&self) -> u64 {
        self.head_seq().saturating_add(1)
    }

    /// Logical earliest retained, deliverable seq (DESIGN §5.1). For an empty
    /// box this is `head_seq + 1`.
    pub fn earliest_seq(&self) -> u64 {
        let floors = self.floors.read();
        floors.earliest_seq(self.seq_base, self.head_seq())
    }

    /// Read a recency clock, mapping the sentinel to `None`.
    pub fn read_ts(value: &AtomicI64) -> Option<i64> {
        match value.load(Ordering::Relaxed) {
            TS_NEVER => None,
            v => Some(v),
        }
    }

    /// Append `records` with seqs assigned in order. Returns the assigned seqs.
    /// Caller has already validated and (for `discard:"reject"`) admitted.
    ///
    /// Assigns contiguous seqs starting at `head_seq + 1`, pushes records into
    /// the index, bumps `head_seq`, accounts retained bytes, sets the write
    /// recency clock, and wakes any SSE/diff long-pollers via [`Notify`].
    pub fn append(&self, records: Vec<StoredRecord>, now_ms: i64) -> Vec<u64> {
        if records.is_empty() {
            return Vec::new();
        }
        let n = records.len() as u64;
        let mut index = self.index.write();

        let start = self.head_seq().saturating_add(1);
        let mut added_bytes: u64 = 0;
        for rec in records {
            added_bytes = added_bytes.saturating_add(rec.bytes);
            index.records.push_back(rec);
        }
        // Publish the new head_seq after the records are in the index so a
        // concurrent reader that observes the higher head also finds the slots.
        let new_head = start + n - 1;
        self.head_seq.store(new_head, Ordering::Release);
        drop(index);

        self.bytes_retained.fetch_add(added_bytes, Ordering::Relaxed);
        self.last_write_ms.store(now_ms, Ordering::Relaxed);
        // Wake long-pollers (diff `wait_ms`) and SSE streams.
        self.notify.notify_waiters();

        (start..=new_head).collect()
    }

    /// Recompute eviction/expiry floors against caps + TTL at `now_ms`, drain
    /// the index front, and update retained bytes (DESIGN §5.2/§5.3).
    ///
    /// Idempotent: safe to call on both the write and read paths. After it runs,
    /// the physically-present records equal the logically-retained set.
    pub fn enforce_retention(&self, now_ms: i64) {
        let config = self.config.read();
        let ttl_ms = config.ttl_ms;
        let cap_records = config.cap_records;
        let cap_bytes = config.cap_bytes;
        drop(config);

        let head = self.head_seq();
        if head == 0 {
            return; // empty box, nothing retained.
        }

        let mut index = self.index.write();
        let mut floors = self.floors.write();

        // --- TTL: advance expiry_floor past every expired record. -----------
        // `$ts` is non-decreasing in seq, so all seqs <= X expired is a prefix
        // predicate; scan the index front (bounded by the number of newly
        // expired records, amortized O(1) under steady state).
        if ttl_ms > 0 {
            let ttl = ttl_ms as i64;
            let base = index.base_seq;
            let mut expired_upto = floors.expiry_floor;
            for (i, rec) in index.records.iter().enumerate() {
                if now_ms.saturating_sub(rec.ts) > ttl {
                    expired_upto = base + i as u64;
                } else {
                    // First non-expired record; the rest are younger still.
                    break;
                }
            }
            if expired_upto > floors.expiry_floor {
                floors.expiry_floor = expired_upto;
            }
        }

        // --- Cap (records): keep at most cap_records retained. --------------
        if cap_records > 0 && head > cap_records {
            let want_floor = head - cap_records; // highest seq to evict.
            if want_floor > floors.evict_floor {
                floors.evict_floor = want_floor;
            }
        }

        // --- Cap (bytes): evict oldest physically-present records until the
        // retained byte total is within cap_bytes. Walk the front, summing the
        // bytes that must drop. -------------------------------------------
        if cap_bytes > 0 {
            let retained_bytes = self.bytes_retained.load(Ordering::Relaxed);
            if retained_bytes > cap_bytes {
                let mut over = retained_bytes - cap_bytes;
                let base = index.base_seq;
                // Only consider records that aren't already below the floor.
                let current_floor = floors.evict_floor.max(floors.expiry_floor);
                let mut evict_to = floors.evict_floor;
                for (i, rec) in index.records.iter().enumerate() {
                    let seq = base + i as u64;
                    if seq <= current_floor {
                        continue; // already logically gone.
                    }
                    if over == 0 {
                        break;
                    }
                    over = over.saturating_sub(rec.bytes);
                    evict_to = seq;
                    if over == 0 {
                        break;
                    }
                }
                if evict_to > floors.evict_floor {
                    floors.evict_floor = evict_to;
                }
            }
        }

        // --- Drain physically-present records below the logical floor. ------
        let earliest = floors.earliest_seq(self.seq_base, head);
        let base = index.base_seq;
        if earliest > base {
            let drop_n = (earliest - base) as usize;
            let drop_n = drop_n.min(index.records.len());
            let mut freed: u64 = 0;
            for rec in index.records.iter().take(drop_n) {
                freed = freed.saturating_add(rec.bytes);
            }
            index.drain_front(drop_n);
            if freed > 0 {
                // saturating: bytes_retained is the authoritative retained sum.
                let prev = self.bytes_retained.load(Ordering::Relaxed);
                self.bytes_retained
                    .store(prev.saturating_sub(freed), Ordering::Relaxed);
            }
        }
    }

    /// Current retained record count (logical floor; DESIGN §5.6).
    pub fn count(&self) -> u64 {
        let head = self.head_seq();
        let earliest = self.earliest_seq();
        if head == 0 || earliest > head {
            0
        } else {
            head - earliest + 1
        }
    }

    /// Current retained payload bytes (approximate under lazy eviction).
    pub fn bytes(&self) -> u64 {
        self.bytes_retained.load(Ordering::Relaxed)
    }
}
