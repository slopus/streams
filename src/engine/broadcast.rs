//! Zero-copy SSE broadcast fan-out (ARCHITECTURE §8.4, Phase-5B Stage 2).
//!
//! When one topic has many SSE watchers, each watcher used to independently
//! re-serialize every record it delivered (`serde_json` over the same
//! [`RecordOut`] N times). For a broadcast (1 writer → N watchers on one topic)
//! that is N× the serialization cost on the hot read path.
//!
//! This module hands every watcher the **same ref-counted serialized frame**.
//! Each record's `record`-frame JSON body is serialized **once** into an
//! `Arc<RawValue>` and cached in a small per-topic ring keyed by `(seq, variant)`,
//! then shared (one `Arc` clone — a refcount bump, no copy) to all watchers. The
//! per-connection envelope (`{topic, records, from_seq, to_seq, head_seq}`) and the
//! composite `id:` cursor are still assembled per connection (they depend on the
//! session's cursor map), but the expensive per-record serialization is paid
//! once and amortized across the whole fan-out.
//!
//! The cache is **bounded** (a fixed-capacity ring of the most recently
//! delivered seqs) so a slow or lagging watcher can never grow it without bound;
//! a miss simply re-serializes (correct, just not shared). Eviction of an old
//! seq from the ring drops its `Arc`s — the bytes are freed once the last watcher
//! holding a clone finishes writing it. This is purely a read-path accelerator:
//! it changes no wire output and holds no lock across a socket write.

use crate::types::RecordOut;
use parking_lot::Mutex;
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

/// How many recent seqs to keep serialized per topic. Watchers tail near head, so
/// a small ring captures the shared fan-out window; lagging watchers re-serialize
/// (a miss) rather than pinning unbounded memory.
const RING_CAP: usize = 1024;

/// The three projection flags that change a record frame's bytes
/// (`include_data`, `include_tags`, `include_meta`), packed into a 0..8 index so
/// each `(seq, variant)` is cached independently. Watchers sharing a projection
/// (the common broadcast case) all hit the same slot.
#[derive(Clone, Copy)]
pub struct FrameVariant {
    include_data: bool,
    bits: u8,
}

impl FrameVariant {
    pub fn new(include_data: bool, include_tags: bool, include_meta: bool) -> Self {
        FrameVariant {
            include_data,
            bits: (include_data as u8) | ((include_tags as u8) << 1) | ((include_meta as u8) << 2),
        }
    }
    fn idx(self) -> usize {
        self.bits as usize
    }
}

/// Number of distinct projection variants (2^3).
const N_VARIANTS: usize = 8;

type CachedVariants = [Option<Arc<RawValue>>; N_VARIANTS];

#[derive(Default)]
struct CacheState {
    /// In insertion/seq order for bounded eviction.
    order: VecDeque<u64>,
    /// Per-seq lazily-filled projection variants. Hash lookup avoids a linear ring
    /// scan for every watcher×record cache hit in high-fanout media streams.
    frames: HashMap<u64, CachedVariants>,
}

/// Per-topic bounded ring of recently-serialized record frames. Cheap to clone the
/// `Arc<BroadcastCache>` onto each topic; the inner ring is mutex-guarded and only
/// touched on the SSE delivery path (never on the write path, so topics with zero
/// watchers pay nothing).
#[derive(Default)]
pub struct BroadcastCache {
    state: Mutex<CacheState>,
}

impl BroadcastCache {
    pub fn new() -> Self {
        BroadcastCache {
            state: Mutex::new(CacheState::default()),
        }
    }

    /// Get the shared serialized frame for `rec` at `seq` under `variant`,
    /// serializing-and-caching once on a miss. Returns an `Arc` clone (a refcount
    /// bump) so N watchers share one buffer.
    pub fn frame(&self, seq: u64, rec: &RecordOut, variant: FrameVariant) -> Arc<RawValue> {
        if let Some(arc) = self.get(seq, variant) {
            return arc;
        }

        // Miss: serialize once, then publish into the ring.
        let arc: Arc<RawValue> = serialize_frame(rec, variant.include_data);
        self.insert_serialized(seq, variant, arc)
    }

    /// Return a cached shared frame without serializing. Used by the SSE fast path so
    /// cache hits avoid cloning the record payload into a temporary `RecordOut`.
    pub fn get(&self, seq: u64, variant: FrameVariant) -> Option<Arc<RawValue>> {
        let v = variant.idx();
        self.state
            .lock()
            .frames
            .get(&seq)
            .and_then(|variants| variants[v].as_ref().cloned())
    }

    /// Return cached shared frames for a batch of seqs using one cache lock. This
    /// is the high-fanout hot path: a WebSocket/SSE diff commonly returns tens to
    /// hundreds of records, and taking one mutex per record dominates under many
    /// watchers.
    pub fn get_many(&self, seqs: &[u64], variant: FrameVariant) -> Vec<Option<Arc<RawValue>>> {
        let v = variant.idx();
        let state = self.state.lock();
        seqs.iter()
            .map(|seq| {
                state
                    .frames
                    .get(seq)
                    .and_then(|variants| variants[v].as_ref().cloned())
            })
            .collect()
    }

    /// Insert an already serialized frame, racing safely with another watcher that
    /// may have filled the same `(seq, variant)` while the caller serialized.
    pub fn insert_serialized(
        &self,
        seq: u64,
        variant: FrameVariant,
        arc: Arc<RawValue>,
    ) -> Arc<RawValue> {
        let v = variant.idx();
        let mut state = self.state.lock();
        // Re-check: another watcher may have filled it while we serialized.
        if let Some(variants) = state.frames.get_mut(&seq) {
            if let Some(existing) = &variants[v] {
                return existing.clone();
            }
            variants[v] = Some(arc.clone());
            return arc;
        }

        // New seq: keep the eviction order seq-sorted in the common tailing case.
        let mut variants: CachedVariants = Default::default();
        variants[v] = Some(arc.clone());
        if state.order.back().map(|last| *last < seq).unwrap_or(true) {
            state.order.push_back(seq);
        } else {
            let pos = state.order.partition_point(|existing| *existing < seq);
            state.order.insert(pos, seq);
        }
        state.frames.insert(seq, variants);
        while state.order.len() > RING_CAP {
            if let Some(old) = state.order.pop_front() {
                state.frames.remove(&old);
            }
        }
        arc
    }
}

/// Serialize one record frame body to a shared `Arc<RawValue>`, **byte-identical**
/// to [`record_frame`](crate::http::watch::record_frame) (which the watch loop
/// used per-connection). It builds the same sorted `serde_json::Map`
/// (`$seq,$ts,$node,$tag,data,meta`, sorted on serialization) and serializes it
/// once. `rec` is already projected for `include_tags`/`include_meta` by
/// `record_out`; `include_data` gates the `data` field here, matching the
/// original.
fn serialize_frame(rec: &RecordOut, include_data: bool) -> Arc<RawValue> {
    let val = crate::http::watch::record_frame(rec, include_data);
    // `to_string` then `from_string` is the supported path to a `RawValue`
    // (its serializer round-trips through the textual form). Errors are
    // impossible for a well-formed JSON object; fall back to `null` defensively.
    let s = serde_json::to_string(&val).unwrap_or_else(|_| "null".to_string());
    let boxed: Box<RawValue> = RawValue::from_string(s)
        .unwrap_or_else(|_| RawValue::from_string("null".to_string()).expect("null is valid json"));
    Arc::from(boxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rec(seq: u64) -> RecordOut {
        RecordOut {
            seq,
            ts: 1234,
            node: Some("n1".to_string()),
            tag: Some("t".to_string()),
            type_: None,
            data: json!({"k": "v"}),
            meta: Some(json!({"m": 1})),
        }
    }

    #[test]
    fn shared_frame_is_byte_identical_to_record_frame() {
        let cache = BroadcastCache::new();
        let r = rec(7);
        let variant = FrameVariant::new(true, true, true);
        let shared = cache.frame(7, &r, variant);
        // Must match the per-connection path's bytes exactly (sorted keys).
        let expected = crate::http::watch::record_frame(&r, true).to_string();
        assert_eq!(shared.get(), expected);
    }

    #[test]
    fn second_call_returns_same_shared_arc() {
        let cache = BroadcastCache::new();
        let r = rec(3);
        let variant = FrameVariant::new(true, false, false);
        let a = cache.frame(3, &r, variant);
        let b = cache.frame(3, &r, variant);
        // Same backing allocation ⇒ the buffer is shared, not re-serialized.
        assert!(Arc::ptr_eq(&a, &b));
        // A different projection variant is cached independently.
        let c = cache.frame(3, &r, FrameVariant::new(false, false, false));
        assert!(!Arc::ptr_eq(&a, &c));
    }

    #[test]
    fn ring_is_bounded() {
        let cache = BroadcastCache::new();
        let variant = FrameVariant::new(true, true, true);
        for seq in 1..=(RING_CAP as u64 + 50) {
            let _ = cache.frame(seq, &rec(seq), variant);
        }
        let state = cache.state.lock();
        assert!(state.order.len() <= RING_CAP);
        assert!(state.frames.len() <= RING_CAP);
        // The oldest seqs were evicted (front-dropped).
        assert!(state.order.front().map(|seq| *seq > 1).unwrap_or(false));
        assert!(!state.frames.contains_key(&1));
    }
}
