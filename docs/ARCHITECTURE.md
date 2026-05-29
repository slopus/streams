# streams — Architecture

This document specifies the on-disk and in-memory architecture: storage, WAL, group commit,
segments, indexing, recovery/crash-consistency, metadata, on-disk layout, the priority scheduler
and elastic throttling, the concurrency model, recommended Rust crates, and the latency-budget
analysis for the 1–5 ms target.

It is written for the **scalable phase (phase 4)** but calls out the **simple phase-2** shape at
each layer so the implementation can grow into it without rework. Assumptions: single process,
single machine, good CPU, local NVMe SSD (not HDD, not networked). The semantics being enforced
are specified in [DESIGN.md](DESIGN.md); the wire contract is [API.md](API.md).

---

## 0. Design principles

1. **Never silent loss.** Every eviction/TTL crossing that passes a consumer's cursor surfaces an
   in-band tombstone carrying `[gap_from, gap_to]`. The storage layer's job is to make the
   *earliest retained seq* always cheaply queryable.
2. **Trim at segment granularity, lazily.** Eviction never rewrites data or deletes individual
   records on the hot path; it advances a watermark and drops whole sealed segment files.
3. **The WAL is the durability boundary.** "Only data not yet in the WAL is lost." Everything
   downstream (in-memory index, segments) is a derivable cache of WAL + checkpoints.
4. **Seqs are mostly-sequential u64** → represent the seq→location index as a base+offset vector,
   not a hash map (§1).

---

## 1. In-memory representation of a box

### 1.1 The core: a base+offset location vector

Eviction only ever removes a contiguous prefix; writes never skip; tag-deletion and node-filtering
are read-time filters, not holes. So the seq→location map is, in practice, a **contiguous
integer-keyed array offset by the earliest retained seq**:

```rust
struct BoxIndex {
    base_seq: u64,             // seq of locs[0]; == earliest_retained physical seq
    locs: VecDeque<RecordLoc>, // index i  <=>  seq (base_seq + i)
}
struct RecordLoc {
    location: u32,  // which segment file (or sentinel = WAL)
    offset: u32,    // byte offset within that file
    len: u32,       // framed length (read a record without touching neighbors)
    ts: u64,        // server commit ms — kept inline for TTL binary search
    flags: u8,      // has_tag, has_node, in_wal_only (not yet checkpointed)
}
```

**Lookup** `seq → loc` is `locs[seq - base_seq]` — O(1), no hashing, cache-friendly. **Eviction**
of a prefix is `locs.drain(..n)` plus `base_seq += n`; we drop whole segments so this is bounded.
`getDifference(from_seq)` becomes "slice `locs[from_seq - base_seq ..]`" — exactly the batched-diff
primitive.

**Why a vector, not `HashMap<u64,Loc>` / `BTreeMap`:** the base+offset trick eliminates the key
entirely (the key *is* the array position). A `HashMap` costs ~3–4× the per-entry memory and
random access on the hot read path; a `BTreeMap` is log(n) + pointer-chasing. With 24-byte
`RecordLoc` entries the index packs into contiguous cache lines, and index memory is bounded by
`cap_records` regardless. (If a future feature ever introduced holes, a `tombstone_local` flag
keeps the slot present so the vector stays dense — but no current feature does.)

### 1.2 Per-box in-memory state

```rust
struct Box {
    config: BoxConfig,                  // ttl_ms, cap_records, cap_bytes, discard, durable,
                                        //   priority, auto_priority, ...
    index: parking_lot::RwLock<BoxIndex>,
    head_seq: AtomicU64,                // last assigned seq (log end)
    earliest_seq: AtomicU64,            // earliest retained, the watermark (DESIGN §5.1)
    epoch: AtomicU64,                   // bumped on create; detects delete+recreate
    bytes_retained: AtomicU64,          // for byte-cap eviction
    delete_filters: arc_swap::ArcSwap<FilterSet>, // tag-delete rules, copy-on-write
    eff_priority: AtomicI64,            // effective priority, recomputed lazily
    last_consumed_ms: AtomicU64,        // for auto-priority by recency
    waiters: tokio::sync::Notify,       // wakes SSE/diff long-pollers on append
    hot_tail: SegmentWriter,            // the open (unsealed) segment + WAL coupling
}
```

`head_seq`/`earliest_seq`/`epoch`/`eff_priority` are atomics so `GET /v0/boxes/:box` is lock-free
and the diff path can detect eviction with a single atomic load before taking the index read lock.
`Notify` is the wakeup primitive that lets SSE/diff hit 1–5 ms without polling. The global registry
is `DashMap<BoxId, Arc<Box>>` for sharded concurrent access across many boxes.

### 1.3 Phase-2 (simple) shape

Phase-2 is this exact structure minus segments and WAL: `RecordLoc.location` is unused and payload
bytes live in a `VecDeque<Bytes>` parallel to `locs`. No persistence; restart = empty. Everything
else — API, base+offset index, atomics, tombstone logic, tag filters, node filtering, priority,
`Notify` wakeups — is **identical and fully exercised**. Phase 4 only re-points `RecordLoc` from
heap `Bytes` to mmap'd segment bytes and inserts the WAL on the write path; the serving and indexing
logic is written once.

---

## 2. WAL (Write-Ahead Log)

### 2.1 Record framing

The WAL is an append-only sequence of length-prefixed, CRC-protected frames (one frame per record;
multi-record writes produce many frames committed as one batch). Multi-byte integers little-endian.

```
 off  size  field
   0    4   frame_len   u32   bytes of this frame EXCLUDING this field
   4    1   type        u8    1=Append 2=BoxCreate 3=BoxDelete 4=RouterCreate
                                5=RouterDelete 6=DeleteFilter 7=EvictWatermark
                                8=CheckpointMark 9=ConfigUpdate
   5    1   flags       u8    bit0=has_tag bit1=has_node bit2=durable
   6    4   box_id      u32   interned numeric box id (string<->id in meta store)
  10    8   seq         u64   server-assigned (0 for non-Append control frames)
  18    8   ts          u64   server commit ms
  26    2   node_len    u16
  28    2   tag_len     u16
  30    4   data_len    u32
  34    N   node        bytes (node_len)
   .    M   tag         bytes (tag_len)
   .    P   data+meta   bytes (data_len)   -- opaque payload
   .    4   crc32c      u32   CRC32C (Castagnoli) over bytes [4 .. crc_start)
```

- **`frame_len` first** lets recovery validate frame boundaries without parsing the body and
  detect a torn tail (frame_len past EOF ⇒ truncated write ⇒ discard from here).
- **CRC32C** (hardware-accelerated on modern x86/ARM) over everything between `frame_len` and the
  CRC. A mismatch ⇒ torn/partial frame ⇒ logical end of log (truncate). This is the crash-consistency
  anchor (§4).
- **`box_id` is an interned u32** (not the string name), keeping frames small; the name↔id mapping
  lives in the metadata store (§5).
- **Control frames** (BoxCreate, DeleteFilter, EvictWatermark, ConfigUpdate, …) share the same WAL,
  so config and data live on one ordered, crash-consistent timeline — there is exactly one truth:
  WAL order.

### 2.2 Append path

```
write request
  -> validate; resolve box_id; assign seq = head_seq.fetch_add(n)   (after a discard:"reject" cap check)
  -> serialize frame(s) into a reusable per-writer scratch BytesMut
  -> hand (frames, durability_class, completion-oneshot) to the single WAL writer task
  -> writer appends bytes to the active wal file (buffered write())
  -> on commit (fsync for durable, or group-commit tick): fulfill oneshots
  -> update in-memory: push RecordLoc into BoxIndex, bump head_seq visibility, Notify watchers
  -> respond { seqs, head_seq, performance }
```

The seq is assigned **before** the WAL commit (so it can be returned) but the record is only
*visible to readers* and *acked to the writer* after its commit class is satisfied. Guarantee: **if
a write was acked, it is in the WAL.** A **single WAL writer task** (fed by an MPSC channel)
serializes all appends — the disk is a single sequential resource, so a single ordered append stream
matches the hardware, makes group commit trivial, and removes write-side lock contention.

### 2.3 Durability classes & group commit

Durability is per-box. Two commit classes, one writer:

| `durable` | Commit class | Behavior |
|---|---|---|
| `true` | fsync-on-commit | Acked only after `fdatasync()` returns. Still **group-committed**: the writer coalesces all pending durable frames in a small window into one `write()` + one `fdatasync()`, then acks them all. |
| `false` | group-commit, no wait | `write()`-en to the page cache and acked immediately. A background `fdatasync()` runs on a timer; writers do not wait. Loss window on crash = un-fsynced tail. |

**Group-commit loop:**

```
loop:
  batch = drain channel (non-blocking) up to MAX_BATCH frames or MAX_BATCH_BYTES
  if empty: park on a Notify until a frame arrives
  write(wal_fd, batch_bytes)                       // one write/writev syscall
  if batch has any durable frame OR fsync timer elapsed:
      fdatasync(wal_fd)                            // one fsync for the whole batch
      ack durable frames
  ack non-durable frames                           // already in page cache
  publish all frames to in-memory indexes; Notify per-box waiters
```

**Tuning for 1–5 ms on NVMe.** NVMe `fdatasync` is ~50–500 µs. An **adaptive window** ≤ 1 ms
amortizes one fsync across hundreds of durable writes under load but collapses to ~0 when quiet (a
lone durable write fsyncs immediately) — group commit only helps under load, never penalizes a lone
write. Use **`fdatasync`** (not `fsync`) — no inode-metadata flush per commit. WAL files are
**preallocated** (`fallocate` to e.g. 64–256 MB) so appends don't extend the inode; the next file is
preallocated ahead of rotation. The fd stays open; no `O_DIRECT` (we want the page cache for fast
read-back and OS coalescing), relying on explicit `fdatasync` for durability.

### 2.4 WAL rotation

The active WAL rotates at its preallocated size (or on checkpoint). Files are named
`wal-<first-frame-seq>.log` (zero-padded); the highest-numbered is active. Rotation: fsync current,
open/preallocate next, atomically update a `CURRENT` pointer in the meta store. Old WAL files are
deletable only after a checkpoint durably absorbs all their frames (§3).

---

## 3. Commit / checkpoint → segments

The WAL is a fast, append-ordered, mixed-box, short-lived log. WAL frames are periodically applied
into per-box **segment files** — the long-term store and read source for `getDifference` — to keep
recovery bounded and reads efficient.

### 3.1 Checkpoint process

A background **compactor** task (triggered by time or WAL rotation):

```
for each box with new frames since last checkpoint:
    append those records (in seq order) to the box's active segment file
    (segment frames are byte-identical to WAL Append frames — a buffered copy of
     contiguous byte ranges, split by box; no re-serialization)
    update the box's .idx file
fsync touched segment + idx files
write a CheckpointMark frame to the WAL (per box: highest_seq_checkpointed, watermarks,
     active-segment positions); fsync the WAL
WAL files whose every frame's seq <= the global min checkpointed seq are now deletable
```

### 3.2 Segment format

Per box, a directory of numbered pairs:

```
seg-<first_seq>.data    append-only record frames (same framing as §2.1, Append frames only)
seg-<first_seq>.idx     fixed-width: per record [offset:u32, len:u32, ts:u64];
                        entry i  <=>  seq (first_seq + i)
```

The `.idx` is the on-disk twin of `BoxIndex`: fixed-stride (16 bytes/entry), so `seq → entry` is
`(seq - first_seq) * 16` — a direct seek, no scan. This makes rebuilding the in-memory index on
restart a **bulk read of `.idx` files** rather than a re-parse of all data. The inline `ts` enables
**binary search for the TTL boundary** without touching the data file. A segment is **sealed** at a
target size (64–256 MB); the newest is "active" (still appended), older ones immutable.

### 3.3 TTL / cap eviction — cheap, no rewrite

Eviction never rewrites a segment. Two mechanisms:
1. **Watermark advance (logical eviction).** `earliest_seq` is authoritative. For count cap, when
   `head_seq - earliest_seq > cap_records`, advance `earliest_seq`. For TTL, advance past records
   whose `ts < now - ttl_ms` (binary search on inline `ts`). Advancing is an `AtomicU64` store +
   `VecDeque::drain` on the index front. A read with `from_seq + 1 < earliest_seq` returns a
   tombstone (DESIGN §5.4).
2. **Segment dropping (physical reclaim).** A whole **sealed** segment whose highest seq <
   `earliest_seq` is entirely evicted → its `.data`+`.idx` files are deleted. Reclaim is
   segment-granular and lazy (Redis `~` / Kafka), so the box may retain slightly more than cap (only
   whole sealed segments drop) — the documented, accepted approximation. The active segment is never
   dropped.

The new watermark is persisted via an **`EvictWatermark`** control frame (folded into the next
CheckpointMark), so eviction and the tombstone boundary survive restart. A crash between
watermark-advance and file-delete is harmless: on restart we re-derive which segments are fully
below the watermark and delete them (idempotent reclaim).

**Full-box policy.** `discard:"old"` (default) evicts oldest as above. `discard:"reject"` (durable
queue): the cap check happens on the append path *before* WAL write and seq assignment, so a rejected
write (`422 box_full`) never enters the log — never ack-then-drop (the NATS DiscardNew foot-gun).

### 3.4 Serving `getDifference`: mmap vs buffered

- **Sealed (immutable) segments → mmap** (`memmap2`): map `.data` once, slice `[offset..offset+len]`
  per record (zero-copy, page-cache-backed). A diff is: bound-check against `earliest_seq`
  (tombstone?), slice the index, then per entry copy framed bytes out of the mmap into the response,
  applying tag-delete + node filters during the copy, bounded by `limit`.
- **Active segment → buffered `pread`** (the growing file is usually still in page cache from the
  write; mmap past EOF is UB and remapping per append is wasteful).
- **Newest records (written, not yet checkpointed) → served directly from WAL bytes** via the same
  `RecordLoc` mechanism (`location = WAL`). So a consumer 1–5 ms behind head reads from the WAL/page
  cache, never waiting for checkpoint — essential to the latency target.

---

## 4. Recovery on restart

Goal: rebuild all in-memory state, lose only data not yet in the WAL, tolerate a crash at any
instant.

```
1. Open data dir; load latest valid metadata snapshot (boxes, routers, filters, name<->id,
   watermarks, CURRENT wal ptr, last_checkpoint_seq).
2. Per box: bulk-load segment .idx files into BoxIndex (fixed-stride sequential read). Set
   base_seq from the lowest surviving segment, earliest_seq from the persisted watermark,
   head_seq from the highest segment seq.
3. Replay the WAL from the frame after the last CheckpointMark. For each frame, in order:
     - frame_len fits remaining bytes? else torn tail -> STOP (truncate here).
     - crc32c valid? else torn/partial -> STOP (truncate here).
     - apply: Append -> push RecordLoc (location=WAL), bump head_seq.
              Control frames -> mutate config/filters/watermarks.
4. Truncate the WAL at the first bad/partial frame boundary (ftruncate) -> clean for new appends.
5. Re-derive droppable segments (sealed, fully below earliest_seq) and delete them (idempotent).
6. Resume: open the truncated/fresh active WAL, start the writer + compactor.
```

**Crash-consistency guarantees:**
- **Torn tail:** detected by `frame_len` overrunning EOF or CRC mismatch; stop at the last fully
  written, CRC-valid frame and truncate. Since a write is acked only after its frame is committed
  (and fsynced, for durable boxes), an **acked durable write is always a complete CRC-valid frame ⇒
  never lost.**
- **Partial `write()`:** the trailing partial frame fails CRC/length and is discarded; never
  interpreted as data.
- **`CheckpointMark` is itself CRC-protected and fsynced.** Crash after writing segments but before
  the CheckpointMark is durable ⇒ recovery re-replays those WAL frames into segments; duplicate
  appends are skipped by seq (a seq already in the segment index is ignored). Crash after the
  CheckpointMark but before deleting absorbed WAL files ⇒ those files are replayed-and-skipped
  (seqs ≤ checkpointed) — harmless.
- **"Only data not yet in the WAL is lost":** for `durable=true` an acked write survives (ack waits
  for fsync); for `durable=false` writes acked but not yet fsynced (within the group-commit timer)
  can be lost on power loss — the documented fast-path tradeoff, surfacing to consumers as ordinary
  eviction-style gaps. In both cases the boundary is precisely "what reached the WAL on disk."

---

## 5. Metadata store

Two-tier, mirroring the WAL philosophy: **mutations are control frames in the WAL** (crash-consistent
and ordered with data), and a **periodically-snapshotted metadata file** lets recovery start without
replaying the WAL from time zero.

```rust
struct Meta {
    boxes:   HashMap<String, BoxId>,    // name -> interned u32 id (stable across restart)
    box_cfg: HashMap<BoxId, BoxConfig>,
    watermarks: HashMap<BoxId, u64>,    // persisted earliest_seq per box
    routers: Vec<Router>,               // {name, source, dest, preserve_*, filter, allow_cycle}
    filters: HashMap<BoxId, FilterSet>, // tag-delete rules
    epochs: HashMap<BoxId, u64>,        // delete+recreate detection
    next_box_id: u32,
    current_wal: String,
    last_checkpoint_seq: u64,           // global lower bound for WAL replay
}
```

**Tag-delete filters** (read-time deletion, must be efficient over many records):

```rust
struct FilterSet {
    exact:    HashSet<Box<str>>,  // Eq tags -> O(1) membership
    prefixes: Vec<Box<str>>,      // Glob "tag*" prefixes, sorted for binary search
}
```

Per-record evaluation during a read: `exact.contains(tag)` is O(1); prefix match is binary search
O(log P) — a handful of comparisons per record inside the read loop. Held behind **`ArcSwap`**, so
adding a rule publishes a new `Arc<FilterSet>` (copy-on-write) and the frequent read path is
wait-free (`load()` the current Arc). A filter mutation is a durable **`DeleteFilter`** control frame
before ack, so deletions survive restart; because deletions are read-time filters tied to box config
(not GC'd tombstones), a lagging consumer cannot miss a deletion (the Kafka `delete.retention.ms`
race is avoided). Node loop-prevention reuses the same read-loop slot (skip if `$node ∈ reader set`)
— one pass for TTL + tag + node filtering.

**Durability & recovery.** The snapshot is written atomically (`snapshot.<n+1>.tmp` → fsync → rename
→ fsync dir → delete old); atomic rename gives crash-atomic metadata swaps. On recovery, load the
latest valid snapshot, then replay WAL control frames after `last_checkpoint_seq` — the same single
pass as §4, so config and data are restored consistently relative to each other. bincode is used for
the compact snapshot; metadata is tiny and changes rarely.

---

## 6. On-disk layout

```
<data_dir>/
├── meta/
│   ├── snapshot.0007.bin            # latest atomic metadata snapshot
│   └── snapshot.0006.bin            # previous (kept until next snapshot fsynced)
├── wal/
│   ├── CURRENT                      # tiny file naming the active wal segment (atomic-renamed)
│   ├── wal-0000000000001024.log     # preallocated, append-only, mixed-box framed records
│   └── wal-0000000000004096.log     # active wal segment (highest first-seq)
└── boxes/
    ├── 0000000A/                    # one dir per box, named by interned box_id (hex)
    │   ├── seg-0000000000000001.data
    │   ├── seg-0000000000000001.idx # fixed-stride [offset,len,ts]; seq->entry by arithmetic
    │   ├── seg-0000000000010001.data
    │   ├── seg-0000000000010001.idx
    │   └── seg-0000000000020001.data  (active segment, newest; + .idx)
    └── 0000000B/
        └── ...
```

WAL is **process-global** (one ordered stream → trivial group commit, matches the single sequential
disk). Segments are **per-box** (independent eviction, per-box mmap, locality for `getDifference`).
Segment files named by first seq sort into seq order; finding a segment for a seq is a binary search
over first-seqs. A box delete is a control frame + a fast rename `boxes/0000000A.deleted` then
background unlink (fast and crash-safe).

---

## 7. Priority scheduler & elastic throttling

The unit of scheduling is **delivery work** for a box: waking SSE watchers, running routers, and
flushing pending write batches / group commit. Writes are admitted on the request path; scheduling
governs the *post-write propagation* that must hit the latency target. The priority **formula and
defaults** are in [DESIGN.md §3](DESIGN.md).

### 7.1 Shape: a bounded pool draining a banded ready-set of *dirty boxes*

```
write/router makes a box "dirty" -> insert into its shard's ready set (at most once)
                                         |
                                         v
   banded weighted-fair queue (DWRR) keyed by effective priority + aging
                                         |
                       pop highest-credit band -> bounded worker pool (N_workers tasks)
                       each worker drains ONE box fully, requeues if more work arrived
```

The schedulable entity is a **box, not a record/watcher**: a write marks the box dirty and inserts
it into the ready set if not already present (a membership bit prevents duplicates). This bounds the
queue to O(#dirty boxes) and coalesces a box's burst of writes into one unit of work. A worker that
picks up box B **drains B fully** (wakes all its SSE watchers, forwards to all router dests, flushes
its commit batch) before moving on — preserving per-box ordering and amortizing the lock.

### 7.2 Banded weighted-fair queue (anti-starvation) + aging

A pure max-heap on priority starves low-priority boxes. Instead, priorities bucket into bands drawn
by **deficit weighted round-robin (DWRR)**:

```
Band  P_eff range    weight
 B4   >= 750           8
 B3   500..749         4
 B2   250..499         2
 B1   0..249           1
 B0   < 0              1   (explicitly deprioritized)
```

Within a band, FIFO by `enqueued_at`. Across bands, each round grants credit proportional to weight;
with the defaults, for every 1 low-priority box serviced up to 8 top-band boxes may be — high
priority strongly favored, but B1/B0 always make forward progress every round.

**Aging** prevents a box stuck at the bottom of a busy band from waiting forever:
`age_boost = AGE_RATE * min(now - enqueued_at, AGE_CAP_MS)` (+100/s, capped at +1000 after 10 s). A
50 ms aging tick promotes boxes across band boundaries. `enqueued_at` resets only when the box is
actually serviced, so a continuously-rewritten box still ages. **Combined guarantee:** no box waits
more than 10 s before reaching the top band, and DWRR drains the top band every round — worst-case
scheduling latency is bounded even under sustained high-priority load. Under unsaturated load the
ready set is near-empty and boxes are serviced within microseconds of being marked dirty (1–5 ms
target).

### 7.3 Elastic throttling — shed cost, never data

A **governor task** every 100 ms samples three cheap signals into `pressure ∈ [0,1]`: ready-set
depth vs `N_workers`, EWMA scheduling latency vs the 5 ms ceiling, and the blocking/compute-pool busy
ratio. `pressure` is published as a lock-free atomic and drives an escalating, composable ladder:

1. **Batch coalescing (`pressure > 0.2`).** Stop waking watchers per-record; coalesce a box's
   pending records into one multi-record frame / diff. Cheap, lossless, often improves throughput.
   Window grows `0..20 ms` with pressure.
2. **Widen group-commit window (`pressure > 0.4`).** `commit_window_ms = lerp(0.5, 10, pressure)` —
   fewer fsyncs/sec, more headroom; cost is up to +9.5 ms write-ack latency, observed as latency,
   never loss.
3. **Defer lowest-value work (`pressure > 0.8`, sustained).** Routers (fan-out) are enqueued one band
   lower; `B0`/negative-priority boxes stop receiving DWRR credit until `pressure < 0.6` (hysteresis)
   — their data is still durably stored and fully pollable via `getDifference`, only the *push* is
   paused. If a per-shard ingest channel is full and `pressure ≈ 1.0`, the write endpoint returns
   **`429` + `Retry-After`** (writers may bypass with `disable_backpressure: true`).

The cardinal rule: **throttling degrades latency and push-eagerness, never correctness.** A deferred
box is always fully consistent on the next `getDifference`. All data loss remains the explicit,
configured cap/TTL path with in-band tombstones; full-write rejection is synchronous (`422`/`429`),
never ack-then-drop.

| Condition | Client-visible effect | Loss? |
|---|---|---|
| Healthy (`pressure < 0.2`) | 1–5 ms delivery, per-record frames | No |
| Mild pressure | Coalesced multi-record frames, ~5–15 ms | No |
| Heavy pressure | Slower write-acks; low-priority pushes paused but pollable | No |
| Saturation on write | `429 + Retry-After` (write rejected synchronously) | No |
| Cap/TTL crosses cursor (independent of pressure) | In-band `tombstone` with `[gap_from, gap_to]` | Explicit, never silent |

---

## 8. Concurrency model

### 8.1 Sharding

Boxes are partitioned across `S` shards by `shard = hash(box_id) % S`, with `S = N_workers` (one
shard per core) by default. Each shard owns its slice of the box map, its ready-set, and its WAL
ingest lane. **State is sharded, not globally locked.** The only global structures are the lock-free
`pressure` atomic and the read-mostly box-name→shard directory (`dashmap`). The single WAL writer
(§2.2) is fed by per-shard MPSC lanes.

### 8.2 Lock strategy: short shard lock + per-box fine lock

- **Per-shard mutex** held only for the O(1) ready-set splice (push/pop a deque, flip a bitset bit) —
  a few instructions, negligible contention even when workers share a shard.
- **Per-box `RwLock`** guarding the append tail, watcher list, and pending-work buffer. A worker
  draining box B holds only B's lock, so two workers drain two different boxes in the same shard
  fully in parallel. Reads (`getDifference`) take the box read lock against committed segments; the
  append tail uses a seqlock so reads rarely block writes.
- **Lock ordering** to avoid deadlock: shard-ready lock → box lock, never reverse; routers acquire
  source then dest in ascending `(shard, box_id)` order.

### 8.3 How operations interleave

| Operation | Path | Contention |
|---|---|---|
| **Write** | HTTP task → shard lane → append under box lock → assign seqs → mark dirty (short shard lock) → return | box's own lock + brief splice; independent boxes never contend |
| **getState** | lock-free atomic loads (head/earliest/count) + `last_consumed_ms` store | lock-free |
| **getDifference** | box read lock over committed segments; bounded batch; bump recency; tombstone if `from_seq+1 < earliest_seq` | box read lock; doesn't block other boxes; rarely blocks the append tail (seqlock) |
| **SSE push** | worker draining the box pushes frames to each watcher's bounded channel; slow consumer's channel full → degrade that connection, not the box | per-box during drain; per-connection channel isolates a slow client |
| **Router** | at drain time, new src records handed to the dest shard via its ingest MPSC (no cross-shard lock); dest box scheduling/priority applies; node filtering at dest read time | cross-shard hop only when src/dst differ; no cross-shard lock acquisition |

### 8.4 Slow-consumer isolation (SSE)

Each SSE connection has a bounded outbound channel (default 1024 frames). If a worker can't enqueue,
it does **not** block the box drain; the connection is marked **lagged**, the server stops buffering
for it, records the last-delivered composite cursor, and on the next successful send emits a tombstone
for the skipped range (for lossy boxes) so the client catches up via `getDifference`. One slow client
is contained to its own connection; the box and all other watchers proceed at full speed.

### 8.5 Mapping onto tokio + a bounded compute pool

- **One multi-threaded tokio runtime** (`worker_threads = num_cpus`) runs all async I/O: HTTP
  (axum/hyper), SSE connections, channel plumbing.
- **Delivery workers** are long-lived tokio tasks (one per shard) running the §7.1 loop; they park on
  a per-shard `Notify`/MPSC when their ready set is empty (no busy-spin) and are woken by mark-dirty.
- **A separate bounded blocking/compute pool** quarantines genuinely blocking or CPU-heavy work so it
  can't starve the reactor: fsync/WAL durability via `spawn_blocking` onto a bounded pool
  (`max_blocking_threads ≈ 2·N_workers`; group commit keeps it small), and large diff serialization /
  segment compaction on a dedicated rayon lane.

**Why this hits the target:** the hot delivery path stays on the async runtime and is pure in-memory
work (heap splice + channel sends), completing in microseconds when unsaturated; blocking work is
quarantined in a bounded pool that can never consume all async threads; backpressure is structural
(every ingest and SSE channel is bounded, with defined `429`/tombstone behavior rather than unbounded
memory growth); and there is no global lock on writes, reads, or pushes for distinct boxes, so
throughput scales ~linearly with cores until the durability pool or NVMe is the bottleneck — at which
point group-commit widening trades latency for throughput, gracefully.

---

## 9. Latency budget (how 1–5 ms is achieved)

The push chain on a non-durable write, unsaturated:

1. **Append + wake** (~tens of µs): append to the in-memory tail, assign seq, write frame bytes to
   the WAL page cache, signal the box's `Notify`. (For `consistency:strong` SSE / `durable` boxes,
   the signal/ack waits for the group-commit fsync — see below.)
2. **Watcher registry, not scan** (~µs): each box keeps its registered watchers; the `Notify` wakes
   only those connections. No periodic poll; idle boxes cost nothing.
3. **Coalesced flush** (~tens of µs): each woken worker reads from its per-box cursor up to
   `limit`/`max_batch_bytes`, applies node + tag filters, builds one frame, writes to the socket and
   flushes (`X-Accel-Buffering: no` + `TCP_NODELAY` → no proxy/Nagle buffering).
4. **Routers add one hop** (~µs): a forwarded record triggers the dest box's `Notify` exactly like a
   direct write — one extra in-process append.
5. **Backpressure cannot stall the writer**: the write path only *signals*; slow-consumer buffering
   happens in the consumer's own task, so fast-consumer latency is independent of slow ones.

Budget breakdown (NVMe-class hardware, unsaturated):

| Stage | Typical | Notes |
|---|---|---|
| HTTP parse + validate | 50–200 µs | small JSON bodies |
| WAL frame serialize + buffered write | 10–50 µs | reusable scratch buffer, page cache |
| `fdatasync` (durable / strong only) | 50–500 µs | one per group-commit batch |
| Index update + `Notify` | < 10 µs | atomic + deque push |
| Worker wake + filter + frame build | 20–100 µs | per-box read lock, in-memory slice |
| Socket write + flush | 10–50 µs | `TCP_NODELAY`, explicit flush |

Non-durable / `eventual`: end-to-end well under 1 ms typical, comfortably inside the 1–5 ms target.
Durable / `strong`: add the group-commit fsync window (≤ 1 ms adaptive), still inside budget. The
only intentional latency knobs are `consistency:strong` (adds the fsync window) and the scheduler's
deliberate pacing of low-priority boxes under CPU pressure — both explicit and visible (in the
`performance.fsync_ms`/`throttle_wait_ms` fields and SSE `error` frames).

---

## 10. Recommended Rust crates

| Crate | Role | Justification |
|---|---|---|
| `tokio` | async runtime | Multi-threaded executor, timers, MPSC, `Notify` — backbone for the WAL writer task, compactor, SSE fan-out. |
| `axum` | HTTP framework | Ergonomic typed routing over hyper; first-class streaming responses for the SSE endpoint. |
| `hyper` | HTTP core | Underlies axum; direct access for fine control over SSE flushing / `X-Accel-Buffering`. |
| `tower` / `tower-http` | middleware | Timeouts, the `429`+`Retry-After` elastic-throttle layer, compression negotiation as middleware. |
| `serde` + `serde_json` | (de)serialization | JSON-first API bodies; `#[serde]` on request/response structs. |
| `bincode` | compact meta snapshots | Fast compact binary for the metadata snapshot. |
| `bytes` | zero-copy buffers | `Bytes`/`BytesMut` for reference-counted payload slices and reusable WAL framing scratch. |
| `crc32fast` | frame integrity | Hardware-accelerated CRC32C for WAL/segment checksums — the torn-tail crash anchor. |
| `memmap2` | segment reads | mmap sealed immutable segments for zero-copy, page-cache-backed `getDifference`. |
| `parking_lot` | locks | Faster, smaller `RwLock`/`Mutex` for the per-box index lock on the hot path. |
| `dashmap` | box registry | Sharded concurrent `HashMap<BoxId, Arc<Box>>` — many boxes without a global lock. |
| `arc-swap` | COW config/filters | Wait-free `load()` of the current `FilterSet` on the read path; rare writers publish a new `Arc`. |
| `smallvec` | tiny allocations | Per-write seq batches / small node/tag buffers avoid heap allocation in the common single-record case. |
| `rustix` (or `nix`) | raw fs syscalls | `fdatasync`, `fallocate`, `pread`, atomic `renameat` + dir fsync — durability primitives std doesn't expose. |
| `ahash` | fast hashing | Backing hasher for `dashmap` / exact-tag `HashSet`. |
| `tracing` + `tracing-subscriber` | observability | Structured spans populate the per-response `performance` block. |
| `metrics` / `prometheus` | metrics | Backs `GET /v0/metrics` (Prometheus text + JSON snapshot). |
| `thiserror` | error model | Ergonomic error enum mapping to the uniform `{"error":{...}}` body and HTTP codes. |
| `rayon` | compute pool | Bounded lane for large diff serialization / segment compaction off the reactor. |

---

## 11. Phase-2 → Phase-4 summary

**Unchanged across phases** (write once in phase 2): the HTTP API surface, the base+offset
`BoxIndex`, `head_seq`/`earliest_seq`/`epoch` atomics, tombstone/gap computation, the tag-delete
`FilterSet` + node loop-prevention read loop, priority/recency tracking, `Notify`-based SSE/diff
wakeups, the banded scheduler (in-memory it just has nothing to fsync).

**Added in phase 4:** the WAL (framing, single-writer group commit, per-box durable fsync), the
compactor (WAL→segment checkpointing), segment files + `.idx` + mmap serving, segment-granular lazy
eviction, metadata snapshots + control-frame replay, and restart recovery. Phase 4 only re-points
`RecordLoc` from heap `Bytes` to `(location, offset, len)` and inserts the WAL on the append path —
the serving and indexing logic is reused intact.

**The two highest-value invariants the storage layer enforces:** (1) *never silent loss* —
`earliest_seq` is always durable and cheaply queryable, so any read crossing it yields an in-band
tombstone; (2) *segment-granular, lazy eviction* — never rewrite or per-record delete on the hot
path; advance a watermark and drop whole sealed segments.
