//! Engine ↔ snapshot glue (ARCHITECTURE §3 checkpoint, §5 metadata store).
//!
//! [`capture`] materializes the engine's durable state into a
//! [`crate::storage::Snapshot`]; [`restore`] rebuilds the in-memory engine from
//! a loaded one. A snapshot lets recovery start without replaying the WAL from
//! frame zero — only the frames after the recorded checkpoint position are
//! replayed (and re-applied Appends are skipped by seq, so the small overlap
//! between the captured position and the materialized state is idempotent).
//!
//! ## Consistency ordering (why this loses nothing)
//!
//! [`capture`] records the WAL checkpoint position **first** (after a durable
//! `CheckpointMark` flush barrier so the published position covers every
//! prior frame), *then* materializes box/router state. Any write committed
//! after the position-read has a WAL offset `>=` the checkpoint, so recovery
//! replays it; any such write that also raced into the materialized snapshot is
//! skipped on replay by seq (`seq <= recovered head`). No acked write can fall
//! into the gap.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::engine::box_state::{BoxState, StoredRecord};
use crate::engine::{Engine, SEQ_BASE};
use crate::storage::{Checkpoint, Snapshot, SnapshotBox, SnapshotRecord, SnapshotRouter};
use crate::types::{BoxConfig, Filter, FilterOp, Router};

/// Capture the engine's current durable state into a [`Snapshot`].
///
/// Requires a WAL (durable engine); returns `None` for a pure in-memory engine
/// (nothing to checkpoint). `id` is the snapshot's monotonic file id.
pub fn capture(engine: &Engine, id: u64) -> Option<Snapshot> {
    let writer = engine.wal.as_ref()?;
    let now = engine.clock.now_ms().max(0) as u64;

    // 1) Flush barrier: write a durable CheckpointMark and wait for its fsync so
    //    the writer's published position covers every prior committed frame.
    //    (Replay treats CheckpointMark as a no-op, so logging it is harmless.)
    let _ = writer.append(
        crate::storage::WalRecord::CheckpointMark {
            last_checkpoint_seq: 0,
            ts: now,
        },
        true,
    );

    // 2) Record the checkpoint position FIRST (before materializing state), so a
    //    racing write lands at/after this offset and is replayed.
    let (wal_idx, wal_offset) = writer.position();

    // 3) Materialize every box's live state + the router set.
    let mut boxes = Vec::with_capacity(engine.boxes.len());
    let mut max_seq = 0u64;
    for entry in engine.boxes.iter() {
        let b = entry.value();
        // Enforce retention so the captured floors/records are current.
        b.enforce_retention(engine.clock.now_ms());
        let snap_box = capture_box(b);
        max_seq = max_seq.max(snap_box.head_seq);
        boxes.push(snap_box);
    }

    let routers = engine
        .routers
        .lock()
        .snapshot_all()
        .into_iter()
        .map(|(r, cursor, total)| router_to_snapshot(r, cursor, total))
        .collect();

    Some(Snapshot {
        id,
        ts: now,
        next_box_id: engine.next_box_id.load(Ordering::Relaxed) as u32,
        checkpoint: Checkpoint {
            wal_idx,
            wal_offset,
            last_checkpoint_seq: max_seq,
        },
        boxes,
        routers,
    })
}

/// Materialize one box into a [`SnapshotBox`] (its live record set + floors).
fn capture_box(b: &BoxState) -> SnapshotBox {
    let config_json = serde_json::to_vec(&*b.config.read()).unwrap_or_default();
    let floors = *b.floors.read();
    let index = b.index.read();

    // The live record set: every physically-present, non-deleted record. Deleted
    // middle-holes and front-reclaimed prefixes are simply absent — the compacted
    // form (ARCHITECTURE §3.1).
    let mut records = Vec::with_capacity(index.records.len());
    let base = index.base_seq;
    for (i, rec) in index.records.iter().enumerate() {
        if rec.deleted {
            continue;
        }
        let seq = base + i as u64;
        records.push(SnapshotRecord {
            seq,
            ts: rec.ts,
            node: rec.node.clone(),
            tag: rec.tag.clone(),
            data_json: serde_json::to_vec(&rec.data).unwrap_or_default(),
            meta_json: rec
                .meta
                .as_ref()
                .map(|m| serde_json::to_vec(m).unwrap_or_default()),
            bytes: rec.bytes,
        });
    }

    SnapshotBox {
        name: b.name.clone(),
        box_id: b.box_id,
        epoch: b.epoch(),
        config_json,
        base_seq: index.base_seq,
        head_seq: b.head_seq(),
        evict_floor: floors.evict_floor,
        expiry_floor: floors.expiry_floor,
        delete_floor: floors.delete_floor,
        delete_below: index.delete_below,
        bytes_retained: b.bytes(),
        live_count: b.count(),
        records,
    }
}

/// Encode a [`Router`] into a [`SnapshotRouter`].
fn router_to_snapshot(r: Router, cursor: u64, total: u64) -> SnapshotRouter {
    SnapshotRouter {
        name: r.name,
        source: r.source,
        dest: r.dest,
        preserve_node: r.preserve_node,
        preserve_tag: r.preserve_tag,
        create_dest: r.create_dest,
        allow_cycle: r.allow_cycle,
        filter: r.filter.map(|f| {
            (
                match f.op {
                    FilterOp::Eq => 0u8,
                    FilterOp::Glob => 1u8,
                },
                f.value,
            )
        }),
        forward_cursor: cursor,
        forwarded_total: total,
    }
}

/// Rebuild the engine's in-memory state from a loaded [`Snapshot`]. Called
/// before WAL replay during recovery (the WAL frames after the checkpoint are
/// then layered on top). Returns the checkpoint position to replay from.
pub fn restore(engine: &Engine, snapshot: Snapshot) -> Checkpoint {
    // Restore the id allocator so newly-created boxes never collide with a
    // snapshotted id.
    engine
        .next_box_id
        .store(snapshot.next_box_id as u64, Ordering::Relaxed);

    for sb in snapshot.boxes {
        let config: BoxConfig = serde_json::from_slice(&sb.config_json).unwrap_or_default();
        let bx = Arc::new(restore_box(sb, config));
        engine.boxes.insert(bx.name.clone(), bx);
    }

    {
        let mut graph = engine.routers.lock();
        for sr in snapshot.routers {
            let filter = sr.filter.map(|(op, value)| Filter {
                op: if op == 0 { FilterOp::Eq } else { FilterOp::Glob },
                value,
            });
            let router = Router {
                name: sr.name,
                source: sr.source,
                dest: sr.dest,
                preserve_node: sr.preserve_node,
                preserve_tag: sr.preserve_tag,
                create_dest: sr.create_dest,
                filter,
                allow_cycle: sr.allow_cycle,
            };
            graph.restore(router, sr.forward_cursor, sr.forwarded_total);
        }
    }

    snapshot.checkpoint
}

/// Rebuild a single [`BoxState`] from a [`SnapshotBox`], re-inserting its live
/// record set at the recorded seqs (so `base_seq`/`head_seq` and the tag index
/// match exactly) and restoring the floors + counters.
fn restore_box(sb: SnapshotBox, config: BoxConfig) -> BoxState {
    let state = BoxState::new(sb.name, sb.box_id, config, SEQ_BASE, sb.epoch);

    {
        let mut index = state.index.write();
        index.base_seq = sb.base_seq;
        index.delete_below = sb.delete_below;
        // Re-insert each live record at its seq. The snapshot is the compacted
        // (deleted-hole-free) form, but holes between live seqs are possible
        // (a middle delete reclaimed neither slot before snapshot). Fill any gap
        // with a deleted tombstone so `seq - base_seq` indexing stays O(1).
        let mut next = sb.base_seq;
        for r in sb.records {
            while next < r.seq {
                index.records.push_back(deleted_hole());
                next += 1;
            }
            if let Some(tag) = &r.tag {
                index.index_tag(r.seq, tag);
            }
            let data: serde_json::Value =
                serde_json::from_slice(&r.data_json).unwrap_or(serde_json::Value::Null);
            let meta = r
                .meta_json
                .as_ref()
                .and_then(|m| serde_json::from_slice(m).ok());
            index.records.push_back(StoredRecord {
                ts: r.ts,
                node: r.node,
                tag: r.tag,
                data,
                meta,
                bytes: r.bytes,
                deleted: false,
            });
            next = r.seq + 1;
        }
    }

    // Floors + watermarks.
    {
        let mut floors = state.floors.write();
        floors.evict_floor = sb.evict_floor;
        floors.expiry_floor = sb.expiry_floor;
        floors.delete_floor = sb.delete_floor;
    }
    state.head_seq.store(sb.head_seq, Ordering::Release);
    state.bytes_retained.store(sb.bytes_retained, Ordering::Relaxed);
    state.live_count.store(sb.live_count, Ordering::Relaxed);

    state
}

/// A lightweight deleted-hole slot used to keep `seq - base_seq` indexing dense
/// when a snapshot's live records have seq gaps (middle deletes not yet
/// reclaimed at snapshot time).
fn deleted_hole() -> StoredRecord {
    StoredRecord {
        ts: 0,
        node: None,
        tag: None,
        data: serde_json::Value::Null,
        meta: None,
        bytes: 0,
        deleted: true,
    }
}
