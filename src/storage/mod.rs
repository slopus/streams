//! Durability/persistence layer (phase 4).
//!
//! The WAL ([`wal`]) is the durability boundary: "only data not yet in the WAL
//! is lost" (ARCHITECTURE §0.3). Everything downstream — the in-memory
//! [`crate::engine`] index, segments — is a derivable cache of WAL + snapshots.
//!
//! The WAL ([`wal`]) provides the **format + single-writer group commit** and a
//! torn-tail-safe reader. The engine appends a frame for every mutating op and,
//! for a `durable` box, blocks the write until the group `fdatasync` returns
//! (Stage 2 wiring). Later stages add the compactor, segments, metadata
//! snapshots, and full restart recovery on top of these primitives; Stage 2
//! already replays the active WAL on startup so durable writes survive restart.

pub mod snapshot;
pub mod wal;

pub use snapshot::{
    load_latest, next_snapshot_id, write_snapshot, Checkpoint, Snapshot, SnapshotBox,
    SnapshotError, SnapshotRecord, SnapshotRouter,
};
pub use wal::{
    BoxConfigOp, CommitToken, MatchSel, RouterOp, Wal, WalConfig, WalError, WalFrame, WalReader,
    WalRecord, WalWriter, FRAME_CRC_LEN, FRAME_HEADER_LEN,
};
