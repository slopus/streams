//! Glue holding the engine's owned [`ShardedWal`]. Dropping a [`WalHandle`] drains
//! and fsyncs every shard's writer queue and joins every writer thread (via each
//! shard `Wal`'s `Drop`), so engine teardown never loses a committed batch.

use crate::storage::ShardedWal;

/// Owns the running WAL shard writers for the lifetime of a durable engine. The
/// engine keeps this in an `Arc` alongside a cloned
/// [`crate::storage::ShardedWalWriter`]; the last drop shuts every shard's writer
/// down cleanly.
pub struct WalHandle {
    _wal: ShardedWal,
}

impl WalHandle {
    pub(crate) fn new(wal: ShardedWal) -> Self {
        WalHandle { _wal: wal }
    }
}
