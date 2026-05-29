//! Glue holding the engine's owned [`Wal`]. Dropping a [`WalHandle`] drains and
//! fsyncs the writer's queue and joins the writer thread (via `Wal`'s `Drop`),
//! so engine teardown never loses a committed batch.

use crate::storage::Wal;

/// Owns the running WAL writer for the lifetime of a durable engine. The engine
/// keeps this in an `Arc` alongside a cloned [`crate::storage::WalWriter`]; the
/// last drop shuts the writer down cleanly.
pub struct WalHandle {
    _wal: Wal,
}

impl WalHandle {
    pub(crate) fn new(wal: Wal) -> Self {
        WalHandle { _wal: wal }
    }
}
