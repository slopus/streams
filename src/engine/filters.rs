//! The per-record read filter pipeline (DESIGN §7.3).
//!
//! Deletion is no longer a standing read-time filter — it is applied eagerly by
//! [`crate::engine::box_state::BoxState::apply_delete`], which marks slots
//! `deleted`. The read pipeline therefore only composes three gates per
//! candidate record: the **TTL** gate (involuntary expiry), the **deleted**
//! check (voluntary, silent), and **node loop-prevention** (silent). A skipped
//! record still advances the reader's cursor.

use crate::types::NodeFilter;

/// Outcome of evaluating one record against the read pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadDecision {
    /// Deliver the record.
    Deliver,
    /// Permanently deleted: silently skip (advance cursor). Never a tombstone.
    Deleted,
    /// Node loop-prevention: silently skip (advance cursor).
    NodeFiltered,
    /// TTL-expired: not delivered; contributes to the expiry floor (tombstone).
    Expired,
}

/// Evaluate one record's `(ts, deleted, node)` against the read pipeline
/// (DESIGN §7.3): TTL gate → deleted skip → node filter. Retention/tombstone is
/// handled by the caller before this.
pub fn evaluate(
    node_filter: Option<&NodeFilter>,
    ttl_ms: u64,
    now_ms: i64,
    record_ts: i64,
    deleted: bool,
    record_node: Option<&str>,
) -> ReadDecision {
    // 1. TTL gate — a record is expired when `now - $ts > ttl_ms` (strict).
    if ttl_ms > 0 && now_ms.saturating_sub(record_ts) > ttl_ms as i64 {
        return ReadDecision::Expired;
    }
    // 2. Deleted skip — permanent, silent voluntary deletion (DESIGN §7).
    if deleted {
        return ReadDecision::Deleted;
    }
    // 3. Node loop-prevention — drop if `$node` ∈ reader node set (DESIGN §6).
    if let (Some(filter), Some(node)) = (node_filter, record_node) {
        if filter.matches(node) {
            return ReadDecision::NodeFiltered;
        }
    }
    ReadDecision::Deliver
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeFilter;

    #[test]
    fn read_pipeline_order_ttl_then_deleted_then_node() {
        let nf = NodeFilter::One("me".to_string());

        // Expired wins over everything.
        assert_eq!(
            evaluate(Some(&nf), 1000, 5000, 1000, true, Some("me")),
            ReadDecision::Expired
        );
        // Not expired, deleted → deleted (before node).
        assert_eq!(
            evaluate(Some(&nf), 1000, 1500, 1000, true, Some("me")),
            ReadDecision::Deleted
        );
        // Not expired, not deleted, node matches → node-filtered.
        assert_eq!(
            evaluate(Some(&nf), 0, 1500, 1000, false, Some("me")),
            ReadDecision::NodeFiltered
        );
        // Survives all gates.
        assert_eq!(
            evaluate(Some(&nf), 0, 1500, 1000, false, Some("other")),
            ReadDecision::Deliver
        );
    }
}
