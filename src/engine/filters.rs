//! Tag-delete filter set and the per-record read filter pipeline.
//!
//! A [`FilterSet`] holds the box's tag-delete rules as an exact `HashSet` plus
//! a sorted prefix list, evaluated per candidate record (ARCHITECTURE §5,
//! DESIGN §7.2). Node loop-prevention and the TTL gate compose into the same
//! per-record read pipeline (DESIGN §7.3).

use crate::types::{Filter, FilterOp, NodeFilter};
use std::collections::HashSet;

/// The active tag-delete rule set for a box. Cheap to clone for copy-on-write
/// publication (held behind an `ArcSwap`-like swap in phase 4; a plain field
/// guarded by the box lock in phase 2).
#[derive(Debug, Clone, Default)]
pub struct FilterSet {
    /// Exact `Eq` tags → O(1) membership.
    exact: HashSet<String>,
    /// `Glob` prefixes, sorted for binary search.
    prefixes: Vec<String>,
}

impl FilterSet {
    pub fn new() -> Self {
        FilterSet::default()
    }

    /// Total number of active rules (exact + prefix).
    pub fn len(&self) -> usize {
        self.exact.len() + self.prefixes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.prefixes.is_empty()
    }

    /// Add a filter. Returns `true` if it was newly added (idempotent/additive).
    pub fn add(&mut self, filter: &Filter) -> bool {
        match filter.op {
            FilterOp::Eq => self.exact.insert(filter.value.clone()),
            FilterOp::Glob => {
                // Keep the prefix list sorted for binary search; dedupe.
                match self.prefixes.binary_search(&filter.value) {
                    Ok(_) => false,
                    Err(pos) => {
                        self.prefixes.insert(pos, filter.value.clone());
                        true
                    }
                }
            }
        }
    }

    /// Whether the given tag matches any active rule.
    ///
    /// Exact match is O(1). For prefix rules, every stored prefix that matches
    /// `tag` is itself one of the byte-prefixes of `tag` (`tag[..k]`). There are
    /// at most `len(tag)+1` such prefixes, so we binary-search the sorted prefix
    /// list for each — O(len(tag) · log P), bounded since `tag` ≤ 256 bytes and
    /// never scans the full list. (DESIGN §7.2.)
    pub fn matches(&self, tag: &str) -> bool {
        if self.exact.contains(tag) {
            return true;
        }
        if self.prefixes.is_empty() {
            return false;
        }
        // The empty prefix (`Glob "*"`) matches everything.
        if self.prefixes.first().map(|p| p.is_empty()).unwrap_or(false) {
            return true;
        }
        let bytes = tag.as_bytes();
        for k in 1..=bytes.len() {
            // Only split on a UTF-8 char boundary to form a valid &str prefix.
            if !tag.is_char_boundary(k) {
                continue;
            }
            let candidate = &tag[..k];
            if self.prefixes.binary_search_by(|p| p.as_str().cmp(candidate)).is_ok() {
                return true;
            }
        }
        false
    }

    /// Reconstruct the canonical filter tuples for listing.
    pub fn to_filters(&self) -> Vec<Filter> {
        let mut out: Vec<Filter> = self
            .exact
            .iter()
            .map(|t| Filter {
                op: FilterOp::Eq,
                value: t.clone(),
            })
            .collect();
        out.extend(self.prefixes.iter().map(|p| Filter {
            op: FilterOp::Glob,
            value: p.clone(),
        }));
        out
    }
}

/// Outcome of evaluating one record against the read pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadDecision {
    /// Deliver the record.
    Deliver,
    /// Tag-deleted: silently skip (advance cursor), or surface with `$deleted`
    /// when `include_deleted` is set.
    TagDeleted,
    /// Node loop-prevention: silently skip (advance cursor).
    NodeFiltered,
    /// TTL-expired: not delivered; contributes to the expiry floor.
    Expired,
}

/// Evaluate one record's `(ts, tag, node)` against the read pipeline
/// (DESIGN §7.3): TTL gate → tag-delete → node filter. Retention/tombstone is
/// handled by the caller before this.
pub fn evaluate(
    filters: &FilterSet,
    node_filter: Option<&NodeFilter>,
    ttl_ms: u64,
    now_ms: i64,
    record_ts: i64,
    record_tag: Option<&str>,
    record_node: Option<&str>,
) -> ReadDecision {
    // 1. TTL gate — a record is expired when `now - $ts > ttl_ms` (strict).
    if ttl_ms > 0 && now_ms.saturating_sub(record_ts) > ttl_ms as i64 {
        return ReadDecision::Expired;
    }
    // 2. Tag-delete filter — records with no tag are never matched (DESIGN §7).
    if let Some(tag) = record_tag {
        if !filters.is_empty() && filters.matches(tag) {
            return ReadDecision::TagDeleted;
        }
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
    use crate::types::Filter;

    #[test]
    fn exact_and_prefix_membership() {
        let mut s = FilterSet::new();
        assert!(s.add(&Filter::from_shorthand("exact")));
        assert!(!s.add(&Filter::from_shorthand("exact"))); // idempotent
        assert!(s.add(&Filter::from_shorthand("a*")));
        assert!(s.add(&Filter::from_shorthand("ab*")));

        assert!(s.matches("exact"));
        assert!(!s.matches("exac"));
        assert!(s.matches("abc")); // matches "ab*" and "a*"
        // The tricky case: stored "a*" and "ab*", tag "ac" — "ab" is not a
        // prefix of "ac" but "a" is, so it must match.
        assert!(s.matches("ac"));
        assert!(s.matches("a"));
        assert!(!s.matches("b"));
        assert_eq!(s.len(), 3);
    }

    #[test]
    fn empty_prefix_matches_everything() {
        let mut s = FilterSet::new();
        assert!(s.add(&Filter::from_shorthand("*"))); // Glob with empty prefix
        assert!(s.matches("anything"));
        assert!(s.matches(""));
    }

    #[test]
    fn read_pipeline_order_ttl_then_tag_then_node() {
        use crate::types::NodeFilter;
        let mut s = FilterSet::new();
        s.add(&Filter::from_shorthand("drop"));
        let nf = NodeFilter::One("me".to_string());

        // Expired wins over everything.
        assert_eq!(
            evaluate(&s, Some(&nf), 1000, 5000, 1000, Some("drop"), Some("me")),
            ReadDecision::Expired
        );
        // Not expired, tag matches → tag-deleted (before node).
        assert_eq!(
            evaluate(&s, Some(&nf), 1000, 1500, 1000, Some("drop"), Some("me")),
            ReadDecision::TagDeleted
        );
        // Not expired, tag doesn't match, node matches → node-filtered.
        assert_eq!(
            evaluate(&s, Some(&nf), 0, 1500, 1000, Some("keep"), Some("me")),
            ReadDecision::NodeFiltered
        );
        // Survives all gates.
        assert_eq!(
            evaluate(&s, Some(&nf), 0, 1500, 1000, Some("keep"), Some("other")),
            ReadDecision::Deliver
        );
    }
}
