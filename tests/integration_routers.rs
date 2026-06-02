//! Phase-3 §2 — in-process integration tests for routers (fan-out) over real HTTP.
//!
//! Black-topic coverage of the documented `/v0/routers` contract (API §6) plus the
//! end-to-end forwarding semantics, exercised against a live bound server via the
//! shared [`common::Harness`]:
//!
//!   * PUT create (`201`) / idempotent re-PUT (`200`) / changed re-PUT (`200`).
//!   * GET a router (`forwarded_total`, config echo) and `404 router_not_found`.
//!   * list (filter by `source`/`dest`/`prefix`).
//!   * DELETE (`deleted:true` once, idempotent `deleted:false`).
//!   * fan-out: a write to `source` appears in `dest` with `$node` preserved and
//!     `$tag` carried per `preserve_tag`; `preserve_node:false`/`preserve_tag:false`
//!     clear them.
//!   * per-source FIFO ordering across a fan-out.
//!   * cycle creation rejected `409 router_cycle` with a `detail.cycle` path.
//!   * `allow_cycle` mirror terminates via the hop cap (no infinite forwarding).
//!   * deleting a router stops further forwarding while already-forwarded records
//!     remain in `dest`.
//!   * `create_dest` behavior (auto-create on; `404 topic_not_found` when off + missing).
//!
//! All assertions are timing-independent (the harness runs a `SystemClock`, so no
//! TTL/priority correctness is asserted here — see the engine unit/property tests).

mod common;

use common::{Harness, StatusCode};
use serde_json::{json, Value};

// --------------------------------------------------------------------------
// Small helpers
// --------------------------------------------------------------------------

/// Diff `topic` from the earliest seq as an unrelated node `consumer` (so node
/// loop-prevention never hides forwarded records), with `$tag` included.
/// Returns the parsed `records` array.
fn diff_all(h: &Harness, topic_name: &str) -> Vec<Value> {
    let (status, body) = h.post(
        &format!("/v0/topics/{topic_name}/diff"),
        json!({ "from_seq": 0, "limit": 1000, "node": "consumer", "include_tags": true }),
    );
    assert_eq!(status, StatusCode::OK, "diff {topic_name}: {body}");
    body["records"].as_array().cloned().unwrap_or_default()
}

/// Write one record to `topic` (origin node + optional tag); assert 2xx.
fn write_one(h: &Harness, topic_name: &str, data: Value, node: Option<&str>, tag: Option<&str>) {
    let mut rec = json!({ "data": data });
    if let Some(t) = tag {
        rec["tag"] = json!(t);
    }
    let mut body = json!({ "records": [rec] });
    if let Some(n) = node {
        body["node"] = json!(n);
    }
    let (status, resp) = h.post(&format!("/v0/topics/{topic_name}"), body);
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "write to {topic_name} expected 2xx, got {status}: {resp}"
    );
}

// --------------------------------------------------------------------------
// PUT create / idempotency
// --------------------------------------------------------------------------

#[test]
fn put_router_create_then_idempotent_and_changed() {
    let h = Harness::start();

    // New router -> 201, with the documented response fields echoed.
    let (status, body) = h.put(
        "/v0/routers/jobs->audit",
        json!({ "source": "jobs", "dest": "audit" }),
    );
    assert_eq!(status, StatusCode::CREATED, "first PUT must 201: {body}");
    assert_eq!(body["router"], "jobs->audit");
    assert_eq!(body["created"], true);
    assert_eq!(body["source"], "jobs");
    assert_eq!(body["dest"], "audit");
    // Documented defaults surface in the response.
    assert_eq!(body["preserve_node"], true);
    assert_eq!(body["preserve_tag"], true);
    assert_eq!(body["allow_cycle"], false);
    assert!(body["filter"].is_null(), "filter defaults to null");

    // Identical re-PUT -> 200, created:false (idempotent upsert).
    let (status, body) = h.put(
        "/v0/routers/jobs->audit",
        json!({ "source": "jobs", "dest": "audit" }),
    );
    assert_eq!(status, StatusCode::OK, "identical re-PUT must 200");
    assert_eq!(body["created"], false);

    // Changed re-PUT (flip preserve_tag) -> still 200, created:false, change applied.
    let (status, body) = h.put(
        "/v0/routers/jobs->audit",
        json!({ "source": "jobs", "dest": "audit", "preserve_tag": false }),
    );
    assert_eq!(status, StatusCode::OK, "changed re-PUT must 200");
    assert_eq!(body["created"], false);
    assert_eq!(body["preserve_tag"], false, "changed config applied");

    // GET reflects the updated config.
    let (status, body) = h.get("/v0/routers/jobs->audit");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["preserve_tag"], false);
}

#[test]
fn put_router_source_equals_dest_is_400() {
    let h = Harness::start();
    let (status, body) = h.put(
        "/v0/routers/loop",
        json!({ "source": "same", "dest": "same" }),
    );
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "source==dest -> 400: {body}"
    );
    assert_eq!(body["error"]["code"], "invalid_request");
}

// --------------------------------------------------------------------------
// GET / list / 404
// --------------------------------------------------------------------------

#[test]
fn get_router_and_404_when_missing() {
    let h = Harness::start();

    // Missing -> 404 router_not_found.
    let (status, body) = h.get("/v0/routers/nope->void");
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], "router_not_found");

    h.put("/v0/routers/a->b", json!({ "source": "a", "dest": "b" }));

    let (status, body) = h.get("/v0/routers/a->b");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["router"], "a->b");
    assert_eq!(body["source"], "a");
    assert_eq!(body["dest"], "b");
    assert_eq!(body["forwarded_total"], 0, "nothing forwarded yet");

    // After a write to the source, forwarded_total advances.
    write_one(&h, "a", json!({ "k": 1 }), Some("o"), None);
    write_one(&h, "a", json!({ "k": 2 }), Some("o"), None);
    let (_status, body) = h.get("/v0/routers/a->b");
    assert_eq!(body["forwarded_total"], 2, "two records forwarded");
}

#[test]
fn list_routers_filters_by_source_and_dest() {
    let h = Harness::start();
    // v2 contract: a derived dest is single-source, so two routers cannot share a
    // dest. The topology below keeps source/dest/prefix filter coverage (source `a`
    // fans to two distinct dests `b`,`c`; a separate `x->d`) under that rule.
    h.put("/v0/routers/a->b", json!({ "source": "a", "dest": "b" }));
    h.put("/v0/routers/a->c", json!({ "source": "a", "dest": "c" }));
    h.put("/v0/routers/x->d", json!({ "source": "x", "dest": "d" }));

    // No filter -> all three.
    let (status, body) = h.get("/v0/routers");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["routers"].as_array().unwrap().len(), 3);

    // Filter by source=a -> two (fan-out from one source to two dests).
    let (_s, body) = h.get("/v0/routers?source=a");
    let names: Vec<&str> = body["routers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["router"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        vec!["a->b", "a->c"],
        "source filter + sorted by name"
    );

    // Filter by dest=c -> one (a single-source derived dest).
    let (_s, body) = h.get("/v0/routers?dest=c");
    let names: Vec<&str> = body["routers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["router"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["a->c"]);

    // Filter by dest=d -> one (the unrelated x->d edge).
    let (_s, body) = h.get("/v0/routers?dest=d");
    let names: Vec<&str> = body["routers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["router"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["x->d"]);

    // Prefix filter narrows to the `a->` namespace.
    let (_s, body) = h.get("/v0/routers?prefix=a->");
    assert_eq!(body["routers"].as_array().unwrap().len(), 2);

    // The single-source rule is enforced: a second router with a DIFFERENT source
    // into an existing derived dest is refused 409 router_dest_fan_in.
    let (status, body) = h.put("/v0/routers/x->c", json!({ "source": "x", "dest": "c" }));
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "multi-source fan-in into dest c is refused under v2: {body}"
    );
    assert_eq!(body["error"]["code"], "topic_exists_incompatible");
    assert_eq!(body["error"]["detail"]["reason"], "router_dest_fan_in");
}

// --------------------------------------------------------------------------
// DELETE (idempotent) — and forwarding stops while forwarded records remain
// --------------------------------------------------------------------------

#[test]
fn delete_router_is_idempotent_and_stops_forwarding() {
    let h = Harness::start();
    h.put(
        "/v0/routers/src->dst",
        json!({ "source": "src", "dest": "dst" }),
    );

    // Forward two records, confirm they land in dst.
    write_one(&h, "src", json!({ "i": 1 }), Some("o"), None);
    write_one(&h, "src", json!({ "i": 2 }), Some("o"), None);
    assert_eq!(diff_all(&h, "dst").len(), 2, "both forwarded before delete");

    // Delete the router -> deleted:true.
    let (status, body) = h.delete("/v0/routers/src->dst");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["router"], "src->dst");
    assert_eq!(body["deleted"], true);

    // Re-delete -> idempotent deleted:false.
    let (status, body) = h.delete("/v0/routers/src->dst");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deleted"], false);

    // A further write to src is NOT forwarded; the two prior copies remain in dst.
    write_one(&h, "src", json!({ "i": 3 }), Some("o"), None);
    let recs = diff_all(&h, "dst");
    assert_eq!(
        recs.len(),
        2,
        "forwarding stopped; already-forwarded records remain"
    );
    let vals: Vec<i64> = recs
        .iter()
        .map(|r| r["data"]["i"].as_i64().unwrap())
        .collect();
    assert_eq!(vals, vec![1, 2], "the third record never reached dst");

    // The router is gone from GET as well.
    let (status, _b) = h.get("/v0/routers/src->dst");
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// --------------------------------------------------------------------------
// Fan-out: $node preserved, $tag per preserve_tag, FIFO ordering
// --------------------------------------------------------------------------

#[test]
fn fanout_preserves_node_and_tag_in_fifo_order() {
    let h = Harness::start();
    h.put(
        "/v0/routers/feed->mirror",
        json!({ "source": "feed", "dest": "mirror" }),
    );

    // Three tagged records from one origin node, written in order.
    write_one(&h, "feed", json!({ "n": 1 }), Some("origin-1"), Some("t1"));
    write_one(&h, "feed", json!({ "n": 2 }), Some("origin-1"), Some("t2"));
    write_one(&h, "feed", json!({ "n": 3 }), Some("origin-1"), Some("t3"));

    let recs = diff_all(&h, "mirror");
    assert_eq!(recs.len(), 3, "all three fanned out");

    // Per-source FIFO: dest order matches source commit order.
    let order: Vec<i64> = recs
        .iter()
        .map(|r| r["data"]["n"].as_i64().unwrap())
        .collect();
    assert_eq!(
        order,
        vec![1, 2, 3],
        "per-source FIFO preserved through fan-out"
    );

    // $node preserved (loop-prevention key carries through).
    for r in &recs {
        assert_eq!(r["$node"], "origin-1", "preserve_node default keeps $node");
    }
    // $tag carried through (preserve_tag default true; include_tags requested on diff).
    let tags: Vec<&str> = recs.iter().map(|r| r["$tag"].as_str().unwrap()).collect();
    assert_eq!(tags, vec!["t1", "t2", "t3"]);

    // dest assigned its own fresh seqs (independent log), ascending & contiguous.
    let seqs: Vec<u64> = recs.iter().map(|r| r["$seq"].as_u64().unwrap()).collect();
    assert_eq!(seqs, vec![1, 2, 3], "dest log is independent, starts at 1");
}

#[test]
fn fanout_preserve_node_and_tag_false_clears_them() {
    let h = Harness::start();
    h.put(
        "/v0/routers/s->d",
        json!({ "source": "s", "dest": "d", "preserve_node": false, "preserve_tag": false }),
    );
    write_one(&h, "s", json!({ "v": 1 }), Some("origin"), Some("the-tag"));

    let recs = diff_all(&h, "d");
    assert_eq!(recs.len(), 1);
    assert!(
        recs[0].get("$node").is_none(),
        "preserve_node:false clears $node, got {:?}",
        recs[0].get("$node")
    );
    assert!(
        recs[0].get("$tag").is_none(),
        "preserve_tag:false clears $tag even with include_tags, got {:?}",
        recs[0].get("$tag")
    );
    assert_eq!(recs[0]["data"]["v"], 1, "data still forwarded verbatim");
}

#[test]
fn fanout_to_multiple_dests_each_gets_a_copy() {
    let h = Harness::start();
    h.put(
        "/v0/routers/feed->a",
        json!({ "source": "feed", "dest": "a" }),
    );
    h.put(
        "/v0/routers/feed->b",
        json!({ "source": "feed", "dest": "b" }),
    );

    write_one(&h, "feed", json!({ "x": 1 }), Some("o"), None);

    assert_eq!(diff_all(&h, "a").len(), 1, "dest a got a copy");
    assert_eq!(diff_all(&h, "b").len(), 1, "dest b got a copy");
}

// --------------------------------------------------------------------------
// Cycle rejection + allow_cycle hop-cap termination
// --------------------------------------------------------------------------

#[test]
fn cycle_creation_rejected_409_with_detail() {
    let h = Harness::start();
    h.put("/v0/routers/a->b", json!({ "source": "a", "dest": "b" }));
    h.put("/v0/routers/b->c", json!({ "source": "b", "dest": "c" }));

    // c->a would close a->b->c->a.
    let (status, body) = h.put("/v0/routers/c->a", json!({ "source": "c", "dest": "a" }));
    assert_eq!(status, StatusCode::CONFLICT, "cycle -> 409: {body}");
    assert_eq!(body["error"]["code"], "router_cycle");

    // The offending cycle path is reported in detail.cycle.
    let cycle = body["error"]["detail"]["cycle"]
        .as_array()
        .expect("detail.cycle path present");
    assert!(
        cycle.len() >= 2,
        "cycle path lists the topic names: {cycle:?}"
    );
    let path: Vec<&str> = cycle.iter().map(|v| v.as_str().unwrap()).collect();
    // Path starts at the new source and ends back at it (a -> ... -> a).
    assert_eq!(path.first(), Some(&"c"), "cycle reported from new source");
    assert_eq!(path.last(), Some(&"c"), "cycle closes back on itself");

    // The rejected router was NOT created.
    let (status, _b) = h.get("/v0/routers/c->a");
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "rejected cycle router not persisted"
    );
}

#[test]
fn allow_cycle_mirror_terminates_via_hop_cap() {
    let h = Harness::start();
    // Two-topic mirror a<->b, both edges allow_cycle.
    h.put(
        "/v0/routers/a->b",
        json!({ "source": "a", "dest": "b", "allow_cycle": true }),
    );
    h.put(
        "/v0/routers/b->a",
        json!({ "source": "b", "dest": "a", "allow_cycle": true }),
    );

    // A single write to `a` would loop forever without the hop cap. It must
    // return promptly and leave only a bounded number of copies (no hang, no
    // unbounded growth). The harness request timeout (30s) would surface a hang.
    write_one(&h, "a", json!({ "x": 1 }), Some("A"), None);

    let (status, a_state) = h.get("/v0/topics/a");
    assert_eq!(status, StatusCode::OK);
    let (_s, b_state) = h.get("/v0/topics/b");

    let a_head = a_state["head_seq"].as_u64().unwrap();
    let b_head = b_state["head_seq"].as_u64().unwrap();
    // MAX_ROUTER_HOPS = 8: a handful of copies at most, never unbounded.
    assert!(
        (1..=16).contains(&a_head),
        "a head bounded by hop cap, got {a_head}"
    );
    assert!(
        (1..=16).contains(&b_head),
        "b head bounded by hop cap, got {b_head}"
    );

    // $node is preserved through the cycle (the loop-prevention key stays intact).
    let recs = diff_all(&h, "b");
    assert!(!recs.is_empty(), "b received at least the first hop");
    assert_eq!(recs[0]["$node"], "A", "node preserved across the mirror");
}

#[test]
fn allow_cycle_re_put_of_self_edge_is_not_a_cycle() {
    // An idempotent re-PUT of an existing DAG edge must not false-positive on its
    // own edge (the engine excludes the router's own edge from the cycle check).
    let h = Harness::start();
    let (status, _b) = h.put("/v0/routers/a->b", json!({ "source": "a", "dest": "b" }));
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = h.put("/v0/routers/a->b", json!({ "source": "a", "dest": "b" }));
    assert_eq!(status, StatusCode::OK, "re-PUT must not be a cycle: {body}");
    assert_eq!(body["created"], false);
}

// --------------------------------------------------------------------------
// create_dest behavior
// --------------------------------------------------------------------------

#[test]
fn create_dest_true_auto_creates_destination() {
    let h = Harness::start();
    // dest "freshdst" does not exist yet.
    let (status, _b) = h.get("/v0/topics/freshdst");
    assert_eq!(status, StatusCode::NOT_FOUND, "dest absent before router");

    // create_dest defaults true -> PUT succeeds and materializes the dest.
    let (status, _b) = h.put(
        "/v0/routers/srcx->freshdst",
        json!({ "source": "srcx", "dest": "freshdst" }),
    );
    assert_eq!(status, StatusCode::CREATED);

    // dest now exists (state read, which never auto-creates, returns 200).
    let (status, body) = h.get("/v0/topics/freshdst");
    assert_eq!(
        status,
        StatusCode::OK,
        "create_dest auto-created the dest: {body}"
    );
    // source auto-created too.
    let (status, _b) = h.get("/v0/topics/srcx");
    assert_eq!(status, StatusCode::OK, "source auto-created as well");
}

#[test]
fn create_dest_false_on_missing_dest_is_404() {
    let h = Harness::start();
    let (status, body) = h.put(
        "/v0/routers/srcy->missingdst",
        json!({ "source": "srcy", "dest": "missingdst", "create_dest": false }),
    );
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "create_dest:false + missing -> 404: {body}"
    );
    assert_eq!(body["error"]["code"], "topic_not_found");

    // The router was not created.
    let (status, _b) = h.get("/v0/routers/srcy->missingdst");
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[test]
fn create_dest_false_with_existing_dest_succeeds() {
    let h = Harness::start();
    // Pre-create the dest, then a create_dest:false router must attach fine.
    let (status, _b) = h.put("/v0/topics/predst", json!({}));
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = h.put(
        "/v0/routers/srcz->predst",
        json!({ "source": "srcz", "dest": "predst", "create_dest": false }),
    );
    assert_eq!(
        status,
        StatusCode::CREATED,
        "existing dest -> create ok: {body}"
    );

    // Forwarding works into the pre-existing dest.
    write_one(&h, "srcz", json!({ "ok": true }), Some("o"), None);
    assert_eq!(diff_all(&h, "predst").len(), 1);
}
