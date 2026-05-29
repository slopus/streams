//! Integration tests for the deletion model over real HTTP (API §5):
//! `POST /v0/boxes/:box/delete` with `before_seq` and/or tag `match`.
//!
//! Deletion is permanent, effective-immediately, **silent** (never a tombstone),
//! and **point-in-time** (not a standing filter). A delete advances `earliest_seq`
//! but never `evict_floor`, so reading across a purely-deleted gap is silent while
//! a separate cap/TTL eviction on the same box still yields a `reason:"cap"`
//! tombstone (the dual watermark). These flows mirror the engine white-box unit
//! tests but assert the documented wire contract end-to-end through axum.
//!
//! Each test boots its own `Harness` (real bound server, `SystemClock`), so the
//! suite is isolated and parallel-safe. No wall-clock sleeps are used for
//! correctness; cap eviction is driven purely by appends, not time.

mod common;

use common::{Harness, StatusCode};
use serde_json::{json, Value};

/// Append `records` to `box_name`, asserting the write succeeded (`200`/`201`),
/// and return the parsed response body.
fn write(h: &Harness, box_name: &str, records: Value) -> Value {
    let (status, body) = h.post(&format!("/v0/boxes/{box_name}"), json!({ "records": records }));
    assert!(
        status == StatusCode::OK || status == StatusCode::CREATED,
        "write to {box_name} should succeed, got {status}: {body}"
    );
    body
}

/// `POST .../delete`, asserting `200`, and return the parsed response body.
fn delete_ok(h: &Harness, box_name: &str, sel: Value) -> Value {
    let (status, body) = h.post(&format!("/v0/boxes/{box_name}/delete"), sel);
    assert_eq!(status, StatusCode::OK, "delete should be 200, got {body}");
    body
}

/// `POST .../diff` from `from_seq` as an unrelated node so loop-prevention never
/// hides records; include tags so tag-based assertions can read `$tag`.
fn diff(h: &Harness, box_name: &str, from_seq: u64) -> Value {
    let (status, body) = h.post(
        &format!("/v0/boxes/{box_name}/diff"),
        json!({ "from_seq": from_seq, "limit": 1000, "node": "reader", "include_tags": true }),
    );
    assert_eq!(status, StatusCode::OK, "diff should be 200, got {body}");
    body
}

/// The ascending `$seq` list of records returned by a diff body.
fn seqs(diff_body: &Value) -> Vec<u64> {
    diff_body["records"]
        .as_array()
        .expect("records array")
        .iter()
        .map(|r| r["$seq"].as_u64().expect("$seq u64"))
        .collect()
}

/// Assert a delete response carries every documented field with sane types and
/// the expected scalar values for `deleted`/`earliest_seq`/`head_seq`/`count`.
fn assert_delete_shape(
    body: &Value,
    box_name: &str,
    deleted: u64,
    earliest_seq: u64,
    head_seq: u64,
    count: u64,
) {
    assert_eq!(body["box"], box_name, "box echoed: {body}");
    assert_eq!(body["deleted"], deleted, "deleted: {body}");
    assert_eq!(body["earliest_seq"], earliest_seq, "earliest_seq: {body}");
    assert_eq!(body["head_seq"], head_seq, "head_seq: {body}");
    assert_eq!(body["count"], count, "count: {body}");
    // `bytes` is approximate under lazy eviction; just assert it is present and a
    // number (it must reflect post-delete occupancy, so it is 0 iff count is 0).
    assert!(body["bytes"].is_u64(), "bytes must be a number: {body}");
    if count == 0 {
        assert_eq!(body["bytes"], 0, "no live records ⇒ 0 bytes: {body}");
    } else {
        assert!(body["bytes"].as_u64().unwrap() > 0, "live records ⇒ >0 bytes: {body}");
    }
    assert!(
        body["performance"]["server_total_ms"].is_number(),
        "performance block present: {body}"
    );
}

// ---------------------------------------------------------------------------
// (a) before_seq snapshot delete — silent, earliest advances, count drops.
// ---------------------------------------------------------------------------
#[test]
fn before_seq_snapshot_delete_is_silent() {
    let h = Harness::start();
    write(
        &h,
        "snap",
        json!([{ "data": 1 }, { "data": 2 }, { "data": 3 }, { "data": 4 }, { "data": 5 }]),
    );

    // Delete every record with $seq < 3 (a snapshot/compaction). Removes 1,2.
    let body = delete_ok(&h, "snap", json!({ "before_seq": 3 }));
    // head stays 5; earliest advances to 3; count drops to 3.
    assert_delete_shape(&body, "snap", 2, 3, 5, 3);

    // A fresh reader from 0 sees only 3,4,5 with NO tombstone (delete is silent).
    let d = diff(&h, "snap", 0);
    assert_eq!(seqs(&d), vec![3, 4, 5]);
    assert_eq!(d["tombstone"], Value::Null, "purely-deleted prefix is silent");
    assert_eq!(d["earliest_seq"], 3);
    assert_eq!(d["caught_up"], true);
}

// ---------------------------------------------------------------------------
// (b) match Eq + match Glob prefix delete — silent, count drops; the Glob
//     boundary case: chat-42:* must NOT match chat-420:.
// ---------------------------------------------------------------------------
#[test]
fn match_eq_and_glob_prefix_delete() {
    let h = Harness::start();
    write(
        &h,
        "jobs",
        json!([
            { "data": 1, "tag": "tenant42:job-1" },
            { "data": 2, "tag": "tenant42:job-2" },
            { "data": 3, "tag": "other:job-9" },
            { "data": 4 }
        ]),
    );

    // Exact (Eq) delete of job-1 (seq 1, the front record ⇒ earliest advances).
    let r1 = delete_ok(&h, "jobs", json!({ "match": ["tag", "Eq", "tenant42:job-1"] }));
    assert_delete_shape(&r1, "jobs", 1, 2, 4, 3);
    let d1 = diff(&h, "jobs", 0);
    assert_eq!(seqs(&d1), vec![2, 3, 4], "seq 1 removed from reads");
    assert_eq!(d1["tombstone"], Value::Null, "delete is silent");
    assert_eq!(d1["caught_up"], true);

    // Prefix (Glob) delete of all tenant42:* ⇒ removes seq 2 (1 already gone).
    let r2 = delete_ok(&h, "jobs", json!({ "match": ["tag", "Glob", "tenant42:*"] }));
    assert_delete_shape(&r2, "jobs", 1, 3, 4, 2);
    let d2 = diff(&h, "jobs", 0);
    assert_eq!(seqs(&d2), vec![3, 4], "tenant42:* gone; other tag + untagged stay");
    assert_eq!(d2["tombstone"], Value::Null);
}

// ---------------------------------------------------------------------------
// (b') Glob boundary: chat-42:* must NOT match chat-420: (prefix is literal,
//      not a path-segment match). A bounded range scan, not a whole-log scan.
// ---------------------------------------------------------------------------
#[test]
fn glob_prefix_does_not_overmatch_sibling_prefix() {
    let h = Harness::start();
    write(
        &h,
        "tix",
        json!([
            { "data": 1, "tag": "chat-42:a" },   // seq 1 — matches chat-42:*
            { "data": 2, "tag": "chat-42:b" },   // seq 2 — matches chat-42:*
            { "data": 3, "tag": "chat-420:c" },  // seq 3 — does NOT match chat-42:*
            { "data": 4, "tag": "zzz" }          // seq 4 — unrelated
        ]),
    );

    let r = delete_ok(&h, "tix", json!({ "match": ["tag", "Glob", "chat-42:*"] }));
    assert_delete_shape(&r, "tix", 2, 3, 4, 2);

    let d = diff(&h, "tix", 0);
    let tags: Vec<&str> = d["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["$tag"].as_str().unwrap())
        .collect();
    assert_eq!(
        tags,
        vec!["chat-420:c", "zzz"],
        "chat-42:* must not delete the sibling prefix chat-420:c"
    );
    assert_eq!(d["tombstone"], Value::Null);
}

// ---------------------------------------------------------------------------
// (c) point-in-time: a same-tag record written AFTER a delete survives — a
//     delete is not a standing filter.
// ---------------------------------------------------------------------------
#[test]
fn delete_is_point_in_time() {
    let h = Harness::start();
    write(&h, "chat", json!([{ "data": "v1", "tag": "chat-42:msg" }])); // seq 1

    // Revoke everything currently tagged chat-42:* (just seq 1).
    let r = delete_ok(&h, "chat", json!({ "match": ["tag", "Glob", "chat-42:*"] }));
    assert_delete_shape(&r, "chat", 1, 2, 1, 0); // box now empty, earliest=head+1.

    // A LATER write with the same matching tag is unaffected by the prior delete.
    write(&h, "chat", json!([{ "data": "v2", "tag": "chat-42:msg" }])); // seq 2

    let d = diff(&h, "chat", 0);
    assert_eq!(seqs(&d), vec![2], "future matching record is not retroactively deleted");
    assert_eq!(d["records"][0]["data"], "v2");
    assert_eq!(d["tombstone"], Value::Null);
    assert_eq!(d["caught_up"], true);
}

// ---------------------------------------------------------------------------
// (d) match + before_seq AND: deletes prior versions, keeps the newer same-tag
//     record (publish v2, then compact prior msg-123 versions).
// ---------------------------------------------------------------------------
#[test]
fn match_and_before_seq_keeps_newer_same_tag() {
    let h = Harness::start();
    write(
        &h,
        "msgs",
        json!([
            { "data": "v1", "tag": "msg-123" }, // seq 1 — prior version
            { "data": "x",  "tag": "msg-999" }, // seq 2 — different tag
            { "data": "v2", "tag": "msg-123" }  // seq 3 — the new version to keep
        ]),
    );

    // Delete records with $seq < 3 AND tag == msg-123 ⇒ only seq 1.
    let r = delete_ok(
        &h,
        "msgs",
        json!({ "before_seq": 3, "match": ["tag", "Eq", "msg-123"] }),
    );
    assert_delete_shape(&r, "msgs", 1, 2, 3, 2);

    let d = diff(&h, "msgs", 0);
    assert_eq!(
        seqs(&d),
        vec![2, 3],
        "seq 1 (prior msg-123) gone; seq 2 (other tag) and seq 3 (newer msg-123) kept"
    );
    // seq 2 keeps its tag; seq 3 is the surviving newer msg-123.
    assert_eq!(d["records"][1]["$tag"], "msg-123");
    assert_eq!(d["records"][1]["data"], "v2");
    assert_eq!(d["tombstone"], Value::Null);
}

// ---------------------------------------------------------------------------
// (e) DUAL WATERMARK: a delete is silent (advances earliest_seq, not
//     evict_floor) while a cap eviction on the SAME box still yields a
//     `reason:"cap"` tombstone.
// ---------------------------------------------------------------------------
#[test]
fn delete_silent_but_cap_eviction_still_tombstones() {
    let h = Harness::start();
    // Bounded box: cap_records = 4, discard old (evicts oldest on overflow).
    let (status, _) = h.put("/v0/boxes/dual", json!({ "cap_records": 4, "discard": "old" }));
    assert!(status == StatusCode::CREATED || status == StatusCode::OK);

    // Write 4 (seqs 1..=4) — exactly at cap, nothing evicted yet.
    write(&h, "dual", json!([{ "data": 1 }, { "data": 2 }, { "data": 3 }, { "data": 4 }]));

    // Voluntary delete of the prefix (seqs 1,2). This advances earliest but is
    // SILENT: reading across the purely-deleted gap yields tombstone:null.
    let r = delete_ok(&h, "dual", json!({ "before_seq": 3 }));
    assert_delete_shape(&r, "dual", 2, 3, 4, 2);
    let d = diff(&h, "dual", 0);
    assert_eq!(seqs(&d), vec![3, 4]);
    assert_eq!(d["tombstone"], Value::Null, "purely-deleted gap is silent");

    // Now overflow the cap with more writes (seqs 5..=10). With cap=4 and
    // discard:old, the oldest LIVE records are involuntarily evicted, advancing
    // evict_floor — so a lagging reader from 0 gets a `reason:"cap"` tombstone.
    write(
        &h,
        "dual",
        json!([{ "data": 5 }, { "data": 6 }, { "data": 7 }, { "data": 8 }, { "data": 9 }, { "data": 10 }]),
    );

    let d2 = diff(&h, "dual", 0);
    let tomb = &d2["tombstone"];
    assert!(tomb.is_object(), "cap overflow must emit a tombstone, got {d2}");
    assert_eq!(tomb["reason"], "cap", "involuntary cap loss ⇒ reason=cap");
    // The gap is authoritative; range fields are present and ordered.
    let gap_from = tomb["gap_from"].as_u64().expect("gap_from");
    let gap_to = tomb["gap_to"].as_u64().expect("gap_to");
    assert!(gap_from >= 1 && gap_from <= gap_to, "valid gap range in {tomb}");
    assert_eq!(d2["head_seq"], 10);
}

// ---------------------------------------------------------------------------
// (f) 400 invalid_request when neither selector is supplied (empty body).
// ---------------------------------------------------------------------------
#[test]
fn empty_body_is_invalid_request() {
    let h = Harness::start();
    write(&h, "b", json!([{ "data": 1 }]));

    let (status, body) = h.post("/v0/boxes/b/delete", json!({}));
    assert_eq!(status, StatusCode::BAD_REQUEST, "empty selector ⇒ 400: {body}");
    assert_eq!(body["error"]["code"], "invalid_request");
}

// ---------------------------------------------------------------------------
// (f') 404 box_not_found: deleting from a box that does not exist (delete never
//      auto-creates). The selector is valid, so this is a 404 not a 400.
// ---------------------------------------------------------------------------
#[test]
fn delete_on_absent_box_is_not_found() {
    let h = Harness::start();
    let (status, body) = h.post("/v0/boxes/ghost/delete", json!({ "before_seq": 10 }));
    assert_eq!(status, StatusCode::NOT_FOUND, "delete never auto-creates: {body}");
    assert_eq!(body["error"]["code"], "box_not_found");
}

// ---------------------------------------------------------------------------
// (g) GET .../delete is gone: only POST is routed, so GET ⇒ 405
//     method_not_allowed (there is no read-side delete endpoint).
// ---------------------------------------------------------------------------
#[test]
fn get_on_delete_path_is_method_not_allowed() {
    let h = Harness::start();
    write(&h, "jobs", json!([{ "data": 1 }]));

    let (status, body) = h.get("/v0/boxes/jobs/delete");
    assert_eq!(
        status,
        StatusCode::METHOD_NOT_ALLOWED,
        "GET .../delete must be 405, got {body}"
    );
    assert_eq!(body["error"]["code"], "method_not_allowed");
}

// ---------------------------------------------------------------------------
// (h) The bare-string `match` shorthand: "X" == ["tag","Eq","X"]; "X*" == Glob.
//     Confirms the documented shorthand works on the wire too.
// ---------------------------------------------------------------------------
#[test]
fn match_bare_string_shorthand() {
    let h = Harness::start();
    write(
        &h,
        "sh",
        json!([
            { "data": 1, "tag": "exact-tag" },  // seq 1
            { "data": 2, "tag": "pre:a" },       // seq 2
            { "data": 3, "tag": "pre:b" },       // seq 3
            { "data": 4, "tag": "keep" }         // seq 4
        ]),
    );

    // Bare string with no trailing '*' ⇒ exact Eq.
    let r1 = delete_ok(&h, "sh", json!({ "match": "exact-tag" }));
    assert_delete_shape(&r1, "sh", 1, 2, 4, 3);

    // Bare string with a trailing '*' ⇒ prefix Glob.
    let r2 = delete_ok(&h, "sh", json!({ "match": "pre:*" }));
    assert_delete_shape(&r2, "sh", 2, 4, 4, 1);

    let d = diff(&h, "sh", 0);
    let tags: Vec<&str> = d["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["$tag"].as_str().unwrap())
        .collect();
    assert_eq!(tags, vec!["keep"], "only the unmatched tag survives");
}
