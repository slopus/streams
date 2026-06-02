//! Phase-3 §2 — topic-lifecycle conformance over HTTP (black-topic, via the shared
//! in-process harness). Exercises the documented JSON shapes + status codes for
//! `PUT`/`GET`/`DELETE`/`POST` `/v0/topics/:topic` and `GET /v0/topics`:
//!
//!   * PUT create (`201`) + idempotent re-PUT (`200`, `created:false`) + config
//!     default echo + config update (`200`, no `topic_exists_incompatible` in /v0).
//!   * GET state (head/earliest/next/count/bytes/effective_priority/config) incl.
//!     `?touch=false`; `404 topic_not_found` (state never auto-creates).
//!   * GET list with `prefix` / `page_size` / opaque-cursor pagination + the
//!     summary shape; corrupt-cursor `400`.
//!   * DELETE idempotent `deleted` flag; `?if_empty=true` ⇒ `409 topic_not_empty`;
//!     router cascade on topic delete.
//!   * auto-create on first write (`201`) vs `create:false` ⇒ `404`.
//!
//! All assertions are over the live wire contract; correctness that needs a
//! controllable clock (TTL/priority) lives in the engine unit/property tests.

mod common;

use common::{Harness, StatusCode};
use serde_json::{json, Value};

// ---------------------------------------------------------------------------
// Small assertion helpers
// ---------------------------------------------------------------------------

/// Assert the canonical error envelope (API §0.5): `{"error":{code,message,..}}`
/// with the expected `error.code`.
fn assert_error_code(body: &Value, expected_code: &str) {
    let err = body
        .get("error")
        .unwrap_or_else(|| panic!("expected an `error` envelope, got {body}"));
    assert_eq!(err["code"], expected_code, "error.code mismatch in {body}");
    assert!(
        err.get("message").and_then(Value::as_str).is_some(),
        "error.message must be a string in {body}"
    );
}

/// The full default config object as documented in API §0.10, echoed verbatim
/// by create/state responses.
fn default_config() -> Value {
    json!({
        "type": "log",
        "ttl_ms": 0,
        "cap_records": 0,
        "cap_bytes": 0,
        "discard": "old",
        "durable": false,
        "durability": "disk",
        "priority": null,
        "auto_priority": true,
        "auto_create": true,
        "idempotency_window_ms": 120000,
        "dedupe_node": true,
        "lease_ms": 30000,
        "claim_jitter_ms": 0,
        "max_deliveries": 0,
        "dead_letter": null,
        "leases_durable": false
    })
}

/// Assert the response carries a `performance` block with a numeric
/// `server_total_ms` (API §0.9).
fn assert_performance(body: &Value) {
    let perf = &body["performance"];
    assert!(
        perf.get("server_total_ms")
            .and_then(Value::as_f64)
            .is_some(),
        "performance.server_total_ms must be a number in {body}"
    );
}

// ---------------------------------------------------------------------------
// 1.1 PUT — create / idempotent re-PUT / config echo / update
// ---------------------------------------------------------------------------

#[test]
fn put_create_returns_201_with_default_config_echo() {
    let h = Harness::start();
    let (status, body) = h.put("/v0/topics/jobs", json!({}));
    assert_eq!(status, StatusCode::CREATED, "first PUT must be 201");
    assert_eq!(body["topic"], "jobs");
    assert_eq!(body["created"], true);
    // Empty `{}` create echoes the full default config object (API §0.10).
    assert_eq!(body["config"], default_config(), "default config echo");
    assert_performance(&body);
}

#[test]
fn put_create_merges_supplied_config_over_defaults() {
    let h = Harness::start();
    let (status, body) = h.put(
        "/v0/topics/jobs",
        json!({ "ttl_ms": 60000, "cap_records": 1_000_000, "durable": true, "priority": 10 }),
    );
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["created"], true);
    let cfg = &body["config"];
    // Supplied fields applied...
    assert_eq!(cfg["ttl_ms"], 60000);
    assert_eq!(cfg["cap_records"], 1_000_000);
    assert_eq!(cfg["durable"], true);
    assert_eq!(cfg["priority"], 10);
    // ...omitted fields take documented defaults.
    assert_eq!(cfg["cap_bytes"], 0);
    assert_eq!(cfg["discard"], "old");
    assert_eq!(cfg["auto_priority"], true);
    assert_eq!(cfg["auto_create"], true);
    assert_eq!(cfg["idempotency_window_ms"], 120000);
    assert_eq!(cfg["dedupe_node"], true);
}

#[test]
fn put_idempotent_repeat_returns_200_created_false() {
    let h = Harness::start();
    let cfg = json!({ "ttl_ms": 30000, "cap_records": 500 });

    let (s1, b1) = h.put("/v0/topics/jobs", cfg.clone());
    assert_eq!(s1, StatusCode::CREATED);
    assert_eq!(b1["created"], true);

    // Identical re-PUT is a no-op 200 with created:false (API §1.1 / §0.8).
    let (s2, b2) = h.put("/v0/topics/jobs", cfg);
    assert_eq!(s2, StatusCode::OK, "idempotent re-PUT must be 200");
    assert_eq!(b2["created"], false);
    assert_eq!(b2["config"], b1["config"], "config unchanged on re-PUT");
}

#[test]
fn put_changed_config_updates_in_place_200_not_409() {
    let h = Harness::start();
    let (s1, _) = h.put("/v0/topics/jobs", json!({ "cap_records": 100 }));
    assert_eq!(s1, StatusCode::CREATED);

    // A *changed* PUT applies the diff going forward — 200, created:false. /v0
    // has no immutable fields, so this is never `409 topic_exists_incompatible`.
    let (s2, b2) = h.put(
        "/v0/topics/jobs",
        json!({ "cap_records": 999, "discard": "reject" }),
    );
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(b2["created"], false);
    assert_eq!(b2["config"]["cap_records"], 999);
    assert_eq!(b2["config"]["discard"], "reject");

    // The update is visible on a subsequent state read.
    let (s3, b3) = h.get("/v0/topics/jobs");
    assert_eq!(s3, StatusCode::OK);
    assert_eq!(b3["config"]["cap_records"], 999);
    assert_eq!(b3["config"]["discard"], "reject");
}

#[test]
fn put_invalid_topic_name_is_400_invalid_request() {
    let h = Harness::start();
    // Leading char must be alphanumeric; `_foo` violates the charset.
    let (status, body) = h.put("/v0/topics/_foo", json!({}));
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error_code(&body, "invalid_request");
}

// ---------------------------------------------------------------------------
// 1.2 GET — topic state
// ---------------------------------------------------------------------------

#[test]
fn get_state_on_missing_topic_is_404_and_never_creates() {
    let h = Harness::start();
    let (status, body) = h.get("/v0/topics/ghost");
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_error_code(&body, "topic_not_found");

    // The failed state read must NOT have created the topic.
    let (ls, lb) = h.get("/v0/topics");
    assert_eq!(ls, StatusCode::OK);
    assert_eq!(
        lb["topics"].as_array().unwrap().len(),
        0,
        "state read must not auto-create"
    );
}

#[test]
fn get_state_fresh_topic_watermarks_and_shape() {
    let h = Harness::start();
    let (s, _) = h.put("/v0/topics/jobs", json!({}));
    assert_eq!(s, StatusCode::CREATED);

    let (status, body) = h.get("/v0/topics/jobs");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["topic"], "jobs");
    // Fresh empty topic: head_seq=0, earliest_seq=head+1=1, next_seq=head+1=1.
    assert_eq!(body["head_seq"], 0, "empty topic head is 0");
    assert_eq!(body["earliest_seq"], 1, "empty topic earliest = head + 1");
    assert_eq!(body["next_seq"], 1, "next_seq = head + 1");
    assert_eq!(body["count"], 0);
    assert_eq!(body["bytes"], 0);
    assert_eq!(body["config"], default_config());
    assert!(
        body["effective_priority"].is_number(),
        "effective_priority numeric"
    );
    // Recency clocks are null until first write/read; this is the first read,
    // so last_write_ts is still null but last_read_ts is now set (touch=true).
    assert_eq!(body["last_write_ts"], Value::Null);
    assert!(
        body["last_read_ts"].is_number(),
        "this read set last_read_ts"
    );
    assert_performance(&body);
}

#[test]
fn get_state_reflects_writes_head_earliest_count_bytes() {
    let h = Harness::start();
    // First write auto-creates → 201, seqs [1,2,3].
    let (ws, wb) = h.post(
        "/v0/topics/jobs",
        json!({ "records": [{ "data": 1 }, { "data": 2 }, { "data": 3 }] }),
    );
    assert_eq!(ws, StatusCode::CREATED);
    assert_eq!(wb["seqs"], json!([1, 2, 3]));

    let (status, body) = h.get("/v0/topics/jobs");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["head_seq"], 3);
    assert_eq!(body["earliest_seq"], 1);
    assert_eq!(body["next_seq"], 4, "next_seq = head + 1");
    assert_eq!(body["count"], 3);
    assert!(
        body["bytes"].as_u64().unwrap() > 0,
        "bytes accounted after writes"
    );
    assert!(
        body["last_write_ts"].is_number(),
        "last_write_ts set after a write"
    );
}

#[test]
fn get_state_touch_false_does_not_bump_last_read() {
    let h = Harness::start();
    h.put("/v0/topics/jobs", json!({}));

    // A monitoring scrape with ?touch=false must not set last_read_ts.
    let (status, body) = h.get("/v0/topics/jobs?touch=false");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["last_read_ts"],
        Value::Null,
        "touch=false must not bump last_read_ts"
    );

    // A default (touch=true) read does set it.
    let (_s2, body2) = h.get("/v0/topics/jobs");
    assert!(
        body2["last_read_ts"].is_number(),
        "default touch=true sets last_read_ts"
    );
}

// ---------------------------------------------------------------------------
// 1.3 GET /v0/topics — list with prefix / page_size / opaque-cursor pagination
// ---------------------------------------------------------------------------

#[test]
fn list_empty_has_no_cursor() {
    let h = Harness::start();
    let (status, body) = h.get("/v0/topics");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["topics"].as_array().unwrap().len(), 0);
    assert!(
        body.get("next_cursor").map(Value::is_null).unwrap_or(true),
        "next_cursor omitted on the final/empty page"
    );
    assert_performance(&body);
}

#[test]
fn list_summary_shape_and_prefix_filter() {
    let h = Harness::start();
    // Two prefixed topics + one unrelated; give one a write so summary fields vary.
    h.put("/v0/topics/jobs:a", json!({ "durable": true }));
    h.post("/v0/topics/jobs:b", json!({ "records": [{ "data": 1 }] }));
    h.put("/v0/topics/events", json!({}));

    // prefix=jobs: returns only the two jobs:* topics, sorted ascending.
    let (status, body) = h.get("/v0/topics?prefix=jobs");
    assert_eq!(status, StatusCode::OK);
    let topics = body["topics"].as_array().unwrap();
    assert_eq!(topics.len(), 2, "prefix must filter to jobs:* only");
    let names: Vec<&str> = topics.iter().map(|b| b["topic"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["jobs:a", "jobs:b"], "list is sorted by name");

    // Summary entry shape (API §1.3): the documented per-topic fields are present.
    let a = &topics[0];
    for field in [
        "topic",
        "head_seq",
        "earliest_seq",
        "count",
        "bytes",
        "durable",
        "effective_priority",
    ] {
        assert!(a.get(field).is_some(), "summary missing `{field}` in {a}");
    }
    assert_eq!(a["topic"], "jobs:a");
    assert_eq!(a["durable"], true, "jobs:a was created durable");
    // jobs:b got one write.
    assert_eq!(topics[1]["topic"], "jobs:b");
    assert_eq!(topics[1]["head_seq"], 1);
    assert_eq!(topics[1]["count"], 1);
    assert_eq!(topics[1]["durable"], false);

    // No more pages → next_cursor omitted.
    assert!(
        body.get("next_cursor").map(Value::is_null).unwrap_or(true),
        "single page must omit next_cursor"
    );
}

#[test]
fn list_paginates_with_opaque_cursor() {
    let h = Harness::start();
    // Create 5 topics with sortable names box0..box4.
    for i in 0..5 {
        let (s, _) = h.put(&format!("/v0/topics/topic{i}"), json!({}));
        assert!(s == StatusCode::CREATED);
    }

    // page_size=2 → first page returns 2 + an opaque next_cursor.
    let (s1, p1) = h.get("/v0/topics?page_size=2");
    assert_eq!(s1, StatusCode::OK);
    let page1: Vec<String> = p1["topics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["topic"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page1, vec!["box0", "box1"], "first page, sorted");
    let cursor1 = p1["next_cursor"]
        .as_str()
        .expect("next_cursor present mid-pagination");
    assert!(!cursor1.is_empty(), "cursor is a non-empty opaque token");

    // Page 2 via ?cursor=...
    let (s2, p2) = h.get(&format!("/v0/topics?page_size=2&cursor={cursor1}"));
    assert_eq!(s2, StatusCode::OK);
    let page2: Vec<String> = p2["topics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["topic"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page2, vec!["box2", "box3"]);
    let cursor2 = p2["next_cursor"].as_str().expect("third page exists");

    // Page 3 is the final page (1 remaining) → no next_cursor.
    let (s3, p3) = h.get(&format!("/v0/topics?page_size=2&cursor={cursor2}"));
    assert_eq!(s3, StatusCode::OK);
    let page3: Vec<String> = p3["topics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["topic"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page3, vec!["box4"]);
    assert!(
        p3.get("next_cursor").map(Value::is_null).unwrap_or(true),
        "final page omits next_cursor"
    );

    // Union of all three pages is the complete, non-overlapping set.
    let mut all = page1;
    all.extend(page2);
    all.extend(page3);
    assert_eq!(all, vec!["box0", "box1", "box2", "box3", "box4"]);
}

#[test]
fn list_corrupt_cursor_is_400() {
    let h = Harness::start();
    h.put("/v0/topics/jobs", json!({}));
    // Not valid base64 of the expected JSON shape.
    let (status, body) = h.get("/v0/topics?cursor=%%%not-base64%%%");
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_error_code(&body, "invalid_request");
}

// ---------------------------------------------------------------------------
// 1.4 DELETE — idempotent, ?if_empty, router cascade
// ---------------------------------------------------------------------------

#[test]
fn delete_existing_topic_returns_deleted_true() {
    let h = Harness::start();
    h.put("/v0/topics/jobs", json!({}));

    let (status, body) = h.delete("/v0/topics/jobs");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["topic"], "jobs");
    assert_eq!(body["deleted"], true);
    assert_eq!(
        body["routers_removed"].as_array().unwrap().len(),
        0,
        "no routers referenced this topic"
    );
    assert_performance(&body);

    // Gone: a state read now 404s.
    let (gs, _) = h.get("/v0/topics/jobs");
    assert_eq!(gs, StatusCode::NOT_FOUND);
}

#[test]
fn delete_absent_topic_is_idempotent_deleted_false() {
    let h = Harness::start();
    let (status, body) = h.delete("/v0/topics/never");
    assert_eq!(
        status,
        StatusCode::OK,
        "delete of absent topic is idempotent 200"
    );
    assert_eq!(body["deleted"], false);
    assert_eq!(body["routers_removed"].as_array().unwrap().len(), 0);
}

#[test]
fn delete_if_empty_rejects_non_empty_with_409() {
    let h = Harness::start();
    // A topic with one record.
    h.post("/v0/topics/jobs", json!({ "records": [{ "data": 1 }] }));

    // if_empty=true must refuse the non-empty topic with 409 topic_not_empty...
    let (status, body) = h.delete("/v0/topics/jobs?if_empty=true");
    assert_eq!(status, StatusCode::CONFLICT);
    assert_error_code(&body, "topic_not_empty");

    // ...and the topic must still exist (nothing deleted).
    let (gs, gb) = h.get("/v0/topics/jobs");
    assert_eq!(gs, StatusCode::OK);
    assert_eq!(gb["count"], 1);
}

#[test]
fn delete_if_empty_allows_empty_topic() {
    let h = Harness::start();
    h.put("/v0/topics/jobs", json!({})); // empty topic, count == 0.

    let (status, body) = h.delete("/v0/topics/jobs?if_empty=true");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deleted"], true);
}

#[test]
fn delete_topic_cascades_routers() {
    let h = Harness::start();
    // Router jobs->audit auto-creates both topics.
    let (rs, _) = h.put(
        "/v0/routers/jobs->audit",
        json!({ "source": "jobs", "dest": "audit" }),
    );
    assert_eq!(rs, StatusCode::CREATED);

    // A second router with `audit` as the source, to prove cascade hits either end.
    let (rs2, _) = h.put(
        "/v0/routers/audit->mirror",
        json!({ "source": "audit", "dest": "mirror" }),
    );
    assert_eq!(rs2, StatusCode::CREATED);

    // Deleting `audit` cascades both routers that reference it (dest of the first,
    // source of the second).
    let (status, body) = h.delete("/v0/topics/audit");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["deleted"], true);
    let mut removed: Vec<String> = body["routers_removed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    removed.sort();
    assert_eq!(
        removed,
        vec!["audit->mirror".to_string(), "jobs->audit".to_string()],
        "both routers touching the deleted topic are cascaded"
    );

    // Confirm via the router API: both are gone.
    let (g1, _) = h.get("/v0/routers/jobs->audit");
    assert_eq!(g1, StatusCode::NOT_FOUND);
    let (g2, _) = h.get("/v0/routers/audit->mirror");
    assert_eq!(g2, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// 2 — auto-create on first write vs create:false ⇒ 404
// ---------------------------------------------------------------------------

#[test]
fn write_auto_creates_on_first_then_200() {
    let h = Harness::start();
    // First write to a not-yet-existent topic → 201 Created.
    let (s1, b1) = h.post("/v0/topics/jobs", json!({ "records": [{ "data": 1 }] }));
    assert_eq!(s1, StatusCode::CREATED, "first write auto-creates → 201");
    assert_eq!(b1["created"], true);
    assert_eq!(b1["first_seq"], 1);
    assert_eq!(b1["last_seq"], 1);
    assert_eq!(b1["head_seq"], 1);

    // Subsequent write to the existing topic → 200, created:false.
    let (s2, b2) = h.post("/v0/topics/jobs", json!({ "records": [{ "data": 2 }] }));
    assert_eq!(s2, StatusCode::OK, "second write → 200");
    assert_eq!(b2["created"], false);
    assert_eq!(b2["first_seq"], 2);
    assert_eq!(b2["head_seq"], 2);
}

#[test]
fn write_create_false_on_absent_topic_is_404() {
    let h = Harness::start();
    let (status, body) = h.post(
        "/v0/topics/jobs",
        json!({ "create": false, "records": [{ "data": 1 }] }),
    );
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "create:false + absent topic → 404"
    );
    assert_error_code(&body, "topic_not_found");

    // The topic must not have been created by the rejected write.
    let (gs, _) = h.get("/v0/topics/jobs");
    assert_eq!(gs, StatusCode::NOT_FOUND);
}

#[test]
fn write_create_false_on_existing_topic_succeeds() {
    let h = Harness::start();
    h.put("/v0/topics/jobs", json!({}));
    // create:false is fine when the topic already exists → plain 200 append.
    let (status, body) = h.post(
        "/v0/topics/jobs",
        json!({ "create": false, "records": [{ "data": 1 }] }),
    );
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["created"], false);
    assert_eq!(body["head_seq"], 1);
}

#[test]
fn write_inline_config_applied_only_on_create() {
    let h = Harness::start();
    // Inline config on the creating write is applied.
    let (s1, _) = h.post(
        "/v0/topics/jobs",
        json!({ "config": { "cap_records": 7, "durable": true }, "records": [{ "data": 1 }] }),
    );
    assert_eq!(s1, StatusCode::CREATED);
    let (_gs, gb) = h.get("/v0/topics/jobs");
    assert_eq!(
        gb["config"]["cap_records"], 7,
        "inline config applied on create"
    );
    assert_eq!(gb["config"]["durable"], true);

    // Inline config on a write to an EXISTING topic is ignored (config goes via PUT).
    let (s2, _) = h.post(
        "/v0/topics/jobs",
        json!({ "config": { "cap_records": 999 }, "records": [{ "data": 2 }] }),
    );
    assert_eq!(s2, StatusCode::OK);
    let (_gs2, gb2) = h.get("/v0/topics/jobs");
    assert_eq!(
        gb2["config"]["cap_records"], 7,
        "inline config ignored on an existing topic"
    );
}
