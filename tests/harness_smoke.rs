//! Stage-1 harness self-check: proves `tests/common` boots a real bound server,
//! answers `/v0/health`, round-trips a JSON write/read, and opens an SSE stream.
//! The parallel-authored integration suites reuse `common::Harness` verbatim.

mod common;

use std::time::Duration;

use common::{Harness, StatusCode};
use serde_json::json;

#[test]
fn harness_boots_and_serves_health() {
    let h = Harness::start();
    assert!(h.base_url().starts_with("http://127.0.0.1:"));

    let (status, body) = h.get("/v0/health");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    assert!(body["uptime_ms"].is_number());
}

#[test]
fn harness_json_round_trip_and_sse() {
    let h = Harness::start();

    // Write two records (auto-create the box -> 201 CREATED on first write).
    let (status, body) = h.post(
        "/v0/boxes/jobs",
        json!({ "node": "w1", "records": [{ "data": { "v": 1 } }, { "data": { "v": 2 } }] }),
    );
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["seqs"], json!([1, 2]));

    // Read state.
    let (status, body) = h.get("/v0/boxes/jobs");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["head_seq"], 2);

    // Open a watch session and stream the first frames (record + caught-up).
    let (status, body) = h.post("/v0/watch", json!({ "boxes": { "jobs": { "from_seq": 0 } } }));
    assert_eq!(status, StatusCode::OK);
    let stream_url = body["stream_url"].as_str().expect("stream_url").to_string();

    let frames = h.sse_frames(&stream_url, 2, Duration::from_secs(5));
    let events: Vec<&str> = frames.iter().map(|f| f.event.as_str()).collect();
    assert!(events.contains(&"record"), "expected a record frame, got {events:?}");
    assert!(events.contains(&"caught-up"), "expected a caught-up frame, got {events:?}");
}
