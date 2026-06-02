//! Phase-5A Stage-4 — queue HTTP + SSE integration tests over a real bound
//! server (the harness in `tests/common`: a real socket, the exact
//! `http::build_router` the binary serves).
//!
//! Wire-contract / black-topic coverage of the documented queue surface (API §10):
//!
//!   * `PUT type:"queue"` create → `GET` returns `type:"queue"` + a `queue`
//!     sub-object (`ready`/`in_flight`/`dead_lettered`) (§1.2/§10.7).
//!   * produce (§2) → `POST /claim` leases jobs ascending by `$seq` with
//!     `lease_id`/`deadline`/`deliveries`; distributes across worker nodes and
//!     respects `max` + the coalescing window's even split (§10.2/§10.3).
//!   * `POST /ack` deletes (ack-is-delete, no redelivery), `nack` requeues
//!     (immediate + delayed), `extend` heartbeats / prevents expiry (§10.4–§10.6).
//!   * lease **expiry** (the visibility timeout) redelivers to another worker, and
//!     a job is **dead-lettered** after `max_deliveries` — both driven over the
//!     bound server by an injected `TestClock` (`Harness::start_with_test_clock`),
//!     so the timing is deterministic and the tests are NOT flaky / use no
//!     wall-clock sleeps for correctness.
//!   * a non-queue (log) topic rejects every queue endpoint with `409 not_a_queue`.
//!   * `GET /work` SSE auto-claims + pushes `event: job` frames, caps in-flight at
//!     `max`, releases leases on disconnect, and `406`s a non-SSE Accept.
//!   * the queue **type survives a restart** (durable jobs log) while the
//!     non-durable leases log **self-heals** (all jobs claimable after restart).

mod common;

use std::io::Read;
use std::time::{Duration, Instant};

use common::{Harness, StatusCode};
use serde_json::json;

/// Create a default (30 s lease) queue topic and produce `n` numbered jobs.
fn make_queue(h: &Harness, name: &str, n: usize) {
    make_queue_cfg(h, name, n, json!({ "type": "queue", "lease_ms": 30000 }));
}

/// Create a queue topic from an explicit config object, then produce `n` jobs.
fn make_queue_cfg(h: &Harness, name: &str, n: usize, cfg: serde_json::Value) {
    let (s, _) = h.put(&format!("/v0/topics/{name}"), cfg);
    assert_eq!(s, StatusCode::CREATED);
    if n > 0 {
        let records: Vec<_> = (0..n).map(|i| json!({ "data": { "i": i } })).collect();
        let (s, _) = h.post(&format!("/v0/topics/{name}"), json!({ "records": records }));
        assert!(s.is_success());
    }
}

/// Fetch `queue.{ready,in_flight,dead_lettered}` for a queue topic.
fn queue_counters(h: &Harness, name: &str) -> (u64, u64, u64) {
    let (_, st) = h.get(&format!("/v0/topics/{name}"));
    let q = &st["queue"];
    (
        q["ready"].as_u64().unwrap(),
        q["in_flight"].as_u64().unwrap(),
        q["dead_lettered"].as_u64().unwrap(),
    )
}

#[test]
fn queue_state_exposes_type_and_counters() {
    let h = Harness::start();
    make_queue(&h, "jobs", 5);

    let (s, body) = h.get("/v0/topics/jobs");
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["type"], json!("queue"));
    let q = &body["queue"];
    assert_eq!(q["ready"], json!(5));
    assert_eq!(q["in_flight"], json!(0));
    assert_eq!(q["dead_lettered"], json!(0));

    // A plain log topic omits the `queue` sub-object.
    let (s, _) = h.put("/v0/topics/plain", json!({}));
    assert_eq!(s, StatusCode::CREATED);
    let (_, body) = h.get("/v0/topics/plain");
    assert_eq!(body["type"], json!("log"));
    assert!(body.get("queue").map(|v| v.is_null()).unwrap_or(true));
}

#[test]
fn claim_ack_full_lifecycle() {
    let h = Harness::start();
    make_queue(&h, "jobs", 10);

    // Claim 4: leases the 4 lowest seqs ascending, deliveries==1.
    let (s, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w1", "max": 4 }));
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["count"], json!(4));
    assert_eq!(body["ready"], json!(6));
    let claimed = body["claimed"].as_array().unwrap();
    let seqs: Vec<u64> = claimed
        .iter()
        .map(|c| c["$seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2, 3, 4]);
    assert!(claimed.iter().all(|c| c["deliveries"] == json!(1)));
    assert!(claimed[0]["lease_id"]
        .as_str()
        .unwrap()
        .starts_with("lease_"));
    assert!(claimed[0]["deadline"].as_i64().unwrap() > 0);

    // in_flight is now 4.
    let (_, st) = h.get("/v0/topics/jobs");
    assert_eq!(st["queue"]["in_flight"], json!(4));

    // Ack 2 of them (ack-is-delete): they leave the jobs log.
    let (s, body) = h.post(
        "/v0/topics/jobs/ack",
        json!({ "node": "w1", "seqs": [1, 2] }),
    );
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["acked"], json!(2));
    assert_eq!(body["skipped"], json!([]));
    let (_, st) = h.get("/v0/topics/jobs");
    assert_eq!(st["count"], json!(8), "2 acked jobs deleted from jobs log");
    assert_eq!(st["queue"]["in_flight"], json!(2)); // seqs 3,4 still leased.

    // Acking a seq not held by this node is silently skipped.
    let (_, body) = h.post(
        "/v0/topics/jobs/ack",
        json!({ "node": "w1", "seqs": [1, 99] }),
    );
    assert_eq!(body["acked"], json!(0));
    assert_eq!(body["skipped"], json!([1, 99]));
}

#[test]
fn nack_requeues_and_extend_heartbeats() {
    let h = Harness::start();
    make_queue(&h, "jobs", 2);

    let (_, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w1", "max": 2 }));
    let seqs: Vec<u64> = body["claimed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["$seq"].as_u64().unwrap())
        .collect();

    // Extend the first lease (heartbeat) — returns the new deadline keyed by seq.
    let (s, body) = h.post(
        "/v0/topics/jobs/extend",
        json!({ "node": "w1", "seqs": [seqs[0]], "lease_ms": 60000 }),
    );
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["extended"], json!(1));
    assert!(body["deadlines"][seqs[0].to_string()].as_i64().unwrap() > 0);

    // Nack both for immediate reclaim → claimable again.
    let (s, body) = h.post(
        "/v0/topics/jobs/nack",
        json!({ "node": "w1", "seqs": seqs, "delay_ms": 0 }),
    );
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["nacked"], json!(2));
    assert_eq!(body["ready"], json!(2));
    assert_eq!(body["in_flight"], json!(0));

    // A fresh worker re-claims them; the delivery counter bumped to 2.
    let (_, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w2", "max": 2 }));
    assert_eq!(body["count"], json!(2));
    assert!(body["claimed"]
        .as_array()
        .unwrap()
        .iter()
        .all(|c| c["deliveries"] == json!(2)));
}

#[test]
fn non_queue_topic_rejects_queue_endpoints() {
    let h = Harness::start();
    let (s, _) = h.put("/v0/topics/log", json!({}));
    assert_eq!(s, StatusCode::CREATED);

    for path in ["claim", "ack", "nack", "extend"] {
        let (s, body) = h.post(
            &format!("/v0/topics/log/{path}"),
            json!({ "node": "w1", "seqs": [1], "lease_ms": 1000 }),
        );
        assert_eq!(s, StatusCode::CONFLICT, "{path} on a log topic is 409");
        assert_eq!(body["error"]["code"], json!("not_a_queue"));
    }

    // A missing topic is 404, not 409.
    let (s, body) = h.post("/v0/topics/nope/claim", json!({ "node": "w1" }));
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert_eq!(body["error"]["code"], json!("topic_not_found"));
}

#[test]
fn work_stream_pushes_job_frames() {
    let h = Harness::start();
    make_queue(&h, "jobs", 3);

    // The harness SSE helper opens with Accept: text/event-stream and collects
    // named frames; the /work stream pushes up to `max` jobs as `event: job`.
    let frames = h.sse_frames(
        "/v0/topics/jobs/work?node=worker-1&max=2",
        2,
        Duration::from_secs(5),
    );
    assert_eq!(frames.len(), 2, "max=2 ⇒ two job frames pushed");
    for f in &frames {
        assert_eq!(f.event, "job");
        let data: serde_json::Value = serde_json::from_str(&f.data).unwrap();
        assert_eq!(data["topic"], json!("jobs"));
        assert!(data["$seq"].as_u64().unwrap() >= 1);
        assert!(data["lease_id"].as_str().unwrap().starts_with("lease_"));
        assert_eq!(data["deliveries"], json!(1));
        // `id:` is the job seq.
        assert_eq!(f.id, data["$seq"].as_u64().unwrap().to_string());
    }
}

#[test]
fn work_stream_rejects_non_sse_accept() {
    let h = Harness::start();
    make_queue(&h, "jobs", 1);

    // A non-SSE Accept ⇒ 406 not_acceptable (no stream opened).
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let resp = client
        .get(format!("{}/v0/topics/jobs/work?node=w1", h.base_url()))
        .header(reqwest::header::ACCEPT, "application/json")
        .send()
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_ACCEPTABLE);
    let body: serde_json::Value = resp.json().unwrap();
    assert_eq!(body["error"]["code"], json!("not_acceptable"));
}

#[test]
fn work_stream_releases_leases_on_disconnect() {
    let h = Harness::start();
    make_queue(&h, "jobs", 2);

    // Open a /work stream, read both job frames, then drop the connection.
    {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let mut resp = client
            .get(format!(
                "{}/v0/topics/jobs/work?node=w1&max=2",
                h.base_url()
            ))
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .send()
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Read until we've seen two `event: job` frames.
        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 4096];
        let start = Instant::now();
        loop {
            if String::from_utf8_lossy(&buf).matches("event: job").count() >= 2 {
                break;
            }
            if start.elapsed() >= Duration::from_secs(5) {
                break;
            }
            match resp.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(_) => break,
            }
        }
        let text = String::from_utf8_lossy(&buf);
        assert!(
            text.matches("event: job").count() >= 2,
            "expected two job frames, got: {text}"
        );
        // resp drops here → connection closes → release-on-disconnect fires.
    }

    // After disconnect, the leases are released; the jobs are claimable again.
    // Poll the queue state briefly (release runs on the server's drop of the
    // stream future, which happens just after our connection closes).
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (_, st) = h.get("/v0/topics/jobs");
        if st["queue"]["in_flight"] == json!(0) && st["queue"]["ready"] == json!(2) {
            break;
        }
        if Instant::now() >= deadline {
            let (_, st) = h.get("/v0/topics/jobs");
            panic!("leases not released on disconnect: {}", st["queue"]);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ===========================================================================
// Stage-4 additions: multi-worker distribution, coalescing even-split, ack/nack
// no-redeliver, extend-prevents-expiry, lease-expiry redelivery (TestClock),
// dead-letter, SSE max-in-flight cap, and restart durability/self-heal.
// ===========================================================================

#[test]
fn claim_distributes_across_multiple_workers() {
    let h = Harness::start();
    make_queue(&h, "jobs", 10);

    // Three workers each claim 3: disjoint, ascending, contiguous seq blocks (the
    // greedy `claim_jitter_ms=0` path serves each claim immediately).
    let mut all = Vec::new();
    for node in ["w1", "w2", "w3"] {
        let (s, body) = h.post("/v0/topics/jobs/claim", json!({ "node": node, "max": 3 }));
        assert_eq!(s, StatusCode::OK);
        assert_eq!(body["count"], json!(3), "{node} gets 3 distinct jobs");
        let seqs: Vec<u64> = body["claimed"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["$seq"].as_u64().unwrap())
            .collect();
        // Each claim returns ascending seqs.
        let mut sorted = seqs.clone();
        sorted.sort_unstable();
        assert_eq!(seqs, sorted, "claim returns ascending seqs");
        all.extend(seqs);
    }
    // No job leased to two workers (9 distinct seqs across the three claims).
    all.sort_unstable();
    assert_eq!(all, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

    // One job remains ready; in_flight is the 9 leased.
    let (ready, in_flight, _dl) = queue_counters(&h, "jobs");
    assert_eq!((ready, in_flight), (1, 9));
}

#[test]
fn coalescing_window_even_split_when_supply_below_demand() {
    // A queue with a coalescing window > 0. Several poll-claims that arrive within
    // the window are gathered into one cohort and the available jobs divided
    // EVENLY (round-robin) — NOT first-arrival-drains-the-head (DESIGN §10.3). We
    // drive concurrent claims from threads against the real bound server.
    let h = Harness::start();
    make_queue_cfg(
        &h,
        "jobs",
        20,
        json!({ "type": "queue", "lease_ms": 30000, "claim_jitter_ms": 120 }),
    );

    // 4 workers each ask for max:10 (demand 40) against 20 available ⇒ ~5 each.
    let base = h.base_url().to_string();
    let handles: Vec<_> = (0..4)
        .map(|i| {
            let url = format!("{base}/v0/topics/jobs/claim");
            std::thread::spawn(move || {
                let client = reqwest::blocking::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()
                    .unwrap();
                let resp = client
                    .post(&url)
                    .json(&json!({ "node": format!("w{i}"), "max": 10 }))
                    .send()
                    .unwrap();
                let body: serde_json::Value = resp.json().unwrap();
                body["claimed"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|c| c["$seq"].as_u64().unwrap())
                    .collect::<Vec<u64>>()
            })
        })
        .collect();

    let mut counts = Vec::new();
    let mut all = Vec::new();
    for hdl in handles {
        let seqs = hdl.join().unwrap();
        counts.push(seqs.len());
        all.extend(seqs);
    }

    // Every one of the 20 jobs leased exactly once across the cohort.
    all.sort_unstable();
    assert_eq!(all, (1..=20).collect::<Vec<u64>>(), "all jobs leased once");
    // Even split: with 20 jobs across 4 claimers, each got 5 (no
    // 10/10/0/0 head-drain). Tolerate the rare case where one claim's window
    // didn't overlap by asserting nobody hogged the head (max share <= 10) and
    // the spread is tight.
    counts.sort_unstable();
    assert_eq!(
        counts,
        vec![5, 5, 5, 5],
        "supply 20 / demand 40 over a cohort ⇒ even 5 each, not head-drain",
    );
}

#[test]
fn ack_does_not_redeliver() {
    let h = Harness::start();
    make_queue(&h, "jobs", 3);

    let (_, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w1", "max": 3 }));
    let seqs: Vec<u64> = body["claimed"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["$seq"].as_u64().unwrap())
        .collect();

    // Ack all three (ack-is-delete).
    let (s, body) = h.post("/v0/topics/jobs/ack", json!({ "node": "w1", "seqs": seqs }));
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["acked"], json!(3));

    // The jobs left the jobs log; a subsequent claim never redelivers them.
    let (ready, in_flight, _dl) = queue_counters(&h, "jobs");
    assert_eq!((ready, in_flight), (0, 0), "acked jobs are gone");
    let (_, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w2", "max": 10 }));
    assert_eq!(body["count"], json!(0), "no redelivery after ack");
}

#[test]
fn extend_prevents_expiry() {
    // Drive time with an injected TestClock so the deadline math is deterministic.
    let h = Harness::start_with_test_clock();
    make_queue_cfg(&h, "jobs", 1, json!({ "type": "queue", "lease_ms": 1000 }));

    let (_, body) = h.post(
        "/v0/topics/jobs/claim",
        json!({ "node": "w1", "max": 1, "lease_ms": 1000 }),
    );
    let seq = body["claimed"][0]["$seq"].as_u64().unwrap();

    // Heartbeat just before the 1 s deadline: extend by another 10 s.
    h.clock().advance(900);
    let (s, body) = h.post(
        "/v0/topics/jobs/extend",
        json!({ "node": "w1", "seqs": [seq], "lease_ms": 10000 }),
    );
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["extended"], json!(1));

    // Past the ORIGINAL deadline but within the extended one ⇒ still leased; no
    // other worker can claim it.
    h.clock().advance(5000); // now +5.9 s; original was +1 s, extended is +10.9 s.
    let (_, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w2", "max": 1 }));
    assert_eq!(body["count"], json!(0), "extend kept the lease alive");
    let (_, in_flight, _dl) = queue_counters(&h, "jobs");
    assert_eq!(in_flight, 1);
}

#[test]
fn lease_expiry_redelivers_to_another_worker() {
    // The visibility timeout: once a lease deadline passes, the job becomes
    // claimable again and is redelivered (delivery counter bumps). Deterministic
    // via the injected TestClock — no real sleep.
    let h = Harness::start_with_test_clock();
    make_queue_cfg(&h, "jobs", 1, json!({ "type": "queue", "lease_ms": 1000 }));

    let (_, body) = h.post(
        "/v0/topics/jobs/claim",
        json!({ "node": "w1", "max": 1, "lease_ms": 1000 }),
    );
    assert_eq!(body["count"], json!(1));
    let seq = body["claimed"][0]["$seq"].as_u64().unwrap();
    assert_eq!(body["claimed"][0]["deliveries"], json!(1));

    // Before the deadline: not claimable by anyone else.
    let (_, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w2", "max": 1 }));
    assert_eq!(body["count"], json!(0));

    // After the deadline passes: redelivered to w2 with deliveries==2.
    h.clock().advance(1001);
    let (_, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w2", "max": 1 }));
    assert_eq!(body["count"], json!(1), "expired lease is reclaimable");
    assert_eq!(body["claimed"][0]["$seq"].as_u64().unwrap(), seq);
    assert_eq!(
        body["claimed"][0]["deliveries"],
        json!(2),
        "redelivery bumps counter"
    );
}

#[test]
fn nack_delayed_holds_until_elapsed() {
    let h = Harness::start_with_test_clock();
    make_queue_cfg(&h, "jobs", 1, json!({ "type": "queue", "lease_ms": 30000 }));

    let (_, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w1", "max": 1 }));
    let seq = body["claimed"][0]["$seq"].as_u64().unwrap();

    // Delayed nack: invisible for 5 s.
    let (s, body) = h.post(
        "/v0/topics/jobs/nack",
        json!({ "node": "w1", "seqs": [seq], "delay_ms": 5000 }),
    );
    assert_eq!(s, StatusCode::OK);
    assert_eq!(body["nacked"], json!(1));
    assert_eq!(body["ready"], json!(0), "not yet claimable (delay pending)");

    // Not claimable before the delay elapses.
    let (_, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w2", "max": 1 }));
    assert_eq!(body["count"], json!(0));

    // After 5 s it becomes claimable.
    h.clock().advance(5001);
    let (_, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w2", "max": 1 }));
    assert_eq!(body["count"], json!(1));
    assert_eq!(body["claimed"][0]["$seq"].as_u64().unwrap(), seq);
}

#[test]
fn dead_letter_after_max_deliveries() {
    // After `max_deliveries` reclaims without an ack, the job is MOVED to the
    // dead_letter topic instead of being redelivered. Driven by the TestClock
    // (expiry between deliveries) so the flow is deterministic over HTTP.
    let h = Harness::start_with_test_clock();
    let (s, _) = h.put("/v0/topics/dlq", json!({}));
    assert_eq!(s, StatusCode::CREATED);
    make_queue_cfg(
        &h,
        "jobs",
        1,
        json!({ "type": "queue", "lease_ms": 1000, "max_deliveries": 2, "dead_letter": "dlq" }),
    );

    // Delivery 1, let it expire.
    let (_, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w", "max": 1 }));
    assert_eq!(body["claimed"][0]["deliveries"], json!(1));
    h.clock().advance(1001);
    // Delivery 2, let it expire.
    let (_, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w", "max": 1 }));
    assert_eq!(body["claimed"][0]["deliveries"], json!(2));
    h.clock().advance(1001);
    // The next claim would be delivery 3 > max_deliveries(2) ⇒ dead-lettered,
    // not redelivered. The claim returns empty (the job left the queue).
    let (_, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w", "max": 1 }));
    assert_eq!(
        body["count"],
        json!(0),
        "job dead-lettered, not redelivered"
    );

    // The source queue is empty with dead_lettered==1; the DL topic holds the job
    // with provenance meta.
    let (ready, in_flight, dl) = queue_counters(&h, "jobs");
    assert_eq!((ready, in_flight, dl), (0, 0, 1));
    let (s, st) = h.get("/v0/topics/jobs");
    assert_eq!(s, StatusCode::OK);
    assert_eq!(st["count"], json!(0), "job deleted from the jobs log");

    let (_, d) = h.post(
        "/v0/topics/dlq/diff",
        json!({ "from_seq": 0, "include_meta": true }),
    );
    let recs = d["records"].as_array().unwrap();
    assert_eq!(recs.len(), 1, "the poison job landed in the DL topic");
    assert_eq!(recs[0]["meta"]["$dead_letter_from"], json!("jobs"));
    assert_eq!(recs[0]["meta"]["$dead_letter_src_seq"], json!("1"));
}

#[test]
fn work_stream_caps_in_flight_at_max() {
    // The /work PUSH stream keeps at most `max` jobs leased to the connection
    // (backpressure at N in-flight, API §10.8). With 5 jobs and max=2, only 2 are
    // pushed and held; the queue's in_flight stays capped at 2 while the stream is
    // live and 3 remain ready. The reader thread keeps the connection OPEN (parked
    // on its 3 s request timeout) so the leases are not released (on-disconnect)
    // before the main thread observes the steady state.
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    let h = Harness::start();
    make_queue(&h, "jobs", 5);

    let base = h.base_url().to_string();
    let frames_seen = Arc::new(AtomicU64::new(0));
    let frames_seen_t = frames_seen.clone();
    let handle = std::thread::spawn(move || {
        // A 3 s total request timeout holds the connection open through the
        // steady-state observation below, then closes it (release-on-disconnect).
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        let mut resp = client
            .get(format!("{base}/v0/topics/jobs/work?node=w1&max=2"))
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .send()
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 4096];
        // Reads until the request timeout fires (no more pushes after max=2).
        loop {
            match resp.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    let c = String::from_utf8_lossy(&buf).matches("event: job").count() as u64;
                    frames_seen_t.store(c, Ordering::Relaxed);
                }
                Err(_) => break, // request timeout: connection closes here.
            }
        }
    });

    // While the stream is live, in_flight should settle at exactly 2 (the cap),
    // with 3 still ready. Poll for the steady state (well within the 3 s the
    // reader holds the connection).
    let deadline = Instant::now() + Duration::from_millis(2500);
    let mut observed = (0u64, 0u64, 0u64);
    while Instant::now() < deadline {
        observed = queue_counters(&h, "jobs");
        if observed.1 == 2 && observed.0 == 3 && frames_seen.load(Ordering::Relaxed) == 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let seen = frames_seen.load(Ordering::Relaxed);
    assert_eq!(
        (observed.0, observed.1),
        (3, 2),
        "work stream caps in_flight at max=2 (3 ready), got {observed:?}",
    );
    assert_eq!(seen, 2, "work stream pushed exactly max=2 jobs, not all 5");
    let _ = handle.join();
}

#[test]
fn queue_survives_restart_and_leases_self_heal() {
    // Durable jobs log + (default) non-durable leases log. After a restart the
    // queue TYPE survives and all jobs are claimable again — the previously
    // in-flight job self-heals (no replayed lease ⇒ claimable), the self-healing
    // visibility timeout (DESIGN §10.6). Two harnesses share one data dir.
    let dir = tempfile::tempdir().unwrap();

    // Boot 1: create a durable queue, produce 3 jobs, claim 1 (in-flight).
    {
        let mut h = Harness::start_persistent(dir.path());
        let (s, _) = h.put(
            "/v0/topics/jobs",
            json!({ "type": "queue", "durable": true, "lease_ms": 30000 }),
        );
        assert_eq!(s, StatusCode::CREATED);
        let records: Vec<_> = (0..3).map(|i| json!({ "data": { "i": i } })).collect();
        let (s, _) = h.post("/v0/topics/jobs", json!({ "records": records }));
        assert!(s.is_success());
        let (s, body) = h.post("/v0/topics/jobs/claim", json!({ "node": "w1", "max": 1 }));
        assert_eq!(s, StatusCode::OK);
        assert_eq!(body["count"], json!(1));
        let (ready, in_flight, _dl) = queue_counters(&h, "jobs");
        assert_eq!(
            (ready, in_flight),
            (2, 1),
            "1 in-flight, 2 ready before restart"
        );
        // Explicit shutdown joins the WAL writer so the durable jobs log is on
        // disk before we re-boot on the same dir.
        h.shutdown();
    }

    // Boot 2: recovery rebuilds from the WAL on the same dir.
    {
        let h = Harness::start_persistent(dir.path());
        let (s, st) = h.get("/v0/topics/jobs");
        assert_eq!(s, StatusCode::OK);
        assert_eq!(st["type"], json!("queue"), "queue type survives restart");
        assert_eq!(st["count"], json!(3), "durable jobs log preserved all jobs");
        // The previously in-flight job has no replayed lease ⇒ all 3 claimable.
        let (ready, in_flight, _dl) = queue_counters(&h, "jobs");
        assert_eq!(
            (ready, in_flight),
            (3, 0),
            "non-durable leases self-heal: all jobs claimable after restart",
        );
        // And they can all be claimed.
        let (_, body) = h.post(
            "/v0/topics/jobs/claim",
            json!({ "node": "w-new", "max": 3 }),
        );
        assert_eq!(body["count"], json!(3));
    }
}
