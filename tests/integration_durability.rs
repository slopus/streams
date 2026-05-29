//! Phase-4 Stage-2 durability tests: a durable write survives an engine
//! teardown + reopen (the "restart" surface for an in-process engine), config /
//! routers / deletes / eviction watermarks are restored, and a torn WAL tail is
//! truncated rather than misread as data.
//!
//! These drive the engine directly (no HTTP) with a unique `tempfile::tempdir`
//! per test, so each is isolated and leaves nothing behind. A separate
//! SIGKILL-against-the-binary test (`kill_during_durable_write_survives`)
//! exercises the real hard-kill path.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use streams::clock::{SharedClock, SystemClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::types::*;

/// A `ServerConfig` pointed at `dir` (durable mode).
fn config_at(dir: &std::path::Path) -> ServerConfig {
    ServerConfig {
        data_dir: Some(dir.to_string_lossy().into_owned()),
        ..ServerConfig::default()
    }
}

fn engine_at(dir: &std::path::Path) -> Arc<Engine> {
    let clock: SharedClock = Arc::new(SystemClock);
    Engine::with_data_dir(config_at(dir), clock).expect("open durable engine")
}

fn one(data: serde_json::Value, tag: Option<&str>) -> WriteRequest {
    WriteRequest {
        records: vec![RecordIn {
            data,
            tag: tag.map(str::to_string),
            node: None,
            meta: None,
        }],
        node: None,
        idempotency_key: None,
        create: None,
        config: None,
        disable_backpressure: false,
    }
}

fn durable_box() -> BoxConfig {
    BoxConfig {
        durable: true,
        ..BoxConfig::default()
    }
}

fn diff_from(from_seq: u64) -> DiffRequest {
    DiffRequest {
        from_seq,
        include_tags: true,
        ..DiffRequest::default()
    }
}

/// The headline guarantee: durable writes are present after a restart.
#[test]
fn durable_writes_survive_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let engine = engine_at(dir.path());
        engine.put_box("jobs", durable_box()).unwrap();
        for i in 1..=5 {
            // durable:true ⇒ fsync-gated; fsync_ms is populated and > 0.
            let resp = engine
                .write("jobs", one(json!({ "i": i }), Some("t")), true)
                .unwrap();
            assert!(
                resp.performance.fsync_ms.unwrap_or(0.0) > 0.0,
                "durable write must report a real fsync_ms"
            );
        }
        // Drop engine → WAL writer drains + fsyncs + joins (Drop path).
    }

    // Reopen the same data dir: state must be fully recovered.
    let engine = engine_at(dir.path());
    let st = engine.box_state("jobs", false).unwrap();
    assert_eq!(st.head_seq, 5, "all 5 durable writes recovered");
    assert_eq!(st.earliest_seq, 1);
    assert_eq!(st.count, 5);
    assert!(st.config.durable, "config recovered");

    let d = engine.diff("jobs", diff_from(0)).unwrap();
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
    assert_eq!(d.records[0].data, json!({ "i": 1 }));
    assert_eq!(d.records[0].tag.as_deref(), Some("t"));
}

/// Regression test for a concurrent-durable-writer ordering bug: many threads
/// writing durable single-record appends to the SAME box must all survive a
/// restart, with a contiguous `[1..=N]` seq set and no loss. The bug: seq
/// assignment (`BoxState::append`, under the index lock) and WAL enqueue were
/// not atomic, so two writers could assign seqs `A < B` yet enqueue `B`'s frame
/// before `A`'s; recovery (apply-in-WAL-order, skip `seq <= head`) then silently
/// dropped `A`. The per-box `append_lock` makes assignment+enqueue atomic, so
/// WAL order matches seq order and every acked durable write is recovered.
#[test]
fn concurrent_durable_writers_no_loss_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let writers = 8u64;
    let per_writer = 250u64;
    let total = writers * per_writer;
    {
        let engine = engine_at(dir.path());
        engine.put_box("hot", durable_box()).unwrap();
        let mut handles = Vec::new();
        for w in 0..writers {
            let engine = engine.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..per_writer {
                    // Each write is durable ⇒ the ack is fsync-gated; once it
                    // returns the frame is on disk and must survive a restart.
                    engine
                        .write("hot", one(json!({ "w": w, "i": i }), None), true)
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let st = engine.box_state("hot", false).unwrap();
        assert_eq!(st.head_seq, total, "all concurrent durable writes acked");
        assert_eq!(st.count, total);
    }

    // Restart: every acked durable write is recovered as a contiguous prefix.
    let engine = engine_at(dir.path());
    let st = engine.box_state("hot", false).unwrap();
    assert_eq!(st.head_seq, total, "no acked durable write lost across restart");
    assert_eq!(st.earliest_seq, 1);
    assert_eq!(st.count, total);

    // The recovered seqs are exactly [1..=total] — dense, contiguous, no gaps.
    let mut seqs = Vec::new();
    let mut from = 0u64;
    loop {
        let d = engine
            .diff(
                "hot",
                DiffRequest {
                    from_seq: from,
                    limit: 1000,
                    ..DiffRequest::default()
                },
            )
            .unwrap();
        if d.records.is_empty() {
            break;
        }
        for r in &d.records {
            seqs.push(r.seq);
        }
        from = d.next_from_seq;
        if d.caught_up {
            break;
        }
    }
    assert_eq!(seqs, (1..=total).collect::<Vec<_>>(), "contiguous [1..=N]");
}

/// Non-durable writes are also replayed when they reached the WAL before a
/// clean teardown (the writer drains + fsyncs on drop). Power-loss can lose the
/// un-fsynced tail (the documented fast-path tradeoff), not a clean teardown.
#[test]
fn nondurable_writes_survive_clean_teardown() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_box("evts", BoxConfig::default()).unwrap(); // durable:false
        for i in 1..=3 {
            engine.write("evts", one(json!({ "i": i }), None), true).unwrap();
        }
    }
    let engine = engine_at(dir.path());
    let st = engine.box_state("evts", false).unwrap();
    assert_eq!(st.head_seq, 3);
    assert_eq!(st.count, 3);
}

/// Deletes replay deterministically: a previously-deleted record stays gone,
/// the delete is still silent (no tombstone), and `count`/`earliest_seq` match.
#[test]
fn deletes_replay_and_stay_gone() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_box("d", durable_box()).unwrap();
        for i in 1..=5 {
            engine.write("d", one(json!({ "i": i }), Some(&format!("tag{i}"))), true).unwrap();
        }
        // Delete tag3 (a middle hole) and everything < seq 2 (a prefix).
        engine
            .delete("d", DeleteRequest { before_seq: Some(2), match_: None })
            .unwrap();
        engine
            .delete("d", DeleteRequest { before_seq: None, match_: Some(Filter::from_shorthand("tag3")) })
            .unwrap();
    }
    let engine = engine_at(dir.path());
    let d = engine.diff("d", diff_from(0)).unwrap();
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(seqs, vec![2, 4, 5], "seq 1 (prefix) and seq 3 (tag) stay deleted");
    assert!(d.tombstone.is_none(), "deletion stays silent across restart");
    assert_eq!(engine.box_state("d", false).unwrap().count, 3);
}

/// Routers (and their auto-created boxes) survive a restart and keep forwarding.
#[test]
fn routers_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine
            .put_router(
                "src->dst",
                RouterCreateRequest {
                    source: "src".into(),
                    dest: "dst".into(),
                    preserve_node: true,
                    preserve_tag: true,
                    create_dest: true,
                    filter: None,
                    allow_cycle: false,
                },
            )
            .unwrap();
    }
    let engine = engine_at(dir.path());
    // The router and both boxes are back.
    let g = engine.get_router("src->dst").unwrap();
    assert_eq!(g.source, "src");
    assert_eq!(g.dest, "dst");
    // Forwarding still works post-restart.
    engine.write("src", one(json!({ "x": 1 }), None), true).unwrap();
    let d = engine.diff("dst", diff_from(0)).unwrap();
    assert_eq!(d.records.len(), 1);
    assert_eq!(d.records[0].data, json!({ "x": 1 }));
}

/// A recovered cap-eviction watermark still yields a tombstone after restart
/// (no silent involuntary loss across a restart).
#[test]
fn evict_floor_tombstone_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        let cfg = BoxConfig { cap_records: 3, durable: true, ..BoxConfig::default() };
        engine.put_box("cap", cfg).unwrap();
        for i in 1..=6 {
            engine.write("cap", one(json!({ "i": i }), None), true).unwrap();
        }
        // head=6, cap=3 ⇒ earliest=4, evict_floor=3.
        assert_eq!(engine.box_state("cap", false).unwrap().earliest_seq, 4);
    }
    let engine = engine_at(dir.path());
    let st = engine.box_state("cap", false).unwrap();
    assert_eq!(st.head_seq, 6);
    assert_eq!(st.earliest_seq, 4, "cap floor recovered");
    assert_eq!(st.count, 3);
    // A consumer at from_seq=0 fell below the recovered evict_floor ⇒ tombstone.
    let d = engine.diff("cap", diff_from(0)).unwrap();
    let tomb = d.tombstone.expect("cap tombstone after restart");
    assert_eq!(tomb.reason, TombstoneReason::Cap);
}

/// A torn tail frame (a partial/interrupted write at the end of the WAL) is
/// truncated on recovery, never interpreted as data — and earlier, complete
/// frames survive.
#[test]
fn torn_tail_is_truncated_not_read_as_data() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_box("t", durable_box()).unwrap();
        for i in 1..=3 {
            engine.write("t", one(json!({ "i": i }), None), true).unwrap();
        }
    }

    // Corrupt the tail of the active WAL file: append garbage bytes that look
    // like the start of a frame but cannot form a complete, CRC-valid one.
    let wal_dir = dir.path().join("wal");
    let mut files: Vec<_> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    files.sort();
    let active = files.last().unwrap().clone();

    // Find the true end-of-data via the WAL reader's own framing logic, then
    // write a half-written next frame right after it (an oversized frame_len with
    // only a few bytes following ⇒ length overrun / CRC failure ⇒ torn tail).
    use std::io::{Seek, SeekFrom, Write};
    let data_end = streams::storage::WalReader::open(&active).unwrap().count_valid_len();
    let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&active).unwrap();
    f.seek(SeekFrom::Start(data_end as u64)).unwrap();
    let mut junk = Vec::new();
    junk.extend_from_slice(&9999u32.to_le_bytes());
    junk.extend_from_slice(&[0xAB; 16]);
    f.write_all(&junk).unwrap();
    f.sync_all().unwrap();
    drop(f);

    // Recovery must truncate the torn tail and recover exactly the 3 good frames.
    let engine = engine_at(dir.path());
    let st = engine.box_state("t", false).unwrap();
    assert_eq!(st.head_seq, 3, "good frames recovered; torn tail discarded");
    assert_eq!(st.count, 3);
    let d = engine.diff("t", diff_from(0)).unwrap();
    assert_eq!(d.records.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1, 2, 3]);

    // And the truncated WAL is writable again: a new durable write appends cleanly
    // and survives a second restart (proves the tail was truncated, not appended
    // after garbage).
    engine.write("t", one(json!({ "i": 4 }), None), true).unwrap();
    drop(engine);
    let engine = engine_at(dir.path());
    assert_eq!(engine.box_state("t", false).unwrap().head_seq, 4);
}

// ===========================================================================
// Stage 4: restart-recovery correctness + the readiness gate.
// ===========================================================================

/// A durable engine is `ready` the instant `with_data_dir` returns (recovery is
/// synchronous and completes before serving), and an empty/missing data dir is a
/// clean fresh start (no error, no boxes, ready).
#[test]
fn fresh_dir_is_clean_start_and_ready() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    assert!(engine.is_ready(), "fresh engine is ready after recovery");
    assert!((engine.replay_progress() - 1.0).abs() < f64::EPSILON);
    assert_eq!(engine.box_count(), 0, "empty data dir ⇒ no boxes");
}

/// write → snapshot → more writes → simulate restart (drop + reopen the same
/// data dir) → the full materialized state matches the pre-crash committed
/// state: head/earliest/count/config/records, and the engine comes back ready.
#[test]
fn write_snapshot_more_writes_restart_matches() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine
            .put_box("jobs", BoxConfig { durable: true, ttl_ms: 0, ..BoxConfig::default() })
            .unwrap();
        for i in 1..=4 {
            engine.write("jobs", one(json!({ "i": i }), Some(&format!("t{i}"))), true).unwrap();
        }
        // Snapshot after 4...
        assert!(engine.write_snapshot().unwrap());
        // ...then more writes land only in the active WAL tail.
        for i in 5..=8 {
            engine.write("jobs", one(json!({ "i": i }), Some(&format!("t{i}"))), true).unwrap();
        }
    }

    // Simulate a restart: a brand-new engine over the same data dir.
    let engine = engine_at(dir.path());
    assert!(engine.is_ready(), "engine is ready after restart recovery");
    let st = engine.box_state("jobs", false).unwrap();
    assert_eq!(st.head_seq, 8, "snapshotted prefix + replayed tail");
    assert_eq!(st.earliest_seq, 1);
    assert_eq!(st.count, 8);
    assert!(st.config.durable, "config restored");

    let d = engine.diff("jobs", diff_from(0)).unwrap();
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(d.records[7].data, json!({ "i": 8 }));
    assert_eq!(d.records[7].tag.as_deref(), Some("t8"));
}

/// No silent loss across restart: a consumer whose cursor fell below the
/// recovered `earliest_seq` still receives a tombstone when the floor was driven
/// by cap eviction (involuntary), while a purely-deleted gap stays silent — both
/// behaviors survive a restart (DESIGN §5.4, ROADMAP Phase-4).
#[test]
fn tombstone_vs_silent_gap_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        // Cap-eviction box: head 5, cap 3 ⇒ evict_floor advances (involuntary).
        engine
            .put_box("capped", BoxConfig { cap_records: 3, durable: true, ..BoxConfig::default() })
            .unwrap();
        for i in 1..=5 {
            engine.write("capped", one(json!({ "i": i }), None), true).unwrap();
        }
        // Deletion box: delete a prefix (voluntary ⇒ silent, no evict_floor bump).
        engine.put_box("pruned", durable_box()).unwrap();
        for i in 1..=5 {
            engine.write("pruned", one(json!({ "i": i }), None), true).unwrap();
        }
        engine
            .delete("pruned", DeleteRequest { before_seq: Some(3), match_: None })
            .unwrap();
    }

    let engine = engine_at(dir.path());

    // Cap box: a cursor below the recovered involuntary floor ⇒ tombstone.
    let cap = engine.diff("capped", diff_from(0)).unwrap();
    let tomb = cap.tombstone.expect("cap tombstone after restart (no silent loss)");
    assert_eq!(tomb.reason, TombstoneReason::Cap);
    assert!(cap.records.iter().all(|r| r.seq >= cap.earliest_seq));

    // Deletion box: a cursor in the purely-deleted gap ⇒ NO tombstone, silent
    // advance past the deleted prefix.
    let pruned = engine.diff("pruned", diff_from(0)).unwrap();
    assert!(pruned.tombstone.is_none(), "voluntary delete stays silent across restart");
    assert_eq!(
        pruned.records.iter().map(|r| r.seq).collect::<Vec<_>>(),
        vec![3, 4, 5],
        "deleted prefix gone; survivors remain"
    );
    assert_eq!(engine.box_state("pruned", false).unwrap().earliest_seq, 3);
}

// --- The readiness gate, exercised through the real `/v0/ready` handler. ---

/// Build the real axum router over `engine` and run one blocking request on a
/// throwaway current-thread runtime; returns `(status, json-body)`.
fn ready_request(engine: Arc<Engine>) -> (u16, serde_json::Value) {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt; // for `oneshot`

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async move {
        let app = streams::http::build_router(engine);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v0/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = resp.status().as_u16();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, body)
    })
}

/// `/v0/ready` returns `503 not_ready` (with `error.detail.replay_progress`)
/// while WAL replay is in progress, and `200 ready` once recovery completes —
/// driven through the real handler by toggling the engine's ready gate.
#[test]
fn ready_gate_503_during_replay_then_200() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_at(dir.path());
    engine.put_box("jobs", durable_box()).unwrap();

    // Simulate "mid-replay": gate closed, 3 of 8 frames applied.
    engine.set_ready_for_test(false, 3, 8);
    let (status, body) = ready_request(engine.clone());
    assert_eq!(status, 503, "not ready ⇒ 503 during replay");
    assert_eq!(body["error"]["code"], json!("not_ready"));
    let progress = body["error"]["detail"]["replay_progress"].as_f64().unwrap();
    assert!((progress - 0.375).abs() < 1e-9, "replay_progress = 3/8");

    // Recovery completes: gate opens.
    engine.set_ready_for_test(true, 0, 0);
    let (status, body) = ready_request(engine.clone());
    assert_eq!(status, 200, "ready ⇒ 200");
    assert_eq!(body["status"], json!("ready"));
    assert_eq!(body["wal_replay_complete"], json!(true));
    assert_eq!(body["boxes"], json!(1));
}

// ===========================================================================
// Stage 3: snapshot / checkpoint round-trips and recovery-via-snapshot.
// ===========================================================================

/// Count `wal-*.log` and `snapshot-*.bin` files under a data dir.
fn count_files(dir: &std::path::Path, sub: &str, prefix: &str, suffix: &str) -> usize {
    let d = dir.join(sub);
    std::fs::read_dir(&d)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| {
                    let name = e.file_name();
                    let n = name.to_string_lossy();
                    n.starts_with(prefix) && n.ends_with(suffix)
                })
                .count()
        })
        .unwrap_or(0)
}

/// A snapshot of a populated engine round-trips: after writing a snapshot and
/// reopening the data dir, the materialized state (head/earliest/count/records/
/// config) is identical — recovered from the snapshot, not a full WAL replay.
#[test]
fn snapshot_round_trips_materialized_state() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_box("jobs", durable_box()).unwrap();
        for i in 1..=10 {
            engine
                .write("jobs", one(json!({ "i": i }), Some(&format!("t{i}"))), true)
                .unwrap();
        }
        // Delete a middle record and a prefix to exercise floors/holes.
        engine
            .delete("jobs", DeleteRequest { before_seq: Some(3), match_: None })
            .unwrap();
        engine
            .delete(
                "jobs",
                DeleteRequest { before_seq: None, match_: Some(Filter::from_shorthand("t6")) },
            )
            .unwrap();

        // Capture the pre-snapshot materialized view.
        let pre = engine.box_state("jobs", false).unwrap();

        // Write a snapshot; it must be durably present and the WAL prefix dropped.
        assert!(engine.write_snapshot().unwrap(), "snapshot written");
        assert_eq!(
            count_files(dir.path(), "meta", "snapshot-", ".bin"),
            1,
            "exactly one snapshot file kept"
        );

        // Re-read state from the SAME engine: snapshot must not perturb it.
        let post = engine.box_state("jobs", false).unwrap();
        assert_eq!(post.head_seq, pre.head_seq);
        assert_eq!(post.earliest_seq, pre.earliest_seq);
        assert_eq!(post.count, pre.count);
    }

    // Reopen: state restored from the snapshot (+ replay of the tiny WAL tail).
    let engine = engine_at(dir.path());
    let st = engine.box_state("jobs", false).unwrap();
    assert_eq!(st.head_seq, 10);
    // seqs 1,2 (prefix) and 6 (tag) deleted ⇒ earliest 3, count 7.
    assert_eq!(st.earliest_seq, 3);
    assert_eq!(st.count, 7);
    assert!(st.config.durable);

    let d = engine.diff("jobs", diff_from(0)).unwrap();
    let seqs: Vec<u64> = d.records.iter().map(|r| r.seq).collect();
    assert_eq!(seqs, vec![3, 4, 5, 7, 8, 9, 10], "deleted seqs stay gone");
    assert!(d.tombstone.is_none(), "deletes stay silent after snapshot recovery");
    assert_eq!(d.records[0].data, json!({ "i": 3 }));
    assert_eq!(d.records[0].tag.as_deref(), Some("t3"));
}

/// After a snapshot, the WAL files fully absorbed by the checkpoint are dropped
/// and recovery resumes from the checkpoint — yet a write made AFTER the
/// snapshot (only in the active WAL tail) is still recovered.
#[test]
fn snapshot_drops_absorbed_wal_and_replays_tail() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_box("b", durable_box()).unwrap();
        for i in 1..=4 {
            engine.write("b", one(json!({ "i": i }), None), true).unwrap();
        }
        assert!(engine.write_snapshot().unwrap());
        // A write AFTER the snapshot lives only in the active WAL tail.
        engine.write("b", one(json!({ "i": 5 }), None), true).unwrap();
    }
    let engine = engine_at(dir.path());
    let st = engine.box_state("b", false).unwrap();
    assert_eq!(st.head_seq, 5, "snapshotted 4 + post-snapshot tail write recovered");
    assert_eq!(st.count, 5);
    let d = engine.diff("b", diff_from(0)).unwrap();
    assert_eq!(d.records.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
}

/// Routers + their auto-created boxes survive a snapshot-based restart and keep
/// forwarding from the right cursor.
#[test]
fn routers_survive_snapshot_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine
            .put_router(
                "src->dst",
                RouterCreateRequest {
                    source: "src".into(),
                    dest: "dst".into(),
                    preserve_node: true,
                    preserve_tag: true,
                    create_dest: true,
                    filter: None,
                    allow_cycle: false,
                },
            )
            .unwrap();
        engine.write("src", one(json!({ "x": 1 }), None), true).unwrap();
        assert!(engine.write_snapshot().unwrap());
    }
    let engine = engine_at(dir.path());
    let g = engine.get_router("src->dst").unwrap();
    assert_eq!(g.source, "src");
    assert_eq!(g.forwarded_total, 1, "forward total restored from snapshot");
    // Forwarding still works post-restart, and doesn't re-forward the old record.
    engine.write("src", one(json!({ "x": 2 }), None), true).unwrap();
    let d = engine.diff("dst", diff_from(0)).unwrap();
    assert_eq!(d.records.len(), 2);
    assert_eq!(d.records.iter().map(|r| r.data.clone()).collect::<Vec<_>>(),
        vec![json!({ "x": 1 }), json!({ "x": 2 })]);
}

/// A cap-eviction watermark captured in a snapshot still yields a tombstone
/// after a snapshot-based restart (no silent involuntary loss across restart).
#[test]
fn evict_floor_survives_snapshot_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        let cfg = BoxConfig { cap_records: 3, durable: true, ..BoxConfig::default() };
        engine.put_box("cap", cfg).unwrap();
        for i in 1..=6 {
            engine.write("cap", one(json!({ "i": i }), None), true).unwrap();
        }
        assert!(engine.write_snapshot().unwrap());
    }
    let engine = engine_at(dir.path());
    let st = engine.box_state("cap", false).unwrap();
    assert_eq!(st.head_seq, 6);
    assert_eq!(st.earliest_seq, 4, "cap floor recovered from snapshot");
    assert_eq!(st.count, 3);
    let tomb = engine.diff("cap", diff_from(0)).unwrap().tombstone.expect("cap tombstone");
    assert_eq!(tomb.reason, TombstoneReason::Cap);
}

/// Repeated snapshots keep exactly one snapshot file (older ones pruned) and
/// each round-trips correctly.
#[test]
fn repeated_snapshots_keep_one_and_stay_consistent() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = engine_at(dir.path());
        engine.put_box("b", durable_box()).unwrap();
        for round in 0..3 {
            for i in 1..=3 {
                engine.write("b", one(json!({ "r": round, "i": i }), None), true).unwrap();
            }
            assert!(engine.write_snapshot().unwrap());
            assert_eq!(count_files(dir.path(), "meta", "snapshot-", ".bin"), 1);
        }
    }
    let engine = engine_at(dir.path());
    assert_eq!(engine.box_state("b", false).unwrap().head_seq, 9);
}

// ===========================================================================
// The real acceptance criterion: SIGKILL the actual `streams` binary mid-life
// and confirm an acked durable write is present after restart.
// ===========================================================================

/// Reserve an ephemeral TCP port (then release it for the child to rebind).
fn pick_port() -> u16 {
    let l = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

/// Spawn the `streams` binary on `port` with `data_dir`, return the child.
fn spawn_server(port: u16, data_dir: &std::path::Path) -> std::process::Child {
    std::process::Command::new(env!("CARGO_BIN_EXE_streams"))
        .env("STREAMS_HOST", "127.0.0.1")
        .env("STREAMS_PORT", port.to_string())
        .env("STREAMS_DATA_DIR", data_dir)
        .env("RUST_LOG", "error")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn streams binary")
}

/// Block until `GET /v0/health` answers `200`, or panic after `deadline`.
fn wait_healthy(client: &reqwest::blocking::Client, base: &str, deadline: Duration) {
    let start = Instant::now();
    loop {
        if let Ok(r) = client.get(format!("{base}/v0/health")).send() {
            if r.status().is_success() {
                return;
            }
        }
        if start.elapsed() > deadline {
            panic!("server did not become healthy within {deadline:?}");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn kill_during_durable_write_survives_sigkill_restart() {
    let dir = tempfile::tempdir().unwrap();
    let port = pick_port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    // --- Boot #1, write a DURABLE record, get an ack, then SIGKILL. ---------
    let mut child = spawn_server(port, dir.path());
    wait_healthy(&client, &base, Duration::from_secs(10));

    let (status, _b) = {
        let r = client
            .put(format!("{base}/v0/boxes/jobs"))
            .json(&json!({ "durable": true }))
            .send()
            .unwrap();
        (r.status(), r)
    };
    assert!(status.is_success(), "create durable box");

    // The write response returns ONLY after the fsync (durable:true), so once we
    // hold a 2xx the record is on disk — a SIGKILL now must not lose it.
    let resp = client
        .post(format!("{base}/v0/boxes/jobs"))
        .json(&json!({ "records": [{ "data": { "n": 42 }, "tag": "k" }] }))
        .send()
        .unwrap();
    assert!(resp.status().is_success(), "durable write acked");
    let body: serde_json::Value = resp.json().unwrap();
    assert_eq!(body["head_seq"], 1);
    let fsync_ms = body["performance"]["fsync_ms"].as_f64().unwrap_or(0.0);
    assert!(fsync_ms > 0.0, "durable write must be fsync-gated (fsync_ms>0)");

    // Hard kill — no graceful shutdown, no drop handlers, nothing flushed beyond
    // what the WAL fsync already durably committed.
    let pid = child.id();
    unsafe {
        libc_kill(pid as i32);
    }
    let _ = child.wait();

    // --- Boot #2 on the SAME data dir: the acked durable write is present. ---
    let mut child2 = spawn_server(port, dir.path());
    wait_healthy(&client, &base, Duration::from_secs(10));

    let st: serde_json::Value = client
        .get(format!("{base}/v0/boxes/jobs"))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(st["head_seq"], 1, "durable write survived SIGKILL + restart");
    assert_eq!(st["count"], 1);

    let diff: serde_json::Value = client
        .post(format!("{base}/v0/boxes/jobs/diff"))
        .json(&json!({ "from_seq": 0, "include_tags": true }))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(diff["records"][0]["data"], json!({ "n": 42 }));
    assert_eq!(diff["records"][0]["$tag"], json!("k"));

    // Clean up the second process.
    let _ = child2.kill();
    let _ = child2.wait();
}

/// Minimal SIGKILL via the C `kill(2)` syscall (no extra crate). Unix-only; the
/// test is gated to unix below.
#[cfg(unix)]
unsafe fn libc_kill(pid: i32) {
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    const SIGKILL: i32 = 9;
    kill(pid, SIGKILL);
}

#[cfg(not(unix))]
unsafe fn libc_kill(_pid: i32) {}

/// Send SIGTERM (graceful shutdown) to `pid`.
#[cfg(unix)]
unsafe fn libc_term(pid: i32) {
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    const SIGTERM: i32 = 15;
    kill(pid, SIGTERM);
}

#[cfg(not(unix))]
unsafe fn libc_term(_pid: i32) {}

/// Graceful shutdown writes a snapshot: SIGTERM the real binary, wait for a
/// clean exit, then confirm a snapshot file is present and a fresh boot recovers
/// the data from it.
#[cfg(unix)]
#[test]
fn graceful_shutdown_writes_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let port = pick_port();
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    let mut child = spawn_server(port, dir.path());
    wait_healthy(&client, &base, Duration::from_secs(10));

    // A durable box + a few writes.
    let r = client
        .put(format!("{base}/v0/boxes/jobs"))
        .json(&json!({ "durable": true }))
        .send()
        .unwrap();
    assert!(r.status().is_success());
    for i in 1..=3 {
        let r = client
            .post(format!("{base}/v0/boxes/jobs"))
            .json(&json!({ "records": [{ "data": { "n": i } }] }))
            .send()
            .unwrap();
        assert!(r.status().is_success());
    }

    // Graceful shutdown (SIGTERM) → the server writes a final snapshot on exit.
    let pid = child.id();
    unsafe { libc_term(pid as i32) };
    // Wait for a clean exit (bounded).
    let exited = {
        let start = Instant::now();
        loop {
            match child.try_wait().unwrap() {
                Some(_) => break true,
                None if start.elapsed() > Duration::from_secs(10) => break false,
                None => std::thread::sleep(Duration::from_millis(25)),
            }
        }
    };
    assert!(exited, "server exited gracefully on SIGTERM");

    // A snapshot file must have been written under <data_dir>/meta.
    let snaps = count_files(dir.path(), "meta", "snapshot-", ".bin");
    assert_eq!(snaps, 1, "graceful shutdown wrote exactly one snapshot");

    // Reboot recovers the data (from the snapshot).
    let mut child2 = spawn_server(port, dir.path());
    wait_healthy(&client, &base, Duration::from_secs(10));
    let st: serde_json::Value = client
        .get(format!("{base}/v0/boxes/jobs"))
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(st["head_seq"], 3, "data recovered after graceful-shutdown snapshot");
    assert_eq!(st["count"], 3);

    let _ = child2.kill();
    let _ = child2.wait();
}
