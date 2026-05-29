//! Shared in-process integration harness (Phase-3 §2).
//!
//! Boots the *real* axum server (`streams::http::build_router`) on an ephemeral
//! `127.0.0.1:0` port inside a dedicated background tokio runtime/thread, waits
//! for `/v0/health` to answer `200`, and exposes a small synchronous HTTP client
//! built on `reqwest::blocking` so integration tests read top-to-bottom without
//! `async`/`await`. The server uses a real `SystemClock` (live HTTP), so any
//! TTL/priority *correctness* assertions belong in the engine unit/property
//! tests with a `TestClock`; this harness is for black-box wire-contract flows.
//!
//! Each `Harness` owns its own engine + port, so tests are fully isolated and
//! may run in parallel.
//!
//! # Public API
//!
//! ```ignore
//! use common::Harness;
//! use serde_json::json;
//!
//! let h = Harness::start();                  // boot a fresh server, wait for health
//! let url = h.base_url();                     // e.g. "http://127.0.0.1:53124"
//!
//! // JSON helpers -> (StatusCode, serde_json::Value):
//! let (status, body) = h.put("/v0/boxes/jobs", json!({ "durable": true }));
//! let (status, body) = h.post("/v0/boxes/jobs", json!({ "records": [{ "data": 1 }] }));
//! let (status, body) = h.get("/v0/boxes/jobs");
//! let (status, body) = h.delete("/v0/boxes/jobs");
//! // `post`/`put`/`delete` send `Content-Type: application/json` automatically.
//! // For an explicit empty body use `post_empty(path)`.
//!
//! // SSE helper: open a watch stream and collect the first N named frames with a
//! // bounded timeout (never hangs on the long-lived stream):
//! let frames = h.sse_frames("/v0/watch/wid_0000000001", 2, Duration::from_secs(5));
//! for f in &frames { println!("{} {} {}", f.id, f.event, f.data); }
//! ```

#![allow(dead_code)] // not every test uses every helper.

use std::net::{Ipv4Addr, SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use reqwest::blocking::Client;
pub use reqwest::StatusCode;
use serde_json::Value;

use streams::clock::{SharedClock, SystemClock};
use streams::config::ServerConfig;
use streams::engine::Engine;
use streams::http;

/// A booted in-process server plus a blocking HTTP client pointed at it.
pub struct Harness {
    base_url: String,
    client: Client,
    /// Kept alive so the background runtime/thread lives as long as the harness.
    _shutdown: tokio::sync::oneshot::Sender<()>,
    _thread: thread::JoinHandle<()>,
    /// Unique per-harness data dir for the WAL; removed on drop so tests stay
    /// isolated and leave no on-disk residue.
    _data_dir: tempfile::TempDir,
}

impl Harness {
    /// Boot a fresh server on an ephemeral port with default (auth-disabled)
    /// config and wait until `/v0/health` returns `200`. Panics on any failure
    /// so the calling test fails loudly.
    pub fn start() -> Harness {
        Harness::start_with(ServerConfig::default())
    }

    /// Like [`start`](Self::start) but with a caller-supplied [`ServerConfig`]
    /// (e.g. to enable bearer auth via `api_keys`). Each harness gets a UNIQUE
    /// temp data dir for the WAL (via `tempfile::tempdir`), so the durable write
    /// path is exercised while tests stay isolated and leave nothing behind.
    pub fn start_with(mut config: ServerConfig) -> Harness {
        // Reserve an ephemeral port on the std listener, then hand the address to
        // the async runtime (which re-binds it). Closing this std listener first
        // avoids the address being held while tokio binds.
        let std_listener =
            StdTcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind ephemeral port");
        let addr: SocketAddr = std_listener.local_addr().expect("local_addr");
        drop(std_listener);

        // Unique per-harness data dir; kept alive (and auto-removed) by the
        // `_data_dir` field on the returned `Harness`.
        let data_dir = tempfile::tempdir().expect("create temp data dir");
        config.data_dir = Some(data_dir.path().to_string_lossy().into_owned());

        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let thread = thread::Builder::new()
            .name("streams-harness".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .expect("build harness runtime");
                rt.block_on(async move {
                    let clock: SharedClock = Arc::new(SystemClock);
                    let engine =
                        Engine::with_data_dir(config, clock).expect("open durable engine");
                    let app = http::build_router(engine);

                    let listener = tokio::net::TcpListener::bind(addr)
                        .await
                        .expect("rebind ephemeral port");
                    let _ = ready_tx.send(());
                    let server = axum::serve(listener, app).with_graceful_shutdown(async {
                        let _ = shutdown_rx.await;
                    });
                    let _ = server.await;
                });
            })
            .expect("spawn harness thread");

        // Wait for the listener to be bound before issuing requests.
        ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("server failed to bind");

        let base_url = format!("http://{addr}");
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("build reqwest client");

        let h = Harness {
            base_url,
            client,
            _shutdown: shutdown_tx,
            _thread: thread,
            _data_dir: data_dir,
        };
        h.wait_healthy(Duration::from_secs(5));
        h
    }

    /// The server base URL, e.g. `http://127.0.0.1:53124` (no trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Block until `GET /v0/health` returns `200`, or panic after `deadline`.
    fn wait_healthy(&self, deadline: Duration) {
        let start = Instant::now();
        loop {
            if let Ok(resp) = self.client.get(self.url("/v0/health")).send() {
                if resp.status().is_success() {
                    return;
                }
            }
            if start.elapsed() > deadline {
                panic!("server did not become healthy within {deadline:?}");
            }
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    // -- JSON request helpers -> (StatusCode, Value) -------------------------

    /// `GET path` (no body). Returns `(status, parsed-json-or-Null)`.
    pub fn get(&self, path: &str) -> (StatusCode, Value) {
        self.send(self.client.get(self.url(path)))
    }

    /// `POST path` with a JSON body and `Content-Type: application/json`.
    pub fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.send(self.client.post(self.url(path)).json(&body))
    }

    /// `PUT path` with a JSON body and `Content-Type: application/json`.
    pub fn put(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.send(self.client.put(self.url(path)).json(&body))
    }

    /// `DELETE path` (no body). Returns `(status, parsed-json-or-Null)`.
    pub fn delete(&self, path: &str) -> (StatusCode, Value) {
        self.send(self.client.delete(self.url(path)))
    }

    /// `POST path` with an explicit empty body (no `Content-Type`). Useful to
    /// exercise the `415`/empty-body paths.
    pub fn post_empty(&self, path: &str) -> (StatusCode, Value) {
        self.send(self.client.post(self.url(path)))
    }

    /// `POST path` with a JSON body and a bearer token header.
    pub fn post_auth(&self, path: &str, body: Value, token: &str) -> (StatusCode, Value) {
        self.send(self.client.post(self.url(path)).bearer_auth(token).json(&body))
    }

    /// `GET path` with a bearer token header.
    pub fn get_auth(&self, path: &str, token: &str) -> (StatusCode, Value) {
        self.send(self.client.get(self.url(path)).bearer_auth(token))
    }

    /// Send a prepared request, returning `(status, body-as-json-or-Null)`.
    fn send(&self, req: reqwest::blocking::RequestBuilder) -> (StatusCode, Value) {
        let resp = req.send().expect("request failed to send");
        let status = resp.status();
        let bytes = resp.bytes().expect("read response body");
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        (status, value)
    }

    // -- SSE helper ----------------------------------------------------------

    /// Open the SSE stream at `path` and collect up to `max_frames` *named-event*
    /// frames (`id`/`event`/`data`), abandoning the read after `deadline` so a
    /// long-lived stream can never hang the test. Heartbeat comments and bare
    /// `retry:` frames are skipped. Panics if the stream does not open `200` with
    /// a `text/event-stream` content-type.
    pub fn sse_frames(&self, path: &str, max_frames: usize, deadline: Duration) -> Vec<SseFrame> {
        use std::io::Read;

        let mut resp = self
            .client
            .get(self.url(path))
            .header(reqwest::header::ACCEPT, "text/event-stream")
            // A read timeout bounds each chunk read so the loop can't block past
            // the deadline if the producer parks.
            .timeout(deadline)
            .send()
            .expect("open SSE stream");
        assert_eq!(resp.status(), StatusCode::OK, "SSE stream must open 200");
        let ct = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            ct.starts_with("text/event-stream"),
            "SSE content-type must be text/event-stream, got {ct:?}"
        );

        let mut buf: Vec<u8> = Vec::new();
        let mut frames: Vec<SseFrame> = Vec::new();
        let mut chunk = [0u8; 4096];
        let start = Instant::now();
        while frames.len() < max_frames && start.elapsed() < deadline {
            match resp.read(&mut chunk) {
                Ok(0) => break, // stream closed.
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    drain_frames(&mut buf, &mut frames);
                }
                Err(_) => break, // timeout / reset: return what we have.
            }
        }
        drain_frames(&mut buf, &mut frames);
        frames
    }
}

/// A parsed SSE frame: a named event with its `id`/`event`/`data` lines joined.
#[derive(Debug, Clone)]
pub struct SseFrame {
    pub id: String,
    pub event: String,
    pub data: String,
}

/// Split the byte buffer on blank-line (`\n\n`) frame boundaries and parse each
/// complete block, leaving any partial trailing block in `buf`.
fn drain_frames(buf: &mut Vec<u8>, out: &mut Vec<SseFrame>) {
    let text = String::from_utf8_lossy(buf).into_owned();
    let bytes = text.as_bytes();
    let mut start = 0usize;
    let mut last_end = 0usize;
    let mut i = 0usize;
    while i + 1 < bytes.len() {
        if bytes[i] == b'\n' && bytes[i + 1] == b'\n' {
            let end = i + 2;
            if let Some(frame) = parse_frame(&text[start..end]) {
                out.push(frame);
            }
            start = end;
            last_end = end;
            i += 2;
        } else {
            i += 1;
        }
    }
    if last_end > 0 {
        buf.drain(0..last_end.min(buf.len()));
    }
}

/// Parse one SSE block into a named-event frame, or `None` for a comment
/// (`:` heartbeat) or a block that carries only `retry:`.
fn parse_frame(raw: &str) -> Option<SseFrame> {
    let mut id = String::new();
    let mut event = String::new();
    let mut data = String::new();
    let mut has_event = false;
    for line in raw.lines() {
        if line.is_empty() || line.starts_with(':') {
            continue;
        }
        if let Some(v) = line.strip_prefix("id:") {
            id = v.trim().to_string();
        } else if let Some(v) = line.strip_prefix("event:") {
            event = v.trim().to_string();
            has_event = true;
        } else if let Some(v) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(v.strip_prefix(' ').unwrap_or(v));
        }
    }
    if has_event {
        Some(SseFrame { id, event, data })
    } else {
        None
    }
}
