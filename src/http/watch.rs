//! Multiplexed SSE watch: POST `/v0/watch` (create session) and
//! GET `/v0/watch/:wid` (open the SSE stream).
//!
//! Frame types (API §7.5): `record`, `tombstone`, `caught-up`, `box-deleted`,
//! `error`; data-bearing frames carry a composite base64url `id:` (the per-box
//! `box → seq` cursor map), heartbeats are bare `:` comments, and `retry:` is
//! sent once at open. Resume via `Last-Event-ID`.

use super::AppState;
use crate::config;
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::types::*;
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap},
    response::sse::{Event, KeepAlive, Sse},
    response::{IntoResponse, Json, Response},
};
use crate::engine::broadcast::FrameVariant;
use base64::Engine as _;
use dashmap::DashMap;
use futures::stream::Stream;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::value::RawValue;
use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

/// The `record` SSE frame envelope. Fields are declared in **sorted key order**
/// (`box`,`from_seq`,`head_seq`,`records`,`to_seq`) so `serde_json` emits bytes
/// byte-identical to the prior `serde_json::json!` map (which sorts keys), while
/// `records` embeds the shared, pre-serialized [`RawValue`] frames verbatim
/// (zero re-serialization of the record bodies).
#[derive(Serialize)]
struct RecordEnvelope<'a> {
    #[serde(rename = "box")]
    box_name: &'a str,
    from_seq: u64,
    head_seq: u64,
    #[serde(serialize_with = "serialize_shared_frames")]
    records: Vec<Arc<RawValue>>,
    to_seq: u64,
}

/// Serialize a slice of shared `Arc<RawValue>` frames as a JSON array, embedding
/// each pre-serialized frame verbatim (`serde_json` recognizes `&RawValue` and
/// copies its bytes without re-parsing). Dereferencing `Arc` to `&RawValue` side-
/// steps the missing `Serialize for Arc<RawValue>` bound.
fn serialize_shared_frames<S>(
    frames: &[Arc<RawValue>],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeSeq;
    let mut seq = serializer.serialize_seq(Some(frames.len()))?;
    for f in frames {
        seq.serialize_element(f.as_ref())?;
    }
    seq.end()
}

/// A stored watch session: the immutable subscription definition plus the
/// authoritative, mutable per-box cursor map (so a GET reconnect resumes
/// exactly; API §7.1/§7.4).
pub struct Session {
    pub req: WatchCreateRequest,
    /// Authoritative `box → last-delivered seq` cursor map.
    pub cursors: Mutex<BTreeMap<String, u64>>,
    /// The principal (api key) that created this session, when auth is enabled.
    /// `None` in dev mode (no keys). The GET stream is authorized by *possessing*
    /// the unguessable `wid` (a bearer capability); a presented bearer, if any,
    /// must also match this binding (defense in depth). See [`Session::authorize`].
    pub principal: Option<String>,
}

impl Session {
    /// Authorize a GET-stream request against this session's principal binding.
    ///
    /// The `wid` itself is the capability: holding the unguessable random `wid`
    /// authorizes the stream (API §7.1). When the session is bound to a principal
    /// (auth enabled), a presented bearer token — if any — must match that
    /// principal in constant time; presenting *no* bearer is allowed (the wid
    /// alone suffices, which is the `EventSource` case). An unbound session (dev
    /// mode) is always allowed.
    pub fn authorize(&self, presented_bearer: Option<&str>) -> bool {
        match (&self.principal, presented_bearer) {
            // Unbound (dev mode): the wid alone authorizes.
            (None, _) => true,
            // Bound, no bearer presented: the wid capability is sufficient.
            (Some(_), None) => true,
            // Bound, bearer presented: it must match the creating principal.
            (Some(p), Some(b)) => {
                use subtle::ConstantTimeEq;
                let m: bool = p.as_bytes().ct_eq(b.as_bytes()).into();
                m
            }
        }
    }
}

/// In-memory registry of watch sessions, keyed by `wid`. Phase 2 keeps them in
/// a `DashMap`; phase 4 may persist. GC of idle sessions is best-effort.
pub struct SessionStore {
    sessions: DashMap<String, Arc<Session>>,
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStore {
    pub fn new() -> Self {
        SessionStore {
            sessions: DashMap::new(),
        }
    }

    /// Mint an UNGUESSABLE watch capability: `wid_` + base64url of 16 random
    /// bytes (128 bits) from the OS CSPRNG. The `wid_` prefix keeps the documented
    /// shape and the path charset; the random suffix makes the `wid` a true bearer
    /// capability that cannot be enumerated (the old monotonic `wid_{n:010x}` was
    /// trivially guessable). Collisions are cryptographically negligible.
    fn alloc_wid() -> String {
        let mut bytes = [0u8; 16];
        rand::fill(&mut bytes);
        let suffix = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
        format!("wid_{suffix}")
    }

    fn insert(&self, session: Session) -> String {
        let wid = Self::alloc_wid();
        self.sessions.insert(wid.clone(), Arc::new(session));
        wid
    }

    fn get(&self, wid: &str) -> Option<Arc<Session>> {
        self.sessions.get(wid).map(|s| s.clone())
    }
}

/// `POST /v0/watch` — create a watch session; returns a `wid` + `stream_url`.
///
/// Validates the `boxes` map (size, names) and resolves each box's initial
/// `from_seq`/`tail` against current watermarks, returning per-box
/// head/earliest so the client can see fall-off before streaming. `?lenient=true`
/// skips unknown boxes instead of `404`.
pub async fn create_watch(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    extensions: axum::http::Extensions,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Result<Json<WatchCreateResponse>> {
    let mut req: WatchCreateRequest = super::parse_json_body(&headers, &body)?;

    if req.boxes.is_empty() {
        return Err(Error::invalid_request("watch must name >=1 box"));
    }
    if req.boxes.len() > config::MAX_WATCH_BOXES {
        return Err(Error::invalid_request(format!(
            "watch names {} boxes, exceeds max {}",
            req.boxes.len(),
            config::MAX_WATCH_BOXES
        )));
    }
    // Clamp heartbeat into the documented bounds (API §7.2).
    req.heartbeat_ms = req
        .heartbeat_ms
        .clamp(config::MIN_HEARTBEAT_MS, config::MAX_HEARTBEAT_MS);

    let lenient = super::query_bool(&params, "lenient", false);
    let states = state.engine.watch_box_states(&req.boxes, lenient)?;

    // Seed the authoritative cursor map from the resolved per-box `from_seq`.
    let mut cursors = BTreeMap::new();
    for (name, st) in &states {
        cursors.insert(name.clone(), st.from_seq);
    }

    // Bind the session to the authenticated creator (when auth is enabled) so the
    // capability `wid` cannot be replayed under a different principal. The
    // `Principal` is stashed by the auth middleware; absent in dev mode.
    let principal = extensions
        .get::<super::Principal>()
        .map(|p| p.0.clone());

    let wid = state.sessions.insert(Session {
        req: req.clone(),
        cursors: Mutex::new(cursors),
        principal,
    });

    Ok(Json(WatchCreateResponse {
        stream_url: format!("/v0/watch/{wid}"),
        wid,
        session_ttl_ms: config::SESSION_TTL_MS,
        boxes: states,
        performance: Performance::default(),
    }))
}

/// `GET /v0/watch/:wid` — open the SSE stream for a session.
///
/// Validates `Accept: text/event-stream` (else `406`), resolves the session and
/// any `Last-Event-ID` rewind, then streams named events with low-latency
/// headers (`X-Accel-Buffering: no`, `Cache-Control: no-store`).
pub async fn stream_watch(
    State(state): State<AppState>,
    Path(wid): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response> {
    require_event_stream_accept(&headers)?;

    let session = state
        .sessions
        .get(&wid)
        .ok_or_else(|| Error::new(ErrorCode::NotFound, "watch session not found (re-POST)"))?;

    // Authorize: holding the unguessable `wid` is the capability. When the session
    // is bound to a principal (auth enabled), a *presented* bearer (header, or the
    // dev-only `?token=` fallback) must match the creating principal; presenting
    // none is allowed (the wid alone authorizes, the `EventSource` case).
    let presented_bearer = bearer_from_request(&headers, &params);
    if !session.authorize(presented_bearer.as_deref()) {
        return Err(Error::new(
            ErrorCode::Unauthorized,
            "watch token does not match the session's principal",
        ));
    }

    // `Last-Event-ID` (or the `cursor` query) may rewind the session cursors to
    // an exact prior map — never advance past the authoritative server state.
    if let Some(leid) = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
    {
        if let Some(map) = decode_cursor_id(leid) {
            let mut cursors = session.cursors.lock();
            for (b, seq) in map {
                if let Some(cur) = cursors.get_mut(&b) {
                    // Rewind only: take the lower of stored vs resumed.
                    *cur = (*cur).min(seq);
                }
            }
        }
    }

    let engine = state.engine.clone();
    let stream = build_stream(engine, session);

    let sse = Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text(": hb"),
    );

    // Low-latency headers (API §7.3).
    let mut resp = sse.into_response();
    let h = resp.headers_mut();
    h.insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    h.insert("x-accel-buffering", "no".parse().unwrap());
    Ok(resp)
}

/// Build the SSE event stream for a resolved session. Reuses the engine's diff
/// primitive per box (TTL + deleted skip + node filter + tombstone), emits
/// `record`/`tombstone`/`caught-up`/`box-deleted` frames with composite `id:`
/// cursors, and parks on each box's `Notify` between flushes (no busy poll).
fn build_stream(
    engine: Arc<Engine>,
    session: Arc<Session>,
) -> impl Stream<Item = std::result::Result<Event, Infallible>> {
    let heartbeat_ms = session.req.heartbeat_ms;
    // The projection variant for this session's record frames (drives which
    // shared broadcast-cache slot every record hits).
    let variant = FrameVariant::new(
        session.req.include_data,
        session.req.include_tags,
        session.req.include_meta,
    );
    async_stream::stream! {
        // `retry:` once at open (deliberate 2 s backoff; API §7.5).
        yield Ok(Event::default().retry(Duration::from_millis(config::SSE_RETRY_MS)));

        // Track which boxes we've already reported as deleted (terminal per box)
        // and whether each box was last seen caught-up (to re-emit on the
        // backlog→tailing transition only).
        let box_names: Vec<String> =
            session.cursors.lock().keys().cloned().collect();
        let mut deleted: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        let mut was_caught_up: HashMap<String, bool> = HashMap::new();
        // Boxes not yet read once: a tombstone on the *first* read is the
        // connect-time "offset out of range" case (`from_seq_too_old`; API §7.5),
        // distinct from a gap that crosses the cursor while live.
        let mut first_read: std::collections::HashSet<String> =
            box_names.iter().cloned().collect();

        loop {
            // Hold the live box `Arc`s for this pass so the `Notified` futures
            // we build at the end (which borrow each box's `Notify`) outlive the
            // per-box loop body.
            let mut live: Vec<Arc<crate::engine::box_state::BoxState>> = Vec::new();

            for name in &box_names {
                if deleted.contains(name) {
                    continue;
                }
                let Some(b) = engine.get_box(name) else {
                    // Box vanished mid-watch ⇒ terminal box-deleted frame.
                    let head = 0;
                    deleted.insert(name.clone());
                    let id = encode_session_id(&session);
                    let data = serde_json::json!({
                        "box": name, "head_seq": head, "reason": "deleted"
                    });
                    yield Ok(Event::default()
                        .id(id)
                        .event("box-deleted")
                        .data(data.to_string()));
                    continue;
                };
                live.push(b.clone());

                // Drain this box up to head in `limit`-sized batches.
                loop {
                    let from_seq = session
                        .cursors
                        .lock()
                        .get(name)
                        .copied()
                        .unwrap_or(0);
                    let req = DiffRequest {
                        from_seq,
                        limit: session.req.limit,
                        node: session.req.node.clone(),
                        include_tags: session.req.include_tags,
                        include_meta: session.req.include_meta,
                        wait_ms: 0,
                    };
                    let Ok(d) = engine.diff(name, req) else {
                        // Diff only fails with box_not_found here.
                        deleted.insert(name.clone());
                        let id = encode_session_id(&session);
                        let data = serde_json::json!({
                            "box": name, "head_seq": 0, "reason": "deleted"
                        });
                        yield Ok(Event::default()
                            .id(id)
                            .event("box-deleted")
                            .data(data.to_string()));
                        break;
                    };

                    // A tombstone crossed this consumer's cursor: emit it first,
                    // its `id` already advances the box cursor to `gap_to`.
                    if let Some(tomb) = &d.tombstone {
                        session
                            .cursors
                            .lock()
                            .insert(name.clone(), tomb.gap_to);
                        // On the first read of a box, a below-floor cursor is the
                        // connect-time `from_seq_too_old` variant (API §7.5);
                        // afterward, report the engine's cap/ttl/mixed reason.
                        let reason = if first_read.contains(name) {
                            TombstoneReason::FromSeqTooOld
                        } else {
                            tomb.reason
                        };
                        let id = encode_session_id(&session);
                        let data = serde_json::json!({
                            "box": name,
                            "reason": reason,
                            "gap_from": tomb.gap_from,
                            "gap_to": tomb.gap_to,
                            "earliest_seq": tomb.earliest_seq,
                            "head_seq": tomb.head_seq,
                        });
                        yield Ok(Event::default()
                            .id(id)
                            .event("tombstone")
                            .data(data.to_string()));
                        was_caught_up.insert(name.clone(), false);
                    }
                    first_read.remove(name);

                    // Advance the authoritative cursor past everything examined.
                    let to_seq = d.next_from_seq;
                    if !d.records.is_empty() {
                        // Zero-copy broadcast: each record frame is serialized
                        // ONCE per box and shared (ref-counted `Arc<RawValue>`)
                        // across all watchers via the box's broadcast cache,
                        // instead of re-serializing per connection. The envelope
                        // (`box`/`from_seq`/`to_seq`/`head_seq`) and the composite
                        // `id:` cursor are still per-connection (they depend on
                        // this session's cursor map). The struct's field order is
                        // sorted to stay byte-identical to the old `json!` map.
                        let records: Vec<Arc<RawValue>> = d
                            .records
                            .iter()
                            .map(|r| b.broadcast.frame(r.seq, r, variant))
                            .collect();
                        session.cursors.lock().insert(name.clone(), to_seq);
                        let id = encode_session_id(&session);
                        let payload = RecordEnvelope {
                            box_name: name.as_str(),
                            from_seq,
                            head_seq: d.head_seq,
                            records,
                            to_seq,
                        };
                        let body = serde_json::to_string(&payload)
                            .unwrap_or_else(|_| "{}".to_string());
                        yield Ok(Event::default()
                            .id(id)
                            .event("record")
                            .data(body));
                        was_caught_up.insert(name.clone(), false);
                    } else if d.tombstone.is_none() {
                        // No records and no tombstone, but the cursor may still
                        // have advanced past filtered records; persist it.
                        session.cursors.lock().insert(name.clone(), to_seq);
                    }

                    if d.caught_up {
                        // Emit `caught-up` once per backlog→tailing transition.
                        if !was_caught_up.get(name).copied().unwrap_or(false) {
                            let id = encode_session_id(&session);
                            let data = serde_json::json!({
                                "box": name, "head_seq": d.head_seq
                            });
                            yield Ok(Event::default()
                                .id(id)
                                .event("caught-up")
                                .data(data.to_string()));
                            was_caught_up.insert(name.clone(), true);
                        }
                        break;
                    }
                }
            }

            // If every box is terminal (deleted), end the stream.
            if box_names.iter().all(|n| deleted.contains(n)) {
                break;
            }

            // Drained pass: park until any watched box appends or the heartbeat
            // window elapses, then re-check. Tokio `Notify` wakeups give the
            // ~1-5 ms push target without busy polling (API §7.6); the axum
            // `KeepAlive` layer emits the `: hb` comment on its own cadence.
            let notifies: Vec<_> = live.iter().map(|b| Box::pin(b.notify.notified())).collect();
            if notifies.is_empty() {
                // No live boxes to wait on; just honor the heartbeat tick.
                tokio::time::sleep(Duration::from_millis(heartbeat_ms)).await;
            } else {
                let wake = futures::future::select_all(notifies);
                tokio::select! {
                    _ = wake => {}
                    _ = tokio::time::sleep(Duration::from_millis(heartbeat_ms)) => {}
                }
            }
        }
    }
}

/// Project a read record onto the SSE `record`-frame JSON, honoring
/// `include_data` (lightweight metadata-only tailing; API §7.5).
///
/// Also used by the zero-copy broadcast cache
/// ([`crate::engine::broadcast`]) to serialize each frame **once** and share the
/// resulting buffer across all watchers — so this MUST stay the single source of
/// truth for a record frame's bytes.
pub(crate) fn record_frame(r: &RecordOut, include_data: bool) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("$seq".into(), serde_json::json!(r.seq));
    obj.insert("$ts".into(), serde_json::json!(r.ts));
    if let Some(node) = &r.node {
        obj.insert("$node".into(), serde_json::json!(node));
    }
    if let Some(tag) = &r.tag {
        obj.insert("$tag".into(), serde_json::json!(tag));
    }
    if include_data {
        obj.insert("data".into(), r.data.clone());
    }
    if let Some(meta) = &r.meta {
        obj.insert("meta".into(), meta.clone());
    }
    serde_json::Value::Object(obj)
}

/// Encode the session's current per-box cursor map as a base64url JSON id
/// (API §7.4). Used as both the SSE `id:` and the `Last-Event-ID` resume token.
fn encode_session_id(session: &Session) -> String {
    let map = session.cursors.lock().clone();
    let json = serde_json::to_vec(&map).unwrap_or_default();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json)
}

/// Decode a `Last-Event-ID` / `cursor` composite id back to a `box → seq` map.
fn decode_cursor_id(id: &str) -> Option<BTreeMap<String, u64>> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(id)
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Extract a presented bearer for the stream GET: the `Authorization: Bearer`
/// header (preferred), falling back to the dev-only `?token=` query parameter
/// (already URL-decoded by axum's `Query` extractor). Returns `None` when neither
/// is present.
fn bearer_from_request(headers: &HeaderMap, params: &HashMap<String, String>) -> Option<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|t| t.trim().to_string())
        .or_else(|| params.get("token").cloned())
}

/// Reject a stream GET whose `Accept` is not `text/event-stream` (API §7,
/// `406 not_acceptable`).
fn require_event_stream_accept(headers: &HeaderMap) -> Result<()> {
    let accept = headers
        .get(axum::http::header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // An absent/`*/*` Accept is tolerated for curl-style clients.
    if accept.is_empty() || accept.contains("text/event-stream") || accept.contains("*/*") {
        Ok(())
    } else {
        Err(Error::new(
            ErrorCode::NotAcceptable,
            "Accept must be text/event-stream",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composite_id_round_trips() {
        let mut cursors = BTreeMap::new();
        cursors.insert("jobs".to_string(), 5210u64);
        cursors.insert("events".to_string(), 88130u64);
        let session = Session {
            req: WatchCreateRequest {
                node: None,
                boxes: HashMap::new(),
                limit: 256,
                max_batch_bytes: 262_144,
                heartbeat_ms: 15_000,
                include_meta: true,
                include_tags: false,
                include_data: true,
                consistency: Consistency::Eventual,
            },
            cursors: Mutex::new(cursors),
            principal: None,
        };
        let id = encode_session_id(&session);
        let decoded = decode_cursor_id(&id).expect("decodes");
        assert_eq!(decoded.get("jobs"), Some(&5210));
        assert_eq!(decoded.get("events"), Some(&88130));
    }

    fn empty_session(principal: Option<String>) -> Session {
        Session {
            req: WatchCreateRequest {
                node: None,
                boxes: HashMap::new(),
                limit: 256,
                max_batch_bytes: 262_144,
                heartbeat_ms: 15_000,
                include_meta: true,
                include_tags: false,
                include_data: true,
                consistency: Consistency::Eventual,
            },
            cursors: Mutex::new(BTreeMap::new()),
            principal,
        }
    }

    #[test]
    fn session_authorize_capability_and_binding() {
        // Unbound (dev mode): the wid alone authorizes, with or without a bearer.
        let dev = empty_session(None);
        assert!(dev.authorize(None));
        assert!(dev.authorize(Some("anything")));

        // Bound: no bearer presented ⇒ the wid capability suffices.
        let bound = empty_session(Some("s3cr3t".to_string()));
        assert!(bound.authorize(None));
        // Bound + matching bearer ⇒ ok.
        assert!(bound.authorize(Some("s3cr3t")));
        // Bound + mismatched bearer ⇒ rejected (cannot replay under another key).
        assert!(!bound.authorize(Some("wrong")));
    }

    #[test]
    fn alloc_wid_is_prefixed_random_and_unique() {
        let a = SessionStore::alloc_wid();
        let b = SessionStore::alloc_wid();
        assert!(a.starts_with("wid_"), "wid keeps the documented prefix: {a}");
        assert_ne!(a, b, "wids must be unique/random, not monotonic");
        // 16 random bytes ⇒ 22 base64url chars (no pad). Total len = 4 + 22 = 26.
        assert_eq!(a.len(), 26, "wid carries >=128 bits of randomness: {a}");
        // Path-safe (base64url + the `_` from the prefix), no `/` or `+` or `=`.
        let suffix = a.strip_prefix("wid_").unwrap();
        assert!(
            suffix.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
            "suffix is base64url: {suffix}"
        );
    }

    #[test]
    fn accept_guard_rejects_non_sse() {
        let mut h = HeaderMap::new();
        h.insert(header::ACCEPT, "application/json".parse().unwrap());
        assert_eq!(
            require_event_stream_accept(&h).unwrap_err().code,
            ErrorCode::NotAcceptable
        );
        h.insert(header::ACCEPT, "text/event-stream".parse().unwrap());
        assert!(require_event_stream_accept(&h).is_ok());
    }
}
