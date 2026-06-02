//! JSON-over-WebSocket data plane.
//!
//! `/v0/ws` sits next to the SSE watch API, but keeps one bidirectional socket open
//! for dynamic subscribe/unsubscribe plus publish commands. It reuses the same
//! engine write and diff paths so durability, filters, cursors, and auth semantics
//! stay consistent with HTTP/SSE.

use super::{run_blocking, AppState};
use crate::auth::{Principal, Scope};
use crate::engine::broadcast::FrameVariant;
use crate::error::{Error, Result};
use crate::types::{
    DiffRequest, ErrorCode, NodeFilter, RecordIn, TopicConfig, WatchTopicOptions, WriteRequest,
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::{IntoResponse, Response},
    Extension,
};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{value::RawValue, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Notify};

#[derive(Debug, Clone)]
struct WsSub {
    cursor: u64,
    node: Option<NodeFilter>,
    limit: u32,
    max_batch_bytes: u64,
    include_data: bool,
    include_tags: bool,
    include_meta: bool,
    caught_up: bool,
}

type SharedSubs = Arc<parking_lot::Mutex<HashMap<String, WsSub>>>;

// Keep WebSocket backpressure close to the socket. A large per-client message
// queue turns overload into seconds of buffered JSON and hundreds of MiB of RAM.
const WS_OUTBOUND_CHANNEL_CAP: usize = 4;

#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum WsCommand {
    Subscribe {
        #[serde(default)]
        request_id: Option<Value>,
        #[serde(default)]
        topic: Option<String>,
        #[serde(default)]
        topics: HashMap<String, WatchTopicOptions>,
        #[serde(default)]
        from_seq: u64,
        #[serde(default)]
        tail: bool,
        #[serde(default)]
        node: Option<NodeFilter>,
        #[serde(default = "default_ws_limit")]
        limit: u32,
        #[serde(default = "default_ws_max_batch_bytes")]
        max_batch_bytes: u64,
        #[serde(default = "default_true")]
        include_data: bool,
        #[serde(default)]
        include_tags: bool,
        #[serde(default = "default_true")]
        include_meta: bool,
        #[serde(default)]
        lenient: bool,
    },
    Unsubscribe {
        #[serde(default)]
        request_id: Option<Value>,
        #[serde(default)]
        topic: Option<String>,
        #[serde(default)]
        topics: Vec<String>,
    },
    Publish {
        #[serde(default)]
        request_id: Option<Value>,
        topic: String,
        records: Vec<RecordIn>,
        #[serde(default)]
        node: Option<String>,
        #[serde(default)]
        idempotency_key: Option<String>,
        #[serde(default)]
        create: Option<bool>,
        #[serde(default)]
        config: Option<TopicConfig>,
        #[serde(default)]
        disable_backpressure: bool,
        #[serde(default = "default_return_seqs")]
        return_seqs: bool,
    },
    Ping {
        #[serde(default)]
        request_id: Option<Value>,
    },
}

fn default_return_seqs() -> bool {
    true
}

fn default_true() -> bool {
    true
}

fn default_ws_limit() -> u32 {
    1000
}

fn default_ws_max_batch_bytes() -> u64 {
    crate::config::DEFAULT_MAX_BATCH_BYTES
}

#[derive(Serialize)]
struct WsRecordEnvelope<'a> {
    op: &'static str,
    topic: &'a str,
    from_seq: u64,
    to_seq: u64,
    head_seq: u64,
    #[serde(serialize_with = "serialize_shared_frames")]
    records: Vec<Arc<RawValue>>,
}

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

/// `GET /v0/ws` — bidirectional dynamic watch/publish WebSocket.
pub async fn websocket(
    State(state): State<AppState>,
    Extension(principal): Extension<Principal>,
    ws: WebSocketUpgrade,
) -> Result<Response> {
    let guard = state
        .live
        .try_acquire_sse(&state.engine.config.limits, principal.key_id)
        .ok_or_else(|| {
            Error::new(
                ErrorCode::Throttled,
                "too many concurrent WebSocket connections",
            )
            .with_retry_after(crate::limits::LIMIT_RETRY_AFTER_S)
        })?;

    Ok(ws
        .on_upgrade(move |socket| handle_socket(socket, state, principal, guard))
        .into_response())
}

async fn handle_socket(
    socket: WebSocket,
    state: AppState,
    principal: Principal,
    _guard: crate::limits::SseGuard,
) {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (out_tx, mut out_rx) = mpsc::channel::<Message>(WS_OUTBOUND_CHANNEL_CAP);
    let subs: SharedSubs = Arc::new(parking_lot::Mutex::new(HashMap::new()));
    let changed = Arc::new(Notify::new());

    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if ws_tx.send(msg).await.is_err() {
                break;
            }
        }
    });

    let deliver = tokio::spawn(delivery_loop(
        state.clone(),
        subs.clone(),
        changed.clone(),
        out_tx.clone(),
    ));

    while let Some(msg) = ws_rx.next().await {
        let Ok(msg) = msg else { break };
        match msg {
            Message::Text(text) => {
                handle_command(&state, &principal, &subs, &changed, &out_tx, text.as_str()).await;
            }
            Message::Binary(_) => {
                send_json(
                    &out_tx,
                    serde_json::json!({
                        "op": "error",
                        "code": "unsupported_message",
                        "message": "binary WebSocket messages are not supported by this JSON endpoint"
                    }),
                )
                .await;
            }
            Message::Ping(bytes) => {
                let _ = out_tx.send(Message::Pong(bytes)).await;
            }
            Message::Close(_) => break,
            Message::Pong(_) => {}
        }
    }

    deliver.abort();
    drop(out_tx);
    let _ = writer.await;
}

async fn handle_command(
    state: &AppState,
    principal: &Principal,
    subs: &SharedSubs,
    changed: &Notify,
    out_tx: &mpsc::Sender<Message>,
    text: &str,
) {
    let cmd: WsCommand = match serde_json::from_str(text) {
        Ok(cmd) => cmd,
        Err(e) => {
            send_error(
                out_tx,
                None,
                "invalid_json",
                &format!("invalid command JSON: {e}"),
            )
            .await;
            return;
        }
    };

    match cmd {
        WsCommand::Subscribe {
            request_id,
            topic,
            mut topics,
            from_seq,
            tail,
            node,
            limit,
            max_batch_bytes,
            include_data,
            include_tags,
            include_meta,
            lenient,
        } => {
            if !principal.allows_scope(Scope::READ) {
                send_error(out_tx, request_id, "forbidden", "api key lacks read scope").await;
                return;
            }
            if let Some(topic) = topic {
                topics
                    .entry(topic)
                    .or_insert(WatchTopicOptions { from_seq, tail });
            }
            if topics.is_empty() {
                send_error(
                    out_tx,
                    request_id,
                    "invalid_request",
                    "subscribe requires topic or topics",
                )
                .await;
                return;
            }
            for name in topics.keys() {
                if !principal.allows_name(name) {
                    send_error(
                        out_tx,
                        request_id.clone(),
                        "forbidden",
                        "api key is not allowed to access this topic name",
                    )
                    .await;
                    return;
                }
            }

            let states = match state.engine.watch_topic_states(&topics, lenient) {
                Ok(states) => states,
                Err(e) => {
                    send_error(out_tx, request_id, e.code.code(), &e.message).await;
                    return;
                }
            };
            {
                let mut guard = subs.lock();
                for (name, st) in &states {
                    guard.insert(
                        name.clone(),
                        WsSub {
                            cursor: st.from_seq,
                            node: node.clone(),
                            limit,
                            max_batch_bytes,
                            include_data,
                            include_tags,
                            include_meta,
                            caught_up: false,
                        },
                    );
                }
            }
            changed.notify_one();
            send_json(
                out_tx,
                serde_json::json!({
                    "op": "subscribed",
                    "request_id": request_id,
                    "topics": states
                }),
            )
            .await;
        }
        WsCommand::Unsubscribe {
            request_id,
            topic,
            mut topics,
        } => {
            if let Some(topic) = topic {
                topics.push(topic);
            }
            if topics.is_empty() {
                send_error(
                    out_tx,
                    request_id,
                    "invalid_request",
                    "unsubscribe requires topic or topics",
                )
                .await;
                return;
            }
            {
                let mut guard = subs.lock();
                for topic in &topics {
                    guard.remove(topic);
                }
            }
            changed.notify_one();
            send_json(
                out_tx,
                serde_json::json!({
                    "op": "unsubscribed",
                    "request_id": request_id,
                    "topics": topics
                }),
            )
            .await;
        }
        WsCommand::Publish {
            request_id,
            topic,
            records,
            node,
            idempotency_key,
            create,
            config,
            disable_backpressure,
            return_seqs,
        } => {
            if !principal.allows_scope(Scope::WRITE) {
                send_error(out_tx, request_id, "forbidden", "api key lacks write scope").await;
                return;
            }
            if config.is_some() && !principal.allows_scope(Scope::ADMIN) {
                send_error(
                    out_tx,
                    request_id,
                    "forbidden",
                    "configuring a new topic on publish requires admin scope",
                )
                .await;
                return;
            }
            if !principal.allows_name(&topic) {
                send_error(
                    out_tx,
                    request_id,
                    "forbidden",
                    "api key is not allowed to access this topic name",
                )
                .await;
                return;
            }

            let req = WriteRequest {
                records,
                node,
                idempotency_key,
                create,
                config,
                disable_backpressure,
            };
            let inline_ephemeral = state
                .engine
                .get_topic(&topic)
                .map(|topic| !topic.config.read().uses_persistent_record_store())
                .unwrap_or(false);
            let result = if inline_ephemeral {
                state.engine.write(&topic, req, return_seqs)
            } else {
                let engine = state.engine.clone();
                let topic_for_write = topic.clone();
                run_blocking(move || engine.write(&topic_for_write, req, return_seqs)).await
            };
            match result {
                Ok(resp) => {
                    send_json(
                        out_tx,
                        serde_json::json!({
                            "op": "ack",
                            "request_id": request_id,
                            "topic": topic,
                            "first_seq": resp.first_seq,
                            "last_seq": resp.last_seq,
                            "seqs": resp.seqs,
                            "head_seq": resp.head_seq,
                            "count": resp.count,
                            "created": resp.created,
                            "deduped": resp.deduped,
                            "performance": resp.performance
                        }),
                    )
                    .await;
                }
                Err(e) => {
                    send_error(out_tx, request_id, e.code.code(), &e.message).await;
                }
            }
        }
        WsCommand::Ping { request_id } => {
            send_json(
                out_tx,
                serde_json::json!({ "op": "pong", "request_id": request_id }),
            )
            .await;
        }
    }
}

async fn delivery_loop(
    state: AppState,
    subs: SharedSubs,
    changed: Arc<Notify>,
    out_tx: mpsc::Sender<Message>,
) {
    loop {
        // Register the dynamic-subscription waiter before taking the snapshot.
        // `Notify` wakeups are not sticky, so creating this future after the
        // snapshot/drain pass can miss a subscribe/unsubscribe that happens in
        // between and leave the delivery loop parked until the fallback sleep.
        let changed_notified = changed.notified();
        tokio::pin!(changed_notified);

        if state.shutdown.is_shutting_down() {
            send_json(
                &out_tx,
                serde_json::json!({
                    "op": "error",
                    "code": "server_shutting_down",
                    "message": "server is shutting down; reconnect"
                }),
            )
            .await;
            return;
        }

        let snapshot: Vec<(String, WsSub)> = subs
            .lock()
            .iter()
            .map(|(name, sub)| (name.clone(), sub.clone()))
            .collect();

        let mut live = Vec::new();
        for (name, sub) in snapshot {
            let Some(topic_state) = state.engine.get_topic(&name) else {
                subs.lock().remove(&name);
                send_json(
                    &out_tx,
                    serde_json::json!({
                        "op": "topic_deleted",
                        "topic": name,
                        "head_seq": 0,
                        "reason": "deleted"
                    }),
                )
                .await;
                continue;
            };
            live.push((name, sub, topic_state));
        }

        // Register topic notifications BEFORE draining. TopicState uses
        // notify_waiters(), so wakeups are not sticky; creating these futures after
        // a caught-up diff leaves a missed-wakeup window for final tail records.
        let notifies: Vec<_> = live
            .iter()
            .map(|(_, _, b)| Box::pin(b.notify.notified()))
            .collect();

        for (name, sub, _) in &live {
            let variant = FrameVariant::new(sub.include_data, sub.include_tags, sub.include_meta);
            loop {
                let current = match subs.lock().get(name.as_str()).cloned() {
                    Some(current) => current,
                    None => break,
                };
                let from_seq = current.cursor;
                let req = DiffRequest {
                    from_seq,
                    limit: current.limit,
                    node: current.node.clone(),
                    include_tags: current.include_tags,
                    include_meta: current.include_meta,
                    wait_ms: 0,
                    max_batch_bytes: current.max_batch_bytes,
                };
                let Ok(diff) = state.engine.diff_shared_frames(name.as_str(), req, variant) else {
                    subs.lock().remove(name.as_str());
                    send_json(
                        &out_tx,
                        serde_json::json!({
                            "op": "topic_deleted",
                            "topic": name,
                            "head_seq": 0,
                            "reason": "deleted"
                        }),
                    )
                    .await;
                    break;
                };

                if let Some(tomb) = &diff.tombstone {
                    if let Some(stored) = subs.lock().get_mut(name.as_str()) {
                        stored.cursor = tomb.gap_to;
                        stored.caught_up = false;
                    }
                    send_json(
                        &out_tx,
                        serde_json::json!({
                            "op": "tombstone",
                            "topic": name,
                            "reason": tomb.reason,
                            "gap_from": tomb.gap_from,
                            "gap_to": tomb.gap_to,
                            "earliest_seq": tomb.earliest_seq,
                            "head_seq": tomb.head_seq
                        }),
                    )
                    .await;
                }

                if !diff.records.is_empty() {
                    if let Some(stored) = subs.lock().get_mut(name.as_str()) {
                        stored.cursor = diff.next_from_seq;
                        stored.caught_up = false;
                    }
                    let payload = WsRecordEnvelope {
                        op: "record",
                        topic: name,
                        from_seq,
                        to_seq: diff.next_from_seq,
                        head_seq: diff.head_seq,
                        records: diff.records,
                    };
                    send_text_with(&out_tx, || {
                        serde_json::to_string(&payload).unwrap_or_else(|_| "{}".into())
                    })
                    .await;
                } else if let Some(stored) = subs.lock().get_mut(name.as_str()) {
                    stored.cursor = diff.next_from_seq;
                }

                if diff.caught_up {
                    let should_emit = {
                        let mut guard = subs.lock();
                        match guard.get_mut(name.as_str()) {
                            Some(stored) if !stored.caught_up => {
                                stored.caught_up = true;
                                true
                            }
                            _ => false,
                        }
                    };
                    if should_emit {
                        send_json(
                            &out_tx,
                            serde_json::json!({
                                "op": "caught_up",
                                "topic": name,
                                "head_seq": diff.head_seq
                            }),
                        )
                        .await;
                    }
                    break;
                }
            }
        }

        if notifies.is_empty() {
            tokio::select! {
                _ = state.shutdown.notified() => {}
                _ = &mut changed_notified => {}
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
            }
        } else {
            let wake = futures::future::select_all(notifies);
            tokio::select! {
                _ = state.shutdown.notified() => {}
                _ = &mut changed_notified => {}
                _ = wake => {}
                _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
            }
        }
    }
}

async fn send_error(
    out_tx: &mpsc::Sender<Message>,
    request_id: Option<Value>,
    code: &str,
    message: &str,
) {
    send_json(
        out_tx,
        serde_json::json!({
            "op": "error",
            "request_id": request_id,
            "code": code,
            "message": message
        }),
    )
    .await;
}

async fn send_json(out_tx: &mpsc::Sender<Message>, value: Value) {
    send_text_with(out_tx, || value.to_string()).await;
}

async fn send_text_with<F>(out_tx: &mpsc::Sender<Message>, build: F)
where
    F: FnOnce() -> String,
{
    let Ok(permit) = out_tx.reserve().await else {
        return;
    };
    permit.send(Message::Text(build().into()));
}
