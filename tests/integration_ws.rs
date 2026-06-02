//! WebSocket integration tests over the real bound server.

mod common;

use std::time::Duration;

use common::Harness;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use topics::config::ServerConfig;

fn ws_url(h: &Harness) -> String {
    h.base_url().replacen("http://", "ws://", 1) + "/v0/ws"
}

async fn next_json<S>(ws: &mut S) -> Value
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("websocket message timeout")
        .expect("websocket ended")
        .expect("websocket message");
    match msg {
        Message::Text(text) => serde_json::from_str(&text).expect("json text message"),
        other => panic!("unexpected websocket message: {other:?}"),
    }
}

async fn read_until_op<S>(ws: &mut S, op: &str) -> Value
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let msg = tokio::time::timeout(remaining, next_json(ws))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for op {op:?}"));
        if msg.get("op").and_then(|v| v.as_str()) == Some(op) {
            return msg;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_subscribe_publish_and_unsubscribe() {
    let topic = "voice-room:test";
    let h = tokio::task::spawn_blocking(Harness::start)
        .await
        .expect("start harness");
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url(&h))
        .await
        .expect("connect websocket");

    ws.send(Message::Text(
        json!({
            "op": "publish",
            "request_id": "create-1",
            "topic": topic,
            "return_seqs": false,
            "config": {
                "durability": "ephemeral",
                "ttl_ms": 10_000,
                "cap_records": 100
            },
            "records": [{
                "node": "speaker",
                "data": { "kind": "seed", "packet": 0 }
            }]
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("send create publish");
    let create_ack = read_until_op(&mut ws, "ack").await;
    assert_eq!(create_ack["request_id"], "create-1");

    ws.send(Message::Text(
        json!({
            "op": "subscribe",
            "request_id": "sub-1",
            "topic": topic,
            "tail": true,
            "node": "listener"
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("send subscribe");
    let subscribed = read_until_op(&mut ws, "subscribed").await;
    assert_eq!(subscribed["request_id"], "sub-1");

    ws.send(Message::Text(
        json!({
            "op": "publish",
            "request_id": "pub-1",
            "topic": topic,
            "return_seqs": false,
            "records": [{
                "node": "speaker",
                "data": { "kind": "audio", "packet": 1 }
            }]
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("send publish");

    let ack = read_until_op(&mut ws, "ack").await;
    assert_eq!(ack["request_id"], "pub-1");
    assert_eq!(ack["last_seq"], 2);

    let record = read_until_op(&mut ws, "record").await;
    assert_eq!(record["topic"], topic);
    assert_eq!(record["records"][0]["data"]["packet"], 1);

    ws.send(Message::Text(
        json!({
            "op": "unsubscribe",
            "request_id": "unsub-1",
            "topic": topic
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("send unsubscribe");
    let unsubscribed = read_until_op(&mut ws, "unsubscribed").await;
    assert_eq!(unsubscribed["request_id"], "unsub-1");

    ws.send(Message::Text(
        json!({
            "op": "publish",
            "request_id": "pub-2",
            "topic": topic,
            "return_seqs": false,
            "records": [{
                "node": "speaker",
                "data": { "kind": "audio", "packet": 2 }
            }]
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("send publish after unsubscribe");
    let ack2 = read_until_op(&mut ws, "ack").await;
    assert_eq!(ack2["request_id"], "pub-2");

    let no_record = tokio::time::timeout(Duration::from_millis(200), ws.next()).await;
    assert!(
        no_record.is_err(),
        "unexpected websocket frame after unsubscribe"
    );

    tokio::task::spawn_blocking(move || drop(h))
        .await
        .expect("drop harness");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_enforces_command_scopes_after_authenticated_upgrade() {
    let topic = "voice-room:secure";
    let h = tokio::task::spawn_blocking(|| {
        Harness::start_with(ServerConfig {
            api_keys: vec!["seed:admin+write+read".to_string(), "ro:read".to_string()],
            ..ServerConfig::default()
        })
    })
    .await
    .expect("start harness");

    let mut seed_req = ws_url(&h)
        .into_client_request()
        .expect("build seed request");
    seed_req
        .headers_mut()
        .insert("Authorization", "Bearer seed".parse().unwrap());
    let (mut seed_ws, _) = tokio_tungstenite::connect_async(seed_req)
        .await
        .expect("connect seed websocket");
    seed_ws
        .send(Message::Text(
            json!({
                "op": "publish",
                "request_id": "seed-1",
                "topic": topic,
                "return_seqs": false,
                "config": { "durability": "ephemeral", "cap_records": 10 },
                "records": [{ "node": "seed", "data": { "packet": 0 } }]
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send seed publish");
    let seed_ack = read_until_op(&mut seed_ws, "ack").await;
    assert_eq!(seed_ack["request_id"], "seed-1");

    let mut ro_req = ws_url(&h)
        .into_client_request()
        .expect("build read-only request");
    ro_req
        .headers_mut()
        .insert("Authorization", "Bearer ro".parse().unwrap());
    let (mut ro_ws, _) = tokio_tungstenite::connect_async(ro_req)
        .await
        .expect("connect read-only websocket");
    ro_ws
        .send(Message::Text(
            json!({
                "op": "subscribe",
                "request_id": "sub-ro",
                "topic": topic,
                "from_seq": 0
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send read-only subscribe");
    let subscribed = read_until_op(&mut ro_ws, "subscribed").await;
    assert_eq!(subscribed["request_id"], "sub-ro");

    ro_ws
        .send(Message::Text(
            json!({
                "op": "publish",
                "request_id": "pub-ro",
                "topic": topic,
                "return_seqs": false,
                "records": [{ "node": "ro", "data": { "packet": 1 } }]
            })
            .to_string()
            .into(),
        ))
        .await
        .expect("send forbidden publish");
    let forbidden = read_until_op(&mut ro_ws, "error").await;
    assert_eq!(forbidden["request_id"], "pub-ro");
    assert_eq!(forbidden["code"], "forbidden");

    tokio::task::spawn_blocking(move || drop(h))
        .await
        .expect("drop harness");
}
