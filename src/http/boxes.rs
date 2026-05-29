//! Box endpoints: PUT/GET/DELETE/POST `/v0/boxes/:box`, GET `/v0/boxes`
//! (list), and POST `/v0/boxes/:box/diff`.

use super::{parse_json_body, query_bool, AppState};
use crate::config;
use crate::error::Result;
use crate::types::*;
use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
};
use std::collections::HashMap;

/// `PUT /v0/boxes/:box` — create/configure a box (idempotent upsert).
///
/// An empty body is treated as `{}` (all-default). `201` when this call brought
/// the box into existence, `200` otherwise.
pub async fn put_box(
    State(state): State<AppState>,
    Path(box_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    // Empty body ⇒ all-default config; non-empty must be JSON.
    let config: BoxConfig = if body.is_empty() {
        BoxConfig::default()
    } else {
        parse_json_body(&headers, &body)?
    };

    let (created, _cfg) = state.engine.put_box(&box_name, config)?;
    // Re-read the merged config so the response reflects the box's current state.
    let stored = state
        .engine
        .get_box(&box_name)
        .map(|b| b.config.read().clone())
        .unwrap_or_default();

    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let resp = BoxCreateResponse {
        box_name,
        created,
        config: stored,
        performance: Performance::default(),
    };
    Ok((status, Json(resp)).into_response())
}

/// `GET /v0/boxes/:box` — box state. `?touch=false` suppresses the auto-priority
/// recency bump (default `true`).
pub async fn get_box(
    State(state): State<AppState>,
    Path(box_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<BoxStateResponse>> {
    let touch = query_bool(&params, "touch", true);
    Ok(Json(state.engine.box_state(&box_name, touch)?))
}

/// `GET /v0/boxes` — list boxes. Listing does not bump auto-priority (default
/// `touch=false`).
pub async fn list_boxes(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<BoxListResponse>> {
    let prefix = params.get("prefix").map(String::as_str);
    let page_size = params
        .get("page_size")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(config::DEFAULT_PAGE_SIZE);
    let cursor = params.get("cursor").map(String::as_str);
    let touch = query_bool(&params, "touch", false);
    Ok(Json(
        state.engine.list_boxes(prefix, page_size, cursor, touch)?,
    ))
}

/// `DELETE /v0/boxes/:box` — delete box (cascades routers). `?if_empty=true`
/// refuses a non-empty box with `409 box_not_empty`.
pub async fn delete_box(
    State(state): State<AppState>,
    Path(box_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<BoxDeleteResponse>> {
    let if_empty = query_bool(&params, "if_empty", false);
    Ok(Json(state.engine.delete_box(&box_name, if_empty)?))
}

/// `POST /v0/boxes/:box` — append record(s). `?return_seqs=false` suppresses the
/// `seqs` array. The `Idempotency-Key` header is honored if the body omits it
/// (body field wins).
pub async fn write(
    State(state): State<AppState>,
    Path(box_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    let mut req: WriteRequest = parse_json_body(&headers, &body)?;

    // Header idempotency key as a fallback; the body field wins (API §0.8).
    if req.idempotency_key.is_none() {
        if let Some(v) = headers.get("idempotency-key").and_then(|v| v.to_str().ok()) {
            req.idempotency_key = Some(v.to_string());
        }
    }

    let return_seqs = query_bool(&params, "return_seqs", true);
    let resp = state.engine.write(&box_name, req, return_seqs)?;

    // `201` only when this write created the box (API §2).
    let status = if resp.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(resp)).into_response())
}

/// `POST /v0/boxes/:box/diff` — read difference from a cursor. An empty body is
/// treated as the all-default request (`from_seq=0`).
pub async fn diff(
    State(state): State<AppState>,
    Path(box_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<DiffResponse>> {
    let req: DiffRequest = if body.is_empty() {
        DiffRequest::default()
    } else {
        parse_json_body(&headers, &body)?
    };

    // `wait_ms` long-poll (API §3): if the call would be caught-up with no
    // records, park on the box's `Notify` up to the clamped wait, then re-read.
    let wait_ms = req.wait_ms.min(config::MAX_WAIT_MS);
    let first = state.engine.diff(&box_name, req.clone())?;
    if wait_ms == 0 || !first.records.is_empty() || first.tombstone.is_some() || !first.caught_up {
        return Ok(Json(first));
    }

    // Caught up with nothing to deliver: wait for an append or the deadline.
    let Some(b) = state.engine.get_box(&box_name) else {
        return Ok(Json(first));
    };
    let notified = b.notify.notified();
    tokio::select! {
        _ = notified => {}
        _ = tokio::time::sleep(std::time::Duration::from_millis(wait_ms as u64)) => {
            return Ok(Json(first));
        }
    }
    // Woken by an append: re-read once from the same cursor.
    Ok(Json(state.engine.diff(&box_name, req)?))
}
