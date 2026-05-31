//! Health / readiness / metrics endpoints (API §8). These do not require auth
//! by default; the auth middleware skips them unless `STREAMS_PROBE_AUTH`.

use super::AppState;
use crate::error::Error;
use crate::types::{ErrorCode, HealthResponse, ReadyResponse};
use axum::{
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Json, Response},
};

/// Crate version, surfaced in `/v0/health`.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// `GET /v0/health` (alias `/healthz`) — liveness. Always `200`.
pub async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    let uptime_ms = state.engine.started_at.elapsed().as_millis() as i64;
    Json(HealthResponse {
        status: "ok".to_string(),
        version: VERSION.to_string(),
        uptime_ms,
    })
}

/// `GET /v0/ready` (alias `/readyz`) — readiness (API §8.2). `200 ready` once
/// restart recovery (snapshot load + WAL replay) has rebuilt the in-memory
/// state; `503 not_ready` while replay is in progress, carrying `Retry-After`
/// and `error.detail.replay_progress` (0.0–1.0). `/v0/health` stays `200`
/// throughout (liveness is independent of the ready gate).
pub async fn ready(State(state): State<AppState>) -> Response {
    if state.engine.is_ready() {
        return Json(ReadyResponse {
            status: "ready".to_string(),
            wal_replay_complete: true,
            boxes: state.engine.box_count(),
        })
        .into_response();
    }
    // Still replaying the WAL: `503 not_ready` with the canonical error envelope,
    // a `Retry-After`, and the replay progress so a probe/LB can back off.
    Error::new(ErrorCode::NotReady, "WAL replay in progress")
        .with_detail(serde_json::json!({
            "replay_progress": state.engine.replay_progress(),
        }))
        .with_retry_after(1)
        .into_response()
}

/// `GET /v0/metrics` — Prometheus text exposition by default; JSON snapshot
/// when `Accept: application/json`. Always `200`. Requires authentication (a
/// read-scoped key) when auth is enabled — it exposes operational state (box
/// count), so it is not in the unauthenticated liveness/readiness probe set
/// (codex LOW #12).
pub async fn metrics(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let wants_json = headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|a| a.contains("application/json"))
        .unwrap_or(false);

    if wants_json {
        let snapshot = serde_json::json!({
            "boxes": state.engine.box_count(),
        });
        (StatusCode::OK, Json(snapshot)).into_response()
    } else {
        let body = render_prometheus(&state);
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            body,
        )
            .into_response()
    }
}

/// Render the Prometheus text exposition body.
fn render_prometheus(state: &AppState) -> String {
    // Phase 2: minimal. Per-box counters are filled in once the engine tracks
    // appends/reads/evictions/tombstones.
    let mut out = String::new();
    out.push_str("# HELP streams_boxes Number of boxes.\n");
    out.push_str("# TYPE streams_boxes gauge\n");
    out.push_str(&format!("streams_boxes {}\n", state.engine.box_count()));
    out
}
