//! Health / readiness / metrics endpoints (API §8). These do not require auth
//! by default; the auth middleware skips them unless `STREAMS_PROBE_AUTH`.

use super::AppState;
use crate::types::{HealthResponse, ReadyResponse};
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

/// `GET /v0/ready` (alias `/readyz`) — readiness. Phase 2 is always ready
/// (no WAL replay); `503 shutting_down` is emitted during drain in main.
pub async fn ready(State(state): State<AppState>) -> Json<ReadyResponse> {
    Json(ReadyResponse {
        status: "ready".to_string(),
        wal_replay_complete: true,
        boxes: state.engine.box_count(),
    })
}

/// `GET /v0/metrics` — Prometheus text exposition by default; JSON snapshot
/// when `Accept: application/json`. Always `200`.
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
