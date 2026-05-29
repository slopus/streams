//! HTTP layer: the axum `Router` for `/v0`, bearer-auth middleware,
//! content-type / body-size guards, the `Error` → HTTP envelope mapping, and
//! the per-response `performance` block plumbing.

pub mod boxes;
pub mod delete;
pub mod health;
pub mod queue;
pub mod routers;
pub mod watch;

use crate::engine::Engine;
use crate::error::Error;
use crate::types::ErrorCode;
use axum::{
    extract::{DefaultBodyLimit, Request},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::{get, post, put},
    Router,
};
use queue::ClaimCoordinator;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::Arc;
use watch::SessionStore;

/// Shared state handed to every handler.
#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<Engine>,
    /// In-memory watch-session registry (API §7.1).
    pub sessions: Arc<SessionStore>,
    /// Per-box coalescing-window claim coordinator + `/work` conn ids (API §10).
    pub coordinator: Arc<ClaimCoordinator>,
}

/// Build the full `/v0` axum router with middleware applied.
pub fn build_router(engine: Arc<Engine>) -> Router {
    let max_body = engine.config.max_body_bytes;
    let state = AppState {
        engine,
        sessions: Arc::new(SessionStore::new()),
        coordinator: Arc::new(ClaimCoordinator::new()),
    };

    let v0 = Router::new()
        // Boxes
        .route("/boxes", get(boxes::list_boxes))
        .route(
            "/boxes/{box}",
            put(boxes::put_box)
                .get(boxes::get_box)
                .delete(boxes::delete_box)
                .post(boxes::write),
        )
        .route("/boxes/{box}/diff", post(boxes::diff))
        .route("/boxes/{box}/delete", post(delete::delete))
        // Queue lifecycle (API §10)
        .route("/boxes/{box}/claim", post(queue::claim))
        .route("/boxes/{box}/ack", post(queue::ack))
        .route("/boxes/{box}/nack", post(queue::nack))
        .route("/boxes/{box}/extend", post(queue::extend))
        .route("/boxes/{box}/work", get(queue::work))
        // Routers
        .route("/routers", get(routers::list_routers))
        .route(
            "/routers/{router}",
            put(routers::put_router)
                .get(routers::get_router)
                .delete(routers::delete_router),
        )
        // Watch / SSE
        .route("/watch", post(watch::create_watch))
        .route("/watch/{wid}", get(watch::stream_watch))
        // Health / readiness / metrics
        .route("/health", get(health::health))
        .route("/ready", get(health::ready))
        .route("/metrics", get(health::metrics));

    Router::new()
        .nest("/v0", v0)
        // Root-level probe aliases for load balancers (API §8).
        .route("/healthz", get(health::health))
        .route("/readyz", get(health::ready))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ))
        // Hard body-size guard (`413 payload_too_large`) applied before parse.
        .layer(DefaultBodyLimit::max(max_body))
        // Rewrite bare 413/404/405/415 onto the canonical error envelope.
        .layer(middleware::from_fn(error_envelope_middleware))
        .with_state(state)
}

/// Bearer-auth middleware. Disabled when no keys are configured (dev mode).
/// Probe endpoints skip auth unless `STREAMS_PROBE_AUTH` is set.
async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let cfg = &state.engine.config;
    if !cfg.auth_enabled() {
        return next.run(req).await;
    }

    let path = req.uri().path();
    let is_probe = is_probe_path(path);
    if is_probe && !cfg.probe_auth {
        return next.run(req).await;
    }

    // Bearer token from header, or `?token=` for EventSource GET.
    let provided = extract_bearer(req.headers())
        .map(str::to_string)
        .or_else(|| query_token(req.uri().query()));

    match provided {
        Some(token) if cfg.api_keys.iter().any(|k| k == &token) => next.run(req).await,
        _ => Error::new(ErrorCode::Unauthorized, "missing or invalid bearer token")
            .into_response(),
    }
}

fn is_probe_path(path: &str) -> bool {
    matches!(
        path,
        "/healthz" | "/readyz" | "/v0/health" | "/v0/ready" | "/v0/metrics"
    )
}

fn extract_bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

fn query_token(query: Option<&str>) -> Option<String> {
    let q = query?;
    for pair in q.split('&') {
        if let Some(v) = pair.strip_prefix("token=") {
            return Some(v.to_string());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Error -> HTTP response (API §0.5)
// ---------------------------------------------------------------------------

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.http_status())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut resp = (status, Json(self.envelope())).into_response();
        if let Some(secs) = self.retry_after_s {
            if let Ok(v) = HeaderValue::from_str(&secs.to_string()) {
                resp.headers_mut().insert(header::RETRY_AFTER, v);
            }
        }
        resp
    }
}

/// Validate the `Content-Type` of a request body as JSON (API §0.3); used by
/// handlers with bodies to return `415 unsupported_media_type`.
pub fn require_json_content_type(headers: &HeaderMap) -> Result<(), Error> {
    match headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok()) {
        Some(ct) if ct.trim_start().starts_with("application/json") => Ok(()),
        _ => Err(Error::new(
            ErrorCode::UnsupportedMediaType,
            "Content-Type must be application/json",
        )),
    }
}

/// Guard the `Content-Type` and deserialize a JSON request body into `T`.
///
/// Returns `415 unsupported_media_type` for a non-JSON content type and
/// `400 invalid_request` for a malformed/ill-typed body. Handlers extract the
/// raw [`Bytes`](axum::body::Bytes) so the content-type check happens *before*
/// parse and so an empty body can be special-cased by the caller.
pub fn parse_json_body<T: DeserializeOwned>(headers: &HeaderMap, body: &[u8]) -> Result<T, Error> {
    require_json_content_type(headers)?;
    serde_json::from_slice(body)
        .map_err(|e| Error::invalid_request(format!("malformed JSON body: {e}")))
}

/// Run a synchronous, possibly-blocking engine call (a mutating op that may wait
/// on a WAL group fsync) on tokio's blocking pool, so the fsync wait never parks
/// a reactor thread (ARCHITECTURE §8.5). Maps a join failure (only on panic) to a
/// `500`.
pub async fn run_blocking<T, F>(f: F) -> Result<T, Error>
where
    F: FnOnce() -> Result<T, Error> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(res) => res,
        Err(e) => Err(Error::internal(format!("engine task failed: {e}"))),
    }
}

/// Parse a boolean query parameter (`true`/`false`/`1`/`0`), falling back to
/// `default` when absent or unparseable.
pub fn query_bool(params: &HashMap<String, String>, key: &str, default: bool) -> bool {
    match params.get(key).map(String::as_str) {
        Some("true") | Some("1") => true,
        Some("false") | Some("0") => false,
        _ => default,
    }
}

/// Map axum's bare body-limit / fallback responses onto the canonical error
/// envelope. The `DefaultBodyLimit` layer rejects oversized bodies with a bare
/// `413` (no body); rewrite it to `413 payload_too_large` (API §0.6). A bare
/// `404`/`405` from routing likewise gets the envelope.
async fn error_envelope_middleware(req: Request, next: Next) -> Response {
    let resp = next.run(req).await;
    let status = resp.status();
    // Only rewrite responses that don't already carry a JSON body of our own.
    let is_ours = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|c| c.starts_with("application/json"))
        .unwrap_or(false);
    if is_ours {
        return resp;
    }
    let code = match status {
        StatusCode::PAYLOAD_TOO_LARGE => Some(ErrorCode::PayloadTooLarge),
        StatusCode::NOT_FOUND => Some(ErrorCode::NotFound),
        StatusCode::METHOD_NOT_ALLOWED => Some(ErrorCode::MethodNotAllowed),
        StatusCode::UNSUPPORTED_MEDIA_TYPE => Some(ErrorCode::UnsupportedMediaType),
        _ => None,
    };
    match code {
        Some(c) => {
            let msg = match c {
                ErrorCode::PayloadTooLarge => "request body exceeds the server limit",
                ErrorCode::NotFound => "resource not found",
                ErrorCode::MethodNotAllowed => "method not allowed for this path",
                ErrorCode::UnsupportedMediaType => "Content-Type must be application/json",
                _ => "error",
            };
            Error::new(c, msg).into_response()
        }
        None => resp,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_bool_parses_common_forms() {
        let mut p = HashMap::new();
        assert!(query_bool(&p, "touch", true));
        assert!(!query_bool(&p, "touch", false));
        p.insert("touch".to_string(), "false".to_string());
        assert!(!query_bool(&p, "touch", true));
        p.insert("touch".to_string(), "1".to_string());
        assert!(query_bool(&p, "touch", false));
    }

    #[test]
    fn parse_json_body_requires_content_type() {
        let headers = HeaderMap::new();
        let r: Result<crate::types::DiffRequest, _> = parse_json_body(&headers, b"{}");
        assert_eq!(r.unwrap_err().code, ErrorCode::UnsupportedMediaType);
    }

    #[test]
    fn parse_json_body_rejects_bad_json() {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
        let r: Result<crate::types::DiffRequest, _> = parse_json_body(&headers, b"{bad");
        assert_eq!(r.unwrap_err().code, ErrorCode::InvalidRequest);
    }
}
