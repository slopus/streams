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

/// The authenticated principal (the matched api key) for a request, stashed in
/// request extensions by [`auth_middleware`] so a handler (e.g. `POST /v0/watch`)
/// can bind a created resource to its creator. `None`/absent in dev mode.
#[derive(Clone)]
pub struct Principal(pub String);

/// Bearer-auth middleware. Disabled when no keys are configured (dev mode).
/// Probe endpoints skip auth unless `STREAMS_PROBE_AUTH` is set.
///
/// `GET /v0/watch/:wid` is special: the `wid` is an unguessable bearer capability
/// (minted by the authenticated `POST /v0/watch`), so the stream GET is authorized
/// by *possessing* the wid and is NOT gated here — the handler enforces the
/// per-session principal binding. This lets browser `EventSource` (GET-only, no
/// custom headers) open the stream with just the secret URL, without putting a
/// long-lived api key in a logged query string.
async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<AppState>,
    mut req: Request,
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

    // Capability-authorized: the SSE stream GET is gated by the wid, not by a
    // bearer in the URL. Defer to the handler (which checks the wid binding).
    if req.method() == axum::http::Method::GET && is_watch_stream_path(path) {
        return next.run(req).await;
    }

    // Bearer token from header, or `?token=` for EventSource GET (dev-only
    // fallback; the header is preferred since a query string leaks via logs).
    let provided = extract_bearer(req.headers())
        .map(str::to_string)
        .or_else(|| query_token(req.uri().query()));

    match provided {
        Some(token) if cfg.key_matches(&token) => {
            // Stash the authenticated principal so a handler can bind a created
            // resource (e.g. a watch session) to its creator.
            req.extensions_mut().insert(Principal(token));
            next.run(req).await
        }
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

/// True for the SSE stream path `GET /v0/watch/:wid` (exactly one path segment
/// after `/v0/watch/`). The session-creating `POST /v0/watch` is NOT matched (it
/// must be authenticated normally).
fn is_watch_stream_path(path: &str) -> bool {
    match path.strip_prefix("/v0/watch/") {
        Some(rest) => !rest.is_empty() && !rest.contains('/'),
        None => false,
    }
}

fn extract_bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
        .map(str::trim)
}

/// Extract the `?token=` query parameter using a real
/// `application/x-www-form-urlencoded` parser: it URL-decodes percent-escapes and
/// `+`, and on a duplicate `token=` it takes the FIRST occurrence (deterministic).
/// This is a documented dev-only fallback for browser `EventSource`; prefer the
/// `Authorization: Bearer` header since a query string leaks via logs/history/
/// proxies.
fn query_token(query: Option<&str>) -> Option<String> {
    let q = query?;
    form_urlencoded::parse(q.as_bytes())
        .find(|(k, _)| k == "token")
        .map(|(_, v)| v.into_owned())
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

    #[test]
    fn query_token_basic_and_decoded() {
        assert_eq!(query_token(None), None);
        assert_eq!(query_token(Some("token=abc")), Some("abc".to_string()));
        assert_eq!(query_token(Some("x=1&token=abc&y=2")), Some("abc".to_string()));
        // Percent-escapes and `+` are decoded (a real form parser, not `strip_prefix`).
        assert_eq!(
            query_token(Some("token=a%2Bb%3Dc")),
            Some("a+b=c".to_string())
        );
        assert_eq!(query_token(Some("token=a+b")), Some("a b".to_string()));
        // No token param ⇒ None (even if another key has a `token`-ish prefix).
        assert_eq!(query_token(Some("tokenx=abc")), None);
    }

    #[test]
    fn query_token_takes_first_duplicate() {
        // Deterministic: first occurrence wins on a duplicated param.
        assert_eq!(
            query_token(Some("token=first&token=second")),
            Some("first".to_string())
        );
    }

    #[test]
    fn watch_stream_path_matches_only_the_get_stream() {
        assert!(is_watch_stream_path("/v0/watch/wid_abc"));
        // The POST create path (no trailing segment) is NOT the stream path.
        assert!(!is_watch_stream_path("/v0/watch"));
        assert!(!is_watch_stream_path("/v0/watch/"));
        // Extra segments must not match (no path traversal past the wid).
        assert!(!is_watch_stream_path("/v0/watch/wid/extra"));
        assert!(!is_watch_stream_path("/v0/boxes/jobs"));
    }
}
