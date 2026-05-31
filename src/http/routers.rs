//! Router endpoints: PUT/GET/DELETE `/v0/routers/:router`, GET `/v0/routers`.

use super::{parse_json_body, run_blocking, AppState};
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

/// `PUT /v0/routers/:router` — create/configure a router (idempotent upsert).
/// `201` when newly created, `200` otherwise.
pub async fn put_router(
    State(state): State<AppState>,
    Path(router): Path<String>,
    extensions: axum::http::Extensions,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    let req: RouterCreateRequest = parse_json_body(&headers, &body)?;

    // Authorization: a prefix-restricted key may only create a router whose
    // source AND dest are within its allowlist (the router-path name was already
    // checked at the route level, but the body's source/dest are not — and the
    // engine auto-creates them; codex HIGH #2). Without this a scoped admin key
    // could route forbidden data into an allowed box, or auto-create boxes
    // outside its allowlist. A full-access / unrestricted key passes transparently.
    if let Some(p) = extensions.get::<crate::auth::Principal>() {
        for name in [req.source.as_str(), req.dest.as_str()] {
            if !p.allows_name(name) {
                return Err(crate::error::Error::new(
                    crate::types::ErrorCode::Forbidden,
                    "api key is not allowed to route to/from this box",
                )
                .with_detail(serde_json::json!({ "box": name })));
            }
        }
    }

    let engine = state.engine.clone();
    let (created, resp) = run_blocking(move || engine.put_router(&router, req)).await?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(resp)).into_response())
}

/// Authorize a prefix-restricted principal against a router's NAME, SOURCE, and
/// DEST (codex HIGH/P1 #9): the route-level check only saw the router path name, so
/// a prefix-limited key could read/delete a router whose name is allowed but whose
/// source/dest boxes are not. A full-access / unrestricted key (or dev mode) passes
/// transparently. A no-op when the router does not exist (the handler then returns
/// the usual not-found / idempotent response).
fn authorize_router_endpoints(
    state: &AppState,
    extensions: &axum::http::Extensions,
    router: &str,
) -> Result<()> {
    let Some(p) = extensions.get::<crate::auth::Principal>() else {
        return Ok(()); // dev mode / full access.
    };
    if p.prefixes.is_empty() {
        return Ok(()); // unrestricted key.
    }
    let Some((source, dest)) = state.engine.router_endpoints(router) else {
        return Ok(()); // absent: defer to the handler's not-found path.
    };
    for name in [router, source.as_str(), dest.as_str()] {
        if !p.allows_name(name) {
            return Err(crate::error::Error::new(
                crate::types::ErrorCode::Forbidden,
                "api key is not allowed to access this router's box(es)",
            )
            .with_detail(serde_json::json!({ "box": name })));
        }
    }
    Ok(())
}

/// `GET /v0/routers/:router`.
pub async fn get_router(
    State(state): State<AppState>,
    Path(router): Path<String>,
    extensions: axum::http::Extensions,
) -> Result<Json<RouterGetResponse>> {
    authorize_router_endpoints(&state, &extensions, &router)?;
    Ok(Json(state.engine.get_router(&router)?))
}

/// `GET /v0/routers` — list routers, filtered by `prefix`/`source`/`dest`.
pub async fn list_routers(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    extensions: axum::http::Extensions,
) -> Result<Json<RouterListResponse>> {
    let prefix = params.get("prefix").map(String::as_str);
    let source = params.get("source").map(String::as_str);
    let dest = params.get("dest").map(String::as_str);
    let page_size = params
        .get("page_size")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(config::DEFAULT_PAGE_SIZE);
    let cursor = params.get("cursor").map(String::as_str);
    // Filter to the caller key's box-name allowlist (empty ⇒ no restriction) so a
    // prefix-limited key cannot enumerate cross-tenant routers (codex MEDIUM #7).
    let allow = super::boxes::principal_prefixes(&extensions);
    Ok(Json(state.engine.list_routers(
        prefix, source, dest, page_size, cursor, &allow,
    )?))
}

/// `DELETE /v0/routers/:router` — idempotent.
pub async fn delete_router(
    State(state): State<AppState>,
    Path(router): Path<String>,
    extensions: axum::http::Extensions,
) -> Result<Json<RouterDeleteResponse>> {
    authorize_router_endpoints(&state, &extensions, &router)?;
    let engine = state.engine.clone();
    let resp = run_blocking(move || engine.delete_router(&router)).await?;
    Ok(Json(resp))
}
