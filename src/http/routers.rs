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
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    let req: RouterCreateRequest = parse_json_body(&headers, &body)?;
    let engine = state.engine.clone();
    let (created, resp) = run_blocking(move || engine.put_router(&router, req)).await?;
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(resp)).into_response())
}

/// `GET /v0/routers/:router`.
pub async fn get_router(
    State(state): State<AppState>,
    Path(router): Path<String>,
) -> Result<Json<RouterGetResponse>> {
    Ok(Json(state.engine.get_router(&router)?))
}

/// `GET /v0/routers` — list routers, filtered by `prefix`/`source`/`dest`.
pub async fn list_routers(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<RouterListResponse>> {
    let prefix = params.get("prefix").map(String::as_str);
    let source = params.get("source").map(String::as_str);
    let dest = params.get("dest").map(String::as_str);
    let page_size = params
        .get("page_size")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(config::DEFAULT_PAGE_SIZE);
    let cursor = params.get("cursor").map(String::as_str);
    Ok(Json(state.engine.list_routers(
        prefix, source, dest, page_size, cursor,
    )?))
}

/// `DELETE /v0/routers/:router` — idempotent.
pub async fn delete_router(
    State(state): State<AppState>,
    Path(router): Path<String>,
) -> Result<Json<RouterDeleteResponse>> {
    let engine = state.engine.clone();
    let resp = run_blocking(move || engine.delete_router(&router)).await?;
    Ok(Json(resp))
}
