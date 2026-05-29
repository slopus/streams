//! Tag-delete filter endpoints: POST/GET `/v0/boxes/:box/delete`.

use super::{parse_json_body, AppState};
use crate::error::Result;
use crate::types::*;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
    response::Json,
};

/// `POST /v0/boxes/:box/delete` — add read-time tag-delete filters. Accepts the
/// canonical tuple form and the bare-string shorthand (both parse to [`Filter`]).
pub async fn add_filters(
    State(state): State<AppState>,
    Path(box_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<DeleteFiltersResponse>> {
    let req: DeleteFiltersRequest = parse_json_body(&headers, &body)?;
    Ok(Json(state.engine.add_filters(&box_name, req.filters)?))
}

/// `GET /v0/boxes/:box/delete` — list active filters.
pub async fn list_filters(
    State(state): State<AppState>,
    Path(box_name): Path<String>,
) -> Result<Json<ListFiltersResponse>> {
    Ok(Json(state.engine.list_filters(&box_name)?))
}
