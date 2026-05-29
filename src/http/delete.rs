//! Deletion endpoint: POST `/v0/boxes/:box/delete` (API §5).
//!
//! Permanent, point-in-time, silent deletion by seq range (`before_seq`)
//! and/or tag `match`. There is no persistent filter and no list endpoint.

use super::{parse_json_body, AppState};
use crate::error::Result;
use crate::types::*;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
    response::Json,
};

/// `POST /v0/boxes/:box/delete` — permanently delete records by `before_seq`
/// and/or tag `match`. At least one selector is required (else `400`).
pub async fn delete(
    State(state): State<AppState>,
    Path(box_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<DeleteResponse>> {
    let req: DeleteRequest = parse_json_body(&headers, &body)?;
    Ok(Json(state.engine.delete(&box_name, req)?))
}
