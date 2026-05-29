//! The engine/HTTP error type and its mapping to the canonical error envelope.
//!
//! `Error` carries an [`ErrorCode`] (which determines the HTTP status and wire
//! code), a human-readable message, and optional structured detail. The
//! conversion to an HTTP response lives in [`crate::http`].

use crate::types::{ErrorBody, ErrorCode, ErrorEnvelope};
use serde_json::Value;

/// A domain/HTTP error. Maps to the `{"error":{code,message,detail?}}` body.
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct Error {
    pub code: ErrorCode,
    pub message: String,
    pub detail: Option<Value>,
    /// `Retry-After` seconds, set for `429`/`503` responses.
    pub retry_after_s: Option<u64>,
}

impl Error {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Error {
            code,
            message: message.into(),
            detail: None,
            retry_after_s: None,
        }
    }

    pub fn with_detail(mut self, detail: Value) -> Self {
        self.detail = Some(detail);
        self
    }

    pub fn with_retry_after(mut self, secs: u64) -> Self {
        self.retry_after_s = Some(secs);
        self
    }

    // Convenience constructors for the common codes.
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Error::new(ErrorCode::InvalidRequest, message)
    }

    pub fn box_not_found(box_name: &str) -> Self {
        Error::new(
            ErrorCode::BoxNotFound,
            format!("box {box_name:?} does not exist"),
        )
        .with_detail(serde_json::json!({ "box": box_name }))
    }

    pub fn not_a_queue(box_name: &str) -> Self {
        Error::new(
            ErrorCode::NotAQueue,
            format!("box {box_name:?} is not a queue"),
        )
        .with_detail(serde_json::json!({ "box": box_name }))
    }

    pub fn router_not_found(router: &str) -> Self {
        Error::new(
            ErrorCode::RouterNotFound,
            format!("router {router:?} does not exist"),
        )
        .with_detail(serde_json::json!({ "router": router }))
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Error::new(ErrorCode::Internal, message)
    }

    /// Build the wire envelope for this error.
    pub fn envelope(&self) -> ErrorEnvelope {
        ErrorEnvelope {
            error: ErrorBody {
                code: self.code.code(),
                message: self.message.clone(),
                detail: self.detail.clone(),
            },
        }
    }

    pub fn http_status(&self) -> u16 {
        self.code.status()
    }
}

pub type Result<T> = std::result::Result<T, Error>;
