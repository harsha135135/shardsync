//! The receiver's single error type. Every rejection carries a stable code from
//! [`ErrorCode`] and a human message; the HTTP layer adds the request ID.
//!
//! Filesystem and database failures stay visible: they become `insufficient_storage` (disk
//! full) or `storage_unavailable`, never a success response (§2 rule 7).

use std::io;

use crate::filesystem::is_out_of_space;
use crate::metadata::is_disk_full;
use crate::protocol::ErrorCode;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiError {
    pub code: ErrorCode,
    pub message: String,
    /// Seconds a client should wait before retrying (sent as `Retry-After`).
    pub retry_after_secs: Option<u64>,
}

impl ApiError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> ApiError {
        ApiError {
            code,
            message: message.into(),
            retry_after_secs: None,
        }
    }

    pub fn not_found() -> ApiError {
        ApiError::new(ErrorCode::NotFound, "no such session for this client")
    }

    pub fn state_conflict(message: impl Into<String>) -> ApiError {
        ApiError::new(ErrorCode::StateConflict, message)
    }

    pub fn invalid_chunk(message: impl Into<String>) -> ApiError {
        ApiError::new(ErrorCode::InvalidChunk, message)
    }

    pub fn busy(message: impl Into<String>) -> ApiError {
        ApiError {
            retry_after_secs: Some(1),
            ..ApiError::new(ErrorCode::Busy, message)
        }
    }

    /// Classifies a filesystem error. Paths are never included in the message.
    pub fn storage(context: &str, error: &io::Error) -> ApiError {
        let code = if is_out_of_space(error) {
            ErrorCode::InsufficientStorage
        } else {
            ErrorCode::StorageUnavailable
        };
        ApiError::new(code, format!("{context}: {error}"))
    }

    /// Classifies a metadata database error.
    pub fn database(context: &str, error: &rusqlite::Error) -> ApiError {
        let code = if is_disk_full(error) {
            ErrorCode::InsufficientStorage
        } else {
            ErrorCode::StorageUnavailable
        };
        ApiError::new(code, format!("{context}: {error}"))
    }
}

impl From<rusqlite::Error> for ApiError {
    fn from(error: rusqlite::Error) -> ApiError {
        ApiError::database("metadata operation failed", &error)
    }
}

impl From<io::Error> for ApiError {
    fn from(error: io::Error) -> ApiError {
        ApiError::storage("storage operation failed", &error)
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.message)
    }
}
