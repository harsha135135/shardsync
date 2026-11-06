//! Wire types shared by the receiver API and the sender: session states, error codes and the
//! JSON bodies of every endpoint (§6).

use serde::{Deserialize, Serialize};

/// Header carrying the client's idempotency key on session creation.
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
/// Header carrying the SHA-256 of the exact manifest bytes. The digest travels outside the
/// hashed document so that reserializing JSON can never change a manifest's identity.
pub const MANIFEST_DIGEST_HEADER: &str = "manifest-sha256";
/// Header naming the request so an error can be followed through the receiver's logs.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Durable session state (§8).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    /// Manifest stored; staging tree being built.
    Preparing,
    /// Accepting chunks.
    Receiving,
    /// Checking full contents; chunk writes are refused.
    Verifying,
    /// Publication intent recorded; completing the rename.
    Publishing,
    /// Terminal success; retries return the same destination.
    Published,
    /// Terminal abandonment; only private staging remains.
    Aborted,
    /// Unrecoverable inconsistency or destination conflict needing an operator.
    Faulted,
}

impl SessionState {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionState::Preparing => "preparing",
            SessionState::Receiving => "receiving",
            SessionState::Verifying => "verifying",
            SessionState::Publishing => "publishing",
            SessionState::Published => "published",
            SessionState::Aborted => "aborted",
            SessionState::Faulted => "faulted",
        }
    }

    pub fn parse(text: &str) -> Option<SessionState> {
        match text {
            "preparing" => Some(SessionState::Preparing),
            "receiving" => Some(SessionState::Receiving),
            "verifying" => Some(SessionState::Verifying),
            "publishing" => Some(SessionState::Publishing),
            "published" => Some(SessionState::Published),
            "aborted" => Some(SessionState::Aborted),
            "faulted" => Some(SessionState::Faulted),
            _ => None,
        }
    }

    /// An active session blocks creation of another transfer on this receiver.
    pub fn is_active(self) -> bool {
        matches!(
            self,
            SessionState::Preparing
                | SessionState::Receiving
                | SessionState::Verifying
                | SessionState::Publishing
        )
    }

    /// The complete transition table. Every durable state change is checked against it.
    pub fn can_become(self, next: SessionState) -> bool {
        use SessionState::*;
        matches!(
            (self, next),
            (Preparing, Receiving)
                | (Preparing, Faulted)
                | (Receiving, Verifying)
                | (Receiving, Aborted)
                | (Receiving, Faulted)
                | (Verifying, Receiving)
                | (Verifying, Publishing)
                | (Verifying, Faulted)
                | (Publishing, Published)
                | (Publishing, Faulted)
        )
    }
}

/// Stable machine-readable error codes. The first seven come from §6; the rest cover status
/// codes that §6 lists without naming a code (see docs/decisions.md).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    InvalidManifest,
    Unauthorized,
    InvalidChunk,
    HashMismatch,
    QuotaExceeded,
    StorageUnavailable,
    StateConflict,
    NotFound,
    Busy,
    InsufficientStorage,
    RequestTimeout,
    Internal,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::InvalidManifest => "invalid_manifest",
            ErrorCode::Unauthorized => "unauthorized",
            ErrorCode::InvalidChunk => "invalid_chunk",
            ErrorCode::HashMismatch => "hash_mismatch",
            ErrorCode::QuotaExceeded => "quota_exceeded",
            ErrorCode::StorageUnavailable => "storage_unavailable",
            ErrorCode::StateConflict => "state_conflict",
            ErrorCode::NotFound => "not_found",
            ErrorCode::Busy => "busy",
            ErrorCode::InsufficientStorage => "insufficient_storage",
            ErrorCode::RequestTimeout => "request_timeout",
            ErrorCode::Internal => "internal",
        }
    }

    /// HTTP status for each code, exactly as listed in §6.
    pub fn http_status(self) -> u16 {
        match self {
            ErrorCode::InvalidManifest | ErrorCode::InvalidChunk => 400,
            ErrorCode::Unauthorized => 401,
            ErrorCode::NotFound => 404,
            ErrorCode::RequestTimeout => 408,
            ErrorCode::StateConflict => 409,
            ErrorCode::QuotaExceeded => 413,
            ErrorCode::HashMismatch => 422,
            ErrorCode::Busy => 429,
            ErrorCode::Internal => 500,
            ErrorCode::StorageUnavailable => 503,
            ErrorCode::InsufficientStorage => 507,
        }
    }
}

/// Body of every error response.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
    pub request_id: String,
}

/// Returned by `POST /v1/sessions`: 201 for a new session, 200 for an identical replay.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSessionResponse {
    pub session_id: String,
    pub state: SessionState,
    pub chunk_size_bytes: u64,
    pub max_chunk_body_bytes: u64,
    pub missing_page_limit: usize,
}

/// Returned by `GET /v1/sessions/{id}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionStatus {
    pub session_id: String,
    pub state: SessionState,
    pub total_bytes: u64,
    pub total_chunks: u64,
    /// Bytes with a durable receipt. Informational: commit performs the authoritative check.
    pub durable_bytes: u64,
    pub durable_chunks: u64,
    /// Bytes checked by the current or most recent verification pass.
    pub verified_bytes: u64,
    /// `completed/<session-id>` once published.
    pub destination: Option<String>,
    pub last_error: Option<String>,
}

/// One position in the `(file_id, chunk_index)` order used by missing-list pagination.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChunkRef {
    pub file_id: u32,
    pub chunk_index: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissingFile {
    pub file_id: u32,
    pub chunk_indexes: Vec<u32>,
}

/// Returned by `GET /v1/sessions/{id}/missing`. Each page reflects receipts at the time it was
/// read; it is not an atomic snapshot across pages (§6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissingPage {
    pub missing: Vec<MissingFile>,
    /// Pass back as `after_file`/`after_chunk` to read the next page; absent on the last page.
    pub next_after: Option<ChunkRef>,
}

/// Returned by commit and abort.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateResponse {
    pub session_id: String,
    pub state: SessionState,
    pub destination: Option<String>,
}

/// Work performed by startup recovery, reported so restart cost is never described as free.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryReport {
    pub sessions_examined: u64,
    pub chunks_rehashed: u64,
    pub bytes_rehashed: u64,
    pub receipts_cleared: u64,
    pub duration_millis: u64,
}

/// Returned by `GET /v1/receiver`: process-level counters used by tests, the demo and benchmarks.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiverDiagnostics {
    /// `"validated"` only on the supported Linux/ext4 profile; otherwise `"unvalidated"`.
    pub durability_profile: String,
    pub filesystem: String,
    /// True only in test builds with the `fault-injection` feature; measurements require false.
    pub fault_injection_enabled: bool,
    /// Chunk bodies written and synced to staging by this process.
    pub chunk_writes: u64,
    /// Chunk uploads answered from an existing durable receipt without writing.
    pub duplicate_chunk_requests: u64,
    pub chunk_buffer_permits: usize,
    /// Most chunk buffers alive at once since start, including queued ones.
    pub buffer_high_water: usize,
    /// Cumulative time in chunk `sync_data` calls, receipt commits, verification reads and
    /// hashing, and publication steps, in microseconds.
    pub chunk_sync_micros: u64,
    pub receipt_commit_micros: u64,
    pub verify_micros: u64,
    pub publish_micros: u64,
    /// Peak resident set size of the receiver process (Linux only).
    pub peak_rss_bytes: Option<u64>,
    pub recovery: RecoveryReport,
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_STATES: [SessionState; 7] = [
        SessionState::Preparing,
        SessionState::Receiving,
        SessionState::Verifying,
        SessionState::Publishing,
        SessionState::Published,
        SessionState::Aborted,
        SessionState::Faulted,
    ];

    #[test]
    fn state_names_round_trip() {
        for state in ALL_STATES {
            assert_eq!(SessionState::parse(state.as_str()), Some(state));
        }
        assert_eq!(SessionState::parse("done"), None);
    }

    #[test]
    fn terminal_states_have_no_successor() {
        for terminal in [
            SessionState::Published,
            SessionState::Aborted,
            SessionState::Faulted,
        ] {
            for next in ALL_STATES {
                assert!(!terminal.can_become(next), "{terminal:?} -> {next:?}");
            }
        }
    }

    #[test]
    fn transition_table_matches_specification() {
        use SessionState::*;
        let allowed = [
            (Preparing, Receiving),
            (Preparing, Faulted),
            (Receiving, Verifying),
            (Receiving, Aborted),
            (Receiving, Faulted),
            (Verifying, Receiving),
            (Verifying, Publishing),
            (Verifying, Faulted),
            (Publishing, Published),
            (Publishing, Faulted),
        ];
        for from in ALL_STATES {
            for to in ALL_STATES {
                assert_eq!(
                    from.can_become(to),
                    allowed.contains(&(from, to)),
                    "{from:?} -> {to:?}"
                );
            }
        }
        // Abort is impossible once verification has begun.
        assert!(!Verifying.can_become(Aborted));
        assert!(!Publishing.can_become(Aborted));
    }

    #[test]
    fn error_codes_map_to_specified_statuses() {
        assert_eq!(ErrorCode::InvalidManifest.http_status(), 400);
        assert_eq!(ErrorCode::Unauthorized.http_status(), 401);
        assert_eq!(ErrorCode::NotFound.http_status(), 404);
        assert_eq!(ErrorCode::StateConflict.http_status(), 409);
        assert_eq!(ErrorCode::QuotaExceeded.http_status(), 413);
        assert_eq!(ErrorCode::HashMismatch.http_status(), 422);
        assert_eq!(ErrorCode::Busy.http_status(), 429);
        assert_eq!(ErrorCode::StorageUnavailable.http_status(), 503);
        assert_eq!(ErrorCode::InsufficientStorage.http_status(), 507);
    }
}
