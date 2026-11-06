//! Protocol constants, admission caps and validated runtime limits.
//!
//! Every limit here protects memory, disk or the metadata database from an oversized request.
//! The caps are design defaults from the specification (§5, §9), not benchmark results.

use std::time::Duration;

/// Protocol revision carried in every manifest.
pub const PROTOCOL_VERSION: u32 = 1;

/// Fixed chunk size for protocol version 1. The final chunk of a file can be smaller.
pub const CHUNK_SIZE_BYTES: u64 = 4 * 1024 * 1024;

/// Admission caps for one transfer (§5).
pub const MAX_TOTAL_BYTES: u64 = 100 * 1024 * 1024 * 1024;
pub const MAX_FILES: usize = 10_000;
pub const MAX_DIRECTORIES: usize = 10_000;
pub const MAX_CHUNKS: u64 = 50_000;
pub const MAX_MANIFEST_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_PATH_DEPTH: usize = 32;
pub const MAX_PATH_BYTES: usize = 1024;
pub const MAX_COMPONENT_BYTES: usize = 255;

/// Idempotency keys are opaque client strings; this bounds what the receiver stores.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;

/// Receiver limits. Defaults follow §9: eight 4 MiB payload permits per process, so chunk
/// buffers never exceed 32 MiB. Total process memory additionally includes manifests, TLS,
/// HTTP, SQLite and runtime allocations.
#[derive(Clone, Debug)]
pub struct ReceiverLimits {
    /// Global chunk-buffer permits. A permit is taken before a body is read.
    pub chunk_buffer_permits: usize,
    /// Capacity of the bounded queue in front of the storage worker.
    pub storage_queue_depth: usize,
    /// Concurrent TCP connections accepted; extra connections are closed immediately.
    pub max_connections: usize,
    /// Concurrent manifest uploads (each may hold up to 16 MiB).
    pub metadata_permits: usize,
    /// How long a request waits for a buffer permit before receiving 429.
    pub permit_wait: Duration,
    /// Longest gap allowed between two pieces of a request body.
    pub body_idle_timeout: Duration,
    /// Longest time allowed to receive request headers.
    pub header_read_timeout: Duration,
    /// Longest time allowed for a TLS handshake.
    pub tls_handshake_timeout: Duration,
    /// Largest number of chunk references in one missing-list page.
    pub missing_page_limit: usize,
}

impl Default for ReceiverLimits {
    fn default() -> Self {
        ReceiverLimits {
            chunk_buffer_permits: 8,
            storage_queue_depth: 8,
            max_connections: 64,
            metadata_permits: 2,
            permit_wait: Duration::from_millis(2_000),
            body_idle_timeout: Duration::from_millis(10_000),
            header_read_timeout: Duration::from_millis(10_000),
            tls_handshake_timeout: Duration::from_millis(10_000),
            missing_page_limit: 1_000,
        }
    }
}

impl ReceiverLimits {
    /// Rejects limits that would disable a protection or make the receiver unusable.
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=64).contains(&self.chunk_buffer_permits) {
            return Err("chunk buffer permits must be between 1 and 64".into());
        }
        if !(1..=1024).contains(&self.storage_queue_depth) {
            return Err("storage queue depth must be between 1 and 1024".into());
        }
        if !(1..=4096).contains(&self.max_connections) {
            return Err("max connections must be between 1 and 4096".into());
        }
        if !(1..=16).contains(&self.metadata_permits) {
            return Err("metadata permits must be between 1 and 16".into());
        }
        if self.body_idle_timeout < Duration::from_millis(50) {
            return Err("body idle timeout must be at least 50 ms".into());
        }
        if !(1..=10_000).contains(&self.missing_page_limit) {
            return Err("missing page limit must be between 1 and 10000".into());
        }
        Ok(())
    }
}

/// Sender settings (§6, §9).
#[derive(Clone, Debug)]
pub struct SenderSettings {
    /// Concurrent chunk uploads.
    pub upload_workers: usize,
    /// How long one request keeps retrying transient failures before the send gives up.
    pub retry_window: Duration,
    /// Per-request timeout. A timeout is treated as an uncertain outcome, not a failure.
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    /// Upload rounds allowed before the sender stops and reports the receiver as not converging.
    pub max_rounds: usize,
}

impl Default for SenderSettings {
    fn default() -> Self {
        SenderSettings {
            upload_workers: 4,
            retry_window: Duration::from_secs(120),
            request_timeout: Duration::from_secs(60),
            connect_timeout: Duration::from_secs(10),
            max_rounds: 20,
        }
    }
}

impl SenderSettings {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=64).contains(&self.upload_workers) {
            return Err("upload workers must be between 1 and 64".into());
        }
        if self.max_rounds == 0 {
            return Err("max rounds must be at least 1".into());
        }
        Ok(())
    }
}

/// Number of chunks in a file of `size_bytes`. Zero-byte files have no chunks.
pub fn chunk_count(size_bytes: u64) -> u64 {
    size_bytes.div_ceil(CHUNK_SIZE_BYTES)
}

/// Byte length of chunk `index` in a file of `size_bytes`, or `None` if the index is past the end.
pub fn chunk_length(size_bytes: u64, index: u32) -> Option<u64> {
    let start = u64::from(index).checked_mul(CHUNK_SIZE_BYTES)?;
    if start >= size_bytes {
        return None;
    }
    Some((size_bytes - start).min(CHUNK_SIZE_BYTES))
}

/// Byte offset of chunk `index`. Offsets are derived locally and never supplied by a client.
pub fn chunk_offset(index: u32) -> u64 {
    // u32::MAX * 4 MiB fits comfortably in u64.
    u64::from(index) * CHUNK_SIZE_BYTES
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_arithmetic_covers_boundaries() {
        assert_eq!(chunk_count(0), 0);
        assert_eq!(chunk_count(1), 1);
        assert_eq!(chunk_count(CHUNK_SIZE_BYTES), 1);
        assert_eq!(chunk_count(CHUNK_SIZE_BYTES + 1), 2);
        assert_eq!(chunk_length(0, 0), None);
        assert_eq!(chunk_length(1, 0), Some(1));
        assert_eq!(
            chunk_length(CHUNK_SIZE_BYTES + 1, 0),
            Some(CHUNK_SIZE_BYTES)
        );
        assert_eq!(chunk_length(CHUNK_SIZE_BYTES + 1, 1), Some(1));
        assert_eq!(chunk_length(CHUNK_SIZE_BYTES + 1, 2), None);
        assert_eq!(chunk_length(u64::MAX, u32::MAX), Some(CHUNK_SIZE_BYTES));
        assert_eq!(chunk_offset(3), 3 * CHUNK_SIZE_BYTES);
    }

    #[test]
    fn default_limits_are_valid_and_bad_limits_are_rejected() {
        assert!(ReceiverLimits::default().validate().is_ok());
        let zero_permits = ReceiverLimits {
            chunk_buffer_permits: 0,
            ..ReceiverLimits::default()
        };
        assert!(zero_permits.validate().is_err());
        assert!(SenderSettings::default().validate().is_ok());
    }
}
