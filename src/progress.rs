//! Counters, the cached status snapshot, and bounded chunk buffers.
//!
//! Shared objects and their rules:
//! - [`Counters`] holds atomics only. Any thread may increment; readers accept slightly stale
//!   values.
//! - [`ProgressSnapshot`] lives in a `tokio::sync::watch` channel. Only the storage worker
//!   writes it; handlers clone one entry and release the borrow immediately (never across an
//!   `.await`). Status reads therefore stay responsive during a long verification.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use tokio::sync::OwnedSemaphorePermit;

use crate::protocol::SessionStatus;

#[derive(Debug, Default)]
pub struct Counters {
    pub chunk_writes: AtomicU64,
    pub duplicate_chunk_requests: AtomicU64,
    /// Time spent in `sync_data` for chunk writes, in microseconds.
    pub chunk_sync_micros: AtomicU64,
    /// Time spent committing durable receipts (SQLite FULL commits), in microseconds.
    pub receipt_commit_micros: AtomicU64,
    /// Time spent reading and hashing staged bytes during verification, in microseconds.
    pub verify_micros: AtomicU64,
    /// Time spent on publication steps (syncs, intent, rename), in microseconds.
    pub publish_micros: AtomicU64,
    live_buffers: AtomicUsize,
    buffer_high_water: AtomicUsize,
}

/// Adds the time elapsed since `started` to a microsecond counter.
pub fn add_elapsed(counter: &AtomicU64, started: std::time::Instant) {
    let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    counter.fetch_add(micros, Ordering::Relaxed);
}

impl Counters {
    pub fn buffer_high_water(&self) -> usize {
        self.buffer_high_water.load(Ordering::SeqCst)
    }

    pub fn live_buffers(&self) -> usize {
        self.live_buffers.load(Ordering::SeqCst)
    }
}

/// One chunk body and the permit that pays for it.
///
/// The permit is taken before the body is read and travels with the buffer through the
/// storage queue, so queued buffers count against the same global limit. Moving this value
/// moves the `Vec` without copying the bytes. The permit returns when the value is dropped.
pub struct PermittedChunk {
    pub bytes: Vec<u8>,
    counters: Arc<Counters>,
    _permit: OwnedSemaphorePermit,
}

impl PermittedChunk {
    pub fn new(permit: OwnedSemaphorePermit, counters: Arc<Counters>) -> PermittedChunk {
        let live = counters.live_buffers.fetch_add(1, Ordering::SeqCst) + 1;
        counters.buffer_high_water.fetch_max(live, Ordering::SeqCst);
        PermittedChunk {
            bytes: Vec::new(),
            counters,
            _permit: permit,
        }
    }
}

impl Drop for PermittedChunk {
    fn drop(&mut self) {
        self.counters.live_buffers.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A session's status together with its owner, so handlers can enforce ownership.
#[derive(Clone, Debug)]
pub struct SessionView {
    pub owner: String,
    pub status: SessionStatus,
}

/// Status of every session the receiver knows, keyed by session ID.
#[derive(Clone, Debug, Default)]
pub struct ProgressSnapshot {
    pub sessions: HashMap<String, SessionView>,
}

/// Peak resident set size of this process in bytes (Linux `VmHWM`).
pub fn peak_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    kib.checked_mul(1024)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Semaphore;

    #[test]
    fn buffers_hold_permits_and_track_high_water() {
        let semaphore = Arc::new(Semaphore::new(2));
        let counters = Arc::new(Counters::default());
        let a = PermittedChunk::new(
            semaphore.clone().try_acquire_owned().unwrap(),
            counters.clone(),
        );
        let b = PermittedChunk::new(
            semaphore.clone().try_acquire_owned().unwrap(),
            counters.clone(),
        );
        assert!(semaphore.clone().try_acquire_owned().is_err());
        assert_eq!(counters.live_buffers(), 2);
        drop(a);
        assert_eq!(counters.live_buffers(), 1);
        assert!(semaphore.clone().try_acquire_owned().is_ok());
        drop(b);
        assert_eq!(counters.live_buffers(), 0);
        assert_eq!(counters.buffer_high_water(), 2);
    }
}
