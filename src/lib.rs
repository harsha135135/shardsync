//! ShardSync: resumable, verified transfer of a fixed file or folder version.
//!
//! The sender prepares an immutable spool and a manifest, then uploads 4 MiB chunks over
//! HTTPS. The receiver acknowledges a chunk only after its bytes are synced and its receipt
//! is committed, verifies the complete content, and publishes it with one no-replace
//! directory rename. After a disconnect or restart, the sender asks which chunks are still
//! missing and continues. See `docs/specification.md` for the design contract.
//!
//! Module map:
//! - `manifest`, `config`: the transfer description and its limits
//! - `snapshot`, `sender`: preparation, resume planning and uploads
//! - `api`, `protocol`: HTTPS routing, authentication and wire types
//! - `storage`, `metadata`, `publish`: the single storage worker, SQLite receipts,
//!   verification, publication and recovery
//! - `filesystem`: every platform-specific filesystem call
//! - `progress`: counters, status snapshots and bounded buffers

pub mod api;
pub mod api_error;
pub mod cleanup;
pub mod config;
pub mod devcert;
pub mod error;
pub mod faults;
pub mod filesystem;
pub mod json_checks;
pub mod manifest;
pub mod metadata;
pub mod progress;
pub mod protocol;
pub mod publish;
pub mod sender;
pub mod snapshot;
pub mod storage;
pub mod tls;
