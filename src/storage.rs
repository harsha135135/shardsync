//! The single storage worker (§3, §7).
//!
//! One dedicated thread owns every writable staging file handle and the only writable SQLite
//! connection. HTTP handlers send it [`StorageCommand`]s through a bounded channel and await
//! a reply on a oneshot channel. Commands are processed one at a time, which rules out
//! overlapping writes, shared file-position bugs and competing database writers. This is a
//! deliberate first-release performance trade-off.
//!
//! Each command rechecks the durable session state when it runs, so an earlier HTTP admission
//! can never bypass a state change that happened while the command was queued.
//!
//! Long verification runs as an incremental job (see `publish.rs`): between 4 MiB steps the
//! worker drains queued commands, so an abort during verification is refused promptly while
//! verification continues.

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::os::fd::OwnedFd;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rusqlite::Connection;
use tokio::sync::{mpsc, oneshot, watch};

use crate::api_error::ApiError;
use crate::config::chunk_offset;
use crate::faults;
use crate::filesystem::{
    create_file_beneath, create_new_file_beneath, ensure_dir_beneath, open_dir, open_dir_beneath,
    open_file_beneath, sync_dir, sync_dir_path,
};
use crate::manifest::{ValidatedManifest, parent_first, sha256_hex};
use crate::metadata::{self, FileRecord, SessionRow};
use crate::progress::{Counters, PermittedChunk, ProgressSnapshot, SessionView};
use crate::protocol::{ChunkRef, ErrorCode, MissingPage, SessionState, StateResponse};
use crate::publish::VerificationJob;

/// Free space kept in reserve beyond a transfer's declared size at admission. Admission checks
/// only aid the operator; later out-of-space errors are still handled (§5).
const ADMISSION_RESERVE_BYTES: u64 = 64 * 1024 * 1024;

/// Layout of a receiver root (§5).
#[derive(Clone, Debug)]
pub struct ReceiverPaths {
    pub root: PathBuf,
}

impl ReceiverPaths {
    pub fn new(root: &Path) -> ReceiverPaths {
        ReceiverPaths {
            root: root.to_path_buf(),
        }
    }
    pub fn metadata_dir(&self) -> PathBuf {
        self.root.join("metadata")
    }
    pub fn database(&self) -> PathBuf {
        self.metadata_dir().join("receiver.sqlite")
    }
    pub fn lock_file(&self) -> PathBuf {
        self.metadata_dir().join("receiver.lock")
    }
    pub fn staging(&self) -> PathBuf {
        self.root.join("staging")
    }
    pub fn completed(&self) -> PathBuf {
        self.root.join("completed")
    }
    pub fn staging_session(&self, id: &str) -> PathBuf {
        self.staging().join(id)
    }
    pub fn completed_session(&self, id: &str) -> PathBuf {
        self.completed().join(id)
    }

    /// Creates the root layout with private permissions and persists the new entries.
    pub fn create_layout(&self) -> Result<(), String> {
        use std::os::unix::fs::DirBuilderExt;
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true).mode(0o700);
        for dir in [
            self.root.clone(),
            self.metadata_dir(),
            self.staging(),
            self.completed(),
        ] {
            builder
                .create(&dir)
                .map_err(|e| format!("create {}: {e}", dir.display()))?;
        }
        sync_dir_path(&self.root).map_err(|e| format!("sync receiver root: {e}"))
    }
}

/// Published destination recorded in the publication intent.
pub fn destination_name(id: &str) -> String {
    format!("completed/{id}")
}

/// Identity written into every staged and published tree.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferIdentity {
    pub session_id: String,
    pub manifest_sha256: String,
    pub protocol_version: u32,
}

pub struct CreateOutcome {
    pub created: bool,
    pub session_id: String,
    pub state: SessionState,
}

pub enum CommitOutcome {
    /// Verification or publication is under way (202).
    Accepted(StateResponse),
    /// Already published; the same destination is returned (200).
    Published(StateResponse),
}

type Reply<T> = oneshot::Sender<Result<T, ApiError>>;

pub enum StorageCommand {
    Create {
        owner: String,
        idempotency_key: String,
        manifest_bytes: Vec<u8>,
        validated: Box<ValidatedManifest>,
        reply: Reply<CreateOutcome>,
    },
    WriteChunk {
        owner: String,
        session_id: String,
        file_id: u32,
        chunk_index: u32,
        chunk: PermittedChunk,
        reply: Reply<()>,
    },
    Missing {
        owner: String,
        session_id: String,
        after: Option<ChunkRef>,
        limit: usize,
        reply: Reply<MissingPage>,
    },
    Commit {
        owner: String,
        session_id: String,
        reply: Reply<CommitOutcome>,
    },
    Abort {
        owner: String,
        session_id: String,
        reply: Reply<StateResponse>,
    },
    /// Finish the current command and stop. Unfinished verification resumes after restart.
    Shutdown { reply: oneshot::Sender<()> },
}

/// Open handles for the session currently receiving chunks.
pub(crate) struct OpenSession {
    pub session_id: String,
    pub data_dir: OwnedFd,
    pub files: Vec<FileRecord>,
    pub handles: HashMap<u32, File>,
}

pub struct StorageWorker {
    pub(crate) paths: ReceiverPaths,
    pub(crate) db: Connection,
    pub(crate) progress: watch::Sender<ProgressSnapshot>,
    pub(crate) counters: Arc<Counters>,
    pub(crate) open: Option<OpenSession>,
    pub(crate) verification: Option<VerificationJob>,
    pub(crate) verified_bytes: HashMap<String, u64>,
}

enum Flow {
    Continue,
    Stop,
}

impl StorageWorker {
    pub fn new(
        paths: ReceiverPaths,
        db: Connection,
        progress: watch::Sender<ProgressSnapshot>,
        counters: Arc<Counters>,
    ) -> StorageWorker {
        StorageWorker {
            paths,
            db,
            progress,
            counters,
            open: None,
            verification: None,
            verified_bytes: HashMap::new(),
        }
    }

    /// Runs until shutdown or until every command sender is gone (after finishing any
    /// verification in progress).
    pub fn run(mut self, mut commands: mpsc::Receiver<StorageCommand>) {
        loop {
            if self.verification.is_some() {
                // Answer queued commands between verification steps without blocking.
                while let Ok(command) = commands.try_recv() {
                    if let Flow::Stop = self.handle(command) {
                        return;
                    }
                }
                if faults::holding("during-verify") {
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                self.advance_verification();
            } else {
                match commands.blocking_recv() {
                    Some(command) => {
                        if let Flow::Stop = self.handle(command) {
                            return;
                        }
                    }
                    None => return,
                }
            }
        }
    }

    fn handle(&mut self, command: StorageCommand) -> Flow {
        match command {
            StorageCommand::Create {
                owner,
                idempotency_key,
                manifest_bytes,
                validated,
                reply,
            } => {
                let result =
                    self.create_session(&owner, &idempotency_key, &manifest_bytes, &validated);
                let _ = reply.send(result);
            }
            StorageCommand::WriteChunk {
                owner,
                session_id,
                file_id,
                chunk_index,
                chunk,
                reply,
            } => {
                let result = self.write_chunk(&owner, &session_id, file_id, chunk_index, chunk);
                let _ = reply.send(result);
            }
            StorageCommand::Missing {
                owner,
                session_id,
                after,
                limit,
                reply,
            } => {
                let result = self
                    .owned_session(&owner, &session_id)
                    .and_then(|_| Ok(metadata::missing_page(&self.db, &session_id, after, limit)?));
                let _ = reply.send(result);
            }
            StorageCommand::Commit {
                owner,
                session_id,
                reply,
            } => {
                // The reply is sent before any verification step runs (§6: acceptance first,
                // then the long pass).
                let result = self.commit(&owner, &session_id);
                let _ = reply.send(result);
            }
            StorageCommand::Abort {
                owner,
                session_id,
                reply,
            } => {
                let result = self.abort(&owner, &session_id);
                let _ = reply.send(result);
            }
            StorageCommand::Shutdown { reply } => {
                let _ = reply.send(());
                return Flow::Stop;
            }
        }
        Flow::Continue
    }

    /// Loads a session visible to `owner`. Another client's session is reported as missing.
    pub(crate) fn owned_session(&self, owner: &str, id: &str) -> Result<SessionRow, ApiError> {
        match metadata::session(&self.db, id)? {
            Some(row) if row.owner == owner => Ok(row),
            _ => Err(ApiError::not_found()),
        }
    }

    /// Publishes a session's current durable status to the status snapshot.
    pub(crate) fn refresh(&mut self, id: &str) {
        match metadata::session(&self.db, id) {
            Ok(Some(row)) => {
                let verified = self.verified_bytes.get(id).copied().unwrap_or(0);
                let view = SessionView {
                    owner: row.owner.clone(),
                    status: row.status(verified),
                };
                self.progress.send_modify(|snapshot| {
                    snapshot.sessions.insert(id.to_string(), view);
                });
            }
            Ok(None) => {}
            Err(error) => tracing::error!(session_id = id, %error, "status refresh failed"),
        }
    }

    pub(crate) fn set_verified_bytes(&mut self, id: &str, bytes: u64) {
        self.verified_bytes.insert(id.to_string(), bytes);
        self.progress.send_modify(|snapshot| {
            if let Some(view) = snapshot.sessions.get_mut(id) {
                view.status.verified_bytes = bytes;
            }
        });
    }

    /// Records a visible last-error condition, best effort.
    pub(crate) fn record_error(&mut self, id: &str, error: &ApiError) {
        if let Err(db_error) = metadata::set_last_error(&self.db, id, Some(&error.message)) {
            tracing::error!(session_id = id, %db_error, "could not record last error");
        }
        self.refresh(id);
        // Even when the database itself cannot be written (a full disk), the condition stays
        // visible in status until the next successful change replaces it.
        let message = error.message.clone();
        self.progress.send_modify(|snapshot| {
            if let Some(view) = snapshot.sessions.get_mut(id) {
                view.status.last_error = Some(message);
            }
        });
    }

    pub(crate) fn transition(
        &mut self,
        id: &str,
        from: SessionState,
        to: SessionState,
        last_error: Option<&str>,
    ) -> Result<(), ApiError> {
        if !metadata::transition(&self.db, id, from, to, last_error)? {
            return Err(ApiError::new(
                ErrorCode::Internal,
                format!("session {id} was not {} as expected", from.as_str()),
            ));
        }
        tracing::info!(
            session_id = id,
            from = from.as_str(),
            to = to.as_str(),
            "state change"
        );
        self.refresh(id);
        Ok(())
    }

    // ----- Session creation (§5) -------------------------------------------------------

    fn create_session(
        &mut self,
        owner: &str,
        idempotency_key: &str,
        manifest_bytes: &[u8],
        validated: &ValidatedManifest,
    ) -> Result<CreateOutcome, ApiError> {
        if let Some(existing) = metadata::session_by_key(&self.db, owner, idempotency_key)? {
            if existing.manifest_sha256 != validated.digest {
                return Err(ApiError::state_conflict(
                    "this idempotency key was already used for a different manifest",
                ));
            }
            // A replay may arrive while an earlier attempt left preparation unfinished.
            if existing.state == SessionState::Preparing {
                self.finish_preparation(&existing.id)?;
            }
            let state = metadata::session(&self.db, &existing.id)?
                .map(|row| row.state)
                .unwrap_or(existing.state);
            return Ok(CreateOutcome {
                created: false,
                session_id: existing.id,
                state,
            });
        }
        if let Some(active) = metadata::active_session(&self.db)? {
            return Err(ApiError::state_conflict(format!(
                "another transfer ({}) is active on this receiver",
                active.id
            )));
        }
        let available = crate::filesystem::available_bytes(&self.paths.staging())
            .map_err(|e| ApiError::storage("check free space", &e))?;
        let needed = validated
            .total_bytes
            .saturating_add(manifest_bytes.len() as u64)
            .saturating_add(ADMISSION_RESERVE_BYTES);
        if available < needed {
            return Err(ApiError::new(
                ErrorCode::InsufficientStorage,
                format!("transfer needs {needed} bytes including reserve; {available} available"),
            ));
        }

        let id = uuid::Uuid::new_v4().simple().to_string();
        // 1. Save and sync the exact manifest bytes before recording Preparing.
        self.save_manifest(&id, manifest_bytes)
            .map_err(|e| ApiError::storage("save manifest", &e))?;
        // 2. Record Preparing with files, chunks and directories.
        metadata::insert_preparing_session(&mut self.db, &id, owner, idempotency_key, validated)?;
        tracing::info!(session_id = %id, owner, files = validated.manifest.files.len(),
            total_bytes = validated.total_bytes, "session preparing");
        faults::hit("after-preparing-commit")?;
        // 3 and 4. Build and sync the staging tree, then commit Receiving.
        self.finish_preparation(&id)?;
        Ok(CreateOutcome {
            created: true,
            session_id: id,
            state: SessionState::Receiving,
        })
    }

    fn save_manifest(&self, id: &str, manifest_bytes: &[u8]) -> std::io::Result<()> {
        let staging = open_dir(&self.paths.staging())?;
        let session_dir = ensure_dir_beneath(&staging, id)?;
        let mut file = create_new_file_beneath(&session_dir, "manifest.json", 0o600)?;
        file.write_all(manifest_bytes)?;
        file.sync_all()?;
        sync_dir(&session_dir)?;
        sync_dir(&staging)
    }

    /// Builds (or rebuilds) the staging tree of a Preparing session and commits Receiving.
    pub(crate) fn finish_preparation(&mut self, id: &str) -> Result<(), ApiError> {
        self.build_staging_tree(id)
            .map_err(|e| ApiError::storage("build staging tree", &e))?;
        faults::hit("after-staging-synced")?;
        self.transition(id, SessionState::Preparing, SessionState::Receiving, None)?;
        faults::hit("after-create-commit")?;
        Ok(())
    }

    /// Idempotently creates `staging/<id>/tree/data` with every directory and every file at
    /// its expected length, then syncs files and directories from children to parents.
    /// Existing content is kept, so this also repairs a tree with missing entries.
    pub(crate) fn build_staging_tree(&mut self, id: &str) -> std::io::Result<()> {
        self.open = None;
        let files = metadata::files(&self.db, id).map_err(std::io::Error::other)?;
        let directories = metadata::directories(&self.db, id).map_err(std::io::Error::other)?;
        let staging = open_dir(&self.paths.staging())?;
        let session_dir = open_dir_beneath(&staging, id)?;
        let tree = ensure_dir_beneath(&session_dir, "tree")?;
        let data = ensure_dir_beneath(&tree, "data")?;
        let ordered = parent_first(directories.iter().map(String::as_str));
        for directory in &ordered {
            ensure_dir_beneath(&data, directory)?;
        }
        for file in &files {
            let handle = create_file_beneath(&data, &file.path)?;
            if handle.metadata()?.len() != file.size_bytes {
                // A sparse logical length does not reserve physical storage (§5).
                handle.set_len(file.size_bytes)?;
            }
            handle.sync_all()?;
        }
        for directory in ordered.iter().rev() {
            sync_dir(open_dir_beneath(&data, directory)?)?;
        }
        sync_dir(&data)?;
        sync_dir(&tree)?;
        sync_dir(&session_dir)
    }

    // ----- Durable chunk receipt (§7) --------------------------------------------------

    /// Opens (or reuses) staging handles for `id`.
    pub(crate) fn open_session(&mut self, id: &str) -> std::io::Result<&mut OpenSession> {
        let reuse = matches!(&self.open, Some(open) if open.session_id == id);
        if !reuse {
            let root = open_dir(&self.paths.root)?;
            let data_dir = open_dir_beneath(&root, &format!("staging/{id}/tree/data"))?;
            let files = metadata::files(&self.db, id).map_err(std::io::Error::other)?;
            self.open = Some(OpenSession {
                session_id: id.to_string(),
                data_dir,
                files,
                handles: HashMap::new(),
            });
        }
        Ok(self.open.as_mut().expect("opened above"))
    }

    fn staged_file(&mut self, id: &str, file_id: u32) -> std::io::Result<&File> {
        let open = self.open_session(id)?;
        if !open.handles.contains_key(&file_id) {
            let record = open
                .files
                .get(file_id as usize)
                .ok_or_else(|| std::io::Error::other("file ID not in session"))?;
            let handle = open_file_beneath(&open.data_dir, &record.path, true)?;
            open.handles.insert(file_id, handle);
        }
        Ok(open.handles.get(&file_id).expect("inserted above"))
    }

    fn write_chunk(
        &mut self,
        owner: &str,
        id: &str,
        file_id: u32,
        chunk_index: u32,
        chunk: PermittedChunk,
    ) -> Result<(), ApiError> {
        // 1. Session state, file ID, chunk index and exact length.
        let session = self.owned_session(owner, id)?;
        if session.state != SessionState::Receiving {
            return Err(ApiError::state_conflict(format!(
                "session is {}; chunks are accepted only while receiving",
                session.state.as_str()
            )));
        }
        let Some(expected) = metadata::chunk(&self.db, id, file_id, chunk_index)? else {
            return Err(ApiError::invalid_chunk(format!(
                "file {file_id} chunk {chunk_index} is not in the manifest"
            )));
        };
        if expected.durable {
            // A retry of a recorded chunk does not change the logical result (§2 rule 2).
            self.counters
                .duplicate_chunk_requests
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Ok(());
        }
        if chunk.bytes.len() as u64 != expected.expected_length {
            return Err(ApiError::invalid_chunk(format!(
                "file {file_id} chunk {chunk_index} must be {} bytes; received {}",
                expected.expected_length,
                chunk.bytes.len()
            )));
        }
        // 2. Hash before touching the staging file.
        if sha256_hex(&chunk.bytes) != expected.expected_sha256 {
            return Err(ApiError::new(
                ErrorCode::HashMismatch,
                format!("file {file_id} chunk {chunk_index} does not match its manifest digest"),
            ));
        }
        // 3 and 4. Write the whole buffer at the derived offset, then sync_data.
        if let Err(error) = self.write_and_sync(id, file_id, chunk_index, &chunk.bytes) {
            let error = ApiError::storage("write chunk", &error);
            tracing::warn!(session_id = id, file_id, chunk_index, %error, "chunk write failed");
            self.open = None;
            self.record_error(id, &error);
            return Err(error);
        }
        drop(chunk);
        self.counters
            .chunk_writes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        faults::hit("after-chunk-sync")?;
        // 5. Record the durable receipt and progress in one FULL-synchronous transaction.
        // A commit error is uncertain: the client must query after recovery (§7).
        let commit_started = std::time::Instant::now();
        let committed = metadata::record_receipt(&mut self.db, id, file_id, chunk_index);
        crate::progress::add_elapsed(&self.counters.receipt_commit_micros, commit_started);
        if let Err(db_error) = committed {
            let error = ApiError::database("record receipt", &db_error);
            tracing::warn!(session_id = id, file_id, chunk_index, %error, "receipt commit failed");
            self.record_error(id, &error);
            return Err(error);
        }
        faults::hit("after-receipt-commit")?;
        self.refresh(id);
        // 6. Only now may the handler send 204.
        Ok(())
    }

    fn write_and_sync(
        &mut self,
        id: &str,
        file_id: u32,
        chunk_index: u32,
        bytes: &[u8],
    ) -> std::io::Result<()> {
        faults::hit("before-chunk-write")?;
        let file = self.staged_file(id, file_id)?;
        file.write_all_at(bytes, chunk_offset(chunk_index))?;
        let started = std::time::Instant::now();
        let synced = file.sync_data();
        crate::progress::add_elapsed(&self.counters.chunk_sync_micros, started);
        synced
    }

    // ----- Commit and abort (§6, §8) ---------------------------------------------------

    fn state_response(row: &SessionRow) -> StateResponse {
        StateResponse {
            session_id: row.id.clone(),
            state: row.state,
            destination: if row.state == SessionState::Published {
                row.destination.clone()
            } else {
                None
            },
        }
    }

    fn commit(&mut self, owner: &str, id: &str) -> Result<CommitOutcome, ApiError> {
        let session = self.owned_session(owner, id)?;
        match session.state {
            SessionState::Published => Ok(CommitOutcome::Published(Self::state_response(&session))),
            SessionState::Verifying => {
                // A storage error may have stopped an earlier pass; a replay restarts it.
                if self.verification.is_none() {
                    self.start_verification(id, crate::publish::VerifyPurpose::Commit)?;
                }
                Ok(CommitOutcome::Accepted(Self::state_response(&session)))
            }
            SessionState::Publishing => Ok(CommitOutcome::Accepted(Self::state_response(&session))),
            SessionState::Preparing => Err(ApiError::state_conflict("session is still preparing")),
            SessionState::Aborted | SessionState::Faulted => Err(ApiError::state_conflict(
                format!("session is {}", session.state.as_str()),
            )),
            SessionState::Receiving => {
                let missing = metadata::missing_count(&self.db, id)?;
                if missing > 0 {
                    return Err(ApiError::state_conflict(format!(
                        "{missing} chunks have no durable receipt"
                    )));
                }
                self.transition(id, SessionState::Receiving, SessionState::Verifying, None)?;
                self.open = None;
                self.start_verification(id, crate::publish::VerifyPurpose::Commit)?;
                let row = metadata::session(&self.db, id)?.ok_or_else(ApiError::not_found)?;
                Ok(CommitOutcome::Accepted(Self::state_response(&row)))
            }
        }
    }

    fn abort(&mut self, owner: &str, id: &str) -> Result<StateResponse, ApiError> {
        let session = self.owned_session(owner, id)?;
        match session.state {
            SessionState::Receiving => {
                self.transition(id, SessionState::Receiving, SessionState::Aborted, None)?;
                self.open = None;
                let row = metadata::session(&self.db, id)?.ok_or_else(ApiError::not_found)?;
                Ok(Self::state_response(&row))
            }
            SessionState::Aborted => Ok(Self::state_response(&session)),
            other => Err(ApiError::state_conflict(format!(
                "a session that is {} cannot be aborted",
                other.as_str()
            ))),
        }
    }
}
