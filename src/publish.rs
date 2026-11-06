//! Verification, atomic publication and startup recovery (§7, §8).
//!
//! Publication sequence after a successful verification:
//! 1. Write and sync `tree/transfer.json` (session ID and manifest identity).
//! 2. Sync every staged file, then every directory from children to parents.
//! 3. Durably record the publication intent and enter `Publishing`.
//! 4. Rename `staging/<id>/tree` to `completed/<id>` with no replacement.
//! 5. Sync both parent directories.
//! 6. Durably record `Published`; only then report completion.
//!
//! The rename and the SQLite update are not one atomic operation, so startup recovery
//! reconciles them (see [`StorageWorker::recover`]).

use std::fs::File;
use std::io::Write;
use std::os::fd::OwnedFd;
use std::os::unix::fs::FileExt;
use std::time::Instant;

use sha2::{Digest, Sha256};

use crate::api_error::ApiError;
use crate::config::{PROTOCOL_VERSION, chunk_count, chunk_length, chunk_offset};
use crate::faults;
use crate::filesystem::{
    create_file_beneath, open_dir, open_dir_beneath, open_file_beneath, rename_no_replace, sync_dir,
};
use crate::manifest::{parent_first, sha256_hex};
use crate::metadata::{self, FileRecord, SessionRow};
use crate::protocol::{ChunkRef, ErrorCode, RecoveryReport, SessionState};
use crate::storage::{StorageWorker, TransferIdentity, destination_name};

/// Why a verification pass is running, which decides what happens when it ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerifyPurpose {
    /// After commit (state Verifying): repair, fault, or publish.
    Commit,
    /// Recovery found Publishing with only staging present: recheck, then rename.
    ResumePublication,
    /// Recovery found Publishing with a matching destination: confirm it, then finish.
    ConfirmPublished,
}

/// An incremental pass over every file of a session, one chunk per step.
///
/// One read computes both the chunk digest and the running full-file digest, so a full-hash
/// mismatch immediately identifies which chunk receipts to clear.
pub struct VerificationJob {
    session_id: String,
    purpose: VerifyPurpose,
    data_dir: OwnedFd,
    files: Vec<FileRecord>,
    file_position: usize,
    chunk_position: u32,
    current: Option<File>,
    full_hasher: Sha256,
    buffer: Vec<u8>,
    verified_bytes: u64,
    bad_chunks: Vec<ChunkRef>,
    /// Files that were missing or had the wrong length; the staging tree must be repaired.
    damaged_files: Vec<u32>,
    full_hash_mismatches: Vec<u32>,
    started: Instant,
}

enum Step {
    Continue,
    Done,
}

impl VerificationJob {
    fn step(&mut self) -> std::io::Result<Step> {
        let Some(file) = self.files.get(self.file_position) else {
            return Ok(Step::Done);
        };
        let chunks = chunk_count(file.size_bytes);
        if self.current.is_none() {
            match open_file_beneath(&self.data_dir, &file.path, false) {
                Ok(handle) if handle.metadata()?.len() == file.size_bytes => {
                    self.current = Some(handle);
                    self.full_hasher = Sha256::new();
                }
                Ok(_) => return Ok(self.mark_file_damaged()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(self.mark_file_damaged());
                }
                Err(error) => return Err(error),
            }
        }
        if u64::from(self.chunk_position) < chunks {
            let index = self.chunk_position;
            let length = chunk_length(file.size_bytes, index).expect("index below chunk count");
            self.buffer.resize(length as usize, 0);
            let handle = self.current.as_ref().expect("opened above");
            handle.read_exact_at(&mut self.buffer, chunk_offset(index))?;
            self.full_hasher.update(&self.buffer);
            if sha256_hex(&self.buffer) != file.chunk_sha256[index as usize] {
                self.bad_chunks.push(ChunkRef {
                    file_id: file.file_id,
                    chunk_index: index,
                });
            }
            self.verified_bytes += length;
            self.chunk_position += 1;
        }
        if u64::from(self.chunk_position) >= chunks {
            let full = hex::encode(self.full_hasher.finalize_reset());
            if full != file.sha256 {
                self.full_hash_mismatches.push(file.file_id);
            }
            self.next_file();
        }
        Ok(Step::Continue)
    }

    /// A missing or wrong-length file: every chunk must be resent after repair.
    fn mark_file_damaged(&mut self) -> Step {
        let file = &self.files[self.file_position];
        for index in 0..chunk_count(file.size_bytes) {
            self.bad_chunks.push(ChunkRef {
                file_id: file.file_id,
                chunk_index: index as u32,
            });
        }
        self.damaged_files.push(file.file_id);
        self.verified_bytes += file.size_bytes;
        self.next_file();
        Step::Continue
    }

    fn next_file(&mut self) {
        self.current = None;
        self.file_position += 1;
        self.chunk_position = 0;
    }

    fn passed(&self) -> bool {
        self.bad_chunks.is_empty() && self.full_hash_mismatches.is_empty()
    }
}

/// Reads `transfer.json` beneath a tree directory.
fn read_identity(tree: &OwnedFd) -> std::io::Result<TransferIdentity> {
    let file = open_file_beneath(tree, "transfer.json", false)?;
    serde_json::from_reader(std::io::BufReader::new(file)).map_err(std::io::Error::other)
}

impl StorageWorker {
    /// Starts a verification pass over the staged tree. The pass advances between commands.
    pub(crate) fn start_verification(
        &mut self,
        id: &str,
        purpose: VerifyPurpose,
    ) -> Result<(), ApiError> {
        let relative = match purpose {
            VerifyPurpose::ConfirmPublished => format!("completed/{id}/data"),
            _ => format!("staging/{id}/tree/data"),
        };
        let root = open_dir(&self.paths.root)?;
        let data_dir = open_dir_beneath(&root, &relative)
            .map_err(|e| ApiError::storage("open tree for verification", &e))?;
        let files = metadata::files(&self.db, id)?;
        tracing::info!(
            session_id = id,
            ?purpose,
            files = files.len(),
            "verification started"
        );
        self.verification = Some(VerificationJob {
            session_id: id.to_string(),
            purpose,
            data_dir,
            files,
            file_position: 0,
            chunk_position: 0,
            current: None,
            full_hasher: Sha256::new(),
            buffer: Vec::new(),
            verified_bytes: 0,
            bad_chunks: Vec::new(),
            damaged_files: Vec::new(),
            full_hash_mismatches: Vec::new(),
            started: Instant::now(),
        });
        self.set_verified_bytes(id, 0);
        Ok(())
    }

    /// Runs one verification step; finishes the pass when every file has been read.
    pub(crate) fn advance_verification(&mut self) {
        let Some(job) = self.verification.as_mut() else {
            return;
        };
        if let Err(error) = faults::hit("during-verify") {
            self.stop_verification_with_error(ApiError::storage("verify", &error));
            return;
        }
        let step_started = Instant::now();
        let step = job.step();
        crate::progress::add_elapsed(&self.counters.verify_micros, step_started);
        match step {
            Ok(Step::Continue) => {
                let (id, bytes) = (job.session_id.clone(), job.verified_bytes);
                self.set_verified_bytes(&id, bytes);
            }
            Ok(Step::Done) => {
                let job = self.verification.take().expect("job present");
                let id = job.session_id.clone();
                if let Err(error) = self.finish_verification(job) {
                    tracing::error!(session_id = %id, %error, "verification could not finish");
                    self.record_error(&id, &error);
                }
            }
            Err(error) => {
                self.stop_verification_with_error(ApiError::storage("verify", &error));
            }
        }
    }

    /// A storage error stops the pass. The recorded state is preserved and its error exposed;
    /// a commit replay or a restart runs verification again (§8).
    fn stop_verification_with_error(&mut self, error: ApiError) {
        if let Some(job) = self.verification.take() {
            tracing::error!(session_id = %job.session_id, %error, "verification stopped");
            self.record_error(&job.session_id, &error);
        }
    }

    fn finish_verification(&mut self, job: VerificationJob) -> Result<(), ApiError> {
        let id = job.session_id.clone();
        self.set_verified_bytes(&id, job.verified_bytes);
        tracing::info!(
            session_id = %id,
            purpose = ?job.purpose,
            verified_bytes = job.verified_bytes,
            bad_chunks = job.bad_chunks.len(),
            millis = job.started.elapsed().as_millis() as u64,
            "verification finished"
        );
        match job.purpose {
            VerifyPurpose::Commit => {
                if !job.bad_chunks.is_empty() {
                    if !job.damaged_files.is_empty() {
                        self.build_staging_tree(&id)
                            .map_err(|e| ApiError::storage("repair staging tree", &e))?;
                    }
                    let cleared = metadata::clear_receipts(&mut self.db, &id, &job.bad_chunks)?;
                    let message = format!(
                        "verification found {} damaged chunks ({cleared} receipts cleared); \
                         resend them and commit again",
                        job.bad_chunks.len()
                    );
                    self.transition(
                        &id,
                        SessionState::Verifying,
                        SessionState::Receiving,
                        Some(&message),
                    )
                } else if !job.full_hash_mismatches.is_empty() {
                    let message = format!(
                        "every chunk matched but the full-file digest of file(s) {:?} did not; \
                         the manifest is inconsistent",
                        job.full_hash_mismatches
                    );
                    self.transition(
                        &id,
                        SessionState::Verifying,
                        SessionState::Faulted,
                        Some(&message),
                    )
                } else {
                    self.publish(&id)
                }
            }
            VerifyPurpose::ResumePublication => {
                if job.passed() {
                    self.complete_publication(&id)
                } else {
                    self.transition(
                        &id,
                        SessionState::Publishing,
                        SessionState::Faulted,
                        Some("staged content changed after verification; it was not published"),
                    )
                }
            }
            VerifyPurpose::ConfirmPublished => {
                if job.passed() {
                    self.sync_publication_parents(&id)?;
                    self.transition(&id, SessionState::Publishing, SessionState::Published, None)
                } else {
                    self.transition(
                        &id,
                        SessionState::Publishing,
                        SessionState::Faulted,
                        Some("published output does not match the manifest; storage fault"),
                    )
                }
            }
        }
    }

    /// Steps 1–3 of publication, then the rename.
    fn publish(&mut self, id: &str) -> Result<(), ApiError> {
        let started = Instant::now();
        let result = self.publish_steps(id);
        crate::progress::add_elapsed(&self.counters.publish_micros, started);
        result
    }

    fn publish_steps(&mut self, id: &str) -> Result<(), ApiError> {
        let row = metadata::session(&self.db, id)?.ok_or_else(ApiError::not_found)?;
        self.open = None;
        self.write_identity_and_sync_tree(&row)
            .map_err(|e| ApiError::storage("sync staged tree", &e))?;
        if !metadata::record_publication_intent(&mut self.db, id, &destination_name(id))? {
            return Err(ApiError::new(
                ErrorCode::Internal,
                "session left verifying before publication",
            ));
        }
        tracing::info!(session_id = id, "publication intent recorded");
        self.refresh(id);
        self.complete_publication(id)
    }

    fn write_identity_and_sync_tree(&mut self, row: &SessionRow) -> std::io::Result<()> {
        let root = open_dir(&self.paths.root)?;
        let tree = open_dir_beneath(&root, &format!("staging/{}/tree", row.id))?;
        let identity = TransferIdentity {
            session_id: row.id.clone(),
            manifest_sha256: row.manifest_sha256.clone(),
            protocol_version: PROTOCOL_VERSION,
        };
        let mut file = create_file_beneath(&tree, "transfer.json")?;
        file.set_len(0)?;
        file.write_all(&serde_json::to_vec_pretty(&identity).map_err(std::io::Error::other)?)?;
        file.sync_all()?;

        let data = open_dir_beneath(&tree, "data")?;
        for record in metadata::files(&self.db, &row.id).map_err(std::io::Error::other)? {
            open_file_beneath(&data, &record.path, false)?.sync_all()?;
        }
        let directories =
            metadata::directories(&self.db, &row.id).map_err(std::io::Error::other)?;
        for directory in parent_first(directories.iter().map(String::as_str))
            .into_iter()
            .rev()
        {
            sync_dir(open_dir_beneath(&data, directory)?)?;
        }
        sync_dir(&data)?;
        sync_dir(&tree)
    }

    /// Steps 4–6: no-replace rename, parent syncs, then `Published`.
    fn complete_publication(&mut self, id: &str) -> Result<(), ApiError> {
        self.open = None;
        faults::hit("before-publish-rename")?;
        let staging = open_dir(&self.paths.staging())?;
        let session_dir = open_dir_beneath(&staging, id)?;
        let completed = open_dir(&self.paths.completed())?;
        match rename_no_replace(&session_dir, "tree", &completed, id) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let message = format!(
                    "destination {} already exists and was not replaced",
                    destination_name(id)
                );
                tracing::error!(session_id = id, "{message}");
                return self.transition(
                    id,
                    SessionState::Publishing,
                    SessionState::Faulted,
                    Some(&message),
                );
            }
            Err(error) => return Err(ApiError::storage("publish rename", &error)),
        }
        faults::hit("after-publish-rename")?;
        self.sync_publication_parents(id)?;
        self.transition(id, SessionState::Publishing, SessionState::Published, None)?;
        faults::hit("after-published-commit")?;
        tracing::info!(session_id = id, destination = %destination_name(id), "published");
        Ok(())
    }

    fn sync_publication_parents(&self, id: &str) -> Result<(), ApiError> {
        let staging = open_dir(&self.paths.staging())?;
        let session_dir = open_dir_beneath(&staging, id)?;
        sync_dir(&session_dir).map_err(|e| ApiError::storage("sync staging parent", &e))?;
        let completed = open_dir(&self.paths.completed())?;
        sync_dir(&completed).map_err(|e| ApiError::storage("sync completed parent", &e))
    }

    // ----- Startup recovery (§7, §8) ----------------------------------------------------

    /// Brings every recorded session to a consistent state before the receiver accepts
    /// requests. A verification pass that must run (Verifying or an interrupted publication)
    /// is queued and advances once the worker loop starts; uploads stay refused meanwhile.
    pub fn recover(&mut self) -> Result<RecoveryReport, String> {
        let started = Instant::now();
        let mut report = RecoveryReport::default();
        let sessions = metadata::all_sessions(&self.db).map_err(|e| e.to_string())?;
        for session in sessions {
            report.sessions_examined += 1;
            let id = session.id.clone();
            let result = match session.state {
                SessionState::Preparing => self.recover_preparing(&session),
                SessionState::Receiving => self.recover_receiving(&id, &mut report),
                SessionState::Verifying => self
                    .build_staging_tree(&id)
                    .map_err(|e| ApiError::storage("repair staging tree", &e))
                    .and_then(|()| self.start_verification(&id, VerifyPurpose::Commit)),
                SessionState::Publishing => self.recover_publishing(&session),
                SessionState::Published => {
                    self.check_published(&session);
                    Ok(())
                }
                SessionState::Aborted | SessionState::Faulted => Ok(()),
            };
            if let Err(error) = result {
                tracing::error!(session_id = %id, %error, "recovery could not finish this session");
                self.record_error(&id, &error);
            }
            self.refresh(&id);
        }
        report.duration_millis = started.elapsed().as_millis() as u64;
        tracing::info!(?report, "recovery finished");
        Ok(report)
    }

    /// Reconstructs an interrupted Preparing session from its durable manifest.
    fn recover_preparing(&mut self, session: &SessionRow) -> Result<(), ApiError> {
        let staging = open_dir(&self.paths.staging())?;
        let session_dir = open_dir_beneath(&staging, &session.id)?;
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(
            &mut open_file_beneath(&session_dir, "manifest.json", false)?,
            &mut bytes,
        )?;
        if sha256_hex(&bytes) != session.manifest_sha256 {
            return self.transition(
                &session.id,
                SessionState::Preparing,
                SessionState::Faulted,
                Some("stored manifest does not match its recorded digest"),
            );
        }
        self.finish_preparation(&session.id)
    }

    /// Repairs missing staging entries, then rechecks every chunk marked durable and clears
    /// receipts whose bytes no longer match.
    fn recover_receiving(&mut self, id: &str, report: &mut RecoveryReport) -> Result<(), ApiError> {
        self.build_staging_tree(id)
            .map_err(|e| ApiError::storage("repair staging tree", &e))?;
        let durable = metadata::durable_chunks(&self.db, id)?;
        let files = metadata::files(&self.db, id)?;
        let root = open_dir(&self.paths.root)?;
        let data = open_dir_beneath(&root, &format!("staging/{id}/tree/data"))?;
        let mut bad = Vec::new();
        let mut buffer = Vec::new();
        let mut handle: Option<(u32, File)> = None;
        for chunk in durable {
            let file = &files[chunk.file_id as usize];
            if handle.as_ref().map(|(fid, _)| *fid) != Some(chunk.file_id) {
                handle = Some((chunk.file_id, open_file_beneath(&data, &file.path, false)?));
            }
            let length = chunk_length(file.size_bytes, chunk.chunk_index).unwrap_or(0);
            buffer.resize(length as usize, 0);
            let (_, staged) = handle.as_ref().expect("opened above");
            let matches = staged
                .read_exact_at(&mut buffer, chunk_offset(chunk.chunk_index))
                .is_ok()
                && sha256_hex(&buffer) == file.chunk_sha256[chunk.chunk_index as usize];
            report.chunks_rehashed += 1;
            report.bytes_rehashed += length;
            if !matches {
                bad.push(chunk);
            }
        }
        if !bad.is_empty() {
            let cleared = metadata::clear_receipts(&mut self.db, id, &bad)?;
            report.receipts_cleared += cleared;
            tracing::warn!(
                session_id = id,
                cleared,
                "recovery cleared damaged receipts"
            );
        }
        Ok(())
    }

    /// Reconciles the publication intent with the filesystem.
    fn recover_publishing(&mut self, session: &SessionRow) -> Result<(), ApiError> {
        let id = &session.id;
        let completed = open_dir(&self.paths.completed())?;
        match open_dir_beneath(&completed, id) {
            Ok(destination) => {
                let expected = TransferIdentity {
                    session_id: id.clone(),
                    manifest_sha256: session.manifest_sha256.clone(),
                    protocol_version: PROTOCOL_VERSION,
                };
                match read_identity(&destination) {
                    Ok(found) if found == expected => {
                        self.start_verification(id, VerifyPurpose::ConfirmPublished)
                    }
                    _ => self.transition(
                        id,
                        SessionState::Publishing,
                        SessionState::Faulted,
                        Some("an unexpected directory occupies the destination; not replaced"),
                    ),
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let staging = open_dir(&self.paths.staging())?;
                if open_dir_beneath(&staging, &format!("{id}/tree")).is_ok() {
                    self.start_verification(id, VerifyPurpose::ResumePublication)
                } else {
                    self.transition(
                        id,
                        SessionState::Publishing,
                        SessionState::Faulted,
                        Some("neither the staged tree nor the destination exists"),
                    )
                }
            }
            Err(error) => Err(ApiError::storage("open destination", &error)),
        }
    }

    /// Reports a Published session whose output is missing or has the wrong identity. The
    /// check is cheap (identity and sizes); full content is verified only at publication.
    fn check_published(&mut self, session: &SessionRow) {
        let problem = (|| -> Result<Option<String>, String> {
            let completed = open_dir(&self.paths.completed()).map_err(|e| e.to_string())?;
            let destination = match open_dir_beneath(&completed, &session.id) {
                Ok(fd) => fd,
                Err(_) => return Ok(Some("published output is missing".into())),
            };
            match read_identity(&destination) {
                Ok(identity) if identity.manifest_sha256 == session.manifest_sha256 => {}
                _ => return Ok(Some("published output has the wrong identity".into())),
            }
            let data = open_dir_beneath(&destination, "data").map_err(|e| e.to_string())?;
            for file in metadata::files(&self.db, &session.id).map_err(|e| e.to_string())? {
                match open_file_beneath(&data, &file.path, false) {
                    Ok(handle)
                        if handle.metadata().map(|m| m.len()).ok() == Some(file.size_bytes) => {}
                    _ => {
                        return Ok(Some(format!(
                            "published file {} is missing or resized",
                            file.file_id
                        )));
                    }
                }
            }
            Ok(None)
        })();
        let message = match problem {
            Ok(None) => return,
            Ok(Some(message)) => format!("storage fault: {message}"),
            Err(error) => format!("storage fault: {error}"),
        };
        tracing::error!(session_id = %session.id, "{message}");
        self.record_error(
            &session.id,
            &ApiError::new(ErrorCode::StorageUnavailable, message),
        );
    }
}
