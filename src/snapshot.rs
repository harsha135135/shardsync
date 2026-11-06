//! The sender's immutable spool (§4).
//!
//! `shardsync prepare` copies a quiescent file or folder into a private spool, flushes the
//! copy, hashes the *copied* bytes, writes the manifest and freezes the spool. Every upload
//! then reads the spool, so changes to the source after preparation cannot change what is
//! transferred. Copying a live, changing directory cannot produce a coherent snapshot; a
//! size or modification-time change during the copy is detected and refused, but that check
//! is a hint, not a proof of consistency.
//!
//! Spool layout:
//!
//! ```text
//! spool/
//!   content/            copied files (read-only) and directories
//!   manifest.json       exact manifest bytes
//!   spool.json          written last; its presence marks a complete, frozen spool
//!   sender-state.json   idempotency key, endpoint and session ID (written by `send`)
//!   upload-log.jsonl    one line per chunk upload attempt (written by `send`)
//! ```

use std::ffi::CStr;
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::fs::{FileExt, FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime};

use rustix::fs::{Mode, OFlags};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{
    CHUNK_SIZE_BYTES, MAX_DIRECTORIES, MAX_FILES, MAX_PATH_DEPTH, PROTOCOL_VERSION, chunk_length,
    chunk_offset,
};
use crate::error::AppError;
use crate::filesystem::{
    create_new_file_beneath, ensure_dir_beneath, open_dir, open_file_beneath, rename_no_replace,
    sync_dir,
};
use crate::manifest::{
    FileEntry, Manifest, ValidatedManifest, parse_and_validate, sha256_hex, validate_relative_path,
};

pub const CONTENT_DIR: &str = "content";
const PARTIAL_CONTENT_DIR: &str = ".content.partial";
pub const MANIFEST_FILE: &str = "manifest.json";
pub const SPOOL_FILE: &str = "spool.json";
pub const STATE_FILE: &str = "sender-state.json";
pub const UPLOAD_LOG_FILE: &str = "upload-log.jsonl";

/// Contents of `spool.json`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpoolRecord {
    pub spool_format: u32,
    pub state: String,
    pub manifest_sha256: String,
    pub total_bytes: u64,
    pub files: usize,
    pub directories: usize,
}

/// Printed by `shardsync prepare`.
#[derive(Clone, Debug, Serialize)]
pub struct PrepareReport {
    pub spool: PathBuf,
    pub manifest_sha256: String,
    pub total_bytes: u64,
    pub total_chunks: u64,
    pub files: usize,
    pub directories: usize,
    pub copy_millis: u64,
    pub hash_millis: u64,
}

/// One entry found while walking the source.
enum Found {
    Directory(String),
    File {
        relative_path: String,
        size_bytes: u64,
    },
}

fn open_entry(dir: impl AsFd, name: &CStr) -> std::io::Result<File> {
    // NOFOLLOW refuses a symlink; NONBLOCK keeps a FIFO from blocking the open.
    let fd = rustix::fs::openat(
        dir,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    Ok(File::from(fd))
}

fn describe_open_error(path: &str, error: std::io::Error) -> AppError {
    let symlink = [
        rustix::io::Errno::LOOP.raw_os_error(),
        rustix::io::Errno::MLINK.raw_os_error(),
    ];
    match error.raw_os_error() {
        Some(code) if symlink.contains(&code) => AppError::invalid(format!(
            "{path:?} is a symbolic link; symbolic links are not transferred"
        )),
        Some(code) if code == rustix::io::Errno::NXIO.raw_os_error() => AppError::invalid(format!(
            "{path:?} is a socket; only regular files and directories are transferred"
        )),
        _ => AppError::environment(format!("open {path:?}: {error}")),
    }
}

fn reject_special(path: &str, file_type: std::fs::FileType) -> Result<(), AppError> {
    let kind = if file_type.is_fifo() {
        "a FIFO"
    } else if file_type.is_socket() {
        "a socket"
    } else if file_type.is_block_device() || file_type.is_char_device() {
        "a device file"
    } else {
        "an unsupported file type"
    };
    Err(AppError::invalid(format!(
        "{path:?} is {kind}; only regular files and directories are transferred"
    )))
}

fn join(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    }
}

/// Copies an open source file into the spool and checks that it did not change meanwhile.
fn copy_file(
    source: &File,
    path: &str,
    destination_dir: &OwnedFd,
    name: &str,
) -> Result<u64, AppError> {
    let before = source
        .metadata()
        .map_err(|e| AppError::environment(format!("stat {path:?}: {e}")))?;
    let mut target = create_new_file_beneath(destination_dir, name, 0o600)
        .map_err(|e| AppError::environment(format!("create spool copy of {path:?}: {e}")))?;
    let mut reader = source;
    let copied = std::io::copy(&mut reader, &mut target)
        .map_err(|e| AppError::environment(format!("copy {path:?}: {e}")))?;
    target
        .sync_all()
        .map_err(|e| AppError::environment(format!("flush spool copy of {path:?}: {e}")))?;
    let after = source
        .metadata()
        .map_err(|e| AppError::environment(format!("stat {path:?}: {e}")))?;
    let unchanged = copied == before.len()
        && after.len() == before.len()
        && after.modified().ok() == before.modified().ok();
    if !unchanged {
        return Err(AppError::invalid(format!(
            "{path:?} changed while it was being copied; supply quiescent input"
        )));
    }
    Ok(copied)
}

/// Walks `source_dir` without following symlinks, copying into `destination_dir`. Entries are
/// visited in byte order of their names so file IDs are deterministic.
fn copy_tree(
    source_dir: &OwnedFd,
    destination_dir: &OwnedFd,
    prefix: &str,
    depth: usize,
    found: &mut Vec<Found>,
    created_dirs: &mut Vec<String>,
) -> Result<(), AppError> {
    let mut names: Vec<std::ffi::CString> = Vec::new();
    let dir = rustix::fs::Dir::read_from(source_dir)
        .map_err(|e| AppError::environment(format!("list {prefix:?}: {e}")))?;
    for entry in dir {
        let entry = entry.map_err(|e| AppError::environment(format!("list {prefix:?}: {e}")))?;
        let name = entry.file_name();
        if name.to_bytes() == b"." || name.to_bytes() == b".." {
            continue;
        }
        names.push(name.to_owned());
    }
    names.sort();
    for name in names {
        let text = name
            .to_str()
            .map_err(|_| AppError::invalid(format!("a name in {prefix:?} is not valid UTF-8")))?;
        let path = join(prefix, text);
        validate_relative_path(&path)
            .map_err(|reason| AppError::invalid(format!("{path:?}: {reason}")))?;
        let entry = open_entry(source_dir, &name).map_err(|e| describe_open_error(&path, e))?;
        let file_type = entry
            .metadata()
            .map_err(|e| AppError::environment(format!("stat {path:?}: {e}")))?
            .file_type();
        if file_type.is_dir() {
            if depth + 1 > MAX_PATH_DEPTH {
                return Err(AppError::invalid(format!(
                    "{path:?} is deeper than {MAX_PATH_DEPTH} levels"
                )));
            }
            found.push(Found::Directory(path.clone()));
            if found
                .iter()
                .filter(|f| matches!(f, Found::Directory(_)))
                .count()
                > MAX_DIRECTORIES
            {
                return Err(AppError::invalid(format!(
                    "more than {MAX_DIRECTORIES} directories"
                )));
            }
            let child_destination = ensure_dir_beneath(destination_dir, text).map_err(|e| {
                AppError::environment(format!("create spool directory {path:?}: {e}"))
            })?;
            created_dirs.push(path.clone());
            let child_source = OwnedFd::from(entry);
            copy_tree(
                &child_source,
                &child_destination,
                &path,
                depth + 1,
                found,
                created_dirs,
            )?;
        } else if file_type.is_file() {
            let size_bytes = copy_file(&entry, &path, destination_dir, text)?;
            found.push(Found::File {
                relative_path: path,
                size_bytes,
            });
            if found
                .iter()
                .filter(|f| matches!(f, Found::File { .. }))
                .count()
                > MAX_FILES
            {
                return Err(AppError::invalid(format!("more than {MAX_FILES} files")));
            }
        } else {
            reject_special(&path, file_type)?;
        }
    }
    Ok(())
}

/// Chunk digests and the full-file digest of a spool file, from one read pass.
fn hash_file(
    content: &OwnedFd,
    relative_path: &str,
    size_bytes: u64,
) -> std::io::Result<(String, Vec<String>)> {
    let file = open_file_beneath(content, relative_path, false)?;
    if file.metadata()?.len() != size_bytes {
        return Err(std::io::Error::other(format!(
            "{relative_path:?} changed size in the spool"
        )));
    }
    let mut full = Sha256::new();
    let mut chunks = Vec::new();
    let mut buffer = vec![0u8; CHUNK_SIZE_BYTES as usize];
    let mut index = 0u32;
    while let Some(length) = chunk_length(size_bytes, index) {
        let slice = &mut buffer[..length as usize];
        file.read_exact_at(slice, chunk_offset(index))?;
        full.update(&*slice);
        chunks.push(sha256_hex(slice));
        index += 1;
    }
    Ok((hex::encode(full.finalize()), chunks))
}

fn env(context: &str) -> impl Fn(std::io::Error) -> AppError + '_ {
    move |error| AppError::environment(format!("{context}: {error}"))
}

/// Prepares a frozen spool from `source` (a regular file or a directory).
pub fn prepare(source: &Path, spool: &Path) -> Result<PrepareReport, AppError> {
    let source_type = std::fs::symlink_metadata(source)
        .map_err(|e| AppError::invalid(format!("source {}: {e}", source.display())))?
        .file_type();
    if source_type.is_symlink() {
        return Err(AppError::invalid(
            "the source is a symbolic link; give the real path",
        ));
    }
    if !source_type.is_file() && !source_type.is_dir() {
        return Err(AppError::invalid(
            "the source must be a regular file or a directory",
        ));
    }
    match std::fs::symlink_metadata(spool) {
        Ok(_) => {
            return Err(AppError::invalid(format!(
                "spool {} already exists; prepare into a new directory",
                spool.display()
            )));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(AppError::environment(format!(
                "spool {}: {error}",
                spool.display()
            )));
        }
    }
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(spool)
            .map_err(env("create spool"))?;
    }
    let spool_fd = open_dir(spool).map_err(env("open spool"))?;

    // 1. Copy into a temporary spool directory.
    let copy_started = Instant::now();
    let partial =
        ensure_dir_beneath(&spool_fd, PARTIAL_CONTENT_DIR).map_err(env("create spool content"))?;
    let mut found = Vec::new();
    let mut created_dirs = Vec::new();
    if source_type.is_file() {
        let name = source
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| AppError::invalid("the source file name must be valid UTF-8"))?
            .to_string();
        validate_relative_path(&name)
            .map_err(|reason| AppError::invalid(format!("{name:?}: {reason}")))?;
        let parent = source
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent_fd = open_dir(parent).map_err(env("open source directory"))?;
        let c_name =
            std::ffi::CString::new(name.clone()).map_err(|_| AppError::invalid("NUL in name"))?;
        let entry = open_entry(&parent_fd, &c_name).map_err(|e| describe_open_error(&name, e))?;
        let file_type = entry.metadata().map_err(env("stat source"))?.file_type();
        if !file_type.is_file() {
            reject_special(&name, file_type)?;
        }
        let size_bytes = copy_file(&entry, &name, &partial, &name)?;
        found.push(Found::File {
            relative_path: name,
            size_bytes,
        });
    } else {
        let root = open_dir(source).map_err(env("open source directory"))?;
        copy_tree(&root, &partial, "", 0, &mut found, &mut created_dirs)?;
    }
    // 2. Flush the copied tree: directories from children to parents, then the spool entry.
    for directory in created_dirs.iter().rev() {
        let fd = crate::filesystem::open_dir_beneath(&partial, directory)
            .map_err(env("open spool directory"))?;
        sync_dir(fd).map_err(env("flush spool directory"))?;
    }
    sync_dir(&partial).map_err(env("flush spool content"))?;
    rename_no_replace(&spool_fd, PARTIAL_CONTENT_DIR, &spool_fd, CONTENT_DIR)
        .map_err(env("finish spool content"))?;
    sync_dir(&spool_fd).map_err(env("flush spool"))?;
    let copy_millis = copy_started.elapsed().as_millis() as u64;

    // 3. Hash the copied bytes, never the source.
    let hash_started = Instant::now();
    let content = crate::filesystem::open_dir_beneath(&spool_fd, CONTENT_DIR)
        .map_err(env("open spool content"))?;
    let mut directories = Vec::new();
    let mut files = Vec::new();
    for entry in found {
        match entry {
            Found::Directory(path) => directories.push(path),
            Found::File {
                relative_path,
                size_bytes,
            } => {
                let (sha256, chunk_sha256) = hash_file(&content, &relative_path, size_bytes)
                    .map_err(env("hash spool file"))?;
                let id =
                    u32::try_from(files.len()).map_err(|_| AppError::invalid("too many files"))?;
                files.push(FileEntry {
                    id,
                    relative_path,
                    size_bytes,
                    sha256,
                    chunk_sha256,
                });
            }
        }
    }
    let manifest = Manifest {
        protocol_version: PROTOCOL_VERSION,
        chunk_size_bytes: CHUNK_SIZE_BYTES,
        directories,
        files,
    };
    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).map_err(|e| AppError::environment(e.to_string()))?;
    // The sender applies the receiver's validation before anything is uploaded.
    let validated = parse_and_validate(&manifest_bytes)
        .map_err(|e| AppError::invalid(format!("manifest: {e}")))?;
    let hash_millis = hash_started.elapsed().as_millis() as u64;

    // 4. Save and flush the manifest, freeze the content, then mark the spool complete.
    let mut manifest_file =
        create_new_file_beneath(&spool_fd, MANIFEST_FILE, 0o400).map_err(env("write manifest"))?;
    manifest_file
        .write_all(&manifest_bytes)
        .map_err(env("write manifest"))?;
    manifest_file.sync_all().map_err(env("flush manifest"))?;
    for file in &validated.manifest.files {
        let handle =
            open_file_beneath(&content, &file.relative_path, false).map_err(env("freeze spool"))?;
        handle
            .set_permissions(std::fs::Permissions::from_mode(0o400))
            .map_err(env("freeze spool"))?;
    }
    let record = SpoolRecord {
        spool_format: 1,
        state: "frozen".into(),
        manifest_sha256: validated.digest.clone(),
        total_bytes: validated.total_bytes,
        files: validated.manifest.files.len(),
        directories: validated.manifest.directories.len(),
    };
    let mut record_file =
        create_new_file_beneath(&spool_fd, SPOOL_FILE, 0o600).map_err(env("write spool record"))?;
    record_file
        .write_all(
            &serde_json::to_vec_pretty(&record)
                .map_err(|e| AppError::environment(e.to_string()))?,
        )
        .map_err(env("write spool record"))?;
    record_file.sync_all().map_err(env("flush spool record"))?;
    sync_dir(&spool_fd).map_err(env("flush spool"))?;

    Ok(PrepareReport {
        spool: spool.to_path_buf(),
        manifest_sha256: validated.digest,
        total_bytes: validated.total_bytes,
        total_chunks: validated.total_chunks,
        files: record.files,
        directories: record.directories,
        copy_millis,
        hash_millis,
    })
}

/// A frozen spool opened for sending.
pub struct LoadedSpool {
    pub root: PathBuf,
    content: OwnedFd,
    pub manifest_bytes: Vec<u8>,
    pub validated: ValidatedManifest,
}

/// Opens a complete spool and checks that its manifest still matches the recorded digest.
pub fn load(spool: &Path) -> Result<LoadedSpool, AppError> {
    let missing = || {
        AppError::invalid(format!(
            "{} is not a complete spool (run `shardsync prepare` first)",
            spool.display()
        ))
    };
    let record_bytes = std::fs::read(spool.join(SPOOL_FILE)).map_err(|_| missing())?;
    let record: SpoolRecord = serde_json::from_slice(&record_bytes).map_err(|_| missing())?;
    if record.state != "frozen" || record.spool_format != 1 {
        return Err(missing());
    }
    let mut manifest_bytes = Vec::new();
    File::open(spool.join(MANIFEST_FILE))
        .and_then(|mut f| f.read_to_end(&mut manifest_bytes))
        .map_err(|_| missing())?;
    let validated = parse_and_validate(&manifest_bytes)
        .map_err(|e| AppError::invalid(format!("spool manifest: {e}")))?;
    if validated.digest != record.manifest_sha256 {
        return Err(AppError::invalid(
            "spool manifest does not match its recorded digest",
        ));
    }
    let spool_fd = open_dir(spool).map_err(|_| missing())?;
    let content =
        crate::filesystem::open_dir_beneath(&spool_fd, CONTENT_DIR).map_err(|_| missing())?;
    Ok(LoadedSpool {
        root: spool.to_path_buf(),
        content,
        manifest_bytes,
        validated,
    })
}

impl LoadedSpool {
    /// Reads one chunk from the spool and checks it against the manifest before upload.
    pub fn read_chunk(&self, file_id: u32, chunk_index: u32) -> Result<Vec<u8>, AppError> {
        let file = self
            .validated
            .manifest
            .files
            .get(file_id as usize)
            .ok_or_else(|| {
                AppError::rejected(format!("receiver asked for unknown file {file_id}"))
            })?;
        let length = chunk_length(file.size_bytes, chunk_index).ok_or_else(|| {
            AppError::rejected(format!(
                "receiver asked for unknown chunk {file_id}/{chunk_index}"
            ))
        })?;
        let handle = open_file_beneath(&self.content, &file.relative_path, false)
            .map_err(|e| AppError::invalid(format!("spool file {:?}: {e}", file.relative_path)))?;
        let mut buffer = vec![0u8; length as usize];
        handle
            .read_exact_at(&mut buffer, chunk_offset(chunk_index))
            .map_err(|e| {
                AppError::invalid(format!("read spool file {:?}: {e}", file.relative_path))
            })?;
        if sha256_hex(&buffer) != file.chunk_sha256[chunk_index as usize] {
            return Err(AppError::invalid(format!(
                "spool file {:?} was modified after preparation; prepare a new spool",
                file.relative_path
            )));
        }
        Ok(buffer)
    }
}

/// What the sender must remember to resume after a crash or a lost response (§6).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SenderState {
    pub endpoint: String,
    pub idempotency_key: String,
    pub manifest_sha256: String,
    pub session_id: Option<String>,
}

pub fn load_state(spool: &Path) -> Result<Option<SenderState>, AppError> {
    match std::fs::read(spool.join(STATE_FILE)) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| AppError::invalid(format!("sender state: {e}"))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(AppError::environment(format!("read sender state: {error}"))),
    }
}

/// Replaces `sender-state.json` atomically: write a temporary file, sync, rename, sync.
pub fn save_state(spool: &Path, state: &SenderState) -> Result<(), AppError> {
    let temporary = spool.join(format!("{STATE_FILE}.tmp"));
    let bytes =
        serde_json::to_vec_pretty(state).map_err(|e| AppError::environment(e.to_string()))?;
    crate::devcert::write_private(&temporary, &bytes).map_err(env("write sender state"))?;
    std::fs::rename(&temporary, spool.join(STATE_FILE)).map_err(env("replace sender state"))?;
    crate::filesystem::sync_dir_path(spool).map_err(env("flush sender state"))
}

/// One line of `upload-log.jsonl`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UploadAttempt {
    pub run: String,
    /// `started` before each send; `not_connected` cancels a start that sent nothing.
    pub event: String,
    pub file_id: u32,
    pub chunk_index: u32,
    pub bytes: u64,
    pub unix_millis: u128,
}

pub fn now_unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Chunk bytes that may have reached the receiver, across every run on this spool: started
/// attempts minus those that never connected.
pub fn logged_attempt_bytes(spool: &Path) -> u64 {
    let Ok(text) = std::fs::read_to_string(spool.join(UPLOAD_LOG_FILE)) else {
        return 0;
    };
    let mut total: i128 = 0;
    for attempt in text
        .lines()
        .filter_map(|line| serde_json::from_str::<UploadAttempt>(line).ok())
    {
        match attempt.event.as_str() {
            "started" => total += i128::from(attempt.bytes),
            "not_connected" => total -= i128::from(attempt.bytes),
            _ => {}
        }
    }
    u64::try_from(total.max(0)).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepares_a_tree_with_empty_directories_and_zero_byte_files() {
        let source = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(source.path().join("a/b")).unwrap();
        std::fs::create_dir(source.path().join("empty")).unwrap();
        std::fs::write(source.path().join("a/b/zero"), b"").unwrap();
        std::fs::write(
            source.path().join("top"),
            vec![7u8; CHUNK_SIZE_BYTES as usize + 3],
        )
        .unwrap();
        let spool_parent = tempfile::tempdir().unwrap();
        let spool = spool_parent.path().join("spool");
        let report = prepare(source.path(), &spool).unwrap();
        assert_eq!(report.files, 2);
        assert_eq!(report.directories, 3);
        let loaded = load(&spool).unwrap();
        let manifest = &loaded.validated.manifest;
        assert_eq!(manifest.directories, vec!["a", "a/b", "empty"]);
        assert_eq!(manifest.files[0].relative_path, "a/b/zero");
        assert_eq!(manifest.files[0].chunk_sha256.len(), 0);
        assert_eq!(manifest.files[1].chunk_sha256.len(), 2);
        assert_eq!(loaded.read_chunk(1, 1).unwrap(), vec![7u8; 3]);
        assert!(
            prepare(source.path(), &spool).is_err(),
            "existing spool must be refused"
        );
    }

    #[test]
    fn refuses_symlinks_and_fifos() {
        let source = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("real"), b"x").unwrap();
        std::os::unix::fs::symlink("real", source.path().join("link")).unwrap();
        let spool_parent = tempfile::tempdir().unwrap();
        let error = prepare(source.path(), &spool_parent.path().join("s1"))
            .err()
            .unwrap();
        assert!(error.message.contains("symbolic link"), "{}", error.message);

        let fifo_source = tempfile::tempdir().unwrap();
        let status = std::process::Command::new("mkfifo")
            .arg(fifo_source.path().join("pipe"))
            .status()
            .unwrap();
        assert!(status.success());
        let error = prepare(fifo_source.path(), &spool_parent.path().join("s2"))
            .err()
            .unwrap();
        assert!(error.message.contains("FIFO"), "{}", error.message);
    }

    #[test]
    fn spool_reads_detect_modification() {
        let source = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("f"), b"original").unwrap();
        let spool_parent = tempfile::tempdir().unwrap();
        let spool = spool_parent.path().join("spool");
        prepare(&source.path().join("f"), &spool).unwrap();
        let path = spool.join("content/f");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(&path, b"tampered").unwrap();
        let loaded = load(&spool).unwrap();
        assert!(
            loaded
                .read_chunk(0, 0)
                .unwrap_err()
                .message
                .contains("modified")
        );
    }
}
