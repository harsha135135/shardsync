//! Manifest types, validation and identity (§5).
//!
//! A manifest is a versioned UTF-8 JSON document listing the directories and files of one
//! transfer. Its identity is the SHA-256 of the exact uploaded bytes, so the receiver stores
//! those bytes unchanged and never reserializes them.
//!
//! Validation runs in a fixed order: size, duplicate keys, typed parsing (unknown fields are
//! rejected), protocol constants, admission caps, then structure (paths, IDs, digests,
//! chunk counts, parents). The sender runs the same validation before uploading.

use std::collections::{BTreeSet, HashSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::{
    CHUNK_SIZE_BYTES, MAX_CHUNKS, MAX_COMPONENT_BYTES, MAX_DIRECTORIES, MAX_FILES,
    MAX_MANIFEST_BYTES, MAX_PATH_BYTES, MAX_PATH_DEPTH, MAX_TOTAL_BYTES, PROTOCOL_VERSION,
    chunk_count,
};
use crate::json_checks::reject_duplicate_keys;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub protocol_version: u32,
    pub chunk_size_bytes: u64,
    /// Every directory beneath the published `data` root, including empty ones. The root
    /// itself is implicit and never listed.
    pub directories: Vec<String>,
    /// Files ordered by their sequential IDs, starting at zero.
    pub files: Vec<FileEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileEntry {
    pub id: u32,
    pub relative_path: String,
    pub size_bytes: u64,
    /// Lowercase hex SHA-256 of the complete file.
    pub sha256: String,
    /// Lowercase hex SHA-256 of each chunk, in order. Offsets are derived, never sent.
    pub chunk_sha256: Vec<String>,
}

/// A manifest that passed every check, with its identity and totals.
#[derive(Clone, Debug)]
pub struct ValidatedManifest {
    pub manifest: Manifest,
    /// Lowercase hex SHA-256 of the exact document bytes.
    pub digest: String,
    pub total_bytes: u64,
    pub total_chunks: u64,
}

/// Why a manifest was refused. `Quota` maps to HTTP 413; everything else to 400.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ManifestError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Quota(String),
}

fn invalid(message: impl Into<String>) -> ManifestError {
    ManifestError::Invalid(message.into())
}

fn quota(message: impl Into<String>) -> ManifestError {
    ManifestError::Quota(message.into())
}

/// Lowercase hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Parses and validates exact manifest bytes.
pub fn parse_and_validate(bytes: &[u8]) -> Result<ValidatedManifest, ManifestError> {
    if bytes.len() > MAX_MANIFEST_BYTES {
        return Err(quota(format!(
            "manifest is {} bytes; the limit is {MAX_MANIFEST_BYTES}",
            bytes.len()
        )));
    }
    reject_duplicate_keys(bytes).map_err(|error| invalid(format!("manifest JSON: {error}")))?;
    let manifest: Manifest =
        serde_json::from_slice(bytes).map_err(|error| invalid(format!("manifest: {error}")))?;
    let (total_bytes, total_chunks) = validate_manifest(&manifest)?;
    Ok(ValidatedManifest {
        manifest,
        digest: sha256_hex(bytes),
        total_bytes,
        total_chunks,
    })
}

/// Checks protocol constants, caps and structure. Returns total bytes and total chunks.
pub fn validate_manifest(manifest: &Manifest) -> Result<(u64, u64), ManifestError> {
    if manifest.protocol_version != PROTOCOL_VERSION {
        return Err(invalid(format!(
            "unsupported protocol_version {}; this receiver supports {PROTOCOL_VERSION}",
            manifest.protocol_version
        )));
    }
    if manifest.chunk_size_bytes != CHUNK_SIZE_BYTES {
        return Err(invalid(format!(
            "chunk_size_bytes must be {CHUNK_SIZE_BYTES} in protocol version {PROTOCOL_VERSION}"
        )));
    }
    if manifest.files.len() > MAX_FILES {
        return Err(quota(format!(
            "{} files exceed the limit of {MAX_FILES}",
            manifest.files.len()
        )));
    }
    if manifest.directories.len() > MAX_DIRECTORIES {
        return Err(quota(format!(
            "{} directories exceed the limit of {MAX_DIRECTORIES}",
            manifest.directories.len()
        )));
    }

    // Every listed path, so duplicates and file/directory conflicts are caught in one place.
    let mut all_paths: HashSet<&str> = HashSet::new();
    let mut directory_paths: HashSet<&str> = HashSet::new();
    for directory in &manifest.directories {
        validate_relative_path(directory)
            .map_err(|reason| invalid(format!("directory {directory:?}: {reason}")))?;
        if !all_paths.insert(directory.as_str()) {
            return Err(invalid(format!("duplicate path {directory:?}")));
        }
        directory_paths.insert(directory.as_str());
    }

    let mut total_bytes: u64 = 0;
    let mut total_chunks: u64 = 0;
    for (position, file) in manifest.files.iter().enumerate() {
        let expected_id = u32::try_from(position)
            .map_err(|_| quota("file count does not fit a 32-bit file ID"))?;
        if file.id != expected_id {
            return Err(invalid(format!(
                "file IDs must be sequential from 0; position {position} has ID {}",
                file.id
            )));
        }
        validate_relative_path(&file.relative_path)
            .map_err(|reason| invalid(format!("file {:?}: {reason}", file.relative_path)))?;
        if !all_paths.insert(file.relative_path.as_str()) {
            return Err(invalid(format!(
                "duplicate path {:?} (a file and a directory, or two files)",
                file.relative_path
            )));
        }
        if !is_sha256_hex(&file.sha256) {
            return Err(invalid(format!(
                "file {}: sha256 must be 64 lowercase hex characters",
                file.id
            )));
        }
        let expected_chunks = chunk_count(file.size_bytes);
        if file.chunk_sha256.len() as u64 != expected_chunks {
            return Err(invalid(format!(
                "file {}: {} bytes need {expected_chunks} chunk digests, found {}",
                file.id,
                file.size_bytes,
                file.chunk_sha256.len()
            )));
        }
        if let Some(bad) = file.chunk_sha256.iter().position(|d| !is_sha256_hex(d)) {
            return Err(invalid(format!(
                "file {}: chunk digest {bad} must be 64 lowercase hex characters",
                file.id
            )));
        }
        total_bytes = total_bytes
            .checked_add(file.size_bytes)
            .ok_or_else(|| quota("total size overflows"))?;
        total_chunks = total_chunks
            .checked_add(expected_chunks)
            .ok_or_else(|| quota("total chunk count overflows"))?;
        if total_bytes > MAX_TOTAL_BYTES {
            return Err(quota(format!(
                "declared content exceeds the limit of {MAX_TOTAL_BYTES} bytes"
            )));
        }
        if total_chunks > MAX_CHUNKS {
            return Err(quota(format!(
                "declared content exceeds the limit of {MAX_CHUNKS} chunks"
            )));
        }
        // A chunk index must fit u32 for the wire format.
        if expected_chunks > u64::from(u32::MAX) {
            return Err(quota(format!("file {} has too many chunks", file.id)));
        }
    }

    // Every non-root parent must be listed, and only as a directory. A file used as a parent
    // is a conflict: its parent path would then be a file path, not a directory path.
    let entries = manifest
        .directories
        .iter()
        .map(String::as_str)
        .chain(manifest.files.iter().map(|f| f.relative_path.as_str()));
    for path in entries {
        if let Some((parent, _)) = path.rsplit_once('/')
            && !directory_paths.contains(parent)
        {
            if all_paths.contains(parent) {
                return Err(invalid(format!(
                    "{path:?} is beneath {parent:?}, which is a file"
                )));
            }
            return Err(invalid(format!(
                "parent directory {parent:?} of {path:?} is not listed"
            )));
        }
    }
    Ok((total_bytes, total_chunks))
}

fn is_sha256_hex(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Validates a path beneath the published `data` root and returns its components.
///
/// Rejects absolute paths, `.` and `..`, empty components, NUL, backslashes, drive-letter
/// prefixes, over-long components or paths, and excessive depth. This is a lexical check;
/// the filesystem module additionally refuses symlinks while walking (§9).
pub fn validate_relative_path(path: &str) -> Result<Vec<&str>, String> {
    if path.is_empty() {
        return Err("empty path".into());
    }
    if path.len() > MAX_PATH_BYTES {
        return Err(format!("longer than {MAX_PATH_BYTES} bytes"));
    }
    if path.contains('\0') {
        return Err("contains NUL".into());
    }
    if path.contains('\\') {
        return Err("contains a backslash".into());
    }
    if path.starts_with('/') {
        return Err("absolute path".into());
    }
    let components: Vec<&str> = path.split('/').collect();
    if components.len() > MAX_PATH_DEPTH {
        return Err(format!("deeper than {MAX_PATH_DEPTH} components"));
    }
    for component in &components {
        if component.is_empty() {
            return Err("empty component".into());
        }
        if *component == "." || *component == ".." {
            return Err(format!("{component:?} component"));
        }
        if component.len() > MAX_COMPONENT_BYTES {
            return Err(format!("component longer than {MAX_COMPONENT_BYTES} bytes"));
        }
    }
    let first = components[0].as_bytes();
    if first.len() >= 2 && first[0].is_ascii_alphabetic() && first[1] == b':' {
        return Err("drive-letter prefix".into());
    }
    Ok(components)
}

/// Orders directory paths so every parent precedes its children (shallowest first).
/// Reverse the result to visit children before parents, as directory syncs require.
pub fn parent_first<'a>(directories: impl IntoIterator<Item = &'a str>) -> Vec<&'a str> {
    let ordered: BTreeSet<(usize, &str)> = directories
        .into_iter()
        .map(|d| (d.matches('/').count(), d))
        .collect();
    ordered.into_iter().map(|(_, d)| d).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const EMPTY_SHA: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    fn digest(n: u8) -> String {
        format!("{n:02x}").repeat(32)
    }

    fn file(id: u32, path: &str, size: u64) -> FileEntry {
        FileEntry {
            id,
            relative_path: path.into(),
            size_bytes: size,
            sha256: digest(9),
            chunk_sha256: (0..chunk_count(size)).map(|_| digest(1)).collect(),
        }
    }

    fn base() -> Manifest {
        Manifest {
            protocol_version: 1,
            chunk_size_bytes: CHUNK_SIZE_BYTES,
            directories: vec!["a".into(), "a/b".into(), "empty".into()],
            files: vec![
                file(0, "top.bin", CHUNK_SIZE_BYTES + 1),
                file(1, "a/b/zero.bin", 0),
            ],
        }
    }

    fn bytes(manifest: &Manifest) -> Vec<u8> {
        serde_json::to_vec(manifest).unwrap()
    }

    fn assert_invalid(manifest: Manifest, needle: &str) {
        match parse_and_validate(&bytes(&manifest)) {
            Err(ManifestError::Invalid(message)) => {
                assert!(message.contains(needle), "{message:?} lacks {needle:?}")
            }
            other => panic!("expected invalid ({needle}), got {other:?}"),
        }
    }

    fn assert_quota(manifest: Manifest) {
        assert!(matches!(
            parse_and_validate(&bytes(&manifest)),
            Err(ManifestError::Quota(_))
        ));
    }

    #[test]
    fn accepts_a_valid_manifest_and_hashes_exact_bytes() {
        let document = bytes(&base());
        let validated = parse_and_validate(&document).unwrap();
        assert_eq!(validated.digest, sha256_hex(&document));
        assert_eq!(validated.total_bytes, CHUNK_SIZE_BYTES + 1);
        assert_eq!(validated.total_chunks, 2);
        // Reformatting the same logical content changes the identity.
        let pretty = serde_json::to_vec_pretty(&base()).unwrap();
        assert_ne!(
            parse_and_validate(&pretty).unwrap().digest,
            validated.digest
        );
        assert_eq!(sha256_hex(b""), EMPTY_SHA);
    }

    #[test]
    fn rejects_duplicate_and_unknown_fields() {
        let duplicate = br#"{"protocol_version":1,"protocol_version":1,"chunk_size_bytes":4194304,"directories":[],"files":[]}"#;
        assert!(matches!(
            parse_and_validate(duplicate),
            Err(ManifestError::Invalid(m)) if m.contains("duplicate key")
        ));
        let unknown = br#"{"protocol_version":1,"chunk_size_bytes":4194304,"directories":[],"files":[],"extra":true}"#;
        assert!(matches!(
            parse_and_validate(unknown),
            Err(ManifestError::Invalid(m)) if m.contains("unknown field")
        ));
        let nested_unknown = br#"{"protocol_version":1,"chunk_size_bytes":4194304,"directories":[],"files":[{"id":0,"relative_path":"x","size_bytes":0,"sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","chunk_sha256":[],"mode":7}]}"#;
        assert!(parse_and_validate(nested_unknown).is_err());
        assert!(parse_and_validate(b"not json").is_err());
        assert!(parse_and_validate(b"{\"protocol_version\":-1}").is_err());
    }

    #[test]
    fn rejects_wrong_protocol_constants() {
        let mut m = base();
        m.protocol_version = 2;
        assert_invalid(m, "protocol_version");
        let mut m = base();
        m.chunk_size_bytes = 1024;
        assert_invalid(m, "chunk_size_bytes");
    }

    #[test]
    fn rejects_bad_ids_digests_and_chunk_counts() {
        let mut m = base();
        m.files[1].id = 5;
        assert_invalid(m, "sequential");
        let mut m = base();
        m.files[0].sha256 = "ABC".into();
        assert_invalid(m, "sha256");
        let mut m = base();
        m.files[0].sha256 = digest(0xab).to_uppercase();
        assert_invalid(m, "sha256");
        let mut m = base();
        m.files[0].chunk_sha256.pop();
        assert_invalid(m, "chunk digests");
        let mut m = base();
        m.files[0].chunk_sha256[1] = "zz".into();
        assert_invalid(m, "chunk digest 1");
    }

    #[test]
    fn rejects_structural_path_problems() {
        let mut m = base();
        m.files[1].relative_path = "top.bin".into();
        assert_invalid(m, "duplicate path");
        let mut m = base();
        m.directories.push("top.bin".into());
        assert_invalid(m, "duplicate path");
        let mut m = base();
        m.directories.retain(|d| d != "a");
        assert_invalid(m, "not listed");
        let mut m = base();
        m.files.push(file(2, "top.bin/inner", 1));
        assert_invalid(m, "which is a file");
        let mut m = base();
        m.directories.push("a".into());
        assert_invalid(m, "duplicate path");
    }

    #[test]
    fn rejects_every_unsafe_path_form() {
        let long_component = "x".repeat(256);
        let long_path = vec!["abcdefgh"; 120].join("/");
        let deep = vec!["d"; 33].join("/");
        let cases: Vec<(String, &str)> = vec![
            ("".into(), "empty path"),
            ("/etc/passwd".into(), "absolute"),
            ("../escape".into(), "\"..\""),
            ("a/../b".into(), "\"..\""),
            ("./a".into(), "\".\""),
            ("a//b".into(), "empty component"),
            ("a/".into(), "empty component"),
            ("a\\b".into(), "backslash"),
            ("a\0b".into(), "NUL"),
            ("C:evil".into(), "drive-letter"),
            (long_component, "component longer"),
            (long_path, "longer than 1024"),
            (deep, "deeper than"),
        ];
        for (path, needle) in cases {
            let error = validate_relative_path(&path).unwrap_err();
            assert!(
                error.contains(needle),
                "{path:?}: {error:?} lacks {needle:?}"
            );
            let mut m = base();
            m.files[0].relative_path = path.clone();
            assert!(parse_and_validate(&bytes(&m)).is_err(), "{path:?} accepted");
        }
        assert_eq!(validate_relative_path("a/b.c").unwrap(), vec!["a", "b.c"]);
        assert!(validate_relative_path("..a/b..").is_ok());
    }

    #[test]
    fn enforces_admission_caps() {
        let mut m = base();
        m.files = (0..=MAX_FILES as u32)
            .map(|i| file(i, &format!("f{i}"), 0))
            .collect();
        m.directories.clear();
        assert_quota(m);

        let mut m = base();
        m.directories = (0..=MAX_DIRECTORIES).map(|i| format!("d{i}")).collect();
        m.files.clear();
        assert_quota(m);

        // Size alone exceeds 100 GiB; the digest list length is checked first, so give it the
        // matching (large) count of short-circuited entries only up to the cap.
        let mut m = base();
        let size = MAX_TOTAL_BYTES + 1;
        m.files = vec![FileEntry {
            id: 0,
            relative_path: "huge".into(),
            size_bytes: size,
            sha256: digest(1),
            chunk_sha256: vec![digest(2); chunk_count(size) as usize],
        }];
        assert_quota(m);

        let oversized = vec![b' '; MAX_MANIFEST_BYTES + 1];
        assert!(matches!(
            parse_and_validate(&oversized),
            Err(ManifestError::Quota(_))
        ));
    }

    #[test]
    fn orders_directories_parent_first() {
        let directories = ["a/b/c", "a", "z", "a/b"];
        assert_eq!(parent_first(directories), vec!["a", "z", "a/b", "a/b/c"]);
    }
}
