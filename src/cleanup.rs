//! Explicit maintenance for a stopped receiver (§6, §8).
//!
//! Abandoned staging is removed only here, after the writer has stopped: the command takes
//! the same exclusive lock as a running receiver, so it refuses to run alongside one. It never
//! touches `completed/`, and it keeps Faulted sessions for an operator to inspect.

use std::collections::HashSet;
use std::path::Path;

use serde::Serialize;

use crate::error::AppError;
use crate::filesystem::{lock_exclusive, sync_dir_path};
use crate::metadata;
use crate::protocol::SessionState;
use crate::storage::ReceiverPaths;

#[derive(Debug, Default, Serialize)]
pub struct CleanupReport {
    /// Staging removed for Aborted sessions.
    pub removed_aborted: Vec<String>,
    /// Leftover staging (the stored manifest) removed for Published sessions.
    pub removed_published_remnants: Vec<String>,
    /// Staging directories with no recorded session.
    pub orphans: Vec<String>,
    /// Orphans removed because `--remove-orphans` was given.
    pub removed_orphans: Vec<String>,
}

pub fn cleanup(root: &Path, remove_orphans: bool) -> Result<CleanupReport, AppError> {
    let paths = ReceiverPaths::new(root);
    if !paths.database().exists() {
        return Err(AppError::invalid(format!(
            "{} is not a receiver root",
            root.display()
        )));
    }
    let _lock = lock_exclusive(&paths.lock_file())
        .map_err(|e| AppError::environment(format!("lock receiver root: {e}")))?;
    let db = metadata::open(&paths.database()).map_err(AppError::environment)?;
    let sessions = metadata::all_sessions(&db).map_err(|e| AppError::environment(e.to_string()))?;
    let mut report = CleanupReport::default();
    let mut known = HashSet::new();
    let remove = |id: &str| -> Result<bool, AppError> {
        let dir = paths.staging_session(id);
        if !dir.exists() {
            return Ok(false);
        }
        std::fs::remove_dir_all(&dir)
            .map_err(|e| AppError::environment(format!("remove staging {id}: {e}")))?;
        Ok(true)
    };
    for session in &sessions {
        known.insert(session.id.clone());
        match session.state {
            SessionState::Aborted if remove(&session.id)? => {
                report.removed_aborted.push(session.id.clone())
            }
            SessionState::Published if remove(&session.id)? => {
                report.removed_published_remnants.push(session.id.clone())
            }
            _ => {}
        }
    }
    let entries = std::fs::read_dir(paths.staging())
        .map_err(|e| AppError::environment(format!("list staging: {e}")))?;
    for entry in entries {
        let entry = entry.map_err(|e| AppError::environment(e.to_string()))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if known.contains(&name) {
            continue;
        }
        report.orphans.push(name.clone());
        if remove_orphans {
            let path = entry.path();
            let result = if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                std::fs::remove_dir_all(&path)
            } else {
                std::fs::remove_file(&path)
            };
            result.map_err(|e| AppError::environment(format!("remove orphan {name}: {e}")))?;
            report.removed_orphans.push(name);
        }
    }
    sync_dir_path(&paths.staging()).map_err(|e| AppError::environment(e.to_string()))?;
    Ok(report)
}
