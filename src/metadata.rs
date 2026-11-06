//! SQLite statements and transactions for receipts and session state (§5).
//!
//! Only the storage worker holds this writable connection. A transaction here protects
//! metadata only; it does not make file writes part of the transaction. The storage worker
//! orders file syncs and commits explicitly (§7).

use std::path::Path;

use rusqlite::{Connection, OptionalExtension, Transaction, params};

use crate::config::{chunk_count, chunk_length};
use crate::manifest::ValidatedManifest;
use crate::protocol::{ChunkRef, MissingFile, MissingPage, SessionState, SessionStatus};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS sessions (
    id                TEXT PRIMARY KEY,
    owner             TEXT NOT NULL,
    idempotency_key   TEXT NOT NULL,
    manifest_sha256   TEXT NOT NULL,
    state             TEXT NOT NULL CHECK (state IN
                          ('preparing','receiving','verifying','publishing',
                           'published','aborted','faulted')),
    total_bytes       INTEGER NOT NULL CHECK (total_bytes >= 0),
    total_chunks      INTEGER NOT NULL CHECK (total_chunks >= 0),
    durable_bytes     INTEGER NOT NULL DEFAULT 0 CHECK (durable_bytes >= 0),
    durable_chunks    INTEGER NOT NULL DEFAULT 0 CHECK (durable_chunks >= 0),
    -- Publication intent: recorded before the final rename (§8).
    destination       TEXT,
    created_at_millis INTEGER NOT NULL,
    updated_at_millis INTEGER NOT NULL,
    last_error        TEXT,
    UNIQUE (owner, idempotency_key)
);
CREATE TABLE IF NOT EXISTS files (
    session_id  TEXT NOT NULL REFERENCES sessions(id),
    file_id     INTEGER NOT NULL CHECK (file_id >= 0),
    path        TEXT NOT NULL,
    size_bytes  INTEGER NOT NULL CHECK (size_bytes >= 0),
    sha256      TEXT NOT NULL,
    PRIMARY KEY (session_id, file_id),
    UNIQUE (session_id, path)
);
CREATE TABLE IF NOT EXISTS chunks (
    session_id      TEXT NOT NULL,
    file_id         INTEGER NOT NULL,
    chunk_index     INTEGER NOT NULL CHECK (chunk_index >= 0),
    expected_sha256 TEXT NOT NULL,
    expected_length INTEGER NOT NULL CHECK (expected_length > 0),
    durable         INTEGER NOT NULL DEFAULT 0 CHECK (durable IN (0, 1)),
    PRIMARY KEY (session_id, file_id, chunk_index),
    FOREIGN KEY (session_id, file_id) REFERENCES files(session_id, file_id)
);
CREATE INDEX IF NOT EXISTS chunks_by_receipt ON chunks (session_id, durable, file_id, chunk_index);
CREATE TABLE IF NOT EXISTS directories (
    session_id TEXT NOT NULL REFERENCES sessions(id),
    path       TEXT NOT NULL,
    PRIMARY KEY (session_id, path)
);
";

pub type DbResult<T> = Result<T, rusqlite::Error>;

/// One row of `sessions`.
#[derive(Clone, Debug)]
pub struct SessionRow {
    pub id: String,
    pub owner: String,
    pub idempotency_key: String,
    pub manifest_sha256: String,
    pub state: SessionState,
    pub total_bytes: u64,
    pub total_chunks: u64,
    pub durable_bytes: u64,
    pub durable_chunks: u64,
    pub destination: Option<String>,
    pub last_error: Option<String>,
}

impl SessionRow {
    pub fn status(&self, verified_bytes: u64) -> SessionStatus {
        SessionStatus {
            session_id: self.id.clone(),
            state: self.state,
            total_bytes: self.total_bytes,
            total_chunks: self.total_chunks,
            durable_bytes: self.durable_bytes,
            durable_chunks: self.durable_chunks,
            verified_bytes,
            destination: if self.state == SessionState::Published {
                self.destination.clone()
            } else {
                None
            },
            last_error: self.last_error.clone(),
        }
    }
}

/// One file of a session with its expected chunk digests in order.
#[derive(Clone, Debug)]
pub struct FileRecord {
    pub file_id: u32,
    pub path: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub chunk_sha256: Vec<String>,
}

/// Expected digest, length and receipt flag of one chunk.
#[derive(Clone, Debug)]
pub struct ChunkRecord {
    pub expected_sha256: String,
    pub expected_length: u64,
    pub durable: bool,
}

fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn to_i64(value: u64) -> i64 {
    // All stored quantities are bounded by the admission caps (100 GiB, 50,000 chunks).
    i64::try_from(value).expect("value bounded by admission caps")
}

fn to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

/// Opens the metadata database and verifies that the durability settings actually applied:
/// WAL journal, `synchronous=FULL` and `foreign_keys=ON`. Refuses to continue otherwise.
pub fn open(path: &Path) -> Result<Connection, String> {
    let connection = Connection::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    configure(&connection)?;
    connection
        .execute_batch(SCHEMA)
        .map_err(|e| format!("create schema: {e}"))?;
    Ok(connection)
}

/// Opens a read-only connection (used by tests and maintenance to inspect state).
pub fn open_read_only(path: &Path) -> DbResult<Connection> {
    Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
}

fn configure(connection: &Connection) -> Result<(), String> {
    let journal: String = connection
        .query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))
        .map_err(|e| format!("set journal_mode: {e}"))?;
    if !journal.eq_ignore_ascii_case("wal") {
        return Err(format!("journal_mode is {journal}, expected wal"));
    }
    connection
        .execute_batch("PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON;")
        .map_err(|e| format!("set pragmas: {e}"))?;
    // macOS needs F_FULLFSYNC for a commit to reach stable storage. Harmless elsewhere.
    #[cfg(target_os = "macos")]
    connection
        .execute_batch("PRAGMA fullfsync = ON; PRAGMA checkpoint_fullfsync = ON;")
        .map_err(|e| format!("set fullfsync: {e}"))?;
    let synchronous: i64 = connection
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .map_err(|e| format!("read synchronous: {e}"))?;
    if synchronous != 2 {
        return Err(format!("synchronous is {synchronous}, expected 2 (FULL)"));
    }
    let foreign_keys: i64 = connection
        .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
        .map_err(|e| format!("read foreign_keys: {e}"))?;
    if foreign_keys != 1 {
        return Err("foreign_keys did not turn on".into());
    }
    Ok(())
}

const SESSION_COLUMNS: &str = "id, owner, idempotency_key, manifest_sha256, state, total_bytes, \
     total_chunks, durable_bytes, durable_chunks, destination, last_error";

fn session_from_row(row: &rusqlite::Row<'_>) -> DbResult<SessionRow> {
    let state_text: String = row.get(4)?;
    let state = SessionState::parse(&state_text).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            4,
            rusqlite::types::Type::Text,
            format!("unknown session state {state_text:?}").into(),
        )
    })?;
    Ok(SessionRow {
        id: row.get(0)?,
        owner: row.get(1)?,
        idempotency_key: row.get(2)?,
        manifest_sha256: row.get(3)?,
        state,
        total_bytes: to_u64(row.get(5)?),
        total_chunks: to_u64(row.get(6)?),
        durable_bytes: to_u64(row.get(7)?),
        durable_chunks: to_u64(row.get(8)?),
        destination: row.get(9)?,
        last_error: row.get(10)?,
    })
}

pub fn session(connection: &Connection, id: &str) -> DbResult<Option<SessionRow>> {
    connection
        .query_row(
            &format!("SELECT {SESSION_COLUMNS} FROM sessions WHERE id = ?1"),
            params![id],
            session_from_row,
        )
        .optional()
}

pub fn session_by_key(
    connection: &Connection,
    owner: &str,
    idempotency_key: &str,
) -> DbResult<Option<SessionRow>> {
    connection
        .query_row(
            &format!(
                "SELECT {SESSION_COLUMNS} FROM sessions WHERE owner = ?1 AND idempotency_key = ?2"
            ),
            params![owner, idempotency_key],
            session_from_row,
        )
        .optional()
}

pub fn all_sessions(connection: &Connection) -> DbResult<Vec<SessionRow>> {
    let mut statement = connection.prepare(&format!(
        "SELECT {SESSION_COLUMNS} FROM sessions ORDER BY created_at_millis, id"
    ))?;
    let rows = statement.query_map([], session_from_row)?;
    rows.collect()
}

/// The session (if any) that blocks a new transfer.
pub fn active_session(connection: &Connection) -> DbResult<Option<SessionRow>> {
    connection
        .query_row(
            &format!(
                "SELECT {SESSION_COLUMNS} FROM sessions \
                 WHERE state IN ('preparing','receiving','verifying','publishing') LIMIT 1"
            ),
            [],
            session_from_row,
        )
        .optional()
}

/// Records a new session in `Preparing` with its files, chunks and directories, in one
/// transaction. The caller has already saved and synced the exact manifest bytes.
pub fn insert_preparing_session(
    connection: &mut Connection,
    id: &str,
    owner: &str,
    idempotency_key: &str,
    validated: &ValidatedManifest,
) -> DbResult<()> {
    let transaction = connection.transaction()?;
    let now = now_millis();
    transaction.execute(
        "INSERT INTO sessions (id, owner, idempotency_key, manifest_sha256, state, total_bytes, \
         total_chunks, created_at_millis, updated_at_millis) \
         VALUES (?1, ?2, ?3, ?4, 'preparing', ?5, ?6, ?7, ?7)",
        params![
            id,
            owner,
            idempotency_key,
            validated.digest,
            to_i64(validated.total_bytes),
            to_i64(validated.total_chunks),
            now
        ],
    )?;
    {
        let mut insert_directory =
            transaction.prepare("INSERT INTO directories (session_id, path) VALUES (?1, ?2)")?;
        for directory in &validated.manifest.directories {
            insert_directory.execute(params![id, directory])?;
        }
        let mut insert_file = transaction.prepare(
            "INSERT INTO files (session_id, file_id, path, size_bytes, sha256) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        let mut insert_chunk = transaction.prepare(
            "INSERT INTO chunks (session_id, file_id, chunk_index, expected_sha256, \
             expected_length) VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for file in &validated.manifest.files {
            insert_file.execute(params![
                id,
                file.id,
                file.relative_path,
                to_i64(file.size_bytes),
                file.sha256
            ])?;
            for (index, digest) in file.chunk_sha256.iter().enumerate() {
                let index = u32::try_from(index).expect("chunk index validated");
                let length = chunk_length(file.size_bytes, index).expect("chunk index validated");
                insert_chunk.execute(params![id, file.id, index, digest, to_i64(length)])?;
            }
        }
    }
    transaction.commit()
}

/// Changes a session's state if it is currently `from`. Returns false if the state had
/// already changed (the caller rechecks). The transition table is enforced here.
pub fn transition(
    connection: &Connection,
    id: &str,
    from: SessionState,
    to: SessionState,
    last_error: Option<&str>,
) -> DbResult<bool> {
    assert!(from.can_become(to), "illegal transition {from:?} -> {to:?}");
    let changed = connection.execute(
        "UPDATE sessions SET state = ?1, last_error = ?2, updated_at_millis = ?3 \
         WHERE id = ?4 AND state = ?5",
        params![to.as_str(), last_error, now_millis(), id, from.as_str()],
    )?;
    Ok(changed == 1)
}

/// Records the publication intent and enters `Publishing` in one transaction.
pub fn record_publication_intent(
    connection: &mut Connection,
    id: &str,
    destination: &str,
) -> DbResult<bool> {
    let transaction = connection.transaction()?;
    let changed = transaction.execute(
        "UPDATE sessions SET state = 'publishing', destination = ?1, last_error = NULL, \
         updated_at_millis = ?2 WHERE id = ?3 AND state = 'verifying'",
        params![destination, now_millis(), id],
    )?;
    transaction.commit()?;
    Ok(changed == 1)
}

pub fn set_last_error(connection: &Connection, id: &str, message: Option<&str>) -> DbResult<()> {
    connection.execute(
        "UPDATE sessions SET last_error = ?1, updated_at_millis = ?2 WHERE id = ?3",
        params![message, now_millis(), id],
    )?;
    Ok(())
}

pub fn chunk(
    connection: &Connection,
    id: &str,
    file_id: u32,
    chunk_index: u32,
) -> DbResult<Option<ChunkRecord>> {
    connection
        .query_row(
            "SELECT expected_sha256, expected_length, durable FROM chunks \
             WHERE session_id = ?1 AND file_id = ?2 AND chunk_index = ?3",
            params![id, file_id, chunk_index],
            |row| {
                Ok(ChunkRecord {
                    expected_sha256: row.get(0)?,
                    expected_length: to_u64(row.get(1)?),
                    durable: row.get::<_, i64>(2)? == 1,
                })
            },
        )
        .optional()
}

/// Marks one chunk durable and updates progress, in one transaction. Returns false if the
/// receipt already existed (no logical change).
pub fn record_receipt(
    connection: &mut Connection,
    id: &str,
    file_id: u32,
    chunk_index: u32,
) -> DbResult<bool> {
    let transaction = connection.transaction()?;
    let changed = mark_durable(&transaction, id, file_id, chunk_index)?;
    transaction.commit()?;
    Ok(changed)
}

fn mark_durable(
    transaction: &Transaction<'_>,
    id: &str,
    file_id: u32,
    chunk_index: u32,
) -> DbResult<bool> {
    let length: Option<i64> = transaction
        .query_row(
            "UPDATE chunks SET durable = 1 WHERE session_id = ?1 AND file_id = ?2 \
             AND chunk_index = ?3 AND durable = 0 RETURNING expected_length",
            params![id, file_id, chunk_index],
            |row| row.get(0),
        )
        .optional()?;
    let Some(length) = length else {
        return Ok(false);
    };
    transaction.execute(
        "UPDATE sessions SET durable_bytes = durable_bytes + ?1, \
         durable_chunks = durable_chunks + 1, last_error = NULL, updated_at_millis = ?2 \
         WHERE id = ?3",
        params![length, now_millis(), id],
    )?;
    Ok(true)
}

/// Clears receipts for chunks that failed a hash check, in one transaction.
pub fn clear_receipts(connection: &mut Connection, id: &str, chunks: &[ChunkRef]) -> DbResult<u64> {
    let transaction = connection.transaction()?;
    let mut cleared = 0;
    for chunk in chunks {
        let length: Option<i64> = transaction
            .query_row(
                "UPDATE chunks SET durable = 0 WHERE session_id = ?1 AND file_id = ?2 \
                 AND chunk_index = ?3 AND durable = 1 RETURNING expected_length",
                params![id, chunk.file_id, chunk.chunk_index],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(length) = length {
            transaction.execute(
                "UPDATE sessions SET durable_bytes = durable_bytes - ?1, \
                 durable_chunks = durable_chunks - 1, updated_at_millis = ?2 WHERE id = ?3",
                params![length, now_millis(), id],
            )?;
            cleared += 1;
        }
    }
    transaction.commit()?;
    Ok(cleared)
}

/// Number of chunks without a durable receipt.
pub fn missing_count(connection: &Connection, id: &str) -> DbResult<u64> {
    let count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM chunks WHERE session_id = ?1 AND durable = 0",
        params![id],
        |row| row.get(0),
    )?;
    Ok(to_u64(count))
}

/// One page of missing chunks in `(file_id, chunk_index)` order, strictly after `after`.
pub fn missing_page(
    connection: &Connection,
    id: &str,
    after: Option<ChunkRef>,
    limit: usize,
) -> DbResult<MissingPage> {
    let (after_file, after_chunk) = match after {
        Some(position) => (i64::from(position.file_id), i64::from(position.chunk_index)),
        None => (-1, -1),
    };
    let mut statement = connection.prepare(
        "SELECT file_id, chunk_index FROM chunks WHERE session_id = ?1 AND durable = 0 \
         AND (file_id > ?2 OR (file_id = ?2 AND chunk_index > ?3)) \
         ORDER BY file_id, chunk_index LIMIT ?4",
    )?;
    let rows = statement.query_map(params![id, after_file, after_chunk, limit as i64], |row| {
        Ok(ChunkRef {
            file_id: row.get(0)?,
            chunk_index: row.get(1)?,
        })
    })?;
    let positions: Vec<ChunkRef> = rows.collect::<DbResult<_>>()?;
    let next_after = if positions.len() == limit {
        positions.last().copied()
    } else {
        None
    };
    let mut missing: Vec<MissingFile> = Vec::new();
    for position in positions {
        match missing.last_mut() {
            Some(group) if group.file_id == position.file_id => {
                group.chunk_indexes.push(position.chunk_index)
            }
            _ => missing.push(MissingFile {
                file_id: position.file_id,
                chunk_indexes: vec![position.chunk_index],
            }),
        }
    }
    Ok(MissingPage {
        missing,
        next_after,
    })
}

/// Every chunk currently marked durable, for the startup recheck.
pub fn durable_chunks(connection: &Connection, id: &str) -> DbResult<Vec<ChunkRef>> {
    let mut statement = connection.prepare(
        "SELECT file_id, chunk_index FROM chunks WHERE session_id = ?1 AND durable = 1 \
         ORDER BY file_id, chunk_index",
    )?;
    let rows = statement.query_map(params![id], |row| {
        Ok(ChunkRef {
            file_id: row.get(0)?,
            chunk_index: row.get(1)?,
        })
    })?;
    rows.collect()
}

/// Files of a session in ID order, each with its expected chunk digests.
pub fn files(connection: &Connection, id: &str) -> DbResult<Vec<FileRecord>> {
    let mut file_statement = connection.prepare(
        "SELECT file_id, path, size_bytes, sha256 FROM files WHERE session_id = ?1 \
         ORDER BY file_id",
    )?;
    let mut records: Vec<FileRecord> = file_statement
        .query_map(params![id], |row| {
            Ok(FileRecord {
                file_id: row.get(0)?,
                path: row.get(1)?,
                size_bytes: to_u64(row.get(2)?),
                sha256: row.get(3)?,
                chunk_sha256: Vec::new(),
            })
        })?
        .collect::<DbResult<_>>()?;
    let mut chunk_statement = connection.prepare(
        "SELECT file_id, expected_sha256 FROM chunks WHERE session_id = ?1 \
         ORDER BY file_id, chunk_index",
    )?;
    let chunks = chunk_statement.query_map(params![id], |row| {
        Ok((row.get::<_, u32>(0)?, row.get::<_, String>(1)?))
    })?;
    for chunk in chunks {
        let (file_id, digest) = chunk?;
        if let Some(record) = records.get_mut(file_id as usize) {
            record.chunk_sha256.push(digest);
        }
    }
    for record in &records {
        debug_assert_eq!(
            record.chunk_sha256.len() as u64,
            chunk_count(record.size_bytes)
        );
    }
    Ok(records)
}

pub fn directories(connection: &Connection, id: &str) -> DbResult<Vec<String>> {
    let mut statement =
        connection.prepare("SELECT path FROM directories WHERE session_id = ?1 ORDER BY path")?;
    let rows = statement.query_map(params![id], |row| row.get(0))?;
    rows.collect()
}

/// True when a SQLite error means the disk is full.
pub fn is_disk_full(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == rusqlite::ErrorCode::DiskFull
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CHUNK_SIZE_BYTES;
    use crate::manifest::{FileEntry, Manifest, parse_and_validate};

    fn validated() -> ValidatedManifest {
        let manifest = Manifest {
            protocol_version: 1,
            chunk_size_bytes: CHUNK_SIZE_BYTES,
            directories: vec!["d".into()],
            files: vec![
                FileEntry {
                    id: 0,
                    relative_path: "d/a".into(),
                    size_bytes: 2 * CHUNK_SIZE_BYTES + 5,
                    sha256: "1".repeat(64),
                    chunk_sha256: vec!["a".repeat(64), "b".repeat(64), "c".repeat(64)],
                },
                FileEntry {
                    id: 1,
                    relative_path: "b".into(),
                    size_bytes: 7,
                    sha256: "2".repeat(64),
                    chunk_sha256: vec!["d".repeat(64)],
                },
            ],
        };
        parse_and_validate(&serde_json::to_vec(&manifest).unwrap()).unwrap()
    }

    fn database() -> (tempfile::TempDir, Connection) {
        let dir = tempfile::tempdir().unwrap();
        let connection = open(&dir.path().join("receiver.sqlite")).unwrap();
        (dir, connection)
    }

    #[test]
    fn durability_pragmas_are_applied() {
        let (_dir, connection) = database();
        let journal: String = connection
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(journal, "wal");
        let synchronous: i64 = connection
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .unwrap();
        assert_eq!(synchronous, 2);
    }

    #[test]
    fn receipts_progress_and_pagination() {
        let (_dir, mut connection) = database();
        let manifest = validated();
        insert_preparing_session(&mut connection, "s1", "alice", "key", &manifest).unwrap();
        assert!(
            transition(
                &connection,
                "s1",
                SessionState::Preparing,
                SessionState::Receiving,
                None
            )
            .unwrap()
        );
        assert_eq!(missing_count(&connection, "s1").unwrap(), 4);

        assert!(record_receipt(&mut connection, "s1", 0, 1).unwrap());
        assert!(!record_receipt(&mut connection, "s1", 0, 1).unwrap());
        let row = session(&connection, "s1").unwrap().unwrap();
        assert_eq!(row.durable_chunks, 1);
        assert_eq!(row.durable_bytes, CHUNK_SIZE_BYTES);

        let first = missing_page(&connection, "s1", None, 2).unwrap();
        assert_eq!(
            first.missing,
            vec![MissingFile {
                file_id: 0,
                chunk_indexes: vec![0, 2]
            }]
        );
        let second = missing_page(&connection, "s1", first.next_after, 2).unwrap();
        assert_eq!(
            second.missing,
            vec![MissingFile {
                file_id: 1,
                chunk_indexes: vec![0]
            }]
        );
        assert_eq!(second.next_after, None);

        let cleared = clear_receipts(
            &mut connection,
            "s1",
            &[
                ChunkRef {
                    file_id: 0,
                    chunk_index: 1,
                },
                ChunkRef {
                    file_id: 0,
                    chunk_index: 2,
                },
            ],
        )
        .unwrap();
        assert_eq!(cleared, 1);
        let row = session(&connection, "s1").unwrap().unwrap();
        assert_eq!((row.durable_chunks, row.durable_bytes), (0, 0));

        let files = files(&connection, "s1").unwrap();
        assert_eq!(files[0].chunk_sha256.len(), 3);
        assert_eq!(files[1].path, "b");
        assert_eq!(directories(&connection, "s1").unwrap(), vec!["d"]);
    }

    #[test]
    fn state_changes_are_conditional_and_keys_are_unique() {
        let (_dir, mut connection) = database();
        let manifest = validated();
        insert_preparing_session(&mut connection, "s1", "alice", "key", &manifest).unwrap();
        assert!(
            !transition(
                &connection,
                "s1",
                SessionState::Receiving,
                SessionState::Verifying,
                None
            )
            .unwrap()
        );
        assert!(
            insert_preparing_session(&mut connection, "s2", "alice", "key", &manifest).is_err()
        );
        insert_preparing_session(&mut connection, "s3", "bob", "key", &manifest).unwrap();
        assert_eq!(
            session_by_key(&connection, "alice", "key")
                .unwrap()
                .unwrap()
                .id,
            "s1"
        );
        assert!(active_session(&connection).unwrap().is_some());
    }

    #[test]
    #[should_panic(expected = "illegal transition")]
    fn illegal_transitions_panic() {
        let (_dir, connection) = database();
        let _ = transition(
            &connection,
            "s1",
            SessionState::Published,
            SessionState::Receiving,
            None,
        );
    }
}
