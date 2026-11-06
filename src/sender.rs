//! The sender: resume planning, bounded upload workers and retries (§6).
//!
//! The receiver is authoritative. After any interruption the sender asks which chunks are
//! still missing and sends only those; its own progress counters are informational.
//!
//! Retry rules:
//! - Transport failures, 429 and 503 are retried with exponential backoff and full jitter,
//!   honouring `Retry-After`, until the per-request retry window ends.
//! - 400, 401, 404, 409, 413 and 422 are not retried; they need a different request.
//! - 507 (receiver out of space) stops the send: capacity must recover first, and a tight
//!   retry loop would not help.
//! - A timeout is an uncertain outcome. Chunk uploads are idempotent and simply retried;
//!   after an uncertain commit the sender reads the session status.

use std::collections::VecDeque;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::{Client, Method, StatusCode};
use serde::Serialize;

use crate::config::SenderSettings;
use crate::error::AppError;
use crate::protocol::{
    ChunkRef, CreateSessionResponse, ErrorBody, IDEMPOTENCY_KEY_HEADER, MANIFEST_DIGEST_HEADER,
    MissingPage, SessionState, SessionStatus, StateResponse,
};
use crate::snapshot::{
    self, LoadedSpool, SenderState, UPLOAD_LOG_FILE, UploadAttempt, now_unix_millis,
};
use crate::tls;

#[derive(Clone, Debug)]
pub struct SendOptions {
    pub spool: PathBuf,
    /// Base URL of the receiver, for example `https://localhost:8443`.
    pub endpoint: String,
    pub ca: PathBuf,
    pub token_file: PathBuf,
    pub settings: SenderSettings,
}

/// Printed as JSON when a send finishes.
#[derive(Clone, Debug, Serialize)]
pub struct SendReport {
    pub session_id: String,
    pub state: SessionState,
    pub destination: Option<String>,
    pub total_bytes: u64,
    /// Chunk bytes this run put on the wire, including retries.
    pub attempted_bytes_this_run: u64,
    /// Chunk bytes attempted across every run on this spool.
    pub attempted_bytes_all_runs: u64,
    /// Bytes sent beyond one copy of the content, across every run on this spool.
    pub resent_bytes_all_runs: u64,
    pub upload_rounds: u64,
    pub elapsed_millis: u64,
    /// Time from commit acceptance to the published result, as observed by polling.
    pub commit_to_published_millis: Option<u64>,
}

/// A receiver connection: client, endpoint and bearer token.
#[derive(Clone)]
struct Connection {
    client: Client,
    endpoint: String,
    token: String,
    retry_window: Duration,
}

/// A response with a non-success status that should not be retried.
fn rejection(status: StatusCode, body: &[u8]) -> AppError {
    let parsed = serde_json::from_slice::<ErrorBody>(body).ok();
    let detail = match &parsed {
        Some(error) => format!(
            "{} ({}, request {})",
            error.message, error.code, error.request_id
        ),
        None => String::from_utf8_lossy(body).into_owned(),
    };
    let message = format!("receiver returned {status}: {detail}");
    let mut error = match status.as_u16() {
        400 | 413 | 422 => AppError::invalid(message),
        401 | 404 | 409 => AppError::rejected(message),
        _ => AppError::environment(message),
    };
    error.remote_code = parsed.map(|body| body.code);
    error
}

fn backoff(attempt: u32) -> Duration {
    let ceiling_ms = 100u64.saturating_mul(1u64 << attempt.min(6)).min(5_000);
    Duration::from_millis(rand::random_range(0..=ceiling_ms))
}

/// Reported around each attempt of a chunk upload, so the upload log counts bytes that may
/// have reached the receiver. An attempt that never connected sent nothing and is cancelled.
#[derive(Clone, Copy, Debug)]
enum Attempt {
    Started,
    NotConnected,
}

type AttemptHook<'a> = Option<&'a (dyn Fn(Attempt) + Sync)>;

impl Connection {
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.endpoint.trim_end_matches('/'))
    }

    /// Sends a request, retrying transient failures. Returns the successful status and body.
    async fn request(
        &self,
        method: Method,
        path: &str,
        headers: &[(&str, String)],
        body: Option<bytes::Bytes>,
        hook: AttemptHook<'_>,
    ) -> Result<(StatusCode, bytes::Bytes), AppError> {
        let started = Instant::now();
        let mut attempt: u32 = 0;
        loop {
            let mut builder = self
                .client
                .request(method.clone(), self.url(path))
                .bearer_auth(&self.token);
            for (name, value) in headers {
                builder = builder.header(*name, value);
            }
            if let Some(body) = &body {
                builder = builder.body(body.clone());
            }
            if let Some(hook) = hook {
                hook(Attempt::Started);
            }
            let wait = match builder.send().await {
                Ok(response) => {
                    let status = response.status();
                    let retry_after = response
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .map(Duration::from_secs);
                    let bytes = match response.bytes().await {
                        Ok(bytes) => bytes,
                        Err(error) => {
                            tracing::debug!(%error, path, "response body failed; retrying");
                            attempt += 1;
                            self.wait_or_give_up(started, backoff(attempt), path)
                                .await?;
                            continue;
                        }
                    };
                    if status.is_success() {
                        return Ok((status, bytes));
                    }
                    match status.as_u16() {
                        429 | 503 => retry_after.unwrap_or_else(|| backoff(attempt)),
                        507 => {
                            return Err(AppError::environment(format!(
                                "{}; free space on the receiver, then run send again",
                                rejection(status, &bytes).message
                            )));
                        }
                        _ => return Err(rejection(status, &bytes)),
                    }
                }
                Err(error) => {
                    if error.is_connect()
                        && let Some(hook) = hook
                    {
                        hook(Attempt::NotConnected);
                    }
                    tracing::debug!(%error, path, "request failed; retrying");
                    backoff(attempt)
                }
            };
            attempt += 1;
            self.wait_or_give_up(started, wait, path).await?;
        }
    }

    async fn wait_or_give_up(
        &self,
        started: Instant,
        wait: Duration,
        path: &str,
    ) -> Result<(), AppError> {
        if started.elapsed() + wait > self.retry_window {
            return Err(AppError::environment(format!(
                "receiver did not accept {path} within the {}-second retry window",
                self.retry_window.as_secs()
            )));
        }
        tokio::time::sleep(wait).await;
        Ok(())
    }

    async fn json<T: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
    ) -> Result<(StatusCode, T), AppError> {
        let (status, bytes) = self.request(method, path, &[], None, None).await?;
        let value = serde_json::from_slice(&bytes)
            .map_err(|e| AppError::environment(format!("unexpected response from {path}: {e}")))?;
        Ok((status, value))
    }

    async fn status(&self, session_id: &str) -> Result<SessionStatus, AppError> {
        Ok(self
            .json(Method::GET, &format!("/v1/sessions/{session_id}"))
            .await?
            .1)
    }

    /// Every missing chunk, read page by page from the beginning.
    async fn all_missing(&self, session_id: &str) -> Result<VecDeque<ChunkRef>, AppError> {
        let mut missing = VecDeque::new();
        let mut after: Option<ChunkRef> = None;
        loop {
            let query = match after {
                Some(position) => format!(
                    "?after_file={}&after_chunk={}",
                    position.file_id, position.chunk_index
                ),
                None => String::new(),
            };
            let (_, page): (_, MissingPage) = self
                .json(
                    Method::GET,
                    &format!("/v1/sessions/{session_id}/missing{query}"),
                )
                .await?;
            for file in page.missing {
                for chunk_index in file.chunk_indexes {
                    missing.push_back(ChunkRef {
                        file_id: file.file_id,
                        chunk_index,
                    });
                }
            }
            match page.next_after {
                Some(next) => after = Some(next),
                None => return Ok(missing),
            }
        }
    }
}

/// Appends upload attempts to the spool's log. Each line is written whole while the lock is
/// held; the lock is never held across an `.await`.
struct UploadLog {
    file: Mutex<std::fs::File>,
    run: String,
}

impl UploadLog {
    fn open(spool: &std::path::Path) -> Result<UploadLog, AppError> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(spool.join(UPLOAD_LOG_FILE))
            .map_err(|e| AppError::environment(format!("open upload log: {e}")))?;
        Ok(UploadLog {
            file: Mutex::new(file),
            run: uuid::Uuid::new_v4().simple().to_string(),
        })
    }

    fn record(&self, chunk: ChunkRef, bytes: u64, event: &str) {
        let attempt = UploadAttempt {
            run: self.run.clone(),
            event: event.to_string(),
            file_id: chunk.file_id,
            chunk_index: chunk.chunk_index,
            bytes,
            unix_millis: now_unix_millis(),
        };
        let Ok(mut line) = serde_json::to_vec(&attempt) else {
            return;
        };
        line.push(b'\n');
        if let Ok(mut file) = self.file.lock() {
            let _ = file.write_all(&line);
        }
    }
}

/// Uploads every queued chunk with a fixed number of workers. The first non-retryable error
/// stops the round.
async fn upload_round(
    connection: &Connection,
    spool: &Arc<LoadedSpool>,
    session_id: &str,
    queue: VecDeque<ChunkRef>,
    workers: usize,
    log: &Arc<UploadLog>,
) -> Result<u64, AppError> {
    let queue = Arc::new(Mutex::new(queue));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..workers {
        let connection = connection.clone();
        let spool = spool.clone();
        let queue = queue.clone();
        let log = log.clone();
        let session_id = session_id.to_string();
        tasks.spawn(async move {
            let mut attempted: u64 = 0;
            loop {
                let next = queue.lock().ok().and_then(|mut q| q.pop_front());
                let Some(chunk) = next else {
                    return Ok::<u64, AppError>(attempted);
                };
                let reader = spool.clone();
                let bytes = tokio::task::spawn_blocking(move || {
                    reader.read_chunk(chunk.file_id, chunk.chunk_index)
                })
                .await
                .map_err(|_| AppError::environment("spool reader panicked"))??;
                let length = bytes.len() as u64;
                let path = format!(
                    "/v1/sessions/{session_id}/files/{}/chunks/{}",
                    chunk.file_id, chunk.chunk_index
                );
                // Every attempt is logged before it is sent, so an attempt cut short by a
                // crash still counts; one that never connected is cancelled.
                let sent = AtomicU64::new(0);
                let hook = |event: Attempt| match event {
                    Attempt::Started => {
                        log.record(chunk, length, "started");
                        sent.fetch_add(length, Ordering::Relaxed);
                    }
                    Attempt::NotConnected => {
                        log.record(chunk, length, "not_connected");
                        sent.fetch_sub(length, Ordering::Relaxed);
                    }
                };
                let result = connection
                    .request(
                        Method::PUT,
                        &path,
                        &[],
                        Some(bytes::Bytes::from(bytes)),
                        Some(&hook),
                    )
                    .await;
                attempted += sent.load(Ordering::Relaxed);
                result?;
            }
        });
    }
    let mut attempted = 0;
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(bytes)) => attempted += bytes,
            Ok(Err(error)) => {
                tasks.abort_all();
                return Err(error);
            }
            Err(error) if error.is_cancelled() => {}
            Err(_) => {
                tasks.abort_all();
                return Err(AppError::environment("upload worker panicked"));
            }
        }
    }
    Ok(attempted)
}

fn read_token(path: &std::path::Path) -> Result<String, AppError> {
    let token = std::fs::read_to_string(path)
        .map_err(|e| AppError::invalid(format!("token file {}: {e}", path.display())))?;
    let token = token.trim().to_string();
    if token.is_empty() {
        return Err(AppError::invalid("token file is empty"));
    }
    Ok(token)
}

fn connect(options: &SendOptions) -> Result<Connection, AppError> {
    options.settings.validate().map_err(AppError::invalid)?;
    if !options.endpoint.starts_with("https://") {
        return Err(AppError::invalid("endpoint must be an https:// URL"));
    }
    let client = tls::http_client(
        &options.ca,
        options.settings.connect_timeout,
        options.settings.request_timeout,
    )
    .map_err(AppError::invalid)?;
    Ok(Connection {
        client,
        endpoint: options.endpoint.clone(),
        token: read_token(&options.token_file)?,
        retry_window: options.settings.retry_window,
    })
}

/// Creates the session (or recovers it after a lost response) and returns its ID.
async fn ensure_session(
    connection: &Connection,
    spool: &LoadedSpool,
    options: &SendOptions,
) -> Result<String, AppError> {
    let mut state = match snapshot::load_state(&spool.root)? {
        Some(state) => {
            if state.manifest_sha256 != spool.validated.digest {
                return Err(AppError::invalid(
                    "sender state belongs to a different manifest",
                ));
            }
            if state.endpoint != options.endpoint {
                return Err(AppError::invalid(format!(
                    "this spool is being sent to {}; prepare a new spool for another receiver",
                    state.endpoint
                )));
            }
            state
        }
        None => {
            let state = SenderState {
                endpoint: options.endpoint.clone(),
                idempotency_key: uuid::Uuid::new_v4().simple().to_string(),
                manifest_sha256: spool.validated.digest.clone(),
                session_id: None,
            };
            // Saved before the request, so a lost response can be recovered with the same key.
            snapshot::save_state(&spool.root, &state)?;
            state
        }
    };
    if let Some(session_id) = &state.session_id {
        return Ok(session_id.clone());
    }
    let headers = [
        (IDEMPOTENCY_KEY_HEADER, state.idempotency_key.clone()),
        (MANIFEST_DIGEST_HEADER, spool.validated.digest.clone()),
    ];
    let (_, bytes) = connection
        .request(
            Method::POST,
            "/v1/sessions",
            &headers,
            Some(bytes::Bytes::from(spool.manifest_bytes.clone())),
            None,
        )
        .await?;
    let created: CreateSessionResponse = serde_json::from_slice(&bytes)
        .map_err(|e| AppError::environment(format!("unexpected create response: {e}")))?;
    tracing::info!(session_id = %created.session_id, "session ready");
    state.session_id = Some(created.session_id.clone());
    snapshot::save_state(&spool.root, &state)?;
    Ok(created.session_id)
}

/// Sends a prepared spool until the receiver reports it published.
pub async fn send(options: SendOptions) -> Result<SendReport, AppError> {
    let started = Instant::now();
    let connection = connect(&options)?;
    let spool = Arc::new(snapshot::load(&options.spool)?);
    let session_id = ensure_session(&connection, &spool, &options).await?;
    let log = Arc::new(UploadLog::open(&spool.root)?);
    let mut attempted_this_run = 0;
    let mut rounds = 0u64;
    let mut commit_accepted_at: Option<Instant> = None;
    let final_status = loop {
        let status = connection.status(&session_id).await?;
        match status.state {
            SessionState::Published => break status,
            SessionState::Faulted | SessionState::Aborted => {
                return Err(AppError::rejected(format!(
                    "session {session_id} is {}: {}",
                    status.state.as_str(),
                    status.last_error.unwrap_or_default()
                )));
            }
            SessionState::Preparing | SessionState::Verifying | SessionState::Publishing => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            SessionState::Receiving => {
                commit_accepted_at = None;
                let missing = connection.all_missing(&session_id).await?;
                if !missing.is_empty() {
                    rounds += 1;
                    if rounds > options.settings.max_rounds as u64 {
                        return Err(AppError::environment(format!(
                            "transfer did not converge after {} upload rounds",
                            options.settings.max_rounds
                        )));
                    }
                    tracing::info!(
                        round = rounds,
                        chunks = missing.len(),
                        "uploading missing chunks"
                    );
                    attempted_this_run += upload_round(
                        &connection,
                        &spool,
                        &session_id,
                        missing,
                        options.settings.upload_workers,
                        &log,
                    )
                    .await?;
                    // Pages are not an atomic snapshot; read again from the start (§6).
                    continue;
                }
                match connection
                    .request(
                        Method::POST,
                        &format!("/v1/sessions/{session_id}/commit"),
                        &[],
                        None,
                        None,
                    )
                    .await
                {
                    Ok((_, bytes)) => {
                        let response: StateResponse =
                            serde_json::from_slice(&bytes).map_err(|e| {
                                AppError::environment(format!("unexpected commit response: {e}"))
                            })?;
                        tracing::info!(state = response.state.as_str(), "commit accepted");
                        commit_accepted_at = Some(Instant::now());
                    }
                    // The session changed meanwhile (for example a receipt was cleared);
                    // the next status read decides what to do.
                    Err(error) if error.remote_code.as_deref() == Some("state_conflict") => {
                        tracing::info!(message = %error.message, "commit refused; rechecking status");
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    };
    drop(log);
    let attempted_all = snapshot::logged_attempt_bytes(&spool.root);
    Ok(SendReport {
        session_id,
        state: final_status.state,
        destination: final_status.destination,
        total_bytes: spool.validated.total_bytes,
        attempted_bytes_this_run: attempted_this_run,
        attempted_bytes_all_runs: attempted_all,
        resent_bytes_all_runs: attempted_all.saturating_sub(spool.validated.total_bytes),
        upload_rounds: rounds,
        elapsed_millis: started.elapsed().as_millis() as u64,
        commit_to_published_millis: commit_accepted_at.map(|t| t.elapsed().as_millis() as u64),
    })
}

/// Reads the receiver's status of the session recorded in a spool.
pub async fn remote_status(options: SendOptions) -> Result<SessionStatus, AppError> {
    let connection = connect(&options)?;
    let state = snapshot::load_state(&options.spool)?
        .and_then(|s| s.session_id)
        .ok_or_else(|| AppError::invalid("this spool has no session yet"))?;
    connection.status(&state).await
}

/// Asks the receiver to abandon the session recorded in a spool.
pub async fn remote_abort(options: SendOptions) -> Result<StateResponse, AppError> {
    let connection = connect(&options)?;
    let session_id = snapshot::load_state(&options.spool)?
        .and_then(|s| s.session_id)
        .ok_or_else(|| AppError::invalid("this spool has no session yet"))?;
    Ok(connection
        .json(Method::POST, &format!("/v1/sessions/{session_id}/abort"))
        .await?
        .1)
}
