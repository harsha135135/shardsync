//! HTTPS routing, authentication and admission limits for the receiver (§6, §9).
//!
//! Handlers are thin: they authenticate, parse and bound the request, then hand ownership of
//! the work to the storage worker and await its reply. They never touch staging files or the
//! writable database. Status reads come from the worker's cached snapshot.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, RawQuery, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use http_body_util::BodyExt;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};

use crate::api_error::ApiError;
use crate::config::{
    CHUNK_SIZE_BYTES, MAX_IDEMPOTENCY_KEY_BYTES, MAX_MANIFEST_BYTES, ReceiverLimits,
};
use crate::devcert::TokensFile;
use crate::error::AppError;
use crate::filesystem::{self, FilesystemProfile};
use crate::manifest::{ManifestError, parse_and_validate};
use crate::progress::{Counters, PermittedChunk, ProgressSnapshot, peak_rss_bytes};
use crate::protocol::{
    ChunkRef, CreateSessionResponse, ErrorBody, ErrorCode, IDEMPOTENCY_KEY_HEADER,
    MANIFEST_DIGEST_HEADER, REQUEST_ID_HEADER, ReceiverDiagnostics, RecoveryReport,
};
use crate::storage::{CommitOutcome, ReceiverPaths, StorageCommand, StorageWorker};
use crate::{metadata, tls};

/// Everything `shardsync receive` needs.
#[derive(Clone, Debug)]
pub struct ReceiverOptions {
    pub root: PathBuf,
    pub listen: SocketAddr,
    pub tls_cert: PathBuf,
    pub tls_key: PathBuf,
    pub tokens_file: PathBuf,
    /// Allows a filesystem outside the supported Linux/ext4 profile. Durability claims do not
    /// apply there; status reports the profile as unvalidated.
    pub allow_unvalidated_platform: bool,
    pub limits: ReceiverLimits,
}

/// State shared by every handler. Immutable except for the atomics in `counters`, the
/// semaphores, and the watch receiver (which only reads).
struct Shared {
    /// Owner name and SHA-256 of each accepted bearer token.
    tokens: Vec<(String, [u8; 32])>,
    limits: ReceiverLimits,
    storage: mpsc::Sender<StorageCommand>,
    progress: watch::Receiver<ProgressSnapshot>,
    counters: Arc<Counters>,
    chunk_permits: Arc<Semaphore>,
    metadata_permits: Arc<Semaphore>,
    profile: FilesystemProfile,
    recovery: RecoveryReport,
}

type AppState = Arc<Shared>;

tokio::task_local! {
    static REQUEST_ID: String;
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let request_id = REQUEST_ID.try_with(Clone::clone).unwrap_or_default();
        let status = StatusCode::from_u16(self.code.http_status())
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        if status.is_server_error() {
            tracing::warn!(request_id, code = self.code.as_str(), message = %self.message, "request failed");
        } else {
            tracing::debug!(request_id, code = self.code.as_str(), message = %self.message, "request rejected");
        }
        let body = ErrorBody {
            code: self.code.as_str().to_string(),
            message: self.message,
            request_id,
        };
        let mut response = (status, Json(body)).into_response();
        if let Some(seconds) = self.retry_after_secs {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(seconds));
        }
        response
    }
}

/// Assigns a request ID, makes it available to error responses, and echoes it as a header.
async fn request_id_layer(request: Request, next: Next) -> Response {
    let id = uuid::Uuid::new_v4().simple().to_string();
    let mut response = REQUEST_ID.scope(id.clone(), next.run(request)).await;
    if let Ok(value) = HeaderValue::from_str(&id) {
        response.headers_mut().insert(REQUEST_ID_HEADER, value);
    }
    response
}

/// Resolves the bearer token to its owner. Every configured token is compared in constant
/// time, without stopping early, so timing does not reveal which entry matched.
fn authenticate(state: &Shared, headers: &HeaderMap) -> Result<String, ApiError> {
    let unauthorized = || ApiError::new(ErrorCode::Unauthorized, "missing or invalid bearer token");
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(unauthorized)?;
    let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    let mut owner = None;
    for (name, expected) in &state.tokens {
        if bool::from(digest.ct_eq(expected)) {
            owner = Some(name.clone());
        }
    }
    owner.ok_or_else(unauthorized)
}

fn is_session_id(text: &str) -> bool {
    text.len() == 32
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Sends a command to the storage worker and waits for its reply.
async fn ask<T>(
    state: &Shared,
    build: impl FnOnce(oneshot::Sender<Result<T, ApiError>>) -> StorageCommand,
) -> Result<T, ApiError> {
    let (reply, response) = oneshot::channel();
    let stopped = || {
        ApiError::new(
            ErrorCode::StorageUnavailable,
            "storage worker is not running",
        )
    };
    state
        .storage
        .send(build(reply))
        .await
        .map_err(|_| stopped())?;
    response.await.map_err(|_| stopped())?
}

fn declared_length(headers: &HeaderMap) -> Result<Option<u64>, ApiError> {
    match headers.get(header::CONTENT_LENGTH) {
        None => Ok(None),
        Some(value) => value
            .to_str()
            .ok()
            .and_then(|text| text.parse::<u64>().ok())
            .map(Some)
            .ok_or_else(|| ApiError::invalid_chunk("invalid Content-Length")),
    }
}

/// Reads a request body into `buffer`, never holding more than `limit` bytes, and failing if
/// the body stalls longer than `idle`. The limit applies even without a Content-Length.
async fn read_body(
    body: Body,
    limit: usize,
    idle: Duration,
    mut buffer: Vec<u8>,
    too_large: ApiError,
) -> Result<Vec<u8>, ApiError> {
    let mut body = body;
    loop {
        let frame = match tokio::time::timeout(idle, body.frame()).await {
            Err(_) => {
                return Err(ApiError::new(
                    ErrorCode::RequestTimeout,
                    "request body stalled past the idle timeout",
                ));
            }
            Ok(None) => return Ok(buffer),
            Ok(Some(Err(error))) => {
                return Err(ApiError::invalid_chunk(format!(
                    "request body ended early: {error}"
                )));
            }
            Ok(Some(Ok(frame))) => frame,
        };
        if let Ok(data) = frame.into_data() {
            if buffer.len() + data.len() > limit {
                return Err(too_large);
            }
            buffer.extend_from_slice(&data);
        }
    }
}

/// Reads and discards up to `limit` body bytes before an early rejection, so an HTTP/1.1
/// client that is still uploading receives the response instead of a reset connection.
/// Larger or stalled bodies are abandoned and the connection is closed instead. Oversized
/// (413), unauthenticated (401) and backpressure (429) rejections never read the body.
async fn discard_body(body: Body, limit: usize, idle: Duration) {
    let mut body = body;
    let mut seen = 0usize;
    while let Ok(Some(Ok(frame))) = tokio::time::timeout(idle, body.frame()).await {
        if let Ok(data) = frame.into_data() {
            seen += data.len();
            if seen > limit {
                return;
            }
        }
    }
}

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

/// Validates the idempotency key and manifest digest headers of a creation request.
fn creation_headers(headers: &HeaderMap) -> Result<(String, String), ApiError> {
    let bad_request = |message: &str| ApiError::new(ErrorCode::InvalidManifest, message);
    let key = header_text(headers, IDEMPOTENCY_KEY_HEADER)
        .filter(|key| {
            !key.is_empty()
                && key.len() <= MAX_IDEMPOTENCY_KEY_BYTES
                && key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
        })
        .ok_or_else(|| bad_request("Idempotency-Key must be 1-128 characters [A-Za-z0-9-_.:]"))?
        .to_string();
    let claimed_digest = header_text(headers, MANIFEST_DIGEST_HEADER)
        .filter(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        .ok_or_else(|| bad_request("Manifest-Sha256 must be 64 lowercase hex characters"))?
        .to_string();
    Ok((key, claimed_digest))
}

async fn create_session(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    let owner = authenticate(&state, &headers)?;
    let (key, claimed_digest) = match creation_headers(&headers) {
        Ok(values) => values,
        Err(error) => {
            discard_body(body, MAX_MANIFEST_BYTES, state.limits.body_idle_timeout).await;
            return Err(error);
        }
    };
    let bad_request = |message: &str| ApiError::new(ErrorCode::InvalidManifest, message);
    let too_large = ApiError::new(
        ErrorCode::QuotaExceeded,
        format!("manifest exceeds {MAX_MANIFEST_BYTES} bytes"),
    );
    if let Some(length) = declared_length(&headers)?
        && length > MAX_MANIFEST_BYTES as u64
    {
        return Err(too_large);
    }
    let _permit = tokio::time::timeout(
        state.limits.permit_wait,
        state.metadata_permits.clone().acquire_owned(),
    )
    .await
    .map_err(|_| ApiError::busy("too many manifest uploads in progress"))?
    .map_err(|_| ApiError::busy("receiver is shutting down"))?;
    let bytes = read_body(
        body,
        MAX_MANIFEST_BYTES,
        state.limits.body_idle_timeout,
        Vec::new(),
        too_large,
    )
    .await?;
    // Parsing a 16 MiB document is CPU work; keep it off the async executor threads.
    let (bytes, validated) =
        tokio::task::spawn_blocking(move || parse_and_validate(&bytes).map(|v| (bytes, v)))
            .await
            .map_err(|_| ApiError::new(ErrorCode::Internal, "manifest validation panicked"))?
            .map(|(bytes, validated)| (bytes, Box::new(validated)))
            .map_err(|error| match error {
                ManifestError::Invalid(message) => {
                    ApiError::new(ErrorCode::InvalidManifest, message)
                }
                ManifestError::Quota(message) => ApiError::new(ErrorCode::QuotaExceeded, message),
            })?;
    if validated.digest != claimed_digest {
        return Err(bad_request(
            "Manifest-Sha256 does not match the uploaded bytes",
        ));
    }
    let outcome = ask(&state, |reply| StorageCommand::Create {
        owner,
        idempotency_key: key,
        manifest_bytes: bytes,
        validated,
        reply,
    })
    .await?;
    let status = if outcome.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let body = CreateSessionResponse {
        session_id: outcome.session_id,
        state: outcome.state,
        chunk_size_bytes: CHUNK_SIZE_BYTES,
        max_chunk_body_bytes: CHUNK_SIZE_BYTES,
        missing_page_limit: state.limits.missing_page_limit,
    };
    Ok((status, Json(body)).into_response())
}

/// Looks up a session in the status snapshot, enforcing ownership.
fn visible_status(
    state: &Shared,
    owner: &str,
    session_id: &str,
) -> Result<crate::protocol::SessionStatus, ApiError> {
    if !is_session_id(session_id) {
        return Err(ApiError::not_found());
    }
    let snapshot = state.progress.borrow();
    match snapshot.sessions.get(session_id) {
        Some(view) if view.owner == owner => Ok(view.status.clone()),
        _ => Err(ApiError::not_found()),
    }
}

async fn session_status(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let owner = authenticate(&state, &headers)?;
    let status = visible_status(&state, &owner, &session_id)?;
    Ok(Json(status).into_response())
}

/// Parses `after_file`, `after_chunk` and `limit` from the query string.
fn parse_missing_query(
    query: Option<&str>,
    max_limit: usize,
) -> Result<(Option<ChunkRef>, usize), ApiError> {
    let bad = |message: &str| ApiError::invalid_chunk(message);
    let (mut after_file, mut after_chunk, mut limit) = (None, None, max_limit);
    for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
        let (name, value) = pair.split_once('=').ok_or_else(|| bad("malformed query"))?;
        match name {
            "after_file" => {
                after_file = Some(
                    value
                        .parse::<u32>()
                        .map_err(|_| bad("after_file must be a u32"))?,
                )
            }
            "after_chunk" => {
                after_chunk = Some(
                    value
                        .parse::<u32>()
                        .map_err(|_| bad("after_chunk must be a u32"))?,
                )
            }
            "limit" => {
                limit = value
                    .parse::<usize>()
                    .map_err(|_| bad("limit must be an integer"))?;
                if limit == 0 || limit > max_limit {
                    return Err(bad(&format!("limit must be between 1 and {max_limit}")));
                }
            }
            _ => return Err(bad("unknown query parameter")),
        }
    }
    let after = match (after_file, after_chunk) {
        (Some(file_id), Some(chunk_index)) => Some(ChunkRef {
            file_id,
            chunk_index,
        }),
        (None, None) => None,
        _ => return Err(bad("after_file and after_chunk must be given together")),
    };
    Ok((after, limit))
}

async fn missing_chunks(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let owner = authenticate(&state, &headers)?;
    visible_status(&state, &owner, &session_id)?;
    let (after, limit) = parse_missing_query(query.as_deref(), state.limits.missing_page_limit)?;
    let page = ask(&state, |reply| StorageCommand::Missing {
        owner,
        session_id,
        after,
        limit,
        reply,
    })
    .await?;
    Ok(Json(page).into_response())
}

async fn put_chunk(
    State(state): State<AppState>,
    Path((session_id, file, index)): Path<(String, String, String)>,
    headers: HeaderMap,
    body: Body,
) -> Result<StatusCode, ApiError> {
    let owner = authenticate(&state, &headers)?;
    // Refuse early, before buffering a body, when the request or the snapshot already shows
    // the outcome. The storage worker rechecks the durable state when the write runs.
    let early = (|| {
        let file_id: u32 = file
            .parse()
            .map_err(|_| ApiError::invalid_chunk("file ID must be an unsigned 32-bit integer"))?;
        let chunk_index: u32 = index.parse().map_err(|_| {
            ApiError::invalid_chunk("chunk index must be an unsigned 32-bit integer")
        })?;
        let status = visible_status(&state, &owner, &session_id)?;
        if status.state != crate::protocol::SessionState::Receiving {
            return Err(ApiError::state_conflict(format!(
                "session is {}; chunks are accepted only while receiving",
                status.state.as_str()
            )));
        }
        Ok((file_id, chunk_index, declared_length(&headers)?))
    })();
    let (file_id, chunk_index, declared) = match early {
        Ok(values) => values,
        Err(error) => {
            let limit = CHUNK_SIZE_BYTES as usize;
            discard_body(body, limit, state.limits.body_idle_timeout).await;
            return Err(error);
        }
    };
    let too_large = ApiError::new(
        ErrorCode::QuotaExceeded,
        format!("chunk body exceeds {CHUNK_SIZE_BYTES} bytes"),
    );
    if declared.is_some_and(|length| length > CHUNK_SIZE_BYTES) {
        return Err(too_large);
    }
    // The permit is acquired before any body byte is read and stays with the buffer.
    let permit = tokio::time::timeout(
        state.limits.permit_wait,
        state.chunk_permits.clone().acquire_owned(),
    )
    .await
    .map_err(|_| ApiError::busy("all chunk buffers are in use"))?
    .map_err(|_| ApiError::busy("receiver is shutting down"))?;
    let mut chunk = PermittedChunk::new(permit, state.counters.clone());
    let capacity = declared.unwrap_or(CHUNK_SIZE_BYTES).min(CHUNK_SIZE_BYTES) as usize;
    chunk.bytes = read_body(
        body,
        CHUNK_SIZE_BYTES as usize,
        state.limits.body_idle_timeout,
        Vec::with_capacity(capacity),
        too_large,
    )
    .await?;
    ask(&state, |reply| StorageCommand::WriteChunk {
        owner,
        session_id,
        file_id,
        chunk_index,
        chunk,
        reply,
    })
    .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn commit_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let owner = authenticate(&state, &headers)?;
    visible_status(&state, &owner, &session_id)?;
    let outcome = ask(&state, |reply| StorageCommand::Commit {
        owner,
        session_id,
        reply,
    })
    .await?;
    Ok(match outcome {
        CommitOutcome::Accepted(body) => (StatusCode::ACCEPTED, Json(body)).into_response(),
        CommitOutcome::Published(body) => (StatusCode::OK, Json(body)).into_response(),
    })
}

async fn abort_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let owner = authenticate(&state, &headers)?;
    visible_status(&state, &owner, &session_id)?;
    let body = ask(&state, |reply| StorageCommand::Abort {
        owner,
        session_id,
        reply,
    })
    .await?;
    Ok(Json(body).into_response())
}

async fn receiver_diagnostics(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    authenticate(&state, &headers)?;
    let body = ReceiverDiagnostics {
        durability_profile: if state.profile.supported {
            "validated"
        } else {
            "unvalidated"
        }
        .into(),
        filesystem: state.profile.name.clone(),
        fault_injection_enabled: cfg!(feature = "fault-injection"),
        chunk_writes: state
            .counters
            .chunk_writes
            .load(std::sync::atomic::Ordering::Relaxed),
        duplicate_chunk_requests: state
            .counters
            .duplicate_chunk_requests
            .load(Ordering::Relaxed),
        chunk_buffer_permits: state.limits.chunk_buffer_permits,
        buffer_high_water: state.counters.buffer_high_water(),
        chunk_sync_micros: state.counters.chunk_sync_micros.load(Ordering::Relaxed),
        receipt_commit_micros: state.counters.receipt_commit_micros.load(Ordering::Relaxed),
        verify_micros: state.counters.verify_micros.load(Ordering::Relaxed),
        publish_micros: state.counters.publish_micros.load(Ordering::Relaxed),
        peak_rss_bytes: peak_rss_bytes(),
        recovery: state.recovery.clone(),
    };
    Ok(Json(body).into_response())
}

async fn not_found() -> ApiError {
    ApiError::new(ErrorCode::NotFound, "no such endpoint")
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/sessions", post(create_session))
        .route("/v1/sessions/{id}", get(session_status))
        .route("/v1/sessions/{id}/missing", get(missing_chunks))
        .route(
            "/v1/sessions/{id}/files/{file}/chunks/{index}",
            put(put_chunk),
        )
        .route("/v1/sessions/{id}/commit", post(commit_session))
        .route("/v1/sessions/{id}/abort", post(abort_session))
        .route("/v1/receiver", get(receiver_diagnostics))
        .fallback(not_found)
        .layer(middleware::from_fn(request_id_layer))
        .with_state(state)
}

fn load_tokens(path: &std::path::Path) -> Result<Vec<(String, [u8; 32])>, String> {
    use std::os::unix::fs::PermissionsExt;
    let metadata =
        std::fs::metadata(path).map_err(|e| format!("tokens file {}: {e}", path.display()))?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(format!(
            "tokens file {} must not be readable by group or others (chmod 600)",
            path.display()
        ));
    }
    let bytes = std::fs::read(path).map_err(|e| format!("read tokens file: {e}"))?;
    crate::json_checks::reject_duplicate_keys(&bytes).map_err(|e| format!("tokens file: {e}"))?;
    let parsed: TokensFile =
        serde_json::from_slice(&bytes).map_err(|e| format!("tokens file: {e}"))?;
    let mut tokens = Vec::new();
    for client in parsed.clients {
        let digest = hex::decode(&client.token_sha256)
            .ok()
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
            .ok_or_else(|| {
                format!(
                    "token_sha256 for {} must be 64 hex characters",
                    client.owner
                )
            })?;
        if client.owner.is_empty() {
            return Err("token owner must not be empty".into());
        }
        tokens.push((client.owner, digest));
    }
    if tokens.is_empty() {
        return Err("tokens file lists no clients".into());
    }
    Ok(tokens)
}

/// Checks that staging and completed share one filesystem, so publication can be a rename.
fn require_same_filesystem(paths: &ReceiverPaths) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    let staging = std::fs::metadata(paths.staging()).map_err(|e| e.to_string())?;
    let completed = std::fs::metadata(paths.completed()).map_err(|e| e.to_string())?;
    if staging.dev() != completed.dev() {
        return Err("staging and completed must be on the same filesystem".into());
    }
    Ok(())
}

/// Starts the receiver: startup checks, recovery, then the HTTPS listener.
pub async fn run_receiver(options: ReceiverOptions) -> Result<(), AppError> {
    options.limits.validate().map_err(AppError::invalid)?;
    let paths = ReceiverPaths::new(&options.root);
    paths.create_layout().map_err(AppError::environment)?;
    let _lock = filesystem::lock_exclusive(&paths.lock_file())
        .map_err(|e| AppError::environment(format!("lock receiver root: {e}")))?;
    require_same_filesystem(&paths).map_err(AppError::environment)?;

    let profile = filesystem::filesystem_profile(&paths.root)
        .map_err(|e| AppError::environment(format!("inspect filesystem: {e}")))?;
    if !profile.supported {
        if !options.allow_unvalidated_platform {
            return Err(AppError::environment(format!(
                "receiver root is on {} ({}); the supported durability profile is Linux on ext4. \
                 Pass --allow-unvalidated-platform for development only",
                profile.name,
                std::env::consts::OS
            )));
        }
        tracing::warn!(filesystem = %profile.name, os = std::env::consts::OS,
            "running outside the supported Linux/ext4 profile; durability claims do not apply");
    }
    filesystem::probe_durability_operations(&paths.staging())
        .map_err(|e| AppError::environment(format!("startup self-check failed: {e}")))?;

    let tokens = load_tokens(&options.tokens_file).map_err(AppError::environment)?;
    let tls_config =
        tls::server_config(&options.tls_cert, &options.tls_key).map_err(AppError::environment)?;
    let db = metadata::open(&paths.database()).map_err(AppError::environment)?;

    let counters = Arc::new(Counters::default());
    let (progress_tx, progress_rx) = watch::channel(ProgressSnapshot::default());
    let mut worker = StorageWorker::new(paths.clone(), db, progress_tx, counters.clone());
    let recovery = worker.recover().map_err(AppError::environment)?;

    let (storage_tx, storage_rx) = mpsc::channel(options.limits.storage_queue_depth);
    let worker_thread = std::thread::Builder::new()
        .name("storage-worker".into())
        .spawn(move || {
            // Broken storage bookkeeping must not leave a silently dead worker behind.
            let outcome =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| worker.run(storage_rx)));
            if outcome.is_err() {
                eprintln!("storage worker panicked; aborting the receiver");
                std::process::abort();
            }
        })
        .map_err(|e| AppError::environment(format!("start storage worker: {e}")))?;

    let shared = Arc::new(Shared {
        tokens,
        limits: options.limits.clone(),
        storage: storage_tx.clone(),
        progress: progress_rx,
        counters,
        chunk_permits: Arc::new(Semaphore::new(options.limits.chunk_buffer_permits)),
        metadata_permits: Arc::new(Semaphore::new(options.limits.metadata_permits)),
        profile,
        recovery,
    });
    let app = router(shared);

    let listener = TcpListener::bind(options.listen)
        .await
        .map_err(|e| AppError::environment(format!("listen on {}: {e}", options.listen)))?;
    let address = listener
        .local_addr()
        .map_err(|e| AppError::environment(e.to_string()))?;
    // Machine-readable line for scripts and tests that start the receiver on port 0.
    println!("SHARDSYNC_LISTENING {address}");
    use std::io::Write as _;
    let _ = std::io::stdout().flush();
    tracing::info!(%address, "receiver listening");

    serve(listener, tls_config, app, &options.limits).await;

    // Let the worker finish the command it is running, then stop.
    let (reply, done) = oneshot::channel();
    if storage_tx
        .send(StorageCommand::Shutdown { reply })
        .await
        .is_ok()
    {
        let _ = done.await;
    }
    let _ = tokio::task::spawn_blocking(move || worker_thread.join()).await;
    tracing::info!("receiver stopped");
    Ok(())
}

/// Accepts TLS connections until SIGINT or SIGTERM. Each connection holds a permit from a
/// fixed pool; connections beyond the limit are closed at once.
async fn serve(
    listener: TcpListener,
    tls_config: Arc<rustls::ServerConfig>,
    app: Router,
    limits: &ReceiverLimits,
) {
    let acceptor = tokio_rustls::TlsAcceptor::from(tls_config);
    let connections = Arc::new(Semaphore::new(limits.max_connections));
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            _ = tokio::signal::ctrl_c() => break,
            _ = terminate.recv() => break,
        };
        let (stream, peer) = match accepted {
            Ok(pair) => pair,
            Err(error) => {
                tracing::warn!(%error, "accept failed");
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
        };
        let Ok(permit) = connections.clone().try_acquire_owned() else {
            tracing::warn!(%peer, "connection limit reached; closing connection");
            drop(stream);
            continue;
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        let handshake_timeout = limits.tls_handshake_timeout;
        let header_timeout = limits.header_read_timeout;
        tokio::spawn(async move {
            let _permit = permit;
            let tls = match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await {
                Ok(Ok(tls)) => tls,
                Ok(Err(error)) => {
                    tracing::debug!(%peer, %error, "TLS handshake failed");
                    return;
                }
                Err(_) => {
                    tracing::debug!(%peer, "TLS handshake timed out");
                    return;
                }
            };
            let service = hyper_util::service::TowerToHyperService::new(app);
            let result = hyper::server::conn::http1::Builder::new()
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(header_timeout)
                .serve_connection(hyper_util::rt::TokioIo::new(tls), service)
                .await;
            if let Err(error) = result {
                tracing::debug!(%peer, %error, "connection ended with an error");
            }
        });
    }
    tracing::info!("shutdown requested");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_query_parsing() {
        assert_eq!(parse_missing_query(None, 100).unwrap(), (None, 100));
        assert_eq!(
            parse_missing_query(Some("after_file=2&after_chunk=7&limit=5"), 100).unwrap(),
            (
                Some(ChunkRef {
                    file_id: 2,
                    chunk_index: 7
                }),
                5
            )
        );
        assert!(parse_missing_query(Some("after_file=2"), 100).is_err());
        assert!(parse_missing_query(Some("limit=0"), 100).is_err());
        assert!(parse_missing_query(Some("limit=101"), 100).is_err());
        assert!(parse_missing_query(Some("after_file=-1&after_chunk=0"), 100).is_err());
        assert!(parse_missing_query(Some("other=1"), 100).is_err());
    }

    #[test]
    fn session_ids_are_strict() {
        assert!(is_session_id(&"a".repeat(32)));
        assert!(!is_session_id(&"A".repeat(32)));
        assert!(!is_session_id("../etc"));
        assert!(!is_session_id(&"a".repeat(31)));
    }
}
