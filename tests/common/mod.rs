//! Shared harness for the integration tests.
//!
//! Every test runs the real `shardsync` binary in separate processes: a receiver that can be
//! killed and restarted on the same port, and (where relevant) the sender CLI. Requests that
//! need precise control use a small HTTPS client built from the library's own TLS settings.
#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Output, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use serde_json::Value;
use shardsync::config::{CHUNK_SIZE_BYTES, chunk_count, chunk_length, chunk_offset};
use shardsync::manifest::{FileEntry, Manifest, sha256_hex};
use shardsync::protocol::{
    ChunkRef, MissingPage, ReceiverDiagnostics, SessionState, SessionStatus,
};

pub const MIB: usize = 1024 * 1024;
pub const CHUNK: usize = CHUNK_SIZE_BYTES as usize;

/// Whether this run must exercise the supported durability profile.
pub fn require_validated() -> bool {
    std::env::var_os("SHARDSYNC_REQUIRE_VALIDATED").is_some_and(|v| v == "1")
}

pub fn binary() -> &'static str {
    env!("CARGO_BIN_EXE_shardsync")
}

/// Deterministic, incompressible test bytes.
pub fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut bytes = Vec::with_capacity(len);
    while bytes.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.extend_from_slice(&state.to_le_bytes());
    }
    bytes.truncate(len);
    bytes
}

/// A temporary workspace with development certificates for owners `demo` and `other`.
pub struct TestEnv {
    pub dir: tempfile::TempDir,
}

impl TestEnv {
    pub fn new() -> TestEnv {
        let dir = match std::env::var_os("SHARDSYNC_TEST_ROOT") {
            Some(root) => {
                std::fs::create_dir_all(&root).unwrap();
                tempfile::tempdir_in(root).unwrap()
            }
            None => tempfile::tempdir().unwrap(),
        };
        shardsync::devcert::write_dev_setup(
            &dir.path().join("dev"),
            &["demo".to_string(), "other".to_string()],
        )
        .unwrap();
        TestEnv { dir }
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }
    pub fn dev(&self, name: &str) -> PathBuf {
        self.dir.path().join("dev").join(name)
    }
    pub fn root(&self) -> PathBuf {
        self.path("receiver")
    }
    pub fn token(&self, owner: &str) -> String {
        std::fs::read_to_string(self.dev(&format!("token-{owner}.txt"))).unwrap()
    }
    pub fn client(&self) -> reqwest::Client {
        shardsync::tls::http_client(
            &self.dev("ca.pem"),
            Duration::from_secs(5),
            Duration::from_secs(60),
        )
        .unwrap()
    }
    pub fn api(&self, receiver: &Receiver) -> Api {
        Api {
            client: self.client(),
            base: receiver.base.clone(),
            token: self.token("demo"),
        }
    }
    pub fn api_as(&self, receiver: &Receiver, owner: &str) -> Api {
        Api {
            token: self.token(owner),
            ..self.api(receiver)
        }
    }

    /// True when the receiver root is outside the supported profile (macOS, tmpfs, ...).
    ///
    /// With `SHARDSYNC_REQUIRE_VALIDATED=1` (set by `scripts/linux-test.sh` and CI) the harness
    /// refuses to fall back to the development profile, so a green run proves the receiver
    /// ran on the supported Linux/ext4 profile.
    pub fn needs_unvalidated_flag(&self) -> bool {
        let profile = shardsync::filesystem::filesystem_profile(self.dir.path());
        let supported = profile.as_ref().map(|p| p.supported).unwrap_or(false);
        if !supported && require_validated() {
            panic!(
                "SHARDSYNC_REQUIRE_VALIDATED is set but the test root is on {:?}",
                profile.map(|p| p.name)
            );
        }
        !supported
    }

    pub fn completed(&self, session_id: &str) -> PathBuf {
        self.root().join("completed").join(session_id)
    }
    pub fn staged_data(&self, session_id: &str) -> PathBuf {
        self.root()
            .join("staging")
            .join(session_id)
            .join("tree/data")
    }

    /// Runs the CLI to completion.
    pub fn cli(&self, args: &[&str]) -> Output {
        Command::new(binary())
            .args(args)
            .env("SHARDSYNC_LOG", "warn")
            .output()
            .unwrap()
    }

    pub fn prepare(&self, source: &Path, spool: &Path) -> Output {
        self.cli(&[
            "prepare",
            source.to_str().unwrap(),
            "--spool",
            spool.to_str().unwrap(),
        ])
    }

    pub fn send_args(&self, receiver: &Receiver, spool: &Path) -> Vec<String> {
        vec![
            "send".into(),
            "--spool".into(),
            spool.display().to_string(),
            "--endpoint".into(),
            receiver.base.clone(),
            "--ca".into(),
            self.dev("ca.pem").display().to_string(),
            "--token-file".into(),
            self.dev("token-demo.txt").display().to_string(),
        ]
    }

    pub fn send(&self, receiver: &Receiver, spool: &Path) -> Output {
        let args = self.send_args(receiver, spool);
        Command::new(binary())
            .args(&args)
            .env("SHARDSYNC_LOG", "warn")
            .output()
            .unwrap()
    }

    /// Starts `shardsync send` in the background.
    pub fn spawn_send(&self, receiver: &Receiver, spool: &Path, extra: &[&str]) -> Child {
        let mut args = self.send_args(receiver, spool);
        args.extend(extra.iter().map(|s| s.to_string()));
        Command::new(binary())
            .args(&args)
            .env("SHARDSYNC_LOG", "warn")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    }
}

/// Options for one receiver process.
#[derive(Clone, Default)]
pub struct ReceiverConfig {
    pub port: Option<u16>,
    pub faults: Option<String>,
    pub args: Vec<String>,
}

pub struct Receiver {
    pub child: Child,
    pub address: SocketAddr,
    pub base: String,
    pub log: PathBuf,
}

impl Receiver {
    pub fn start(env: &TestEnv) -> Receiver {
        Receiver::start_with(env, ReceiverConfig::default())
    }

    pub fn start_with(env: &TestEnv, config: ReceiverConfig) -> Receiver {
        let listen = format!("127.0.0.1:{}", config.port.unwrap_or(0));
        let log = env.path(&format!("receiver-{}.log", uuid::Uuid::new_v4().simple()));
        let mut command = Command::new(binary());
        command
            .args([
                "receive",
                "--root",
                env.root().to_str().unwrap(),
                "--listen",
                &listen,
                "--tls-cert",
                env.dev("server-cert.pem").to_str().unwrap(),
                "--tls-key",
                env.dev("server-key.pem").to_str().unwrap(),
                "--tokens",
                env.dev("tokens.json").to_str().unwrap(),
            ])
            .args(&config.args)
            .env("SHARDSYNC_LOG", "info")
            .stdout(Stdio::piped())
            .stderr(std::fs::File::create(&log).unwrap());
        if env.needs_unvalidated_flag() {
            command.arg("--allow-unvalidated-platform");
        }
        match &config.faults {
            Some(faults) => command.env("SHARDSYNC_FAULT", faults),
            None => command.env_remove("SHARDSYNC_FAULT"),
        };
        let mut child = command.spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Some(address) = line.strip_prefix("SHARDSYNC_LISTENING ") {
                    let _ = sender.send(address.parse::<SocketAddr>().unwrap());
                }
            }
        });
        let address = match receiver.recv_timeout(Duration::from_secs(30)) {
            Ok(address) => address,
            Err(_) => {
                let _ = child.kill();
                panic!(
                    "receiver did not start:\n{}",
                    std::fs::read_to_string(&log).unwrap_or_default()
                );
            }
        };
        Receiver {
            child,
            base: format!("https://localhost:{}", address.port()),
            address,
            log,
        }
    }

    pub fn port(&self) -> u16 {
        self.address.port()
    }

    /// SIGKILL, as an abrupt crash would.
    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Waits for the process to exit on its own (for example at an `abort` fault point).
    pub fn wait_for_exit(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            if Instant::now() > deadline {
                panic!("receiver did not exit:\n{}", self.log_text());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Restarts on the same port without fault injection.
    pub fn restart(mut self, env: &TestEnv) -> Receiver {
        self.kill();
        let port = self.port();
        Receiver::start_with(
            env,
            ReceiverConfig {
                port: Some(port),
                ..ReceiverConfig::default()
            },
        )
    }

    pub fn restart_with(mut self, env: &TestEnv, mut config: ReceiverConfig) -> Receiver {
        self.kill();
        config.port = Some(self.port());
        Receiver::start_with(env, config)
    }

    pub fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A transfer described in memory: directories and file contents.
#[derive(Clone)]
pub struct Transfer {
    pub directories: Vec<String>,
    pub files: Vec<(String, Vec<u8>)>,
}

impl Transfer {
    pub fn single(name: &str, bytes: Vec<u8>) -> Transfer {
        Transfer {
            directories: vec![],
            files: vec![(name.to_string(), bytes)],
        }
    }

    pub fn manifest(&self) -> Manifest {
        Manifest {
            protocol_version: 1,
            chunk_size_bytes: CHUNK_SIZE_BYTES,
            directories: self.directories.clone(),
            files: self
                .files
                .iter()
                .enumerate()
                .map(|(id, (path, bytes))| FileEntry {
                    id: id as u32,
                    relative_path: path.clone(),
                    size_bytes: bytes.len() as u64,
                    sha256: sha256_hex(bytes),
                    chunk_sha256: (0..chunk_count(bytes.len() as u64))
                        .map(|i| sha256_hex(self.chunk_of(bytes, i as u32)))
                        .collect(),
                })
                .collect(),
        }
    }

    pub fn manifest_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&self.manifest()).unwrap()
    }

    fn chunk_of<'a>(&self, bytes: &'a [u8], index: u32) -> &'a [u8] {
        let length = chunk_length(bytes.len() as u64, index).unwrap() as usize;
        let start = chunk_offset(index) as usize;
        &bytes[start..start + length]
    }

    pub fn chunk(&self, file_id: u32, index: u32) -> Vec<u8> {
        self.chunk_of(&self.files[file_id as usize].1, index)
            .to_vec()
    }

    pub fn all_chunks(&self) -> Vec<ChunkRef> {
        let mut refs = Vec::new();
        for (file_id, (_, bytes)) in self.files.iter().enumerate() {
            for index in 0..chunk_count(bytes.len() as u64) {
                refs.push(ChunkRef {
                    file_id: file_id as u32,
                    chunk_index: index as u32,
                });
            }
        }
        refs
    }

    pub fn total_bytes(&self) -> u64 {
        self.files.iter().map(|(_, b)| b.len() as u64).sum()
    }

    /// Writes the transfer as a real directory tree (for the CLI).
    pub fn write_to(&self, dir: &Path) {
        std::fs::create_dir_all(dir).unwrap();
        for directory in &self.directories {
            std::fs::create_dir_all(dir.join(directory)).unwrap();
        }
        for (path, bytes) in &self.files {
            std::fs::write(dir.join(path), bytes).unwrap();
        }
    }

    /// Asserts that a published `data` directory holds exactly this transfer.
    pub fn assert_published(&self, data: &Path) {
        let mut expected_paths: Vec<String> = self
            .directories
            .iter()
            .cloned()
            .chain(self.files.iter().map(|(p, _)| p.clone()))
            .collect();
        expected_paths.sort();
        let mut found = Vec::new();
        collect_paths(data, data, &mut found);
        found.sort();
        assert_eq!(found, expected_paths, "published tree structure");
        for (path, bytes) in &self.files {
            let published = std::fs::read(data.join(path)).unwrap();
            assert!(published == *bytes, "published bytes differ for {path}");
        }
    }
}

fn collect_paths(base: &Path, dir: &Path, found: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        let relative = path
            .strip_prefix(base)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        found.push(relative);
        if entry.file_type().unwrap().is_dir() {
            collect_paths(base, &path, found);
        }
    }
}

/// An authenticated HTTPS client for one receiver.
#[derive(Clone)]
pub struct Api {
    pub client: reqwest::Client,
    pub base: String,
    pub token: String,
}

pub struct Reply {
    pub status: StatusCode,
    pub body: Value,
}

impl Reply {
    pub fn code(&self) -> &str {
        self.body["code"].as_str().unwrap_or("")
    }
}

impl Api {
    async fn reply(response: reqwest::Response) -> Reply {
        let status = response.status();
        let bytes = response.bytes().await.unwrap();
        let body = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        };
        Reply { status, body }
    }

    pub async fn create_raw(&self, key: &str, digest: &str, manifest: Vec<u8>) -> Reply {
        let response = self
            .client
            .post(format!("{}/v1/sessions", self.base))
            .bearer_auth(&self.token)
            .header("idempotency-key", key)
            .header("manifest-sha256", digest)
            .body(manifest)
            .send()
            .await
            .unwrap();
        Api::reply(response).await
    }

    /// Like `create_raw`, but returns `None` when the receiver closed the connection before
    /// answering, which is allowed for a body it refuses to read (413).
    pub async fn try_create_raw(
        &self,
        key: &str,
        digest: &str,
        manifest: Vec<u8>,
    ) -> Option<Reply> {
        let response = self
            .client
            .post(format!("{}/v1/sessions", self.base))
            .bearer_auth(&self.token)
            .header("idempotency-key", key)
            .header("manifest-sha256", digest)
            .body(manifest)
            .send()
            .await
            .ok()?;
        Some(Api::reply(response).await)
    }

    /// Like `put_chunk`, but returns `None` when the connection closed before an answer.
    pub async fn try_put_chunk(
        &self,
        session: &str,
        file_id: u32,
        index: u32,
        bytes: Vec<u8>,
    ) -> Option<Reply> {
        let response = self
            .client
            .put(format!(
                "{}/v1/sessions/{session}/files/{file_id}/chunks/{index}",
                self.base
            ))
            .bearer_auth(&self.token)
            .body(bytes)
            .send()
            .await
            .ok()?;
        Some(Api::reply(response).await)
    }

    pub async fn create(&self, key: &str, transfer: &Transfer) -> Reply {
        let bytes = transfer.manifest_bytes();
        let digest = sha256_hex(&bytes);
        self.create_raw(key, &digest, bytes).await
    }

    /// Creates a session and returns its ID, asserting 201.
    pub async fn new_session(&self, transfer: &Transfer) -> String {
        let key = uuid::Uuid::new_v4().simple().to_string();
        let reply = self.create(&key, transfer).await;
        assert_eq!(reply.status, StatusCode::CREATED, "{}", reply.body);
        reply.body["session_id"].as_str().unwrap().to_string()
    }

    pub async fn put_chunk(
        &self,
        session: &str,
        file_id: u32,
        index: u32,
        bytes: Vec<u8>,
    ) -> Reply {
        let response = self
            .client
            .put(format!(
                "{}/v1/sessions/{session}/files/{file_id}/chunks/{index}",
                self.base
            ))
            .bearer_auth(&self.token)
            .body(bytes)
            .send()
            .await
            .unwrap();
        Api::reply(response).await
    }

    pub async fn upload(&self, session: &str, transfer: &Transfer, chunk: ChunkRef) -> Reply {
        self.put_chunk(
            session,
            chunk.file_id,
            chunk.chunk_index,
            transfer.chunk(chunk.file_id, chunk.chunk_index),
        )
        .await
    }

    pub async fn upload_all(&self, session: &str, transfer: &Transfer) {
        for chunk in transfer.all_chunks() {
            let reply = self.upload(session, transfer, chunk).await;
            assert_eq!(reply.status, StatusCode::NO_CONTENT, "{}", reply.body);
        }
    }

    pub async fn get(&self, path: &str) -> Reply {
        let response = self
            .client
            .get(format!("{}{path}", self.base))
            .bearer_auth(&self.token)
            .send()
            .await
            .unwrap();
        Api::reply(response).await
    }

    pub async fn post(&self, path: &str) -> Reply {
        let response = self
            .client
            .post(format!("{}{path}", self.base))
            .bearer_auth(&self.token)
            .send()
            .await
            .unwrap();
        Api::reply(response).await
    }

    pub async fn status(&self, session: &str) -> SessionStatus {
        let reply = self.get(&format!("/v1/sessions/{session}")).await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        serde_json::from_value(reply.body).unwrap()
    }

    pub async fn missing(&self, session: &str) -> Vec<ChunkRef> {
        let mut all = Vec::new();
        let mut query = String::new();
        loop {
            let reply = self
                .get(&format!("/v1/sessions/{session}/missing{query}"))
                .await;
            assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
            let page: MissingPage = serde_json::from_value(reply.body).unwrap();
            for file in page.missing {
                for chunk_index in file.chunk_indexes {
                    all.push(ChunkRef {
                        file_id: file.file_id,
                        chunk_index,
                    });
                }
            }
            match page.next_after {
                Some(next) => {
                    query = format!(
                        "?after_file={}&after_chunk={}",
                        next.file_id, next.chunk_index
                    )
                }
                None => return all,
            }
        }
    }

    pub async fn commit(&self, session: &str) -> Reply {
        self.post(&format!("/v1/sessions/{session}/commit")).await
    }

    pub async fn abort(&self, session: &str) -> Reply {
        self.post(&format!("/v1/sessions/{session}/abort")).await
    }

    pub async fn diagnostics(&self) -> ReceiverDiagnostics {
        let reply = self.get("/v1/receiver").await;
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        serde_json::from_value(reply.body).unwrap()
    }

    /// Polls until the session reaches `state`.
    pub async fn wait_for(&self, session: &str, state: SessionState) -> SessionStatus {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let status = self.status(session).await;
            if status.state == state {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "session never became {state:?}; last status {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Polls until the receiver answers at all (after a restart).
    pub async fn wait_until_up(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while self
            .client
            .get(format!("{}/v1/receiver", self.base))
            .bearer_auth(&self.token)
            .send()
            .await
            .is_err()
        {
            assert!(Instant::now() < deadline, "receiver never came up");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

/// Waits until `path` exists (used with `pause:<gate>` fault points).
pub fn wait_for_path(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "{} never appeared",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Session row counts read directly from the receiver's database (read-only).
pub fn session_count(env: &TestEnv) -> i64 {
    let connection =
        shardsync::metadata::open_read_only(&env.root().join("metadata/receiver.sqlite")).unwrap();
    connection
        .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .unwrap()
}

pub fn parse_report(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "command failed ({:?}):\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

/// Opens a raw TLS connection to the receiver for hand-written HTTP requests.
pub async fn raw_tls(
    env: &TestEnv,
    receiver: &Receiver,
) -> tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
    let config = shardsync::tls::client_config(&env.dev("ca.pem")).unwrap();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
    let tcp = tokio::net::TcpStream::connect(receiver.address)
        .await
        .unwrap();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    connector.connect(name, tcp).await.unwrap()
}
