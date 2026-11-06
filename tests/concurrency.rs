//! Milestone 6: bounded buffers, backpressure, body limits and racing commands.

mod common;

use std::time::{Duration, Instant};

use common::*;
use reqwest::StatusCode;
use shardsync::protocol::SessionState;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn duplicate_puts_write_once() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(CHUNK, 1));
    let session = api.new_session(&transfer).await;
    let chunk = transfer.all_chunks()[0];
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..6 {
        let (api, transfer, session) = (api.clone(), transfer.clone(), session.clone());
        tasks.spawn(async move { api.upload(&session, &transfer, chunk).await.status });
    }
    while let Some(status) = tasks.join_next().await {
        assert_eq!(status.unwrap(), StatusCode::NO_CONTENT);
    }
    let diagnostics = api.diagnostics().await;
    assert_eq!(diagnostics.chunk_writes, 1, "one durable logical receipt");
    assert_eq!(diagnostics.duplicate_chunk_requests, 5);
    assert_eq!(api.status(&session).await.durable_chunks, 1);
}

#[tokio::test]
async fn abort_commit_race() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let (mut aborted, mut published) = (0, 0);
    for round in 0..12u64 {
        let transfer = Transfer::single("f", pseudo_random(1000, round));
        let session = api.new_session(&transfer).await;
        api.upload_all(&session, &transfer).await;
        let (abort, commit) = tokio::join!(api.abort(&session), api.commit(&session));
        match (abort.status, commit.status) {
            (StatusCode::OK, StatusCode::CONFLICT) => {
                aborted += 1;
                assert_eq!(api.status(&session).await.state, SessionState::Aborted);
                assert!(!env.completed(&session).exists());
            }
            (StatusCode::CONFLICT, StatusCode::ACCEPTED) => {
                published += 1;
                api.wait_for(&session, SessionState::Published).await;
                transfer.assert_published(&env.completed(&session).join("data"));
            }
            other => panic!("inconsistent outcome {other:?}"),
        }
    }
    assert_eq!(aborted + published, 12);
}

#[tokio::test]
async fn body_limit_without_content_length() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(CHUNK, 2));
    let session = api.new_session(&transfer).await;
    // A chunked body with no Content-Length that keeps going past 4 MiB.
    let pieces: Vec<Result<Vec<u8>, std::io::Error>> =
        (0..5).map(|_| Ok(vec![0x5Au8; MIB + 1])).collect();
    let body = reqwest::Body::wrap_stream(futures_stream(pieces));
    let response = api
        .client
        .put(format!(
            "{}/v1/sessions/{session}/files/0/chunks/0",
            api.base
        ))
        .bearer_auth(&api.token)
        .body(body)
        .send()
        .await;
    match response {
        Ok(response) => assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE),
        // The receiver may close the connection while the client is still sending.
        Err(error) => assert!(error.is_request() || error.is_body(), "{error}"),
    }
    assert_eq!(api.status(&session).await.durable_chunks, 0);
    assert_eq!(api.diagnostics().await.chunk_writes, 0);
}

/// A minimal `Stream` over prepared pieces, avoiding an extra dependency.
fn futures_stream(
    pieces: Vec<Result<Vec<u8>, std::io::Error>>,
) -> impl futures_core::Stream<Item = Result<Vec<u8>, std::io::Error>> + Send + 'static {
    struct Pieces(std::collections::VecDeque<Result<Vec<u8>, std::io::Error>>);
    impl futures_core::Stream for Pieces {
        type Item = Result<Vec<u8>, std::io::Error>;
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            std::task::Poll::Ready(self.0.pop_front())
        }
    }
    Pieces(pieces.into())
}

#[tokio::test]
async fn slow_client_times_out_and_releases_permit() {
    let env = TestEnv::new();
    let receiver = Receiver::start_with(
        &env,
        ReceiverConfig {
            args: vec![
                "--chunk-permits".into(),
                "1".into(),
                "--body-idle-timeout-ms".into(),
                "500".into(),
                "--permit-wait-ms".into(),
                "3000".into(),
            ],
            ..ReceiverConfig::default()
        },
    );
    let api = env.api(&receiver);
    let transfer = Transfer {
        directories: vec![],
        files: vec![
            ("a".into(), pseudo_random(CHUNK, 3)),
            ("b".into(), pseudo_random(100, 4)),
        ],
    };
    let session = api.new_session(&transfer).await;

    // A client that sends headers and a little of the body, then stalls, holding the only
    // buffer permit.
    let mut slow = raw_tls(&env, &receiver).await;
    let head = format!(
        "PUT /v1/sessions/{session}/files/0/chunks/0 HTTP/1.1\r\nhost: localhost\r\n\
         authorization: Bearer {}\r\ncontent-length: {CHUNK}\r\n\r\n",
        env.token("demo")
    );
    slow.write_all(head.as_bytes()).await.unwrap();
    slow.write_all(&[1u8; 1000]).await.unwrap();
    slow.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let started = Instant::now();
    let chunk = transfer.all_chunks()[1];
    let reply = api.upload(&session, &transfer, chunk).await;
    assert_eq!(reply.status, StatusCode::NO_CONTENT, "{}", reply.body);
    let waited = started.elapsed();
    assert!(
        waited >= Duration::from_millis(250),
        "the permit was busy: {waited:?}"
    );
    assert!(
        waited < Duration::from_secs(3),
        "the stalled body released it: {waited:?}"
    );

    // The stalled request was answered with a timeout.
    let mut response = vec![0u8; 512];
    let read = tokio::time::timeout(Duration::from_secs(5), slow.read(&mut response))
        .await
        .unwrap()
        .unwrap_or(0);
    let text = String::from_utf8_lossy(&response[..read]);
    assert!(text.is_empty() || text.contains("408"), "{text}");
    assert_eq!(api.status(&session).await.durable_chunks, 1);
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn buffer_high_water_at_most_8() {
    let env = TestEnv::new();
    // Make storage slower than the network so buffers pile up behind the worker.
    let receiver = Receiver::start_with(
        &env,
        ReceiverConfig {
            faults: Some("after-chunk-sync:delay:40".into()),
            args: vec!["--permit-wait-ms".into(), "200".into()],
            ..ReceiverConfig::default()
        },
    );
    let api = env.api(&receiver);
    let transfer = Transfer {
        directories: vec![],
        files: (0..24)
            .map(|i| (format!("f{i}"), pseudo_random(CHUNK, i as u64)))
            .collect(),
    };
    let session = api.new_session(&transfer).await;
    let mut tasks = tokio::task::JoinSet::new();
    let busy = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    for chunk in transfer.all_chunks() {
        let (api, transfer, session, busy) =
            (api.clone(), transfer.clone(), session.clone(), busy.clone());
        tasks.spawn(async move {
            loop {
                let reply = api.upload(&session, &transfer, chunk).await;
                match reply.status {
                    StatusCode::NO_CONTENT => return,
                    StatusCode::TOO_MANY_REQUESTS => {
                        busy.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    other => panic!("unexpected {other}: {}", reply.body),
                }
            }
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }
    let diagnostics = api.diagnostics().await;
    assert!(
        diagnostics.buffer_high_water <= 8,
        "{}",
        diagnostics.buffer_high_water
    );
    assert!(
        diagnostics.buffer_high_water >= 2,
        "uploads really overlapped"
    );
    assert!(
        busy.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "backpressure returned 429"
    );
    assert!(api.missing(&session).await.is_empty());
    assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
    api.wait_for(&session, SessionState::Published).await;
}

#[tokio::test]
async fn connection_limit_closes_extra_connections() {
    let env = TestEnv::new();
    let receiver = Receiver::start_with(
        &env,
        ReceiverConfig {
            args: vec!["--max-connections".into(), "2".into()],
            ..ReceiverConfig::default()
        },
    );
    let _first = raw_tls(&env, &receiver).await;
    let _second = raw_tls(&env, &receiver).await;
    // A third connection is accepted by the kernel but closed before the TLS handshake.
    let config = shardsync::tls::client_config(&env.dev("ca.pem")).unwrap();
    let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
    let tcp = tokio::net::TcpStream::connect(receiver.address)
        .await
        .unwrap();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let third = tokio::time::timeout(Duration::from_secs(5), connector.connect(name, tcp)).await;
    assert!(
        matches!(third, Ok(Err(_))),
        "third connection must be refused"
    );
    drop(_first);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let api = env.api(&receiver);
    api.wait_until_up().await;
}
