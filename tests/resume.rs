//! Milestone 3: durable receipts, the missing list and resumption after interruptions.

mod common;

use std::time::{Duration, Instant};

use common::*;
use reqwest::StatusCode;
use shardsync::protocol::{ChunkRef, SessionState};
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn truncated_body_leaves_no_receipt() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(CHUNK, 1));
    let session = api.new_session(&transfer).await;

    let mut stream = raw_tls(&env, &receiver).await;
    let head = format!(
        "PUT /v1/sessions/{session}/files/0/chunks/0 HTTP/1.1\r\nhost: localhost\r\n\
         authorization: Bearer {}\r\ncontent-length: {CHUNK}\r\n\r\n",
        env.token("demo")
    );
    stream.write_all(head.as_bytes()).await.unwrap();
    stream
        .write_all(&transfer.chunk(0, 0)[..MIB])
        .await
        .unwrap();
    stream.flush().await.unwrap();
    // Disconnect halfway through the chunk.
    drop(stream);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let status = api.status(&session).await;
    assert_eq!(status.durable_chunks, 0);
    assert_eq!(api.missing(&session).await, transfer.all_chunks());
    assert_eq!(api.diagnostics().await.chunk_writes, 0);
    // The exact chunk can simply be sent again.
    assert_eq!(
        api.upload(&session, &transfer, transfer.all_chunks()[0])
            .await
            .status,
        StatusCode::NO_CONTENT
    );
    assert!(api.missing(&session).await.is_empty());
}

#[tokio::test]
async fn missing_pages_cover_every_chunk_once_in_order() {
    let env = TestEnv::new();
    let receiver = Receiver::start_with(
        &env,
        ReceiverConfig {
            args: vec!["--missing-page-limit".into(), "3".into()],
            ..ReceiverConfig::default()
        },
    );
    let api = env.api(&receiver);
    let transfer = Transfer {
        directories: vec![],
        files: (0..10)
            .map(|i| (format!("f{i}"), pseudo_random(7 + i, i as u64)))
            .collect(),
    };
    let session = api.new_session(&transfer).await;
    for file_id in [1u32, 4, 5] {
        let chunk = ChunkRef {
            file_id,
            chunk_index: 0,
        };
        assert_eq!(
            api.upload(&session, &transfer, chunk).await.status,
            StatusCode::NO_CONTENT
        );
    }
    let first = api.get(&format!("/v1/sessions/{session}/missing")).await;
    assert_eq!(first.body["missing"].as_array().unwrap().len(), 3);
    assert_eq!(first.body["next_after"]["file_id"], 3);
    let missing = api.missing(&session).await;
    let expected: Vec<ChunkRef> = transfer
        .all_chunks()
        .into_iter()
        .filter(|c| ![1, 4, 5].contains(&c.file_id))
        .collect();
    assert_eq!(missing, expected);
    let too_big = api
        .get(&format!("/v1/sessions/{session}/missing?limit=4"))
        .await;
    assert_eq!(too_big.status, StatusCode::BAD_REQUEST);
    let status = api.status(&session).await;
    assert_eq!(status.durable_chunks, 3);
    assert_eq!(status.durable_bytes, (7 + 1) + (7 + 4) + (7 + 5));
}

fn session_from_state(spool: &std::path::Path) -> Option<String> {
    let text = std::fs::read_to_string(spool.join("sender-state.json")).ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    value["session_id"].as_str().map(str::to_string)
}

async fn wait_for_durable(api: &Api, spool: &std::path::Path, chunks: u64) -> String {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(session) = session_from_state(spool)
            && api.status(&session).await.durable_chunks >= chunks
        {
            return session;
        }
        assert!(Instant::now() < deadline, "transfer made no progress");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn sender_resumes_after_receiver_crash() {
    let env = TestEnv::new();
    let transfer = Transfer::single("data.bin", pseudo_random(6 * CHUNK + 99, 2));
    transfer.write_to(&env.path("source"));
    let spool = env.path("spool");
    parse_report(&env.prepare(&env.path("source/data.bin"), &spool));

    // Crash right after the third receipt commits, before its acknowledgement.
    let mut receiver = Receiver::start_with(
        &env,
        ReceiverConfig {
            faults: Some("after-receipt-commit@3:abort".into()),
            ..ReceiverConfig::default()
        },
    );
    let sender = env.spawn_send(&receiver, &spool, &["--workers", "2"]);
    receiver.wait_for_exit(Duration::from_secs(60));
    let receiver = receiver.restart(&env);
    let output = sender.wait_with_output().unwrap();
    let report = parse_report(&output);
    let session = report["session_id"].as_str().unwrap();
    transfer.assert_published(&env.completed(session).join("data"));
    let resent = report["resent_bytes_all_runs"].as_u64().unwrap();
    assert!(resent > 0, "the unacknowledged chunk must be resent");
    assert!(
        resent <= 4 * CHUNK as u64,
        "only in-flight chunks are resent: {resent}"
    );
    let diagnostics = env.api(&receiver).diagnostics().await;
    assert!(
        diagnostics.recovery.chunks_rehashed >= 3,
        "{:?}",
        diagnostics.recovery
    );
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn lost_create_response_recovers_same_session() {
    let env = TestEnv::new();
    std::fs::write(env.path("f.bin"), pseudo_random(CHUNK + 1, 3)).unwrap();
    let spool = env.path("spool");
    parse_report(&env.prepare(&env.path("f.bin"), &spool));
    // The session is durably Receiving, but the process dies before replying.
    let mut receiver = Receiver::start_with(
        &env,
        ReceiverConfig {
            faults: Some("after-create-commit:abort".into()),
            ..ReceiverConfig::default()
        },
    );
    let sender = env.spawn_send(&receiver, &spool, &[]);
    receiver.wait_for_exit(Duration::from_secs(30));
    assert_eq!(session_count(&env), 1);
    let _receiver = receiver.restart(&env);
    let report = parse_report(&sender.wait_with_output().unwrap());
    assert_eq!(report["state"], "published");
    assert_eq!(session_count(&env), 1, "the replay must reuse the session");
    assert_eq!(
        session_from_state(&spool).as_deref(),
        report["session_id"].as_str()
    );
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn sender_restart_resends_only_missing_chunks() {
    let env = TestEnv::new();
    let transfer = Transfer::single("f.bin", pseudo_random(8 * CHUNK, 4));
    transfer.write_to(&env.path("source"));
    let spool = env.path("spool");
    parse_report(&env.prepare(&env.path("source/f.bin"), &spool));
    let receiver = Receiver::start_with(
        &env,
        ReceiverConfig {
            faults: Some("after-receipt-commit:delay:250".into()),
            ..ReceiverConfig::default()
        },
    );
    let api = env.api(&receiver);
    let mut sender = env.spawn_send(&receiver, &spool, &["--workers", "1"]);
    let session = wait_for_durable(&api, &spool, 3).await;
    sender.kill().unwrap();
    sender.wait().unwrap();
    let durable_before = api.status(&session).await.durable_chunks;

    let report = parse_report(&env.send(&receiver, &spool));
    assert_eq!(report["session_id"], session.as_str());
    let this_run = report["attempted_bytes_this_run"].as_u64().unwrap();
    let remaining = (8 - durable_before) * CHUNK as u64;
    assert!(
        this_run <= remaining + CHUNK as u64,
        "{this_run} > {remaining} + one chunk"
    );
    transfer.assert_published(&env.completed(&session).join("data"));
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn sender_restart_with_missing_spool_fails_cleanly() {
    let env = TestEnv::new();
    std::fs::write(env.path("f.bin"), pseudo_random(4 * CHUNK, 5)).unwrap();
    let spool = env.path("spool");
    parse_report(&env.prepare(&env.path("f.bin"), &spool));
    let receiver = Receiver::start_with(
        &env,
        ReceiverConfig {
            faults: Some("after-receipt-commit:delay:300".into()),
            ..ReceiverConfig::default()
        },
    );
    let api = env.api(&receiver);
    let mut sender = env.spawn_send(&receiver, &spool, &["--workers", "1"]);
    let session = wait_for_durable(&api, &spool, 1).await;
    sender.kill().unwrap();
    sender.wait().unwrap();
    std::fs::remove_dir_all(&spool).unwrap();

    let output = env.send(&receiver, &spool);
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("not a complete spool"));
    // The receiver's session is untouched and can still be abandoned explicitly.
    let status = api.status(&session).await;
    assert_eq!(status.state, SessionState::Receiving);
    assert!(status.durable_chunks >= 1);
}
