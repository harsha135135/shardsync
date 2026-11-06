//! Process-kill recovery at every documented durability boundary (§7, §8, §11).
//!
//! Each test crashes a real receiver process at a named fault point (or kills it outright),
//! restarts it on the same port, and checks the recovered state. These tests establish
//! process-crash recovery only; they say nothing about power loss.
#![cfg(feature = "fault-injection")]

mod common;

use std::time::Duration;

use common::*;
use reqwest::StatusCode;
use shardsync::protocol::{ChunkRef, SessionState};

const CRASH_WAIT: Duration = Duration::from_secs(30);

fn faulty(env: &TestEnv, faults: &str) -> Receiver {
    Receiver::start_with(
        env,
        ReceiverConfig {
            faults: Some(faults.into()),
            ..ReceiverConfig::default()
        },
    )
}

/// Starts a session that will crash at `faults`, uploads every chunk except those the
/// fault interrupts, and returns the receiver, session ID and transfer.
async fn session_with_faults(
    env: &TestEnv,
    faults: &str,
    transfer: &Transfer,
) -> (Receiver, String) {
    let receiver = faulty(env, faults);
    let session = env.api(&receiver).new_session(transfer).await;
    (receiver, session)
}

#[tokio::test]
async fn after_chunk_sync_chunk_is_resent() {
    let env = TestEnv::new();
    let transfer = Transfer::single("f", pseudo_random(1000, 1));
    let (mut receiver, session) =
        session_with_faults(&env, "after-chunk-sync:abort", &transfer).await;
    let api = env.api(&receiver);
    let chunk = transfer.all_chunks()[0];
    let result = api
        .client
        .put(format!(
            "{}/v1/sessions/{session}/files/0/chunks/0",
            api.base
        ))
        .bearer_auth(&api.token)
        .body(transfer.chunk(0, 0))
        .send()
        .await;
    assert!(result.is_err(), "the crash happens before any response");
    receiver.wait_for_exit(CRASH_WAIT);
    let receiver = receiver.restart(&env);
    let api = env.api(&receiver);
    // The bytes may be on disk, but there is no receipt, so the chunk is missing again.
    assert_eq!(api.missing(&session).await, vec![chunk]);
    assert_eq!(
        api.upload(&session, &transfer, chunk).await.status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(api.diagnostics().await.chunk_writes, 1);
}

#[tokio::test]
async fn after_receipt_commit_retry_is_idempotent() {
    let env = TestEnv::new();
    let transfer = Transfer::single("f", pseudo_random(1000, 2));
    let (mut receiver, session) =
        session_with_faults(&env, "after-receipt-commit:abort", &transfer).await;
    let api = env.api(&receiver);
    let chunk = transfer.all_chunks()[0];
    let _ = api
        .client
        .put(format!(
            "{}/v1/sessions/{session}/files/0/chunks/0",
            api.base
        ))
        .bearer_auth(&api.token)
        .body(transfer.chunk(0, 0))
        .send()
        .await;
    receiver.wait_for_exit(CRASH_WAIT);
    let receiver = receiver.restart(&env);
    let api = env.api(&receiver);
    // The receipt survived the crash; the retry succeeds without rewriting.
    assert!(api.missing(&session).await.is_empty());
    assert_eq!(
        api.upload(&session, &transfer, chunk).await.status,
        StatusCode::NO_CONTENT
    );
    let diagnostics = api.diagnostics().await;
    assert_eq!(diagnostics.chunk_writes, 0);
    assert_eq!(diagnostics.duplicate_chunk_requests, 1);
    assert_eq!(diagnostics.recovery.chunks_rehashed, 1);
    assert_eq!(api.status(&session).await.durable_chunks, 1);
}

#[tokio::test]
async fn restart_rehash_clears_corrupt_receipt() {
    let env = TestEnv::new();
    let transfer = Transfer::single("f", pseudo_random(2 * CHUNK, 3));
    let mut receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let session = api.new_session(&transfer).await;
    api.upload_all(&session, &transfer).await;
    receiver.kill();
    // Damage the second chunk on disk while the receiver is down.
    let staged = env.staged_data(&session).join("f");
    let mut bytes = std::fs::read(&staged).unwrap();
    bytes[CHUNK + 5] ^= 0x01;
    std::fs::write(&staged, &bytes).unwrap();

    let receiver = receiver.restart(&env);
    let api = env.api(&receiver);
    assert_eq!(
        api.missing(&session).await,
        vec![ChunkRef {
            file_id: 0,
            chunk_index: 1
        }]
    );
    let recovery = api.diagnostics().await.recovery;
    assert_eq!(recovery.receipts_cleared, 1);
    assert_eq!(recovery.chunks_rehashed, 2);
    assert_eq!(recovery.bytes_rehashed, 2 * CHUNK as u64);
    api.upload_all(&session, &transfer).await;
    assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
    api.wait_for(&session, SessionState::Published).await;
    transfer.assert_published(&env.completed(&session).join("data"));
}

#[tokio::test]
async fn injected_enospc_is_visible() {
    let env = TestEnv::new();
    let transfer = Transfer::single("f", pseudo_random(1000, 4));
    let (receiver, session) =
        session_with_faults(&env, "before-chunk-write@1:enospc", &transfer).await;
    let api = env.api(&receiver);
    let chunk = transfer.all_chunks()[0];
    let reply = api.upload(&session, &transfer, chunk).await;
    assert_eq!(reply.status, StatusCode::INSUFFICIENT_STORAGE);
    assert_eq!(reply.code(), "insufficient_storage");
    let status = api.status(&session).await;
    assert_eq!(status.durable_chunks, 0);
    assert!(status.last_error.unwrap().contains("No space"));
    assert!(
        std::fs::read_dir(env.root().join("completed"))
            .unwrap()
            .next()
            .is_none()
    );
    // After capacity recovers, the same chunk succeeds and clears the error.
    assert_eq!(
        api.upload(&session, &transfer, chunk).await.status,
        StatusCode::NO_CONTENT
    );
    assert_eq!(api.status(&session).await.last_error, None);
}

/// Runs only when `SHARDSYNC_SMALL_FS` names a small (256 MiB), empty ext4 mount: CI and
/// `scripts/linux-test.sh --small-fs` create one. In CI a missing mount fails the test.
#[tokio::test]
async fn real_enospc_on_small_ext4() {
    let Some(mount) = std::env::var_os("SHARDSYNC_SMALL_FS") else {
        // CI always provides the mount; a silent skip there would overstate the evidence.
        assert!(
            std::env::var_os("CI").is_none(),
            "CI must set SHARDSYNC_SMALL_FS for the real out-of-space test"
        );
        eprintln!("SHARDSYNC_SMALL_FS not set; skipping the real out-of-space test");
        return;
    };
    let base =
        std::path::PathBuf::from(mount).join(format!("run-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&base).unwrap();
    let env = TestEnv::new();
    // Point the receiver root at the small filesystem.
    std::os::unix::fs::symlink(&base, env.root()).unwrap_or_else(|_| {
        panic!("could not link receiver root");
    });
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(4 * CHUNK, 5));
    let session = api.new_session(&transfer).await;
    // Fill the filesystem after admission so the sparse staging file cannot grow.
    let filler = base.join("filler");
    let mut file = std::fs::File::create(&filler).unwrap();
    let block = vec![0xAAu8; MIB];
    use std::io::Write;
    while file.write_all(&block).is_ok() {}
    let _ = file.sync_all();
    let mut saw_full = false;
    for chunk in transfer.all_chunks() {
        let reply = api.upload(&session, &transfer, chunk).await;
        if reply.status == StatusCode::INSUFFICIENT_STORAGE {
            saw_full = true;
            break;
        }
    }
    assert!(saw_full, "a write onto a full filesystem must report 507");
    assert!(api.status(&session).await.last_error.is_some());
    assert!(
        std::fs::read_dir(base.join("completed"))
            .unwrap()
            .next()
            .is_none()
    );
    drop(file);
    std::fs::remove_file(&filler).unwrap();
    api.upload_all(&session, &transfer).await;
    assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
    api.wait_for(&session, SessionState::Published).await;
    transfer.assert_published(&base.join("completed").join(&session).join("data"));
    drop(receiver);
    // Leave the small filesystem empty for the next build profile's run.
    std::fs::remove_dir_all(&base).unwrap();
}

async fn crash_during_publication(point: &str) {
    let env = TestEnv::new();
    let transfer = Transfer {
        directories: vec!["d".into()],
        files: vec![
            ("d/a".into(), pseudo_random(CHUNK + 3, 6)),
            ("b".into(), vec![]),
        ],
    };
    let (mut receiver, session) =
        session_with_faults(&env, &format!("{point}:abort"), &transfer).await;
    let api = env.api(&receiver);
    api.upload_all(&session, &transfer).await;
    let reply = api.commit(&session).await;
    assert_eq!(reply.status, StatusCode::ACCEPTED);
    receiver.wait_for_exit(CRASH_WAIT);
    let receiver = receiver.restart(&env);
    let api = env.api(&receiver);
    let status = api.wait_for(&session, SessionState::Published).await;
    assert_eq!(
        status.destination.as_deref(),
        Some(format!("completed/{session}").as_str())
    );
    transfer.assert_published(&env.completed(&session).join("data"));
    assert!(
        !env.root()
            .join("staging")
            .join(&session)
            .join("tree")
            .exists()
    );
    // A commit replay returns the same destination.
    let replay = api.commit(&session).await;
    assert_eq!(replay.status, StatusCode::OK);
    assert_eq!(replay.body["destination"], format!("completed/{session}"));
}

#[tokio::test]
async fn before_publish_rename() {
    crash_during_publication("before-publish-rename").await;
}

#[tokio::test]
async fn after_publish_rename() {
    crash_during_publication("after-publish-rename").await;
}

#[tokio::test]
async fn after_published_commit() {
    crash_during_publication("after-published-commit").await;
}

#[tokio::test]
async fn restart_after_202_resumes_verification() {
    let env = TestEnv::new();
    let gate = env.path("verify-gate");
    let transfer = Transfer::single("f", pseudo_random(3 * CHUNK, 7));
    let (mut receiver, session) = session_with_faults(
        &env,
        &format!("during-verify:pause:{}", gate.display()),
        &transfer,
    )
    .await;
    let api = env.api(&receiver);
    api.upload_all(&session, &transfer).await;
    assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
    wait_for_path(&env.path("verify-gate.reached"), CRASH_WAIT);
    assert_eq!(api.status(&session).await.state, SessionState::Verifying);
    receiver.kill();
    let receiver = receiver.restart(&env);
    let api = env.api(&receiver);
    api.wait_for(&session, SessionState::Published).await;
    transfer.assert_published(&env.completed(&session).join("data"));
}

#[tokio::test]
async fn crash_during_preparation_is_reconstructed() {
    for point in ["after-preparing-commit", "after-staging-synced"] {
        let env = TestEnv::new();
        let transfer = Transfer {
            directories: vec!["x".into(), "x/y".into()],
            files: vec![("x/y/f".into(), pseudo_random(10, 8))],
        };
        let mut receiver = faulty(&env, &format!("{point}:abort"));
        let api = env.api(&receiver);
        let result = api
            .client
            .post(format!("{}/v1/sessions", api.base))
            .bearer_auth(&api.token)
            .header("idempotency-key", "prep")
            .header(
                "manifest-sha256",
                shardsync::manifest::sha256_hex(&transfer.manifest_bytes()),
            )
            .body(transfer.manifest_bytes())
            .send()
            .await;
        assert!(result.is_err(), "{point}: crash before the response");
        receiver.wait_for_exit(CRASH_WAIT);
        let receiver = receiver.restart(&env);
        let api = env.api(&receiver);
        // The replay finds the reconstructed session under the same identity.
        let replay = api.create("prep", &transfer).await;
        assert_eq!(replay.status, StatusCode::OK, "{point}: {}", replay.body);
        assert_eq!(replay.body["state"], "receiving");
        let session = replay.body["session_id"].as_str().unwrap().to_string();
        assert_eq!(session_count(&env), 1);
        api.upload_all(&session, &transfer).await;
        assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
        api.wait_for(&session, SessionState::Published).await;
        transfer.assert_published(&env.completed(&session).join("data"));
    }
}
