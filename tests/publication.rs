//! Milestone 4: verification, the session state machine, abort semantics and atomic
//! no-replace publication.

mod common;

use common::*;
use reqwest::StatusCode;
use shardsync::protocol::{ChunkRef, SessionState};

#[tokio::test]
async fn commit_requires_every_receipt_and_replays_return_the_same_result() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(2 * CHUNK, 1));
    let session = api.new_session(&transfer).await;
    api.upload(&session, &transfer, transfer.all_chunks()[0])
        .await;
    let early = api.commit(&session).await;
    assert_eq!(early.status, StatusCode::CONFLICT);
    assert!(early.body["message"].as_str().unwrap().contains("1 chunks"));
    assert_eq!(api.status(&session).await.state, SessionState::Receiving);

    api.upload_all(&session, &transfer).await;
    assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
    let status = api.wait_for(&session, SessionState::Published).await;
    assert_eq!(status.verified_bytes, 2 * CHUNK as u64);
    let first = api.commit(&session).await;
    let second = api.commit(&session).await;
    assert_eq!(first.status, StatusCode::OK);
    assert_eq!(first.body["destination"], second.body["destination"]);
    // A published session refuses further chunks and abort, and nothing is deleted.
    let reply = api
        .upload(&session, &transfer, transfer.all_chunks()[0])
        .await;
    assert_eq!(reply.status, StatusCode::CONFLICT);
    assert_eq!(api.abort(&session).await.status, StatusCode::CONFLICT);
    transfer.assert_published(&env.completed(&session).join("data"));
}

#[tokio::test]
async fn commit_detects_corruption_and_returns_to_receiving() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(3 * CHUNK, 2));
    let session = api.new_session(&transfer).await;
    api.upload_all(&session, &transfer).await;
    // Corrupt staged bytes after their receipts were recorded.
    let staged = env.staged_data(&session).join("f");
    let mut bytes = std::fs::read(&staged).unwrap();
    bytes[2 * CHUNK + 1] ^= 0x80;
    std::fs::write(&staged, &bytes).unwrap();

    assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
    let status = api.wait_for(&session, SessionState::Receiving).await;
    assert!(status.last_error.unwrap().contains("damaged chunks"));
    assert_eq!(
        api.missing(&session).await,
        vec![ChunkRef {
            file_id: 0,
            chunk_index: 2
        }]
    );
    assert!(
        !env.completed(&session).exists(),
        "incomplete output stays private"
    );
    api.upload_all(&session, &transfer).await;
    assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
    api.wait_for(&session, SessionState::Published).await;
    transfer.assert_published(&env.completed(&session).join("data"));
}

#[tokio::test]
async fn deleted_staging_file_is_rebuilt_and_resent() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(CHUNK + 10, 3));
    let session = api.new_session(&transfer).await;
    api.upload_all(&session, &transfer).await;
    std::fs::remove_file(env.staged_data(&session).join("f")).unwrap();
    assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
    api.wait_for(&session, SessionState::Receiving).await;
    assert_eq!(api.missing(&session).await, transfer.all_chunks());
    api.upload_all(&session, &transfer).await;
    assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
    api.wait_for(&session, SessionState::Published).await;
    transfer.assert_published(&env.completed(&session).join("data"));
}

#[tokio::test]
async fn all_chunks_ok_but_full_hash_wrong_faults() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(CHUNK + 7, 4));
    // An internally inconsistent manifest: correct chunk digests, wrong full digest.
    let mut manifest = transfer.manifest();
    manifest.files[0].sha256 = "ab".repeat(32);
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let reply = api
        .create_raw(
            "inconsistent",
            &shardsync::manifest::sha256_hex(&bytes),
            bytes,
        )
        .await;
    assert_eq!(reply.status, StatusCode::CREATED);
    let session = reply.body["session_id"].as_str().unwrap().to_string();
    api.upload_all(&session, &transfer).await;
    assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
    let status = api.wait_for(&session, SessionState::Faulted).await;
    assert!(status.last_error.unwrap().contains("inconsistent"));
    assert!(!env.completed(&session).exists());
    // Faulted is terminal: no endless retry loop, and a new transfer may start.
    assert_eq!(api.commit(&session).await.status, StatusCode::CONFLICT);
    api.new_session(&Transfer::single("g", vec![1])).await;
}

#[tokio::test]
async fn existing_destination_faults_without_overwrite() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(100, 5));
    let session = api.new_session(&transfer).await;
    api.upload_all(&session, &transfer).await;
    let occupied = env.completed(&session);
    std::fs::create_dir_all(&occupied).unwrap();
    std::fs::write(occupied.join("keep.txt"), b"operator data").unwrap();

    assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
    let status = api.wait_for(&session, SessionState::Faulted).await;
    assert!(status.last_error.unwrap().contains("already exists"));
    assert_eq!(
        std::fs::read(occupied.join("keep.txt")).unwrap(),
        b"operator data"
    );
    assert_eq!(std::fs::read_dir(&occupied).unwrap().count(), 1);
    // The verified staging tree is still there for an operator.
    assert!(env.staged_data(&session).join("f").exists());
}

#[tokio::test]
async fn abort_semantics_and_cleanup() {
    let env = TestEnv::new();
    let mut receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(100, 6));
    let session = api.new_session(&transfer).await;
    api.upload_all(&session, &transfer).await;
    let first = api.abort(&session).await;
    assert_eq!(first.status, StatusCode::OK);
    assert_eq!(first.body["state"], "aborted");
    assert_eq!(
        api.abort(&session).await.status,
        StatusCode::OK,
        "repeat abort succeeds"
    );
    assert_eq!(api.commit(&session).await.status, StatusCode::CONFLICT);
    let chunk = transfer.all_chunks()[0];
    assert_eq!(
        api.upload(&session, &transfer, chunk).await.status,
        StatusCode::CONFLICT
    );

    // A published session for comparison: cleanup must never touch completed output.
    let kept = Transfer::single("kept", pseudo_random(10, 7));
    let published = api.new_session(&kept).await;
    api.upload_all(&published, &kept).await;
    api.commit(&published).await;
    api.wait_for(&published, SessionState::Published).await;

    // Cleanup refuses to run alongside a live receiver.
    let root = env.root();
    let busy = env.cli(&["cleanup", "--root", root.to_str().unwrap()]);
    assert_eq!(busy.status.code(), Some(4));
    receiver.kill();
    std::fs::create_dir(root.join("staging/orphan-dir")).unwrap();
    let report = parse_report(&env.cli(&["cleanup", "--root", root.to_str().unwrap()]));
    assert_eq!(report["removed_aborted"][0], session.as_str());
    assert_eq!(report["orphans"][0], "orphan-dir");
    assert!(report["removed_orphans"].as_array().unwrap().is_empty());
    assert!(!root.join("staging").join(&session).exists());
    assert!(root.join("staging/orphan-dir").exists());
    kept.assert_published(&env.completed(&published).join("data"));
    let report = parse_report(&env.cli(&[
        "cleanup",
        "--root",
        root.to_str().unwrap(),
        "--remove-orphans",
    ]));
    assert_eq!(report["removed_orphans"][0], "orphan-dir");
    kept.assert_published(&env.completed(&published).join("data"));
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn abort_during_verify_is_409_and_verification_continues() {
    let env = TestEnv::new();
    let gate = env.path("gate");
    let receiver = Receiver::start_with(
        &env,
        ReceiverConfig {
            faults: Some(format!("during-verify:pause:{}", gate.display())),
            ..ReceiverConfig::default()
        },
    );
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(2 * CHUNK, 8));
    let session = api.new_session(&transfer).await;
    api.upload_all(&session, &transfer).await;
    assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
    wait_for_path(
        &env.path("gate.reached"),
        std::time::Duration::from_secs(10),
    );

    // The worker is mid-verification, yet it answers promptly.
    let reply = api.abort(&session).await;
    assert_eq!(reply.status, StatusCode::CONFLICT);
    assert_eq!(reply.code(), "state_conflict");
    let chunk = transfer.all_chunks()[0];
    assert_eq!(
        api.upload(&session, &transfer, chunk).await.status,
        StatusCode::CONFLICT
    );
    assert_eq!(
        api.commit(&session).await.status,
        StatusCode::ACCEPTED,
        "replay while verifying"
    );
    assert_eq!(api.status(&session).await.state, SessionState::Verifying);

    std::fs::write(&gate, b"").unwrap();
    api.wait_for(&session, SessionState::Published).await;
    transfer.assert_published(&env.completed(&session).join("data"));
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn client_disconnect_does_not_cancel_publication() {
    let env = TestEnv::new();
    let gate = env.path("gate");
    let receiver = Receiver::start_with(
        &env,
        ReceiverConfig {
            faults: Some(format!("during-verify:pause:{}", gate.display())),
            ..ReceiverConfig::default()
        },
    );
    let transfer = Transfer::single("f", pseudo_random(CHUNK + 1, 9));
    let session = {
        let api = env.api(&receiver);
        let session = api.new_session(&transfer).await;
        api.upload_all(&session, &transfer).await;
        assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
        session
        // The committing client and its connections are dropped here.
    };
    wait_for_path(
        &env.path("gate.reached"),
        std::time::Duration::from_secs(10),
    );
    std::fs::write(&gate, b"").unwrap();
    let observer = env.api(&receiver);
    observer.wait_for(&session, SessionState::Published).await;
    transfer.assert_published(&env.completed(&session).join("data"));
}
