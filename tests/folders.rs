//! Milestone 5: folder manifests, exact structure, and path safety on both ends.

mod common;

use common::*;
use reqwest::StatusCode;
use shardsync::manifest::sha256_hex;
use shardsync::protocol::SessionState;

#[tokio::test]
async fn exact_structure_published() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let transfer = Transfer {
        directories: vec![
            "docs".into(),
            "docs/empty".into(),
            "models".into(),
            "models/v1".into(),
            "models/v1/deep".into(),
        ],
        files: vec![
            ("README".into(), b"read me".to_vec()),
            ("docs/zero-a".into(), vec![]),
            ("models/v1/deep/zero-b".into(), vec![]),
            (
                "models/v1/weights.bin".into(),
                pseudo_random(2 * CHUNK + 333, 1),
            ),
            (
                "models/v1/notes with spaces.txt".into(),
                b"spaces are fine".to_vec(),
            ),
        ],
    };
    let source = env.path("export");
    transfer.write_to(&source);
    let spool = env.path("spool");
    let prepared = parse_report(&env.prepare(&source, &spool));
    assert_eq!(prepared["files"], 5);
    assert_eq!(prepared["directories"], 5);
    let report = parse_report(&env.send(&receiver, &spool));
    let session = report["session_id"].as_str().unwrap();
    transfer.assert_published(&env.completed(session).join("data"));
}

#[tokio::test]
async fn malicious_paths_rejected() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let outside = env.path("outside-target");
    for (index, path) in [
        "../escape",
        "/etc/passwd",
        "a/../../b",
        "a\\b",
        "C:file",
        "a//b",
        "./a",
    ]
    .into_iter()
    .enumerate()
    {
        let mut manifest = Transfer::single("x", vec![1]).manifest();
        manifest.files[0].relative_path = path.to_string();
        let bytes = serde_json::to_vec(&manifest).unwrap();
        let reply = api
            .create_raw(&format!("path-{index}"), &sha256_hex(&bytes), bytes)
            .await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{path}");
        assert_eq!(reply.code(), "invalid_manifest", "{path}");
    }
    // An unlisted parent, and a file used as a directory.
    let mut manifest = Transfer::single("x", vec![1]).manifest();
    manifest.files[0].relative_path = "missing-parent/x".into();
    let bytes = serde_json::to_vec(&manifest).unwrap();
    assert_eq!(
        api.create_raw("parent", &sha256_hex(&bytes), bytes)
            .await
            .status,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(session_count(&env), 0);
    assert!(!outside.exists());
}

#[tokio::test]
async fn staging_symlink_is_refused_without_writing_outside() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer {
        directories: vec!["sub".into()],
        files: vec![("sub/f".into(), pseudo_random(100, 2))],
    };
    let session = api.new_session(&transfer).await;
    // Swap a staged directory for a symlink to somewhere else.
    let outside = env.path("outside");
    std::fs::create_dir(&outside).unwrap();
    let staged_sub = env.staged_data(&session).join("sub");
    std::fs::remove_dir_all(&staged_sub).unwrap();
    std::os::unix::fs::symlink(&outside, &staged_sub).unwrap();

    let reply = api
        .upload(&session, &transfer, transfer.all_chunks()[0])
        .await;
    assert!(reply.status.is_server_error(), "{}", reply.status);
    assert_eq!(
        std::fs::read_dir(&outside).unwrap().count(),
        0,
        "nothing written outside"
    );
    assert_eq!(api.status(&session).await.durable_chunks, 0);
}

#[tokio::test]
async fn sender_refuses_special_files() {
    let env = TestEnv::new();
    let with_link = env.path("with-link");
    std::fs::create_dir_all(with_link.join("d")).unwrap();
    std::fs::write(with_link.join("d/real"), b"x").unwrap();
    std::os::unix::fs::symlink("real", with_link.join("d/link")).unwrap();
    let output = env.prepare(&with_link, &env.path("spool-1"));
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("symbolic link"));

    let with_fifo = env.path("with-fifo");
    std::fs::create_dir(&with_fifo).unwrap();
    assert!(
        std::process::Command::new("mkfifo")
            .arg(with_fifo.join("pipe"))
            .status()
            .unwrap()
            .success()
    );
    let output = env.prepare(&with_fifo, &env.path("spool-2"));
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("FIFO"));

    let output = env.prepare(&with_link.join("d/link"), &env.path("spool-3"));
    assert_eq!(output.status.code(), Some(2));
}

#[tokio::test]
async fn folder_transfer_resumes_across_files() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer {
        directories: vec!["a".into(), "b".into()],
        files: vec![
            ("a/1".into(), pseudo_random(CHUNK + 1, 3)),
            ("b/2".into(), pseudo_random(10, 4)),
            ("b/3".into(), vec![]),
        ],
    };
    let session = api.new_session(&transfer).await;
    let chunks = transfer.all_chunks();
    api.upload(&session, &transfer, chunks[2]).await;
    assert_eq!(api.missing(&session).await, chunks[..2].to_vec());
    api.upload_all(&session, &transfer).await;
    assert_eq!(api.commit(&session).await.status, StatusCode::ACCEPTED);
    api.wait_for(&session, SessionState::Published).await;
    transfer.assert_published(&env.completed(&session).join("data"));
}
