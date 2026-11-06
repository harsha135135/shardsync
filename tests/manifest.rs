//! Milestone 2: manifest admission, idempotent creation and the immutable spool.

mod common;

use common::*;
use reqwest::StatusCode;
use shardsync::manifest::sha256_hex;
use shardsync::protocol::SessionState;

#[tokio::test]
async fn invalid_manifests_are_rejected_without_creating_sessions() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let valid = Transfer::single("f", pseudo_random(10, 1)).manifest_bytes();
    let text = String::from_utf8(valid.clone()).unwrap();
    let cases: Vec<(&str, Vec<u8>, StatusCode)> = vec![
        ("not json", b"{".to_vec(), StatusCode::BAD_REQUEST),
        (
            "duplicate key",
            text.replacen(
                "{\"protocol_version\":1",
                "{\"protocol_version\":1,\"protocol_version\":1",
                1,
            )
            .into_bytes(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "unknown field",
            text.replacen(
                "{\"protocol_version\":1",
                "{\"protocol_version\":1,\"x\":0",
                1,
            )
            .into_bytes(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "escaping path",
            text.replace("\"relative_path\":\"f\"", "\"relative_path\":\"../f\"")
                .into_bytes(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "wrong chunk size",
            text.replace("4194304", "1048576").into_bytes(),
            StatusCode::BAD_REQUEST,
        ),
        (
            "size without matching chunk digests",
            text.replace("\"size_bytes\":10", "\"size_bytes\":107374182401")
                .into_bytes(),
            StatusCode::BAD_REQUEST,
        ),
    ];
    for (name, bytes, expected) in cases {
        let digest = sha256_hex(&bytes);
        let reply = api
            .create_raw(&format!("key-{}", name.replace(' ', "-")), &digest, bytes)
            .await;
        assert_eq!(reply.status, expected, "{name}: {}", reply.body);
        assert_eq!(reply.code(), "invalid_manifest", "{name}");
    }
    // A manifest larger than 16 MiB is refused from its declared length.
    // It is not read, so the client may see the 413 or a closed connection.
    let huge = vec![b' '; 16 * MIB + 1];
    if let Some(reply) = api
        .try_create_raw("key-huge", &sha256_hex(&huge), huge)
        .await
    {
        assert_eq!(reply.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(reply.code(), "quota_exceeded");
    }
    // The digest header must describe the exact uploaded bytes.
    let reply = api
        .create_raw("key-digest", &"0".repeat(64), valid.clone())
        .await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    // The idempotency key is required.
    let reply = api.create_raw("", &sha256_hex(&valid), valid).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert_eq!(session_count(&env), 0);
}

#[tokio::test]
async fn admission_caps_return_413() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let mut manifest = Transfer::single("f", vec![]).manifest();
    manifest.files = (0..10_001u32)
        .map(|id| shardsync::manifest::FileEntry {
            id,
            relative_path: format!("f{id}"),
            size_bytes: 0,
            sha256: sha256_hex(b""),
            chunk_sha256: vec![],
        })
        .collect();
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let reply = api.create_raw("caps", &sha256_hex(&bytes), bytes).await;
    assert_eq!(
        reply.status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "{}",
        reply.body
    );
    assert_eq!(reply.code(), "quota_exceeded");
}

#[tokio::test]
async fn idempotent_create_and_key_conflicts() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(100, 2));
    let first = api.create("same-key", &transfer).await;
    assert_eq!(first.status, StatusCode::CREATED);
    let replay = api.create("same-key", &transfer).await;
    assert_eq!(replay.status, StatusCode::OK);
    assert_eq!(replay.body["session_id"], first.body["session_id"]);

    let different = Transfer::single("f", pseudo_random(100, 3));
    let conflict = api.create("same-key", &different).await;
    assert_eq!(conflict.status, StatusCode::CONFLICT);
    assert_eq!(conflict.code(), "state_conflict");

    // One active transfer per receiver.
    let second = api.create("another-key", &different).await;
    assert_eq!(second.status, StatusCode::CONFLICT);
    // The same key belongs to each owner separately; the receiver is still busy, though.
    let other = env
        .api_as(&receiver, "other")
        .create("same-key", &transfer)
        .await;
    assert_eq!(other.status, StatusCode::CONFLICT);
    assert_eq!(session_count(&env), 1);

    let session = first.body["session_id"].as_str().unwrap();
    assert_eq!(api.abort(session).await.status, StatusCode::OK);
    assert_eq!(
        api.create("another-key", &different).await.status,
        StatusCode::CREATED
    );
}

#[tokio::test]
async fn source_change_after_prepare_is_ignored() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let source = env.path("source");
    let original = Transfer {
        directories: vec!["d".into()],
        files: vec![
            ("d/a.bin".into(), pseudo_random(CHUNK + 5, 4)),
            ("b.txt".into(), b"before".to_vec()),
        ],
    };
    original.write_to(&source);
    let spool = env.path("spool");
    parse_report(&env.prepare(&source, &spool));
    // Change, add and remove source files after preparation.
    std::fs::write(source.join("b.txt"), b"after, and longer").unwrap();
    std::fs::write(source.join("new.txt"), b"new").unwrap();
    std::fs::remove_file(source.join("d/a.bin")).unwrap();
    let report = parse_report(&env.send(&receiver, &spool));
    let session = report["session_id"].as_str().unwrap();
    let mut expected = original.clone();
    expected.files.sort_by(|a, b| a.0.cmp(&b.0));
    expected.assert_published(&env.completed(session).join("data"));
    assert_eq!(
        env.api(&receiver).status(session).await.state,
        SessionState::Published
    );
}

#[tokio::test]
async fn prepare_rejects_bad_sources_and_existing_spools() {
    let env = TestEnv::new();
    let output = env.prepare(&env.path("missing"), &env.path("spool"));
    assert_eq!(output.status.code(), Some(2));
    std::fs::write(env.path("f"), b"x").unwrap();
    parse_report(&env.prepare(&env.path("f"), &env.path("spool")));
    let again = env.prepare(&env.path("f"), &env.path("spool"));
    assert_eq!(again.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&again.stderr).contains("already exists"));
}
