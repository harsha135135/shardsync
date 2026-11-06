//! Milestone 1: single-file transfer with full hashes, TLS and bearer tokens.

mod common;

use common::*;
use reqwest::StatusCode;
use shardsync::protocol::SessionState;

#[tokio::test]
async fn boundary_sizes_round_trip_through_the_cli() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    for (index, size) in [0, 1, CHUNK, CHUNK + 1, 3 * CHUNK + 17]
        .into_iter()
        .enumerate()
    {
        let source = env.path(&format!("source-{index}.bin"));
        let bytes = pseudo_random(size, index as u64);
        std::fs::write(&source, &bytes).unwrap();
        let spool = env.path(&format!("spool-{index}"));
        parse_report(&env.prepare(&source, &spool));
        let report = parse_report(&env.send(&receiver, &spool));
        assert_eq!(report["state"], "published");
        let session = report["session_id"].as_str().unwrap();
        let published = env
            .completed(session)
            .join("data")
            .join(format!("source-{index}.bin"));
        assert!(std::fs::read(&published).unwrap() == bytes, "size {size}");
        assert_eq!(report["resent_bytes_all_runs"], 0);
        // Only the version directory appears; nothing is merged into an existing folder.
        let identity: serde_json::Value = serde_json::from_slice(
            &std::fs::read(env.completed(session).join("transfer.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(identity["session_id"], session);
    }
}

#[tokio::test]
async fn hash_mismatch_422() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(1000, 1));
    let session = api.new_session(&transfer).await;
    let mut corrupt = transfer.chunk(0, 0);
    corrupt[10] ^= 0xFF;
    let reply = api.put_chunk(&session, 0, 0, corrupt).await;
    assert_eq!(reply.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(reply.code(), "hash_mismatch");
    assert!(reply.body["request_id"].as_str().unwrap().len() == 32);
    assert_eq!(api.status(&session).await.durable_chunks, 0);
    assert_eq!(
        api.diagnostics().await.chunk_writes,
        0,
        "rejected before any write"
    );
}

#[tokio::test]
async fn wrong_length_400() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(1000, 2));
    let session = api.new_session(&transfer).await;
    let reply = api
        .put_chunk(&session, 0, 0, transfer.chunk(0, 0)[..999].to_vec())
        .await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST);
    assert_eq!(reply.code(), "invalid_chunk");
    assert_eq!(api.status(&session).await.durable_chunks, 0);
}

#[tokio::test]
async fn bad_indexes_are_400_and_oversized_bodies_413() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(1000, 3));
    let session = api.new_session(&transfer).await;
    for (file, index) in [(0u32, 1u32), (1, 0)] {
        let reply = api.put_chunk(&session, file, index, vec![0; 10]).await;
        assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{file}/{index}");
    }
    let reply = api
        .client
        .put(format!(
            "{}/v1/sessions/{session}/files/x/chunks/0",
            api.base
        ))
        .bearer_auth(&api.token)
        .body(vec![0u8; 1])
        .send()
        .await
        .unwrap();
    assert_eq!(reply.status(), StatusCode::BAD_REQUEST);
    // An oversized body is refused without being read; the client may see the 413 or a
    // closed connection, depending on how much it had sent.
    if let Some(reply) = api.try_put_chunk(&session, 0, 0, vec![0; CHUNK + 1]).await {
        assert_eq!(reply.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(reply.code(), "quota_exceeded");
    }
    assert_eq!(api.status(&session).await.durable_chunks, 0);
}

#[tokio::test]
async fn bad_token_401_and_other_clients_session_404() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let transfer = Transfer::single("f", pseudo_random(10, 4));
    let session = api.new_session(&transfer).await;

    let stranger = Api {
        token: "not-a-token".into(),
        ..api.clone()
    };
    let reply = stranger.get(&format!("/v1/sessions/{session}")).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED);
    assert_eq!(reply.code(), "unauthorized");
    let anonymous = api
        .client
        .get(format!("{}/v1/sessions/{session}", api.base))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);

    // Ownership is enforced on every endpoint, including status and missing chunks.
    let other = env.api_as(&receiver, "other");
    for path in [
        format!("/v1/sessions/{session}"),
        format!("/v1/sessions/{session}/missing"),
    ] {
        assert_eq!(
            other.get(&path).await.status,
            StatusCode::NOT_FOUND,
            "{path}"
        );
    }
    assert_eq!(other.commit(&session).await.status, StatusCode::NOT_FOUND);
    assert_eq!(other.abort(&session).await.status, StatusCode::NOT_FOUND);
    let reply = other.put_chunk(&session, 0, 0, transfer.chunk(0, 0)).await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
    assert_eq!(api.status(&session).await.state, SessionState::Receiving);
}

#[tokio::test]
async fn client_trusting_the_wrong_ca_fails_the_handshake() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let other_ca = env.path("other-ca");
    shardsync::devcert::write_dev_setup(&other_ca, &["x".to_string()]).unwrap();
    let client = shardsync::tls::http_client(
        &other_ca.join("ca.pem"),
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(5),
    )
    .unwrap();
    let result = client
        .get(format!("{}/v1/receiver", receiver.base))
        .bearer_auth(env.token("demo"))
        .send()
        .await;
    assert!(result.is_err(), "an untrusted certificate must be refused");
    // Plain HTTP is refused by the client before anything is sent.
    let plain = env
        .client()
        .get(format!("http://localhost:{}/v1/receiver", receiver.port()))
        .send()
        .await;
    assert!(plain.is_err());
}

#[tokio::test]
async fn unknown_routes_return_json_errors_with_request_ids() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let api = env.api(&receiver);
    let reply = api.get("/v1/nothing").await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
    assert_eq!(reply.code(), "not_found");
    let reply = api.get("/v1/sessions/not-a-session-id").await;
    assert_eq!(reply.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn receiver_reports_its_durability_profile() {
    let env = TestEnv::new();
    let receiver = Receiver::start(&env);
    let diagnostics = env.api(&receiver).diagnostics().await;
    if require_validated() {
        assert_eq!(diagnostics.durability_profile, "validated");
        assert_eq!(diagnostics.filesystem, "ext4");
    } else {
        assert!(["validated", "unvalidated"].contains(&diagnostics.durability_profile.as_str()));
    }
    assert_eq!(diagnostics.chunk_buffer_permits, 8);
}
