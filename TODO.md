# ShardSync build TODO

Specification: [docs/specification.md](docs/specification.md) (version 1.1). Implementation choices: [docs/decisions.md](docs/decisions.md).

**Tick rule.** An item is ticked only when all of the following pass on Linux/ext4, both in `scripts/linux-test.sh --small-fs` and in GitHub Actions:
- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --features fault-injection`, in debug and release builds, with `SHARDSYNC_REQUIRE_VALIDATED=1`

## 1. Single file with full hashes

- [x] Manifest, TLS (rustls, development CA, only the configured root trusted) and bearer tokens
- [x] Create, upload, commit, full-hash verification and publication of one file
- [x] 0 bytes, 1 byte, exactly 4 MiB, 4 MiB + 1 and multi-chunk files round-trip through the CLI
- [x] 422 on a hash mismatch; 400 on a wrong length or index; 413 on an oversized body; 401 on a bad token; 404 for another client's session; a client trusting the wrong CA fails the handshake

## 2. Immutable spool and manifest validation

- [x] Private spool copy, flush, hash of the copied bytes, frozen manifest; source changes after preparation do not affect the transfer
- [x] Manifest digest over the exact bytes; duplicate keys and unknown fields rejected; every admission cap (413) and structural rule (400)
- [x] Idempotent creation (201, then 200 with the same session); key reuse with a different manifest returns 409; one active transfer per receiver

## 3. Durable receipts and resume

- [x] Single storage worker owning every writable file and the writable SQLite connection; bounded command queue; cached status snapshot
- [x] SQLite WAL, `synchronous=FULL` and `foreign_keys=ON` verified at startup
- [x] Receipt sequence: check, hash, write, `sync_data`, commit, then acknowledge
- [x] Missing-list pagination with a cursor
- [x] Startup recheck of every durable chunk, clearing damaged receipts
- [x] Sender state persisted before creation, so a lost create response resumes the same session
- [x] Bounded retries with backoff and jitter; status queried after uncertain outcomes

## 4. Atomic publication and recovery

- [x] Incremental verification inside the worker: abort during verification returns 409 while the pass continues
- [x] Damaged chunks return the session to Receiving; consistent chunks with a wrong full digest go to Faulted
- [x] Publication intent, no-replace rename, parent directory syncs, then Published
- [x] Recovery reconciles every crash point: before the rename, after the rename, after the Published commit, and during verification
- [x] An existing destination is never replaced
- [x] Abort semantics; cleanup of abandoned staging that never touches completed output

## 5. Folders and paths

- [x] Directory manifests, empty directories and zero-byte files published exactly
- [x] Descriptor-relative walks with `O_NOFOLLOW` on both ends; a symlinked staging directory is refused without writing outside
- [x] Malicious manifest paths rejected; the sender refuses symlinks, FIFOs, sockets and device files

## 6. Bounded concurrency, demonstration and benchmarks

- [x] Eight 4 MiB buffer permits taken before a body is read and carried through the queue; high-water mark at most 8 under load
- [x] 429 backpressure; body limit enforced without a Content-Length; idle-body timeout releases the permit; connection limit
- [x] Duplicate concurrent uploads produce one durable receipt; abort and commit races end consistently
- [x] `scripts/demo.sh`: interrupt both processes at about 50%, restart both, resume, and verify the published tree byte for byte
- [x] `scripts/benchmark.sh`: uninterrupted and interrupted runs on the same seeded datasets, 5 repetitions, raw results in `benchmarks/results/`

## Failure matrix (§11)

| Scenario | Test |
|---|---|
| Disconnect halfway through a chunk | `resume::truncated_body_leaves_no_receipt` |
| Crash after file sync, before DB commit | `crash::after_chunk_sync_chunk_is_resent` |
| Crash after DB commit, before ACK | `crash::after_receipt_commit_retry_is_idempotent` |
| Duplicate concurrent chunk requests | `concurrency::duplicate_puts_write_once` |
| Corrupt or wrong-length body | `single_file::hash_mismatch_422`, `single_file::wrong_length_400` |
| Corrupt staging bytes after receipt | `crash::restart_rehash_clears_corrupt_receipt`, `publication::commit_detects_corruption_and_returns_to_receiving` |
| Disk full during write or flush | `crash::injected_enospc_is_visible`, `crash::real_enospc_on_small_ext4` |
| Crash around final rename | `crash::before_publish_rename`, `crash::after_publish_rename`, `crash::after_published_commit` |
| Existing destination or malicious path | `publication::existing_destination_faults_without_overwrite`, `folders::malicious_paths_rejected`, `folders::staging_symlink_is_refused_without_writing_outside` |
| Source changes after preparation | `manifest::source_change_after_prepare_is_ignored` |
| Zero-byte files and empty directories | `folders::exact_structure_published` |

**Further §11 cases:**
- `publication::abort_during_verify_is_409_and_verification_continues`
- `publication::client_disconnect_does_not_cancel_publication`
- `crash::restart_after_202_resumes_verification`
- `crash::crash_during_preparation_is_reconstructed`
- `publication::all_chunks_ok_but_full_hash_wrong_faults`
- `resume::lost_create_response_recovers_same_session`
- `resume::sender_resumes_after_receiver_crash`
- `resume::sender_restart_resends_only_missing_chunks`
- `resume::sender_restart_with_missing_spool_fails_cleanly`
- `concurrency::abort_commit_race`
- `concurrency::slow_client_times_out_and_releases_permit`
- `concurrency::body_limit_without_content_length`
- `concurrency::buffer_high_water_at_most_8`

**Evidence:** 80 tests (32 unit, 48 integration), passing in debug and release builds on Linux/ext4 with `SHARDSYNC_REQUIRE_VALIDATED=1`. That includes the real out-of-space test on a 256 MiB ext4 loop mount. Clippy is clean with `-D warnings`.

## Not in this release

These are later improvements in §11; each needs its own correctness and performance evidence:
- batched durable receipts
- more than one storage worker
- configurable chunk size
- compression and deduplication
- macOS as a supported receiver

Power-loss testing (VM or filesystem fault injection) is also outside this release. The recovery evidence here comes from process kills.
