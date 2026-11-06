# Implementation decisions

The [specification](specification.md) (version 1.1) is the design contract. This file records where the implementation had to choose something the specification leaves open, and every place the implementation differs from the original plan. Each entry names its reason and, where one exists, the test that checks it.

## Protocol and errors

1. **Error codes beyond §6's list.** §6 names status codes 404, 429 and 507 but gives no error code for them. They use `not_found`, `busy` and `insufficient_storage`. Two more codes exist:
   - `request_timeout` (408) for a request body that stalls past the idle timeout. §6 does not list 408.
   - `internal` (500) for a defect, such as a state that changed unexpectedly inside the single worker.

   `quota_exceeded` stays on 413. Every error body carries `code`, `message` and `request_id`. Test: `protocol::tests::error_codes_map_to_specified_statuses`.
2. **Diagnostics endpoint.** `GET /v1/receiver` (authenticated) is an addition to §6. It returns the following, which tests, the demonstration and the benchmarks read:
   - the durability profile and filesystem;
   - write and duplicate counters;
   - the buffer high-water mark;
   - cumulative `sync_data`, receipt-commit, verification and publication time;
   - peak RSS (Linux);
   - the startup recovery report;
   - whether the build includes fault injection.
3. **Early rejections and closed connections.**
   - **Drained:** before answering 400, 404 or 409 without reading the body, the receiver reads and discards at most one chunk (4 MiB) or one manifest (16 MiB). An HTTP/1.1 client that is still uploading then receives the response instead of a reset connection.
   - **Never read:** a 401 (unauthenticated), 413 (oversized) or 429 (backpressure) rejection does not read the body. Such a response may reach a client as a closed connection.
   - **Connection: close:** every error response carries `Connection: close`, and the receiver closes the connection after sending it. A client therefore never reuses a connection whose request body may be partly unread. Without this, a pooled connection reused after an early 413 failed intermittently (`concurrency::body_limit_without_content_length` in CI).
   - **Effect on the sender:** it never sends an oversized body and retries transport errors, so this matters only for misbehaving clients.
   - Tests: `single_file::bad_indexes_are_400_and_oversized_bodies_413`, `manifest::invalid_manifests_are_rejected_without_creating_sessions`.
4. **Manifest file IDs.** Each file record carries an explicit `id` field. §5's field table lists "stable numeric IDs" without naming the field. IDs must be sequential from 0, in array order.
5. **Creation headers.**
   - The idempotency key travels in `Idempotency-Key`: 1–128 characters from `[A-Za-z0-9-_.:]`.
   - The digest of the exact manifest bytes travels in `Manifest-Sha256`, outside the hashed document.
   - A digest that does not match the uploaded bytes is a 400.
6. **Missing-list cursor.** The query parameters are `after_file`, `after_chunk` and `limit`. Pages hold at most 1,000 references by default; the limit is configurable with `--missing-page-limit`. The response's `next_after` is the last visited pair, as §6 describes. Test: `resume::missing_pages_cover_every_chunk_once_in_order`.

## Storage worker and state

7. **Incremental verification.** An abort during verification must return 409 while verification continues (§11). Rather than add a second writer, verification runs as a job inside the single storage worker, one 4 MiB step at a time. Between steps the worker answers queued commands, and each command rechecks the durable state. Test: `publication::abort_during_verify_is_409_and_verification_continues`.
8. **One read per chunk during verification.** §8 verifies full-file digests first, then checks individual chunks when one mismatches. The implementation computes chunk digests and the full digest in the same read. The outcomes are identical (repair, fault, or publish), and the data is read once instead of twice.
9. **Damaged staging files.** If verification finds a staged file missing or the wrong length, it rebuilds the staging tree, clears every receipt of that file and returns to Receiving. Test: `publication::deleted_staging_file_is_rebuilt_and_resent`.
10. **Verification storage errors.** A read error stops the pass. The session stays in Verifying and its `last_error` is set. A commit replay restarts the pass, and so does a restart; §8 mentions only recovery.
11. **Publication intent.** The intent is the `destination` column on `sessions`, written in the same transaction that enters Publishing. This keeps the four logical tables of §5.
12. **Published output found damaged at startup.** Published is terminal, and Faulted is entered only from an active state. So a Published session whose output is missing, resized, or has the wrong identity keeps its Published state and gets `last_error = "storage fault: ..."`. The startup check covers identity and sizes only. Full content was verified before publication.
13. **Last error when the database is full.** If even the `last_error` update cannot be written, the condition still appears in status, held in memory by the worker, until the next successful change. Test: `crash::real_enospc_on_small_ext4`.
14. **Admission reserve.** Session creation needs free space of at least the declared bytes, plus the manifest, plus 64 MiB. Later out-of-space errors are still handled; §5 says admission checks only help.
15. **One process per receiver root.** The receiver holds an exclusive `flock` on `metadata/receiver.lock`. `shardsync cleanup` takes the same lock, so it refuses to run alongside a live receiver.
16. **Cleanup scope.** Cleanup removes:
    - Aborted staging;
    - the staging remnant of each Published session (only the stored manifest remains there after the rename);
    - orphan staging entries, but only with `--remove-orphans`.

    It keeps Faulted sessions for an operator and never touches `completed/`.

## Platform and durability profile

17. **Supported profile.** The supported profile is Linux on ext4. It is detected through the statfs magic `0xEF53`, which ext2, ext3 and ext4 share, so `stat -f` prints `ext2/ext3`.
18. **Running elsewhere.** Anywhere else, macOS included, the receiver refuses to start unless `--allow-unvalidated-platform` is given. Status then reports `durability_profile: "unvalidated"`.
19. **Startup self-check.** Every start probes the receiver's staging directory and refuses to start on any failure:
    - `sync_data`;
    - directory sync;
    - that a no-replace rename onto an existing name fails and leaves the destination unchanged.
20. **Platform calls.**
    - All filesystem calls go through `rustix`.
    - No-replace rename is `renameat2(RENAME_NOREPLACE)` on Linux and `renameatx_np(RENAME_EXCL)` on macOS.
    - On macOS, SQLite also sets `fullfsync`; std's `sync_all` and `sync_data` already use `F_FULLFSYNC` there.
21. **Test enforcement.** With `SHARDSYNC_REQUIRE_VALIDATED=1`, set by `scripts/linux-test.sh` and CI, the test harness panics instead of starting a receiver with the development flag. A green run therefore proves the supported profile was exercised. Test: `single_file::receiver_reports_its_durability_profile`.
22. **Disk-full evidence.** There are two disk-full tests:
    - An injected `ENOSPC` at the chunk-write boundary.
    - A real `ENOSPC` on a loop-mounted **256 MiB** ext4 filesystem, run in CI and by `scripts/linux-test.sh --small-fs`. The plan said 64 MiB, but that would fail admission because of the 64 MiB reserve.

    The real test fails rather than skips when `CI` is set and the mount is missing.

## Sender and spool

23. **Symlinks are refused, not skipped.** §4 says preparation walks "without following symbolic links". A symlink anywhere in the source stops preparation with an error, because silently skipping one would drop data from the transfer. Sockets, FIFOs and device files are refused as well.
24. **Single-file input.** A single file is published as `completed/<id>/data/<file name>`.
25. **Freezing the spool.** Spool files become mode 0400. Directories stay 0700 so the spool can be deleted normally. Because root ignores mode bits, the sender also checks every chunk's digest before uploading it. A modified spool stops the send with an error.
26. **Resent bytes.** The sender appends one line to `upload-log.jsonl` in the spool before each chunk attempt (`started`). After an attempt that never connected, so sent nothing, it appends `not_connected`. Resent bytes are started minus not-connected bytes, summed across every run on the spool, minus one copy of the content. An attempt cut short by a crash is still counted. Test: `resume::sender_resumes_after_receiver_crash`.
27. **507 stops the send.** "Insufficient storage" needs capacity to recover, so the sender exits with code 4 instead of retrying. Other transient failures are retried with exponential backoff and full jitter for up to `--retry-window-secs` (default 120) per request.

## Command line

28. **Development credentials.** The development command is `shardsync dev-setup`, not the planned `dev-ca`, because it writes tokens as well as certificates:
    - `ca.pem`, `server-cert.pem` and `server-key.pem`;
    - `tokens.json`, which holds token SHA-256 digests and never the tokens;
    - `token-<owner>.txt`.
29. **Tokens file permissions.** The receiver refuses a tokens file that group or others can read.
30. **Exit codes.**
    - 0: success;
    - 2: invalid input;
    - 3: transfer rejected or faulted by the receiver;
    - 4: environment or runtime failure (unreachable receiver, disk, configuration).
