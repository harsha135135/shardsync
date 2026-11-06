# ShardSync

[![CI](https://github.com/harsha135135/shardsync/actions/workflows/ci.yml/badge.svg)](https://github.com/harsha135135/shardsync/actions/workflows/ci.yml)

Resumable, verified transfer of a fixed file or folder version, written in Rust. If the connection drops or either process restarts, the sender asks the receiver which 4 MiB chunks it still needs and sends only those. The receiver publishes the result in one step, and only once every byte matches the manifest.

**Status:** all six milestones in the [specification](docs/specification.md) are complete. [TODO.md](TODO.md) records what passes, and [docs/decisions.md](docs/decisions.md) records the choices the specification left open.

**Supported durability profile: Linux on ext4.** The receiver checks its filesystem at startup and refuses to run anywhere else unless you pass `--allow-unvalidated-platform`. That flag exists for development on macOS, and no durability claim applies with it.

## Why

Moving a large dataset, model artifact or build output over an unreliable link raises two questions:
- After an interruption, what must be sent again?
- When may the receiving side treat the result as complete?

Copying again from the start wastes time. Writing straight into the destination leaves half-written output that looks finished.

ShardSync answers both with explicit boundaries:
- A chunk is acknowledged only after its bytes are synced to disk and its receipt is committed to SQLite.
- Output becomes visible through one no-replace directory rename, after the whole content has been verified.

It is a transfer tool, not continuous folder synchronization. It does not merge edits, delete destination files, or decide which of two modified folders wins.

## Run it

Requires a Rust toolchain (`rust-toolchain.toml` pins 1.99.1). The Linux scripts also need Docker when run from macOS.

```sh
cargo test                       # unit and integration tests, development profile
./scripts/linux-test.sh          # fmt, clippy, all tests in debug and release, on Linux/ext4
./scripts/linux-test.sh --small-fs   # also the real out-of-space test (privileged container)
./scripts/demo.sh                # interrupt, restart both processes, resume, verify
./scripts/benchmark.sh           # repeated measurements, raw results in benchmarks/results/
```

On macOS, `linux-test.sh`, `demo.sh` and `benchmark.sh` run inside the pinned Docker image. All data stays on named volumes, which are ext4 inside the Docker VM, so the receiver runs on its supported profile there. On a Linux host, `demo.sh` and `benchmark.sh` run directly.

To run each step yourself, use `target/release/shardsync` after `cargo build --release`:

```sh
shardsync dev-setup --out dev                  # development CA, receiver certificate, token
shardsync receive --root recv --listen 127.0.0.1:8443 \
  --tls-cert dev/server-cert.pem --tls-key dev/server-key.pem --tokens dev/tokens.json
shardsync prepare ./export --spool spool       # copy, flush, hash and freeze the input
shardsync send --spool spool --endpoint https://localhost:8443 \
  --ca dev/ca.pem --token-file dev/token-demo.txt
shardsync status --spool spool --endpoint https://localhost:8443 --ca dev/ca.pem --token-file dev/token-demo.txt
shardsync cleanup --root recv                  # with the receiver stopped: remove abandoned staging
```

Interrupt `send` or `receive` at any point and run the same command again; the transfer resumes. Published output appears at `recv/completed/<session-id>/data/`.

Exit codes: 0 success, 2 invalid input, 3 transfer rejected or faulted, 4 environment or runtime error.

## How it works

- **Immutable input.** `prepare` copies the input into a private spool:
  - It walks without following symlinks and refuses symlinks, FIFOs, sockets and devices.
  - It flushes the copy, then hashes the *copied* bytes.
  - It writes the manifest and makes the files read-only.

  Uploads read only the spool, so later changes to the source cannot change what arrives.
- **Manifest identity.** The manifest is JSON that lists directories, then files with sizes, full SHA-256 digests and per-chunk digests. Its identity is the SHA-256 of the exact uploaded bytes. The receiver checks before creating anything:
  - duplicate keys and unknown fields;
  - every admission cap (100 GiB, 10,000 files, 50,000 chunks, path length and depth);
  - every path rule.
- **One storage worker.** A single thread owns every writable staging file and the only writable SQLite connection. HTTP handlers hand it commands through a bounded queue and await the reply. Each command rechecks the durable session state when it runs. This rules out overlapping writes and competing database writers. It also limits throughput, as the measurements below show.
- **Durable receipt.** For each new chunk the worker:
  1. checks the session state, file ID, chunk index and exact length;
  2. checks the SHA-256;
  3. writes the bytes at the offset derived from the manifest;
  4. calls `sync_data`;
  5. commits the receipt with `synchronous=FULL`.

  Only then does the handler return 204. A retry of a chunk that already has a receipt succeeds without writing.
- **Resume.** The sender saves its idempotency key before creating the session, so a lost response is recovered with the same key. After any interruption it pages through `GET /missing` and uploads what is listed, with four workers. The receiver's list is authoritative.
- **Verification, interleaved.** Commit returns 202 once Verifying is durable. Verification then reads every file one 4 MiB step at a time, computing chunk and full-file digests in the same read. Between steps the worker answers queued requests, so status stays live and an abort is refused promptly.
  - **Damaged chunks:** their receipts are cleared and the session returns to Receiving.
  - **Chunks consistent but the full digest wrong:** the manifest itself is inconsistent, and the session becomes Faulted.
- **Atomic publication.** The worker:
  1. writes `transfer.json` (session ID and manifest digest);
  2. syncs every file, then the directories from children to parents;
  3. records the publication intent;
  4. renames `staging/<id>/tree` to `completed/<id>` with `RENAME_NOREPLACE`;
  5. syncs both parent directories;
  6. records Published.

  An existing destination is never replaced.
- **Recovery.** At startup, before accepting requests, the receiver rehashes every chunk marked durable and clears damaged receipts. It also reconciles the publication intent with the filesystem:
  - **Matching destination:** confirm it, then finish.
  - **Only staging:** re-verify, then rename.
  - **Anything else at the destination:** Faulted.
- **Bounded memory.** Eight 4 MiB permits are taken before a body is read. Each permit travels with its buffer through the queue, so chunk buffers never exceed 32 MiB. The body limit applies even without a Content-Length. A stalled body times out and frees its permit, and extra connections are closed.
- **Security.**
  - TLS uses rustls, and the client trusts only the configured CA.
  - Each client has a bearer token. The receiver stores only token digests and compares them in constant time.
  - Session ownership is checked on every endpoint.
  - Manifest paths are opened one component at a time with `O_NOFOLLOW`, so a symlink inside staging is refused rather than followed.
  - SHA-256 checks integrity; it is not authentication.

| Endpoint | Success |
|---|---|
| `POST /v1/sessions` | 201 new session, 200 identical replay |
| `GET /v1/sessions/{id}` | 200 status (state, durable bytes, verification progress, last error) |
| `GET /v1/sessions/{id}/missing` | 200 one page of missing chunks, with a cursor |
| `PUT /v1/sessions/{id}/files/{file}/chunks/{index}` | 204 after the durable receipt |
| `POST /v1/sessions/{id}/commit` | 202 verifying or publishing, 200 already published |
| `POST /v1/sessions/{id}/abort` | 200 aborted (only while receiving) |
| `GET /v1/receiver` | 200 counters, recovery report and durability profile |

## Evidence

All 80 tests (32 unit, 48 integration) pass in debug and release builds on Linux/ext4, both in CI and in the Docker test image. Those runs set `SHARDSYNC_REQUIRE_VALIDATED=1`, so the harness refuses to fall back to the development profile. The integration tests run the real binary in separate processes. Crash tests stop a receiver at a named fault point with `abort()`, or kill it with SIGKILL, then restart it on the same port. Fault points exist only in builds with the `fault-injection` test feature.

| Specification check | Test |
|---|---|
| Boundary sizes (0, 1, 4 MiB, 4 MiB + 1, multi-chunk) round-trip; 422 hash mismatch, 400 wrong length, 401 bad token, 404 another client's session, wrong CA refused | `tests/single_file.rs` |
| Invalid manifests and caps are rejected without creating a session; idempotent create; key conflicts; one active transfer; source changes after preparation are ignored | `tests/manifest.rs` |
| Disconnect mid-chunk leaves no receipt; pagination covers every chunk once; sender resumes after a receiver crash, after a lost create response, and after its own restart; a missing spool fails cleanly | `tests/resume.rs` |
| Crash after sync and before commit, after commit and before the acknowledgment, and around the rename (three points); restart during verification; crash during preparation; startup rehash clears a corrupt receipt; injected and real `ENOSPC` | `tests/crash.rs` |
| Commit needs every receipt; corruption returns to Receiving; a deleted staging file is rebuilt; an inconsistent manifest faults; an existing destination is never replaced; abort semantics and cleanup; abort during verification is 409; a client disconnect does not cancel publication | `tests/publication.rs` |
| Exact folder structure; malicious paths; a symlinked staging directory is refused; the sender refuses symlinks and FIFOs | `tests/folders.rs` |
| Duplicate concurrent uploads write once; abort/commit races are consistent; body limit without Content-Length; slow clients release their permit; buffer high-water mark at most 8; connection limit | `tests/concurrency.rs` |
| SQLite WAL, `synchronous=FULL`, `foreign_keys=ON` applied; no-replace rename never overwrites; symlinks refused beneath a root; the state transition table | unit tests |

The real out-of-space test fills a 256 MiB loop-mounted ext4 filesystem after admission. It checks that writes return 507 with a visible last error, that nothing appears in `completed/`, and that the transfer finishes once space is freed. It runs in CI, and locally with `--small-fs`.

## Measurements

Every number below comes from a file in [benchmarks/results/](benchmarks/results/), written by the scripts at revision `a4de75e`, using a release build without optional features. The Rust sources are unchanged since `cb3d883`.

**Environment:**
- Apple M4 Pro, macOS 26.5.2, 24 GiB.
- Docker Desktop 29.6.2: a Linux VM with kernel 6.12.76-linuxkit (aarch64), 12 CPUs and 7.75 GiB.
- A named ext4 volume, with sender and receiver talking over loopback.
- 4 upload workers and 8 buffer permits.

**Caveat:** inside Docker Desktop a hypervisor sits between ext4 and the SSD. So the `sync_data` and FULL-commit times here are this VM's, not bare-metal fsync latency.

### Demonstration

Command: `./scripts/demo.sh`, on a tree of 1,995 files totalling 998 MiB (seed 7). [Raw result](benchmarks/results/demo-20261106T180822.json).

| Step | Result |
|---|---|
| Prepare | 2.97 s: 1.65 s copying and flushing, 1.31 s hashing |
| Interrupt | SIGKILL to receiver and sender at 51% durable bytes; `completed/` stays empty |
| Receiver restart | 452 ms, of which the startup recheck read 510 MiB (1,000 chunks) in 447 ms; no receipt needed clearing |
| Resume to published | 3.05 s, including 1.37 s from commit acceptance to Published |
| Resent | 1.0 MiB |
| Peak RSS | receiver 63.0 MiB before the kill, 58.4 MiB after restart; sender 26.7 MiB and 29.0 MiB |
| Result | published tree equal to the spool and to the source (2,160 entries compared by SHA-256) |
| Total | 8.8 s from preparation to publication, including the interruption |

### Repeated runs

Command: `./scripts/benchmark.sh`. [Raw results](benchmarks/results/benchmark-20261106T180726.json).
- **Runs:** 5 repetitions per scenario. Each run starts a fresh receiver root and spool, with no warm-up.
- **Values:** medians, with the range in parentheses.
- **Interrupted runs:** both processes get SIGKILL at 50% durable bytes and both restart. Their sending time includes the restart and the resent data.
- **Correctness:** all 20 runs published output identical to the spool.

| | 1 GiB file | 1 GiB file, interrupted | 1,995-file tree, 510 MiB | Tree, interrupted |
|---|---|---|---|---|
| Payload throughput | 369 MiB/s (341–374) | 327 MiB/s (317–331) | 119 MiB/s (112–121) | 113 MiB/s (112–114) |
| Preparation (copy, flush, hash) | 1.77 s | 1.77 s | 2.59 s | 2.60 s |
| Preparation to publication | 4.54 s (4.41–5.43) | 4.94 s (4.83–5.17) | 6.89 s (6.41–7.15) | 7.10 s (7.00–7.18) |
| Commit to published | 1.27 s | 1.27 s | 0.85 s | 0.84 s |
| Resent | 0 | 16.0 MiB (12.0–16.0) | 0 | 1.0 MiB (0.7–7.4) |
| Startup recheck | — | 536 MiB in 347 ms | — | 259 MiB in 286 ms |
| Receiver `sync_data` / receipt commits | 0.49 s / 0.21 s (256 chunks) | 0.49 s / 0.20 s | 1.26 s / 0.93 s (2,021 chunks) | 1.19 s / 0.89 s |
| Receiver verification (read and hash) | 1.25 s | 1.26 s | 0.65 s | 0.65 s |
| CPU, sender / receiver | 1.16 s / 2.75 s | 1.17 s / 3.07 s | 0.75 s / 1.88 s | 0.75 s / 2.09 s |
| Peak RSS, sender / receiver | 23.2 / 58.5 MiB | 23.3 / 61.4 MiB | 25.4 / 49.3 MiB | 25.0 / 56.6 MiB |

### What this shows

- **Resuming costs little beyond the restart.**
  - Interrupted single-file transfers resent 12–16 MiB: the three or four chunks that four workers had in flight.
  - They finished a median of 0.40 s later than uninterrupted ones.
  - Rechecking the 536 MiB the receiver already held took about 0.35 s.
- **Per-chunk durability dominates many small files.**
  - On the tree, `sync_data` and receipt commits took 2.2 s of the 4.3 s sending time (2,021 receipts).
  - On the large file they took 0.7 s of 2.8 s (256 receipts).
  - Batched receipts are the obvious next step, and they would need their own crash tests.
- **Verification is a second full read.** One thread read and hashed the 1 GiB file at about 820 MiB/s, and that sets the commit-to-published time.
- **Memory stays bounded.**
  - Receiver peak RSS stayed between 48.5 and 63.6 MiB across the 20 runs and the demonstration. That includes up to 32 MiB of chunk buffers plus TLS, HTTP, SQLite and the runtime.
  - The sender peaked between 23 and 29 MiB.

These numbers describe one laptop and this configuration. They are not targets, and they make no claim about other tools.

## Limitations

- **Supported profile only.** Linux on a local ext4 filesystem is the only supported receiver profile. macOS works for development behind `--allow-unvalidated-platform`. Network filesystems and removable media are outside the durability profile.
- **Process kills, not power loss.** Recovery evidence comes from killing processes. Power-loss safety would need VM or filesystem fault injection, which this release does not include.
- **Quiescent input.** Preparation needs input that is not changing. A size or modification-time change during the copy is detected and refused, but that is a hint, not proof of a coherent snapshot.
- **One receipt per chunk.** Every chunk costs one `sync_data` and one FULL SQLite commit in one storage worker, which dominates for many small files (see the tree results). Batched receipts and more storage workers are later improvements that need their own evidence.
- **One active transfer per receiver.** One receiver handles one active transfer at a time. It does not preserve ownership, ACLs, hard links or executable bits, and output uses the receiver's own permissions.
- **Fixed chunk size.** The chunk size is fixed at 4 MiB in protocol version 1.
- **Measurements need context.** They come from one laptop, through Docker Desktop's Linux VM and over loopback. They describe that setup only. Comparing with rsync or similar tools would need their differing preparation and publication semantics disclosed, and is not attempted here.

## Layout

```text
src/
  main.rs         command-line interface and exit codes
  lib.rs          module list
  config.rs       protocol constants, admission caps, receiver and sender limits
  manifest.rs     manifest types, validation, path rules, identity
  json_checks.rs  duplicate-key rejection at any depth
  snapshot.rs     spool preparation, freezing, chunk reads, sender state
  sender.rs       resume planning, upload workers, retries, upload log
  protocol.rs     wire types, session states and transitions, error codes
  api.rs          HTTPS server, routing, authentication, admission limits
  api_error.rs    error classification (storage, database, client)
  storage.rs      the single storage worker and the durable receipt sequence
  metadata.rs     SQLite schema, pragmas, statements and transactions
  publish.rs      verification, publication and startup recovery
  filesystem.rs   descriptor-relative opens, no-replace rename, syncs, startup probe
  progress.rs     counters, status snapshot, permitted chunk buffers
  cleanup.rs      maintenance for a stopped receiver
  tls.rs          rustls client and server configuration
  devcert.rs      development certificates and tokens
  faults.rs       test-only fault points (feature `fault-injection`)
tests/            integration tests against the real binary
scripts/          Linux test runner, demonstration and benchmarks
docs/             specification and implementation decisions
benchmarks/results/  raw measurement files
```

Dependencies are pinned in `Cargo.lock`. The main ones, at the resolved versions:
- tokio 1.54
- axum 0.8
- hyper 1.12
- rustls 0.23 with the ring provider
- reqwest 0.13
- rusqlite 0.41, with SQLite bundled
- rustix 1.1
- sha2 0.11
- rcgen 0.14
