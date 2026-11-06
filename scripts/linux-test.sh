#!/usr/bin/env bash
# Runs the full verification on Linux inside Docker: format, lint, and the test suite with
# fault injection in debug and release builds.
#
# The receiver root, TMPDIR and build cache live on named Docker volumes (ext4 inside the
# Docker VM), never on a bind mount from the host, so durability tests exercise ext4.
#
# Usage: scripts/linux-test.sh [--small-fs]   (--small-fs adds the real out-of-space test;
#                                              it needs a privileged container for a loop mount)
set -euo pipefail
cd "$(dirname "$0")/.."

image=shardsync-test:1.99.1
docker build --quiet -t "$image" -f docker/Dockerfile.test docker >/dev/null

privileged=()
small_fs_setup=""
if [[ "${1:-}" == "--small-fs" ]]; then
  privileged=(--privileged)
  small_fs_setup='
    truncate -s 256M /work/testfs/small.img
    mkfs.ext4 -q -F /work/testfs/small.img
    mkdir -p /mnt/small && mount -o loop /work/testfs/small.img /mnt/small
    export SHARDSYNC_SMALL_FS=/mnt/small'
fi

docker run --rm ${privileged[@]+"${privileged[@]}"} \
  -v "$PWD":/src:ro \
  -v shardsync-target:/work/target \
  -v shardsync-cargo-registry:/usr/local/cargo/registry \
  -v shardsync-testfs:/work/testfs \
  -e CARGO_TARGET_DIR=/work/target \
  -e CARGO_TERM_COLOR=never \
  -w /src "$image" bash -euo pipefail -c "
    mkdir -p /work/testfs/tmp
    export TMPDIR=/work/testfs/tmp SHARDSYNC_TEST_ROOT=/work/testfs/tmp SHARDSYNC_REQUIRE_VALIDATED=1
    $small_fs_setup
    echo \"kernel: \$(uname -sr)  test filesystem: \$(stat -f -c %T /work/testfs/tmp)\"
    rustc --version
    cargo fmt --check
    cargo clippy --locked --all-targets --all-features -- -D warnings
    cargo test --locked --features fault-injection
    cargo test --locked --release --features fault-injection
  "
