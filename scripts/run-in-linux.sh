#!/usr/bin/env bash
# Runs one of the measurement scripts (demo.py or benchmark.py) on Linux.
#
# On a Linux host the script runs directly. Elsewhere (macOS) it runs inside the pinned
# Docker image, with all data on a named volume (ext4 inside the Docker VM) and results
# written back to benchmarks/results/ in this repository. Host details are recorded because
# Docker Desktop runs Linux in a virtual machine.
set -euo pipefail
cd "$(dirname "$0")/.."
script="$1"
shift

if [[ "$(uname -s)" == "Linux" ]]; then
  exec python3 "scripts/$script" "$@"
fi

image=shardsync-test:1.99.1
docker build --quiet -t "$image" -f docker/Dockerfile.test docker >/dev/null
mkdir -p benchmarks/results

host_cpu="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)"
host_os="$(sw_vers -productName 2>/dev/null || uname -s) $(sw_vers -productVersion 2>/dev/null || uname -r)"
host_memory="$(sysctl -n hw.memsize 2>/dev/null || echo unknown)"
docker_resources="$(docker info --format 'Docker {{.ServerVersion}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes memory, {{.OperatingSystem}}')"

docker run --rm \
  -v "$PWD":/src:ro \
  -v "$PWD/benchmarks/results":/results \
  -v shardsync-target:/work/target \
  -v shardsync-cargo-registry:/usr/local/cargo/registry \
  -v shardsync-testfs:/work/testfs \
  -e CARGO_TARGET_DIR=/work/target \
  -e HOST_CPU="$host_cpu" \
  -e HOST_OS="$host_os" \
  -e HOST_MEMORY_BYTES="$host_memory" \
  -e CONTAINER_NOTE="$docker_resources (Linux VM on the host; fsync timing includes the hypervisor)" \
  -w /src "$image" python3 "/src/scripts/$script" --results /results "$@"
