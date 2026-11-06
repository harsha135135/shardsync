#!/usr/bin/env bash
# Repeated uninterrupted and interrupted transfers with raw results in benchmarks/results/.
# Options are passed to scripts/benchmark.py (for example --repetitions 3 --single-mib 512).
exec "$(dirname "$0")/run-in-linux.sh" benchmark.py "$@"
