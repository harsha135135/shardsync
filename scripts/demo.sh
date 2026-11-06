#!/usr/bin/env bash
# Interrupt a transfer, restart both processes, resume, and verify the published output.
# Options are passed to scripts/demo.py (for example --dataset single --size-mib 2048).
exec "$(dirname "$0")/run-in-linux.sh" demo.py "$@"
