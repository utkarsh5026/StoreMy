#!/usr/bin/env bash
# Watch db/ sources and re-run Rust tests (requires inotify-tools).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ "${1:-}" != "" ]]; then
  echo "Watching db/ — package: $1"
  cargo test -p "$1"
  while inotifywait -e modify,create,delete -r db/src db/tests 2>/dev/null; do
    echo "$(date -Iseconds): cargo test -p $1"
    cargo test -p "$1"
    echo ""
  done
else
  echo "Watching db/ — full workspace tests"
  cargo test --workspace
  while inotifywait -e modify,create,delete -r db/src db/tests 2>/dev/null; do
    echo "$(date -Iseconds): cargo test --workspace"
    cargo test --workspace
    echo ""
  done
fi
