#!/usr/bin/env bash
# Build and test gate for Kineticz.
# Runs the compile and the race-enabled test suite. Exits non-zero on any
# failure so a caller (Claude Code PreToolUse hook, deploy script, or a human)
# can block on a broken tree.
#
# Usage: ./scripts/verify.sh
# Exit 0 = build clean and all tests pass. Exit 1 = something failed.

set -euo pipefail

cd "$(dirname "$0")/.."

echo "[verify] go build ./..."
if ! go build ./...; then
  echo "[verify] BUILD FAILED. Fix compile errors before committing or deploying." >&2
  exit 1
fi

echo "[verify] go test -race ./..."
if ! go test -race ./...; then
  echo "[verify] TESTS FAILED. Fix failing tests before committing or deploying." >&2
  exit 1
fi

echo "[verify] PASS: build clean, all tests green."
