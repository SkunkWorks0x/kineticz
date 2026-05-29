#!/usr/bin/env bash
# Claude Code PreToolUse hook: gate git commit on build + tests.
#
# Fires before any Bash tool call. If the command is a `git commit`, runs the
# verify gate (go build + go test -race). Blocks the commit with exit 2 if the
# gate fails, so broken code cannot be committed. Allows everything else.
#
# Exit code convention (Claude Code specific):
#   0 = allow the tool call
#   2 = BLOCK the tool call; stderr message is fed back to Claude
#   (other non-zero codes do NOT block, so this script uses 0 and 2 only)
#
# Install at .claude/hooks/pre-commit-gate.sh (executable), wired in
# .claude/settings.json under hooks.PreToolUse matcher "Bash".

set -uo pipefail

# Read the hook payload from stdin and extract the command being run.
INPUT="$(cat)"
CMD="$(echo "${INPUT}" | jq -r '.tool_input.command // ""')"

# Only gate git commit. Let every other Bash command through untouched.
if ! echo "${CMD}" | grep -qE '\bgit\s+commit\b'; then
  exit 0
fi

REPO_ROOT="$(echo "${INPUT}" | jq -r '.cwd // "."')"
cd "${REPO_ROOT}" 2>/dev/null || cd .

# Only gate inside the kineticz repo (presence of go.mod). Skip elsewhere.
if [[ ! -f go.mod ]]; then
  exit 0
fi

echo "[pre-commit-gate] git commit detected. Running build + race tests..." >&2

if ! go build ./... 2>&1; then
  echo "[pre-commit-gate] BLOCKED: go build failed. Fix compile errors before committing." >&2
  exit 2
fi

if ! go test -race ./... 2>&1; then
  echo "[pre-commit-gate] BLOCKED: go test -race failed. Fix failing tests before committing." >&2
  exit 2
fi

echo "[pre-commit-gate] PASS: build clean, tests green. Allowing commit." >&2
exit 0
