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

# Fire only when the command runs `git commit`. Parse the git subcommand
# instead of pattern-matching the word "commit": skip git's global options
# (and the value some take, like `-C <dir>`) so `git -C <dir> commit` and
# `git -c k=v commit` still gate, while `git commit-tree`, `git commit-graph`,
# and a stray "commit" in `git log --grep commit` do not.
runs_git_commit() {
  local -a tok
  IFS=$' \t\n' read -rd '' -a tok < <(printf '%s' "${CMD}")
  local n=${#tok[@]} i j skip
  for ((i = 0; i < n; i++)); do
    [[ "${tok[i]}" == "git" ]] || continue
    skip=0
    for ((j = i + 1; j < n; j++)); do
      if ((skip)); then skip=0; continue; fi
      case "${tok[j]}" in
        -C|-c|--git-dir|--work-tree|--namespace|--exec-path) skip=1 ;;
        -*) ;;
        commit) return 0 ;;
        *) break ;;
      esac
    done
  done
  return 1
}

if ! runs_git_commit; then
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
