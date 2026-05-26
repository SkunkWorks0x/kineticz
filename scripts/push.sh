#!/usr/bin/env bash
#
# Push Kineticz to the public hackathon submission repo.
# Run from the repository root.
#
# Prereq: the GitHub repo at SkunkWorks0x/kineticz must already exist and
# be set to public visibility (Settings → General → Danger Zone → Change
# visibility). GitHub's About sidebar will pick up the MIT license at root
# once the push lands.

set -euo pipefail

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "error: not inside a git repository" >&2
    exit 1
fi

if [[ ! -f LICENSE ]]; then
    echo "error: LICENSE missing at repo root; submission will not be detectable as MIT" >&2
    exit 1
fi

REMOTE_URL="git@github.com:SkunkWorks0x/kineticz.git"

if git remote get-url origin >/dev/null 2>&1; then
    current=$(git remote get-url origin)
    if [[ "$current" != "$REMOTE_URL" ]]; then
        echo "warning: existing origin points to $current; expected $REMOTE_URL" >&2
        echo "        run: git remote set-url origin $REMOTE_URL" >&2
        exit 1
    fi
else
    git remote add origin "$REMOTE_URL"
fi

git push -u origin main

echo
echo "Verify next:"
echo "  1. GitHub repo page: https://github.com/SkunkWorks0x/kineticz"
echo "  2. About sidebar must show 'MIT' under 'License'"
echo "  3. Repo visibility must be Public"
