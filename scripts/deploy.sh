#!/usr/bin/env bash
# Deploy Kineticz to Cloud Run with a build-and-verify gate.
#
# This script exists because `gcloud run services replace` does not rebuild
# the image. Running replace alone redeploys whatever :latest already points
# to, which silently ships stale code. This script always builds and pushes
# first, captures the pushed digest, deploys, then reads the live revision's
# digest and fails if they do not match.
#
# Usage: ./scripts/deploy.sh
# Prerequisites: verify.sh passes, gcloud authenticated, docker logged in to
# Artifact Registry, run from a clean working tree on the intended commit.

set -euo pipefail

cd "$(dirname "$0")/.."

PROJECT="blade-agent-488115"
REGION="us-central1"
IMAGE="us-central1-docker.pkg.dev/${PROJECT}/kineticz/kineticz:latest"
SERVICE="kineticz"

# 1. Gate on build + tests before shipping anything.
echo "[deploy] running verify gate..."
./scripts/verify.sh

# 2. Warn on a dirty tree. Deploying uncommitted changes loses the link
#    between the running image and a commit.
if [[ -n "$(git status --porcelain)" ]]; then
  echo "[deploy] WARNING: working tree is dirty. The deployed image will not match any commit." >&2
  read -r -p "[deploy] continue anyway? (y/N) " ans
  [[ "${ans}" == "y" || "${ans}" == "Y" ]] || { echo "[deploy] aborted."; exit 1; }
fi

COMMIT="$(git rev-parse --short HEAD)"
echo "[deploy] building image for commit ${COMMIT}..."

# 3. Build and push. --no-cache guarantees the image is the current tree.
docker build --no-cache --platform linux/amd64 --provenance=false -t "${IMAGE}" .
PUSH_OUT="$(docker push "${IMAGE}")"
echo "${PUSH_OUT}"

# 4. Extract the pushed digest from the push output.
PUSHED_DIGEST="$(echo "${PUSH_OUT}" | grep -oE 'sha256:[a-f0-9]{64}' | tail -1)"
if [[ -z "${PUSHED_DIGEST}" ]]; then
  echo "[deploy] ERROR: could not parse pushed digest. Aborting before deploy." >&2
  exit 1
fi
echo "[deploy] pushed digest: ${PUSHED_DIGEST}"

# 5. Deploy. service.yaml must have a fresh deploy-ts or replace rolls nothing.
echo "[deploy] applying service.yaml..."
gcloud run services replace service.yaml --region="${REGION}"

# 6. Read back the live revision's resolved digest and compare.
LIVE_REV="$(gcloud run services describe "${SERVICE}" --region="${REGION}" \
  --format='value(status.latestReadyRevisionName)')"
LIVE_DIGEST="$(gcloud run revisions describe "${LIVE_REV}" --region="${REGION}" \
  --format='value(status.imageDigest)')"

echo "[deploy] live revision: ${LIVE_REV}"
echo "[deploy] live digest:   ${LIVE_DIGEST}"

if [[ "${LIVE_DIGEST}" == *"${PUSHED_DIGEST#sha256:}"* ]] || [[ "${LIVE_DIGEST}" == "${PUSHED_DIGEST}" ]]; then
  echo "[deploy] SUCCESS: live revision ${LIVE_REV} runs the image just built from ${COMMIT}."
else
  echo "[deploy] FAILED: live digest does not match the pushed image." >&2
  echo "[deploy] Likely cause: service.yaml deploy-ts was not bumped, so replace rolled no new revision." >&2
  echo "[deploy] Bump kineticz.dev/deploy-ts to current UTC and re-run." >&2
  exit 1
fi

# 7. Health check.
echo "[deploy] health check..."
curl -s "https://kineticz-c5u5pppdwa-uc.a.run.app/health" && echo
echo "[deploy] done."
