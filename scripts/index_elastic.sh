#!/usr/bin/env bash
# Provision the Elastic backing store for Kineticz RRF retrieval.
#
# Creates (idempotently): an E5 text_embedding inference endpoint (or reuses the
# preconfigured one), an ingest pipeline that embeds mitigation signatures at
# index time, the `contracts` and `mitigations` indices, and sample data.
#
# The KNN leg embeds query text server-side via query_vector_builder, so the Go
# binary never computes vectors. Indexing sends text; Elastic computes the
# 384-dim multilingual-e5-small vector through the default pipeline.
#
# Reads ELASTIC_URL and ELASTIC_API_KEY from env. Optional ELASTIC_INFERENCE_MODEL
# overrides the inference endpoint id (default: the 9.x preconfigured endpoint).
# No secrets are hardcoded.
#
# Review the request bodies below before running against a live cluster.

set -euo pipefail

: "${ELASTIC_URL:?set ELASTIC_URL (no trailing slash), e.g. https://my-deployment.es.<region>.gcp.cloud.es.io}"
: "${ELASTIC_API_KEY:?set ELASTIC_API_KEY (the base64 \"encoded\" value from POST /_security/api_key)}"

BASE="${ELASTIC_URL%/}"
AUTH="Authorization: ApiKey ${ELASTIC_API_KEY}"
INFERENCE_ENDPOINT="${ELASTIC_INFERENCE_MODEL:-.multilingual-e5-small-elasticsearch}"
EMBED_MODEL_ID=".multilingual-e5-small"
PIPELINE="kineticz-mitigations-embed"

# ML-optional mode (ELASTIC_ML_OPTIONAL=true or the --bm25-only flag) unsets the
# mitigations default pipeline before seeding, so docs index lexical-only with no
# E5 inference and no ML node. The pipeline and dense_vector mapping stay defined
# for explicit ?pipeline= use once ML capacity exists.
ML_OPTIONAL="${ELASTIC_ML_OPTIONAL:-false}"
if [[ "${1:-}" == "--bm25-only" ]]; then
  ML_OPTIONAL=true
fi

# es METHOD PATH [BODY] - curl wrapper. --fail-with-body makes curl exit non-zero
# (and print the error body) on HTTP >=400, so set -e aborts the run instead of
# proceeding past a rejected create.
es() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl --fail-with-body -sS -X "$method" "${BASE}${path}" -H "$AUTH" -H 'Content-Type: application/json' -d "$body"
  else
    curl --fail-with-body -sS -X "$method" "${BASE}${path}" -H "$AUTH"
  fi
}

# status METHOD PATH - HTTP status code only, for existence probes (no --fail so
# a 404 returns the code rather than aborting).
status() { curl -sS -o /dev/null -w '%{http_code}' -X "$1" "${BASE}${2}" -H "$AUTH"; }

echo "[index] target:             ${BASE}"
echo "[index] inference endpoint: ${INFERENCE_ENDPOINT}"

# 1. Inference endpoint. Preconfigured ids (leading dot) already exist on 9.x;
#    GET confirms. A custom id returns 404 and is created from the E5 model.
echo "[index] checking inference endpoint..."
ep="$(status GET "/_inference/text_embedding/${INFERENCE_ENDPOINT}")"
case "$ep" in
  200) echo "[index] inference endpoint exists; reusing it." ;;
  404)
    echo "[index] not found; creating ${INFERENCE_ENDPOINT} from ${EMBED_MODEL_ID}..."
    es PUT "/_inference/text_embedding/${INFERENCE_ENDPOINT}" "{
  \"service\": \"elasticsearch\",
  \"service_settings\": { \"num_allocations\": 1, \"num_threads\": 1, \"model_id\": \"${EMBED_MODEL_ID}\" }
}"; echo ;;
  *)
    echo "[index] ERROR: status ${ep} probing inference endpoint. Check ELASTIC_URL, the API key, and the monitor_inference privilege." >&2
    exit 1 ;;
esac

# 2. Ingest pipeline: embed table_metadata into diff_embedding at index time.
echo "[index] creating ingest pipeline ${PIPELINE}..."
es PUT "/_ingest/pipeline/${PIPELINE}" "{
  \"description\": \"Embed mitigation signature with multilingual-e5-small\",
  \"processors\": [
    { \"inference\": {
        \"model_id\": \"${INFERENCE_ENDPOINT}\",
        \"input_output\": [ { \"input_field\": \"table_metadata\", \"output_field\": \"diff_embedding\" } ]
    } }
  ]
}"; echo

# 3. contracts index: yaml (text) + name (keyword).
echo "[index] creating contracts index..."
if [[ "$(status GET /contracts)" == "404" ]]; then
  es PUT /contracts '{
  "mappings": { "properties": {
    "name": { "type": "keyword" },
    "yaml": { "type": "text" }
  } }
}'; echo
else
  echo "[index] contracts exists; skipping create."
fi

# 4. mitigations index: BM25 text fields + E5 dense_vector populated by the
#    default pipeline. hnsw (not the 9.x-default bbq_hnsw) keeps it on the
#    broadest license tier; switch to bbq_hnsw if your tier allows it.
echo "[index] creating mitigations index..."
if [[ "$(status GET /mitigations)" == "404" ]]; then
  es PUT /mitigations "{
  \"settings\": { \"index\": { \"default_pipeline\": \"${PIPELINE}\" } },
  \"mappings\": { \"properties\": {
    \"columns\":        { \"type\": \"text\" },
    \"table_metadata\": { \"type\": \"text\" },
    \"summary\":        { \"type\": \"text\" },
    \"diff_id\":        { \"type\": \"keyword\" },
    \"diff_embedding\": { \"type\": \"dense_vector\", \"dims\": 384, \"index\": true, \"similarity\": \"cosine\", \"index_options\": { \"type\": \"hnsw\", \"m\": 16, \"ef_construction\": 100 } }
  } }
}"; echo
else
  echo "[index] mitigations exists; skipping create."
fi

# 5. Sample contracts. _id is the connector signature ("type/name"), matching the
#    contractName the pipeline builds. _bulk carries _id in the body, so the
#    slash needs no path-encoding here.
#    SAMPLE DATA: generated (no fixture was supplied). Replace with real
#    contracts before the demo.
echo "[index] indexing sample contracts..."
es POST "/contracts/_bulk?refresh=wait_for" '{"index":{"_id":"postgres/orders_pg"}}
{"name":"postgres/orders_pg","yaml":"name: postgres/orders_pg\nversion: 1\ncolumns:\n  - id: bigint\n  - amount: numeric\n  - status: string\n  - created_at: timestamp\n"}
{"index":{"_id":"postgres/users_pg"}}
{"name":"postgres/users_pg","yaml":"name: postgres/users_pg\nversion: 1\ncolumns:\n  - id: bigint\n  - email: string\n  - full_name: string\n  - created_at: timestamp\n"}
{"index":{"_id":"snowflake/events"}}
{"name":"snowflake/events","yaml":"name: snowflake/events\nversion: 1\ncolumns:\n  - event_id: string\n  - status: string\n  - occurred_at: timestamp\n"}
'; echo

# 6. Sample mitigations. By default the index pipeline embeds table_metadata into
#    diff_embedding on write. In ML-optional mode the default pipeline is unset
#    just below, so the same docs land lexical-only (no vector, no ML node).
#    SAMPLE DATA: generated (no fixture was supplied). diff-001..003 mirror the
#    summaries in internal/elastic/testdata/rrf_search.json.
if [[ "${ML_OPTIONAL}" == "true" ]]; then
  echo "[index] ML-optional: unsetting default_pipeline on mitigations (BM25-only writes)..."
  es PUT "/mitigations/_settings" '{"index":{"default_pipeline":"_none"}}'; echo
  echo "[index] indexing sample mitigations (lexical-only, ML-free)..."
else
  echo "[index] indexing sample mitigations (embeddings computed on ingest)..."
fi
es POST "/mitigations/_bulk?refresh=wait_for" '{"index":{"_id":"diff-001"}}
{"diff_id":"diff-001","summary":"Add nullable timestamp column with default NULL","columns":"created_at updated_at","table_metadata":"postgres/orders_pg orders schema"}
{"index":{"_id":"diff-002"}}
{"diff_id":"diff-002","summary":"Rename email column with backfill view","columns":"email email_address","table_metadata":"postgres/users_pg users schema"}
{"index":{"_id":"diff-003"}}
{"diff_id":"diff-003","summary":"Split full_name into first_name and last_name","columns":"full_name first_name last_name","table_metadata":"postgres/users_pg users schema"}
{"index":{"_id":"diff-004"}}
{"diff_id":"diff-004","summary":"Widen numeric precision for amount column","columns":"amount total","table_metadata":"postgres/orders_pg orders schema"}
{"index":{"_id":"diff-005"}}
{"diff_id":"diff-005","summary":"Add NOT NULL default to status enum","columns":"status","table_metadata":"snowflake/events events schema"}
{"index":{"_id":"diff-006"}}
{"diff_id":"diff-006","summary":"Backfill nullable foreign key after type change","columns":"customer_id","table_metadata":"postgres/orders_pg orders schema"}
'; echo

echo "[index] done. Verify (with ELASTIC_API_KEY exported in your shell):"
echo "  curl -sS -H \"Authorization: ApiKey \$ELASTIC_API_KEY\" \"${BASE}/mitigations/_count\""
echo "  curl -sS -H \"Authorization: ApiKey \$ELASTIC_API_KEY\" \"${BASE}/mitigations/_doc/diff-001\"  # diff_embedding should be 384 floats"
