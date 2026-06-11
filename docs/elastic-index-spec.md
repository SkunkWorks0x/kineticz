# Elastic index spec (live retrieval)

Extracted read-only from `internal/elastic/client.go` and `scripts/index_elastic.sh`.
Source of truth for what Imani provisions before the RRF leg works.

## Indices

| Index | Access | Path | _id |
|---|---|---|---|
| `contracts` | read | `GET /contracts/_doc/<url-escaped _id>` (client.go:115) | connector signature `type/name`, e.g. `postgres/orders_pg` |
| `mitigations` | read | `POST /mitigations/_search` (client.go:206) | mitigation id, e.g. `diff-001` |

The Go client never writes to Elastic. The `Client` interface exposes only
`LookupContract` (client.go:19-21); every method is a GET or `_search`. Audit
writes go to MongoDB, not Elastic.

## Mappings

`contracts` (index_elastic.sh:89-94):

| Field | Type | Read by |
|---|---|---|
| `name` | keyword | none |
| `yaml` | text | `_source.yaml` → `ContractContext.YAMLDefinition` (client.go:125-137) |

`mitigations` (index_elastic.sh:104-113):

| Field | Type | Role |
|---|---|---|
| `columns` | text | BM25 leg (`multi_match`) |
| `table_metadata` | text | BM25 leg; also the ingest pipeline's embedding input |
| `summary` | text | returned → `Mitigation.Summary` |
| `diff_id` | keyword | returned → `Mitigation.DiffID` |
| `diff_embedding` | dense_vector, **dims 384**, `index:true`, `similarity:cosine`, hnsw (m=16, ef_construction=100) | KNN leg target |

`mitigations` carries `index.default_pipeline = kineticz-mitigations-embed`
(index_elastic.sh:105). There is **no rejection/status field** in the mapping;
`parseMitigationHits` decodes only `summary` + `diff_id` (client.go:216-241).

## Embeddings: Elastic-side, no Go vectors

The Go binary sends text only and never computes a vector (client.go:144).

- **Query time:** the KNN leg uses `query_vector_builder.text_embedding`
  (client.go:162-167) with `model_id = c.inferenceModel`, `model_text = signature`.
  Elastic embeds the query through the inference endpoint at search time.
- **Index time:** ingest pipeline `kineticz-mitigations-embed` runs an `inference`
  processor that embeds `table_metadata` → `diff_embedding` (index_elastic.sh:76-84).

Model: inference endpoint `.multilingual-e5-small-elasticsearch` (default;
overridable via `ELASTIC_INFERENCE_MODEL`, client.go:79), backed by model
`.multilingual-e5-small`, **384 dims, cosine** (index_elastic.sh:25-26, 111).

**Provisioning requirement:** a deployed E5 inference endpoint on an ML node with
capacity (`num_allocations:1, num_threads:1`, index_elastic.sh:65-68). Without an
ML node the KNN/inference leg returns HTTP 429 and the client degrades to BM25.

## The 429 / no_ml_nodes path

1. Elastic returns 429 for the KNN/inference leg. `searchMitigations` wraps any
   `status >= 400` as `*ElasticError` (client.go:210-211).
2. It propagates `searchMitigationsRRF` → `retrieveMitigations` (client.go:249) →
   `classifyVectorError` (client.go:253).
3. `classifyVectorError` (client.go:263-284) sets:
   - status from `ee.StatusCode`, **client.go:266**
   - reason `no_ml_nodes` when the body contains "no ml node" / "no suitable
     nodes", **client.go:269-270**
   - reason `429` when status is 429 without that text, **client.go:273-274**
4. The client then retries BM25-only and returns `mode = bm25_fallback`
   (client.go:254-256); `recordLookup` audits `ELASTIC_LOOKUP_DEGRADED` with
   `vector_status` + `vector_reason` (client.go:297-311).

## RRF query params (client.go:145-178)

```
retriever.rrf:
  retrievers:
    - standard.multi_match: { query: <signature>, fields: [columns, table_metadata] }   # BM25
    - knn: { field: diff_embedding, k: 10, num_candidates: 100,
             query_vector_builder.text_embedding: { model_id: <inferenceModel>, model_text: <signature> } }
  rank_constant: 60          # confirmed
  rank_window_size: 100
size: 3                       # top-k returned
```

BM25 fallback (no ML, client.go:186-197): plain `query.multi_match` on
`[columns, table_metadata]`, `size: 3`.

The `signature` is the connector signature `ConnectorType/ConnectorName`
(e.g. `postgres/orders_pg`), built in cmd/kineticz/main.go.

## Valid sample mitigation doc (demo: orders_pg)

Normal path: index text only; the default pipeline computes `diff_embedding`:

```json
{ "index": { "_id": "diff-001" } }
{ "diff_id": "diff-001",
  "summary": "Add nullable timestamp column with default NULL",
  "columns": "created_at updated_at",
  "table_metadata": "postgres/orders_pg orders schema" }
```

`POST /mitigations/_bulk?refresh=wait_for`. After ingest, `diff_embedding` is 384
floats (E5 of `table_metadata`). In ML-optional mode (`--bm25-only` /
`ELASTIC_ML_OPTIONAL=true`) the same doc lands lexical-only with no vector
(index_elastic.sh:137-156). To index a pre-embedded doc with the pipeline off,
add `"diff_embedding": [ <384 floats> ]`.

This mitigation (`diff-001`) matches the demo schema drift: the `orders_pg`
`created_at` nullable fix.

## Read/write separation (audit finding #3)

- **Read path:** `elastic.Client.LookupContract` → `fetchContractYAML`
  (GET `/contracts/_doc`) + `retrieveMitigations` (POST `/mitigations/_search`).
  No write method exists on the client.
- **Rejected-diff write path:** `evaluate.RejectedIndexer.Index(ctx, sha, diff)`
  (gate.go:21-23), invoked at gate.go:88, wired in cmd/kineticz/main.go:425 to
  `noopIndexer{}`, which drops the diff on the floor (main.go:493-498).

The two paths are separate, and the rejected-diff indexer is still the no-op.
The `mitigations` mapping has no rejection field, so if a real indexer replaces
the no-op, add a `rejection_status` field and a `must_not` filter on the read
query first.

# Provisioning runbook (Imani)

## 1. Auth: already wired

The client authenticates with an Elastic API key: header
`Authorization: ApiKey <encoded>` (client.go:336). The value is the base64
`encoded` field from `POST /_security/api_key`, passed through verbatim
(client.go:334-336; same value the script uses, index_elastic.sh:21).

Env vars (cmd/kineticz/main.go:309-311):

| Var | Meaning | Default |
|---|---|---|
| `ELASTIC_URL` | cluster base URL, no trailing slash | required |
| `ELASTIC_API_KEY` | base64 `encoded` API key | required |
| `ELASTIC_INFERENCE_MODEL` | inference endpoint id | `.multilingual-e5-small-elasticsearch` |

**service.yaml cross-check (correction):** both `ELASTIC_URL` (service.yaml:64-68)
and `ELASTIC_API_KEY` (service.yaml:69-73) are wired as Secret Manager
`secretKeyRef`s (`key: latest`). The API-key secret is bound today (commit
`fd008ef`); the "only the URL is wired" assumption is stale, and secret names are
UPPER_SNAKE, not `kineticz-elastic-url`. Action: ensure the Secret Manager secrets
`ELASTIC_URL` and `ELASTIC_API_KEY` hold real `latest` versions. No service.yaml
edit needed. `ELASTIC_INFERENCE_MODEL` is not wired, so the runtime uses the
default endpoint; add it to service.yaml only if you provision a custom endpoint id.

## 2. Manual Elastic Cloud prerequisites (before the script)

The script cannot create ML capacity. By hand in the deployment first:

- **ML node capacity:** enable Machine Learning / size an ML node (or ML
  autoscaling). The KNN/inference leg needs it.
- **E5 model deployment:**
  - Default endpoint `.multilingual-e5-small-elasticsearch` (9.x preconfigured):
    the script reuses it (GET → 200). It auto-allocates on first inference when ML
    capacity exists; otherwise deploy the `.multilingual-e5-small` trained model in
    Kibana (ML → Trained Models) with **1 allocation, 1 thread**.
  - Custom endpoint id: the script creates it via `PUT _inference` with
    `num_allocations:1, num_threads:1` (index_elastic.sh:64-68). It still needs an ML node.

No ML node yet? Run the script `--bm25-only` for a lexical-only demo (step 4).

## 3. What the script creates (`scripts/index_elastic.sh`)

| Step | Action | Lines |
|---|---|---|
| 1 | inference endpoint: GET probe, reuse (200) or create (404) with E5 model | 57-72 |
| 2 | ingest pipeline `kineticz-mitigations-embed` (embeds `table_metadata`→`diff_embedding`) | 74-84 |
| 3 | `contracts` index (if 404): `name` keyword, `yaml` text | 86-97 |
| 4 | `mitigations` index (if 404): text fields + `diff_embedding` dense_vector 384, `default_pipeline` | 99-116 |
| 5 | sample data: 3 contracts + 6 mitigations | 118-156 |

## 4. Run

```sh
export ELASTIC_URL="https://<deployment>.es.<region>.gcp.cloud.es.io"   # no trailing slash
export ELASTIC_API_KEY="<base64 encoded from POST /_security/api_key>"
# optional, only for a custom endpoint id:
# export ELASTIC_INFERENCE_MODEL="my-e5-endpoint"

./scripts/index_elastic.sh              # full E5 path, ML node required
./scripts/index_elastic.sh --bm25-only  # OR lexical-only, no ML node
```

`--bm25-only` (or `ELASTIC_ML_OPTIONAL=true`) unsets the `mitigations`
`default_pipeline` before seeding, so docs index lexical-only with no E5 and no ML
node (index_elastic.sh:29-36, 137-139). Query-time RRF still degrades to BM25. The
API key needs privileges to probe/create inference, create pipelines and indices,
and bulk index; the script's curl uses `--fail-with-body` and aborts on the first 4xx.

## 5. Verify the KNN leg (no `no_ml_nodes` 429)

The inference probe confirms the model is allocated on an ML node:

```sh
curl -sS -H "Authorization: ApiKey $ELASTIC_API_KEY" -H 'Content-Type: application/json' \
  "$ELASTIC_URL/_inference/text_embedding/.multilingual-e5-small-elasticsearch" \
  -d '{"input":"postgres/orders_pg orders schema"}'
```

200 with a 384-float `text_embedding` = ML node up. `429` / "no ml node" = capacity missing.

End-to-end: the client's exact RRF query (client.go:145-178):

```sh
curl -sS -H "Authorization: ApiKey $ELASTIC_API_KEY" -H 'Content-Type: application/json' \
  "$ELASTIC_URL/mitigations/_search" -d '{
  "retriever":{"rrf":{"retrievers":[
    {"standard":{"query":{"multi_match":{"query":"postgres/orders_pg","fields":["columns","table_metadata"]}}}},
    {"knn":{"field":"diff_embedding","query_vector_builder":{"text_embedding":{"model_id":".multilingual-e5-small-elasticsearch","model_text":"postgres/orders_pg"}},"k":10,"num_candidates":100}}
  ],"rank_constant":60,"rank_window_size":100}},"size":3}'
```

200 with hits (`diff-001/004/006` for orders_pg) = KNN works; the app records
`ELASTIC_LOOKUP_OK` mode `rrf`. `429` = `no_ml_nodes`, app degrades to
`bm25_fallback`. Index-time check: `GET /mitigations/_doc/diff-001` →
`diff_embedding` is 384 floats (index_elastic.sh:160).
