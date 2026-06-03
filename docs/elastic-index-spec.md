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
| `name` | keyword | — |
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

## Embeddings — Elastic-side, no Go vectors

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
   - status from `ee.StatusCode` — **client.go:266**
   - reason `no_ml_nodes` when the body contains "no ml node" / "no suitable
     nodes" — **client.go:269-270**
   - reason `429` when status is 429 without that text — **client.go:273-274**
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

Normal path — index text only; the default pipeline computes `diff_embedding`:

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
  `noopIndexer{}` — drops the diff on the floor (main.go:493-498).

The two paths are separate, and the rejected-diff indexer is still the no-op.
The `mitigations` mapping has no rejection field, so if a real indexer replaces
the no-op, add a `rejection_status` field and a `must_not` filter on the read
query first.
