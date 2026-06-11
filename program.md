# program.md: Kineticz System Specification

## What Kineticz Does

Kineticz is an orchestration engine that detects broken data pipelines, diagnoses
the root cause from retrieved history and its own prior repairs, and proposes a
targeted code patch to restore them. It runs autonomously up to a human-reviewed
merge request, and records every step in a tamper-evident audit ledger.

## Architecture

### Core loop

1. **Detect.** A Fivetran webhook delivers a schema-change event. The receiver
   verifies HMAC-SHA256 with a constant-time compare, deduplicates by event ID,
   mints a CorrelationToken, and hands off to a background pipeline.
2. **Diagnose.** `diagnose.Engine` fans out concurrent Go goroutines to Dynatrace
   (consumer health) and Elastic (contract and mitigations), and queries the Arize
   Phoenix MCP server for the agent's own prior repair traces. It assembles these
   into a diagnosis. Gemini does not route these calls; the engine does.
3. **Propose.** Gemini 3.5 Flash generates a candidate patch as a unified diff. The
   coordinator parses it, validates it for single-file, non-binary, non-empty,
   traversal-free hunks, and re-anchors each hunk to its unique exact context match
   in the target file.
4. **Gate.** A local syntactic evaluator decides pass or fail. The patched bytes
   must parse as Go (`go/parser`) and exported function signatures must stay
   unchanged (`go/ast`). It runs no build or test. It does not prove behavioral
   correctness. Phoenix records the verdict as a span; it observes the decision
   and does not make it.
5. **Apply.** The patch lands as a GitLab merge request after passing the local
   gate. A human reviews the MR for semantics before merge.
6. **Audit.** MongoDB Atlas stores each transition, hash-chained and
   Ed25519-signed.

### External services

| Service          | Role                          | Integration pattern                          | Live status              |
|------------------|-------------------------------|----------------------------------------------|--------------------------|
| Fivetran         | Schema-change source          | HMAC-verified inbound webhook                | live                     |
| Gemini 3.5 Flash | Patch generation              | Vertex AI `generateContent` REST             | live                     |
| Dynatrace        | Downstream consumer health    | DQL over REST                                | soft-fail, no live tenant|
| Elastic          | Historical contract and fixes | BM25 over the connector signature            | live (BM25 only)         |
| Arize Phoenix    | Observability + introspection | OpenTelemetry spans, Phoenix MCP queries     | live                     |
| GitLab           | Patch application             | v4 REST: branch commit and merge request     | live                     |
| MongoDB Atlas    | Tamper-evident audit log      | hash-chained, Ed25519-signed, ACID writes    | live                     |

### Key design constraints

- **Local syntactic gate.** The evaluator returns true or false from `go/parser`
  and `go/ast` checks. No probability thresholds, no confidence scores. It runs
  in-process. It validates form, not behavior.
- **Phoenix observes, does not evaluate.** Arize Phoenix ingests OpenTelemetry
  spans for every stage. It records what happened. It does not gate patches.
- **Go fan-out for tool calls.** `diagnose.Engine` issues the Dynatrace and Elastic
  calls concurrently with goroutines. Elastic failure is a hard fail. Dynatrace and
  Phoenix failures are soft fails, and the diagnosis proceeds degraded.
- **Re-anchored byte-exact apply.** Gemini's hunk line numbers are not trusted. The
  coordinator re-anchors each hunk to its unique exact context match, or rejects
  the diff fail-closed. The applier never tolerates content mismatch.
- **Hash-chained audit.** Every entry references the previous entry's hash. Interior
  tampering is detectable. Tail-truncation needs an external count anchor.
- **Ed25519 signatures.** Each entry is signed. The seed loads from
  `KINETICZ_ED25519_SEED` so restarts continue the chain.
- **Schema drift focus.** The primary failure mode Kineticz targets is an upstream
  schema change that breaks a downstream consumer.

### What "deterministic" covers

The gate and the audit chain are deterministic. Gemini's patch generation runs at
temperature 0.2 and is probabilistic. The same schema drift can take one iteration
on one run and several on another. The re-anchor and the gate make the apply and
the verdict reproducible; the patch text is not.

### Language and runtime

- **Go.** Single binary, plus a baked Node.js Phoenix MCP subprocess
  (`phoenix-mcp`, pinned in the Dockerfile).
- **Module:** `github.com/skunkworks0x/kineticz`
- **Host:** Google Cloud Run. Gemini via Vertex AI with metadata-server auth.

### Data flow

```
Schema-change event
    -> Fivetran webhook (HMAC-verified, constant-time)
    -> audit: FIVETRAN_RECEIVED
    -> diagnose.Engine (Go fan-out: Dynatrace health + Elastic BM25)
                        (Phoenix MCP: prior repair traces)
    -> Gemini 3.5 Flash candidate unified diff
    -> re-anchor to unique exact context + byte-exact apply
    -> local gate (parses as Go + preserves exported signatures)
    -> [PASS] GitLab merge request + audit: COMMIT_OK, MR_CREATED -> human review
    -> [BLOCK] rejected diff audited
    -> Phoenix observes every stage as OpenTelemetry spans
```
