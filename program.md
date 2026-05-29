# program.md: Kineticz System Specification

## What Kineticz Does
Kineticz is an orchestration engine that detects broken data pipelines, diagnoses the root cause, and applies targeted code patches to restore them. It runs autonomously with human-auditable safety gates.

## Architecture

### Core Loop
1. **Detect**: A Fivetran webhook delivers a schema-change event. The receiver verifies HMAC-SHA256, deduplicates by event ID, mints a CorrelationToken, and hands off to a background pipeline.
2. **Diagnose**: Gemini 3.5 Flash routes parallel tool calls between Dynatrace (live consumer health) and Elastic (historical contract and mitigations) to identify the root cause.
3. **Propose**: Gemini generates a candidate code patch as a unified diff, parsed and validated for single-file, non-binary, non-empty, traversal-free hunks.
4. **Gate**: A local deterministic evaluator decides pass or fail. The patched bytes must parse as Go (go/parser) and exported function signatures must stay unchanged (go/ast). Binary pass/fail. No soft scores. Phoenix records the verdict as a trace span; it observes the decision and does not make it.
5. **Apply**: The patch lands as a GitLab merge request only after passing the local gate.
6. **Audit**: MongoDB Atlas stores each transition. Every entry is hash-chained and Ed25519-signed.

### External Services
| Service          | Role                          | Integration Pattern                          |
|------------------|-------------------------------|----------------------------------------------|
| Fivetran         | Schema-change source          | HMAC-verified inbound webhook                |
| Gemini 3.5 Flash | AI routing + patch generation | Parallel tool calls via Vertex AI REST       |
| Dynatrace        | Live consumer health          | DQL query on failure events                  |
| Elastic          | Historical pipeline memory    | BM25 + KNN over Reciprocal Rank Fusion       |
| Arize Phoenix    | Observability                 | OpenTelemetry trace export                   |
| GitLab           | Patch application             | v4 REST: branch commit + merge request       |
| MongoDB Atlas    | Tamper-evident audit log      | Hash-chained, Ed25519-signed, ACID writes    |

### Key Design Constraints
- **Local deterministic gate**: The evaluator returns true or false from go/parser and go/ast checks. No probability thresholds, no confidence scores. The gate runs in-process, not in a remote service.
- **Phoenix observes, does not evaluate**: Arize Phoenix ingests OpenTelemetry spans for every pipeline stage. It records what happened. It does not gate patches.
- **Parallel tool calls**: Gemini fans out the Dynatrace and Elastic calls at once. Elastic failure is a hard fail; Dynatrace failure is a soft fail (Degraded mode).
- **Hash-chained audit**: Every entry references the previous entry's hash. A break in the chain is detectable.
- **Ed25519 signatures**: Each audit entry is signed. The signing key identifies which Kineticz instance wrote it. The seed is loaded from KINETICZ_ED25519_SEED so restarts continue the chain.
- **Schema drift focus**: The primary failure mode Kineticz targets is an upstream schema change that breaks downstream consumers.

### Language and Runtime
- **Go**. Single binary, no runtime dependencies.
- **Module**: `github.com/skunkworks0x/kineticz`

### Data Flow
```
Schema-change event
    → Fivetran webhook (HMAC-verified)
    → audit: FIVETRAN_RECEIVED
    → Gemini 3.5 Flash (parallel: Dynatrace health + Elastic history)
    → candidate unified diff
    → local gate (parses-as-Go + preserves exported signatures)
    → [PASS] GitLab merge request + audit: COMMIT_OK, MR_CREATED
    → [BLOCK] rejected diff audited and indexed to Elastic
    → Phoenix observes every stage as OpenTelemetry spans
```
