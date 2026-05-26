# program.md — Kineticz System Specification

## What Kineticz Does
Kineticz is an orchestration engine that detects broken data pipelines, diagnoses the root cause, and applies targeted code patches to restore them. It runs autonomously with human-auditable safety gates.

## Architecture

### Core Loop
1. **Detect**: Dynatrace telemetry surfaces pipeline failures (schema drift, throughput drops, error spikes).
2. **Diagnose**: Gemini 3.5 Flash routes parallel tool calls between Dynatrace (live metrics) and Elastic (historical memory) to identify the root cause.
3. **Propose**: Gemini generates a candidate code patch based on the diagnosis.
4. **Gate**: Arize evaluates the proposed patch against a deterministic boolean rubric. Binary pass/fail. No soft scores.
5. **Apply**: Patch lands only after passing the Arize gate.
6. **Audit**: MongoDB Atlas stores the result. Each entry is hash-chained and Ed25519-signed.

### External Services
| Service          | Role                        | Integration Pattern           |
|------------------|-----------------------------|-------------------------------|
| Gemini 3.5 Flash | AI routing + patch generation | Parallel tool calls via API   |
| Dynatrace        | Live telemetry               | Pull metrics on failure events|
| Elastic          | Historical pipeline memory   | Query past failures + fixes   |
| Arize            | Deterministic safety gate    | Boolean rubric evaluation     |
| MongoDB Atlas    | Tamper-evident audit log     | Hash-chained, Ed25519-signed  |

### Key Design Constraints
- **Deterministic gating**: Arize rubric returns true or false. No probability thresholds, no "confidence scores."
- **Parallel tool calls**: Gemini calls Dynatrace and Elastic concurrently, not sequentially.
- **Hash-chained audit**: Every entry references the previous entry's hash. Breaks in the chain are detectable.
- **Ed25519 signatures**: Each audit entry is signed. The signing key identifies which Kineticz instance wrote it.
- **Schema drift focus**: The primary failure mode Kineticz targets is upstream schema changes that break downstream consumers.

### Language and Runtime
- **Go** — single binary, no runtime dependencies
- **Module**: `github.com/skunkworks0x/kineticz`

### Data Flow
```
Pipeline Failure
    → Dynatrace alert
    → Gemini 3.5 Flash (parallel: Dynatrace metrics + Elastic history)
    → Candidate patch
    → Arize boolean rubric
    → [PASS] Apply patch + write audit entry
    → [FAIL] Log rejection + alert operator
```
