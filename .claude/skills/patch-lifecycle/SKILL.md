---
name: patch-lifecycle
description: Use when working on the detect, diagnose, propose, gate, apply, or audit pipeline stages, or modifying the Patch struct
---

# patch-lifecycle

## When to use
Touching the detect → diagnose → propose → gate → apply → audit pipeline or the `Patch` struct.

## Rules
- Patch carries a stage enum: `Detected`, `Diagnosed`, `Proposed`, `Gated`, `Applied`, `Rejected`.
- Advance state through stage transition methods. Forbid field writes that skip a stage.
- Write an `audit.Entry` via `audit.Chain()` on every transition.
- Block `Applied` unless `Gated == true` from Arize.
- Treat `Rejected` as terminal. No transitions out.
- Format patch payload as a unified diff (GitLab format).
- Carry a `CorrelationToken` from `internal/corr` on every transition.
