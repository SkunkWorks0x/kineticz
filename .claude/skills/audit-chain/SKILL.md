---
name: audit-chain
description: Use for any code path that writes state to MongoDB or mutates pipeline state
---

# audit-chain

## When to use
Any code that writes state to MongoDB or mutates pipeline state.

## Rules
- Route every state mutation through `audit.Chain()`.
- Thread the correlation token via `context.Context` using a typed key from `internal/corr`.
- Forbid direct MongoDB writes that bypass the audit chain.
- Verify each `audit.Entry` with `audit.Verify()` before commit.
- Treat the genesis entry as `PreviousHash == nil`. `Verify` handles this case.
- Write entries to the `audit_ledger` collection. Index on `CorrelationToken` and `Timestamp`.
