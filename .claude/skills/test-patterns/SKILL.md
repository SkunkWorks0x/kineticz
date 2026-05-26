---
name: test-patterns
description: Use when writing or modifying any test file in the Kineticz codebase
---

# test-patterns

## When to use
Writing or modifying any test file.

## Rules
- Use table-driven tests for pure logic. Name subtests `Test<Function>/<scenario>`.
- Gate integration tests behind `//go:build integration`.
- Put fixtures in `testdata/` directories.
- Use golden files for diff-based assertions on unified diff output.
- Run tests with `-race` on every invocation.
- Forbid `t.Skip` without a linked issue URL in the skip message.
- Mock external services at the HTTP layer with `httptest.NewServer` for integration tests. Do not mock at the interface layer.
- Write test names that read as feature descriptions. Judges will read them.
