---
name: integration-boundary
description: Use when creating any new external service client for Dynatrace, Elastic, Arize, GitLab, Fivetran, MongoDB, or Gemini
---

# integration-boundary

## When to use
Creating any new external service client for Dynatrace, Elastic, Arize, GitLab, Fivetran, MongoDB, or Gemini.

## Rules
- Put the interface in `internal/<service>/client.go`. Put the mock in `internal/<service>/mock.go`.
- Take `context.Context` as the first arg on every method.
- Define a structured error type per service. Example: `type DynatraceError struct { StatusCode int; Body string }`.
- Retry transient errors (5xx, timeout) with exponential backoff. Do not retry 4xx.
- Keep raw HTTP calls inside the client package. Other packages call the interface.
- Accept `*http.Client` in every client constructor for testability.
- Expose capabilities via Model Context Protocol where the partner supports it. Document the MCP server URL in the client package README.
