# CLAUDE.md — Kineticz System Rules

## Build and Test Commands
- Build project: `go build ./...`
- Run all tests: `go test -v -race ./...`
- Tidy dependencies: `go mod tidy`

## Go Style & Safety
- **Typed IDs:** Use `type CorrelationToken string`. No raw floating strings.
- **Error Handling:** Check all errors. Wrap with `fmt.Errorf("context: %w", err)`.
- **Concurrency:** Use `sync/atomic` for counters. Buffered channels only for fixed workers.
- **Validation:** Use `json.NewDecoder.DisallowUnknownFields()`. Every payload needs a `Validate() error` method.
- **Audit:** All state changes must be hash-chained and Ed25519-signed in MongoDB Atlas.

## Mission Identity
- Project Name: **Kineticz**
- Identity: High-performance, deterministic DataOps orchestration.
- Stack: Gemini 3.5 Flash (routing), Dynatrace (telemetry), Elastic (memory), Arize (gating), MongoDB Atlas (audit)

## Writing & Communication (Anti-Slop)
All prose — comments, docs, commit messages, READMEs, error messages, CLI output — must follow these rules:
- No throat-clearing: kill "Here's the thing", "It's worth noting", "Let's dive in", "Let that sink in", "This matters because", "At the end of the day".
- No "not X, it's Y" or "It's not just X, it's Y" contrasts. State Y directly.
- No em dashes. Use periods or commas.
- No adverbs (-ly words, "really", "just", "simply", "actually", "basically", "essentially", "fundamentally").
- No false agency on objects ("the system leverages", "the pipeline enables"). Name the actor.
- No vague declaratives ("The implications are significant"). Name the specific thing.
- No rule-of-three lists unless the domain actually has three items.
- Active voice. Specific nouns. Vary sentence length. Two items beat three.
- State facts. No softening, hedging, or hand-holding.

## Behavioral Rules
- Read before writing. Run `cat` or `head` on any file before editing it.
- One change per commit. Commit messages: imperative, under 50 chars, no slop.
- If a task is ambiguous, state your assumption and proceed. Do not ask for clarification on things you can infer from context.
- Do not refactor adjacent code unless explicitly asked.
- Do not add dependencies without stating why and what alternatives you rejected.
