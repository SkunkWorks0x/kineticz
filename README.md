# Kineticz

High-performance, deterministic DataOps orchestration. Written in Go.

Module: `github.com/skunkworks0x/kineticz` (Go 1.26.2)

## Status

Pre-implementation. The repository contains design constraints and a module scaffold. No runtime code yet.

## Design Constraints

Every change to this codebase obeys the rules below. Source of truth: `CLAUDE.md`.

### Types
- IDs are typed. Example: `type CorrelationToken string`. No raw floating strings.
- Every JSON payload has a `Validate() error` method.
- Decoders use `json.NewDecoder.DisallowUnknownFields()`.

### Errors
- Check every returned error.
- Wrap with `fmt.Errorf("context: %w", err)`.

### Concurrency
- Counters use `sync/atomic`.
- Buffered channels only for fixed-size worker pools.

### Audit
- State changes are hash-chained and Ed25519-signed.
- Audit storage: MongoDB Atlas.

## Commands

```
go build ./...
go test -v -race ./...
go mod tidy
```

## Layout

```
.
├── CLAUDE.md             System rules: style, safety, identity
├── .claude/rules/        Project-specific style and safety rules
├── go.mod                Go module definition
├── program.md            Empty
└── README.md             This file
```

## Contributing

Read `CLAUDE.md` and `.claude/rules/no-slop.md` before writing code or prose. Both are enforced on every diff.

One change per commit. Imperative commit messages under 50 characters.
