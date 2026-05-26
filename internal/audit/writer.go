package audit

import "context"

type Writer interface {
	Append(ctx context.Context, action string, payload []byte) error
}

// ThoughtWriter is the extended interface for callers that have a Gemini
// reasoning block to persist alongside the action+payload. Implementations
// must populate Entry.Thought so the SHA-256 hash covers it (see
// CanonicalBytes). Existing audit.Writer implementations should implement
// this too; Append is the equivalent of AppendWithThought with thought="".
type ThoughtWriter interface {
	Writer
	AppendWithThought(ctx context.Context, action string, payload []byte, thought string) error
}
