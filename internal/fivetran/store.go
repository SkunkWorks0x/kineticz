package fivetran

import (
	"context"

	"github.com/skunkworks0x/kineticz/internal/audit"
)

// EventStore is the audit-ledger interface the Receiver depends on. It
// extends audit.Writer with idempotency (HasEntry) and event-tagged appends
// (AppendWithEvent). audit/mongodb.Writer satisfies it; tests provide a fake.
type EventStore interface {
	audit.Writer
	HasEntry(ctx context.Context, eventID string) (bool, error)
	AppendWithEvent(ctx context.Context, action string, payload []byte, eventID string) error
}
