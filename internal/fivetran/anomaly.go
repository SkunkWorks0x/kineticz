package fivetran

import (
	"errors"
	"fmt"
	"time"
)

// ColumnChange describes a single column-level mutation in an upstream schema.
type ColumnChange struct {
	Column  string `json:"column"`
	Action  string `json:"action"` // added, removed, type_changed
	OldType string `json:"old_type,omitempty"`
	NewType string `json:"new_type,omitempty"`
}

// Anomaly is the normalized form of a Fivetran webhook event that triggers
// the Kineticz pipeline. Validate() runs at the receiver boundary before any
// downstream stage sees it.
type Anomaly struct {
	EventID       string         `json:"event_id"`
	EventType     string         `json:"event_type"`
	SchemaName    string         `json:"schema_name"`
	TableName     string         `json:"table_name"`
	ColumnChanges []ColumnChange `json:"column_changes"`
	Timestamp     time.Time      `json:"timestamp"`
}

// Sentinel errors. Inspected by the Receiver for HTTP status mapping and by
// the orchestrator for branching.
var (
	ErrInvalidSignature = errors.New("fivetran: HMAC signature mismatch")
	ErrDuplicateEvent   = errors.New("fivetran: event already processed")
	ErrMalformedPayload = errors.New("fivetran: malformed webhook payload")
)

// Validate confirms the Anomaly is well-formed before the orchestrator
// consumes it. Wraps ErrMalformedPayload so callers can errors.Is.
func (a *Anomaly) Validate() error {
	if a == nil {
		return fmt.Errorf("%w: nil Anomaly", ErrMalformedPayload)
	}
	if a.EventID == "" {
		return fmt.Errorf("%w: missing EventID", ErrMalformedPayload)
	}
	if a.EventType != "schema_change" && a.EventType != "transformation_failed" {
		return fmt.Errorf("%w: unsupported EventType %q", ErrMalformedPayload, a.EventType)
	}
	if a.SchemaName == "" {
		return fmt.Errorf("%w: missing SchemaName", ErrMalformedPayload)
	}
	if a.TableName == "" {
		return fmt.Errorf("%w: missing TableName", ErrMalformedPayload)
	}
	if a.Timestamp.IsZero() {
		return fmt.Errorf("%w: missing Timestamp", ErrMalformedPayload)
	}
	return nil
}
