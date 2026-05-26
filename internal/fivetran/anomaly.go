package fivetran

import (
	"errors"
	"fmt"
	"time"
)

// Anomaly is the real Fivetran webhook payload, mapped 1:1 to the fields
// Fivetran documents at https://fivetran.com/docs/. The "anomaly" naming is
// historical; not every received webhook is a failure. TriggersRepair()
// reports whether this delivery should kick off the diagnose→repair pipeline.
type Anomaly struct {
	Event              string         `json:"event"`
	Created            time.Time      `json:"created"`
	ConnectorType      string         `json:"connector_type"`
	ConnectorID        string         `json:"connector_id"`
	ConnectorName      string         `json:"connector_name"`
	SyncID             string         `json:"sync_id"`
	DestinationGroupID string         `json:"destination_group_id"`
	Data               map[string]any `json:"data,omitempty"`
}

var (
	ErrInvalidSignature = errors.New("fivetran: HMAC signature mismatch")
	ErrDuplicateEvent   = errors.New("fivetran: event already processed")
	ErrMalformedPayload = errors.New("fivetran: malformed webhook payload")
)

// Validate confirms the webhook carries the fields required to deduplicate
// and route. Other fields are optional and propagated as-is.
func (a *Anomaly) Validate() error {
	if a == nil {
		return fmt.Errorf("%w: nil Anomaly", ErrMalformedPayload)
	}
	if a.Event == "" {
		return fmt.Errorf("%w: missing event", ErrMalformedPayload)
	}
	if a.ConnectorID == "" {
		return fmt.Errorf("%w: missing connector_id", ErrMalformedPayload)
	}
	if a.SyncID == "" {
		return fmt.Errorf("%w: missing sync_id", ErrMalformedPayload)
	}
	return nil
}

// EventID returns the natural deduplication key for the unique partial index.
// One sync emits both sync_start and sync_end events with the same sync_id,
// so the event type must be part of the key.
func (a *Anomaly) EventID() string {
	return a.Event + ":" + a.SyncID
}

// TriggersRepair reports whether this webhook should kick off the diagnose →
// repair → evaluate → commit loop. Non-triggering events are still audited
// for visibility but receive a 200 OK and no pipeline goroutine.
func (a *Anomaly) TriggersRepair() bool {
	if a.Event == "transformation_failed" {
		return true
	}
	if a.Event == "sync_end" {
		if status, ok := a.Data["status"].(string); ok && status == "FAILURE_WITH_TASK" {
			return true
		}
	}
	return false
}
