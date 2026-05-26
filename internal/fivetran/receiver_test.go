package fivetran

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/skunkworks0x/kineticz/internal/audit"
)

type fakeStore struct {
	mu        sync.Mutex
	entries   []recordedEvent
	appendErr error
	preExists string // when non-empty, AppendWithEvent returns ErrDuplicateEvent for this event_id
}

type recordedEvent struct {
	Action  string
	EventID string
	Payload []byte
}

func (s *fakeStore) Append(_ context.Context, action string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.appendErr != nil {
		return s.appendErr
	}
	s.entries = append(s.entries, recordedEvent{Action: action, Payload: append([]byte(nil), payload...)})
	return nil
}

func (s *fakeStore) AppendWithEvent(_ context.Context, action string, payload []byte, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.appendErr != nil {
		return s.appendErr
	}
	if eventID != "" {
		if s.preExists == eventID {
			return audit.ErrDuplicateEvent
		}
		for _, e := range s.entries {
			if e.EventID == eventID {
				return audit.ErrDuplicateEvent
			}
		}
	}
	s.entries = append(s.entries, recordedEvent{Action: action, EventID: eventID, Payload: append([]byte(nil), payload...)})
	return nil
}

func (s *fakeStore) HasEntry(_ context.Context, eventID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if eventID == "" {
		return false, nil
	}
	for _, e := range s.entries {
		if e.EventID == eventID {
			return true, nil
		}
	}
	return false, nil
}

func (s *fakeStore) snapshot() []recordedEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]recordedEvent, len(s.entries))
	copy(out, s.entries)
	return out
}

const secret = "test-shared-secret"

// triggerBody is a real Fivetran sync_end webhook with FAILURE_WITH_TASK,
// which TriggersRepair() reports as true.
const triggerBody = `{"event":"sync_end","created":"2026-05-26T10:00:00.000Z","connector_type":"postgres","connector_id":"conn_abc123","connector_name":"users_pg","sync_id":"syn_def456","destination_group_id":"warehouse_main","data":{"status":"FAILURE_WITH_TASK"}}`

// ackOnlyBody is a real Fivetran sync_start webhook that should be acked
// with 200 OK but not trigger the pipeline.
const ackOnlyBody = `{"event":"sync_start","created":"2026-05-26T10:00:00.000Z","connector_type":"postgres","connector_id":"conn_abc123","connector_name":"users_pg","sync_id":"syn_def456","destination_group_id":"warehouse_main"}`

func signBody(body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	// Fivetran sends uppercase hex; mimic that here so the test exercises
	// the case-insensitive comparison path in verifySignature.
	return strings.ToUpper(hex.EncodeToString(mac.Sum(nil)))
}

func TestReceiver(t *testing.T) {
	cases := []struct {
		name           string
		body           string
		signature      string
		preExistsID    string
		wantStatus     int
		wantAuditCount int
		wantPipeline   bool
	}{
		{
			name:           "trigger_event_returns_202_and_runs_pipeline",
			body:           triggerBody,
			signature:      signBody([]byte(triggerBody)),
			wantStatus:     http.StatusAccepted,
			wantAuditCount: 1,
			wantPipeline:   true,
		},
		{
			name:           "non_trigger_event_returns_200_and_skips_pipeline",
			body:           ackOnlyBody,
			signature:      signBody([]byte(ackOnlyBody)),
			wantStatus:     http.StatusOK,
			wantAuditCount: 1, // still audit-recorded for visibility
			wantPipeline:   false,
		},
		{
			name:       "invalid_signature_returns_401",
			body:       triggerBody,
			signature:  "deadbeef",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "missing_signature_returns_401",
			body:       triggerBody,
			signature:  "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:           "duplicate_event_returns_200_and_skips_pipeline",
			body:           triggerBody,
			signature:      signBody([]byte(triggerBody)),
			preExistsID:    "sync_end:syn_def456",
			wantStatus:     http.StatusOK,
			wantAuditCount: 0,
		},
		{
			name:       "malformed_json_returns_400",
			body:       `not json`,
			signature:  signBody([]byte(`not json`)),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_required_fields_returns_400",
			body:       `{"event":"sync_end"}`,
			signature:  signBody([]byte(`{"event":"sync_end"}`)),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeStore{preExists: tc.preExistsID}
			pipelineCalled := make(chan struct{}, 1)
			pipeline := func(_ context.Context, _ Anomaly) {
				pipelineCalled <- struct{}{}
			}
			r := NewReceiver(store, secret, pipeline)

			req := httptest.NewRequest(http.MethodPost, "/webhooks/fivetran", bytes.NewReader([]byte(tc.body)))
			if tc.signature != "" {
				req.Header.Set(SignatureHeader, tc.signature)
			}
			rec := httptest.NewRecorder()
			r.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}

			got := store.snapshot()
			if len(got) != tc.wantAuditCount {
				t.Errorf("audit count = %d, want %d (entries: %+v)", len(got), tc.wantAuditCount, got)
			}

			if tc.wantPipeline {
				select {
				case <-pipelineCalled:
				case <-time.After(2 * time.Second):
					t.Error("pipeline goroutine never invoked")
				}
			} else {
				select {
				case <-pipelineCalled:
					t.Error("pipeline was invoked but should not have been")
				case <-time.After(50 * time.Millisecond):
				}
			}
		})
	}
}

func TestReceiver_PanicInPipelineIsRecoveredAndAudited(t *testing.T) {
	store := &fakeStore{}
	r := NewReceiver(store, secret, func(context.Context, Anomaly) {
		panic("simulated downstream crash")
	})
	req := httptest.NewRequest(http.MethodPost, "/webhooks/fivetran", bytes.NewReader([]byte(triggerBody)))
	req.Header.Set(SignatureHeader, signBody([]byte(triggerBody)))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("PIPELINE_PANICKED audit never written. actions: %v", store.snapshot())
		case <-time.After(20 * time.Millisecond):
		}
		for _, e := range store.snapshot() {
			if e.Action == "PIPELINE_PANICKED" {
				return
			}
		}
	}
}

func TestReceiver_OversizeBodyReturns400(t *testing.T) {
	store := &fakeStore{}
	r := NewReceiver(store, secret, func(context.Context, Anomaly) {})
	huge := bytes.Repeat([]byte("a"), MaxBodyBytes+1)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/fivetran", bytes.NewReader(huge))
	req.Header.Set(SignatureHeader, signBody(huge))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 on oversize", rec.Code)
	}
}

func TestReceiver_AcceptedResponseIncludesCorrelationToken(t *testing.T) {
	store := &fakeStore{}
	r := NewReceiver(store, secret, func(context.Context, Anomaly) {})

	req := httptest.NewRequest(http.MethodPost, "/webhooks/fivetran", bytes.NewReader([]byte(triggerBody)))
	req.Header.Set(SignatureHeader, signBody([]byte(triggerBody)))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"correlation_token":"`) {
		t.Errorf("response missing correlation_token: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"event_id":"sync_end:syn_def456"`) {
		t.Errorf("response missing composite event_id: %s", rec.Body.String())
	}
}

func TestAnomaly_TriggersRepair(t *testing.T) {
	cases := []struct {
		name string
		a    Anomaly
		want bool
	}{
		{"sync_end_with_FAILURE_WITH_TASK_triggers", Anomaly{Event: "sync_end", Data: map[string]any{"status": "FAILURE_WITH_TASK"}}, true},
		{"sync_end_with_SUCCESSFUL_does_not_trigger", Anomaly{Event: "sync_end", Data: map[string]any{"status": "SUCCESSFUL"}}, false},
		{"transformation_failed_triggers", Anomaly{Event: "transformation_failed"}, true},
		{"sync_start_does_not_trigger", Anomaly{Event: "sync_start"}, false},
		{"connection_successful_does_not_trigger", Anomaly{Event: "connection_successful"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.TriggersRepair(); got != tc.want {
				t.Errorf("TriggersRepair = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestAnomaly_EventID(t *testing.T) {
	a := Anomaly{Event: "sync_end", SyncID: "syn_abc"}
	if got := a.EventID(); got != "sync_end:syn_abc" {
		t.Errorf("EventID = %q, want sync_end:syn_abc", got)
	}
}

func TestAnomaly_Validate(t *testing.T) {
	cases := []struct {
		name      string
		a         *Anomaly
		wantErrIs error
	}{
		{
			name: "valid",
			a: &Anomaly{
				Event:       "sync_end",
				ConnectorID: "conn_x",
				SyncID:      "syn_y",
			},
		},
		{"nil", nil, ErrMalformedPayload},
		{"missing_event", &Anomaly{ConnectorID: "c", SyncID: "s"}, ErrMalformedPayload},
		{"missing_connector_id", &Anomaly{Event: "sync_end", SyncID: "s"}, ErrMalformedPayload},
		{"missing_sync_id", &Anomaly{Event: "sync_end", ConnectorID: "c"}, ErrMalformedPayload},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.a.Validate()
			if tc.wantErrIs == nil {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErrIs.Error()) {
				t.Errorf("err = %v, want contains %v", err, tc.wantErrIs)
			}
		})
	}
}
