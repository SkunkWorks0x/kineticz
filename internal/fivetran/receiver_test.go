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

const validBody = `{"event_id":"syn_abc123","event_type":"schema_change","schema_name":"users_schema","table_name":"users","column_changes":[{"column":"created_at","action":"added","new_type":"timestamp"}],"timestamp":"2026-05-26T10:00:00Z"}`

func signBody(body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
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
			name:           "valid_signature_and_payload_returns_202",
			body:           validBody,
			signature:      signBody([]byte(validBody)),
			wantStatus:     http.StatusAccepted,
			wantAuditCount: 1,
			wantPipeline:   true,
		},
		{
			name:       "invalid_signature_returns_401",
			body:       validBody,
			signature:  "deadbeef",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "missing_signature_returns_401",
			body:       validBody,
			signature:  "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:           "duplicate_event_returns_200_and_skips_pipeline",
			body:           validBody,
			signature:      signBody([]byte(validBody)),
			preExistsID:    "syn_abc123",
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
			name:       "valid_json_but_validate_fails_returns_400",
			body:       `{"event_id":"","event_type":"schema_change","schema_name":"s","table_name":"t","timestamp":"2026-05-26T10:00:00Z"}`,
			signature:  signBody([]byte(`{"event_id":"","event_type":"schema_change","schema_name":"s","table_name":"t","timestamp":"2026-05-26T10:00:00Z"}`)),
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
				t.Errorf("audit count = %d, want %d", len(got), tc.wantAuditCount)
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
	req := httptest.NewRequest(http.MethodPost, "/webhooks/fivetran", bytes.NewReader([]byte(validBody)))
	req.Header.Set(SignatureHeader, signBody([]byte(validBody)))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	// Wait for the goroutine to panic, recover, and audit.
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

func TestReceiver_AcceptedResponseIncludesCorrelationToken(t *testing.T) {
	store := &fakeStore{}
	r := NewReceiver(store, secret, func(context.Context, Anomaly) {})

	req := httptest.NewRequest(http.MethodPost, "/webhooks/fivetran", bytes.NewReader([]byte(validBody)))
	req.Header.Set(SignatureHeader, signBody([]byte(validBody)))
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"correlation_token":"`) {
		t.Errorf("response missing correlation_token: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"event_id":"syn_abc123"`) {
		t.Errorf("response missing event_id: %s", rec.Body.String())
	}
}

func TestAnomaly_Validate(t *testing.T) {
	cases := []struct {
		name      string
		a         *Anomaly
		wantErrIs error
	}{
		{
			name: "valid_schema_change",
			a: &Anomaly{
				EventID:    "id1",
				EventType:  "schema_change",
				SchemaName: "s",
				TableName:  "t",
				Timestamp:  time.Unix(1716696000, 0),
			},
		},
		{
			name: "unsupported_event_type",
			a: &Anomaly{
				EventID:    "id1",
				EventType:  "row_changed",
				SchemaName: "s",
				TableName:  "t",
				Timestamp:  time.Unix(1716696000, 0),
			},
			wantErrIs: ErrMalformedPayload,
		},
		{
			name:      "nil_anomaly",
			a:         nil,
			wantErrIs: ErrMalformedPayload,
		},
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
