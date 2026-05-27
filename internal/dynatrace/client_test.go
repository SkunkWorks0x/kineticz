package dynatrace

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/skunkworks0x/kineticz/internal/corr"
)

type recordingAudit struct {
	mu      sync.Mutex
	entries []recordedAuditEntry
}

type recordedAuditEntry struct {
	Action  string
	Payload []byte
	Token   corr.CorrelationToken
}

func (r *recordingAudit) Append(ctx context.Context, action string, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	tok, _ := corr.FromContext(ctx)
	r.entries = append(r.entries, recordedAuditEntry{
		Action:  action,
		Payload: append([]byte(nil), payload...),
		Token:   tok,
	})
	return nil
}

func loadFixture(t *testing.T, name string) []byte {
	t.Helper()
	p := filepath.Join("testdata", name)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read %s: %v", p, err)
	}
	return b
}

func newTestClient(t *testing.T, baseURL string, aw *recordingAudit) *client {
	t.Helper()
	return &client{
		http:    http.DefaultClient,
		audit:   aw,
		baseURL: baseURL,
		token:   "test-dt-token",
		backoff: 1 * time.Millisecond,
		retries: 3,
	}
}

func TestIngestBizevent(t *testing.T) {
	ingestFix := loadFixture(t, "ingest_accepted.json")

	cases := []struct {
		name         string
		ctxToken     bool
		token        corr.CorrelationToken
		handler      http.HandlerFunc
		wantErrIs    error
		wantAuditOps []string
	}{
		{
			name:     "happy_path_sets_id_to_token_and_audits_ok",
			ctxToken: true,
			token:    "tok-ingest",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v2/bizevents/ingest" {
					http.Error(w, "wrong path", http.StatusNotFound)
					return
				}
				var events []map[string]any
				if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
					http.Error(w, "decode error", http.StatusBadRequest)
					return
				}
				if len(events) != 1 {
					http.Error(w, "expected 1 event", http.StatusBadRequest)
					return
				}
				if events[0]["id"] != "tok-ingest" {
					http.Error(w, "id mismatch", http.StatusBadRequest)
					return
				}
				if events[0]["kineticz.correlation_token"] != "tok-ingest" {
					http.Error(w, "custom metadata missing", http.StatusBadRequest)
					return
				}
				w.WriteHeader(http.StatusAccepted)
				_, _ = w.Write(ingestFix)
			},
			wantAuditOps: []string{"DYNATRACE_INGEST_OK"},
		},
		{
			name:     "missing_correlation_token_returns_ErrCorrelationMissing",
			ctxToken: false,
			handler: func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("should not reach HTTP, got %s %s", r.Method, r.URL.Path)
				http.Error(w, "unexpected", http.StatusBadRequest)
			},
			wantErrIs:    ErrCorrelationMissing,
			wantAuditOps: nil,
		},
		{
			name:     "5xx_exhausts_retries_and_returns_ErrTelemetryUnavailable",
			ctxToken: true,
			token:    "tok-5xx",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte(`{"error":"upstream"}`))
			},
			wantErrIs:    ErrTelemetryUnavailable,
			wantAuditOps: []string{"DYNATRACE_INGEST_FAILED"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			aw := &recordingAudit{}
			c := newTestClient(t, server.URL, aw)

			ctx := context.Background()
			if tc.ctxToken {
				ctx = corr.WithToken(ctx, tc.token)
			}
			err := c.IngestBizevent(ctx, "sync.complete", map[string]any{"contract": "users_v1"})

			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, want errors.Is(%v)", err, tc.wantErrIs)
				}
			} else if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}

			aw.mu.Lock()
			defer aw.mu.Unlock()
			if len(aw.entries) != len(tc.wantAuditOps) {
				t.Fatalf("audit count = %d, want %d (%v)", len(aw.entries), len(tc.wantAuditOps), tc.wantAuditOps)
			}
			for i, want := range tc.wantAuditOps {
				if aw.entries[i].Action != want {
					t.Errorf("audit[%d].Action = %s, want %s", i, aw.entries[i].Action, want)
				}
				if tc.ctxToken && aw.entries[i].Token != tc.token {
					t.Errorf("audit[%d].Token = %q, want %q", i, aw.entries[i].Token, tc.token)
				}
			}
		})
	}
}

func TestQueryConsumerHealth(t *testing.T) {
	dqlFix := loadFixture(t, "dql_results.json")

	cases := []struct {
		name          string
		ctxToken      bool
		handler       http.HandlerFunc
		wantErrIs     error
		wantConsumers []string
		wantAuditOps  []string
	}{
		{
			name:     "happy_path_parses_records",
			ctxToken: true,
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v2/dql/query" {
					http.Error(w, "wrong path", http.StatusNotFound)
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(dqlFix)
			},
			wantConsumers: []string{"service-a", "service-b"},
			wantAuditOps:  []string{"DYNATRACE_DQL_OK"},
		},
		{
			name:     "missing_correlation_token_returns_ErrCorrelationMissing",
			ctxToken: false,
			handler: func(w http.ResponseWriter, r *http.Request) {
				t.Errorf("should not reach HTTP, got %s %s", r.Method, r.URL.Path)
				http.Error(w, "unexpected", http.StatusBadRequest)
			},
			wantErrIs:    ErrCorrelationMissing,
			wantAuditOps: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			aw := &recordingAudit{}
			c := newTestClient(t, server.URL, aw)

			ctx := context.Background()
			if tc.ctxToken {
				ctx = corr.WithToken(ctx, "tok-dql")
			}
			results, err := c.QueryConsumerHealth(ctx, 1716696000000, 1716696300000)

			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, want errors.Is(%v)", err, tc.wantErrIs)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				if len(results) != len(tc.wantConsumers) {
					t.Fatalf("len(results) = %d, want %d", len(results), len(tc.wantConsumers))
				}
				for i, want := range tc.wantConsumers {
					if results[i].Consumer != want {
						t.Errorf("results[%d].Consumer = %s, want %s", i, results[i].Consumer, want)
					}
				}
			}

			aw.mu.Lock()
			defer aw.mu.Unlock()
			if len(aw.entries) != len(tc.wantAuditOps) {
				t.Fatalf("audit count = %d, want %d (%v)", len(aw.entries), len(tc.wantAuditOps), tc.wantAuditOps)
			}
		})
	}
}
