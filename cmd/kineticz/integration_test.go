//go:build integration

package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/skunkworks0x/kineticz/internal/audit"
	"github.com/skunkworks0x/kineticz/internal/commit"
	"github.com/skunkworks0x/kineticz/internal/dynatrace"
	"github.com/skunkworks0x/kineticz/internal/elastic"
	"github.com/skunkworks0x/kineticz/internal/engine/diagnose"
	"github.com/skunkworks0x/kineticz/internal/evaluate"
	"github.com/skunkworks0x/kineticz/internal/fivetran"
	"github.com/skunkworks0x/kineticz/internal/gemini"
	"github.com/skunkworks0x/kineticz/internal/gitlab"
	"github.com/skunkworks0x/kineticz/internal/phoenix"
	"github.com/skunkworks0x/kineticz/internal/repair"
)

const intSecret = "integration-secret"

const intWebhookBody = `{"event":"sync_end","created":"2026-05-26T10:00:00.000Z","connector_type":"postgres","connector_id":"conn_int","connector_name":"users","sync_id":"syn_int","destination_group_id":"warehouse_main","data":{"status":"FAILURE_WITH_TASK"}}`

const intOrigFile = "package pipeline\n\nfunc Greeting(name string) string {\n\tgreet := \"Hello\"\n\treturn greet + \", \" + name\n}\n"

const intDiff = `diff --git a/internal/pipeline/users.go b/internal/pipeline/users.go
--- a/internal/pipeline/users.go
+++ b/internal/pipeline/users.go
@@ -1,6 +1,6 @@
 package pipeline

 func Greeting(name string) string {
-	greet := "Hello"
+	greet := "Hi"
 	return greet + ", " + name
 }
`

type mockStore struct {
	mu      sync.Mutex
	entries []audit.Entry
	actions []string
}

func (s *mockStore) Append(_ context.Context, action string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.actions = append(s.actions, action)
	s.entries = append(s.entries, audit.Entry{Action: action, Payload: append([]byte(nil), payload...)})
	return nil
}

func (s *mockStore) AppendWithThought(ctx context.Context, action string, payload []byte, _ string) error {
	return s.Append(ctx, action, payload)
}

func (s *mockStore) AppendWithEvent(ctx context.Context, action string, payload []byte, eventID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.actions = append(s.actions, action)
	s.entries = append(s.entries, audit.Entry{Action: action, Payload: append([]byte(nil), payload...), SourceEventID: eventID})
	return nil
}

func (s *mockStore) HasEntry(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (s *mockStore) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.actions))
	copy(out, s.actions)
	return out
}

type stubTarget struct{ content []byte }

func (s *stubTarget) Read(_ context.Context, _ string) ([]byte, error) { return s.content, nil }

type intIndexer struct{}

func (intIndexer) Index(_ context.Context, _ string, _ []byte) error { return nil }

func TestFullPipeline_HappyPath(t *testing.T) {
	store := &mockStore{}

	esMock := &elastic.Mock{
		LookupContractFn: func(context.Context, elastic.ContractQuery) (*elastic.ContractContext, error) {
			return &elastic.ContractContext{
				YAMLDefinition:     "name: users\n",
				MitigationPatterns: []elastic.Mitigation{{DiffID: "diff-1", Score: 0.5, Summary: "add column"}},
				RRFConfidence:      0.5,
			}, nil
		},
	}
	dtMock := &dynatrace.Mock{
		QueryConsumerHealthFn: func(context.Context, int64, int64) ([]dynatrace.ConsumerHealth, error) {
			return []dynatrace.ConsumerHealth{{Consumer: "svc-a", ErrorRate: 0.01, LatencyP95Ms: 100}}, nil
		},
	}
	gMock := &gemini.Mock{
		GenerateFn: func(context.Context, gemini.GenerateRequest) (*gemini.Response, error) {
			return &gemini.Response{Candidates: []gemini.Candidate{{
				Content: gemini.Content{Parts: []gemini.Part{
					{Text: "Analyzed schema drift.", Thought: true},
					{Text: intDiff},
				}},
			}}}, nil
		},
	}
	glMock := &gitlab.Mock{
		CreateCommitFn: func(context.Context, gitlab.CommitRequest) (string, error) {
			return "sha-integration", nil
		},
		CreateMRFn: func(context.Context, gitlab.MRRequest) (*gitlab.MRResult, error) {
			return &gitlab.MRResult{MRIID: 99, MRURL: "https://gitlab.example/mr/99"}, nil
		},
	}

	deps := Deps{
		EventStore:     store,
		Audit:          store,
		Diagnose:       diagnose.New(esMock, dtMock, store),
		Repair:         repair.New(gMock, store, &stubTarget{content: []byte(intOrigFile)}, commit.ApplyDiff),
		Evaluate:       evaluate.New(store, intIndexer{}),
		Commit:         commit.New(glMock, store),
		ProjectID:      "kineticz/pipelines",
		TargetBranch:   "main",
		FivetranSecret: intSecret,
	}

	handler := WireHandler(deps)

	mac := hmac.New(sha256.New, []byte(intSecret))
	mac.Write([]byte(intWebhookBody))
	sig := hex.EncodeToString(mac.Sum(nil))

	req := httptest.NewRequest(http.MethodPost, "/webhooks/fivetran", bytes.NewReader([]byte(intWebhookBody)))
	req.Header.Set(fivetran.SignatureHeader, sig)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}

	// Wait for the pipeline goroutine to complete and write PIPELINE_COMPLETE.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("pipeline did not complete in time. actions: %v", store.snapshot())
		case <-time.After(50 * time.Millisecond):
		}
		actions := store.snapshot()
		for _, a := range actions {
			if a == "PIPELINE_COMPLETE" {
				goto done
			}
			if a == "PIPELINE_FAILED" {
				t.Fatalf("pipeline failed. actions: %v", actions)
			}
		}
	}
done:
	actions := store.snapshot()
	// Required ordering: FIVETRAN_RECEIVED first, PIPELINE_COMPLETE last.
	if actions[0] != "FIVETRAN_RECEIVED" {
		t.Errorf("first action = %s, want FIVETRAN_RECEIVED", actions[0])
	}
	if actions[len(actions)-1] != "PIPELINE_COMPLETE" {
		t.Errorf("last action = %s, want PIPELINE_COMPLETE", actions[len(actions)-1])
	}
	// Required stages appeared.
	want := []string{"DIAGNOSIS_OK", "REPAIR_APPROVED", "EVALUATE_PASS", "COMMIT_OK", "MR_CREATED"}
	for _, w := range want {
		found := false
		for _, a := range actions {
			if a == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing required audit action %q. saw: %v", w, actions)
		}
	}
}

// A dead Phoenix MCP server (spawn failure) must not touch the apply path: the
// pipeline still completes and the diagnose stage records the degraded mode.
func TestFullPipeline_PhoenixDead(t *testing.T) {
	store := &mockStore{}

	esMock := &elastic.Mock{
		LookupContractFn: func(context.Context, elastic.ContractQuery) (*elastic.ContractContext, error) {
			return &elastic.ContractContext{YAMLDefinition: "name: users\n"}, nil
		},
	}
	dtMock := &dynatrace.Mock{
		QueryConsumerHealthFn: func(context.Context, int64, int64) ([]dynatrace.ConsumerHealth, error) {
			return []dynatrace.ConsumerHealth{{Consumer: "svc-a", ErrorRate: 0.01, LatencyP95Ms: 100}}, nil
		},
	}
	gMock := &gemini.Mock{
		GenerateFn: func(context.Context, gemini.GenerateRequest) (*gemini.Response, error) {
			return &gemini.Response{Candidates: []gemini.Candidate{{
				Content: gemini.Content{Parts: []gemini.Part{{Text: intDiff}}},
			}}}, nil
		},
	}
	glMock := &gitlab.Mock{
		CreateCommitFn: func(context.Context, gitlab.CommitRequest) (string, error) { return "sha-dead-phoenix", nil },
		CreateMRFn: func(context.Context, gitlab.MRRequest) (*gitlab.MRResult, error) {
			return &gitlab.MRResult{MRIID: 7, MRURL: "https://gitlab.example/mr/7"}, nil
		},
	}

	// NodeDialer pointed at a binary that does not exist: connect fails, the
	// client reconnects once, fails again, and the leg degrades.
	deadPhoenix := phoenix.New(phoenix.NodeDialer("/nonexistent/kineticz-node", "/nonexistent/index.js", nil), "default")

	deps := Deps{
		EventStore:     store,
		Audit:          store,
		Diagnose:       diagnose.New(esMock, dtMock, store, diagnose.WithPhoenix(deadPhoenix, "default")),
		Repair:         repair.New(gMock, store, &stubTarget{content: []byte(intOrigFile)}, commit.ApplyDiff),
		Evaluate:       evaluate.New(store, intIndexer{}),
		Commit:         commit.New(glMock, store),
		ProjectID:      "kineticz/pipelines",
		TargetBranch:   "main",
		FivetranSecret: intSecret,
	}

	handler := WireHandler(deps)
	mac := hmac.New(sha256.New, []byte(intSecret))
	mac.Write([]byte(intWebhookBody))
	sig := hex.EncodeToString(mac.Sum(nil))

	req := httptest.NewRequest(http.MethodPost, "/webhooks/fivetran", bytes.NewReader([]byte(intWebhookBody)))
	req.Header.Set(fivetran.SignatureHeader, sig)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", rec.Code, rec.Body.String())
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("pipeline did not complete. actions: %v", store.snapshot())
		case <-time.After(50 * time.Millisecond):
		}
		done := false
		for _, a := range store.snapshot() {
			if a == "PIPELINE_COMPLETE" {
				done = true
			}
			if a == "PIPELINE_FAILED" {
				t.Fatalf("pipeline failed with dead Phoenix. actions: %v", store.snapshot())
			}
		}
		if done {
			break
		}
	}

	actions := store.snapshot()
	for _, w := range []string{"DIAGNOSIS_OK", "PHOENIX_HISTORY_DEGRADED", "PIPELINE_COMPLETE"} {
		found := false
		for _, a := range actions {
			if a == w {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing audit action %q with dead Phoenix. saw: %v", w, actions)
		}
	}
}
