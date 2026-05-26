package gemini

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/skunkworks0x/kineticz/internal/corr"
)

type recordingAudit struct {
	mu      sync.Mutex
	actions []string
}

func (r *recordingAudit) Append(_ context.Context, action string, _ []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.actions = append(r.actions, action)
	return nil
}

func staticToken(_ context.Context) (string, error) { return "fake-access-token", nil }

func TestVertexGenerate(t *testing.T) {
	cases := []struct {
		name         string
		handler      http.HandlerFunc
		wantErrIs    error
		wantOK       bool
		wantThought  string
		wantAuditOps []string
	}{
		{
			name: "happy_path_parses_response_and_thought",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, ":generateContent") {
					http.Error(w, "wrong path", http.StatusNotFound)
					return
				}
				if auth := r.Header.Get("Authorization"); auth != "Bearer fake-access-token" {
					http.Error(w, "missing auth", http.StatusUnauthorized)
					return
				}
				if r.Header.Get("X-Correlation-Token") != "tok-vertex" {
					http.Error(w, "missing correlation header", http.StatusBadRequest)
					return
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					http.Error(w, "decode", http.StatusBadRequest)
					return
				}
				si, _ := body["systemInstruction"].(map[string]any)
				if si == nil {
					http.Error(w, "missing systemInstruction", http.StatusBadRequest)
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{
					"candidates": [{
						"content": {"parts": [
							{"text": "Step A: examine schema.", "thought": true},
							{"text": "Step B: propose patch.", "thought": true},
							{"text": "diff --git a/x.go b/x.go\n@@ ..."}
						]},
						"finishReason": "STOP"
					}],
					"usageMetadata": {"promptTokenCount": 50, "candidatesTokenCount": 120, "totalTokenCount": 170}
				}`))
			},
			wantOK:       true,
			wantThought:  "Step A: examine schema.\nStep B: propose patch.",
			wantAuditOps: []string{"GEMINI_GENERATE_OK"},
		},
		{
			name: "4xx_returns_GeminiError_and_audits_failure",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"bad request"}`))
			},
			wantAuditOps: []string{"GEMINI_GENERATE_FAILED"},
		},
		{
			name: "5xx_exhausts_retries_and_returns_ErrGeminiUnavailable",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":"upstream"}`))
			},
			wantErrIs:    ErrGeminiUnavailable,
			wantAuditOps: []string{"GEMINI_GENERATE_FAILED"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()

			aw := &recordingAudit{}
			c := &vertexClient{
				http:      server.Client(),
				audit:     aw,
				baseURL:   server.URL,
				projectID: "test-project",
				location:  "us-central1",
				model:     "gemini-3.5-flash",
				tokenFunc: staticToken,
				backoff:   1 * time.Millisecond,
				retries:   3,
			}

			ctx := corr.WithToken(context.Background(), "tok-vertex")
			resp, err := c.Generate(ctx, GenerateRequest{
				SystemInstruction: "You are a deterministic patch generator.",
				UserPrompt:        "Generate a unified diff.",
				Temperature:       0.2,
				MaxOutputTokens:   1024,
			})

			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, want errors.Is(%v)", err, tc.wantErrIs)
				}
			} else if tc.wantOK {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if got := ExtractThought(resp); got != tc.wantThought {
					t.Errorf("thought = %q, want %q", got, tc.wantThought)
				}
				if resp.Usage.TotalTokenCount != 170 {
					t.Errorf("total tokens = %d, want 170", resp.Usage.TotalTokenCount)
				}
			} else {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				var gerr *GeminiError
				if !errors.As(err, &gerr) {
					t.Errorf("err = %v, want *GeminiError", err)
				}
			}

			aw.mu.Lock()
			defer aw.mu.Unlock()
			if len(aw.actions) != len(tc.wantAuditOps) {
				t.Fatalf("audits = %v, want %v", aw.actions, tc.wantAuditOps)
			}
			for i, want := range tc.wantAuditOps {
				if aw.actions[i] != want {
					t.Errorf("audit[%d] = %s, want %s", i, aw.actions[i], want)
				}
			}
		})
	}
}

func TestExtractThought_NilResponse(t *testing.T) {
	if got := ExtractThought(nil); got != "" {
		t.Errorf("nil response: got %q, want empty", got)
	}
}
