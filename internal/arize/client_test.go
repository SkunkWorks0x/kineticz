package arize

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newTestClient(server *httptest.Server) *httpClient {
	return &httpClient{
		http:     server.Client(),
		baseURL:  server.URL,
		apiKey:   "test-api-key",
		rubricID: "kineticz-rubric",
		backoff:  1 * time.Millisecond,
		retries:  3,
	}
}

func TestEvaluate(t *testing.T) {
	cases := []struct {
		name      string
		handler   http.HandlerFunc
		wantPass  bool
		wantErrIs error
	}{
		{
			name: "pass_returns_response",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/evaluate" {
					http.Error(w, "wrong path", http.StatusNotFound)
					return
				}
				if r.Header.Get("Authorization") != "Bearer test-api-key" {
					http.Error(w, "auth missing", http.StatusUnauthorized)
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"pass":true,"rationale":"checks pass"}`))
			},
			wantPass: true,
		},
		{
			name: "fail_returns_response_and_ErrRubricFailed",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"pass":false,"rationale":"signature changed"}`))
			},
			wantPass:  false,
			wantErrIs: ErrRubricFailed,
		},
		{
			name: "5xx_exhausts_retries_returns_ErrArizeUnavailable",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
			},
			wantErrIs: ErrArizeUnavailable,
		},
		{
			name: "4xx_returns_ArizeError_no_retry",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":"missing rubric"}`))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.handler)
			defer server.Close()
			c := newTestClient(server)

			resp, err := c.Evaluate(context.Background(), EvaluateRequest{
				Diff:    []byte("diff --git a/x b/x\n@@ ..."),
				Context: map[string]any{"contract": "users_v1"},
			})

			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, want errors.Is(%v)", err, tc.wantErrIs)
				}
				if tc.wantErrIs == ErrRubricFailed {
					if resp == nil || resp.Pass != false {
						t.Errorf("expected response with Pass=false on fail")
					}
				}
			} else if tc.name == "4xx_returns_ArizeError_no_retry" {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				var aerr *ArizeError
				if !errors.As(err, &aerr) {
					t.Errorf("err = %v, want *ArizeError", err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				if resp.Pass != tc.wantPass {
					t.Errorf("Pass = %v, want %v", resp.Pass, tc.wantPass)
				}
			}
		})
	}
}
