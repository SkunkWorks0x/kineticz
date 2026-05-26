package httputil

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestDo(t *testing.T) {
	cases := []struct {
		name        string
		handler     http.HandlerFunc
		maxRetries  int
		ctxTimeout  time.Duration
		wantErrIs   error
		wantStatus  int
		wantAttempts int32
	}{
		{
			name: "2xx_returns_immediately",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
			maxRetries:   3,
			wantStatus:   http.StatusOK,
			wantAttempts: 1,
		},
		{
			name: "4xx_does_not_retry",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
			},
			maxRetries:   3,
			wantStatus:   http.StatusBadRequest,
			wantAttempts: 1,
		},
		{
			name: "5xx_exhausts_retries_and_returns_ErrUnavailable",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
			},
			maxRetries:   3,
			wantErrIs:    ErrUnavailable,
			wantAttempts: 3,
		},
		{
			name: "5xx_then_200_succeeds_on_retry",
			handler: makeFlakyHandler(2),
			maxRetries:   3,
			wantStatus:   http.StatusOK,
			wantAttempts: 3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var attempts int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(&attempts, 1)
				tc.handler(w, r)
			})
			server := httptest.NewServer(handler)
			defer server.Close()

			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL, bytes.NewReader([]byte(`{}`)))
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}

			resp, err := Do(context.Background(), server.Client(), req, tc.maxRetries, 1*time.Millisecond)

			if tc.wantErrIs != nil {
				if !errors.Is(err, tc.wantErrIs) {
					t.Fatalf("err = %v, want errors.Is(%v)", err, tc.wantErrIs)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if resp.StatusCode != tc.wantStatus {
					t.Errorf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
				}
				_ = resp.Body.Close()
			}
			if got := atomic.LoadInt32(&attempts); got != tc.wantAttempts {
				t.Errorf("attempts = %d, want %d", got, tc.wantAttempts)
			}
		})
	}
}

func TestDo_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	cancel()

	_, err = Do(ctx, server.Client(), req, 5, 1*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// makeFlakyHandler returns a handler that fails the first `failures` requests
// with 503, then returns 200 for all subsequent requests.
func makeFlakyHandler(failures int32) http.HandlerFunc {
	var count int32
	return func(w http.ResponseWriter, r *http.Request) {
		c := atomic.AddInt32(&count, 1)
		if c <= failures {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}
