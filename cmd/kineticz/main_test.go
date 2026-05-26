package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthHandler(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		wantStatus int
		wantStatusField string
	}{
		{"GET_returns_ok", http.MethodGet, http.StatusOK, "ok"},
		{"POST_also_returns_ok", http.MethodPost, http.StatusOK, "ok"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "/health", nil)
			rec := httptest.NewRecorder()
			healthHandler(rec, req)

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("content-type = %q, want application/json", got)
			}
			var body map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body["status"] != tc.wantStatusField {
				t.Errorf("status field = %q, want %q", body["status"], tc.wantStatusField)
			}
		})
	}
}

func TestGetenvFallback(t *testing.T) {
	t.Setenv("KINETICZ_TEST_SET", "actual")
	if got := getenv("KINETICZ_TEST_SET", "fallback"); got != "actual" {
		t.Errorf("getenv set: %q, want actual", got)
	}
	if got := getenv("KINETICZ_TEST_UNSET_XYZ", "fallback"); got != "fallback" {
		t.Errorf("getenv unset: %q, want fallback", got)
	}
}
