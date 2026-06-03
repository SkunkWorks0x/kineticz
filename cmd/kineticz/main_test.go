package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestPubkeyHandler(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	h := pubkeyHandler(pub)
	req := httptest.NewRequest(http.MethodGet, "/audit/pubkey", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body struct {
		Algorithm string `json:"algorithm"`
		Encoding  string `json:"encoding"`
		PublicKey string `json:"public_key"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Algorithm != "ed25519" {
		t.Errorf("algorithm = %q, want ed25519", body.Algorithm)
	}
	if body.Encoding != "hex" {
		t.Errorf("encoding = %q, want hex", body.Encoding)
	}
	decoded, err := hex.DecodeString(body.PublicKey)
	if err != nil {
		t.Fatalf("decode public_key hex: %v", err)
	}
	if string(decoded) != string(pub) {
		t.Errorf("decoded public key does not match input")
	}
}

func TestHealthHandler(t *testing.T) {
	cases := []struct {
		name            string
		method          string
		wantStatus      int
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

func TestValidConnectorName(t *testing.T) {
	cases := []struct {
		name string
		in   string
		ok   bool
	}{
		{"legit", "users", true},
		{"underscore", "users_pg", true},
		{"hyphen", "orders-2", true},
		{"empty", "", false},
		{"dotdot", "..", false},
		{"traversal", "../../x", false},
		{"slash", "a/b", false},
		{"backslash", `a\b`, false},
		{"absolute", "/etc/passwd", false},
		{"dot", "a.b", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validConnectorName(tc.in)
			if tc.ok && err != nil {
				t.Errorf("validConnectorName(%q) = %v, want nil", tc.in, err)
			}
			if !tc.ok && err == nil {
				t.Errorf("validConnectorName(%q) = nil, want error", tc.in)
			}
		})
	}
}

// A validated connector_name resolves under internal/pipeline/; the traversal
// name the validator rejects would otherwise escape that directory via Join.
func TestConnectorNameTargetStaysUnderPipeline(t *testing.T) {
	const base = "internal/pipeline"
	if got := filepath.Join("internal", "pipeline", "users"+".go"); !strings.HasPrefix(got, base+"/") {
		t.Errorf("legit target %q escaped %q", got, base)
	}
	if got := filepath.Join("internal", "pipeline", "../../x"+".go"); strings.HasPrefix(got, base+"/") {
		t.Errorf("expected %q to escape %q", got, base)
	}
	if err := validConnectorName("../../x"); err == nil {
		t.Error("validConnectorName must reject the traversal that escapes Join")
	}
}
