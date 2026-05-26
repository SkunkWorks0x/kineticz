package fivetran

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/skunkworks0x/kineticz/internal/corr"
)

// SignatureHeader is the HTTP header carrying the HMAC-SHA256 hex digest of
// the raw request body. Fivetran's actual header name may differ; verify
// against the partner's webhook docs before production.
const SignatureHeader = "X-Fivetran-Signature"

// PipelineFunc is the orchestrator callback invoked after a successful
// receipt. It runs the full diagnose → repair → evaluate → commit loop in
// a goroutine spawned from ServeHTTP. The receiver itself does not block on
// pipeline completion.
type PipelineFunc func(ctx context.Context, anomaly Anomaly)

// Receiver is the http.Handler for Fivetran webhook deliveries. It verifies
// the HMAC signature, enforces idempotency against the audit ledger, writes
// FIVETRAN_RECEIVED, mints a CorrelationToken, and hands off to the pipeline.
type Receiver struct {
	store    EventStore
	secret   []byte
	pipeline PipelineFunc
}

// NewReceiver wires a Receiver. secret is the shared HMAC secret with
// Fivetran. pipeline runs in a goroutine after each successful receipt and
// must be safe for concurrent execution.
func NewReceiver(store EventStore, secret string, pipeline PipelineFunc) *Receiver {
	return &Receiver{
		store:    store,
		secret:   []byte(secret),
		pipeline: pipeline,
	}
}

// ServeHTTP implements http.Handler. Status codes:
//
//	202 Accepted  — receipt persisted, pipeline running in background
//	200 OK        — duplicate event; ignored idempotently
//	400 Bad Request — malformed payload
//	401 Unauthorized — HMAC mismatch
//	500 Internal — audit write or store lookup failure
func (r *Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	defer req.Body.Close()

	if !r.verifySignature(body, req.Header.Get(SignatureHeader)) {
		http.Error(w, ErrInvalidSignature.Error(), http.StatusUnauthorized)
		return
	}

	anomaly, err := parseAnomaly(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := anomaly.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	already, err := r.store.HasEntry(req.Context(), anomaly.EventID)
	if err != nil {
		http.Error(w, fmt.Sprintf("idempotency lookup failed: %v", err), http.StatusInternalServerError)
		return
	}
	if already {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"duplicate","event_id":"` + anomaly.EventID + `"}`))
		return
	}

	token := corr.New()
	ctx := corr.WithToken(req.Context(), token)
	payload, _ := json.Marshal(anomaly)
	if err := r.store.AppendWithEvent(ctx, "FIVETRAN_RECEIVED", payload, anomaly.EventID); err != nil {
		http.Error(w, fmt.Sprintf("audit write failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Detach the context so the request returning does not cancel the
	// pipeline. The CorrelationToken value still propagates.
	if r.pipeline != nil {
		bgCtx := context.WithoutCancel(ctx)
		go r.pipeline(bgCtx, anomaly)
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"accepted","event_id":"` + anomaly.EventID + `","correlation_token":"` + string(token) + `"}`))
}

func (r *Receiver) verifySignature(body []byte, headerValue string) bool {
	if headerValue == "" || len(r.secret) == 0 {
		return false
	}
	mac := hmac.New(sha256.New, r.secret)
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(headerValue))
}

func parseAnomaly(body []byte) (Anomaly, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var out Anomaly
	if err := dec.Decode(&out); err != nil {
		return Anomaly{}, fmt.Errorf("%w: %v", ErrMalformedPayload, err)
	}
	return out, nil
}
