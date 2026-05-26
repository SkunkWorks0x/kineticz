package fivetran

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/skunkworks0x/kineticz/internal/audit"
	"github.com/skunkworks0x/kineticz/internal/corr"
)

// SignatureHeader carries the HMAC-SHA256 hex digest of the raw request body.
// `[unverified]` against the official Fivetran webhook docs as of this commit;
// the constant was chosen to mirror the industry-standard sha256 suffix used
// by GitHub and others. Confirm at https://fivetran.com/docs/ before final
// submission and rename if Fivetran uses a different value.
const SignatureHeader = "X-Fivetran-Signature-256"

// MaxBodyBytes caps inbound webhook payload size to prevent OOM via a flood
// of large requests. 1 MiB exceeds typical schema-change payloads with
// headroom; raise if real Fivetran payloads exceed this.
const MaxBodyBytes = 1 << 20

// PipelineTimeout caps total wall-clock time for the orchestration goroutine.
// The goroutine runs with a context detached from the request, so this is
// the only deadline preventing a hung Gemini call from leaking forever.
const PipelineTimeout = 5 * time.Minute

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
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, MaxBodyBytes))
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

	token := corr.New()
	ctx := corr.WithToken(req.Context(), token)
	payload, _ := json.Marshal(anomaly)

	// Atomic idempotency: the unique partial index on source_event_id makes
	// concurrent deliveries with the same event ID race in the database.
	// The loser receives audit.ErrDuplicateEvent and skips processing.
	err = r.store.AppendWithEvent(ctx, "FIVETRAN_RECEIVED", payload, anomaly.EventID)
	if errors.Is(err, audit.ErrDuplicateEvent) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"duplicate","event_id":"` + anomaly.EventID + `"}`))
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("audit write failed: %v", err), http.StatusInternalServerError)
		return
	}

	if r.pipeline != nil {
		r.spawnPipeline(ctx, anomaly)
	}

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"accepted","event_id":"` + anomaly.EventID + `","correlation_token":"` + string(token) + `"}`))
}

// spawnPipeline runs the orchestrator with a detached context (so the request
// returning does not cancel it) wrapped in a 5-minute timeout (so a hung
// upstream cannot leak the goroutine forever). Panics inside the pipeline
// are recovered and recorded as PIPELINE_PANICKED audit entries.
func (r *Receiver) spawnPipeline(ctx context.Context, anomaly Anomaly) {
	bgCtx := context.WithoutCancel(ctx)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				stack := debug.Stack()
				payload, _ := json.Marshal(map[string]any{
					"event_id": anomaly.EventID,
					"panic":    fmt.Sprintf("%v", p),
					"stack":    string(stack),
				})
				_ = r.store.Append(bgCtx, "PIPELINE_PANICKED", payload)
			}
		}()
		timedCtx, cancel := context.WithTimeout(bgCtx, PipelineTimeout)
		defer cancel()
		r.pipeline(timedCtx, anomaly)
	}()
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
