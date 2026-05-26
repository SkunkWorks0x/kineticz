package arize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/skunkworks0x/kineticz/internal/httputil"
)

// Placeholder endpoint. Verify against Arize Phoenix or Arize AX evaluation
// API docs before production deployment.
const evaluatePath = "/v1/evaluate"

// Client is the interface evaluate depends on. Concrete impl: httpClient.
type Client interface {
	Evaluate(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error)
}

// EvaluateRequest is what the evaluate gate submits to Arize for boolean
// rubric scoring.
type EvaluateRequest struct {
	Diff    []byte
	Context map[string]any
}

// EvaluateResponse carries the rubric's pass/fail verdict. On fail, the
// Evaluate call also returns ErrRubricFailed so callers can branch with
// errors.Is while still inspecting the rationale.
type EvaluateResponse struct {
	Pass      bool
	Rationale string
}

type ArizeError struct {
	StatusCode int
	Body       string
}

func (e *ArizeError) Error() string {
	return fmt.Sprintf("arize: HTTP %d: %s", e.StatusCode, e.Body)
}

var (
	ErrArizeUnavailable = errors.New("arize: service unavailable")
	ErrRubricFailed     = errors.New("arize: rubric returned fail")
)

type httpClient struct {
	http     *http.Client
	baseURL  string
	apiKey   string
	rubricID string
	backoff  time.Duration
	retries  int
}

// NewHTTPClient constructs an Arize Client over the given HTTP transport.
// rubricID identifies the boolean rubric Kineticz registered with Arize.
func NewHTTPClient(h *http.Client, baseURL, apiKey, rubricID string) *httpClient {
	return &httpClient{
		http:     h,
		baseURL:  baseURL,
		apiKey:   apiKey,
		rubricID: rubricID,
		backoff:  100 * time.Millisecond,
		retries:  3,
	}
}

func (c *httpClient) Evaluate(ctx context.Context, req EvaluateRequest) (*EvaluateResponse, error) {
	body, err := json.Marshal(map[string]any{
		"rubric_id": c.rubricID,
		"diff":      string(req.Diff),
		"context":   req.Context,
	})
	if err != nil {
		return nil, fmt.Errorf("arize: marshal: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+evaluatePath, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("arize: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := httputil.Do(ctx, c.http, httpReq, c.retries, c.backoff)
	if err != nil {
		if errors.Is(err, httputil.ErrUnavailable) {
			return nil, fmt.Errorf("%w: %v", ErrArizeUnavailable, err)
		}
		return nil, err
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("arize: read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, &ArizeError{StatusCode: resp.StatusCode, Body: string(rb)}
	}

	var parsed struct {
		Pass      bool   `json:"pass"`
		Rationale string `json:"rationale"`
	}
	if err := json.Unmarshal(rb, &parsed); err != nil {
		return nil, fmt.Errorf("arize: decode: %w", err)
	}

	out := &EvaluateResponse{Pass: parsed.Pass, Rationale: parsed.Rationale}
	if !parsed.Pass {
		return out, fmt.Errorf("%w: %s", ErrRubricFailed, parsed.Rationale)
	}
	return out, nil
}
